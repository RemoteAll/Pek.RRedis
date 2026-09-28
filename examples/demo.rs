//! Pek.RRedis 互通验证 Demo —— Rust 侧（pek-rredis）
//!
//! 与 C# 侧 `demo/csharp/PekRRedisDemo` 使用**同一套固定样本（fixtures）**：
//!
//! ```text
//! write   ：写入样本（字符串/整数/布尔/时间/JSON/哈希/列表/集合/有序集合/队列 + 本侧标记）
//! verify  ：读取并校验样本（含对方写入的数据），写下回执 receipt
//! push    ：向可靠队列推入 N 条消息
//! consume ：用可靠队列消费 N 条消息并确认
//! qstatus ：查看可靠队列、Ack 队列与消费者状态（可读到对方消费者写的 Status JSON）
//! lock    ：申请分布式锁、持有若干秒后释放（可与对方进程抢锁）
//! stream-push    ：向 Stream 写入 N 条消息（奇数为基元、偶数为对象）
//! stream-consume ：用消费组消费 N 条并确认（--no-ack 留作死信）
//! stream-status  ：查看流长度 / 消费组 / 挂起 / 消费者
//! pubsub-publish / pubsub-subscribe ：跨语言 PubSub（普通/模式/分片）
//! write-advanced / verify-advanced ：高级 API 面互通（GETEX/BITFIELD/HGETDEL/LMOVE/SMISMEMBER/ZMPOP/FUNCTION 等）
//! verify-ops / reset-ops / verify-ops-empty ：运维 API 面互通（SLOWLOG/LATENCY）
//! write-async / verify-async / push-async / consume-async ：tokio 异步包装层跨语言互通
//! deferred-add / deferred-process ：RedisDeferred 跨语言集合去重与批处理
//! stat-stage / stat-process-once ：RedisStat 跨语言统计聚合与延迟落盘
//! eventbus-publish / eventbus-subscribe ：RedisEventBus 跨语言事件发布与订阅
//! exists：只读探针，检查某个键是否存在（给严格拓扑联调用）
//! find-slot-key：离线寻找命中指定 Cluster 槽位范围的 key（给严格 cluster 联调用）
//! set-key：写入任意单键字符串（给严格拓扑/TLS 联调用）
//! selftest：离线校验编码器字节格式（无需 Redis）
//! report  ：查看双方回执
//! clean   ：清理本 Demo 的键
//! auto    ：write + verify + report
//! ```
//!
//! 用法：
//!
//! ```powershell
//! # 无 Redis 也可跑：使用进程内迷你 Redis 做本地自检
//! cargo run --example demo -- selftest
//! cargo run --example demo -- auto --mock
//!
//! # 与 C# 侧互相验证（同一连接串指向同一 Redis）
//! cargo run --example demo -- auto   --config "server=127.0.0.1:6379;password=xxx;db=15"
//! dotnet run --project demo\csharp\PekRRedisDemo -- auto --config "server=127.0.0.1:6379;password=xxx;db=15"
//! ```
//!
//! 环境变量 `REDIS_CONFIG` 可作为默认连接串。

#[path = "../tests/support/mod.rs"]
mod support;

use std::future::Future;
use std::sync::{Arc, mpsc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::{Local, NaiveDateTime};
use serde::{Deserialize, Serialize};
use tokio::runtime::Builder;

use pek_rredis::encoder::Json;
use pek_rredis::{
    AsyncFullRedis, FromRedisPayload, FullRedis, RedisDeferred, RedisEventBus, RedisStat,
    ToRedisPayload, hash_slot,
};

/// 本侧标识（文件名/进程来源）
const SIDE: &str = "rust";
/// 对方标识
const OTHER: &str = "csharp";

fn main() {
    std::process::exit(run());
}

fn run() -> i32 {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = args
        .first()
        .filter(|a| !a.starts_with('-'))
        .cloned()
        .unwrap_or_else(|| "auto".into());

    let opt = |name: &str, fallback: &str| -> String {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1).cloned())
            .unwrap_or_else(|| fallback.to_string())
    };
    let has_flag = |name: &str| args.iter().any(|a| a == name);

    let prefix = opt("--prefix", "pekrredis:demo:");
    let mut config = opt(
        "--config",
        &std::env::var("REDIS_CONFIG").unwrap_or_else(|_| "server=127.0.0.1:6379;db=15".into()),
    );

    // 纯离线命令不需要连接 Redis
    if command == "selftest" {
        return selftest();
    }
    if command == "find-slot-key" {
        let from: u16 = opt("--from", "0").parse().unwrap_or(0);
        let to: u16 = opt("--to", "16383").parse().unwrap_or(16_383);
        let prefix = opt("--key-prefix", "cluster:key:");
        let suffix = opt("--key-suffix", "");
        return find_slot_key(&prefix, &suffix, from, to);
    }

    // --mock：使用进程内迷你 Redis，本地即可完整演示（无需安装 Redis）
    let mut _mock = None;
    if has_flag("--mock") {
        let server = support::start_mock_redis();
        config = format!("server={};db=0", server.addr);
        _mock = Some(server);
        println!("[mock] 已启动进程内迷你 Redis：{}", config);
    }

    let base = match FullRedis::from_config(&config) {
        Ok(r) => r,
        Err(e) => {
            println!("✘ 无法创建客户端：{e}");
            return 2;
        }
    };
    let rds = FullRedis::with_prefix(base.redis().clone(), Some(prefix.clone()));
    let async_rds = AsyncFullRedis::from_sync(rds.clone());

    let mut ctx = DemoCtx {
        rds,
        prefix: prefix.clone(),
        failures: Vec::new(),
    };
    let mut async_ctx = AsyncDemoCtx {
        rds: async_rds,
        prefix,
        failures: Vec::new(),
    };
    let code = match command.as_str() {
        "write" => {
            ctx.write();
            0
        }
        "verify" => ctx.verify(),
        "push" => {
            let n: usize = opt("--count", "5").parse().unwrap_or(5);
            ctx.push(n);
            0
        }
        "consume" => {
            let n: usize = opt("--count", "5").parse().unwrap_or(5);
            ctx.consume(n);
            0
        }
        "qstatus" => {
            ctx.queue_status();
            0
        }
        "lock" => {
            let s: u64 = opt("--seconds", "3").parse().unwrap_or(3);
            ctx.lock(s);
            0
        }
        "stream-push" => {
            let n: usize = opt("--count", "5").parse().unwrap_or(5);
            ctx.stream_push(n, &opt("--group", "demo"));
            0
        }
        "stream-consume" => {
            let n: usize = opt("--count", "5").parse().unwrap_or(5);
            let retry: i64 = opt("--retry-seconds", "60").parse().unwrap_or(60);
            ctx.stream_consume(n, &opt("--group", "demo"), has_flag("--no-ack"), retry);
            0
        }
        "stream-status" => {
            ctx.stream_status(&opt("--group", "demo"));
            0
        }
        "delay-push" => {
            let n: usize = opt("--count", "3").parse().unwrap_or(3);
            let delay: i64 = opt("--delay", "2").parse().unwrap_or(2);
            ctx.delay_push(n, delay);
            0
        }
        "delay-consume" => {
            let n: usize = opt("--count", "3").parse().unwrap_or(3);
            let wait: u64 = opt("--wait", "15").parse().unwrap_or(15);
            ctx.delay_consume(n, wait);
            0
        }
        "pubsub-publish" => {
            let channel = opt("--channel", "pubsub:demo");
            let message = opt("--message", &format!("hello-from-{SIDE}"));
            ctx.pubsub_publish(&channel, &message, has_flag("--shard"));
            0
        }
        "pubsub-subscribe" => {
            let channel = opt("--channel", "pubsub:demo");
            let expect = opt("--expect", &format!("hello-from-{OTHER}"));
            let expect_channel = if has_flag("--pattern") {
                Some(opt("--expect-channel", "pubsub:demo"))
            } else {
                None
            };
            let timeout: u64 = opt("--timeout", "10").parse().unwrap_or(10);
            ctx.pubsub_subscribe(
                &channel,
                &expect,
                expect_channel.as_deref(),
                timeout,
                has_flag("--pattern"),
                has_flag("--shard"),
            );
            0
        }
        "write-advanced" => {
            ctx.write_advanced();
            0
        }
        "verify-advanced" => ctx.verify_advanced(),
        "deferred-add" => {
            let name = opt("--name", "deferred:demo");
            let keys = split_csv(&opt("--keys", ""));
            ctx.deferred_add(&name, &keys);
            0
        }
        "deferred-process" => {
            let name = opt("--name", "deferred:demo");
            let batch_size: usize = opt("--batch-size", "10").parse().unwrap_or(10);
            ctx.deferred_process(&name, batch_size)
        }
        "stat-stage" => {
            let name = opt("--name", "stat:demo");
            let key = opt("--key", "station:1");
            let delay: i64 = opt("--delay", "0").parse().unwrap_or(0);
            let pairs = parse_pairs_i32(&opt("--pairs", "pv=1"));
            ctx.stat_stage(&name, &key, &pairs, delay)
        }
        "stat-process-once" => {
            let name = opt("--name", "stat:demo");
            let timeout: u64 = opt("--timeout", "10").parse().unwrap_or(10);
            ctx.stat_process_once(&name, timeout)
        }
        "eventbus-publish" => {
            let topic = opt("--topic", "eventbus:demo");
            let group = opt("--group", "demo");
            let name = opt("--name", &format!("event-from-{SIDE}"));
            let count: i32 = opt("--count", "1").parse().unwrap_or(1);
            ctx.eventbus_publish(&topic, &group, &name, count)
        }
        "eventbus-subscribe" => {
            let topic = opt("--topic", "eventbus:demo");
            let group = opt("--group", "demo");
            let timeout: u64 = opt("--timeout", "10").parse().unwrap_or(10);
            ctx.eventbus_subscribe(&topic, &group, timeout, has_flag("--from-first"))
        }
        "write-async" => block_on_i32(async_ctx.write()),
        "verify-async" => block_on_i32(async_ctx.verify()),
        "push-async" => {
            let n: usize = opt("--count", "5").parse().unwrap_or(5);
            block_on_i32(async_ctx.push(n))
        }
        "consume-async" => {
            let n: usize = opt("--count", "5").parse().unwrap_or(5);
            block_on_i32(async_ctx.consume(n))
        }
        "verify-ops" => ctx.verify_ops(),
        "reset-ops" => ctx.reset_ops(),
        "verify-ops-empty" => ctx.verify_ops_empty(),
        "set-key" => {
            let key = opt("--key", "probe");
            let value = opt("--value", "value");
            let expire: i64 = opt("--expire", "0").parse().unwrap_or(0);
            ctx.set_key(&key, &value, expire);
            0
        }
        "exists" => {
            let key = opt("--key", "csharp:marker");
            ctx.exists(&key);
            0
        }
        "report" => {
            ctx.report();
            0
        }
        "clean" => {
            ctx.clean();
            0
        }
        "auto" => {
            ctx.write();
            let code = ctx.verify();
            ctx.report();
            code
        }
        other => {
            println!("未知命令：{other}（可用：selftest/find-slot-key/set-key/write/verify/write-async/verify-async/push-async/consume-async/write-advanced/verify-advanced/verify-ops/reset-ops/verify-ops-empty/deferred-add/deferred-process/stat-stage/stat-process-once/eventbus-publish/eventbus-subscribe/exists/push/consume/qstatus/lock/stream-push/stream-consume/stream-status/delay-push/delay-consume/pubsub-publish/pubsub-subscribe/report/clean/auto）");
            2
        }
    };

    let mut failures = ctx.failures;
    failures.extend(async_ctx.failures);

    if code != 0 {
        println!("\n结果：失败 {} 项", failures.len());
        for f in &failures {
            println!("  - {f}");
        }
        return 1;
    }

    println!("\n结果：全部通过");
    0
}

// ======================= 固定样本规范 =======================

fn sample_time() -> NaiveDateTime {
    NaiveDateTime::parse_from_str("2026-09-26 10:00:00.123", "%Y-%m-%d %H:%M:%S%.f").unwrap()
}

/// JSON 内时间：C# FastJson 不写毫秒（System.Text.Json 写 ISO 8601），
/// 因此 JSON 样本用整秒，两端均可精确往返；裸时间样本仍验证毫秒格式。
fn sample_json_time() -> NaiveDateTime {
    NaiveDateTime::parse_from_str("2026-09-26 10:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
}

fn find_slot_key(prefix: &str, suffix: &str, from: u16, to: u16) -> i32 {
    for index in 0..200_000u32 {
        let key = format!("{prefix}{index}{suffix}");
        let slot = hash_slot(&key);
        if slot >= from && slot <= to {
            println!("[find-slot-key] key={key} slot={slot}");
            return 0;
        }
    }

    println!("✘ 未找到命中槽位范围 {from}..={to} 的 key");
    2
}

fn block_on_i32<F>(future: F) -> i32
where
    F: Future<Output = i32>,
{
    Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

fn split_csv(text: &str) -> Vec<String> {
    text.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn parse_pairs_i32(text: &str) -> Vec<(String, i32)> {
    split_csv(text)
        .into_iter()
        .filter_map(|item| {
            let (field, value) = item.split_once('=')?;
            let value = value.trim().parse::<i32>().ok()?;
            Some((field.trim().to_string(), value))
        })
        .collect()
}

const SAMPLE_STRING: &str = "Hello 互通";
const SAMPLE_INT: i32 = 123456789;
const SAMPLE_COUNT: i32 = 7;
const SAMPLE_NAME: &str = "互通Demo";
const ADV_FUNCTION_LIBRARY: &str =
    "#!lua name=advlib\nredis.register_function('echo', function(keys, args) return args[1] end)\n";
const OPS_SLOWLOG_ID: i64 = 101;
const OPS_SLOWLOG_TIMESTAMP: i64 = 1_727_424_000;
const OPS_SLOWLOG_DURATION_US: i64 = 12_345;
const OPS_SLOWLOG_COMMAND: [&str; 3] = ["SET", "ops:key", "42"];
const OPS_LATENCY_EVENT: &str = "command";
const OPS_LATENCY_TIMESTAMP: i64 = 1_727_424_001;
const OPS_LATENCY_LATEST_MS: i64 = 15;
const OPS_LATENCY_MAX_MS: i64 = 42;
const OPS_DOCTOR_TEXT: &str = "latency spikes";

/// 固定样本模型（与 C# `DemoModel` 字段一致，属性名 PascalCase）
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
struct DemoModel {
    name: String,
    /// 宽容时间：兼容 C# FastJson 的 `yyyy-MM-dd HH:mm:ss` 与 ISO 8601
    #[serde(with = "pek_rredis::encoder::datetime")]
    create_time: NaiveDateTime,
    count: i32,
}

/// Stream 对象消息（字段路径用编码器文本时间，与 C# 写入逐字节一致）
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
struct StreamDemo {
    name: String,
    #[serde(with = "pek_rredis::encoder::datetime_text")]
    create_time: NaiveDateTime,
    count: i32,
}

/// 校验回执（C# 端按原始 JSON 读取，字段名 PascalCase 便于人工比对）
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
struct DemoReceipt {
    side: String,
    time: NaiveDateTime,
    failures: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
struct ServiceEventDemo {
    name: String,
    count: i32,
}

struct DemoCtx {
    rds: FullRedis,
    prefix: String,
    failures: Vec<String>,
}

struct AsyncDemoCtx {
    rds: AsyncFullRedis,
    prefix: String,
    failures: Vec<String>,
}

impl AsyncDemoCtx {
    fn check(&mut self, ok: bool, what: &str, detail: Option<String>) {
        match (ok, detail) {
            (true, _) => println!("  ✔ {what}"),
            (false, Some(d)) => {
                println!("  ✘ {what}  {d}");
                self.failures.push(format!("{what}：{d}"));
            }
            (false, None) => {
                println!("  ✘ {what}");
                self.failures.push(what.to_string());
            }
        }
    }

    async fn write_receipt(&mut self) {
        let receipt = DemoReceipt {
            side: SIDE.into(),
            time: Local::now().naive_local(),
            failures: self.failures.clone(),
        };
        let key = format!("{SIDE}:receipt");
        self.rds
            .set(key.clone(), Json(receipt), 3600)
            .await
            .unwrap();
        println!("  · 已写入回执 {}{key}", self.prefix);
    }

    async fn write(&mut self) -> i32 {
        println!("[write-async/{SIDE}] 异步写入固定样本 → prefix={}", self.prefix);
        self.rds
            .remove_many(
                [
                    "str", "int", "bool", "dt", "json", "hash", "list", "set", "zset", "queue",
                ]
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
            )
            .await
            .unwrap();
        self.rds.set("str".into(), SAMPLE_STRING.to_string(), 0).await.unwrap();
        self.rds.set("int".into(), SAMPLE_INT, 0).await.unwrap();
        self.rds.set("bool".into(), true, 0).await.unwrap();
        self.rds.set("dt".into(), sample_time(), 0).await.unwrap();
        let model = DemoModel {
            name: SAMPLE_NAME.into(),
            create_time: sample_json_time(),
            count: SAMPLE_COUNT,
        };
        self.rds.set("json".into(), Json(model), 0).await.unwrap();

        let hash = self.rds.get_hash::<i32>("hash");
        hash.set("a".to_string(), 1).await.unwrap();
        hash.set("b".to_string(), 2).await.unwrap();

        let list = self.rds.get_list::<i32>("list");
        list.push_back_many(vec![1, 2, 3]).await.unwrap();

        self.rds
            .get_set::<String>("set")
            .add(vec!["x".into(), "y".into()])
            .await
            .unwrap();

        let zset = self.rds.get_sorted_set::<String>("zset");
        zset.add("m1".to_string(), 1.5).await.unwrap();
        zset.add("m2".to_string(), 0.5).await.unwrap();

        let queue = self.rds.get_queue::<String>("queue");
        queue.add("q1".into()).await.unwrap();
        queue.add("q2".into()).await.unwrap();

        self.rds
            .set(format!("{SIDE}:marker"), Local::now().naive_local(), 3600)
            .await
            .unwrap();
        println!("  ✔ 已写入：str/int/bool/dt/json/hash/list/set/zset/queue/{SIDE}:marker");
        0
    }

    async fn verify(&mut self) -> i32 {
        println!("[verify-async/{SIDE}] 异步校验固定样本（含对方 {OTHER} 写入的数据）");
        let mine = self.rds.get_string(format!("{SIDE}:marker")).await.unwrap();
        let other = self.rds.get_string(format!("{OTHER}:marker")).await.unwrap();
        println!(
            "  · 本侧标记：{}；对方 {OTHER} 标记：{}",
            if mine.is_some() { "有" } else { "无" },
            other.unwrap_or_else(|| "无（对方尚未运行 write）".into())
        );

        let str_raw = self.rds.get_string("str".into()).await.unwrap();
        self.check(str_raw.as_deref() == Some(SAMPLE_STRING), "str 读回", str_raw.clone());
        let int_val = self.rds.get::<i32>("int".into()).await.unwrap();
        self.check(int_val == Some(SAMPLE_INT), "int 读回", int_val.map(|v| v.to_string()));
        let bool_val = self.rds.get::<bool>("bool".into()).await.unwrap();
        let bool_raw = self.rds.get_string("bool".into()).await.unwrap();
        self.check(bool_val == Some(true), "bool 读回", bool_val.map(|v| v.to_string()));
        self.check(bool_raw.as_deref() == Some("True"), "bool 原始字节 = True", bool_raw.clone());
        let dt_val = self.rds.get::<NaiveDateTime>("dt".into()).await.unwrap();
        let dt_raw = self.rds.get_string("dt".into()).await.unwrap();
        self.check(dt_val == Some(sample_time()), "dt 读回", dt_val.map(|v| v.to_string()));
        self.check(dt_raw.as_deref() == Some("2026-09-26 10:00:00.123"), "dt 原始字节", dt_raw.clone());

        let json_raw = self.rds.get_string("json".into()).await.unwrap();
        let model = self.rds.get::<Json<DemoModel>>("json".into()).await.unwrap();
        self.check(
            model.map(|j| j.0)
                == Some(DemoModel {
                    name: SAMPLE_NAME.into(),
                    create_time: sample_json_time(),
                    count: SAMPLE_COUNT,
                }),
            "json 反序列化",
            json_raw.clone(),
        );
        self.check(
            json_raw.clone().unwrap_or_default().contains("\"Name\"")
                && json_raw.clone().unwrap_or_default().contains("\"CreateTime\"")
                && json_raw.clone().unwrap_or_default().contains("\"Count\""),
            "json 原始字段名 PascalCase",
            json_raw.clone(),
        );

        let hash = self.rds.get_hash::<i32>("hash");
        let ha = hash.get("a".to_string()).await.unwrap();
        let hb = hash.get("b".to_string()).await.unwrap();
        self.check(
            ha == Some(1) && hb == Some(2),
            "hash a=1,b=2",
            Some(format!("{:?},{:?}", ha, hb)),
        );

        let list = self.rds.get_list::<i32>("list");
        self.check(list.get_all().await.unwrap() == vec![1, 2, 3], "list [1,2,3]", None);

        let set = self.rds.get_set::<String>("set");
        let mut members = set.members().await.unwrap();
        members.sort();
        self.check(
            members == vec!["x".to_string(), "y".to_string()],
            "set {x,y}",
            Some(format!("{:?}", members)),
        );

        let zset = self.rds.get_sorted_set::<String>("zset");
        let m1 = zset.score("m1".to_string()).await.unwrap();
        let m2 = zset.score("m2".to_string()).await.unwrap();
        self.check(
            m1 == Some(1.5) && m2 == Some(0.5),
            "zset 分数 1.5/0.5",
            Some(format!("{:?},{:?}", m1, m2)),
        );

        let queue = self.rds.get_queue::<String>("queue");
        let q1 = queue.take_one(-1).await.unwrap();
        let q2 = queue.take_one(-1).await.unwrap();
        self.check(
            q1.as_deref() == Some("q1") && q2.as_deref() == Some("q2"),
            "queue 消费顺序 q1,q2",
            Some(format!("{:?},{:?}", q1, q2)),
        );
        queue.add("q1".into()).await.unwrap();
        queue.add("q2".into()).await.unwrap();

        self.write_receipt().await;
        if self.failures.is_empty() { 0 } else { 1 }
    }

    async fn push(&mut self, count: usize) -> i32 {
        println!("[push-async/{SIDE}] 异步向可靠队列推入 {count} 条消息");
        let queue = self.rds.get_reliable_queue::<String>("reliable");
        for i in 1..=count {
            queue.add(format!("msg-{i:04}")).await.unwrap();
        }
        println!(
            "  ✔ 队列长度：{}（消息格式 msg-0001 ...）",
            queue.count().await.unwrap()
        );
        0
    }

    async fn consume(&mut self, count: usize) -> i32 {
        println!("[consume-async/{SIDE}] 异步用可靠队列消费 {count} 条消息并确认（对方 push 的消息同样可消费）");
        let queue = self.rds.get_reliable_queue::<String>("reliable");
        let mut got = 0;
        for _ in 0..count {
            match queue.take_one(-1).await.unwrap() {
                Some(msg) => {
                    println!("  · 消费到 {msg}（异步确认）");
                    queue.acknowledge(vec![msg.clone()]).await.unwrap();
                    got += 1;
                }
                None => break,
            }
        }
        println!("  ✔ 已确认 {got} 条；剩余队列长度：{}", queue.count().await.unwrap());
        0
    }
}

impl DemoCtx {
    /// 带前缀的完整键名。
    fn full_key(&self, key: &str) -> String {
        self.rds.get_key(key)
    }

    /// 写入一个值（自动补前缀）。
    fn set<V: ToRedisPayload>(&self, key: &str, value: V, expire: i64) -> pek_rredis::Result<bool> {
        self.rds.redis().set(self.full_key(key), value, expire)
    }

    /// 读取原始字符串（自动补前缀）。
    fn get_string(&self, key: &str) -> pek_rredis::Result<Option<String>> {
        self.rds.redis().get_string(&self.full_key(key))
    }

    /// 读取并解码（自动补前缀）。
    fn get<T: FromRedisPayload>(&self, key: &str) -> pek_rredis::Result<Option<T>> {
        self.rds.redis().get::<T>(&self.full_key(key))
    }

    fn check(&mut self, ok: bool, what: &str, detail: Option<String>) {
        match (ok, detail) {
            (true, _) => println!("  ✔ {what}"),
            (false, Some(d)) => {
                println!("  ✘ {what}  {d}");
                self.failures.push(format!("{what}：{d}"));
            }
            (false, None) => {
                println!("  ✘ {what}");
                self.failures.push(what.to_string());
            }
        }
    }

    fn write_receipt(&mut self) {
        let receipt = DemoReceipt {
            side: SIDE.into(),
            time: Local::now().naive_local(),
            failures: self.failures.clone(),
        };
        let key = format!("{SIDE}:receipt");
        self.set(&key, Json(&receipt), 3600).unwrap();
        println!("  · 已写入回执 {}{key}", self.prefix);
    }

    // ---------------- write ----------------

    fn write(&mut self) {
        println!("[write/{SIDE}] 写入固定样本 → prefix={}", self.prefix);
        let rds = self.rds.clone();

        // 先清空固定样本键（列表/队列是追加语义，必须先重置才能被对方精确校验）
        rds.remove_many(&[
            "str", "int", "bool", "dt", "json", "hash", "list", "set", "zset", "queue",
        ])
        .unwrap();

        rds.redis().set(self.full_key("str"), SAMPLE_STRING, 0).unwrap();
        rds.redis().set(self.full_key("int"), SAMPLE_INT, 0).unwrap();
        rds.redis().set(self.full_key("bool"), true, 0).unwrap();
        rds.redis().set(self.full_key("dt"), sample_time(), 0).unwrap();

        let model = DemoModel {
            name: SAMPLE_NAME.into(),
            create_time: sample_json_time(),
            count: SAMPLE_COUNT,
        };
        rds.redis().set(self.full_key("json"), Json(&model), 0).unwrap();

        let hash = rds.get_hash::<i32>("hash");
        hash.set(&"a".to_string(), &1).unwrap();
        hash.set(&"b".to_string(), &2).unwrap();

        let list = rds.get_list::<i32>("list");
        list.push_back_many(&[1, 2, 3]).unwrap();

        rds.get_set::<String>("set")
            .add(&["x".into(), "y".into()])
            .unwrap();

        let zset = rds.get_sorted_set::<String>("zset");
        zset.add(&"m1".to_string(), 1.5).unwrap();
        zset.add(&"m2".to_string(), 0.5).unwrap();

        // 普通队列：LPUSH q1 → LPUSH q2，RPOP 顺序为 q1、q2
        let queue = rds.get_queue::<String>("queue");
        queue.add(&"q1".into()).unwrap();
        queue.add(&"q2".into()).unwrap();

        // 本侧标记与时间戳（供对方确认数据来源）
        let now = Local::now().naive_local();
        rds.redis()
            .set(self.full_key(&format!("{SIDE}:marker")), now.to_string(), 3600)
            .unwrap();

        println!("  ✔ 已写入：str/int/bool/dt/json/hash/list/set/zset/queue/{SIDE}:marker");
    }

    // ---------------- verify ----------------

    fn verify(&mut self) -> i32 {
        println!("[verify/{SIDE}] 校验固定样本（含对方 {OTHER} 写入的数据）");
        let rds = self.rds.clone();

        let mine = self.get_string(&format!("{SIDE}:marker")).unwrap();
        let other = self.get_string(&format!("{OTHER}:marker")).unwrap();
        println!(
            "  · 本侧标记：{}；对方 {OTHER} 标记：{}",
            if mine.is_some() { "有" } else { "无" },
            other.unwrap_or_else(|| "无（对方尚未运行 write）".into())
        );

        // 原始字节：确认字符串/布尔/时间的存储格式（C# 默认编码器的输出）
        let str_raw = self.get_string("str").unwrap();
        self.check(
            str_raw.as_deref() == Some(SAMPLE_STRING),
            "str 读回",
            str_raw.clone(),
        );
        let int_val = self.get::<i32>("int").unwrap();
        self.check(int_val == Some(SAMPLE_INT), "int 读回", int_val.map(|v| v.to_string()));
        let bool_val = self.get::<bool>("bool").unwrap();
        let bool_raw = self.get_string("bool").unwrap();
        self.check(bool_val == Some(true), "bool 读回", bool_val.map(|v| v.to_string()));
        self.check(
            bool_raw.as_deref() == Some("True"),
            "bool 原始字节 = True",
            bool_raw.clone(),
        );
        let dt_val = self.get::<NaiveDateTime>("dt").unwrap();
        let dt_raw = self.get_string("dt").unwrap();
        self.check(dt_val == Some(sample_time()), "dt 读回", dt_val.map(|v| v.to_string()));
        self.check(
            dt_raw.as_deref() == Some("2026-09-26 10:00:00.123"),
            "dt 原始字节",
            dt_raw.clone(),
        );

        // JSON：C# System.Text.Json 会把非 ASCII 转义（\uXXXX），此处按语义校验
        let json_raw = self.get_string("json").unwrap();
        let model = self.get::<Json<DemoModel>>("json").unwrap();
        self.check(
            model.as_ref().map(|m| m.0.clone())
                == Some(DemoModel {
                    name: SAMPLE_NAME.into(),
                    create_time: sample_json_time(),
                    count: SAMPLE_COUNT,
                }),
            "json 反序列化",
            json_raw.clone(),
        );
        self.check(
            json_raw
                .as_deref()
                .map(|t| t.contains("\"Name\":") && t.contains("\"Count\":7"))
                .unwrap_or(false),
            "json 原始字段名 PascalCase",
            json_raw.clone(),
        );

        // 哈希 / 列表 / 集合 / 有序集合
        let hash = rds.get_hash::<i32>("hash");
        self.check(
            hash.get(&"a".to_string()).unwrap() == Some(1)
                && hash.get(&"b".to_string()).unwrap() == Some(2),
            "hash a=1,b=2",
            None,
        );
        let list = rds.get_list::<i32>("list");
        self.check(list.get_all().unwrap() == vec![1, 2, 3], "list [1,2,3]", None);
        let set = rds.get_set::<String>("set");
        self.check(
            set.contains(&"x".to_string()).unwrap() && set.contains(&"y".to_string()).unwrap(),
            "set {x,y}",
            None,
        );
        let zset = rds.get_sorted_set::<String>("zset");
        self.check(
            zset.score(&"m1".to_string()).unwrap() == Some(1.5)
                && zset.score(&"m2".to_string()).unwrap() == Some(0.5),
            "zset 分数 1.5/0.5",
            None,
        );

        // 队列：消费对方入队的消息（跨语言消费验证），随后恢复
        let queue = rds.get_queue::<String>("queue");
        let q1 = queue.take_one(-1).unwrap();
        let q2 = queue.take_one(-1).unwrap();
        self.check(
            q1.as_deref() == Some("q1") && q2.as_deref() == Some("q2"),
            "queue 消费顺序 q1,q2",
            Some(format!("{:?},{:?}", q1, q2)),
        );
        queue.add(&"q1".into()).unwrap();
        queue.add(&"q2".into()).unwrap();

        self.write_receipt();

        if self.failures.is_empty() { 0 } else { 1 }
    }

    fn write_advanced(&mut self) {
        println!("[write-advanced/{SIDE}] 写入高级 API 联调样本 → prefix={}", self.prefix);
        let rds = self.rds.clone();

        let _ = rds.function_delete("advlib");
        rds.remove_many(&[
            "adv:writer",
            "adv:getex",
            "adv:bits",
            "adv:hash",
            "adv:list:move:src",
            "adv:list:move:dst",
            "adv:list:multi:1",
            "adv:list:multi:2",
            "adv:list:block:empty",
            "adv:list:block:right",
            "adv:list:block:left",
            "adv:set:1",
            "adv:set:2",
            "adv:zset:score",
            "adv:zset:rand",
            "adv:zset:range",
            "adv:zset:range:dest",
            "adv:zset:pop:1",
            "adv:zset:pop:2",
        ])
        .unwrap();

        rds.redis().set(self.full_key("adv:writer"), SIDE, 3600).unwrap();
        rds.redis()
            .set(self.full_key("adv:getex"), format!("from-{SIDE}"), 0)
            .unwrap();
        rds.redis()
            .set(self.full_key("adv:bits"), vec![0b1010_0000u8], 0)
            .unwrap();

        let hash = rds.get_hash::<String>("adv:hash");
        hash.set(&"del".to_string(), &"value-del".to_string()).unwrap();
        hash.set(&"ex".to_string(), &"value-ex".to_string()).unwrap();

        let move_src = rds.get_list::<String>("adv:list:move:src");
        move_src
            .push_back_many(&["1".into(), "2".into(), "3".into()])
            .unwrap();
        let multi = rds.get_list::<String>("adv:list:multi:2");
        multi.push_back_many(&["m1".into(), "m2".into()]).unwrap();
        let block_right = rds.get_list::<String>("adv:list:block:right");
        block_right
            .push_back_many(&["ra".into(), "rb".into()])
            .unwrap();
        let block_left = rds.get_list::<String>("adv:list:block:left");
        block_left
            .push_back_many(&["la".into(), "lb".into()])
            .unwrap();

        rds.get_set::<String>("adv:set:1")
            .add(&["a".into(), "b".into(), "c".into()])
            .unwrap();
        rds.get_set::<String>("adv:set:2")
            .add(&["b".into(), "c".into(), "d".into()])
            .unwrap();

        let zscore = rds.get_sorted_set::<String>("adv:zset:score");
        zscore.add(&"a".to_string(), 1.0).unwrap();
        zscore.add(&"b".to_string(), 2.0).unwrap();
        let zrand = rds.get_sorted_set::<String>("adv:zset:rand");
        zrand.add(&"ra".to_string(), 1.0).unwrap();
        zrand.add(&"rb".to_string(), 2.0).unwrap();
        zrand.add(&"rc".to_string(), 3.0).unwrap();
        let zrange = rds.get_sorted_set::<String>("adv:zset:range");
        zrange.add(&"a".to_string(), 1.0).unwrap();
        zrange.add(&"b".to_string(), 2.0).unwrap();
        zrange.add(&"c".to_string(), 3.0).unwrap();
        let zpop = rds.get_sorted_set::<String>("adv:zset:pop:1");
        zpop.add(&"p1".to_string(), 1.0).unwrap();
        zpop.add(&"p2".to_string(), 2.0).unwrap();

        let lib = rds.function_load(ADV_FUNCTION_LIBRARY, true).unwrap();
        println!("  ✔ 已写入高级样本：adv:* + function lib={lib}");
    }

    fn verify_advanced(&mut self) -> i32 {
        println!("[verify-advanced/{SIDE}] 校验高级 API 面（含对方 {OTHER} 写入的数据）");

        let writer = self.get_string("adv:writer").unwrap();
        self.check(
            writer.as_deref() == Some(OTHER),
            "adv writer marker",
            writer.clone(),
        );

        let getex: Option<String> = self.rds.get_ex("adv:getex", 120).unwrap();
        self.check(
            getex.as_deref() == Some(&format!("from-{OTHER}")),
            "GETEX 读取对方样本",
            getex.clone(),
        );
        let expire = self.rds.expire_time("adv:getex").unwrap();
        self.check(expire > 0, "EXPIRETIME > 0", Some(expire.to_string()));
        let pexpire = self.rds.pexpire_time("adv:getex").unwrap();
        self.check(pexpire > 0, "PEXPIRETIME > 0", Some(pexpire.to_string()));
        let persist: Option<String> = self.rds.get_ex("adv:getex", 0).unwrap();
        self.check(
            persist.as_deref() == Some(&format!("from-{OTHER}")),
            "GETEX PERSIST 读回",
            persist.clone(),
        );
        let expire2 = self.rds.expire_time("adv:getex").unwrap();
        self.check(expire2 == -1, "GETEX PERSIST 清除过期", Some(expire2.to_string()));
        let idle = self.rds.object_idle_time("adv:getex").unwrap();
        self.check(idle == Some(0), "OBJECT IDLETIME", idle.map(|v| v.to_string()));
        let freq = self.rds.object_freq("adv:getex").unwrap();
        self.check(freq == Some(0), "OBJECT FREQ", freq.map(|v| v.to_string()));

        let bit = self.rds.bit_field("adv:bits", &["GET", "u8", "0"]).unwrap();
        self.check(bit == vec![160], "BITFIELD GET u8 0", Some(format!("{bit:?}")));

        let hash = self.rds.get_hash::<String>("adv:hash");
        let deleted = hash.hgetdel(&"del".to_string()).unwrap();
        self.check(
            deleted.as_deref() == Some("value-del"),
            "HGETDEL 返回旧值",
            deleted.clone(),
        );
        self.check(
            hash.get(&"del".to_string()).unwrap().is_none(),
            "HGETDEL 删除字段",
            None,
        );
        let kept = hash.hgetex(&"ex".to_string(), 60).unwrap();
        self.check(
            kept.as_deref() == Some("value-ex"),
            "HGETEX 返回字段值",
            kept.clone(),
        );

        let moved = self
            .rds
            .lmove::<String>("adv:list:move:src", "adv:list:move:dst", false, true)
            .unwrap();
        self.check(
            moved.as_deref() == Some("3"),
            "LMOVE RIGHT->LEFT",
            moved.clone(),
        );
        let blmoved = self
            .rds
            .blmove::<String>("adv:list:move:src", "adv:list:move:dst", true, false, 1)
            .unwrap();
        self.check(
            blmoved.as_deref() == Some("1"),
            "BLMOVE LEFT->RIGHT",
            blmoved.clone(),
        );
        let moved_list = self.rds.get_list::<String>("adv:list:move:dst").get_all().unwrap();
        self.check(
            moved_list == vec!["3".to_string(), "1".to_string()],
            "LMOVE/BLMOVE 目标列表顺序",
            Some(format!("{moved_list:?}")),
        );

        match self
            .rds
            .lmpop::<String>(&["adv:list:multi:1", "adv:list:multi:2"], true, 2)
            .unwrap()
        {
            Some((key, items)) => self.check(
                key.ends_with("adv:list:multi:2")
                    && items == vec!["m1".to_string(), "m2".to_string()],
                "LMPOP 多键弹出",
                Some(format!("{key} => {items:?}")),
            ),
            None => self.check(false, "LMPOP 多键弹出", Some("None".into())),
        }

        match self
            .rds
            .brpop_multi::<String>(&["adv:list:block:empty", "adv:list:block:right"], 1)
            .unwrap()
        {
            Some((key, value)) => self.check(
                key.ends_with("adv:list:block:right") && value == "rb",
                "BRPOP 多键阻塞弹出",
                Some(format!("{key} => {value}")),
            ),
            None => self.check(false, "BRPOP 多键阻塞弹出", Some("None".into())),
        }

        match self
            .rds
            .blpop_multi::<String>(&["adv:list:block:empty", "adv:list:block:left"], 1)
            .unwrap()
        {
            Some((key, value)) => self.check(
                key.ends_with("adv:list:block:left") && value == "la",
                "BLPOP 多键阻塞弹出",
                Some(format!("{key} => {value}")),
            ),
            None => self.check(false, "BLPOP 多键阻塞弹出", Some("None".into())),
        }

        let members = self
            .rds
            .get_set::<String>("adv:set:1")
            .mismember(&["a".into(), "x".into(), "c".into()])
            .unwrap();
        self.check(
            members == vec![true, false, true],
            "SMISMEMBER 成员存在性",
            Some(format!("{members:?}")),
        );
        let sinter = self.rds.sinter_card(&["adv:set:1", "adv:set:2"], 0).unwrap();
        self.check(sinter == 2, "SINTERCARD 交集基数", Some(sinter.to_string()));

        let scores = self.rds.zmscore("adv:zset:score", &["a", "b"]).unwrap();
        self.check(
            scores == vec![Some(1.0), Some(2.0)],
            "ZMSCORE 批量分数",
            Some(format!("{scores:?}")),
        );
        let rand_members = self.rds.zrand_member::<String>("adv:zset:rand", 2).unwrap();
        let rand_ok = rand_members.len() == 2
            && rand_members
                .iter()
                .all(|item| ["ra", "rb", "rc"].contains(&item.as_str()));
        self.check(
            rand_ok,
            "ZRANDMEMBER 随机成员",
            Some(format!("{rand_members:?}")),
        );
        let stored = self
            .rds
            .get_sorted_set::<String>("adv:zset:range")
            .range_store("adv:zset:range:dest", 1.5, 3.0, true, false, 0, 10)
            .unwrap();
        self.check(stored == 2, "ZRANGESTORE 存储数量", Some(stored.to_string()));
        let stored_values = self
            .rds
            .get_sorted_set::<String>("adv:zset:range:dest")
            .range_by_score(0.0, 10.0, 0, 10)
            .unwrap();
        self.check(
            stored_values == vec!["b".to_string(), "c".to_string()],
            "ZRANGESTORE 结果可读",
            Some(format!("{stored_values:?}")),
        );

        match self
            .rds
            .zmpop::<String>(&["adv:zset:pop:2", "adv:zset:pop:1"], true, 2)
            .unwrap()
        {
            Some((key, items)) => self.check(
                key.ends_with("adv:zset:pop:1")
                    && items
                        == vec![
                            ("p1".to_string(), 1.0),
                            ("p2".to_string(), 2.0),
                        ],
                "ZMPOP 弹出最小分成员",
                Some(format!("{key} => {items:?}")),
            ),
            None => self.check(false, "ZMPOP 弹出最小分成员", Some("None".into())),
        }

        let wait = self.rds.wait(1, 10).unwrap();
        self.check(wait == 0, "WAIT 单实例确认数", Some(wait.to_string()));

        let libs = self.rds.function_list(Some("advlib")).unwrap();
        self.check(
            !libs.is_empty(),
            "FUNCTION LIST 可见对方函数库",
            Some(libs.len().to_string()),
        );
        let echo: Option<String> = self.rds.fcall("echo", &[], &["hello-interop"]).unwrap();
        self.check(
            echo.as_deref() == Some("hello-interop"),
            "FCALL 回声函数",
            echo.clone(),
        );
        let echo_ro: Option<String> = self.rds.fcall_ro("echo", &[], &["hello-ro"]).unwrap();
        self.check(
            echo_ro.as_deref() == Some("hello-ro"),
            "FCALL_RO 回声函数",
            echo_ro.clone(),
        );
        self.rds.function_delete("advlib").unwrap();
        let libs_after = self.rds.function_list(Some("advlib")).unwrap();
        self.check(
            libs_after.is_empty(),
            "FUNCTION DELETE 删除函数库",
            Some(libs_after.len().to_string()),
        );

        self.write_receipt();
        if self.failures.is_empty() { 0 } else { 1 }
    }

    fn verify_ops(&mut self) -> i32 {
        println!("[verify-ops/{SIDE}] 校验运维 API 面（SLOWLOG/LATENCY）");

        let slowlog_len = self.rds.slowlog_len().unwrap_or_default();
        self.check(
            slowlog_len == 1,
            "SLOWLOG LEN == 1",
            Some(slowlog_len.to_string()),
        );

        let slowlog = self.rds.slowlog_get(10).unwrap_or_default();
        let entry = slowlog.first().cloned();
        self.check(
            entry.is_some(),
            "SLOWLOG GET 返回条目",
            Some(format!("count={}", slowlog.len())),
        );
        if let Some(entry) = entry {
            self.check(
                entry.id == OPS_SLOWLOG_ID,
                "SLOWLOG id",
                Some(entry.id.to_string()),
            );
            self.check(
                entry.timestamp == OPS_SLOWLOG_TIMESTAMP,
                "SLOWLOG timestamp",
                Some(entry.timestamp.to_string()),
            );
            self.check(
                entry.duration_us == OPS_SLOWLOG_DURATION_US,
                "SLOWLOG duration_us",
                Some(entry.duration_us.to_string()),
            );
            self.check(
                entry.command == OPS_SLOWLOG_COMMAND.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                "SLOWLOG command",
                Some(format!("{:?}", entry.command)),
            );
        }

        let history = self
            .rds
            .latency_history(OPS_LATENCY_EVENT)
            .unwrap_or_default();
        self.check(
            history == vec![(OPS_LATENCY_TIMESTAMP, OPS_LATENCY_LATEST_MS)],
            "LATENCY HISTORY 命中样本事件",
            Some(format!("{:?}", history)),
        );

        let latest = self.rds.latency_latest().unwrap_or_default();
        self.check(
            latest.iter().any(|(event, ts, latest_ms, max_ms)| {
                event == OPS_LATENCY_EVENT
                    && *ts == OPS_LATENCY_TIMESTAMP
                    && *latest_ms == OPS_LATENCY_LATEST_MS
                    && *max_ms == OPS_LATENCY_MAX_MS
            }),
            "LATENCY LATEST 包含样本事件",
            Some(format!("{:?}", latest)),
        );

        let doctor = self.rds.latency_doctor().unwrap_or_default();
        self.check(
            doctor.contains(OPS_DOCTOR_TEXT),
            "LATENCY DOCTOR 返回诊断文本",
            Some(doctor),
        );

        if self.failures.is_empty() { 0 } else { 1 }
    }

    fn reset_ops(&mut self) -> i32 {
        println!("[reset-ops/{SIDE}] 重置运维 API 样本（SLOWLOG/LATENCY）");

        let before = self.rds.slowlog_len().unwrap_or_default();
        self.check(
            before == 1,
            "SLOWLOG RESET 前条数 == 1",
            Some(before.to_string()),
        );
        self.rds.slowlog_reset().unwrap();
        let after = self.rds.slowlog_len().unwrap_or_default();
        self.check(
            after == 0,
            "SLOWLOG RESET 后条数 == 0",
            Some(after.to_string()),
        );

        let reset = self.rds.latency_reset(&[OPS_LATENCY_EVENT]).unwrap_or_default();
        self.check(
            reset == 1,
            "LATENCY RESET 清空样本事件",
            Some(reset.to_string()),
        );
        let history = self
            .rds
            .latency_history(OPS_LATENCY_EVENT)
            .unwrap_or_default();
        self.check(
            history.is_empty(),
            "LATENCY HISTORY 已清空",
            Some(format!("{:?}", history)),
        );

        if self.failures.is_empty() { 0 } else { 1 }
    }

    fn verify_ops_empty(&mut self) -> i32 {
        println!("[verify-ops-empty/{SIDE}] 校验运维 API 样本已被清空");

        let slowlog_len = self.rds.slowlog_len().unwrap_or_default();
        self.check(
            slowlog_len == 0,
            "SLOWLOG LEN == 0",
            Some(slowlog_len.to_string()),
        );

        let slowlog = self.rds.slowlog_get(10).unwrap_or_default();
        self.check(
            slowlog.is_empty(),
            "SLOWLOG GET 为空",
            Some(format!("count={}", slowlog.len())),
        );

        let history = self
            .rds
            .latency_history(OPS_LATENCY_EVENT)
            .unwrap_or_default();
        self.check(
            history.is_empty(),
            "LATENCY HISTORY 为空",
            Some(format!("{:?}", history)),
        );

        let latest = self.rds.latency_latest().unwrap_or_default();
        self.check(
            latest.iter().all(|(event, _, _, _)| event != OPS_LATENCY_EVENT),
            "LATENCY LATEST 不含样本事件",
            Some(format!("{:?}", latest)),
        );

        if self.failures.is_empty() { 0 } else { 1 }
    }

    // ---------------- 可靠队列 ----------------

    fn push(&mut self, count: usize) {
        println!("[push/{SIDE}] 向可靠队列推入 {count} 条消息");
        let queue = self.rds.get_reliable_queue::<String>("reliable");
        for i in 1..=count {
            // 注意：Rust 的零填充写法是 {i:04}（{i:0000} 会被解析为宽度 0）
            queue.add(&format!("msg-{i:04}")).unwrap();
        }
        println!("  ✔ 队列长度：{}（消息格式 msg-0001 ...）", queue.count().unwrap());
    }

    fn consume(&mut self, count: usize) {
        println!("[consume/{SIDE}] 用可靠队列消费 {count} 条消息并确认（对方 push 的消息同样可消费）");
        let queue = self.rds.get_reliable_queue::<String>("reliable");
        let mut got = 0;
        for _ in 0..count {
            match queue.take_one(-1).unwrap() {
                Some(msg) => {
                    println!("  · 消费到 {msg}（Ack 队列：{}）", queue.ack_key());
                    queue.acknowledge(&[msg.as_str()]).unwrap();
                    got += 1;
                }
                None => break,
            }
        }
        println!("  ✔ 已确认 {got} 条；剩余队列长度：{}", queue.count().unwrap());
    }

    fn queue_status(&mut self) {
        println!("[qstatus/{SIDE}] 可靠队列状态（可看到对方消费者的 Status JSON）");
        let queue = self.rds.get_reliable_queue::<String>("reliable");
        println!("  · 主队列长度：{}", queue.count().unwrap());

        let ack_keys = self
            .rds
            .search(&format!("{}reliable:Ack:*", self.prefix), 0)
            .unwrap();
        println!(
            "  · Ack 队列 {} 个：{}",
            ack_keys.len(),
            ack_keys.join(", ")
        );

        let status_keys = self
            .rds
            .search(&format!("{}reliable:Status:*", self.prefix), 0)
            .unwrap();
        for key in status_keys {
            let json = self.get_string(&key).unwrap();
            // 用 Rust 自己的类型解析对方（C#）写入的状态 JSON
            let parsed = self
                .get::<pek_rredis::RedisQueueStatus>(&key)
                .unwrap()
                .map(|st| {
                    format!(
                        "Key={} Machine={:?} Consumes={} Acks={} LastActive={}",
                        st.key, st.machine_name, st.consumes, st.acks, st.last_active
                    )
                })
                .unwrap_or_else(|| "解析失败".into());
            println!("  · 状态 {key}");
            println!("     原始：{}", json.unwrap_or_default());
            println!("     解析：{parsed}");
        }
    }

    fn lock(&mut self, seconds: u64) {
        println!("[lock/{SIDE}] 申请分布式锁 {}lock（持有 {seconds} 秒）", self.prefix);
        let timeout = (seconds * 1000 + 1000) as i32;
        let expire = (seconds * 1000) as i32;

        let lock = match self.rds.acquire_lock_ex("lock", timeout, expire, false) {
            Ok(Some(l)) => l,
            Ok(None) => {
                println!("  ✘ 未拿到锁（对方正持有）");
                return;
            }
            Err(e) => {
                println!("  ✘ 加锁失败：{e}");
                return;
            }
        };

        let value = self.get_string("lock").unwrap().unwrap_or_default();
        println!("  ✔ 已持锁，锁值 = {value}（格式 令牌|绝对过期毫秒）");
        std::thread::sleep(Duration::from_secs(seconds));
        drop(lock);
        println!("  · 释放锁");
    }

    fn report(&mut self) {
        println!("[report/{SIDE}] 双方回执");
        for side in [SIDE, OTHER] {
            let json = self.get_string(&format!("{side}:receipt")).unwrap();
            println!(
                "  · {side:<6}：{}",
                json.unwrap_or_else(|| "无（对方尚未运行 verify）".into())
            );
        }
    }

    fn exists(&mut self, key: &str) {
        let value = self.get_string(key).unwrap();
        println!(
            "[exists/{SIDE}] key={key} exists={} value={}",
            value.is_some(),
            value.unwrap_or_default()
        );
    }

    fn set_key(&mut self, key: &str, value: &str, expire: i64) {
        self.set(key, value.to_string(), expire).unwrap();
        println!("[set-key/{SIDE}] key={key} value={value} expire={expire}");
    }

    fn deferred_add(&mut self, name: &str, keys: &[String]) {
        println!("[deferred-add/{SIDE}] name={name} keys={}", keys.join(","));
        let deferred = RedisDeferred::new(self.rds.clone(), name);
        let added = deferred.add(keys.iter().cloned()).unwrap();
        println!("  ✔ added={added}");
    }

    fn deferred_process(&mut self, name: &str, batch_size: usize) -> i32 {
        println!("[deferred-process/{SIDE}] name={name} batch-size={batch_size}");
        let mut deferred = RedisDeferred::new(self.rds.clone(), name);
        deferred.batch_size = batch_size;

        let mut got = Vec::new();
        let processed = deferred
            .process_once(|keys| {
                got = keys.to_vec();
                Ok(())
            })
            .unwrap();
        got.sort();
        println!("  ✔ processed={processed} keys={}", got.join(","));
        0
    }

    fn stat_stage(&mut self, name: &str, key: &str, pairs: &[(String, i32)], delay_seconds: i64) -> i32 {
        println!(
            "[stat-stage/{SIDE}] name={name} key={key} delay={delay_seconds}s pairs={}",
            pairs
                .iter()
                .map(|(field, value)| format!("{field}={value}"))
                .collect::<Vec<_>>()
                .join(",")
        );
        let stat = RedisStat::new(self.rds.clone(), name).unwrap();
        for (field, value) in pairs {
            stat.increment(key, field, *value).unwrap();
        }
        let queued = stat.add_delay_queue(key, delay_seconds).unwrap();
        println!("  ✔ queued={queued}");
        0
    }

    fn stat_process_once(&mut self, name: &str, timeout_seconds: u64) -> i32 {
        println!("[stat-process-once/{SIDE}] name={name} timeout={timeout_seconds}s");
        let stat = RedisStat::new(self.rds.clone(), name).unwrap();
        let started = std::time::Instant::now();
        loop {
            let moved = stat.transfer_due_once(10).unwrap();
            let mut saved = None;
            let consumed = stat
                .consume_once(-1, |key, data| {
                    saved = Some((key.to_string(), data));
                    Ok(())
                })
                .unwrap();
            if consumed {
                let (key, data) = saved.unwrap();
                let mut items: Vec<(String, i32)> = data.into_iter().collect();
                items.sort_by(|a, b| a.0.cmp(&b.0));
                let text = items
                    .iter()
                    .map(|(field, value)| format!("{field}={value}"))
                    .collect::<Vec<_>>()
                    .join(",");
                println!("  ✔ key={key} moved={moved} data={text}");
                return 0;
            }
            if started.elapsed().as_secs() >= timeout_seconds {
                println!("  ✘ timeout waiting stat save");
                return 1;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn eventbus_publish(&mut self, topic: &str, group: &str, name: &str, count: i32) -> i32 {
        println!("[eventbus-publish/{SIDE}] topic={topic} group={group} name={name} count={count}");
        let bus = RedisEventBus::<ServiceEventDemo>::new(self.rds.clone(), topic, group).unwrap();
        let id = bus
            .publish(&ServiceEventDemo {
                name: name.to_string(),
                count,
            })
            .unwrap();
        println!("  ✔ id={id}");
        0
    }

    fn eventbus_subscribe(&mut self, topic: &str, group: &str, timeout_seconds: u64, from_first: bool) -> i32 {
        println!("[eventbus-subscribe/{SIDE}] topic={topic} group={group} timeout={timeout_seconds}s");
        let bus = RedisEventBus::<ServiceEventDemo>::new(self.rds.clone(), topic, group).unwrap();
        if from_first {
            bus.set_from_last_offset(false);
        }
        println!("  · ready");

        let started = std::time::Instant::now();
        loop {
            let mut seen = None;
            let processed = bus
                .consume_once(|event, message| {
                    seen = Some((event.clone(), message.id.clone()));
                    Ok(())
                })
                .unwrap();
            if processed {
                let (event, id) = seen.unwrap();
                println!("  ✔ id={id} name={} count={}", event.name, event.count);
                return 0;
            }
            if started.elapsed().as_secs() >= timeout_seconds {
                println!("  ✘ timeout waiting event");
                return 1;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    // ---------------- PubSub ----------------

    fn pubsub_publish(&mut self, channel: &str, message: &str, shard: bool) {
        println!(
            "[pubsub-publish/{SIDE}] {} channel={channel} message={message}",
            if shard { "SPUBLISH" } else { "PUBLISH" }
        );

        let pubsub = self.rds.get_pubsub(channel);
        let delivered = if shard {
            pubsub.spublish(message).unwrap_or(0)
        } else {
            pubsub.publish(message).unwrap_or(0)
        };
        println!("  ✔ delivered={delivered}");
    }

    fn pubsub_subscribe(
        &mut self,
        channel: &str,
        expected_message: &str,
        expected_channel: Option<&str>,
        timeout_seconds: u64,
        pattern: bool,
        shard: bool,
    ) {
        println!(
            "[pubsub-subscribe/{SIDE}] {} channel={channel} timeout={timeout_seconds}s expect={expected_message}",
            if pattern {
                "PSUBSCRIBE"
            } else if shard {
                "SSUBSCRIBE"
            } else {
                "SUBSCRIBE"
            }
        );

        let pubsub = self.rds.get_pubsub(channel);
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel2 = cancel.clone();
        let (tx, rx) = mpsc::sync_channel::<(Option<String>, String, String)>(1);

        let handle = std::thread::spawn(move || {
            if pattern {
                pubsub.psubscribe(cancel2.clone(), move |pat, ch, msg| {
                    println!("  · 收到 pattern={pat} channel={ch} message={msg}");
                    let _ = tx.send((Some(pat.to_string()), ch.to_string(), msg.to_string()));
                    cancel2.store(true, Ordering::SeqCst);
                })
            } else if shard {
                pubsub.ssubscribe(cancel2.clone(), move |ch, msg| {
                    println!("  · 收到 channel={ch} message={msg}");
                    let _ = tx.send((None, ch.to_string(), msg.to_string()));
                    cancel2.store(true, Ordering::SeqCst);
                })
            } else {
                pubsub.subscribe(cancel2.clone(), move |ch, msg| {
                    println!("  · 收到 channel={ch} message={msg}");
                    let _ = tx.send((None, ch.to_string(), msg.to_string()));
                    cancel2.store(true, Ordering::SeqCst);
                })
            }
        });

        let received = rx.recv_timeout(Duration::from_secs(timeout_seconds));
        cancel.store(true, Ordering::SeqCst);
        let join_result = handle.join().unwrap();
        if let Err(err) = join_result {
            self.check(false, "PubSub 订阅执行成功", Some(err.to_string()));
            return;
        }

        match received {
            Ok((_pattern, actual_channel, actual_message)) => {
                let ok = actual_message == expected_message
                    && expected_channel
                        .map(|value| value == actual_channel)
                        .unwrap_or(true);
                self.check(
                    ok,
                    "PubSub 收到预期消息",
                    Some(format!("channel={actual_channel} message={actual_message}")),
                );
            }
            Err(_) => self.check(
                false,
                "PubSub 收到预期消息",
                Some(format!("timeout={timeout_seconds}s channel={channel}")),
            ),
        }
    }

    fn clean(&mut self) {
        let n = self
            .rds
            .remove_pattern(&format!("{}*", self.prefix))
            .unwrap_or(0);
        println!("[clean/{SIDE}] 已删除 {n} 个键（模式 {}*）", self.prefix);
    }

    // ---------------- Stream 消息队列 ----------------

    fn stream_push(&mut self, count: usize, group: &str) {
        println!("[stream-push/{SIDE}] 写入 {count} 条 Stream 消息（group={group}）");

        let mut stream = self.rds.get_stream("stream:demo");
        let _ = stream.set_group(group);

        for i in 1..=count {
            let code = format!("stream-{i:04}");
            if i % 2 == 1 {
                let id = stream.add(&code, None).unwrap();
                println!(
                    "  · XADD 基元 {code} → {:?}（字段 {}）",
                    id, stream.primitive_key
                );
            } else {
                let id = stream
                    .add(
                        &StreamDemo {
                            name: code.clone(),
                            create_time: sample_time(),
                            count: i as i32,
                        },
                        None,
                    )
                    .unwrap();
                println!("  · XADD 对象 {code} → {:?}（字段 Name/CreateTime/Count）", id);
            }
        }

        println!("  ✔ 流长度：{}", stream.count().unwrap());
    }

    fn stream_consume(&mut self, count: usize, group: &str, no_ack: bool, retry_seconds: i64) {
        println!(
            "[stream-consume/{SIDE}] 消费最多 {count} 条（group={group}，{}，retry={retry_seconds}s）",
            if no_ack { "不确认" } else { "确认" }
        );

        let mut stream = self.rds.get_stream("stream:demo");
        stream.retry_interval_seconds = retry_seconds;
        let _ = stream.set_group(group);

        let msgs = stream.take_messages(count, 1000).unwrap();
        if msgs.is_empty() {
            println!("  · 没有消息");
            return;
        }

        for m in &msgs {
            println!("  · {} body=[{}]", m.id, m.body.join(","));
        }

        if !no_ack {
            let ids: Vec<&str> = msgs.iter().map(|m| m.id.as_str()).collect();
            let n = stream.acknowledge(&ids).unwrap();
            let pending = stream
                .pending_info(group)
                .unwrap()
                .map(|p| p.count)
                .unwrap_or(0);
            println!("  ✔ 已确认 {n} 条；挂起数：{pending}");
        } else {
            println!("  · 未确认（留作死信，可用另一侧 --retry-seconds 0 抢占）");
        }
    }

    fn stream_status(&mut self, group: &str) {
        let mut stream = self.rds.get_stream("stream:demo");
        let _ = stream.set_group(group);

        let len = stream.count().unwrap();
        let info = stream.get_info().unwrap();
        println!(
            "[stream-status/{SIDE}] XLEN={len} last-id={:?} groups={:?}",
            info.as_ref().and_then(|i| i.last_generated_id.clone()),
            info.as_ref().map(|i| i.groups),
        );

        for g in stream.get_groups().unwrap() {
            println!(
                "  · 组 {} consumers={} pending={} last-delivered={:?}",
                g.name, g.consumers, g.pending, g.last_delivered_id
            );
        }

        if let Some(pi) = stream.pending_info(group).unwrap() {
            let detail = pi
                .consumers
                .iter()
                .map(|(n, c)| format!("{n}={c}"))
                .collect::<Vec<_>>()
                .join(",");
            println!("  · 挂起：{} {detail}", pi.count);
        }

        for c in stream.get_consumers(group).unwrap() {
            println!("  · 消费者 {} pending={} idle={}ms", c.name, c.pending, c.idle);
        }
    }

    // ---------------- 延迟队列 ----------------

    fn delay_push(&mut self, count: usize, delay_seconds: i64) {
        println!("[delay-push/{SIDE}] 写入 {count} 条延迟消息（delay={delay_seconds}s）");
        let queue = self.rds.get_delay_queue::<String>("delay:demo");
        for i in 1..=count {
            queue.add(&format!("delay-{i:04}"), delay_seconds).unwrap();
        }
        println!(
            "  ✔ 延迟队列长度：{}（score = Unix 秒 + 延迟，与 C# 一致）",
            queue.count().unwrap()
        );
    }

    fn delay_consume(&mut self, count: usize, wait_seconds: u64) {
        println!("[delay-consume/{SIDE}] 等待并消费最多 {count} 条（最多等 {wait_seconds}s）");
        let queue = self.rds.get_delay_queue::<String>("delay:demo");

        let start = std::time::Instant::now();
        let mut got = 0usize;
        while got < count && start.elapsed().as_secs() < wait_seconds {
            match queue.take_one(-1).unwrap() {
                Some(v) => {
                    println!("  · 取到 {v}");
                    got += 1;
                }
                None => std::thread::sleep(Duration::from_millis(200)),
            }
        }

        println!("  ✔ 共取到 {got} 条；剩余：{}", queue.count().unwrap());
    }
}

// ======================= 离线字节格式自检 =======================

fn selftest() -> i32 {
    println!("[selftest] 离线校验编码器字节格式（与 C# 侧编码器逐字节对齐）");
    let mut failures = Vec::new();

    let mut check = |ok: bool, what: &str, detail: String| {
        if ok {
            println!("  ✔ {what}");
        } else {
            println!("  ✘ {what}  {detail}");
            failures.push(format!("{what}：{detail}"));
        }
    };

    let enc = |v: &dyn ToRedisPayload| -> String {
        String::from_utf8(v.to_redis_payload().unwrap().unwrap()).unwrap()
    };

    check(enc(&"hello") == "hello", "字符串原样（无引号）", enc(&"hello"));
    check(enc(&123) == "123", "整数文本", enc(&123));
    check(enc(&true) == "True", "布尔 True", enc(&true));
    check(enc(&false) == "False", "布尔 False", enc(&false));
    check(enc(&1.5) == "1.5", "浮点 1.5", enc(&1.5));
    check(
        enc(&sample_time()) == "2026-09-26 10:00:00.123",
        "时间 yyyy-MM-dd HH:mm:ss.fff",
        enc(&sample_time()),
    );

    // 解码方向：Rust 能读 C# 写入的格式
    check(
        <bool as FromRedisPayload>::from_redis_payload(b"OK").unwrap(),
        "解码 \"OK\" → true",
        String::new(),
    );
    check(
        <bool as FromRedisPayload>::from_redis_payload(b"True").unwrap(),
        "解码 \"True\" → true",
        String::new(),
    );
    let dt = <NaiveDateTime as FromRedisPayload>::from_redis_payload(b"2026-09-26 10:00:00.123")
        .unwrap();
    check(dt == sample_time(), "解码时间文本", dt.to_string());

    // JSON 复杂对象
    let model = DemoModel {
        name: SAMPLE_NAME.into(),
        create_time: sample_json_time(),
        count: SAMPLE_COUNT,
    };
    let json = enc(&Json(&model));
    println!("  · JSON 编码：{json}");
    let back = <Json<DemoModel> as FromRedisPayload>::from_redis_payload(json.as_bytes()).unwrap();
    check(
        back.0 == model,
        "JSON 往返",
        format!("{:?}", back.0),
    );

    if failures.is_empty() {
        println!("\n结果：全部通过");
        0
    } else {
        println!("\n结果：失败 {} 项", failures.len());
        for f in &failures {
            println!("  - {f}");
        }
        1
    }
}
