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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use chrono::{Local, NaiveDateTime};
use serde::{Deserialize, Serialize};

use pek_rredis::encoder::Json;
use pek_rredis::{FromRedisPayload, FullRedis, ToRedisPayload};

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

    // selftest 不需要连接 Redis
    if command == "selftest" {
        return selftest();
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

    let mut ctx = DemoCtx {
        rds,
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
            println!("未知命令：{other}（可用：selftest/write/verify/push/consume/qstatus/lock/stream-push/stream-consume/stream-status/delay-push/delay-consume/pubsub-publish/pubsub-subscribe/report/clean/auto）");
            2
        }
    };

    if code != 0 {
        println!("\n结果：失败 {} 项", ctx.failures.len());
        for f in &ctx.failures {
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

const SAMPLE_STRING: &str = "Hello 互通";
const SAMPLE_INT: i32 = 123456789;
const SAMPLE_COUNT: i32 = 7;
const SAMPLE_NAME: &str = "互通Demo";

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

struct DemoCtx {
    rds: FullRedis,
    prefix: String,
    failures: Vec<String>,
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

        // 回执
        let receipt = DemoReceipt {
            side: SIDE.into(),
            time: Local::now().naive_local(),
            failures: self.failures.clone(),
        };
        let key = format!("{SIDE}:receipt");
        self.set(&key, Json(&receipt), 3600).unwrap();
        println!("  · 已写入回执 {}{key}", self.prefix);

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
