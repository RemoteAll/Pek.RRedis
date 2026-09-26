//! 功能对齐审计测试：逐项验证与 DH.NRedis（C#）对齐的补漏功能。
//!
//! 覆盖本轮审计新增的全部命令：`GETEX`/`EXPIRETIME`/`PEXPIRETIME`/`OBJECT IDLETIME|FREQ`/
//! `BITFIELD`、`HGETDEL`/`HGETEX`、`LMOVE`/`BLMOVE`/`LMPOP`/`BRPOP`/`BLPOP`、
//! `SMISMEMBER`/`SINTERCARD`、`ZMSCORE`/`ZRANDMEMBER`/`ZRANGESTORE`/`ZDIFF`/`ZUNION`/`ZINTER`/
//! `ZMPOP`/`BZPOPMIN`、`SWAPDB`/`WAIT`/`REPLICAOF`/`SLOWLOG`/`LATENCY`/
//! `FUNCTION`/`FCALL`、RedLock 与 `consume_json` 大循环消费。

mod support;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pek_rredis::{Error, FullRedis, ServerType, acquire_red_lock};
use support::*;

// ==================== 键命令扩展 ====================

#[test]
fn get_ex_sets_and_clears_expiry() {
    let (_server, redis) = mock_full();
    redis.redis().set("k", "v", 0).unwrap();

    let got: Option<String> = redis.get_ex("k", 120).unwrap();
    assert_eq!(got.as_deref(), Some("v"));
    assert!(redis.expire_time("k").unwrap() > 0, "EX 应设置过期");
    assert!(redis.pexpire_time("k").unwrap() > 0);

    // expire = 0 → PERSIST
    let _: Option<String> = redis.get_ex("k", 0).unwrap();
    assert_eq!(redis.expire_time("k").unwrap(), -1);
    assert_eq!(redis.pexpire_time("k").unwrap(), -1);

    // expire < 0 → PX（毫秒）
    let _: Option<String> = redis.get_ex("k", -30_000).unwrap();
    assert!(redis.pexpire_time("k").unwrap() > 0);

    // 不存在的键
    assert_eq!(redis.expire_time("none").unwrap(), -2);
    let missing: Option<String> = redis.get_ex("none", 10).unwrap();
    assert!(missing.is_none());
}

#[test]
fn object_idle_time_and_freq() {
    let (_server, redis) = mock_full();
    redis.redis().set("ok", "1", 0).unwrap();
    assert_eq!(redis.object_idle_time("ok").unwrap(), Some(0));
    assert_eq!(redis.object_freq("ok").unwrap(), Some(0));
    assert_eq!(redis.object_idle_time("missing").unwrap(), None);
    assert_eq!(redis.object_freq("missing").unwrap(), None);
}

#[test]
fn bit_field_roundtrip() {
    let (_server, redis) = mock_full();

    let rs = redis
        .bit_field("bits", &["SET", "u8", "0", "255", "GET", "u8", "0"])
        .unwrap();
    assert_eq!(rs, vec![0, 255]);

    // INCRBY 溢出按位宽回绕（255 + 1 → 0）
    let rs = redis.bit_field("bits", &["INCRBY", "u8", "0", "1"]).unwrap();
    assert_eq!(rs, vec![0]);

    // 有符号与 `#` 字段偏移
    let rs = redis
        .bit_field("bits2", &["SET", "i8", "#0", "-1", "GET", "i8", "#0"])
        .unwrap();
    assert_eq!(rs, vec![0, -1]);
}

// ==================== 哈希扩展 ====================

#[test]
fn hash_hgetdel_and_hgetex() {
    let (_server, redis) = mock_full();
    let hash = redis.get_hash::<String>("h");

    hash.set(&"f1".to_string(), &"v1".to_string()).unwrap();
    let old = hash.hgetdel(&"f1".to_string()).unwrap();
    assert_eq!(old.as_deref(), Some("v1"), "HGETDEL 应返回旧值");
    assert!(hash.get(&"f1".to_string()).unwrap().is_none(), "字段应被删除");
    assert!(hash.hgetdel(&"f1".to_string()).unwrap().is_none());

    hash.set(&"f2".to_string(), &"v2".to_string()).unwrap();
    let v = hash.hgetex(&"f2".to_string(), 60).unwrap();
    assert_eq!(v.as_deref(), Some("v2"), "HGETEX 应返回字段值");
    let v = hash.hgetex(&"f2".to_string(), 0).unwrap();
    assert_eq!(v.as_deref(), Some("v2"), "PERSIST 分支");
}

// ==================== 列表扩展 ====================

#[test]
fn list_index_of_insert_and_remove_at() {
    let (_server, redis) = mock_full();
    let list = redis.get_list::<String>("l");
    list.push_back_many(&["a".into(), "b".into(), "c".into()]).unwrap();

    assert_eq!(list.index_of(&"b".to_string()).unwrap(), Some(1));
    assert_eq!(list.index_of(&"x".to_string()).unwrap(), None);

    assert_eq!(list.insert_at(1, &"x".to_string()).unwrap(), 4);
    assert_eq!(list.get_all().unwrap(), vec!["a", "x", "b", "c"]);

    assert_eq!(list.remove_at(0).unwrap(), 1);
    assert_eq!(list.get_all().unwrap(), vec!["x", "b", "c"]);

    assert_eq!(list.remove_at(99).unwrap(), 0, "越界删除返回 0");
    assert_eq!(list.insert_at(99, &"y".to_string()).unwrap(), -1, "越界插入返回 -1");
}

#[test]
fn lmove_blmove_and_lmpop() {
    let (_server, redis) = mock_full();
    let src = redis.get_list::<String>("src");
    src.push_back_many(&["1".into(), "2".into(), "3".into()]).unwrap();

    // RIGHT → LEFT
    let moved: Option<String> = redis.lmove("src", "dst", false, true).unwrap();
    assert_eq!(moved.as_deref(), Some("3"));
    let dst = redis.get_list::<String>("dst");
    assert_eq!(dst.get_all().unwrap(), vec!["3"]);

    // BLMOVE（mock 不阻塞，等价立即移动）：LEFT → RIGHT
    let moved: Option<String> = redis.blmove("src", "dst", true, false, 1).unwrap();
    assert_eq!(moved.as_deref(), Some("1"));
    assert_eq!(dst.get_all().unwrap(), vec!["3", "1"]);

    // LMPOP：跳过空键，从 src 右侧弹（此时 src 仅剩 "2"）
    let popped = redis
        .lmpop::<String>(&["empty1", "src"], false, 2)
        .unwrap()
        .expect("应有数据");
    assert_eq!(popped.0, "src");
    assert_eq!(popped.1, vec!["2"]);

    let none = redis.lmpop::<String>(&["empty1", "empty2"], true, 1).unwrap();
    assert!(none.is_none());
}

#[test]
fn brpop_and_blpop_multi_key() {
    let (_server, redis) = mock_full();
    let list = redis.get_list::<String>("mq2");
    list.push_back_many(&["a".into(), "b".into()]).unwrap();

    // 第一个键为空 → 落到第二个键
    let got: Option<(String, String)> = redis.brpop_multi(&["mq1", "mq2"], 1).unwrap();
    assert_eq!(got, Some(("mq2".to_string(), "b".to_string())));

    let got: Option<(String, String)> = redis.blpop_multi(&["mq1", "mq2"], 1).unwrap();
    assert_eq!(got, Some(("mq2".to_string(), "a".to_string())));

    let none: Option<(String, String)> = redis.brpop_multi(&["mq1", "mq2"], 1).unwrap();
    assert!(none.is_none(), "无数据应返回 None");
}

// ==================== 集合扩展 ====================

#[test]
fn set_mismember_and_sinter_card() {
    let (_server, redis) = mock_full();
    let s1 = redis.get_set::<String>("s1");
    s1.add(&["a".into(), "b".into(), "c".into()]).unwrap();
    let s2 = redis.get_set::<String>("s2");
    s2.add(&["b".into(), "c".into(), "d".into()]).unwrap();

    let flags = s1.mismember(&["a".into(), "x".into(), "c".into()]).unwrap();
    assert_eq!(flags, vec![true, false, true]);

    assert_eq!(redis.sinter_card(&["s1", "s2"], 0).unwrap(), 2);
    assert_eq!(redis.sinter_card(&["s1", "s2"], 1).unwrap(), 1, "LIMIT 生效");
    assert_eq!(redis.sinter_card(&["s1", "none"], 0).unwrap(), 0);
}

// ==================== 有序集合扩展 ====================

#[test]
fn zset_add_options_and_aggregations() {
    let (_server, redis) = mock_full();
    let z1 = redis.get_sorted_set::<String>("z1");
    z1.add(&"a".to_string(), 1.0).unwrap();
    z1.add(&"b".to_string(), 2.0).unwrap();
    let z2 = redis.get_sorted_set::<String>("z2");
    z2.add(&"b".to_string(), 3.0).unwrap();
    z2.add(&"c".to_string(), 4.0).unwrap();

    // INCR 选项：a: 1 + 5 = 6
    let v = z1.add_with_options("INCR", &[(5.0, "a".to_string())]).unwrap();
    assert_eq!(v, 6.0);
    // NX 选项：已存在成员不更新
    let n = z1.add_with_options("NX", &[(0.0, "a".to_string())]).unwrap();
    assert_eq!(n, 0.0);

    // 并集（SUM）：a=6, b=2+3=5, c=4 → 按分数排序 c,b,a
    assert_eq!(z1.union(&["z2"], None, None).unwrap(), vec!["c", "b", "a"]);
    // 并集（权重 [1,2]，SUM）：a=6×1, b=2×1+3×2=8, c=4×2=8
    let weighted = z1
        .union_with_scores(&["z2"], Some(&[1.0, 2.0]), Some("SUM"))
        .unwrap();
    assert_eq!(
        weighted.iter().map(|(m, s)| (m.as_str(), *s)).collect::<Vec<_>>(),
        vec![("a", 6.0), ("b", 8.0), ("c", 8.0)]
    );
    // 并集（MAX）：a=6, b=max(2,3)=3, c=4 → b,c,a
    assert_eq!(
        z1.union(&["z2"], None, Some("MAX")).unwrap(),
        vec!["b", "c", "a"]
    );

    // 交集：仅 b（2+3=5）
    assert_eq!(z1.inter(&["z2"], None, None).unwrap(), vec!["b"]);
    let inter = z1.inter_with_scores(&["z2"], None, None).unwrap();
    assert_eq!(inter, vec![("b".to_string(), 5.0)]);
    assert_eq!(z1.inter_store("zinter", &["z2"], None, None).unwrap(), 1);

    // 差集：a 不在 z2 中
    assert_eq!(z1.diff(&["z2"]).unwrap(), vec!["a"]);
    assert_eq!(z1.diff_store("zdiff", &["z2"]).unwrap(), 1);
    let stored = redis.get_sorted_set::<String>("zdiff");
    assert_eq!(stored.range(0, -1).unwrap(), vec!["a"]);

    // ZRANGESTORE（BYSCORE）
    assert_eq!(
        z1.range_store("zr", 2.0, 10.0, true, false, 0, 0).unwrap(),
        2
    );
    let zr = redis.get_sorted_set::<String>("zr");
    assert_eq!(zr.range(0, -1).unwrap(), vec!["b", "a"]);

    // ZRANGESTORE（REV + LIMIT）
    assert_eq!(
        z1.range_store("zr2", 0.0, -1.0, false, true, 0, 1).unwrap(),
        1
    );
    let zr2 = redis.get_sorted_set::<String>("zr2");
    assert_eq!(zr2.range(0, -1).unwrap(), vec!["a"]);
}

#[test]
fn zmscore_zrandmember_zmpop_bzpop() {
    let (_server, redis) = mock_full();
    let z = redis.get_sorted_set::<String>("zs");
    z.add(&"a".to_string(), 1.0).unwrap();
    z.add(&"b".to_string(), 2.0).unwrap();
    z.add(&"c".to_string(), 3.0).unwrap();

    assert_eq!(
        redis.zmscore("zs", &["a", "x", "c"]).unwrap(),
        vec![Some(1.0), None, Some(3.0)]
    );

    let mut rand = redis.zrand_member::<String>("zs", 10).unwrap();
    rand.sort();
    assert_eq!(rand, vec!["a", "b", "c"]);
    let scored = redis.zrand_member_with_scores::<String>("zs", 2).unwrap();
    assert_eq!(scored.len(), 2);

    // ZMPOP MIN 1
    let popped = redis
        .zmpop::<String>(&["zs"], true, 1)
        .unwrap()
        .expect("应有数据");
    assert_eq!(popped.0, "zs");
    assert_eq!(popped.1, vec![("a".to_string(), 1.0)]);

    // BZPOPMAX → c
    let got = redis
        .bzpopmax::<String>(&["empty", "zs"], 1)
        .unwrap()
        .expect("应有数据");
    assert_eq!(got, ("zs".to_string(), "c".to_string(), 3.0));

    // BZPOPMIN → b
    let got = redis
        .bzpopmin::<String>(&["zs"], 1)
        .unwrap()
        .expect("应有数据");
    assert_eq!(got, ("zs".to_string(), "b".to_string(), 2.0));

    let none = redis.bzpopmin::<String>(&["zs"], 1).unwrap();
    assert!(none.is_none());
}

// ==================== 服务器信息与运维命令 ====================

#[test]
fn server_info_ops_and_functions() {
    let (server, redis) = mock_full();

    assert_eq!(redis.server_type().unwrap(), ServerType::Redis);
    assert_eq!(redis.version_parts().unwrap(), (7, 4, 0));
    assert!(redis.info_all().unwrap().contains_key("redis_version"));
    assert!(matches!(
        redis.redis().require_version("99.0", "TEST").unwrap_err(),
        Error::Unsupported(_)
    ));

    // SWAPDB / WAIT / REPLICAOF
    redis.swapdb(0, 1).unwrap();
    assert_eq!(redis.wait(1, 100).unwrap(), 0);
    redis.replica_of(None, 6379).unwrap();
    redis.replica_of(Some("127.0.0.1"), 6380).unwrap();

    // SLOWLOG
    assert_eq!(redis.slowlog_len().unwrap(), 0);
    seed_slowlog(&server, 7, 1_695_798_000, 1500, &["GET", "foo"]);
    let entries = redis.slowlog_get(10).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].id, 7);
    assert_eq!(entries[0].timestamp, 1_695_798_000);
    assert_eq!(entries[0].duration_us, 1500);
    assert_eq!(entries[0].command, vec!["GET", "foo"]);
    assert_eq!(entries[0].client_addr.as_deref(), Some("127.0.0.1:0"));
    redis.slowlog_reset().unwrap();
    assert_eq!(redis.slowlog_len().unwrap(), 0);

    // LATENCY
    seed_latency(&server, "command", 1_695_798_000, 15, 40);
    assert_eq!(
        redis.latency_latest().unwrap(),
        vec![("command".to_string(), 1_695_798_000, 15, 40)]
    );
    assert_eq!(
        redis.latency_history("command").unwrap(),
        vec![(1_695_798_000, 15)]
    );
    assert_eq!(redis.latency_reset(&["command"]).unwrap(), 1);
    assert!(redis.latency_latest().unwrap().is_empty());
    assert!(!redis.latency_doctor().unwrap().is_empty());

    // FUNCTION + FCALL
    let lib = redis
        .function_load("#!lua name=mylib\nredis.register_function('echo')", false)
        .unwrap();
    assert_eq!(lib, "mylib");
    redis
        .function_load("#!lua name=mylib\nredis.register_function('echo')", true)
        .unwrap();
    assert!(!redis.function_list(None).unwrap().is_empty());
    assert!(!redis.function_list(Some("mylib")).unwrap().is_empty());

    let got: Option<String> = redis.fcall("mini_echo", &["k1"], &["hello"]).unwrap();
    assert_eq!(got.as_deref(), Some("hello"));
    let got: Option<String> = redis.fcall_ro("mini_echo", &[], &["ro"]).unwrap();
    assert_eq!(got.as_deref(), Some("ro"));

    redis.function_delete("mylib").unwrap();
    assert!(redis.function_list(None).unwrap().is_empty());
}

#[test]
fn prometheus_metrics_export() {
    let (_server, redis) = mock_full();
    let text = redis.get_prometheus_metrics().unwrap();
    assert!(text.contains("newlife_redis_connected_clients 1"), "{text}");
    assert!(text.contains("newlife_redis_used_memory_bytes 1024"), "{text}");
    assert!(text.contains("newlife_redis_commands_processed_total 7"), "{text}");
    assert!(text.contains("newlife_redis_db0_keys 1"), "{text}");
}

// ==================== RedLock ====================

#[test]
fn red_lock_acquire_and_release() {
    let a = start_mock_redis();
    let b = start_mock_redis();
    let c = start_mock_redis();
    let r1 = FullRedis::from_config(&format!("server={};db=0", a.addr)).unwrap();
    let r2 = FullRedis::from_config(&format!("server={};db=0", b.addr)).unwrap();
    let r3 = FullRedis::from_config(&format!("server={};db=0", c.addr)).unwrap();

    let lock = acquire_red_lock(&[r1, r2, r3], "rl:key", 1000, 30_000)
        .unwrap()
        .expect("应获得 RedLock");

    assert_eq!(lock.locked_count(), 3);
    let token = lock.token().to_string();
    assert_eq!(token.len(), 22, "令牌应为 22 位随机串");
    for s in [&a, &b, &c] {
        assert_eq!(
            raw_get(s, "rl:key").as_deref(),
            Some(token.as_bytes()),
            "令牌应写入每个实例"
        );
    }

    drop(lock);
    for s in [&a, &b, &c] {
        assert!(raw_get(s, "rl:key").is_none(), "释放后应删除锁");
    }
}

#[test]
fn red_lock_times_out_without_quorum() {
    let a = start_mock_redis();
    let live = FullRedis::from_config(&format!("server={};db=0", a.addr)).unwrap();
    // 端口 1 必然连接失败 → 仅 1/2 成功，达不到 quorum(2)
    let dead = FullRedis::from_config("server=127.0.0.1:1;db=0").unwrap();

    let start = std::time::Instant::now();
    let lock = acquire_red_lock(&[live, dead], "rl:nq", 500, 30_000).unwrap();
    assert!(lock.is_none(), "达不到多数派应返回 None");
    assert!(
        start.elapsed().as_millis() >= 300,
        "应等待到超时才返回"
    );
    assert!(raw_get(&a, "rl:nq").is_none(), "失败后应回滚已写实例");
}

#[test]
fn red_lock_via_full_redis_applies_prefix() {
    let a = start_mock_redis();
    let b = start_mock_redis();
    let c = start_mock_redis();
    let main = FullRedis::from_config(&format!("server={};db=0;prefix=app:", a.addr)).unwrap();
    let o1 = FullRedis::from_config(&format!("server={};db=0", b.addr)).unwrap();
    let o2 = FullRedis::from_config(&format!("server={};db=0", c.addr)).unwrap();

    let lock = main
        .acquire_red_lock(&[o1, o2], "shared", 1000, 30_000)
        .unwrap()
        .expect("应获得 RedLock");
    assert_eq!(lock.key(), "app:shared");
    for s in [&a, &b, &c] {
        assert!(raw_get(s, "app:shared").is_some(), "前缀应生效");
    }
}

// ==================== 类型化大循环消费（consume_json） ====================

#[test]
fn consume_json_success_acks_message() {
    let (server, redis) = mock_full();
    let queue = redis.get_reliable_queue::<String>("cq_ok");
    queue
        .add(&"{\"Id\":\"m1\",\"Name\":\"n\"}".to_string())
        .unwrap();

    let cancel = AtomicBool::new(false);
    let hits = Arc::new(Mutex::new(Vec::<String>::new()));
    let sink = hits.clone();
    queue
        .consume_json::<serde_json::Value, _>(
            0,
            Duration::from_millis(2),
            None,
            &cancel,
            |msg, raw| {
                assert!(raw.contains("m1"));
                sink.lock()
                    .unwrap()
                    .push(msg["Id"].as_str().unwrap_or_default().to_string());
                cancel.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .unwrap();

    assert_eq!(hits.lock().unwrap().as_slice(), ["m1"]);
    assert_eq!(list_len(&server, "cq_ok"), 0, "主队列应已清空");
    assert_eq!(list_len(&server, queue.ack_key()), 0, "确认队列应已清空");
}

#[test]
fn consume_json_failure_counts_and_retries() {
    let (server, redis) = mock_full();
    let mut queue = redis.get_reliable_queue::<String>("cq_err");
    queue.retry_interval_seconds = 0; // 便于手动触发回滚
    queue.add(&"{\"Id\":\"m2\"}".to_string()).unwrap();

    let cancel = AtomicBool::new(false);
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    queue
        .consume_json::<serde_json::Value, _>(
            0,
            Duration::from_millis(2),
            None,
            &cancel,
            |_, _| {
                counter.fetch_add(1, Ordering::SeqCst);
                cancel.store(true, Ordering::SeqCst);
                Err("处理失败".into())
            },
        )
        .unwrap();

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    // 失败不确认：消息留在 Ack 队列，主队列为空
    assert_eq!(list_len(&server, "cq_err"), 0);
    assert_eq!(list_len(&server, queue.ack_key()), 1);
    // 错误计数写入备份库（mock 共用存储）：{topic}:Error:{id}
    assert_eq!(
        raw_get(&server, "cq_err:Error:m2").as_deref(),
        Some(b"1".as_slice())
    );

    // retry_ack 回滚重投后可再次消费并确认
    queue.retry_ack().unwrap();
    assert_eq!(list_len(&server, "cq_err"), 1);
    assert_eq!(list_len(&server, queue.ack_key()), 0);

    let cancel2 = AtomicBool::new(false);
    queue
        .consume_json::<serde_json::Value, _>(
            0,
            Duration::from_millis(2),
            None,
            &cancel2,
            |_, _| {
                cancel2.store(true, Ordering::SeqCst);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(list_len(&server, "cq_err"), 0);
    assert_eq!(list_len(&server, queue.ack_key()), 0);
}

#[test]
fn consume_json_discards_after_ten_failures() {
    let (server, redis) = mock_full();
    let mut queue = redis.get_reliable_queue::<String>("cq_poison");
    queue.retry_interval_seconds = 0;
    queue.add(&"{\"Id\":\"poison\"}".to_string()).unwrap();

    let mut attempts = 0;
    for _ in 0..12 {
        // 第 10 次失败后消息被确认丢弃，错误计数固定为 10
        if raw_get(&server, "cq_poison:Error:poison").as_deref() == Some(b"10".as_slice()) {
            break;
        }
        if list_len(&server, "cq_poison") == 0 {
            queue.retry_ack().unwrap();
        }

        let cancel = AtomicBool::new(false);
        queue
            .consume_json::<serde_json::Value, _>(
                0,
                Duration::from_millis(1),
                None,
                &cancel,
                |_, _| {
                    attempts += 1;
                    cancel.store(true, Ordering::SeqCst);
                    Err("fail".into())
                },
            )
            .unwrap();
    }

    assert_eq!(attempts, 10, "累计失败 10 次后应丢弃");
    assert_eq!(list_len(&server, "cq_poison"), 0);
    assert_eq!(list_len(&server, queue.ack_key()), 0, "毒消息应被确认丢弃");
    assert_eq!(
        raw_get(&server, "cq_poison:Error:poison").as_deref(),
        Some(b"10".as_slice())
    );
}

// ==================== 版本门禁 ====================

#[test]
fn require_version_blocks_old_server() {
    let (_server, redis) = mock_full();
    // mock 汇报 7.4.0：7.0 命令放行
    assert!(redis.redis().require_version("7.0", "LMPOP").is_ok());
    // 未来版本命令应报 Unsupported
    let err = redis.redis().require_version("8.2", "NEWCMD").unwrap_err();
    match err {
        Error::Unsupported(msg) => assert!(msg.contains("NEWCMD"), "{msg}"),
        other => panic!("期望 Unsupported，实际 {other:?}"),
    }
}

// ==================== Tair 扩展（阿里云） ====================

#[test]
fn tair_ex_extensions() {
    let (_server, redis) = mock_full();

    // TairString：EXSET / EXGET
    let r = redis.ex_set("ek", "hello", 0, 0).unwrap();
    assert_eq!(r.as_deref(), Some("OK"));
    let (val, ver) = redis.ex_get::<String>("ek").unwrap().unwrap();
    assert_eq!(val.as_deref(), Some("hello"));
    assert_eq!(ver, 1);

    // TairString：EXINCRBY（值 + 版本号）
    assert_eq!(redis.ex_incr_by("ec", 5, 0, 0).unwrap().unwrap(), (5, 1));
    assert_eq!(redis.ex_incr_by("ec", 3, 0, 0).unwrap().unwrap(), (8, 2));

    // TairHash：EXHSET / EXHGET / EXHGETWITHVER
    assert_eq!(redis.ex_hset("eh", "f1", "v1", 0, false, 0).unwrap(), 1);
    assert_eq!(redis.ex_hset("eh", "f1", "v2", 0, false, 0).unwrap(), 0);
    assert_eq!(redis.ex_hset("eh", "f1", "v3", 0, true, 0).unwrap(), 0, "NX 已存在不更新");
    assert_eq!(
        redis.ex_hget::<String>("eh", "f1").unwrap().as_deref(),
        Some("v2")
    );
    let (val, fver) = redis.ex_hget_with_ver::<String>("eh", "f1").unwrap().unwrap();
    assert_eq!(val.as_deref(), Some("v2"));
    assert_eq!(fver, 2);

    // TairHash：EXHINCRBY / EXHMGET / EXHPTTL
    assert_eq!(redis.ex_hincr_by("eh", "cnt", 7, 0).unwrap(), 7);
    assert_eq!(redis.ex_hincr_by("eh", "cnt", 1, 0).unwrap(), 8);
    let m = redis.ex_hmget::<String>("eh", &["f1", "cnt", "none"]).unwrap();
    assert_eq!(m[0].as_deref(), Some("v2"));
    assert_eq!(m[1].as_deref(), Some("8"));
    assert!(m[2].is_none());
    assert_eq!(redis.ex_hpttl("eh", "f1").unwrap(), -1);
    assert_eq!(redis.ex_hpttl("eh", "none").unwrap(), -2);

    // TairHash：EXHKEYS / EXHVALS / EXHLEN / EXHDEL
    let mut keys = redis.ex_hkeys("eh").unwrap();
    keys.sort();
    assert_eq!(keys, vec!["cnt", "f1"]);
    assert_eq!(redis.ex_hlen("eh").unwrap(), 2);
    let mut vals = redis.ex_hvals::<String>("eh").unwrap();
    vals.sort();
    assert_eq!(vals, vec!["8", "v2"]);
    assert_eq!(redis.ex_hdel("eh", &["f1"]).unwrap(), 1);
    assert!(redis.ex_hget::<String>("eh", "f1").unwrap().is_none());
}

// ==================== 字符串大循环消费（consume_raw） ====================

#[test]
fn consume_raw_success_and_error_count() {
    let (server, redis) = mock_full();
    let mut queue = redis.get_reliable_queue::<String>("cr");
    queue.retry_interval_seconds = 0;
    queue.add(&"plain-message".to_string()).unwrap();

    // 第一次：处理失败 → 计数 +1，消息留在 Ack 队列
    let cancel = AtomicBool::new(false);
    queue
        .consume_raw(0, Duration::from_millis(1), &cancel, |raw| {
            assert_eq!(raw, "plain-message");
            cancel.store(true, Ordering::SeqCst);
            Err("fail".into())
        })
        .unwrap();
    assert_eq!(list_len(&server, queue.ack_key()), 1);
    let md5_key = format!("cr:Error:{:x}", md5::compute(b"plain-message"));
    assert_eq!(
        raw_get(&server, &md5_key).as_deref(),
        Some(b"1".as_slice())
    );

    // 回滚后成功消费
    queue.retry_ack().unwrap();
    let cancel2 = AtomicBool::new(false);
    queue
        .consume_raw(0, Duration::from_millis(1), &cancel2, |_| {
            cancel2.store(true, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    assert_eq!(list_len(&server, "cr"), 0);
    assert_eq!(list_len(&server, queue.ack_key()), 0);
}
