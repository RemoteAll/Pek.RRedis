//! 端到端互操作测试（进程内迷你 Redis，离线可跑）。
//!
//! 重点验证与 DH.NRedis 互通所依赖的行为：
//! 字节编码格式、数据结构命令、管道、队列语义（含可靠队列 Ack/回滚）、延迟队列、
//! 分布式锁抢占，以及 SCAN 搜索与模式删除。

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use pek_rredis::encoder::Json;
use pek_rredis::{FullRedis, FromRedisPayload, ToRedisPayload};

use support::{exists, list_len, mock_full, raw_get};

// ================== 编码格式（与 C# 互通的核心） ==================

#[test]
fn string_payloads_match_csharp_wire_format() {
    let (server, full) = mock_full();
    let rds = full.redis();

    assert!(rds.set("s", "hello", 0).unwrap());
    assert_eq!(raw_get(&server, "s").unwrap(), b"hello");

    assert!(rds.set("i", 123_i32, 0).unwrap());
    assert_eq!(raw_get(&server, "i").unwrap(), b"123");

    assert!(rds.set("b", true, 0).unwrap());
    assert_eq!(raw_get(&server, "b").unwrap(), b"True");

    let dt = NaiveDateTime::parse_from_str("2026-09-26 10:00:00.123456", "%Y-%m-%d %H:%M:%S%.f")
        .unwrap();
    assert!(rds.set("d", dt, 0).unwrap());
    assert_eq!(raw_get(&server, "d").unwrap(), b"2026-09-26 10:00:00.123");

    // 读取方向：C# 写入的 "True" 可读回 bool
    assert_eq!(rds.get::<bool>("b").unwrap(), Some(true));
    assert_eq!(rds.get::<i32>("i").unwrap(), Some(123));
    assert_eq!(
        rds.get::<NaiveDateTime>("d").unwrap().unwrap().to_string(),
        "2026-09-26 10:00:00.123"
    );
}

#[test]
fn json_object_roundtrip_matches_system_text_json() {
    let (server, full) = mock_full();

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    #[serde(rename_all = "PascalCase")]
    struct User {
        name: String,
        create_time: NaiveDateTime,
    }

    let user = User {
        name: "NewLife".into(),
        create_time: NaiveDateTime::parse_from_str("2026-09-26 10:00:00", "%Y-%m-%d %H:%M:%S")
            .unwrap(),
    };

    assert!(full.redis().set("user", Json(&user), 0).unwrap());
    assert_eq!(
        String::from_utf8(raw_get(&server, "user").unwrap()).unwrap(),
        r#"{"Name":"NewLife","CreateTime":"2026-09-26T10:00:00"}"#
    );

    let back: Json<User> = full.redis().get("user").unwrap().unwrap();
    assert_eq!(back.0, user);
}

#[test]
fn set_get_expire_and_batch_operations() {
    let (_server, full) = mock_full();
    let rds = full.redis();

    // 默认过期与负数走 SETEX
    assert!(rds.set("k1", "v1", 120).unwrap());
    assert!(rds.get_expire("k1").unwrap() > 0);

    // 批量写入/读取
    rds.set_all(
        &[("a", "1"), ("b", "2"), ("c", "3")],
        60,
    )
    .unwrap();
    assert_eq!(rds.get_string("a").unwrap().as_deref(), Some("1"));
    let dic = rds.get_all::<i32>(&["a", "b", "c", "missing"]).unwrap();
    assert_eq!(dic.get("a"), Some(&1));
    assert_eq!(dic.get("c"), Some(&3));
    assert!(!dic.contains_key("missing"));

    // 自增
    assert_eq!(rds.increment("counter", 1).unwrap(), 1);
    assert_eq!(rds.increment("counter", 4).unwrap(), 5);
    assert_eq!(rds.decrement("counter", 2).unwrap(), 3);
    assert_eq!(rds.increment_float("f", 1.5).unwrap(), 1.5);

    // 仅当不存在
    assert!(!rds.add("a", "x", 0).unwrap());
    assert!(rds.add("new", "x", 0).unwrap());
    assert_eq!(rds.get_string("new").unwrap().as_deref(), Some("x"));

    // 删除与存在性
    assert!(rds.contains_key("new").unwrap());
    assert_eq!(rds.remove("new").unwrap(), 1);
    assert!(!rds.contains_key("new").unwrap());
}

// ================== 数据结构 ==================

#[test]
fn hash_structure_operations() {
    let (_server, full) = mock_full();
    let hash = full.get_hash::<i64>("h:counter");

    hash.set(&"pv".to_string(), &1).unwrap();
    hash.set(&"pv".to_string(), &2).unwrap();
    hash.set(&"uv".to_string(), &9).unwrap();

    assert_eq!(hash.count().unwrap(), 2);
    assert_eq!(hash.get(&"pv".to_string()).unwrap(), Some(2));
    assert_eq!(hash.incr_by(&"pv".to_string(), 10).unwrap(), 12);
    assert_eq!(hash.get(&"pv".to_string()).unwrap(), Some(12));

    let dic = hash.get_all_map().unwrap();
    assert_eq!(dic.len(), 2);
    assert_eq!(dic.get("uv").unwrap(), &Some(9));

    assert_eq!(hash.remove(&["uv".to_string()]).unwrap(), 1);
    assert!(!hash.contains_key(&"uv".to_string()).unwrap());
}

#[test]
fn list_structure_operations() {
    let (_server, full) = mock_full();
    let list = full.get_list::<i64>("l:queue");

    list.push_back(&1).unwrap();
    list.push_back(&2).unwrap();
    list.push_front(&0).unwrap();
    assert_eq!(list.range(0, -1).unwrap(), vec![0, 1, 2]);
    assert_eq!(list.len().unwrap(), 3);

    assert_eq!(list.get(1).unwrap(), Some(1));
    list.set(1, &10).unwrap();
    assert_eq!(list.get_all().unwrap(), vec![0, 10, 2]);

    assert_eq!(list.pop_back().unwrap(), Some(2));
    assert_eq!(list.pop_front().unwrap(), Some(0));
    assert_eq!(list.len().unwrap(), 1);

    // RPOPLPUSH：右侧弹出并插入目标左侧（可靠队列基础）
    list.push_back(&7).unwrap();
    let dest = full.get_list::<i64>("l:ack");
    assert_eq!(list.rpoplpush("l:ack").unwrap(), Some(7));
    assert_eq!(dest.get_all().unwrap(), vec![7]);
}

#[test]
fn set_and_sorted_set_operations() {
    let (_server, full) = mock_full();

    let set = full.get_set::<String>("s:tags");
    assert_eq!(set.add(&["a".into(), "b".into(), "a".into()]).unwrap(), 2);
    assert!(set.contains(&"a".to_string()).unwrap());
    assert_eq!(set.len().unwrap(), 2);
    assert_eq!(set.remove(&["a".to_string()]).unwrap(), 1);

    let zset = full.get_sorted_set::<String>("z:rank");
    zset.add(&"m1".to_string(), 1.5).unwrap();
    zset.add(&"m2".to_string(), 0.5).unwrap();
    zset.increment(&"m1".to_string(), 0.5).unwrap();

    assert_eq!(zset.score(&"m1".to_string()).unwrap(), Some(2.0));
    let all = zset.range_with_scores(0, -1).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].0, "m2");
    assert_eq!(zset.range_by_score(1.0, 3.0, 0, 10).unwrap(), vec!["m1"]);

    // 延迟队列依赖的 ZRANGEBYSCORE 数值边界（score=0）
    zset.add(&"due".to_string(), 0.0).unwrap();
    assert_eq!(zset.range_by_score(0.0, 0.0, 0, 1).unwrap(), vec!["due"]);
}

#[test]
fn hyperloglog_and_stack_operations() {
    let (_server, full) = mock_full();

    let hll = full.get_hyper_log_log("hll:pv");
    hll.add(&["a", "b", "c", "a"]).unwrap();
    assert_eq!(hll.count().unwrap(), 3);

    let stack = full.get_stack::<i64>("st");
    stack.push_many(&[1, 2, 3]).unwrap();
    assert_eq!(stack.take_one(-1).unwrap(), Some(3));
    assert_eq!(stack.take(5).unwrap(), vec![2, 1]);
}

// ================== 管道/搜索/服务器信息 ==================

#[test]
fn pipeline_batches_commands() {
    let (_server, full) = mock_full();
    let rds = full.redis();

    let mut pipeline = rds.pipeline();
    pipeline.set("p:1", "v1").unwrap();
    pipeline.set("p:2", "v2").unwrap();
    pipeline.get("p:1");
    pipeline.remove("p:2");

    let results = pipeline.execute().unwrap();
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].as_string().as_deref(), Some("OK"));
    assert_eq!(results[2].as_string().as_deref(), Some("v1"));
    assert_eq!(results[3].as_i64(), Some(1));
}

#[test]
fn search_and_remove_by_pattern() {
    let (_server, full) = mock_full();
    let rds = full.redis();

    for key in ["app:a", "app:b", "app:c", "other"] {
        rds.set(key, "1", 0).unwrap();
    }

    let mut found = full.search("app:*", 0).unwrap();
    found.sort();
    assert_eq!(found, vec!["app:a", "app:b", "app:c"]);

    assert_eq!(full.remove_pattern("app:*").unwrap(), 3);
    assert_eq!(full.search("app:*", 0).unwrap().len(), 0);
    assert!(rds.contains_key("other").unwrap());
}

#[test]
fn info_and_version() {
    let (_server, full) = mock_full();
    let info = full.redis().info().unwrap();
    assert_eq!(info.get("redis_version").unwrap(), "7.2.4");
    assert_eq!(full.redis().version().unwrap().as_deref(), Some("7.2.4"));
}

// ================== 队列 ==================

#[test]
fn simple_queue_is_fifo_and_supports_batch_take() {
    let (_server, full) = mock_full();
    let queue = full.get_queue::<String>("q:orders");

    queue.add_many(&["a".into(), "b".into(), "c".into()]).unwrap();
    assert_eq!(queue.count().unwrap(), 3);

    // 管道批量消费：左进右出，顺序为 a、b、c
    assert_eq!(queue.take(3).unwrap(), vec!["a", "b", "c"]);
    assert_eq!(queue.count().unwrap(), 0);
    assert_eq!(queue.take(3).unwrap(), Vec::<String>::new());

    queue.add(&"d".into()).unwrap();
    assert_eq!(queue.take_one(-1).unwrap(), Some("d".into()));
}

#[test]
fn reliable_queue_ack_then_rollback_own_dead_letters() {
    let (server, full) = mock_full();
    let mut queue = full.get_reliable_queue::<String>("rq:orders");
    queue.retry_interval_seconds = 0; // 让死信检测立即可用

    queue.add(&"m1".into()).unwrap();

    // 取出后消息进入确认队列（模拟处理中）
    let msg = queue.take_one(-1).unwrap().unwrap();
    assert_eq!(msg, "m1");
    assert_eq!(list_len(&server, queue.ack_key()), 1);
    assert_eq!(queue.count().unwrap(), 0);

    // 正常确认：确认队列清空
    assert_eq!(queue.acknowledge(&["m1"]).unwrap(), 1);
    assert_eq!(list_len(&server, queue.ack_key()), 0);
    assert_eq!(queue.status().acks, 1);

    // 模拟处理失败：取出后不确认，下一次 retry_ack 回滚到主队列
    queue.add(&"m2".into()).unwrap();
    let msg = queue.take_one(-1).unwrap().unwrap();
    assert_eq!(msg, "m2");
    assert_eq!(queue.count().unwrap(), 0);

    queue.retry_ack().unwrap();
    assert_eq!(queue.count().unwrap(), 1, "未确认消息应被回滚到主队列");
    assert_eq!(queue.take_one(-1).unwrap(), Some("m2".into()));
}

#[test]
fn reliable_queue_rolls_back_dead_consumer() {
    let (server, full) = mock_full();

    // 消费者 A 取走消息后“崩溃”（状态时间过期）
    let mut a = full.get_reliable_queue::<String>("rq2:jobs");
    a.retry_interval_seconds = 0;
    a.add(&"x".into()).unwrap();
    assert_eq!(a.take_one(-1).unwrap(), Some("x".into()));
    assert_eq!(list_len(&server, a.ack_key()), 1);

    // 篡改 A 的状态为很久以前
    let stale = format!(
        r#"{{"Key":"{}","MachineName":"old","UserName":"u","ProcessId":1,"Ip":"1.1.1.1","CreateTime":"2020-01-01T00:00:00+08:00","LastActive":"2020-01-01T00:00:00+08:00","Consumes":1,"Acks":0}}"#,
        a.consumer_key()
    );
    support::set_raw(&server, a.status_key(), &stale, 60);

    // 消费者 B 触发全局回滚
    let b = full.get_reliable_queue::<String>("rq2:jobs");
    let rolled = b.rollback_all_ack().unwrap();
    assert_eq!(rolled, 1);
    assert_eq!(b.count().unwrap(), 1, "死信应回到主队列");
    assert!(!exists(&server, a.status_key()), "失效状态应被清理");

    // 消息可再次消费
    assert_eq!(b.take_one(-1).unwrap(), Some("x".into()));
}

#[test]
fn reliable_queue_publish_and_consume_kv_pair() {
    let (_server, full) = mock_full();
    let mut queue = full.get_reliable_queue::<String>("rq3:kv");
    queue.retry_interval_seconds = 0;

    queue.publish(&[("msg:001", &"hello".to_string())], 120).unwrap();

    let handled = queue
        .consume(-1, |msg| {
            assert_eq!(msg, "hello");
            Ok(msg.len())
        })
        .unwrap();
    assert_eq!(handled, Some(5));

    // 消息体与消息键都被清理
    assert_eq!(queue.count().unwrap(), 0);
    assert_eq!(full.redis().get_string("msg:001").unwrap(), None);
}

#[test]
fn delay_queue_delivers_due_messages_and_transfers() {
    let (server, full) = mock_full();
    let delay = full.get_delay_queue::<String>("dq:orders");
    let target = full.get_queue::<String>("dq:main");

    // 已到期（score = now - 1）
    delay.add(&"d1".into(), -1).unwrap();
    delay.add(&"d2".into(), -1).unwrap();
    // 未到期
    delay.add(&"later".into(), 3600).unwrap();
    assert_eq!(delay.count().unwrap(), 3);

    // 抢占到期消息：只有 d1、d2 可被取走
    let due = delay.take_due(10).unwrap();
    assert_eq!(due, vec!["d1", "d2"]);
    assert_eq!(delay.count().unwrap(), 1, "未到期消息不可被抢占");

    // 放回到期消息，验证转移循环（独立线程，与 C# TransferAsync 等价）
    delay.add(&"d1".into(), -1).unwrap();
    delay.add(&"d2".into(), -1).unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    {
        let cancel_flag = cancel.clone();
        std::thread::scope(|s| {
            let handle = s.spawn(|| delay.transfer_loop(&target, cancel_flag));
            std::thread::sleep(Duration::from_millis(300));
            cancel.store(true, Ordering::SeqCst);
            handle.join().unwrap().unwrap();
        });
    }

    // 到期消息被转移到主队列（FIFO），未到期消息仍保留
    assert_eq!(target.take(10).unwrap(), vec!["d1", "d2"]);
    assert_eq!(delay.count().unwrap(), 1);
    assert!(exists(&server, "dq:orders"));
}

// ================== 分布式锁 ==================

#[test]
fn distributed_lock_is_mutually_exclusive() {
    let (_server, full) = mock_full();
    let other = full.redis().create_sub(0).unwrap();
    let other = FullRedis::with_prefix(other, None);

    let lock = full.acquire_lock_ex("lk", 0, 5000, true).unwrap();
    assert!(lock.is_some());

    // 他人抢锁失败（等待 0 毫秒）
    let conflict = other.acquire_lock_ex("lk", 0, 5000, false).unwrap();
    assert!(conflict.is_none());

    // 释放后可抢
    drop(lock);
    let lock2 = other.acquire_lock_ex("lk", 0, 5000, false).unwrap();
    assert!(lock2.is_some());
}

#[test]
fn distributed_lock_steals_expired_lock_without_misdeleting() {
    let (server, full) = mock_full();
    let other = FullRedis::with_prefix(full.redis().create_sub(0).unwrap(), None);

    // A 持锁 100ms 后过期（未主动释放）
    let mut lock_a = full.acquire_lock_ex("lk2", 0, 100, true).unwrap().unwrap();
    std::thread::sleep(Duration::from_millis(250));

    // B 抢占成功
    let lock_b = other.acquire_lock_ex("lk2", 2000, 5000, false).unwrap();
    assert!(lock_b.is_some());

    // A 释放时不应误删 B 的锁（令牌归属校验）
    lock_a.release();
    assert!(exists(&server, "lk2"), "A 不得删除他人持有的锁");

    drop(lock_b);
    assert!(!exists(&server, "lk2"), "B 释放后锁应消失");
}

// ================== 编码器可直接使用 ==================

#[test]
fn encoder_can_be_used_directly_for_cross_language_payloads() {
    // C# 端写入的 JSON（System.Text.Json）
    let json = br#"{"Name":"HiLink","CreateTime":"2026-09-26T10:00:00"}"#;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    #[serde(rename_all = "PascalCase")]
    struct User {
        name: String,
        create_time: NaiveDateTime,
    }

    let user = <Json<User> as FromRedisPayload>::from_redis_payload(json).unwrap();
    assert_eq!(user.0.name, "HiLink");

    // Rust 端写回同样格式
    let payload = Json(&user.0).to_redis_payload().unwrap().unwrap();
    assert_eq!(payload, json.to_vec());
}
