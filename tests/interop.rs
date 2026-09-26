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

// ================== Stream 消息队列（Redis 5.0+） ==================

/// 与 C# `DemoModel` 对应的对象消息（Stream 对象消息按属性名扁平化）。
/// 注意：Stream 字段路径用编码器文本时间（`yyyy-MM-dd HH:mm:ss.fff`）。
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "PascalCase")]
struct StreamOrder {
    code: String,
    count: i32,
    #[serde(with = "pek_rredis::encoder::datetime_text")]
    create_time: NaiveDateTime,
}

fn sample_time() -> NaiveDateTime {
    NaiveDateTime::parse_from_str("2026-09-26 10:00:00.123", "%Y-%m-%d %H:%M:%S%.f").unwrap()
}

#[test]
fn stream_add_encodes_like_csharp() {
    let (_server, full) = mock_full();
    let stream = full.get_stream("stream:enc");

    // 基元 → __data 字段；对象 → 属性名扁平化；数组 → 成对字段
    assert!(stream.add(&"hello", None).unwrap().is_some());
    stream.add(&7, None).unwrap();
    stream.add(&true, None).unwrap();
    stream
        .add(
            &StreamOrder {
                code: "A-001".into(),
                count: 3,
                create_time: sample_time(),
            },
            None,
        )
        .unwrap();
    stream.add(&vec!["k1", "v1", "k2", "v2"], None).unwrap();

    let msgs = stream.range(None, None, -1).unwrap();
    assert_eq!(msgs.len(), 5);

    assert_eq!(msgs[0].body, vec!["__data", "hello"]);
    assert_eq!(msgs[1].body, vec!["__data", "7"]);
    assert_eq!(msgs[2].body, vec!["__data", "True"]);

    // 对象消息：字段名 = C# 属性名；时间经编码器写入（保留毫秒）
    assert_eq!(msgs[3].field("Code"), Some("A-001"));
    assert_eq!(msgs[3].field("Count"), Some("3"));
    assert_eq!(msgs[3].field("CreateTime"), Some("2026-09-26 10:00:00.123"));

    assert_eq!(msgs[4].body, vec!["k1", "v1", "k2", "v2"]);

    // 消息 Id 形如 "ms-seq"
    assert!(msgs[0].id.contains('-'));
}

#[test]
fn stream_reads_csharp_style_written_messages() {
    // 模拟 C# 端对象消息：属性名 + 编码器文本（时间带 .fff），Id 由服务端生成
    let (_server, full) = mock_full();
    let stream = full.get_stream("stream:csharp");

    stream
        .add_fields(
            &[
                ("Code".into(), b"A-100".to_vec()),
                ("Count".into(), b"7".to_vec()),
                ("CreateTime".into(), b"2026-09-26 10:00:00.123".to_vec()),
            ],
            Some("1695792000000-0"),
            false,
        )
        .unwrap();

    let msgs = stream.range(Some("-"), Some("+"), 10).unwrap();
    assert_eq!(msgs[0].id, "1695792000000-0");

    let order = msgs[0].to_struct::<StreamOrder>().expect("应能映射为结构体");
    assert_eq!(
        order,
        StreamOrder {
            code: "A-100".into(),
            count: 7,
            create_time: sample_time(),
        }
    );

    // 非 group 独立消费也能拿到
    let mut stream2 = full.get_stream("stream:csharp");
    let bodies = stream2.take_messages(10, 0).unwrap();
    assert_eq!(bodies.len(), 1);
}

#[test]
fn stream_non_group_read_advances_start_id() {
    let (_server, full) = mock_full();
    let mut stream = full.get_stream("stream:free");

    stream.add(&"a", None).unwrap();
    stream.add(&"b", None).unwrap();

    let msgs = stream.take_messages(10, 0).unwrap();
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].body, vec!["__data", "a"]);

    // 游标已前移，不会重复消费
    assert!(stream.take_messages(10, 0).unwrap().is_empty());

    stream.add(&"c", None).unwrap();
    assert_eq!(stream.take_bodies::<String>(10).unwrap(), vec!["c"]);
}

#[test]
fn stream_group_consume_ack_and_status() {
    let (_server, full) = mock_full();
    let mut stream = full.get_stream("stream:group");

    assert!(stream.set_group("g1").unwrap(), "首次应创建消费组");
    assert!(!stream.set_group("g1").unwrap(), "已存在则不再创建");
    assert_eq!(stream.get_groups().unwrap().len(), 1);

    for i in 1..=3 {
        stream.add(&format!("m-{i}"), None).unwrap();
    }

    let msgs = stream.take_messages(10, 0).unwrap();
    assert_eq!(msgs.len(), 3);
    assert_eq!(msgs.iter().map(|m| m.primitive::<String>()).collect::<Vec<_>>(),
        vec![Some("m-1".into()), Some("m-2".into()), Some("m-3".into())]);

    // 未确认 → 挂起 3 条，且能查到消费者
    let pending = stream.pending_info("g1").unwrap().unwrap();
    assert_eq!(pending.count, 3);
    assert_eq!(pending.consumers.len(), 1);
    let consumers = stream.get_consumers("g1").unwrap();
    assert_eq!(consumers.len(), 1);
    assert_eq!(consumers[0].pending, 3);

    // 确认后挂起清零
    let ids: Vec<&str> = msgs.iter().map(|m| m.id.as_str()).collect();
    assert_eq!(stream.acknowledge(&ids).unwrap(), 3);
    assert_eq!(stream.pending_info("g1").unwrap().unwrap().count, 0);

    // 流信息
    let info = stream.get_info().unwrap().unwrap();
    assert_eq!(info.length, 3);
    assert_eq!(info.groups, 1);
    assert!(info.last_generated_id.is_some());
}

#[test]
fn stream_object_messages_roundtrip_in_group() {
    let (_server, full) = mock_full();
    let mut stream = full.get_stream("stream:orders");
    stream.set_group("orders").unwrap();

    for (code, count) in [("A-001", 3), ("A-002", 5)] {
        stream
            .add(
                &StreamOrder {
                    code: code.into(),
                    count,
                    create_time: sample_time(),
                },
                None,
            )
            .unwrap();
    }

    let orders: Vec<StreamOrder> = stream.take_structs(10).unwrap();
    assert_eq!(orders.len(), 2);
    assert_eq!(orders[0].code, "A-001");
    assert_eq!(orders[1].count, 5);
    assert_eq!(orders[0].create_time, sample_time());
}

#[test]
fn stream_retry_ack_steals_pending_from_other_consumer() {
    let (_server, full) = mock_full();
    let key = "stream:steal";

    // 消费者 A 取走但“崩溃”未确认
    {
        let mut a = full.get_stream(key);
        a.set_group("g").unwrap();
        a.add(&"m1", None).unwrap();
        let msgs = a.take_messages(1, 0).unwrap();
        assert_eq!(msgs.len(), 1);
    }

    // 消费者 B（同组）在空闲超时后抢占
    let mut b = full.get_stream(key);
    b.set_group("g").unwrap();
    b.retry_interval_seconds = 0;
    std::thread::sleep(Duration::from_millis(20)); // 让 A 的挂起消息 idle > 0

    assert_eq!(b.retry_ack().unwrap(), 1, "应抢回 1 条死信");

    // 抢回的消息优先被消费（claims 路径），确认后挂起清零
    let msgs = b.take_messages(10, 0).unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].primitive::<String>(), Some("m1".into()));
    let ids: Vec<&str> = msgs.iter().map(|m| m.id.as_str()).collect();
    assert_eq!(b.acknowledge(&ids).unwrap(), 1);
    assert_eq!(b.pending_info("g").unwrap().unwrap().count, 0);
}

#[test]
fn stream_trim_and_delete() {
    let (_server, full) = mock_full();
    let stream = full.get_stream("stream:trim");

    for i in 1..=10 {
        stream
            .add_fields(
                &[("n".into(), i.to_string().into_bytes())],
                Some(&format!("16957920000{i:02}-0")),
                false,
            )
            .unwrap();
    }
    assert_eq!(stream.count().unwrap(), 10);

    assert_eq!(stream.trim(5, true).unwrap(), 5);
    assert_eq!(stream.count().unwrap(), 5);

    let msgs = stream.range(None, None, -1).unwrap();
    assert_eq!(stream.delete(&msgs[0].id).unwrap(), 1);
    assert_eq!(stream.count().unwrap(), 4);
}

// ================== 编码器可直接使用 ==================

#[test]
fn encoder_can_be_used_directly_for_cross_language_payloads() {
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
