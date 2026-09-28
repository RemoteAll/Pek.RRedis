//! 真实 Redis 联调测试（可选）。
//!
//! 未设置 `REDIS_ADDR` 时全部跳过（打印提示，测试视为通过）；
//! 设置后自动连接真实 Redis 验证协议与数据互操作：
//!
//! ```powershell
//! $env:REDIS_ADDR = "127.0.0.1:6379"
//! $env:REDIS_PASSWORD = "123456"   # 可选
//! $env:REDIS_DB = "15"             # 可选，默认 15，避免污染业务库
//! cargo test --test live_redis -- --nocapture
//! ```
//!
//! 建议与 C# 侧 DH.NRedis 的 XUnitTest 指向同一个 Redis 实例：
//! 本套件写入的键都带 `pekrr:` 前缀，可安全清理。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use chrono::NaiveDateTime;

use pek_rredis::{FullRedis, FullRedis as Redis};

fn live_redis() -> Option<Redis> {
    let addr = match std::env::var("REDIS_ADDR") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            println!("[live] 未设置 REDIS_ADDR，跳过真实 Redis 联调测试");
            return None;
        }
    };

    let db: i32 = std::env::var("REDIS_DB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15);
    let mut config = format!("server={addr};db={db};timeout=5000");
    if let Ok(pwd) = std::env::var("REDIS_PASSWORD")
        && !pwd.is_empty()
    {
        config.push_str(&format!(";password={pwd}"));
    }

    match FullRedis::from_config(&config) {
        Ok(rds) => Some(rds),
        Err(e) => {
            println!("[live] 连接 {addr} 失败：{e}，跳过");
            None
        }
    }
}

fn prefix() -> String {
    format!("pekrr:{}:", std::process::id())
}

#[test]
fn live_basic_roundtrip_and_cross_format() {
    let Some(rds) = live_redis() else { return };
    let p = prefix();

    // 字符串 / 整数 / 布尔 / 时间：写入后可被 C# 端按同格式读取
    assert!(rds.redis().set(format!("{p}s"), "hello", 0).unwrap());
    assert!(rds.redis().set(format!("{p}i"), 123_i64, 0).unwrap());
    assert!(rds.redis().set(format!("{p}b"), true, 0).unwrap());

    let dt =
        NaiveDateTime::parse_from_str("2026-09-26 10:00:00.123", "%Y-%m-%d %H:%M:%S%.f").unwrap();
    assert!(rds.redis().set(format!("{p}d"), dt, 0).unwrap());

    // 原始字节应与 C# DefaultPacketEncoder 输出一致
    assert_eq!(
        rds.redis().get_string(&format!("{p}b")).unwrap().unwrap(),
        "True"
    );
    assert_eq!(
        rds.redis().get_string(&format!("{p}d")).unwrap().unwrap(),
        "2026-09-26 10:00:00.123"
    );
    assert_eq!(
        rds.redis().get::<bool>(&format!("{p}b")).unwrap(),
        Some(true)
    );
    assert_eq!(
        rds.redis()
            .get::<NaiveDateTime>(&format!("{p}d"))
            .unwrap()
            .unwrap(),
        dt
    );

    rds.remove_pattern(&format!("{p}*")).unwrap();
}

#[test]
fn live_hash_list_set_zset() {
    let Some(rds) = live_redis() else { return };
    let p = prefix();

    let hash = rds.get_hash::<i64>(&format!("{p}h"));
    hash.set(&"f".to_string(), &1).unwrap();
    hash.incr_by(&"f".to_string(), 41).unwrap();
    assert_eq!(hash.get(&"f".to_string()).unwrap(), Some(42));

    let list = rds.get_list::<i64>(&format!("{p}l"));
    list.push_back_many(&[1, 2, 3]).unwrap();
    assert_eq!(list.range(0, -1).unwrap(), vec![1, 2, 3]);

    let set = rds.get_set::<String>(&format!("{p}set"));
    set.add(&["a".into(), "b".into()]).unwrap();
    assert_eq!(set.len().unwrap(), 2);

    let zset = rds.get_sorted_set::<String>(&format!("{p}z"));
    zset.add(&"m1".to_string(), 2.0).unwrap();
    zset.add(&"m2".to_string(), 1.0).unwrap();
    assert_eq!(zset.range(0, -1).unwrap(), vec!["m2", "m1"]);

    rds.remove_pattern(&format!("{p}*")).unwrap();
}

#[test]
fn live_queue_and_reliable_queue() {
    let Some(rds) = live_redis() else { return };
    let p = prefix();

    // 普通队列（LPUSH + RPOP）
    let queue = rds.get_queue::<String>(&format!("{p}q"));
    queue.add_many(&["a".into(), "b".into()]).unwrap();
    assert_eq!(queue.take_one(-1).unwrap(), Some("a".into()));
    assert_eq!(queue.take_one(-1).unwrap(), Some("b".into()));

    // 可靠队列：取出 → 未确认回滚 → 再消费 → 确认
    let mut reliable = rds.get_reliable_queue::<String>(&format!("{p}rq"));
    reliable.retry_interval_seconds = 1;
    reliable.add(&"m1".into()).unwrap();
    assert_eq!(reliable.take_one(-1).unwrap(), Some("m1".into()));

    reliable.retry_ack().unwrap();
    assert_eq!(reliable.take_one(-1).unwrap(), Some("m1".into()));
    reliable.acknowledge(&["m1"]).unwrap();

    // 延迟队列：到期后可取
    let delay = rds.get_delay_queue::<String>(&format!("{p}dq"));
    delay.add(&"later".into(), 1).unwrap();
    std::thread::sleep(Duration::from_millis(1500));
    assert_eq!(delay.take_due(10).unwrap(), vec!["later"]);

    rds.remove_pattern(&format!("{p}*")).unwrap();
}

#[test]
fn live_search_and_lock() {
    let Some(rds) = live_redis() else { return };
    let p = prefix();

    for i in 0..5 {
        rds.redis().set(format!("{p}key:{i}"), "1", 0).unwrap();
    }
    let mut found = rds.search(&format!("{p}key:*"), 0).unwrap();
    found.sort();
    assert_eq!(found.len(), 5);

    // 分布式锁：互斥 + 释放
    let key = format!("{p}lock");
    let lock = rds.acquire_lock_ex(&key, 1000, 3000, false).unwrap();
    assert!(lock.is_some());
    let other = Redis::with_prefix(rds.redis().create_sub(15).unwrap(), None);
    assert!(
        other
            .acquire_lock_ex(&key, 0, 3000, false)
            .unwrap()
            .is_none()
    );
    drop(lock);
    assert!(
        other
            .acquire_lock_ex(&key, 0, 3000, false)
            .unwrap()
            .is_some()
    );

    rds.remove_pattern(&format!("{p}*")).unwrap();
}

#[test]
fn live_pubsub_roundtrip() {
    let Some(rds) = live_redis() else { return };
    let p = prefix();
    let channel = format!("{p}chan");

    let subscriber_rds = rds.clone();
    let channel2 = channel.clone();
    let ready = Arc::new(AtomicBool::new(false));
    let ready2 = ready.clone();
    let received: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
    let received2 = received.clone();

    let cancel = Arc::new(AtomicBool::new(false));
    let cancel2 = cancel.clone();

    let handle = std::thread::spawn(move || {
        let pubsub = subscriber_rds.get_pubsub(&channel2);
        // 简化：进入订阅前不等待，直接开始；发布端稍后重试
        pubsub
            .subscribe(cancel2, |_ch, msg| {
                ready2.store(true, Ordering::SeqCst);
                received2.lock().unwrap().push(msg.to_string());
            })
            .ok();
    });

    // 等待订阅建立后发布（最多重试 30 次）
    let pubsub = rds.get_pubsub(&channel);
    let mut published = 0;
    for _ in 0..30 {
        std::thread::sleep(Duration::from_millis(100));
        published = pubsub.publish("hello").unwrap_or(0);
        if published > 0 {
            break;
        }
    }

    std::thread::sleep(Duration::from_millis(200));
    cancel.store(true, Ordering::SeqCst);
    let _ = handle.join();

    assert!(published > 0, "至少有一个订阅者收到消息");
    assert!(ready.load(Ordering::SeqCst));
    assert_eq!(
        received.lock().unwrap().first().map(|s| s.as_str()),
        Some("hello")
    );
}
