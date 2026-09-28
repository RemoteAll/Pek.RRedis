mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::runtime::Builder;

use pek_rredis::{AsyncFullRedis, ServerType};

fn runtime() -> tokio::runtime::Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn async_full_redis_roundtrips_basic_commands() {
    let (_server, sync) = support::mock_full();
    let redis = AsyncFullRedis::from_sync(sync);

    runtime().block_on(async {
        assert!(
            redis
                .redis()
                .set("async:key".into(), "value".to_string(), 0)
                .await
                .unwrap()
        );
        let value: Option<String> = redis.redis().get("async:key".into()).await.unwrap();
        assert_eq!(value.as_deref(), Some("value"));

        let keys = redis.search("async:*".into(), 0).await.unwrap();
        assert_eq!(keys, vec!["async:key".to_string()]);
    });
}

#[test]
fn async_queue_and_reliable_queue_roundtrip() {
    let (_server, sync) = support::mock_full();
    let redis = AsyncFullRedis::from_sync(sync);

    runtime().block_on(async {
        let queue = redis.get_queue::<String>("async-queue");
        assert_eq!(queue.add("q1".to_string()).await.unwrap(), 1);
        assert_eq!(queue.take_one(-1).await.unwrap().as_deref(), Some("q1"));

        let reliable = redis.get_reliable_queue::<String>("async-rq");
        assert_eq!(reliable.add("m1".to_string()).await.unwrap(), 1);
        let item = reliable.take_one(-1).await.unwrap();
        assert_eq!(item.as_deref(), Some("m1"));
        assert_eq!(
            reliable.acknowledge(vec!["m1".to_string()]).await.unwrap(),
            1
        );
    });
}

#[test]
fn async_stream_roundtrip() {
    let (_server, sync) = support::mock_full();
    let redis = AsyncFullRedis::from_sync(sync);

    runtime().block_on(async {
        let stream = redis.get_stream("async-stream");
        let id = stream.add("hello".to_string(), None).await.unwrap();
        assert!(id.is_some());
        let bodies: Vec<String> = stream.take_bodies(1).await.unwrap();
        assert_eq!(bodies, vec!["hello".to_string()]);
    });
}

#[test]
fn async_hash_list_set_sortedset_roundtrip() {
    let (_server, sync) = support::mock_full();
    let redis = AsyncFullRedis::from_sync(sync);

    runtime().block_on(async {
        let hash = redis.get_hash::<i64>("h:counter");
        assert_eq!(hash.set("pv".to_string(), 1).await.unwrap(), 1);
        assert_eq!(hash.set("pv".to_string(), 2).await.unwrap(), 0);
        assert_eq!(hash.incr_by("pv".to_string(), 10).await.unwrap(), 12);
        assert_eq!(hash.get("pv".to_string()).await.unwrap(), Some(12));
        assert!(hash.contains_key("pv".to_string()).await.unwrap());

        let list = redis.get_list::<i64>("l:queue");
        assert_eq!(list.push_back(1).await.unwrap(), 1);
        assert_eq!(list.push_back(2).await.unwrap(), 2);
        assert_eq!(list.push_front(0).await.unwrap(), 3);
        assert_eq!(list.range(0, -1).await.unwrap(), vec![0, 1, 2]);
        list.set(1, 10).await.unwrap();
        assert_eq!(list.get_all().await.unwrap(), vec![0, 10, 2]);

        let set = redis.get_set::<String>("s:tags");
        assert_eq!(
            set.add(vec!["a".into(), "b".into(), "a".into()])
                .await
                .unwrap(),
            2
        );
        assert!(set.contains("a".to_string()).await.unwrap());
        assert_eq!(set.remove(vec!["a".to_string()]).await.unwrap(), 1);

        let zset = redis.get_sorted_set::<String>("z:rank");
        zset.add("m1".to_string(), 1.5).await.unwrap();
        zset.add("m2".to_string(), 0.5).await.unwrap();
        zset.increment("m1".to_string(), 0.5).await.unwrap();
        assert_eq!(zset.score("m1".to_string()).await.unwrap(), Some(2.0));
        let all = zset.range_with_scores(0, -1).await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].0, "m2");
        assert_eq!(
            zset.range_by_score(1.0, 3.0, 0, 10).await.unwrap(),
            vec!["m1"]
        );
    });
}

#[test]
fn async_hyperloglog_and_stack_roundtrip() {
    let (_server, sync) = support::mock_full();
    let redis = AsyncFullRedis::from_sync(sync);

    runtime().block_on(async {
        let hll = redis.get_hyper_log_log("hll:pv");
        assert!(
            hll.add(vec!["a".into(), "b".into(), "c".into(), "a".into()])
                .await
                .unwrap()
        );
        assert_eq!(hll.count().await.unwrap(), 3);

        let stack = redis.get_stack::<i64>("st");
        assert_eq!(stack.push_many(vec![1, 2, 3]).await.unwrap(), 3);
        assert_eq!(stack.take_one(-1).await.unwrap(), Some(3));
        assert_eq!(stack.take(5).await.unwrap(), vec![2, 1]);
    });
}

#[test]
fn async_pubsub_roundtrip() {
    let (_server, sync) = support::mock_full();
    let redis = AsyncFullRedis::from_sync(sync);

    runtime().block_on(async {
        let cancel = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(AtomicBool::new(false));
        let received = Arc::new(std::sync::Mutex::new(Vec::new()));

        let subscriber = redis.clone();
        let cancel2 = cancel.clone();
        let ready2 = ready.clone();
        let received2 = received.clone();

        let handle = tokio::spawn(async move {
            subscriber
                .get_pubsub("async:chan")
                .subscribe(cancel2, move |_channel, message| {
                    ready2.store(true, Ordering::SeqCst);
                    received2.lock().unwrap().push(message);
                })
                .await
        });

        let pubsub = redis.get_pubsub("async:chan");
        let mut published = 0;
        for _ in 0..30 {
            std::thread::sleep(std::time::Duration::from_millis(50));
            published = pubsub.publish("hello".to_string()).await.unwrap_or(0);
            if published > 0 {
                break;
            }
        }

        std::thread::sleep(std::time::Duration::from_millis(150));
        cancel.store(true, Ordering::SeqCst);
        handle.await.unwrap().unwrap();

        assert!(published > 0);
        assert!(ready.load(Ordering::SeqCst));
        assert_eq!(
            received.lock().unwrap().first().map(|s| s.as_str()),
            Some("hello")
        );
        assert_eq!(
            pubsub
                .pubsub_numsub(vec!["async:chan".to_string()])
                .await
                .unwrap()[0]
                .1,
            0
        );
    });
}

#[test]
fn async_pattern_and_shard_pubsub_roundtrip() {
    let (_server, sync) = support::mock_full();
    let redis = AsyncFullRedis::from_sync(sync);

    runtime().block_on(async {
        let pattern_cancel = Arc::new(AtomicBool::new(false));
        let shard_cancel = Arc::new(AtomicBool::new(false));
        let pattern_received = Arc::new(std::sync::Mutex::new(Vec::new()));
        let shard_received = Arc::new(std::sync::Mutex::new(Vec::new()));

        let pattern_subscriber = redis.clone();
        let pattern_cancel2 = pattern_cancel.clone();
        let pattern_received2 = pattern_received.clone();
        let pattern_handle = tokio::spawn(async move {
            pattern_subscriber
                .get_pubsub("async:*")
                .psubscribe(pattern_cancel2, move |pattern, channel, message| {
                    pattern_received2
                        .lock()
                        .unwrap()
                        .push(format!("{pattern}|{channel}|{message}"));
                })
                .await
        });

        let shard_subscriber = redis.clone();
        let shard_cancel2 = shard_cancel.clone();
        let shard_received2 = shard_received.clone();
        let shard_handle = tokio::spawn(async move {
            shard_subscriber
                .get_pubsub("async:shard")
                .ssubscribe(shard_cancel2, move |channel, message| {
                    shard_received2
                        .lock()
                        .unwrap()
                        .push(format!("{channel}|{message}"));
                })
                .await
        });

        let pubsub = redis.get_pubsub("async:chan");
        let shard_pubsub = redis.get_pubsub("async:shard");

        for _ in 0..30 {
            std::thread::sleep(std::time::Duration::from_millis(50));
            let pattern_count = pubsub.publish("pattern".to_string()).await.unwrap_or(0);
            let shard_count = shard_pubsub
                .spublish("shard".to_string())
                .await
                .unwrap_or(0);
            if pattern_count > 0 && shard_count > 0 {
                break;
            }
        }

        std::thread::sleep(std::time::Duration::from_millis(150));
        pattern_cancel.store(true, Ordering::SeqCst);
        shard_cancel.store(true, Ordering::SeqCst);
        pattern_handle.await.unwrap().unwrap();
        shard_handle.await.unwrap().unwrap();

        assert_eq!(
            pattern_received.lock().unwrap().first().map(|s| s.as_str()),
            Some("async:*|async:chan|pattern")
        );
        assert_eq!(
            shard_received.lock().unwrap().first().map(|s| s.as_str()),
            Some("async:shard|shard")
        );
        assert_eq!(pubsub.pubsub_numpat().await.unwrap(), 0);
    });
}

#[test]
fn async_full_redis_explicit_surface_roundtrip() {
    let (server, sync) = support::mock_full();
    support::seed_slowlog(&server, 1, 1_700_000_000, 120, &["GET", "api:key"]);
    support::seed_latency(&server, "command", 1_700_000_000, 15, 20);
    let redis = AsyncFullRedis::from_sync(sync);

    runtime().block_on(async {
        let client = redis.redis();
        assert!(client.ping().await.unwrap());

        let info = client.info().await.unwrap();
        assert_eq!(info.get("redis_version").map(|s| s.as_str()), Some("7.4.0"));
        assert_eq!(client.version_parts().await.unwrap(), (7, 4, 0));
        assert_eq!(client.server_type().await.unwrap(), ServerType::Redis);

        assert!(redis.set("api:key".into(), "v".to_string(), 0).await.unwrap());
        assert_eq!(redis.get_string("api:key".into()).await.unwrap().as_deref(), Some("v"));
        assert!(redis.contains_key("api:key".into()).await.unwrap());
        assert_eq!(redis.append("api:key".into(), ":1".into()).await.unwrap(), 3);
        assert_eq!(redis.strlen("api:key".into()).await.unwrap(), 3);
        assert_eq!(redis.get_range("api:key".into(), 0, -1).await.unwrap(), "v:1");
        assert!(redis.set_expire("api:key".into(), 30).await.unwrap());
        assert_eq!(redis.get_expire("api:key".into()).await.unwrap(), 30);
        assert!(redis.rename("api:key".into(), "api:key2".into()).await.unwrap());
        assert_eq!(
            redis.get_string("api:key2".into()).await.unwrap().as_deref(),
            Some("v:1")
        );
        let paged = redis.search_paged("api:*".into(), 0, 10).await.unwrap();
        assert_eq!(paged.0, 0);
        assert_eq!(paged.1, vec!["api:key2".to_string()]);

        assert_eq!(redis.increment("counter".into(), 2).await.unwrap(), 2);
        assert_eq!(redis.decrement("counter".into(), 1).await.unwrap(), 1);

        let optional_lock = redis
            .acquire_lock_ex("lock:optional".into(), 200, 400, false)
            .await
            .unwrap();
        assert!(optional_lock.is_some());
        drop(optional_lock);
        redis.swapdb(0, 1).await.unwrap();

        let script: Option<String> = redis
            .eval("return ARGV[1]".into(), Vec::new(), vec!["lua".into()])
            .await
            .unwrap();
        assert_eq!(script.as_deref(), Some("lua"));

        assert_eq!(redis.slowlog_len().await.unwrap(), 1);
        let slowlog = redis.slowlog_get(10).await.unwrap();
        assert_eq!(slowlog.len(), 1);
        assert_eq!(slowlog[0].command[0], "GET");

        assert_eq!(
            redis.latency_history("command".into()).await.unwrap(),
            vec![(1_700_000_000, 15)]
        );
        let latest = redis.latency_latest().await.unwrap();
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].0, "command");
        assert_eq!(redis.latency_reset(vec!["command".into()]).await.unwrap(), 1);
        assert!(redis.latency_doctor().await.unwrap().contains("latency spikes"));

        let lib = redis
            .function_load(
                "#!lua name=testlib\nredis.register_function('echo', function(keys, args) return args[1] end)"
                    .into(),
                false,
            )
            .await
            .unwrap();
        assert_eq!(lib, "testlib");
        assert_eq!(redis.function_list(Some("testlib".into())).await.unwrap().len(), 1);
        let echoed: Option<String> = redis
            .fcall("echo".into(), Vec::new(), vec!["fcall".into()])
            .await
            .unwrap();
        assert_eq!(echoed.as_deref(), Some("fcall"));
        let echoed_ro: Option<String> = redis
            .fcall_ro("echo".into(), Vec::new(), vec!["fcall-ro".into()])
            .await
            .unwrap();
        assert_eq!(echoed_ro.as_deref(), Some("fcall-ro"));
        redis.function_delete("testlib".into()).await.unwrap();
        assert!(redis.function_list(Some("testlib".into())).await.unwrap().is_empty());

        let metrics = redis.get_prometheus_metrics().await.unwrap();
        assert!(metrics.contains("newlife_redis_connected_clients 1"));
        assert!(metrics.contains("newlife_redis_used_memory_bytes 1024"));
    });
}

#[test]
fn async_stream_management_surface_roundtrip() {
    let (_server, sync) = support::mock_full();
    let redis = AsyncFullRedis::from_sync(sync);

    runtime().block_on(async {
        let stream = redis.get_stream("async-stream-admin");

        assert!(stream.group_create("g1".into(), None).await.unwrap());
        assert!(stream.group_set_id("g1".into(), "0".into()).await.unwrap());
        assert_eq!(stream.get_groups().await.unwrap()[0].name, "g1");

        let id = stream
            .add("hello".to_string(), None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stream.get_info().await.unwrap().unwrap().length, 1);

        let range = stream.range(None, None, 10).await.unwrap();
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].id, id);

        let read_back = stream.read("0-0".into(), 10, 0).await.unwrap();
        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].id, id);

        assert!(!stream.set_group("g1".into()).await.unwrap());
        let taken = stream.take_message().await.unwrap().unwrap();
        assert_eq!(taken.id, id);

        let pending_info = stream.pending_info("g1".into()).await.unwrap().unwrap();
        assert_eq!(pending_info.count, 1);

        let pending = stream.pending("g1".into(), None, None, 10).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, id);

        let consumers = stream.get_consumers("g1".into()).await.unwrap();
        assert_eq!(consumers.len(), 1);
        assert!(!consumers[0].name.is_empty());

        let group_items = stream
            .read_group(
                "g1".into(),
                consumers[0].name.clone(),
                10,
                0,
                Some("0".into()),
            )
            .await
            .unwrap();
        assert_eq!(group_items.len(), 1);
        assert_eq!(group_items[0].id, id);

        assert_eq!(stream.acknowledge(vec![id.clone()]).await.unwrap(), 1);
        assert_eq!(
            stream
                .group_delete_consumer("g1".into(), consumers[0].name.clone())
                .await
                .unwrap(),
            0
        );
        assert_eq!(stream.group_destroy("g1".into()).await.unwrap(), 1);
        assert_eq!(stream.delete(id.clone()).await.unwrap(), 1);
        assert_eq!(stream.count().await.unwrap(), 0);
    });
}

#[test]
fn async_full_redis_helper_surface_roundtrip() {
    let (_server, sync) = support::mock_full();
    let redis = AsyncFullRedis::from_sync(sync);

    runtime().block_on(async {
        assert_eq!(
            redis
                .rpush(
                    "list:a".into(),
                    vec!["a".to_string(), "b".to_string(), "c".to_string()],
                )
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            redis
                .lpos("list:a".into(), "b".into(), 0, 1, 0)
                .await
                .unwrap(),
            vec![1]
        );

        let moved: Option<String> = redis
            .lmove("list:a".into(), "list:b".into(), false, true)
            .await
            .unwrap();
        assert_eq!(moved.as_deref(), Some("c"));

        let popped = redis
            .blpop_multi::<String>(vec!["list:b".into()], 0)
            .await
            .unwrap();
        assert_eq!(popped.as_ref().map(|(_, value)| value.as_str()), Some("c"));

        assert_eq!(
            redis
                .sadd("set:1".into(), vec!["a".to_string(), "b".to_string()])
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            redis
                .sadd("set:2".into(), vec!["b".to_string(), "c".to_string()])
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            redis
                .smismember("set:1".into(), vec!["a".into(), "x".into()])
                .await
                .unwrap(),
            vec![true, false]
        );
        assert_eq!(
            redis
                .sinter_card(vec!["set:1".into(), "set:2".into()], 0)
                .await
                .unwrap(),
            1
        );

        assert_eq!(redis.set_bit("bits".into(), 1, 1).await.unwrap(), 0);
        assert_eq!(redis.get_bit("bits".into(), 1).await.unwrap(), 1);
        assert_eq!(redis.bit_count("bits".into(), 0, -1).await.unwrap(), 1);
        assert_eq!(redis.bit_pos("bits".into(), 1, 0, -1).await.unwrap(), 1);

        let zset = redis.get_sorted_set::<String>("z:helper");
        zset.add("m1".to_string(), 1.0).await.unwrap();
        zset.add("m2".to_string(), 2.0).await.unwrap();
        assert_eq!(
            redis
                .zmscore(
                    "z:helper".into(),
                    vec!["m1".into(), "m2".into(), "m3".into()]
                )
                .await
                .unwrap(),
            vec![Some(1.0), Some(2.0), None]
        );
        assert_eq!(
            redis
                .zrand_member_with_scores::<String>("z:helper".into(), 2)
                .await
                .unwrap()
                .len(),
            2
        );
    });
}
