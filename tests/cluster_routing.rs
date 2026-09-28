mod support;

use std::ops::RangeInclusive;
use std::sync::Arc;

use pek_rredis::{FullRedis, RedisClusterTopology, hash_slot};

use support::{
    exists, raw_get, redirect_once, require_asking, set_cluster_nodes, set_info_mode,
    set_info_replication, set_info_sentinel, set_info_text, set_raw, start_mock_redis,
};

fn cluster_nodes(entries: &[(&str, &str, &str)]) -> String {
    entries
        .iter()
        .map(|(id, addr, spec)| format!("{id} {addr}@0 master - 0 0 1 connected {spec}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn cluster_nodes_with_replica(master_addr: &str, replica_addr: &str, other_addr: &str) -> String {
    format!(
        "master-a {master_addr}@0 master - 0 0 1 connected 0-5460\nreplica-a {replica_addr}@0 slave master-a 0 0 1 connected 0-5460\nmaster-b {other_addr}@0 master - 0 0 2 connected 5461-16383"
    )
}

fn key_in_slot(range: RangeInclusive<u16>) -> String {
    for i in 0..200_000u32 {
        let key = format!("cluster:key:{i}");
        if range.contains(&hash_slot(&key)) {
            return key;
        }
    }
    panic!("未找到目标槽位范围内的测试 key");
}

#[test]
fn cluster_routes_single_key_writes_to_selected_primary() {
    let server_a = start_mock_redis();
    let server_b = start_mock_redis();
    let full = FullRedis::from_config(&format!("server={};db=0", server_a.addr)).unwrap();

    let topology = RedisClusterTopology::from_cluster_nodes(
        &cluster_nodes(&[
            ("master-a", &server_a.addr, "0-5460"),
            ("master-b", &server_b.addr, "5461-16383"),
        ]),
        false,
    );
    full.set_topology(Arc::new(topology));

    let key = key_in_slot(5461..=16383);
    assert!(full.set(&key, "value", 0).unwrap());

    assert_eq!(raw_get(&server_a, &key), None);
    assert_eq!(raw_get(&server_b, &key).as_deref(), Some(&b"value"[..]));
}

#[test]
fn cluster_routes_reads_to_replica_when_enabled() {
    let master = start_mock_redis();
    let replica = start_mock_redis();
    let other = start_mock_redis();
    let full = FullRedis::from_config(&format!("server={};db=0", master.addr)).unwrap();

    let topology = RedisClusterTopology::from_cluster_nodes(
        &cluster_nodes_with_replica(&master.addr, &replica.addr, &other.addr),
        true,
    );
    full.set_topology(Arc::new(topology));

    let key = key_in_slot(0..=5460);
    set_raw(&replica, &key, "from-replica", 0);

    assert_eq!(
        full.get::<String>(&key).unwrap().as_deref(),
        Some("from-replica")
    );
    assert!(!exists(&master, &key));
}

#[test]
fn cluster_routes_same_slot_rename_and_copy_by_source_key() {
    let server_a = start_mock_redis();
    let server_b = start_mock_redis();
    let full = FullRedis::from_config(&format!("server={};db=0", server_a.addr)).unwrap();

    let topology = RedisClusterTopology::from_cluster_nodes(
        &cluster_nodes(&[
            ("master-a", &server_a.addr, "0-5460"),
            ("master-b", &server_b.addr, "5461-16383"),
        ]),
        false,
    );
    full.set_topology(Arc::new(topology));

    let source = "{cluster-route}:src";
    let renamed = "{cluster-route}:renamed";
    let copied = "{cluster-route}:copy";

    assert!(hash_slot(source) > 5460);
    assert!(full.set(source, "v1", 0).unwrap());
    assert!(full.rename(source, renamed).unwrap());
    assert!(full.copy(renamed, copied, None, false).unwrap());

    assert!(!exists(&server_a, source));
    assert_eq!(raw_get(&server_b, renamed).as_deref(), Some(&b"v1"[..]));
    assert_eq!(raw_get(&server_b, copied).as_deref(), Some(&b"v1"[..]));
}

#[test]
fn cluster_moved_redirect_updates_slot_mapping() {
    let server_a = start_mock_redis();
    let server_b = start_mock_redis();
    let full = FullRedis::from_config(&format!("server={};db=0", server_a.addr)).unwrap();

    let topology = RedisClusterTopology::from_cluster_nodes(
        &cluster_nodes(&[
            ("master-a", &server_a.addr, "0-5460"),
            ("master-b", &server_b.addr, "5461-16383"),
        ]),
        false,
    );
    full.set_topology(Arc::new(topology));

    let key = key_in_slot(0..=5460);
    let slot = hash_slot(&key);
    set_raw(&server_a, &key, "stale", 0);
    set_raw(&server_b, &key, "fresh", 0);
    redirect_once(&server_a, &key, &format!("MOVED {slot} {}", server_b.addr));

    assert_eq!(full.get::<String>(&key).unwrap().as_deref(), Some("fresh"));
    assert_eq!(full.get::<String>(&key).unwrap().as_deref(), Some("fresh"));
}

#[test]
fn cluster_ask_redirect_sends_asking_without_persisting_mapping() {
    let server_a = start_mock_redis();
    let server_b = start_mock_redis();
    let full = FullRedis::from_config(&format!("server={};db=0", server_a.addr)).unwrap();

    let topology = RedisClusterTopology::from_cluster_nodes(
        &cluster_nodes(&[
            ("master-a", &server_a.addr, "0-5460"),
            ("master-b", &server_b.addr, "5461-16383"),
        ]),
        false,
    );
    full.set_topology(Arc::new(topology));

    let key = key_in_slot(0..=5460);
    let slot = hash_slot(&key);
    set_raw(&server_a, &key, "source", 0);
    set_raw(&server_b, &key, "ask-target", 0);
    redirect_once(&server_a, &key, &format!("ASK {slot} {}", server_b.addr));
    require_asking(&server_b, &key);

    assert_eq!(
        full.get::<String>(&key).unwrap().as_deref(),
        Some("ask-target")
    );
    assert_eq!(full.get::<String>(&key).unwrap().as_deref(), Some("source"));
}

#[test]
fn cluster_chained_redirects_follow_until_final_target() {
    let server_a = start_mock_redis();
    let server_b = start_mock_redis();
    let server_c = start_mock_redis();
    let full = FullRedis::from_config(&format!("server={};db=0", server_a.addr)).unwrap();

    let topology = RedisClusterTopology::from_cluster_nodes(
        &cluster_nodes(&[
            ("master-a", &server_a.addr, "0-5460"),
            ("master-b", &server_b.addr, "5461-10922"),
            ("master-c", &server_c.addr, "10923-16383"),
        ]),
        false,
    );
    full.set_topology(Arc::new(topology));

    let key = key_in_slot(0..=5460);
    let slot = hash_slot(&key);
    set_raw(&server_c, &key, "final", 0);
    redirect_once(&server_a, &key, &format!("MOVED {slot} {}", server_b.addr));
    redirect_once(&server_b, &key, &format!("MOVED {slot} {}", server_c.addr));

    assert_eq!(full.get::<String>(&key).unwrap().as_deref(), Some("final"));
    assert_eq!(full.get::<String>(&key).unwrap().as_deref(), Some("final"));
}

#[test]
fn cluster_multi_key_commands_group_by_node_and_preserve_order() {
    let server_a = start_mock_redis();
    let server_b = start_mock_redis();
    let full = FullRedis::from_config(&format!("server={};db=0", server_a.addr)).unwrap();

    let topology = RedisClusterTopology::from_cluster_nodes(
        &cluster_nodes(&[
            ("master-a", &server_a.addr, "0-5460"),
            ("master-b", &server_b.addr, "5461-16383"),
        ]),
        false,
    );
    full.set_topology(Arc::new(topology));

    let key_a = key_in_slot(0..=5460);
    let key_b = key_in_slot(5461..=16383);

    full.set_all(&[(key_a.as_str(), "v1"), (key_b.as_str(), "v2")], 0)
        .unwrap();
    assert_eq!(raw_get(&server_a, &key_a).as_deref(), Some(&b"v1"[..]));
    assert_eq!(raw_get(&server_b, &key_b).as_deref(), Some(&b"v2"[..]));

    let values = full
        .redis()
        .get_all_raw(&[key_b.as_str(), key_a.as_str()])
        .unwrap();
    assert_eq!(values[0].as_deref(), Some(&b"v2"[..]));
    assert_eq!(values[1].as_deref(), Some(&b"v1"[..]));

    let dic = full
        .get_all::<String>(&[key_a.as_str(), key_b.as_str()])
        .unwrap();
    assert_eq!(dic.get(&key_a).map(|v| v.as_str()), Some("v1"));
    assert_eq!(dic.get(&key_b).map(|v| v.as_str()), Some("v2"));

    assert_eq!(
        full.redis()
            .touch(&[key_a.as_str(), key_b.as_str()])
            .unwrap(),
        2
    );
    assert_eq!(
        full.redis()
            .unlink(&[key_a.as_str(), key_b.as_str()])
            .unwrap(),
        2
    );
    assert!(!exists(&server_a, &key_a));
    assert!(!exists(&server_b, &key_b));

    full.set(&key_a, "v1", 0).unwrap();
    full.set(&key_b, "v2", 0).unwrap();
    assert_eq!(
        full.remove_many(&[key_a.as_str(), key_b.as_str()]).unwrap(),
        2
    );
    assert!(!exists(&server_a, &key_a));
    assert!(!exists(&server_b, &key_b));
}

#[test]
fn cluster_search_remove_pattern_and_global_key_queries_span_nodes() {
    let server_a = start_mock_redis();
    let server_b = start_mock_redis();
    let full = FullRedis::from_config(&format!("server={};db=0", server_a.addr)).unwrap();

    let topology = RedisClusterTopology::from_cluster_nodes(
        &cluster_nodes(&[
            ("master-a", &server_a.addr, "0-5460"),
            ("master-b", &server_b.addr, "5461-16383"),
        ]),
        false,
    );
    full.set_topology(Arc::new(topology));

    let key_a = format!("{}:search", key_in_slot(0..=5460));
    let key_b = format!("{}:search", key_in_slot(5461..=16383));
    let other = format!("{}:other", key_in_slot(5461..=16383));

    full.set(&key_a, "v1", 0).unwrap();
    full.set(&key_b, "v2", 0).unwrap();
    full.set(&other, "v3", 0).unwrap();

    assert_eq!(full.redis().dbsize().unwrap(), 3);

    let mut expected = vec![key_a.clone(), key_b.clone()];
    expected.sort();

    let mut keys = full.redis().keys_raw("*:search").unwrap();
    keys.sort();
    assert_eq!(keys, expected);

    let mut search = full.search("*:search", 0).unwrap();
    search.sort();
    assert_eq!(search, expected);

    assert_eq!(full.remove_pattern("*:search").unwrap(), 2);
    assert!(!exists(&server_a, &key_a));
    assert!(!exists(&server_b, &key_b));
    assert!(exists(&server_b, &other));
    assert_eq!(full.redis().dbsize().unwrap(), 1);
}

#[test]
fn cluster_mode_option_auto_loads_topology() {
    let seed = start_mock_redis();
    let target = start_mock_redis();
    set_cluster_nodes(
        &seed,
        &cluster_nodes(&[
            ("master-a", &seed.addr, "0-5460"),
            ("master-b", &target.addr, "5461-16383"),
        ]),
    );

    let full = FullRedis::from_config(&format!("server={};db=0;mode=cluster", seed.addr)).unwrap();
    let key = key_in_slot(5461..=16383);
    assert!(full.set(&key, "autoload", 0).unwrap());

    assert_eq!(raw_get(&seed, &key), None);
    assert_eq!(raw_get(&target, &key).as_deref(), Some(&b"autoload"[..]));
}

#[test]
fn auto_detect_cluster_loads_topology_from_info_and_cluster_nodes() {
    let seed = start_mock_redis();
    let target = start_mock_redis();
    set_info_mode(&seed, "cluster");
    set_cluster_nodes(
        &seed,
        &cluster_nodes(&[
            ("master-a", &seed.addr, "0-5460"),
            ("master-b", &target.addr, "5461-16383"),
        ]),
    );

    let full =
        FullRedis::from_config(&format!("server={};db=0;autodetect=true", seed.addr)).unwrap();
    let key = key_in_slot(5461..=16383);
    assert!(full.set(&key, "autodetect", 0).unwrap());

    assert_eq!(raw_get(&seed, &key), None);
    assert_eq!(raw_get(&target, &key).as_deref(), Some(&b"autodetect"[..]));
}

#[test]
fn topology_refresh_seconds_reload_cluster_nodes_mapping() {
    let seed = start_mock_redis();
    let target_a = start_mock_redis();
    let target_b = start_mock_redis();

    set_cluster_nodes(
        &seed,
        &cluster_nodes(&[
            ("master-a", &seed.addr, "0-5460"),
            ("master-b", &target_a.addr, "5461-16383"),
        ]),
    );

    let full = FullRedis::from_config(&format!(
        "server={};db=0;mode=cluster;topologyrefreshseconds=0",
        seed.addr
    ))
    .unwrap();

    let tag = key_in_slot(5461..=16383);
    let key1 = format!("{{{tag}}}:before");
    assert!(full.set(&key1, "before-refresh", 0).unwrap());
    assert_eq!(
        raw_get(&target_a, &key1).as_deref(),
        Some(&b"before-refresh"[..])
    );

    set_cluster_nodes(
        &seed,
        &cluster_nodes(&[
            ("master-a", &seed.addr, "0-5460"),
            ("master-b", &target_b.addr, "5461-16383"),
        ]),
    );

    let key2 = format!("{{{tag}}}:after");
    assert!(full.set(&key2, "after-refresh", 0).unwrap());
    assert_eq!(raw_get(&target_a, &key2), None);
    assert_eq!(
        raw_get(&target_b, &key2).as_deref(),
        Some(&b"after-refresh"[..])
    );
}

#[test]
fn replication_mode_auto_loads_topology_and_prefers_master_for_writes() {
    let seed = start_mock_redis();
    let replica = start_mock_redis();

    set_info_replication(
        &seed,
        &format!(
            "# Replication\r\nrole:master\r\nconnected_slaves:1\r\nslave0:ip=127.0.0.1,port={},state=online,offset=1,lag=0\r\n",
            replica.addr.rsplit(':').next().unwrap()
        ),
    );
    set_info_replication(
        &replica,
        &format!(
            "# Replication\r\nrole:slave\r\nmaster_host:127.0.0.1\r\nmaster_port:{}\r\nconnected_slaves:0\r\n",
            seed.addr.rsplit(':').next().unwrap()
        ),
    );

    let full = FullRedis::from_config(&format!(
        "server={},{};db=0;mode=replication;readfromreplicas=true",
        seed.addr, replica.addr
    ))
    .unwrap();

    let key = "replication:auto";
    assert!(full.set(key, "master-write", 0).unwrap());
    assert_eq!(raw_get(&seed, key).as_deref(), Some(&b"master-write"[..]));
    assert_eq!(raw_get(&replica, key), None);

    set_raw(&replica, key, "replica-read", 0);
    assert_eq!(
        full.get::<String>(key).unwrap().as_deref(),
        Some("replica-read")
    );
}

#[test]
fn sentinel_mode_discovers_master_and_delegates_to_replication_topology() {
    let sentinel = start_mock_redis();
    let master = start_mock_redis();
    let replica = start_mock_redis();

    set_info_sentinel(
        &sentinel,
        &format!(
            "# Sentinel\r\nsentinel_masters:1\r\nmaster0:name=redis-master,status=ok,address={},slaves=1,sentinels=1\r\n",
            master.addr
        ),
    );
    set_info_replication(
        &master,
        &format!(
            "# Replication\r\nrole:master\r\nconnected_slaves:1\r\nslave0:ip=127.0.0.1,port={},state=online,offset=1,lag=0\r\n",
            replica.addr.rsplit(':').next().unwrap()
        ),
    );
    set_info_replication(
        &replica,
        &format!(
            "# Replication\r\nrole:slave\r\nmaster_host:127.0.0.1\r\nmaster_port:{}\r\nconnected_slaves:0\r\n",
            master.addr.rsplit(':').next().unwrap()
        ),
    );

    let full = FullRedis::from_config(&format!(
        "server={};db=0;mode=sentinel;sentinelmastername=redis-master;readfromreplicas=true",
        sentinel.addr
    ))
    .unwrap();

    let key = "sentinel:auto";
    assert!(full.set(key, "via-sentinel", 0).unwrap());
    assert_eq!(raw_get(&master, key).as_deref(), Some(&b"via-sentinel"[..]));

    set_raw(&replica, key, "replica-value", 0);
    assert_eq!(
        full.get::<String>(key).unwrap().as_deref(),
        Some("replica-value")
    );
}

#[test]
fn auto_detect_replication_loads_topology_from_info() {
    let seed = start_mock_redis();
    let replica = start_mock_redis();
    let replica_port = replica.addr.rsplit(':').next().unwrap();
    let seed_port = seed.addr.rsplit(':').next().unwrap();

    let default_info = format!(
        "# Server\r\nredis_version:7.4.0\r\nredis_mode:standalone\r\nos:Windows\r\n# Replication\r\nrole:master\r\nconnected_slaves:1\r\nslave0:ip=127.0.0.1,port={replica_port},state=online,offset=1,lag=0\r\n"
    );
    set_info_text(&seed, &default_info);
    set_info_replication(&seed, &default_info);
    set_info_replication(
        &replica,
        &format!(
            "# Replication\r\nrole:slave\r\nmaster_host:127.0.0.1\r\nmaster_port:{seed_port}\r\nconnected_slaves:0\r\n"
        ),
    );

    let full = FullRedis::from_config(&format!(
        "server={},{};db=0;autodetect=true;readfromreplicas=true",
        seed.addr, replica.addr
    ))
    .unwrap();

    let key = "autodetect:replication";
    assert!(full.set(key, "master", 0).unwrap());
    assert_eq!(raw_get(&seed, key).as_deref(), Some(&b"master"[..]));

    set_raw(&replica, key, "replica", 0);
    assert_eq!(full.get::<String>(key).unwrap().as_deref(), Some("replica"));
}

#[test]
fn auto_detect_sentinel_loads_topology_from_info() {
    let sentinel = start_mock_redis();
    let master = start_mock_redis();
    let replica = start_mock_redis();
    let replica_port = replica.addr.rsplit(':').next().unwrap();
    let master_port = master.addr.rsplit(':').next().unwrap();

    let sentinel_info = format!(
        "# Sentinel\r\nredis_mode:standalone\r\nsentinel_masters:1\r\nmaster0:name=redis-master,status=ok,address={},slaves=1,sentinels=1\r\n",
        master.addr
    );
    set_info_text(&sentinel, &sentinel_info);
    set_info_sentinel(&sentinel, &sentinel_info);

    set_info_replication(
        &master,
        &format!(
            "# Replication\r\nrole:master\r\nconnected_slaves:1\r\nslave0:ip=127.0.0.1,port={replica_port},state=online,offset=1,lag=0\r\n"
        ),
    );
    set_info_replication(
        &replica,
        &format!(
            "# Replication\r\nrole:slave\r\nmaster_host:127.0.0.1\r\nmaster_port:{master_port}\r\nconnected_slaves:0\r\n"
        ),
    );

    let full = FullRedis::from_config(&format!(
        "server={};db=0;autodetect=true;readfromreplicas=true",
        sentinel.addr
    ))
    .unwrap();

    let key = "autodetect:sentinel";
    assert!(full.set(key, "sentinel-master", 0).unwrap());
    assert_eq!(
        raw_get(&master, key).as_deref(),
        Some(&b"sentinel-master"[..])
    );

    set_raw(&replica, key, "sentinel-replica", 0);
    assert_eq!(
        full.get::<String>(key).unwrap().as_deref(),
        Some("sentinel-replica")
    );
}

#[test]
fn sentinel_mode_can_delegate_to_cluster_topology() {
    let sentinel = start_mock_redis();
    let cluster_seed = start_mock_redis();
    let cluster_target = start_mock_redis();

    let sentinel_info = format!(
        "# Sentinel\r\nredis_mode:standalone\r\nsentinel_masters:1\r\nmaster0:name=redis-cluster,status=ok,address={},slaves=0,sentinels=1\r\n",
        cluster_seed.addr
    );
    set_info_sentinel(&sentinel, &sentinel_info);
    set_info_text(&sentinel, &sentinel_info);

    set_info_mode(&cluster_seed, "cluster");
    set_cluster_nodes(
        &cluster_seed,
        &cluster_nodes(&[
            ("master-a", &cluster_seed.addr, "0-5460"),
            ("master-b", &cluster_target.addr, "5461-16383"),
        ]),
    );

    let full = FullRedis::from_config(&format!(
        "server={};db=0;mode=sentinel;sentinelmastername=redis-cluster",
        sentinel.addr
    ))
    .unwrap();

    let key = key_in_slot(5461..=16383);
    assert!(full.set(&key, "sentinel-cluster", 0).unwrap());

    assert_eq!(raw_get(&cluster_seed, &key), None);
    assert_eq!(
        raw_get(&cluster_target, &key).as_deref(),
        Some(&b"sentinel-cluster"[..])
    );
}
