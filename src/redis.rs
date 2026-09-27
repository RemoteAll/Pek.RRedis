//! Redis 基础客户端（同步）。
//!
//! 对应 DH.NRedis 的 `Redis` 类：实例内部持有连接池，可在多线程间共享（[`Redis`] 实现 `Clone`，
//! 克隆只是复制 `Arc`）。命令层与 C# 保持同名语义：
//!
//! | DH.NRedis | pek-rredis |
//! |-----------|------------|
//! | `rds.Set(key, value, expire)` | [`Redis::set`] |
//! | `rds.Get<T>(key)` | [`Redis::get`] |
//! | `rds.Add(key, value, expire)` | [`Redis::add`]（`SET ... NX`） |
//! | `rds.Replace<T>(key, value)` | [`Redis::replace`]（`GETSET`） |
//! | `rds.SetAll(dic, expire)` / `GetAll<T>(keys)` | [`Redis::set_all`] / [`Redis::get_all`] |
//! | `rds.StartPipeline()` / `StopPipeline()` | [`Redis::pipeline`]（[`Pipeline`]） |
//!
//! 网络异常自动重试（默认 3 次），服务端 `-ERR` 与 C# 一样立即抛出，不重试。

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::cluster::{ClusterNode, RedisClusterTopology, RedisReplicationTopology, Topology};
use crate::client::{ConnConfig, RedisClient};
use crate::encoder::{FromRedisPayload, ToRedisPayload};
use crate::error::{Error, Result};
use crate::options::{RedisOptions, ServerMode};
use crate::pool::Pool;
use crate::resp::RespValue;

#[derive(Clone, Copy)]
struct RouteHint<'a> {
    key: &'a str,
    write: bool,
}

type KeyRouteEntry<'a> = (usize, &'a str);
type KeyRouteGroup<'a> = (String, Vec<KeyRouteEntry<'a>>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscoveredSlave {
    endpoint: String,
    link_up: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DiscoveredMaster {
    name: Option<String>,
    endpoint: String,
    status: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ReplicationDiscovery {
    role: Option<String>,
    master_endpoint: Option<String>,
    slaves: Vec<DiscoveredSlave>,
    masters: Vec<DiscoveredMaster>,
}

/// 内部共享状态。
pub struct RedisInner {
    /// 连接选项
    pub options: RedisOptions,
    /// 连接池
    pub pool: Arc<Pool>,
    /// 成功命令数
    pub commands: AtomicU64,
    /// 失败命令数
    pub errors: AtomicU64,
    /// 可选拓扑选择器（cluster/sentinel/replication）。
    pub topology: RwLock<Option<Arc<dyn Topology>>>,
    /// endpoint -> 定向连接客户端缓存。
    pub endpoint_clients: Mutex<HashMap<String, Redis>>,
    /// 拓扑初始化锁，避免并发重复加载。
    pub topology_init: Mutex<()>,
    /// 最近一次拓扑刷新时间。
    pub topology_refreshed_at: Mutex<Option<Instant>>,
}

/// Redis 客户端。克隆共享同一个连接池与配置。
#[derive(Clone)]
pub struct Redis {
    inner: Arc<RedisInner>,
}

impl std::fmt::Debug for Redis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Redis")
            .field("servers", &self.inner.options.servers)
            .field("db", &self.inner.options.db)
            .field("prefix", &self.inner.options.prefix)
            .finish()
    }
}

impl Redis {
    /// 使用选项创建客户端。
    pub fn new(options: RedisOptions) -> Result<Self> {
        if options.servers.is_empty() {
            return Err(Error::Config("缺少 Server 配置".into()));
        }

        let endpoints = options.endpoints();
        let rotation = Arc::new(AtomicUsize::new(0));

        let factory = {
            let endpoints = endpoints.clone();
            let rotation = rotation.clone();
            let proto = options.protocol_version;
            let timeout = options.timeout_ms;
            let max_size = options.max_message_size;
            let user_name = options.user_name.clone();
            let password = options.password.clone();
            let tls = options.tls;
            let tls_server_name = options.tls_server_name.clone();
            let tls_insecure = options.tls_insecure;
            let db = options.db;

            move || -> Result<RedisClient> {
                let n = endpoints.len();
                let start = rotation.fetch_add(1, Ordering::Relaxed) % n;
                let mut last: Option<Error> = None;

                for i in 0..n {
                    let endpoint = endpoints[(start + i) % n].clone();
                    let cfg = ConnConfig {
                        endpoint,
                        user_name: user_name.clone(),
                        password: password.clone(),
                        db,
                        timeout_ms: timeout,
                        protocol_version: proto,
                        max_message_size: max_size,
                        tls,
                        tls_server_name: tls_server_name.clone(),
                        tls_insecure,
                    };
                    match RedisClient::connect(&cfg) {
                        Ok(c) => return Ok(c),
                        Err(e) => last = Some(e),
                    }
                }

                Err(last.unwrap_or_else(|| Error::Config("没有可用的服务器地址".into())))
            }
        };

        let pool = Pool::new(options.pool.clone(), factory);

        Ok(Self {
            inner: Arc::new(RedisInner {
                options,
                pool,
                commands: AtomicU64::new(0),
                errors: AtomicU64::new(0),
                topology: RwLock::new(None),
                endpoint_clients: Mutex::new(HashMap::new()),
                topology_init: Mutex::new(()),
                topology_refreshed_at: Mutex::new(None),
            }),
        })
    }

    /// 使用地址、密码、库号创建（等价 C# `new FullRedis(server, password, db)`）。
    pub fn open(server: &str, password: Option<&str>, db: i32) -> Result<Self> {
        Self::new(RedisOptions::new(server, password, db))
    }

    /// 使用连接字符串创建（等价 C# `FullRedis.Create(config)`）。
    pub fn from_config(config: &str) -> Result<Self> {
        Self::new(RedisOptions::from_config(config)?)
    }

    /// 连接选项。
    pub fn options(&self) -> &RedisOptions {
        &self.inner.options
    }

    /// 连接池。
    pub fn pool(&self) -> &Arc<Pool> {
        &self.inner.pool
    }

    /// 成功/失败命令计数。
    pub fn stats(&self) -> (u64, u64) {
        (
            self.inner.commands.load(Ordering::Relaxed),
            self.inner.errors.load(Ordering::Relaxed),
        )
    }

    /// 设置拓扑选择器。配置后，支持按 key 路由到指定节点执行命令。
    pub fn set_topology(&self, topology: Arc<dyn Topology>) {
        *self.inner.topology.write().unwrap() = Some(topology);
        *self.inner.topology_refreshed_at.lock().unwrap() = Some(Instant::now());
    }

    /// 清空拓扑选择器，恢复普通多地址轮询行为。
    pub fn clear_topology(&self) {
        *self.inner.topology.write().unwrap() = None;
        self.inner.endpoint_clients.lock().unwrap().clear();
        *self.inner.topology_refreshed_at.lock().unwrap() = None;
    }

    /// 服务器地址（逗号分隔）。
    pub fn server(&self) -> String {
        self.inner.options.servers.join(",")
    }

    /// 构造一条独立连接的配置（订阅、专用连接等场景）。
    pub fn conn_config(&self) -> ConnConfig {
        let o = &self.inner.options;
        let endpoint = o
            .endpoints()
            .into_iter()
            .next()
            .unwrap_or_else(|| "127.0.0.1:6379".into());

        ConnConfig {
            endpoint,
            user_name: o.user_name.clone(),
            password: o.password.clone(),
            db: o.db,
            timeout_ms: o.timeout_ms,
            protocol_version: o.protocol_version,
            max_message_size: o.max_message_size,
            tls: o.tls,
            tls_server_name: o.tls_server_name.clone(),
            tls_insecure: o.tls_insecure,
        }
    }

    // ================== 执行内核 ==================

    /// 执行命令（带重试）。
    pub fn execute(&self, args: &[&[u8]]) -> Result<RespValue> {
        self.execute_inner(args, None, None)
    }

    /// 执行阻塞命令（BRPOP / BRPOPLPUSH / BLMOVE 等）。
    pub fn execute_blocking(&self, args: &[&[u8]], block_seconds: i64) -> Result<RespValue> {
        self.execute_inner(args, Some(block_seconds), None)
    }

    /// 执行命令并忽略应答（用于无需结果的写操作）。
    pub fn execute_ignore(&self, args: &[&[u8]]) -> Result<()> {
        self.execute(args).map(|_| ())
    }

    fn execute_on_key(&self, key: &str, write: bool, args: &[&[u8]]) -> Result<RespValue> {
        self.execute_inner(args, None, Some(RouteHint { key, write }))
    }

    fn execute_inner(
        &self,
        args: &[&[u8]],
        block: Option<i64>,
        route: Option<RouteHint<'_>>,
    ) -> Result<RespValue> {
        let result = self.execute_inner_routed(args, block, route);
        match result {
            Ok(value) => {
                self.inner.commands.fetch_add(1, Ordering::Relaxed);
                Ok(value)
            }
            Err(err) => {
                self.inner.errors.fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    fn execute_inner_routed(
        &self,
        args: &[&[u8]],
        block: Option<i64>,
        route: Option<RouteHint<'_>>,
    ) -> Result<RespValue> {
        if route.is_some() {
            self.ensure_topology()?;
        }
        if let Some(route) = route
            && let Some(topology) = self.topology() 
            && let Some(node) = topology.select_node(route.key, route.write)
        {
            let mut current = node;
            let mut send_asking = false;

            for redirect_count in 0..=5 {
                match self.execute_on_endpoint(&current.endpoint, args, block, send_asking) {
                    Ok(value) => {
                        topology.reset_node(&current.endpoint);
                        return Ok(value);
                    }
                    Err(Error::Server(message)) => {
                        if let Some(redirect) = parse_redirect(&message) {
                            if redirect_count == 5 {
                                return Err(Error::Operation(format!(
                                    "cluster 重定向次数过多：key=[{}] error=[{}]",
                                    route.key, message
                                )));
                            }
                            if redirect.kind == RedirectKind::Moved {
                                topology.remember_redirect(redirect.slot, redirect.endpoint, true);
                            }
                            current = topology
                                .map_endpoint(redirect.endpoint, Some(route.key))
                                .unwrap_or_else(|| ClusterNode::new(redirect.endpoint.to_string()));
                            send_asking = redirect.kind == RedirectKind::Ask;
                            continue;
                        }
                        return Err(Error::Server(message));
                    }
                    Err(err) => {
                        if let Some(next) = topology.reselect_node(route.key, route.write, &current)
                        {
                            current = next;
                            send_asking = false;
                            continue;
                        }
                        return Err(err);
                    }
                }
            }
        }

        self.execute_with_pool(&self.inner.pool, args, block, false)
    }

    fn execute_on_endpoint(
        &self,
        endpoint: &str,
        args: &[&[u8]],
        block: Option<i64>,
        send_asking: bool,
    ) -> Result<RespValue> {
        let redis = self.redis_for_endpoint(endpoint)?;
        self.execute_with_pool(&redis.inner.pool, args, block, send_asking)
    }

    fn execute_with_pool(
        &self,
        pool: &Arc<Pool>,
        args: &[&[u8]],
        block: Option<i64>,
        send_asking: bool,
    ) -> Result<RespValue> {
        let attempts = self.inner.options.retry.max(1);
        let mut last_err: Option<Error> = None;

        for attempt in 0..attempts {
            let mut client = pool.get()?;
            if send_asking {
                client.command(&[b"ASKING"])?;
            }
            let result = match block {
                Some(seconds) => client.command_blocking(args, seconds),
                None => client.command(args),
            };

            match result {
                Ok(v) => return Ok(v),
                Err(Error::Server(msg)) => return Err(Error::Server(msg)),
                Err(e) => {
                    last_err = Some(e);
                    // 连接在此处随 PooledClient 析构被标记损坏并销毁
                }
            }

            if attempt + 1 < attempts {
                std::thread::sleep(Duration::from_millis(50u64 << attempt.min(5)));
            }
        }

        Err(last_err.unwrap_or_else(|| Error::Pool("命令执行失败且无错误信息".into())))
    }

    fn topology(&self) -> Option<Arc<dyn Topology>> {
        self.inner.topology.read().unwrap().clone()
    }

    fn redis_for_endpoint(&self, endpoint: &str) -> Result<Redis> {
        if let Some(redis) = self.inner.endpoint_clients.lock().unwrap().get(endpoint).cloned() {
            return Ok(redis);
        }

        let mut options = self.inner.options.clone();
        options.servers = vec![endpoint.to_string()];
        options.mode = ServerMode::Standalone;
        options.auto_detect = false;
        options.pool.min = 0;

        let redis = Redis::new(options)?;
        let mut cache = self.inner.endpoint_clients.lock().unwrap();
        Ok(cache
            .entry(endpoint.to_string())
            .or_insert_with(|| redis.clone())
            .clone())
    }

    fn route_key_groups<'a>(
        &self,
        keys: &[&'a str],
        write: bool,
    ) -> Result<Option<Vec<KeyRouteGroup<'a>>>> {
        self.ensure_topology()?;
        let Some(topology) = self.topology() else {
            return Ok(None);
        };

        let mut groups: Vec<KeyRouteGroup<'a>> = Vec::new();
        let mut indexes: HashMap<String, usize> = HashMap::new();

        for (index, key) in keys.iter().copied().enumerate() {
            let node = topology.select_node(key, write).ok_or_else(|| {
                Error::Operation(format!("集群模式下未找到 key [{key}] 的可用节点"))
            })?;
            if let Some(group_index) = indexes.get(&node.endpoint).copied() {
                groups[group_index].1.push((index, key));
            } else {
                let group_index = groups.len();
                indexes.insert(node.endpoint.clone(), group_index);
                groups.push((node.endpoint, vec![(index, key)]));
            }
        }

        Ok(Some(groups))
    }

    fn cluster_query_endpoints(&self) -> Result<Option<Vec<String>>> {
        self.ensure_topology()?;
        let Some(topology) = self.topology() else {
            return Ok(None);
        };
        let nodes = topology.nodes();

        let mut endpoints = Vec::new();
        let mut push_unique = |endpoint: &str| {
            if !endpoints.iter().any(|item| item == endpoint) {
                endpoints.push(endpoint.to_string());
            }
        };

        let primaries: Vec<_> = nodes.iter().filter(|node| !node.is_replica).collect();
        if !primaries.is_empty() {
            for node in primaries {
                push_unique(&node.endpoint);
            }
        } else {
            for node in &nodes {
                push_unique(&node.endpoint);
            }
        }

        Ok(Some(endpoints))
    }

    fn ensure_topology(&self) -> Result<()> {
        if self.topology().is_some() && !self.topology_refresh_due() {
            return Ok(());
        }

        let wanted_mode = match self.inner.options.mode {
            ServerMode::Cluster => Some(ServerMode::Cluster),
            ServerMode::Replication => Some(ServerMode::Replication),
            ServerMode::Sentinel => Some(ServerMode::Sentinel),
            ServerMode::Auto if self.inner.options.auto_detect => self.detect_topology_mode()?,
            _ => None,
        };
        let Some(wanted_mode) = wanted_mode else {
            return Ok(());
        };

        let _guard = self.inner.topology_init.lock().unwrap();
        if self.topology().is_some() && !self.topology_refresh_due() {
            return Ok(());
        }

        let topology = self.load_topology(wanted_mode)?;
        self.set_topology(topology);
        Ok(())
    }

    fn topology_refresh_due(&self) -> bool {
        if self.topology().is_none() {
            return true;
        }

        let seconds = self.inner.options.topology_refresh_seconds;
        if seconds == 0 {
            return true;
        }

        self.inner
            .topology_refreshed_at
            .lock()
            .unwrap()
            .map(|ts| ts.elapsed() >= Duration::from_secs(seconds))
            .unwrap_or(true)
    }

    fn detect_topology_mode(&self) -> Result<Option<ServerMode>> {
        let info = self.fetch_info(None, None)?;
        Ok(detect_topology_mode_from_info(
            &info,
            self.inner.options.endpoints().len(),
        ))
    }

    fn load_topology(&self, mode: ServerMode) -> Result<Arc<dyn Topology>> {
        match mode {
            ServerMode::Cluster => self.load_cluster_topology(None),
            ServerMode::Replication => self.load_replication_topology(
                self.inner.options.endpoints(),
                ServerMode::Replication,
            ),
            ServerMode::Sentinel => self.load_sentinel_topology(),
            _ => Err(Error::Operation(format!("不支持的拓扑模式初始化：{mode:?}"))),
        }
    }

    fn load_cluster_topology(&self, endpoint: Option<&str>) -> Result<Arc<dyn Topology>> {
        let nodes = match endpoint {
            Some(endpoint) => self
                .execute_on_endpoint(endpoint, &[b"CLUSTER", b"NODES"], None, false)?
                .as_string(),
            None => self
                .execute_with_pool(&self.inner.pool, &[b"CLUSTER", b"NODES"], None, false)?
                .as_string(),
        }
        .ok_or_else(|| Error::Protocol("CLUSTER NODES 返回结构非法".into()))?;

        Ok(Arc::new(RedisClusterTopology::from_cluster_nodes(
            &nodes,
            self.inner.options.read_from_replicas,
        )))
    }

    fn load_replication_topology(
        &self,
        seeds: Vec<String>,
        mode: ServerMode,
    ) -> Result<Arc<dyn Topology>> {
        let mut pending: VecDeque<String> = seeds.into_iter().collect();
        let mut seen = HashSet::new();
        let mut discovered: HashMap<String, ClusterNode> = HashMap::new();
        let mut last_error: Option<Error> = None;

        while let Some(endpoint) = pending.pop_front() {
            if !seen.insert(endpoint.clone()) {
                continue;
            }

            match self.fetch_info(Some(&endpoint), Some("Replication")) {
                Ok(info) => {
                    let parsed = parse_replication_discovery(&info);
                    let role_is_slave = parsed
                        .role
                        .as_deref()
                        .map(|role| role.eq_ignore_ascii_case("slave"))
                        .unwrap_or(false);
                    let master_endpoint = parsed.master_endpoint.clone();
                    let slaves = parsed.slaves;

                    {
                        let node = discovered
                            .entry(endpoint.clone())
                            .or_insert_with(|| ClusterNode::new(endpoint.clone()));
                        node.link_up = true;
                        node.is_replica = role_is_slave;
                        if let Some(master_endpoint) = &master_endpoint {
                            node.master_id = Some(master_endpoint.clone());
                        }
                    }

                    if let Some(master_endpoint) = master_endpoint {
                        let node = discovered
                            .entry(master_endpoint.clone())
                            .or_insert_with(|| ClusterNode::new(master_endpoint.clone()));
                        node.is_replica = false;
                        pending.push_back(master_endpoint);
                    }

                    if !role_is_slave {
                        for slave in slaves {
                            let slave_endpoint = slave.endpoint.clone();
                            let node = discovered
                                .entry(slave_endpoint.clone())
                                .or_insert_with(|| ClusterNode::new(slave_endpoint.clone()));
                            node.is_replica = true;
                            node.link_up = slave.link_up;
                            node.master_id = Some(endpoint.clone());
                            pending.push_back(slave_endpoint);
                        }
                    }
                }
                Err(err) => {
                    last_error = Some(err);
                    if let Some(node) = discovered.get_mut(&endpoint) {
                        node.link_up = false;
                    } else if discovered.is_empty() {
                        return Err(last_error.expect("replication info error should exist"));
                    } else {
                        let mut node = ClusterNode::new(endpoint.clone());
                        node.link_up = false;
                        discovered.insert(endpoint, node);
                    }
                }
            }
        }

        if discovered.is_empty() {
            return Err(last_error.unwrap_or_else(|| Error::Operation("未发现主从节点信息".into())));
        }

        Ok(Arc::new(RedisReplicationTopology::new(
            mode,
            discovered.into_values().collect(),
            self.inner.options.read_from_replicas,
        )))
    }

    fn load_sentinel_topology(&self) -> Result<Arc<dyn Topology>> {
        let mut last_error: Option<Error> = None;
        let mut masters = Vec::new();

        for endpoint in self.inner.options.endpoints() {
            match self.fetch_info(Some(&endpoint), Some("Sentinel")) {
                Ok(info) => {
                    masters = parse_replication_discovery(&info).masters;
                    if !masters.is_empty() {
                        break;
                    }
                }
                Err(err) => last_error = Some(err),
            }
        }

        let seeds: Vec<String> = masters
            .into_iter()
            .filter(|master| {
                self.inner
                    .options
                    .sentinel_master_name
                    .as_deref()
                    .map(|name| {
                        master
                            .name
                            .as_deref()
                            .map(|value| value.eq_ignore_ascii_case(name))
                            .unwrap_or(false)
                    })
                    .unwrap_or(true)
            })
            .map(|master| master.endpoint)
            .collect();

        if seeds.is_empty() {
            return Err(last_error.unwrap_or_else(|| Error::Operation("哨兵未返回可用主节点".into())));
        }

        let info = self.fetch_info(Some(&seeds[0]), None)?;
        match detect_topology_mode_from_info(&info, seeds.len()) {
            Some(ServerMode::Cluster) => self.load_cluster_topology(Some(&seeds[0])),
            _ => self.load_replication_topology(seeds, ServerMode::Sentinel),
        }
    }

    fn fetch_info(&self, endpoint: Option<&str>, section: Option<&str>) -> Result<HashMap<String, String>> {
        Ok(parse_info(&self.fetch_info_text(endpoint, section)?))
    }

    fn fetch_info_text(&self, endpoint: Option<&str>, section: Option<&str>) -> Result<String> {
        let value = match (endpoint, section) {
            (Some(endpoint), Some(section)) => {
                self.execute_on_endpoint(endpoint, &[b"INFO", section.as_bytes()], None, false)?
            }
            (Some(endpoint), None) => self.execute_on_endpoint(endpoint, &[b"INFO"], None, false)?,
            (None, Some(section)) => {
                self.execute_with_pool(&self.inner.pool, &[b"INFO", section.as_bytes()], None, false)?
            }
            (None, None) => self.execute_with_pool(&self.inner.pool, &[b"INFO"], None, false)?,
        };

        value
            .as_string()
            .ok_or_else(|| Error::Protocol("INFO 返回结构非法".into()))
    }

    /// 在独占连接上执行自定义逻辑（自动从池中借还）。
    pub fn with_client<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut RedisClient) -> Result<T>,
    {
        let mut client = self.inner.pool.get()?;
        f(&mut client)
    }

    // ================== 键操作 ==================

    /// 数据库键数量（`DBSIZE`）。
    pub fn dbsize(&self) -> Result<i64> {
        if let Some(endpoints) = self.cluster_query_endpoints()? {
            let mut total = 0i64;
            for endpoint in endpoints {
                total += self
                    .execute_on_endpoint(&endpoint, &[b"DBSIZE"], None, false)?
                    .as_i64()
                    .unwrap_or(0);
            }
            return Ok(total);
        }
        Ok(self.execute(&[b"DBSIZE"])?.as_i64().unwrap_or(0))
    }

    /// 获取所有键（`KEYS *`）。数量超过 10000 时拒绝，防止阻塞 Redis（与 C# 一致）。
    pub fn keys(&self) -> Result<Vec<String>> {
        if self.dbsize()? > 10_000 {
            return Err(Error::Operation(
                "数量过大时禁止获取所有键，请使用 FullRedis::search 分页扫描".into(),
            ));
        }
        self.keys_raw("*")
    }

    /// 按模式获取键（`KEYS pattern`，生产环境慎用）。
    pub fn keys_raw(&self, pattern: &str) -> Result<Vec<String>> {
        if let Some(endpoints) = self.cluster_query_endpoints()? {
            let mut result = Vec::new();
            for endpoint in endpoints {
                let items = self
                    .execute_on_endpoint(&endpoint, &[b"KEYS", pattern.as_bytes()], None, false)?
                    .into_array()
                    .unwrap_or_default();
                for item in items {
                    if let Some(key) = item.as_string()
                        && !result.iter().any(|existing| existing == &key)
                    {
                        result.push(key);
                    }
                }
            }
            return Ok(result);
        }
        let rs = self.execute(&[b"KEYS", pattern.as_bytes()])?;
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.as_string())
            .collect())
    }

    /// 是否存在（`EXISTS`）。
    pub fn contains_key(&self, key: &str) -> Result<bool> {
        Ok(self
            .execute_on_key(key, false, &[b"EXISTS", key.as_bytes()])?
            .as_i64()
            .unwrap_or(0)
            > 0)
    }

    /// 删除单个键（`DEL`）。
    pub fn remove(&self, key: &str) -> Result<i64> {
        if key.is_empty() {
            return Ok(0);
        }
        Ok(self
            .execute_on_key(key, true, &[b"DEL", key.as_bytes()])?
            .as_i64()
            .unwrap_or(0))
    }

    /// 批量删除（`DEL key...`）。
    pub fn remove_many(&self, keys: &[&str]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        if let Some(groups) = self.route_key_groups(keys, true)? {
            let mut total = 0i64;
            for (endpoint, entries) in groups {
                let mut args: Vec<&[u8]> = Vec::with_capacity(entries.len() + 1);
                args.push(b"DEL");
                for (_, key) in &entries {
                    args.push(key.as_bytes());
                }
                total += self
                    .execute_on_endpoint(&endpoint, &args, None, false)?
                    .as_i64()
                    .unwrap_or(0);
            }
            return Ok(total);
        }
        let mut args: Vec<&[u8]> = Vec::with_capacity(keys.len() + 1);
        args.push(b"DEL");
        for k in keys {
            args.push(k.as_bytes());
        }
        Ok(self.execute(&args)?.as_i64().unwrap_or(0))
    }

    /// 设置过期时间（秒）（`EXPIRE`）。
    pub fn set_expire(&self, key: &str, seconds: i64) -> Result<bool> {
        Ok(self
            .execute_on_key(key, true, &[b"EXPIRE", key.as_bytes(), seconds.to_string().as_bytes()])?
            .as_i64()
            .unwrap_or(0)
            == 1)
    }

    /// 设置过期时间（毫秒）（`PEXPIRE`）。
    pub fn set_expire_ms(&self, key: &str, milliseconds: i64) -> Result<bool> {
        Ok(self
            .execute_on_key(
                key,
                true,
                &[b"PEXPIRE", key.as_bytes(), milliseconds.to_string().as_bytes()],
            )?
            .as_i64()
            .unwrap_or(0)
            == 1)
    }

    /// 获取剩余有效期（秒）（`TTL`）。-1 永不过期，-2 键不存在。
    pub fn get_expire(&self, key: &str) -> Result<i64> {
        Ok(self
            .execute_on_key(key, false, &[b"TTL", key.as_bytes()])?
            .as_i64()
            .unwrap_or(-2))
    }

    /// 获取剩余有效期（毫秒）（`PTTL`）。
    pub fn get_expire_ms(&self, key: &str) -> Result<i64> {
        Ok(self
            .execute_on_key(key, false, &[b"PTTL", key.as_bytes()])?
            .as_i64()
            .unwrap_or(-2))
    }

    /// 移除过期时间（`PERSIST`）。
    pub fn persist(&self, key: &str) -> Result<bool> {
        Ok(self
            .execute_on_key(key, true, &[b"PERSIST", key.as_bytes()])?
            .as_i64()
            .unwrap_or(0)
            == 1)
    }

    /// 键类型（`TYPE`），不存在返回 `None`。
    pub fn type_of(&self, key: &str) -> Result<Option<String>> {
        let s = self
            .execute_on_key(key, false, &[b"TYPE", key.as_bytes()])?
            .as_string();
        match s.as_deref() {
            Some("none") | None => Ok(None),
            _ => Ok(s),
        }
    }

    /// 重命名（`RENAME` / `RENAMENX`）。
    pub fn rename(&self, key: &str, new_key: &str, overwrite: bool) -> Result<bool> {
        let cmd: &[u8] = if overwrite { b"RENAME" } else { b"RENAMENX" };
        let rs = self.execute_on_key(key, true, &[cmd, key.as_bytes(), new_key.as_bytes()])?;
        if overwrite {
            Ok(rs.as_string().as_deref() == Some("OK"))
        } else {
            Ok(rs.as_i64().unwrap_or(0) == 1)
        }
    }

    /// 异步删除（`UNLINK`）。
    pub fn unlink(&self, keys: &[&str]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        if let Some(groups) = self.route_key_groups(keys, true)? {
            let mut total = 0i64;
            for (endpoint, entries) in groups {
                let mut args: Vec<&[u8]> = Vec::with_capacity(entries.len() + 1);
                args.push(b"UNLINK");
                for (_, key) in &entries {
                    args.push(key.as_bytes());
                }
                total += self
                    .execute_on_endpoint(&endpoint, &args, None, false)?
                    .as_i64()
                    .unwrap_or(0);
            }
            return Ok(total);
        }
        let mut args: Vec<&[u8]> = Vec::with_capacity(keys.len() + 1);
        args.push(b"UNLINK");
        for k in keys {
            args.push(k.as_bytes());
        }
        Ok(self.execute(&args)?.as_i64().unwrap_or(0))
    }

    /// 刷新访问时间（`TOUCH`）。
    pub fn touch(&self, keys: &[&str]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        if let Some(groups) = self.route_key_groups(keys, true)? {
            let mut total = 0i64;
            for (endpoint, entries) in groups {
                let mut args: Vec<&[u8]> = Vec::with_capacity(entries.len() + 1);
                args.push(b"TOUCH");
                for (_, key) in &entries {
                    args.push(key.as_bytes());
                }
                total += self
                    .execute_on_endpoint(&endpoint, &args, None, false)?
                    .as_i64()
                    .unwrap_or(0);
            }
            return Ok(total);
        }
        let mut args: Vec<&[u8]> = Vec::with_capacity(keys.len() + 1);
        args.push(b"TOUCH");
        for k in keys {
            args.push(k.as_bytes());
        }
        Ok(self.execute(&args)?.as_i64().unwrap_or(0))
    }

    /// 拷贝键（`COPY`，Redis 6.2+）。
    pub fn copy(&self, source: &str, destination: &str, db: Option<i32>, replace: bool) -> Result<bool> {
        let mut args: Vec<Vec<u8>> = vec![
            b"COPY".to_vec(),
            source.as_bytes().to_vec(),
            destination.as_bytes().to_vec(),
        ];
        if let Some(db) = db {
            args.push(b"DB".to_vec());
            args.push(db.to_string().into_bytes());
        }
        if replace {
            args.push(b"REPLACE".to_vec());
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(self.execute_on_key(source, true, &refs)?.as_i64().unwrap_or(0) == 1)
    }

    /// 随机键（`RANDOMKEY`）。
    pub fn random_key(&self) -> Result<Option<String>> {
        let rs = self.execute(&[b"RANDOMKEY"])?;
        if rs.is_null() {
            Ok(None)
        } else {
            Ok(rs.as_string())
        }
    }

    /// 键内存占用（`MEMORY USAGE`），不存在返回 `None`。
    pub fn memory_usage(&self, key: &str, samples: i32) -> Result<Option<i64>> {
        let rs = if samples > 0 {
            self.execute_on_key(key, false, &[
                b"MEMORY",
                b"USAGE",
                key.as_bytes(),
                b"SAMPLES",
                samples.to_string().as_bytes(),
            ])?
        } else {
            self.execute_on_key(key, false, &[b"MEMORY", b"USAGE", key.as_bytes()])?
        };
        Ok(rs.as_i64())
    }

    /// 对象内部编码（`OBJECT ENCODING`）。
    pub fn object_encoding(&self, key: &str) -> Result<Option<String>> {
        let rs = self.execute_on_key(key, false, &[b"OBJECT", b"ENCODING", key.as_bytes()])?;
        if rs.is_null() {
            Ok(None)
        } else {
            Ok(rs.as_string())
        }
    }

    /// 单步 SCAN。返回 `(下一个游标, 键列表)`；游标为 0 表示遍历结束。
    pub fn scan(&self, cursor: u64, pattern: &str, count: usize) -> Result<(u64, Vec<String>)> {
        let count = if count == 0 { 100 } else { count };
        if let Some(endpoints) = self.cluster_query_endpoints()? {
            if cursor != 0 {
                return Ok((0, Vec::new()));
            }

            let mut keys = Vec::new();
            for endpoint in endpoints {
                let mut items = self
                    .execute_on_endpoint(
                        &endpoint,
                        &[
                            b"SCAN",
                            b"0",
                            b"MATCH",
                            pattern.as_bytes(),
                            b"COUNT",
                            count.to_string().as_bytes(),
                        ],
                        None,
                        false,
                    )?
                    .into_array()
                    .ok_or_else(|| Error::Protocol("SCAN 返回结构非法".into()))?;
                if items.len() != 2 {
                    return Err(Error::Protocol("SCAN 返回结构非法".into()));
                }
                let list = items.pop().unwrap().into_array().unwrap_or_default();
                for item in list {
                    if let Some(key) = item.as_string()
                        && !keys.iter().any(|existing| existing == &key)
                    {
                        keys.push(key);
                    }
                }
            }
            return Ok((0, keys));
        }

        let rs = self.execute(&[
            b"SCAN",
            cursor.to_string().as_bytes(),
            b"MATCH",
            pattern.as_bytes(),
            b"COUNT",
            count.to_string().as_bytes(),
        ])?;

        let mut items = rs
            .into_array()
            .ok_or_else(|| Error::Protocol("SCAN 返回结构非法".into()))?;
        if items.len() != 2 {
            return Err(Error::Protocol("SCAN 返回结构非法".into()));
        }

        let list = items.pop().unwrap().into_array().unwrap_or_default();
        let keys = list.into_iter().filter_map(|v| v.as_string()).collect();
        let next = items
            .pop()
            .and_then(|v| v.as_string())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        Ok((next, keys))
    }

    // ================== 字符串 ==================

    /// 设置值（`SET` / `SETEX`）。
    ///
    /// `expire_seconds < 0` 时使用默认过期时间 [`RedisOptions::expire`]。
    pub fn set<K: AsRef<str>, V: ToRedisPayload>(
        &self,
        key: K,
        value: V,
        expire_seconds: i64,
    ) -> Result<bool> {
        let mut expire = expire_seconds;
        if expire < 0 {
            expire = self.inner.options.expire;
        }

        let payload = value.to_redis_payload()?.unwrap_or_default();
        let key = key.as_ref();

        let rs = if expire <= 0 {
            self.execute_on_key(key, true, &[b"SET", key.as_bytes(), &payload])?
        } else {
            self.execute_on_key(key, true, &[
                b"SETEX",
                key.as_bytes(),
                expire.to_string().as_bytes(),
                &payload,
            ])?
        };

        Ok(rs.as_string().as_deref() == Some("OK"))
    }

    /// 获取原始字节值。键不存在返回 `None`。
    pub fn get_raw(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let rs = self.execute_on_key(key, false, &[b"GET", key.as_bytes()])?;
        if rs.is_null() {
            return Ok(None);
        }
        Ok(rs.as_bytes())
    }

    /// 获取并解码为指定类型。解码失败返回 `None`（与 C# 编码器容错行为一致）。
    pub fn get<T: FromRedisPayload>(&self, key: &str) -> Result<Option<T>> {
        match self.get_raw(key)? {
            None => Ok(None),
            Some(bytes) => Ok(T::from_redis_payload(&bytes).ok()),
        }
    }

    /// 获取字符串值。
    pub fn get_string(&self, key: &str) -> Result<Option<String>> {
        Ok(self.get_raw(key)?.map(|b| String::from_utf8_lossy(&b).into_owned()))
    }

    /// 仅在键不存在时设置（`SET ... NX`）。
    pub fn add<K: AsRef<str>, V: ToRedisPayload>(
        &self,
        key: K,
        value: V,
        expire_seconds: i64,
    ) -> Result<bool> {
        let mut expire = expire_seconds;
        if expire < 0 {
            expire = self.inner.options.expire;
        }

        let payload = value.to_redis_payload()?.unwrap_or_default();
        let key = key.as_ref();

        let rs = if expire > 0 {
            self.execute_on_key(key, true, &[
                b"SET",
                key.as_bytes(),
                &payload,
                b"EX",
                expire.to_string().as_bytes(),
                b"NX",
            ])?
        } else {
            self.execute_on_key(key, true, &[b"SET", key.as_bytes(), &payload, b"NX"])?
        };

        Ok(!rs.is_null())
    }

    /// 设置新值并返回旧值（`GETSET`）。
    pub fn replace<T: FromRedisPayload + ToRedisPayload>(
        &self,
        key: &str,
        value: T,
    ) -> Result<Option<T>> {
        let payload = value.to_redis_payload()?.unwrap_or_default();
        let rs = self.execute_on_key(key, true, &[b"GETSET", key.as_bytes(), &payload])?;
        if rs.is_null() {
            return Ok(None);
        }
        Ok(rs.as_bytes().and_then(|b| T::from_redis_payload(&b).ok()))
    }

    /// 设置新值并返回旧值（`SET ... GET`，Redis 6.2+）。
    pub fn set_get<T: FromRedisPayload + ToRedisPayload>(
        &self,
        key: &str,
        value: T,
        expire_seconds: i64,
    ) -> Result<Option<T>> {
        let payload = value.to_redis_payload()?.unwrap_or_default();
        let mut args: Vec<Vec<u8>> = vec![
            b"SET".to_vec(),
            key.as_bytes().to_vec(),
            payload,
        ];
        if expire_seconds > 0 {
            args.push(b"EX".to_vec());
            args.push(expire_seconds.to_string().into_bytes());
        }
        args.push(b"GET".to_vec());

        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        let rs = self.execute_on_key(key, true, &refs)?;
        if rs.is_null() {
            return Ok(None);
        }
        Ok(rs.as_bytes().and_then(|b| T::from_redis_payload(&b).ok()))
    }

    /// 追加内容（`APPEND`），返回追加后的长度。
    pub fn append(&self, key: &str, value: &str) -> Result<i64> {
        Ok(self
            .execute_on_key(key, true, &[b"APPEND", key.as_bytes(), value.as_bytes()])?
            .as_i64()
            .unwrap_or(0))
    }

    /// 字符串长度（`STRLEN`）。
    pub fn strlen(&self, key: &str) -> Result<i64> {
        Ok(self
            .execute_on_key(key, false, &[b"STRLEN", key.as_bytes()])?
            .as_i64()
            .unwrap_or(0))
    }

    /// 截取子串（`GETRANGE`，含头含尾，-1 表示末尾）。
    pub fn get_range(&self, key: &str, start: i64, end: i64) -> Result<String> {
        Ok(self
            .execute_on_key(key, false, &[
                b"GETRANGE",
                key.as_bytes(),
                start.to_string().as_bytes(),
                end.to_string().as_bytes(),
            ])?
            .as_string()
            .unwrap_or_default())
    }

    /// 覆盖区间（`SETRANGE`），返回新长度。
    pub fn set_range(&self, key: &str, offset: i64, value: &str) -> Result<i64> {
        Ok(self
            .execute_on_key(key, true, &[
                b"SETRANGE",
                key.as_bytes(),
                offset.to_string().as_bytes(),
                value.as_bytes(),
            ])?
            .as_i64()
            .unwrap_or(0))
    }

    /// 自增（`INCR` / `INCRBY`）。
    pub fn increment(&self, key: &str, delta: i64) -> Result<i64> {
        let rs = if delta == 1 {
            self.execute_on_key(key, true, &[b"INCR", key.as_bytes()])?
        } else {
            self.execute_on_key(key, true, &[b"INCRBY", key.as_bytes(), delta.to_string().as_bytes()])?
        };
        Ok(rs.as_i64().unwrap_or(0))
    }

    /// 浮点自增（`INCRBYFLOAT`）。
    pub fn increment_float(&self, key: &str, delta: f64) -> Result<f64> {
        let rs = self.execute_on_key(key, true, &[
            b"INCRBYFLOAT",
            key.as_bytes(),
            crate::encoder::format_f64(delta).as_bytes(),
        ])?;
        rs.as_f64()
            .ok_or_else(|| Error::Type("INCRBYFLOAT 返回不是数字".into()))
    }

    /// 自减（`DECR` / `DECRBY`）。
    pub fn decrement(&self, key: &str, delta: i64) -> Result<i64> {
        let rs = if delta == 1 {
            self.execute_on_key(key, true, &[b"DECR", key.as_bytes()])?
        } else {
            self.execute_on_key(key, true, &[b"DECRBY", key.as_bytes(), delta.to_string().as_bytes()])?
        };
        Ok(rs.as_i64().unwrap_or(0))
    }

    /// 位设置（`SETBIT`）。
    pub fn set_bit(&self, key: &str, offset: u64, value: u8) -> Result<i64> {
        Ok(self
            .execute_on_key(key, true, &[
                b"SETBIT",
                key.as_bytes(),
                offset.to_string().as_bytes(),
                value.to_string().as_bytes(),
            ])?
            .as_i64()
            .unwrap_or(0))
    }

    /// 位读取（`GETBIT`）。
    pub fn get_bit(&self, key: &str, offset: u64) -> Result<i64> {
        Ok(self
            .execute_on_key(key, false, &[b"GETBIT", key.as_bytes(), offset.to_string().as_bytes()])?
            .as_i64()
            .unwrap_or(0))
    }

    /// 统计置位数量（`BITCOUNT`）。
    pub fn bit_count(&self, key: &str, start: i64, end: i64) -> Result<i64> {
        Ok(self
            .execute_on_key(key, false, &[
                b"BITCOUNT",
                key.as_bytes(),
                start.to_string().as_bytes(),
                end.to_string().as_bytes(),
            ])?
            .as_i64()
            .unwrap_or(0))
    }

    /// 查找首个置位/清零位（`BITPOS`）。
    pub fn bit_pos(&self, key: &str, bit: i32, start: i64, end: i64) -> Result<i64> {
        Ok(self
            .execute_on_key(key, false, &[
                b"BITPOS",
                key.as_bytes(),
                bit.to_string().as_bytes(),
                start.to_string().as_bytes(),
                end.to_string().as_bytes(),
            ])?
            .as_i64()
            .unwrap_or(-1))
    }

    /// 位运算（`BITOP`），返回目标键长度。
    pub fn bit_op(&self, operation: &str, dest_key: &str, keys: &[&str]) -> Result<i64> {
        let mut args: Vec<&[u8]> = Vec::with_capacity(keys.len() + 3);
        args.push(b"BITOP");
        args.push(operation.as_bytes());
        args.push(dest_key.as_bytes());
        for k in keys {
            args.push(k.as_bytes());
        }
        Ok(self.execute(&args)?.as_i64().unwrap_or(0))
    }

    // ================== 批量读写 ==================

    /// 批量获取（`MGET`）。返回与入参键一一对应的值（不存在的键为 `None`）。
    pub fn get_all_raw(&self, keys: &[&str]) -> Result<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        if let Some(groups) = self.route_key_groups(keys, false)? {
            let mut results = vec![None; keys.len()];
            for (endpoint, entries) in groups {
                let mut args: Vec<&[u8]> = Vec::with_capacity(entries.len() + 1);
                args.push(b"MGET");
                for (_, key) in &entries {
                    args.push(key.as_bytes());
                }

                let items = self
                    .execute_on_endpoint(&endpoint, &args, None, false)?
                    .into_array()
                    .ok_or_else(|| Error::Protocol("MGET 返回结构非法".into()))?;
                if items.len() != entries.len() {
                    return Err(Error::Protocol("MGET 返回数量与请求键数量不一致".into()));
                }

                for ((index, _), value) in entries.iter().zip(items) {
                    results[*index] = if value.is_null() { None } else { value.as_bytes() };
                }
            }
            return Ok(results);
        }

        let mut args: Vec<&[u8]> = Vec::with_capacity(keys.len() + 1);
        args.push(b"MGET");
        for k in keys {
            args.push(k.as_bytes());
        }

        let items = self
            .execute(&args)?
            .into_array()
            .ok_or_else(|| Error::Protocol("MGET 返回结构非法".into()))?;

        Ok(items
            .into_iter()
            .map(|v| if v.is_null() { None } else { v.as_bytes() })
            .collect())
    }

    /// 批量获取并解码为 `HashMap`（键为传入的原始键名，解码失败的值不放入结果）。
    pub fn get_all<T: FromRedisPayload>(&self, keys: &[&str]) -> Result<HashMap<String, T>> {
        let values = self.get_all_raw(keys)?;
        let mut dic = HashMap::with_capacity(keys.len());
        for (key, raw) in keys.iter().zip(values) {
            if let Some(bytes) = raw
                && let Ok(v) = T::from_redis_payload(&bytes) {
                    dic.insert((*key).to_string(), v);
                }
        }
        Ok(dic)
    }

    /// 批量设置（`MSET`），随后按需批量设置过期时间（管道 `EXPIRE`），与 C# `SetAll` 一致。
    pub fn set_all<K, V>(&self, values: &[(K, V)], expire_seconds: i64) -> Result<()>
    where
        K: AsRef<str>,
        V: ToRedisPayload,
    {
        if values.is_empty() {
            return Ok(());
        }

        let mut expire = expire_seconds;
        if expire < 0 {
            expire = self.inner.options.expire;
        }

        // 少量数据直接逐个写入（C# 对 <=2 项做同样优化）
        if values.len() <= 2 {
            for (k, v) in values {
                self.set(k.as_ref(), v, expire)?;
            }
            return Ok(());
        }

        self.ensure_topology()?;
        if self.topology().is_some() {
            for (k, v) in values {
                self.set(k.as_ref(), v, expire)?;
            }
            return Ok(());
        }

        let mut frame: Vec<Vec<u8>> = vec![b"MSET".to_vec()];
        let mut keys: Vec<String> = Vec::with_capacity(values.len());
        for (k, v) in values {
            let payload = v.to_redis_payload()?.unwrap_or_default();
            frame.push(k.as_ref().as_bytes().to_vec());
            frame.push(payload);
            keys.push(k.as_ref().to_string());
        }

        {
            let refs: Vec<&[u8]> = frame.iter().map(|a| a.as_slice()).collect();
            self.execute_ignore(&refs)?;
        }

        if expire > 0 {
            let mut pipeline = self.pipeline();
            for key in &keys {
                pipeline.cmd(&[b"EXPIRE", key.as_bytes(), expire.to_string().as_bytes()]);
            }
            pipeline.execute_ignore()?;
        }

        Ok(())
    }

    // ================== 服务器 ==================

    /// `PING`。
    pub fn ping(&self) -> Result<bool> {
        Ok(self.execute(&[b"PING"])?.as_string().as_deref() == Some("PONG"))
    }

    /// `INFO`，解析为字典。
    pub fn info(&self) -> Result<HashMap<String, String>> {
        let text = self.execute(&[b"INFO"])?.as_string().unwrap_or_default();
        Ok(parse_info(&text))
    }

    /// 服务器版本号字符串（`redis_version`）。
    pub fn version(&self) -> Result<Option<String>> {
        Ok(self.info()?.get("redis_version").cloned())
    }

    /// 服务器版本号元组 `(major, minor, patch)`，解析失败返回 `(0, 0, 0)`。
    ///
    /// 与 C# `Redis.Version` 一致：部分兼容实现（Garnet/Pika）版本号可能非标准，解析失败视为 0。
    pub fn version_parts(&self) -> Result<(u32, u32, u32)> {
        Ok(parse_version_parts(self.version()?.as_deref().unwrap_or("")))
    }

    /// 版本门禁：低于要求版本时返回 [`Error::Unsupported`]（对应 C# `RequireVersion`）。
    pub fn require_version(&self, required: &str, command: &str) -> Result<()> {
        let (rm, rn, rp) = parse_version_parts(required);
        let (m, n, p) = self.version_parts()?;
        if (m, n, p) < (rm, rn, rp) {
            return Err(Error::Unsupported(format!(
                "命令 {command} 需要 Redis {required}+ 版本，当前服务器版本: {m}.{n}.{p}。请升级 Redis 或使用兼容版本。"
            )));
        }
        Ok(())
    }

    /// 服务器类型（对应 C# `Redis.ServerType`，依据 `INFO` 探测 Garnet/Pika/Dragonfly 等兼容实现）。
    pub fn server_type(&self) -> Result<ServerType> {
        let info = self.info()?;
        Ok(detect_server_type(&info))
    }

    /// `INFO all`，解析为字典（对应 C# `GetInfo(all: true)`）。
    pub fn info_all(&self) -> Result<HashMap<String, String>> {
        let text = self
            .execute(&[b"INFO", b"all"])?
            .as_string()
            .unwrap_or_default();
        Ok(parse_info(&text))
    }

    /// 当前服务器时间（`TIME`）：`(秒, 微秒)`。
    pub fn time(&self) -> Result<(i64, i64)> {
        let items = self
            .execute(&[b"TIME"])?
            .into_array()
            .ok_or_else(|| Error::Protocol("TIME 返回结构非法".into()))?;
        let secs = items
            .first()
            .and_then(|v| v.as_string())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let micros = items
            .get(1)
            .and_then(|v| v.as_string())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        Ok((secs, micros))
    }

    /// 清空当前库（`FLUSHDB`）。
    pub fn clear(&self) -> Result<()> {
        self.execute_ignore(&[b"FLUSHDB"])
    }

    /// 切换库的等价实现：创建指向另一个库的子级客户端（对应 C# `CreateSub(db)`）。
    ///
    /// C# 中 `Select` 只在连接内部生效；Rust 侧保持"配置即状态"，避免多线程连接池状态不一致。
    pub fn create_sub(&self, db: i32) -> Result<Redis> {
        let mut options = self.inner.options.clone();
        options.db = db;
        Redis::new(options)
    }

    /// 加载脚本（`SCRIPT LOAD`），返回 SHA1。
    pub fn script_load(&self, script: &str) -> Result<String> {
        Ok(self
            .execute(&[b"SCRIPT", b"LOAD", script.as_bytes()])?
            .as_string()
            .unwrap_or_default())
    }

    /// 判断脚本是否存在（`SCRIPT EXISTS`）。
    pub fn script_exists(&self, sha1: &str) -> Result<bool> {
        let rs = self.execute(&[b"SCRIPT", b"EXISTS", sha1.as_bytes()])?;
        Ok(rs
            .into_array()
            .and_then(|v| v.first().and_then(|x| x.as_i64()))
            .unwrap_or(0)
            == 1)
    }

    /// 清空脚本缓存（`SCRIPT FLUSH`）。
    pub fn script_flush(&self) -> Result<()> {
        self.execute_ignore(&[b"SCRIPT", b"FLUSH"])
    }

    /// 执行脚本（`EVAL`），返回原始应答。
    pub fn eval_raw(&self, script: &str, keys: &[&str], args: &[&str]) -> Result<RespValue> {
        let mut argv: Vec<Vec<u8>> = Vec::with_capacity(keys.len() + args.len() + 3);
        argv.push(b"EVAL".to_vec());
        argv.push(script.as_bytes().to_vec());
        argv.push(keys.len().to_string().into_bytes());
        for k in keys {
            argv.push(k.as_bytes().to_vec());
        }
        for a in args {
            argv.push(a.as_bytes().to_vec());
        }

        let refs: Vec<&[u8]> = argv.iter().map(|a| a.as_slice()).collect();
        if let Some(key) = keys.first() {
            self.execute_on_key(key, true, &refs)
        } else {
            self.execute(&refs)
        }
    }

    /// 执行脚本并解码结果。
    pub fn eval<T: FromRedisPayload>(
        &self,
        script: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<Option<T>> {
        let rs = self.eval_raw(script, keys, args)?;
        if rs.is_null() {
            return Ok(None);
        }
        Ok(rs.as_bytes().and_then(|b| T::from_redis_payload(&b).ok()))
    }

    // ================== 管道 ==================

    /// 创建管道（批量提交命令，减少往返）。
    ///
    /// 对应 C# 的 `StartPipeline()` / `StopPipeline()`：
    /// 管道内的命令一次写出、一次读回，中途不做错误重试。
    pub fn pipeline(&self) -> Pipeline<'_> {
        Pipeline {
            redis: self,
            commands: Vec::new(),
        }
    }
}

/// 命令管道。
pub struct Pipeline<'a> {
    redis: &'a Redis,
    commands: Vec<Vec<Vec<u8>>>,
}

impl<'a> Pipeline<'a> {
    /// 追加一条原始命令。
    pub fn cmd(&mut self, args: &[&[u8]]) -> &mut Self {
        self.commands.push(args.iter().map(|a| a.to_vec()).collect());
        self
    }

    /// 追加 `GET key`。
    pub fn get(&mut self, key: &str) -> &mut Self {
        self.cmd(&[b"GET", key.as_bytes()])
    }

    /// 追加 `SET key value`。
    pub fn set<K: AsRef<str>, V: ToRedisPayload>(&mut self, key: K, value: V) -> Result<&mut Self> {
        let payload = value.to_redis_payload()?.unwrap_or_default();
        Ok(self.cmd(&[b"SET", key.as_ref().as_bytes(), &payload]))
    }

    /// 追加 `DEL key`。
    pub fn remove(&mut self, key: &str) -> &mut Self {
        self.cmd(&[b"DEL", key.as_bytes()])
    }

    /// 已缓存命令数。
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// 执行全部命令并返回应答（`Vec<RespValue>`）。
    pub fn execute(&mut self) -> Result<Vec<RespValue>> {
        if self.commands.is_empty() {
            return Ok(Vec::new());
        }

        let mut client = self.redis.pool().get()?;
        let results = client.command_many(&self.commands);
        self.commands.clear();

        match results {
            Ok(values) => Ok(values),
            Err(e) => Err(e),
        }
    }

    /// 执行全部命令但丢弃应答。
    pub fn execute_ignore(&mut self) -> Result<()> {
        self.execute().map(|_| ())
    }

    /// 执行并检查每条应答是否为 `OK`。
    pub fn execute_ok(&mut self) -> Result<()> {
        for value in self.execute()? {
            if let RespValue::Error(msg) = value { return Err(Error::Server(msg)) }
        }
        Ok(())
    }
}

/// 解析 `INFO` 返回的 `key:value` 文本（与 C# `SplitAsDictionary(":", "\r\n")` 一致）。
pub fn parse_info(text: &str) -> HashMap<String, String> {
    let mut dic = HashMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            dic.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    dic
}

/// 服务器类型（对应 C# `ServerType`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerType {
    /// 未知类型
    Unknown = 0,
    /// 标准 Redis 服务器
    Redis = 1,
    /// Garnet 服务器（微软的 Redis 兼容实现）
    Garnet = 2,
    /// Pika 服务器（RocksDB 存储引擎兼容实现）
    Pika = 3,
    /// DragonflyDB（新型高性能 Redis 兼容数据库）
    DragonflyDb = 4,
    /// 华为云 DCS 集群版
    HuaweiCloud = 10,
    /// 阿里云 KVStore（Tair）
    AlibabaCloud = 11,
    /// 腾讯云 Redis
    TencentCloud = 12,
}

/// 解析版本号文本（`"7.2.4"` / `"Garnet/1.0"` → 最多取 3 段数字）。
pub fn parse_version_parts(text: &str) -> (u32, u32, u32) {
    let mut parts = [0u32; 3];
    let mut idx = 0;
    for seg in text.split(['.', '-', '/', '+']) {
        if idx >= 3 {
            break;
        }
        let digits: String = seg.chars().take_while(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() {
            if idx == 0 && !seg.is_empty() {
                // 首段不是数字（如 "Garnet"），继续看后续段
                continue;
            }
            break;
        }
        parts[idx] = digits.parse().unwrap_or(0);
        idx += 1;
    }
    (parts[0], parts[1], parts[2])
}

/// 从 `INFO` 结果探测服务器类型（与 C# `Redis.DetectServerType` 一致）。
pub fn detect_server_type(info: &HashMap<String, String>) -> ServerType {
    let redis_version = info.get("redis_version").map(|s| s.as_str()).unwrap_or("");
    let server = info.get("server").map(|s| s.as_str()).unwrap_or("");
    let os = info.get("os").map(|s| s.as_str()).unwrap_or("");

    if redis_version.contains("Garnet") || server.contains("Garnet") {
        return ServerType::Garnet;
    }
    if redis_version.to_ascii_lowercase().contains("pika") || server.contains("Pika") {
        return ServerType::Pika;
    }
    if server.to_ascii_lowercase().contains("dragonfly") {
        return ServerType::DragonflyDb;
    }
    if os.contains("Huawei") {
        return ServerType::HuaweiCloud;
    }
    if server.contains("Tair") || server.contains("Alibaba") {
        return ServerType::AlibabaCloud;
    }
    if server.contains("Tencent") {
        return ServerType::TencentCloud;
    }
    ServerType::Redis
}

fn detect_topology_mode_from_info(
    info: &HashMap<String, String>,
    configured_servers: usize,
) -> Option<ServerMode> {
    if let Some(mode) = info.get("redis_mode")
        && mode.eq_ignore_ascii_case("cluster")
    {
        return Some(ServerMode::Cluster);
    }
    if info.contains_key("sentinel_masters") {
        return Some(ServerMode::Sentinel);
    }
    if info.contains_key("role")
        && configured_servers > 1
        && info
            .get("redis_mode")
            .map(|mode| mode.eq_ignore_ascii_case("standalone"))
            .unwrap_or(true)
    {
        return Some(ServerMode::Replication);
    }
    None
}

fn parse_replication_discovery(info: &HashMap<String, String>) -> ReplicationDiscovery {
    let mut result = ReplicationDiscovery {
        role: info.get("role").cloned(),
        master_endpoint: None,
        slaves: Vec::new(),
        masters: Vec::new(),
    };

    if let (Some(host), Some(port)) = (info.get("master_host"), info.get("master_port"))
        && !host.is_empty()
        && let Ok(port) = port.parse::<u16>()
    {
        result.master_endpoint = Some(format!("{host}:{port}"));
    }

    if let Some(count) = info.get("connected_slaves").and_then(|v| v.parse::<usize>().ok()) {
        for index in 0..count {
            if let Some(text) = info.get(&format!("slave{index}"))
                && let Some(slave) = parse_slave_info(text)
            {
                result.slaves.push(slave);
            }
        }
    }

    if let Some(count) = info.get("sentinel_masters").and_then(|v| v.parse::<usize>().ok()) {
        for index in 0..count {
            if let Some(text) = info.get(&format!("master{index}"))
                && let Some(master) = parse_master_info(text)
            {
                result.masters.push(master);
            }
        }
    }

    result
}

fn parse_kv_csv(text: &str) -> HashMap<String, String> {
    let mut dic = HashMap::new();
    for part in text.split(',') {
        let part = part.trim();
        if let Some((key, value)) = part.split_once('=') {
            dic.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    dic
}

fn parse_slave_info(text: &str) -> Option<DiscoveredSlave> {
    let dic = parse_kv_csv(text);
    let host = dic.get("ip")?;
    let port = dic.get("port")?.parse::<u16>().ok()?;
    let state = dic.get("state").map(|v| v.as_str()).unwrap_or("online");
    Some(DiscoveredSlave {
        endpoint: format!("{host}:{port}"),
        link_up: !state.eq_ignore_ascii_case("offline"),
    })
}

fn parse_master_info(text: &str) -> Option<DiscoveredMaster> {
    let dic = parse_kv_csv(text);
    let endpoint = dic.get("address")?.to_string();
    Some(DiscoveredMaster {
        name: dic.get("name").cloned(),
        endpoint,
        status: dic.get("status").cloned(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedirectKind {
    Moved,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Redirect<'a> {
    kind: RedirectKind,
    slot: u16,
    endpoint: &'a str,
}

fn parse_redirect(message: &str) -> Option<Redirect<'_>> {
    let mut parts = message.split_whitespace();
    let kind = parts.next()?;
    let kind = if kind.eq_ignore_ascii_case("MOVED") {
        RedirectKind::Moved
    } else if kind.eq_ignore_ascii_case("ASK") {
        RedirectKind::Ask
    } else {
        return None;
    };
    let slot = parts.next()?.parse().ok()?;
    let endpoint = parts.next()?;
    Some(Redirect {
        kind,
        slot,
        endpoint,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_info_reads_key_values() {
        let text = "# Server\r\nredis_version:7.2.4\r\nredis_mode:standalone\r\nos:Linux\r\n";
        let dic = parse_info(text);
        assert_eq!(dic.get("redis_version").unwrap(), "7.2.4");
        assert_eq!(dic.get("redis_mode").unwrap(), "standalone");
    }

    #[test]
    fn options_are_exposed() {
        let rds = Redis::new(RedisOptions::new("127.0.0.1:6379", Some("pwd"), 3)).unwrap();
        assert_eq!(rds.options().db, 3);
        assert_eq!(rds.server(), "127.0.0.1:6379");
        assert_eq!(rds.stats(), (0, 0));
    }

    #[test]
    fn parse_redirect_parses_moved_and_ask() {
        let moved = parse_redirect("MOVED 3999 127.0.0.1:7002").unwrap();
        assert_eq!(moved.kind, RedirectKind::Moved);
        assert_eq!(moved.slot, 3999);
        assert_eq!(moved.endpoint, "127.0.0.1:7002");

        let ask = parse_redirect("ASK 12000 10.0.0.2:6379").unwrap();
        assert_eq!(ask.kind, RedirectKind::Ask);
        assert_eq!(ask.slot, 12000);
        assert_eq!(ask.endpoint, "10.0.0.2:6379");
        assert_eq!(parse_redirect("ERR something"), None);
    }

    #[test]
    fn detect_topology_mode_from_info_distinguishes_cluster_replication_and_sentinel() {
        let cluster = parse_info("redis_mode:cluster\r\n");
        assert_eq!(detect_topology_mode_from_info(&cluster, 1), Some(ServerMode::Cluster));

        let repl = parse_info("redis_mode:standalone\r\nrole:master\r\nconnected_slaves:1\r\n");
        assert_eq!(detect_topology_mode_from_info(&repl, 2), Some(ServerMode::Replication));

        let sentinel = parse_info("sentinel_masters:1\r\nmaster0:name=m,status=ok,address=127.0.0.1:6379\r\n");
        assert_eq!(detect_topology_mode_from_info(&sentinel, 1), Some(ServerMode::Sentinel));
    }

    #[test]
    fn parse_replication_discovery_extracts_master_slave_and_sentinel_nodes() {
        let repl = parse_info(
            "role:slave\r\nmaster_host:127.0.0.1\r\nmaster_port:6379\r\nconnected_slaves:2\r\nslave0:ip=127.0.0.1,port=6380,state=online\r\nslave1:ip=127.0.0.1,port=6381,state=offline\r\n",
        );
        let parsed = parse_replication_discovery(&repl);
        assert_eq!(parsed.master_endpoint.as_deref(), Some("127.0.0.1:6379"));
        assert_eq!(parsed.slaves.len(), 2);
        assert!(parsed.slaves[0].link_up);
        assert!(!parsed.slaves[1].link_up);

        let sentinel = parse_info(
            "sentinel_masters:1\r\nmaster0:name=redis-master,status=ok,address=127.0.0.1:6379,slaves=2,sentinels=3\r\n",
        );
        let parsed = parse_replication_discovery(&sentinel);
        assert_eq!(parsed.masters.len(), 1);
        assert_eq!(parsed.masters[0].name.as_deref(), Some("redis-master"));
        assert_eq!(parsed.masters[0].endpoint, "127.0.0.1:6379");
    }
}
