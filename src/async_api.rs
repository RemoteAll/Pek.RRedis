//! tokio 异步包装层。
//!
//! 当前实现不重写底层 RESP/TCP/TLS 传输，而是把现有同步客户端包装到
//! `tokio::task::spawn_blocking` 中执行：
//!
//! - 优点：与同步实现保持完全同源语义，复用现有 cluster/sentinel/replication/TLS/队列逻辑；
//! - 代价：底层仍是阻塞 I/O，需要 tokio runtime 的 blocking 线程池承载；
//! - 适用：把现有业务逐步迁到 async 上层，同时不重复维护另一套 Redis 协议栈。

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};

use chrono::NaiveDateTime;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::task;

use crate::error::{Error, Result};
use crate::full::{FullRedis, LockHandle, SlowLogEntry};
use crate::geo::{GeoMember, RedisGeo};
use crate::hash::RedisHash;
use crate::hyperloglog::HyperLogLog;
use crate::list::RedisList;
use crate::pubsub::PubSub;
use crate::queues::{
    ConsumerInfo, GroupInfo, Message, PendingInfo, PendingItem, RedisDelayQueue, RedisQueue,
    RedisReliableQueue, RedisStream, StreamInfo,
};
use crate::redis::{Redis, ServerType};
use crate::set::RedisSet;
use crate::sortedset::RedisSortedSet;
use crate::stack::RedisStack;
use crate::{FromRedisPayload, ToRedisPayload};

async fn spawn_result<T, F>(f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Operation(format!("异步任务执行失败：{e}")))?
}

async fn spawn_locked<T, S, F>(state: Arc<Mutex<S>>, f: F) -> Result<T>
where
    T: Send + 'static,
    S: Send + 'static,
    F: FnOnce(&mut S) -> Result<T> + Send + 'static,
{
    spawn_result(move || {
        let mut state = state
            .lock()
            .map_err(|_| Error::Operation("异步状态锁已中毒".into()))?;
        f(&mut state)
    })
    .await
}

/// tokio 异步 Redis 基础客户端。
#[derive(Clone)]
pub struct AsyncRedis {
    inner: Redis,
}

impl std::fmt::Debug for AsyncRedis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncRedis")
            .field("servers", &self.inner.options().servers)
            .field("db", &self.inner.options().db)
            .finish()
    }
}

impl AsyncRedis {
    /// 用同步句柄创建异步包装。
    pub fn from_sync(inner: Redis) -> Self {
        Self { inner }
    }

    /// 使用地址、密码、库号创建。
    pub fn open(server: &str, password: Option<&str>, db: i32) -> Result<Self> {
        Ok(Self::from_sync(Redis::open(server, password, db)?))
    }

    /// 使用连接字符串创建。
    pub fn from_config(config: &str) -> Result<Self> {
        Ok(Self::from_sync(Redis::from_config(config)?))
    }

    /// 回到底层同步句柄（共享同一连接池）。
    pub fn sync(&self) -> &Redis {
        &self.inner
    }

    /// 对底层同步句柄执行任意闭包，并放入 tokio blocking 池。
    pub async fn with_sync<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(Redis) -> Result<T> + Send + 'static,
    {
        let redis = self.inner.clone();
        spawn_result(move || f(redis)).await
    }

    /// 执行命令。
    pub async fn execute(&self, args: Vec<Vec<u8>>) -> Result<crate::RespValue> {
        self.with_sync(move |redis| {
            let refs: Vec<&[u8]> = args.iter().map(|arg| arg.as_slice()).collect();
            redis.execute(&refs)
        })
        .await
    }

    /// 执行阻塞命令。
    pub async fn execute_blocking(
        &self,
        args: Vec<Vec<u8>>,
        block_seconds: i64,
    ) -> Result<crate::RespValue> {
        self.with_sync(move |redis| {
            let refs: Vec<&[u8]> = args.iter().map(|arg| arg.as_slice()).collect();
            redis.execute_blocking(&refs, block_seconds)
        })
        .await
    }

    /// 设置键值。
    pub async fn set<V>(&self, key: String, value: V, expire_seconds: i64) -> Result<bool>
    where
        V: ToRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| redis.set(&key, value, expire_seconds))
            .await
    }

    /// 读取键值。
    pub async fn get<V>(&self, key: String) -> Result<Option<V>>
    where
        V: FromRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| redis.get::<V>(&key)).await
    }

    /// 批量设置。
    pub async fn set_all<V>(&self, items: Vec<(String, V)>, expire_seconds: i64) -> Result<()>
    where
        V: ToRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| {
            let borrowed: Vec<(&str, &V)> = items
                .iter()
                .map(|(key, value)| (key.as_str(), value))
                .collect();
            redis.set_all(&borrowed, expire_seconds)
        })
        .await
    }

    /// 批量获取（保持原始键名）。
    pub async fn get_all<V>(&self, keys: Vec<String>) -> Result<HashMap<String, V>>
    where
        V: FromRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            redis.get_all(&refs)
        })
        .await
    }

    /// 删除单个键。
    pub async fn remove(&self, key: String) -> Result<i64> {
        self.with_sync(move |redis| redis.remove(&key)).await
    }

    /// 批量删除。
    pub async fn remove_many(&self, keys: Vec<String>) -> Result<i64> {
        self.with_sync(move |redis| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            redis.remove_many(&refs)
        })
        .await
    }

    /// 数据库键数量。
    pub async fn dbsize(&self) -> Result<i64> {
        self.with_sync(|redis| redis.dbsize()).await
    }

    /// 模式取键。
    pub async fn keys_raw(&self, pattern: String) -> Result<Vec<String>> {
        self.with_sync(move |redis| redis.keys_raw(&pattern)).await
    }

    /// 健康检查。
    pub async fn ping(&self) -> Result<bool> {
        self.with_sync(|redis| redis.ping()).await
    }

    /// 键是否存在。
    pub async fn contains_key(&self, key: String) -> Result<bool> {
        self.with_sync(move |redis| redis.contains_key(&key)).await
    }

    /// 设置过期时间。
    pub async fn set_expire(&self, key: String, seconds: i64) -> Result<bool> {
        self.with_sync(move |redis| redis.set_expire(&key, seconds)).await
    }

    /// 获取过期时间。
    pub async fn get_expire(&self, key: String) -> Result<i64> {
        self.with_sync(move |redis| redis.get_expire(&key)).await
    }

    /// 重命名键。
    pub async fn rename(&self, key: String, new_key: String, overwrite: bool) -> Result<bool> {
        self.with_sync(move |redis| redis.rename(&key, &new_key, overwrite))
            .await
    }

    /// 获取原始二进制值。
    pub async fn get_raw(&self, key: String) -> Result<Option<Vec<u8>>> {
        self.with_sync(move |redis| redis.get_raw(&key)).await
    }

    /// 获取字符串值。
    pub async fn get_string(&self, key: String) -> Result<Option<String>> {
        self.with_sync(move |redis| redis.get_string(&key)).await
    }

    /// 仅当不存在时设置。
    pub async fn add<V>(&self, key: String, value: V, expire_seconds: i64) -> Result<bool>
    where
        V: ToRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| redis.add(&key, value, expire_seconds))
            .await
    }

    /// 替换键值并返回旧值。
    pub async fn replace<V>(&self, key: String, value: V) -> Result<Option<V>>
    where
        V: FromRedisPayload + ToRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| redis.replace(&key, value)).await
    }

    /// 设置新值并返回旧值。
    pub async fn set_get<V>(&self, key: String, value: V, expire_seconds: i64) -> Result<Option<V>>
    where
        V: FromRedisPayload + ToRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| redis.set_get(&key, value, expire_seconds))
            .await
    }

    /// 追加字符串。
    pub async fn append(&self, key: String, value: String) -> Result<i64> {
        self.with_sync(move |redis| redis.append(&key, &value)).await
    }

    /// 字符串长度。
    pub async fn strlen(&self, key: String) -> Result<i64> {
        self.with_sync(move |redis| redis.strlen(&key)).await
    }

    /// 字符串区间读取。
    pub async fn get_range(&self, key: String, start: i64, end: i64) -> Result<String> {
        self.with_sync(move |redis| redis.get_range(&key, start, end)).await
    }

    /// 字符串区间覆盖。
    pub async fn set_range(&self, key: String, offset: i64, value: String) -> Result<i64> {
        self.with_sync(move |redis| redis.set_range(&key, offset, &value))
            .await
    }

    /// 整数自增。
    pub async fn increment(&self, key: String, delta: i64) -> Result<i64> {
        self.with_sync(move |redis| redis.increment(&key, delta)).await
    }

    /// 浮点自增。
    pub async fn increment_float(&self, key: String, delta: f64) -> Result<f64> {
        self.with_sync(move |redis| redis.increment_float(&key, delta))
            .await
    }

    /// 整数自减。
    pub async fn decrement(&self, key: String, delta: i64) -> Result<i64> {
        self.with_sync(move |redis| redis.decrement(&key, delta)).await
    }

    /// 服务器信息。
    pub async fn info(&self) -> Result<HashMap<String, String>> {
        self.with_sync(|redis| redis.info()).await
    }

    /// 服务器版本文本。
    pub async fn version(&self) -> Result<Option<String>> {
        self.with_sync(|redis| redis.version()).await
    }

    /// 服务器版本号。
    pub async fn version_parts(&self) -> Result<(u32, u32, u32)> {
        self.with_sync(|redis| redis.version_parts()).await
    }

    /// 服务器类型。
    pub async fn server_type(&self) -> Result<ServerType> {
        self.with_sync(|redis| redis.server_type()).await
    }

    /// 执行 Lua 脚本并返回原始应答。
    pub async fn eval_raw(
        &self,
        script: String,
        keys: Vec<String>,
        args: Vec<String>,
    ) -> Result<crate::RespValue> {
        self.with_sync(move |redis| {
            let key_refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            let arg_refs: Vec<&str> = args.iter().map(|arg| arg.as_str()).collect();
            redis.eval_raw(&script, &key_refs, &arg_refs)
        })
        .await
    }

    /// 执行 Lua 脚本并解码返回值。
    pub async fn eval<V>(&self, script: String, keys: Vec<String>, args: Vec<String>) -> Result<Option<V>>
    where
        V: FromRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| {
            let key_refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            let arg_refs: Vec<&str> = args.iter().map(|arg| arg.as_str()).collect();
            redis.eval(&script, &key_refs, &arg_refs)
        })
        .await
    }
}

/// tokio 异步增强版 Redis。
#[derive(Clone)]
pub struct AsyncFullRedis {
    inner: FullRedis,
}

impl std::fmt::Debug for AsyncFullRedis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncFullRedis")
            .field("servers", &self.inner.redis().options().servers)
            .field("db", &self.inner.redis().options().db)
            .field("prefix", &self.inner.prefix())
            .finish()
    }
}

impl AsyncFullRedis {
    /// 用同步句柄创建异步包装。
    pub fn from_sync(inner: FullRedis) -> Self {
        Self { inner }
    }

    /// 使用地址、密码、库号创建。
    pub fn open(server: &str, password: Option<&str>, db: i32) -> Result<Self> {
        Ok(Self::from_sync(FullRedis::open(server, password, db)?))
    }

    /// 使用连接字符串创建。
    pub fn from_config(config: &str) -> Result<Self> {
        Ok(Self::from_sync(FullRedis::from_config(config)?))
    }

    /// 基础异步客户端。
    pub fn redis(&self) -> AsyncRedis {
        AsyncRedis::from_sync(self.inner.redis().clone())
    }

    /// 执行任意同步闭包。
    pub async fn with_sync<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(FullRedis) -> Result<T> + Send + 'static,
    {
        let redis = self.inner.clone();
        spawn_result(move || f(redis)).await
    }

    /// 搜索。
    pub async fn search(&self, pattern: String, count: usize) -> Result<Vec<String>> {
        self.with_sync(move |redis| redis.search(&pattern, count)).await
    }

    /// 按模式删除。
    pub async fn remove_pattern(&self, pattern: String) -> Result<i64> {
        self.with_sync(move |redis| redis.remove_pattern(&pattern)).await
    }

    /// 分布式锁。
    pub async fn acquire_lock(&self, key: String, ms_timeout: i32) -> Result<LockHandle> {
        self.with_sync(move |redis| redis.acquire_lock(&key, ms_timeout))
            .await
    }

    /// 创建子库。
    pub fn create_sub(&self, db: i32) -> Result<Self> {
        Ok(Self::from_sync(self.inner.create_sub(db)?))
    }

    /// 批量删除。
    pub async fn remove_many(&self, keys: Vec<String>) -> Result<i64> {
        self.with_sync(move |redis| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            redis.remove_many(&refs)
        })
        .await
    }

    /// 批量获取。
    pub async fn get_all<V>(&self, keys: Vec<String>) -> Result<HashMap<String, V>>
    where
        V: FromRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            redis.get_all(&refs)
        })
        .await
    }

    /// 批量设置。
    pub async fn set_all<V>(&self, values: Vec<(String, V)>, expire_seconds: i64) -> Result<()>
    where
        V: ToRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| {
            let borrowed: Vec<(&str, &V)> = values
                .iter()
                .map(|(key, value)| (key.as_str(), value))
                .collect();
            redis.set_all(&borrowed, expire_seconds)
        })
        .await
    }

    /// 获取哈希全部字段。
    pub async fn get_hash_all<V>(&self, key: String) -> Result<HashMap<String, V>>
    where
        V: FromRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| redis.get_hash_all(&key)).await
    }

    /// 全量服务器信息。
    pub async fn info_all(&self) -> Result<HashMap<String, String>> {
        self.with_sync(|redis| redis.info_all()).await
    }

    /// 服务器版本号。
    pub async fn version_parts(&self) -> Result<(u32, u32, u32)> {
        self.with_sync(|redis| redis.version_parts()).await
    }

    /// 服务器类型。
    pub async fn server_type(&self) -> Result<ServerType> {
        self.with_sync(|redis| redis.server_type()).await
    }

    /// 执行 Lua 脚本。
    pub async fn eval<V>(&self, script: String, keys: Vec<String>, args: Vec<String>) -> Result<Option<V>>
    where
        V: FromRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| {
            let key_refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            let arg_refs: Vec<&str> = args.iter().map(|arg| arg.as_str()).collect();
            redis.eval(&script, &key_refs, &arg_refs)
        })
        .await
    }

    /// 执行带前缀的 Lua 脚本。
    pub async fn eval_prefixed<V>(
        &self,
        script: String,
        keys: Vec<String>,
        args: Vec<String>,
    ) -> Result<Option<V>>
    where
        V: FromRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| {
            let key_refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            let arg_refs: Vec<&str> = args.iter().map(|arg| arg.as_str()).collect();
            redis.eval_prefixed(&script, &key_refs, &arg_refs)
        })
        .await
    }

    /// 执行 Lua 脚本并返回原始应答。
    pub async fn eval_raw(
        &self,
        script: String,
        keys: Vec<String>,
        args: Vec<String>,
    ) -> Result<crate::RespValue> {
        self.with_sync(move |redis| {
            let key_refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            let arg_refs: Vec<&str> = args.iter().map(|arg| arg.as_str()).collect();
            redis.eval_raw(&script, &key_refs, &arg_refs)
        })
        .await
    }

    /// 设置键值。
    pub async fn set<V>(&self, key: String, value: V, expire_seconds: i64) -> Result<bool>
    where
        V: ToRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| redis.set(&key, value, expire_seconds))
            .await
    }

    /// 仅当不存在时设置。
    pub async fn add<V>(&self, key: String, value: V, expire_seconds: i64) -> Result<bool>
    where
        V: ToRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| redis.add(&key, value, expire_seconds))
            .await
    }

    /// 替换值并返回旧值。
    pub async fn replace<V>(&self, key: String, value: V) -> Result<Option<V>>
    where
        V: FromRedisPayload + ToRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| redis.replace(&key, value)).await
    }

    /// 读取键值。
    pub async fn get<V>(&self, key: String) -> Result<Option<V>>
    where
        V: FromRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| redis.get(&key)).await
    }

    /// 读取字符串值。
    pub async fn get_string(&self, key: String) -> Result<Option<String>> {
        self.with_sync(move |redis| redis.get_string(&key)).await
    }

    /// 删除单个键。
    pub async fn remove(&self, key: String) -> Result<i64> {
        self.with_sync(move |redis| redis.remove(&key)).await
    }

    /// 键是否存在。
    pub async fn contains_key(&self, key: String) -> Result<bool> {
        self.with_sync(move |redis| redis.contains_key(&key)).await
    }

    /// 设置过期时间。
    pub async fn set_expire(&self, key: String, seconds: i64) -> Result<bool> {
        self.with_sync(move |redis| redis.set_expire(&key, seconds)).await
    }

    /// 获取过期时间。
    pub async fn get_expire(&self, key: String) -> Result<i64> {
        self.with_sync(move |redis| redis.get_expire(&key)).await
    }

    /// 设置新值并返回旧值。
    pub async fn set_get<V>(&self, key: String, value: V, expire_seconds: i64) -> Result<Option<V>>
    where
        V: FromRedisPayload + ToRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| redis.set_get(&key, value, expire_seconds))
            .await
    }

    /// 追加字符串。
    pub async fn append(&self, key: String, value: String) -> Result<i64> {
        self.with_sync(move |redis| redis.append(&key, &value)).await
    }

    /// 字符串长度。
    pub async fn strlen(&self, key: String) -> Result<i64> {
        self.with_sync(move |redis| redis.strlen(&key)).await
    }

    /// 字符串区间读取。
    pub async fn get_range(&self, key: String, start: i64, end: i64) -> Result<String> {
        self.with_sync(move |redis| redis.get_range(&key, start, end)).await
    }

    /// 字符串区间覆盖。
    pub async fn set_range(&self, key: String, offset: i64, value: String) -> Result<i64> {
        self.with_sync(move |redis| redis.set_range(&key, offset, &value))
            .await
    }

    /// 整数自增。
    pub async fn increment(&self, key: String, delta: i64) -> Result<i64> {
        self.with_sync(move |redis| redis.increment(&key, delta)).await
    }

    /// 浮点自增。
    pub async fn increment_float(&self, key: String, delta: f64) -> Result<f64> {
        self.with_sync(move |redis| redis.increment_float(&key, delta))
            .await
    }

    /// 整数自减。
    pub async fn decrement(&self, key: String, delta: i64) -> Result<i64> {
        self.with_sync(move |redis| redis.decrement(&key, delta)).await
    }

    /// 键类型。
    pub async fn type_of(&self, key: String) -> Result<Option<String>> {
        self.with_sync(move |redis| redis.type_of(&key)).await
    }

    /// 重命名键。
    pub async fn rename(&self, key: String, new_key: String) -> Result<bool> {
        self.with_sync(move |redis| redis.rename(&key, &new_key)).await
    }

    /// 键数量。
    pub async fn count(&self) -> Result<i64> {
        self.with_sync(|redis| redis.count()).await
    }

    /// 获取值并更新过期策略。
    pub async fn get_ex<V>(&self, key: String, expire: i32) -> Result<Option<V>>
    where
        V: FromRedisPayload + Send + 'static,
    {
        self.with_sync(move |redis| redis.get_ex(&key, expire)).await
    }

    /// 秒级绝对过期时间。
    pub async fn expire_time(&self, key: String) -> Result<i64> {
        self.with_sync(move |redis| redis.expire_time(&key)).await
    }

    /// 毫秒级绝对过期时间。
    pub async fn pexpire_time(&self, key: String) -> Result<i64> {
        self.with_sync(move |redis| redis.pexpire_time(&key)).await
    }

    /// 对象空闲时间。
    pub async fn object_idle_time(&self, key: String) -> Result<Option<i64>> {
        self.with_sync(move |redis| redis.object_idle_time(&key)).await
    }

    /// 对象访问频率。
    pub async fn object_freq(&self, key: String) -> Result<Option<i64>> {
        self.with_sync(move |redis| redis.object_freq(&key)).await
    }

    /// 等待副本确认。
    pub async fn wait(&self, num_replicas: i32, timeout_ms: i64) -> Result<i64> {
        self.with_sync(move |redis| redis.wait(num_replicas, timeout_ms)).await
    }

    /// 慢日志条数。
    pub async fn slowlog_len(&self) -> Result<i64> {
        self.with_sync(|redis| redis.slowlog_len()).await
    }

    /// 清空慢日志。
    pub async fn slowlog_reset(&self) -> Result<()> {
        self.with_sync(|redis| redis.slowlog_reset()).await
    }

    /// 读取慢日志。
    pub async fn slowlog_get(&self, count: i64) -> Result<Vec<SlowLogEntry>> {
        self.with_sync(move |redis| redis.slowlog_get(count)).await
    }

    /// 延迟历史。
    pub async fn latency_history(&self, event: String) -> Result<Vec<(i64, i64)>> {
        self.with_sync(move |redis| redis.latency_history(&event)).await
    }

    /// 最新延迟统计。
    pub async fn latency_latest(&self) -> Result<Vec<(String, i64, i64, i64)>> {
        self.with_sync(|redis| redis.latency_latest()).await
    }

    /// 重置延迟统计。
    pub async fn latency_reset(&self, events: Vec<String>) -> Result<i64> {
        self.with_sync(move |redis| {
            let refs: Vec<&str> = events.iter().map(|event| event.as_str()).collect();
            redis.latency_reset(&refs)
        })
        .await
    }

    /// 延迟诊断报告。
    pub async fn latency_doctor(&self) -> Result<String> {
        self.with_sync(|redis| redis.latency_doctor()).await
    }

    /// 加载函数库。
    pub async fn function_load(&self, library_code: String, replace: bool) -> Result<String> {
        self.with_sync(move |redis| redis.function_load(&library_code, replace))
            .await
    }

    /// 列出函数库。
    pub async fn function_list(&self, library_name: Option<String>) -> Result<Vec<crate::RespValue>> {
        self.with_sync(move |redis| redis.function_list(library_name.as_deref()))
            .await
    }

    /// 删除函数库。
    pub async fn function_delete(&self, library_name: String) -> Result<()> {
        self.with_sync(move |redis| redis.function_delete(&library_name))
            .await
    }

    /// 导出 Prometheus 指标。
    pub async fn get_prometheus_metrics(&self) -> Result<String> {
        self.with_sync(|redis| redis.get_prometheus_metrics()).await
    }

    /// 普通队列。
    pub fn get_queue<V>(&self, topic: &str) -> AsyncRedisQueue<V>
    where
        V: FromRedisPayload + ToRedisPayload + Send + 'static,
    {
        AsyncRedisQueue::new(self.inner.get_queue::<V>(topic))
    }

    /// 可靠队列。
    pub fn get_reliable_queue<V>(&self, topic: &str) -> AsyncRedisReliableQueue<V>
    where
        V: FromRedisPayload + ToRedisPayload + Send + 'static,
    {
        AsyncRedisReliableQueue::new(self.inner.get_reliable_queue::<V>(topic))
    }

    /// 延迟队列。
    pub fn get_delay_queue<V>(&self, topic: &str) -> AsyncRedisDelayQueue<V>
    where
        V: FromRedisPayload + ToRedisPayload + Send + 'static,
    {
        AsyncRedisDelayQueue::new(self.inner.get_delay_queue::<V>(topic))
    }

    /// Stream 队列。
    pub fn get_stream(&self, topic: &str) -> AsyncRedisStream {
        AsyncRedisStream::new(self.inner.get_stream(topic))
    }

    /// 发布订阅。
    pub fn get_pubsub(&self, channel: &str) -> AsyncPubSub {
        AsyncPubSub::new(self.inner.get_pubsub(channel))
    }

    /// 哈希结构。
    pub fn get_hash<V>(&self, key: &str) -> AsyncRedisHash<V>
    where
        V: FromRedisPayload + Send + 'static,
    {
        AsyncRedisHash::new(self.inner.get_hash::<V>(key))
    }

    /// 哈希结构（自定义字段键类型）。
    pub fn get_hash_map<V, K>(&self, key: &str) -> AsyncRedisHash<V, K>
    where
        V: FromRedisPayload + Send + 'static,
        K: FromRedisPayload + Send + 'static,
    {
        AsyncRedisHash::new(self.inner.get_hash_map::<V, K>(key))
    }

    /// 列表结构。
    pub fn get_list<V>(&self, key: &str) -> AsyncRedisList<V>
    where
        V: FromRedisPayload + ToRedisPayload + Send + 'static,
    {
        AsyncRedisList::new(self.inner.get_list::<V>(key))
    }

    /// 集合结构。
    pub fn get_set<V>(&self, key: &str) -> AsyncRedisSet<V>
    where
        V: FromRedisPayload + ToRedisPayload + Send + 'static,
    {
        AsyncRedisSet::new(self.inner.get_set::<V>(key))
    }

    /// 有序集合。
    pub fn get_sorted_set<V>(&self, key: &str) -> AsyncRedisSortedSet<V>
    where
        V: FromRedisPayload + ToRedisPayload + Send + 'static,
    {
        AsyncRedisSortedSet::new(self.inner.get_sorted_set::<V>(key))
    }

    /// 地理位置结构。
    pub fn get_geo(&self, key: &str) -> AsyncRedisGeo {
        AsyncRedisGeo::new(self.inner.get_geo(key))
    }

    /// HyperLogLog 结构。
    pub fn get_hyper_log_log(&self, key: &str) -> AsyncHyperLogLog {
        AsyncHyperLogLog::new(self.inner.get_hyper_log_log(key))
    }

    /// 栈结构。
    pub fn get_stack<V>(&self, key: &str) -> AsyncRedisStack<V>
    where
        V: FromRedisPayload + ToRedisPayload + Send + 'static,
    {
        AsyncRedisStack::new(self.inner.get_stack::<V>(key))
    }
}

/// tokio 异步哈希结构。
#[derive(Clone)]
pub struct AsyncRedisHash<V, K = String> {
    inner: Arc<Mutex<RedisHash<V, K>>>,
}

impl<V, K> AsyncRedisHash<V, K>
where
    V: Send + 'static,
    K: Send + 'static,
{
    fn new(inner: RedisHash<V, K>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn with_sync<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut RedisHash<V, K>) -> Result<T> + Send + 'static,
    {
        spawn_locked(self.inner.clone(), f).await
    }

    pub async fn count(&self) -> Result<i64>
    where
        V: FromRedisPayload,
        K: FromRedisPayload,
    {
        spawn_locked(self.inner.clone(), |hash| hash.count()).await
    }

    pub async fn is_empty(&self) -> Result<bool>
    where
        V: FromRedisPayload,
        K: FromRedisPayload,
    {
        spawn_locked(self.inner.clone(), |hash| hash.is_empty()).await
    }

    pub async fn keys(&self) -> Result<Vec<K>>
    where
        V: FromRedisPayload,
        K: FromRedisPayload,
    {
        spawn_locked(self.inner.clone(), |hash| hash.keys()).await
    }

    pub async fn values(&self) -> Result<Vec<V>>
    where
        V: FromRedisPayload,
        K: FromRedisPayload,
    {
        spawn_locked(self.inner.clone(), |hash| hash.values()).await
    }

    pub async fn get(&self, field: K) -> Result<Option<V>>
    where
        V: FromRedisPayload,
        K: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.get(&field)).await
    }

    pub async fn set(&self, field: K, value: V) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
        K: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.set(&field, &value)).await
    }

    pub async fn contains_key(&self, field: K) -> Result<bool>
    where
        V: FromRedisPayload,
        K: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.contains_key(&field)).await
    }

    pub async fn remove(&self, fields: Vec<K>) -> Result<i64>
    where
        V: FromRedisPayload,
        K: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.remove(&fields)).await
    }

    pub async fn hgetdel(&self, field: K) -> Result<Option<V>>
    where
        V: FromRedisPayload,
        K: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.hgetdel(&field)).await
    }

    pub async fn hgetex(&self, field: K, expire_seconds: i64) -> Result<Option<V>>
    where
        V: FromRedisPayload,
        K: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.hgetex(&field, expire_seconds)).await
    }

    pub async fn get_all(&self) -> Result<Vec<(K, Option<V>)>>
    where
        V: FromRedisPayload,
        K: FromRedisPayload,
    {
        spawn_locked(self.inner.clone(), |hash| hash.get_all()).await
    }

    pub async fn get_all_map(&self) -> Result<HashMap<K, Option<V>>>
    where
        V: FromRedisPayload,
        K: FromRedisPayload + std::hash::Hash + Eq,
    {
        spawn_locked(self.inner.clone(), |hash| hash.get_all_map()).await
    }

    pub async fn hmget(&self, fields: Vec<K>) -> Result<Vec<Option<V>>>
    where
        V: FromRedisPayload,
        K: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.hmget(&fields)).await
    }

    pub async fn hmset(&self, pairs: Vec<(K, V)>) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
        K: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.hmset(&pairs)).await
    }

    pub async fn incr_by(&self, field: K, delta: i64) -> Result<i64>
    where
        V: FromRedisPayload,
        K: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.incr_by(&field, delta)).await
    }

    pub async fn incr_by_float(&self, field: K, delta: f64) -> Result<f64>
    where
        V: FromRedisPayload,
        K: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.incr_by_float(&field, delta)).await
    }

    pub async fn set_nx(&self, field: K, value: V) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
        K: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.set_nx(&field, &value)).await
    }

    pub async fn strlen(&self, field: K) -> Result<i64>
    where
        V: FromRedisPayload,
        K: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.strlen(&field)).await
    }

    pub async fn random_field(&self, count: i64, with_values: bool) -> Result<Vec<String>>
    where
        V: FromRedisPayload,
        K: FromRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.random_field(count, with_values)).await
    }

    pub async fn scan(&self, cursor: u64, pattern: String, count: usize) -> Result<(u64, Vec<(K, Option<V>)>)>
    where
        V: FromRedisPayload,
        K: FromRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.scan(cursor, &pattern, count)).await
    }

    pub async fn search(&self, pattern: String, count: usize) -> Result<Vec<(K, V)>>
    where
        V: FromRedisPayload,
        K: FromRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |hash| hash.search(&pattern, count)).await
    }
}

/// tokio 异步列表结构。
#[derive(Clone)]
pub struct AsyncRedisList<V> {
    inner: Arc<Mutex<RedisList<V>>>,
}

impl<V> AsyncRedisList<V>
where
    V: Send + 'static,
{
    fn new(inner: RedisList<V>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn with_sync<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut RedisList<V>) -> Result<T> + Send + 'static,
    {
        spawn_locked(self.inner.clone(), f).await
    }

    pub async fn len(&self) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), |list| list.len()).await
    }

    pub async fn is_empty(&self) -> Result<bool>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), |list| list.is_empty()).await
    }

    pub async fn push_back(&self, value: V) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.push_back(&value)).await
    }

    pub async fn push_front(&self, value: V) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.push_front(&value)).await
    }

    pub async fn push_back_many(&self, values: Vec<V>) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.push_back_many(&values)).await
    }

    pub async fn push_front_many(&self, values: Vec<V>) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.push_front_many(&values)).await
    }

    pub async fn pop_back(&self) -> Result<Option<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), |list| list.pop_back()).await
    }

    pub async fn pop_front(&self) -> Result<Option<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), |list| list.pop_front()).await
    }

    pub async fn pop_back_blocking(&self, timeout_seconds: i64) -> Result<Option<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.pop_back_blocking(timeout_seconds)).await
    }

    pub async fn pop_front_blocking(&self, timeout_seconds: i64) -> Result<Option<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.pop_front_blocking(timeout_seconds)).await
    }

    pub async fn get(&self, index: i64) -> Result<Option<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.get(index)).await
    }

    pub async fn set(&self, index: i64, value: V) -> Result<()>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.set(index, &value)).await
    }

    pub async fn range(&self, start: i64, end: i64) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.range(start, end)).await
    }

    pub async fn get_all(&self) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), |list| list.get_all()).await
    }

    pub async fn rpoplpush(&self, destination: String) -> Result<Option<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.rpoplpush(&destination)).await
    }

    pub async fn brpoplpush(&self, destination: String, timeout_seconds: i64) -> Result<Option<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.brpoplpush(&destination, timeout_seconds)).await
    }

    pub async fn insert_before(&self, pivot: V, value: V) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.insert_before(&pivot, &value)).await
    }

    pub async fn insert_after(&self, pivot: V, value: V) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.insert_after(&pivot, &value)).await
    }

    pub async fn remove(&self, count: i64, value: V) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.remove(count, &value)).await
    }

    pub async fn trim(&self, start: i64, end: i64) -> Result<()>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.trim(start, end)).await
    }

    pub async fn clear(&self) -> Result<()>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), |list| list.clear()).await
    }

    pub async fn position(&self, value: V) -> Result<Option<i64>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.position(&value)).await
    }

    pub async fn index_of(&self, value: V) -> Result<Option<i64>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.index_of(&value)).await
    }

    pub async fn insert_at(&self, index: i64, value: V) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.insert_at(index, &value)).await
    }

    pub async fn remove_at(&self, index: i64) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |list| list.remove_at(index)).await
    }

    pub async fn contains(&self, value: V) -> Result<bool>
    where
        V: FromRedisPayload + ToRedisPayload + PartialEq,
    {
        spawn_locked(self.inner.clone(), move |list| list.contains(&value)).await
    }
}

/// tokio 异步集合结构。
#[derive(Clone)]
pub struct AsyncRedisSet<V> {
    inner: Arc<Mutex<RedisSet<V>>>,
}

impl<V> AsyncRedisSet<V>
where
    V: Send + 'static,
{
    fn new(inner: RedisSet<V>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn with_sync<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut RedisSet<V>) -> Result<T> + Send + 'static,
    {
        spawn_locked(self.inner.clone(), f).await
    }

    pub async fn len(&self) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), |set| set.len()).await
    }

    pub async fn is_empty(&self) -> Result<bool>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), |set| set.is_empty()).await
    }

    pub async fn add(&self, members: Vec<V>) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.add(&members)).await
    }

    pub async fn remove(&self, members: Vec<V>) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.remove(&members)).await
    }

    pub async fn contains(&self, member: V) -> Result<bool>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.contains(&member)).await
    }

    pub async fn mismember(&self, members: Vec<V>) -> Result<Vec<bool>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.mismember(&members)).await
    }

    pub async fn members(&self) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), |set| set.members()).await
    }

    pub async fn pop(&self, count: i64) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.pop(count)).await
    }

    pub async fn random_get(&self, count: i64) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.random_get(count)).await
    }

    pub async fn move_to(&self, destination: String, member: V) -> Result<bool>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.move_to(&destination, &member)).await
    }

    pub async fn diff(&self, keys: Vec<String>) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            set.diff(&refs)
        })
        .await
    }

    pub async fn diff_store(&self, destination: String, keys: Vec<String>) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            set.diff_store(&destination, &refs)
        })
        .await
    }

    pub async fn inter(&self, keys: Vec<String>) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            set.inter(&refs)
        })
        .await
    }

    pub async fn inter_store(&self, destination: String, keys: Vec<String>) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            set.inter_store(&destination, &refs)
        })
        .await
    }

    pub async fn union(&self, keys: Vec<String>) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            set.union(&refs)
        })
        .await
    }

    pub async fn union_store(&self, destination: String, keys: Vec<String>) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            set.union_store(&destination, &refs)
        })
        .await
    }

    pub async fn scan_all(&self, pattern: String, count: usize) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.scan_all(&pattern, count)).await
    }
}

/// tokio 异步有序集合。
#[derive(Clone)]
pub struct AsyncRedisSortedSet<V> {
    inner: Arc<Mutex<RedisSortedSet<V>>>,
}

impl<V> AsyncRedisSortedSet<V>
where
    V: Send + 'static,
{
    fn new(inner: RedisSortedSet<V>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn with_sync<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut RedisSortedSet<V>) -> Result<T> + Send + 'static,
    {
        spawn_locked(self.inner.clone(), f).await
    }

    pub async fn len(&self) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), |set| set.len()).await
    }

    pub async fn is_empty(&self) -> Result<bool>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), |set| set.is_empty()).await
    }

    pub async fn add(&self, member: V, score: f64) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.add(&member, score)).await
    }

    pub async fn add_many(&self, members: Vec<V>, score: f64) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.add_many(&members, score)).await
    }

    pub async fn remove(&self, members: Vec<V>) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.remove(&members)).await
    }

    pub async fn score(&self, member: V) -> Result<Option<f64>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.score(&member)).await
    }

    pub async fn increment(&self, member: V, score: f64) -> Result<f64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.increment(&member, score)).await
    }

    pub async fn count(&self, min: f64, max: f64) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.count(min, max)).await
    }

    pub async fn range(&self, start: i64, stop: i64) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.range(start, stop)).await
    }

    pub async fn range_with_scores(&self, start: i64, stop: i64) -> Result<Vec<(V, f64)>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.range_with_scores(start, stop)).await
    }

    pub async fn range_by_score(&self, min: f64, max: f64, offset: i64, count: i64) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.range_by_score(min, max, offset, count)).await
    }

    pub async fn range_by_score_with_scores(
        &self,
        min: f64,
        max: f64,
        offset: i64,
        count: i64,
    ) -> Result<Vec<(V, f64)>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            set.range_by_score_with_scores(min, max, offset, count)
        })
        .await
    }

    pub async fn rank(&self, member: V) -> Result<Option<i64>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.rank(&member)).await
    }

    pub async fn rev_rank(&self, member: V) -> Result<Option<i64>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.rev_rank(&member)).await
    }

    pub async fn pop_min(&self, count: i64) -> Result<Vec<(V, f64)>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.pop_min(count)).await
    }

    pub async fn pop_max(&self, count: i64) -> Result<Vec<(V, f64)>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.pop_max(count)).await
    }

    pub async fn remove_range_by_rank(&self, start: i64, stop: i64) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.remove_range_by_rank(start, stop)).await
    }

    pub async fn remove_range_by_score(&self, min: f64, max: f64) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.remove_range_by_score(min, max)).await
    }

    pub async fn scan_all(&self, pattern: String, count: usize) -> Result<Vec<(V, f64)>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.scan_all(&pattern, count)).await
    }

    pub async fn add_with_options(&self, options: String, items: Vec<(f64, V)>) -> Result<f64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| set.add_with_options(&options, &items)).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn range_store(
        &self,
        destination: String,
        min: f64,
        max: f64,
        by_score: bool,
        rev: bool,
        offset: i64,
        count: i64,
    ) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            set.range_store(&destination, min, max, by_score, rev, offset, count)
        })
        .await
    }

    pub async fn diff(&self, keys: Vec<String>) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            set.diff(&refs)
        })
        .await
    }

    pub async fn diff_store(&self, destination: String, keys: Vec<String>) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            set.diff_store(&destination, &refs)
        })
        .await
    }

    pub async fn union(
        &self,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
    ) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            set.union(&refs, weights.as_deref(), aggregate.as_deref())
        })
        .await
    }

    pub async fn union_with_scores(
        &self,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
    ) -> Result<Vec<(V, f64)>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            set.union_with_scores(&refs, weights.as_deref(), aggregate.as_deref())
        })
        .await
    }

    pub async fn union_store(
        &self,
        destination: String,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
    ) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            set.union_store(&destination, &refs, weights.as_deref(), aggregate.as_deref())
        })
        .await
    }

    pub async fn inter(
        &self,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
    ) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            set.inter(&refs, weights.as_deref(), aggregate.as_deref())
        })
        .await
    }

    pub async fn inter_with_scores(
        &self,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
    ) -> Result<Vec<(V, f64)>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            set.inter_with_scores(&refs, weights.as_deref(), aggregate.as_deref())
        })
        .await
    }

    pub async fn inter_store(
        &self,
        destination: String,
        keys: Vec<String>,
        weights: Option<Vec<f64>>,
        aggregate: Option<String>,
    ) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |set| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            set.inter_store(&destination, &refs, weights.as_deref(), aggregate.as_deref())
        })
        .await
    }
}

/// tokio 异步栈结构。
#[derive(Clone)]
pub struct AsyncRedisStack<V> {
    inner: Arc<Mutex<RedisStack<V>>>,
}

impl<V> AsyncRedisStack<V>
where
    V: Send + 'static,
{
    fn new(inner: RedisStack<V>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn with_sync<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut RedisStack<V>) -> Result<T> + Send + 'static,
    {
        spawn_locked(self.inner.clone(), f).await
    }

    pub async fn len(&self) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), |stack| stack.len()).await
    }

    pub async fn is_empty(&self) -> Result<bool>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), |stack| stack.is_empty()).await
    }

    pub async fn push(&self, value: V) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |stack| stack.push(&value)).await
    }

    pub async fn push_many(&self, values: Vec<V>) -> Result<i64>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |stack| stack.push_many(&values)).await
    }

    pub async fn take_one(&self, timeout_seconds: i64) -> Result<Option<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |stack| stack.take_one(timeout_seconds)).await
    }

    pub async fn take(&self, count: usize) -> Result<Vec<V>>
    where
        V: FromRedisPayload + ToRedisPayload,
    {
        spawn_locked(self.inner.clone(), move |stack| stack.take(count)).await
    }
}

/// tokio 异步地理位置结构。
#[derive(Clone)]
pub struct AsyncRedisGeo {
    inner: Arc<Mutex<RedisGeo>>,
}

impl AsyncRedisGeo {
    fn new(inner: RedisGeo) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn with_sync<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut RedisGeo) -> Result<T> + Send + 'static,
    {
        spawn_locked(self.inner.clone(), f).await
    }

    pub async fn add(&self, name: String, longitude: f64, latitude: f64) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |geo| geo.add(&name, longitude, latitude)).await
    }

    pub async fn add_items(&self, items: Vec<GeoMember>) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |geo| geo.add_items(&items)).await
    }

    pub async fn distance(&self, from: String, to: String, unit: Option<String>) -> Result<Option<f64>> {
        spawn_locked(self.inner.clone(), move |geo| geo.distance(&from, &to, unit.as_deref())).await
    }

    pub async fn position(&self, members: Vec<String>) -> Result<Vec<Option<(f64, f64)>>> {
        spawn_locked(self.inner.clone(), move |geo| {
            let refs: Vec<&str> = members.iter().map(|member| member.as_str()).collect();
            geo.position(&refs)
        })
        .await
    }

    pub async fn geohash(&self, members: Vec<String>) -> Result<Vec<Option<String>>> {
        spawn_locked(self.inner.clone(), move |geo| {
            let refs: Vec<&str> = members.iter().map(|member| member.as_str()).collect();
            geo.geohash(&refs)
        })
        .await
    }

    pub async fn radius_by_coord(
        &self,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: String,
        count: i32,
    ) -> Result<Vec<GeoMember>> {
        spawn_locked(self.inner.clone(), move |geo| {
            geo.radius_by_coord(longitude, latitude, radius, &unit, count)
        })
        .await
    }

    pub async fn radius_by_member(
        &self,
        member: String,
        radius: f64,
        unit: String,
        count: i32,
    ) -> Result<Vec<GeoMember>> {
        spawn_locked(self.inner.clone(), move |geo| {
            geo.radius_by_member(&member, radius, &unit, count)
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn search(
        &self,
        member: Option<String>,
        longitude: Option<f64>,
        latitude: Option<f64>,
        radius: f64,
        unit: String,
        count: i32,
        ascending: bool,
    ) -> Result<Vec<GeoMember>> {
        spawn_locked(self.inner.clone(), move |geo| {
            geo.search(member.as_deref(), longitude, latitude, radius, &unit, count, ascending)
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn search_store(
        &self,
        destination: String,
        member: Option<String>,
        longitude: Option<f64>,
        latitude: Option<f64>,
        radius: f64,
        unit: String,
        count: i32,
        ascending: bool,
        store_distance: bool,
    ) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |geo| {
            geo.search_store(
                &destination,
                member.as_deref(),
                longitude,
                latitude,
                radius,
                &unit,
                count,
                ascending,
                store_distance,
            )
        })
        .await
    }
}

/// tokio 异步 HyperLogLog。
#[derive(Clone)]
pub struct AsyncHyperLogLog {
    inner: Arc<Mutex<HyperLogLog>>,
}

impl AsyncHyperLogLog {
    fn new(inner: HyperLogLog) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn with_sync<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut HyperLogLog) -> Result<T> + Send + 'static,
    {
        spawn_locked(self.inner.clone(), f).await
    }

    pub async fn add(&self, items: Vec<String>) -> Result<bool> {
        spawn_locked(self.inner.clone(), move |hll| {
            let refs: Vec<&str> = items.iter().map(|item| item.as_str()).collect();
            hll.add(&refs)
        })
        .await
    }

    pub async fn count(&self) -> Result<i64> {
        spawn_locked(self.inner.clone(), |hll| hll.count()).await
    }

    pub async fn merge(&self, keys: Vec<String>) -> Result<bool> {
        spawn_locked(self.inner.clone(), move |hll| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            hll.merge(&refs)
        })
        .await
    }
}

/// tokio 异步发布订阅。
#[derive(Clone)]
pub struct AsyncPubSub {
    inner: PubSub,
}

impl AsyncPubSub {
    fn new(inner: PubSub) -> Self {
        Self { inner }
    }

    pub async fn with_sync<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(PubSub) -> Result<T> + Send + 'static,
    {
        let pubsub = self.inner.clone();
        spawn_result(move || f(pubsub)).await
    }

    pub async fn publish(&self, message: String) -> Result<i64> {
        let pubsub = self.inner.clone();
        spawn_result(move || pubsub.publish(&message)).await
    }

    pub async fn publish_to(&self, channel: String, message: String) -> Result<i64> {
        let pubsub = self.inner.clone();
        spawn_result(move || pubsub.publish_to(&channel, &message)).await
    }

    pub async fn spublish(&self, message: String) -> Result<i64> {
        let pubsub = self.inner.clone();
        spawn_result(move || pubsub.spublish(&message)).await
    }

    pub async fn subscribe<F>(&self, cancel: Arc<AtomicBool>, on_message: F) -> Result<()>
    where
        F: FnMut(String, String) + Send + 'static,
    {
        let pubsub = self.inner.clone();
        spawn_result(move || {
            let mut on_message = on_message;
            pubsub.subscribe(cancel, move |channel, message| {
                on_message(channel.to_string(), message.to_string());
            })
        })
        .await
    }

    pub async fn psubscribe<F>(&self, cancel: Arc<AtomicBool>, on_message: F) -> Result<()>
    where
        F: FnMut(String, String, String) + Send + 'static,
    {
        let pubsub = self.inner.clone();
        spawn_result(move || {
            let mut on_message = on_message;
            pubsub.psubscribe(cancel, move |pattern, channel, message| {
                on_message(
                    pattern.to_string(),
                    channel.to_string(),
                    message.to_string(),
                );
            })
        })
        .await
    }

    pub async fn ssubscribe<F>(&self, cancel: Arc<AtomicBool>, on_message: F) -> Result<()>
    where
        F: FnMut(String, String) + Send + 'static,
    {
        let pubsub = self.inner.clone();
        spawn_result(move || {
            let mut on_message = on_message;
            pubsub.ssubscribe(cancel, move |channel, message| {
                on_message(channel.to_string(), message.to_string());
            })
        })
        .await
    }

    pub async fn pubsub_channels(&self, pattern: Option<String>) -> Result<Vec<String>> {
        let pubsub = self.inner.clone();
        spawn_result(move || pubsub.pubsub_channels(pattern.as_deref())).await
    }

    pub async fn pubsub_numsub(&self, channels: Vec<String>) -> Result<Vec<(String, i64)>> {
        let pubsub = self.inner.clone();
        spawn_result(move || {
            let refs: Vec<&str> = channels.iter().map(|channel| channel.as_str()).collect();
            pubsub.pubsub_numsub(&refs)
        })
        .await
    }

    pub async fn pubsub_numpat(&self) -> Result<i64> {
        let pubsub = self.inner.clone();
        spawn_result(move || pubsub.pubsub_numpat()).await
    }
}

/// tokio 异步普通队列。
#[derive(Clone)]
pub struct AsyncRedisQueue<V> {
    inner: Arc<Mutex<RedisQueue<V>>>,
}

impl<V> AsyncRedisQueue<V>
where
    V: FromRedisPayload + ToRedisPayload + Send + 'static,
{
    fn new(inner: RedisQueue<V>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn count(&self) -> Result<i64> {
        spawn_locked(self.inner.clone(), |queue| queue.count()).await
    }

    pub async fn is_empty(&self) -> Result<bool> {
        spawn_locked(self.inner.clone(), |queue| queue.is_empty()).await
    }

    pub async fn add(&self, value: V) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |queue| queue.add(&value)).await
    }

    pub async fn add_many(&self, values: Vec<V>) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |queue| queue.add_many(&values)).await
    }

    pub async fn take_one(&self, timeout_seconds: i64) -> Result<Option<V>> {
        spawn_locked(self.inner.clone(), move |queue| queue.take_one(timeout_seconds)).await
    }

    pub async fn take(&self, count: usize) -> Result<Vec<V>> {
        spawn_locked(self.inner.clone(), move |queue| queue.take(count)).await
    }
}

/// tokio 异步可靠队列。
#[derive(Clone)]
pub struct AsyncRedisReliableQueue<V> {
    inner: Arc<Mutex<RedisReliableQueue<V>>>,
}

impl<V> AsyncRedisReliableQueue<V>
where
    V: FromRedisPayload + ToRedisPayload + Send + 'static,
{
    fn new(inner: RedisReliableQueue<V>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn count(&self) -> Result<i64> {
        spawn_locked(self.inner.clone(), |queue| queue.count()).await
    }

    pub async fn add(&self, value: V) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |queue| queue.add(&value)).await
    }

    pub async fn add_many(&self, values: Vec<V>) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |queue| queue.add_many(&values)).await
    }

    pub async fn publish(&self, messages: Vec<(String, V)>, expire_seconds: i64) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |queue| {
            let borrowed: Vec<(&str, &V)> = messages.iter().map(|(key, value)| (key.as_str(), value)).collect();
            queue.publish(&borrowed, expire_seconds)
        })
        .await
    }

    pub async fn take_one(&self, timeout_seconds: i64) -> Result<Option<V>> {
        spawn_locked(self.inner.clone(), move |queue| queue.take_one(timeout_seconds)).await
    }

    pub async fn take(&self, count: usize) -> Result<Vec<V>> {
        spawn_locked(self.inner.clone(), move |queue| queue.take(count)).await
    }

    pub async fn acknowledge(&self, keys: Vec<String>) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |queue| {
            let refs: Vec<&str> = keys.iter().map(|key| key.as_str()).collect();
            queue.acknowledge(&refs)
        })
        .await
    }

    pub async fn take_ack(&self, count: usize) -> Result<Vec<String>> {
        spawn_locked(self.inner.clone(), move |queue| queue.take_ack(count)).await
    }

    pub async fn rollback_all_ack(&self) -> Result<i64> {
        spawn_locked(self.inner.clone(), |queue| queue.rollback_all_ack()).await
    }

    pub async fn add_delay(&self, value: V, delay_seconds: i64) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |queue| queue.add_delay(&value, delay_seconds)).await
    }
}

/// tokio 异步延迟队列。
#[derive(Clone)]
pub struct AsyncRedisDelayQueue<V> {
    inner: Arc<Mutex<RedisDelayQueue<V>>>,
}

impl<V> AsyncRedisDelayQueue<V>
where
    V: FromRedisPayload + ToRedisPayload + Send + 'static,
{
    fn new(inner: RedisDelayQueue<V>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn count(&self) -> Result<i64> {
        spawn_locked(self.inner.clone(), |queue| queue.count()).await
    }

    pub async fn add(&self, value: V, delay_seconds: i64) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |queue| queue.add(&value, delay_seconds)).await
    }

    pub async fn add_many(&self, values: Vec<V>) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |queue| queue.add_many(&values)).await
    }

    pub async fn remove(&self, value: V) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |queue| queue.remove(&value)).await
    }

    pub async fn take_due(&self, count: usize) -> Result<Vec<V>> {
        spawn_locked(self.inner.clone(), move |queue| queue.take_due(count)).await
    }

    pub async fn take_one(&self, timeout_seconds: i64) -> Result<Option<V>> {
        spawn_locked(self.inner.clone(), move |queue| queue.take_one(timeout_seconds)).await
    }
}

/// tokio 异步 Stream。
#[derive(Clone)]
pub struct AsyncRedisStream {
    inner: Arc<Mutex<RedisStream>>,
}

impl AsyncRedisStream {
    fn new(inner: RedisStream) -> Self {
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn set_group(&self, group: String) -> Result<bool> {
        spawn_locked(self.inner.clone(), move |stream| stream.set_group(&group)).await
    }

    pub async fn count(&self) -> Result<i64> {
        spawn_locked(self.inner.clone(), |stream| stream.count()).await
    }

    pub async fn is_empty(&self) -> Result<bool> {
        spawn_locked(self.inner.clone(), |stream| stream.is_empty()).await
    }

    pub async fn add<S>(&self, value: S, msg_id: Option<String>) -> Result<Option<String>>
    where
        S: Serialize + Send + 'static,
    {
        spawn_locked(self.inner.clone(), move |stream| stream.add(&value, msg_id.as_deref())).await
    }

    pub async fn take_messages(&self, count: usize, block_ms: i64) -> Result<Vec<Message>> {
        spawn_locked(self.inner.clone(), move |stream| stream.take_messages(count, block_ms)).await
    }

    pub async fn take_bodies<B>(&self, count: usize) -> Result<Vec<B>>
    where
        B: FromRedisPayload + Send + 'static,
    {
        spawn_locked(self.inner.clone(), move |stream| stream.take_bodies::<B>(count)).await
    }

    pub async fn take_structs<T>(&self, count: usize) -> Result<Vec<T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        spawn_locked(self.inner.clone(), move |stream| stream.take_structs::<T>(count)).await
    }

    pub async fn take_one_body<B>(&self) -> Result<Option<B>>
    where
        B: FromRedisPayload + Send + 'static,
    {
        spawn_locked(self.inner.clone(), |stream| stream.take_one_body::<B>()).await
    }

    pub async fn take_one_struct<T>(&self) -> Result<Option<T>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        spawn_locked(self.inner.clone(), |stream| stream.take_one_struct::<T>()).await
    }

    pub async fn acknowledge(&self, ids: Vec<String>) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |stream| {
            let refs: Vec<&str> = ids.iter().map(|id| id.as_str()).collect();
            stream.acknowledge(&refs)
        })
        .await
    }

    pub async fn take_message(&self) -> Result<Option<Message>> {
        spawn_locked(self.inner.clone(), |stream| stream.take_message()).await
    }

    pub async fn retry_ack(&self) -> Result<usize> {
        spawn_locked(self.inner.clone(), |stream| stream.retry_ack()).await
    }

    pub async fn delete(&self, id: String) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |stream| stream.delete(&id)).await
    }

    pub async fn trim(&self, max_len: i64, accurate: bool) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |stream| stream.trim(max_len, accurate)).await
    }

    pub async fn trim_before(&self, time: NaiveDateTime) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |stream| stream.trim_before(time)).await
    }

    pub async fn range(
        &self,
        start_id: Option<String>,
        end_id: Option<String>,
        count: i64,
    ) -> Result<Vec<Message>> {
        spawn_locked(self.inner.clone(), move |stream| {
            stream.range(start_id.as_deref(), end_id.as_deref(), count)
        })
        .await
    }

    pub async fn range_time(
        &self,
        start: NaiveDateTime,
        end: NaiveDateTime,
        count: i64,
    ) -> Result<Vec<Message>> {
        spawn_locked(self.inner.clone(), move |stream| stream.range_time(start, end, count)).await
    }

    pub async fn read(&self, start_id: String, count: usize, block_ms: i64) -> Result<Vec<Message>> {
        spawn_locked(self.inner.clone(), move |stream| stream.read(&start_id, count, block_ms)).await
    }

    pub async fn read_group(
        &self,
        group: String,
        consumer: String,
        count: usize,
        block_ms: i64,
        id: Option<String>,
    ) -> Result<Vec<Message>> {
        spawn_locked(self.inner.clone(), move |stream| {
            stream.read_group(&group, &consumer, count, block_ms, id.as_deref())
        })
        .await
    }

    pub async fn pending_info(&self, group: String) -> Result<Option<PendingInfo>> {
        spawn_locked(self.inner.clone(), move |stream| stream.pending_info(&group)).await
    }

    pub async fn pending(
        &self,
        group: String,
        start_id: Option<String>,
        end_id: Option<String>,
        count: i64,
    ) -> Result<Vec<PendingItem>> {
        spawn_locked(self.inner.clone(), move |stream| {
            stream.pending(&group, start_id.as_deref(), end_id.as_deref(), count)
        })
        .await
    }

    pub async fn claim(
        &self,
        group: String,
        consumer: String,
        id: String,
        ms_idle: i64,
    ) -> Result<crate::RespValue> {
        spawn_locked(self.inner.clone(), move |stream| {
            stream.claim(&group, &consumer, &id, ms_idle)
        })
        .await
    }

    pub async fn group_create(&self, group: String, start_id: Option<String>) -> Result<bool> {
        spawn_locked(self.inner.clone(), move |stream| stream.group_create(&group, start_id.as_deref())).await
    }

    pub async fn group_destroy(&self, group: String) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |stream| stream.group_destroy(&group)).await
    }

    pub async fn group_delete_consumer(&self, group: String, consumer: String) -> Result<i64> {
        spawn_locked(self.inner.clone(), move |stream| {
            stream.group_delete_consumer(&group, &consumer)
        })
        .await
    }

    pub async fn group_set_id(&self, group: String, start_id: String) -> Result<bool> {
        spawn_locked(self.inner.clone(), move |stream| stream.group_set_id(&group, &start_id)).await
    }

    pub async fn get_info(&self) -> Result<Option<StreamInfo>> {
        spawn_locked(self.inner.clone(), |stream| stream.get_info()).await
    }

    pub async fn get_groups(&self) -> Result<Vec<GroupInfo>> {
        spawn_locked(self.inner.clone(), |stream| stream.get_groups()).await
    }

    pub async fn get_consumers(&self, group: String) -> Result<Vec<ConsumerInfo>> {
        spawn_locked(self.inner.clone(), move |stream| stream.get_consumers(&group)).await
    }
}

