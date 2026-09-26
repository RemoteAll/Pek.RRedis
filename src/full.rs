//! 增强版 Redis（对应 DH.NRedis 的 `FullRedis`）。
//!
//! 在基础 [`Redis`] 之上提供：
//! - **键前缀** `Prefix` / `get_key`：与 C# `EnsureStart` 一致（已有前缀则不重复添加，比较不区分大小写）；
//! - **SCAN 模糊搜索** [`FullRedis::search`]（替代危险的 `KEYS`）；
//! - **数据结构工厂**：[`FullRedis::get_hash`]、[`FullRedis::get_list`]、[`FullRedis::get_set`]、
//!   [`FullRedis::get_sorted_set`]、[`FullRedis::get_geo`]、[`FullRedis::get_hyper_log_log`]、
//!   [`FullRedis::get_stack`]，以及队列工厂 [`FullRedis::get_queue`]、[`FullRedis::get_reliable_queue`]、
//!   [`FullRedis::get_delay_queue`]；
//! - **分布式锁** [`FullRedis::acquire_lock`]：锁值格式与 C# `CacheLock` 完全一致
//!   （`{token}|{绝对过期毫秒}` + 秒级 TTL + GETSET 抢占超时锁），C# 与 Rust 可互相抢锁、互不误删；
//! - **脚本** [`FullRedis::eval`] 与 **子库** [`FullRedis::create_sub`]。

use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::options::RedisOptions;
use crate::redis::{Redis, ServerType};
use crate::resp::RespValue;
use crate::util::{decode, decode_array, decode_scored, decode_scored_pairs, int_or};

pub use crate::encoder::{FromRedisPayload, ToRedisPayload};
use crate::geo::RedisGeo;
use crate::hash::RedisHash;
use crate::hyperloglog::HyperLogLog;
use crate::list::RedisList;
use crate::pubsub::PubSub;
use crate::queues::{RedisDelayQueue, RedisQueue, RedisReliableQueue, RedisStream};
use crate::set::RedisSet;
use crate::sortedset::RedisSortedSet;
use crate::stack::RedisStack;

/// 增强版 Redis。克隆共享同一连接池。
#[derive(Clone)]
pub struct FullRedis {
    redis: Redis,
    prefix: Option<String>,
}

impl std::fmt::Debug for FullRedis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FullRedis")
            .field("servers", &self.redis.options().servers)
            .field("db", &self.redis.options().db)
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl FullRedis {
    /// 使用选项创建。
    pub fn new(options: RedisOptions) -> Result<Self> {
        let prefix = options.prefix.clone().filter(|p| !p.is_empty());
        Ok(Self {
            redis: Redis::new(options)?,
            prefix,
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

    /// 基础客户端。
    pub fn redis(&self) -> &Redis {
        &self.redis
    }

    /// 键前缀。
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// 从共享句柄构造（用于从 [`Redis`] 提升）。
    pub fn with_prefix(redis: Redis, prefix: Option<String>) -> Self {
        Self {
            redis,
            prefix: prefix.filter(|p| !p.is_empty()),
        }
    }

    /// 键前缀处理：未以前缀开头时拼接前缀（不区分大小写，与 C# `EnsureStart` 一致）。
    pub fn get_key(&self, key: &str) -> String {
        match &self.prefix {
            None => key.to_string(),
            Some(prefix) => {
                if key.is_empty() {
                    return prefix.clone();
                }
                if key
                    .get(..prefix.len())
                    .map(|h| h.eq_ignore_ascii_case(prefix))
                    .unwrap_or(false)
                {
                    key.to_string()
                } else {
                    format!("{prefix}{key}")
                }
            }
        }
    }

    /// 去掉键前缀（[`FullRedis::search`] 等返回值还原）。
    pub fn trim_key<'k>(&self, key: &'k str) -> &'k str {
        match &self.prefix {
            Some(prefix) if key.len() >= prefix.len() && key[..prefix.len()].eq_ignore_ascii_case(prefix) => {
                &key[prefix.len()..]
            }
            _ => key,
        }
    }

    /// 为同一服务器创建不同库的子级实例（对应 C# `CreateSub(db)`），保留前缀。
    pub fn create_sub(&self, db: i32) -> Result<Self> {
        let mut options = self.redis.options().clone();
        options.db = db;
        options.prefix = self.prefix.clone();
        Self::new(options)
    }

    // ================== 搜索 ==================

    /// 分页扫描键空间（`SCAN MATCH`），返回 `(下一个游标, 键列表)`。
    ///
    /// 返回的键已去掉前缀。
    pub fn search_paged(
        &self,
        pattern: &str,
        cursor: u64,
        count: usize,
    ) -> Result<(u64, Vec<String>)> {
        let full_pattern = self.get_key(pattern);
        let (next, keys) = self.redis.scan(cursor, &full_pattern, count)?;
        Ok((next, keys))
    }

    /// 模糊搜索（`SCAN MATCH`），`count <= 0` 表示扫描全部。
    ///
    /// 对应 C# `FullRedis.Search(pattern, offset, count)`，但修正了 C# 中 `count<=0` 时
    /// 不返回任何结果的边界行为（此处按“不限量”处理）。
    /// 返回的键**去掉了前缀**，便于与业务键名直接比较。
    pub fn search(&self, pattern: &str, count: usize) -> Result<Vec<String>> {
        let mut result = Vec::new();
        let mut cursor = 0u64;
        let batch = if count == 0 { 1000 } else { count.max(10) };

        loop {
            let (next, keys) = self.search_paged(pattern, cursor, batch)?;
            for key in keys {
                result.push(self.trim_key(&key).to_string());
                if count > 0 && result.len() >= count {
                    return Ok(result);
                }
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }

        Ok(result)
    }

    /// 按模式删除（`SCAN` + 分批 `DEL`），返回删除数量。
    pub fn remove_pattern(&self, pattern: &str) -> Result<i64> {
        let keys = self.search(pattern, 0)?;
        if keys.is_empty() {
            return Ok(0);
        }

        let mut removed = 0;
        for chunk in keys.chunks(100) {
            let full: Vec<String> = chunk.iter().map(|k| self.get_key(k)).collect();
            let refs: Vec<&str> = full.iter().map(|s| s.as_str()).collect();
            removed += self.redis.remove_many(&refs)?;
        }
        Ok(removed)
    }

    /// 批量删除（自动补前缀）。
    pub fn remove_many(&self, keys: &[&str]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }
        if keys.len() == 1 {
            return self.redis.remove(&self.get_key(keys[0]));
        }
        let full: Vec<String> = keys.iter().map(|k| self.get_key(k)).collect();
        let refs: Vec<&str> = full.iter().map(|s| s.as_str()).collect();
        self.redis.remove_many(&refs)
    }

    /// 批量获取（自动补前缀），键为传入的原始键名。
    pub fn get_all<V: FromRedisPayload>(&self, keys: &[&str]) -> Result<HashMap<String, V>> {
        let full: Vec<String> = keys.iter().map(|k| self.get_key(k)).collect();
        let refs: Vec<&str> = full.iter().map(|s| s.as_str()).collect();
        let values = self.redis.get_all_raw(&refs)?;

        let mut dic = HashMap::with_capacity(keys.len());
        for (key, raw) in keys.iter().zip(values) {
            if let Some(bytes) = raw
                && let Ok(v) = V::from_redis_payload(&bytes) {
                    dic.insert((*key).to_string(), v);
                }
        }
        Ok(dic)
    }

    // ================== 脚本 ==================

    /// 执行 Lua 脚本（`EVAL`）。
    ///
    /// 与 C# 一致：脚本内的 KEYS 不会自动补前缀，调用方需自行用 [`FullRedis::get_key`] 处理，
    /// 或使用 [`FullRedis::eval_prefixed`]。
    pub fn eval<T: FromRedisPayload>(
        &self,
        script: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<Option<T>> {
        self.redis.eval(script, keys, args)
    }

    /// 执行 Lua 脚本，自动为 KEYS 补前缀。
    pub fn eval_prefixed<T: FromRedisPayload>(
        &self,
        script: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<Option<T>> {
        let full: Vec<String> = keys.iter().map(|k| self.get_key(k)).collect();
        let refs: Vec<&str> = full.iter().map(|s| s.as_str()).collect();
        self.redis.eval(script, &refs, args)
    }

    /// 执行脚本并返回原始应答。
    pub fn eval_raw(&self, script: &str, keys: &[&str], args: &[&str]) -> Result<RespValue> {
        self.redis.eval_raw(script, keys, args)
    }

    // ================== 分布式锁 ==================

    /// 申请分布式锁（默认阻塞等待，失败时报错），对应 C# `cache.AcquireLock(key, msTimeout)`。
    pub fn acquire_lock(&self, key: &str, ms_timeout: i32) -> Result<LockHandle> {
        match self.acquire_lock_ex(key, ms_timeout, ms_timeout, true)? {
            Some(lock) => Ok(lock),
            None => Err(Error::Operation(format!("Lock [{key}] failed! msTimeout={ms_timeout}"))),
        }
    }

    /// 申请分布式锁（完整参数），对应 C# `cache.AcquireLock(key, msTimeout, msExpire, throwOnFailure)`。
    ///
    /// 锁值格式与 C# `CacheLock` 一致：`{token}|{绝对过期毫秒}`，TTL 为 `ceil(msExpire/1000)` 秒。
    pub fn acquire_lock_ex(
        &self,
        key: &str,
        ms_timeout: i32,
        ms_expire: i32,
        throw_on_failure: bool,
    ) -> Result<Option<LockHandle>> {
        let key = self.get_key(key);
        let now = boot_ticks_ms();

        // 令牌：32 位十六进制（等价 C# Guid.NewGuid().ToString("N")）
        let token = new_token();
        let expire_seconds = if ms_expire > 0 {
            ((ms_expire as i64) + 999) / 1000
        } else {
            0
        };

        let mut current = now;
        let end = now + ms_timeout.max(0) as u64;

        loop {
            let value = format!("{token}|{}", current + ms_expire.max(0) as u64);

            // 申请加锁
            if self.redis.add(&key, value.as_str(), expire_seconds)? {
                return Ok(Some(LockHandle {
                    redis: self.clone(),
                    key,
                    token,
                    has_lock: true,
                }));
            }

            // 死锁超期检测与抢占
            let dt = parse_lock_expire(self.redis.get_string(&key)?);
            if dt <= current {
                let old = self
                    .redis
                    .replace::<String>(&key, value.clone())?
                    .map(|s| parse_lock_expire(Some(s)))
                    .unwrap_or(0);
                if old <= dt {
                    self.redis.set_expire(&key, expire_seconds)?;
                    return Ok(Some(LockHandle {
                        redis: self.clone(),
                        key,
                        token,
                        has_lock: true,
                    }));
                }
            }

            // 超时退出
            current = boot_ticks_ms();
            if current >= end {
                break;
            }

            sleep(Duration::from_millis(200));
        }

        if throw_on_failure {
            Err(Error::Operation(format!(
                "Lock [{key}] failed! msTimeout={ms_timeout}"
            )))
        } else {
            Ok(None)
        }
    }

    // ================== 数据结构工厂 ==================

    /// 哈希结构（对应 C# `GetDictionary<T>(key)` / `new RedisHash<String,T>`）。
    pub fn get_hash<V: FromRedisPayload>(&self, key: &str) -> RedisHash<V> {
        RedisHash::new(self.clone(), key)
    }

    /// 哈希结构（自定义字段键类型）。
    pub fn get_hash_map<V: FromRedisPayload, K: FromRedisPayload>(
        &self,
        key: &str,
    ) -> RedisHash<V, K> {
        RedisHash::new(self.clone(), key)
    }

    /// 列表结构（对应 C# `GetList<T>(key)`）。
    pub fn get_list<V: FromRedisPayload + ToRedisPayload>(&self, key: &str) -> RedisList<V> {
        RedisList::new(self.clone(), key)
    }

    /// 集合结构（对应 C# `GetSet<T>(key)`）。
    pub fn get_set<V: FromRedisPayload + ToRedisPayload>(&self, key: &str) -> RedisSet<V> {
        RedisSet::new(self.clone(), key)
    }

    /// 有序集合（对应 C# `GetSortedSet<T>(key)`）。
    pub fn get_sorted_set<V: FromRedisPayload + ToRedisPayload>(
        &self,
        key: &str,
    ) -> RedisSortedSet<V> {
        RedisSortedSet::new(self.clone(), key)
    }

    /// 地理位置结构（对应 C# `new RedisGeo(redis, key)`）。
    pub fn get_geo(&self, key: &str) -> RedisGeo {
        RedisGeo::new(self.clone(), key)
    }

    /// 基数统计结构（对应 C# `new HyperLogLog(redis, key)`）。
    pub fn get_hyper_log_log(&self, key: &str) -> HyperLogLog {
        HyperLogLog::new(self.clone(), key)
    }

    /// 栈结构（对应 C# `GetStack<T>(key)`，右进右出）。
    pub fn get_stack<V: FromRedisPayload + ToRedisPayload>(&self, key: &str) -> RedisStack<V> {
        RedisStack::new(self.clone(), key)
    }

    /// 发布订阅（对应 C# `new PubSub(redis, key)`）。
    pub fn get_pubsub(&self, channel: &str) -> PubSub {
        PubSub::new(self.clone(), channel)
    }

    /// 普通队列（对应 C# `GetQueue<T>(topic)`，左进右出，非可靠）。
    pub fn get_queue<V: FromRedisPayload + ToRedisPayload>(&self, topic: &str) -> RedisQueue<V> {
        RedisQueue::new(self.clone(), topic)
    }

    /// 可靠队列（对应 C# `GetReliableQueue<T>(topic)`，消费弹到 Ack 队列，处理成功后确认）。
    pub fn get_reliable_queue<V: FromRedisPayload + ToRedisPayload>(
        &self,
        topic: &str,
    ) -> RedisReliableQueue<V> {
        RedisReliableQueue::new(self.clone(), topic)
    }

    /// 延迟队列（对应 C# `GetDelayQueue<T>(topic)`，ZSET 按到期时间排序）。
    pub fn get_delay_queue<V: FromRedisPayload + ToRedisPayload>(
        &self,
        topic: &str,
    ) -> RedisDelayQueue<V> {
        RedisDelayQueue::new(self.clone(), topic)
    }

    /// Stream 消息队列（对应 C# `GetStream<T>(topic)`，Redis 5.0+）。
    ///
    /// Rust 侧为非泛型：生产用 [`RedisStream::add`]（接受任意 `Serialize`），
    /// 消费用 [`RedisStream::take_bodies`] / [`RedisStream::take_structs`] 指定类型。
    pub fn get_stream(&self, topic: &str) -> RedisStream {
        RedisStream::new(self.clone(), topic)
    }

    // ================== 服务器信息（对齐 C# Redis 属性） ==================

    /// `INFO all` 全部信息段（对应 C# `GetInfo(all: true)`）。
    pub fn info_all(&self) -> Result<HashMap<String, String>> {
        self.redis.info_all()
    }

    /// 服务器版本号 `(major, minor, patch)`（对应 C# `Redis.Version`）。
    pub fn version_parts(&self) -> Result<(u32, u32, u32)> {
        self.redis.version_parts()
    }

    /// 服务器类型（对应 C# `Redis.ServerType`）。
    pub fn server_type(&self) -> Result<ServerType> {
        self.redis.server_type()
    }

    // ================== 基础字符串/键命令（含前缀，对齐 C# FullRedis 覆写） ==================

    /// `SET`（对应 C# `FullRedis.Set`），键自动补前缀。
    pub fn set<V: ToRedisPayload>(&self, key: &str, value: V, expire_seconds: i64) -> Result<bool> {
        self.redis.set(self.get_key(key), value, expire_seconds)
    }

    /// `SET ... NX`（对应 C# `FullRedis.Add`）。
    pub fn add<V: ToRedisPayload>(&self, key: &str, value: V, expire_seconds: i64) -> Result<bool> {
        self.redis.add(self.get_key(key), value, expire_seconds)
    }

    /// `GETSET`（对应 C# `FullRedis.Replace<T>`），返回旧值。
    pub fn replace<V: FromRedisPayload + ToRedisPayload>(
        &self,
        key: &str,
        value: V,
    ) -> Result<Option<V>> {
        self.redis.replace(&self.get_key(key), value)
    }

    /// `GET` 解码为指定类型（对应 C# `FullRedis.Get<T>`）。
    pub fn get<V: FromRedisPayload>(&self, key: &str) -> Result<Option<V>> {
        self.redis.get(&self.get_key(key))
    }

    /// `GET` 原始字符串（对应 C# `FullRedis.Get<String>`）。
    pub fn get_string(&self, key: &str) -> Result<Option<String>> {
        self.redis.get_string(&self.get_key(key))
    }

    /// `DEL`（对应 C# `FullRedis.Remove`）。
    pub fn remove(&self, key: &str) -> Result<i64> {
        self.redis.remove(&self.get_key(key))
    }

    /// `EXISTS`（对应 C# `FullRedis.ContainsKey`）。
    pub fn contains_key(&self, key: &str) -> Result<bool> {
        self.redis.contains_key(&self.get_key(key))
    }

    /// `EXPIRE`（对应 C# `FullRedis.SetExpire`）。
    pub fn set_expire(&self, key: &str, seconds: i64) -> Result<bool> {
        self.redis.set_expire(&self.get_key(key), seconds)
    }

    /// `TTL`（对应 C# `FullRedis.GetExpire`），-1 永不过期，-2 不存在。
    pub fn get_expire(&self, key: &str) -> Result<i64> {
        self.redis.get_expire(&self.get_key(key))
    }

    /// `APPEND`（对应 C# `FullRedis.Append`）。
    pub fn append(&self, key: &str, value: &str) -> Result<i64> {
        self.redis.append(&self.get_key(key), value)
    }

    /// 字符串长度（`STRLEN`，对应 C# `FullRedis.StrLen`）。
    pub fn strlen(&self, key: &str) -> Result<i64> {
        self.redis.strlen(&self.get_key(key))
    }

    /// 子串（`GETRANGE`，对应 C# `FullRedis.GetRange`）。
    pub fn get_range(&self, key: &str, start: i64, end: i64) -> Result<String> {
        self.redis.get_range(&self.get_key(key), start, end)
    }

    /// 覆盖子串（`SETRANGE`，对应 C# `FullRedis.SetRange`）。
    pub fn set_range(&self, key: &str, offset: i64, value: &str) -> Result<i64> {
        self.redis.set_range(&self.get_key(key), offset, value)
    }

    /// 设置位（`SETBIT`，对应 C# `FullRedis.SetBit`）。
    pub fn set_bit(&self, key: &str, offset: u64, value: u8) -> Result<i64> {
        self.redis.set_bit(&self.get_key(key), offset, value)
    }

    /// 读取位（`GETBIT`，对应 C# `FullRedis.GetBit`）。
    pub fn get_bit(&self, key: &str, offset: u64) -> Result<i64> {
        self.redis.get_bit(&self.get_key(key), offset)
    }

    /// 位计数（`BITCOUNT`，对应 C# `FullRedis.BitCount`）。
    pub fn bit_count(&self, key: &str, start: i64, end: i64) -> Result<i64> {
        self.redis.bit_count(&self.get_key(key), start, end)
    }

    /// 首个置位/清零位（`BITPOS`，对应 C# `FullRedis.BitPos`）。
    pub fn bit_pos(&self, key: &str, bit: i32, start: i64, end: i64) -> Result<i64> {
        self.redis.bit_pos(&self.get_key(key), bit, start, end)
    }

    /// 整数自增（`INCRBY`，对应 C# `FullRedis.Increment`）。
    pub fn increment(&self, key: &str, delta: i64) -> Result<i64> {
        self.redis.increment(&self.get_key(key), delta)
    }

    /// 浮点自增（`INCRBYFLOAT`，对应 C# `FullRedis.Increment(Double)`）。
    pub fn increment_float(&self, key: &str, delta: f64) -> Result<f64> {
        self.redis.increment_float(&self.get_key(key), delta)
    }

    /// 整数自减（对应 C# `FullRedis.Decrement`）。
    pub fn decrement(&self, key: &str, delta: i64) -> Result<i64> {
        self.redis.decrement(&self.get_key(key), delta)
    }

    /// 键类型（`TYPE`，对应 C# `FullRedis.TYPE`），不存在返回 `None`。
    pub fn type_of(&self, key: &str) -> Result<Option<String>> {
        self.redis.type_of(&self.get_key(key))
    }

    /// 重命名（`RENAME`，目标键自动补前缀）。
    pub fn rename(&self, key: &str, new_key: &str) -> Result<bool> {
        self.redis.rename(&self.get_key(key), &self.get_key(new_key), true)
    }

    /// 异步删除（`UNLINK`，键自动补前缀）。
    pub fn unlink(&self, keys: &[&str]) -> Result<i64> {
        let keys: Vec<String> = keys.iter().map(|k| self.get_key(k)).collect();
        let refs: Vec<&str> = keys.iter().map(|k| k.as_str()).collect();
        self.redis.unlink(&refs)
    }

    /// 刷新访问时间（`TOUCH`，键自动补前缀）。
    pub fn touch(&self, keys: &[&str]) -> Result<i64> {
        let keys: Vec<String> = keys.iter().map(|k| self.get_key(k)).collect();
        let refs: Vec<&str> = keys.iter().map(|k| k.as_str()).collect();
        self.redis.touch(&refs)
    }

    /// 随机键（`RANDOMKEY`，无前缀语义，对应 C# `Redis.RandomKey`）。
    pub fn random_key(&self) -> Result<Option<String>> {
        self.redis.random_key()
    }

    /// 当前库键数量（`DBSIZE`，对应 C# `Redis.Count`）。
    pub fn count(&self) -> Result<i64> {
        self.redis.dbsize()
    }

    /// 列表右弹（对应 C# `FullRedis.RPOP<T>`）。
    pub fn rpop<V: FromRedisPayload + ToRedisPayload>(&self, key: &str) -> Result<Option<V>> {
        self.get_list::<V>(key).pop_back()
    }

    /// 列表左弹（对应 C# `FullRedis.LPOP<T>`）。
    pub fn lpop<V: FromRedisPayload + ToRedisPayload>(&self, key: &str) -> Result<Option<V>> {
        self.get_list::<V>(key).pop_front()
    }

    /// 右弹并左推（`RPOPLPUSH`，对应 C# `FullRedis.RPOPLPUSH<T>`）。
    pub fn rpoplpush<V: FromRedisPayload + ToRedisPayload>(
        &self,
        source: &str,
        destination: &str,
    ) -> Result<Option<V>> {
        self.get_list::<V>(source).rpoplpush(destination)
    }

    /// 阻塞版 `RPOPLPUSH`（对应 C# `FullRedis.BRPOPLPUSH<T>`）。
    pub fn brpoplpush<V: FromRedisPayload + ToRedisPayload>(
        &self,
        source: &str,
        destination: &str,
        timeout_seconds: i64,
    ) -> Result<Option<V>> {
        self.get_list::<V>(source).brpoplpush(destination, timeout_seconds)
    }

    /// 集合全部成员（`SMEMBERS`，对应 C# `FullRedis.SMEMBERS<T>`）。
    pub fn smembers<V: FromRedisPayload + ToRedisPayload>(&self, key: &str) -> Result<Vec<V>> {
        self.get_set::<V>(key).members()
    }

    /// 集合基数（`SCARD`，对应 C# `FullRedis.SCARD`）。
    pub fn scard(&self, key: &str) -> Result<i64> {
        self.get_set::<String>(key).len()
    }

    /// 集合成员判断（`SISMEMBER`，对应 C# `FullRedis.SISMEMBER<T>`）。
    pub fn sismember<V: FromRedisPayload + ToRedisPayload>(
        &self,
        key: &str,
        member: &V,
    ) -> Result<bool> {
        self.get_set::<V>(key).contains(member)
    }

    /// 集合移动（`SMOVE`，对应 C# `FullRedis.SMOVE`）。
    pub fn smove<V: FromRedisPayload + ToRedisPayload>(
        &self,
        source: &str,
        destination: &str,
        member: &V,
    ) -> Result<bool> {
        self.get_set::<V>(source).move_to(destination, member)
    }

    /// 集合随机成员（`SRANDMEMBER`，对应 C# `FullRedis.SRANDMEMBER<T>`）。
    pub fn srandmember<V: FromRedisPayload + ToRedisPayload>(
        &self,
        key: &str,
        count: i64,
    ) -> Result<Vec<V>> {
        self.get_set::<V>(key).random_get(count)
    }

    /// 集合随机弹出（`SPOP`，对应 C# `FullRedis.SPOP<T>`）。
    pub fn spop<V: FromRedisPayload + ToRedisPayload>(
        &self,
        key: &str,
        count: i64,
    ) -> Result<Vec<V>> {
        self.get_set::<V>(key).pop(count)
    }

    // ================== 键命令扩展（对齐 C# FullRedis） ==================

    /// 获取值并设置/移除过期时间（`GETEX`，对应 C# `GetEx<T>`）。
    ///
    /// - `expire > 0`：`EX` 秒级过期
    /// - `expire < 0`：`PX` 毫秒级过期（取绝对值）
    /// - `expire = 0`：`PERSIST` 移除过期时间
    pub fn get_ex<V: FromRedisPayload>(&self, key: &str, expire: i32) -> Result<Option<V>> {
        let key = self.get_key(key);
        let rs = if expire > 0 {
            self.redis.execute(&[
                b"GETEX",
                key.as_bytes(),
                b"EX",
                expire.to_string().as_bytes(),
            ])?
        } else if expire < 0 {
            self.redis.execute(&[
                b"GETEX",
                key.as_bytes(),
                b"PX",
                (-(expire as i64)).to_string().as_bytes(),
            ])?
        } else {
            self.redis
                .execute(&[b"GETEX", key.as_bytes(), b"PERSIST"])?
        };
        Ok(decode(rs))
    }

    /// 键过期时间戳（秒级，`EXPIRETIME`，Redis 7.0+）。-1 永不过期，-2 不存在。
    pub fn expire_time(&self, key: &str) -> Result<i64> {
        self.redis.require_version("7.0", "EXPIRETIME")?;
        let key = self.get_key(key);
        Ok(self
            .redis
            .execute(&[b"EXPIRETIME", key.as_bytes()])?
            .as_i64()
            .unwrap_or(-2))
    }

    /// 键过期时间戳（毫秒级，`PEXPIRETIME`，Redis 7.0+）。-1 永不过期，-2 不存在。
    pub fn pexpire_time(&self, key: &str) -> Result<i64> {
        self.redis.require_version("7.0", "PEXPIRETIME")?;
        let key = self.get_key(key);
        Ok(self
            .redis
            .execute(&[b"PEXPIRETIME", key.as_bytes()])?
            .as_i64()
            .unwrap_or(-2))
    }

    /// 键空闲时间秒数（`OBJECT IDLETIME`），键不存在或未启用 LRU 时返回 `None`。
    pub fn object_idle_time(&self, key: &str) -> Result<Option<i64>> {
        let key = self.get_key(key);
        Ok(self
            .redis
            .execute(&[b"OBJECT", b"IDLETIME", key.as_bytes()])?
            .as_i64())
    }

    /// 键访问频率（`OBJECT FREQ`，需 maxmemory-policy 为 LFU），否则 `None`。
    pub fn object_freq(&self, key: &str) -> Result<Option<i64>> {
        let key = self.get_key(key);
        Ok(self
            .redis
            .execute(&[b"OBJECT", b"FREQ", key.as_bytes()])?
            .as_i64())
    }

    /// 位域批量操作（`BITFIELD`，对应 C# `BitField(key, args...)`）。
    ///
    /// 参数原样透传，如 `["SET", "u8", "0", "255", "GET", "u8", "0"]`；
    /// 返回每个子命令的结果（`OVERFLOW FAIL` 时对应位置为 0）。
    pub fn bit_field(&self, key: &str, args: &[&str]) -> Result<Vec<i64>> {
        let key = self.get_key(key);
        let mut argv: Vec<Vec<u8>> = Vec::with_capacity(args.len() + 2);
        argv.push(b"BITFIELD".to_vec());
        argv.push(key.into_bytes());
        for a in args {
            argv.push(a.as_bytes().to_vec());
        }
        let refs: Vec<&[u8]> = argv.iter().map(|a| a.as_slice()).collect();
        let rs = self.redis.execute(&refs)?;
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .map(|v| v.as_i64().unwrap_or(0))
            .collect())
    }

    // ================== 列表扩展（对齐 C# FullRedis） ==================

    /// 跨键移动元素（`LMOVE`，对应 C# `LMove<T>`）。`from_left`/`to_left` 对应 C# 的 `LEFT`/`RIGHT` 字符串。
    pub fn lmove<V: FromRedisPayload>(
        &self,
        source: &str,
        destination: &str,
        from_left: bool,
        to_left: bool,
    ) -> Result<Option<V>> {
        let source = self.get_key(source);
        let destination = self.get_key(destination);
        let rs = self.redis.execute(&[
            b"LMOVE",
            source.as_bytes(),
            destination.as_bytes(),
            if from_left { b"LEFT" } else { b"RIGHT" },
            if to_left { b"LEFT" } else { b"RIGHT" },
        ])?;
        Ok(decode(rs))
    }

    /// 跨键阻塞移动元素（`BLMOVE`，对应 C# `BLMove<T>`）。
    pub fn blmove<V: FromRedisPayload>(
        &self,
        source: &str,
        destination: &str,
        from_left: bool,
        to_left: bool,
        timeout_seconds: i64,
    ) -> Result<Option<V>> {
        let source = self.get_key(source);
        let destination = self.get_key(destination);
        let rs = self.redis.execute_blocking(
            &[
                b"BLMOVE",
                source.as_bytes(),
                destination.as_bytes(),
                if from_left { b"LEFT" } else { b"RIGHT" },
                if to_left { b"LEFT" } else { b"RIGHT" },
                timeout_seconds.max(0).to_string().as_bytes(),
            ],
            timeout_seconds,
        )?;
        Ok(decode(rs))
    }

    /// 多键弹出（`LMPOP`，Redis 7.0+，对应 C# `LMPop<T>`）。
    ///
    /// 返回 `(键, 弹出元素列表)`，无数据时 `None`。
    pub fn lmpop<V: FromRedisPayload>(
        &self,
        keys: &[&str],
        from_left: bool,
        count: usize,
    ) -> Result<Option<(String, Vec<V>)>> {
        self.redis.require_version("7.0", "LMPOP")?;
        let mut argv: Vec<Vec<u8>> = Vec::with_capacity(keys.len() + 5);
        argv.push(b"LMPOP".to_vec());
        argv.push(keys.len().to_string().into_bytes());
        for k in keys {
            argv.push(self.get_key(k).into_bytes());
        }
        argv.push(if from_left { b"LEFT".to_vec() } else { b"RIGHT".to_vec() });
        argv.push(b"COUNT".to_vec());
        argv.push(count.to_string().into_bytes());

        let refs: Vec<&[u8]> = argv.iter().map(|a| a.as_slice()).collect();
        let rs = self.redis.execute(&refs)?;
        parse_multi_pop(rs, decode_array)
    }

    /// 多键阻塞弹出（`BRPOP`，对应 C# `BRPOP<T>(keys, secTimeout)`）。
    pub fn brpop_multi<V: FromRedisPayload>(
        &self,
        keys: &[&str],
        timeout_seconds: i64,
    ) -> Result<Option<(String, V)>> {
        self.block_pop(b"BRPOP", keys, timeout_seconds)
    }

    /// 多键阻塞弹出（`BLPOP`，对应 C# `BLPOP<T>(keys, secTimeout)`）。
    pub fn blpop_multi<V: FromRedisPayload>(
        &self,
        keys: &[&str],
        timeout_seconds: i64,
    ) -> Result<Option<(String, V)>> {
        self.block_pop(b"BLPOP", keys, timeout_seconds)
    }

    fn block_pop<V: FromRedisPayload>(
        &self,
        cmd: &[u8],
        keys: &[&str],
        timeout_seconds: i64,
    ) -> Result<Option<(String, V)>> {
        let mut argv: Vec<Vec<u8>> = Vec::with_capacity(keys.len() + 2);
        argv.push(cmd.to_vec());
        for k in keys {
            argv.push(self.get_key(k).into_bytes());
        }
        argv.push(timeout_seconds.max(0).to_string().into_bytes());

        let refs: Vec<&[u8]> = argv.iter().map(|a| a.as_slice()).collect();
        let rs = self.redis.execute_blocking(&refs, timeout_seconds)?;
        let mut items = rs.into_array().unwrap_or_default().into_iter();
        let Some(key) = items.next().and_then(|v| v.as_string()) else {
            return Ok(None);
        };
        Ok(items.next().and_then(decode).map(|v| (key, v)))
    }

    // ================== 集合扩展（对齐 C# FullRedis） ==================

    /// 批量成员存在性（`SMISMEMBER`，对应 C# `SMIsMember`），返回与输入顺序一致的布尔列表。
    pub fn smismember(&self, key: &str, members: &[&str]) -> Result<Vec<bool>> {
        let key = self.get_key(key);
        let mut argv: Vec<Vec<u8>> = Vec::with_capacity(members.len() + 2);
        argv.push(b"SMISMEMBER".to_vec());
        argv.push(key.into_bytes());
        for m in members {
            argv.push(m.as_bytes().to_vec());
        }
        let refs: Vec<&[u8]> = argv.iter().map(|a| a.as_slice()).collect();
        let rs = self.redis.execute(&refs)?;
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .map(|v| v.as_i64().unwrap_or(0) != 0)
            .collect())
    }

    /// 多键交集的基数（`SINTERCARD`，Redis 7.0+，对应 C# `SInterCard`）。`limit = 0` 表示不限制。
    pub fn sinter_card(&self, keys: &[&str], limit: usize) -> Result<i64> {
        self.redis.require_version("7.0", "SINTERCARD")?;
        let mut argv: Vec<Vec<u8>> = Vec::with_capacity(keys.len() + 4);
        argv.push(b"SINTERCARD".to_vec());
        argv.push(keys.len().to_string().into_bytes());
        for k in keys {
            argv.push(self.get_key(k).into_bytes());
        }
        if limit > 0 {
            argv.push(b"LIMIT".to_vec());
            argv.push(limit.to_string().into_bytes());
        }
        let refs: Vec<&[u8]> = argv.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.execute(&refs)?, 0))
    }

    // ================== 有序集合扩展（对齐 C# FullRedis） ==================

    /// 批量成员分数（`ZMSCORE`，对应 C# `ZMScore`），不存在为 `None`。
    pub fn zmscore(&self, key: &str, members: &[&str]) -> Result<Vec<Option<f64>>> {
        let key = self.get_key(key);
        let mut argv: Vec<Vec<u8>> = Vec::with_capacity(members.len() + 2);
        argv.push(b"ZMSCORE".to_vec());
        argv.push(key.into_bytes());
        for m in members {
            argv.push(m.as_bytes().to_vec());
        }
        let refs: Vec<&[u8]> = argv.iter().map(|a| a.as_slice()).collect();
        let rs = self.redis.execute(&refs)?;
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .map(|v| v.as_f64())
            .collect())
    }

    /// 随机成员（`ZRANDMEMBER key count`，对应 C# `ZRandMember<T>`，`count >= 0` 去重）。
    pub fn zrand_member<V: FromRedisPayload>(&self, key: &str, count: i64) -> Result<Vec<V>> {
        let key = self.get_key(key);
        let rs = self.redis.execute(&[
            b"ZRANDMEMBER",
            key.as_bytes(),
            count.to_string().as_bytes(),
        ])?;
        Ok(decode_array(rs))
    }

    /// 随机成员及分数（`ZRANDMEMBER key count WITHSCORES`）。
    pub fn zrand_member_with_scores<V: FromRedisPayload>(
        &self,
        key: &str,
        count: i64,
    ) -> Result<Vec<(V, f64)>> {
        let key = self.get_key(key);
        let rs = self.redis.execute(&[
            b"ZRANDMEMBER",
            key.as_bytes(),
            count.to_string().as_bytes(),
            b"WITHSCORES",
        ])?;
        Ok(decode_scored(rs))
    }

    /// 多键弹出最小/最大成员（`ZMPOP`，Redis 7.0+，对应 C# `ZMPop<T>`）。
    ///
    /// 返回 `(键, [(成员, 分数)])`，无数据时 `None`。
    #[allow(clippy::type_complexity)]
    pub fn zmpop<V: FromRedisPayload>(
        &self,
        keys: &[&str],
        min: bool,
        count: usize,
    ) -> Result<Option<(String, Vec<(V, f64)>)>> {
        self.redis.require_version("7.0", "ZMPOP")?;
        let mut argv: Vec<Vec<u8>> = Vec::with_capacity(keys.len() + 5);
        argv.push(b"ZMPOP".to_vec());
        argv.push(keys.len().to_string().into_bytes());
        for k in keys {
            argv.push(self.get_key(k).into_bytes());
        }
        argv.push(if min { b"MIN".to_vec() } else { b"MAX".to_vec() });
        argv.push(b"COUNT".to_vec());
        argv.push(count.to_string().into_bytes());

        let refs: Vec<&[u8]> = argv.iter().map(|a| a.as_slice()).collect();
        let rs = self.redis.execute(&refs)?;
        parse_multi_pop(rs, decode_scored_pairs)
    }

    /// 多键阻塞弹出最小分成员（`BZPOPMIN`，对应 C# `BZPopMin`），返回 `(键, 成员, 分数)`。
    pub fn bzpopmin<V: FromRedisPayload>(
        &self,
        keys: &[&str],
        timeout_seconds: i64,
    ) -> Result<Option<(String, V, f64)>> {
        self.bzpop(b"BZPOPMIN", keys, timeout_seconds)
    }

    /// 多键阻塞弹出最大分成员（`BZPOPMAX`，对应 C# `BZPopMax`），返回 `(键, 成员, 分数)`。
    pub fn bzpopmax<V: FromRedisPayload>(
        &self,
        keys: &[&str],
        timeout_seconds: i64,
    ) -> Result<Option<(String, V, f64)>> {
        self.bzpop(b"BZPOPMAX", keys, timeout_seconds)
    }

    fn bzpop<V: FromRedisPayload>(
        &self,
        cmd: &[u8],
        keys: &[&str],
        timeout_seconds: i64,
    ) -> Result<Option<(String, V, f64)>> {
        let mut argv: Vec<Vec<u8>> = Vec::with_capacity(keys.len() + 2);
        argv.push(cmd.to_vec());
        for k in keys {
            argv.push(self.get_key(k).into_bytes());
        }
        argv.push(timeout_seconds.max(0).to_string().into_bytes());

        let refs: Vec<&[u8]> = argv.iter().map(|a| a.as_slice()).collect();
        let rs = self.redis.execute_blocking(&refs, timeout_seconds)?;
        let mut items = rs.into_array().unwrap_or_default().into_iter();
        let Some(key) = items.next().and_then(|v| v.as_string()) else {
            return Ok(None);
        };
        let value = items.next().and_then(decode::<V>);
        let score = items.next().and_then(|v| v.as_f64()).unwrap_or(0.0);
        Ok(value.map(|v| (key, v, score)))
    }

    // ================== 服务器管理命令（对齐 C# FullRedis） ==================

    /// 交换两个库（`SWAPDB`，对应 C# `SwapDB`）。
    pub fn swapdb(&self, db1: i32, db2: i32) -> Result<()> {
        self.redis.execute_ignore(&[
            b"SWAPDB",
            db1.to_string().as_bytes(),
            db2.to_string().as_bytes(),
        ])
    }

    /// 等待写命令同步到指定副本数（`WAIT`，对应 C# `Wait`），返回已确认副本数。
    pub fn wait(&self, num_replicas: i32, timeout_ms: i64) -> Result<i64> {
        Ok(int_or(
            self.redis.execute(&[
                b"WAIT",
                num_replicas.to_string().as_bytes(),
                timeout_ms.to_string().as_bytes(),
            ])?,
            0,
        ))
    }

    /// 慢日志条数（`SLOWLOG LEN`，对应 C# `SlowLogLen`）。
    pub fn slowlog_len(&self) -> Result<i64> {
        Ok(int_or(self.redis.execute(&[b"SLOWLOG", b"LEN"])?, 0))
    }

    /// 清空慢日志（`SLOWLOG RESET`，对应 C# `SlowLogReset`）。
    pub fn slowlog_reset(&self) -> Result<()> {
        self.redis.execute_ignore(&[b"SLOWLOG", b"RESET"])
    }

    /// 读取慢日志（`SLOWLOG GET`，对应 C# `SlowLogGet`）。
    pub fn slowlog_get(&self, count: i64) -> Result<Vec<SlowLogEntry>> {
        let rs = self
            .redis
            .execute(&[b"SLOWLOG", b"GET", count.to_string().as_bytes()])?;
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .filter_map(SlowLogEntry::parse)
            .collect())
    }

    /// 事件历史延迟峰值（`LATENCY HISTORY`，对应 C# `LatencyHistory`），返回 `[(时间戳, 延迟毫秒)]`。
    pub fn latency_history(&self, event: &str) -> Result<Vec<(i64, i64)>> {
        let rs = self
            .redis
            .execute(&[b"LATENCY", b"HISTORY", event.as_bytes()])?;
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| {
                let mut items = v.into_array()?.into_iter();
                let ts = items.next()?.as_i64()?;
                let ms = items.next()?.as_i64()?;
                Some((ts, ms))
            })
            .collect())
    }

    /// 最新延迟统计（`LATENCY LATEST`，对应 C# `LatencyLatest`），返回 `[(事件, 时间戳, 最新, 最大)]`。
    pub fn latency_latest(&self) -> Result<Vec<(String, i64, i64, i64)>> {
        let rs = self.redis.execute(&[b"LATENCY", b"LATEST"])?;
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| {
                let mut items = v.into_array()?.into_iter();
                let event = items.next()?.as_string()?;
                let ts = items.next()?.as_i64()?;
                let latest = items.next()?.as_i64()?;
                let max = items.next()?.as_i64()?;
                Some((event, ts, latest, max))
            })
            .collect())
    }

    /// 重置延迟统计（`LATENCY RESET [event ...]`，对应 C# `LatencyReset`），返回重置条数。
    pub fn latency_reset(&self, events: &[&str]) -> Result<i64> {
        let mut argv: Vec<Vec<u8>> = Vec::with_capacity(events.len() + 2);
        argv.push(b"LATENCY".to_vec());
        argv.push(b"RESET".to_vec());
        for e in events {
            argv.push(e.as_bytes().to_vec());
        }
        let refs: Vec<&[u8]> = argv.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.execute(&refs)?, 0))
    }

    /// 延迟诊断报告（`LATENCY DOCTOR`，对应 C# `LatencyDoctor`）。
    pub fn latency_doctor(&self) -> Result<String> {
        Ok(self
            .redis
            .execute(&[b"LATENCY", b"DOCTOR"])?
            .as_string()
            .unwrap_or_default())
    }

    /// 复制跟随设置（`REPLICAOF`，对应 C# `ReplicaOf`）。`host = None` 等价 `REPLICAOF NO ONE`。
    pub fn replica_of(&self, host: Option<&str>, port: u16) -> Result<()> {
        match host {
            Some(host) => self.redis.execute_ignore(&[
                b"REPLICAOF",
                host.as_bytes(),
                port.to_string().as_bytes(),
            ]),
            None => self.redis.execute_ignore(&[b"REPLICAOF", b"NO", b"ONE"]),
        }
    }

    // ================== Redis 函数（7.0+，对齐 C# FullRedis） ==================

    /// 调用已加载函数（`FCALL`，对应 C# `FCall<T>`）。
    pub fn fcall<V: FromRedisPayload>(
        &self,
        function: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<Option<V>> {
        self.call_function(b"FCALL", function, keys, args)
    }

    /// 只读模式调用已加载函数（`FCALL_RO`，对应 C# `FCallRO<T>`）。
    pub fn fcall_ro<V: FromRedisPayload>(
        &self,
        function: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<Option<V>> {
        self.call_function(b"FCALL_RO", function, keys, args)
    }

    fn call_function<V: FromRedisPayload>(
        &self,
        cmd: &[u8],
        function: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<Option<V>> {
        let mut argv: Vec<Vec<u8>> = Vec::with_capacity(keys.len() + args.len() + 3);
        argv.push(cmd.to_vec());
        argv.push(function.as_bytes().to_vec());
        argv.push(keys.len().to_string().into_bytes());
        for k in keys {
            argv.push(self.get_key(k).into_bytes());
        }
        for a in args {
            argv.push(a.as_bytes().to_vec());
        }
        let refs: Vec<&[u8]> = argv.iter().map(|a| a.as_slice()).collect();
        let rs = self.redis.execute(&refs)?;
        if rs.is_null() {
            return Ok(None);
        }
        Ok(decode(rs))
    }

    /// 加载函数库（`FUNCTION LOAD [REPLACE]`，Redis 7.0+，对应 C# `FunctionLoad`），返回函数库名。
    pub fn function_load(&self, library_code: &str, replace: bool) -> Result<String> {
        self.redis.require_version("7.0", "FUNCTION LOAD")?;
        let rs = if replace {
            self.redis.execute(&[
                b"FUNCTION",
                b"LOAD",
                b"REPLACE",
                library_code.as_bytes(),
            ])?
        } else {
            self.redis
                .execute(&[b"FUNCTION", b"LOAD", library_code.as_bytes()])?
        };
        Ok(rs.as_string().unwrap_or_default())
    }

    /// 列出函数库（`FUNCTION LIST [LIBRARYNAME name]`，Redis 7.0+），返回原始应答数组。
    pub fn function_list(&self, library_name: Option<&str>) -> Result<Vec<RespValue>> {
        self.redis.require_version("7.0", "FUNCTION LIST")?;
        let rs = match library_name {
            Some(name) => self.redis.execute(&[
                b"FUNCTION",
                b"LIST",
                b"LIBRARYNAME",
                name.as_bytes(),
            ])?,
            None => self.redis.execute(&[b"FUNCTION", b"LIST"])?,
        };
        Ok(rs.into_array().unwrap_or_default())
    }

    /// 删除函数库（`FUNCTION DELETE`，Redis 7.0+，对应 C# `FunctionDelete`）。
    pub fn function_delete(&self, library_name: &str) -> Result<()> {
        self.redis.require_version("7.0", "FUNCTION DELETE")?;
        self.redis
            .execute_ignore(&[b"FUNCTION", b"DELETE", library_name.as_bytes()])
    }

    /// 导出 Prometheus 文本格式指标（对应 C# `GetPrometheusMetrics`，前缀同为 `newlife_redis`，便于共用看板）。
    pub fn get_prometheus_metrics(&self) -> Result<String> {
        let inf = self.redis.info()?;
        if inf.is_empty() {
            return Ok(String::new());
        }

        let prefix = "newlife_redis";
        let mut sb = String::new();

        // 连接数
        push_metric(&mut sb, &inf, prefix, "connected_clients", "connected_clients");
        push_metric(&mut sb, &inf, prefix, "blocked_clients", "blocked_clients");
        // 内存
        push_metric(&mut sb, &inf, prefix, "used_memory_bytes", "used_memory");
        push_metric(&mut sb, &inf, prefix, "used_memory_rss_bytes", "used_memory_rss");
        push_metric(
            &mut sb,
            &inf,
            prefix,
            "mem_fragmentation_ratio",
            "mem_fragmentation_ratio",
        );
        // 命令统计
        push_metric(
            &mut sb,
            &inf,
            prefix,
            "commands_processed_total",
            "total_commands_processed",
        );
        push_metric(
            &mut sb,
            &inf,
            prefix,
            "instantaneous_ops_per_sec",
            "instantaneous_ops_per_sec",
        );
        // CPU
        push_metric(&mut sb, &inf, prefix, "used_cpu_sys_seconds", "used_cpu_sys");
        push_metric(&mut sb, &inf, prefix, "used_cpu_user_seconds", "used_cpu_user");
        // 键空间（db0）
        if let Some(v) = inf.get("db0") {
            for part in v.split(',') {
                if let Some((k, val)) = part.split_once('=') {
                    sb.push_str(&format!("{prefix}_db0_{k} {val}\n"));
                }
            }
        }
        // 复制
        push_metric(&mut sb, &inf, prefix, "connected_slaves", "connected_slaves");

        Ok(sb)
    }

    // ================== RedLock（对齐 C# FullRedis.AcquireRedLock） ==================

    /// 借助多个独立实例获取 RedLock 分布式锁（对应 C# `AcquireRedLock`）。
    ///
    /// `key` 自动补前缀；`self` 与 `other_instances` 构成全部实例集合。
    pub fn acquire_red_lock(
        &self,
        other_instances: &[FullRedis],
        key: &str,
        ms_timeout: i64,
        ms_expire: i64,
    ) -> Result<Option<crate::services::RedLock>> {
        let key = self.get_key(key);
        let mut instances = vec![self.clone()];
        instances.extend(other_instances.iter().cloned());
        crate::services::acquire_red_lock(&instances, &key, ms_timeout, ms_expire)
    }
}

/// 追加一条 `prefix_name value` 指标行（对应 C# `GetPrometheusMetrics` 的字段映射）。
fn push_metric(
    sb: &mut String,
    inf: &HashMap<String, String>,
    prefix: &str,
    name: &str,
    key: &str,
) {
    if let Some(v) = inf.get(key) {
        sb.push_str(&format!("{prefix}_{name} {v}\n"));
    }
}

/// 解析多键弹出命令（`LMPOP`/`ZMPOP`）的应答：`[key, [items...]]`。
fn parse_multi_pop<T, F>(value: RespValue, decode_items: F) -> Result<Option<(String, Vec<T>)>>
where
    F: FnOnce(RespValue) -> Vec<T>,
{
    let mut items = value.into_array().unwrap_or_default().into_iter();
    let Some(key) = items.next().and_then(|v| v.as_string()) else {
        return Ok(None);
    };
    let values = decode_items(items.next().unwrap_or(RespValue::Array(Vec::new())));
    Ok(Some((key, values)))
}

/// 慢日志条目（`SLOWLOG GET`）。
#[derive(Debug, Clone, PartialEq)]
pub struct SlowLogEntry {
    /// 条目唯一 ID
    pub id: i64,
    /// 记录时间（Unix 秒）
    pub timestamp: i64,
    /// 执行耗时（微秒）
    pub duration_us: i64,
    /// 命令与参数
    pub command: Vec<String>,
    /// 客户端地址（Redis 4.0+）
    pub client_addr: Option<String>,
    /// 客户端名称（Redis 4.0+）
    pub client_name: Option<String>,
}

impl SlowLogEntry {
    fn parse(value: RespValue) -> Option<Self> {
        let mut items = value.into_array()?.into_iter();
        let id = items.next()?.as_i64()?;
        let timestamp = items.next()?.as_i64()?;
        let duration_us = items.next()?.as_i64()?;
        let command = items
            .next()
            .and_then(|v| v.into_array())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.as_string())
            .collect();
        let client_addr = items.next().and_then(|v| v.as_string());
        let client_name = items.next().and_then(|v| v.as_string());
        Some(Self {
            id,
            timestamp,
            duration_us,
            command,
            client_addr,
            client_name,
        })
    }
}


/// 分布式锁句柄。析构时自动释放（仅当锁值仍属于本实例的令牌）。
pub struct LockHandle {
    redis: FullRedis,
    key: String,
    token: String,
    has_lock: bool,
}

impl LockHandle {
    /// 锁键。
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 锁令牌。
    pub fn token(&self) -> &str {
        &self.token
    }

    /// 是否仍持有锁。
    pub fn has_lock(&self) -> bool {
        self.has_lock
    }

    /// 主动释放锁。
    pub fn release(&mut self) {
        if !self.has_lock {
            return;
        }
        self.has_lock = false;

        if let Ok(Some(value)) = self.redis.redis().get_string(&self.key)
            && value.starts_with(&format!("{}|", self.token)) {
                let _ = self.redis.redis().remove(&self.key);
            }
    }
}

impl Drop for LockHandle {
    fn drop(&mut self) {
        self.release();
    }
}

/// 解析锁值中的过期时间戳（兼容旧格式纯数字）。
pub fn parse_lock_expire(value: Option<String>) -> u64 {
    let Some(value) = value else { return 0 };
    let text = match value.split_once('|') {
        Some((_, tail)) => tail,
        None => value.as_str(),
    };
    text.trim().parse().unwrap_or(0)
}

/// 32 位十六进制随机令牌（对应 C# `Guid.NewGuid().ToString("N")`）。
pub fn new_token() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let bytes: [u8; 16] = rng.r#gen();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// 机器启动以来的毫秒数，与 C# `Environment.TickCount64` 同一时钟域，
/// 保证跨语言抢锁时超时判断一致。
pub fn boot_ticks_ms() -> u64 {
    #[cfg(windows)]
    {
        unsafe extern "system" {
            fn GetTickCount64() -> u64;
        }
        // SAFETY: GetTickCount64 无参数、永不失败
        unsafe { GetTickCount64() }
    }

    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/uptime")
            .ok()
            .and_then(|s| {
                s.split_whitespace()
                    .next()
                    .and_then(|v| v.parse::<f64>().ok())
            })
            .map(|secs| (secs * 1000.0) as u64)
            .unwrap_or_else(epoch_ms)
    }

    #[cfg(not(any(windows, target_os = "linux")))]
    {
        epoch_ms()
    }
}

#[allow(dead_code)]
fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_is_applied_like_csharp_ensure_start() {
        let redis = Redis::open("127.0.0.1:6379", None, 0).unwrap();
        let full = FullRedis::with_prefix(redis, Some("app:".into()));

        assert_eq!(full.get_key("user"), "app:user");
        assert_eq!(full.get_key("app:user"), "app:user");
        // 不区分大小写（与 C# EnsureStart 一致）
        assert_eq!(full.get_key("APP:user"), "APP:user");
        assert_eq!(full.trim_key("app:user"), "user");
        assert_eq!(full.trim_key("other"), "other");
    }

    #[test]
    fn prefix_last_segment_trim_is_safe_on_multibyte() {
        let redis = Redis::open("127.0.0.1:6379", None, 0).unwrap();
        let full = FullRedis::with_prefix(redis, Some("前缀:".into()));
        assert_eq!(full.get_key("键"), "前缀:键");
        assert_eq!(full.trim_key("前缀:键"), "键");
    }

    #[test]
    fn lock_expire_parsing_handles_both_formats() {
        assert_eq!(parse_lock_expire(Some("abc|1234".into())), 1234);
        assert_eq!(parse_lock_expire(Some("1234".into())), 1234);
        assert_eq!(parse_lock_expire(None), 0);
        assert_eq!(parse_lock_expire(Some("abc|notnumber".into())), 0);
    }

    #[test]
    fn token_is_32_hex_chars() {
        let token = new_token();
        assert_eq!(token.len(), 32);
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn boot_ticks_is_monotonic_ish() {
        let a = boot_ticks_ms();
        let b = boot_ticks_ms();
        assert!(b >= a);
        assert!(b > 0);
    }

    #[test]
    fn lock_value_format_matches_csharp() {
        // C#: $"{token}|{now + msExpire}"，本实现复刻该格式
        let token = new_token();
        let now = boot_ticks_ms();
        let value = format!("{token}|{}", now + 5000);
        assert!(value.starts_with(&token));
        assert_eq!(parse_lock_expire(Some(value)), now + 5000);
    }

    #[test]
    fn eval_keys_are_not_prefixed_in_raw_mode() {
        // 仅验证签名可编译与 key 处理函数行为（真实 EVAL 需连接服务器）
        let redis = Redis::open("127.0.0.1:6379", None, 0).unwrap();
        let full = FullRedis::with_prefix(redis, Some("p:".into()));
        let keys = ["k1", "k2"];
        let full_keys: Vec<String> = keys.iter().map(|k| full.get_key(k)).collect();
        assert_eq!(full_keys, vec!["p:k1", "p:k2"]);
    }
}
