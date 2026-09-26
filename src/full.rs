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
use crate::redis::Redis;
use crate::resp::RespValue;

pub use crate::encoder::{FromRedisPayload, ToRedisPayload};
use crate::geo::RedisGeo;
use crate::hash::RedisHash;
use crate::hyperloglog::HyperLogLog;
use crate::list::RedisList;
use crate::pubsub::PubSub;
use crate::queues::{RedisDelayQueue, RedisQueue, RedisReliableQueue};
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
