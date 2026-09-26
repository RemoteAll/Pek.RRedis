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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use crate::client::{ConnConfig, RedisClient};
use crate::encoder::{FromRedisPayload, ToRedisPayload};
use crate::error::{Error, Result};
use crate::options::RedisOptions;
use crate::pool::Pool;
use crate::resp::RespValue;

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
        }
    }

    // ================== 执行内核 ==================

    /// 执行命令（带重试）。
    pub fn execute(&self, args: &[&[u8]]) -> Result<RespValue> {
        self.execute_inner(args, None)
    }

    /// 执行阻塞命令（BRPOP / BRPOPLPUSH / BLMOVE 等）。
    pub fn execute_blocking(&self, args: &[&[u8]], block_seconds: i64) -> Result<RespValue> {
        self.execute_inner(args, Some(block_seconds))
    }

    /// 执行命令并忽略应答（用于无需结果的写操作）。
    pub fn execute_ignore(&self, args: &[&[u8]]) -> Result<()> {
        self.execute(args).map(|_| ())
    }

    fn execute_inner(&self, args: &[&[u8]], block: Option<i64>) -> Result<RespValue> {
        let attempts = self.inner.options.retry.max(1);
        let mut last_err: Option<Error> = None;

        for attempt in 0..attempts {
            let mut client = self.inner.pool.get()?;
            let result = match block {
                Some(seconds) => client.command_blocking(args, seconds),
                None => client.command(args),
            };

            match result {
                Ok(v) => {
                    self.inner.commands.fetch_add(1, Ordering::Relaxed);
                    return Ok(v);
                }
                Err(Error::Server(msg)) => {
                    // 服务端错误与 C# 一致：立即抛出，不重试
                    self.inner.errors.fetch_add(1, Ordering::Relaxed);
                    return Err(Error::Server(msg));
                }
                Err(e) => {
                    last_err = Some(e);
                    // 连接在此处随 PooledClient 析构被标记损坏并销毁
                }
            }

            if attempt + 1 < attempts {
                std::thread::sleep(Duration::from_millis(50u64 << attempt.min(5)));
            }
        }

        self.inner.errors.fetch_add(1, Ordering::Relaxed);
        Err(last_err.unwrap_or_else(|| Error::Pool("命令执行失败且无错误信息".into())))
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
            .execute(&[b"EXISTS", key.as_bytes()])?
            .as_i64()
            .unwrap_or(0)
            > 0)
    }

    /// 删除单个键（`DEL`）。
    pub fn remove(&self, key: &str) -> Result<i64> {
        if key.is_empty() {
            return Ok(0);
        }
        Ok(self.execute(&[b"DEL", key.as_bytes()])?.as_i64().unwrap_or(0))
    }

    /// 批量删除（`DEL key...`）。
    pub fn remove_many(&self, keys: &[&str]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
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
            .execute(&[b"EXPIRE", key.as_bytes(), seconds.to_string().as_bytes()])?
            .as_i64()
            .unwrap_or(0)
            == 1)
    }

    /// 设置过期时间（毫秒）（`PEXPIRE`）。
    pub fn set_expire_ms(&self, key: &str, milliseconds: i64) -> Result<bool> {
        Ok(self
            .execute(&[b"PEXPIRE", key.as_bytes(), milliseconds.to_string().as_bytes()])?
            .as_i64()
            .unwrap_or(0)
            == 1)
    }

    /// 获取剩余有效期（秒）（`TTL`）。-1 永不过期，-2 键不存在。
    pub fn get_expire(&self, key: &str) -> Result<i64> {
        Ok(self.execute(&[b"TTL", key.as_bytes()])?.as_i64().unwrap_or(-2))
    }

    /// 获取剩余有效期（毫秒）（`PTTL`）。
    pub fn get_expire_ms(&self, key: &str) -> Result<i64> {
        Ok(self.execute(&[b"PTTL", key.as_bytes()])?.as_i64().unwrap_or(-2))
    }

    /// 移除过期时间（`PERSIST`）。
    pub fn persist(&self, key: &str) -> Result<bool> {
        Ok(self
            .execute(&[b"PERSIST", key.as_bytes()])?
            .as_i64()
            .unwrap_or(0)
            == 1)
    }

    /// 键类型（`TYPE`），不存在返回 `None`。
    pub fn type_of(&self, key: &str) -> Result<Option<String>> {
        let s = self.execute(&[b"TYPE", key.as_bytes()])?.as_string();
        match s.as_deref() {
            Some("none") | None => Ok(None),
            _ => Ok(s),
        }
    }

    /// 重命名（`RENAME` / `RENAMENX`）。
    pub fn rename(&self, key: &str, new_key: &str, overwrite: bool) -> Result<bool> {
        let cmd: &[u8] = if overwrite { b"RENAME" } else { b"RENAMENX" };
        let rs = self.execute(&[cmd, key.as_bytes(), new_key.as_bytes()])?;
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
        Ok(self.execute(&refs)?.as_i64().unwrap_or(0) == 1)
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
            self.execute(&[
                b"MEMORY",
                b"USAGE",
                key.as_bytes(),
                b"SAMPLES",
                samples.to_string().as_bytes(),
            ])?
        } else {
            self.execute(&[b"MEMORY", b"USAGE", key.as_bytes()])?
        };
        Ok(rs.as_i64())
    }

    /// 对象内部编码（`OBJECT ENCODING`）。
    pub fn object_encoding(&self, key: &str) -> Result<Option<String>> {
        let rs = self.execute(&[b"OBJECT", b"ENCODING", key.as_bytes()])?;
        if rs.is_null() {
            Ok(None)
        } else {
            Ok(rs.as_string())
        }
    }

    /// 单步 SCAN。返回 `(下一个游标, 键列表)`；游标为 0 表示遍历结束。
    pub fn scan(&self, cursor: u64, pattern: &str, count: usize) -> Result<(u64, Vec<String>)> {
        let count = if count == 0 { 100 } else { count };
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
            self.execute(&[b"SET", key.as_bytes(), &payload])?
        } else {
            self.execute(&[
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
        let rs = self.execute(&[b"GET", key.as_bytes()])?;
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
            self.execute(&[
                b"SET",
                key.as_bytes(),
                &payload,
                b"EX",
                expire.to_string().as_bytes(),
                b"NX",
            ])?
        } else {
            self.execute(&[b"SET", key.as_bytes(), &payload, b"NX"])?
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
        let rs = self.execute(&[b"GETSET", key.as_bytes(), &payload])?;
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
        let rs = self.execute(&refs)?;
        if rs.is_null() {
            return Ok(None);
        }
        Ok(rs.as_bytes().and_then(|b| T::from_redis_payload(&b).ok()))
    }

    /// 追加内容（`APPEND`），返回追加后的长度。
    pub fn append(&self, key: &str, value: &str) -> Result<i64> {
        Ok(self
            .execute(&[b"APPEND", key.as_bytes(), value.as_bytes()])?
            .as_i64()
            .unwrap_or(0))
    }

    /// 字符串长度（`STRLEN`）。
    pub fn strlen(&self, key: &str) -> Result<i64> {
        Ok(self.execute(&[b"STRLEN", key.as_bytes()])?.as_i64().unwrap_or(0))
    }

    /// 截取子串（`GETRANGE`，含头含尾，-1 表示末尾）。
    pub fn get_range(&self, key: &str, start: i64, end: i64) -> Result<String> {
        Ok(self
            .execute(&[
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
            .execute(&[
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
            self.execute(&[b"INCR", key.as_bytes()])?
        } else {
            self.execute(&[b"INCRBY", key.as_bytes(), delta.to_string().as_bytes()])?
        };
        Ok(rs.as_i64().unwrap_or(0))
    }

    /// 浮点自增（`INCRBYFLOAT`）。
    pub fn increment_float(&self, key: &str, delta: f64) -> Result<f64> {
        let rs = self.execute(&[
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
            self.execute(&[b"DECR", key.as_bytes()])?
        } else {
            self.execute(&[b"DECRBY", key.as_bytes(), delta.to_string().as_bytes()])?
        };
        Ok(rs.as_i64().unwrap_or(0))
    }

    /// 位设置（`SETBIT`）。
    pub fn set_bit(&self, key: &str, offset: u64, value: u8) -> Result<i64> {
        Ok(self
            .execute(&[
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
            .execute(&[b"GETBIT", key.as_bytes(), offset.to_string().as_bytes()])?
            .as_i64()
            .unwrap_or(0))
    }

    /// 统计置位数量（`BITCOUNT`）。
    pub fn bit_count(&self, key: &str, start: i64, end: i64) -> Result<i64> {
        Ok(self
            .execute(&[
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
            .execute(&[
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
        self.execute(&refs)
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
}
