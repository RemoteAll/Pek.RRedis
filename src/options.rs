//! 连接配置：连接字符串解析、选项与连接池参数。
//!
//! 与 DH.NRedis 的 `Redis.Init` / `RedisOptions` / `RedisPoolConfig` 对齐：
//!
//! | DH.NRedis | pek-rredis |
//! |-----------|------------|
//! | `FullRedis("127.0.0.1:6379", "pass", 7)` | [`RedisOptions::new`] |
//! | `rds.Init("server=...;password=...;db=3")` | [`RedisOptions::from_config`] |
//! | `RedisOptions` / `RedisPoolConfig` | [`RedisOptions`] / [`RedisPoolConfig`] |
//!
//! 支持的连接字符串键（不区分大小写）：`Server`、`Port`、`UserName`、`Password`、`Db`、
//! `Timeout`（`responseTimeout` / `connectTimeout`）、`Prefix`、`ProtocolVersion`、
//! `MaxMessageSize`、`Expire`、`PoolMin`、`PoolMax`、`PoolIdleTime`、`MaxLifetime`、`WaitTimeout`。

use std::collections::BTreeMap;

use crate::error::{Error, Result};

/// 连接池配置。默认值与 DH.NRedis `RedisPoolConfig` 一致。
#[derive(Debug, Clone)]
pub struct RedisPoolConfig {
    /// 连接池最小空闲数。回收时保留至少该数量的空闲连接。默认 10
    pub min: usize,
    /// 连接池最大容量。默认 100000
    pub max: usize,
    /// 空闲清理时间（秒）。超过该时间未使用的连接被清理。默认 30
    pub idle_time: u64,
    /// 连接最大生命周期（秒）。超时强制回收，适用于主从切换等。默认 300
    pub max_lifetime: u64,
    /// 池满时借出等待超时（秒）。默认 15
    pub wait_timeout: u64,
}

impl Default for RedisPoolConfig {
    fn default() -> Self {
        Self {
            min: 10,
            max: 100_000,
            idle_time: 30,
            max_lifetime: 300,
            wait_timeout: 15,
        }
    }
}

impl RedisPoolConfig {
    /// 从配置字典加载（对应 C# `RedisPoolConfig.Load`）。
    pub fn load(&mut self, dic: &BTreeMap<String, String>) {
        if let Some(v) = get_int(dic, &["PoolMin", "poolmin"])
            && v >= 0 {
                self.min = v as usize;
            }
        if let Some(v) = get_int(dic, &["PoolMax", "poolmax"])
            && v >= 0 {
                self.max = v as usize;
            }
        if let Some(v) = get_int(dic, &["PoolIdleTime", "poolidletime"])
            && v >= 0 {
                self.idle_time = v as u64;
            }
        if let Some(v) = get_int(dic, &["MaxLifetime", "maxlifetime"])
            && v >= 0 {
                self.max_lifetime = v as u64;
            }
        if let Some(v) = get_int(dic, &["WaitTimeout", "waittimeout"])
            && v >= 0 {
                self.wait_timeout = v as u64;
            }
    }
}

/// Redis 连接选项。
#[derive(Debug, Clone)]
pub struct RedisOptions {
    /// 实例名，用于日志与监控
    pub instance_name: Option<String>,
    /// 服务器地址列表，支持 `host:port`，多个地址用逗号分隔（网络异常时自动切换）
    pub servers: Vec<String>,
    /// 用户名（Redis 6.0+ ACL）
    pub user_name: Option<String>,
    /// 密码
    pub password: Option<String>,
    /// 数据库索引
    pub db: i32,
    /// 读写超时（毫秒），默认 3000
    pub timeout_ms: u64,
    /// 出错重试次数，默认 3
    pub retry: usize,
    /// 键前缀
    pub prefix: Option<String>,
    /// 协议版本。0/2 表示 RESP2，3 表示 RESP3（需 Redis 6.0+）
    pub protocol_version: i32,
    /// 默认过期时间（秒）。小于等于 0 表示不过期
    pub expire: i64,
    /// 最大消息大小（字节）。默认 1MB，超限直接报错
    pub max_message_size: usize,
    /// 连接池配置
    pub pool: RedisPoolConfig,
}

impl Default for RedisOptions {
    fn default() -> Self {
        Self {
            instance_name: None,
            servers: Vec::new(),
            user_name: None,
            password: None,
            db: 0,
            timeout_ms: 3_000,
            retry: 3,
            prefix: None,
            protocol_version: 0,
            expire: 0,
            max_message_size: 1024 * 1024,
            pool: RedisPoolConfig::default(),
        }
    }
}

impl RedisOptions {
    /// 使用服务器地址、密码与库号创建选项。
    ///
    /// 等价于 C# `new FullRedis(server, password, db)`。
    pub fn new(server: &str, password: Option<&str>, db: i32) -> Self {
        Self {
            servers: split_servers(server),
            password: password
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            db,
            ..Default::default()
        }
    }

    /// 从连接字符串创建。
    ///
    /// 等价于 C# `new FullRedis(); rds.Init(config)`。
    pub fn from_config(config: &str) -> Result<Self> {
        let mut opt = Self::default();
        opt.apply_config(config)?;
        Ok(opt)
    }

    /// 应用连接字符串（可多次调用，后到的配置覆盖先前的）。
    pub fn apply_config(&mut self, config: &str) -> Result<()> {
        if config.trim().is_empty() {
            return Err(Error::Config("连接字符串为空".into()));
        }

        let dic = parse_config(config);

        if let Some(v) = dic.get("server").cloned() {
            let v = v.trim();
            if !v.is_empty() {
                self.servers = split_servers(v);
            }
        }
        if self.servers.is_empty() {
            // 兼容以位置参数形式给出的地址： "127.0.0.1:6379,pass,3"
            if let Some(v) = dic.get("[0]") {
                self.servers = split_servers(v);
            }
        }

        // 独立的 Port 配置拼接到未写端口的地址
        if let Some(port) = get_int(&dic, &["Port"])
            && port > 0 {
                for s in &mut self.servers {
                    if !s.contains(':') {
                        s.push(':');
                        s.push_str(&port.to_string());
                    }
                }
            }

        if let Some(v) = dic.get("username") {
            self.user_name = Some(v.trim().to_string());
        }
        if let Some(v) = dic.get("password") {
            self.password = Some(v.clone());
        }
        if let Some(v) = get_int(&dic, &["Db"]) {
            self.db = v as i32;
        }
        if let Some(v) = get_int(&dic, &["Timeout", "responseTimeout", "connectTimeout"])
            && v > 0 {
                self.timeout_ms = v as u64;
            }
        if let Some(v) = dic.get("prefix")
            && !v.is_empty() {
                self.prefix = Some(v.clone());
            }
        if let Some(v) = get_int(&dic, &["ProtocolVersion"]) {
            self.protocol_version = v as i32;
        }
        if let Some(v) = get_int(&dic, &["MaxMessageSize"])
            && v >= 0 {
                self.max_message_size = v as usize;
            }
        if let Some(v) = get_int(&dic, &["Expire"])
            && v >= 0 {
                self.expire = v;
            }
        self.pool.load(&dic);

        if self.servers.is_empty() {
            return Err(Error::Config(format!("连接字符串缺少 Server：{config}")));
        }

        Ok(())
    }

    /// 规范化后的地址列表：`host:port`，默认端口 6379。
    pub fn endpoints(&self) -> Vec<String> {
        self.servers
            .iter()
            .map(|s| {
                let s = s.trim();
                let s = s.strip_prefix("tcp://").unwrap_or(s);
                if s.contains(':') {
                    s.to_string()
                } else {
                    format!("{s}:6379")
                }
            })
            .collect()
    }
}

/// 解析连接字符串为字典（键不区分大小写）。
///
/// 与 C# `Redis.ParseConfig` 规则一致：含 `;` 或 `=` 少于等于两段时使用 `;` 分隔，
/// 否则按旧版 `,` 分隔。
pub fn parse_config(config: &str) -> BTreeMap<String, String> {
    let separator = if config.contains(';') || config.split('=').count() <= 2 {
        ';'
    } else {
        ','
    };

    let mut dic = BTreeMap::new();
    for part in config.split(separator) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((k, v)) = part.split_once('=') {
            dic.insert(normalize_key(k.trim()), v.trim().to_string());
        } else {
            // 无等号的位置参数，按顺序编号
            let key = format!("[{}]", dic.len());
            dic.insert(key, part.to_string());
        }
    }

    dic
}

fn normalize_key(key: &str) -> String {
    // 保留位置参数标记，其余统一小写
    if key.starts_with('[') {
        return key.to_string();
    }
    key.to_lowercase()
}

fn get_int(dic: &BTreeMap<String, String>, keys: &[&str]) -> Option<i64> {
    for k in keys {
        if let Some(v) = dic.get(&k.to_lowercase())
            && let Ok(n) = v.trim().parse::<i64>() {
                return Some(n);
            }
    }
    None
}

fn split_servers(server: &str) -> Vec<String> {
    server
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_connection_string() {
        let opt = RedisOptions::from_config(
            "server=127.0.0.1:6379;password=123456;db=3;timeout=5000;prefix=app:;PoolMax=100",
        )
        .unwrap();

        assert_eq!(opt.endpoints(), vec!["127.0.0.1:6379"]);
        assert_eq!(opt.password.as_deref(), Some("123456"));
        assert_eq!(opt.db, 3);
        assert_eq!(opt.timeout_ms, 5000);
        assert_eq!(opt.prefix.as_deref(), Some("app:"));
        assert_eq!(opt.pool.max, 100);
    }

    #[test]
    fn parse_legacy_comma_config() {
        let opt = RedisOptions::from_config("server=127.0.0.1:6379,password=abcdef,db=1").unwrap();
        assert_eq!(opt.endpoints(), vec!["127.0.0.1:6379"]);
        assert_eq!(opt.password.as_deref(), Some("abcdef"));
        assert_eq!(opt.db, 1);
    }

    #[test]
    fn parse_multi_servers_and_default_port() {
        let opt = RedisOptions::from_config("server=10.0.0.1,10.0.0.2:6380;password=x").unwrap();
        assert_eq!(opt.endpoints(), vec!["10.0.0.1:6379", "10.0.0.2:6380"]);
    }

    #[test]
    fn separate_port_key_is_applied() {
        let opt = RedisOptions::from_config("server=redis.local;port=6380").unwrap();
        assert_eq!(opt.endpoints(), vec!["redis.local:6380"]);
    }

    #[test]
    fn missing_server_is_error() {
        let err = RedisOptions::from_config("password=123").unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }

    #[test]
    fn tcp_scheme_is_stripped() {
        let opt = RedisOptions::new("tcp://127.0.0.1:6379", None, 0);
        assert_eq!(opt.endpoints(), vec!["127.0.0.1:6379"]);
    }

    #[test]
    fn pool_config_loads_all_keys() {
        let opt = RedisOptions::from_config(
            "server=127.0.0.1;PoolMin=1;PoolMax=50;PoolIdleTime=10;MaxLifetime=60;WaitTimeout=5",
        )
        .unwrap();
        assert_eq!(opt.pool.min, 1);
        assert_eq!(opt.pool.max, 50);
        assert_eq!(opt.pool.idle_time, 10);
        assert_eq!(opt.pool.max_lifetime, 60);
        assert_eq!(opt.pool.wait_timeout, 5);
    }
}
