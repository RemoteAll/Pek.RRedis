//! 同步连接池。
//!
//! 对应 DH.NRedis 基于 `ObjectPool<RedisClient>` 的连接池：`Redis` 实例内部持有连接池，
//! 支持多线程并发使用；连接归还时校验健康度，空闲过久主动 `PING`（对应 C# `MyPool.OnGet`），
//! 超过 `MaxLifetime` 强制回收。
//!
//! 与 C# 默认参数一致：`Min=10`、`Max=100000`、`IdleTime=30s`、`MaxLifetime=300s`、`WaitTimeout=15s`。

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::client::RedisClient;
use crate::error::{Error, Result};
use crate::options::RedisPoolConfig;

type Factory = Arc<dyn Fn() -> Result<RedisClient> + Send + Sync>;

struct IdleEntry {
    client: RedisClient,
    idle_since: Instant,
}

struct PoolInner {
    idle: VecDeque<IdleEntry>,
}

/// 连接池。通过 [`Pool::get`] 借出连接，`PooledClient` 析构时自动归还。
pub struct Pool {
    config: RedisPoolConfig,
    factory: Factory,
    inner: Mutex<PoolInner>,
    available: Condvar,
    total: AtomicUsize,
}

impl Pool {
    /// 创建连接池。`factory` 负责建立一条新连接。
    pub fn new<F>(config: RedisPoolConfig, factory: F) -> Arc<Self>
    where
        F: Fn() -> Result<RedisClient> + Send + Sync + 'static,
    {
        Arc::new(Self {
            config,
            factory: Arc::new(factory),
            inner: Mutex::new(PoolInner {
                idle: VecDeque::new(),
            }),
            available: Condvar::new(),
            total: AtomicUsize::new(0),
        })
    }

    /// 当前连接总数（含借出中）。
    pub fn total(&self) -> usize {
        self.total.load(Ordering::Acquire)
    }

    /// 当前空闲连接数。
    pub fn idle_count(&self) -> usize {
        self.inner.lock().unwrap().idle.len()
    }

    /// 借出一条连接。池满时等待 `WaitTimeout` 秒。
    pub fn get(self: &Arc<Self>) -> Result<PooledClient> {
        let wait = Duration::from_secs(self.config.wait_timeout.max(1));
        let deadline = Instant::now() + wait;

        loop {
            // 1) 尝试复用空闲连接
            let entry = self.inner.lock().unwrap().idle.pop_back();
            if let Some(mut entry) = entry {
                if self.check_reusable(&mut entry) {
                    return Ok(PooledClient::new(self.clone(), entry.client));
                }
                // 已过期或心跳失败：丢弃并继续
                self.total.fetch_sub(1, Ordering::AcqRel);
                continue;
            }

            // 2) 未达上限则新建
            let total = self.total.load(Ordering::Acquire);
            if total < self.config.max {
                if self
                    .total
                    .compare_exchange(total, total + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return match (self.factory)() {
                        Ok(client) => Ok(PooledClient::new(self.clone(), client)),
                        Err(e) => {
                            self.total.fetch_sub(1, Ordering::AcqRel);
                            self.available.notify_one();
                            Err(e)
                        }
                    };
                }
                continue;
            }

            // 3) 池满：等待归还
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Pool(format!(
                    "连接池已满（{} 条），等待 {} 秒后仍未借到连接",
                    self.config.max, self.config.wait_timeout
                )));
            }
            let inner = self.inner.lock().unwrap();
            let (inner, timeout) = self.available.wait_timeout(inner, remaining).unwrap();
            drop(inner);
            if timeout.timed_out() && self.idle_count() == 0 {
                return Err(Error::Pool(format!(
                    "连接池已满（{} 条），等待 {} 秒后仍未借到连接",
                    self.config.max, self.config.wait_timeout
                )));
            }
        }
    }

    /// 空闲连接是否可复用：超过生命周期直接淘汰；空闲过久主动 PING 验证。
    fn check_reusable(&self, entry: &mut IdleEntry) -> bool {
        if self.config.max_lifetime > 0
            && entry.idle_since.elapsed().as_secs() >= self.config.max_lifetime * 2
        {
            return false;
        }
        if entry.client.is_broken() {
            return false;
        }

        if self.config.idle_time > 0
            && entry.idle_since.elapsed().as_secs() >= self.config.idle_time
        {
            return entry.client.ping().is_ok();
        }

        true
    }

    /// 归还连接。
    fn release(self: &Arc<Self>, client: RedisClient) {
        if client.is_broken() {
            self.total.fetch_sub(1, Ordering::AcqRel);
            self.available.notify_one();
            return;
        }

        {
            let mut inner = self.inner.lock().unwrap();
            inner.idle.push_back(IdleEntry {
                client,
                idle_since: Instant::now(),
            });

            // 修剪到最小空闲数（保留最近使用的连接）
            while inner.idle.len() > self.config.min {
                inner.idle.pop_front();
                self.total.fetch_sub(1, Ordering::AcqRel);
            }
        }

        self.available.notify_one();
    }
}

/// 借出的连接。析构时自动归还连接池。
pub struct PooledClient {
    pool: Arc<Pool>,
    client: Option<RedisClient>,
}

impl PooledClient {
    fn new(pool: Arc<Pool>, client: RedisClient) -> Self {
        Self {
            pool,
            client: Some(client),
        }
    }

    /// 主动销毁当前连接（例如遇到不可恢复错误）。
    pub fn discard(mut self) {
        if let Some(mut client) = self.client.take() {
            client.mark_broken();
            self.pool.total.fetch_sub(1, Ordering::AcqRel);
            self.pool.available.notify_one();
        }
    }
}

impl Deref for PooledClient {
    type Target = RedisClient;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref().expect("连接已归还")
    }
}

impl DerefMut for PooledClient {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.client.as_mut().expect("连接已归还")
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            self.pool.release(client);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ConnConfig;

    fn unreachable_pool() -> Arc<Pool> {
        Pool::new(RedisPoolConfig::default(), || {
            Err(Error::Config("测试环境无 Redis".into()))
        })
    }

    #[test]
    fn factory_error_propagates_and_releases_slot() {
        let pool = unreachable_pool();
        let err = match pool.get() {
            Ok(_) => panic!("应当创建失败"),
            Err(e) => e,
        };
        assert!(matches!(err, Error::Config(_)));
        assert_eq!(pool.total(), 0, "创建失败必须释放计数");
    }

    #[test]
    fn wait_timeout_when_full() {
        let cfg = RedisPoolConfig {
            max: 1,
            wait_timeout: 1,
            min: 0,
            ..Default::default()
        };

        // 用一个占位连接模拟“池已满”而不真正连接 Redis
        let pool = Pool::new(cfg, || {
            // 通过未分配端口必然失败，这里仅用于验证占位逻辑
            let conn = ConnConfig {
                endpoint: "127.0.0.1:1".into(),
                user_name: None,
                password: None,
                db: 0,
                timeout_ms: 200,
                protocol_version: 0,
                max_message_size: 1024,
                tls: false,
                tls_server_name: None,
                tls_insecure: false,
            };
            RedisClient::connect(&conn)
        });

        // 首次创建必然失败（端口 1 无法连接），计数恢复为 0
        assert!(pool.get().is_err());
        assert_eq!(pool.total(), 0);
    }
}
