//! 服务扩展：RedLock 分布式锁（对应 DH.NRedis `Services.RedisRedLock`）。
//!
//! 算法与 C# 完全一致：
//! - 令牌为 22 位随机字符串；
//! - 依次在全部实例上以「秒级过期」（`ms_expire / 1000`，整数除法）写入令牌；
//! - `quorum = n / 2 + 1` 个实例成功，且 `ms_expire - 耗时 - ms_expire * 1% > 0` 时视为获取成功；
//! - 失败时回滚已写实例，按 `RetryDelay(200ms) + rand(0..50ms)` 重试直到 `ms_timeout` 超时；
//! - 释放时使用与 C# 相同的 Lua 脚本（比较令牌后删除，避免误删他人锁）。
//!
//! > 注意：与 C# 保持一致，加锁写入使用的是**普通 `SET`（非 `NX`）**。
//! > 这是 DH.NRedis 的原样行为（互操作需要一致），其安全性依赖业务方对
//! > 「同一 key 的 RedLock 只由一个语言端持有」的约定。

use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::full::FullRedis;

/// 解锁 Lua（与 C# `RedisRedLock.TryUnlock` 完全相同）。
const UNLOCK_SCRIPT: &str = "if redis.call('get', KEYS[1]) == ARGV[1] then\n    return redis.call('del', KEYS[1])\nelse\n    return 0\nend";

/// RedLock 分布式锁句柄。`Drop` 时自动在已加锁实例上释放。
pub struct RedLock {
    key: String,
    token: String,
    instances: Vec<FullRedis>,
    clock_drift_factor: f64,
    retry_delay_ms: i64,
}

impl RedLock {
    /// 锁键名（已含前缀）。
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 随机令牌。
    pub fn token(&self) -> &str {
        &self.token
    }

    /// 成功加锁的实例数量。
    pub fn locked_count(&self) -> usize {
        self.instances.len()
    }

    /// 时钟漂移补偿因子（与 C# 默认一致：0.01 = 1%）。
    pub fn clock_drift_factor(&self) -> f64 {
        self.clock_drift_factor
    }

    /// 重试延迟毫秒（与 C# 默认一致：200）。
    pub fn retry_delay_ms(&self) -> i64 {
        self.retry_delay_ms
    }

    /// 主动释放所有实例上的锁（等价 C# `Dispose`）。
    pub fn release(&mut self) {
        for rds in self.instances.drain(..) {
            unlock_instance(&rds, &self.key, &self.token);
        }
    }
}

impl Drop for RedLock {
    fn drop(&mut self) {
        self.release();
    }
}

/// 在多个独立实例上获取 RedLock（对应 C# `RedisRedLock.Acquire`）。
///
/// - `instances`：建议 3 个以上奇数个独立实例；`key` 需为已含前缀的完整键名；
/// - `ms_timeout` / `ms_expire`：获取锁超时与锁有效期（毫秒），必须 > 0；
/// - 获取失败返回 `Ok(None)`（超时语义与 C# 一致）。
pub fn acquire_red_lock(
    instances: &[FullRedis],
    key: &str,
    ms_timeout: i64,
    ms_expire: i64,
) -> Result<Option<RedLock>> {
    if instances.is_empty() {
        return Err(Error::Config("RedLock 需要至少一个 Redis 实例".into()));
    }
    if key.is_empty() {
        return Err(Error::Config("RedLock 锁键不能为空".into()));
    }
    if ms_timeout <= 0 {
        return Err(Error::Config("RedLock 超时时间必须大于 0".into()));
    }
    if ms_expire <= 0 {
        return Err(Error::Config("RedLock 过期时间必须大于 0".into()));
    }

    let token = random_token();
    let quorum = instances.len() / 2 + 1;
    let clock_drift_factor = 0.01f64;
    let retry_delay_ms = 200i64;
    let expire_seconds = ms_expire / 1000;
    let start = Instant::now();

    let mut locked: Vec<FullRedis> = Vec::with_capacity(instances.len());

    loop {
        let lock_start = Instant::now();
        locked.clear();

        // 尝试在每个实例上加锁（单实例失败不中断）
        for rds in instances {
            if let Ok(true) =
                rds.redis()
                    .set(key, token.as_str(), expire_seconds)
            {
                locked.push(rds.clone());
            }
        }

        let lock_elapsed = lock_start.elapsed().as_millis() as i64;
        let validity = ms_expire - lock_elapsed - (ms_expire as f64 * clock_drift_factor) as i64;
        if locked.len() >= quorum && validity > 0 {
            return Ok(Some(RedLock {
                key: key.to_string(),
                token,
                instances: std::mem::take(&mut locked),
                clock_drift_factor,
                retry_delay_ms,
            }));
        }

        // 加锁失败，释放已获得的锁
        for rds in locked.drain(..) {
            unlock_instance(&rds, key, &token);
        }

        let elapsed = start.elapsed().as_millis() as i64;
        if elapsed >= ms_timeout {
            return Ok(None);
        }

        // 等待后重试（+50ms 以内的随机抖动）
        let jitter = {
            use rand::Rng;
            rand::thread_rng().gen_range(0..50)
        };
        let delay = (retry_delay_ms + jitter).min(ms_timeout - elapsed).max(1);
        std::thread::sleep(Duration::from_millis(delay as u64));
    }
}

/// 使用比较令牌的 Lua 脚本解锁（失败不抛异常，与 C# `TryUnlock` 一致）。
fn unlock_instance(rds: &FullRedis, key: &str, token: &str) {
    let _ = rds.redis().eval_raw(UNLOCK_SCRIPT, &[key], &[token]);
}

/// 22 位随机令牌（对应 C# `Rand.NextString(22)`）。
fn random_token() -> String {
    use rand::Rng;
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..22)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}
