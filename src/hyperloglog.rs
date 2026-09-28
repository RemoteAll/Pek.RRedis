//! 基数统计 HyperLogLog（对应 DH.NRedis `HyperLogLog`）。

use crate::error::Result;
use crate::full::FullRedis;
use crate::util::int_or;

/// HyperLogLog 结构。用于海量去重计数（误差约 0.81%）。
pub struct HyperLogLog {
    redis: FullRedis,
    key: String,
}

impl HyperLogLog {
    /// 由工厂方法创建（[`FullRedis::get_hyper_log_log`]）。键自动补前缀。
    pub fn new(redis: FullRedis, key: &str) -> Self {
        let key = redis.get_key(key);
        Self { redis, key }
    }

    /// 实际键名（含前缀）。
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 添加元素（`PFADD`），返回是否产生了变化。
    pub fn add(&self, items: &[&str]) -> Result<bool> {
        if items.is_empty() {
            return Ok(false);
        }
        let mut args: Vec<&[u8]> = Vec::with_capacity(items.len() + 2);
        args.push(b"PFADD");
        args.push(self.key.as_bytes());
        for item in items {
            args.push(item.as_bytes());
        }
        Ok(int_or(self.redis.redis().execute(&args)?, 0) > 0)
    }

    /// 估算基数（`PFCOUNT`）。
    pub fn count(&self) -> Result<i64> {
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"PFCOUNT", self.key.as_bytes()])?,
            0,
        ))
    }

    /// 合并多个键到当前键（`PFMERGE`）。
    pub fn merge(&self, keys: &[&str]) -> Result<bool> {
        if keys.is_empty() {
            return Ok(false);
        }
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(keys.len() + 2);
        args.push(b"PFMERGE".to_vec());
        args.push(self.key.as_bytes().to_vec());
        for k in keys {
            args.push(self.redis.get_key(k).into_bytes());
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(self.redis.redis().execute(&refs)?.as_string().as_deref() == Some("OK"))
    }
}
