//! 栈结构（对应 DH.NRedis `RedisStack<T>`：右进右出，后进先出）。

use std::marker::PhantomData;

use crate::encoder::{FromRedisPayload, ToRedisPayload};
use crate::error::Result;
use crate::full::FullRedis;
use crate::util::{decode, int_or, payload};

/// Redis 栈（基于列表实现，`RPUSH` 进、`RPOP` 出）。
pub struct RedisStack<V> {
    redis: FullRedis,
    key: String,
    _marker: PhantomData<fn() -> V>,
}

impl<V> RedisStack<V>
where
    V: FromRedisPayload + ToRedisPayload,
{
    /// 由工厂方法创建（[`FullRedis::get_stack`]）。键自动补前缀。
    pub fn new(redis: FullRedis, key: &str) -> Self {
        let key = redis.get_key(key);
        Self {
            redis,
            key,
            _marker: PhantomData,
        }
    }

    /// 实际键名（含前缀）。
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 元素个数（`LLEN`）。
    pub fn len(&self) -> Result<i64> {
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"LLEN", self.key.as_bytes()])?,
            0,
        ))
    }

    /// 是否为空。
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// 压栈（`RPUSH`），返回栈深度。
    pub fn push(&self, value: &V) -> Result<i64> {
        let value = payload(value)?;
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"RPUSH", self.key.as_bytes(), &value])?,
            0,
        ))
    }

    /// 批量压栈（`RPUSH`）。
    pub fn push_many(&self, values: &[V]) -> Result<i64> {
        if values.is_empty() {
            return Ok(0);
        }
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(values.len() + 2);
        args.push(b"RPUSH".to_vec());
        args.push(self.key.as_bytes().to_vec());
        for v in values {
            args.push(payload(v)?);
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.redis().execute(&refs)?, 0))
    }

    /// 出栈（`RPOP`）。`timeout_seconds < 0` 时立即返回，否则阻塞等待。
    pub fn take_one(&self, timeout_seconds: i64) -> Result<Option<V>> {
        if timeout_seconds < 0 {
            let rs = self
                .redis
                .redis()
                .execute(&[b"RPOP", self.key.as_bytes()])?;
            return Ok(decode(rs));
        }

        let rs = self.redis.redis().execute_blocking(
            &[
                b"BRPOP",
                self.key.as_bytes(),
                timeout_seconds.to_string().as_bytes(),
            ],
            timeout_seconds,
        )?;

        let mut items = rs.into_array().unwrap_or_default();
        if items.len() < 2 {
            return Ok(None);
        }
        Ok(decode(items.pop().unwrap()))
    }

    /// 批量出栈（最多 `count` 个）。
    pub fn take(&self, count: usize) -> Result<Vec<V>> {
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            match self.take_one(-1)? {
                Some(v) => result.push(v),
                None => break,
            }
        }
        Ok(result)
    }
}
