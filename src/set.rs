//! 集合结构（对应 DH.NRedis `RedisSet<T>`）。

use std::marker::PhantomData;

use crate::encoder::{FromRedisPayload, ToRedisPayload};
use crate::error::Result;
use crate::full::FullRedis;
use crate::util::{decode_array, int_or, payload};

/// Redis 集合。
pub struct RedisSet<V> {
    redis: FullRedis,
    key: String,
    _marker: PhantomData<fn() -> V>,
}

impl<V> RedisSet<V>
where
    V: FromRedisPayload + ToRedisPayload,
{
    /// 由工厂方法创建（[`FullRedis::get_set`]）。键自动补前缀。
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

    /// 元素个数（`SCARD`）。
    pub fn len(&self) -> Result<i64> {
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"SCARD", self.key.as_bytes()])?,
            0,
        ))
    }

    /// 是否为空。
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// 添加元素（`SADD`），返回新增数量。
    pub fn add(&self, members: &[V]) -> Result<i64> {
        self.batch(b"SADD", members)
    }

    /// 删除元素（`SREM`），返回删除数量。
    pub fn remove(&self, members: &[V]) -> Result<i64> {
        self.batch(b"SREM", members)
    }

    fn batch(&self, cmd: &[u8], members: &[V]) -> Result<i64> {
        if members.is_empty() {
            return Ok(0);
        }
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(members.len() + 2);
        args.push(cmd.to_vec());
        args.push(self.key.as_bytes().to_vec());
        for m in members {
            args.push(payload(m)?);
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.redis().execute(&refs)?, 0))
    }

    /// 是否包含元素（`SISMEMBER`）。
    pub fn contains(&self, member: &V) -> Result<bool> {
        let m = payload(member)?;
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"SISMEMBER", self.key.as_bytes(), &m])?,
            0,
        ) > 0)
    }

    /// 批量成员存在性（`SMISMEMBER`，对应 C# `SMIsMember`），返回顺序与输入一致。
    pub fn mismember(&self, members: &[V]) -> Result<Vec<bool>> {
        if members.is_empty() {
            return Ok(Vec::new());
        }
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(members.len() + 2);
        args.push(b"SMISMEMBER".to_vec());
        args.push(self.key.as_bytes().to_vec());
        for m in members {
            args.push(payload(m)?);
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        let rs = self.redis.redis().execute(&refs)?;
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .map(|v| v.as_i64().unwrap_or(0) != 0)
            .collect())
    }

    /// 全部元素（`SMEMBERS`）。
    pub fn members(&self) -> Result<Vec<V>> {
        let rs = self
            .redis
            .redis()
            .execute(&[b"SMEMBERS", self.key.as_bytes()])?;
        Ok(decode_array(rs))
    }

    /// 随机弹出元素（`SPOP`）。
    pub fn pop(&self, count: i64) -> Result<Vec<V>> {
        let rs = self.redis.redis().execute(&[
            b"SPOP",
            self.key.as_bytes(),
            count.to_string().as_bytes(),
        ])?;
        Ok(decode_array(rs))
    }

    /// 随机获取元素（`SRANDMEMBER`，不删除）。
    pub fn random_get(&self, count: i64) -> Result<Vec<V>> {
        let rs = self.redis.redis().execute(&[
            b"SRANDMEMBER",
            self.key.as_bytes(),
            count.to_string().as_bytes(),
        ])?;
        Ok(decode_array(rs))
    }

    /// 移动元素到目标集合（`SMOVE`）。
    pub fn move_to(&self, destination: &str, member: &V) -> Result<bool> {
        let dest = self.redis.get_key(destination);
        let m = payload(member)?;
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"SMOVE", self.key.as_bytes(), dest.as_bytes(), &m])?,
            0,
        ) > 0)
    }

    /// 差集（`SDIFF`）。
    pub fn diff(&self, keys: &[&str]) -> Result<Vec<V>> {
        let rs = self.set_op(b"SDIFF", keys)?;
        Ok(decode_array(rs))
    }

    /// 差集存储（`SDIFFSTORE`）。
    pub fn diff_store(&self, destination: &str, keys: &[&str]) -> Result<i64> {
        self.store_op(b"SDIFFSTORE", destination, keys)
    }

    /// 交集（`SINTER`）。
    pub fn inter(&self, keys: &[&str]) -> Result<Vec<V>> {
        let rs = self.set_op(b"SINTER", keys)?;
        Ok(decode_array(rs))
    }

    /// 交集存储（`SINTERSTORE`）。
    pub fn inter_store(&self, destination: &str, keys: &[&str]) -> Result<i64> {
        self.store_op(b"SINTERSTORE", destination, keys)
    }

    /// 并集（`SUNION`）。
    pub fn union(&self, keys: &[&str]) -> Result<Vec<V>> {
        let rs = self.set_op(b"SUNION", keys)?;
        Ok(decode_array(rs))
    }

    /// 并集存储（`SUNIONSTORE`）。
    pub fn union_store(&self, destination: &str, keys: &[&str]) -> Result<i64> {
        self.store_op(b"SUNIONSTORE", destination, keys)
    }

    fn set_op(&self, cmd: &[u8], keys: &[&str]) -> Result<crate::resp::RespValue> {
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(keys.len() + 2);
        args.push(cmd.to_vec());
        args.push(self.key.as_bytes().to_vec());
        for k in keys {
            args.push(self.redis.get_key(k).into_bytes());
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        self.redis.redis().execute(&refs)
    }

    fn store_op(&self, cmd: &[u8], destination: &str, keys: &[&str]) -> Result<i64> {
        let dest = self.redis.get_key(destination);
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(keys.len() + 3);
        args.push(cmd.to_vec());
        args.push(dest.into_bytes());
        args.push(self.key.as_bytes().to_vec());
        for k in keys {
            args.push(self.redis.get_key(k).into_bytes());
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.redis().execute(&refs)?, 0))
    }

    /// 扫描全部元素（`SSCAN` 循环）。
    pub fn scan_all(&self, pattern: &str, count: usize) -> Result<Vec<V>> {
        let mut result = Vec::new();
        let mut cursor = 0u64;
        loop {
            let rs = self.redis.redis().execute(&[
                b"SSCAN",
                self.key.as_bytes(),
                cursor.to_string().as_bytes(),
                b"MATCH",
                pattern.as_bytes(),
                b"COUNT",
                (if count == 0 { 1000 } else { count })
                    .to_string()
                    .as_bytes(),
            ])?;

            let mut items = rs
                .into_array()
                .ok_or_else(|| crate::error::Error::Protocol("SSCAN 返回结构非法".into()))?;
            if items.len() != 2 {
                return Err(crate::error::Error::Protocol("SSCAN 返回结构非法".into()));
            }

            let list = items.pop().unwrap();
            cursor = items
                .pop()
                .and_then(|v| v.as_string())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            result.extend(decode_array(list));
            if cursor == 0 {
                break;
            }
        }
        Ok(result)
    }
}
