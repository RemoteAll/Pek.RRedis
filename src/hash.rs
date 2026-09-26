//! 哈希结构（对应 DH.NRedis `RedisHash<TKey, TValue>`）。
//!
//! 字段与值都经编码器转换（默认：字符串原样、数字转文本、复杂对象 JSON），
//! 与 C# 端读写同一 Redis 哈希时格式一致。

use std::collections::HashMap;
use std::marker::PhantomData;

use crate::encoder::{FromRedisPayload, ToRedisPayload};
use crate::error::Result;
use crate::full::FullRedis;
use crate::util::{decode, decode_pairs, decode_bytes, int_or, payload, strings};

/// 字段与值对（值为 `None` 表示字段存在但解码失败）。
pub type FieldPairs<K, V> = Vec<(K, Option<V>)>;

/// 类型标记别名，避免在字段上直接写复杂类型。
type HashMarker<K, V> = PhantomData<(fn() -> K, fn() -> V)>;

/// Redis 哈希（字典）。
///
/// 泛型参数顺序与 C# 相反，默认字段键为 [`String`]：`RedisHash<V, K = String>`。
/// 需要自定义字段键类型时使用 [`FullRedis::get_hash_map`]。
pub struct RedisHash<V, K = String> {
    redis: FullRedis,
    key: String,
    _marker: HashMarker<K, V>,
}

impl<V, K> RedisHash<V, K>
where
    V: FromRedisPayload,
    K: FromRedisPayload,
{
    /// 由工厂方法创建（[`FullRedis::get_hash`]）。键自动补前缀。
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

    /// 字段数量（`HLEN`）。
    pub fn count(&self) -> Result<i64> {
        Ok(int_or(self.redis.redis().execute(&[b"HLEN", self.key.as_bytes()])?, 0))
    }

    /// 是否为空。
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.count()? == 0)
    }

    /// 所有字段（`HKEYS`）。
    pub fn keys(&self) -> Result<Vec<K>> {
        let rs = self.redis.redis().execute(&[b"HKEYS", self.key.as_bytes()])?;
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.as_bytes().and_then(|b| decode_bytes(&b)))
            .collect())
    }

    /// 所有值（`HVALS`）。
    pub fn values(&self) -> Result<Vec<V>> {
        let rs = self.redis.redis().execute(&[b"HVALS", self.key.as_bytes()])?;
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.as_bytes().and_then(|b| decode_bytes(&b)))
            .collect())
    }

    /// 获取字段值（`HGET`）。
    pub fn get(&self, field: &K) -> Result<Option<V>>
    where
        K: ToRedisPayload,
    {
        let field = payload(field)?;
        let rs = self
            .redis
            .redis()
            .execute(&[b"HGET", self.key.as_bytes(), &field])?;
        Ok(decode(rs))
    }

    /// 设置字段值（`HSET`），返回新增字段数。
    pub fn set(&self, field: &K, value: &V) -> Result<i64>
    where
        K: ToRedisPayload,
        V: ToRedisPayload,
    {
        let field = payload(field)?;
        let value = payload(value)?;
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"HSET", self.key.as_bytes(), &field, &value])?,
            0,
        ))
    }

    /// 是否包含字段（`HEXISTS`）。
    pub fn contains_key(&self, field: &K) -> Result<bool>
    where
        K: ToRedisPayload,
    {
        let field = payload(field)?;
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"HEXISTS", self.key.as_bytes(), &field])?,
            0,
        ) > 0)
    }

    /// 删除字段（`HDEL`），返回删除数量。
    pub fn remove(&self, fields: &[K]) -> Result<i64>
    where
        K: ToRedisPayload,
    {
        if fields.is_empty() {
            return Ok(0);
        }
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(fields.len() + 2);
        args.push(b"HDEL".to_vec());
        args.push(self.key.as_bytes().to_vec());
        for f in fields {
            args.push(payload(f)?);
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.redis().execute(&refs)?, 0))
    }

    /// 获取全部字段值（`HGETALL`），保持返回顺序。
    pub fn get_all(&self) -> Result<FieldPairs<K, V>> {
        let rs = self
            .redis
            .redis()
            .execute(&[b"HGETALL", self.key.as_bytes()])?;
        Ok(decode_pairs(rs))
    }

    /// 获取全部字段值（`HGETALL`）为字典。
    pub fn get_all_map(&self) -> Result<HashMap<K, Option<V>>>
    where
        K: std::hash::Hash + Eq,
    {
        Ok(self.get_all()?.into_iter().collect())
    }

    /// 批量获取字段（`HMGET`）。
    pub fn hmget(&self, fields: &[K]) -> Result<Vec<Option<V>>>
    where
        K: ToRedisPayload,
    {
        if fields.is_empty() {
            return Ok(Vec::new());
        }
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(fields.len() + 2);
        args.push(b"HMGET".to_vec());
        args.push(self.key.as_bytes().to_vec());
        for f in fields {
            args.push(payload(f)?);
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        let rs = self.redis.redis().execute(&refs)?;

        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .map(|v| decode(v))
            .collect())
    }

    /// 批量设置字段（`HSET` 多组），返回新增字段数。
    pub fn hmset(&self, pairs: &[(K, V)]) -> Result<i64>
    where
        K: ToRedisPayload,
        V: ToRedisPayload,
    {
        if pairs.is_empty() {
            return Ok(0);
        }
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(pairs.len() * 2 + 2);
        args.push(b"HSET".to_vec());
        args.push(self.key.as_bytes().to_vec());
        for (k, v) in pairs {
            args.push(payload(k)?);
            args.push(payload(v)?);
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.redis().execute(&refs)?, 0))
    }

    /// 字段自增（`HINCRBY`）。
    pub fn incr_by(&self, field: &K, delta: i64) -> Result<i64>
    where
        K: ToRedisPayload,
    {
        let field = payload(field)?;
        Ok(int_or(
            self.redis.redis().execute(&[
                b"HINCRBY",
                self.key.as_bytes(),
                &field,
                delta.to_string().as_bytes(),
            ])?,
            0,
        ))
    }

    /// 字段浮点自增（`HINCRBYFLOAT`）。
    pub fn incr_by_float(&self, field: &K, delta: f64) -> Result<f64>
    where
        K: ToRedisPayload,
    {
        let field = payload(field)?;
        let rs = self.redis.redis().execute(&[
            b"HINCRBYFLOAT",
            self.key.as_bytes(),
            &field,
            crate::encoder::format_f64(delta).as_bytes(),
        ])?;
        Ok(rs.as_f64().unwrap_or(0.0))
    }

    /// 字段不存在时设置（`HSETNX`）。
    pub fn set_nx(&self, field: &K, value: &V) -> Result<i64>
    where
        K: ToRedisPayload,
        V: ToRedisPayload,
    {
        let field = payload(field)?;
        let value = payload(value)?;
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"HSETNX", self.key.as_bytes(), &field, &value])?,
            0,
        ))
    }

    /// 字段值字节长度（`HSTRLEN`）。
    pub fn strlen(&self, field: &K) -> Result<i64>
    where
        K: ToRedisPayload,
    {
        let field = payload(field)?;
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"HSTRLEN", self.key.as_bytes(), &field])?,
            0,
        ))
    }

    /// 随机字段（`HRANDFIELD`，Redis 6.2+）。
    pub fn random_field(&self, count: i64, with_values: bool) -> Result<Vec<String>> {
        let rs = if with_values {
            self.redis.redis().execute(&[
                b"HRANDFIELD",
                self.key.as_bytes(),
                count.to_string().as_bytes(),
                b"WITHVALUES",
            ])?
        } else {
            self.redis.redis().execute(&[
                b"HRANDFIELD",
                self.key.as_bytes(),
                count.to_string().as_bytes(),
            ])?
        };
        Ok(strings(rs))
    }

    /// 单步扫描（`HSCAN`）：返回 `(下一个游标, 键值对)`。
    pub fn scan(
        &self,
        cursor: u64,
        pattern: &str,
        count: usize,
    ) -> Result<(u64, FieldPairs<K, V>)> {
        let count = if count == 0 { 100 } else { count };
        let rs = self.redis.redis().execute(&[
            b"HSCAN",
            self.key.as_bytes(),
            cursor.to_string().as_bytes(),
            b"MATCH",
            pattern.as_bytes(),
            b"COUNT",
            count.to_string().as_bytes(),
        ])?;

        let mut items = rs
            .into_array()
            .ok_or_else(|| crate::error::Error::Protocol("HSCAN 返回结构非法".into()))?;
        if items.len() != 2 {
            return Err(crate::error::Error::Protocol("HSCAN 返回结构非法".into()));
        }

        let list = items.pop().unwrap();
        let next = items
            .pop()
            .and_then(|v| v.as_string())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        Ok((next, decode_pairs(list)))
    }

    /// 扫描全部匹配字段（`HSCAN` 循环）。
    pub fn search(&self, pattern: &str, count: usize) -> Result<Vec<(K, V)>> {
        let mut result = Vec::new();
        let mut cursor = 0u64;
        loop {
            let (next, pairs) = self.scan(cursor, pattern, if count == 0 { 1000 } else { count })?;
            for (k, v) in pairs {
                if let Some(v) = v {
                    result.push((k, v));
                }
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        Ok(result)
    }
}
