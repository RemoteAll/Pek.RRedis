//! 有序集合（对应 DH.NRedis `RedisSortedSet<T>`）。
//!
//! 成员经编码器转换；分数为 `f64`，与 C# 端互读时成员字节必须一致（字符串成员天然互通）。

use std::marker::PhantomData;

use crate::encoder::{FromRedisPayload, ToRedisPayload, format_f64};
use crate::error::Result;
use crate::full::FullRedis;
use crate::util::{decode_array, decode_scored, int_or, payload};

/// Redis 有序集合。
pub struct RedisSortedSet<V> {
    redis: FullRedis,
    key: String,
    _marker: PhantomData<fn() -> V>,
}

impl<V> RedisSortedSet<V>
where
    V: FromRedisPayload + ToRedisPayload,
{
    /// 由工厂方法创建（[`FullRedis::get_sorted_set`]）。键自动补前缀。
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

    /// 元素个数（`ZCARD`）。
    pub fn len(&self) -> Result<i64> {
        Ok(int_or(self.redis.redis().execute(&[b"ZCARD", self.key.as_bytes()])?, 0))
    }

    /// 是否为空。
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// 单个添加（`ZADD`），返回新增数量。
    pub fn add(&self, member: &V, score: f64) -> Result<i64> {
        let member = payload(member)?;
        Ok(int_or(
            self.redis.redis().execute(&[
                b"ZADD",
                self.key.as_bytes(),
                format_f64(score).as_bytes(),
                &member,
            ])?,
            0,
        ))
    }

    /// 批量添加同一分数（`ZADD`）。
    pub fn add_many(&self, members: &[V], score: f64) -> Result<i64> {
        if members.is_empty() {
            return Ok(0);
        }
        let score = format_f64(score);
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(members.len() * 2 + 2);
        args.push(b"ZADD".to_vec());
        args.push(self.key.as_bytes().to_vec());
        for m in members {
            args.push(score.clone().into_bytes());
            args.push(payload(m)?);
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.redis().execute(&refs)?, 0))
    }

    /// 删除成员（`ZREM`）。
    pub fn remove(&self, members: &[V]) -> Result<i64> {
        if members.is_empty() {
            return Ok(0);
        }
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(members.len() + 2);
        args.push(b"ZREM".to_vec());
        args.push(self.key.as_bytes().to_vec());
        for m in members {
            args.push(payload(m)?);
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.redis().execute(&refs)?, 0))
    }

    /// 获取分数（`ZSCORE`）。
    pub fn score(&self, member: &V) -> Result<Option<f64>> {
        let member = payload(member)?;
        let rs = self
            .redis
            .redis()
            .execute(&[b"ZSCORE", self.key.as_bytes(), &member])?;
        Ok(rs.as_f64())
    }

    /// 成员自增分数（`ZINCRBY`）。
    pub fn increment(&self, member: &V, score: f64) -> Result<f64> {
        let member = payload(member)?;
        let rs = self.redis.redis().execute(&[
            b"ZINCRBY",
            self.key.as_bytes(),
            format_f64(score).as_bytes(),
            &member,
        ])?;
        Ok(rs.as_f64().unwrap_or(0.0))
    }

    /// 分数区间计数（`ZCOUNT`）。
    pub fn count(&self, min: f64, max: f64) -> Result<i64> {
        Ok(int_or(
            self.redis.redis().execute(&[
                b"ZCOUNT",
                self.key.as_bytes(),
                format_f64(min).as_bytes(),
                format_f64(max).as_bytes(),
            ])?,
            0,
        ))
    }

    /// 排名区间获取（`ZRANGE`，按分数升序）。
    pub fn range(&self, start: i64, stop: i64) -> Result<Vec<V>> {
        let rs = self.redis.redis().execute(&[
            b"ZRANGE",
            self.key.as_bytes(),
            start.to_string().as_bytes(),
            stop.to_string().as_bytes(),
        ])?;
        Ok(decode_array(rs))
    }

    /// 排名区间获取（含分数）。
    pub fn range_with_scores(&self, start: i64, stop: i64) -> Result<Vec<(V, f64)>> {
        let rs = self.redis.redis().execute(&[
            b"ZRANGE",
            self.key.as_bytes(),
            start.to_string().as_bytes(),
            stop.to_string().as_bytes(),
            b"WITHSCORES",
        ])?;
        Ok(decode_scored(rs))
    }

    /// 分数区间获取（`ZRANGEBYSCORE` + LIMIT，含头含尾）。
    pub fn range_by_score(&self, min: f64, max: f64, offset: i64, count: i64) -> Result<Vec<V>> {
        let rs = self.redis.redis().execute(&[
            b"ZRANGEBYSCORE",
            self.key.as_bytes(),
            format_f64(min).as_bytes(),
            format_f64(max).as_bytes(),
            b"LIMIT",
            offset.to_string().as_bytes(),
            count.to_string().as_bytes(),
        ])?;
        Ok(decode_array(rs))
    }

    /// 分数区间获取（含分数）。
    pub fn range_by_score_with_scores(
        &self,
        min: f64,
        max: f64,
        offset: i64,
        count: i64,
    ) -> Result<Vec<(V, f64)>> {
        let rs = self.redis.redis().execute(&[
            b"ZRANGEBYSCORE",
            self.key.as_bytes(),
            format_f64(min).as_bytes(),
            format_f64(max).as_bytes(),
            b"WITHSCORES",
            b"LIMIT",
            offset.to_string().as_bytes(),
            count.to_string().as_bytes(),
        ])?;
        Ok(decode_scored(rs))
    }

    /// 成员排名（`ZRANK`，0 基，分数升序），不存在返回 `None`。
    pub fn rank(&self, member: &V) -> Result<Option<i64>> {
        let member = payload(member)?;
        let rs = self
            .redis
            .redis()
            .execute(&[b"ZRANK", self.key.as_bytes(), &member])?;
        Ok(if rs.is_null() { None } else { rs.as_i64() })
    }

    /// 成员排名（`ZREVRANK`，分数降序）。
    pub fn rev_rank(&self, member: &V) -> Result<Option<i64>> {
        let member = payload(member)?;
        let rs = self
            .redis
            .redis()
            .execute(&[b"ZREVRANK", self.key.as_bytes(), &member])?;
        Ok(if rs.is_null() { None } else { rs.as_i64() })
    }

    /// 弹出分数最小的成员（`ZPOPMIN`）。
    pub fn pop_min(&self, count: i64) -> Result<Vec<(V, f64)>> {
        let rs = self.redis.redis().execute(&[
            b"ZPOPMIN",
            self.key.as_bytes(),
            count.to_string().as_bytes(),
        ])?;
        Ok(decode_scored(rs))
    }

    /// 弹出分数最大的成员（`ZPOPMAX`）。
    pub fn pop_max(&self, count: i64) -> Result<Vec<(V, f64)>> {
        let rs = self.redis.redis().execute(&[
            b"ZPOPMAX",
            self.key.as_bytes(),
            count.to_string().as_bytes(),
        ])?;
        Ok(decode_scored(rs))
    }

    /// 按排名区间删除（`ZREMRANGEBYRANK`）。
    pub fn remove_range_by_rank(&self, start: i64, stop: i64) -> Result<i64> {
        Ok(int_or(
            self.redis.redis().execute(&[
                b"ZREMRANGEBYRANK",
                self.key.as_bytes(),
                start.to_string().as_bytes(),
                stop.to_string().as_bytes(),
            ])?,
            0,
        ))
    }

    /// 按分数区间删除（`ZREMRANGEBYSCORE`）。
    pub fn remove_range_by_score(&self, min: f64, max: f64) -> Result<i64> {
        Ok(int_or(
            self.redis.redis().execute(&[
                b"ZREMRANGEBYSCORE",
                self.key.as_bytes(),
                format_f64(min).as_bytes(),
                format_f64(max).as_bytes(),
            ])?,
            0,
        ))
    }

    /// 扫描全部成员（`ZSCAN` 循环），返回 `(成员, 分数)`。
    pub fn scan_all(&self, pattern: &str, count: usize) -> Result<Vec<(V, f64)>> {
        let mut result = Vec::new();
        let mut cursor = 0u64;
        loop {
            let rs = self.redis.redis().execute(&[
                b"ZSCAN",
                self.key.as_bytes(),
                cursor.to_string().as_bytes(),
                b"MATCH",
                pattern.as_bytes(),
                b"COUNT",
                (if count == 0 { 1000 } else { count }).to_string().as_bytes(),
            ])?;

            let mut items = rs
                .into_array()
                .ok_or_else(|| crate::error::Error::Protocol("ZSCAN 返回结构非法".into()))?;
            if items.len() != 2 {
                return Err(crate::error::Error::Protocol("ZSCAN 返回结构非法".into()));
            }

            let list = items.pop().unwrap();
            cursor = items
                .pop()
                .and_then(|v| v.as_string())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            result.extend(decode_scored(list));
            if cursor == 0 {
                break;
            }
        }
        Ok(result)
    }
}
