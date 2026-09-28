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
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"ZCARD", self.key.as_bytes()])?,
            0,
        ))
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
                (if count == 0 { 1000 } else { count })
                    .to_string()
                    .as_bytes(),
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

    // ================== 集合运算与扩展（Redis 6.2+，对齐 C# RedisSortedSet） ==================

    /// 带选项添加（`ZADD key [XX|NX|CH|INCR] score member ...`，对应 C# `Add(options, members)`）。
    ///
    /// `options` 仅透传 `XX`/`NX`/`CH`/`INCR`（大小写不敏感），其余忽略（与 C# 校验一致）。
    pub fn add_with_options(&self, options: &str, items: &[(f64, V)]) -> Result<f64> {
        if items.is_empty() {
            return Ok(0.0);
        }
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(items.len() * 2 + 3);
        args.push(b"ZADD".to_vec());
        args.push(self.key.as_bytes().to_vec());
        let opt = options.trim();
        if !opt.is_empty()
            && matches!(
                opt.to_ascii_uppercase().as_str(),
                "XX" | "NX" | "CH" | "INCR"
            )
        {
            args.push(opt.as_bytes().to_vec());
        }
        for (score, m) in items {
            args.push(format_f64(*score).into_bytes());
            args.push(payload(m)?);
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(self.redis.redis().execute(&refs)?.as_f64().unwrap_or(0.0))
    }

    /// 范围存储（`ZRANGESTORE`，Redis 6.2+，对应 C# `RangeStore`），返回存储的元素数量。
    ///
    /// `by_score=true` 时 min/max 为分数（`BYSCORE`），否则为排名；`count > 0` 时附加 `LIMIT offset count`。
    #[allow(clippy::too_many_arguments)]
    pub fn range_store(
        &self,
        destination: &str,
        min: f64,
        max: f64,
        by_score: bool,
        rev: bool,
        offset: i64,
        count: i64,
    ) -> Result<i64> {
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(12);
        args.push(b"ZRANGESTORE".to_vec());
        args.push(self.redis.get_key(destination).into_bytes());
        args.push(self.key.as_bytes().to_vec());
        args.push(format_f64(min).into_bytes());
        args.push(format_f64(max).into_bytes());
        if by_score {
            args.push(b"BYSCORE".to_vec());
        }
        if rev {
            args.push(b"REV".to_vec());
        }
        if count > 0 {
            args.push(b"LIMIT".to_vec());
            args.push(offset.to_string().into_bytes());
            args.push(count.to_string().into_bytes());
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.redis().execute(&refs)?, 0))
    }

    /// 差集（`ZDIFF`，Redis 6.2+，对应 C# `Diff`）。
    pub fn diff(&self, keys: &[&str]) -> Result<Vec<V>> {
        let rs = self.aggregate_keys(b"ZDIFF", keys, None, None, false)?;
        Ok(decode_array(rs))
    }

    /// 差集存储（`ZDIFFSTORE`，Redis 6.2+，对应 C# `DiffStore`），返回目标集合元素数量。
    pub fn diff_store(&self, destination: &str, keys: &[&str]) -> Result<i64> {
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(keys.len() + 4);
        args.push(b"ZDIFFSTORE".to_vec());
        args.push(self.redis.get_key(destination).into_bytes());
        args.push((keys.len() + 1).to_string().into_bytes());
        args.push(self.key.as_bytes().to_vec());
        for k in keys {
            args.push(self.redis.get_key(k).into_bytes());
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.redis().execute(&refs)?, 0))
    }

    /// 并集（`ZUNION`，Redis 6.2+，对应 C# `Union`），支持权重与聚合方式。
    pub fn union(
        &self,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Option<&str>,
    ) -> Result<Vec<V>> {
        let rs = self.aggregate_keys(b"ZUNION", keys, weights, aggregate, false)?;
        Ok(decode_array(rs))
    }

    /// 并集（含分数，`ZUNION ... WITHSCORES`）。
    pub fn union_with_scores(
        &self,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Option<&str>,
    ) -> Result<Vec<(V, f64)>> {
        let rs = self.aggregate_keys(b"ZUNION", keys, weights, aggregate, true)?;
        Ok(decode_scored(rs))
    }

    /// 并集存储（`ZUNIONSTORE`，Redis 6.2+），返回目标集合元素数量。
    pub fn union_store(
        &self,
        destination: &str,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Option<&str>,
    ) -> Result<i64> {
        self.aggregate_store(b"ZUNIONSTORE", destination, keys, weights, aggregate)
    }

    /// 交集（`ZINTER`，Redis 6.2+，对应 C# `Inter`），支持权重与聚合方式。
    pub fn inter(
        &self,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Option<&str>,
    ) -> Result<Vec<V>> {
        let rs = self.aggregate_keys(b"ZINTER", keys, weights, aggregate, false)?;
        Ok(decode_array(rs))
    }

    /// 交集（含分数，`ZINTER ... WITHSCORES`）。
    pub fn inter_with_scores(
        &self,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Option<&str>,
    ) -> Result<Vec<(V, f64)>> {
        let rs = self.aggregate_keys(b"ZINTER", keys, weights, aggregate, true)?;
        Ok(decode_scored(rs))
    }

    /// 交集存储（`ZINTERSTORE`，Redis 6.2+），返回目标集合元素数量。
    pub fn inter_store(
        &self,
        destination: &str,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Option<&str>,
    ) -> Result<i64> {
        self.aggregate_store(b"ZINTERSTORE", destination, keys, weights, aggregate)
    }

    /// `ZUNION`/`ZINTER` 公共参数构造。
    fn aggregate_keys(
        &self,
        cmd: &[u8],
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Option<&str>,
        with_scores: bool,
    ) -> Result<crate::resp::RespValue> {
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(keys.len() * 2 + 10);
        args.push(cmd.to_vec());
        args.push((keys.len() + 1).to_string().into_bytes());
        args.push(self.key.as_bytes().to_vec());
        for k in keys {
            args.push(self.redis.get_key(k).into_bytes());
        }
        append_weights(&mut args, weights);
        append_aggregate(&mut args, aggregate);
        if with_scores {
            args.push(b"WITHSCORES".to_vec());
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        self.redis.redis().execute(&refs)
    }

    /// `ZUNIONSTORE`/`ZINTERSTORE` 公共参数构造。
    fn aggregate_store(
        &self,
        cmd: &[u8],
        destination: &str,
        keys: &[&str],
        weights: Option<&[f64]>,
        aggregate: Option<&str>,
    ) -> Result<i64> {
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(keys.len() * 2 + 12);
        args.push(cmd.to_vec());
        args.push(self.redis.get_key(destination).into_bytes());
        args.push((keys.len() + 1).to_string().into_bytes());
        args.push(self.key.as_bytes().to_vec());
        for k in keys {
            args.push(self.redis.get_key(k).into_bytes());
        }
        append_weights(&mut args, weights);
        append_aggregate(&mut args, aggregate);
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.redis().execute(&refs)?, 0))
    }
}

/// 追加 `WEIGHTS w1 w2 ...`。
fn append_weights(args: &mut Vec<Vec<u8>>, weights: Option<&[f64]>) {
    if let Some(weights) = weights {
        args.push(b"WEIGHTS".to_vec());
        for w in weights {
            args.push(format_f64(*w).into_bytes());
        }
    }
}

/// 追加 `AGGREGATE SUM|MIN|MAX`。
fn append_aggregate(args: &mut Vec<Vec<u8>>, aggregate: Option<&str>) {
    if let Some(agg) = aggregate.filter(|a| !a.is_empty()) {
        args.push(b"AGGREGATE".to_vec());
        args.push(agg.as_bytes().to_vec());
    }
}
