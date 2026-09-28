//! 列表结构（对应 DH.NRedis `RedisList<T>`）。右边进入、左边弹出，可当队列或栈使用。

use std::marker::PhantomData;

use crate::encoder::{FromRedisPayload, ToRedisPayload};
use crate::error::Result;
use crate::full::FullRedis;
use crate::util::{decode, decode_bytes, int_or, payload};

/// Redis 列表。
pub struct RedisList<V> {
    redis: FullRedis,
    key: String,
    _marker: PhantomData<fn() -> V>,
}

impl<V> RedisList<V>
where
    V: FromRedisPayload + ToRedisPayload,
{
    /// 由工厂方法创建（[`FullRedis::get_list`]）。键自动补前缀。
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

    /// 尾部添加（`RPUSH`），返回添加后的长度。
    pub fn push_back(&self, value: &V) -> Result<i64> {
        let value = payload(value)?;
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"RPUSH", self.key.as_bytes(), &value])?,
            0,
        ))
    }

    /// 头部添加（`LPUSH`），返回添加后的长度。
    pub fn push_front(&self, value: &V) -> Result<i64> {
        let value = payload(value)?;
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"LPUSH", self.key.as_bytes(), &value])?,
            0,
        ))
    }

    /// 尾部批量添加（`RPUSH`）。
    pub fn push_back_many(&self, values: &[V]) -> Result<i64> {
        self.push_many(b"RPUSH", values)
    }

    /// 头部批量添加（`LPUSH`）。
    pub fn push_front_many(&self, values: &[V]) -> Result<i64> {
        self.push_many(b"LPUSH", values)
    }

    fn push_many(&self, cmd: &[u8], values: &[V]) -> Result<i64> {
        if values.is_empty() {
            return Ok(0);
        }
        let mut args: Vec<Vec<u8>> = Vec::with_capacity(values.len() + 2);
        args.push(cmd.to_vec());
        args.push(self.key.as_bytes().to_vec());
        for v in values {
            args.push(payload(v)?);
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.redis().execute(&refs)?, 0))
    }

    /// 尾部弹出（`RPOP`）。
    pub fn pop_back(&self) -> Result<Option<V>> {
        let rs = self
            .redis
            .redis()
            .execute(&[b"RPOP", self.key.as_bytes()])?;
        Ok(decode(rs))
    }

    /// 头部弹出（`LPOP`）。
    pub fn pop_front(&self) -> Result<Option<V>> {
        let rs = self
            .redis
            .redis()
            .execute(&[b"LPOP", self.key.as_bytes()])?;
        Ok(decode(rs))
    }

    /// 尾部阻塞弹出（`BRPOP`）。`timeout_seconds < 0` 时等价 [`RedisList::pop_back`]。
    pub fn pop_back_blocking(&self, timeout_seconds: i64) -> Result<Option<V>> {
        if timeout_seconds < 0 {
            return self.pop_back();
        }
        let rs = self.redis.redis().execute_blocking(
            &[
                b"BRPOP",
                self.key.as_bytes(),
                timeout_seconds.to_string().as_bytes(),
            ],
            timeout_seconds,
        )?;

        // BRPOP 返回 [key, value]
        let mut items = rs.into_array().unwrap_or_default();
        if items.len() < 2 {
            return Ok(None);
        }
        Ok(decode(items.pop().unwrap()))
    }

    /// 头部阻塞弹出（`BLPOP`）。
    pub fn pop_front_blocking(&self, timeout_seconds: i64) -> Result<Option<V>> {
        if timeout_seconds < 0 {
            return self.pop_front();
        }
        let rs = self.redis.redis().execute_blocking(
            &[
                b"BLPOP",
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

    /// 获取指定位置元素（`LINDEX`）。
    pub fn get(&self, index: i64) -> Result<Option<V>> {
        let rs = self.redis.redis().execute(&[
            b"LINDEX",
            self.key.as_bytes(),
            index.to_string().as_bytes(),
        ])?;
        Ok(decode(rs))
    }

    /// 设置指定位置元素（`LSET`）。
    pub fn set(&self, index: i64, value: &V) -> Result<()> {
        let value = payload(value)?;
        self.redis.redis().execute_ignore(&[
            b"LSET",
            self.key.as_bytes(),
            index.to_string().as_bytes(),
            &value,
        ])
    }

    /// 区间获取（`LRANGE`，含头含尾，`-1` 表示末尾）。
    pub fn range(&self, start: i64, end: i64) -> Result<Vec<V>> {
        let rs = self.redis.redis().execute(&[
            b"LRANGE",
            self.key.as_bytes(),
            start.to_string().as_bytes(),
            end.to_string().as_bytes(),
        ])?;
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.as_bytes().and_then(|b| decode_bytes(&b)))
            .collect())
    }

    /// 获取全部元素。
    pub fn get_all(&self) -> Result<Vec<V>> {
        self.range(0, -1)
    }

    /// 删除并返回最右侧元素并插入目标列表左侧（`RPOPLPUSH`，高可靠消费）。
    pub fn rpoplpush(&self, destination: &str) -> Result<Option<V>> {
        let dest = self.redis.get_key(destination);
        let rs =
            self.redis
                .redis()
                .execute(&[b"RPOPLPUSH", self.key.as_bytes(), dest.as_bytes()])?;
        Ok(decode(rs))
    }

    /// 阻塞版 `RPOPLPUSH`（`BRPOPLPUSH`）。`timeout_seconds < 0` 时不阻塞。
    pub fn brpoplpush(&self, destination: &str, timeout_seconds: i64) -> Result<Option<V>> {
        let dest = self.redis.get_key(destination);
        if timeout_seconds < 0 {
            return self.rpoplpush(destination);
        }

        let rs = self.redis.redis().execute_blocking(
            &[
                b"BRPOPLPUSH",
                self.key.as_bytes(),
                dest.as_bytes(),
                timeout_seconds.to_string().as_bytes(),
            ],
            timeout_seconds,
        )?;
        Ok(decode(rs))
    }

    /// 在基准值前插入（`LINSERT BEFORE`），返回新长度。
    pub fn insert_before(&self, pivot: &V, value: &V) -> Result<i64> {
        self.insert(b"BEFORE", pivot, value)
    }

    /// 在基准值后插入（`LINSERT AFTER`），返回新长度。
    pub fn insert_after(&self, pivot: &V, value: &V) -> Result<i64> {
        self.insert(b"AFTER", pivot, value)
    }

    fn insert(&self, position: &[u8], pivot: &V, value: &V) -> Result<i64> {
        let pivot = payload(pivot)?;
        let value = payload(value)?;
        Ok(int_or(
            self.redis.redis().execute(&[
                b"LINSERT",
                self.key.as_bytes(),
                position,
                &pivot,
                &value,
            ])?,
            0,
        ))
    }

    /// 删除指定数量的匹配值（`LREM`）。
    pub fn remove(&self, count: i64, value: &V) -> Result<i64> {
        let value = payload(value)?;
        Ok(int_or(
            self.redis.redis().execute(&[
                b"LREM",
                self.key.as_bytes(),
                count.to_string().as_bytes(),
                &value,
            ])?,
            0,
        ))
    }

    /// 修剪列表（`LTRIM`）。
    pub fn trim(&self, start: i64, end: i64) -> Result<()> {
        self.redis.redis().execute_ignore(&[
            b"LTRIM",
            self.key.as_bytes(),
            start.to_string().as_bytes(),
            end.to_string().as_bytes(),
        ])
    }

    /// 清空列表（`LTRIM 0 -1`）。
    pub fn clear(&self) -> Result<()> {
        self.trim(0, -1)
    }

    /// 查找元素位置（`LPOS`，Redis 6.0+），未找到返回 `None`。
    pub fn position(&self, value: &V) -> Result<Option<i64>> {
        let value = payload(value)?;
        let rs = self
            .redis
            .redis()
            .execute(&[b"LPOS", self.key.as_bytes(), &value])?;
        Ok(if rs.is_null() { None } else { rs.as_i64() })
    }

    /// 查找元素首次出现的下标（对应 C# `IndexOf`，语义同 [`RedisList::position`]）。
    pub fn index_of(&self, value: &V) -> Result<Option<i64>> {
        self.position(value)
    }

    /// 在指定下标处插入元素（对应 C# `Insert(index, item)`：以该位置旧元素为 `LINSERT BEFORE` 基准）。
    ///
    /// 下标越界返回 `-1`（C# 会抛异常，语义差异见 README「已知差异」）。
    pub fn insert_at(&self, index: i64, value: &V) -> Result<i64> {
        match self.get(index)? {
            Some(pivot) => self.insert_before(&pivot, value),
            None => Ok(-1),
        }
    }

    /// 删除指定下标的元素（对应 C# `RemoveAt(index)`：先取下标元素再 `LREM 1`）。
    pub fn remove_at(&self, index: i64) -> Result<i64> {
        match self.get(index)? {
            Some(value) => self.remove(1, &value),
            None => Ok(0),
        }
    }

    /// 元素数量（`LPOS COUNT 0` 语义不便跨版本，此处用 LRANGE 遍历判断，与 C# `Contains` 等价）。
    pub fn contains(&self, value: &V) -> Result<bool>
    where
        V: PartialEq,
    {
        Ok(self.position(value)?.is_some())
    }
}
