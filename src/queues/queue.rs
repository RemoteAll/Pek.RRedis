//! 普通队列（对应 DH.NRedis `RedisQueue<T>`）：左进右出（`LPUSH` + `RPOP`/`BRPOP`）。
//!
//! 默认弹出即消费（不需要确认）；消费者处理失败消息会丢失。
//! 每条消息的字节格式与 C# 编码器一致（字符串原样、对象 JSON）。

use std::marker::PhantomData;
use std::thread::sleep;
use std::time::Duration;

use crate::encoder::{FromRedisPayload, ToRedisPayload};
use crate::error::{Error, Result};
use crate::full::FullRedis;
use crate::queues::base::QueueSettings;
use crate::util::{decode, int_or, payload};

/// 普通 Redis 队列。
pub struct RedisQueue<V> {
    redis: FullRedis,
    key: String,
    settings: QueueSettings,
    _marker: PhantomData<fn() -> V>,
}

impl<V> RedisQueue<V>
where
    V: FromRedisPayload + ToRedisPayload,
{
    /// 由工厂方法创建（[`FullRedis::get_queue`]）。键自动补前缀。
    pub fn new(redis: FullRedis, topic: &str) -> Self {
        let key = redis.get_key(topic);
        Self {
            redis,
            key,
            settings: QueueSettings::default(),
            _marker: PhantomData,
        }
    }

    /// 队列公共设置（可变）。
    pub fn settings_mut(&mut self) -> &mut QueueSettings {
        &mut self.settings
    }

    /// 实际键名（含前缀）。
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 消息数量（`LLEN`）。
    pub fn count(&self) -> Result<i64> {
        Ok(int_or(self.redis.redis().execute(&[b"LLEN", self.key.as_bytes()])?, 0))
    }

    /// 是否为空。
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.count()? == 0)
    }

    /// 生产一条消息，返回入队后的队列长度。
    pub fn add(&self, value: &V) -> Result<i64> {
        self.add_many(std::slice::from_ref(value))
    }

    /// 批量生产，返回入队后的队列长度（单次 `LPUSH`）。
    pub fn add_many(&self, values: &[V]) -> Result<i64> {
        if values.is_empty() {
            return Ok(0);
        }

        let mut args: Vec<Vec<u8>> = Vec::with_capacity(values.len() + 2);
        args.push(b"LPUSH".to_vec());
        args.push(self.key.as_bytes().to_vec());
        for v in values {
            let bytes = payload(v)?;
            args.push(self.settings.attach_trace(bytes));
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();

        // 与 C# 一致：正常插入不会返回 0，返回 0/空说明中间代理异常，按间隔重试
        let mut last = 0;
        for attempt in 0..=self.settings.retry_times_when_send_failed {
            last = int_or(self.redis.redis().execute(&refs)?, 0);
            if last > 0 {
                return Ok(last);
            }
            if attempt < self.settings.retry_times_when_send_failed {
                sleep(Duration::from_millis(self.settings.retry_interval_ms));
            }
        }

        if self.settings.throw_on_failure {
            return Err(Error::Operation(format!("发布到队列[{}]失败！", self.key)));
        }
        Ok(last)
    }

    /// 消费获取。`timeout_seconds < 0` 时不阻塞（`RPOP`），否则阻塞（`BRPOP`）。
    pub fn take_one(&self, timeout_seconds: i64) -> Result<Option<V>> {
        if timeout_seconds < 0 {
            let rs = self.redis.redis().execute(&[b"RPOP", self.key.as_bytes()])?;
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

    /// 批量消费（管道 `RPOP`），遇到空队列提前结束。
    pub fn take(&self, count: usize) -> Result<Vec<V>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut pipeline = self.redis.redis().pipeline();
        for _ in 0..count {
            pipeline.cmd(&[b"RPOP", self.key.as_bytes()]);
        }

        let mut result = Vec::with_capacity(count);
        for value in pipeline.execute()? {
            match decode::<V>(value) {
                Some(v) => result.push(v),
                None => break,
            }
        }
        Ok(result)
    }
}
