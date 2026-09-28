//! 延迟队列（对应 DH.NRedis `RedisDelayQueue<T>`）。
//!
//! 基于有序集合：`score = Unix 秒时间戳 + 延迟秒数`（与 C# `DateTime.UtcNow.ToInt() + delay` 一致）。
//! 消费时用 `ZRANGEBYSCORE 0 now LIMIT 0 n` 找到到期消息，再以 `ZREM` 作为抢占标志（与 C# `TryPop` 相同）。

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;

use chrono::Utc;

use crate::encoder::{FromRedisPayload, ToRedisPayload, format_f64};
use crate::error::{Error, Result};
use crate::full::FullRedis;
use crate::queues::base::QueueSettings;
use crate::queues::queue::RedisQueue;
use crate::util::{decode, int_or, payload};

/// 延迟队列。
pub struct RedisDelayQueue<V> {
    redis: FullRedis,
    key: String,
    settings: QueueSettings,
    /// 默认延迟秒数。默认 60（对应 C# `Delay`）
    pub default_delay_seconds: i64,
    /// 转移到期消息的轮询间隔秒数。默认 10（对应 C# `TransferInterval`）
    pub transfer_interval_seconds: u64,
    _marker: PhantomData<fn() -> V>,
}

impl<V> RedisDelayQueue<V>
where
    V: FromRedisPayload + ToRedisPayload,
{
    /// 由工厂方法创建（[`FullRedis::get_delay_queue`]）。键自动补前缀。
    pub fn new(redis: FullRedis, topic: &str) -> Self {
        let key = redis.get_key(topic);
        Self {
            redis,
            key,
            settings: QueueSettings::default(),
            default_delay_seconds: 60,
            transfer_interval_seconds: 10,
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

    /// 待消费消息数（`ZCARD`，含未到期的）。
    pub fn count(&self) -> Result<i64> {
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"ZCARD", self.key.as_bytes()])?,
            0,
        ))
    }

    /// 是否为空。
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.count()? == 0)
    }

    /// 添加延迟消息，`delay_seconds` 为延迟秒数。
    pub fn add(&self, value: &V, delay_seconds: i64) -> Result<i64> {
        let payload = self.settings.attach_trace(payload(value)?);
        let score = Utc::now().timestamp() + delay_seconds;

        let mut last = 0;
        for attempt in 0..=self.settings.retry_times_when_send_failed {
            last = int_or(
                self.redis.redis().execute(&[
                    b"ZADD",
                    self.key.as_bytes(),
                    format_f64(score as f64).as_bytes(),
                    &payload,
                ])?,
                0,
            );

            // ZADD 返回新增成员数（更新已有成员返回 0），只要不是错误即视为成功
            if last >= 0 {
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

    /// 批量添加（使用默认延迟 [`RedisDelayQueue::default_delay_seconds`]）。
    pub fn add_many(&self, values: &[V]) -> Result<i64> {
        if values.is_empty() {
            return Ok(0);
        }

        let score = Utc::now().timestamp() + self.default_delay_seconds;
        let score = format_f64(score as f64);

        let mut args: Vec<Vec<u8>> = Vec::with_capacity(values.len() * 2 + 2);
        args.push(b"ZADD".to_vec());
        args.push(self.key.as_bytes().to_vec());
        for v in values {
            let bytes = self.settings.attach_trace(payload(v)?);
            args.push(score.clone().into_bytes());
            args.push(bytes);
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        Ok(int_or(self.redis.redis().execute(&refs)?, 0))
    }

    /// 删除尚未消费的延迟消息。
    pub fn remove(&self, value: &V) -> Result<i64> {
        let payload = payload(value)?;
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"ZREM", self.key.as_bytes(), &payload])?,
            0,
        ))
    }

    /// 取出最多 `count` 条已到期消息（`ZRANGEBYSCORE` + `ZREM` 抢占）。
    pub fn take_due(&self, count: usize) -> Result<Vec<V>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let score = Utc::now().timestamp();
        let rs = self.redis.redis().execute(&[
            b"ZRANGEBYSCORE",
            self.key.as_bytes(),
            b"0",
            format_f64(score as f64).as_bytes(),
            b"LIMIT",
            b"0",
            count.to_string().as_bytes(),
        ])?;

        let mut result = Vec::new();
        for item in rs.into_array().unwrap_or_default() {
            let Some(bytes) = item.as_bytes() else {
                continue;
            };
            // 争夺消费：只有一个线程能成功删除
            let removed = int_or(
                self.redis
                    .redis()
                    .execute(&[b"ZREM", self.key.as_bytes(), &bytes])?,
                0,
            );
            if removed > 0
                && let Some(v) = decode::<V>(crate::resp::RespValue::Bulk(bytes))
            {
                result.push(v);
            }
        }

        Ok(result)
    }

    /// 获取一条到期消息。`timeout_seconds == 0` 时最多等待 60 秒（与 C# 相同），
    /// 负数则只尝试一次不等待。
    pub fn take_one(&self, timeout_seconds: i64) -> Result<Option<V>> {
        let mut timeout = if timeout_seconds == 0 {
            60
        } else {
            timeout_seconds
        };

        loop {
            let mut items = self.take_due(1)?;
            if let Some(v) = items.pop() {
                return Ok(Some(v));
            }

            if timeout <= 0 {
                return Ok(None);
            }

            sleep(Duration::from_secs(1));
            timeout -= 1;
        }
    }

    /// 立即把当前所有到期消息转移到目标队列，返回转移条数（不睡眠，可供定时器回调复用）。
    ///
    /// 与 [`RedisDelayQueue::transfer_loop`] 的“每批 10 条、连续取直到取空”语义一致。
    pub fn transfer_due(&self, target: &RedisQueue<V>) -> Result<usize> {
        let mut total = 0;
        loop {
            let messages = self.take_due(10)?;
            if messages.is_empty() {
                return Ok(total);
            }
            total += messages.len();
            target.add_many(&messages)?;
        }
    }

    /// 将到期消息转移到目标队列（对应 C# `TransferAsync`）。
    ///
    /// 阻塞循环直到 `cancel` 置位；建议在独立线程中调用（每个队列进程内开一个即可）。
    /// 若由 DH.RustBase 定时器驱动，请改用 [`RedisDelayQueue::transfer_due`]。
    pub fn transfer_loop(&self, target: &RedisQueue<V>, cancel: Arc<AtomicBool>) -> Result<()> {
        while !cancel.load(Ordering::Relaxed) {
            if self.transfer_due(target)? == 0 {
                // 没有到期消息，歇一会（可被 cancel 打断）
                for _ in 0..(self.transfer_interval_seconds * 10) {
                    if cancel.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    sleep(Duration::from_millis(100));
                }
            }
        }
        Ok(())
    }
}
