//! 可靠队列（对应 DH.NRedis `RedisReliableQueue<T>`）。
//!
//! 核心设计与 C# 完全一致，保证两边可以互相消费彼此的队列：
//!
//! - 主队列 `key`：`LPUSH` 生产，`RPOPLPUSH` 消费（弹出同时备份到确认队列）；
//! - 确认队列 `key:Ack:{ukey}`：消费成功用 `LREM` 删除；ukey 为 8 位随机串，每消费者一份；
//! - 状态键 `key:Status:{ukey}`：JSON（PascalCase、ISO 时间），7 天过期；
//! - 全局清理权键 `key:AllStatus`：`SET NX EX RetryInterval` 抢占，谁抢到谁执行 [`RedisReliableQueue::rollback_all_ack`]；
//! - 死信判定：`LastActive + RetryInterval * 10 < now` 时回滚该消费者的确认队列；
//! - 高级用法：`Publish(key→消息体, expire)` + [`RedisReliableQueue::consume`] 实现「至少一次 + 幂等」消费。

use std::marker::PhantomData;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;

use chrono::{Local, NaiveDateTime};

use crate::encoder::{FromRedisPayload, ToRedisPayload};
use crate::error::{Error, Result};
use crate::full::FullRedis;
use crate::queues::base::QueueSettings;
use crate::queues::delay::RedisDelayQueue;
use crate::queues::status::{RedisQueueStatus, new_consumer_key};
use crate::util::{decode, int_or, payload};

/// 可靠队列。
pub struct RedisReliableQueue<V> {
    redis: FullRedis,
    /// 主队列键（含前缀）
    key: String,
    /// 本消费者标识（8 位随机）
    consumer_key: String,
    /// 确认队列键 `{key}:Ack:{ukey}`
    ack_key: String,
    /// 状态键 `{key}:Status:{ukey}`
    status_key: String,
    /// 全局清理权键 `{key}:AllStatus`
    all_status_key: String,
    status: Mutex<RedisQueueStatus>,
    next_retry: Mutex<NaiveDateTime>,
    settings: QueueSettings,
    /// 消费者死信超时判定步长（秒）。默认 60，对应 C# `RetryInterval`
    pub retry_interval_seconds: i64,
    _marker: PhantomData<fn() -> V>,
}

impl<V> RedisReliableQueue<V>
where
    V: FromRedisPayload + ToRedisPayload,
{
    /// 由工厂方法创建（[`FullRedis::get_reliable_queue`]）。键自动补前缀。
    pub fn new(redis: FullRedis, topic: &str) -> Self {
        let key = redis.get_key(topic);
        let consumer_key = new_consumer_key();
        let status = RedisQueueStatus {
            key: consumer_key.clone(),
            ..Default::default()
        };

        Self {
            ack_key: format!("{key}:Ack:{consumer_key}"),
            status_key: format!("{key}:Status:{consumer_key}"),
            all_status_key: format!("{key}:AllStatus"),
            redis,
            key,
            consumer_key,
            status: Mutex::new(status),
            next_retry: Mutex::new(NaiveDateTime::default()),
            settings: QueueSettings::default(),
            retry_interval_seconds: 60,
            _marker: PhantomData,
        }
    }

    /// 队列公共设置（可变）。
    pub fn settings_mut(&mut self) -> &mut QueueSettings {
        &mut self.settings
    }

    /// 主队列键（含前缀）。
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 确认队列键。
    pub fn ack_key(&self) -> &str {
        &self.ack_key
    }

    /// 状态键。
    pub fn status_key(&self) -> &str {
        &self.status_key
    }

    /// 本消费者标识。
    pub fn consumer_key(&self) -> &str {
        &self.consumer_key
    }

    /// 当前消费者状态快照。
    pub fn status(&self) -> RedisQueueStatus {
        self.status.lock().unwrap().clone()
    }

    /// 主队列消息数量（`LLEN`）。
    pub fn count(&self) -> Result<i64> {
        Ok(int_or(self.redis.redis().execute(&[b"LLEN", self.key.as_bytes()])?, 0))
    }

    /// 是否为空。
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.count()? == 0)
    }

    // ================== 生产 ==================

    /// 生产一条消息，返回主队列长度。
    pub fn add(&self, value: &V) -> Result<i64> {
        self.add_many(std::slice::from_ref(value))
    }

    /// 批量生产，返回主队列长度。
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

    /// 高级生产：消息体写入 KV（带过期），消息键写入队列（`Publish` + `Consume` 配对）。
    pub fn publish(&self, messages: &[(&str, &V)], expire_seconds: i64) -> Result<i64> {
        if messages.is_empty() {
            return Ok(0);
        }

        let pairs: Vec<(String, Vec<u8>)> = messages
            .iter()
            .map(|(k, v)| {
                let key = self.redis.get_key(k);
                let value = payload(v)?;
                Ok((key, value))
            })
            .collect::<Result<_>>()?;

        // 消息体写入 kv
        let mut pipeline = self.redis.redis().pipeline();
        for (key, value) in &pairs {
            pipeline.cmd(&[b"SET", key.as_bytes(), value]);
            if expire_seconds > 0 {
                pipeline.cmd(&[
                    b"EXPIRE",
                    key.as_bytes(),
                    expire_seconds.to_string().as_bytes(),
                ]);
            }
        }
        pipeline.execute_ignore()?;

        // 消息键写入队列
        let mut args: Vec<&[u8]> = Vec::with_capacity(messages.len() + 2);
        args.push(b"LPUSH");
        args.push(self.key.as_bytes());
        for (k, _) in messages {
            args.push(k.as_bytes());
        }
        Ok(int_or(self.redis.redis().execute(&args)?, 0))
    }

    // ================== 消费 ==================

    /// 消费获取：从主队列弹出并备份到确认队列。`timeout_seconds < 0` 时不阻塞。
    pub fn take_one(&self, timeout_seconds: i64) -> Result<Option<V>> {
        self.retry_ack()?;

        let rs = if timeout_seconds >= 0 {
            self.redis.redis().execute_blocking(
                &[
                    b"BRPOPLPUSH",
                    self.key.as_bytes(),
                    self.ack_key.as_bytes(),
                    timeout_seconds.to_string().as_bytes(),
                ],
                timeout_seconds,
            )?
        } else {
            self.redis
                .redis()
                .execute(&[b"RPOPLPUSH", self.key.as_bytes(), self.ack_key.as_bytes()])?
        };

        if rs.is_null() {
            return Ok(None);
        }

        self.status.lock().unwrap().consumes += 1;
        Ok(decode(rs))
    }

    /// 批量消费（管道 `RPOPLPUSH`），返回消费到的消息。
    pub fn take(&self, count: usize) -> Result<Vec<V>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        self.retry_ack()?;

        let mut pipeline = self.redis.redis().pipeline();
        for _ in 0..count {
            pipeline.cmd(&[b"RPOPLPUSH", self.key.as_bytes(), self.ack_key.as_bytes()]);
        }

        let mut result = Vec::with_capacity(count);
        for value in pipeline.execute()? {
            match decode::<V>(value) {
                Some(v) => {
                    self.status.lock().unwrap().consumes += 1;
                    result.push(v);
                }
                None => break,
            }
        }
        Ok(result)
    }

    /// 确认消费，从确认队列删除（`LREM ack 1 消息`），返回删除数量。
    pub fn acknowledge(&self, keys: &[&str]) -> Result<i64> {
        if keys.is_empty() {
            return Ok(0);
        }

        let mut removed = 0;
        let mut pipeline = self.redis.redis().pipeline();
        for key in keys {
            pipeline.cmd(&[
                b"LREM",
                self.ack_key.as_bytes(),
                b"1",
                key.as_bytes(),
            ]);
        }
        for value in pipeline.execute()? {
            removed += value.as_i64().unwrap_or(0);
        }

        self.status.lock().unwrap().acks += keys.len() as i64;
        Ok(removed)
    }

    /// 从确认队列弹出消息（不确认），用于恢复现场。
    pub fn take_ack(&self, count: usize) -> Result<Vec<String>> {
        let mut result = Vec::with_capacity(count);
        for _ in 0..count {
            let rs = self
                .redis
                .redis()
                .execute(&[b"RPOP", self.ack_key.as_bytes()])?;
            match rs.as_string() {
                Some(v) => result.push(v),
                None => break,
            }
        }
        Ok(result)
    }

    /// 高级消费：`Publish` 配对使用；处理成功后删除消息体并确认。
    ///
    /// 拿不到消息体（重复消费或业务已删除）时直接确认并返回 `None`。
    pub fn consume<F, R>(&self, timeout_seconds: i64, handler: F) -> Result<Option<R>>
    where
        F: FnOnce(&V) -> Result<R>,
    {
        let Some(msg_id) = self.take_one_string(timeout_seconds)? else {
            return Ok(None);
        };

        let Some(message) = self.redis.redis().get::<V>(&msg_id)? else {
            self.acknowledge(&[&msg_id])?;
            return Ok(None);
        };

        let result = handler(&message)?;

        self.redis.redis().remove(&msg_id)?;
        self.acknowledge(&[&msg_id])?;

        Ok(Some(result))
    }

    /// 类型化大循环消费（对应 C# `QueueExtensions.ConsumeAsync<T>`）：
    /// 取出 JSON 消息 → 反序列化为 `T` → 交给 `on_message` 处理 → 成功后自动确认。
    ///
    /// 与 C# 完全一致的行为：
    /// - 处理失败（或 JSON 解析失败）时消息**不确认**，等待
    ///   [`RedisReliableQueue::retry_ack`] 在重试窗口后回滚重投；
    /// - 同时在**备份库**（`db == 15 ? 0 : db + 1`）的 `{topic}:Error:{id}` 计数 +1
    ///   （TTL 30 天）；同一消息累计失败 ≥ 10 次后自动确认（丢弃），避免毒消息死循环；
    /// - 消息标识依次取 `id_field`（若指定）、`Id`、`guid`、`OrderId`、`Code`，
    ///   均缺失时回退为消息体 MD5（与 C# `mqMsg.MD5()` 相同算法）。
    ///
    /// `poll_interval` 为无消息时的休眠间隔（C# 固定 1 秒，Rust 侧可调便于测试）。
    pub fn consume_json<T, F>(
        &self,
        timeout_seconds: i64,
        poll_interval: Duration,
        id_field: Option<&str>,
        cancel: &AtomicBool,
        mut on_message: F,
    ) -> Result<()>
    where
        T: serde::de::DeserializeOwned,
        F: FnMut(&T, &str) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>,
    {
        let mut id_fields: Vec<&str> = vec!["Id", "guid", "OrderId", "Code"];
        if let Some(id_field) = id_field.filter(|f| !f.is_empty() && !id_fields.contains(f)) {
            id_fields.insert(0, id_field);
        }

        // 备份库：错误计数与 C# 相同落在 db + 1（db == 15 时回到 0）
        let db = self.redis.redis().options().db;
        let bak_db = if db == 15 { 0 } else { db + 1 };
        let bak = self.redis.redis().create_sub(bak_db)?;

        while !cancel.load(Ordering::Relaxed) {
            let Some(raw) = self.take_one_string(timeout_seconds)? else {
                sleep(poll_interval);
                continue;
            };

            // 消息标识：字段优先，缺失时用消息体 MD5（与 C# 相同）
            let value = serde_json::from_str::<serde_json::Value>(&raw).ok();
            let mut msg_id = value
                .as_ref()
                .and_then(|v| extract_message_id(v, &id_fields))
                .unwrap_or_default();
            if msg_id.is_empty() {
                msg_id = format!("{:x}", md5::compute(raw.as_bytes()));
            }

            let result = match value.as_ref() {
                Some(value) => match serde_json::from_value::<T>(value.clone()) {
                    Ok(message) => on_message(&message, &raw),
                    Err(e) => Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>),
                },
                None => Err(Box::new(std::io::Error::other(format!(
                    "JSON 解析失败: {raw}"
                ))) as Box<dyn std::error::Error + Send + Sync>),
            };

            match result {
                Ok(()) => {
                    self.redis.redis().remove(&raw)?;
                    self.acknowledge(&[&raw])?;
                }
                Err(_) => {
                    // 错误次数达到 10 次则确认丢弃（与 C# 一致）
                    let error_key = format!("{}:Error:{}", self.key, msg_id);
                    let count = bak.increment(&error_key, 1)?;
                    if count < 10 {
                        bak.set_expire(&error_key, 30 * 24 * 3600)?;
                    } else {
                        self.redis.redis().remove(&raw)?;
                        self.acknowledge(&[&raw])?;
                    }
                }
            }
        }

        Ok(())
    }

    /// 字符串大循环消费（对应 C# `QueueExtensions.ConsumeAsync<T>(Action<String>)` 重载）：
    /// 直接把原始消息字符串交给 `on_message`，成功后自动确认。
    ///
    /// 失败处理与 [`RedisReliableQueue::consume_json`] 完全一致（错误计数、10 次后丢弃）。
    pub fn consume_raw<F>(
        &self,
        timeout_seconds: i64,
        poll_interval: Duration,
        cancel: &AtomicBool,
        mut on_message: F,
    ) -> Result<()>
    where
        F: FnMut(&str) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>,
    {
        let db = self.redis.redis().options().db;
        let bak_db = if db == 15 { 0 } else { db + 1 };
        let bak = self.redis.redis().create_sub(bak_db)?;

        while !cancel.load(Ordering::Relaxed) {
            let Some(raw) = self.take_one_string(timeout_seconds)? else {
                sleep(poll_interval);
                continue;
            };

            match on_message(&raw) {
                Ok(()) => {
                    self.redis.redis().remove(&raw)?;
                    self.acknowledge(&[&raw])?;
                }
                Err(_) => {
                    let msg_id = format!("{:x}", md5::compute(raw.as_bytes()));
                    let error_key = format!("{}:Error:{}", self.key, msg_id);
                    let count = bak.increment(&error_key, 1)?;
                    if count < 10 {
                        bak.set_expire(&error_key, 30 * 24 * 3600)?;
                    } else {
                        self.redis.redis().remove(&raw)?;
                        self.acknowledge(&[&raw])?;
                    }
                }
            }
        }

        Ok(())
    }

    fn take_one_string(&self, timeout_seconds: i64) -> Result<Option<String>> {
        self.retry_ack()?;

        let rs = if timeout_seconds >= 0 {
            self.redis.redis().execute_blocking(
                &[
                    b"BRPOPLPUSH",
                    self.key.as_bytes(),
                    self.ack_key.as_bytes(),
                    timeout_seconds.to_string().as_bytes(),
                ],
                timeout_seconds,
            )?
        } else {
            self.redis
                .redis()
                .execute(&[b"RPOPLPUSH", self.key.as_bytes(), self.ack_key.as_bytes()])?
        };

        if rs.is_null() {
            return Ok(None);
        }

        self.status.lock().unwrap().consumes += 1;
        Ok(rs.as_string())
    }

    // ================== 死信处理 ==================

    /// 定时回滚本消费者的确认队列死信，并周期性执行全局清理。
    pub fn retry_ack(&self) -> Result<usize> {
        let now = Local::now().naive_local();
        {
            let mut next = self.next_retry.lock().unwrap();
            if *next >= now {
                return Ok(0);
            }
            *next = now + chrono::Duration::seconds(self.retry_interval_seconds);
        }

        // 拿到死信，重新放入队列
        let list = self.rollback_ack(&self.ack_key, &self.key)?;

        // 更新状态
        self.update_status()?;

        // 抢夺全局清理权，减少全局扫描次数
        let json = self.status.lock().unwrap().to_json();
        if self
            .redis
            .redis()
            .add(&self.all_status_key, json.as_str(), self.retry_interval_seconds)?
        {
            self.rollback_all_ack()?;
        }

        Ok(list.len())
    }

    /// 回滚指定确认队列的消息到主队列，返回回滚数量。
    pub fn rollback_ack(&self, from_ack_key: &str, to_key: &str) -> Result<Vec<String>> {
        let mut list = Vec::new();
        loop {
            let rs = self.redis.redis().execute(&[
                b"RPOPLPUSH",
                from_ack_key.as_bytes(),
                to_key.as_bytes(),
            ])?;
            match rs.as_string() {
                Some(v) => list.push(v),
                None => break,
            }
        }
        Ok(list)
    }

    /// 全局回滚死信（由抢到 `AllStatus` 的实例执行）。
    pub fn rollback_all_ack(&self) -> Result<i64> {
        let mut count = 0;

        // 扫描所有消费者状态，回滚失联消费者的确认队列
        let mut known_acks: Vec<String> = Vec::new();
        let status_pattern = format!("{}:Status:*", self.key);
        for status_item in self.redis.search(&status_pattern, 0)? {
            // search 返回的是去掉前缀的键，这里还原完整键
            let full_status = self.redis.get_key(&status_item);
            let Some(consumer) = full_status.rsplit(':').next().map(|s| s.to_string()) else {
                continue;
            };

            let ack_key = format!("{}:Ack:{}", self.key, consumer);
            known_acks.push(self.redis.trim_key(&ack_key).to_string());

            let Some(text) = self.redis.redis().get_string(&full_status)? else {
                continue;
            };
            let Some(status) = RedisQueueStatus::from_json(&text) else {
                continue;
            };

            let deadline =
                status.last_active + chrono::Duration::seconds(self.retry_interval_seconds * 10);
            if deadline < Local::now().naive_local() {
                if self.redis.redis().contains_key(&ack_key)? {
                    let list = self.rollback_ack(&ack_key, &self.key)?;
                    count += list.len() as i64;
                }
                self.redis.redis().remove(&full_status)?;
            }
        }

        // 清理已经失去 Status 的 Ack 队列
        let ack_pattern = format!("{}:Ack:*", self.key);
        for ack_item in self.redis.search(&ack_pattern, 0)? {
            if !known_acks.contains(&ack_item) {
                let full_ack = self.redis.get_key(&ack_item);
                self.redis.redis().remove(&full_ack)?;
            }
        }

        Ok(count)
    }

    /// 清空所有 Ack 队列（危险操作，需所有消费者停止）。
    pub fn clear_all_ack(&self) -> Result<i64> {
        let pattern = format!("{}:Ack:*", self.key);
        self.redis.remove_pattern(&pattern)
    }

    /// 状态更新（7 天过期），写入 `{key}:Status:{ukey}`。
    fn update_status(&self) -> Result<()> {
        let json = {
            let mut status = self.status.lock().unwrap();
            status.last_active = Local::now().naive_local();
            status.to_json()
        };

        self.redis
            .redis()
            .set(&self.status_key, json.as_str(), 7 * 24 * 3600)?;
        Ok(())
    }

    // ================== 延迟队列 ==================

    /// 延迟队列（键为 `{key}:Delay`，与 C# `InitDelay` 一致）。
    pub fn delay_queue(&self) -> RedisDelayQueue<V> {
        RedisDelayQueue::new(self.redis.clone(), &format!("{}:Delay", self.key))
    }

    /// 添加延迟消息。
    pub fn add_delay(&self, value: &V, delay_seconds: i64) -> Result<i64> {
        self.delay_queue().add(value, delay_seconds)
    }
}

/// 从 JSON 对象中按候选字段名依次提取消息标识（对应 C# `QueueExtensions` 的 `ids` 轮询逻辑）。
fn extract_message_id(value: &serde_json::Value, fields: &[&str]) -> Option<String> {
    let obj = value.as_object()?;
    for field in fields {
        if let Some(v) = obj.get(*field) {
            let text = match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => continue,
                other => other.to_string(),
            };
            if !text.is_empty() {
                return Some(text);
            }
        }
    }
    None
}
