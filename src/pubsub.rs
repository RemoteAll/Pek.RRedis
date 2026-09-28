//! 发布订阅（对应 DH.NRedis `PubSub`）。
//!
//! 与 C# 行为一致：
//! - 频道名与键一样应用前缀（`RedisBase` 构造时 `GetKey`）；
//! - 频道字符串支持用 `,` / `;` 分隔多个频道；
//! - 订阅使用**独立连接**长循环，通过 [`std::sync::atomic::AtomicBool`] 取消；
//! - 支持 `SUBSCRIBE` / `PSUBSCRIBE` / `SSUBSCRIBE`（Redis 7 分片订阅）与 `PUBSUB` 自省命令。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::client::RedisClient;
use crate::error::{Error, Result};
use crate::full::FullRedis;
use crate::resp::RespValue;
use crate::util::int_or;

/// 发布订阅对象。构造时传入的频道名会自动应用前缀。
#[derive(Clone)]
pub struct PubSub {
    redis: FullRedis,
    /// 完整键（含前缀），C# `Publish` 使用该键
    key: String,
    /// 拆分后的频道列表（含前缀）
    channels: Vec<String>,
}

impl PubSub {
    /// 由工厂方法创建（[`FullRedis::get_pubsub`]）。
    pub fn new(redis: FullRedis, channel: &str) -> Self {
        let key = redis.get_key(channel);
        let channels = channel
            .split([',', ';'])
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(|s| redis.get_key(s))
            .collect();

        Self {
            redis,
            key,
            channels,
        }
    }

    /// 拆分后的频道列表（含前缀）。
    pub fn channels(&self) -> &[String] {
        &self.channels
    }

    /// 发布消息（`PUBLISH`），返回接收客户端数量。
    ///
    /// 与 C# `PubSub.Publish` 一致：发布到构造时的完整键（未拆分）。
    /// 拆分多频道发布请使用 [`PubSub::publish_to`]。
    pub fn publish(&self, message: &str) -> Result<i64> {
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"PUBLISH", self.key.as_bytes(), message.as_bytes()])?,
            0,
        ))
    }

    /// 发布消息到指定频道（`PUBLISH`）。
    pub fn publish_to(&self, channel: &str, message: &str) -> Result<i64> {
        let ch = self.redis.get_key(channel);
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"PUBLISH", ch.as_bytes(), message.as_bytes()])?,
            0,
        ))
    }

    /// 分片发布（`SPUBLISH`，Redis 7.0+）。
    pub fn spublish(&self, message: &str) -> Result<i64> {
        Ok(int_or(
            self.redis
                .redis()
                .execute(&[b"SPUBLISH", self.key.as_bytes(), message.as_bytes()])?,
            0,
        ))
    }

    /// 订阅循环（`SUBSCRIBE`）。回调参数为 `(频道, 消息)`。
    ///
    /// 本方法阻塞当前线程，直到 `cancel` 被置为 `true`。请在线程中调用。
    pub fn subscribe<F>(&self, cancel: Arc<AtomicBool>, mut on_message: F) -> Result<()>
    where
        F: FnMut(&str, &str),
    {
        let mut client = self.open_client()?;

        let mut args: Vec<&[u8]> = Vec::with_capacity(self.channels.len() + 1);
        args.push(b"SUBSCRIBE");
        for c in &self.channels {
            args.push(c.as_bytes());
        }
        client.command(&args)?;
        for _ in 1..self.channels.len() {
            client.read_message()?;
        }

        while !cancel.load(Ordering::Relaxed) {
            match client.read_message() {
                Ok(v) => {
                    if let Some(arr) = as_array(v)
                        && arr.len() == 3
                        && arr[0].as_string().as_deref() == Some("message")
                        && let (Some(ch), Some(msg)) = (arr[1].as_string(), arr[2].as_string())
                    {
                        on_message(&ch, &msg);
                    }
                }
                Err(e) if is_timeout(&e) => continue,
                Err(e) => return Err(e),
            }
        }

        let mut args: Vec<&[u8]> = Vec::with_capacity(self.channels.len() + 1);
        args.push(b"UNSUBSCRIBE");
        for c in &self.channels {
            args.push(c.as_bytes());
        }
        let _ = client.command(&args);

        Ok(())
    }

    /// 模式订阅循环（`PSUBSCRIBE`）。回调参数为 `(模式, 频道, 消息)`。
    pub fn psubscribe<F>(&self, cancel: Arc<AtomicBool>, mut on_message: F) -> Result<()>
    where
        F: FnMut(&str, &str, &str),
    {
        let mut client = self.open_client()?;

        let mut args: Vec<&[u8]> = Vec::with_capacity(self.channels.len() + 1);
        args.push(b"PSUBSCRIBE");
        for c in &self.channels {
            args.push(c.as_bytes());
        }
        client.command(&args)?;
        for _ in 1..self.channels.len() {
            client.read_message()?;
        }

        while !cancel.load(Ordering::Relaxed) {
            match client.read_message() {
                Ok(v) => {
                    if let Some(arr) = as_array(v)
                        && arr.len() == 4
                        && arr[0].as_string().as_deref() == Some("pmessage")
                        && let (Some(pattern), Some(ch), Some(msg)) =
                            (arr[1].as_string(), arr[2].as_string(), arr[3].as_string())
                    {
                        on_message(&pattern, &ch, &msg);
                    }
                }
                Err(e) if is_timeout(&e) => continue,
                Err(e) => return Err(e),
            }
        }

        let mut args: Vec<&[u8]> = Vec::with_capacity(self.channels.len() + 1);
        args.push(b"PUNSUBSCRIBE");
        for c in &self.channels {
            args.push(c.as_bytes());
        }
        let _ = client.command(&args);

        Ok(())
    }

    /// 分片订阅循环（`SSUBSCRIBE`，Redis 7.0+）。回调参数为 `(频道, 消息)`。
    pub fn ssubscribe<F>(&self, cancel: Arc<AtomicBool>, mut on_message: F) -> Result<()>
    where
        F: FnMut(&str, &str),
    {
        let mut client = self.open_client()?;

        let mut args: Vec<&[u8]> = Vec::with_capacity(self.channels.len() + 1);
        args.push(b"SSUBSCRIBE");
        for c in &self.channels {
            args.push(c.as_bytes());
        }
        client.command(&args)?;
        for _ in 1..self.channels.len() {
            client.read_message()?;
        }

        while !cancel.load(Ordering::Relaxed) {
            match client.read_message() {
                Ok(v) => {
                    if let Some(arr) = as_array(v)
                        && arr.len() == 3
                        && arr[0].as_string().as_deref() == Some("smessage")
                        && let (Some(ch), Some(msg)) = (arr[1].as_string(), arr[2].as_string())
                    {
                        on_message(&ch, &msg);
                    }
                }
                Err(e) if is_timeout(&e) => continue,
                Err(e) => return Err(e),
            }
        }

        let mut args: Vec<&[u8]> = Vec::with_capacity(self.channels.len() + 1);
        args.push(b"SUNSUBSCRIBE");
        for c in &self.channels {
            args.push(c.as_bytes());
        }
        let _ = client.command(&args);

        Ok(())
    }

    /// 活跃频道列表（`PUBSUB CHANNELS`）。
    pub fn pubsub_channels(&self, pattern: Option<&str>) -> Result<Vec<String>> {
        let rs = match pattern {
            Some(p) => self
                .redis
                .redis()
                .execute(&[b"PUBSUB", b"CHANNELS", p.as_bytes()])?,
            None => self.redis.redis().execute(&[b"PUBSUB", b"CHANNELS"])?,
        };
        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| v.as_string())
            .collect())
    }

    /// 各频道订阅者数量（`PUBSUB NUMSUB`）。
    pub fn pubsub_numsub(&self, channels: &[&str]) -> Result<Vec<(String, i64)>> {
        let mut args: Vec<&[u8]> = Vec::with_capacity(channels.len() + 2);
        args.push(b"PUBSUB");
        args.push(b"NUMSUB");
        for c in channels {
            args.push(c.as_bytes());
        }

        let items = self
            .redis
            .redis()
            .execute(&args)?
            .into_array()
            .unwrap_or_default();

        let mut result = Vec::with_capacity(items.len() / 2);
        let mut iter = items.into_iter();
        while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
            if let Some(ch) = k.as_string() {
                result.push((ch, v.as_i64().unwrap_or(0)));
            }
        }
        Ok(result)
    }

    /// 模式订阅数量（`PUBSUB NUMPAT`）。
    pub fn pubsub_numpat(&self) -> Result<i64> {
        Ok(int_or(
            self.redis.redis().execute(&[b"PUBSUB", b"NUMPAT"])?,
            0,
        ))
    }

    fn open_client(&self) -> Result<RedisClient> {
        let mut client = RedisClient::connect(&self.redis.redis().conn_config())?;
        // 短超时轮询，便于及时响应取消
        client.set_read_timeout(Some(Duration::from_millis(500)))?;
        Ok(client)
    }
}

fn as_array(value: RespValue) -> Option<Vec<RespValue>> {
    match value {
        RespValue::Array(v) | RespValue::Push(v) => Some(v),
        _ => None,
    }
}

fn is_timeout(e: &Error) -> bool {
    matches!(
        e,
        Error::Io(io) if matches!(io.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut)
    )
}
