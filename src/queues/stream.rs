//! Redis Stream 消息队列（对应 DH.NRedis `RedisStream<T>`），Redis 5.0+。
//!
//! 与 C# 端的关键约定（保证两边可互相生产/消费同一个 Stream）：
//!
//! | 项目 | 约定 |
//! |------|------|
//! | 基元消息 | 字段名 [`RedisStream::primitive_key`]（默认 `__data`），值为编码器输出的文本 |
//! | 对象消息 | 属性名/值扁平化写入（属性名 PascalCase 与 C# 一致；值为编码器格式的文本） |
//! | 数组消息 | 两两一组展开为字段名/字段值 |
//! | 消费组 | `XGROUP CREATE {key} {g} {startId} MKSTREAM`；`SETID $` 对应 `from_last_offset` |
//! | 消费 | `XREADGROUP GROUP g c [BLOCK ms] [COUNT n] STREAMS key >`（历史消息用 `0`） |
//! | 确认 | `XACK`；死信通过 `XPENDING` + `XCLAIM` 抢占（`retry_ack`，与 C# `RetryAck` 同规则） |
//! | 消费者名 | `{机器名}@{8位随机}`（与 C# `Environment.MachineName@Rand(8)` 同格式） |
//!
//! 类型映射（Rust 侧）：
//! - 生产：`add(&value, msg_id)`，`value` 为任意 `serde::Serialize`（基元/结构体/数组/字典）；
//! - 消费：[`RedisStream::take_messages`] 拿原始 [`Message`]，或以
//!   [`RedisStream::take_bodies`]（`__data` 基元）/[`RedisStream::take_structs`]（字段映射为结构体）取强类型。

use std::sync::Mutex;
use std::sync::atomic::{AtomicI32, AtomicI64, Ordering};
use std::thread::sleep;
use std::time::Duration;

use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::encoder::FromRedisPayload;
use crate::error::{Error, Result};
use crate::full::FullRedis;
use crate::queues::base::QueueSettings;
use crate::queues::status::{machine_name, new_consumer_key};
use crate::resp::RespValue;
use crate::util::int_or;

/// 单条 Stream 消息：Id + 扁平字段数组（`[字段名, 字段值, ...]`）。
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    /// 消息 Id，形如 `1695792000000-0`
    pub id: String,
    /// 扁平字段数组：字段名、字段值交替
    pub body: Vec<String>,
}

impl Message {
    /// 字段值（按字段名查找）。
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields()
            .into_iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v)
    }

    /// 全部字段键值对（跳过奇数尾部）。
    pub fn fields(&self) -> Vec<(&str, &str)> {
        let mut result = Vec::with_capacity(self.body.len() / 2);
        let mut i = 0;
        while i + 1 < self.body.len() {
            result.push((self.body[i].as_str(), self.body[i + 1].as_str()));
            i += 2;
        }
        result
    }

    /// 基元消息的原始值（与 C# `Message.GetBody` 的 `__data` 分支一致）。
    pub fn primitive_value(&self) -> Option<&[u8]> {
        self.primitive_value_with("__data")
    }

    /// 指定基元字段名的原始值。
    pub fn primitive_value_with(&self, primitive_key: &str) -> Option<&[u8]> {
        if self.body.len() == 2 && self.body[0] == primitive_key {
            Some(self.body[1].as_bytes())
        } else {
            None
        }
    }

    /// 基元消息解码（标量、时间、`Json<T>` 等，规则同编码器）。
    pub fn primitive<T: FromRedisPayload>(&self) -> Option<T> {
        self.primitive_value()
            .and_then(|b| T::from_redis_payload(b).ok())
    }

    /// 字段映射为 JSON 对象。值采用启发式类型解析（数字/布尔/null/嵌套 JSON 按 JSON 处理，
    /// 其余保留字符串）；需要严格“全部按字符串”语义时请直接用 [`Message::fields`]。
    pub fn to_json_object(&self) -> Value {
        Value::Object(
            self.fields()
                .into_iter()
                .map(|(k, v)| (k.to_string(), smart_field_value(v)))
                .collect(),
        )
    }

    /// 字段映射为结构体（属性名需与字段名一致，通常 `#[serde(rename_all = "PascalCase")]`）。
    ///
    /// 先尝试全部按字符串（保留 `"0012"` 这类前导零），失败后再回退为启发式类型。
    pub fn to_struct<T: DeserializeOwned>(&self) -> Option<T> {
        if self.body.len() == 2 && self.body[0] == "__data" {
            return None;
        }

        let all_string = Value::Object(
            self.fields()
                .into_iter()
                .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
                .collect(),
        );

        if let Ok(v) = serde_json::from_value::<T>(all_string) {
            return Some(v);
        }
        if let Ok(v) = serde_json::from_value::<T>(self.to_json_object()) {
            return Some(v);
        }
        None
    }
}

fn smart_field_value(text: &str) -> Value {
    if let Ok(v) = serde_json::from_str::<Value>(text) {
        match v {
            Value::Object(_) | Value::Array(_) | Value::Number(_) | Value::Bool(_) | Value::Null => {
                return v;
            }
            _ => {}
        }
    }
    Value::String(text.to_string())
}

/// 消息流信息（对应 C# `StreamInfo`）。
#[derive(Debug, Clone, Default)]
pub struct StreamInfo {
    /// 长度
    pub length: i64,
    /// 基数树键数
    pub radix_tree_keys: i64,
    /// 基数树节点数
    pub radix_tree_nodes: i64,
    /// 消费组数量
    pub groups: i64,
    /// 最后生成的 Id
    pub last_generated_id: Option<String>,
    /// 第一个消息 Id 与字段
    pub first_id: Option<String>,
    /// 第一个消息字段
    pub first_values: Vec<String>,
    /// 最后一个消息 Id
    pub last_id: Option<String>,
    /// 最后一个消息字段
    pub last_values: Vec<String>,
    /// 累计添加条目数（Redis 7.0+）
    pub entries_added: i64,
    /// 最后生成时间（按 Id 毫秒时间戳换算为本地时间）
    pub last_generated: Option<NaiveDateTime>,
}

impl StreamInfo {
    /// 从 `XINFO STREAM` 应答解析。
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_items(items: &[RespValue]) -> Self {
        let mut info = Self::default();
        let mut i = 0;
        while i + 1 < items.len() {
            let key = items[i].as_string().unwrap_or_default();
            let value = &items[i + 1];
            match key.as_str() {
                "length" => info.length = value.as_i64().unwrap_or(0),
                "radix-tree-keys" => info.radix_tree_keys = value.as_i64().unwrap_or(0),
                "radix-tree-nodes" => info.radix_tree_nodes = value.as_i64().unwrap_or(0),
                "groups" => info.groups = value.as_i64().unwrap_or(0),
                "entries-added" => info.entries_added = value.as_i64().unwrap_or(0),
                "last-generated-id" => info.last_generated_id = value.as_string(),
                "first-entry" => {
                    if let Some(entry) = parse_entry(value) {
                        info.first_id = Some(entry.id);
                        info.first_values = entry.body;
                    }
                }
                "last-entry" => {
                    if let Some(entry) = parse_entry(value) {
                        info.last_id = Some(entry.id);
                        info.last_values = entry.body;
                    }
                }
                _ => {}
            }
            i += 2;
        }

        info.last_generated = info.last_generated_id.as_deref().and_then(id_to_local_time);
        info
    }
}

/// 消费组信息（对应 C# `GroupInfo`）。
#[derive(Debug, Clone, Default)]
pub struct GroupInfo {
    /// 名称
    pub name: String,
    /// 消费者数量
    pub consumers: i64,
    /// 挂起消息数
    pub pending: i64,
    /// 最后投递 Id
    pub last_delivered_id: Option<String>,
    /// 最后投递时间
    pub last_delivered: Option<NaiveDateTime>,
}

impl GroupInfo {
    /// 从 `XINFO GROUPS` 的单项解析。
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_items(items: &[RespValue]) -> Self {
        let mut g = Self::default();
        let mut i = 0;
        while i + 1 < items.len() {
            let key = items[i].as_string().unwrap_or_default();
            let value = &items[i + 1];
            match key.as_str() {
                "name" => g.name = value.as_string().unwrap_or_default(),
                "consumers" => g.consumers = value.as_i64().unwrap_or(0),
                "pending" => g.pending = value.as_i64().unwrap_or(0),
                "last-delivered-id" => g.last_delivered_id = value.as_string(),
                _ => {}
            }
            i += 2;
        }
        g.last_delivered = g.last_delivered_id.as_deref().and_then(id_to_local_time);
        g
    }
}

/// 消费者信息（对应 C# `ConsumerInfo`）。
#[derive(Debug, Clone, Default)]
pub struct ConsumerInfo {
    /// 名称
    pub name: String,
    /// 挂起消息数
    pub pending: i64,
    /// 空闲毫秒数
    pub idle: i64,
}

impl ConsumerInfo {
    /// 从 `XINFO CONSUMERS` 的单项解析。
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_items(items: &[RespValue]) -> Self {
        let mut c = Self::default();
        let mut i = 0;
        while i + 1 < items.len() {
            let key = items[i].as_string().unwrap_or_default();
            let value = &items[i + 1];
            match key.as_str() {
                "name" => c.name = value.as_string().unwrap_or_default(),
                "pending" => c.pending = value.as_i64().unwrap_or(0),
                "idle" => c.idle = value.as_i64().unwrap_or(0),
                _ => {}
            }
            i += 2;
        }
        c
    }
}

/// 等待信息汇总（对应 C# `PendingInfo`，`XPENDING key group`）。
#[derive(Debug, Clone, Default)]
pub struct PendingInfo {
    /// 挂起总数
    pub count: i64,
    /// 最小 Id
    pub start_id: Option<String>,
    /// 最大 Id
    pub end_id: Option<String>,
    /// 各消费者挂起数量
    pub consumers: Vec<(String, i64)>,
}

impl PendingInfo {
    /// 从 `XPENDING key group` 应答解析。
    #[allow(clippy::field_reassign_with_default)]
    pub fn from_items(items: &[RespValue]) -> Self {
        let mut p = Self::default();
        p.count = items.first().and_then(|v| v.as_i64()).unwrap_or(0);
        p.start_id = items.get(1).and_then(|v| v.as_string());
        p.end_id = items.get(2).and_then(|v| v.as_string());
        if let Some(list) = items.get(3).and_then(|v| v.as_array()) {
            for item in list {
                if let Some(pair) = item.as_array()
                    && pair.len() == 2
                        && let Some(name) = pair[0].as_string() {
                            p.consumers.push((name, pair[1].as_i64().unwrap_or(0)));
                        }
            }
        }
        p
    }
}

/// 等待项（对应 C# `PendingItem`）。
#[derive(Debug, Clone, Default)]
pub struct PendingItem {
    /// 消息 Id
    pub id: String,
    /// 当前消费者
    pub consumer: String,
    /// 空闲毫秒数
    pub idle: i64,
    /// 投递次数
    pub delivery: i64,
}

impl PendingItem {
    /// 从 `XPENDING key group start end count` 的单项解析。
    pub fn from_items(items: &[RespValue]) -> Self {
        Self {
            id: items.first().and_then(|v| v.as_string()).unwrap_or_default(),
            consumer: items.get(1).and_then(|v| v.as_string()).unwrap_or_default(),
            idle: items.get(2).and_then(|v| v.as_i64()).unwrap_or(0),
            delivery: items.get(3).and_then(|v| v.as_i64()).unwrap_or(0),
        }
    }
}

/// Redis Stream 队列。
pub struct RedisStream {
    redis: FullRedis,
    key: String,
    settings: QueueSettings,

    /// 死信重试间隔（秒）。默认 60，对应 C# `RetryInterval`
    pub retry_interval_seconds: i64,
    /// 基元类型消息的字段名。默认 `__data`
    pub primitive_key: String,
    /// 最大长度，超过则裁剪（生产时每 1000 条带一次 `MAXLEN ~`）。默认 100 万
    pub max_length: i64,
    /// 最大重试次数，超过则丢弃死信。默认 10
    pub max_retry: i64,
    /// 阻塞读取毫秒数。默认 15000，对应 C# `BlockTime`
    pub block_time_ms: i64,
    /// 独立消费时的起始 Id（消费组模式不使用）。默认 `0-0`
    pub start_id: String,
    /// 消费组名称。设置后使用消费组消费（可用 [`RedisStream::set_group`] 自动创建）
    pub group: Option<String>,
    /// 消费者名称（同组内唯一）
    pub consumer: String,
    /// 首次消费策略：true 表示从最后位置开始（`SETID $`）
    pub from_last_offset: bool,

    count: AtomicI64,
    set_group_id: AtomicI32,
    claims: AtomicI64,
    next_retry: Mutex<NaiveDateTime>,
}

impl RedisStream {
    /// 由工厂方法创建（[`FullRedis::get_stream`]）。键自动补前缀。
    pub fn new(redis: FullRedis, topic: &str) -> Self {
        let key = redis.get_key(topic);
        let consumer = format!(
            "{}@{}",
            machine_name().unwrap_or_else(|| "unknown".into()),
            new_consumer_key()
        );

        Self {
            redis,
            key,
            settings: QueueSettings::default(),
            retry_interval_seconds: 60,
            primitive_key: "__data".into(),
            max_length: 1_000_000,
            max_retry: 10,
            block_time_ms: 15_000,
            start_id: "0-0".into(),
            group: None,
            consumer,
            from_last_offset: false,
            count: AtomicI64::new(0),
            set_group_id: AtomicI32::new(0),
            claims: AtomicI64::new(0),
            next_retry: Mutex::new(NaiveDateTime::default()),
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

    /// 设置消费组，不存在则自动创建（对应 C# `SetGroup`）。
    pub fn set_group(&mut self, group: &str) -> Result<bool> {
        self.group = Some(group.to_string());

        let exists = self
            .get_groups()?
            .into_iter()
            .any(|g| g.name == group);
        if exists {
            Ok(false)
        } else {
            self.group_create(group, None)
        }
    }

    /// 消息总数（`XLEN`）。
    pub fn count(&self) -> Result<i64> {
        Ok(int_or(self.call(&[b"XLEN", self.key.as_bytes()])?, 0))
    }

    /// 是否为空。
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.count()? == 0)
    }

    // ================== 生产 ==================

    /// 生产消息（`XADD`）。
    ///
    /// 编码规则与 C# `AddInternal` 一致：
    /// - 基元（字符串/数字/布尔/时间）→ 单字段 `[{primitive_key}, 编码文本]`；
    /// - 结构体/字典对象 → 属性名与值扁平化（属性名建议 PascalCase）；
    /// - 数组 → 两两一组作为字段名/字段值。
    pub fn add<S: Serialize>(&self, value: &S, msg_id: Option<&str>) -> Result<Option<String>> {
        let json = serde_json::to_value(value)?;
        let fields = value_to_fields(&json, &self.primitive_key)?;

        let n = self.count.fetch_add(1, Ordering::AcqRel) + 1;
        let trim = self.max_length > 0 && n % 1000 == 0;

        self.add_fields(&fields, msg_id, trim)
    }

    /// 直接写字段（`XADD`），字段值已是编码后的字节。
    pub fn add_fields(
        &self,
        fields: &[(String, Vec<u8>)],
        msg_id: Option<&str>,
        trim: bool,
    ) -> Result<Option<String>> {
        if fields.is_empty() {
            return Err(Error::Type("XADD 需要至少一个字段".into()));
        }

        let mut args: Vec<Vec<u8>> = Vec::with_capacity(fields.len() * 2 + 6);
        args.push(b"XADD".to_vec());
        args.push(self.key.as_bytes().to_vec());
        if trim {
            args.push(b"MAXLEN".to_vec());
            args.push(b"~".to_vec());
            args.push(self.max_length.to_string().into_bytes());
        }
        args.push(msg_id.unwrap_or("*").as_bytes().to_vec());
        for (k, v) in fields {
            args.push(k.as_bytes().to_vec());
            args.push(v.clone());
        }

        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();

        // 与 C# 一致：返回空视为失败，按配置重试
        for attempt in 0..=self.settings.retry_times_when_send_failed {
            let id = self.call(&refs)?.as_string();
            if id.is_some() {
                return Ok(id);
            }
            if attempt < self.settings.retry_times_when_send_failed {
                sleep(Duration::from_millis(self.settings.retry_interval_ms));
            }
        }

        if self.settings.throw_on_failure {
            return Err(Error::Operation(format!("发布到队列[{}]失败！", self.key)));
        }
        Ok(None)
    }

    // ================== 消费 ==================

    /// 批量消费原始消息（对应 C# `TakeMessagesAsync`）。
    ///
    /// - 消费组模式：优先处理 `retry_ack` 抢回的挂起消息，再 `>` 读取新消息，最后用 `0` 补读自身挂起；
    /// - 独立模式：按 [`RedisStream::start_id`] 顺序 `XREAD` 并自动前移游标。
    pub fn take_messages(&mut self, count: usize, block_ms: i64) -> Result<Vec<Message>> {
        let group = self.group.clone();

        if let Some(group) = &group {
            if self.from_last_offset && self.set_group_id.swap(1, Ordering::AcqRel) == 0 {
                self.group_set_id(group, "$")?;
            }

            let stolen = self.retry_ack()? as i64;
            if stolen > 0 {
                self.claims.fetch_add(stolen, Ordering::AcqRel);
            }

            if self.claims.load(Ordering::Acquire) > 0 {
                let rs = self.read_group(group, &self.consumer.clone(), count, 3_000, Some("0"))?;
                if !rs.is_empty() {
                    self.claims
                        .fetch_sub(rs.len() as i64, Ordering::AcqRel);
                    return Ok(rs);
                }
                self.claims.store(0, Ordering::Release);
            }
        }

        let rs = match &group {
            Some(g) => self.read_group(g, &self.consumer.clone(), count, block_ms, Some(">"))?,
            None => self.read_blocking(self.start_id.clone(), count, block_ms)?,
        };

        if !rs.is_empty() {
            if group.is_none()
                && let Some(last) = rs.last() {
                    self.start_id = last.id.clone();
                }
            return Ok(rs);
        }

        // 消费组：拿不到新消息时，尝试补读自身挂起消息（含抢来的）
        if let Some(g) = &group {
            return self.read_group(g, &self.consumer.clone(), count, 3_000, Some("0"));
        }

        Ok(Vec::new())
    }

    /// 消费一条原始消息。
    pub fn take_message(&mut self) -> Result<Option<Message>> {
        Ok(self.take_messages(1, 0)?.into_iter().next())
    }

    /// 批量消费基元消息（`__data` 字段解码，规则同编码器）。
    pub fn take_bodies<B: FromRedisPayload>(&mut self, count: usize) -> Result<Vec<B>> {
        Ok(self
            .take_messages(count, 0)?
            .iter()
            .filter_map(|m| m.primitive::<B>())
            .collect())
    }

    /// 批量消费对象消息（字段映射为结构体，属性名需与字段名一致）。
    pub fn take_structs<T: DeserializeOwned>(&mut self, count: usize) -> Result<Vec<T>> {
        Ok(self
            .take_messages(count, 0)?
            .iter()
            .filter_map(|m| m.to_struct::<T>())
            .collect())
    }

    /// 消费一条基元消息。
    pub fn take_one_body<B: FromRedisPayload>(&mut self) -> Result<Option<B>> {
        Ok(self.take_bodies::<B>(1)?.into_iter().next())
    }

    /// 消费一条对象消息。
    pub fn take_one_struct<T: DeserializeOwned>(&mut self) -> Result<Option<T>> {
        Ok(self.take_structs::<T>(1)?.into_iter().next())
    }

    /// 确认消息（`XACK`）。返回确认成功条数。
    pub fn acknowledge(&self, ids: &[&str]) -> Result<i64> {
        let Some(group) = &self.group else {
            return Ok(0);
        };
        if ids.is_empty() {
            return Ok(0);
        }

        let mut args: Vec<&[u8]> = Vec::with_capacity(ids.len() + 3);
        args.push(b"XACK");
        args.push(self.key.as_bytes());
        args.push(group.as_bytes());
        for id in ids {
            args.push(id.as_bytes());
        }
        Ok(int_or(self.call(&args)?, 0))
    }

    /// 处理未确认死信（对应 C# `RetryAck`）：`XPENDING` + `XCLAIM` 抢占，超次数丢弃，清理空闲消费者。
    ///
    /// 返回本次抢回的消息数。
    pub fn retry_ack(&mut self) -> Result<usize> {
        let Some(group) = self.group.clone() else {
            return Ok(0);
        };

        let now = Local::now().naive_local();
        {
            let mut next = self.next_retry.lock().unwrap();
            if *next >= now {
                return Ok(0);
            }
            *next = now + chrono::Duration::seconds(self.retry_interval_seconds);
        }

        // 消费组不存在时自动创建（Redis 重启/主从切换后可能丢失）
        self.set_group(&group)?;

        let ms_idle = self.retry_interval_seconds * 1000;
        let mut count = 0usize;
        let mut id: Option<String> = None;
        let mut times = 10;

        while times > 0 {
            times -= 1;

            let list = self.pending(&group, id.as_deref(), None, 100)?;
            if list.is_empty() {
                break;
            }
            let last_id = list.last().map(|p| p.id.clone());

            for item in &list {
                if item.id.is_empty() {
                    continue;
                }

                if item.consumer == self.consumer {
                    // 不要抢自己的消息；失败次数过多的直接确认丢弃
                    if item.delivery >= self.max_retry {
                        self.ack_one(&group, &item.id)?;
                    }
                } else if item.idle > ms_idle {
                    if item.delivery >= self.max_retry {
                        // 抢夺后确认，即丢弃
                        self.claim(&group, &self.consumer.clone(), &item.id, ms_idle)?;
                        self.ack_one(&group, &item.id)?;
                    } else {
                        // 抢夺消息，所有者改为当前消费者
                        self.claim(&group, &self.consumer.clone(), &item.id, ms_idle)?;
                        count += 1;
                    }
                }
            }

            // 下一个开始 Id：最后一条的毫秒序号 +1
            match last_id {
                Some(last) if !last.is_empty() => {
                    id = next_stream_id_prefix(&last);
                    if id.is_none() {
                        break;
                    }
                }
                _ => break,
            }
        }

        // 清理空闲消费者：无挂起且超 1 小时不活跃
        for consumer in self.get_consumers(&group)? {
            if !consumer.name.is_empty() && consumer.pending == 0 && consumer.idle > 3_600_000 {
                self.group_delete_consumer(&group, &consumer.name)?;
            }
        }

        Ok(count)
    }

    fn ack_one(&self, group: &str, id: &str) -> Result<i64> {
        Ok(int_or(
            self.call(&[b"XACK", self.key.as_bytes(), group.as_bytes(), id.as_bytes()])?,
            0,
        ))
    }

    // ================== 内部命令 ==================

    /// 删除消息（`XDEL`）。
    pub fn delete(&self, id: &str) -> Result<i64> {
        Ok(int_or(
            self.call(&[b"XDEL", self.key.as_bytes(), id.as_bytes()])?,
            0,
        ))
    }

    /// 裁剪长度（`XTRIM MAXLEN`）。
    pub fn trim(&self, max_len: i64, accurate: bool) -> Result<i64> {
        let len = max_len.to_string();
        let rs = if accurate {
            self.call(&[b"XTRIM", self.key.as_bytes(), b"MAXLEN", len.as_bytes()])?
        } else {
            self.call(&[b"XTRIM", self.key.as_bytes(), b"MAXLEN", b"~", len.as_bytes()])?
        };
        Ok(int_or(rs, 0))
    }

    /// 丢弃指定时间之前的消息（`XTRIM MINID`，Redis 6.2+）。
    ///
    /// 说明：C# 侧用秒级时间戳拼接（`ToLong()` 为秒，而 Stream Id 前缀是毫秒），本实现按毫秒处理。
    pub fn trim_before(&self, time: NaiveDateTime) -> Result<i64> {
        let ms = time.and_utc().timestamp_millis();
        Ok(int_or(
            self.call(&[
                b"XTRIM",
                self.key.as_bytes(),
                b"MINID",
                format!("{ms}").as_bytes(),
            ])?,
            0,
        ))
    }

    /// 区间读取（`XRANGE`，`-`/`+` 表示最小/最大）。
    pub fn range(&self, start_id: Option<&str>, end_id: Option<&str>, count: i64) -> Result<Vec<Message>> {
        let start = start_id.filter(|s| !s.is_empty()).unwrap_or("-");
        let end = end_id.filter(|s| !s.is_empty()).unwrap_or("+");

        let rs = if count > 0 {
            self.call(&[
                b"XRANGE",
                self.key.as_bytes(),
                start.as_bytes(),
                end.as_bytes(),
                b"COUNT",
                count.to_string().as_bytes(),
            ])?
        } else {
            self.call(&[
                b"XRANGE",
                self.key.as_bytes(),
                start.as_bytes(),
                end.as_bytes(),
            ])?
        };

        Ok(parse_messages(rs))
    }

    /// 时间段读取（按毫秒时间戳换算 Id）。
    pub fn range_time(
        &self,
        start: NaiveDateTime,
        end: NaiveDateTime,
        count: i64,
    ) -> Result<Vec<Message>> {
        let s = format!("{}-0", start.and_utc().timestamp_millis());
        let e = format!("{}-0", end.and_utc().timestamp_millis());
        self.range(Some(&s), Some(&e), count)
    }

    /// 独立消费（`XREAD`）。
    pub fn read(&self, start_id: &str, count: usize, block_ms: i64) -> Result<Vec<Message>> {
        self.read_blocking(start_id.to_string(), count, block_ms)
    }

    fn read_blocking(&self, start_id: String, count: usize, block_ms: i64) -> Result<Vec<Message>> {
        let mut args: Vec<Vec<u8>> = vec![b"XREAD".to_vec()];
        if block_ms > 0 {
            args.push(b"BLOCK".to_vec());
            args.push(block_ms.to_string().into_bytes());
        }
        if count > 0 {
            args.push(b"COUNT".to_vec());
            args.push(count.to_string().into_bytes());
        }
        args.push(b"STREAMS".to_vec());
        args.push(self.key.as_bytes().to_vec());
        args.push(start_id.as_bytes().to_vec());

        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        let rs = self.call_blocking(&refs, block_ms)?;
        Ok(parse_xread(rs))
    }

    /// 消费组消费（`XREADGROUP`）。`id` 为 `>` 表示新消息，`0` 表示自身挂起。
    pub fn read_group(
        &self,
        group: &str,
        consumer: &str,
        count: usize,
        block_ms: i64,
        id: Option<&str>,
    ) -> Result<Vec<Message>> {
        let id = id.filter(|s| !s.is_empty()).unwrap_or(">");

        let mut args: Vec<Vec<u8>> = vec![
            b"XREADGROUP".to_vec(),
            b"GROUP".to_vec(),
            group.as_bytes().to_vec(),
            consumer.as_bytes().to_vec(),
        ];
        if block_ms > 0 {
            args.push(b"BLOCK".to_vec());
            args.push(block_ms.to_string().into_bytes());
        }
        if count > 0 {
            args.push(b"COUNT".to_vec());
            args.push(count.to_string().into_bytes());
        }
        args.push(b"STREAMS".to_vec());
        args.push(self.key.as_bytes().to_vec());
        args.push(id.as_bytes().to_vec());

        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        let rs = self.call_blocking(&refs, block_ms)?;
        Ok(parse_xread(rs))
    }

    /// 等待列表汇总（`XPENDING key group`）。
    pub fn pending_info(&self, group: &str) -> Result<Option<PendingInfo>> {
        let rs = self.call(&[b"XPENDING", self.key.as_bytes(), group.as_bytes()])?;
        if rs.is_null() {
            return Ok(None);
        }
        if rs.as_array().is_none() {
            return Ok(None);
        }
        Ok(Some(PendingInfo::from_items(&rs.into_array().unwrap_or_default())))
    }

    /// 等待列表明细（`XPENDING key group start end count`）。
    pub fn pending(
        &self,
        group: &str,
        start_id: Option<&str>,
        end_id: Option<&str>,
        count: i64,
    ) -> Result<Vec<PendingItem>> {
        let start = start_id.filter(|s| !s.is_empty()).unwrap_or("-");
        let end = end_id.filter(|s| !s.is_empty()).unwrap_or("+");

        let rs = if count > 0 {
            self.call(&[
                b"XPENDING",
                self.key.as_bytes(),
                group.as_bytes(),
                start.as_bytes(),
                end.as_bytes(),
                count.to_string().as_bytes(),
            ])?
        } else {
            self.call(&[
                b"XPENDING",
                self.key.as_bytes(),
                group.as_bytes(),
                start.as_bytes(),
                end.as_bytes(),
            ])?
        };

        Ok(rs
            .into_array()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|v| {
                let items = v.into_array()?;
                Some(PendingItem::from_items(&items))
            })
            .collect())
    }

    /// 抢夺挂起消息（`XCLAIM`）。
    pub fn claim(&self, group: &str, consumer: &str, id: &str, ms_idle: i64) -> Result<RespValue> {
        self.call(&[
            b"XCLAIM",
            self.key.as_bytes(),
            group.as_bytes(),
            consumer.as_bytes(),
            ms_idle.to_string().as_bytes(),
            id.as_bytes(),
        ])
    }

    // ================== 消费组 ==================

    /// 创建消费组（`XGROUP CREATE ... MKSTREAM`）。`start_id` 默认 `0`。
    pub fn group_create(&self, group: &str, start_id: Option<&str>) -> Result<bool> {
        let start = start_id.filter(|s| !s.is_empty()).unwrap_or("0");
        let rs = self.call(&[
            b"XGROUP",
            b"CREATE",
            self.key.as_bytes(),
            group.as_bytes(),
            start.as_bytes(),
            b"MKSTREAM",
        ])?;
        Ok(rs.as_string().as_deref() == Some("OK"))
    }

    /// 销毁消费组（`XGROUP DESTROY`）。
    pub fn group_destroy(&self, group: &str) -> Result<i64> {
        Ok(int_or(
            self.call(&[b"XGROUP", b"DESTROY", self.key.as_bytes(), group.as_bytes()])?,
            0,
        ))
    }

    /// 删除消费者（`XGROUP DELCONSUMER`），返回其挂起消息数。
    pub fn group_delete_consumer(&self, group: &str, consumer: &str) -> Result<i64> {
        Ok(int_or(
            self.call(&[
                b"XGROUP",
                b"DELCONSUMER",
                self.key.as_bytes(),
                group.as_bytes(),
                consumer.as_bytes(),
            ])?,
            0,
        ))
    }

    /// 设置消费组起始 Id（`XGROUP SETID`）。`start_id` 默认 `$`。
    pub fn group_set_id(&self, group: &str, start_id: &str) -> Result<bool> {
        let start = if start_id.is_empty() { "$" } else { start_id };
        let rs = self.call(&[
            b"XGROUP",
            b"SETID",
            self.key.as_bytes(),
            group.as_bytes(),
            start.as_bytes(),
        ])?;
        Ok(rs.as_string().as_deref() == Some("OK"))
    }

    // ================== 队列信息 ==================

    /// 流信息（`XINFO STREAM`），键不存在返回 `None`。
    pub fn get_info(&self) -> Result<Option<StreamInfo>> {
        match self.call(&[b"XINFO", b"STREAM", self.key.as_bytes()]) {
            Ok(rs) => {
                let items = rs.into_array().unwrap_or_default();
                Ok(Some(StreamInfo::from_items(&items)))
            }
            Err(Error::Server(msg)) if msg.to_uppercase().contains("NO SUCH KEY") => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// 消费组列表（`XINFO GROUPS`），键不存在返回空。
    pub fn get_groups(&self) -> Result<Vec<GroupInfo>> {
        match self.call(&[b"XINFO", b"GROUPS", self.key.as_bytes()]) {
            Ok(rs) => Ok(rs
                .into_array()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| v.into_array().map(|items| GroupInfo::from_items(&items)))
                .collect()),
            Err(Error::Server(msg)) if msg.to_uppercase().contains("NO SUCH KEY") => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }

    /// 消费者列表（`XINFO CONSUMERS`）。
    pub fn get_consumers(&self, group: &str) -> Result<Vec<ConsumerInfo>> {
        match self.call(&[b"XINFO", b"CONSUMERS", self.key.as_bytes(), group.as_bytes()]) {
            Ok(rs) => Ok(rs
                .into_array()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| v.into_array().map(|items| ConsumerInfo::from_items(&items)))
                .collect()),
            Err(Error::Server(msg))
                if msg.to_uppercase().contains("NO SUCH KEY") || msg.to_uppercase().contains("NOGROUP") =>
            {
                Ok(Vec::new())
            }
            Err(e) => Err(e),
        }
    }

    // ================== 底层 ==================

    fn call(&self, args: &[&[u8]]) -> Result<RespValue> {
        self.redis.redis().execute(args)
    }

    fn call_blocking(&self, args: &[&[u8]], block_ms: i64) -> Result<RespValue> {
        if block_ms <= 0 {
            return self.call(args);
        }

        let seconds = block_ms / 1000 + 2;
        match self.redis.redis().execute_blocking(args, seconds) {
            Ok(v) => Ok(v),
            // Redis 的 BLOCK 超时返回空数组/空值；网络层读超时同样按无消息处理
            Err(Error::Io(e))
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                Ok(RespValue::Null)
            }
            Err(e) => Err(e),
        }
    }
}

/// 序列化的值与 C# `ToDictionary()` 的字段展开规则（基元 → `__data`；对象/数组 → 扁平字段）。
fn value_to_fields(value: &Value, primitive_key: &str) -> Result<Vec<(String, Vec<u8>)>> {
    match value {
        Value::Object(map) => {
            let mut fields = Vec::with_capacity(map.len());
            for (k, v) in map {
                fields.push((k.clone(), encode_field_value(v)?));
            }
            Ok(fields)
        }
        Value::Array(items) => {
            let mut fields = Vec::with_capacity(items.len());
            let mut i = 0;
            while i + 1 < items.len() {
                let name = items[i].as_str().ok_or_else(|| {
                    Error::Type("数组消息的元素需要两两成对（字段名、字段值）".into())
                })?;
                fields.push((name.to_string(), encode_field_value(&items[i + 1])?));
                i += 2;
            }
            Ok(fields)
        }
        other => Ok(vec![(
            primitive_key.to_string(),
            encode_field_value(other)?,
        )]),
    }
}

/// 字段值编码：字符串原样、数字/布尔/时间文本、嵌套对象 JSON（与 C# 编码器一致）。
fn encode_field_value(value: &Value) -> Result<Vec<u8>> {
    match value {
        Value::String(s) => Ok(s.as_bytes().to_vec()),
        Value::Number(n) => Ok(n.to_string().into_bytes()),
        Value::Bool(b) => Ok(if *b { b"True".to_vec() } else { b"False".to_vec() }),
        Value::Null => Ok(Vec::new()),
        other => Ok(serde_json::to_vec(other)?),
    }
}

/// 解析 `XRANGE` / `XREAD` 的单条消息。
pub fn parse_entry(value: &RespValue) -> Option<Message> {
    let items = value.as_array()?;
    if items.len() != 2 {
        return None;
    }

    let id = items[0].as_string()?;
    let body = items[1]
        .as_array()?
        .iter()
        .map(|v| v.as_string().unwrap_or_default())
        .collect();

    Some(Message { id, body })
}

/// 解析 `XRANGE` 应答（消息数组）。
pub fn parse_messages(value: RespValue) -> Vec<Message> {
    value
        .into_array()
        .unwrap_or_default()
        .iter()
        .filter_map(parse_entry)
        .collect()
}

/// 解析 `XREAD` / `XREADGROUP` 应答：`[[key, [entry...]]]`。
fn parse_xread(value: RespValue) -> Vec<Message> {
    let Some(streams) = value.into_array() else {
        return Vec::new();
    };
    if streams.len() != 1 {
        return Vec::new();
    }
    let Some(items) = streams[0].as_array() else {
        return Vec::new();
    };
    if items.len() != 2 {
        return Vec::new();
    }

    items[1]
        .as_array()
        .unwrap_or_default()
        .iter()
        .filter_map(parse_entry)
        .collect()
}

/// `1695792000000-3` → `1695792000001-0`（用于 XPENDING 翻页）。
fn next_stream_id_prefix(id: &str) -> Option<String> {
    let (ms, _) = id.split_once('-')?;
    let ms: i64 = ms.parse().ok()?;
    Some(format!("{}-0", ms + 1))
}

/// Stream Id 前缀（毫秒）转本地时间。
fn id_to_local_time(id: &str) -> Option<NaiveDateTime> {
    let (ms, _) = id.split_once('-')?;
    let ms: i64 = ms.parse().ok()?;
    let utc = Utc.timestamp_millis_opt(ms).single()?;
    let dt: DateTime<Utc> = utc;
    Some(dt.with_timezone(&Local).naive_local())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitive_value_matches_csharp_data_field() {
        let msg = Message {
            id: "1-0".into(),
            body: vec!["__data".into(), "hello".into()],
        };
        assert_eq!(msg.primitive_value(), Some(b"hello".as_slice()));
        assert_eq!(msg.primitive::<String>(), Some("hello".into()));
        assert_eq!(
            msg.primitive::<NaiveDateTime>(),
            None,
            "非时间文本不应解析成功"
        );
    }

    #[test]
    fn object_message_maps_to_struct() {
        use serde::Deserialize;

        #[derive(Deserialize, Debug, PartialEq)]
        #[serde(rename_all = "PascalCase")]
        struct Demo {
            name: String,
            count: i32,
            #[serde(with = "crate::encoder::datetime_text")]
            create_time: NaiveDateTime,
        }

        // C# 端对象消息的字段编码：属性名 + 编码器文本（时间带 .fff）
        let msg = Message {
            id: "1-0".into(),
            body: vec![
                "Name".into(),
                "互通Demo".into(),
                "Count".into(),
                "7".into(),
                "CreateTime".into(),
                "2026-09-26 10:00:00.123".into(),
            ],
        };

        let demo = msg.to_struct::<Demo>().expect("应能映射为结构体");
        assert_eq!(demo.name, "互通Demo");
        assert_eq!(demo.count, 7);
        assert_eq!(demo.create_time.to_string(), "2026-09-26 10:00:00.123");
    }

    #[test]
    fn leading_zero_strings_survive_string_first_pass() {
        use serde::Deserialize;

        #[derive(Deserialize, Debug, PartialEq)]
        struct Row {
            code: String,
        }

        let msg = Message {
            id: "1-0".into(),
            body: vec!["code".into(), "0012".into()],
        };
        assert_eq!(msg.to_struct::<Row>().unwrap().code, "0012");
    }

    #[test]
    fn value_expansion_matches_csharp_rules() {
        let fields = value_to_fields(&serde_json::json!("hello"), "__data").unwrap();
        assert_eq!(fields, vec![("__data".to_string(), b"hello".to_vec())]);

        let fields = value_to_fields(&serde_json::json!(7), "__data").unwrap();
        assert_eq!(fields, vec![("__data".to_string(), b"7".to_vec())]);

        let fields = value_to_fields(&serde_json::json!(true), "__data").unwrap();
        assert_eq!(fields, vec![("__data".to_string(), b"True".to_vec())]);

        let fields = value_to_fields(&serde_json::json!({"Name": "x", "Count": 7}), "__data").unwrap();
        assert!(fields.contains(&("Name".to_string(), b"x".to_vec())));
        assert!(fields.contains(&("Count".to_string(), b"7".to_vec())));

        let fields = value_to_fields(&serde_json::json!(["k1", "v1", "k2", "v2"]), "__data").unwrap();
        assert_eq!(
            fields,
            vec![
                ("k1".to_string(), b"v1".to_vec()),
                ("k2".to_string(), b"v2".to_vec()),
            ]
        );
    }

    #[test]
    fn stream_id_helpers() {
        assert_eq!(next_stream_id_prefix("1695792000000-3"), Some("1695792000001-0".into()));
        assert_eq!(next_stream_id_prefix("bad"), None);

        let t = id_to_local_time("1695792000000-0").unwrap();
        assert!(t.to_string().starts_with("2023-09-27"));
    }

    #[test]
    fn parse_xread_shape() {
        // 模拟 RESP2：[[key, [[id, [f, v]], ...]]]
        let value = RespValue::Array(vec![RespValue::Array(vec![
            RespValue::Bulk(b"stream".to_vec()),
            RespValue::Array(vec![RespValue::Array(vec![
                RespValue::Bulk(b"1-0".to_vec()),
                RespValue::Array(vec![
                    RespValue::Bulk(b"__data".to_vec()),
                    RespValue::Bulk(b"hi".to_vec()),
                ]),
            ])]),
        ])]);

        let msgs = parse_xread(value);
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].id, "1-0");
        assert_eq!(msgs[0].primitive::<String>(), Some("hi".into()));
    }
}
