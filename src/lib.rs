//! # pek-rredis —— DH.NRedis 的 Rust 实现
//!
//! Pek 生态的 Rust Redis 客户端（独立项目）：让 C#/.NET 项目（DH.NRedis 技术栈）能够
//! **渐进迁移**到 Rust——两边连接**同一个 Redis**、使用**同一套键与字节格式**，
//! 业务可以一个用例一个用例地从 C# 搬到 Rust，而不是推倒重来。
//!
//! ```text
//! ┌─────────────────────────┐        ┌─────────────────────────┐
//! │  C# 现有系统（DH.NRedis）│        │  Rust 新代码（Pek.RRedis）│
//! │  FullRedis / RedisHash  │  互通  │  FullRedis / RedisHash  │
//! │  Queue / PubSub / Lock  │◄──────►│  Queue / PubSub / Lock  │
//! └───────────┬─────────────┘        └───────────┬─────────────┘
//!             │        同一 Redis 实例 / 同一字节格式              │
//!             └───────────────┬───────────────────┘
//!                             ▼
//!                     Redis（RESP2 / RESP3）
//! ```
//!
//! ## 概念对照
//!
//! | C# / DH.NRedis | Rust / pek-rredis |
//! |----------------|-------------------|
//! | `new FullRedis(server, pwd, db)` / `Init(config)` | [`FullRedis::open`] / [`FullRedis::from_config`] |
//! | `Redis`（基础命令） | [`Redis`]（[`set`](Redis::set)、[`get`](Redis::get)、[`get_all`](Redis::get_all)、[`set_all`](Redis::set_all)、[`eval`](Redis::eval)…） |
//! | `RedisClient`（RESP 协议） | [`client::RedisClient`]、[`resp`] |
//! | `RedisJsonEncoder` / `DefaultPacketEncoder` | [`encoder`]（[`ToRedisPayload`] / [`FromRedisPayload`] / [`Json<T>`](Json)） |
//! | 连接池 `ObjectPool<RedisClient>` | [`pool::Pool`] |
//! | `RedisHash<TKey,TValue>` | [`RedisHash<V, K=String>`](RedisHash) |
//! | `RedisList<T>` / `RedisSet<T>` / `RedisSortedSet<T>` / `RedisStack<T>` | [`RedisList`] / [`RedisSet`] / [`RedisSortedSet`] / [`RedisStack`] |
//! | `RedisGeo` / `HyperLogLog` / `PubSub` | [`RedisGeo`] / [`HyperLogLog`] / [`PubSub`] |
//! | `RedisQueue<T>` / `RedisReliableQueue<T>` / `RedisDelayQueue<T>` | [`RedisQueue`] / [`RedisReliableQueue`] / [`RedisDelayQueue`] |
//! | `RedisStream<T>`（Stream 消息队列） | [`RedisStream`]（非泛型：`add` 接受任意 `Serialize`，`take_bodies`/`take_structs` 指定消费类型） |
//! | `QueueBase.AttachTraceId`（链路追踪注入） | [`queues::QueueSettings::trace`](queues::QueueSettings) |
//! | `Cache.AcquireLock`（分布式锁） | [`FullRedis::acquire_lock`] |
//! | `RedisRedLock`（多实例分布式锁） | [`acquire_red_lock`] / [`FullRedis::acquire_red_lock`]（[`RedLock`]） |
//! | `QueueExtensions.ConsumeAsync<T>`（类型化消费循环） | [`RedisReliableQueue::consume_json`] / [`consume_raw`](RedisReliableQueue::consume_raw) |
//! | Tair 扩展（阿里云 `Ex*`） | [`FullRedis::ex_set`] / [`FullRedis::ex_get`] / [`FullRedis::ex_hset`] …（[`tair`]） |
//! | `Redis.ServerType` / `Redis.Version` | [`Redis::server_type`] / [`Redis::version_parts`] / [`Redis::require_version`] |
//! | `StartPipeline` / `StopPipeline` | [`Redis::pipeline`] / [`Pipeline`] |
//!
//! ## 互通保证（与 C# 端同一字节格式）
//!
//! | 数据类型 | 存储格式（两端一致） |
//! |----------|----------------------|
//! | 字符串 | 原始 UTF-8（**不加引号**） |
//! | 布尔 | `"True"` / `"False"`（读取兼容 `true/OK/1`） |
//! | 整数/浮点 | 往返最短文本（`123`、`1.5`） |
//! | 时间 | `yyyy-MM-dd HH:mm:ss.fff`（读取兼容 ISO 8601） |
//! | 复杂对象 | JSON（属性名建议 PascalCase 以对齐 C#） |
//! | 队列 | `LPUSH`/`RPOP`、确认队列 `{key}:Ack:{ukey}`、状态键 `{key}:Status:{ukey}`（JSON PascalCase）、AllStatus 抢锁 |
//! | 延迟队列 | `ZADD score = Unix 秒 + 延迟`，`ZRANGEBYSCORE` + `ZREM` 抢占 |
//! | 分布式锁 | `{token}|{绝对过期毫秒}` + 秒级 TTL + `GETSET` 抢占超时锁 |
//!
//! ## 快速开始
//!
//! ```no_run
//! use pek_rredis::{encoder::Json, FullRedis, Result};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! #[serde(rename_all = "PascalCase")] // 与 C# 属性名对齐
//! struct User {
//!     name: String,
//!     // JSON 内时间：兼容 C# FastJson 文本格式与 ISO 8601
//!     #[serde(with = "pek_rredis::encoder::datetime")]
//!     create_time: chrono::NaiveDateTime,
//! }
//!
//! # fn main() -> Result<()> {
//! // 1) 连接（连接字符串格式与 C# 相同）
//! let redis = FullRedis::from_config("server=127.0.0.1:6379;password=123456;db=7;prefix=app:")?;
//!
//! // 2) 基本读写：与 C# FullRedis 行为一致（SETEX / GET）
//! let user = User { name: "NewLife".into(), create_time: chrono::Local::now().naive_local() };
//! redis.redis().set("user", Json(&user), 3600)?;
//! let user2: Option<Json<User>> = redis.redis().get("user")?;
//! assert!(user2.is_some());
//!
//! // 3) 数据结构
//! let hash = redis.get_hash::<i64>("counter");
//! hash.incr_by(&"pv".to_string(), 1)?;
//!
//! // 4) 队列（C# 与 Rust 可互相生产/消费）
//! let queue = redis.get_queue::<String>("orders");
//! queue.add(&"order-001".to_string())?;
//! let item = queue.take_one(5)?;
//! println!("{item:?}");
//!
//! // 5) 分布式锁（与 C# CacheLock 互斥）
//! let lock = redis.acquire_lock("order:lock", 5000)?;
//! // ... 业务 ...
//! drop(lock); // 自动释放
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod encoder;
mod error;
pub mod full;
pub mod geo;
pub mod hash;
pub mod hyperloglog;
pub mod list;
pub mod options;
pub mod pool;
pub mod pubsub;
pub mod queues;
pub mod redis;
pub mod resp;
pub mod services;
pub mod set;
pub mod sortedset;
pub mod stack;
pub mod tair;
pub(crate) mod util;

pub use encoder::{FromRedisPayload, Json, ToRedisPayload};
pub use error::{Error, Result};
pub use full::{FullRedis, LockHandle, SlowLogEntry};
pub use options::{RedisOptions, RedisPoolConfig};
pub use redis::{Pipeline, Redis, ServerType};
pub use resp::RespValue;
pub use services::{acquire_red_lock, RedLock};

// 常用类型直达
pub use geo::{GeoMember, RedisGeo};
pub use hash::RedisHash;
pub use hyperloglog::HyperLogLog;
pub use list::RedisList;
pub use pubsub::PubSub;
pub use queues::{
    ConsumerInfo, GroupInfo, Message, PendingInfo, PendingItem, RedisDelayQueue, RedisQueue,
    RedisQueueStatus, RedisReliableQueue, RedisStream, StreamInfo,
};
pub use set::RedisSet;
pub use sortedset::RedisSortedSet;
pub use stack::RedisStack;
