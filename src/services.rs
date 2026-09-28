//! 服务扩展：RedLock 分布式锁，以及 RedisDeferred / RedisStat / RedisEventBus
//! 这类更偏宿主层的高阶语义封装。
//!
//! 算法与 C# 完全一致：
//! - 令牌为 22 位随机字符串；
//! - 依次在全部实例上以「秒级过期」（`ms_expire / 1000`，整数除法）写入令牌；
//! - `quorum = n / 2 + 1` 个实例成功，且 `ms_expire - 耗时 - ms_expire * 1% > 0` 时视为获取成功；
//! - 失败时回滚已写实例，按 `RetryDelay(200ms) + rand(0..50ms)` 重试直到 `ms_timeout` 超时；
//! - 释放时使用与 C# 相同的 Lua 脚本（比较令牌后删除，避免误删他人锁）。
//!
//! > 注意：与 C# 保持一致，加锁写入使用的是**普通 `SET`（非 `NX`）**。
//! > 这是 DH.NRedis 的原样行为（互操作需要一致），其安全性依赖业务方对
//! > 「同一 key 的 RedLock 只由一个语言端持有」的约定。

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::Duration as ChronoDuration;
use chrono::Local;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};
use crate::full::FullRedis;
use crate::queues::{Message, RedisStream};
use crate::set::RedisSet;

#[cfg(test)]
#[path = "../tests/support/mod.rs"]
mod test_support;

/// 解锁 Lua（与 C# `RedisRedLock.TryUnlock` 完全相同）。
const UNLOCK_SCRIPT: &str = "if redis.call('get', KEYS[1]) == ARGV[1] then\n    return redis.call('del', KEYS[1])\nelse\n    return 0\nend";

/// RedLock 分布式锁句柄。`Drop` 时自动在已加锁实例上释放。
pub struct RedLock {
    key: String,
    token: String,
    instances: Vec<FullRedis>,
    clock_drift_factor: f64,
    retry_delay_ms: i64,
}

impl RedLock {
    /// 锁键名（已含前缀）。
    pub fn key(&self) -> &str {
        &self.key
    }

    /// 随机令牌。
    pub fn token(&self) -> &str {
        &self.token
    }

    /// 成功加锁的实例数量。
    pub fn locked_count(&self) -> usize {
        self.instances.len()
    }

    /// 时钟漂移补偿因子（与 C# 默认一致：0.01 = 1%）。
    pub fn clock_drift_factor(&self) -> f64 {
        self.clock_drift_factor
    }

    /// 重试延迟毫秒（与 C# 默认一致：200）。
    pub fn retry_delay_ms(&self) -> i64 {
        self.retry_delay_ms
    }

    /// 主动释放所有实例上的锁（等价 C# `Dispose`）。
    pub fn release(&mut self) {
        for rds in self.instances.drain(..) {
            unlock_instance(&rds, &self.key, &self.token);
        }
    }
}

impl Drop for RedLock {
    fn drop(&mut self) {
        self.release();
    }
}

/// 在多个独立实例上获取 RedLock（对应 C# `RedisRedLock.Acquire`）。
///
/// - `instances`：建议 3 个以上奇数个独立实例；`key` 需为已含前缀的完整键名；
/// - `ms_timeout` / `ms_expire`：获取锁超时与锁有效期（毫秒），必须 > 0；
/// - 获取失败返回 `Ok(None)`（超时语义与 C# 一致）。
pub fn acquire_red_lock(
    instances: &[FullRedis],
    key: &str,
    ms_timeout: i64,
    ms_expire: i64,
) -> Result<Option<RedLock>> {
    if instances.is_empty() {
        return Err(Error::Config("RedLock 需要至少一个 Redis 实例".into()));
    }
    if key.is_empty() {
        return Err(Error::Config("RedLock 锁键不能为空".into()));
    }
    if ms_timeout <= 0 {
        return Err(Error::Config("RedLock 超时时间必须大于 0".into()));
    }
    if ms_expire <= 0 {
        return Err(Error::Config("RedLock 过期时间必须大于 0".into()));
    }

    let token = random_token();
    let quorum = instances.len() / 2 + 1;
    let clock_drift_factor = 0.01f64;
    let retry_delay_ms = 200i64;
    let expire_seconds = ms_expire / 1000;
    let start = Instant::now();

    let mut locked: Vec<FullRedis> = Vec::with_capacity(instances.len());

    loop {
        let lock_start = Instant::now();
        locked.clear();

        // 尝试在每个实例上加锁（单实例失败不中断）
        for rds in instances {
            if let Ok(true) =
                rds.redis()
                    .set(key, token.as_str(), expire_seconds)
            {
                locked.push(rds.clone());
            }
        }

        let lock_elapsed = lock_start.elapsed().as_millis() as i64;
        let validity = ms_expire - lock_elapsed - (ms_expire as f64 * clock_drift_factor) as i64;
        if locked.len() >= quorum && validity > 0 {
            return Ok(Some(RedLock {
                key: key.to_string(),
                token,
                instances: std::mem::take(&mut locked),
                clock_drift_factor,
                retry_delay_ms,
            }));
        }

        // 加锁失败，释放已获得的锁
        for rds in locked.drain(..) {
            unlock_instance(&rds, key, &token);
        }

        let elapsed = start.elapsed().as_millis() as i64;
        if elapsed >= ms_timeout {
            return Ok(None);
        }

        // 等待后重试（+50ms 以内的随机抖动）
        let jitter = {
            use rand::Rng;
            rand::thread_rng().gen_range(0..50)
        };
        let delay = (retry_delay_ms + jitter).min(ms_timeout - elapsed).max(1);
        std::thread::sleep(Duration::from_millis(delay as u64));
    }
}

/// 使用比较令牌的 Lua 脚本解锁（失败不抛异常，与 C# `TryUnlock` 一致）。
fn unlock_instance(rds: &FullRedis, key: &str, token: &str) {
    let _ = rds.redis().eval_raw(UNLOCK_SCRIPT, &[key], &[token]);
}

/// 22 位随机令牌（对应 C# `Rand.NextString(22)`）。
fn random_token() -> String {
    use rand::Rng;
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..22)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}

/// 服务层回调错误。
pub type ServiceError = Box<dyn std::error::Error + Send + Sync>;
/// 服务层回调结果。
pub type ServiceCallbackResult<T = ()> = std::result::Result<T, ServiceError>;

/// Redis 延迟批处理器（对齐 DH.NRedis `Services.RedisDeferred`）。
///
/// Rust 侧不内置计时器线程，而是暴露 `process_once` / `run`，由调用方决定放在线程、tokio
/// 任务或宿主框架的定时器里执行。
pub struct RedisDeferred {
    name: String,
    pending: RedisSet<String>,
    /// 空闲时轮询周期。默认 10 秒。
    pub period: Duration,
    /// 每批处理条数。默认 10。
    pub batch_size: usize,
}

impl RedisDeferred {
    /// 创建延迟批处理器。
    pub fn new(redis: FullRedis, name: &str) -> Self {
        Self {
            name: name.to_string(),
            pending: redis.get_set::<String>(name),
            period: Duration::from_secs(10),
            batch_size: 10,
        }
    }

    /// 名称。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 放入待处理 key（集合去重）。返回新增数量。
    pub fn add<I, S>(&self, keys: I) -> Result<i64>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let values: Vec<String> = keys.into_iter().map(Into::into).collect();
        self.pending.add(&values)
    }

    /// 取出一批待处理 key。
    pub fn take_batch(&self) -> Result<Vec<String>> {
        self.pending.pop(self.batch_size as i64)
    }

    /// 处理一批 key。处理失败时会原样写回集合，保持与 C# 一致的“失败重试”语义。
    pub fn process_once<F>(&self, mut process: F) -> Result<usize>
    where
        F: FnMut(&[String]) -> ServiceCallbackResult,
    {
        let keys = self.take_batch()?;
        if keys.is_empty() {
            return Ok(0);
        }

        match process(&keys) {
            Ok(()) => Ok(keys.len()),
            Err(err) => {
                let _ = self.pending.add(&keys);
                Err(Error::Operation(err.to_string()))
            }
        }
    }

    /// 循环处理。无消息时按 `period` 休眠；终止由调用方提供的取消标记控制。
    pub fn run<F>(&self, cancel: &AtomicBool, mut process: F) -> Result<()>
    where
        F: FnMut(&[String]) -> ServiceCallbackResult,
    {
        while !cancel.load(std::sync::atomic::Ordering::Relaxed) {
            let keys = self.take_batch()?;
            if keys.is_empty() {
                std::thread::sleep(self.period);
                continue;
            }

            if let Err(_err) = process(&keys) {
                let _ = self.pending.add(&keys);
                std::thread::sleep(self.period);
            }
        }
        Ok(())
    }
}

/// Redis 流式统计服务（对齐 DH.NRedis `Services.RedisStat`）。
///
/// 语义保持一致：
/// - 统计值写在主库的 HASH；
/// - 待处理 key 进入备份库的延迟队列/可靠队列；
/// - 消费时对 HASH 先重命名做快照，再回调保存；
/// - 延迟转移与可靠消费分开暴露，便于按 Rust 宿主模型自行放在线程或 tokio 中运行。
pub struct RedisStat {
    name: String,
    redis: FullRedis,
    redis_queue: FullRedis,
}

impl RedisStat {
    /// 创建统计服务。备份库规则与 C# 一致：`db == 15 ? 0 : db + 1`。
    pub fn new(redis: FullRedis, name: &str) -> Result<Self> {
        let db = redis.redis().options().db;
        let bak_db = if db == 15 { 0 } else { db + 1 };
        let redis_queue = redis.create_sub(bak_db)?;
        Ok(Self {
            name: name.to_string(),
            redis,
            redis_queue,
        })
    }

    /// 名称。
    pub fn name(&self) -> &str {
        &self.name
    }

    fn reliable_queue(&self) -> crate::queues::RedisReliableQueue<String> {
        self.redis_queue.get_reliable_queue::<String>(&self.name)
    }

    fn plain_queue(&self) -> crate::queues::RedisQueue<String> {
        self.redis_queue.get_queue::<String>(&self.name)
    }

    fn snapshot_key(&self, key: &str) -> String {
        format!("{key}:__stat_snapshot")
    }

    /// 统计累加（`HINCRBY`）。
    pub fn increment(&self, key: &str, field: &str, value: i32) -> Result<i64> {
        self.redis
            .get_hash::<i64>(key)
            .incr_by(&field.to_string(), value as i64)
    }

    /// 把 key 放入延迟队列；利用 `exists:{key}` 做 10 分钟去重。
    pub fn add_delay_queue(&self, key: &str, delay_seconds: i64) -> Result<i64> {
        let exists_key = format!("exists:{key}");
        let added = self.redis.add(&exists_key, Local::now().naive_local(), 600)?;
        if added {
            self.reliable_queue().add_delay(&key.to_string(), delay_seconds)
        } else {
            Ok(0)
        }
    }

    /// 把到期延迟消息转移到主可靠队列；建议单独放在线程或 tokio 任务中运行。
    pub fn transfer_due(&self, cancel: Arc<AtomicBool>) -> Result<()> {
        let reliable = self.reliable_queue();
        reliable.delay_queue().transfer_loop(&self.plain_queue(), cancel)
    }

    /// 立即转移一批已到期延迟消息到主可靠队列，返回转移条数。
    pub fn transfer_due_once(&self, batch_size: usize) -> Result<usize> {
        let reliable = self.reliable_queue();
        let messages = reliable.delay_queue().take_due(batch_size)?;
        if messages.is_empty() {
            return Ok(0);
        }

        self.plain_queue().add_many(&messages)?;
        Ok(messages.len())
    }

    /// 处理单个统计 key：先重命名快照，再读回 HASH，成功保存后删除快照。
    pub fn process_key<F>(&self, key: &str, on_save: &mut F) -> Result<bool>
    where
        F: FnMut(&str, HashMap<String, i32>) -> ServiceCallbackResult,
    {
        let snapshot_key = self.snapshot_key(key);
        if !self.redis.contains_key(&snapshot_key)? {
            if !self.redis.rename(key, &snapshot_key)? {
                return Ok(false);
            }

            let _ = self.redis.remove(&format!("exists:{key}"));
        }

        let raw = self.redis.get_hash_all::<i64>(&snapshot_key)?;
        let mut values = HashMap::with_capacity(raw.len());
        for (field, value) in raw {
            let value = i32::try_from(value).map_err(|_| {
                Error::Type(format!("统计值超出 i32 范围：field={field} value={value}"))
            })?;
            values.insert(field, value);
        }

        match on_save(key, values) {
            Ok(()) => {
                let _ = self.redis.remove(&snapshot_key);
                Ok(true)
            }
            Err(err) => Err(Error::Operation(err.to_string())),
        }
    }

    /// 可靠消费统计 key。失败时保持未确认，由 `RedisReliableQueue::consume_raw` 按既有规则重试/计数。
    pub fn consume_loop<F>(
        &self,
        timeout_seconds: i64,
        poll_interval: Duration,
        cancel: &AtomicBool,
        mut on_save: F,
    ) -> Result<()>
    where
        F: FnMut(&str, HashMap<String, i32>) -> ServiceCallbackResult,
    {
        self.reliable_queue().consume_raw(timeout_seconds, poll_interval, cancel, |key| {
            self.process_key(key, &mut on_save)
                .map(|_| ())
                .map_err(|e| Box::new(std::io::Error::other(e.to_string())) as ServiceError)
        })
    }

    /// 处理一条统计 key，成功保存后返回 `true`，超时/无消息返回 `false`。
    pub fn consume_once<F>(&self, timeout_seconds: i64, mut on_save: F) -> Result<bool>
    where
        F: FnMut(&str, HashMap<String, i32>) -> ServiceCallbackResult,
    {
        let Some(key) = self.reliable_queue().take_one(timeout_seconds)? else {
            return Ok(false);
        };

        self.process_key(&key, &mut on_save)?;
        self.reliable_queue().acknowledge(&[&key])?;
        Ok(true)
    }
}

/// Redis 事件总线（对齐 DH.NRedis `Services.RedisEventBus<T>` 的 Redis 语义层）。
///
/// Rust 侧不复刻 `.NET EventBus<T>`/DI/本地订阅注册模型，而是提供：
/// - `publish`：写入 Stream；
/// - `consume_once` / `consume_loop`：按消费组读取并在成功后确认；
/// 调用方可用当前语言/框架自己的回调注册、channel 或 actor 机制做进程内分发。
pub struct RedisEventBus<T> {
    stream: Mutex<RedisStream>,
    expire: Mutex<Option<Duration>>,
    maintenance_interval: Mutex<Duration>,
    last_maintenance: Mutex<Option<Instant>>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> RedisEventBus<T>
where
    T: Serialize + DeserializeOwned,
{
    /// 创建事件总线。默认 `from_last_offset = true`，与 C# 默认行为一致。
    pub fn new(redis: FullRedis, topic: &str, group: &str) -> Result<Self> {
        let mut stream = redis.get_stream(topic);
        stream.from_last_offset = true;
        stream.set_group(group)?;
        Ok(Self {
            stream: Mutex::new(stream),
            expire: Mutex::new(Some(Duration::from_secs(3 * 24 * 3600))),
            maintenance_interval: Mutex::new(Duration::from_secs(600)),
            last_maintenance: Mutex::new(None),
            _marker: PhantomData,
        })
    }

    /// 设置首次消费策略。默认 `true`，表示像 C# `FromLastOffset=true` 一样从最新位置开始。
    /// 若要把总线当作“补历史消息”的 worker 使用，可显式设为 `false`。
    pub fn set_from_last_offset(&self, enabled: bool) {
        self.stream.lock().unwrap().from_last_offset = enabled;
    }

    /// 设置基于时间的保留期。默认 3 天；`None` 表示关闭按时间裁剪。
    pub fn set_expire(&self, expire: Option<Duration>) {
        *self.expire.lock().unwrap() = expire;
    }

    /// 设置维护周期。默认 10 分钟；`Duration::ZERO` 表示每次发布/消费前都尝试维护。
    pub fn set_maintenance_interval(&self, interval: Duration) {
        *self.maintenance_interval.lock().unwrap() = interval;
    }

    /// 发布事件，返回 Stream 消息 Id。
    pub fn publish(&self, event: &T) -> Result<String> {
        let mut stream = self.stream.lock().unwrap();
        self.maybe_maintain(&mut stream)?;
        stream
            .add(event, None)?
            .ok_or_else(|| Error::Operation("发布事件失败：未返回消息 Id".into()))
    }

    /// 处理一条事件消息。处理成功后自动确认；处理失败则保持挂起，由后续 `retry_ack` 抢回。
    pub fn consume_once<F>(&self, mut handler: F) -> Result<bool>
    where
        F: FnMut(&T, &Message) -> ServiceCallbackResult,
    {
        let message = {
            let mut stream = self.stream.lock().unwrap();
            self.maybe_maintain(&mut stream)?;
            stream.take_message()?
        };

        let Some(message) = message else {
            return Ok(false);
        };

        let event = message
            .to_struct::<T>()
            .ok_or_else(|| Error::Type(format!("事件反序列化失败：id={}", message.id)))?;

        handler(&event, &message).map_err(|e| Error::Operation(e.to_string()))?;

        let stream = self.stream.lock().unwrap();
        stream.acknowledge(&[message.id.as_str()])?;
        Ok(true)
    }

    /// 消费循环。没有消息时短暂休眠，避免空转。
    pub fn consume_loop<F>(&self, cancel: &AtomicBool, mut handler: F) -> Result<()>
    where
        F: FnMut(&T, &Message) -> ServiceCallbackResult,
    {
        while !cancel.load(std::sync::atomic::Ordering::Relaxed) {
            let processed = self.consume_once(|event, message| handler(event, message))?;
            if !processed {
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        Ok(())
    }

    fn maybe_maintain(&self, stream: &mut RedisStream) -> Result<()> {
        let interval = *self.maintenance_interval.lock().unwrap();
        let mut last = self.last_maintenance.lock().unwrap();
        let now = Instant::now();
        if interval > Duration::ZERO
            && let Some(previous) = *last
            && now.duration_since(previous) < interval
        {
            return Ok(());
        }

        if let Some(expire) = *self.expire.lock().unwrap()
            && expire > Duration::ZERO
        {
            let expire = ChronoDuration::from_std(expire)
                .map_err(|_| Error::Config("事件总线保留期超出 chrono 可表示范围".into()))?;
            stream.trim_before(Local::now().naive_local() - expire)?;
        }

        *last = Some(now);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::thread;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(rename_all = "PascalCase")]
    struct EventDemo {
        name: String,
        count: i32,
    }

    #[test]
    fn deferred_batches_unique_keys() {
        let (_server, redis) = super::test_support::mock_full();
        let mut deferred = RedisDeferred::new(redis, "deferred:demo");
        deferred.batch_size = 2;

        assert_eq!(deferred.add(["a", "b", "a"]).unwrap(), 2);

        let mut got = Vec::new();
        let processed = deferred
            .process_once(|keys| {
                got = keys.to_vec();
                Ok(())
            })
            .unwrap();

        assert_eq!(processed, 2);
        got.sort();
        assert_eq!(got, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn deferred_run_requeues_after_handler_error() {
        let (_server, redis) = super::test_support::mock_full();
        let deferred = Arc::new(RedisDeferred::new(redis, "deferred:retry"));
        deferred.add(["x"]).unwrap();

        let cancel = Arc::new(AtomicBool::new(false));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let worker = {
            let deferred = deferred.clone();
            let cancel = cancel.clone();
            let calls = calls.clone();
            thread::spawn(move || {
                deferred.run(cancel.as_ref(), |keys| {
                    let attempt = calls.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        return Err(Box::new(std::io::Error::other(format!(
                            "first failure: {}",
                            keys.join(",")
                        ))));
                    }

                    cancel.store(true, Ordering::SeqCst);
                    Ok(())
                })
            })
        };

        worker.join().unwrap().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(deferred.take_batch().unwrap().is_empty());
    }

    #[test]
    fn event_bus_publish_and_consume() {
        let (_server, redis) = super::test_support::mock_full();
        let bus = RedisEventBus::<EventDemo>::new(redis, "events:demo", "group-a").unwrap();
        bus.set_from_last_offset(false);

        let msg_id = bus
            .publish(&EventDemo {
                name: "created".into(),
                count: 7,
            })
            .unwrap();
        assert!(!msg_id.is_empty());

        let mut seen = None;
        let processed = bus
            .consume_once(|event, message| {
                seen = Some((event.clone(), message.id.clone()));
                Ok(())
            })
            .unwrap();

        assert!(processed);
        let (event, id) = seen.unwrap();
        assert_eq!(event.name, "created");
        assert_eq!(event.count, 7);
        assert_eq!(id, msg_id);
    }

    #[test]
    fn event_bus_trims_expired_messages() {
        let (_server, redis) = super::test_support::mock_full();

        let old_stream = redis.get_stream("events:retention");
        old_stream
            .add(
                &EventDemo {
                    name: "old".into(),
                    count: 1,
                },
                Some("1000-0"),
            )
            .unwrap();

        let bus = RedisEventBus::<EventDemo>::new(redis.clone(), "events:retention", "group-r").unwrap();
        bus.set_expire(Some(Duration::from_secs(1)));
        bus.set_maintenance_interval(Duration::ZERO);
        let new_id = bus.publish(&EventDemo {
            name: "new".into(),
            count: 2,
        })
        .unwrap();

        let stream = redis.get_stream("events:retention");
        assert_eq!(stream.count().unwrap(), 1);
        let messages = stream.range(None, None, 10).unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].id, new_id);
        let event = messages[0].to_struct::<EventDemo>().unwrap();
        assert_eq!(event.name, "new");
        assert_eq!(event.count, 2);
    }

    #[test]
    fn stat_process_key_recovers_from_snapshot_after_save_error() {
        let (_server, redis) = super::test_support::mock_full();
        let stat = RedisStat::new(redis, "stat:retry").unwrap();
        stat.increment("station:9", "pv", 9).unwrap();

        let first = stat.process_key("station:9", &mut |_key, _data| {
            Err(Box::new(std::io::Error::other("save failed")))
        });
        assert!(first.is_err());

        let snapshot_key = stat.snapshot_key("station:9");
        assert!(stat.redis.contains_key(&snapshot_key).unwrap());

        let mut saved = None;
        let second = stat
            .process_key("station:9", &mut |key, data| {
                saved = Some((key.to_string(), data));
                Ok(())
            })
            .unwrap();
        assert!(second);

        let (key, data) = saved.unwrap();
        assert_eq!(key, "station:9");
        assert_eq!(data.get("pv"), Some(&9));
        assert!(!stat.redis.contains_key(&snapshot_key).unwrap());
    }

    #[test]
    fn stat_transfer_due_once_and_consume_once() {
        let (_server, redis) = super::test_support::mock_full();
        let stat = RedisStat::new(redis, "stat:once").unwrap();
        stat.increment("station:once", "pv", 11).unwrap();
        stat.reliable_queue()
            .delay_queue()
            .add(&"station:once".to_string(), 0)
            .unwrap();

        let moved = stat.transfer_due_once(10).unwrap();
        assert_eq!(moved, 1);

        let mut saved = None;
        let consumed = stat
            .consume_once(-1, |key, data| {
                saved = Some((key.to_string(), data));
                Ok(())
            })
            .unwrap();
        assert!(consumed);

        let (key, data) = saved.unwrap();
        assert_eq!(key, "station:once");
        assert_eq!(data.get("pv"), Some(&11));
    }

    #[test]
    fn stat_transfer_and_consume_end_to_end() {
        let (_server, redis) = super::test_support::mock_full();
        let stat = Arc::new(RedisStat::new(redis, "stat:demo").unwrap());
        stat.increment("station:1", "pv", 2).unwrap();
        stat.increment("station:1", "uv", 3).unwrap();
        stat.add_delay_queue("station:1", 0).unwrap();

        let cancel = Arc::new(AtomicBool::new(false));
        let transfer_cancel = cancel.clone();
        let stat_bg = stat.clone();
        let transfer = thread::spawn(move || stat_bg.transfer_due(transfer_cancel));

        let (tx, rx) = mpsc::channel();
        let consume_cancel = cancel.clone();
        let consumer = thread::spawn(move || {
            stat.consume_loop(-1, Duration::from_millis(10), consume_cancel.as_ref(), |key, data| {
                tx.send((key.to_string(), data)).unwrap();
                cancel.store(true, Ordering::SeqCst);
                Ok(())
            })
        });

        let (key, data) = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(key, "station:1");
        assert_eq!(data.get("pv"), Some(&2));
        assert_eq!(data.get("uv"), Some(&3));

        consumer.join().unwrap().unwrap();
        transfer.join().unwrap().unwrap();
    }
}
