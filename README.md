# pek-rredis — DH.NRedis 的 Rust 实现

Pek 生态的 Rust Redis 客户端（独立项目）：让 C#/.NET 项目（DH.NRedis 技术栈）能够**渐进迁移**到 Rust——
两边连接**同一个 Redis**、使用**同一套键与字节格式**，业务可以一个用例一个用例地从 C# 搬到 Rust，
而不是推倒重来；最终用 Rust 整体替换 C# 客户端。

```
┌─────────────────────────┐        ┌─────────────────────────┐
│  C# 现有系统（DH.NRedis）│        │  Rust 新代码（Pek.RRedis）│
│  FullRedis / RedisHash  │  互通  │  FullRedis / RedisHash  │
│  Queue / PubSub / Lock  │◄──────►│  Queue / PubSub / Lock  │
└───────────┬─────────────┘        └───────────┬─────────────┘
            │        同一 Redis 实例 / 同一字节格式             │
            └───────────────┬───────────────────┘
                            ▼
                    Redis（RESP2 / RESP3）
```

---

## 一、当前能力（v0.1.0）

| 模块 | 对应 DH.NRedis | 状态 |
|------|----------------|------|
| `resp` | `RedisClient` 组包/解析 | ✅ RESP2 全量 + RESP3（Map/Set/Double/Bool/Null/Verbatim/Push/Attribute） |
| `client` | `RedisClient`（TCP、AUTH、SELECT、HELLO） | ✅ 含超时、惰性握手、断线标记 |
| `pool` | `ObjectPool<RedisClient>` | ✅ Min/Max/IdleTime/MaxLifetime/WaitTimeout，空闲 PING 健康检查 |
| `encoder` | `RedisJsonEncoder` / `DefaultPacketEncoder` | ✅ 字节格式逐条对齐（见下表） |
| `redis`（基础命令） | `Redis` | ✅ 字符串/键/过期/位图/批量/服务器信息/脚本/管道 |
| `full` | `FullRedis` | ✅ 前缀、SCAN 搜索、模式删除、Eval、分布式锁、结构工厂 |
| `RedisHash` / `RedisList` / `RedisSet` / `RedisSortedSet` / `RedisStack` | 同名 | ✅ 常用命令 + SCAN 系列 |
| `RedisGeo` / `HyperLogLog` | 同名 | ✅ 常用命令 |
| `PubSub` | `PubSub` | ✅ 订阅/模式订阅/分片订阅/自省 |
| `RedisQueue` | 同名 | ✅ `LPUSH` + `RPOP`/`BRPOP`，批量管道消费 |
| `RedisReliableQueue` | 同名 | ✅ Ack 队列、状态键、死信回滚、全局清理、`Publish`/`Consume` 高级用法、延迟队列挂载 |
| `RedisDelayQueue` | 同名 | ✅ `ZADD`(到期时间) + `ZRANGEBYSCORE`/`ZREM` 抢占 + 转移循环 |
| `RedisStream`（Stream 消息队列） | 同名 | 🚧 规划中（XADD/XREADGROUP/XAUTOCLAIM） |
| `Clusters`（Cluster/Sentinel/Replication） | 同名 | 🚧 规划中（当前支持多地址故障切换） |
| `RedisEventBus` / `RedisRedLock` / ASP.NET 集成 | `Services` | 🚧 规划中 |
| 异步 API | `*Async` | 🚧 规划中（当前同步阻塞 + 线程/`spawn_blocking`） |

测试：**71 项**（46 单元 + 18 进程内端到端 + 2 文档 + 5 真实 Redis 可选），`cargo test` 离线全绿，
`cargo clippy --all-targets` 零告警。

---

## 二、互通保证（与 C# 端逐字节对齐）

### 值编码（`encoder`）

| 数据类型 | 编码结果（两端一致） | 读取兼容 |
|----------|----------------------|----------|
| `null` | 空数据包 | — |
| 字符串 | 原始 UTF-8，**不加引号** | — |
| `Byte[]` | 原始二进制 | — |
| 布尔 | `True` / `False` | `true` / `OK` / `1` / `0` |
| 整数 | 十进制文本 `123` | — |
| 浮点 | 往返最短文本 `1.5` | `Infinity` / `NaN` |
| 时间 | `yyyy-MM-dd HH:mm:ss.fff` | 同时接受 ISO 8601（含时区偏移） |
| 复杂对象 | JSON（System.Text.Json 风格，属性名建议 PascalCase） | 容忍 BOM |

### 键空间与结构

- 键前缀：与 C# `EnsureStart` 一致（已有前缀不重复添加，比较不区分大小写）；
- 哈希/列表/集合/有序集合/Geo/HLL 均为原生 Redis 结构，只要**成员字节一致**即可互通；
- `SCAN` 搜索返回**去掉前缀**的键（C# 返回原始键），并按 `count<=0 表示不限量` 修正了 C# 的边界行为。

### 队列布局（可靠队列）

| 键 | 格式 | 说明 |
|----|------|------|
| `{key}` | List | 主队列，`LPUSH` 生产、`RPOPLPUSH` 消费 |
| `{key}:Ack:{ukey}` | List | 确认队列，`ukey` 为 8 位随机串（每消费者一份） |
| `{key}:Status:{ukey}` | String(JSON) | 消费者状态（PascalCase、ISO 时间），7 天过期 |
| `{key}:AllStatus` | String | 全局清理权，`SET NX EX RetryInterval` 抢占 |
| `{key}:Delay` | ZSet | 延迟消息，`score = Unix 秒 + 延迟` |

死信判定：`LastActive + RetryInterval * 10 < Now` 时回滚其确认队列——**C# 与 Rust 消费者可混合部署**，
各自持有独立 `ukey`，互不干扰，也都能接管对方宕机后留下的死信。

### 分布式锁

锁值 `{token}|{绝对过期毫秒}`（token 为 32 位十六进制），TTL `ceil(msExpire/1000)` 秒；
超时锁通过 `GETSET` 抢占、释放时校验 token 归属（不误删他人锁）。
**时钟域**与 C# `Environment.TickCount64` 一致（Windows `GetTickCount64` / Linux `/proc/uptime`），
因此 C# 与 Rust 可以互抢对方超时的锁。

---

## 三、快速开始

```rust
use pek_rredis::{encoder::Json, FullRedis, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")] // 与 C# 属性名对齐
struct User {
    name: String,
    create_time: chrono::NaiveDateTime,
}

fn main() -> Result<()> {
    // 1) 连接字符串格式与 C# 一致
    let redis = FullRedis::from_config("server=127.0.0.1:6379;password=123456;db=7;prefix=app:")?;

    // 2) 基本读写（SETEX / GET，字节格式与 C# 一致）
    let user = User { name: "NewLife".into(), create_time: chrono::Local::now().naive_local() };
    redis.redis().set("user", Json(&user), 3600)?;
    let user2: Option<Json<User>> = redis.redis().get("user")?;

    // 3) 哈希 / 列表 / 集合 / 有序集合 / Geo / HLL / 栈
    let hash = redis.get_hash::<i64>("counter");
    hash.incr_by(&"pv".to_string(), 1)?;

    // 4) 普通队列（C# 与 Rust 可互相生产消费）
    let queue = redis.get_queue::<String>("orders");
    queue.add(&"order-001".to_string())?;
    let item = queue.take_one(5)?;

    // 5) 可靠队列（消费失败自动回滚）
    let reliable = redis.get_reliable_queue::<String>("payments");
    reliable.add(&"p-001".to_string())?;
    if let Some(msg) = reliable.take_one(5)? {
        // ... 业务处理 ...
        reliable.acknowledge(&[msg.as_str()])?;
    }

    // 6) 延迟队列（到期转移主队列）
    let delay = redis.get_delay_queue::<String>("orders:delay");
    delay.add(&"order-002".to_string(), 30)?;

    // 7) 分布式锁（与 C# CacheLock 互斥）
    let lock = redis.acquire_lock("order:lock", 5000)?;
    drop(lock); // 自动释放

    // 8) 管道（对应 StartPipeline/StopPipeline）
    let mut pipe = redis.redis().pipeline();
    pipe.set("k1", "v1")?;
    pipe.get("k1");
    let results = pipe.execute()?;
    println!("{results:?}");

    Ok(())
}
```

构建与测试：

```powershell
cd G:\Code\Pek.Rust\Pek.RRedis
cargo test          # 71 项测试，离线可跑（含进程内迷你 Redis 端到端）
cargo clippy        # 零告警
```

真实 Redis 联调（可选）：

```powershell
$env:REDIS_ADDR = "127.0.0.1:6379"; $env:REDIS_PASSWORD = "123456"
cargo test --test live_redis -- --nocapture
```

> 依赖镜像：工程内 `.cargo/config.toml` 已配置 rsproxy。

---

## 四、C# ↔ Rust 概念对照

| C# / DH.NRedis | Rust / pek-rredis |
|----------------|-------------------|
| `new FullRedis(server, pwd, db)` / `Init(config)` | `FullRedis::open` / `FullRedis::from_config` |
| `Redis` | `Redis`（`set/get/add/replace/get_all/set_all/eval/pipeline`…） |
| `RedisClient` | `client::RedisClient`（单连接），`pool::Pool`（连接池） |
| `RedisJsonEncoder` | `encoder::{ToRedisPayload, FromRedisPayload, Json<T>}` |
| `RedisHash<K,V>` | `RedisHash<V, K = String>`（`get_hash` / `get_hash_map`） |
| `RedisList<T>` / `RedisSet<T>` / `RedisSortedSet<T>` / `RedisStack<T>` | 同名类型（`get_list` / `get_set` / `get_sorted_set` / `get_stack`） |
| `RedisGeo` / `HyperLogLog` / `PubSub` | 同名类型 |
| `RedisQueue<T>` / `RedisReliableQueue<T>` / `RedisDelayQueue<T>` | 同名类型（`get_queue` / `get_reliable_queue` / `get_delay_queue`） |
| `QueueBase.AttachTraceId` | `queues::QueueSettings::trace` |
| `Cache.AcquireLock` | `FullRedis::acquire_lock` / `acquire_lock_ex`，返回 `LockHandle`（Drop 自动释放） |
| `StartPipeline` / `StopPipeline` | `Redis::pipeline()` / `Pipeline::execute` |
| `CreateSub(db)` | `Redis::create_sub` / `FullRedis::create_sub` |
| `Search(pattern, offset, count)` | `FullRedis::search(pattern, count)` / `search_paged` |

---

## 五、与 DH.NRedis 的已知差异

1. **运行期 `SELECT`**：C# 可在同一实例切换库；Rust 侧以「配置即状态」为原则，请用 `create_sub(db)` 创建子实例（连接池状态不会被多线程共享污染）。
2. **`Search` 边界**：C# 中 `count <= 0` 不返回任何键（含 `Remove("*")` 的边界行为）；Rust 按「不限量」处理并返回去前缀键名。
3. **`GetAll` 键名**：C# `FullRedis.GetAll` 返回**带前缀**的键名；Rust 返回调用方传入的原始键名。
4. **解码容错**：两端一致——解码失败返回 `None`（不抛异常）；服务端 `-ERR` 抛 `Error::Server` 且不重试。
5. **暂未实现**：`RedisStream`、集群/哨兵/主从感知、`RedisEventBus`、`RedLock`、ASP.NET Core 集成、TLS、异步 API（见路线图）。

---

## 六、路线图

| 阶段 | 内容 |
|------|------|
| v0.2 | `RedisStream`（XADD/XREADGROUP/XAUTOCLAIM + `__data` 字段约定）、`BLMOVE`/`LMPOP` 等新命令、`RedisRedLock` |
| v0.3 | 集群/哨兵/主从（`Cluster`/`Sentinel`/`Replication`）、RESP3 推送与 `CLIENT TRACKING` |
| v0.4 | 异步 API（tokio）、TLS、`RedisEventBus` 与 ASP.NET Core 风格 DI 集成 |
| v1.0 | 与 DH.NRedis 的 XUnitTest 跑同一套集成测试做双向互操作回归 |

---

## 七、渐进迁移建议

1. **共用键前缀**：Rust 先只读写新业务键（如 `r:order:*`），C# 保持原键空间；
2. **影子消费**：可靠队列天然支持多消费者（各自 `ukey`），让 Rust 消费者先在旁路消费比对结果；
3. **逐用例切换**：把一个用例的生产者/消费者从 C# 切到 Rust，验证后删除 C# 路径；
4. **收尾**：C# 侧只保留管理与运维工具，数据格式不变，切换零迁移成本。
