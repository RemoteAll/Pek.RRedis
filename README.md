# pek-rredis — DH.NRedis 的 Rust 实现

Pek 生态的 Rust Redis 客户端（独立项目）：让 C#/.NET 项目（DH.NRedis 技术栈）能够**渐进迁移**到 Rust——
两边连接**同一个 Redis**、使用**同一套键与字节格式**，业务可以一个用例一个用例地从 C# 搬到 Rust，
而不是推倒重来；最终用 Rust 整体替换 C# 客户端。

```text
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
| ---- | -------------- | ---- |
| `resp` | `RedisClient` 组包/解析 | ✅ RESP2 全量 + RESP3（Map/Set/Double/Bool/Null/Verbatim/Push/Attribute） |
| `client` | `RedisClient`（TCP、AUTH、SELECT、HELLO） | ✅ 含超时、惰性握手、断线标记 |
| `pool` | `ObjectPool<RedisClient>` | ✅ Min/Max/IdleTime/MaxLifetime/WaitTimeout，空闲 PING 健康检查 |
| `encoder` | `RedisJsonEncoder` / `DefaultPacketEncoder` | ✅ 字节格式逐条对齐（见下表） |
| `redis`（基础命令） | `Redis` | ✅ 字符串/键/过期/位图/`BITFIELD`/批量/服务器信息（`ServerType`/`Version` 探测与 `require_version` 门禁）/脚本/管道 |
| `full` | `FullRedis` | ✅ 前缀（含基础命令与便利公开面前缀包装：`SetAll`/`SetGet`/`BitOp`/`Copy`/`MemoryUsage`/`ObjectEncoding`/`LPUSH`/`RPUSH`/`SADD`/`SREM`/`LPOS`/阻塞 `BLPOP`/`BRPOP`）、SCAN 搜索、模式删除、Eval/FCall、分布式锁、RedLock、结构工厂 |
| `RedisHash` / `RedisList` / `RedisSet` / `RedisSortedSet` / `RedisStack` | 同名 | ✅ 全量命令对齐（含 `HGETDEL`/`HGETEX`、`LMOVE`/`BLMOVE`/`LMPOP`、`SMISMEMBER`/`SINTERCARD`、`ZUNION/ZINTER/ZDIFF` 族、`ZRANGESTORE`、`ZMPOP`/`BZPOPMIN/MAX`） |
| `RedisGeo` / `HyperLogLog` | 同名 | ✅ 常用命令 |
| `PubSub` | `PubSub` | ✅ 订阅/模式订阅/分片订阅/自省 |
| `RedisQueue` | 同名 | ✅ `LPUSH` + `RPOP`/`BRPOP`，批量管道消费 |
| `RedisReliableQueue` | 同名 | ✅ Ack 队列、状态键、死信回滚、全局清理、`Publish`/`Consume`；`consume_json`/`consume_raw` 大循环（10 次失败丢弃 + 备份库计数，与 C# `ConsumeAsync` 一致） |
| `RedisDelayQueue` | 同名 | ✅ `ZADD`(到期时间) + `ZRANGEBYSCORE`/`ZREM` 抢占 + 转移循环 |
| `RedisStream`（Stream 消息队列） | 同名 | ✅ `XADD`/`XRANGE`/`XREAD`/`XREADGROUP`/`XACK`/`XPENDING`/`XCLAIM`/`XGROUP`/`XINFO`/`XTRIM`/`XDEL`；`__data` 基元约定、对象字段扁平化、消费组、死信抢占（`retry_ack`） |
| `RedisRedLock` | `Services.RedisRedLock` | ✅ `acquire_red_lock`：quorum/令牌回滚/`EVAL` 比较删除，与 C# 算法逐行一致 |
| Tair 扩展（阿里云） | `FullRedis.Ex*` | ✅ `ex_set`/`ex_get`/`ex_incr_by`/`ex_hset`/`ex_hget`/`ex_hmget`/`ex_hget_with_ver`/`ex_hincr_by`/`ex_hpttl`/`ex_hkeys`/`ex_hvals`/`ex_hlen`/`ex_hdel`（需 Tair 实例） |
| `Clusters`（Cluster/Sentinel/Replication） | 同名 | ✅ 已支持 Cluster / Sentinel / Replication：`CLUSTER NODES` 解析、slot/hash tag 路由、`MOVED`/`ASK`/`ASKING`、链式重定向、`mode=cluster\|sentinel\|replication` 与 `autoDetect` 自动加载拓扑、`TopologyRefreshSeconds` 刷新、单 key/多 key/搜索聚合、读副本与节点 shielding/backoff |
| `RedisEventBus` / `RedisStat` / `RedisDeferred` | `services::{RedisEventBus, RedisStat, RedisDeferred}` | ✅ 已提供 Rust 原生服务层封装，并已纳入 strict live gate；保留同等 Redis 语义，但不复刻 .NET DI / TimerX / `IEventBus` 宿主接口 |
| ASP.NET 集成（`RedisCacheProvider`/`CacheExtensions`） | 应用层状态注入 / 工厂函数 | ➖ 不做 1:1 迁移（宿主接口由 Rust Web/服务框架自身提供） |
| TLS | `Ssl`/`Tls`/`rediss://` | ✅ 已支持：`rediss://`、`Ssl=true`/`Tls=true`、`TlsServerName`、`TlsInsecure`；基于 rustls，同步客户端可直接走 TLS 连接 |
| 异步 API | `*Async` | ✅ 已覆盖 `AsyncRedis`/`AsyncFullRedis`、Hash/List/Set/SortedSet/Stack/Geo/HyperLogLog、PubSub、Queue/ReliableQueue/DelayQueue/Stream，且关键 direct/helper 命令已提供显式 async 入口；统一基于 tokio `spawn_blocking` 复用现有同步语义 |

测试：**165 项**（83 单元 + 78 集成/端到端测试（10 async + 25 互通 + 22 审计 + 16 cluster 路由 + 5 live）+ 4 文档），
`cargo test` 离线全绿，`cargo clippy --all-targets` 零告警；另有 C#/Rust 两个可执行 Demo 做交叉验证（见第四节）。

### 与 C# 全量 API 审计（2026-09-26 复核）

对 DH.NRedis 全部 public 方法逐项核对（`Redis`/`FullRedis`/各结构体/队列/`Services`/`Clusters`），结论：

- ✅ **命令级能力与 `FullRedis` 便利公开面已全部对齐并测试**：本轮补齐 `GETEX`、`EXPIRETIME`/`PEXPIRETIME`、`OBJECT IDLETIME`/`FREQ`/`ENCODING`、`BITFIELD`/`BITOP`、
  `HGETDEL`/`HGETEX`、`LMOVE`/`BLMOVE`/`LMPOP`/多键 `BRPOP`/`BLPOP`、`SMISMEMBER`/`SINTERCARD`、`ZMSCORE`/`ZRANDMEMBER`/
  `ZRANGESTORE`/`ZDIFF(STORE)`/`ZUNION(STORE)`/`ZINTER(STORE)`（含 `WEIGHTS`/`AGGREGATE`/`WITHSCORES`）/`ZMPOP`/`BZPOPMIN`/`BZPOPMAX`、
  `SWAPDB`/`WAIT`/`SLOWLOG`/`LATENCY`/`REPLICAOF`、`FUNCTION LOAD/LIST/DELETE` 与 `FCALL`/`FCALL_RO`、
  `ServerType`/`Version` 探测、`FullRedis` 基础命令与便利公开面前缀层（含 `SetAll`/`SetGet`/`Copy`/`MemoryUsage`/`LPUSH`/`RPUSH`/`SADD`/`SREM`/`LPOS`/单键阻塞 `BLPOP`/`BRPOP`）、`RedisRedLock`、`consume_json`/`consume_raw`、Tair `Ex*`。
- ➖ **明确不迁移（.NET 生态专属，与数据格式无关）**：`Bench`、`WriteLog`、`RedisCacheProvider`（ASP.NET `ICacheProvider`）、
  `RedisEventBus`、`RedisStat`、`RedisDeferred`、`CacheExtensions`、DI 扩展；`Tracer`/`Counter` 由 `QueueSettings::trace` 等效覆盖。
- ✅ **异步对象模型已补齐**：`Redis` / `FullRedis` / Hash / List / Set / SortedSet / Stack / Geo / HyperLogLog / PubSub / 普通队列 / 可靠队列 / 延迟队列 / Stream 都已有命名 async wrapper；关键 direct/helper 命令也已显式补齐，剩余极少数长尾能力仍可通过各 wrapper 自带的 `with_sync`，或 `AsyncRedis::with_sync` / `AsyncFullRedis::with_sync` 在 tokio 中复用同步能力。
- ⚠️ **发现的 C# 侧问题（Rust 按官方行为实现）**：
  1. `RedisHash.HGetDel`/`HGetEx` 缺少 `FIELDS 1` 参数，对真实 Redis 会报语法错误；
  2. `RedisRedLock` 加锁使用普通 `SET`（非 `NX`），无互斥保证；Rust 为保持行为一致原样复刻，**混用两端 RedLock 不可依赖互斥**。

---

## 二、互通保证（与 C# 端逐字节对齐）

### 值编码（`encoder`）

| 数据类型 | 编码结果（两端一致） | 读取兼容 |
| -------- | -------------------- | -------- |
| `null` | 空数据包 | — |
| 字符串 | 原始 UTF-8，**不加引号** | — |
| `Byte[]` | 原始二进制 | — |
| 布尔 | `True` / `False` | `true` / `OK` / `1` / `0` |
| 整数 | 十进制文本 `123` | — |
| 浮点 | 往返最短文本 `1.5` | `Infinity` / `NaN` |
| 时间 | `yyyy-MM-dd HH:mm:ss.fff` | 同时接受 ISO 8601（含时区偏移） |
| 复杂对象 | JSON（属性名建议 PascalCase；时间字段见下） | 容忍 BOM，时间兼容 ISO 与 NewLife 文本 |

> **JSON 内的时间字段**：Rust 结构体上的时间字段请加
> `#[serde(with = "pek_rredis::encoder::datetime")]`——写入 ISO 8601（与 System.Text.Json 一致），
> 读取同时兼容 C# **FastJson** 的 `2026-09-26 10:00:00` 文本格式（chrono 的 serde 默认只认 ISO，
> 不加会在读 C# 数据时失败）。另注意 FastJson 写 JSON 时**不含毫秒**，跨语言样本建议时间取整秒。
>
> **Stream 对象消息 / 字段路径**：时间字段用 `#[serde(with = "pek_rredis::encoder::datetime_text")]`，
> 输出 `2026-09-26 10:00:00.123`，与 C# `DefaultPacketEncoder` 的字段编码逐字节一致。

### 键空间与结构

- 键前缀：与 C# `EnsureStart` 一致（已有前缀不重复添加，比较不区分大小写）；
- 哈希/列表/集合/有序集合/Geo/HLL 均为原生 Redis 结构，只要**成员字节一致**即可互通；
- `SCAN` 搜索返回**去掉前缀**的键（C# 返回原始键），并按 `count<=0 表示不限量` 修正了 C# 的边界行为。

### 队列布局（可靠队列）

| 键 | 格式 | 说明 |
| -- | ---- | ---- |
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
    // JSON 内时间：兼容 C# FastJson 文本格式与 ISO 8601（chrono serde 默认只认 ISO）
    #[serde(with = "pek_rredis::encoder::datetime")]
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
cargo test          # 165 项测试，离线可跑（含进程内迷你 Redis 端到端）
cargo clippy        # 零告警
```

真实 Redis 联调（可选）：

```powershell
$env:REDIS_ADDR = "127.0.0.1:6379"; $env:REDIS_PASSWORD = "123456"
cargo test --test live_redis -- --nocapture
```

> 依赖镜像：工程内 `.cargo/config.toml` 已配置 rsproxy。

---

## 四、Demo：与 C# 端相互验证

仓库内置两个命令与样本**逐项对应**的演示程序，互为验证（详见 [`demo/README.md`](demo/README.md)）：

| 侧 | 程序 | 运行方式 |
| -- | ---- | -------- |
| C# | `demo/csharp/PekRRedisDemo`（引用 DH.NRedis 源码工程） | `dotnet run --project demo\csharp\PekRRedisDemo -- auto --config "<连接串>"` |
| Rust | `examples/demo.rs` | `cargo run --example demo -- auto --config "<连接串>"` |

若要按更苛刻口径做可重复回归，不再只看“核心路径跑过”，请直接执行严格门槛脚本与矩阵：[interop-strict.md](interop-strict.md)。

```powershell
powershell -ExecutionPolicy Bypass -File scripts\interop-strict.ps1 -NoBuild
```

已实测的验证内容（2026-09-27）：

- `selftest`：两侧编码器字节格式全绿（字符串/整数/布尔/时间/JSON，含互相解码）；
- 交叉读写：C# `write` → Rust `verify` 14/14；Rust `write` → C# `verify` 14/14，双方回执 `Failures` 均为空；
- 可靠队列：C# 生产 → Rust 消费并 Ack；Rust 生产 → C# 消费并 Ack；双方 `qstatus` 能解析对方的 Status JSON；
- Stream：C# 写入（基元 `__data` + 对象字段）→ Rust 消费并 Ack；Rust 写入 → C# 消费并 Ack；
  C# 消费不确认 → Rust `retry_ack`（`XPENDING`+`XCLAIM`）抢回并确认，双方 `stream-status` 互认消费者与挂起；
- 延迟队列：C# 写入（delay=2s）→ Rust 到期后消费；Rust 写入 → C# 到期后消费（`ZSET score` 两端一致）；
- 分布式锁：C# 持锁期间 Rust 抢锁失败，释放后 Rust 立即接管（两种锁值格式兼容）。
- PubSub：普通订阅（C# `SUBSCRIBE` ← Rust `PUBLISH`）、模式订阅（Rust `PSUBSCRIBE` ← C# `PUBLISH`）、分片订阅（C# `SSUBSCRIBE` ← Rust `SPUBLISH`）均已双向实跑，`delivered=1` 且订阅端收到预期频道与消息；
- 双方回执：`report` 显示 C# / Rust 两侧 `Failures` 都为空。

没有真实 Redis 也能跑（内置迷你 Redis，RESP2 子集）：

```powershell
cargo run --example mock_redis                                            # 终端1
cargo run --example demo -- auto --mock                                   # 终端2（Rust 自演）
dotnet run --project demo\csharp\PekRRedisDemo -- auto --config "server=127.0.0.1:16379;db=0"   # 终端3
cargo run --example demo -- verify --config "server=127.0.0.1:16379;db=0"                       # 交叉验证
```

---

## 五、C# ↔ Rust 概念对照

| C# / DH.NRedis | Rust / pek-rredis |
| -------------- | ----------------- |
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
| `RedisRedLock.Acquire` | `acquire_red_lock` / `FullRedis::acquire_red_lock`（返回 `RedLock`，Drop 自动释放） |
| `QueueExtensions.ConsumeAsync<T>` | `RedisReliableQueue::consume_json`（类型化）/ `consume_raw`（字符串） |
| `FullRedis.Ex*`（Tair 扩展） | `FullRedis::ex_set`/`ex_get`/`ex_hset`/`ex_hincr_by`…（`tair` 模块） |
| `Redis.ServerType` / `Redis.Version` | `Redis::server_type` / `version_parts` / `require_version` |
| `StartPipeline` / `StopPipeline` | `Redis::pipeline()` / `Pipeline::execute` |
| `CreateSub(db)` | `Redis::create_sub` / `FullRedis::create_sub` |
| `Search(pattern, offset, count)` | `FullRedis::search(pattern, count)` / `search_paged` |

---

## 六、与 DH.NRedis 的已知差异

1. **运行期 `SELECT`**：C# 可在同一实例切换库；Rust 侧以「配置即状态」为原则，请用 `create_sub(db)` 创建子实例（连接池状态不会被多线程共享污染）。
2. **`Search` 边界**：C# 中 `count <= 0` 不返回任何键（含 `Remove("*")` 的边界行为）；Rust 按「不限量」处理并返回去前缀键名。
3. **`GetAll` 键名**：C# `FullRedis.GetAll` 返回**带前缀**的键名；Rust 返回调用方传入的原始键名。
4. **解码容错**：两端一致——解码失败返回 `None`（不抛异常）；服务端 `-ERR` 抛 `Error::Server` 且不重试。
5. **异步 API 形态差异**：Rust 当前 async 层是 tokio `spawn_blocking` 包装，同步复用既有连接池、cluster/sentinel/replication/TLS 与队列实现；这保证行为一致，但底层仍是阻塞 I/O，而不是独立的 async socket/RESP 栈。
6. **JSON 时间格式**：C# 默认 JsonHost 为 FastJson（`2026-09-26 10:00:00`，无毫秒）；Rust 写 ISO 8601。
   Rust 侧结构体时间字段需用 `#[serde(with = "pek_rredis::encoder::datetime")]` 才能互读（裸值路径无此问题）。
7. **Stream 对象消息字段顺序**：C# 按属性声明顺序，Rust 按字典序（serde_json 默认）；字段名与值为准，顺序不影响语义。
   字段内时间用 `encoder::datetime_text`（`yyyy-MM-dd HH:mm:ss.fff`，与 C# 字段编码逐字节一致）。
8. **Tair `Ex*`**：仅阿里云 Tair（KVStore）实例可用；标准 Redis 执行会返回未知命令（与 C# 行为相同）。
9. **`AutoPipeline`/`FullPipeline`**：C# 的自动管道优化未复刻，Rust 使用显式 `pipeline()`（语义等价）。
10. **异步 wrapper 覆盖面**：当前已覆盖 `Redis` / `FullRedis` / Hash / List / Set / SortedSet / Stack / Geo / HyperLogLog / PubSub / 普通队列 / 可靠队列 / 延迟队列 / Stream；关键 direct/helper 方法已提供显式 async 入口，其余极少数长尾能力仍可通过 `with_sync` 复用同步实现。
11. **仅宿主适配层不做 1:1 迁移**：`RedisCacheProvider`/`CacheExtensions` 这类依赖 .NET DI/接口抽象的宿主层不直接照搬；对应 Redis 语义已由 `RedisEventBus` / `RedisStat` / `RedisDeferred` 与现有 `FullRedis`/队列工厂覆盖。

---

## 七、路线图（2026-09-26 更新）

| 阶段 | 内容 |
| ---- | ---- |
| v0.2 | 集群/哨兵/主从（`Cluster`/`Sentinel`/`Replication`）；RESP3 推送与 `CLIENT TRACKING` |
| v0.3 | 可选：进一步细化零散 async 公开面与错误分类，或演进为原生 async socket/RESP 栈 |
| v1.0 | 与 DH.NRedis 的 XUnitTest 跑同一套集成测试做双向互操作回归 |

> 已提前完成：全部命令级补齐、`RedisRedLock`、`consume_json`/`consume_raw`、Tair `Ex*`（2026-09-26 审计）。
> 已提前完成：集群/哨兵/主从与 TLS；当前 async 层也已提供 tokio 初版包装并通过专项测试。
> 现有功能闭环已经完成；后续若继续演进，主要是进一步细化错误分类与 async 人体工学，或评估是否值得演进为原生 async socket/RESP 栈。

---

## 八、渐进迁移建议

1. **共用键前缀**：Rust 先只读写新业务键（如 `r:order:*`），C# 保持原键空间；
2. **影子消费**：可靠队列天然支持多消费者（各自 `ukey`），让 Rust 消费者先在旁路消费比对结果；
3. **逐用例切换**：把一个用例的生产者/消费者从 C# 切到 Rust，验证后删除 C# 路径；
4. **收尾**：C# 侧只保留管理与运维工具，数据格式不变，切换零迁移成本。
