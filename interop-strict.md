# Strict Interop Gate

本文件定义比“核心路径跑过”更苛刻的验收口径。

## 标准

一项能力只有同时满足下列条件，才算“严格验证通过”：

1. C# 与 Rust 连接同一个 Redis 实例。
2. 存在跨语言方向性证据：至少一侧写入/发布，另一侧读取/消费/订阅；若该能力天然双向，则要求双向都跑。
3. 验证结果必须来自可执行命令，而不是只靠 mock 单元测试、parity 测试或源码审计。
4. 并发能力必须包含负向与正向两种证据：
   - 负向：对方持有/占用时，本侧失败。
   - 正向：对方释放后，本侧成功。
5. 最终双方 `report` 的 `Failures` 都为空。

## 当前严格通过项

以下能力已纳入 [scripts/interop-strict.ps1](scripts/interop-strict.ps1) 并已在 127.0.0.1:16379 的迷你 Redis 上实跑通过：

| 能力 | 严格证据 |
| ---- | ---- |
| 编码器字节格式 | C# `selftest` + Rust `selftest` |
| 固定样本双向读写 | C# `write` → Rust `verify`；Rust `write` → C# `verify` |
| Helper / direct 高级 API | C# `write-advanced` → Rust `verify-advanced`；Rust `write-advanced` → C# `verify-advanced` |
| Replication 拓扑 | C# `mode=replication` 写入 → Rust 直连 master 读到且 replica 读不到；Rust `mode=replication` 写入 → C# 直连 master 读到且 replica 读不到 |
| Sentinel 拓扑 | C# `mode=sentinel` 写入 → Rust 直连 master 读到且 replica 读不到；Rust `mode=sentinel` 写入 → C# 直连 master 读到且 replica 读不到 |
| Cluster 拓扑 | C# `mode=cluster` 对命中 5461..16383 槽位的 `{...}` key 写入 → Rust 直连 seed 读不到、直连目标节点读到；Rust 反向同理 |
| 运维 API | 在同一份预置 slowlog/latency 样本上，C# 与 Rust 都能读到同一结果；随后分别由一侧执行 `SLOWLOG RESET` / `LATENCY RESET`，另一侧确认样本已清空 |
| TLS 传输 | C# 与 Rust 都通过 `rediss://` / `Ssl=true` 连接同一个自签名 TLS mock Redis，双向完成固定样本 `write` / `verify` |
| Async API | C# `write` → Rust `verify-async`；Rust `write-async` → C# `verify`；C# `push` → Rust `consume-async`；Rust `push-async` → C# `consume` |
| RedisDeferred | Rust `deferred-add` → C# `deferred-process` 读到去重批次；C# `deferred-add` → Rust `deferred-process` 反向同理 |
| RedisStat | Rust `stat-stage`（HASH 累加 + AddDelayQueue）→ C# `stat-process-once` 读到 `pv/uv` 聚合结果；C# `stat-stage` → Rust `stat-process-once` 反向同理 |
| RedisEventBus | C# 先起 `eventbus-subscribe`，Rust `eventbus-publish` 后收到 `name/count`；Rust 先起订阅，C# 发布后收到同样事件 |
| 可靠队列 | C# `push` → Rust `consume`；Rust `push` → C# `consume`；双方 `qstatus` |
| Stream | C# `stream-push` → Rust `stream-consume`；Rust `stream-push` → C# `stream-consume`；C# `--no-ack` → Rust `--retry-seconds 0` 抢回；双方 `stream-status` |
| 延迟队列 | C# `delay-push` → Rust `delay-consume`；Rust `delay-push` → C# `delay-consume` |
| 分布式锁 | C# 持锁时 Rust 抢锁失败；C# 释放后 Rust 抢锁成功 |
| PubSub 普通订阅 | Rust `pubsub-publish` → C# `pubsub-subscribe` |
| PubSub 模式订阅 | C# `pubsub-publish` → Rust `pubsub-subscribe --pattern` |
| PubSub 分片订阅 | Rust `pubsub-publish --shard` → C# `pubsub-subscribe --shard` |
| 双方回执 | Rust `report` + C# `report`，`Failures=[]` |

## 当前尚未纳入严格门槛的范围

当前迁移范围内的核心互通/拓扑/运维/异步能力，以及 `RedisDeferred` / `RedisStat` / `RedisEventBus` 这三块服务层语义，已经全部做成 C#↔Rust 的双边 live 矩阵并纳入严格 gate。现在剩下未纳入 strict live gate 的范围，收敛到下面这一类真正的宿主专属适配层：

| 范围 | 现状 |
| ---- | ---- |
| .NET 宿主接口层 | `RedisCacheProvider`、`CacheExtensions` 这类依赖 ASP.NET / DI / .NET 接口抽象的宿主适配层不做 1:1 迁移；Rust 侧改由当前 Web/服务框架自己的状态注入与工厂方式承接 |

## 执行

先确保 Redis 已可用；默认可直接复用本仓库的迷你 Redis：

```powershell
cargo build --examples
dotnet build demo\csharp\PekRRedisDemo\PekRRedisDemo.csproj
cargo run --example mock_redis
```

然后执行严格门槛脚本：

```powershell
powershell -ExecutionPolicy Bypass -File scripts\interop-strict.ps1
```

若使用真实 Redis：

```powershell
powershell -ExecutionPolicy Bypass -File scripts\interop-strict.ps1 -Config "server=10.0.0.5:6379;password=***;db=15"
```
