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
| 可靠队列 | C# `push` → Rust `consume`；Rust `push` → C# `consume`；双方 `qstatus` |
| Stream | C# `stream-push` → Rust `stream-consume`；Rust `stream-push` → C# `stream-consume`；C# `--no-ack` → Rust `--retry-seconds 0` 抢回；双方 `stream-status` |
| 延迟队列 | C# `delay-push` → Rust `delay-consume`；Rust `delay-push` → C# `delay-consume` |
| 分布式锁 | C# 持锁时 Rust 抢锁失败；C# 释放后 Rust 抢锁成功 |
| PubSub 普通订阅 | Rust `pubsub-publish` → C# `pubsub-subscribe` |
| PubSub 模式订阅 | C# `pubsub-publish` → Rust `pubsub-subscribe --pattern` |
| PubSub 分片订阅 | Rust `pubsub-publish --shard` → C# `pubsub-subscribe --shard` |
| 双方回执 | Rust `report` + C# `report`，`Failures=[]` |

## 当前尚未纳入严格门槛的范围

下面这些能力虽然已有源码审计、parity/mock/live 测试或语义对齐保证，但还没有全部做成 C#↔Rust 的双边 live 矩阵，因此在本文件口径下不算“严格通过”：

| 范围 | 现状 |
| ---- | ---- |
| Cluster / Sentinel / Replication | Rust 侧已有完整实现和测试，但缺少 C#↔Rust 双进程对同一拓扑实例的联调矩阵 |
| TLS / rediss | Rust 侧已支持并有本地 TLS 单测，但缺少 C#↔Rust 同实例 TLS 联调 |
| Helper / 运维命令族 | 如 `FUNCTION`、`SLOWLOG`、`LATENCY`、`BITFIELD`、`LMOVE`、`SMISMEMBER`、`ZMSCORE` 等，目前主要由 Rust parity/mock 测试证明 |
| Async API | Rust async 包装已补齐并通过测试，但尚未形成独立的跨语言 live 验收维度 |

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