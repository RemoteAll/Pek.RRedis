# Pek.RRedis ↔ DH.NRedis 相互验证 Demo

两个可执行 Demo，用**同一套固定样本（fixtures）**证明 C# 与 Rust 两端数据完全互通：

| 侧 | 工程 | 命令 |
|----|------|------|
| C# | `demo/csharp/PekRRedisDemo`（引用 DH.NRedis 源码工程） | `dotnet run --project demo\csharp\PekRRedisDemo -- <命令>` |
| Rust | `examples/demo.rs`（本仓库 crate 的示例） | `cargo run --example demo -- <命令>` |

两者命令与样本**逐项对应**：一方 `write`，另一方 `verify` 即可交叉校验；每次 `verify` 会写下回执（`{prefix}{side}:receipt`），`report` 可查看双方回执。

如果要按“严格门槛”重复验证，而不是手工挑几条命令实跑，请直接使用根目录的 [interop-strict.md](../interop-strict.md) 与 [scripts/interop-strict.ps1](../scripts/interop-strict.ps1)。

---

## 一、快速开始

### 场景 A：本机没有 Redis（用内置迷你 Redis）

```powershell
# 终端 1：启动迷你 Redis（RESP2 子集实现，仅供演示/联调）
cargo run --example mock_redis            # 默认 127.0.0.1:16379

# 终端 2：C# 写入 + 自检
dotnet run --project demo\csharp\PekRRedisDemo -- auto --config "server=127.0.0.1:16379;db=0"

# 终端 3：Rust 校验 C# 写入的数据（并写 Rust 回执）
cargo run --example demo -- auto --config "server=127.0.0.1:16379;db=0"
```

也可以完全离线自检编码器字节格式（不需要任何 Redis）：

```powershell
cargo run --example demo -- selftest
dotnet run --project demo\csharp\PekRRedisDemo -- selftest
```

### 场景 B：连接真实 Redis（推荐在联调/CI 中做双向验证）

```powershell
cargo run --example demo -- auto   --config "server=10.0.0.5:6379;password=***;db=15"
dotnet run --project demo\csharp\PekRRedisDemo -- auto --config "server=10.0.0.5:6379;password=***;db=15"
cargo run --example demo -- report --config "server=10.0.0.5:6379;password=***;db=15"
```

标准交叉验证顺序：

```text
C# write ─► Rust verify ─► Rust write ─► C# verify ─► 双方 report
```

### 场景 C：可靠队列跨语言消费（最容易出问题的地方）

```powershell
# C# 生产 → Rust 消费并确认
dotnet run --project demo\csharp\PekRRedisDemo -- push --count 5 --config "<config>"
cargo run --example demo -- consume --count 5 --config "<config>"

# Rust 生产 → C# 消费并确认
cargo run --example demo -- push --count 3 --config "<config>"
dotnet run --project demo\csharp\PekRRedisDemo -- consume --count 3 --config "<config>"

# 双方各自解析对方的消费者状态（Status JSON）
cargo run --example demo -- qstatus --config "<config>"
dotnet run --project demo\csharp\PekRRedisDemo -- qstatus --config "<config>"
```

### 场景 D：跨语言分布式锁互斥

```powershell
# 终端 1：C# 持锁 8 秒
dotnet run --project demo\csharp\PekRRedisDemo -- lock --seconds 8 --config "<config>"

# 终端 2：Rust 尝试抢锁 → 输出"未拿到锁"；等 C# 释放后再抢 → 成功
cargo run --example demo -- lock --seconds 1 --config "<config>"
```

### 场景 E：Stream 消息队列（含消费组与死信抢占）

```powershell
# C# 写入（基元 + 对象）→ Rust 消费确认

dotnet run --project demo\csharp\PekRRedisDemo -- stream-push --count 5 --config "<config>"
cargo run --example demo -- stream-consume --count 5 --config "<config>"

# Rust 写入 → C# 消费确认
cargo run --example demo -- stream-push --count 3 --config "<config>"
dotnet run --project demo\csharp\PekRRedisDemo -- stream-consume --count 3 --config "<config>"

# 死信抢占：C# 消费不确认，Rust 用 retry=0 抢回
cargo run --example demo -- stream-push --count 2 --config "<config>"
dotnet run --project demo\csharp\PekRRedisDemo -- stream-consume --count 2 --no-ack --config "<config>"
cargo run --example demo -- stream-status --config "<config>"          # 可见挂起为该 C# 消费者持有
cargo run --example demo -- stream-consume --count 2 --retry-seconds 0 --config "<config>"
dotnet run --project demo\csharp\PekRRedisDemo -- stream-status --config "<config>"   # 挂起应清零
```

---

## 二、命令一览（两侧一致）

| 命令 | 说明 |
|------|------|
| `selftest` | 离线校验编码器字节格式（无需 Redis） |
| `write` | 先清空固定样本键，再写入样本 + 本侧标记 |
| `verify` | 校验全部样本（含对方写入的），写回执，失败返回非 0 |
| `push --count N` | 向可靠队列 `{p}reliable` 推入 `msg-0001...` |
| `consume --count N` | 用可靠队列消费 N 条并确认（Ack 队列可见） |
| `qstatus` | 打印主队列长度、Ack 队列列表、双方消费者的 Status JSON（并用本侧类型解析） |
| `lock --seconds N` | 申请分布式锁 `{p}lock`，持有 N 秒后释放 |
| `stream-push --count N [--group G]` | 向 `{p}stream:demo` 写入 N 条消息（奇数基元 `__data`、偶数对象字段） |
| `stream-consume --count N [--group G] [--no-ack] [--retry-seconds S]` | 消费组消费并确认；`--no-ack` 留作死信，`--retry-seconds 0` 可立即抢占他人死信 |
| `stream-status [--group G]` | 流长度 / 消费组 / 挂起明细 / 消费者（两侧可互认） |
| `delay-push --count N [--delay S]` | 写入 N 条延迟消息（`score = Unix 秒 + 延迟`） |
| `delay-consume --count N [--wait S]` | 等待到期并消费（两侧可跨语言互相消费） |
| `report` | 查看双方回执 receipt |
| `clean` | 删除 `{prefix}*` 全部键 |
| `auto` | `write` + `verify` + `report` |

通用参数：`--config "<连接串>"`（默认取环境变量 `REDIS_CONFIG`，再默认 `server=127.0.0.1:6379;db=15`）、
`--prefix`（默认 `pekrredis:demo:`）、Rust 侧额外支持 `--mock`（进程内迷你 Redis）。

---

## 三、固定样本规范（双方必须一致）

| 键（前缀后） | 写入方式 | 期望原始字节 / 值 |
|--------------|----------|-------------------|
| `str` | `SET` 字符串 | `Hello 互通`（无引号） |
| `int` | `SET` 整数 | `123456789` |
| `bool` | `SET` 布尔 | `True`（读回 `true`） |
| `dt` | `SET` 时间 | `2026-09-26 10:00:00.123`（毫秒必存） |
| `json` | `SET` 对象 | `{"Name":"互通Demo","CreateTime":"...","Count":7}`，属性名 PascalCase；**JSON 内时间用整秒**（见下） |
| `hash` | `HSET` | 字段 `a=1`、`b=2`（文本 `1`/`2`） |
| `list` | `RPUSH` | `[1,2,3]` |
| `set` | `SADD` | `{x,y}` |
| `zset` | `ZADD` | `m1=1.5`、`m2=0.5` |
| `queue` | `LPUSH q1`、`LPUSH q2` | `RPOP` 顺序 `q1`、`q2` |
| `{side}:marker` | `SETEX` | 本侧写入时间（用来说明数据来源） |
| `reliable:*` | 可靠队列 | `{p}reliable`、`{p}reliable:Ack:{ukey}`、`{p}reliable:Status:{ukey}` |
| `{side}:receipt` | `SETEX` | 回执 JSON：`{Side, Time, Failures}` |

---

## 四、本次实测结论（2026-09-27，本机）

用内置迷你 Redis 完成的双向验证（C# 进程 ↔ RESP ↔ Rust 进程）：

- `selftest`：两侧字节格式全绿；C# 能读 Rust 的 ISO 时间 JSON，Rust 能读 C# 的文本时间 JSON；
- 交叉读写：C# `write` → Rust `verify` 14/14 通过；Rust `write` → C# `verify` 14/14 通过；
- 可靠队列：C# 生产 5 条 → Rust 全部消费并确认；Rust 生产 3 条 → C# 全部消费并确认；双方 `qstatus` 都能解析对方的 Status JSON；
- Stream：C# 写入 5 条（基元 `__data` + 对象字段，时间 `2026-09-26 10:00:00.123`）→ Rust 消费并确认；
  Rust 写入 3 条 → C# 消费并确认；C# 消费 2 条不确认 → Rust `--retry-seconds 0` 通过 `XPENDING`+`XCLAIM` 抢回并确认，双方 `stream-status` 互认消费者与挂起数（挂起归零）；
- 延迟队列：C# 写入 3 条（delay=2s）→ Rust 到期后全部取到；Rust 写入 2 条 → C# 到期后全部取到（`score = Unix 秒 + 延迟` 两端一致）；
- 分布式锁：C# 持锁期间 Rust 抢锁失败（`✘ 未拿到锁`），C# 释放后 Rust 立即拿到；锁值两种格式（旧包纯数字 / 新 `token|tick`）互相兼容；
- PubSub：普通订阅（C# `SUBSCRIBE` ← Rust `PUBLISH`）、模式订阅（Rust `PSUBSCRIBE` ← C# `PUBLISH`）、分片订阅（C# `SSUBSCRIBE` ← Rust `SPUBLISH`）均已实跑，发布端 `delivered=1`，订阅端成功收到预期频道与消息；
- 双方 receipt 互读，`Failures` 均为空。

## 五、实测发现（写文档/排查时注意）

1. **JSON 内时间**：C# 默认 JsonHost 是 **FastJson**，序列化 `DateTime` 为 `2026-09-26 10:00:00`（**丢毫秒**，NewLife 文本格式）；`System.Text.Json` 则是 ISO 8601。
   - C# 两种 JsonHost 都能**读** ISO 8601（已实测），FastJson 也能读自己写的文本格式；
   - Rust 的 serde 默认只认 ISO → **结构体时间字段请加** `#[serde(with = "pek_rredis::encoder::datetime")]`，读写双向兼容；
   - 跨语言 JSON 样本建议时间取整秒，避免毫秒精度差异。
2. **写入语义**：列表/队列是追加语义，`write` 前必须先删除样本键（两侧 Demo 均已先清理），否则会被对方校验出多余元素。
3. **锁值格式**：DH.NCore 旧包写纯数字时间戳，新包/本库写 `token|绝对过期毫秒`；两端 `ParseExpire` 都兼容两种格式，可混用。
4. **Rust 格式化小坑**：`format!("{i:0000}")` 是"零标记 + 宽度 0"，不会补零；应写 `format!("{i:04}")`。
5. **Stream 字段内的时间**：C# 对象消息写入 Stream 走**编码器**（`yyyy-MM-dd HH:mm:ss.fff`），不是 JSON 路径；
   Rust 结构体时间字段需用 `#[serde(with = "pek_rredis::encoder::datetime_text")]`（JSON 路径则用 `...::datetime`）。
6. **Stream 字段顺序**：C# 按属性声明顺序，Rust 按字典序；字段名/值为准，顺序不影响语义（对比原始 body 时注意）。
7. **格式化占位符不通用**：Rust 补零是 `{i:04}`，C# 补零是 `{i:0000}`；C# 写 `{i:04}` 会把 `4` 当字面量输出 `14`（本次 Demo 已实际踩坑并修正）。

## 六、把 Demo 当回归用

- 把场景 B 的 5 条命令写进脚本/CI：任何一端改动后跑一遍即可发现格式回归；
- Rust 侧 `tests/interop.rs` 已把这些规格固化（进程内迷你 Redis，无需外部依赖）；
- 真实 Redis 联调：`$env:REDIS_ADDR` + `cargo test --test live_redis`。
