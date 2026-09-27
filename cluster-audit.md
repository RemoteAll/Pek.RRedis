# Cluster 审计与实现清单

日期：2026-09-26

## 目标

为 `pek-rredis` 补齐当前尚未实现的集群相关能力，范围包括：

- Redis Cluster 槽位路由
- MOVED / ASK 重定向
- 主从复制节点发现与读写路由
- Sentinel 发现与模式探测

本文件是第三轮 API 审计后的下一阶段设计锚点，避免重复探索 C# 参考实现。

## 一、C# 参考实现结论

### 1.1 接入点不在命令实现，而在执行框架

C# 侧不是在每个命令里单独处理集群，而是在 `FullRedis.Execute` 上统一接入：

- `FullRedis.Mode` / `AutoDetect` / `Cluster`
- `FullRedis.InitCluster()`：先读 `INFO`，再判定 `cluster` / `sentinel` / `standalone(master-slave)`
- `FullRedis.Execute(key, func, write)`：按 key 选择节点
- `FullRedis.ExecuteOnNode(...)`：异常重试、换节点、回收坏连接
- `FullRedis.GetPool(IRedisNode)`：每个节点独立连接池

这说明 Rust 一期的关键不是多补几个 `CLUSTER` 命令，而是补一个“key-aware execution layer”。

### 1.2 Cluster 的核心职责

`Clusters/RedisCluster.cs` 的职责很明确：

- `CLUSTER NODES` 拉取拓扑
- 解析节点、主从、slot range、importing/migrating
- `CRC16(key) % 16384` 定位槽位
- 支持 hash tag：`{...}`
- 根据 `write` 优先主节点，读可回落从节点
- `MOVED` / `ASK` 异常后重选节点
- 节点失败后按错误次数进行 shielding
- 定时刷新拓扑

`ClusterNode.cs` 还表明节点模型至少要包含：

- `id`
- `endpoint`
- `flags`
- `master`
- `link_state`
- `slots`
- `importings`
- `migratings`

### 1.3 Sentinel 与主从复制不是独立世界，而是统一抽象

C# 通过 `IRedisCluster` 统一三种模式：

- `RedisCluster`
- `RedisReplication`
- `RedisSentinel`

共有接口：

- `Nodes`
- `SelectNode(key, write)`
- `ReselectNode(key, write, node, exception)`
- `ResetNode(node)`

这意味着 Rust 也应该先抽象“拓扑/选节点器”，再把 Cluster、Replication、Sentinel 挂进去，而不是做三套互不相干的分支。

## 二、Rust 当前基线

### 2.1 已有能力

当前 Rust 侧已经具备可复用底座：

- `RedisOptions.servers`：支持多个地址
- `Redis::new()`：可在多个 endpoint 之间轮转连接
- `Redis::execute_inner()`：网络异常自动重试
- `Pool`：已有通用连接池实现
- `RedisClient`：握手、AUTH、SELECT、RESP2/3 已稳定

### 2.2 当前缺口

但 Rust 还没有以下结构：

- `mode` / `auto_detect` / `cluster` 之类的拓扑状态
- 节点抽象（node model）
- 槽位模型（slot range）
- 每节点连接池映射
- key 到节点的路由器
- MOVED / ASK 识别与重定向执行
- `CLUSTER NODES` / `INFO Replication` / `INFO Sentinel` 的拓扑刷新器

### 2.3 当前最关键的架构缺口

当前 Rust 的执行入口是：

- `Redis::execute(&[&[u8]])`
- `Redis::execute_blocking(&[&[u8]], timeout)`

这两个接口只知道“命令参数数组”，不知道“哪个参数是 key”。

因此，**最小一期必须先补一个带 key 上下文的内部执行骨架**，否则集群无法可靠落地。

## 三、建议的一期实现边界

### 3.1 一期必须完成

建议把第一阶段控制在“可替换生产 cluster 路由”最小闭环：

  - hash tag + CRC16 路由
  - MOVED / ASK 重定向
  - 节点级连接池

### 3.2 一期明确不做

为了尽快落地，一期不建议同时做这些：

- 槽位迁移管理命令（`ADDSLOTS` / `DELSLOTS` / rebalance）
- 完整 Sentinel 事件订阅
- 自动 failover 编排
- 跨 slot 多 key 自动拆分合并
- async API

这些可以在 cluster routing 稳定后再加。

## 四、建议的数据结构

### 4.1 配置层

建议在 `src/options.rs` 扩充：

- `ServerMode` 枚举：`Auto / Standalone / Cluster / Sentinel / Replication`
- `auto_detect: bool`
- `topology_refresh_seconds: u64`
- `read_from_replicas: bool`
- `sentinel_master_name: Option<String>`

### 4.2 拓扑层

建议新增 `src/cluster/` 模块，最少包含：

- `node.rs`
  - `RedisNodeState`
  - `ClusterNode`
  - `SlotRange`
- `topology.rs`
  - `Topology` trait
  - `select_node(key, write)`
  - `reselect_node(key, write, node, err)`
  - `reset_node(node)`
- `cluster.rs`
  - `RedisClusterTopology`
- `replication.rs`
  - `RedisReplicationTopology`
- `sentinel.rs`
  - `RedisSentinelTopology`

### 4.3 执行层

建议不要立刻推翻现有 `Redis::execute()`，而是新增内部 helper：

- `execute_on_key(key, write, build_command)`
- `execute_on_keys(keys, write, build_command)`

其中：

- 单 key 命令走拓扑选择
- 多 key 命令先检查同 slot
- 非 key 命令仍走现有全局连接池

这样可以把变更面控制在最小范围内。

## 五、推荐实现顺序

### Phase 0：配置与抽象骨架

- 在 `RedisOptions` 增加 mode/autodetect 等字段
- 新建 `src/cluster/` 模块和基础 trait / struct
- 先不接业务命令，只保证编译通过

当前状态：已完成。

### Phase 1：Cluster 路由闭环

- 解析 `CLUSTER NODES`
- 实现 slot 路由与 hash tag
- 实现 MOVED / ASK 重试
- 为节点维护独立连接池
- 先让 `FullRedis` 的单 key 常用命令走 cluster helper

当前状态：已完成。

- 已完成：`CLUSTER NODES` 行解析、`RedisClusterTopology` 快照、hash slot/hash tag、按 endpoint 定向连接、单 key 基础命令按 key 路由、`mode=cluster` / `autoDetect` 自动加载拓扑、`TopologyRefreshSeconds` 定时刷新、链式 `MOVED`/`ASK` 重定向。
- 已完成验证：新增 `tests/cluster_routing.rs`，覆盖写入落主节点、读副本、同 slot `rename/copy` 路由、配置驱动自动初始化、拓扑刷新切换节点、链式重定向。
- 已完成重定向骨架：识别 `MOVED` / `ASK`，`ASK` 会先补发 `ASKING`，`MOVED` 会把 slot->endpoint 记入拓扑覆盖表后再重发。
- 已完成：节点 shielding/backoff 策略、redirect 次数上限。
- 尚未完成：更细粒度错误分类。

### Phase 2：同 slot 多 key 与更多命令面

- `MGET` / `DEL many` / `UNLINK many` / `TOUCH many`
- 检查 keys 是否同 slot
- 不同 slot 时明确报错，而不是静默错误路由

当前状态：已完成当前计划中的功能闭环。

- 已完成：`MGET` / `DEL many` / `UNLINK many` / `TOUCH many` 按节点分组执行；`SetAll` 在 cluster 下按 key 正确路由；`DBSIZE` / `KEYS` / `SCAN` 聚合到所有主节点，使 `search/remove_pattern` 在 cluster 下可用。
- 说明：Rust 当前对这批多 key 命令采用“按节点分组聚合”的兼容实现，比“直接 cross-slot 报错”更接近 DH.NRedis 现有行为，也更实用。

### Phase 3：Replication

- `INFO Replication` 探测主从
- master 优先写，读可选 replica
- 不可达节点 shielding

当前状态：已完成。

- 已完成：`INFO Replication` 解析、主从节点探索、`mode=replication` 与 `autoDetect` 自动发现、master 优先写、可选读副本、节点 shielding/backoff。
- 已完成验证：`replication_mode_auto_loads_topology_and_prefers_master_for_writes`、`auto_detect_replication_loads_topology_from_info`、`replication_topology_reselects_to_master_when_replica_read_fails`。

### Phase 4：Sentinel

- `INFO Sentinel` 获取监控集
- 判断哨兵背后是 replication 还是 cluster
- 动态刷新 downstream topology

当前状态：已完成。

- 已完成：`INFO Sentinel` 解析、`mode=sentinel` 与 `autoDetect` 自动发现、根据 downstream `INFO` 自动委托到 replication 或 cluster 拓扑。
- 已完成验证：`sentinel_mode_discovers_master_and_delegates_to_replication_topology`、`auto_detect_sentinel_loads_topology_from_info`、`sentinel_mode_can_delegate_to_cluster_topology`。

### Phase 5：Async API

- 目标：不再维护第二套 RESP / 连接池 / TLS / cluster 路由实现，而是用 tokio `spawn_blocking` 把现有同步能力安全暴露为 async。

当前状态：已完成闭环，并对显式 async 公开面做了第二轮补齐。

- 已完成：新增 `src/async_api.rs`，公开 `AsyncRedis`、`AsyncFullRedis`、`AsyncRedisHash`、`AsyncRedisList`、`AsyncRedisSet`、`AsyncRedisSortedSet`、`AsyncRedisStack`、`AsyncRedisGeo`、`AsyncHyperLogLog`、`AsyncPubSub`、`AsyncRedisQueue`、`AsyncRedisReliableQueue`、`AsyncRedisDelayQueue`、`AsyncRedisStream`。
- 已完成：`AsyncRedis::with_sync` / `AsyncFullRedis::with_sync` 作为通用逃生口；结构型 wrapper 也提供 `with_sync`，便于在不补第二套协议栈的前提下承接长尾同步能力。
- 已完成验证：新增/扩展 `tests/async_api.rs`，现共 10 条用例，覆盖基础 KV、Hash、List、Set、SortedSet、HyperLogLog、Stack、PubSub（普通/模式/分片）、普通队列、可靠队列、Stream，以及显式 async helper/管理命令，确认 tokio 包装不改变现有行为语义。
- 当前剩余：无功能性缺口；后续主要是可选的人体工学增强、错误分类细化，或是否需要原生 async socket/RESP 栈。

## 六、一期测试清单

现有 mock Redis 还不具备 cluster 行为，一期至少新增这些测试基础设施：

- `CLUSTER NODES` 返回样本
- MOVED 返回样本
- ASK 返回样本
- 同 key hash tag 路由断言
- 同 slot / cross-slot 多 key 断言
- 节点失败后重选断言

当前状态：已完成当前一期目标。

- 已完成：同 key/slot 路由断言、读副本断言、同 slot `rename/copy` 路由断言、`MOVED` 持久映射断言、`ASK` 临时跳转与 `ASKING` 断言、链式重定向断言、多 key 分组断言、cluster `search/remove_pattern` 聚合断言、`mode=cluster` / `autoDetect` 自动加载拓扑断言、replication/sentinel 自动发现断言、节点失败 shielding 断言。

建议新增测试文件：

- `tests/cluster_routing.rs`（已落地，当前承担一期路由骨架验证）
- 后续仍建议补 `tests/cluster_parity.rs`，若后面继续细化错误分类，可单独承载更细故障语义

最小覆盖：

- `select_slot_by_crc16_and_hashtag`
- `moved_redirect_updates_slot_mapping`
- `ask_redirect_retries_without_persisting_wrong_slot`
- `multi_key_requires_same_slot`
- `replication_prefers_master_for_write`

## 七、当前建议

下一步最合理的工程动作不是直接碰 Sentinel，而是：

1. 先加 `ServerMode` 和 `cluster` 模块骨架
2. 再把 `CLUSTER NODES` + slot 路由 + MOVED/ASK 做成可测闭环
3. 之后再接 `Replication` 和 `Sentinel`

原因：

- Cluster 是当前剩余能力里最核心的一块
- 它还会反向决定 async API 的抽象边界
- Sentinel / Replication 复用同一套 node/topology 抽象，晚一点做反而更稳

## 八、非迁移边界再确认

本阶段不属于 Rust Redis 客户端迁移目标的 .NET 专属服务层包括：

- `RedisCacheProvider`
- `RedisEventBus`
- `RedisStat`
- `RedisDeferred`
- `CacheExtensions`
- DI 扩展
- `Bench` / `WriteLog`

这些属于宿主框架/ASP.NET/运行时集成层，不属于 Redis 协议与数据互通本体。