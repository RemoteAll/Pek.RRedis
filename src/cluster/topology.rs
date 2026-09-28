use crate::cluster::{ClusterNode, hash_slot};
use crate::options::ServerMode;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// 拓扑选择器：统一抽象 Cluster / Sentinel / Replication。
pub trait Topology: Send + Sync {
    /// 当前拓扑模式。
    fn mode(&self) -> ServerMode;

    /// 当前已知节点快照。
    fn nodes(&self) -> Vec<ClusterNode>;

    /// 根据 key 与读写类型选择节点。
    fn select_node(&self, key: &str, write: bool) -> Option<ClusterNode>;

    /// 根据异常结果重选节点。
    fn reselect_node(&self, key: &str, write: bool, current: &ClusterNode) -> Option<ClusterNode>;

    /// 按 endpoint 映射到已知节点。
    fn map_endpoint(&self, endpoint: &str, _key: Option<&str>) -> Option<ClusterNode> {
        self.nodes()
            .into_iter()
            .find(|node| node.endpoint == endpoint)
    }

    /// 记住 slot 的重定向目标。`persistent=true` 表示 MOVED，`false` 表示 ASK 等临时跳转。
    fn remember_redirect(&self, _slot: u16, _endpoint: &str, _persistent: bool) {}

    /// 执行成功后重置节点状态。
    fn reset_node(&self, _endpoint: &str) {}
}

#[derive(Debug, Clone, Default)]
struct NodeHealth {
    errors: u32,
    retry_after: Option<Instant>,
}

#[derive(Debug, Default)]
struct EndpointHealth {
    states: RwLock<HashMap<String, NodeHealth>>,
}

impl Clone for EndpointHealth {
    fn clone(&self) -> Self {
        Self {
            states: RwLock::new(self.states.read().unwrap().clone()),
        }
    }
}

impl EndpointHealth {
    fn is_available(&self, endpoint: &str) -> bool {
        self.states
            .read()
            .unwrap()
            .get(endpoint)
            .and_then(|state| state.retry_after)
            .map(|retry_after| retry_after <= Instant::now())
            .unwrap_or(true)
    }

    fn mark_failure(&self, endpoint: &str) {
        let mut states = self.states.write().unwrap();
        let state = states.entry(endpoint.to_string()).or_default();
        state.errors = state.errors.saturating_add(1);
        let backoff = Duration::from_secs(1u64 << state.errors.min(6));
        state.retry_after = Some(Instant::now() + backoff);
    }

    fn reset(&self, endpoint: &str) {
        self.states.write().unwrap().remove(endpoint);
    }

    fn prefer_available(&self, nodes: Vec<ClusterNode>) -> Vec<ClusterNode> {
        let available: Vec<ClusterNode> = nodes
            .iter()
            .filter(|node| self.is_available(&node.endpoint))
            .cloned()
            .collect();
        if !available.is_empty() {
            available
        } else {
            nodes
        }
    }
}

/// 由 `CLUSTER NODES` 文本快照构造的 Cluster 拓扑。
#[derive(Debug)]
pub struct RedisClusterTopology {
    nodes: Vec<ClusterNode>,
    read_from_replicas: bool,
    slot_overrides: RwLock<HashMap<u16, String>>,
    health: EndpointHealth,
}

impl Clone for RedisClusterTopology {
    fn clone(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
            read_from_replicas: self.read_from_replicas,
            slot_overrides: RwLock::new(self.slot_overrides.read().unwrap().clone()),
            health: self.health.clone(),
        }
    }
}

impl RedisClusterTopology {
    /// 从 `CLUSTER NODES` 文本快照构造拓扑。
    pub fn from_cluster_nodes(text: &str, read_from_replicas: bool) -> Self {
        let mut nodes: Vec<ClusterNode> = text
            .lines()
            .filter_map(ClusterNode::parse_cluster_nodes_line)
            .collect();

        nodes.sort_by(|a, b| {
            let a_slot = a
                .slots
                .iter()
                .map(|slot| slot.from)
                .min()
                .unwrap_or(u16::MAX);
            let b_slot = b
                .slots
                .iter()
                .map(|slot| slot.from)
                .min()
                .unwrap_or(u16::MAX);
            a_slot
                .cmp(&b_slot)
                .then_with(|| a.is_replica.cmp(&b.is_replica))
                .then_with(|| a.endpoint.cmp(&b.endpoint))
        });

        Self {
            nodes,
            read_from_replicas,
            slot_overrides: RwLock::new(HashMap::new()),
            health: EndpointHealth::default(),
        }
    }

    /// 原始节点快照是否为空。
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// 按 endpoint 映射到已知节点。
    pub fn map_endpoint(&self, endpoint: &str) -> Option<ClusterNode> {
        self.nodes
            .iter()
            .find(|node| node.endpoint == endpoint)
            .cloned()
    }

    fn redirected_node(&self, slot: u16, endpoint: &str) -> ClusterNode {
        if let Some(mut node) = self.map_endpoint(endpoint) {
            if !node.contains_slot(slot) {
                node.slots.push(crate::cluster::SlotRange::new(slot, slot));
            }
            node
        } else {
            let mut node = ClusterNode::new(endpoint.to_string());
            node.slots.push(crate::cluster::SlotRange::new(slot, slot));
            node
        }
    }

    fn candidates_for_slot(&self, slot: u16, write: bool) -> Vec<ClusterNode> {
        let all: Vec<ClusterNode> = self
            .nodes
            .iter()
            .filter(|node| node.link_up && node.contains_slot(slot))
            .cloned()
            .collect();

        if write {
            let primaries: Vec<ClusterNode> = all
                .iter()
                .filter(|node| !node.is_replica)
                .cloned()
                .collect();
            if !primaries.is_empty() {
                return self.health.prefer_available(primaries);
            }
            return self.health.prefer_available(all);
        }

        if self.read_from_replicas {
            let replicas: Vec<ClusterNode> =
                all.iter().filter(|node| node.is_replica).cloned().collect();
            if !replicas.is_empty() {
                return self.health.prefer_available(replicas);
            }
        }

        let primaries: Vec<ClusterNode> = all
            .iter()
            .filter(|node| !node.is_replica)
            .cloned()
            .collect();
        if !primaries.is_empty() {
            return self.health.prefer_available(primaries);
        }

        self.health.prefer_available(all)
    }
}

impl Topology for RedisClusterTopology {
    fn mode(&self) -> ServerMode {
        ServerMode::Cluster
    }

    fn nodes(&self) -> Vec<ClusterNode> {
        self.nodes.clone()
    }

    fn select_node(&self, key: &str, write: bool) -> Option<ClusterNode> {
        let slot = hash_slot(key);
        if let Some(endpoint) = self.slot_overrides.read().unwrap().get(&slot).cloned() {
            return Some(self.redirected_node(slot, &endpoint));
        }
        self.candidates_for_slot(slot, write).into_iter().next()
    }

    fn reselect_node(&self, key: &str, write: bool, current: &ClusterNode) -> Option<ClusterNode> {
        self.health.mark_failure(&current.endpoint);
        let slot = hash_slot(key);
        if let Some(endpoint) = self.slot_overrides.read().unwrap().get(&slot).cloned()
            && endpoint != current.endpoint
            && self.health.is_available(&endpoint)
        {
            return Some(self.redirected_node(slot, &endpoint));
        }
        self.candidates_for_slot(slot, write)
            .into_iter()
            .find(|node| node.endpoint != current.endpoint)
    }

    fn map_endpoint(&self, endpoint: &str, _key: Option<&str>) -> Option<ClusterNode> {
        self.map_endpoint(endpoint)
    }

    fn remember_redirect(&self, slot: u16, endpoint: &str, persistent: bool) {
        if persistent {
            self.slot_overrides
                .write()
                .unwrap()
                .insert(slot, endpoint.to_string());
        }
    }

    fn reset_node(&self, endpoint: &str) {
        self.health.reset(endpoint);
    }
}

/// 主从/哨兵共用的复制拓扑。
#[derive(Debug, Clone)]
pub struct RedisReplicationTopology {
    mode: ServerMode,
    nodes: Vec<ClusterNode>,
    read_from_replicas: bool,
    health: EndpointHealth,
}

impl RedisReplicationTopology {
    /// 创建主从/哨兵拓扑。
    pub fn new(mode: ServerMode, mut nodes: Vec<ClusterNode>, read_from_replicas: bool) -> Self {
        nodes.sort_by(|a, b| {
            a.is_replica
                .cmp(&b.is_replica)
                .then_with(|| a.endpoint.cmp(&b.endpoint))
        });
        Self {
            mode,
            nodes,
            read_from_replicas,
            health: EndpointHealth::default(),
        }
    }

    fn preferred_nodes(&self, write: bool) -> Vec<ClusterNode> {
        let primaries: Vec<ClusterNode> = self
            .nodes
            .iter()
            .filter(|node| node.link_up && !node.is_replica)
            .cloned()
            .collect();
        let replicas: Vec<ClusterNode> = self
            .nodes
            .iter()
            .filter(|node| node.link_up && node.is_replica)
            .cloned()
            .collect();

        let ordered = if write {
            if !primaries.is_empty() {
                primaries
            } else {
                replicas
            }
        } else if self.read_from_replicas && !replicas.is_empty() {
            let mut ordered = replicas;
            ordered.extend(primaries);
            ordered
        } else if !primaries.is_empty() {
            let mut ordered = primaries;
            ordered.extend(replicas);
            ordered
        } else {
            replicas
        };

        self.health.prefer_available(ordered)
    }
}

impl Topology for RedisReplicationTopology {
    fn mode(&self) -> ServerMode {
        self.mode
    }

    fn nodes(&self) -> Vec<ClusterNode> {
        self.nodes.clone()
    }

    fn select_node(&self, _key: &str, write: bool) -> Option<ClusterNode> {
        self.preferred_nodes(write).into_iter().next()
    }

    fn reselect_node(&self, _key: &str, write: bool, current: &ClusterNode) -> Option<ClusterNode> {
        self.health.mark_failure(&current.endpoint);
        self.preferred_nodes(write)
            .into_iter()
            .find(|node| node.endpoint != current.endpoint)
    }

    fn map_endpoint(&self, endpoint: &str, _key: Option<&str>) -> Option<ClusterNode> {
        self.nodes
            .iter()
            .find(|node| node.endpoint == endpoint)
            .cloned()
    }

    fn reset_node(&self, endpoint: &str) {
        self.health.reset(endpoint);
    }
}

/// 最小静态拓扑实现，供一期骨架与单元测试使用。
#[derive(Debug, Clone)]
pub struct StaticTopology {
    mode: ServerMode,
    nodes: Vec<ClusterNode>,
    read_from_replicas: bool,
}

impl StaticTopology {
    /// 创建静态拓扑。
    pub fn new(mode: ServerMode, nodes: Vec<ClusterNode>, read_from_replicas: bool) -> Self {
        Self {
            mode,
            nodes,
            read_from_replicas,
        }
    }

    fn candidates_for_slot(&self, slot: u16, write: bool) -> Vec<ClusterNode> {
        let mut matches: Vec<ClusterNode> = self
            .nodes
            .iter()
            .filter(|node| node.link_up && node.contains_slot(slot))
            .cloned()
            .collect();
        if write || !self.read_from_replicas {
            matches.retain(|node| !node.is_replica);
        }
        matches
    }
}

impl Topology for StaticTopology {
    fn mode(&self) -> ServerMode {
        self.mode
    }

    fn nodes(&self) -> Vec<ClusterNode> {
        self.nodes.clone()
    }

    fn select_node(&self, key: &str, write: bool) -> Option<ClusterNode> {
        let slot = hash_slot(key);
        self.candidates_for_slot(slot, write)
            .into_iter()
            .next()
            .or_else(|| {
                self.nodes
                    .iter()
                    .find(|node| node.link_up && node.contains_slot(slot))
                    .cloned()
            })
    }

    fn reselect_node(&self, key: &str, write: bool, current: &ClusterNode) -> Option<ClusterNode> {
        let slot = hash_slot(key);
        self.candidates_for_slot(slot, write)
            .into_iter()
            .find(|node| node.endpoint != current.endpoint)
            .or_else(|| {
                self.nodes
                    .iter()
                    .find(|node| {
                        node.link_up
                            && node.contains_slot(slot)
                            && node.endpoint != current.endpoint
                    })
                    .cloned()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::SlotRange;

    const CLUSTER_NODES: &str = "master-a 10.0.0.1:6379@16379 master - 0 0 1 connected 0-5460\nreplica-a 10.0.0.2:6379@16379 slave master-a 0 0 1 connected 0-5460\nmaster-b 10.0.0.3:6379@16379 master - 0 0 2 connected 5461-10922\nreplica-b 10.0.0.4:6379@16379 slave master-b 0 0 2 connected 5461-10922\nmaster-c 10.0.0.5:6379@16379 master - 0 0 3 connected 10923-16383";

    #[test]
    fn static_topology_prefers_primary_for_write() {
        let slot = hash_slot("{order}:1");
        let mut primary = ClusterNode::new("10.0.0.1:6379");
        primary.slots.push(SlotRange::new(slot, slot));

        let mut replica = ClusterNode::new("10.0.0.2:6379");
        replica.is_replica = true;
        replica.slots.push(SlotRange::new(slot, slot));

        let topology =
            StaticTopology::new(ServerMode::Cluster, vec![replica, primary.clone()], true);
        let selected = topology.select_node("{order}:1", true).unwrap();
        assert_eq!(selected.endpoint, primary.endpoint);
    }

    #[test]
    fn static_topology_can_read_from_replica() {
        let slot = hash_slot("{order}:1");
        let mut primary = ClusterNode::new("10.0.0.1:6379");
        primary.slots.push(SlotRange::new(slot, slot));

        let mut replica = ClusterNode::new("10.0.0.2:6379");
        replica.is_replica = true;
        replica.slots.push(SlotRange::new(slot, slot));

        let topology =
            StaticTopology::new(ServerMode::Cluster, vec![replica.clone(), primary], true);
        let selected = topology.select_node("{order}:1", false).unwrap();
        assert_eq!(selected.endpoint, replica.endpoint);
    }

    #[test]
    fn cluster_topology_parses_snapshot_and_selects_primary() {
        let topology = RedisClusterTopology::from_cluster_nodes(CLUSTER_NODES, false);
        assert_eq!(topology.nodes().len(), 5);

        let selected = topology.select_node("alpha", true).unwrap();
        assert_eq!(selected.endpoint, "10.0.0.1:6379");
        assert!(!selected.is_replica);
    }

    #[test]
    fn cluster_topology_prefers_replica_for_reads_when_enabled() {
        let topology = RedisClusterTopology::from_cluster_nodes(CLUSTER_NODES, true);
        let selected = topology.select_node("alpha", false).unwrap();
        assert_eq!(selected.endpoint, "10.0.0.2:6379");
        assert!(selected.is_replica);
    }

    #[test]
    fn cluster_topology_can_reselect_other_node_in_same_slot() {
        let topology = RedisClusterTopology::from_cluster_nodes(CLUSTER_NODES, true);
        let current = topology.select_node("alpha", true).unwrap();
        let next = topology.reselect_node("alpha", false, &current).unwrap();

        assert_ne!(next.endpoint, current.endpoint);
        assert!(next.is_replica);
    }

    #[test]
    fn cluster_topology_maps_known_endpoint() {
        let topology = RedisClusterTopology::from_cluster_nodes(CLUSTER_NODES, false);
        let mapped = topology.map_endpoint("10.0.0.3:6379").unwrap();
        assert_eq!(mapped.endpoint, "10.0.0.3:6379");
    }

    #[test]
    fn cluster_topology_remembers_moved_slot_override() {
        let topology = RedisClusterTopology::from_cluster_nodes(CLUSTER_NODES, false);
        let key = "alpha";
        let slot = hash_slot(key);

        topology.remember_redirect(slot, "10.0.0.5:6379", true);
        let redirected = topology.select_node(key, true).unwrap();

        assert_eq!(redirected.endpoint, "10.0.0.5:6379");
        assert!(redirected.contains_slot(slot));
    }

    #[test]
    fn replication_topology_reselects_to_master_when_replica_read_fails() {
        let master = ClusterNode::new("127.0.0.1:6379");

        let mut replica = ClusterNode::new("127.0.0.1:6380");
        replica.is_replica = true;

        let topology = RedisReplicationTopology::new(
            ServerMode::Replication,
            vec![master.clone(), replica.clone()],
            true,
        );

        let current = topology.select_node("any-key", false).unwrap();
        assert_eq!(current.endpoint, replica.endpoint);

        let next = topology.reselect_node("any-key", false, &current).unwrap();
        assert_eq!(next.endpoint, master.endpoint);

        let selected = topology.select_node("any-key", false).unwrap();
        assert_eq!(selected.endpoint, master.endpoint);

        topology.reset_node(&replica.endpoint);
        let selected = topology.select_node("any-key", false).unwrap();
        assert_eq!(selected.endpoint, replica.endpoint);
    }
}
