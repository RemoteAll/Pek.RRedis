/// 槽位区间。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotRange {
    pub from: u16,
    pub to: u16,
}

impl SlotRange {
    /// 创建槽位区间；若 `from > to` 会自动交换。
    pub fn new(from: u16, to: u16) -> Self {
        if from <= to {
            Self { from, to }
        } else {
            Self { from: to, to: from }
        }
    }

    /// 是否包含指定槽位。
    pub fn contains(&self, slot: u16) -> bool {
        slot >= self.from && slot <= self.to
    }
}

/// Cluster 节点迁移方向。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotMigrationKind {
    Importing,
    Migrating,
}

/// Cluster 节点迁移信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotMigration {
    pub slot: u16,
    pub node_id: String,
    pub kind: SlotMigrationKind,
}

/// 集群/主从/哨兵统一节点模型。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNode {
    pub endpoint: String,
    pub node_id: Option<String>,
    pub flags: Vec<String>,
    pub master_id: Option<String>,
    pub is_replica: bool,
    pub link_up: bool,
    pub slots: Vec<SlotRange>,
    pub migrations: Vec<SlotMigration>,
}

impl ClusterNode {
    /// 创建最小节点模型。
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            node_id: None,
            flags: Vec::new(),
            master_id: None,
            is_replica: false,
            link_up: true,
            slots: Vec::new(),
            migrations: Vec::new(),
        }
    }

    /// 当前节点是否覆盖指定槽位。
    pub fn contains_slot(&self, slot: u16) -> bool {
        self.slots.iter().any(|range| range.contains(slot))
    }

    /// 从 `CLUSTER NODES` 的单行文本解析节点。
    pub fn parse_cluster_nodes_line(line: &str) -> Option<Self> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 8 {
            return None;
        }

        let endpoint = parts[1].split('@').next().unwrap_or(parts[1]).to_string();
        let flags: Vec<String> = parts[2].split(',').map(|flag| flag.to_string()).collect();
        let master = parts[3];

        let mut node = Self {
            endpoint,
            node_id: Some(parts[0].to_string()),
            flags: flags.clone(),
            master_id: if master == "-" {
                None
            } else {
                Some(master.to_string())
            },
            is_replica: flags.iter().any(|flag| flag.eq_ignore_ascii_case("slave") || flag.eq_ignore_ascii_case("replica")),
            link_up: !flags.iter().any(|flag| flag.eq_ignore_ascii_case("fail?")),
            slots: Vec::new(),
            migrations: Vec::new(),
        };

        for part in &parts[8..] {
            if let Some(migration) = parse_migration(part) {
                node.migrations.push(migration);
                continue;
            }
            if let Some(range) = parse_slot_range(part) {
                node.slots.push(range);
            }
        }

        Some(node)
    }
}

fn parse_slot_range(text: &str) -> Option<SlotRange> {
    if text.starts_with('[') {
        return None;
    }
    if let Some((from, to)) = text.split_once('-') {
        Some(SlotRange::new(from.parse().ok()?, to.parse().ok()?))
    } else {
        let slot: u16 = text.parse().ok()?;
        Some(SlotRange::new(slot, slot))
    }
}

fn parse_migration(text: &str) -> Option<SlotMigration> {
    let text = text.strip_prefix('[')?.strip_suffix(']')?;
    if let Some((slot, node_id)) = text.split_once("-<-") {
        return Some(SlotMigration {
            slot: slot.parse().ok()?,
            node_id: node_id.to_string(),
            kind: SlotMigrationKind::Importing,
        });
    }
    if let Some((slot, node_id)) = text.split_once("->-") {
        return Some(SlotMigration {
            slot: slot.parse().ok()?,
            node_id: node_id.to_string(),
            kind: SlotMigrationKind::Migrating,
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cluster_node_line_with_ranges_and_single_slot() {
        let line = "7cf3c4e1a1c3a6bb52778bbfcc457ca1d9460de8 127.0.0.1:6001@16001 myself,master - 0 0 2 connected 1-4 103-105 107 109";
        let node = ClusterNode::parse_cluster_nodes_line(line).unwrap();

        assert_eq!(node.endpoint, "127.0.0.1:6001");
        assert!(!node.is_replica);
        assert!(node.link_up);
        assert!(node.contains_slot(1));
        assert!(node.contains_slot(104));
        assert!(node.contains_slot(107));
        assert!(!node.contains_slot(108));
    }

    #[test]
    fn parses_cluster_node_migrations() {
        let line = "abc 10.0.0.1:6379 master - 0 0 1 connected [123-<-srcnode] [456->-dstnode]";
        let node = ClusterNode::parse_cluster_nodes_line(line).unwrap();

        assert_eq!(node.migrations.len(), 2);
        assert_eq!(node.migrations[0].slot, 123);
        assert_eq!(node.migrations[0].kind, SlotMigrationKind::Importing);
        assert_eq!(node.migrations[1].slot, 456);
        assert_eq!(node.migrations[1].kind, SlotMigrationKind::Migrating);
    }

    #[test]
    fn replica_and_fail_flag_are_detected() {
        let line = "replica 10.0.0.2:6379@16379 slave,fail? masterid 0 0 2 connected 0-100";
        let node = ClusterNode::parse_cluster_nodes_line(line).unwrap();

        assert!(node.is_replica);
        assert!(!node.link_up);
        assert_eq!(node.master_id.as_deref(), Some("masterid"));
    }
}