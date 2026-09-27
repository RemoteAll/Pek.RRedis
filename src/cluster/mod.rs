mod node;
mod topology;

pub use node::{ClusterNode, SlotRange};
pub use topology::{RedisClusterTopology, RedisReplicationTopology, StaticTopology, Topology};

/// 提取 Redis Cluster hash tag。
///
/// - 若 key 中存在非空 `{...}`，则只对其中内容计算槽位；
/// - 否则对整个 key 计算槽位。
pub fn extract_hash_tag(key: &str) -> &str {
    let Some(start) = key.find('{') else {
        return key;
    };
    let rest = &key[start + 1..];
    let Some(end_rel) = rest.find('}') else {
        return key;
    };
    if end_rel == 0 {
        key
    } else {
        &rest[..end_rel]
    }
}

/// 计算 Redis Cluster 哈希槽位（`CRC16(tag) % 16384`）。
pub fn hash_slot(key: &str) -> u16 {
    crc16_xmodem(extract_hash_tag(key).as_bytes()) % 16_384
}

fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc = 0u16;
    for byte in data {
        crc ^= (*byte as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_hash_tag_when_present() {
        assert_eq!(extract_hash_tag("user:{1001}:name"), "1001");
    }

    #[test]
    fn empty_or_missing_tag_falls_back_to_full_key() {
        assert_eq!(extract_hash_tag("foo{}{bar}"), "foo{}{bar}");
        assert_eq!(extract_hash_tag("foo{bar"), "foo{bar");
        assert_eq!(extract_hash_tag("plain:key"), "plain:key");
    }

    #[test]
    fn same_hash_tag_maps_to_same_slot() {
        let a = hash_slot("{user1000}.following");
        let b = hash_slot("{user1000}.followers");
        let c = hash_slot("{user1001}.followers");

        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a < 16_384 && c < 16_384);
    }
}