//! 队列状态（对应 DH.NRedis `RedisQueueStatus`）。
//!
//! JSON 字段名与 C# 一致（PascalCase），时间字段兼容 System.Text.Json 的 ISO 8601 输出
//! （含时区偏移）与 NewLife 文本格式，保证 C# 端读取本端写入的状态、反之亦然。

use chrono::NaiveDateTime;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// 队列消费者状态。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase", default)]
pub struct RedisQueueStatus {
    /// 标识消费者的唯一键
    pub key: String,
    /// 机器名
    #[serde(alias = "machineName")]
    pub machine_name: Option<String>,
    /// 用户名
    #[serde(alias = "userName")]
    pub user_name: Option<String>,
    /// 进程 ID
    #[serde(alias = "processId", alias = "Pid")]
    pub process_id: i32,
    /// IP 地址
    #[serde(alias = "ip", alias = "IP")]
    pub ip: Option<String>,
    /// 开始时间
    #[serde(alias = "createTime", with = "flex_datetime")]
    pub create_time: NaiveDateTime,
    /// 最后活跃时间
    #[serde(alias = "lastActive", with = "flex_datetime")]
    pub last_active: NaiveDateTime,
    /// 消费消息数
    pub consumes: i64,
    /// 确认消息数
    pub acks: i64,
}

impl Default for RedisQueueStatus {
    fn default() -> Self {
        let now = chrono::Local::now().naive_local();
        Self {
            key: String::new(),
            machine_name: machine_name(),
            user_name: user_name(),
            process_id: std::process::id() as i32,
            ip: None,
            create_time: now,
            last_active: now,
            consumes: 0,
            acks: 0,
        }
    }
}

impl RedisQueueStatus {
    /// 序列化为 JSON 文本（写入 `{key}:Status:{ukey}`）。
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    /// 解析 JSON 文本（兼容 C# 输出）。
    pub fn from_json(text: &str) -> Option<Self> {
        serde_json::from_str(text)
            .ok()
            .or_else(|| serde_json::from_str(text.trim_matches('\0')).ok())
    }
}

/// 本机名（与 C# `Environment.MachineName` 对应）。
pub fn machine_name() -> Option<String> {
    std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .ok()
        .filter(|s| !s.is_empty())
}

/// 当前用户名（与 C# `Environment.UserName` 对应）。
pub fn user_name() -> Option<String> {
    std::env::var("USERNAME")
        .or_else(|_| std::env::var("USER"))
        .ok()
        .filter(|s| !s.is_empty())
}

/// 生成 8 位随机消费标识（与 C# `Rand.NextString(8)` 用途一致，具体字符集不参与互通）。
pub fn new_consumer_key() -> String {
    use rand::Rng;
    const CHARS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut rng = rand::thread_rng();
    (0..8)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}

mod flex_datetime {
    use super::*;

    pub fn serialize<S>(dt: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format_iso(dt))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        crate::encoder::parse_datetime(&text).map_err(serde::de::Error::custom)
    }

    /// System.Text.Json 风格：无小数秒时省略秒的小数部分。
    fn format_iso(dt: &NaiveDateTime) -> String {
        use chrono::Timelike;
        if dt.nanosecond() == 0 {
            dt.format("%Y-%m-%dT%H:%M:%S").to_string()
        } else {
            dt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_csharp_pascal_case_json() {
        // C# System.Text.Json 输出（DateTime 为 Local 时带偏移）
        let json = r#"{"Key":"abc12345","MachineName":"DEV-PC","UserName":"qcjxb","ProcessId":4321,"Ip":"192.168.1.10","CreateTime":"2026-09-26T10:00:00+08:00","LastActive":"2026-09-26T10:05:00+08:00","Consumes":12,"Acks":11}"#;
        let status = RedisQueueStatus::from_json(json).unwrap();
        assert_eq!(status.key, "abc12345");
        assert_eq!(status.process_id, 4321);
        assert_eq!(status.consumes, 12);
        assert_eq!(status.create_time.to_string(), "2026-09-26 10:00:00");
    }

    #[test]
    fn parses_newlife_fastjson_datetime() {
        let json = r#"{"Key":"abc","MachineName":null,"UserName":null,"ProcessId":1,"Ip":null,"CreateTime":"2026-09-26 10:00:00","LastActive":"2026-09-26 10:00:00.123","Consumes":0,"Acks":0}"#;
        let status = RedisQueueStatus::from_json(json).unwrap();
        assert_eq!(status.last_active.to_string(), "2026-09-26 10:00:00.123");
    }

    #[test]
    fn roundtrips_to_pascal_case_iso_json() {
        let status = RedisQueueStatus {
            key: "k1".into(),
            machine_name: Some("PC".into()),
            create_time: chrono::NaiveDateTime::parse_from_str(
                "2026-09-26 10:00:00",
                "%Y-%m-%d %H:%M:%S",
            )
            .unwrap(),
            ..Default::default()
        };
        let json = status.to_json();
        assert!(json.contains(r#""Key":"k1""#));
        assert!(json.contains(r#""CreateTime":"2026-09-26T10:00:00""#));
        // C# 端可读
        let back = RedisQueueStatus::from_json(&json).unwrap();
        assert_eq!(back.key, "k1");
    }

    #[test]
    fn missing_fields_use_defaults() {
        let status = RedisQueueStatus::from_json(r#"{"Key":"only"}"#).unwrap();
        assert_eq!(status.key, "only");
        assert_eq!(status.consumes, 0);
    }

    #[test]
    fn consumer_key_is_8_chars() {
        let key = new_consumer_key();
        assert_eq!(key.len(), 8);
    }
}
