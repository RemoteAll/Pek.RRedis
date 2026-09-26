//! 与 DH.NRedis 完全对齐的值编码器。
//!
//! C# 端 `DefaultPacketEncoder` / `RedisJsonEncoder` 决定了对象在 Redis 中的字节格式，
//! 本模块逐条复刻其规则，确保 **C# 写入的数据 Rust 能读，Rust 写入的数据 C# 能读**：
//!
//! | 值类型 | C# 编码结果 | Rust 对应 |
//! |--------|-------------|-----------|
//! | `null` | 空（不写入） | `None` |
//! | `String` | 原始 UTF-8 字节（**无引号**） | [`str`] / [`String`] |
//! | `Byte[]` / `IPacket` | 原始二进制 | [`Vec<u8>`] / `&[u8]` |
//! | `Boolean` | `"True"` / `"False"` | [`bool`] |
//! | 整数 | `ToString()`，如 `"123"` | 各整数类型 |
//! | 浮点/`Decimal` | 往返最短表示，如 `"1.5"` | [`f32`] / [`f64`] |
//! | `DateTime` | `"yyyy-MM-dd HH:mm:ss.fff"` | [`chrono::NaiveDateTime`] 等 |
//! | 复杂对象 | JSON（System.Text.Json / FastJson） | [`Json<T>`] |
//!
//! 解码方向兼容 C# 的宽松转换：`"OK"`/`"1"`/`"true"` 均可转 `bool`；
//! 时间同时接受 NewLife 文本格式与 ISO 8601（System.Text.Json 输出格式）。

use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Utc};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

/// 可编码为 Redis 载荷（与 C# `IPacketEncoder.Encode` 对齐）。
pub trait ToRedisPayload {
    /// 编码。返回 `None` 表示空值（对应 C# 编码 `null` 得到空数据包）。
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>>;
}

/// 可从 Redis 载荷解码（与 C# `IPacketEncoder.Decode` 对齐）。
pub trait FromRedisPayload: Sized {
    /// 解码。
    fn from_redis_payload(payload: &[u8]) -> Result<Self>;
}

/// JSON 载荷包装器：复杂对象以 JSON 存储（对应 C# 的复杂类型分支）。
///
/// ```no_run
/// use pek_rredis::encoder::Json;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// #[serde(rename_all = "PascalCase")] // 与 C# 属性名对齐
/// struct User { name: String, create_time: chrono::NaiveDateTime }
///
/// # fn main() -> pek_rredis::Result<()> {
/// let user = User { name: "NewLife".into(), create_time: chrono::Local::now().naive_local() };
/// // redis.set("user", Json(&user), 3600)?;
/// // let user2: Json<User> = redis.get("user")?.unwrap();
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Json<T>(pub T);

impl<T: Serialize> ToRedisPayload for Json<T> {
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
        Ok(Some(serde_json::to_vec(&self.0)?))
    }
}

impl<T: DeserializeOwned> FromRedisPayload for Json<T> {
    fn from_redis_payload(payload: &[u8]) -> Result<Self> {
        let value = serde_json::from_slice(strip_bom(payload))?;
        Ok(Json(value))
    }
}

impl<T: ToRedisPayload + ?Sized> ToRedisPayload for &T {
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
        (*self).to_redis_payload()
    }
}

impl<T: ToRedisPayload> ToRedisPayload for Option<T> {
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
        match self {
            Some(v) => v.to_redis_payload(),
            None => Ok(None),
        }
    }
}

impl ToRedisPayload for str {
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
        Ok(Some(self.as_bytes().to_vec()))
    }
}

impl ToRedisPayload for String {
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
        Ok(Some(self.as_bytes().to_vec()))
    }
}

impl ToRedisPayload for [u8] {
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
        Ok(Some(self.to_vec()))
    }
}

impl ToRedisPayload for Vec<u8> {
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
        Ok(Some(self.clone()))
    }
}

impl ToRedisPayload for bool {
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
        // 与 C# Boolean.ToString() 一致
        Ok(Some(if *self { b"True".to_vec() } else { b"False".to_vec() }))
    }
}

macro_rules! impl_to_payload_display {
    ($($t:ty),*) => {
        $(
            impl ToRedisPayload for $t {
                fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
                    Ok(Some(self.to_string().into_bytes()))
                }
            }
        )*
    };
}

impl_to_payload_display!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);

impl ToRedisPayload for f32 {
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
        Ok(Some(format_f64(*self as f64).into_bytes()))
    }
}

impl ToRedisPayload for f64 {
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
        Ok(Some(format_f64(*self).into_bytes()))
    }
}

impl ToRedisPayload for NaiveDateTime {
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
        Ok(Some(format_datetime(self).into_bytes()))
    }
}

impl ToRedisPayload for DateTime<Utc> {
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
        Ok(Some(format_datetime(&self.naive_utc()).into_bytes()))
    }
}

impl ToRedisPayload for DateTime<Local> {
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
        Ok(Some(format_datetime(&self.naive_local()).into_bytes()))
    }
}

impl ToRedisPayload for serde_json::Value {
    fn to_redis_payload(&self) -> Result<Option<Vec<u8>>> {
        Ok(Some(serde_json::to_vec(self)?))
    }
}

impl FromRedisPayload for String {
    fn from_redis_payload(payload: &[u8]) -> Result<Self> {
        Ok(String::from_utf8_lossy(payload).into_owned())
    }
}

impl FromRedisPayload for Vec<u8> {
    fn from_redis_payload(payload: &[u8]) -> Result<Self> {
        Ok(payload.to_vec())
    }
}

impl FromRedisPayload for bool {
    fn from_redis_payload(payload: &[u8]) -> Result<Self> {
        let s = utf8(payload)?;
        parse_bool(s).ok_or_else(|| Error::Type(format!("无法把 {s:?} 转换为布尔值")))
    }
}

macro_rules! impl_from_payload_parse {
    ($($t:ty),*) => {
        $(
            impl FromRedisPayload for $t {
                fn from_redis_payload(payload: &[u8]) -> Result<Self> {
                    let s = utf8(payload)?;
                    s.trim().parse().map_err(|_| {
                        Error::Type(format!("无法把 {s:?} 转换为 {}", stringify!($t)))
                    })
                }
            }
        )*
    };
}

impl_from_payload_parse!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);

impl FromRedisPayload for f32 {
    fn from_redis_payload(payload: &[u8]) -> Result<Self> {
        Ok(parse_f64(utf8(payload)?)? as f32)
    }
}

impl FromRedisPayload for f64 {
    fn from_redis_payload(payload: &[u8]) -> Result<Self> {
        parse_f64(utf8(payload)?)
    }
}

impl FromRedisPayload for NaiveDateTime {
    fn from_redis_payload(payload: &[u8]) -> Result<Self> {
        parse_datetime(utf8(payload)?)
    }
}

impl FromRedisPayload for DateTime<Utc> {
    fn from_redis_payload(payload: &[u8]) -> Result<Self> {
        let s = utf8(payload)?.trim();
        // 优先按带时区偏移的 ISO 8601 解析，保证 UTC 语义正确
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Ok(dt.with_timezone(&Utc));
        }

        let naive = parse_datetime(s)?;
        Ok(Utc.from_utc_datetime(&naive))
    }
}

impl FromRedisPayload for DateTime<Local> {
    fn from_redis_payload(payload: &[u8]) -> Result<Self> {
        let naive = parse_datetime(utf8(payload)?)?;
        Local
            .from_local_datetime(&naive)
            .single()
            .ok_or_else(|| Error::Type(format!("本地时间无法解析：{naive}")))
    }
}

impl FromRedisPayload for serde_json::Value {
    fn from_redis_payload(payload: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(strip_bom(payload))?)
    }
}

/// JSON 中时间字段的宽容序列化模块。
///
/// chrono 的 serde 默认实现**只能读 ISO 8601**；而 C# 侧 FastJson 会把
/// `DateTime` 写成 `"2026-09-26 10:00:00"`（NewLife 文本格式，且不含毫秒）。
/// 给结构体字段加上本模块即可双向互通：
///
/// ```no_run
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// #[serde(rename_all = "PascalCase")]
/// struct User {
///     name: String,
///     #[serde(with = "pek_rredis::encoder::datetime")]
///     create_time: chrono::NaiveDateTime,
/// }
/// ```
///
/// - 写入：ISO 8601（`2026-09-26T10:00:00`），与 System.Text.Json 一致，C# 两种 JsonHost 都能读；
/// - 读取：ISO 8601、带时区偏移、NewLife 文本（`yyyy-MM-dd HH:mm:ss[.fff]`）均可。
pub mod datetime {
    use chrono::NaiveDateTime;
    use serde::{Deserialize, Deserializer, Serializer};

    /// 序列化为 ISO 8601 文本。
    pub fn serialize<S>(value: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.format("%Y-%m-%dT%H:%M:%S%.f").to_string())
    }

    /// 反序列化，兼容 ISO 8601 与 NewLife 文本格式。
    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        super::parse_datetime(&text).map_err(serde::de::Error::custom)
    }
}

/// Stream/字段路径的「编码器文本」时间模块。
///
/// C# 把对象属性写入 Stream 字段或队列消息时走的是 `DefaultPacketEncoder`，
/// `DateTime` 输出 `yyyy-MM-dd HH:mm:ss.fff`（与 JSON 路径不同）。
/// 给 Stream 对象消息的结构体时间字段加本模块，写入即可与 C# 逐字节一致：
///
/// ```no_run
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// #[serde(rename_all = "PascalCase")]
/// struct Order {
///     code: String,
///     #[serde(with = "pek_rredis::encoder::datetime_text")]
///     create_time: chrono::NaiveDateTime,
/// }
/// ```
///
/// - 写入：`2026-09-26 10:00:00.123`（与 C# 编码器完全一致）；
/// - 读取：兼容本格式、ISO 8601 与带时区偏移。
pub mod datetime_text {
    use chrono::NaiveDateTime;
    use serde::{Deserialize, Deserializer, Serializer};

    /// 序列化为 `yyyy-MM-dd HH:mm:ss.fff`。
    pub fn serialize<S>(value: &NaiveDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&super::format_datetime(value))
    }

    /// 反序列化，兼容编码器文本与 ISO 8601。
    pub fn deserialize<'de, D>(deserializer: D) -> Result<NaiveDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        super::parse_datetime(&text).map_err(serde::de::Error::custom)
    }
}

/// 格式化为 C# `DateTime.ToString("yyyy-MM-dd HH:mm:ss.fff")` 形式。
pub fn format_datetime(dt: &NaiveDateTime) -> String {
    dt.format("%Y-%m-%d %H:%M:%S.%3f").to_string()
}

/// 浮点格式化：与 .NET Core 的往返最短表示行为对齐（`0.1` 而非 `0.10000000000000001`）。
pub fn format_f64(v: f64) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Infinity".into() } else { "-Infinity".into() };
    }
    let s = format!("{v}");
    if s.contains(['e', 'E']) {
        s
    } else if v == v.trunc() && v.abs() < 1e16 {
        // 整数值输出为 "1" 而不是 "1.0"，与 C# 一致
        s
    } else {
        s
    }
}

/// 严格（不变文化）浮点解析，容忍空白与 `Infinity`/`NaN`。
pub fn parse_f64(s: &str) -> Result<f64> {
    let s = s.trim();
    match s {
        "Infinity" | "inf" | "+inf" => return Ok(f64::INFINITY),
        "-Infinity" | "-inf" => return Ok(f64::NEG_INFINITY),
        "NaN" | "nan" => return Ok(f64::NAN),
        _ => {}
    }
    s.parse::<f64>()
        .map_err(|_| Error::Type(format!("无法把 {s:?} 转换为浮点数")))
}

/// 宽松布尔解析，与 C# `ChangeType` + `RedisJsonEncoder` 的特殊 `OK` 规则一致。
pub fn parse_bool(s: &str) -> Option<bool> {
    match s.trim() {
        "OK" | "ok" | "true" | "True" | "TRUE" | "1" => Some(true),
        "false" | "False" | "FALSE" | "0" | "" => Some(false),
        _ => None,
    }
}

/// 时间解析：同时接受 NewLife 文本格式与 ISO 8601。
///
/// - `2026-09-26 10:00:00` / `2026-09-26 10:00:00.123`（C# 编码器输出）
/// - `2026-09-26T10:00:00` / `2026-09-26T10:00:00.123456` / `2026-09-26T10:00:00Z` / 带偏移
pub fn parse_datetime(s: &str) -> Result<NaiveDateTime> {
    let s = s.trim();
    if s.is_empty() {
        return Err(Error::Type("时间为空".into()));
    }

    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Ok(dt);
        }
        if fmt == "%Y-%m-%d"
            && let Ok(d) = chrono::NaiveDate::parse_from_str(s, fmt) {
                return Ok(d.and_hms_opt(0, 0, 0).unwrap());
            }
    }

    // 带时区偏移的 ISO 8601：换算成本机时区的朴素时间，便于与 DateTime.Now 语义对齐
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        let utc = dt.with_timezone(&Utc);
        return Ok(utc.with_timezone(&Local).naive_local());
    }

    Err(Error::Type(format!("无法解析时间：{s}")))
}

/// 获取 UTF-8 视图（严格模式，非法字节报类型错误）。
fn utf8(payload: &[u8]) -> Result<&str> {
    std::str::from_utf8(payload).map_err(|e| Error::Type(format!("数据不是合法 UTF-8：{e}")))
}

/// 去掉 UTF-8 BOM（.NET 某些序列化输出可能带）。
fn strip_bom(payload: &[u8]) -> &[u8] {
    payload.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(payload)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    fn payload_of<T: ToRedisPayload>(v: T) -> String {
        String::from_utf8(v.to_redis_payload().unwrap().unwrap()).unwrap()
    }

    #[test]
    fn primitive_payloads_match_csharp() {
        // C# 端 DefaultPacketEncoder.OnEncode 的分支结果
        assert_eq!(payload_of("hello"), "hello"); // 字符串无引号
        assert_eq!(payload_of(true), "True");
        assert_eq!(payload_of(false), "False");
        assert_eq!(payload_of(123_i32), "123");
        assert_eq!(payload_of(-5_i64), "-5");
        assert_eq!(payload_of(1.5_f64), "1.5");
        assert_eq!(payload_of(0.1_f64), "0.1");
        assert_eq!(payload_of(12_u8), "12");
        assert_eq!(payload_of(vec![1u8, 2, 3]), "\u{1}\u{2}\u{3}");
    }

    #[test]
    fn datetime_payload_matches_csharp_fff_format() {
        let dt = NaiveDateTime::parse_from_str("2026-09-26 10:00:00.123456", "%Y-%m-%d %H:%M:%S%.f")
            .unwrap();
        assert_eq!(payload_of(dt), "2026-09-26 10:00:00.123");

        let dt2 = NaiveDateTime::parse_from_str("2026-09-26 10:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(payload_of(dt2), "2026-09-26 10:00:00.000");
    }

    #[test]
    fn json_payload_is_compact() {
        #[derive(Serialize)]
        #[serde(rename_all = "PascalCase")]
        struct User {
            name: String,
            create_time: NaiveDateTime,
        }

        let user = User {
            name: "NewLife".into(),
            create_time: NaiveDateTime::parse_from_str("2026-09-26 10:00:00", "%Y-%m-%d %H:%M:%S")
                .unwrap(),
        };
        // 属性名保持 C# 风格，时间输出 ISO 8601（与 System.Text.Json 一致）
        assert_eq!(
            payload_of(Json(&user)),
            r#"{"Name":"NewLife","CreateTime":"2026-09-26T10:00:00"}"#
        );
    }

    #[test]
    fn decode_csharp_style_scalars() {
        assert!(<bool as FromRedisPayload>::from_redis_payload(b"True").unwrap());
        assert!(<bool as FromRedisPayload>::from_redis_payload(b"OK").unwrap());
        assert!(<bool as FromRedisPayload>::from_redis_payload(b"1").unwrap());
        assert!(!<bool as FromRedisPayload>::from_redis_payload(b"0").unwrap());
        assert_eq!(<i32 as FromRedisPayload>::from_redis_payload(b"42").unwrap(), 42);
        assert_eq!(<f64 as FromRedisPayload>::from_redis_payload(b"1.5").unwrap(), 1.5);
        assert_eq!(
            <String as FromRedisPayload>::from_redis_payload("中文字符".as_bytes()).unwrap(),
            "中文字符"
        );
    }

    #[test]
    fn decode_both_datetime_styles() {
        let a = <NaiveDateTime as FromRedisPayload>::from_redis_payload(b"2026-09-26 10:00:00.123")
            .unwrap();
        let b = <NaiveDateTime as FromRedisPayload>::from_redis_payload(b"2026-09-26T10:00:00.123")
            .unwrap();
        let c = <NaiveDateTime as FromRedisPayload>::from_redis_payload(b"2026-09-26").unwrap();
        assert_eq!(a, b);
        assert_eq!(c.date().to_string(), "2026-09-26");

        let z = <DateTime<Utc> as FromRedisPayload>::from_redis_payload(b"2026-09-26T10:00:00Z")
            .unwrap();
        assert_eq!(z.naive_utc().to_string(), "2026-09-26 10:00:00");
    }

    #[test]
    fn decode_json_from_csharp_system_text_json() {
        #[derive(Deserialize, Debug, PartialEq)]
        #[serde(rename_all = "PascalCase")]
        struct User {
            name: String,
            create_time: NaiveDateTime,
        }

        let json = br#"{"Name":"NewLife","CreateTime":"2026-09-26T10:00:00"}"#;
        let user: Json<User> = Json::from_redis_payload(json).unwrap();
        assert_eq!(user.0.name, "NewLife");
        assert_eq!(user.0.create_time.to_string(), "2026-09-26 10:00:00");
    }

    #[test]
    fn datetime_serde_module_accepts_newlife_text_and_iso() {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct Row {
            #[serde(with = "crate::encoder::datetime")]
            t: NaiveDateTime,
        }

        // C# FastJson 输出（文本格式、无毫秒）
        let row: Row = serde_json::from_str(r#"{"t":"2026-09-26 10:00:00"}"#).unwrap();
        assert_eq!(row.t.to_string(), "2026-09-26 10:00:00");

        // C# System.Text.Json / Rust 输出（ISO 8601）
        let row2: Row = serde_json::from_str(r#"{"t":"2026-09-26T10:00:00.123"}"#).unwrap();
        assert_eq!(row2.t.to_string(), "2026-09-26 10:00:00.123");

        // 带时区偏移也要能读（换算为本机时区，因此断言用同一算法推导期望值）
        let row3: Row = serde_json::from_str(r#"{"t":"2026-09-26T10:00:00+08:00"}"#).unwrap();
        let expected = DateTime::parse_from_rfc3339("2026-09-26T10:00:00+08:00")
            .unwrap()
            .with_timezone(&Local)
            .naive_local();
        assert_eq!(row3.t, expected);

        // 写回为 ISO（C# 两种 JsonHost 都能读）
        let text = serde_json::to_string(&row).unwrap();
        assert_eq!(text, r#"{"t":"2026-09-26T10:00:00"}"#);
    }

    #[test]
    fn f64_roundtrip_is_stable() {
        for v in [0.5_f64, 1.25, -3.75, 100.0, 1e-9, 1234.5678] {
            let s = payload_of(v);
            let back = <f64 as FromRedisPayload>::from_redis_payload(s.as_bytes()).unwrap();
            assert_eq!(back, v, "值 {v} 经 {s:?} 往返不一致");
        }
    }
}
