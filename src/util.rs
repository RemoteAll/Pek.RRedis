//! 内部工具：结构体命令层的公共编解码小函数。

use crate::encoder::{FromRedisPayload, ToRedisPayload};
use crate::error::Result;
use crate::resp::RespValue;

/// 编码成员/字段/值，空值编码为空字节串（与 C# 编码 `null` 的行为一致）。
pub(crate) fn payload<T: ToRedisPayload + ?Sized>(value: &T) -> Result<Vec<u8>> {
    Ok(value.to_redis_payload()?.unwrap_or_default())
}

/// 从应答中解码一个值，失败返回 `None`（容错行为与 C# 编码器一致）。
pub(crate) fn decode<T: FromRedisPayload>(value: RespValue) -> Option<T> {
    if value.is_null() {
        return None;
    }
    value.as_bytes().and_then(|b| T::from_redis_payload(&b).ok())
}

/// 从字节解码。
pub(crate) fn decode_bytes<T: FromRedisPayload>(bytes: &[u8]) -> Option<T> {
    T::from_redis_payload(bytes).ok()
}

/// 应答转整数（缺失/非法时取默认值，与 C# `ToInt()` 一致）。
pub(crate) fn int_or(value: RespValue, default: i64) -> i64 {
    value.as_i64().unwrap_or(default)
}

/// 数组应答转值列表（跳过 null）。
pub(crate) fn decode_array<T: FromRedisPayload>(value: RespValue) -> Vec<T> {
    value
        .into_array()
        .unwrap_or_default()
        .into_iter()
        .filter_map(decode)
        .collect()
}

/// 扁平数组 `[k1, v1, k2, v2, ...]` 转键值对列表（含 null 值）。
pub(crate) fn decode_pairs<K: FromRedisPayload, V: FromRedisPayload>(
    value: RespValue,
) -> Vec<(K, Option<V>)> {
    let items = value.into_array().unwrap_or_default();
    let mut result = Vec::with_capacity(items.len() / 2);

    let mut iter = items.into_iter();
    while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
        if let Some(key) = decode::<K>(k) {
            let value = if v.is_null() { None } else { decode::<V>(v) };
            result.push((key, value));
        }
    }

    result
}

/// 扁平数组 `[m1, s1, m2, s2, ...]` 转「成员 + 分数」列表。
pub(crate) fn decode_scored<T: FromRedisPayload>(value: RespValue) -> Vec<(T, f64)> {
    let items = value.into_array().unwrap_or_default();
    let mut result = Vec::with_capacity(items.len() / 2);

    let mut iter = items.into_iter();
    while let (Some(m), Some(s)) = (iter.next(), iter.next()) {
        if let Some(member) = decode::<T>(m) {
            let score = s.as_f64().unwrap_or(0.0);
            result.push((member, score));
        }
    }

    result
}

/// 字符串数组转 `Vec<String>`。
pub(crate) fn strings(value: RespValue) -> Vec<String> {
    value
        .into_array()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_string())
        .collect()
}
