//! 队列公共设置（对应 DH.NRedis `QueueBase` 的属性）。

/// 队列公共设置。字段含义与 C# `QueueBase` 一致。
#[derive(Debug, Clone)]
pub struct QueueSettings {
    /// 追踪标识注入 `(参数名, 追踪值)`。
    ///
    /// 对应 C# `Redis.Tracer.AttachParameter` + `DefaultSpan.Current`：
    /// 仅当消息编码结果是 JSON 对象文本时，在尾部注入 `"参数名":"追踪值"`。
    /// 默认 `None`（与 C# 未配置 Tracer 时相同，不修改消息）。
    pub trace: Option<(String, String)>,

    /// 发送失败重试次数（失败包含被代理吞掉的返回 0）。默认 3
    pub retry_times_when_send_failed: usize,

    /// 发送失败重试间隔（毫秒）。默认 1000
    pub retry_interval_ms: u64,

    /// 最终失败时是否抛出异常。默认 false（与 C# 一致，仅记录）
    pub throw_on_failure: bool,
}

impl Default for QueueSettings {
    fn default() -> Self {
        Self {
            trace: None,
            retry_times_when_send_failed: 3,
            retry_interval_ms: 1000,
            throw_on_failure: false,
        }
    }
}

impl QueueSettings {
    /// 向编码后的消息注入追踪标识（复刻 C# `RedisHelper.AttachTraceId` 的字符串分支）。
    pub(crate) fn attach_trace(&self, payload: Vec<u8>) -> Vec<u8> {
        let Some((name, value)) = &self.trace else {
            return payload;
        };

        let Ok(text) = std::str::from_utf8(&payload) else {
            return payload;
        };
        if !text.starts_with('{') || !text.ends_with('}') {
            return payload;
        }

        let needle = format!("\"{name}\":");
        if !text
            .to_ascii_lowercase()
            .contains(&needle.to_ascii_lowercase())
        {
            let mut out = String::with_capacity(text.len() + name.len() + value.len() + 8);
            out.push_str(&text[..text.len() - 1]);
            out.push_str(&format!(",\"{name}\":\"{value}\"}}"));
            return out.into_bytes();
        }

        let empty_tail = format!(",\"{name}\":null}}");
        if text
            .to_ascii_lowercase()
            .ends_with(&empty_tail.to_ascii_lowercase())
        {
            let keep = text.len() - empty_tail.len();
            let mut out = String::with_capacity(text.len() + value.len());
            out.push_str(&text[..keep]);
            out.push_str(&format!(",\"{name}\":\"{value}\"}}"));
            return out.into_bytes();
        }

        payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_trace_keeps_payload() {
        let s = QueueSettings::default();
        assert_eq!(s.attach_trace(b"{\"a\":1}".to_vec()), b"{\"a\":1}".to_vec());
    }

    #[test]
    fn trace_appends_to_json_object() {
        let s = QueueSettings {
            trace: Some(("traceId".into(), "abc".into())),
            ..Default::default()
        };
        let out = s.attach_trace(br#"{"Name":"NewLife"}"#.to_vec());
        assert_eq!(
            String::from_utf8(out).unwrap(),
            r#"{"Name":"NewLife","traceId":"abc"}"#
        );
    }

    #[test]
    fn trace_replaces_null_slot() {
        let s = QueueSettings {
            trace: Some(("traceId".into(), "abc".into())),
            ..Default::default()
        };
        let out = s.attach_trace(br#"{"a":1,"traceId":null}"#.to_vec());
        assert_eq!(
            String::from_utf8(out).unwrap(),
            r#"{"a":1,"traceId":"abc"}"#
        );
    }

    #[test]
    fn trace_skips_non_json() {
        let s = QueueSettings {
            trace: Some(("traceId".into(), "abc".into())),
            ..Default::default()
        };
        assert_eq!(s.attach_trace(b"hello".to_vec()), b"hello".to_vec());
    }
}
