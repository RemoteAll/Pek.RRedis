//! RESP2 / RESP3 协议编解码。
//!
//! 对应 DH.NRedis 的 `RedisClient` 中的组包（`GetRequest`）与解析（`ParseResponse`）逻辑，
//! 支持 RESP2 全部类型与 RESP3 的 Map / Set / Double / Bool / Null / Verbatim / Push 等扩展类型，
//! 保证与 C# 端访问同一 Redis 实例时协议行为一致。

use std::io::BufRead;

use crate::error::{Error, Result};

/// RESP 值。RESP2 使用前 5 种，其余为 RESP3 扩展类型。
#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    /// 状态回复，如 `+OK`
    Simple(String),
    /// 错误回复，如 `-ERR unknown command`
    Error(String),
    /// 整数回复 `:123`
    Integer(i64),
    /// 二进制安全字符串 `$n`
    Bulk(Vec<u8>),
    /// 空值。RESP2 的 `$-1` / `*-1`，RESP3 的 `_`
    Null,
    /// 数组 `*n`
    Array(Vec<RespValue>),
    /// RESP3 映射 `%n`
    Map(Vec<(RespValue, RespValue)>),
    /// RESP3 集合 `~n`
    Set(Vec<RespValue>),
    /// RESP3 双精度浮点 `,`
    Double(f64),
    /// RESP3 布尔 `#`
    Bool(bool),
    /// RESP3 文本 `=`
    Verbatim(String),
    /// RESP3 推送 `>`（订阅消息等）
    Push(Vec<RespValue>),
    /// RESP3 大数字 `(`
    BigNumber(String),
    /// RESP3 属性 `|`，客户端读取后应跳过
    Attribute(Vec<(RespValue, RespValue)>),
}

impl RespValue {
    /// 是否为空值。
    pub fn is_null(&self) -> bool {
        matches!(self, RespValue::Null)
    }

    /// 是否为错误回复。
    pub fn is_error(&self) -> bool {
        matches!(self, RespValue::Error(_))
    }

    /// 转为 `&[u8]`。整数/布尔等标量跟随 C# 端 `IPacket.ToStr()` 的语义转字符串字节。
    pub fn as_bytes(&self) -> Option<Vec<u8>> {
        match self {
            RespValue::Bulk(b) => Some(b.clone()),
            RespValue::Simple(s) | RespValue::Error(s) | RespValue::Verbatim(s) => {
                Some(s.as_bytes().to_vec())
            }
            RespValue::Integer(v) => Some(v.to_string().into_bytes()),
            RespValue::Double(v) => Some(crate::encoder::format_f64(*v).into_bytes()),
            RespValue::Bool(v) => Some(if *v { b"1".to_vec() } else { b"0".to_vec() }),
            RespValue::BigNumber(s) => Some(s.as_bytes().to_vec()),
            _ => None,
        }
    }

    /// 转为 UTF-8 字符串（非法字节使用替换字符）。
    pub fn as_string(&self) -> Option<String> {
        self.as_bytes()
            .map(|b| String::from_utf8_lossy(&b).into_owned())
    }

    /// 转为整数。兼容 C# 端对字符串结果的 `ToInt()` 转换。
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            RespValue::Integer(v) => Some(*v),
            RespValue::Bool(v) => Some(if *v { 1 } else { 0 }),
            RespValue::Double(v) => Some(*v as i64),
            RespValue::Bulk(b) => String::from_utf8_lossy(b).trim().parse().ok(),
            RespValue::Simple(s) | RespValue::Verbatim(s) => s.trim().parse().ok(),
            _ => None,
        }
    }

    /// 转为 64 位浮点。支持 RESP3 Double 与字符串形式。
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            RespValue::Double(v) => Some(*v),
            RespValue::Integer(v) => Some(*v as f64),
            RespValue::Bulk(b) => crate::encoder::parse_f64(&String::from_utf8_lossy(b)).ok(),
            RespValue::Simple(s) | RespValue::Verbatim(s) => crate::encoder::parse_f64(s).ok(),
            _ => None,
        }
    }

    /// 转为布尔。兼容 C# 端 `RedisJsonEncoder` 对 `OK` 的特殊处理。
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            RespValue::Bool(v) => Some(*v),
            RespValue::Integer(v) => Some(*v != 0),
            RespValue::Simple(s) => crate::encoder::parse_bool(s),
            RespValue::Bulk(b) => crate::encoder::parse_bool(&String::from_utf8_lossy(b)),
            _ => None,
        }
    }

    /// 拆出数组元素（Push 也按数组处理）。
    pub fn into_array(self) -> Option<Vec<RespValue>> {
        match self {
            RespValue::Array(v) | RespValue::Set(v) | RespValue::Push(v) => Some(v),
            _ => None,
        }
    }

    /// 借用数组元素。
    pub fn as_array(&self) -> Option<&[RespValue]> {
        match self {
            RespValue::Array(v) | RespValue::Set(v) | RespValue::Push(v) => Some(v),
            _ => None,
        }
    }
}

/// 将命令编码为 RESP2 数组帧，写入 `out`。
///
/// 与 C# 端 `GetRequest` 输出完全一致：`*<n>\r\n$<len>\r\n<data>\r\n...`。
pub fn encode_command(out: &mut Vec<u8>, args: &[&[u8]]) {
    out.extend_from_slice(b"*");
    out.extend_from_slice(args.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for arg in args {
        out.extend_from_slice(b"$");
        out.extend_from_slice(arg.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
}

/// 将命令编码为独立的 RESP2 数组帧。
pub fn encode_command_vec(args: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + args.iter().map(|a| a.len() + 16).sum::<usize>());
    encode_command(&mut out, args);
    out
}

/// 流式 RESP 解码器。
///
/// 可包裹任意 [`BufRead`]（生产环境使用 `BufReader<TcpStream>`，测试可用 `Cursor`），
/// 支持跨缓冲区分片到达的数据包。
pub struct Decoder<R: BufRead> {
    reader: R,
}

impl<R: BufRead> Decoder<R> {
    /// 创建解码器。
    pub fn new(reader: R) -> Self {
        Self { reader }
    }

    /// 取回底层读取器。
    pub fn into_inner(self) -> R {
        self.reader
    }

    /// 读取一个完整的 RESP 值。
    pub fn read_value(&mut self) -> Result<RespValue> {
        let line = self.read_line()?;
        self.parse_value(&line)
    }

    /// 读取回复。遇到 RESP3 属性帧（`|`）时读取并丢弃，返回其后的真实回复。
    pub fn read_reply(&mut self) -> Result<RespValue> {
        loop {
            let value = self.read_value()?;
            if matches!(value, RespValue::Attribute(_)) {
                continue;
            }
            return Ok(value);
        }
    }

    /// 读取一行（去掉 CRLF）。连接被对端关闭时报协议错误。
    fn read_line(&mut self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(32);
        let n = self.reader.read_until(b'\n', &mut buf)?;
        if n == 0 {
            return Err(Error::Protocol("连接已被对端关闭".into()));
        }
        if buf.last() == Some(&b'\n') {
            buf.pop();
        }
        if buf.last() == Some(&b'\r') {
            buf.pop();
        }
        Ok(buf)
    }

    fn parse_value(&mut self, line: &[u8]) -> Result<RespValue> {
        let Some((&kind, rest)) = line.split_first() else {
            return Err(Error::Protocol("空协议行".into()));
        };

        match kind {
            b'+' => Ok(RespValue::Simple(
                String::from_utf8_lossy(rest).into_owned(),
            )),
            b'-' => Ok(RespValue::Error(String::from_utf8_lossy(rest).into_owned())),
            b':' => Ok(RespValue::Integer(parse_i64(rest)?)),
            b'$' => self.read_bulk(rest),
            b'=' => self.read_verbatim(rest),
            b'*' => self.read_aggregate(rest, AggKind::Array),
            b'~' => self.read_aggregate(rest, AggKind::Set),
            b'>' => self.read_aggregate(rest, AggKind::Push),
            b'%' => Ok(RespValue::Map(self.read_map_items(rest)?)),
            b'|' => Ok(RespValue::Attribute(self.read_map_items(rest)?)),
            b',' => parse_double(rest).map(RespValue::Double),
            b'#' => match rest {
                b"t" => Ok(RespValue::Bool(true)),
                b"f" => Ok(RespValue::Bool(false)),
                _ => Err(Error::Protocol(format!(
                    "非法 RESP3 布尔值：{}",
                    String::from_utf8_lossy(rest)
                ))),
            },
            b'_' => Ok(RespValue::Null),
            b'(' => Ok(RespValue::BigNumber(
                String::from_utf8_lossy(rest).into_owned(),
            )),
            other => Err(Error::Protocol(format!(
                "未知的 RESP 类型标记：{}",
                other as char
            ))),
        }
    }

    fn read_bulk(&mut self, header: &[u8]) -> Result<RespValue> {
        let len = parse_i64(header)?;
        if len < 0 {
            return Ok(RespValue::Null);
        }
        let data = self.read_exact(len as usize)?;
        self.expect_crlf()?;
        Ok(RespValue::Bulk(data))
    }

    fn read_verbatim(&mut self, header: &[u8]) -> Result<RespValue> {
        let len = parse_i64(header)?;
        if len < 0 {
            return Ok(RespValue::Null);
        }
        let data = self.read_exact(len as usize)?;
        self.expect_crlf()?;
        // 形如 "txt:some text"，去掉 3 字节格式前缀
        let text = if data.len() > 4 && data[3] == b':' {
            String::from_utf8_lossy(&data[4..]).into_owned()
        } else {
            String::from_utf8_lossy(&data).into_owned()
        };
        Ok(RespValue::Verbatim(text))
    }

    fn read_aggregate(&mut self, header: &[u8], kind: AggKind) -> Result<RespValue> {
        let len = parse_i64(header)?;
        if len < 0 {
            return Ok(RespValue::Null);
        }
        let mut items = Vec::with_capacity(len.min(1024) as usize);
        for _ in 0..len {
            items.push(self.read_value()?);
        }

        Ok(match kind {
            AggKind::Array => RespValue::Array(items),
            AggKind::Set => RespValue::Set(items),
            AggKind::Push => RespValue::Push(items),
        })
    }

    fn read_map_items(&mut self, header: &[u8]) -> Result<Vec<(RespValue, RespValue)>> {
        let len = parse_i64(header)?;
        if len < 0 {
            return Ok(Vec::new());
        }
        let mut items = Vec::with_capacity(len.min(1024) as usize);
        for _ in 0..len {
            let k = self.read_value()?;
            let v = self.read_value()?;
            items.push((k, v));
        }

        Ok(items)
    }

    fn read_exact(&mut self, len: usize) -> Result<Vec<u8>> {
        let mut data = vec![0u8; len];
        self.reader.read_exact(&mut data)?;
        Ok(data)
    }

    fn expect_crlf(&mut self) -> Result<()> {
        let mut crlf = [0u8; 2];
        self.reader.read_exact(&mut crlf)?;
        if &crlf != b"\r\n" {
            return Err(Error::Protocol("批量字符串缺少 CRLF 结尾".into()));
        }
        Ok(())
    }
}

enum AggKind {
    Array,
    Set,
    Push,
}

fn parse_i64(bytes: &[u8]) -> Result<i64> {
    let s = std::str::from_utf8(bytes).map_err(|_| Error::Protocol("协议整数非 UTF-8".into()))?;
    s.trim()
        .parse()
        .map_err(|_| Error::Protocol(format!("非法协议整数：{s}")))
}

fn parse_double(bytes: &[u8]) -> Result<f64> {
    let s = std::str::from_utf8(bytes).map_err(|_| Error::Protocol("协议浮点非 UTF-8".into()))?;
    match s.trim() {
        "inf" | "+inf" => Ok(f64::INFINITY),
        "-inf" => Ok(f64::NEG_INFINITY),
        "nan" => Ok(f64::NAN),
        other => other
            .parse()
            .map_err(|_| Error::Protocol(format!("非法协议浮点：{other}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn decode(bytes: &[u8]) -> RespValue {
        let mut decoder = Decoder::new(Cursor::new(bytes.to_vec()));
        decoder.read_reply().unwrap()
    }

    #[test]
    fn encode_command_matches_csharp_frame() {
        // C# 端 GetRequest 输出：SET key value => *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
        let frame = encode_command_vec(&[b"SET", b"key", b"value"]);
        assert_eq!(
            frame,
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n".to_vec()
        );
    }

    #[test]
    fn encode_command_with_binary_payload() {
        let payload: Vec<u8> = vec![0, 1, b'"', b'\r', b'\n', 255];
        let frame = encode_command_vec(&[b"LPUSH", b"q", &payload]);
        let mut expected = b"*3\r\n$5\r\nLPUSH\r\n$1\r\nq\r\n$6\r\n".to_vec();
        expected.extend_from_slice(&payload);
        expected.extend_from_slice(b"\r\n");
        assert_eq!(frame, expected);
    }

    #[test]
    fn decode_resp2_scalars() {
        assert_eq!(decode(b"+OK\r\n"), RespValue::Simple("OK".into()));
        assert_eq!(decode(b":42\r\n"), RespValue::Integer(42));
        assert_eq!(
            decode(b"$5\r\nvalue\r\n"),
            RespValue::Bulk(b"value".to_vec())
        );
        assert_eq!(decode(b"$-1\r\n"), RespValue::Null);
        assert_eq!(decode(b"*-1\r\n"), RespValue::Null);
        assert_eq!(decode(b"$0\r\n\r\n"), RespValue::Bulk(Vec::new()));
        assert_eq!(decode(b"+PONG\r\n"), RespValue::Simple("PONG".into()));
    }

    #[test]
    fn decode_resp2_error_and_nested_array() {
        let v = decode(b"-ERR unknown command 'foo'\r\n");
        assert_eq!(v, RespValue::Error("ERR unknown command 'foo'".into()));

        let v = decode(b"*3\r\n$3\r\nfoo\r\n:7\r\n*2\r\n$1\r\na\r\n$-1\r\n");
        assert_eq!(
            v,
            RespValue::Array(vec![
                RespValue::Bulk(b"foo".to_vec()),
                RespValue::Integer(7),
                RespValue::Array(vec![RespValue::Bulk(b"a".to_vec()), RespValue::Null]),
            ])
        );
    }

    #[test]
    #[should_panic]
    fn decode_bad_bulk_length_panics() {
        decode(b"$x\r\n");
    }

    #[test]
    fn decode_resp3_types() {
        assert_eq!(decode(b"_\r\n"), RespValue::Null);
        assert_eq!(decode(b"#t\r\n"), RespValue::Bool(true));
        assert_eq!(decode(b"#f\r\n"), RespValue::Bool(false));
        assert_eq!(decode(b",1.5\r\n"), RespValue::Double(1.5));
        assert_eq!(decode(b",inf\r\n"), RespValue::Double(f64::INFINITY));
        assert_eq!(
            decode(b"=9\r\ntxt:hello\r\n"),
            RespValue::Verbatim("hello".into())
        );
        assert_eq!(
            decode(b"(3492890328409238509324850943850943825024385\r\n"),
            RespValue::BigNumber("3492890328409238509324850943850943825024385".into())
        );
    }

    #[test]
    fn decode_resp3_map_set_push() {
        let v = decode(b"%2\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nk2\r\n:5\r\n");
        assert_eq!(
            v,
            RespValue::Map(vec![
                (
                    RespValue::Bulk(b"k".to_vec()),
                    RespValue::Bulk(b"v".to_vec())
                ),
                (RespValue::Bulk(b"k2".to_vec()), RespValue::Integer(5)),
            ])
        );

        let v = decode(b"~2\r\n$1\r\na\r\n$1\r\nb\r\n");
        assert_eq!(
            v,
            RespValue::Set(vec![
                RespValue::Bulk(b"a".to_vec()),
                RespValue::Bulk(b"b".to_vec()),
            ])
        );

        let v = decode(b">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n");
        assert_eq!(
            v,
            RespValue::Push(vec![
                RespValue::Bulk(b"message".to_vec()),
                RespValue::Bulk(b"ch".to_vec()),
                RespValue::Bulk(b"hi".to_vec()),
            ])
        );
    }

    #[test]
    fn attribute_frames_are_skipped_by_read_reply() {
        let bytes = b"|1\r\n$3\r\nwhy\r\n$3\r\nfoo\r\n+OK\r\n";
        let mut decoder = Decoder::new(Cursor::new(bytes.to_vec()));
        assert_eq!(
            decoder.read_reply().unwrap(),
            RespValue::Simple("OK".into())
        );
    }

    #[test]
    fn fragmented_input_is_reassembled() {
        // 缓冲容量设为 1，强制每个字节都可能跨缓冲到达
        let payload = b"*3\r\n$3\r\nfoo\r\n$5\r\nhello\r\n$1\r\n-\r\n";
        let mut reader = std::io::BufReader::with_capacity(1, Cursor::new(payload.to_vec()));
        let mut decoder = Decoder::new(&mut reader);
        let v = decoder.read_value().unwrap();
        assert_eq!(
            v,
            RespValue::Array(vec![
                RespValue::Bulk(b"foo".to_vec()),
                RespValue::Bulk(b"hello".to_vec()),
                RespValue::Bulk(b"-".to_vec()),
            ])
        );
    }

    #[test]
    fn resp3_double_parses_from_bulk_for_csharp_compat() {
        // C# 端部分命令（如 ZSCORE）在 RESP2 下返回批量字符串
        let v = decode(b"$18\r\n1.2999999999999999\r\n");
        assert_eq!(v.as_f64(), Some(1.2999999999999999));
    }
}
