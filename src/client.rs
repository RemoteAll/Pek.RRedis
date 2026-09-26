//! Redis 同步客户端：TCP 连接、认证、协议协商、命令收发。
//!
//! 对应 DH.NRedis 的 `RedisClient`。一个 `RedisClient` 对应一条 TCP 连接，
//! 由连接池管理，不在多线程间共享；每次命令「发请求 → 读应答」无需加锁。
//!
//! 兼容要点：
//! - 握手顺序与 C# 一致：`HELLO 3`（启用 RESP3 时）→ `AUTH` → `SELECT db`；
//! - `-ERR` 回复与 C# 一样抛出 [`Error::Server`]，不参与重试；
//! - 阻塞命令（BRPOP 等）通过 [`RedisClient::command_blocking`] 临时放宽读超时，
//!   与 C# 调整 `Redis.Timeout` 的行为等价。

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::resp::{Decoder, RespValue, encode_command};

/// 建立连接所需的配置快照。
#[derive(Debug, Clone)]
pub struct ConnConfig {
    /// 服务端地址（`host:port`）
    pub endpoint: String,
    /// 用户名（Redis 6.0+ ACL）
    pub user_name: Option<String>,
    /// 密码
    pub password: Option<String>,
    /// 目标库
    pub db: i32,
    /// 读写超时（毫秒）
    pub timeout_ms: u64,
    /// 协议版本：0/2 = RESP2，3 = RESP3
    pub protocol_version: i32,
    /// 最大消息大小（字节）
    pub max_message_size: usize,
}

/// 单条 TCP 连接上的 Redis 客户端。
pub struct RedisClient {
    endpoint: String,
    writer: TcpStream,
    reader: BufReader<TcpStream>,
    max_message_size: usize,
    protocol_version: i32,
    user_name: Option<String>,
    password: Option<String>,
    db: i32,
    logged_in: bool,
    selected_db: i32,
    broken: bool,
}

impl RedisClient {
    /// 建立连接并完成握手（HELLO / AUTH / SELECT）。
    pub fn connect(cfg: &ConnConfig) -> Result<Self> {
        let timeout = Duration::from_millis(if cfg.timeout_ms > 0 { cfg.timeout_ms } else { 3000 });
        let stream = connect_tcp(&cfg.endpoint, timeout)?;
        stream.set_nodelay(true).ok();

        let mut client = Self {
            endpoint: cfg.endpoint.clone(),
            reader: BufReader::with_capacity(16 * 1024, stream.try_clone()?),
            writer: stream,
            max_message_size: cfg.max_message_size,
            protocol_version: cfg.protocol_version,
            user_name: cfg.user_name.clone().filter(|s| !s.is_empty()),
            password: cfg.password.clone().filter(|s| !s.is_empty()),
            db: cfg.db,
            logged_in: false,
            selected_db: -1,
            broken: false,
        };
        client.writer.set_read_timeout(Some(timeout))?;
        client.writer.set_write_timeout(Some(timeout))?;

        client.handshake()?;
        Ok(client)
    }

    /// 服务端地址。
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// 协商后的协议版本（2 或 3）。
    pub fn protocol_version(&self) -> i32 {
        self.protocol_version
    }

    /// 连接是否已损坏（发生 IO/协议错误后不可再复用）。
    pub fn is_broken(&self) -> bool {
        self.broken
    }

    /// 标记连接不可复用。
    pub fn mark_broken(&mut self) {
        self.broken = true;
    }

    // ======== 基础读写 ========

    /// 执行一条命令，返回原始应答。服务端错误转为 [`Error::Server`]。
    pub fn command(&mut self, args: &[&[u8]]) -> Result<RespValue> {
        self.ensure_ready(args)?;
        self.write_frame(args)?;
        self.read_reply()
    }

    /// 执行阻塞命令（BRPOP / BRPOPLPUSH / BLMOVE 等），临时放宽读超时。
    ///
    /// `block_seconds` 为命令自身的阻塞秒数（0 表示永久阻塞）。
    pub fn command_blocking(&mut self, args: &[&[u8]], block_seconds: i64) -> Result<RespValue> {
        let previous = self.writer.read_timeout().ok().flatten();
        let extra = if block_seconds <= 0 { 60 } else { block_seconds + 2 };
        self.writer
            .set_read_timeout(Some(Duration::from_secs(extra as u64)))?;

        let rs = self.command(args);
        self.writer.set_read_timeout(previous)?;

        rs
    }

    /// 读取一条推送/回复（不发送命令）。用于订阅循环等场景。
    ///
    /// 与 [`RedisClient::command`] 一样，服务端错误转为 [`Error::Server`]；
    /// 读超时错误不标记连接损坏，调用方可继续轮询（便于取消检查）。
    pub fn read_message(&mut self) -> Result<RespValue> {
        self.read_reply()
    }

    /// 调整读超时（订阅循环中用短超时以便及时响应取消）。
    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.writer.set_read_timeout(timeout)?;
        Ok(())
    }

    /// 批量执行命令（管道）：一次性写入全部请求，再顺序读取全部应答。
    pub fn command_many(&mut self, cmds: &[Vec<Vec<u8>>]) -> Result<Vec<RespValue>> {
        for cmd in cmds {
            let args: Vec<&[u8]> = cmd.iter().map(|a| a.as_slice()).collect();
            self.ensure_ready(&args)?;
            let frame = self.encode_frame(&args)?;
            self.writer.write_all(&frame)?;
        }
        self.writer.flush()?;

        let mut results = Vec::with_capacity(cmds.len());
        for _ in 0..cmds.len() {
            results.push(self.read_reply()?);
        }
        Ok(results)
    }

    /// 发送 `PING` 验证连接存活。
    pub fn ping(&mut self) -> Result<()> {
        let rs = self.command(&[b"PING"])?;
        if rs.as_string().as_deref() == Some("PONG") {
            Ok(())
        } else {
            Err(Error::Protocol("PING 未返回 PONG".into()))
        }
    }

    /// 尽力排空残留数据（连接归还池前的清理，对应 C# `RedisClient.Reset`）。
    pub fn drain(&mut self) {
        // 先丢掉 BufReader 中已缓冲的数据
        let buffered = self.reader.buffer().len();
        if buffered > 0 {
            self.reader.consume(buffered);
        }

        // 再把套接字上已就绪的数据读掉（非阻塞）
        if self.writer.set_nonblocking(true).is_ok() {
            let mut buf = [0u8; 4096];
            let mut stream = match self.writer.try_clone() {
                Ok(s) => s,
                Err(_) => {
                    let _ = self.writer.set_nonblocking(false);
                    return;
                }
            };
            loop {
                match stream.read(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
            let _ = self.writer.set_nonblocking(false);
        }
    }

    /// 主动退出（QUIT），并标记连接不可再用。
    pub fn quit(&mut self) -> Result<()> {
        let _ = self.command(&[b"QUIT"]);
        self.logged_in = false;
        Ok(())
    }

    // ======== 握手与状态 ========

    fn handshake(&mut self) -> Result<()> {
        if self.protocol_version >= 3 {
            // 与 C# CheckLogin 相同：HELLO 可同时完成协议协商与认证，失败时降级 RESP2
            match self.hello(3) {
                Ok(_) => {
                    self.logged_in = true;
                }
                Err(Error::Server(_)) => {
                    self.protocol_version = 2;
                }
                Err(e) => return Err(e),
            }
        }

        if !self.logged_in {
            if self.password.is_some() {
                self.auth()?;
            }
            self.logged_in = true;
        }

        if self.db != 0 {
            self.select(self.db)?;
        } else {
            self.selected_db = 0;
        }

        Ok(())
    }

    fn hello(&mut self, protover: i32) -> Result<RespValue> {
        let mut args: Vec<Vec<u8>> = vec![b"HELLO".to_vec(), protover.to_string().into_bytes()];
        if let Some(pwd) = &self.password {
            args.push(b"AUTH".to_vec());
            args.push(self.user_name.clone().unwrap_or_else(|| "default".into()).into_bytes());
            args.push(pwd.clone().into_bytes());
        }

        let arg_refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        self.write_frame(&arg_refs)?;
        self.read_reply()
    }

    fn auth(&mut self) -> Result<()> {
        let Some(pwd) = self.password.clone() else {
            return Ok(());
        };

        let rs = if let Some(user) = self.user_name.clone() {
            self.raw_command(&[b"AUTH", user.as_bytes(), pwd.as_bytes()])?
        } else {
            self.raw_command(&[b"AUTH", pwd.as_bytes()])?
        };

        if rs.as_string().as_deref() == Some("OK") {
            Ok(())
        } else {
            Err(Error::Server("AUTH 失败".into()))
        }
    }

    /// 切换数据库。
    pub fn select(&mut self, db: i32) -> Result<()> {
        let rs = self.raw_command(&[b"SELECT", db.to_string().as_bytes()])?;
        if rs.as_string().as_deref() == Some("OK") {
            self.selected_db = db;
            Ok(())
        } else {
            Err(Error::Server(format!("SELECT {db} 失败")))
        }
    }

    /// 与 C# `CheckLogin` / `CheckSelect` 对应的惰性握手。
    fn ensure_ready(&mut self, args: &[&[u8]]) -> Result<()> {
        let cmd = args.first().map(|a| String::from_utf8_lossy(a).to_uppercase());
        let cmd = cmd.as_deref().unwrap_or("");

        if !self.logged_in && !matches!(cmd, "AUTH" | "HELLO") {
            if self.protocol_version >= 3 {
                match self.hello(3) {
                    Ok(_) => self.logged_in = true,
                    Err(Error::Server(_)) => {
                        self.protocol_version = 2;
                        if self.password.is_some() {
                            self.auth()?;
                        }
                        self.logged_in = true;
                    }
                    Err(e) => return Err(e),
                }
            } else {
                if self.password.is_some() {
                    self.auth()?;
                }
                self.logged_in = true;
            }
        }

        if self.selected_db != self.db && !matches!(cmd, "AUTH" | "SELECT" | "INFO" | "HELLO" | "QUIT")
        {
            if self.db > 0 {
                self.select(self.db)?;
            } else {
                self.selected_db = self.db;
            }
        }

        Ok(())
    }

    /// 不触发握手检查的底层命令（仅握手内部使用）。
    fn raw_command(&mut self, args: &[&[u8]]) -> Result<RespValue> {
        self.write_frame(args)?;
        self.read_reply()
    }

    // ======== 帧读写 ========

    fn write_frame(&mut self, args: &[&[u8]]) -> Result<()> {
        let frame = self.encode_frame(args)?;
        if let Err(e) = self.writer.write_all(&frame).and_then(|_| self.writer.flush()) {
            self.broken = true;
            return Err(Error::Io(e));
        }
        Ok(())
    }

    /// 组帧并做大小限制校验（不发送）。
    fn encode_frame(&self, args: &[&[u8]]) -> Result<Vec<u8>> {
        let mut frame = Vec::with_capacity(32 + args.iter().map(|a| a.len() + 16).sum::<usize>());
        encode_command(&mut frame, args);

        if self.max_message_size > 0 && frame.len() >= self.max_message_size {
            let cmd = args
                .first()
                .map(|a| String::from_utf8_lossy(a).into_owned())
                .unwrap_or_default();
            return Err(Error::Protocol(format!(
                "命令[{cmd}]的数据包大小[{}]超过最大限制[{}]，大 key 会拖累整个 Redis 实例，可通过 MaxMessageSize 调节。",
                frame.len(),
                self.max_message_size
            )));
        }

        Ok(frame)
    }

    fn read_reply(&mut self) -> Result<RespValue> {
        let mut decoder = Decoder::new(&mut self.reader);
        match decoder.read_reply() {
            Ok(RespValue::Error(msg)) => Err(Error::Server(msg)),
            Ok(v) => Ok(v),
            Err(Error::Io(e))
                if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) =>
            {
                // 读超时可继续使用连接，交由调用方决定是否重试
                Err(Error::Io(e))
            }
            Err(e) => {
                self.broken = true;
                Err(e)
            }
        }
    }
}

impl std::fmt::Debug for RedisClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisClient")
            .field("endpoint", &self.endpoint)
            .field("protocol", &self.protocol_version)
            .field("db", &self.db)
            .finish()
    }
}

/// TCP 连接：支持域名解析、IPv4/IPv6 与 `[::1]:6379` 形式。
fn connect_tcp(endpoint: &str, timeout: Duration) -> Result<TcpStream> {
    let addrs = resolve(endpoint)?;
    let started = Instant::now();
    let mut last_err: Option<std::io::Error> = None;

    for addr in addrs {
        let remaining = timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            break;
        }
        match TcpStream::connect_timeout(&addr, remaining) {
            Ok(s) => return Ok(s),
            Err(e) => last_err = Some(e),
        }
    }

    Err(Error::Io(last_err.unwrap_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, format!("无法连接 {endpoint}"))
    })))
}

fn resolve(endpoint: &str) -> Result<Vec<std::net::SocketAddr>> {
    let endpoint = endpoint.trim().strip_prefix("tcp://").unwrap_or(endpoint.trim());
    if endpoint.is_empty() {
        return Err(Error::Config("服务器地址为空".into()));
    }

    let candidates = if endpoint.contains(']') || endpoint.matches(':').count() <= 1 {
        vec![endpoint.to_string()]
    } else {
        // 无方括号的 IPv6 裸地址
        vec![endpoint.to_string()]
    };

    let mut result = Vec::new();
    for candidate in candidates {
        match candidate.to_socket_addrs() {
            Ok(addrs) => result.extend(addrs),
            Err(e) => {
                return Err(Error::Config(format!("无法解析服务器地址 {candidate}：{e}")));
            }
        }
    }

    if result.is_empty() {
        return Err(Error::Config(format!("服务器地址无可用解析结果：{endpoint}")));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_defaults_and_ports() {
        let addrs = resolve("127.0.0.1:6379").unwrap();
        assert_eq!(addrs[0].to_string(), "127.0.0.1:6379");
    }

    #[test]
    fn resolve_requires_content() {
        assert!(matches!(resolve("   "), Err(Error::Config(_))));
    }
}
