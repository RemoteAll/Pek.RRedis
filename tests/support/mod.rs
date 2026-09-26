//! 测试支撑：进程内迷你 Redis（RESP2）服务器。
//!
//! 目的：在没有真实 Redis 的环境（含 CI）下，对客户端做**端到端**验证——
//! 覆盖协议收发、连接池、管道、编码器格式、数据结构命令与队列语义。
//! 只实现测试所需的命令子集；语义按 Redis 官方文档对齐（含 `SET ... NX EX GET`、
//! `RPOPLPUSH`、`LREM`、`ZRANGEBYSCORE LIMIT`、`SET NX` 锁抢占等）。

#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pek_rredis::FullRedis;
use pek_rredis::resp::{Decoder, RespValue};

type Bytes = Vec<u8>;

#[derive(Default, Clone, Debug)]
enum Value {
    #[default]
    None,
    Str(Bytes),
    List(VecDeque<Bytes>),
    Hash(Vec<(Bytes, Bytes)>),
    Set(Vec<Bytes>),
    ZSet(Vec<(Bytes, f64)>),
    Stream(StreamState),
}

/// Stream 数据。
#[derive(Default, Clone, Debug)]
struct StreamState {
    entries: VecDeque<StreamEntry>,
    last_id: (u64, u64),
    entries_added: u64,
    groups: Vec<StreamGroup>,
}

#[derive(Clone, Debug)]
struct StreamEntry {
    ms: u64,
    seq: u64,
    fields: Vec<(Bytes, Bytes)>,
}

impl StreamEntry {
    fn id(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }
}

#[derive(Clone, Debug, Default)]
struct StreamGroup {
    name: String,
    last_delivered: (u64, u64),
    consumers: Vec<StreamConsumer>,
    pending: Vec<StreamPending>,
}

#[derive(Clone, Debug, Default)]
struct StreamConsumer {
    name: String,
    last_active: Option<Instant>,
}

#[derive(Clone, Debug)]
struct StreamPending {
    ms: u64,
    seq: u64,
    consumer: String,
    delivery_time: Instant,
    delivery: u32,
}

impl StreamPending {
    fn id(&self) -> String {
        format!("{}-{}", self.ms, self.seq)
    }
}

#[derive(Default, Clone)]
struct Entry {
    value: Value,
    expire_at: Option<Instant>,
}

#[derive(Default)]
pub struct Store {
    data: HashMap<Bytes, Entry>,
}

impl Store {
    fn get_live(&mut self, key: &[u8]) -> Option<&mut Entry> {
        let expired = match self.data.get(key) {
            Some(e) => e.expire_at.map(|t| t <= Instant::now()).unwrap_or(false),
            None => false,
        };
        if expired {
            self.data.remove(key);
        }
        self.data.get_mut(key)
    }

    fn remove(&mut self, key: &[u8]) -> bool {
        self.get_live(key);
        self.data.remove(key).is_some()
    }

    fn str_value(&mut self, key: &[u8]) -> Option<Bytes> {
        match self.get_live(key).map(|e| &e.value) {
            Some(Value::Str(s)) => Some(s.clone()),
            _ => None,
        }
    }

    fn set_str(&mut self, key: &[u8], value: Bytes) {
        let entry = self.data.entry(key.to_vec()).or_default();
        entry.value = Value::Str(value);
    }
}

/// 已启动的迷你 Redis。`addr` 为监听地址；析构时自动关闭。
pub struct MockRedis {
    pub addr: String,
    pub store: Arc<Mutex<Store>>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Drop for MockRedis {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // 触发 accept 线程退出
        let _ = TcpStream::connect(&self.addr);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// 启动迷你 Redis。
pub fn start_mock_redis() -> MockRedis {
    start_mock_redis_on(0)
}

/// 在指定端口启动迷你 Redis（`port=0` 表示随机端口）。
pub fn start_mock_redis_on(port: u16) -> MockRedis {
    let listener = TcpListener::bind(("127.0.0.1", port)).expect("无法绑定测试端口");
    let addr = listener.local_addr().unwrap().to_string();
    listener.set_nonblocking(true).unwrap();

    let store: Arc<Mutex<Store>> = Arc::new(Mutex::new(Store::default()));
    let stop = Arc::new(AtomicBool::new(false));

    let handle = {
        let store = store.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if stop.load(Ordering::SeqCst) {
                            break;
                        }
                        let store = store.clone();
                        thread::spawn(move || {
                            let _ = handle_conn(stream, store);
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        })
    };

    MockRedis {
        addr,
        store,
        stop,
        handle: Some(handle),
    }
}

/// 连接迷你 Redis 的 `FullRedis`（db=0，无密码，无前缀）。
pub fn mock_full() -> (MockRedis, FullRedis) {
    let server = start_mock_redis();
    let redis = FullRedis::from_config(&format!("server={};db=0", server.addr)).unwrap();
    (server, redis)
}

fn handle_conn(stream: TcpStream, store: Arc<Mutex<Store>>) -> std::io::Result<()> {
    stream.set_nodelay(true).ok();
    // Windows 上 accept 返回的套接字会继承监听套接字的非阻塞属性，这里显式改回阻塞
    stream.set_nonblocking(false).ok();
    stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
    let mut writer = stream.try_clone()?;
    let mut decoder = Decoder::new(std::io::BufReader::new(stream));

    loop {
        let value = match decoder.read_value() {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        let args: Vec<Bytes> = match value {
            RespValue::Array(items) => items
                .into_iter()
                .map(|v| v.as_bytes().unwrap_or_default())
                .collect(),
            _ => return Ok(()),
        };
        if args.is_empty() {
            continue;
        }

        let (reply, quit) = dispatch(&store, &args);
        writer.write_all(&reply)?;
        writer.flush()?;
        if quit {
            return Ok(());
        }
    }
}

// ==================== 应答编码 ====================

fn simple(text: &str) -> Bytes {
    format!("+{text}\r\n").into_bytes()
}

fn error(text: &str) -> Bytes {
    format!("-{text}\r\n").into_bytes()
}

fn int(v: i64) -> Bytes {
    format!(":{v}\r\n").into_bytes()
}

fn bulk(data: &[u8]) -> Bytes {
    let mut out = format!("${}\r\n", data.len()).into_bytes();
    out.extend_from_slice(data);
    out.extend_from_slice(b"\r\n");
    out
}

fn nil() -> Bytes {
    b"$-1\r\n".to_vec()
}

fn array(items: Vec<Bytes>) -> Bytes {
    let mut out = format!("*{}\r\n", items.len()).into_bytes();
    for item in items {
        out.extend_from_slice(&item);
    }
    out
}

fn ok() -> Bytes {
    simple("OK")
}

fn wrong_type() -> Bytes {
    error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn not_int() -> Bytes {
    error("ERR value is not an integer or out of range")
}

fn not_float() -> Bytes {
    error("ERR value is not a valid float")
}

// ==================== 命令分发 ====================

fn dispatch(store: &Arc<Mutex<Store>>, args: &[Bytes]) -> (Bytes, bool) {
    let cmd = String::from_utf8_lossy(&args[0]).to_uppercase();
    let mut store = store.lock().unwrap();

    // Stream 命令（XADD / XREAD / XREADGROUP / XACK / XPENDING / XCLAIM / XGROUP / XINFO / XTRIM / XDEL / XLEN / XRANGE）
    if cmd.starts_with('X')
        && let Some(result) = dispatch_stream(&mut store, &cmd, args) {
            return result;
        }

    match cmd.as_str() {
        "PING" => (simple("PONG"), false),
        "QUIT" => (ok(), true),
        "SELECT" | "AUTH" | "CLIENT" => (ok(), false),
        "HELLO" => (error("ERR unknown command 'HELLO'"), false),
        "INFO" => (bulk(b"# Server\r\nredis_version:7.2.4\r\nredis_mode:standalone\r\nos:Windows\r\n"), false),
        "DBSIZE" => (int(store.data.len() as i64), false),
        "FLUSHDB" => {
            store.data.clear();
            (ok(), false)
        }

        // ---------- 键 ----------
        "DEL" | "UNLINK" => {
            let mut n = 0;
            for key in &args[1..] {
                if store.remove(key) {
                    n += 1;
                }
            }
            (int(n), false)
        }
        "EXISTS" => {
            let mut n = 0;
            for key in &args[1..] {
                if store.get_live(key).is_some() {
                    n += 1;
                }
            }
            (int(n), false)
        }
        "EXPIRE" => {
            let seconds: i64 = parse_i64(&args[2]).unwrap_or(0);
            let exists = store.get_live(&args[1]).is_some();
            if exists {
                if let Some(e) = store.get_live(&args[1]) {
                    e.expire_at = if seconds > 0 {
                        Some(Instant::now() + Duration::from_secs(seconds as u64))
                    } else {
                        None
                    };
                }
                (int(1), false)
            } else {
                (int(0), false)
            }
        }
        "TTL" => {
            let ttl = match store.get_live(&args[1]) {
                None => -2,
                Some(e) => match e.expire_at {
                    None => -1,
                    Some(t) => {
                        let remain = t.saturating_duration_since(Instant::now());
                        remain.as_millis().div_ceil(1000) as i64
                    }
                },
            };
            (int(ttl), false)
        }
        "PERSIST" => {
            if let Some(e) = store.get_live(&args[1]) {
                let had = e.expire_at.is_some();
                e.expire_at = None;
                (int(if had { 1 } else { 0 }), false)
            } else {
                (int(0), false)
            }
        }
        "TYPE" => {
            let t = match store.get_live(&args[1]).map(|e| &e.value) {
                Some(Value::Str(_)) => "string",
                Some(Value::List(_)) => "list",
                Some(Value::Hash(_)) => "hash",
                Some(Value::Set(_)) => "set",
                Some(Value::ZSet(_)) => "zset",
                Some(Value::Stream(_)) => "stream",
                _ => "none",
            };
            (simple(t), false)
        }
        "KEYS" => {
            let pattern = &args[1];
            let keys: Vec<Bytes> = store
                .data
                .iter()
                .filter(|(k, e)| {
                    e.expire_at.map(|t| t > Instant::now()).unwrap_or(true) && glob_match(pattern, k)
                })
                .map(|(k, _)| k.clone())
                .collect();
            (array(keys.iter().map(|k| bulk(k)).collect()), false)
        }
        "SCAN" => {
            let pattern = args
                .iter()
                .position(|a| a.eq_ignore_ascii_case(b"MATCH"))
                .and_then(|i| args.get(i + 1).cloned())
                .unwrap_or_else(|| b"*".to_vec());
            let keys: Vec<Bytes> = store
                .data
                .iter()
                .filter(|(k, e)| {
                    e.expire_at.map(|t| t > Instant::now()).unwrap_or(true) && glob_match(&pattern, k)
                })
                .map(|(k, _)| k.clone())
                .collect();
            (array(vec![bulk(b"0"), array(keys.iter().map(|k| bulk(k)).collect())]), false)
        }
        "RANDOMKEY" => (nil(), false),

        // ---------- 字符串 ----------
        "SET" => dispatch_set(&mut store, args),
        "SETEX" => {
            let seconds: i64 = parse_i64(&args[2]).unwrap_or(0);
            store.set_str(&args[1], args[3].clone());
            if let Some(e) = store.get_live(&args[1]) {
                e.expire_at = Some(Instant::now() + Duration::from_secs(seconds.max(1) as u64));
            }
            (ok(), false)
        }
        "GET" => match store.str_value(&args[1]) {
            Some(v) => (bulk(&v), false),
            None => {
                if store.get_live(&args[1]).is_some() {
                    (wrong_type(), false)
                } else {
                    (nil(), false)
                }
            }
        },
        "GETSET" => {
            let old = store.str_value(&args[1]);
            store.set_str(&args[1], args[2].clone());
            match old {
                Some(v) => (bulk(&v), false),
                None => (nil(), false),
            }
        }
        "MGET" => {
            let items = args[1..]
                .iter()
                .map(|k| match store.str_value(k) {
                    Some(v) => bulk(&v),
                    None => nil(),
                })
                .collect();
            (array(items), false)
        }
        "MSET" => {
            let mut i = 1;
            while i + 1 < args.len() {
                store.set_str(&args[i], args[i + 1].clone());
                i += 2;
            }
            (ok(), false)
        }
        "INCR" | "INCRBY" | "DECR" | "DECRBY" => {
            let delta = match cmd.as_str() {
                "INCR" => 1,
                "DECR" => -1,
                _ => {
                    let mut d = parse_i64(&args[2]).unwrap_or(0);
                    if cmd == "DECRBY" {
                        d = -d;
                    }
                    d
                }
            };
            let current = match store.str_value(&args[1]) {
                Some(v) => match parse_i64(&v) {
                    Some(n) => n,
                    None => return (not_int(), false),
                },
                None => 0,
            };
            let next = current + delta;
            store.set_str(&args[1], next.to_string().into_bytes());
            (int(next), false)
        }
        "INCRBYFLOAT" => {
            let delta = parse_f64(&args[2]).unwrap_or(0.0);
            let current = match store.str_value(&args[1]) {
                Some(v) => parse_f64(&v).unwrap_or(0.0),
                None => 0.0,
            };
            let next = current + delta;
            let text = format!("{next}");
            store.set_str(&args[1], text.clone().into_bytes());
            (bulk(text.as_bytes()), false)
        }
        "APPEND" => {
            let mut current = store.str_value(&args[1]).unwrap_or_default();
            current.extend_from_slice(&args[2]);
            let len = current.len() as i64;
            store.set_str(&args[1], current);
            (int(len), false)
        }
        "STRLEN" => (int(store.str_value(&args[1]).map(|v| v.len()).unwrap_or(0) as i64), false),
        "GETRANGE" => {
            let data = store.str_value(&args[1]).unwrap_or_default();
            let start = parse_i64(&args[2]).unwrap_or(0);
            let end = parse_i64(&args[3]).unwrap_or(-1);
            let slice = range_slice(&data, start, end);
            (bulk(&slice), false)
        }

        // ---------- 哈希 ----------
        "HSET" => {
            let created = hash_set(&mut store, &args[1], args[2..].to_vec());
            (int(created), false)
        }
        "HSETNX" => {
            let exists = hash_get(&mut store, &args[1], &args[2]).is_some();
            if exists {
                (int(0), false)
            } else {
                hash_set(&mut store, &args[1], vec![args[2].clone(), args[3].clone()]);
                (int(1), false)
            }
        }
        "HGET" => match hash_get(&mut store, &args[1], &args[2]) {
            Some(v) => (bulk(&v), false),
            None => (nil(), false),
        },
        "HDEL" => {
            let mut n = 0;
            match store.get_live(&args[1]).map(|e| &mut e.value) {
                Some(Value::Hash(items)) => {
                    for field in &args[2..] {
                        let before = items.len();
                        items.retain(|(k, _)| k != field);
                        if items.len() != before {
                            n += 1;
                        }
                    }
                }
                Some(_) => return (wrong_type(), false),
                None => {}
            }
            (int(n), false)
        }
        "HEXISTS" => match hash_get(&mut store, &args[1], &args[2]) {
            Some(_) => (int(1), false),
            None => (int(0), false),
        },
        "HLEN" => {
            let len = match store.get_live(&args[1]).map(|e| &e.value) {
                Some(Value::Hash(items)) => items.len() as i64,
                _ => 0,
            };
            (int(len), false)
        }
        "HKEYS" | "HVALS" => {
            let items = match store.get_live(&args[1]).map(|e| &e.value) {
                Some(Value::Hash(items)) => items
                    .iter()
                    .map(|(k, v)| bulk(if cmd == "HKEYS" { k } else { v }))
                    .collect(),
                _ => Vec::new(),
            };
            (array(items), false)
        }
        "HGETALL" => {
            let mut out = Vec::new();
            if let Some(Value::Hash(items)) = store.get_live(&args[1]).map(|e| &e.value) {
                for (k, v) in items {
                    out.push(bulk(k));
                    out.push(bulk(v));
                }
            }
            (array(out), false)
        }
        "HMGET" => {
            let items = args[2..]
                .iter()
                .map(|f| match hash_get(&mut store, &args[1], f) {
                    Some(v) => bulk(&v),
                    None => nil(),
                })
                .collect();
            (array(items), false)
        }
        "HINCRBY" => {
            let delta = parse_i64(&args[3]).unwrap_or(0);
            let current = hash_get(&mut store, &args[1], &args[2])
                .and_then(|v| parse_i64(&v))
                .unwrap_or(0);
            let next = current + delta;
            hash_set(
                &mut store,
                &args[1],
                vec![args[2].clone(), next.to_string().into_bytes()],
            );
            (int(next), false)
        }
        "HINCRBYFLOAT" => {
            let delta = parse_f64(&args[3]).unwrap_or(0.0);
            let current = hash_get(&mut store, &args[1], &args[2])
                .and_then(|v| parse_f64(&v))
                .unwrap_or(0.0);
            let next = current + delta;
            let text = format!("{next}");
            hash_set(&mut store, &args[1], vec![args[2].clone(), text.clone().into_bytes()]);
            (bulk(text.as_bytes()), false)
        }
        "HSTRLEN" => {
            let len = hash_get(&mut store, &args[1], &args[2])
                .map(|v| v.len())
                .unwrap_or(0);
            (int(len as i64), false)
        }
        "HSCAN" => {
            let mut out = Vec::new();
            if let Some(Value::Hash(items)) = store.get_live(&args[1]).map(|e| &e.value) {
                for (k, v) in items {
                    out.push(bulk(k));
                    out.push(bulk(v));
                }
            }
            (array(vec![bulk(b"0"), array(out)]), false)
        }

        // ---------- 列表 ----------
        "LPUSH" | "RPUSH" => {
            let items: Vec<Bytes> = args[2..].to_vec();
            let entry = store.data.entry(args[1].clone()).or_default();
            match &mut entry.value {
                Value::List(list) => {
                    if cmd == "LPUSH" {
                        for item in items {
                            list.push_front(item);
                        }
                    } else {
                        for item in items {
                            list.push_back(item);
                        }
                    }
                }
                Value::None => {
                    let mut list = VecDeque::new();
                    if cmd == "LPUSH" {
                        for item in items {
                            list.push_front(item);
                        }
                    } else {
                        for item in items {
                            list.push_back(item);
                        }
                    }
                    entry.value = Value::List(list);
                }
                _ => return (wrong_type(), false),
            }
            let len = match &entry.value {
                Value::List(l) => l.len() as i64,
                _ => 0,
            };
            (int(len), false)
        }
        "RPOP" | "LPOP" => {
            let popped = match store.get_live(&args[1]).map(|e| &mut e.value) {
                Some(Value::List(list)) => {
                    if cmd == "RPOP" {
                        list.pop_back()
                    } else {
                        list.pop_front()
                    }
                }
                Some(_) => return (wrong_type(), false),
                None => None,
            };
            match popped {
                Some(v) => (bulk(&v), false),
                None => (nil(), false),
            }
        }
        "BRPOP" | "BLPOP" => {
            // 测试中不模拟阻塞：无数据立即返回 nil 数组
            let popped = match store.get_live(&args[1]).map(|e| &mut e.value) {
                Some(Value::List(list)) => list.pop_back(),
                _ => None,
            };
            match popped {
                Some(v) => (array(vec![bulk(&args[1]), bulk(&v)]), false),
                None => (nil(), false),
            }
        }
        "LLEN" => {
            let len = match store.get_live(&args[1]).map(|e| &e.value) {
                Some(Value::List(l)) => l.len() as i64,
                _ => 0,
            };
            (int(len), false)
        }
        "LRANGE" => {
            let start = parse_i64(&args[2]).unwrap_or(0);
            let stop = parse_i64(&args[3]).unwrap_or(-1);
            let items = match store.get_live(&args[1]).map(|e| &e.value) {
                Some(Value::List(l)) => {
                    let len = l.len() as i64;
                    let (s, e) = normalize_range(start, stop, len);
                    if s > e {
                        Vec::new()
                    } else {
                        l.iter().skip(s as usize).take((e - s + 1) as usize).map(|v| bulk(v)).collect()
                    }
                }
                _ => Vec::new(),
            };
            (array(items), false)
        }
        "LINDEX" => {
            let index = parse_i64(&args[2]).unwrap_or(0);
            let item = match store.get_live(&args[1]).map(|e| &e.value) {
                Some(Value::List(l)) => {
                    let len = l.len() as i64;
                    let idx = if index < 0 { len + index } else { index };
                    if idx >= 0 && idx < len {
                        Some(l[idx as usize].clone())
                    } else {
                        None
                    }
                }
                _ => None,
            };
            match item {
                Some(v) => (bulk(&v), false),
                None => (nil(), false),
            }
        }
        "LSET" => {
            let index = parse_i64(&args[2]).unwrap_or(0);
            match store.get_live(&args[1]).map(|e| &mut e.value) {
                Some(Value::List(l)) => {
                    let len = l.len() as i64;
                    let idx = if index < 0 { len + index } else { index };
                    if idx >= 0 && idx < len {
                        l[idx as usize] = args[3].clone();
                        (ok(), false)
                    } else {
                        (error("ERR index out of range"), false)
                    }
                }
                _ => (wrong_type(), false),
            }
        }
        "LTRIM" => {
            let start = parse_i64(&args[2]).unwrap_or(0);
            let stop = parse_i64(&args[3]).unwrap_or(-1);
            if let Some(Value::List(l)) = store.get_live(&args[1]).map(|e| &mut e.value) {
                let len = l.len() as i64;
                let (s, e) = normalize_range(start, stop, len);
                let kept: VecDeque<Bytes> = if s > e {
                    VecDeque::new()
                } else {
                    l.iter().skip(s as usize).take((e - s + 1) as usize).cloned().collect()
                };
                *l = kept;
            }
            (ok(), false)
        }
        "LREM" => {
            let count = parse_i64(&args[2]).unwrap_or(0);
            let value = &args[3];
            let mut removed = 0i64;
            if let Some(Value::List(l)) = store.get_live(&args[1]).map(|e| &mut e.value) {
                if count >= 0 {
                    let limit = if count == 0 { usize::MAX } else { count as usize };
                    let mut i = 0;
                    while i < l.len() && (removed as usize) < limit {
                        if &l[i] == value {
                            l.remove(i);
                            removed += 1;
                        } else {
                            i += 1;
                        }
                    }
                } else {
                    let limit = (-count) as usize;
                    let mut i = l.len();
                    while i > 0 && (removed as usize) < limit {
                        i -= 1;
                        if &l[i] == value {
                            l.remove(i);
                            removed += 1;
                        }
                    }
                }
            }
            (int(removed), false)
        }
        "LPOS" => {
            let pos = match store.get_live(&args[1]).map(|e| &e.value) {
                Some(Value::List(l)) => l.iter().position(|v| v == &args[2]).map(|i| i as i64),
                _ => None,
            };
            match pos {
                Some(i) => (int(i), false),
                None => (nil(), false),
            }
        }
        "LINSERT" => {
            let before = args[2].eq_ignore_ascii_case(b"BEFORE");
            let mut len = -1;
            if let Some(Value::List(l)) = store.get_live(&args[1]).map(|e| &mut e.value) {
                if let Some(idx) = l.iter().position(|v| v == &args[3]) {
                    let at = if before { idx } else { idx + 1 };
                    l.insert(at, args[4].clone());
                    len = l.len() as i64;
                } else {
                    len = -1;
                }
            }
            (int(len), false)
        }
        "RPOPLPUSH" | "BRPOPLPUSH" => {
            let popped = match store.get_live(&args[1]).map(|e| &mut e.value) {
                Some(Value::List(l)) => l.pop_back(),
                _ => None,
            };
            match popped {
                Some(v) => {
                    let entry = store.data.entry(args[2].clone()).or_default();
                    match &mut entry.value {
                        Value::List(l) => l.push_front(v.clone()),
                        Value::None => {
                            let mut l = VecDeque::new();
                            l.push_front(v.clone());
                            entry.value = Value::List(l);
                        }
                        _ => return (wrong_type(), false),
                    }
                    (bulk(&v), false)
                }
                None => (nil(), false),
            }
        }

        // ---------- 集合 ----------
        "SADD" => {
            let entry = store.data.entry(args[1].clone()).or_default();
            let mut added = 0;
            match &mut entry.value {
                Value::Set(set) => {
                    for m in &args[2..] {
                        if !set.contains(m) {
                            set.push(m.clone());
                            added += 1;
                        }
                    }
                }
                Value::None => {
                    let mut set = Vec::new();
                    for m in &args[2..] {
                        if !set.contains(m) {
                            set.push(m.clone());
                            added += 1;
                        }
                    }
                    entry.value = Value::Set(set);
                }
                _ => return (wrong_type(), false),
            }
            (int(added), false)
        }
        "SREM" => {
            let mut removed = 0;
            if let Some(Value::Set(set)) = store.get_live(&args[1]).map(|e| &mut e.value) {
                for m in &args[2..] {
                    let before = set.len();
                    set.retain(|v| v != m);
                    if set.len() != before {
                        removed += 1;
                    }
                }
            }
            (int(removed), false)
        }
        "SMEMBERS" => {
            let items = match store.get_live(&args[1]).map(|e| &e.value) {
                Some(Value::Set(set)) => set.iter().map(|v| bulk(v)).collect(),
                _ => Vec::new(),
            };
            (array(items), false)
        }
        "SISMEMBER" => {
            let found = match store.get_live(&args[1]).map(|e| &e.value) {
                Some(Value::Set(set)) => set.contains(&args[2]),
                _ => false,
            };
            (int(if found { 1 } else { 0 }), false)
        }
        "SCARD" => {
            let len = match store.get_live(&args[1]).map(|e| &e.value) {
                Some(Value::Set(set)) => set.len() as i64,
                _ => 0,
            };
            (int(len), false)
        }
        "SPOP" | "SRANDMEMBER" => {
            let count = args.get(2).and_then(|a| parse_i64(a)).unwrap_or(1);
            let mut items = Vec::new();
            if let Some(Value::Set(set)) = store.get_live(&args[1]).map(|e| &mut e.value) {
                let take = (count.max(0) as usize).min(set.len());
                for _ in 0..take {
                    if cmd == "SPOP" {
                        items.push(set.remove(0));
                    } else {
                        items.push(set[0].clone());
                    }
                }
            }
            (array(items.iter().map(|v| bulk(v)).collect()), false)
        }
        "SMOVE" => {
            let member = args[3].clone();
            let removed = match store.get_live(&args[1]).map(|e| &mut e.value) {
                Some(Value::Set(set)) => {
                    let before = set.len();
                    set.retain(|v| v != &member);
                    set.len() != before
                }
                _ => false,
            };
            if removed {
                let entry = store.data.entry(args[2].clone()).or_default();
                match &mut entry.value {
                    Value::Set(set) => set.push(member),
                    Value::None => entry.value = Value::Set(vec![member]),
                    _ => return (wrong_type(), false),
                }
            }
            (int(if removed { 1 } else { 0 }), false)
        }

        // ---------- 有序集合 ----------
        "ZADD" => {
            let mut i = 2;
            let mut added = 0;
            while i + 1 < args.len() {
                let score = match parse_f64(&args[i]) {
                    Some(s) => s,
                    None => return (not_float(), false),
                };
                let member = &args[i + 1];
                let entry = store.data.entry(args[1].clone()).or_default();
                match &mut entry.value {
                    Value::ZSet(items) => match items.iter_mut().find(|(m, _)| m == member) {
                        Some((_, s)) => *s = score,
                        None => {
                            items.push((member.clone(), score));
                            added += 1;
                        }
                    },
                    Value::None => {
                        entry.value = Value::ZSet(vec![(member.clone(), score)]);
                        added += 1;
                    }
                    _ => return (wrong_type(), false),
                }
                i += 2;
            }
            (int(added), false)
        }
        "ZREM" => {
            let mut removed = 0;
            if let Some(Value::ZSet(items)) = store.get_live(&args[1]).map(|e| &mut e.value) {
                for m in &args[2..] {
                    let before = items.len();
                    items.retain(|(k, _)| k != m);
                    if items.len() != before {
                        removed += 1;
                    }
                }
            }
            (int(removed), false)
        }
        "ZSCORE" => {
            let score = match store.get_live(&args[1]).map(|e| &e.value) {
                Some(Value::ZSet(items)) => items
                    .iter()
                    .find(|(m, _)| m == &args[2])
                    .map(|(_, s)| *s),
                _ => None,
            };
            match score {
                Some(s) => (bulk(format!("{s}").as_bytes()), false),
                None => (nil(), false),
            }
        }
        "ZINCRBY" => {
            let delta = parse_f64(&args[2]).unwrap_or(0.0);
            let member = args[3].clone();
            let mut next = delta;
            if let Some(Value::ZSet(items)) = store.get_live(&args[1]).map(|e| &mut e.value) {
                match items.iter_mut().find(|(m, _)| m == &member) {
                    Some((_, s)) => {
                        *s += delta;
                        next = *s;
                    }
                    None => items.push((member, next)),
                }
            }
            (bulk(format!("{next}").as_bytes()), false)
        }
        "ZCARD" => {
            let len = match store.get_live(&args[1]).map(|e| &e.value) {
                Some(Value::ZSet(items)) => items.len() as i64,
                _ => 0,
            };
            (int(len), false)
        }
        "ZCOUNT" => {
            let min = parse_score(&args[2]).unwrap_or(f64::NEG_INFINITY);
            let max = parse_score(&args[3]).unwrap_or(f64::INFINITY);
            let count = match store.get_live(&args[1]).map(|e| &e.value) {
                Some(Value::ZSet(items)) => items.iter().filter(|(_, s)| *s >= min && *s <= max).count() as i64,
                _ => 0,
            };
            (int(count), false)
        }
        "ZRANGE" => {
            let start = parse_i64(&args[2]).unwrap_or(0);
            let stop = parse_i64(&args[3]).unwrap_or(-1);
            let with_scores = args.iter().any(|a| a.eq_ignore_ascii_case(b"WITHSCORES"));
            let mut out = Vec::new();
            if let Some(Value::ZSet(items)) = store.get_live(&args[1]).map(|e| &e.value) {
                let mut sorted = items.clone();
                sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                let len = sorted.len() as i64;
                let (s, e) = normalize_range(start, stop, len);
                if s <= e {
                    for (m, score) in sorted.iter().skip(s as usize).take((e - s + 1) as usize) {
                        out.push(bulk(m));
                        if with_scores {
                            out.push(bulk(format!("{score}").as_bytes()));
                        }
                    }
                }
            }
            (array(out), false)
        }
        "ZRANGEBYSCORE" => {
            let min = parse_score(&args[2]).unwrap_or(f64::NEG_INFINITY);
            let max = parse_score(&args[3]).unwrap_or(f64::INFINITY);
            let with_scores = args.iter().any(|a| a.eq_ignore_ascii_case(b"WITHSCORES"));
            let limit = args
                .iter()
                .position(|a| a.eq_ignore_ascii_case(b"LIMIT"))
                .map(|i| {
                    (
                        parse_i64(&args[i + 1]).unwrap_or(0).max(0) as usize,
                        parse_i64(&args[i + 2]).unwrap_or(0).max(0) as usize,
                    )
                });

            let mut out = Vec::new();
            if let Some(Value::ZSet(items)) = store.get_live(&args[1]).map(|e| &e.value) {
                let mut sorted = items.clone();
                sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                let filtered: Vec<_> = sorted
                    .into_iter()
                    .filter(|(_, s)| *s >= min && *s <= max)
                    .collect();
                let items2 = match limit {
                    Some((offset, count)) => filtered
                        .into_iter()
                        .skip(offset)
                        .take(if count == 0 { usize::MAX } else { count })
                        .collect::<Vec<_>>(),
                    None => filtered,
                };
                for (m, score) in items2 {
                    out.push(bulk(&m));
                    if with_scores {
                        out.push(bulk(format!("{score}").as_bytes()));
                    }
                }
            }
            (array(out), false)
        }
        "ZRANK" | "ZREVRANK" => {
            let mut rank = None;
            if let Some(Value::ZSet(items)) = store.get_live(&args[1]).map(|e| &e.value) {
                let mut sorted = items.clone();
                sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                if cmd == "ZREVRANK" {
                    sorted.reverse();
                }
                rank = sorted.iter().position(|(m, _)| m == &args[2]).map(|i| i as i64);
            }
            match rank {
                Some(i) => (int(i), false),
                None => (nil(), false),
            }
        }
        "ZPOPMIN" | "ZPOPMAX" => {
            let count = args.get(2).and_then(|a| parse_i64(a)).unwrap_or(1).max(0) as usize;
            let mut out = Vec::new();
            if let Some(Value::ZSet(items)) = store.get_live(&args[1]).map(|e| &mut e.value) {
                items.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                for _ in 0..count.min(items.len()) {
                    let idx = if cmd == "ZPOPMIN" { 0 } else { items.len() - 1 };
                    let (m, s) = items.remove(idx);
                    out.push(bulk(&m));
                    out.push(bulk(format!("{s}").as_bytes()));
                }
            }
            (array(out), false)
        }
        "ZREMRANGEBYRANK" => {
            let start = parse_i64(&args[2]).unwrap_or(0);
            let stop = parse_i64(&args[3]).unwrap_or(-1);
            let mut removed = 0;
            if let Some(Value::ZSet(items)) = store.get_live(&args[1]).map(|e| &mut e.value) {
                let mut sorted = items.clone();
                sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                let len = sorted.len() as i64;
                let (s, e) = normalize_range(start, stop, len);
                if s <= e {
                    let victims: Vec<Bytes> = sorted[s as usize..=(e as usize)]
                        .iter()
                        .map(|(m, _)| m.clone())
                        .collect();
                    for v in victims {
                        items.retain(|(m, _)| m != &v);
                        removed += 1;
                    }
                }
            }
            (int(removed), false)
        }
        "ZREMRANGEBYSCORE" => {
            let min = parse_score(&args[2]).unwrap_or(f64::NEG_INFINITY);
            let max = parse_score(&args[3]).unwrap_or(f64::INFINITY);
            let mut removed = 0i64;
            if let Some(Value::ZSet(items)) = store.get_live(&args[1]).map(|e| &mut e.value) {
                let before = items.len();
                items.retain(|(_, s)| *s < min || *s > max);
                removed = (before - items.len()) as i64;
            }
            (int(removed), false)
        }
        "ZSCAN" => {
            let mut out = Vec::new();
            if let Some(Value::ZSet(items)) = store.get_live(&args[1]).map(|e| &e.value) {
                for (m, s) in items {
                    out.push(bulk(m));
                    out.push(bulk(format!("{s}").as_bytes()));
                }
            }
            (array(vec![bulk(b"0"), array(out)]), false)
        }

        // ---------- HyperLogLog（测试用精确集合模拟） ----------
        "PFADD" => {
            let mut changed = 0;
            let mut set = match store.get_live(&args[1]).map(|e| e.value.clone()) {
                Some(Value::Set(s)) => s,
                _ => Vec::new(),
            };
            for item in &args[2..] {
                if !set.contains(item) {
                    set.push(item.clone());
                    changed += 1;
                }
            }
            store.data.entry(args[1].clone()).or_default().value = Value::Set(set);
            (int(if changed > 0 { 1 } else { 0 }), false)
        }
        "PFCOUNT" => {
            let mut set: Vec<Bytes> = Vec::new();
            for key in &args[1..] {
                if let Some(Value::Set(items)) = store.get_live(key).map(|e| &e.value) {
                    for item in items {
                        if !set.contains(item) {
                            set.push(item.clone());
                        }
                    }
                }
            }
            (int(set.len() as i64), false)
        }
        "PFMERGE" => {
            let mut set: Vec<Bytes> = Vec::new();
            for key in &args[2..] {
                if let Some(Value::Set(items)) = store.get_live(key).map(|e| &e.value) {
                    for item in items {
                        if !set.contains(item) {
                            set.push(item.clone());
                        }
                    }
                }
            }
            store.data.entry(args[1].clone()).or_default().value = Value::Set(set);
            (ok(), false)
        }

        _ => (
            error(&format!("ERR unknown command '{}'", String::from_utf8_lossy(&args[0]))),
            false,
        ),
    }
}

fn dispatch_set(store: &mut Store, args: &[Bytes]) -> (Bytes, bool) {
    let key = &args[1];
    let value = &args[2];

    let mut nx = false;
    let mut xx = false;
    let mut get = false;
    let mut expire: Option<Duration> = None;

    let mut i = 3;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GET" => get = true,
            "EX" => {
                let secs = parse_i64(&args[i + 1]).unwrap_or(0);
                expire = Some(Duration::from_secs(secs.max(1) as u64));
                i += 1;
            }
            "PX" => {
                let ms = parse_i64(&args[i + 1]).unwrap_or(0);
                expire = Some(Duration::from_millis(ms.max(1) as u64));
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }

    let old = store.str_value(key);
    let exists = store.get_live(key).is_some();

    if (nx && exists) || (xx && !exists) {
        return if get {
            match old {
                Some(v) => (bulk(&v), false),
                None => (nil(), false),
            }
        } else {
            (nil(), false)
        };
    }

    store.set_str(key, value.clone());
    if let Some(ttl) = expire
        && let Some(e) = store.get_live(key) {
            e.expire_at = Some(Instant::now() + ttl);
        }

    if get {
        match old {
            Some(v) => (bulk(&v), false),
            None => (nil(), false),
        }
    } else {
        (ok(), false)
    }
}

fn hash_set(store: &mut Store, key: &[u8], pairs: Vec<Bytes>) -> i64 {
    let entry = store.data.entry(key.to_vec()).or_default();
    let mut created = 0;

    match &mut entry.value {
        Value::Hash(items) => {
            let mut i = 0;
            while i + 1 < pairs.len() {
                match items.iter_mut().find(|(k, _)| k == &pairs[i]) {
                    Some((_, v)) => *v = pairs[i + 1].clone(),
                    None => {
                        items.push((pairs[i].clone(), pairs[i + 1].clone()));
                        created += 1;
                    }
                }
                i += 2;
            }
        }
        Value::None => {
            let mut items = Vec::new();
            let mut i = 0;
            while i + 1 < pairs.len() {
                items.push((pairs[i].clone(), pairs[i + 1].clone()));
                created += 1;
                i += 2;
            }
            entry.value = Value::Hash(items);
        }
        _ => {}
    }

    created
}

fn hash_get(store: &mut Store, key: &[u8], field: &[u8]) -> Option<Bytes> {
    match store.get_live(key).map(|e| &e.value) {
        Some(Value::Hash(items)) => items
            .iter()
            .find(|(k, _)| k == field)
            .map(|(_, v)| v.clone()),
        _ => None,
    }
}

// ==================== Stream 命令 ====================

fn dispatch_stream(store: &mut Store, cmd: &str, args: &[Bytes]) -> Option<(Bytes, bool)> {
    match cmd {
        "XADD" => Some(xadd(store, args)),
        "XLEN" => Some(xlen(store, args)),
        "XRANGE" => Some(xrange(store, args)),
        "XDEL" => Some(xdel(store, args)),
        "XTRIM" => Some(xtrim(store, args)),
        "XREAD" => Some(xread(store, args)),
        "XREADGROUP" => Some(xreadgroup(store, args)),
        "XACK" => Some(xack(store, args)),
        "XPENDING" => Some(xpending(store, args)),
        "XCLAIM" => Some(xclaim(store, args)),
        "XGROUP" => Some(xgroup(store, args)),
        "XINFO" => Some(xinfo(store, args)),
        _ => None,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn parse_stream_id(bytes: &[u8]) -> Option<(u64, u64)> {
    let text = std::str::from_utf8(bytes).ok()?.trim();
    let (ms, seq) = text.split_once('-')?;
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

fn parse_range_bound(bytes: &[u8], is_start: bool) -> (u64, u64) {
    let text = std::str::from_utf8(bytes).unwrap_or("").trim();
    match text {
        "-" => (0, 0),
        "+" => (u64::MAX, u64::MAX),
        _ => match text.split_once('-') {
            Some((ms, seq)) => {
                let ms: u64 = ms.parse().unwrap_or(0);
                let seq: u64 = seq.parse().unwrap_or(if is_start { 0 } else { u64::MAX });
                (ms, seq)
            }
            None => {
                let ms: u64 = text.parse().unwrap_or(0);
                (ms, if is_start { 0 } else { u64::MAX })
            }
        },
    }
}

fn stream_entry_frame(entry: &StreamEntry) -> Bytes {
    let mut items = Vec::with_capacity(entry.fields.len() * 2);
    for (k, v) in &entry.fields {
        items.push(bulk(k));
        items.push(bulk(v));
    }
    array(vec![bulk(entry.id().as_bytes()), array(items)])
}

fn fresh_stream() -> Value {
    Value::Stream(StreamState::default())
}

fn ensure_stream<'a>(store: &'a mut Store, key: &[u8]) -> &'a mut StreamState {
    let entry = store.data.entry(key.to_vec()).or_default();
    if !matches!(entry.value, Value::Stream(_)) {
        entry.value = fresh_stream();
    }
    match &mut entry.value {
        Value::Stream(s) => s,
        _ => unreachable!(),
    }
}

fn get_stream<'a>(store: &'a mut Store, key: &[u8]) -> Option<&'a mut StreamState> {
    match store.get_live(key).map(|e| &mut e.value) {
        Some(Value::Stream(s)) => Some(s),
        _ => None,
    }
}

fn no_group_error() -> Bytes {
    error("NOGROUP No such key or consumer group in XREADGROUP/XACK/XPENDING")
}

/// 解析 `XADD` / `XTRIM` 的 `MAXLEN [~|=] n` 选项，返回 `(maxlen, 位置)`。
fn parse_maxlen(args: &[Bytes], mut i: usize) -> (Option<u64>, usize) {
    let mut maxlen = None;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "MAXLEN" | "MINID" => {
                i += 1;
                if i < args.len() && (args[i] == b"~" || args[i] == b"=") {
                    i += 1;
                }
                if i < args.len() {
                    if opt == "MAXLEN" {
                        maxlen = parse_i64(&args[i]).map(|n| n.max(0) as u64);
                    }
                    i += 1;
                }
            }
            _ => break,
        }
    }
    (maxlen, i)
}

fn xadd(store: &mut Store, args: &[Bytes]) -> (Bytes, bool) {
    if args.len() < 4 {
        return (error("ERR wrong number of arguments for 'xadd' command"), false);
    }

    let (maxlen, i) = parse_maxlen(args, 2);
    if i >= args.len() {
        return (error("ERR wrong number of arguments for 'xadd' command"), false);
    }

    let id_token = String::from_utf8_lossy(&args[i]).to_string();
    let mut i = i + 1;
    if i >= args.len() || !(args.len() - i).is_multiple_of(2) {
        return (error("ERR wrong number of arguments for 'xadd' command"), false);
    }

    let state = ensure_stream(store, &args[1]);
    let (ms, seq) = if id_token == "*" {
        let ms = now_ms();
        if ms <= state.last_id.0 {
            (state.last_id.0, state.last_id.1 + 1)
        } else {
            (ms, 0)
        }
    } else if let Some(v) = parse_stream_id(id_token.as_bytes()) {
        v
    } else {
        return (
            error("ERR Invalid stream ID specified as stream command argument"),
            false,
        );
    };

    let mut fields = Vec::new();
    while i + 1 < args.len() {
        fields.push((args[i].clone(), args[i + 1].clone()));
        i += 2;
    }

    state.last_id = (ms, seq);
    state.entries_added += 1;
    state.entries.push_back(StreamEntry { ms, seq, fields });

    if let Some(max) = maxlen {
        while state.entries.len() as u64 > max {
            state.entries.pop_front();
        }
    }

    (bulk(format!("{ms}-{seq}").as_bytes()), false)
}

fn xlen(store: &mut Store, args: &[Bytes]) -> (Bytes, bool) {
    let len = get_stream(store, &args[1]).map(|s| s.entries.len()).unwrap_or(0);
    (int(len as i64), false)
}

fn xrange(store: &mut Store, args: &[Bytes]) -> (Bytes, bool) {
    let Some(state) = get_stream(store, &args[1]) else {
        return (array(vec![]), false);
    };

    let start = parse_range_bound(&args[2], true);
    let end = parse_range_bound(&args[3], false);
    let count = args
        .iter()
        .position(|a| a.eq_ignore_ascii_case(b"COUNT"))
        .and_then(|i| args.get(i + 1))
        .and_then(|a| parse_i64(a))
        .unwrap_or(-1);

    let items: Vec<Bytes> = state
        .entries
        .iter()
        .filter(|e| (e.ms, e.seq) >= start && (e.ms, e.seq) <= end)
        .take(if count > 0 { count as usize } else { usize::MAX })
        .map(stream_entry_frame)
        .collect();

    (array(items), false)
}

fn xdel(store: &mut Store, args: &[Bytes]) -> (Bytes, bool) {
    let Some(state) = get_stream(store, &args[1]) else {
        return (int(0), false);
    };

    let mut removed = 0;
    for id in &args[2..] {
        if let Some(v) = parse_stream_id(id) {
            let before = state.entries.len();
            state.entries.retain(|e| !(e.ms == v.0 && e.seq == v.1));
            removed += (before - state.entries.len()) as i64;
        }
    }
    (int(removed), false)
}

fn xtrim(store: &mut Store, args: &[Bytes]) -> (Bytes, bool) {
    let Some(state) = get_stream(store, &args[1]) else {
        return (int(0), false);
    };

    let kind = String::from_utf8_lossy(&args[2]).to_uppercase();
    let mut i = 3;
    if i < args.len() && (args[i] == b"~" || args[i] == b"=") {
        i += 1;
    }
    let Some(value) = args.get(i) else {
        return (error("ERR wrong number of arguments for 'xtrim' command"), false);
    };

    let mut removed = 0;
    match kind.as_str() {
        "MAXLEN" => {
            let max = parse_i64(value).unwrap_or(0).max(0) as u64;
            while state.entries.len() as u64 > max {
                state.entries.pop_front();
                removed += 1;
            }
        }
        "MINID" => {
            let min = parse_range_bound(value, true);
            let before = state.entries.len();
            state.entries.retain(|e| (e.ms, e.seq) >= min);
            removed = (before - state.entries.len()) as i64;
        }
        _ => return (error("ERR syntax error"), false),
    }

    (int(removed), false)
}

fn xread(store: &mut Store, args: &[Bytes]) -> (Bytes, bool) {
    let Some(pos) = args.iter().position(|a| a.eq_ignore_ascii_case(b"STREAMS")) else {
        return (error("ERR syntax error"), false);
    };
    let Some(key) = args.get(pos + 1) else {
        return (error("ERR syntax error"), false);
    };
    let Some(id_arg) = args.get(pos + 2) else {
        return (error("ERR syntax error"), false);
    };

    let count = args
        .iter()
        .position(|a| a.eq_ignore_ascii_case(b"COUNT"))
        .and_then(|i| args.get(i + 1))
        .and_then(|a| parse_i64(a))
        .unwrap_or(-1);

    let Some(state) = get_stream(store, key) else {
        return (nil(), false);
    };

    let from = if id_arg == b"$" {
        state.last_id
    } else if let Some(v) = parse_stream_id(id_arg) {
        v
    } else {
        return (error("ERR Invalid stream ID"), false);
    };

    let items: Vec<Bytes> = state
        .entries
        .iter()
        .filter(|e| (e.ms, e.seq) > from)
        .take(if count > 0 { count as usize } else { usize::MAX })
        .map(stream_entry_frame)
        .collect();

    if items.is_empty() {
        (nil(), false)
    } else {
        (array(vec![array(vec![bulk(key), array(items)])]), false)
    }
}

fn xreadgroup(store: &mut Store, args: &[Bytes]) -> (Bytes, bool) {
    let Some(gpos) = args.iter().position(|a| a.eq_ignore_ascii_case(b"GROUP")) else {
        return (error("ERR syntax error"), false);
    };
    let group_name = String::from_utf8_lossy(&args[gpos + 1]).to_string();
    let consumer_name = String::from_utf8_lossy(&args[gpos + 2]).to_string();

    let Some(pos) = args.iter().position(|a| a.eq_ignore_ascii_case(b"STREAMS")) else {
        return (error("ERR syntax error"), false);
    };
    let key = args[pos + 1].clone();
    let id_arg = args[pos + 2].clone();

    let count = args
        .iter()
        .position(|a| a.eq_ignore_ascii_case(b"COUNT"))
        .and_then(|i| args.get(i + 1))
        .and_then(|a| parse_i64(a))
        .unwrap_or(-1);
    let take = if count > 0 { count as usize } else { usize::MAX };

    let Some(state) = get_stream(store, &key) else {
        return (no_group_error(), false);
    };
    let Some(gidx) = state.groups.iter().position(|g| g.name == group_name) else {
        return (no_group_error(), false);
    };

    // 消费者登记/刷新活跃时间
    let existing = state.groups[gidx]
        .consumers
        .iter()
        .position(|c| c.name == consumer_name);
    match existing {
        Some(i) => state.groups[gidx].consumers[i].last_active = Some(Instant::now()),
        None => state.groups[gidx].consumers.push(StreamConsumer {
            name: consumer_name.clone(),
            last_active: Some(Instant::now()),
        }),
    }

    let mut items: Vec<Bytes> = Vec::new();

    if id_arg == b">" {
        let from = state.groups[gidx].last_delivered;
        let msgs: Vec<StreamEntry> = state
            .entries
            .iter()
            .filter(|e| (e.ms, e.seq) > from)
            .take(take)
            .cloned()
            .collect();

        for entry in &msgs {
            state.groups[gidx].last_delivered = (entry.ms, entry.seq);
            state.groups[gidx].pending.push(StreamPending {
                ms: entry.ms,
                seq: entry.seq,
                consumer: consumer_name.clone(),
                delivery_time: Instant::now(),
                delivery: 1,
            });
            items.push(stream_entry_frame(entry));
        }
    } else {
        let from = parse_stream_id(&id_arg).unwrap_or((0, 0));
        let ids: Vec<(u64, u64)> = state.groups[gidx]
            .pending
            .iter()
            .filter(|p| p.consumer == consumer_name && (p.ms, p.seq) > from)
            .take(take)
            .map(|p| (p.ms, p.seq))
            .collect();

        for pending in state.groups[gidx].pending.iter_mut() {
            if ids.contains(&(pending.ms, pending.seq)) {
                pending.delivery += 1;
                pending.delivery_time = Instant::now();
            }
        }

        for id in &ids {
            if let Some(entry) = state
                .entries
                .iter()
                .find(|e| e.ms == id.0 && e.seq == id.1)
            {
                items.push(stream_entry_frame(entry));
            }
        }
    }

    if items.is_empty() {
        (nil(), false)
    } else {
        (array(vec![array(vec![bulk(&key), array(items)])]), false)
    }
}

fn xack(store: &mut Store, args: &[Bytes]) -> (Bytes, bool) {
    let Some(state) = get_stream(store, &args[1]) else {
        return (no_group_error(), false);
    };
    let group_name = String::from_utf8_lossy(&args[2]).to_string();
    let Some(gidx) = state.groups.iter().position(|g| g.name == group_name) else {
        return (no_group_error(), false);
    };

    let mut removed = 0i64;
    for id in &args[3..] {
        if let Some(v) = parse_stream_id(id) {
            let group = &mut state.groups[gidx];
            let before = group.pending.len();
            group.pending.retain(|p| !(p.ms == v.0 && p.seq == v.1));
            removed += (before - group.pending.len()) as i64;
        }
    }
    (int(removed), false)
}

fn xpending(store: &mut Store, args: &[Bytes]) -> (Bytes, bool) {
    let Some(state) = get_stream(store, &args[1]) else {
        return (no_group_error(), false);
    };
    let group_name = String::from_utf8_lossy(&args[2]).to_string();
    let Some(gidx) = state.groups.iter().position(|g| g.name == group_name) else {
        return (no_group_error(), false);
    };

    if args.len() == 3 {
        // 汇总
        let group = &state.groups[gidx];
        if group.pending.is_empty() {
            return (array(vec![int(0), nil(), nil(), nil()]), false);
        }

        let mut sorted = group.pending.clone();
        sorted.sort_by_key(|p| (p.ms, p.seq));

        let mut consumers: Vec<(String, i64)> = Vec::new();
        for p in &sorted {
            match consumers.iter_mut().find(|(n, _)| *n == p.consumer) {
                Some((_, c)) => *c += 1,
                None => consumers.push((p.consumer.clone(), 1)),
            }
        }

        let consumer_frames: Vec<Bytes> = consumers
            .iter()
            .map(|(name, c)| array(vec![bulk(name.as_bytes()), bulk(c.to_string().as_bytes())]))
            .collect();

        return (
            array(vec![
                int(sorted.len() as i64),
                bulk(sorted[0].id().as_bytes()),
                bulk(sorted[sorted.len() - 1].id().as_bytes()),
                array(consumer_frames),
            ]),
            false,
        );
    }

    // 明细
    let start = parse_range_bound(&args[3], true);
    let end = parse_range_bound(&args[4], false);
    let count = parse_i64(&args[5]).unwrap_or(10);

    let mut sorted = state.groups[gidx].pending.clone();
    sorted.sort_by_key(|p| (p.ms, p.seq));

    let items: Vec<Bytes> = sorted
        .into_iter()
        .filter(|p| (p.ms, p.seq) >= start && (p.ms, p.seq) <= end)
        .take(if count > 0 { count as usize } else { usize::MAX })
        .map(|p| {
            array(vec![
                bulk(p.id().as_bytes()),
                bulk(p.consumer.as_bytes()),
                bulk(p.delivery_time.elapsed().as_millis().to_string().as_bytes()),
                bulk(p.delivery.to_string().as_bytes()),
            ])
        })
        .collect();

    (array(items), false)
}

fn xclaim(store: &mut Store, args: &[Bytes]) -> (Bytes, bool) {
    let Some(state) = get_stream(store, &args[1]) else {
        return (no_group_error(), false);
    };
    let group_name = String::from_utf8_lossy(&args[2]).to_string();
    let consumer_name = String::from_utf8_lossy(&args[3]).to_string();
    let min_idle = parse_i64(&args[4]).unwrap_or(0).max(0) as u128;

    let Some(gidx) = state.groups.iter().position(|g| g.name == group_name) else {
        return (no_group_error(), false);
    };

    // 目标消费者登记
    if !state.groups[gidx]
        .consumers
        .iter()
        .any(|c| c.name == consumer_name)
    {
        state.groups[gidx].consumers.push(StreamConsumer {
            name: consumer_name.clone(),
            last_active: Some(Instant::now()),
        });
    }

    let mut items: Vec<Bytes> = Vec::new();
    for id in &args[5..] {
        if id.eq_ignore_ascii_case(b"JUSTID") || id.eq_ignore_ascii_case(b"IDLE") || id.eq_ignore_ascii_case(b"TIME") || id.eq_ignore_ascii_case(b"RETRYCOUNT") || id.eq_ignore_ascii_case(b"FORCE") {
            continue;
        }
        let Some(v) = parse_stream_id(id) else { continue };

        let claimed = {
            let group = &mut state.groups[gidx];
            match group
                .pending
                .iter_mut()
                .find(|p| p.ms == v.0 && p.seq == v.1)
            {
                Some(p) if p.delivery_time.elapsed().as_millis() >= min_idle => {
                    p.consumer = consumer_name.clone();
                    p.delivery_time = Instant::now();
                    p.delivery += 1;
                    true
                }
                _ => false,
            }
        };

        if claimed
            && let Some(entry) = state.entries.iter().find(|e| e.ms == v.0 && e.seq == v.1) {
                items.push(stream_entry_frame(entry));
            }
    }

    (array(items), false)
}

fn xgroup(store: &mut Store, args: &[Bytes]) -> (Bytes, bool) {
    let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
    let key = args[2].clone();
    let group_name = String::from_utf8_lossy(&args[3]).to_string();

    match sub.as_str() {
        "CREATE" => {
            let start = args.get(4).cloned().unwrap_or_else(|| b"0".to_vec());
            let mkstream = args.iter().any(|a| a.eq_ignore_ascii_case(b"MKSTREAM"));

            let exists = matches!(store.data.get(&key).map(|e| &e.value), Some(Value::Stream(_)));
            if !exists && !mkstream {
                return (error("ERR The XGROUP subcommand requires the key to exist"), false);
            }

            let state = ensure_stream(store, &key);
            if state.groups.iter().any(|g| g.name == group_name) {
                return (error("BUSYGROUP Consumer Group name already exists"), false);
            }

            let last = if start.eq_ignore_ascii_case(b"$") {
                state.last_id
            } else {
                parse_stream_id(&start).unwrap_or((0, 0))
            };

            state.groups.push(StreamGroup {
                name: group_name,
                last_delivered: last,
                consumers: Vec::new(),
                pending: Vec::new(),
            });
            (ok(), false)
        }
        "DESTROY" => {
            let Some(state) = get_stream(store, &key) else {
                return (int(0), false);
            };
            let before = state.groups.len();
            state.groups.retain(|g| g.name != group_name);
            (int((before - state.groups.len()) as i64), false)
        }
        "DELCONSUMER" => {
            let Some(consumer_name) = args.get(4).map(|a| String::from_utf8_lossy(a).to_string()) else {
                return (error("ERR wrong number of arguments for 'xgroup' command"), false);
            };
            let Some(state) = get_stream(store, &key) else {
                return (no_group_error(), false);
            };
            let Some(gidx) = state.groups.iter().position(|g| g.name == group_name) else {
                return (no_group_error(), false);
            };

            let group = &mut state.groups[gidx];
            let pending_count = group.pending.iter().filter(|p| p.consumer == consumer_name).count();
            group.pending.retain(|p| p.consumer != consumer_name);
            group.consumers.retain(|c| c.name != consumer_name);
            (int(pending_count as i64), false)
        }
        "SETID" => {
            let Some(state) = get_stream(store, &key) else {
                return (no_group_error(), false);
            };
            let Some(gidx) = state.groups.iter().position(|g| g.name == group_name) else {
                return (no_group_error(), false);
            };

            let start = args.get(4).cloned().unwrap_or_else(|| b"$".to_vec());
            let last = if start.eq_ignore_ascii_case(b"$") {
                state.last_id
            } else {
                parse_stream_id(&start).unwrap_or((0, 0))
            };
            state.groups[gidx].last_delivered = last;
            (ok(), false)
        }
        _ => (error("ERR unknown XGROUP subcommand"), false),
    }
}

fn xinfo(store: &mut Store, args: &[Bytes]) -> (Bytes, bool) {
    let sub = String::from_utf8_lossy(&args[1]).to_uppercase();

    match sub.as_str() {
        "STREAM" => {
            let Some(state) = get_stream(store, &args[2]) else {
                return (error("ERR no such key"), false);
            };

            let last_id = format!("{}-{}", state.last_id.0, state.last_id.1);
            let first = state.entries.front().map(stream_entry_frame).unwrap_or_else(nil);
            let last = state.entries.back().map(stream_entry_frame).unwrap_or_else(nil);

            let items: Vec<Bytes> = vec![
                bulk(b"length"),
                int(state.entries.len() as i64),
                bulk(b"radix-tree-keys"),
                int(1),
                bulk(b"radix-tree-nodes"),
                int(2),
                bulk(b"last-generated-id"),
                bulk(last_id.as_bytes()),
                bulk(b"entries-added"),
                int(state.entries_added as i64),
                bulk(b"groups"),
                int(state.groups.len() as i64),
                bulk(b"first-entry"),
                first,
                bulk(b"last-entry"),
                last,
            ];

            (array(items), false)
        }
        "GROUPS" => {
            let Some(state) = get_stream(store, &args[2]) else {
                return (error("ERR no such key"), false);
            };

            let groups: Vec<Bytes> = state
                .groups
                .iter()
                .map(|g| {
                    let last = format!("{}-{}", g.last_delivered.0, g.last_delivered.1);
                    array(vec![
                        bulk(b"name"),
                        bulk(g.name.as_bytes()),
                        bulk(b"consumers"),
                        int(g.consumers.len() as i64),
                        bulk(b"pending"),
                        int(g.pending.len() as i64),
                        bulk(b"last-delivered-id"),
                        bulk(last.as_bytes()),
                    ])
                })
                .collect();

            (array(groups), false)
        }
        "CONSUMERS" => {
            let Some(state) = get_stream(store, &args[2]) else {
                return (error("ERR no such key"), false);
            };
            let group_name = String::from_utf8_lossy(&args[3]).to_string();
            let Some(gidx) = state.groups.iter().position(|g| g.name == group_name) else {
                return (no_group_error(), false);
            };

            let consumers: Vec<Bytes> = state.groups[gidx]
                .consumers
                .iter()
                .map(|c| {
                    let pending = state.groups[gidx]
                        .pending
                        .iter()
                        .filter(|p| p.consumer == c.name)
                        .count();
                    let idle = c
                        .last_active
                        .map(|t| t.elapsed().as_millis() as i64)
                        .unwrap_or(0);
                    array(vec![
                        bulk(b"name"),
                        bulk(c.name.as_bytes()),
                        bulk(b"pending"),
                        int(pending as i64),
                        bulk(b"idle"),
                        int(idle),
                    ])
                })
                .collect();

            (array(consumers), false)
        }
        _ => (error("ERR unknown XINFO subcommand"), false),
    }
}

// ==================== 工具 ====================

fn parse_i64(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.trim().parse().ok()
}

fn parse_f64(bytes: &[u8]) -> Option<f64> {
    std::str::from_utf8(bytes).ok()?.trim().parse().ok()
}

fn parse_score(bytes: &[u8]) -> Option<f64> {
    match std::str::from_utf8(bytes).ok()?.trim() {
        "-inf" => Some(f64::NEG_INFINITY),
        "+inf" | "inf" => Some(f64::INFINITY),
        other => other.parse().ok(),
    }
}

fn normalize_range(start: i64, stop: i64, len: i64) -> (i64, i64) {
    let s = if start < 0 { len + start } else { start };
    let e = if stop < 0 { len + stop } else { stop };
    (s.max(0), e.min(len - 1))
}

fn range_slice(data: &[u8], start: i64, end: i64) -> Vec<u8> {
    if data.is_empty() {
        return Vec::new();
    }
    let len = data.len() as i64;
    let (s, e) = normalize_range(start, end, len);
    if s > e {
        Vec::new()
    } else {
        data[s as usize..=(e as usize)].to_vec()
    }
}

/// 简易 glob：支持 `*` 与 `?`（字节级匹配）。
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    fn inner(p: &[u8], t: &[u8]) -> bool {
        if p.is_empty() {
            return t.is_empty();
        }
        match p[0] {
            b'*' => {
                for i in 0..=t.len() {
                    if inner(&p[1..], &t[i..]) {
                        return true;
                    }
                }
                false
            }
            b'?' => !t.is_empty() && inner(&p[1..], &t[1..]),
            c => !t.is_empty() && t[0] == c && inner(&p[1..], &t[1..]),
        }
    }
    inner(pattern, text)
}

/// 读取存储中的字符串值（供断言使用）。
pub fn raw_get(server: &MockRedis, key: &str) -> Option<Bytes> {
    let mut store = server.store.lock().unwrap();
    store.str_value(key.as_bytes())
}

/// 写入一个带过期时间的状态键（模拟死信场景）。
pub fn set_raw(server: &MockRedis, key: &str, value: &str, ttl_seconds: i64) {
    let mut store = server.store.lock().unwrap();
    store.set_str(key.as_bytes(), value.as_bytes().to_vec());
    if ttl_seconds > 0
        && let Some(e) = store.get_live(key.as_bytes()) {
            e.expire_at = Some(Instant::now() + Duration::from_secs(ttl_seconds as u64));
        }
}

/// 键是否存在（含类型）。
pub fn exists(server: &MockRedis, key: &str) -> bool {
    let mut store = server.store.lock().unwrap();
    store.get_live(key.as_bytes()).is_some()
}

/// 列表长度。
pub fn list_len(server: &MockRedis, key: &str) -> usize {
    let mut store = server.store.lock().unwrap();
    match store.get_live(key.as_bytes()).map(|e| &e.value) {
        Some(Value::List(l)) => l.len(),
        _ => 0,
    }
}
