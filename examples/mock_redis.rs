//! 独立可执行的迷你 Redis（RESP2），用于在没有真实 Redis 的机器上做跨语言联调。
//!
//! ```powershell
//! # 终端 1：启动迷你 Redis（默认 127.0.0.1:16379）
//! cargo run --example mock_redis
//!
//! # 终端 2：C# 侧写入 + 自检
//! dotnet run --project demo\csharp\PekRRedisDemo -- auto --config "server=127.0.0.1:16379;db=0"
//!
//! # 终端 3：Rust 侧校验 C# 写入的数据
//! cargo run --example demo -- verify --config "server=127.0.0.1:16379;db=0"
//! ```
//!
//! 注意：这是测试用的最小实现（命令子集），仅用于 Demo/联调；生产请连接真实 Redis。
//! 按 Enter 退出。

#[path = "../tests/support/mod.rs"]
mod support;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let get_opt = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let port: u16 = args
        .iter()
        .position(|a| a == "--port")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(16_379);

    let server = support::start_mock_redis_on(port);

    if let Some(mode) = get_opt("--info-mode") {
        support::set_info_mode(&server, &mode);
    }
    if let Some(path) = get_opt("--info-text-file") {
        let text = std::fs::read_to_string(path).expect("read --info-text-file");
        support::set_info_text(&server, &text);
    }
    if let Some(path) = get_opt("--info-replication-file") {
        let text = std::fs::read_to_string(path).expect("read --info-replication-file");
        support::set_info_replication(&server, &text);
    }
    if let Some(path) = get_opt("--info-sentinel-file") {
        let text = std::fs::read_to_string(path).expect("read --info-sentinel-file");
        support::set_info_sentinel(&server, &text);
    }
    if let Some(path) = get_opt("--cluster-nodes-file") {
        let text = std::fs::read_to_string(path).expect("read --cluster-nodes-file");
        support::set_cluster_nodes(&server, &text);
    }

    println!("[mock_redis] 已启动：{}（RESP2 子集实现，按 Enter 退出）", server.addr);
    println!("MOCK_ADDR={}", server.addr);

    let mut line = String::new();
    let _ = std::io::stdin().read_line(&mut line);
    println!("[mock_redis] 退出");
}
