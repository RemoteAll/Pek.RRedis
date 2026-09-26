//! 统一错误类型。
//!
//! 对应 DH.NRedis 的 `RedisException` / 连接池异常 / 编码异常等，
//! 按来源分类，便于上层区分「网络/协议/服务端/类型/配置」问题。

use thiserror::Error;

/// 库统一错误。
#[derive(Debug, Error)]
pub enum Error {
    /// 网络 I/O 错误（连接断开、超时等）
    #[error("I/O 错误：{0}")]
    Io(#[from] std::io::Error),

    /// 协议解析错误（对端返回了非法 RESP 数据）
    #[error("协议错误：{0}")]
    Protocol(String),

    /// Redis 服务端错误回复（`-ERR ...`）
    #[error("Redis 服务端错误：{0}")]
    Server(String),

    /// 类型转换错误（解码为目标类型失败）
    #[error("类型转换错误：{0}")]
    Type(String),

    /// 连接池错误（池满、借出超时、创建失败）
    #[error("连接池错误：{0}")]
    Pool(String),

    /// 操作被拒绝（违反客户端保护策略，如禁用了全量 KEYS）
    #[error("操作被拒绝：{0}")]
    Operation(String),

    /// 配置错误（连接字符串非法、缺少服务器地址等）
    #[error("配置错误：{0}")]
    Config(String),

    /// JSON 序列化/反序列化错误
    #[error("JSON 错误：{0}")]
    Json(#[from] serde_json::Error),

    /// 功能暂未支持
    #[error("暂不支持：{0}")]
    Unsupported(String),
}

impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Io(io) => io,
            other => std::io::Error::other(other.to_string()),
        }
    }
}

/// 库统一结果类型。
pub type Result<T> = std::result::Result<T, Error>;
