use std::net::SocketAddr;

use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    Pretty,
    Json,
}

#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Config {
    #[arg(long, env = "HYDRANT_LISTEN_ADDR", default_value = "127.0.0.1:8080")]
    pub listen_addr: SocketAddr,

    #[arg(long, env = "HYDRANT_LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    #[arg(long, env = "HYDRANT_LOG_FORMAT", value_enum, default_value_t = LogFormat::Pretty)]
    pub log_format: LogFormat,

    #[arg(long, env = "HYDRANT_SHUTDOWN_TIMEOUT_SECS", default_value_t = 10)]
    pub shutdown_timeout_secs: u64,
}
