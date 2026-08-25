use std::{net::SocketAddr, time::Duration};

use clap::{Parser, ValueEnum};

use crate::hub::SlowConsumerPolicy;

const DEFAULT_TOPIC_CAPACITY: usize = 256;
const DEFAULT_WS_SEND_TIMEOUT_SECS: u64 = 10;
const DEFAULT_SSE_KEEPALIVE_SECS: u64 = 15;
const DEFAULT_MAX_MESSAGE_BYTES: usize = 65536;

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

    #[arg(
        long,
        env = "HYDRANT_TOPIC_CAPACITY",
        value_parser = parse_capacity,
        default_value_t = DEFAULT_TOPIC_CAPACITY,
    )]
    pub topic_capacity: usize,

    #[arg(
        long,
        env = "HYDRANT_SLOW_CONSUMER_POLICY",
        value_enum,
        default_value_t = SlowConsumerPolicy::Disconnect,
    )]
    pub slow_consumer_policy: SlowConsumerPolicy,

    #[arg(
        long,
        env = "HYDRANT_WS_SEND_TIMEOUT_SECS",
        value_parser = parse_positive_secs,
        default_value_t = DEFAULT_WS_SEND_TIMEOUT_SECS,
    )]
    pub ws_send_timeout_secs: u64,

    #[arg(
        long,
        env = "HYDRANT_SSE_KEEPALIVE_SECS",
        value_parser = parse_positive_secs,
        default_value_t = DEFAULT_SSE_KEEPALIVE_SECS,
    )]
    pub sse_keepalive_secs: u64,

    #[arg(
        long,
        env = "HYDRANT_MAX_MESSAGE_BYTES",
        default_value_t = DEFAULT_MAX_MESSAGE_BYTES,
    )]
    pub max_message_bytes: usize,
}

impl Config {
    #[must_use]
    pub fn settings(&self) -> Settings {
        Settings {
            topic_capacity: self.topic_capacity,
            slow_consumer_policy: self.slow_consumer_policy,
            ws_send_timeout: Duration::from_secs(self.ws_send_timeout_secs),
            sse_keepalive: Duration::from_secs(self.sse_keepalive_secs),
            max_message_bytes: self.max_message_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub topic_capacity: usize,
    pub slow_consumer_policy: SlowConsumerPolicy,
    pub ws_send_timeout: Duration,
    pub sse_keepalive: Duration,
    pub max_message_bytes: usize,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            topic_capacity: DEFAULT_TOPIC_CAPACITY,
            slow_consumer_policy: SlowConsumerPolicy::Disconnect,
            ws_send_timeout: Duration::from_secs(DEFAULT_WS_SEND_TIMEOUT_SECS),
            sse_keepalive: Duration::from_secs(DEFAULT_SSE_KEEPALIVE_SECS),
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
        }
    }
}

fn parse_capacity(raw: &str) -> Result<usize, String> {
    let value: usize = raw.parse().map_err(|e| format!("{e}"))?;
    if (1..=usize::MAX >> 1).contains(&value) {
        Ok(value)
    } else {
        Err("must be between 1 and usize::MAX / 2".to_owned())
    }
}

fn parse_positive_secs(raw: &str) -> Result<u64, String> {
    let value: u64 = raw.parse().map_err(|e| format!("{e}"))?;
    if value == 0 {
        Err("must be at least 1".to_owned())
    } else {
        Ok(value)
    }
}
