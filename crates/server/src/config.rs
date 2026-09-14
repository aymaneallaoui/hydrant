use std::{net::SocketAddr, time::Duration};

use clap::{Parser, ValueEnum};

use crate::hub::SlowConsumerPolicy;

const DEFAULT_TOPIC_CAPACITY: usize = 256;
const DEFAULT_SEND_TIMEOUT_SECS: u64 = 10;
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
        env = "HYDRANT_SEND_TIMEOUT_SECS",
        value_parser = parse_positive_secs,
        default_value_t = DEFAULT_SEND_TIMEOUT_SECS,
    )]
    pub send_timeout_secs: u64,

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

    #[arg(long, env = "HYDRANT_MAX_CONNECTIONS", value_parser = parse_max_connections)]
    pub max_connections: Option<usize>,
}

impl Config {
    #[must_use]
    pub fn settings(&self) -> Settings {
        Settings {
            topic_capacity: self.topic_capacity,
            slow_consumer_policy: self.slow_consumer_policy,
            send_timeout: Duration::from_secs(self.send_timeout_secs),
            sse_keepalive: Duration::from_secs(self.sse_keepalive_secs),
            max_message_bytes: self.max_message_bytes,
            max_connections: self.max_connections,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Settings {
    pub topic_capacity: usize,
    pub slow_consumer_policy: SlowConsumerPolicy,
    pub send_timeout: Duration,
    pub sse_keepalive: Duration,
    pub max_message_bytes: usize,
    pub max_connections: Option<usize>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            topic_capacity: DEFAULT_TOPIC_CAPACITY,
            slow_consumer_policy: SlowConsumerPolicy::Disconnect,
            send_timeout: Duration::from_secs(DEFAULT_SEND_TIMEOUT_SECS),
            sse_keepalive: Duration::from_secs(DEFAULT_SSE_KEEPALIVE_SECS),
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            max_connections: None,
        }
    }
}

fn parse_capacity(raw: &str) -> Result<usize, String> {
    let value: usize = raw.parse().map_err(|e| format!("{e}"))?;
    if !(1..=usize::MAX >> 1).contains(&value) {
        return Err("must be between 1 and usize::MAX / 2".to_owned());
    }
    if value.is_power_of_two() {
        return Ok(value);
    }
    let above = value.next_power_of_two();
    Err(format!(
        "must be a power of two; nearest valid values are {} and {above}",
        above >> 1
    ))
}

fn parse_positive_secs(raw: &str) -> Result<u64, String> {
    let value: u64 = raw.parse().map_err(|e| format!("{e}"))?;
    if value == 0 {
        Err("must be at least 1".to_owned())
    } else {
        Ok(value)
    }
}

fn parse_max_connections(raw: &str) -> Result<usize, String> {
    let value: usize = raw.parse().map_err(|e| format!("{e}"))?;
    if value == 0 {
        Err("must be at least 1".to_owned())
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capacity_accepts_powers_of_two() {
        for value in [1_usize, 2, 256, 1024] {
            assert_eq!(parse_capacity(&value.to_string()), Ok(value));
        }
    }

    #[test]
    fn capacity_rejects_non_powers_naming_neighbours() {
        let err = parse_capacity("300").expect_err("300 is not a power of two");
        assert!(err.contains("256") && err.contains("512"), "{err}");
        let err = parse_capacity("257").expect_err("257 is not a power of two");
        assert!(err.contains("256") && err.contains("512"), "{err}");
        assert!(parse_capacity("3").expect_err("3").contains("2 and 4"));
    }

    #[test]
    fn capacity_rejects_zero_and_oversized() {
        assert!(parse_capacity("0").is_err());
        assert!(parse_capacity(&usize::MAX.to_string()).is_err());
        assert!(parse_capacity("abc").is_err());
    }
}
