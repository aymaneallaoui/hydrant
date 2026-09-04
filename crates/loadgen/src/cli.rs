use std::{net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, bail};
use clap::{Parser, ValueEnum};
use serde::Serialize;

use crate::payload::MIN_PAYLOAD_BYTES;

const MAX_TOPIC_LEN: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    Ws,
    Sse,
}

#[derive(Debug, Clone, Parser)]
#[command(version, about = "Load generator for hydrant-server")]
pub struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    pub url: String,

    #[arg(long, default_value = "loadgen", value_parser = parse_topic)]
    pub topic: String,

    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..))]
    pub connections: u64,

    #[arg(long, value_enum, default_value_t = Transport::Ws)]
    pub transport: Transport,

    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u32).range(1..))]
    pub rate: u32,

    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
    pub duration: u64,

    #[arg(long, default_value_t = 64, value_parser = parse_payload_bytes)]
    pub payload_bytes: usize,

    #[arg(long, default_value_t = 256, value_parser = clap::value_parser!(u64).range(1..))]
    pub connect_concurrency: u64,

    #[arg(long, default_value_t = 5)]
    pub drain_timeout: u64,

    #[arg(long, value_name = "PATH")]
    pub json: Option<PathBuf>,
}

impl Args {
    #[must_use]
    pub fn duration(&self) -> Duration {
        Duration::from_secs(self.duration)
    }

    #[must_use]
    pub fn drain_timeout(&self) -> Duration {
        Duration::from_secs(self.drain_timeout)
    }

    pub fn connections(&self) -> anyhow::Result<usize> {
        usize::try_from(self.connections).context("--connections is too large")
    }

    pub fn connect_concurrency(&self) -> anyhow::Result<usize> {
        usize::try_from(self.connect_concurrency)
            .map(|n| n.min(tokio::sync::Semaphore::MAX_PERMITS))
            .context("--connect-concurrency is too large")
    }
}

fn parse_topic(raw: &str) -> Result<String, String> {
    let valid = !raw.is_empty()
        && raw.len() <= MAX_TOPIC_LEN
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if valid {
        Ok(raw.to_owned())
    } else {
        Err(format!(
            "must be 1 to {MAX_TOPIC_LEN} characters from [A-Za-z0-9._-]"
        ))
    }
}

fn parse_payload_bytes(raw: &str) -> Result<usize, String> {
    let value: usize = raw.parse().map_err(|e| format!("{e}"))?;
    if value < MIN_PAYLOAD_BYTES {
        Err(format!(
            "must be at least {MIN_PAYLOAD_BYTES} to fit the timestamp and sequence header"
        ))
    } else {
        Ok(value)
    }
}

#[derive(Debug, Clone)]
pub struct Target {
    pub addr: SocketAddr,
    pub authority: String,
}

pub async fn resolve_target(url: &str) -> anyhow::Result<Target> {
    let (scheme, rest) = url
        .split_once("://")
        .with_context(|| format!("--url {url:?} has no scheme (expected http:// or ws://)"))?;
    match scheme.to_ascii_lowercase().as_str() {
        "http" | "ws" => {}
        "https" | "wss" => bail!("TLS is not supported: use an http:// or ws:// URL"),
        other => bail!("unsupported URL scheme {other:?} (expected http:// or ws://)"),
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.is_empty() || authority.contains('@') {
        bail!("--url {url:?} needs a plain host[:port] authority");
    }
    let (host, port) = split_host_port(authority)?;
    let addr = tokio::net::lookup_host((host, port))
        .await
        .with_context(|| format!("resolving {authority}"))?
        .next()
        .with_context(|| format!("{authority} resolved to no addresses"))?;
    Ok(Target {
        addr,
        authority: authority.to_owned(),
    })
}

fn split_host_port(authority: &str) -> anyhow::Result<(&str, u16)> {
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, tail) = rest
            .split_once(']')
            .with_context(|| format!("unterminated IPv6 literal in {authority:?}"))?;
        (host, tail.strip_prefix(':'))
    } else {
        match authority.rsplit_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (authority, None),
        }
    };
    let port = match port {
        Some(raw) => raw
            .parse()
            .with_context(|| format!("invalid port {raw:?} in {authority:?}"))?,
        None => 80,
    };
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn defaults_parse() {
        let args = Args::try_parse_from(["loadgen"]).expect("defaults");
        assert_eq!(args.connections, 100);
        assert_eq!(args.transport, Transport::Ws);
        assert_eq!(args.payload_bytes, 64);
    }

    #[test]
    fn rejects_too_small_payload() {
        let err = Args::try_parse_from(["loadgen", "--payload-bytes", "10"]).expect_err("small");
        assert!(err.to_string().contains("at least"), "{err}");
    }

    #[test]
    fn rejects_zero_rate_and_bad_topic() {
        assert!(Args::try_parse_from(["loadgen", "--rate", "0"]).is_err());
        assert!(Args::try_parse_from(["loadgen", "--topic", "bad topic"]).is_err());
        assert!(Args::try_parse_from(["loadgen", "--connections", "0"]).is_err());
    }

    #[tokio::test]
    async fn resolves_plain_urls_and_rejects_tls() {
        let target = resolve_target("http://127.0.0.1:9000/ignored")
            .await
            .expect("resolve");
        assert_eq!(target.addr.port(), 9000);
        assert_eq!(target.authority, "127.0.0.1:9000");
        assert_eq!(
            resolve_target("ws://127.0.0.1")
                .await
                .expect("ws")
                .addr
                .port(),
            80
        );
        let err = resolve_target("https://127.0.0.1").await.expect_err("tls");
        assert!(err.to_string().contains("TLS"), "{err}");
        assert!(resolve_target("127.0.0.1:80").await.is_err());
    }
}
