use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use tokio::{net::TcpStream, time::timeout};

use anyhow::{Context, bail};
use clap::{Parser, ValueEnum};
use serde::Serialize;

use crate::payload::MIN_PAYLOAD_BYTES;

const MAX_TOPIC_LEN: usize = 128;
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_SPREAD_LOOPBACK: u32 = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    Ws,
    Sse,
}

#[derive(Debug, Clone, Parser)]
#[command(
    version,
    about = "Load generator for hydrant-server",
    after_help = "Exit codes: 0 run completed, 1 error, 2 no publish succeeded, \
3 loss above --max-loss-percent"
)]
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

    #[arg(
        long,
        value_name = "N",
        default_value_t = 1,
        value_parser = clap::value_parser!(u32).range(1..),
        help = "Spread subscriber connections round-robin over N consecutive IPv4 addresses \
starting at the --url host, which must be in 127.0.0.0/8; the publisher keeps the --url host",
        long_help = "Spread subscriber connections round-robin over N consecutive IPv4 \
addresses starting at the --url host (for example 127.0.0.1 to 127.0.0.64 with N=64). The \
host must be in 127.0.0.0/8, every address in that block is local, and the server must \
listen on 0.0.0.0. This lifts the per-destination ephemeral port cap without root. The \
publisher keeps using the --url host."
    )]
    pub spread_loopback: u32,

    #[arg(
        long,
        value_name = "SECS",
        default_value_t = 0,
        help = "Hold established connections idle for SECS before publishing"
    )]
    pub hold_idle: u64,

    #[arg(long, value_name = "PATH")]
    pub json: Option<PathBuf>,

    #[arg(long, value_name = "PERCENT", value_parser = parse_percent)]
    pub max_loss_percent: Option<f64>,
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

    #[must_use]
    pub fn hold_idle(&self) -> Duration {
        Duration::from_secs(self.hold_idle)
    }

    pub fn connections(&self) -> anyhow::Result<usize> {
        usize::try_from(self.connections).context("--connections is too large")
    }

    pub fn validate_spread(&self) -> anyhow::Result<()> {
        if self.spread_loopback > MAX_SPREAD_LOOPBACK {
            bail!(
                "--spread-loopback {} exceeds the maximum of {MAX_SPREAD_LOOPBACK}",
                self.spread_loopback
            );
        }
        if u64::from(self.spread_loopback) > self.connections {
            bail!(
                "--spread-loopback {} exceeds --connections {}",
                self.spread_loopback,
                self.connections
            );
        }
        Ok(())
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

fn parse_percent(raw: &str) -> Result<f64, String> {
    let value: f64 = raw.parse().map_err(|e| format!("{e}"))?;
    if value.is_finite() && (0.0..=100.0).contains(&value) {
        Ok(value)
    } else {
        Err("must be between 0 and 100".to_owned())
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

#[derive(Debug, Clone)]
pub struct Candidates {
    pub addrs: Vec<SocketAddr>,
    pub authority: String,
}

impl Candidates {
    pub async fn first_reachable(self) -> anyhow::Result<Target> {
        let mut failures = Vec::new();
        for addr in &self.addrs {
            match timeout(PROBE_TIMEOUT, TcpStream::connect(addr)).await {
                Ok(Ok(_)) => {
                    return Ok(Target {
                        addr: *addr,
                        authority: self.authority,
                    });
                }
                Ok(Err(err)) => failures.push(format!("{addr}: {err}")),
                Err(_) => failures.push(format!("{addr}: timed out")),
            }
        }
        bail!(
            "no address for {} accepted a TCP connection ({})",
            self.authority,
            failures.join("; ")
        )
    }
}

pub fn spread_targets(base: &Target, count: u32) -> anyhow::Result<Vec<Target>> {
    if count <= 1 {
        return Ok(vec![base.clone()]);
    }
    let IpAddr::V4(first) = base.addr.ip() else {
        bail!(
            "--spread-loopback needs an IPv4 --url host, got {}",
            base.addr.ip()
        );
    };
    if first.octets()[0] != 127 {
        bail!("--spread-loopback needs a --url host in 127.0.0.0/8, got {first}");
    }
    let start = u32::from(first);
    let last = start
        .checked_add(count - 1)
        .map(Ipv4Addr::from)
        .filter(|addr| addr.octets()[0] == 127)
        .with_context(|| format!("--spread-loopback {count} from {first} leaves 127.0.0.0/8"))?;
    let port = base.addr.port();
    debug_assert!(last >= first);
    Ok((0..count)
        .map(|offset| {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::from(start + offset)), port);
            Target {
                addr,
                authority: addr.to_string(),
            }
        })
        .collect())
}

pub async fn resolve_target(url: &str) -> anyhow::Result<Target> {
    resolve_candidates(url).await?.first_reachable().await
}

pub async fn resolve_candidates(url: &str) -> anyhow::Result<Candidates> {
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
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .with_context(|| format!("resolving {authority}"))?
        .collect();
    if addrs.is_empty() {
        bail!("{authority} resolved to no addresses");
    }
    Ok(Candidates {
        addrs,
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
        let found = resolve_candidates("http://127.0.0.1:9000/ignored")
            .await
            .expect("resolve");
        assert_eq!(found.addrs, vec!["127.0.0.1:9000".parse().expect("addr")]);
        assert_eq!(found.authority, "127.0.0.1:9000");
        assert_eq!(
            resolve_candidates("ws://127.0.0.1")
                .await
                .expect("ws")
                .addrs[0]
                .port(),
            80
        );
        let err = resolve_candidates("https://127.0.0.1")
            .await
            .expect_err("tls");
        assert!(err.to_string().contains("TLS"), "{err}");
        assert!(resolve_candidates("127.0.0.1:80").await.is_err());
    }

    fn base_target(addr: &str) -> Target {
        let addr: SocketAddr = addr.parse().expect("addr");
        Target {
            addr,
            authority: addr.to_string(),
        }
    }

    #[test]
    fn spread_targets_are_consecutive_and_keep_the_port() {
        let targets = spread_targets(&base_target("127.0.0.254:9000"), 4).expect("spread");
        let addrs: Vec<String> = targets.iter().map(|t| t.addr.to_string()).collect();
        assert_eq!(
            addrs,
            [
                "127.0.0.254:9000",
                "127.0.0.255:9000",
                "127.0.1.0:9000",
                "127.0.1.1:9000"
            ]
        );
        assert_eq!(targets[3].authority, "127.0.1.1:9000");
    }

    #[test]
    fn spread_of_one_is_the_base_target() {
        let base = base_target("10.1.2.3:80");
        let targets = spread_targets(&base, 1).expect("spread");
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].addr, base.addr);
    }

    #[test]
    fn spread_rejects_non_loopback_ipv6_and_overflow() {
        assert!(spread_targets(&base_target("10.0.0.1:80"), 2).is_err());
        assert!(spread_targets(&base_target("[::1]:80"), 2).is_err());
        assert!(spread_targets(&base_target("127.255.255.255:80"), 2).is_err());
        assert!(spread_targets(&base_target("127.255.255.255:80"), 1).is_ok());
    }

    #[test]
    fn spread_and_hold_flags_parse() {
        let args = Args::try_parse_from(["loadgen", "--spread-loopback", "64", "--hold-idle", "5"])
            .expect("args");
        assert_eq!((args.spread_loopback, args.hold_idle), (64, 5));
        assert!(Args::try_parse_from(["loadgen", "--spread-loopback", "0"]).is_err());
    }

    #[test]
    fn spread_is_capped_by_connections_and_an_absolute_maximum() {
        let parse = |extra: &[&str]| {
            let mut argv = vec!["loadgen"];
            argv.extend_from_slice(extra);
            Args::try_parse_from(argv).expect("args")
        };
        assert!(
            parse(&["--connections", "64", "--spread-loopback", "64"])
                .validate_spread()
                .is_ok()
        );
        let err = parse(&["--connections", "10", "--spread-loopback", "64"])
            .validate_spread()
            .expect_err("more addresses than connections");
        assert!(err.to_string().contains("exceeds --connections"), "{err}");
        let err = parse(&[
            "--connections",
            "100000000",
            "--spread-loopback",
            "16000000",
        ])
        .validate_spread()
        .expect_err("absurd spread");
        assert!(err.to_string().contains("maximum"), "{err}");
    }

    fn closed_port_addr() -> SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        listener.local_addr().expect("addr")
    }

    #[tokio::test]
    async fn probe_skips_unreachable_addresses() {
        let dead = closed_port_addr();
        let live_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let live = live_listener.local_addr().expect("addr");
        let target = Candidates {
            addrs: vec![dead, live],
            authority: "test".to_owned(),
        }
        .first_reachable()
        .await
        .expect("second address is reachable");
        assert_eq!(target.addr, live);
    }

    #[tokio::test]
    async fn probe_failure_lists_every_tried_address() {
        let (a, b) = (closed_port_addr(), closed_port_addr());
        let err = Candidates {
            addrs: vec![a, b],
            authority: "test".to_owned(),
        }
        .first_reachable()
        .await
        .expect_err("nothing listens");
        let message = err.to_string();
        assert!(message.contains(&a.to_string()), "{message}");
        assert!(message.contains(&b.to_string()), "{message}");
    }

    #[test]
    fn max_loss_percent_is_validated() {
        let ok = Args::try_parse_from(["loadgen", "--max-loss-percent", "0.5"]).expect("ok");
        assert_eq!(ok.max_loss_percent, Some(0.5));
        assert!(Args::try_parse_from(["loadgen", "--max-loss-percent", "101"]).is_err());
        assert!(Args::try_parse_from(["loadgen", "--max-loss-percent", "-1"]).is_err());
        assert!(Args::try_parse_from(["loadgen", "--max-loss-percent", "NaN"]).is_err());
    }
}
