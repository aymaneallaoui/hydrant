use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{
        Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;
use serde::Serialize;

use crate::{
    cli::{Args, Transport},
    payload,
    subscriber::{ConnectFailure, EndReason},
};

const HISTOGRAM_MAX_MICROS: u64 = 3_600_000_000;
const MAX_SHARDS: usize = 16;
const MAX_DISTINCT_ERRORS: usize = 8;

pub struct Shared {
    base: Instant,
    shards: Vec<Mutex<Histogram<u64>>>,
    pub established: AtomicU64,
    pub connect_failed: AtomicU64,
    pub closed_early: AtomicU64,
    pub received: AtomicU64,
    pub malformed: AtomicU64,
    pub published_ok: AtomicU64,
    pub published_failed: AtomicU64,
    fd_exhausted: AtomicU64,
    last_receive_nanos: AtomicU64,
    closed_reasons: Mutex<BTreeMap<String, u64>>,
    connect_errors: Mutex<BTreeMap<String, u64>>,
}

impl Shared {
    #[must_use]
    pub fn new() -> Self {
        let shard_count = std::thread::available_parallelism()
            .map_or(1, std::num::NonZero::get)
            .min(MAX_SHARDS);
        let shards = (0..shard_count)
            .map(|_| {
                Mutex::new(
                    Histogram::new_with_bounds(1, HISTOGRAM_MAX_MICROS, 3)
                        .expect("histogram bounds are valid constants"),
                )
            })
            .collect();
        Self {
            base: Instant::now(),
            shards,
            established: AtomicU64::new(0),
            connect_failed: AtomicU64::new(0),
            closed_early: AtomicU64::new(0),
            received: AtomicU64::new(0),
            malformed: AtomicU64::new(0),
            published_ok: AtomicU64::new(0),
            published_failed: AtomicU64::new(0),
            fd_exhausted: AtomicU64::new(0),
            last_receive_nanos: AtomicU64::new(0),
            closed_reasons: Mutex::new(BTreeMap::new()),
            connect_errors: Mutex::new(BTreeMap::new()),
        }
    }

    #[must_use]
    pub fn elapsed_nanos(&self) -> u64 {
        u64::try_from(self.base.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    pub fn record_payload(&self, shard_hint: usize, payload: &[u8]) {
        let Some((sent_nanos, _seq)) = payload::decode(payload) else {
            self.malformed.fetch_add(1, Relaxed);
            return;
        };
        let micros = payload::unix_nanos().saturating_sub(sent_nanos) / 1000;
        lock(&self.shards[shard_hint % self.shards.len()]).saturating_record(micros.max(1));
        self.last_receive_nanos
            .fetch_max(self.elapsed_nanos(), Relaxed);
        self.received.fetch_add(1, Relaxed);
    }

    pub fn record_connect_failure(&self, failure: &ConnectFailure) {
        self.connect_failed.fetch_add(1, Relaxed);
        if failure.fd_exhausted {
            self.fd_exhausted.fetch_add(1, Relaxed);
        }
        count_capped(&self.connect_errors, &failure.message);
    }

    pub fn record_closed_early(&self, reason: &EndReason) {
        self.closed_early.fetch_add(1, Relaxed);
        count_capped(&self.closed_reasons, &reason.to_string());
    }

    #[must_use]
    pub fn fd_exhausted(&self) -> u64 {
        self.fd_exhausted.load(Relaxed)
    }

    #[must_use]
    pub fn first_connect_error(&self) -> Option<String> {
        lock(&self.connect_errors).keys().next().cloned()
    }

    #[must_use]
    pub fn report(&self, args: &Args, facts: &RunFacts) -> Report {
        let mut merged = Histogram::<u64>::new_with_bounds(1, HISTOGRAM_MAX_MICROS, 3)
            .expect("histogram bounds are valid constants");
        for shard in &self.shards {
            merged
                .add(&*lock(shard))
                .expect("shards share the merged histogram's bounds");
        }
        let established = self.established.load(Relaxed);
        let received = self.received.load(Relaxed);
        let published_ok = self.published_ok.load(Relaxed);
        let expected = published_ok.saturating_mul(established);
        let missing = expected.saturating_sub(received);
        let connect_secs = facts.connect_elapsed.as_secs_f64();
        let window_secs = facts.publish_start_nanos.map_or(0.0, |start| {
            to_f64(self.last_receive_nanos.load(Relaxed).saturating_sub(start)) / 1e9
        });
        Report {
            params: Params::from_args(args),
            interrupted: facts.interrupted,
            connections: ConnectionStats {
                requested: args.connections,
                established,
                failed: self.connect_failed.load(Relaxed),
                closed_early: self.closed_early.load(Relaxed),
                closed_early_reasons: lock(&self.closed_reasons).clone(),
                connect_errors: lock(&self.connect_errors).clone(),
                fd_limit_failures: self.fd_exhausted(),
                connect_secs: round3(connect_secs),
                connects_per_sec: round3(ratio(to_f64(established), connect_secs)),
            },
            publish: PublishStats {
                ok: published_ok,
                failed: self.published_failed.load(Relaxed),
            },
            deliveries: DeliveryStats {
                expected,
                received,
                missing,
                loss_percent: round3(ratio(to_f64(missing), to_f64(expected)) * 100.0),
                malformed: self.malformed.load(Relaxed),
            },
            latency_ms: (!merged.is_empty()).then(|| LatencyMs {
                min: micros_to_ms(merged.min()),
                mean: round3(merged.mean() / 1000.0),
                p50: micros_to_ms(merged.value_at_quantile(0.5)),
                p90: micros_to_ms(merged.value_at_quantile(0.9)),
                p99: micros_to_ms(merged.value_at_quantile(0.99)),
                p999: micros_to_ms(merged.value_at_quantile(0.999)),
                max: micros_to_ms(merged.max()),
            }),
            throughput: Throughput {
                deliveries_per_sec: round3(ratio(to_f64(received), window_secs)),
                window_secs: round3(window_secs),
            },
        }
    }
}

impl Default for Shared {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RunFacts {
    pub connect_elapsed: Duration,
    pub publish_start_nanos: Option<u64>,
    pub interrupted: bool,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn count_capped(map: &Mutex<BTreeMap<String, u64>>, key: &str) {
    let mut map = lock(map);
    if let Some(count) = map.get_mut(key) {
        *count += 1;
    } else if map.len() < MAX_DISTINCT_ERRORS {
        map.insert(key.to_owned(), 1);
    } else {
        *map.entry("other".to_owned()).or_insert(0) += 1;
    }
}

#[allow(clippy::cast_precision_loss)] // counts and nanosecond spans stay far below 2^53
fn to_f64(value: u64) -> f64 {
    value as f64
}

fn micros_to_ms(micros: u64) -> f64 {
    to_f64(micros) / 1000.0
}

fn round3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

fn ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator > 0.0 {
        numerator / denominator
    } else {
        0.0
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Params {
    pub url: String,
    pub topic: String,
    pub transport: Transport,
    pub connections: u64,
    pub rate: u32,
    pub duration_secs: u64,
    pub payload_bytes: usize,
    pub connect_concurrency: u64,
    pub drain_timeout_secs: u64,
}

impl Params {
    fn from_args(args: &Args) -> Self {
        Self {
            url: args.url.clone(),
            topic: args.topic.clone(),
            transport: args.transport,
            connections: args.connections,
            rate: args.rate,
            duration_secs: args.duration,
            payload_bytes: args.payload_bytes,
            connect_concurrency: args.connect_concurrency,
            drain_timeout_secs: args.drain_timeout,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectionStats {
    pub requested: u64,
    pub established: u64,
    pub failed: u64,
    pub closed_early: u64,
    pub closed_early_reasons: BTreeMap<String, u64>,
    pub connect_errors: BTreeMap<String, u64>,
    pub fd_limit_failures: u64,
    pub connect_secs: f64,
    pub connects_per_sec: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PublishStats {
    pub ok: u64,
    pub failed: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeliveryStats {
    pub expected: u64,
    pub received: u64,
    pub missing: u64,
    pub loss_percent: f64,
    pub malformed: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyMs {
    pub min: f64,
    pub mean: f64,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Throughput {
    pub deliveries_per_sec: f64,
    pub window_secs: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub params: Params,
    pub interrupted: bool,
    pub connections: ConnectionStats,
    pub publish: PublishStats,
    pub deliveries: DeliveryStats,
    pub latency_ms: Option<LatencyMs>,
    pub throughput: Throughput,
}

impl Report {
    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }

    #[must_use]
    pub fn render(&self) -> String {
        let p = &self.params;
        let c = &self.connections;
        let d = &self.deliveries;
        let mut text = String::new();
        if self.interrupted {
            text.push_str("run interrupted, partial results\n\n");
        }

        section(
            &mut text,
            "parameters",
            vec![
                ("url".into(), p.url.clone()),
                ("topic".into(), p.topic.clone()),
                (
                    "transport".into(),
                    format!("{:?}", p.transport).to_lowercase(),
                ),
                ("connections".into(), p.connections.to_string()),
                ("rate (msg/s)".into(), p.rate.to_string()),
                ("duration (s)".into(), p.duration_secs.to_string()),
                ("payload (bytes)".into(), p.payload_bytes.to_string()),
                (
                    "connect concurrency".into(),
                    p.connect_concurrency.to_string(),
                ),
                ("drain timeout (s)".into(), p.drain_timeout_secs.to_string()),
            ],
        );

        let mut rows = vec![
            ("requested".to_owned(), c.requested.to_string()),
            ("established".into(), c.established.to_string()),
            ("failed".into(), c.failed.to_string()),
            ("closed early".into(), c.closed_early.to_string()),
        ];
        for (reason, count) in &c.closed_early_reasons {
            rows.push((format!("  closed early: {reason}"), count.to_string()));
        }
        for (error, count) in c.connect_errors.iter().take(3) {
            rows.push((format!("  connect error x{count}"), error.clone()));
        }
        rows.push(("connect phase (s)".into(), format!("{:.3}", c.connect_secs)));
        rows.push((
            "connects per second".into(),
            format!("{:.0}", c.connects_per_sec),
        ));
        section(&mut text, "connections", rows);

        section(
            &mut text,
            "publish",
            vec![
                ("ok".into(), self.publish.ok.to_string()),
                ("failed".into(), self.publish.failed.to_string()),
            ],
        );

        section(
            &mut text,
            "deliveries",
            vec![
                ("expected".into(), d.expected.to_string()),
                ("received".into(), d.received.to_string()),
                ("missing".into(), d.missing.to_string()),
                ("loss (%)".into(), format!("{:.3}", d.loss_percent)),
                ("malformed".into(), d.malformed.to_string()),
                (
                    "throughput (deliveries/s)".into(),
                    format!("{:.0}", self.throughput.deliveries_per_sec),
                ),
            ],
        );

        let latency_rows = self.latency_ms.as_ref().map_or_else(
            || vec![("no deliveries".to_owned(), String::new())],
            |l| {
                [
                    ("min", l.min),
                    ("mean", l.mean),
                    ("p50", l.p50),
                    ("p90", l.p90),
                    ("p99", l.p99),
                    ("p99.9", l.p999),
                    ("max", l.max),
                ]
                .into_iter()
                .map(|(name, value)| (name.to_owned(), format!("{value:.3}")))
                .collect()
            },
        );
        section(&mut text, "latency (ms)", latency_rows);

        if c.fd_limit_failures > 0 {
            let _ = write!(text, "\n{}\n", fd_hint(c.fd_limit_failures));
        }
        text
    }
}

fn section(text: &mut String, title: &str, rows: Vec<(String, String)>) {
    if !text.is_empty() && !text.ends_with("\n\n") {
        text.push('\n');
    }
    let _ = writeln!(text, "{title}");
    for (label, value) in rows {
        let _ = writeln!(text, "  {label:<28}{value}");
    }
}

#[must_use]
pub fn fd_hint(count: u64) -> String {
    format!(
        "hint: {count} connection attempts failed with 'Too many open files'; \
raise the file-descriptor limit with `ulimit -n` and retry"
    )
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    fn args() -> Args {
        Args::try_parse_from(["loadgen"]).expect("defaults")
    }

    fn facts() -> RunFacts {
        RunFacts {
            connect_elapsed: Duration::from_secs(1),
            publish_start_nanos: Some(0),
            interrupted: false,
        }
    }

    fn payload_aged(age_micros: u64) -> String {
        payload::encode(payload::unix_nanos() - age_micros * 1000, 1, 64).expect("fits")
    }

    #[test]
    fn merges_shards_and_reports_sane_percentiles() {
        let shared = Shared::new();
        for i in 0..1000_u64 {
            let age = if i == 999 { 500_000 } else { 2_000 };
            shared.record_payload(
                usize::try_from(i).expect("fits"),
                payload_aged(age).as_bytes(),
            );
        }
        shared.record_payload(0, b"not a payload");
        shared.established.store(1, Relaxed);
        shared.published_ok.store(1001, Relaxed);

        let report = shared.report(&args(), &facts());
        let latency = report.latency_ms.expect("latency recorded");
        assert_eq!(report.deliveries.received, 1000);
        assert_eq!(report.deliveries.malformed, 1);
        assert_eq!(report.deliveries.expected, 1001);
        assert_eq!(report.deliveries.missing, 1);
        assert!(latency.min >= 2.0 && latency.min < 20.0, "{latency:?}");
        assert!(latency.p50 >= 2.0 && latency.p50 < 20.0, "{latency:?}");
        assert!(latency.p99 < 20.0 && latency.max >= 500.0, "{latency:?}");
        assert!(latency.min <= latency.p50 && latency.p50 <= latency.p99);
        assert!(latency.p99 <= latency.p999 && latency.p999 <= latency.max);
    }

    #[test]
    fn clamps_out_of_range_latency_instead_of_panicking() {
        let shared = Shared::new();
        let future = payload::encode(u64::MAX, 1, 64).expect("fits");
        shared.record_payload(0, future.as_bytes());
        let ancient = payload::encode(1, 1, 64).expect("fits");
        shared.record_payload(1, ancient.as_bytes());
        let report = shared.report(&args(), &facts());
        assert_eq!(report.deliveries.received, 2);
        assert!(report.latency_ms.is_some());
    }

    #[test]
    fn empty_run_has_no_latency_and_zero_loss() {
        let report = Shared::new().report(&args(), &facts());
        assert!(report.latency_ms.is_none());
        assert!(report.deliveries.loss_percent.abs() < f64::EPSILON);
        assert!(report.render().contains("no deliveries"));
        assert!(
            report
                .to_json()
                .expect("json")
                .contains("\"latency_ms\": null")
        );
    }

    #[test]
    fn distinct_error_keys_are_capped() {
        let shared = Shared::new();
        for i in 0..50 {
            shared.record_connect_failure(&ConnectFailure::new(format!("error {i}")));
        }
        let report = shared.report(&args(), &facts());
        assert_eq!(report.connections.failed, 50);
        assert!(report.connections.connect_errors.len() <= MAX_DISTINCT_ERRORS + 1);
    }
}
