use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{
        Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
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
const NOT_STARTED: u64 = u64::MAX;
const RATE_WARNING_RATIO: f64 = 0.95;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeqOutcome {
    InOrder,
    Gap(u64),
    Duplicate,
}

#[derive(Debug, Default)]
pub struct SeqTracker {
    next: u64,
}

impl SeqTracker {
    pub fn observe(&mut self, seq: u64) -> SeqOutcome {
        match seq.cmp(&self.next) {
            std::cmp::Ordering::Equal => {
                self.next = seq.saturating_add(1);
                SeqOutcome::InOrder
            }
            std::cmp::Ordering::Greater => {
                let missing = seq - self.next;
                self.next = seq.saturating_add(1);
                SeqOutcome::Gap(missing)
            }
            std::cmp::Ordering::Less => SeqOutcome::Duplicate,
        }
    }
}

pub struct Shared {
    shards: Vec<Mutex<Histogram<u64>>>,
    pub established: AtomicU64,
    pub connect_failed: AtomicU64,
    pub closed_early: AtomicU64,
    pub received: AtomicU64,
    pub malformed: AtomicU64,
    pub published_ok: AtomicU64,
    pub published_failed: AtomicU64,
    pub duplicates: AtomicU64,
    pub gaps: AtomicU64,
    pub scheduled: AtomicU64,
    pub not_sent: AtomicU64,
    publish_start_nanos: AtomicU64,
    publish_end_nanos: AtomicU64,
    first_publish_error: Mutex<Option<String>>,
    fd_exhausted: AtomicU64,
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
            shards,
            established: AtomicU64::new(0),
            connect_failed: AtomicU64::new(0),
            closed_early: AtomicU64::new(0),
            received: AtomicU64::new(0),
            malformed: AtomicU64::new(0),
            published_ok: AtomicU64::new(0),
            published_failed: AtomicU64::new(0),
            duplicates: AtomicU64::new(0),
            gaps: AtomicU64::new(0),
            scheduled: AtomicU64::new(0),
            not_sent: AtomicU64::new(0),
            publish_start_nanos: AtomicU64::new(NOT_STARTED),
            publish_end_nanos: AtomicU64::new(NOT_STARTED),
            first_publish_error: Mutex::new(None),
            fd_exhausted: AtomicU64::new(0),
            closed_reasons: Mutex::new(BTreeMap::new()),
            connect_errors: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn publish_started(&self, nanos: u64) {
        self.publish_start_nanos.store(nanos, Relaxed);
    }

    pub fn publish_ended(&self, nanos: u64) {
        self.publish_end_nanos.store(nanos, Relaxed);
    }

    pub fn record_publish_error(&self, error: &str) {
        lock(&self.first_publish_error).get_or_insert_with(|| error.to_owned());
    }

    #[must_use]
    pub fn first_publish_error(&self) -> Option<String> {
        lock(&self.first_publish_error).clone()
    }

    pub fn record_payload(&self, shard_hint: usize, seqs: &mut SeqTracker, payload: &[u8]) {
        let Some((sent_nanos, seq)) = payload::decode(payload) else {
            self.malformed.fetch_add(1, Relaxed);
            return;
        };
        match seqs.observe(seq) {
            SeqOutcome::Duplicate => {
                self.duplicates.fetch_add(1, Relaxed);
                return;
            }
            SeqOutcome::Gap(missing) => {
                self.gaps.fetch_add(missing, Relaxed);
            }
            SeqOutcome::InOrder => {}
        }
        self.record_latency(shard_hint, payload::monotonic_nanos(), sent_nanos);
    }

    fn record_latency(&self, shard_hint: usize, now_nanos: u64, sent_nanos: u64) {
        let Some(elapsed) = now_nanos.checked_sub(sent_nanos) else {
            self.malformed.fetch_add(1, Relaxed);
            return;
        };
        let micros = elapsed / 1000;
        lock(&self.shards[shard_hint % self.shards.len()]).saturating_record(micros.max(1));
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
        let phase_secs = self.publish_phase_secs();
        let requested_rate = f64::from(args.rate);
        let achieved_rate = ratio(to_f64(published_ok), phase_secs);
        let started = self.publish_start_nanos.load(Relaxed) != NOT_STARTED;
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
                scheduled: self.scheduled.load(Relaxed),
                ok: published_ok,
                failed: self.published_failed.load(Relaxed),
                not_sent: self.not_sent.load(Relaxed),
                requested_rate,
                achieved_rate: round3(achieved_rate),
                rate_shortfall: started && achieved_rate < requested_rate * RATE_WARNING_RATIO,
                phase_secs: round3(phase_secs),
                first_error: self.first_publish_error(),
            },
            deliveries: DeliveryStats {
                expected,
                received,
                missing,
                loss_percent: round3(ratio(to_f64(missing), to_f64(expected)) * 100.0),
                malformed: self.malformed.load(Relaxed),
                duplicates: self.duplicates.load(Relaxed),
                gaps: self.gaps.load(Relaxed),
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
                deliveries_per_sec: round3(ratio(to_f64(received), phase_secs)),
                publish_phase_secs: round3(phase_secs),
            },
        }
    }

    fn publish_phase_secs(&self) -> f64 {
        let start = self.publish_start_nanos.load(Relaxed);
        if start == NOT_STARTED {
            return 0.0;
        }
        let end = match self.publish_end_nanos.load(Relaxed) {
            NOT_STARTED => payload::monotonic_nanos(),
            end => end,
        };
        to_f64(end.saturating_sub(start)) / 1e9
    }
}

impl Default for Shared {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RunFacts {
    pub connect_elapsed: Duration,
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
    pub spread_loopback: u32,
    pub hold_idle_secs: u64,
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
            spread_loopback: args.spread_loopback,
            hold_idle_secs: args.hold_idle,
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
    pub scheduled: u64,
    pub ok: u64,
    pub failed: u64,
    pub not_sent: u64,
    pub requested_rate: f64,
    pub achieved_rate: f64,
    pub rate_shortfall: bool,
    pub phase_secs: f64,
    pub first_error: Option<String>,
}

impl PublishStats {
    fn rows(&self) -> Vec<(String, String)> {
        let mut rows = vec![
            ("scheduled".to_owned(), self.scheduled.to_string()),
            ("ok".into(), self.ok.to_string()),
            ("failed".into(), self.failed.to_string()),
            ("not sent".into(), self.not_sent.to_string()),
            (
                "requested rate (msg/s)".into(),
                format!("{:.1}", self.requested_rate),
            ),
            (
                "achieved rate (msg/s)".into(),
                format!("{:.1}", self.achieved_rate),
            ),
            (
                "publish phase (s)".into(),
                format!("{:.3}", self.phase_secs),
            ),
        ];
        if let Some(error) = &self.first_error {
            rows.push(("first error".into(), error.clone()));
        }
        rows
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DeliveryStats {
    pub expected: u64,
    pub received: u64,
    pub missing: u64,
    pub loss_percent: f64,
    pub malformed: u64,
    pub duplicates: u64,
    pub gaps: u64,
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
    pub publish_phase_secs: f64,
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
                (
                    "spread loopback addresses".into(),
                    p.spread_loopback.to_string(),
                ),
                ("hold idle (s)".into(), p.hold_idle_secs.to_string()),
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

        let pb = &self.publish;
        section(&mut text, "publish", pb.rows());

        section(
            &mut text,
            "deliveries",
            vec![
                ("expected".into(), d.expected.to_string()),
                ("received".into(), d.received.to_string()),
                ("missing".into(), d.missing.to_string()),
                ("loss (%)".into(), format!("{:.3}", d.loss_percent)),
                ("malformed".into(), d.malformed.to_string()),
                ("duplicates".into(), d.duplicates.to_string()),
                ("gaps seen".into(), d.gaps.to_string()),
                (
                    "deliveries/s over publish phase".into(),
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

        if pb.rate_shortfall {
            let _ = write!(
                text,
                "\nwarning: achieved publish rate {:.1} msg/s is below 95% of the requested {:.1} msg/s; \
latency figures describe a lower load than requested\n",
                pb.achieved_rate, pb.requested_rate
            );
        }
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
        let _ = writeln!(text, "  {label:<34}{value}");
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
            interrupted: false,
        }
    }

    #[test]
    fn seq_tracker_classifies_in_order_gap_and_duplicate() {
        let mut seqs = SeqTracker::default();
        assert_eq!(seqs.observe(0), SeqOutcome::InOrder);
        assert_eq!(seqs.observe(1), SeqOutcome::InOrder);
        assert_eq!(seqs.observe(4), SeqOutcome::Gap(2));
        assert_eq!(seqs.observe(4), SeqOutcome::Duplicate);
        assert_eq!(seqs.observe(3), SeqOutcome::Duplicate);
        assert_eq!(seqs.observe(5), SeqOutcome::InOrder);
    }

    #[test]
    fn seq_tracker_counts_a_late_first_message_as_a_gap() {
        let mut seqs = SeqTracker::default();
        assert_eq!(seqs.observe(3), SeqOutcome::Gap(3));
    }

    #[test]
    fn duplicates_do_not_mask_loss() {
        let shared = Shared::new();
        let mut seqs = SeqTracker::default();
        let now = payload::monotonic_nanos();
        for seq in [0, 0, 0, 2] {
            let body = payload::encode(now, seq, 64).expect("fits");
            shared.record_payload(0, &mut seqs, body.as_bytes());
        }
        shared.established.store(1, Relaxed);
        shared.published_ok.store(4, Relaxed);

        let report = shared.report(&args(), &facts());
        assert_eq!(report.deliveries.received, 2);
        assert_eq!(report.deliveries.duplicates, 2);
        assert_eq!(report.deliveries.gaps, 1);
        assert_eq!(report.deliveries.expected, 4);
        assert_eq!(report.deliveries.missing, 2);
        assert!((report.deliveries.loss_percent - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn merges_shards_and_reports_exact_percentiles() {
        let shared = Shared::new();
        for i in 0..1000_u64 {
            let latency_nanos = if i == 999 { 500_000_000 } else { 2_000_000 };
            shared.record_latency(
                usize::try_from(i).expect("fits"),
                1_000_000_000,
                1_000_000_000 - latency_nanos,
            );
        }
        shared.established.store(1, Relaxed);
        shared.published_ok.store(1001, Relaxed);

        let report = shared.report(&args(), &facts());
        let latency = report.latency_ms.expect("latency recorded");
        assert_eq!(report.deliveries.received, 1000);
        assert_eq!(report.deliveries.expected, 1001);
        assert_eq!(report.deliveries.missing, 1);
        assert!((latency.min - 2.0).abs() < 0.01, "{latency:?}");
        assert!((latency.p50 - 2.0).abs() < 0.01, "{latency:?}");
        assert!((latency.p99 - 2.0).abs() < 0.01, "{latency:?}");
        assert!((latency.max - 500.0).abs() < 1.0, "{latency:?}");
        assert!((latency.mean - 2.498).abs() < 0.01, "{latency:?}");
    }

    #[test]
    fn negative_latency_is_malformed_not_a_best_case_sample() {
        let shared = Shared::new();
        shared.record_latency(0, 1_000, 2_000);
        let report = shared.report(&args(), &facts());
        assert_eq!(report.deliveries.malformed, 1);
        assert_eq!(report.deliveries.received, 0);
        assert!(report.latency_ms.is_none());
    }

    #[test]
    fn unparseable_payload_is_malformed() {
        let shared = Shared::new();
        shared.record_payload(0, &mut SeqTracker::default(), b"not a payload");
        let report = shared.report(&args(), &facts());
        assert_eq!(report.deliveries.malformed, 1);
        assert_eq!(report.deliveries.received, 0);
    }

    #[test]
    fn sub_microsecond_latency_is_floored_to_one_microsecond() {
        let shared = Shared::new();
        shared.record_latency(0, 5_000, 5_000);
        let latency = shared
            .report(&args(), &facts())
            .latency_ms
            .expect("recorded");
        assert!((latency.min - 0.001).abs() < 1e-9, "{latency:?}");
    }

    #[test]
    fn rate_shortfall_is_flagged_and_rendered() {
        let shared = Shared::new();
        shared.publish_started(0);
        shared.publish_ended(10_000_000_000);
        shared.published_ok.store(50, Relaxed);
        let report = shared.report(&args(), &facts());
        assert!((report.publish.achieved_rate - 5.0).abs() < 1e-9);
        assert!((report.publish.phase_secs - 10.0).abs() < 1e-9);
        assert!(report.publish.rate_shortfall);
        assert!(report.render().contains("lower load than requested"));

        shared.published_ok.store(100, Relaxed);
        let report = shared.report(&args(), &facts());
        assert!(!report.publish.rate_shortfall);
        assert!(!report.render().contains("warning"));
    }

    #[test]
    fn throughput_uses_the_publish_phase_not_the_last_receive() {
        let shared = Shared::new();
        shared.publish_started(0);
        shared.publish_ended(10_000_000_000);
        shared.received.store(5000, Relaxed);
        let report = shared.report(&args(), &facts());
        assert!((report.throughput.deliveries_per_sec - 500.0).abs() < 1e-9);
        assert!((report.throughput.publish_phase_secs - 10.0).abs() < 1e-9);
    }

    #[test]
    fn throughput_covers_the_full_publish_phase_when_receives_stop_early() {
        let shared = Shared::new();
        shared.publish_started(0);
        shared.publish_ended(10_000_000_000);
        for second_fraction in 0..1000_u64 {
            shared.record_latency(0, second_fraction * 1_000_000, 0);
        }
        let report = shared.report(&args(), &facts());
        assert_eq!(report.deliveries.received, 1000);
        assert!((report.throughput.deliveries_per_sec - 100.0).abs() < 1e-9);
        assert!((report.throughput.publish_phase_secs - 10.0).abs() < 1e-9);
    }

    #[test]
    fn empty_run_has_no_latency_and_zero_loss() {
        let report = Shared::new().report(&args(), &facts());
        assert!(report.latency_ms.is_none());
        assert!(report.deliveries.loss_percent.abs() < f64::EPSILON);
        assert!(!report.publish.rate_shortfall);
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
