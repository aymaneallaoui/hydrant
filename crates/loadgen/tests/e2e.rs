use std::time::Duration;

use clap::Parser;
use hydrant_loadgen::{cli::Args, run};
use hydrant_server::{config::Settings, serve};
use tokio::{net::TcpListener, sync::oneshot};

const CONNECTIONS: u64 = 20;

async fn start_server() -> (String, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("local addr"));
    let (tx, rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        serve(
            listener,
            Settings::default(),
            Duration::from_secs(5),
            async move {
                let _ = rx.await;
            },
        )
        .await
        .expect("server");
    });
    (url, tx, handle)
}

async fn run_transport(transport: &str) {
    let (url, shutdown, server) = start_server().await;
    let connections = CONNECTIONS.to_string();
    let args = Args::try_parse_from([
        "loadgen",
        "--url",
        &url,
        "--transport",
        transport,
        "--connections",
        &connections,
        "--rate",
        "5",
        "--duration",
        "1",
        "--drain-timeout",
        "5",
    ])
    .expect("args");

    let report = run(args).await.expect("run");

    assert_eq!(report.connections.requested, CONNECTIONS);
    assert_eq!(report.connections.established, CONNECTIONS);
    assert_eq!(report.connections.failed, 0);
    assert_eq!(report.connections.closed_early, 0);
    assert_eq!(report.publish.scheduled, 5);
    assert_eq!(report.publish.ok, 5);
    assert_eq!(report.publish.failed, 0);
    assert_eq!(report.publish.not_sent, 0);
    assert_eq!(report.deliveries.expected, report.publish.ok * CONNECTIONS);
    assert_eq!(report.deliveries.received, report.deliveries.expected);
    assert_eq!(report.deliveries.missing, 0);
    assert_eq!(report.deliveries.malformed, 0);
    let latency = report.latency_ms.expect("latency histogram is non-empty");
    assert!(
        latency.min <= latency.p50 && latency.p50 <= latency.p99 && latency.p99 <= latency.max,
        "{latency:?}"
    );
    assert!(latency.p50 < 1000.0 && latency.max < 3000.0, "{latency:?}");
    assert_eq!(report.deliveries.duplicates, 0);
    assert_eq!(report.deliveries.gaps, 0);
    let phase = report.throughput.publish_phase_secs;
    assert!((1.0..2.0).contains(&phase), "{phase}");
    let expected_rate =
        f64::from(u32::try_from(report.deliveries.received).expect("small")) / phase;
    assert!(
        (report.throughput.deliveries_per_sec - expected_rate).abs() < 1.0,
        "{:?}",
        report.throughput
    );
    assert!(!report.publish.rate_shortfall);
    assert!(!report.interrupted);

    shutdown.send(()).expect("shutdown");
    server.await.expect("server task");
}

#[tokio::test]
async fn websocket_run_delivers_everything() {
    run_transport("ws").await;
}

#[tokio::test]
async fn sse_run_delivers_everything() {
    run_transport("sse").await;
}

#[tokio::test]
async fn unreachable_server_is_a_setup_error() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    drop(listener);
    let args =
        Args::try_parse_from(["loadgen", "--url", &url, "--connections", "3"]).expect("args");

    let err = run(args).await.expect_err("nothing listens");
    assert!(
        format!("{err:#}").contains("accepted a TCP connection"),
        "{err:#}"
    );
}

#[tokio::test]
async fn server_going_away_mid_run_is_reported_as_loss_of_service() {
    let (url, shutdown, server) = start_server().await;
    let args = Args::try_parse_from([
        "loadgen",
        "--url",
        &url,
        "--connections",
        "10",
        "--rate",
        "20",
        "--duration",
        "3",
    ])
    .expect("args");
    let run = tokio::spawn(run(args));
    tokio::time::sleep(Duration::from_secs(1)).await;
    shutdown.send(()).expect("shutdown");
    server.await.expect("server task");

    let report = run.await.expect("run task").expect("run");

    assert_eq!(report.connections.established, 10);
    assert_eq!(report.connections.closed_early, 10);
    assert!(
        report.publish.ok >= 1 && report.publish.ok < 60,
        "{:?}",
        report.publish
    );
    assert!(report.publish.failed > 0, "{:?}", report.publish);
    assert_eq!(
        report.publish.ok + report.publish.failed + report.publish.not_sent,
        60
    );
    assert!(report.publish.first_error.is_some());
}

#[tokio::test]
async fn oversized_payload_fails_every_publish_with_http_413() {
    let (url, shutdown, server) = start_server().await;
    let args = Args::try_parse_from([
        "loadgen",
        "--url",
        &url,
        "--connections",
        "2",
        "--rate",
        "5",
        "--duration",
        "1",
        "--payload-bytes",
        "70000",
        "--drain-timeout",
        "1",
    ])
    .expect("args");

    let report = run(args).await.expect("run");

    assert_eq!(report.publish.ok, 0);
    assert_eq!(report.publish.failed, 5);
    assert_eq!(report.publish.first_error.as_deref(), Some("HTTP 413"));
    assert_eq!(report.deliveries.received, 0);

    shutdown.send(()).expect("shutdown");
    server.await.expect("server task");
}
