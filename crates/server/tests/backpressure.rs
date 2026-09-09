mod common;

use std::time::Duration;

use common::{SseClient, TestServer, metric, small_buffer_stream, wait_until};
use hydrant_server::config::Settings;
use tokio_tungstenite::{client_async, connect_async};

const LARGE_MESSAGE: usize = 65536;
const LARGE_MESSAGES: usize = 300;

fn stall_settings() -> Settings {
    Settings {
        send_timeout: Duration::from_secs(1),
        topic_capacity: 1024,
        ..Settings::default()
    }
}

async fn publish_large(server: &TestServer, topic: &str) {
    let body = vec![b'x'; LARGE_MESSAGE];
    for _ in 0..LARGE_MESSAGES {
        assert_eq!(server.publish(topic, &body).await.0, 202);
    }
}

async fn gauge(server: &TestServer, transport: &str) -> u64 {
    metric(
        &server.metrics().await,
        &format!("hydrant_connections{{transport=\"{transport}\"}} "),
    )
}

#[tokio::test]
async fn stalled_sse_client_is_evicted_by_send_timeout() {
    let mut server = TestServer::start(stall_settings()).await;
    let stalled = SseClient::over(small_buffer_stream(server.addr).await, "room").await;
    assert_eq!(gauge(&server, "sse").await, 1);

    publish_large(&server, "room").await;

    wait_until("sse gauge to return to 0", || async {
        gauge(&server, "sse").await == 0
    })
    .await;
    let body = server.metrics().await;
    assert!(metric(&body, "hydrant_slow_consumer_disconnects_total ") >= 1);
    assert_eq!(metric(&body, "hydrant_topics "), 0);

    drop(stalled);
    server.stop().await;
}

#[tokio::test]
async fn stalled_websocket_client_is_evicted_by_send_timeout() {
    let mut server = TestServer::start(stall_settings()).await;
    let stream = small_buffer_stream(server.addr).await;
    let (ws, _) = client_async(format!("ws://{}/topics/room/ws", server.addr), stream)
        .await
        .expect("ws handshake");
    assert_eq!(gauge(&server, "ws").await, 1);

    publish_large(&server, "room").await;

    wait_until("ws gauge to return to 0", || async {
        gauge(&server, "ws").await == 0
    })
    .await;
    let body = server.metrics().await;
    assert!(metric(&body, "hydrant_slow_consumer_disconnects_total ") >= 1);
    assert_eq!(metric(&body, "hydrant_topics "), 0);

    drop(ws);
    server.stop().await;
}

#[tokio::test]
async fn idle_sse_disconnect_releases_topic_and_gauge() {
    let mut server = TestServer::start(Settings::default()).await;
    let sse = SseClient::connect(server.addr, "room").await;
    let body = server.metrics().await;
    assert_eq!(metric(&body, "hydrant_connections{transport=\"sse\"} "), 1);
    assert_eq!(metric(&body, "hydrant_topics "), 1);

    drop(sse);

    wait_until("idle disconnect to release resources", || async {
        let body = server.metrics().await;
        metric(&body, "hydrant_connections{transport=\"sse\"} ") == 0
            && metric(&body, "hydrant_topics ") == 0
    })
    .await;
    assert_eq!(
        metric(
            &server.metrics().await,
            "hydrant_slow_consumer_disconnects_total "
        ),
        0
    );

    server.stop().await;
}

#[tokio::test]
async fn connection_cap_rejects_with_503_and_recovers() {
    let settings = Settings {
        max_connections: Some(2),
        ..Settings::default()
    };
    let mut server = TestServer::start(settings).await;
    let first = SseClient::connect(server.addr, "room").await;
    let (ws, _) = connect_async(format!("ws://{}/topics/room/ws", server.addr))
        .await
        .expect("second connection fits");

    let (status, body) = server.request("GET", "/topics/room/sse", b"").await;
    assert_eq!(status, 503);
    assert!(body.contains("connection limit"), "{body}");
    let err = connect_async(format!("ws://{}/topics/room/ws", server.addr))
        .await
        .expect_err("ws over the cap");
    assert!(
        matches!(&err, tokio_tungstenite::tungstenite::Error::Http(r) if r.status() == 503),
        "{err:?}"
    );

    let body = server.metrics().await;
    assert_eq!(metric(&body, "hydrant_connections_rejected_total "), 2);
    assert_eq!(metric(&body, "hydrant_connections{transport=\"sse\"} "), 1);
    assert_eq!(metric(&body, "hydrant_connections{transport=\"ws\"} "), 1);

    drop(first);
    wait_until("a slot to free up", || async {
        gauge(&server, "sse").await == 0
    })
    .await;
    let again = SseClient::connect(server.addr, "room").await;

    drop(again);
    drop(ws);
    server.stop().await;
}

#[tokio::test]
async fn unlimited_by_default() {
    let mut server = TestServer::start(Settings::default()).await;
    let mut clients = Vec::new();
    for _ in 0..8 {
        clients.push(SseClient::connect(server.addr, "room").await);
    }
    let body = server.metrics().await;
    assert_eq!(metric(&body, "hydrant_connections{transport=\"sse\"} "), 8);
    assert_eq!(metric(&body, "hydrant_connections_rejected_total "), 0);
    drop(clients);
    server.stop().await;
}
