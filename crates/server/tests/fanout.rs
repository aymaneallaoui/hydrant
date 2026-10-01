mod common;

use std::time::{Duration, Instant};

use common::{IO_TIMEOUT, SseClient, TestServer, metric};
use futures_util::{SinkExt, StreamExt};
use hydrant_server::config::Settings;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, protocol::frame::coding::CloseCode},
};

async fn start() -> TestServer {
    TestServer::start(Settings::default()).await
}

async fn next_ws(
    ws: &mut (
             impl futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
             + Unpin
         ),
) -> Message {
    tokio::time::timeout(IO_TIMEOUT, ws.next())
        .await
        .expect("ws read timed out")
        .expect("ws closed")
        .expect("ws error")
}

#[tokio::test]
async fn websocket_subscriber_receives_published_message() {
    let mut server = start().await;
    let (mut ws, _) = connect_async(format!("ws://{}/topics/room/ws", server.addr))
        .await
        .expect("ws connect");

    let (status, body) = server.publish("room", b"hello").await;
    assert_eq!((status, body.as_str()), (202, "{\"receivers\":1}"));
    assert_eq!(next_ws(&mut ws).await, Message::text("hello"));

    drop(ws);
    server.stop().await;
}

#[tokio::test]
async fn websocket_delivers_a_max_size_payload_byte_exact() {
    let mut server = start().await;
    let (mut ws, _) = connect_async(format!("ws://{}/topics/room/ws", server.addr))
        .await
        .expect("ws connect");
    let payload = "x".repeat(Settings::default().max_message_bytes);

    assert_eq!(server.publish("room", payload.as_bytes()).await.0, 202);
    assert_eq!(next_ws(&mut ws).await, Message::text(payload));

    drop(ws);
    server.stop().await;
}

#[tokio::test]
async fn websocket_accepts_a_max_size_frame_from_the_client_and_stays_healthy() {
    let mut server = start().await;
    let (mut ws, _) = connect_async(format!("ws://{}/topics/room/ws", server.addr))
        .await
        .expect("ws connect");
    let frame = "x".repeat(Settings::default().max_message_bytes);

    ws.send(Message::text(frame)).await.expect("send max frame");
    assert_eq!(server.publish("room", b"after").await.0, 202);
    assert_eq!(next_ws(&mut ws).await, Message::text("after"));

    drop(ws);
    server.stop().await;
}

#[tokio::test]
async fn sse_subscriber_receives_id_and_data() {
    let mut server = start().await;
    let mut sse = SseClient::connect(server.addr, "room").await;

    assert_eq!(server.publish("room", b"first").await.0, 202);
    assert_eq!(server.publish("room", b"second").await.0, 202);

    let first = sse.next_event().await;
    assert!(first.contains("id: 1\ndata: first\n\n"), "{first:?}");
    let second = sse.next_event().await;
    assert!(second.contains("id: 2\ndata: second\n\n"), "{second:?}");

    drop(sse);
    server.stop().await;
}

#[tokio::test]
async fn topics_are_isolated() {
    let mut server = start().await;
    let mut sse = SseClient::connect(server.addr, "a").await;

    let (_, other) = server.publish("b", b"for-b").await;
    assert_eq!(other, "{\"receivers\":0}");
    let (_, own) = server.publish("a", b"for-a").await;
    assert_eq!(own, "{\"receivers\":1}");

    let event = sse.next_event().await;
    assert!(event.contains("data: for-a"), "{event:?}");

    drop(sse);
    server.stop().await;
}

#[tokio::test]
async fn publish_without_subscribers_is_accepted_and_creates_no_topic() {
    let mut server = start().await;

    let (status, body) = server.publish("nobody", b"x").await;
    assert_eq!((status, body.as_str()), (202, "{\"receivers\":0}"));
    assert_eq!(metric(&server.metrics().await, "hydrant_topics "), 0);

    server.stop().await;
}

#[tokio::test]
async fn invalid_topic_name_is_rejected() {
    let mut server = start().await;

    assert_eq!(server.publish("bad!name", b"x").await.0, 400);
    assert_eq!(server.publish(&"x".repeat(129), b"x").await.0, 400);
    assert_eq!(
        server.request("GET", "/topics/bad!name/sse", b"").await.0,
        400
    );
    assert_eq!(
        server.request("GET", "/topics/bad!name/ws", b"").await.0,
        400
    );

    server.stop().await;
}

#[tokio::test]
async fn oversized_body_is_rejected() {
    let settings = Settings {
        max_message_bytes: 16,
        ..Settings::default()
    };
    let mut server = TestServer::start(settings).await;

    assert_eq!(server.publish("t", &[b'a'; 17]).await.0, 413);
    assert_eq!(server.publish("t", &[b'a'; 16]).await.0, 202);

    server.stop().await;
}

#[tokio::test]
async fn non_utf8_body_is_rejected() {
    let mut server = start().await;

    assert_eq!(server.publish("t", &[0xff, 0xfe]).await.0, 400);

    server.stop().await;
}

#[tokio::test]
async fn empty_body_is_rejected_and_not_delivered_or_counted() {
    let mut server = start().await;
    let mut sse = SseClient::connect(server.addr, "room").await;

    let (status, message) = server.publish("room", b"").await;
    assert_eq!(status, 400);
    assert!(message.contains("empty"), "{message}");
    let body = server.metrics().await;
    assert_eq!(metric(&body, "hydrant_messages_published_total "), 0);

    assert_eq!(server.publish("room", b"real").await.0, 202);
    let event = sse.next_event().await;
    assert!(event.contains("id: 1\ndata: real\n\n"), "{event:?}");
    let body = server.metrics().await;
    assert_eq!(metric(&body, "hydrant_messages_published_total "), 1);

    drop(sse);
    server.stop().await;
}

#[tokio::test]
async fn metrics_reflect_connections_and_publishes() {
    let mut server = start().await;
    let sse = SseClient::connect(server.addr, "room").await;
    let (ws, _) = connect_async(format!("ws://{}/topics/room/ws", server.addr))
        .await
        .expect("ws connect");
    server.publish("room", b"m").await;

    let body = server.metrics().await;
    assert_eq!(metric(&body, "hydrant_connections{transport=\"sse\"} "), 1);
    assert_eq!(metric(&body, "hydrant_connections{transport=\"ws\"} "), 1);
    assert_eq!(metric(&body, "hydrant_topics "), 1);
    assert_eq!(metric(&body, "hydrant_messages_published_total "), 1);
    assert_eq!(metric(&body, "hydrant_messages_dropped_total "), 0);
    assert_eq!(metric(&body, "hydrant_slow_consumer_disconnects_total "), 0);

    drop(sse);
    drop(ws);
    server.stop().await;
}

#[tokio::test]
async fn shutdown_closes_websocket_promptly() {
    let mut server = start().await;
    let (mut ws, _) = connect_async(format!("ws://{}/topics/room/ws", server.addr))
        .await
        .expect("ws connect");
    let sse = SseClient::connect(server.addr, "room").await;

    let started = Instant::now();
    server.trigger_shutdown();

    match next_ws(&mut ws).await {
        Message::Close(Some(frame)) => assert_eq!(frame.code, CloseCode::Away),
        other => panic!("expected close frame, got {other:?}"),
    }
    while let Some(Ok(_)) = ws.next().await {}
    server.join().await;
    drop(sse);

    assert!(
        started.elapsed() < Duration::from_secs(3),
        "shutdown took {:?}",
        started.elapsed()
    );
}
