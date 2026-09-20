use std::time::Duration;

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use hydrant_server::{config::Settings, routes::router, serve, state::AppState};
use tokio::{net::TcpListener, sync::oneshot};
use tower::ServiceExt;

async fn get(uri: &str) -> axum::response::Response {
    router(AppState::new(Settings::default()))
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response")
}

async fn body_string(res: axum::response::Response) -> String {
    let bytes = res.into_body().collect().await.expect("body").to_bytes();
    String::from_utf8(bytes.to_vec()).expect("utf-8 body")
}

#[tokio::test]
async fn healthz_returns_ok() {
    let res = get("/healthz").await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(body_string(res).await, "ok");
}

#[tokio::test]
async fn metrics_exposes_build_info() {
    let res = get("/metrics").await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.headers()
            .get(header::CONTENT_TYPE)
            .expect("content-type"),
        "text/plain; version=0.0.4"
    );
    let body = body_string(res).await;
    let expected = format!(
        "hydrant_build_info{{version=\"{}\"}} 1",
        env!("CARGO_PKG_VERSION")
    );
    assert!(
        body.contains(&expected),
        "missing build_info line in {body}"
    );
}

#[tokio::test]
async fn serve_returns_ok_after_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let (tx, rx) = oneshot::channel::<()>();
    let server = tokio::spawn(serve(
        listener,
        Settings::default(),
        Duration::from_secs(5),
        async move {
            rx.await.expect("shutdown sender dropped");
        },
    ));

    tx.send(()).expect("send shutdown");
    let result = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("serve did not return promptly")
        .expect("serve task panicked");
    result.expect("serve returned an error");
}

#[tokio::test]
async fn publish_while_closed_returns_503_and_is_not_counted() {
    let state = AppState::new(Settings::default());
    state.hub().close();
    let res = router(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/topics/t")
                .body(Body::from("x"))
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(state.metrics().published_total(), 0);
}

#[tokio::test]
async fn bind_listener_serves_with_custom_backlog_on_v4_and_v6() {
    for (addr, backlog) in [("127.0.0.1:0", 4096_u32), ("[::1]:0", 1)] {
        let Ok(listener) = hydrant_server::bind_listener(addr.parse().expect("addr"), backlog)
        else {
            assert_eq!(addr, "[::1]:0", "only IPv6 may be unavailable");
            continue;
        };
        let local = listener.local_addr().expect("local addr");
        let (tx, rx) = oneshot::channel::<()>();
        let server = tokio::spawn(serve(
            listener,
            Settings::default(),
            Duration::from_secs(1),
            async move {
                let _ = rx.await;
            },
        ));
        let mut stream = tokio::net::TcpStream::connect(local)
            .await
            .expect("connect");
        tokio::io::AsyncWriteExt::write_all(
            &mut stream,
            b"GET /healthz HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n",
        )
        .await
        .expect("write");
        let mut out = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut stream, &mut out)
            .await
            .expect("read");
        assert!(out.starts_with("HTTP/1.1 200"), "{out}");
        let _ = tx.send(());
        server.await.expect("join").expect("serve");
    }
}
