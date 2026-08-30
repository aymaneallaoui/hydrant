#![allow(dead_code)]

use std::{net::SocketAddr, time::Duration};

use hydrant_server::{config::Settings, serve};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};

pub const IO_TIMEOUT: Duration = Duration::from_secs(5);

pub struct TestServer {
    pub addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl TestServer {
    pub async fn start(settings: Settings) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let (tx, rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(serve(
            listener,
            settings,
            Duration::from_secs(10),
            async move {
                let _ = rx.await;
            },
        ));
        Self {
            addr,
            shutdown: Some(tx),
            handle: Some(handle),
        }
    }

    pub async fn stop(&mut self) {
        self.shutdown
            .take()
            .expect("already stopped")
            .send(())
            .expect("send shutdown");
        self.join().await;
    }

    pub fn trigger_shutdown(&mut self) {
        self.shutdown
            .take()
            .expect("already stopped")
            .send(())
            .expect("send shutdown");
    }

    pub async fn join(&mut self) {
        let handle = self.handle.take().expect("already joined");
        tokio::time::timeout(IO_TIMEOUT, handle)
            .await
            .expect("serve did not return in time")
            .expect("serve task panicked")
            .expect("serve returned an error");
    }

    pub async fn request(&self, method: &str, path: &str, body: &[u8]) -> (u16, String) {
        let (status, body) = self.request_raw(method, path, body).await;
        (status, String::from_utf8(body).expect("utf-8 response"))
    }

    pub async fn request_raw(&self, method: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
        let mut stream = TcpStream::connect(self.addr).await.expect("connect");
        let head = format!(
            "{method} {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await.expect("write head");
        stream.write_all(body).await.expect("write body");
        let mut response = Vec::new();
        tokio::time::timeout(IO_TIMEOUT, stream.read_to_end(&mut response))
            .await
            .expect("response timed out")
            .expect("read response");
        let split = response
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("header terminator");
        let status = std::str::from_utf8(&response[..split])
            .expect("utf-8 head")
            .split_whitespace()
            .nth(1)
            .expect("status code")
            .parse()
            .expect("numeric status");
        (status, response[split + 4..].to_vec())
    }

    pub async fn publish(&self, topic: &str, body: &[u8]) -> (u16, String) {
        self.request("POST", &format!("/topics/{topic}"), body)
            .await
    }

    pub async fn metrics(&self) -> String {
        self.request("GET", "/metrics", b"").await.1
    }
}

pub struct SseClient {
    stream: TcpStream,
    seen: String,
}

impl SseClient {
    pub async fn connect(addr: SocketAddr, topic: &str) -> Self {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let request = format!(
            "GET /topics/{topic}/sse HTTP/1.1\r\nHost: test\r\nAccept: text/event-stream\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        let mut client = Self {
            stream,
            seen: String::new(),
        };
        client.read_until("\r\n\r\n").await;
        assert!(client.seen.starts_with("HTTP/1.1 200"), "{}", client.seen);
        client.seen.clear();
        client
    }

    pub async fn next_event(&mut self) -> String {
        self.read_until("\n\n").await;
        let end = self.seen.find("\n\n").expect("event terminator") + 2;
        self.seen.drain(..end).collect()
    }

    async fn read_until(&mut self, marker: &str) {
        let mut buf = [0u8; 1024];
        while !self.seen.contains(marker) {
            let n = tokio::time::timeout(IO_TIMEOUT, self.stream.read(&mut buf))
                .await
                .expect("sse read timed out")
                .expect("sse read");
            assert!(n > 0, "sse stream ended; saw {:?}", self.seen);
            self.seen.push_str(&String::from_utf8_lossy(&buf[..n]));
        }
    }
}

pub fn metric(body: &str, line_prefix: &str) -> u64 {
    body.lines()
        .find_map(|line| line.strip_prefix(line_prefix))
        .unwrap_or_else(|| panic!("metric {line_prefix} missing in {body}"))
        .trim()
        .parse()
        .expect("numeric metric")
}
