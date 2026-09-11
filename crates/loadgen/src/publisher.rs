use std::{io, sync::atomic::Ordering::Relaxed, time::Duration};

use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, sleep_until, timeout},
};

use crate::{cli::Target, payload, stats::Shared};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_HEAD_BYTES: usize = 16 * 1024;
const SLEEP_FLOOR: Duration = Duration::from_millis(1);
pub const GRACE: Duration = Duration::from_millis(500);
const HEAD_END: &[u8] = b"\r\n\r\n";

pub struct Publisher {
    target: Target,
    path: String,
    stream: Option<TcpStream>,
    response: Vec<u8>,
}

impl Publisher {
    #[must_use]
    pub fn new(target: Target, topic: &str) -> Self {
        Self {
            target,
            path: format!("/topics/{topic}"),
            stream: None,
            response: Vec::with_capacity(512),
        }
    }

    pub async fn connect(&mut self) -> io::Result<()> {
        let stream = TcpStream::connect(self.target.addr).await?;
        stream.set_nodelay(true)?;
        self.stream = Some(stream);
        Ok(())
    }

    pub async fn post(&mut self, body: &str, limit: Duration) -> io::Result<u16> {
        timeout(limit.min(REQUEST_TIMEOUT), self.post_inner(body))
            .await
            .unwrap_or_else(|_| {
                self.stream = None;
                Err(io::Error::new(io::ErrorKind::TimedOut, "publish timed out"))
            })
    }

    async fn post_inner(&mut self, body: &str) -> io::Result<u16> {
        let mut stream = if let Some(stream) = self.stream.take() {
            stream
        } else {
            let stream = TcpStream::connect(self.target.addr).await?;
            stream.set_nodelay(true)?;
            stream
        };
        let request = format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{body}",
            self.path,
            self.target.authority,
            body.len()
        );
        stream.write_all(request.as_bytes()).await?;
        let (status, reusable) = read_response(&mut stream, &mut self.response).await?;
        if reusable {
            self.stream = Some(stream);
        }
        Ok(status)
    }
}

async fn read_response<R: AsyncRead + Unpin>(
    stream: &mut R,
    buf: &mut Vec<u8>,
) -> io::Result<(u16, bool)> {
    buf.clear();
    let head_end = loop {
        if let Some(pos) = buf.windows(HEAD_END.len()).position(|w| w == HEAD_END) {
            break pos;
        }
        if buf.len() > MAX_HEAD_BYTES {
            return Err(invalid("response head too large"));
        }
        if stream.read_buf(buf).await? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
    };
    let head = std::str::from_utf8(&buf[..head_end]).map_err(|_| invalid("non-utf8 head"))?;
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| invalid("bad status line"))?;

    let mut content_length = None;
    let mut keep_alive = true;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let (name, value) = (name.trim(), value.trim());
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.parse::<usize>().ok();
        } else if name.eq_ignore_ascii_case("connection") && value.eq_ignore_ascii_case("close") {
            keep_alive = false;
        }
    }
    let Some(content_length) = content_length else {
        return Ok((status, false));
    };
    let mut missing = content_length.saturating_sub(buf.len() - head_end - HEAD_END.len());
    while missing > 0 {
        buf.clear();
        let read = stream.read_buf(buf).await?;
        if read == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        missing = missing.saturating_sub(read);
    }
    Ok((status, keep_alive))
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[must_use]
pub fn total_messages(rate: u32, duration: Duration) -> u64 {
    u64::from(rate).saturating_mul(duration.as_secs())
}

#[must_use]
pub fn scheduled_offset(index: u64, rate: u32) -> Duration {
    let nanos = u128::from(index) * 1_000_000_000 / u128::from(rate);
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

#[must_use]
pub fn latency_stamp(intended_nanos: u64, now_nanos: u64) -> u64 {
    intended_nanos.min(now_nanos)
}

pub async fn run(
    shared: &Shared,
    mut publisher: Publisher,
    rate: u32,
    duration: Duration,
    payload_bytes: usize,
) {
    let total = total_messages(rate, duration);
    let start = Instant::now();
    let start_nanos = payload::monotonic_nanos();
    let end = start + duration;
    let deadline = end + GRACE;
    shared.scheduled.store(total, Relaxed);
    shared.publish_started(start_nanos);

    let mut index = 0;
    while index < total {
        let offset = scheduled_offset(index, rate);
        let intended = start + offset;
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        if intended >= now + SLEEP_FLOOR {
            sleep_until(intended).await;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let intended_nanos =
            start_nanos.saturating_add(u64::try_from(offset.as_nanos()).unwrap_or(u64::MAX));
        let stamp = latency_stamp(intended_nanos, payload::monotonic_nanos());
        let Some(body) = payload::encode(stamp, index, payload_bytes) else {
            shared.published_failed.fetch_add(1, Relaxed);
            index += 1;
            continue;
        };
        index += 1;
        match publisher.post(&body, remaining).await {
            Ok(202) => {
                shared.published_ok.fetch_add(1, Relaxed);
            }
            Ok(status) => {
                shared.record_publish_error(&format!("HTTP {status}"));
                shared.published_failed.fetch_add(1, Relaxed);
            }
            Err(err) => {
                shared.record_publish_error(&err.to_string());
                shared.published_failed.fetch_add(1, Relaxed);
            }
        }
    }
    shared.not_sent.store(total - index, Relaxed);
    if index == total {
        sleep_until(end).await;
    }
    shared.publish_ended(payload::monotonic_nanos());
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
    };

    use tokio::{io::ReadBuf, net::TcpListener};

    use super::*;
    use crate::cli::Target;

    struct Scripted(VecDeque<Vec<u8>>);

    impl Scripted {
        fn new(chunks: &[&[u8]]) -> Self {
            Self(chunks.iter().map(|c| c.to_vec()).collect())
        }
    }

    impl AsyncRead for Scripted {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if let Some(mut chunk) = self.0.pop_front() {
                let take = chunk.len().min(buf.remaining());
                buf.put_slice(&chunk[..take]);
                if take < chunk.len() {
                    self.0.push_front(chunk.split_off(take));
                }
            }
            Poll::Ready(Ok(()))
        }
    }

    async fn parse(chunks: &[&[u8]]) -> io::Result<(u16, bool)> {
        read_response(&mut Scripted::new(chunks), &mut Vec::new()).await
    }

    #[tokio::test]
    async fn content_length_response_is_reusable() {
        let raw: &[u8] = b"HTTP/1.1 202 Accepted\r\nContent-Length: 14\r\n\r\n{\"receivers\":0}";
        assert_eq!(parse(&[raw]).await.expect("parse"), (202, true));
    }

    #[tokio::test]
    async fn header_names_are_case_insensitive() {
        let raw: &[u8] = b"HTTP/1.1 202 Accepted\r\ncontent-LENGTH: 2\r\n\r\nok";
        assert_eq!(parse(&[raw]).await.expect("parse"), (202, true));
    }

    #[tokio::test]
    async fn connection_close_is_not_reusable() {
        let raw: &[u8] =
            b"HTTP/1.1 413 Payload Too Large\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
        assert_eq!(parse(&[raw]).await.expect("parse"), (413, false));
    }

    #[tokio::test]
    async fn missing_content_length_is_not_reusable() {
        let raw: &[u8] = b"HTTP/1.1 202 Accepted\r\nServer: x\r\n\r\n";
        assert_eq!(parse(&[raw]).await.expect("parse"), (202, false));
    }

    #[tokio::test]
    async fn chunked_response_is_not_reusable() {
        let raw: &[u8] =
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n0\r\n\r\n";
        assert_eq!(parse(&[raw]).await.expect("parse"), (200, false));
    }

    #[tokio::test]
    async fn split_head_and_body_reads_are_reassembled() {
        let chunks: [&[u8]; 6] = [
            b"HTTP/1.1 20",
            b"2 Accepted\r\nContent-Le",
            b"ngth: 14\r\n\r",
            b"\n{\"recei",
            b"vers\"",
            b":0}",
        ];
        assert_eq!(parse(&chunks).await.expect("parse"), (202, true));
    }

    #[tokio::test]
    async fn truncated_body_is_an_error() {
        let raw: &[u8] = b"HTTP/1.1 202 Accepted\r\nContent-Length: 14\r\n\r\n{\"rec";
        let err = parse(&[raw]).await.expect_err("eof mid-body");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn garbage_status_line_is_an_error() {
        let err = parse(&[b"nonsense\r\n\r\n"]).await.expect_err("bad status");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn schedule_is_indexed_and_exact() {
        assert_eq!(total_messages(3, Duration::from_secs(10)), 30);
        assert_eq!(total_messages(200_000, Duration::from_secs(5)), 1_000_000);
        assert_eq!(scheduled_offset(0, 3), Duration::ZERO);
        assert_eq!(scheduled_offset(3, 3), Duration::from_secs(1));
        assert_eq!(scheduled_offset(29, 3), Duration::from_nanos(9_666_666_666));
        assert_eq!(scheduled_offset(1, 1000), Duration::from_millis(1));
    }

    #[test]
    fn stamp_is_the_intended_time_unless_sent_early() {
        assert_eq!(latency_stamp(100, 500), 100);
        assert_eq!(latency_stamp(500, 100), 100);
    }

    struct FakeServer {
        target: Target,
        bodies: Arc<Mutex<Vec<String>>>,
    }

    async fn fake_server(delay: Duration) -> FakeServer {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&bodies);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let recorded = Arc::clone(&recorded);
                tokio::spawn(async move {
                    let mut pending = Vec::new();
                    let mut buf = [0_u8; 4096];
                    loop {
                        let Ok(n) = stream.read(&mut buf).await else {
                            return;
                        };
                        if n == 0 {
                            return;
                        }
                        pending.extend_from_slice(&buf[..n]);
                        while let Some(split) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&pending[..split]).into_owned();
                            let length: usize = head
                                .lines()
                                .find_map(|l| l.strip_prefix("Content-Length: "))
                                .and_then(|v| v.parse().ok())
                                .expect("content-length");
                            if pending.len() < split + 4 + length {
                                break;
                            }
                            let body =
                                String::from_utf8_lossy(&pending[split + 4..split + 4 + length])
                                    .into_owned();
                            pending.drain(..split + 4 + length);
                            recorded.lock().expect("lock").push(body);
                            tokio::time::sleep(delay).await;
                            let reply = b"HTTP/1.1 202 Accepted\r\nContent-Length: 14\r\n\r\n{\"receivers\":0}";
                            if stream.write_all(reply).await.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });
        FakeServer {
            target: Target {
                addr,
                authority: addr.to_string(),
            },
            bodies,
        }
    }

    fn publisher_for(server: &FakeServer) -> Publisher {
        Publisher::new(server.target.clone(), "t")
    }

    #[tokio::test]
    async fn run_sends_exactly_rate_times_duration_at_intended_stamps() {
        let server = fake_server(Duration::ZERO).await;
        let shared = Shared::new();
        let started = Instant::now();

        run(
            &shared,
            publisher_for(&server),
            20,
            Duration::from_secs(1),
            64,
        )
        .await;

        assert!(started.elapsed() >= Duration::from_secs(1));
        assert_eq!(shared.published_ok.load(Relaxed), 20);
        assert_eq!(shared.published_failed.load(Relaxed), 0);
        assert_eq!(shared.not_sent.load(Relaxed), 0);
        let bodies = server.bodies.lock().expect("lock").clone();
        assert_eq!(bodies.len(), 20);
        let decoded: Vec<(u64, u64)> = bodies
            .iter()
            .map(|b| payload::decode(b.as_bytes()).expect("payload"))
            .collect();
        for (i, (stamp, seq)) in decoded.iter().enumerate() {
            assert_eq!(*seq, i as u64);
            assert_eq!(*stamp - decoded[0].0, 50_000_000 * i as u64, "message {i}");
        }
    }

    #[tokio::test]
    async fn hard_stop_bounds_the_run_when_the_server_stalls() {
        let server = fake_server(Duration::from_secs(30)).await;
        let shared = Shared::new();
        let started = Instant::now();

        run(
            &shared,
            publisher_for(&server),
            5,
            Duration::from_secs(1),
            64,
        )
        .await;

        let elapsed = started.elapsed();
        assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
        assert_eq!(shared.published_ok.load(Relaxed), 0);
        assert_eq!(shared.published_failed.load(Relaxed), 1);
        assert_eq!(shared.not_sent.load(Relaxed), 4);
        assert!(shared.first_publish_error().is_some());
    }

    #[tokio::test]
    async fn falling_behind_catches_up_without_skipping_then_stops_at_the_deadline() {
        let server = fake_server(Duration::from_millis(100)).await;
        let shared = Shared::new();
        let started = Instant::now();

        run(
            &shared,
            publisher_for(&server),
            20,
            Duration::from_secs(1),
            64,
        )
        .await;

        assert!(started.elapsed() < Duration::from_secs(2));
        let ok = shared.published_ok.load(Relaxed);
        let failed = shared.published_failed.load(Relaxed);
        let not_sent = shared.not_sent.load(Relaxed);
        assert_eq!(ok + failed + not_sent, 20);
        assert!(
            not_sent > 0 && ok >= 5,
            "ok {ok} failed {failed} not_sent {not_sent}"
        );
        let bodies = server.bodies.lock().expect("lock").clone();
        for (i, body) in bodies.iter().enumerate() {
            assert_eq!(
                payload::decode(body.as_bytes()).expect("payload").1,
                i as u64
            );
        }
    }
}
