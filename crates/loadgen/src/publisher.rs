use std::{io, sync::atomic::Ordering::Relaxed, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{Instant, MissedTickBehavior, interval_at, timeout},
};

use crate::{cli::Target, payload, stats::Shared};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_HEAD_BYTES: usize = 16 * 1024;
const TICK_FLOOR_PER_SEC: u32 = 1000;
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

    pub async fn post(&mut self, body: &str) -> io::Result<u16> {
        timeout(REQUEST_TIMEOUT, self.post_inner(body))
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

async fn read_response(stream: &mut TcpStream, buf: &mut Vec<u8>) -> io::Result<(u16, bool)> {
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

pub async fn run(
    shared: &Shared,
    mut publisher: Publisher,
    rate: u32,
    duration: Duration,
    payload_bytes: usize,
) {
    let per_tick = rate.div_ceil(TICK_FLOOR_PER_SEC);
    let period = Duration::from_secs_f64(f64::from(per_tick) / f64::from(rate));
    let start = Instant::now();
    let end = start + duration;
    let mut ticker = interval_at(start, period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut seq = 0_u64;
    loop {
        let tick = ticker.tick().await;
        if tick >= end {
            break;
        }
        for _ in 0..per_tick {
            let Some(body) = payload::encode(payload::unix_nanos(), seq, payload_bytes) else {
                shared.published_failed.fetch_add(1, Relaxed);
                continue;
            };
            seq += 1;
            match publisher.post(&body).await {
                Ok(202) => shared.published_ok.fetch_add(1, Relaxed),
                Ok(_) | Err(_) => shared.published_failed.fetch_add(1, Relaxed),
            };
        }
    }
}
