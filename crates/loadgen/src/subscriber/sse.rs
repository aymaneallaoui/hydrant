use std::{cell::RefCell, fmt, io};

use tokio::{io::AsyncWriteExt, net::TcpStream};

use super::{ConnectFailure, Ctx, EndReason, Job, connect_or_stop, stopped};
use crate::stats::SeqTracker;

const SCRATCH_BYTES: usize = 16 * 1024;
const MAX_HEAD_BYTES: usize = 16 * 1024;
const HEAD_END: &[u8] = b"\r\n\r\n";

thread_local! {
    static SCRATCH: RefCell<[u8; SCRATCH_BYTES]> = const { RefCell::new([0; SCRATCH_BYTES]) };
}

pub async fn run(job: Job) {
    let Job {
        ctx,
        index,
        permit,
        mut stop,
        alive: _alive,
    } = job;
    let mut seqs = SeqTracker::default();
    let mut on_event = |data: &[u8]| ctx.shared.record_payload(index, &mut seqs, data);
    let mut decoder = SseDecoder::new();

    let connected = connect_or_stop(&mut stop, connect(&ctx, &mut decoder, &mut on_event)).await;
    drop(permit);
    let stream = match connected {
        Ok(stream) => {
            ctx.connect_succeeded();
            stream
        }
        Err(failure) => {
            ctx.connect_failed(&failure);
            return;
        }
    };

    loop {
        tokio::select! {
            biased;
            () = stopped(&mut stop) => return,
            step = read_step(&stream, &mut decoder, &mut on_event) => {
                let reason = match step {
                    Ok(Step::Progress) => continue,
                    Ok(Step::Eof) => EndReason::Eof,
                    Err(err) => EndReason::Error(err.to_string()),
                };
                ctx.shared.record_closed_early(&reason);
                return;
            }
        }
    }
}

async fn connect(
    ctx: &Ctx,
    decoder: &mut SseDecoder,
    on_event: &mut impl FnMut(&[u8]),
) -> Result<TcpStream, ConnectFailure> {
    let mut stream = TcpStream::connect(ctx.target.addr)
        .await
        .map_err(|err| ConnectFailure::from_io(&err))?;
    let request = format!(
        "GET /topics/{}/sse HTTP/1.1\r\nHost: {}\r\nAccept: text/event-stream\r\nCache-Control: no-cache\r\n\r\n",
        ctx.topic, ctx.target.authority
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|err| ConnectFailure::from_io(&err))?;
    while !decoder.head_complete() {
        match read_step(&stream, decoder, on_event).await {
            Ok(Step::Progress) => {}
            Ok(Step::Eof) => return Err(ConnectFailure::new("connection closed before response")),
            Err(StepError::Io(err)) => return Err(ConnectFailure::from_io(&err)),
            Err(StepError::Decode(err)) => return Err(ConnectFailure::new(err.to_string())),
        }
    }
    Ok(stream)
}

enum Step {
    Progress,
    Eof,
}

enum StepError {
    Io(io::Error),
    Decode(DecodeError),
}

impl fmt::Display for StepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => err.fmt(f),
            Self::Decode(err) => err.fmt(f),
        }
    }
}

/// Reads through a per-thread scratch buffer so an idle connection holds no read buffer.
async fn read_step(
    stream: &TcpStream,
    decoder: &mut SseDecoder,
    on_event: &mut impl FnMut(&[u8]),
) -> Result<Step, StepError> {
    loop {
        stream.readable().await.map_err(StepError::Io)?;
        let outcome = SCRATCH.with_borrow_mut(|buf| match stream.try_read(buf) {
            Ok(0) => Ok(Some(Step::Eof)),
            Ok(n) => decoder
                .feed(&buf[..n], on_event)
                .map(|()| Some(Step::Progress))
                .map_err(StepError::Decode),
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(err) => Err(StepError::Io(err)),
        });
        if let Some(step) = outcome? {
            return Ok(step);
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum DecodeError {
    Status(u16),
    BadHead,
    NotEventStream(Option<String>),
    HeadTooLarge,
    BadChunk,
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status(code) => write!(f, "sse subscribe rejected: HTTP {code}"),
            Self::BadHead => f.write_str("malformed HTTP response head"),
            Self::NotEventStream(Some(content_type)) => write!(
                f,
                "sse subscribe got content-type {content_type:?}, expected text/event-stream"
            ),
            Self::NotEventStream(None) => f.write_str(
                "sse subscribe response has no content-type, expected text/event-stream",
            ),
            Self::HeadTooLarge => f.write_str("HTTP response head too large"),
            Self::BadChunk => f.write_str("malformed chunked encoding"),
        }
    }
}

pub struct SseDecoder {
    head: Vec<u8>,
    body: Option<Body>,
}

struct Body {
    chunks: Option<ChunkDecoder>,
    lines: LineParser,
}

impl SseDecoder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            head: Vec::new(),
            body: None,
        }
    }

    #[must_use]
    pub fn head_complete(&self) -> bool {
        self.body.is_some()
    }

    pub fn feed(
        &mut self,
        bytes: &[u8],
        on_event: &mut impl FnMut(&[u8]),
    ) -> Result<(), DecodeError> {
        if let Some(body) = &mut self.body {
            return body.feed(bytes, on_event);
        }
        self.head.extend_from_slice(bytes);
        let Some(end) = self
            .head
            .windows(HEAD_END.len())
            .position(|window| window == HEAD_END)
        else {
            return if self.head.len() > MAX_HEAD_BYTES {
                Err(DecodeError::HeadTooLarge)
            } else {
                Ok(())
            };
        };
        let chunked = parse_head(&self.head[..end])?;
        let leftover = self.head.split_off(end + HEAD_END.len());
        self.head = Vec::new();
        let body = self.body.insert(Body {
            chunks: chunked.then(ChunkDecoder::new),
            lines: LineParser::default(),
        });
        body.feed(&leftover, on_event)
    }
}

impl Default for SseDecoder {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_head(head: &[u8]) -> Result<bool, DecodeError> {
    let text = std::str::from_utf8(head).map_err(|_| DecodeError::BadHead)?;
    let mut lines = text.split("\r\n");
    let status: u16 = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .ok_or(DecodeError::BadHead)?;
    if status != 200 {
        return Err(DecodeError::Status(status));
    }
    let mut chunked = false;
    let mut content_type = None;
    for (name, value) in lines.filter_map(|line| line.split_once(':')) {
        let name = name.trim();
        if name.eq_ignore_ascii_case("transfer-encoding") {
            chunked |= value.to_ascii_lowercase().contains("chunked");
        } else if name.eq_ignore_ascii_case("content-type") {
            content_type = Some(value.trim());
        }
    }
    if !content_type.is_some_and(is_event_stream) {
        return Err(DecodeError::NotEventStream(content_type.map(str::to_owned)));
    }
    Ok(chunked)
}

fn is_event_stream(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .is_some_and(|media| media.trim().eq_ignore_ascii_case("text/event-stream"))
}

impl Body {
    fn feed(&mut self, bytes: &[u8], on_event: &mut impl FnMut(&[u8])) -> Result<(), DecodeError> {
        let Self { chunks, lines } = self;
        if let Some(chunks) = chunks {
            chunks.feed(bytes, &mut |data| lines.feed(data, on_event))
        } else {
            lines.feed(bytes, on_event);
            Ok(())
        }
    }
}

enum ChunkState {
    Size { value: usize, digits: bool },
    Extension(usize),
    SizeLf(usize),
    Data(usize),
    DataCr,
    DataLf,
    Done,
}

struct ChunkDecoder {
    state: ChunkState,
}

impl ChunkDecoder {
    fn new() -> Self {
        Self {
            state: ChunkState::Size {
                value: 0,
                digits: false,
            },
        }
    }

    fn feed(&mut self, bytes: &[u8], emit: &mut impl FnMut(&[u8])) -> Result<(), DecodeError> {
        let mut i = 0;
        while i < bytes.len() {
            if let ChunkState::Data(remaining) = self.state {
                let take = remaining.min(bytes.len() - i);
                emit(&bytes[i..i + take]);
                i += take;
                self.state = if take == remaining {
                    ChunkState::DataCr
                } else {
                    ChunkState::Data(remaining - take)
                };
                continue;
            }
            let byte = bytes[i];
            i += 1;
            self.state = match self.state {
                ChunkState::Size { value, digits } => match byte {
                    b'\r' if digits => ChunkState::SizeLf(value),
                    b';' if digits => ChunkState::Extension(value),
                    _ => {
                        let digit = char::from(byte).to_digit(16).ok_or(DecodeError::BadChunk)?;
                        let value = value
                            .checked_mul(16)
                            .and_then(|v| v.checked_add(digit as usize))
                            .ok_or(DecodeError::BadChunk)?;
                        ChunkState::Size {
                            value,
                            digits: true,
                        }
                    }
                },
                ChunkState::Extension(value) if byte == b'\r' => ChunkState::SizeLf(value),
                ChunkState::Extension(value) => ChunkState::Extension(value),
                ChunkState::SizeLf(value) if byte == b'\n' => {
                    if value == 0 {
                        ChunkState::Done
                    } else {
                        ChunkState::Data(value)
                    }
                }
                ChunkState::DataCr if byte == b'\r' => ChunkState::DataLf,
                ChunkState::DataLf if byte == b'\n' => ChunkState::Size {
                    value: 0,
                    digits: false,
                },
                ChunkState::Done => ChunkState::Done,
                ChunkState::SizeLf(_)
                | ChunkState::DataCr
                | ChunkState::DataLf
                | ChunkState::Data(_) => return Err(DecodeError::BadChunk),
            };
        }
        Ok(())
    }
}

#[derive(Default)]
struct LineParser {
    line: Vec<u8>,
    data: Vec<u8>,
    has_data: bool,
    skip_lf: bool,
}

impl LineParser {
    fn feed(&mut self, bytes: &[u8], on_event: &mut impl FnMut(&[u8])) {
        for &byte in bytes {
            if std::mem::take(&mut self.skip_lf) && byte == b'\n' {
                continue;
            }
            match byte {
                b'\n' => self.end_line(on_event),
                b'\r' => {
                    self.skip_lf = true;
                    self.end_line(on_event);
                }
                _ => self.line.push(byte),
            }
        }
    }

    fn end_line(&mut self, on_event: &mut impl FnMut(&[u8])) {
        if self.line.is_empty() {
            if self.has_data {
                on_event(&self.data);
                self.data.clear();
                self.has_data = false;
            }
            return;
        }
        if self.line[0] != b':' {
            let (field, value) = match self.line.iter().position(|&b| b == b':') {
                Some(colon) => {
                    let value = &self.line[colon + 1..];
                    (
                        &self.line[..colon],
                        value.strip_prefix(b" ").unwrap_or(value),
                    )
                }
                None => (&self.line[..], &[][..]),
            };
            if field == b"data" {
                if self.has_data {
                    self.data.push(b'\n');
                }
                self.data.extend_from_slice(value);
                self.has_data = true;
            }
        }
        self.line.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(body: &str) -> Vec<u8> {
        format!("{:x}\r\n{body}\r\n", body.len()).into_bytes()
    }

    fn response(chunked: bool) -> Vec<u8> {
        let events = [
            ": keep-alive\n\n",
            "id: 1\ndata: 100 1 xxx\n\nid: 2\ndata: 200 2 yyy\n\n",
            ": \n\n",
            "id: 3\r\ndata: 300 3 zzz\r\n\r\n",
            "data: multi\ndata: line\n\n",
        ];
        let mut out = if chunked {
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n"
                .to_vec()
        } else {
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n".to_vec()
        };
        for event in events {
            if chunked {
                out.extend(chunk(event));
            } else {
                out.extend(event.as_bytes());
            }
        }
        out
    }

    fn decode_with_splits(bytes: &[u8], splits: &[usize]) -> Vec<Vec<u8>> {
        let mut decoder = SseDecoder::new();
        let mut events = Vec::new();
        let mut on_event = |data: &[u8]| events.push(data.to_vec());
        let mut start = 0;
        for &split in splits.iter().chain(std::iter::once(&bytes.len())) {
            decoder
                .feed(&bytes[start..split], &mut on_event)
                .expect("decode");
            start = split;
        }
        assert!(decoder.head_complete());
        events
    }

    fn expected() -> Vec<Vec<u8>> {
        vec![
            b"100 1 xxx".to_vec(),
            b"200 2 yyy".to_vec(),
            b"300 3 zzz".to_vec(),
            b"multi\nline".to_vec(),
        ]
    }

    #[test]
    fn decodes_in_one_piece() {
        for chunked in [true, false] {
            assert_eq!(decode_with_splits(&response(chunked), &[]), expected());
        }
    }

    #[test]
    fn decodes_with_every_two_way_split() {
        for chunked in [true, false] {
            let bytes = response(chunked);
            for split in 0..=bytes.len() {
                assert_eq!(
                    decode_with_splits(&bytes, &[split]),
                    expected(),
                    "chunked={chunked} split={split}"
                );
            }
        }
    }

    #[test]
    fn decodes_one_byte_at_a_time() {
        for chunked in [true, false] {
            let bytes = response(chunked);
            let splits: Vec<usize> = (1..bytes.len()).collect();
            assert_eq!(decode_with_splits(&bytes, &splits), expected());
        }
    }

    #[test]
    fn rejects_non_200_and_bad_chunks() {
        let mut decoder = SseDecoder::new();
        let err = decoder
            .feed(b"HTTP/1.1 400 Bad Request\r\n\r\n", &mut |_| {})
            .expect_err("status");
        assert_eq!(err, DecodeError::Status(400));

        let mut decoder = SseDecoder::new();
        let err = decoder
            .feed(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n",
                &mut |_| {},
            )
            .expect_err("chunk");
        assert_eq!(err, DecodeError::BadChunk);
    }

    #[test]
    fn chunk_extensions_are_ignored() {
        let mut decoder = SseDecoder::new();
        let mut events = Vec::new();
        let bytes =
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n9;ext=1\r\ndata: a\n\n\r\n";
        decoder
            .feed(bytes, &mut |data| events.push(data.to_vec()))
            .expect("decode");
        assert_eq!(events, vec![b"a".to_vec()]);
    }

    fn head_result(head: &str) -> Result<bool, DecodeError> {
        parse_head(head.as_bytes())
    }

    #[test]
    fn accepts_event_stream_content_types() {
        for value in ["text/event-stream", "Text/Event-Stream; charset=utf-8"] {
            let head = format!("HTTP/1.1 200 OK\r\ncontent-type: {value}");
            assert_eq!(head_result(&head), Ok(false), "{value}");
        }
    }

    #[test]
    fn rejects_other_or_missing_content_types() {
        let err = head_result("HTTP/1.1 200 OK\r\nContent-Type: text/html").expect_err("html");
        assert_eq!(
            err,
            DecodeError::NotEventStream(Some("text/html".to_owned()))
        );
        assert!(err.to_string().contains("text/html"));
        let err =
            head_result("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked").expect_err("missing");
        assert_eq!(err, DecodeError::NotEventStream(None));
    }
}
