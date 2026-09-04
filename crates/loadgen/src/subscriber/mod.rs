pub mod sse;
pub mod ws;

use std::{
    fmt, io,
    sync::{Arc, atomic::Ordering::Relaxed},
    time::Duration,
};

use tokio::sync::{OwnedSemaphorePermit, mpsc, watch};

use crate::{
    cli::{Target, Transport},
    stats::Shared,
};

pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const EMFILE: i32 = 24;
const ENFILE: i32 = 23;

pub struct Ctx {
    pub shared: Arc<Shared>,
    pub target: Target,
    pub topic: String,
    pub transport: Transport,
    pub connected: mpsc::UnboundedSender<()>,
}

impl Ctx {
    fn connect_succeeded(&self) {
        self.shared.established.fetch_add(1, Relaxed);
        let _ = self.connected.send(());
    }

    fn connect_failed(&self, failure: &ConnectFailure) {
        self.shared.record_connect_failure(failure);
        let _ = self.connected.send(());
    }
}

pub struct Job {
    pub ctx: Arc<Ctx>,
    pub index: usize,
    pub permit: OwnedSemaphorePermit,
    pub stop: watch::Receiver<bool>,
    pub alive: mpsc::Sender<()>,
}

pub async fn run(job: Job) {
    match job.ctx.transport {
        Transport::Ws => ws::run(job).await,
        Transport::Sse => sse::run(job).await,
    }
}

async fn stopped(stop: &mut watch::Receiver<bool>) {
    let _ = stop.wait_for(|stopped| *stopped).await;
}

#[derive(Debug, Clone)]
pub struct ConnectFailure {
    pub message: String,
    pub fd_exhausted: bool,
}

impl ConnectFailure {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            fd_exhausted: false,
        }
    }

    #[must_use]
    pub fn from_io(err: &io::Error) -> Self {
        let fd_exhausted = matches!(err.raw_os_error(), Some(EMFILE | ENFILE))
            || err.to_string().contains("Too many open files");
        Self {
            message: if fd_exhausted {
                "Too many open files".to_owned()
            } else {
                err.to_string()
            },
            fd_exhausted,
        }
    }
}

#[derive(Debug, Clone)]
pub enum EndReason {
    WsClose(Option<u16>),
    Eof,
    Error(String),
}

impl fmt::Display for EndReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WsClose(Some(code)) => write!(f, "ws close {code}"),
            Self::WsClose(None) => f.write_str("ws close (no code)"),
            Self::Eof => f.write_str("eof"),
            Self::Error(message) => write!(f, "error: {message}"),
        }
    }
}
