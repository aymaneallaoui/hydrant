pub mod sse;
pub mod ws;

use std::{
    fmt,
    future::Future,
    io,
    sync::{Arc, atomic::Ordering::Relaxed},
    time::Duration,
};

use tokio::{
    sync::{OwnedSemaphorePermit, mpsc, watch},
    time::timeout,
};

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
    pub payload_bytes: usize,
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

async fn connect_or_stop<T>(
    stop: &mut watch::Receiver<bool>,
    connect: impl Future<Output = Result<T, ConnectFailure>>,
) -> Result<T, ConnectFailure> {
    tokio::select! {
        biased;
        () = stopped(stop) => Err(ConnectFailure::new("interrupted during connect")),
        result = timeout(CONNECT_TIMEOUT, connect) => {
            result.unwrap_or_else(|_| Err(ConnectFailure::new("connect timed out")))
        }
    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::{net::TcpListener, sync::Semaphore};

    use super::*;

    async fn stop_ends_a_stalled_handshake(transport: Transport) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (connected_tx, mut connected_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared::new());
        let ctx = Arc::new(Ctx {
            shared: Arc::clone(&shared),
            target: Target {
                addr,
                authority: addr.to_string(),
            },
            topic: "t".to_owned(),
            payload_bytes: 64,
            transport,
            connected: connected_tx,
        });
        let (stop_tx, stop_rx) = watch::channel(false);
        let (alive_tx, mut alive_rx) = mpsc::channel::<()>(1);
        let permit = Arc::new(Semaphore::new(1))
            .acquire_owned()
            .await
            .expect("permit");
        let task = tokio::spawn(run(Job {
            ctx,
            index: 0,
            permit,
            stop: stop_rx,
            alive: alive_tx,
        }));

        let (_silent, _) = listener.accept().await.expect("accept");
        stop_tx.send(true).expect("send stop");

        let bound = CONNECT_TIMEOUT / 6;
        timeout(bound, alive_rx.recv())
            .await
            .expect("subscriber must end promptly after stop");
        task.await.expect("task");
        connected_rx.recv().await.expect("attempt is reported");
        assert_eq!(shared.established.load(Relaxed), 0);
        assert_eq!(shared.connect_failed.load(Relaxed), 1);
    }

    #[tokio::test]
    async fn stop_ends_a_stalled_ws_handshake_promptly() {
        stop_ends_a_stalled_handshake(Transport::Ws).await;
    }

    #[tokio::test]
    async fn stop_ends_a_stalled_sse_handshake_promptly() {
        stop_ends_a_stalled_handshake(Transport::Sse).await;
    }
}
