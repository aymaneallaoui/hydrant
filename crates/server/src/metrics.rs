use std::{
    error::Error,
    fmt::{self, Write as _},
    sync::{
        Arc,
        atomic::{
            AtomicU64,
            Ordering::{AcqRel, Acquire, Relaxed},
        },
    },
};

use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Sse,
    Ws,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionLimit;

impl fmt::Display for ConnectionLimit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("server is at its connection limit")
    }
}

impl Error for ConnectionLimit {}

#[derive(Debug, Default)]
pub struct Metrics {
    max_connections: Option<u64>,
    connections_total: AtomicU64,
    connections_rejected: AtomicU64,
    connections_sse: AtomicU64,
    connections_ws: AtomicU64,
    published: AtomicU64,
    dropped: AtomicU64,
    slow_consumer_disconnects: AtomicU64,
    ws_idle: Notify,
}

impl Metrics {
    #[must_use]
    pub fn with_max_connections(max_connections: Option<usize>) -> Self {
        Self {
            max_connections: max_connections.map(|max| u64::try_from(max).unwrap_or(u64::MAX)),
            ..Self::default()
        }
    }

    pub fn connection(
        self: &Arc<Self>,
        transport: Transport,
    ) -> Result<ConnectionGuard, ConnectionLimit> {
        let admitted = if let Some(max) = self.max_connections {
            self.connections_total
                .fetch_update(AcqRel, Acquire, |open| (open < max).then_some(open + 1))
                .is_ok()
        } else {
            self.connections_total.fetch_add(1, Relaxed);
            true
        };
        if !admitted {
            self.connections_rejected.fetch_add(1, Relaxed);
            return Err(ConnectionLimit);
        }
        self.gauge(transport).fetch_add(1, Relaxed);
        Ok(ConnectionGuard {
            metrics: Arc::clone(self),
            transport,
        })
    }

    pub(crate) fn message_published(&self) {
        self.published.fetch_add(1, Relaxed);
    }

    #[must_use]
    pub fn published_total(&self) -> u64 {
        self.published.load(Relaxed)
    }

    #[must_use]
    pub fn connections_rejected_total(&self) -> u64 {
        self.connections_rejected.load(Relaxed)
    }

    #[must_use]
    pub fn connections_open(&self) -> u64 {
        self.connections_total.load(Relaxed)
    }

    pub(crate) fn messages_dropped(&self, count: u64) {
        self.dropped.fetch_add(count, Relaxed);
    }

    pub(crate) fn slow_consumer_disconnected(&self) {
        self.slow_consumer_disconnects.fetch_add(1, Relaxed);
    }

    #[must_use]
    pub fn dropped_total(&self) -> u64 {
        self.dropped.load(Relaxed)
    }

    #[must_use]
    pub fn slow_consumer_disconnects_total(&self) -> u64 {
        self.slow_consumer_disconnects.load(Relaxed)
    }

    pub async fn ws_drained(&self) {
        loop {
            let notified = self.ws_idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.connections_ws.load(Relaxed) == 0 {
                return;
            }
            notified.await;
        }
    }

    #[must_use]
    pub fn render(&self, topics: usize) -> String {
        let mut out = String::with_capacity(1024);
        let _ = write!(
            out,
            "# HELP hydrant_build_info Build information.\n\
             # TYPE hydrant_build_info gauge\n\
             hydrant_build_info{{version=\"{}\"}} 1\n\
             # HELP hydrant_connections Open subscriber connections.\n\
             # TYPE hydrant_connections gauge\n\
             hydrant_connections{{transport=\"sse\"}} {}\n\
             hydrant_connections{{transport=\"ws\"}} {}\n\
             # HELP hydrant_topics Topics with at least one subscriber.\n\
             # TYPE hydrant_topics gauge\n\
             hydrant_topics {topics}\n\
             # HELP hydrant_messages_published_total Messages accepted for publish.\n\
             # TYPE hydrant_messages_published_total counter\n\
             hydrant_messages_published_total {}\n\
             # HELP hydrant_messages_dropped_total Messages skipped by lagging subscribers under the drop policy.\n\
             # TYPE hydrant_messages_dropped_total counter\n\
             hydrant_messages_dropped_total {}\n\
             # HELP hydrant_slow_consumer_disconnects_total Subscribers disconnected for being too slow.\n\
             # TYPE hydrant_slow_consumer_disconnects_total counter\n\
             hydrant_slow_consumer_disconnects_total {}\n\
             # HELP hydrant_connections_rejected_total Subscribe requests refused because the connection limit was reached.\n\
             # TYPE hydrant_connections_rejected_total counter\n\
             hydrant_connections_rejected_total {}\n",
            env!("CARGO_PKG_VERSION"),
            self.connections_sse.load(Relaxed),
            self.connections_ws.load(Relaxed),
            self.published.load(Relaxed),
            self.dropped.load(Relaxed),
            self.slow_consumer_disconnects.load(Relaxed),
            self.connections_rejected.load(Relaxed),
        );
        out
    }

    fn gauge(&self, transport: Transport) -> &AtomicU64 {
        match transport {
            Transport::Sse => &self.connections_sse,
            Transport::Ws => &self.connections_ws,
        }
    }
}

#[derive(Debug)]
pub struct ConnectionGuard {
    metrics: Arc<Metrics>,
    transport: Transport,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let previous = self.metrics.gauge(self.transport).fetch_sub(1, Relaxed);
        self.metrics.connections_total.fetch_sub(1, Relaxed);
        if previous == 1 && self.transport == Transport::Ws {
            self.metrics.ws_idle.notify_waiters();
        }
    }
}
