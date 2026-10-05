pub mod config;
pub mod error;
pub mod hub;
pub mod metrics;
pub mod routes;
pub mod shutdown;
pub mod state;
pub mod telemetry;

use std::{future::IntoFuture, io, net::SocketAddr, sync::Arc, time::Duration};

use tokio::{
    net::{TcpListener, TcpSocket},
    sync::Notify,
};

use crate::{config::Settings, routes::router, state::AppState};

/// Binds like `TcpListener::bind` (`SO_REUSEADDR` on, except on Windows) but with an explicit listen backlog.
/// Must be called inside a tokio runtime.
pub fn bind_listener(addr: SocketAddr, backlog: u32) -> io::Result<TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    #[cfg(not(windows))]
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(backlog)
}

pub async fn serve(
    listener: TcpListener,
    settings: Settings,
    shutdown_timeout: Duration,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    tracing::info!(addr = %listener.local_addr()?, "listening");

    let state = AppState::new(settings);
    let hub = state.hub().clone();
    let metrics = Arc::clone(state.metrics());

    let triggered = Arc::new(Notify::new());
    let trigger = Arc::clone(&triggered);
    let graceful = async move {
        shutdown.await;
        tracing::info!("shutdown signal received, draining connections");
        hub.close();
        trigger.notify_one();
    };

    // into_make_service turns the router into routes once; serving the Router directly rebuilds
    // its route table for every accepted connection and keeps that copy alive with the connection.
    let server = axum::serve(listener, router(state).into_make_service())
        .with_graceful_shutdown(graceful)
        .into_future();
    tokio::pin!(server);

    tokio::select! {
        result = &mut server => result?,
        () = triggered.notified() => {
            let drained = async {
                let result = (&mut server).await;
                metrics.ws_drained().await;
                result
            };
            if let Ok(result) = tokio::time::timeout(shutdown_timeout, drained).await {
                result?;
            } else {
                tracing::warn!(timeout = ?shutdown_timeout, "drain timed out, closing remaining connections");
            }
        }
    }

    tracing::info!("server stopped");
    Ok(())
}
