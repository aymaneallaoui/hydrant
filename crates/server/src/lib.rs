pub mod config;
pub mod error;
pub mod hub;
pub mod metrics;
pub mod routes;
pub mod shutdown;
pub mod state;
pub mod telemetry;

use std::{future::IntoFuture, sync::Arc, time::Duration};

use tokio::{net::TcpListener, sync::Notify};

use crate::{config::Settings, routes::router, state::AppState};

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

    let server = axum::serve(listener, router(state))
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
