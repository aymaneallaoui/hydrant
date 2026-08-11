pub mod config;
pub mod routes;
pub mod shutdown;
pub mod telemetry;

use std::{future::IntoFuture, sync::Arc, time::Duration};

use tokio::{net::TcpListener, sync::Notify};

use crate::routes::router;

pub async fn serve(
    listener: TcpListener,
    shutdown_timeout: Duration,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    tracing::info!(addr = %listener.local_addr()?, "listening");

    let triggered = Arc::new(Notify::new());
    let trigger = Arc::clone(&triggered);
    let graceful = async move {
        shutdown.await;
        tracing::info!("shutdown signal received, draining connections");
        trigger.notify_one();
    };

    let server = axum::serve(listener, router())
        .with_graceful_shutdown(graceful)
        .into_future();
    tokio::pin!(server);

    tokio::select! {
        result = &mut server => result?,
        () = triggered.notified() => {
            if tokio::time::timeout(shutdown_timeout, &mut server).await.is_err() {
                tracing::warn!(timeout = ?shutdown_timeout, "drain timed out, closing remaining connections");
            }
        }
    }

    tracing::info!("server stopped");
    Ok(())
}
