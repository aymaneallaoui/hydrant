use std::time::Duration;

use clap::Parser;
use hydrant_server::{config::Config, serve, shutdown, telemetry};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::parse();
    telemetry::init(&cfg)?;
    let listener = TcpListener::bind(cfg.listen_addr).await?;
    serve(
        listener,
        Duration::from_secs(cfg.shutdown_timeout_secs),
        shutdown::signal(),
    )
    .await
}
