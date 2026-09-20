use std::time::Duration;

use clap::Parser;
use hydrant_server::{bind_listener, config::Config, serve, shutdown, telemetry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::parse();
    telemetry::init(&cfg)?;
    let listener = bind_listener(cfg.listen_addr, cfg.listen_backlog)?;
    serve(
        listener,
        cfg.settings(),
        Duration::from_secs(cfg.shutdown_timeout_secs),
        shutdown::signal(),
    )
    .await
}
