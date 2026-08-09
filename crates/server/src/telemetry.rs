use tracing_subscriber::{EnvFilter, prelude::*};

use crate::config::{Config, LogFormat};

pub fn init(cfg: &Config) -> anyhow::Result<()> {
    let filter = EnvFilter::try_new(&cfg.log_level)?;
    let registry = tracing_subscriber::registry().with(filter);
    match cfg.log_format {
        LogFormat::Json => registry
            .with(tracing_subscriber::fmt::layer().json())
            .init(),
        LogFormat::Pretty => registry.with(tracing_subscriber::fmt::layer()).init(),
    }
    Ok(())
}
