use std::sync::Arc;

use crate::{config::Settings, hub::Hub, metrics::Metrics};

#[derive(Debug, Clone)]
pub struct AppState {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    hub: Hub,
    metrics: Arc<Metrics>,
    settings: Settings,
}

impl AppState {
    #[must_use]
    pub fn new(settings: Settings) -> Self {
        let metrics = Arc::new(Metrics::default());
        let hub = Hub::new(
            settings.topic_capacity,
            settings.slow_consumer_policy,
            Arc::clone(&metrics),
        );
        Self {
            inner: Arc::new(Inner {
                hub,
                metrics,
                settings,
            }),
        }
    }

    #[must_use]
    pub fn hub(&self) -> &Hub {
        &self.inner.hub
    }

    #[must_use]
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.inner.metrics
    }

    #[must_use]
    pub fn settings(&self) -> &Settings {
        &self.inner.settings
    }
}
