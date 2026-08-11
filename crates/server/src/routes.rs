use axum::{Router, http::header, routing::get};
use tower_http::trace::TraceLayer;

const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4";

pub fn router() -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .layer(TraceLayer::new_for_http())
}

async fn healthz() -> &'static str {
    "ok"
}

async fn metrics() -> ([(header::HeaderName, &'static str); 1], String) {
    let body = format!(
        "# HELP hydrant_build_info Build information.\n\
         # TYPE hydrant_build_info gauge\n\
         hydrant_build_info{{version=\"{}\"}} 1\n",
        env!("CARGO_PKG_VERSION")
    );
    ([(header::CONTENT_TYPE, METRICS_CONTENT_TYPE)], body)
}
