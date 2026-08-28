mod publish;
mod sse;
mod ws;

use axum::{
    Router,
    extract::{DefaultBodyLimit, FromRequestParts, Path, State},
    http::{header, request::Parts},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use tower_http::trace::TraceLayer;

use crate::{error::ApiError, hub::TopicName, state::AppState};

const METRICS_CONTENT_TYPE: &str = "text/plain; version=0.0.4";

pub fn router(state: AppState) -> Router {
    let max_message_bytes = state.settings().max_message_bytes;
    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(metrics))
        .route(
            "/topics/{topic}",
            post(publish::publish).layer(DefaultBodyLimit::max(max_message_bytes)),
        )
        .route("/topics/{topic}/sse", get(sse::subscribe))
        .route("/topics/{topic}/ws", get(ws::subscribe))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, METRICS_CONTENT_TYPE)],
        state.metrics().render(state.hub().topic_count()),
    )
}

impl<S: Send + Sync> FromRequestParts<S> for TopicName {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Path(raw) = Path::<String>::from_request_parts(parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        raw.parse::<TopicName>()
            .map_err(|err| ApiError::from(err).into_response())
    }
}
