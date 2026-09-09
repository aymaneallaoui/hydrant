use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::{
    hub::{HubClosed, TopicNameError},
    metrics::ConnectionLimit,
};

#[derive(Debug)]
pub enum ApiError {
    InvalidTopic(TopicNameError),
    InvalidPayload,
    ShuttingDown,
    AtCapacity,
}

impl From<TopicNameError> for ApiError {
    fn from(err: TopicNameError) -> Self {
        Self::InvalidTopic(err)
    }
}

impl From<HubClosed> for ApiError {
    fn from(_: HubClosed) -> Self {
        Self::ShuttingDown
    }
}

impl From<ConnectionLimit> for ApiError {
    fn from(_: ConnectionLimit) -> Self {
        Self::AtCapacity
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            Self::InvalidTopic(err) => (StatusCode::BAD_REQUEST, err.to_string()),
            Self::InvalidPayload => (
                StatusCode::BAD_REQUEST,
                "payload must be valid UTF-8".to_owned(),
            ),
            Self::ShuttingDown => (
                StatusCode::SERVICE_UNAVAILABLE,
                "server is shutting down".to_owned(),
            ),
            Self::AtCapacity => (
                StatusCode::SERVICE_UNAVAILABLE,
                "server is at its connection limit".to_owned(),
            ),
        }
        .into_response()
    }
}
