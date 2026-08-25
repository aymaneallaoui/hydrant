use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::hub::{HubClosed, TopicNameError};

#[derive(Debug)]
pub enum ApiError {
    InvalidTopic(TopicNameError),
    InvalidPayload,
    ShuttingDown,
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
        }
        .into_response()
    }
}
