use axum::{
    body::Bytes,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

use crate::{error::ApiError, hub::TopicName, state::AppState};

/// The topic rejection is deferred until the body is read: answering and closing with unread request bytes makes the kernel send an RST that can destroy the response.
pub async fn publish(
    State(state): State<AppState>,
    topic: Result<TopicName, Response>,
    body: Bytes,
) -> Result<impl IntoResponse, Response> {
    let topic = topic?;
    if body.is_empty() {
        return Err(ApiError::EmptyPayload.into_response());
    }
    let payload = body
        .try_into()
        .map_err(|_| ApiError::InvalidPayload.into_response())?;
    let receivers = state
        .hub()
        .publish(&topic, payload)
        .map_err(|err| ApiError::from(err).into_response())?;
    Ok((
        StatusCode::ACCEPTED,
        [(header::CONTENT_TYPE, "application/json")],
        format!("{{\"receivers\":{receivers}}}"),
    ))
}
