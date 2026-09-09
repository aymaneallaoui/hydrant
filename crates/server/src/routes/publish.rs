use axum::{
    body::Bytes,
    extract::State,
    http::{StatusCode, header},
    response::IntoResponse,
};

use crate::{error::ApiError, hub::TopicName, state::AppState};

pub async fn publish(
    State(state): State<AppState>,
    topic: TopicName,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let payload = body.try_into().map_err(|_| ApiError::InvalidPayload)?;
    let receivers = state.hub().publish(&topic, payload)?;
    Ok((
        StatusCode::ACCEPTED,
        [(header::CONTENT_TYPE, "application/json")],
        format!("{{\"receivers\":{receivers}}}"),
    ))
}
