use std::convert::Infallible;

use axum::{
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
};
use futures_util::stream::{self, Stream};

use crate::{
    error::ApiError,
    hub::{Subscription, TopicName},
    metrics::{ConnectionGuard, Transport},
    state::AppState,
};

pub async fn subscribe(
    State(state): State<AppState>,
    topic: TopicName,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let guard = state.metrics().connection(Transport::Sse);
    let subscription = state.hub().subscribe(&topic)?;
    let events = stream::unfold((subscription, guard), next_event);
    Ok(Sse::new(events).keep_alive(KeepAlive::new().interval(state.settings().sse_keepalive)))
}

async fn next_event(
    (mut subscription, guard): (Subscription, ConnectionGuard),
) -> Option<(Result<Event, Infallible>, (Subscription, ConnectionGuard))> {
    let message = subscription.recv().await.ok()?;
    let event = Event::default()
        .id(message.seq.to_string())
        .data(message.payload.as_str());
    Some((Ok(event), (subscription, guard)))
}
