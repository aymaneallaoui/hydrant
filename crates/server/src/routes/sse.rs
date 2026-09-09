use std::{convert::Infallible, sync::Arc, time::Duration};

use axum::{
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
};
use futures_util::stream::{self, Stream};
use tokio::{sync::mpsc, time::timeout};

use crate::{
    error::ApiError,
    hub::{Subscription, TopicName},
    metrics::{ConnectionGuard, Metrics, Transport},
    state::AppState,
};

pub async fn subscribe(
    State(state): State<AppState>,
    topic: TopicName,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let settings = state.settings();
    let guard = state.metrics().connection(Transport::Sse)?;
    let subscription = state.hub().subscribe(&topic)?;
    let (events_tx, events_rx) = mpsc::channel(1);
    tokio::spawn(pump(
        subscription,
        guard,
        events_tx,
        Arc::clone(state.metrics()),
        settings.send_timeout,
    ));
    let events = stream::unfold(events_rx, |mut rx| async move {
        rx.recv().await.map(|event| (Ok(event), rx))
    });
    Ok(Sse::new(events).keep_alive(KeepAlive::new().interval(settings.sse_keepalive)))
}

// The pump owns the subscription and gauge guard so a stalled socket cannot pin them.
async fn pump(
    mut subscription: Subscription,
    _guard: ConnectionGuard,
    events: mpsc::Sender<Event>,
    metrics: Arc<Metrics>,
    send_timeout: Duration,
) {
    loop {
        let message = tokio::select! {
            () = events.closed() => return,
            next = subscription.recv() => match next {
                Ok(message) => message,
                Err(_) => return,
            },
        };
        let event = Event::default()
            .id(message.seq.to_string())
            .data(message.payload.as_str());
        match timeout(send_timeout, events.send(event)).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return,
            Err(_) => {
                metrics.slow_consumer_disconnected();
                return;
            }
        }
    }
}
