use std::time::Duration;

use axum::{
    extract::{
        State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    },
    response::Response,
};
use tokio::time::timeout;

use crate::{
    error::ApiError,
    hub::{Subscription, SubscriptionEnd, TopicName},
    metrics::{ConnectionGuard, Metrics, Transport},
    state::AppState,
};

const CLOSE_REPLY_GRACE: Duration = Duration::from_secs(1);

pub async fn subscribe(
    State(state): State<AppState>,
    topic: TopicName,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let settings = state.settings();
    let send_timeout = settings.send_timeout;
    let guard = state.metrics().connection(Transport::Ws)?;
    let subscription = state.hub().subscribe(&topic)?;
    let metrics = state.metrics().clone();
    Ok(upgrade
        .max_message_size(settings.max_message_bytes)
        .max_frame_size(settings.max_message_bytes)
        .on_upgrade(move |socket| pump(socket, subscription, guard, metrics, send_timeout)))
}

async fn pump(
    mut socket: WebSocket,
    mut subscription: Subscription,
    _guard: ConnectionGuard,
    metrics: std::sync::Arc<Metrics>,
    send_timeout: Duration,
) {
    let close = loop {
        tokio::select! {
            incoming = socket.recv() => match incoming {
                Some(Ok(_)) => {}
                Some(Err(_)) | None => return,
            },
            next = subscription.recv() => match next {
                Ok(message) => {
                    match timeout(send_timeout, socket.send(Message::Text(message.payload))).await {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => return,
                        Err(_) => {
                            metrics.slow_consumer_disconnected();
                            return;
                        }
                    }
                }
                Err(SubscriptionEnd::SlowConsumer) => {
                    break CloseFrame { code: close_code::POLICY, reason: "slow consumer".into() };
                }
                Err(SubscriptionEnd::Closed) => {
                    break CloseFrame { code: close_code::AWAY, reason: "server shutting down".into() };
                }
            },
        }
    };

    if timeout(send_timeout, socket.send(Message::Close(Some(close))))
        .await
        .is_ok_and(|sent| sent.is_ok())
    {
        let _ = timeout(CLOSE_REPLY_GRACE, async {
            while let Some(Ok(_)) = socket.recv().await {}
        })
        .await;
    }
}
