use std::{
    collections::HashMap,
    error::Error,
    fmt,
    str::FromStr,
    sync::{Arc, Mutex, PoisonError, RwLock},
};

use axum::extract::ws::Utf8Bytes;
use clap::ValueEnum;
use tokio::sync::broadcast::{self, error::RecvError};

use crate::metrics::Metrics;

const MAX_TOPIC_NAME_LEN: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicName(Arc<str>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicNameError {
    Empty,
    TooLong,
    InvalidChar(char),
}

impl fmt::Display for TopicNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("topic name is empty"),
            Self::TooLong => write!(f, "topic name exceeds {MAX_TOPIC_NAME_LEN} characters"),
            Self::InvalidChar(c) => write!(f, "topic name contains invalid character {c:?}"),
        }
    }
}

impl Error for TopicNameError {}

impl FromStr for TopicName {
    type Err = TopicNameError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        if raw.is_empty() {
            return Err(TopicNameError::Empty);
        }
        if let Some(bad) = raw
            .chars()
            .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
        {
            return Err(TopicNameError::InvalidChar(bad));
        }
        if raw.len() > MAX_TOPIC_NAME_LEN {
            return Err(TopicNameError::TooLong);
        }
        Ok(Self(Arc::from(raw)))
    }
}

impl fmt::Display for TopicName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for TopicName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SlowConsumerPolicy {
    Drop,
    Disconnect,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub seq: u64,
    pub payload: Utf8Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionEnd {
    SlowConsumer,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HubClosed;

impl fmt::Display for HubClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("hub is shutting down")
    }
}

impl Error for HubClosed {}

#[derive(Debug)]
struct Topic {
    sender: broadcast::Sender<Message>,
    next_seq: Mutex<u64>,
}

#[derive(Debug, Default)]
struct State {
    topics: HashMap<TopicName, Topic>,
    closed: bool,
}

#[derive(Debug)]
struct Inner {
    state: RwLock<State>,
    capacity: usize,
    policy: SlowConsumerPolicy,
    metrics: Arc<Metrics>,
}

impl Inner {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, State> {
        self.state.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, State> {
        self.state.write().unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Debug, Clone)]
pub struct Hub {
    inner: Arc<Inner>,
}

impl Hub {
    #[must_use]
    pub fn new(capacity: usize, policy: SlowConsumerPolicy, metrics: Arc<Metrics>) -> Self {
        assert!(
            (1..=usize::MAX >> 1).contains(&capacity),
            "topic capacity out of range"
        );
        Self {
            inner: Arc::new(Inner {
                state: RwLock::new(State::default()),
                capacity,
                policy,
                metrics,
            }),
        }
    }

    pub fn publish(&self, topic: &TopicName, payload: Utf8Bytes) -> Result<usize, HubClosed> {
        let state = self.inner.read();
        if state.closed {
            return Err(HubClosed);
        }
        self.inner.metrics.message_published();
        let Some(topic) = state.topics.get(topic) else {
            return Ok(0);
        };
        // seq assignment and send share one critical section so subscribers see seq in order.
        let mut next_seq = topic
            .next_seq
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let seq = *next_seq;
        *next_seq += 1;
        Ok(topic.sender.send(Message { seq, payload }).unwrap_or(0))
    }

    pub fn subscribe(&self, name: &TopicName) -> Result<Subscription, HubClosed> {
        let receiver = {
            let state = self.inner.read();
            if state.closed {
                return Err(HubClosed);
            }
            state.topics.get(name).map(|topic| topic.sender.subscribe())
        };
        let receiver = if let Some(receiver) = receiver {
            receiver
        } else {
            let mut state = self.inner.write();
            if state.closed {
                return Err(HubClosed);
            }
            let capacity = self.inner.capacity;
            state
                .topics
                .entry(name.clone())
                .or_insert_with(|| Topic {
                    sender: broadcast::channel(capacity).0,
                    next_seq: Mutex::new(1),
                })
                .sender
                .subscribe()
        };
        Ok(Subscription {
            receiver,
            cleanup: TopicCleanup {
                inner: Arc::clone(&self.inner),
                name: name.clone(),
            },
        })
    }

    pub fn close(&self) {
        let mut state = self.inner.write();
        state.closed = true;
        state.topics.clear();
    }

    #[must_use]
    pub fn topic_count(&self) -> usize {
        self.inner.read().topics.len()
    }

    #[must_use]
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.inner.metrics
    }
}

#[derive(Debug)]
pub struct Subscription {
    // Field order is load-bearing: `receiver` must drop before `cleanup` runs its check.
    receiver: broadcast::Receiver<Message>,
    cleanup: TopicCleanup,
}

impl Subscription {
    pub async fn recv(&mut self) -> Result<Message, SubscriptionEnd> {
        let inner = &self.cleanup.inner;
        loop {
            match self.receiver.recv().await {
                Ok(message) => return Ok(message),
                Err(RecvError::Closed) => return Err(SubscriptionEnd::Closed),
                Err(RecvError::Lagged(skipped)) => match inner.policy {
                    SlowConsumerPolicy::Drop => inner.metrics.messages_dropped(skipped),
                    SlowConsumerPolicy::Disconnect => {
                        inner.metrics.slow_consumer_disconnected();
                        return Err(SubscriptionEnd::SlowConsumer);
                    }
                },
            }
        }
    }
}

#[derive(Debug)]
struct TopicCleanup {
    inner: Arc<Inner>,
    name: TopicName,
}

impl Drop for TopicCleanup {
    fn drop(&mut self) {
        let mut state = self.inner.write();
        let unused = state
            .topics
            .get(&self.name)
            .is_some_and(|topic| topic.sender.receiver_count() == 0);
        if unused {
            state.topics.remove(&self.name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(raw: &str) -> TopicName {
        raw.parse().expect("valid topic name")
    }

    fn payload(raw: &'static str) -> Utf8Bytes {
        Utf8Bytes::from_static(raw)
    }

    fn hub(capacity: usize, policy: SlowConsumerPolicy) -> Hub {
        Hub::new(capacity, policy, Arc::default())
    }

    #[test]
    fn topic_name_accepts_allowed_characters() {
        for raw in ["a", "Room-1", "a.b_c-D9", &"x".repeat(128)] {
            assert!(raw.parse::<TopicName>().is_ok(), "{raw}");
        }
    }

    #[test]
    fn topic_name_rejects_invalid_input() {
        assert_eq!("".parse::<TopicName>(), Err(TopicNameError::Empty));
        assert_eq!(
            "x".repeat(129).parse::<TopicName>(),
            Err(TopicNameError::TooLong)
        );
        assert_eq!(
            "a b".parse::<TopicName>(),
            Err(TopicNameError::InvalidChar(' '))
        );
        assert_eq!(
            "a/b".parse::<TopicName>(),
            Err(TopicNameError::InvalidChar('/'))
        );
        assert_eq!(
            "caf\u{e9}".parse::<TopicName>(),
            Err(TopicNameError::InvalidChar('\u{e9}'))
        );
    }

    #[tokio::test]
    async fn publish_reaches_all_subscribers_in_order() {
        let hub = hub(8, SlowConsumerPolicy::Disconnect);
        let topic = name("t");
        let mut a = hub.subscribe(&topic).expect("subscribe");
        let mut b = hub.subscribe(&topic).expect("subscribe");

        assert_eq!(hub.publish(&topic, payload("one")), Ok(2));
        assert_eq!(hub.publish(&topic, payload("two")), Ok(2));

        for sub in [&mut a, &mut b] {
            let first = sub.recv().await.expect("first");
            let second = sub.recv().await.expect("second");
            assert_eq!((first.seq, first.payload.as_str()), (1, "one"));
            assert_eq!((second.seq, second.payload.as_str()), (2, "two"));
        }
    }

    #[test]
    fn publish_without_subscribers_creates_no_topic() {
        let hub = hub(8, SlowConsumerPolicy::Disconnect);
        assert_eq!(hub.publish(&name("t"), payload("x")), Ok(0));
        assert_eq!(hub.topic_count(), 0);
    }

    #[test]
    fn topic_is_removed_after_last_subscription_drops() {
        let hub = hub(8, SlowConsumerPolicy::Disconnect);
        let topic = name("t");
        let a = hub.subscribe(&topic).expect("subscribe");
        let b = hub.subscribe(&topic).expect("subscribe");
        assert_eq!(hub.topic_count(), 1);

        drop(a);
        assert_eq!(hub.topic_count(), 1);
        drop(b);
        assert_eq!(hub.topic_count(), 0);
    }

    #[tokio::test]
    async fn drop_policy_skips_and_counts_overflow() {
        let hub = hub(4, SlowConsumerPolicy::Drop);
        let topic = name("t");
        let mut sub = hub.subscribe(&topic).expect("subscribe");
        for _ in 0..10 {
            assert_eq!(hub.publish(&topic, payload("m")), Ok(1));
        }

        let next = sub.recv().await.expect("oldest retained message");
        assert_eq!(next.seq, 7);
        assert_eq!(hub.metrics().dropped_total(), 6);
        assert_eq!(hub.metrics().slow_consumer_disconnects_total(), 0);
        for expected in 8..=10 {
            assert_eq!(sub.recv().await.expect("retained").seq, expected);
        }
    }

    #[tokio::test]
    async fn disconnect_policy_reports_slow_consumer() {
        let hub = hub(4, SlowConsumerPolicy::Disconnect);
        let topic = name("t");
        let mut sub = hub.subscribe(&topic).expect("subscribe");
        for _ in 0..10 {
            assert_eq!(hub.publish(&topic, payload("m")), Ok(1));
        }

        assert_eq!(sub.recv().await.unwrap_err(), SubscriptionEnd::SlowConsumer);
        assert_eq!(hub.metrics().slow_consumer_disconnects_total(), 1);
        assert_eq!(hub.metrics().dropped_total(), 0);
    }

    #[tokio::test]
    async fn close_ends_subscriptions_and_refuses_new_ones() {
        let hub = hub(4, SlowConsumerPolicy::Disconnect);
        let topic = name("t");
        let mut sub = hub.subscribe(&topic).expect("subscribe");

        hub.close();

        assert_eq!(sub.recv().await.unwrap_err(), SubscriptionEnd::Closed);
        assert_eq!(hub.topic_count(), 0);
        assert_eq!(hub.subscribe(&topic).unwrap_err(), HubClosed);
        assert_eq!(hub.publish(&topic, payload("x")), Err(HubClosed));
    }

    #[test]
    fn publish_after_close_fails_and_is_not_counted() {
        let hub = hub(4, SlowConsumerPolicy::Disconnect);
        let topic = name("t");
        assert_eq!(hub.publish(&topic, payload("before")), Ok(0));
        assert_eq!(hub.metrics().published_total(), 1);

        hub.close();

        assert_eq!(hub.publish(&topic, payload("after")), Err(HubClosed));
        assert_eq!(hub.metrics().published_total(), 1);
    }
}
