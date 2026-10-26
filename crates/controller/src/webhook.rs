use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use reqwest::{Client, StatusCode};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use weave_core::auth::Token;
use weave_core::logging::ErrorChain;
use weave_core::webhook::{Event, EventType, Subject};

#[derive(Debug, Clone)]
pub struct Config {
    pub url: Option<String>,

    pub token: Option<String>,

    pub events: Vec<EventType>,

    pub queue_capacity: usize,

    pub max_attempts: u32,

    pub backoff: Duration,

    pub timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            url: None,
            token: None,
            events: Vec::new(),
            queue_capacity: 256,
            max_attempts: 4,
            backoff: Duration::from_millis(250),
            timeout: Duration::from_secs(5),
        }
    }
}

pub struct Emitter {
    tx: mpsc::Sender<Event>,
    allowed: Vec<EventType>,
    dropped: AtomicU64,
    dropping: AtomicBool,
}

impl Emitter {
    pub fn new(config: Config) -> Option<Self> {
        let url = config
            .url
            .as_deref()
            .map(str::trim)
            .filter(|url| !url.is_empty())?
            .to_string();
        let allowed = if config.events.is_empty() {
            EventType::ALL.to_vec()
        } else {
            config.events
        };
        let client = match Client::builder().timeout(config.timeout).build() {
            Ok(client) => client,
            Err(err) => {
                tracing::error!(error = %ErrorChain(&err), "building the webhook HTTP client failed; webhooks are off");
                return None;
            }
        };

        let (tx, rx) = mpsc::channel(config.queue_capacity);
        tokio::spawn(deliver_loop(
            rx,
            client,
            url.clone(),
            config.token.as_deref().and_then(Token::new),
            config.max_attempts,
            config.backoff,
        ));

        let events = allowed
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>()
            .join(",");
        tracing::info!(%url, %events, "webhooks enabled");

        Some(Self {
            tx,
            allowed,
            dropped: AtomicU64::new(0),
            dropping: AtomicBool::new(false),
        })
    }

    pub fn emit(&self, event_type: EventType, subject: impl Into<Subject>) {
        if !self.allowed.contains(&event_type) {
            return;
        }
        let event = Event {
            event_id: event_id(),
            event_type,
            occurred_at: now_rfc3339(),
            subject: subject.into(),
        };

        match self.tx.try_send(event) {
            Ok(()) => {
                if self.dropping.swap(false, Ordering::Relaxed) {
                    tracing::info!(
                        dropped = self.dropped.load(Ordering::Relaxed),
                        "webhook queue is accepting events again"
                    );
                }
            }
            Err(TrySendError::Full(event)) => {
                let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                if !self.dropping.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        event_type = %event.event_type,
                        dropped,
                        "webhook queue is full; dropping events"
                    );
                }
            }
            Err(TrySendError::Closed(event)) => {
                tracing::error!(
                    event_type = %event.event_type,
                    "the webhook worker has stopped; dropping event"
                );
            }
        }
    }

    #[cfg(test)]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

fn event_id() -> String {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).expect("generating a webhook event id");
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown webhook event type `{0}`; known types are {known}", known = known_events())]
pub struct UnknownEventType(String);

fn known_events() -> String {
    EventType::ALL
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn parse_events(configured: &[String]) -> Result<Vec<EventType>, UnknownEventType> {
    configured
        .iter()
        .map(|name| name.trim())
        .filter(|name| !name.is_empty())
        .map(|name| EventType::parse(name).ok_or_else(|| UnknownEventType(name.to_string())))
        .collect()
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

async fn deliver_loop(
    mut rx: mpsc::Receiver<Event>,
    client: Client,
    url: String,
    token: Option<Token>,
    max_attempts: u32,
    backoff: Duration,
) {
    while let Some(event) = rx.recv().await {
        deliver(&client, &url, token.as_ref(), &event, max_attempts, backoff).await;
    }
}

async fn deliver(
    client: &Client,
    url: &str,
    token: Option<&Token>,
    event: &Event,
    max_attempts: u32,
    backoff: Duration,
) {
    let mut delay = backoff;
    for attempt in 1..=max_attempts {
        let mut request = client.post(url).json(event);
        if let Some(token) = token {
            request = request.header(reqwest::header::AUTHORIZATION, token.header_value());
        }

        match request.send().await {
            Ok(response) if response.status().is_success() => {
                tracing::debug!(
                    event_id = %event.event_id,
                    event_type = %event.event_type,
                    "webhook delivered"
                );
                return;
            }

            Ok(response) if is_permanent(response.status()) => {
                tracing::warn!(
                    event_id = %event.event_id,
                    status = %response.status(),
                    "webhook receiver rejected the event; not retrying"
                );
                return;
            }
            Ok(response) => tracing::warn!(
                event_id = %event.event_id,
                status = %response.status(),
                attempt,
                "webhook delivery failed"
            ),
            Err(err) => tracing::warn!(
                event_id = %event.event_id,
                error = %ErrorChain(&err),
                attempt,
                "webhook delivery failed"
            ),
        }

        if attempt < max_attempts {
            tokio::time::sleep(delay).await;
            delay *= 2;
        }
    }
    tracing::warn!(
        event_id = %event.event_id,
        event_type = %event.event_type,
        max_attempts,
        "giving up on webhook delivery"
    );
}

fn is_permanent(status: StatusCode) -> bool {
    status.is_client_error()
        && !matches!(
            status,
            StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
        )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode, header::AUTHORIZATION};
    use axum::routing::post;
    use axum::{Json, Router};
    use weave_core::webhook::{NodeSummary, StreamSummary};
    use weave_core::{NodeCapabilities, NodeStatus, PathStatus};

    pub(crate) struct Delivery {
        auth: Option<String>,
        pub(crate) event: Event,
    }

    pub(crate) struct Sink {
        pub(crate) url: String,
        deliveries: mpsc::UnboundedReceiver<Delivery>,
    }

    #[derive(Clone)]
    struct SinkState {
        deliveries: mpsc::UnboundedSender<Delivery>,
        status: StatusCode,

        stall: bool,
    }

    async fn record(
        State(state): State<SinkState>,
        headers: HeaderMap,
        Json(event): Json<Event>,
    ) -> StatusCode {
        let auth = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let _ = state.deliveries.send(Delivery { auth, event });
        if state.stall {
            std::future::pending::<()>().await;
        }
        state.status
    }

    pub(crate) async fn sink(status: StatusCode) -> Sink {
        spawn_sink(status, false).await
    }

    async fn spawn_sink(status: StatusCode, stall: bool) -> Sink {
        let (tx, deliveries) = mpsc::unbounded_channel();
        let app = Router::new()
            .route("/hook", post(record))
            .with_state(SinkState {
                deliveries: tx,
                status,
                stall,
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Sink {
            url: format!("http://{addr}/hook"),
            deliveries,
        }
    }

    impl Sink {
        pub(crate) async fn next(&mut self) -> Delivery {
            tokio::time::timeout(Duration::from_secs(5), self.deliveries.recv())
                .await
                .expect("a delivery within the timeout")
                .expect("the sink is still running")
        }

        pub(crate) async fn expect_idle(&mut self) {
            let extra =
                tokio::time::timeout(Duration::from_millis(300), self.deliveries.recv()).await;
            assert!(extra.is_err(), "expected no further delivery");
        }
    }

    fn config(url: &str) -> Config {
        Config {
            url: Some(url.to_string()),
            max_attempts: 3,
            backoff: Duration::from_millis(1),
            timeout: Duration::from_secs(2),
            ..Config::default()
        }
    }

    fn node(id: &str) -> NodeSummary {
        NodeSummary {
            id: id.to_string(),
            status: NodeStatus::Ready,
            endpoint: format!("http://{id}:8080"),
            capabilities: NodeCapabilities::default(),
            topology: weave_core::NodeTopology::default(),
        }
    }

    #[tokio::test]
    async fn new_returns_none_without_a_url() {
        assert!(Emitter::new(Config::default()).is_none());
        assert!(
            Emitter::new(Config {
                url: Some("  ".to_string()),
                ..Config::default()
            })
            .is_none(),
            "an env var set to the empty string reads as unset"
        );
    }

    #[tokio::test]
    async fn delivers_the_event_with_a_bearer_token_when_one_is_configured() {
        let mut sink = sink(StatusCode::OK).await;
        let emitter = Emitter::new(Config {
            token: Some("hook-token".to_string()),
            ..config(&sink.url)
        })
        .unwrap();

        emitter.emit(EventType::NodeRegistered, node("guest-1"));

        let delivery = sink.next().await;
        assert_eq!(delivery.auth.as_deref(), Some("Bearer hook-token"));
        assert_eq!(delivery.event.event_type, EventType::NodeRegistered);
        assert_eq!(delivery.event.subject.id(), "guest-1");
        assert_eq!(delivery.event.event_id.len(), 32);
        assert!(
            delivery
                .event
                .event_id
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert!(!delivery.event.occurred_at.is_empty());
    }

    #[tokio::test]
    async fn emitters_started_together_never_share_an_event_id() {
        let mut sink = sink(StatusCode::OK).await;
        let earlier = Emitter::new(config(&sink.url)).unwrap();
        let later = Emitter::new(config(&sink.url)).unwrap();
        for _ in 0..50 {
            earlier.emit(EventType::NodeRegistered, node("guest-1"));
            later.emit(EventType::NodeRegistered, node("guest-1"));
        }

        let mut ids = std::collections::HashSet::new();
        for _ in 0..100 {
            assert!(ids.insert(sink.next().await.event.event_id));
        }
    }

    #[tokio::test]
    async fn omits_the_authorization_header_without_a_token() {
        let mut sink = sink(StatusCode::OK).await;
        let emitter = Emitter::new(config(&sink.url)).unwrap();

        emitter.emit(EventType::NodeOffline, node("guest-1"));

        assert_eq!(sink.next().await.auth, None);
    }

    #[tokio::test]
    async fn retries_a_failing_receiver_up_to_the_cap_then_gives_up() {
        let mut sink = sink(StatusCode::INTERNAL_SERVER_ERROR).await;
        let emitter = Emitter::new(config(&sink.url)).unwrap();

        emitter.emit(EventType::NodeRegistered, node("guest-1"));

        let first = sink.next().await.event;
        for _ in 1..3 {
            assert_eq!(
                sink.next().await.event.event_id,
                first.event_id,
                "a retry carries the id of the delivery it repeats"
            );
        }
        sink.expect_idle().await;
    }

    #[tokio::test]
    async fn retries_a_throttled_or_timed_out_delivery() {
        for status in [StatusCode::TOO_MANY_REQUESTS, StatusCode::REQUEST_TIMEOUT] {
            let mut sink = sink(status).await;
            let emitter = Emitter::new(config(&sink.url)).unwrap();

            emitter.emit(EventType::NodeRegistered, node("guest-1"));

            for _ in 0..3 {
                sink.next().await;
            }
            sink.expect_idle().await;
        }
    }

    #[tokio::test]
    async fn does_not_retry_a_rejected_event() {
        let mut sink = sink(StatusCode::BAD_REQUEST).await;
        let emitter = Emitter::new(config(&sink.url)).unwrap();

        emitter.emit(EventType::NodeRegistered, node("guest-1"));

        sink.next().await;
        sink.expect_idle().await;
    }

    #[tokio::test]
    async fn drops_rather_than_blocks_when_the_queue_is_full() {
        let mut sink = spawn_sink(StatusCode::OK, true).await;
        let emitter = Emitter::new(Config {
            queue_capacity: 2,
            timeout: Duration::from_secs(30),
            ..config(&sink.url)
        })
        .unwrap();

        emitter.emit(EventType::NodeRegistered, node("guest-1"));

        sink.next().await;
        for _ in 0..16 {
            emitter.emit(EventType::NodeRegistered, node("guest-1"));
        }

        assert!(emitter.dropped() > 0, "expected the emitter to drop events");
    }

    #[tokio::test]
    async fn an_allowlist_filters_deliveries() {
        let mut sink = sink(StatusCode::OK).await;
        let emitter = Emitter::new(Config {
            events: vec![EventType::NodeOffline],
            ..config(&sink.url)
        })
        .unwrap();

        emitter.emit(EventType::NodeRegistered, node("guest-1"));
        emitter.emit(EventType::NodeOnline, node("guest-1"));
        emitter.emit(EventType::NodeOffline, node("guest-1"));

        assert_eq!(sink.next().await.event.event_type, EventType::NodeOffline);
        sink.expect_idle().await;
    }

    #[tokio::test]
    async fn an_allowlist_can_name_the_stream_event() {
        let mut sink = sink(StatusCode::OK).await;
        let emitter = Emitter::new(Config {
            events: vec![EventType::StreamChanged],
            ..config(&sink.url)
        })
        .unwrap();

        emitter.emit(EventType::NodeRegistered, node("guest-1"));
        emitter.emit(
            EventType::StreamChanged,
            StreamSummary {
                format_policy: weave_core::FormatPolicy::Report,
                name: "basic".to_string(),
                generation: 1,
                observed_generation: Some(1),
                status: PathStatus::Pending,
                conditions: Vec::new(),
                destinations: Vec::new(),
            },
        );

        let event = sink.next().await.event;
        assert_eq!(event.event_type, EventType::StreamChanged);
        assert_eq!(event.subject.id(), "basic");
        sink.expect_idle().await;
    }

    #[tokio::test]
    async fn an_unreachable_receiver_does_not_stall_the_emitter() {
        let emitter = Emitter::new(Config {
            url: Some("http://192.0.2.1:1/hook".to_string()),
            timeout: Duration::from_millis(50),
            ..config("unused")
        })
        .unwrap();

        let queued = tokio::time::timeout(Duration::from_millis(200), async {
            for _ in 0..8 {
                emitter.emit(EventType::NodeRegistered, node("guest-1"));
            }
        })
        .await;

        assert!(queued.is_ok(), "emit must not wait on the receiver");
    }

    #[tokio::test]
    async fn no_configured_events_means_every_event() {
        assert_eq!(parse_events(&[]), Ok(Vec::new()));
        assert_eq!(
            parse_events(&[String::new()]),
            Ok(Vec::new()),
            "an env var set to the empty string reads as unset"
        );
        let emitter = Emitter::new(config("http://127.0.0.1:1/hook")).unwrap();
        assert_eq!(emitter.allowed, EventType::ALL.to_vec());
    }

    #[test]
    fn configured_event_names_are_trimmed() {
        assert_eq!(
            parse_events(&[" node.offline".to_string(), "stream.changed ".to_string()]),
            Ok(vec![EventType::NodeOffline, EventType::StreamChanged])
        );
    }

    #[test]
    fn an_unknown_event_name_is_an_error_that_lists_the_known_ones() {
        let error =
            parse_events(&["node.offline".to_string(), "node.exploded".to_string()]).unwrap_err();
        assert_eq!(error, UnknownEventType("node.exploded".to_string()));
        assert_eq!(
            error.to_string(),
            "unknown webhook event type `node.exploded`; known types are node.registered, node.online, node.offline, node.forgotten, stream.changed"
        );
    }

    #[test]
    fn a_timestamp_is_rfc_3339_in_utc() {
        let now = now_rfc3339();
        assert!(now.ends_with('Z'), "{now} is not UTC in RFC 3339 form");
        assert!(
            time::OffsetDateTime::parse(&now, &time::format_description::well_known::Rfc3339)
                .is_ok()
        );
    }
}
