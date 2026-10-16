use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Bytes;

use crate::logging::ErrorChain;
use crate::{ApiError, ApiErrorCode};

pub const CONTROLLER_URL_VAR: &str = "WEAVE_CONTROLLER_URL";
pub const DEFAULT_CONTROLLER_URL: &str = "http://127.0.0.1:8082";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
pub enum ControllersError {
    #[error("{CONTROLLER_URL_VAR} names no controller")]
    Empty,
    #[error("{CONTROLLER_URL_VAR} entry {url:?} is not an http or https URL with a host")]
    InvalidUrl { url: String },
    #[error("building the controller HTTP client")]
    Client(#[source] reqwest::Error),
}

#[derive(Debug, thiserror::Error)]
#[error("no answer from {url}")]
pub struct Unanswered {
    pub url: String,
    #[source]
    pub source: reqwest::Error,
}

impl Unanswered {
    pub fn response(&self) -> axum::response::Response {
        tracing::warn!(error = %ErrorChain(&self.source), url = %self.url, "proxying to controller failed");
        if self.source.is_timeout() && !self.source.is_connect() {
            ApiError::new(
                ApiErrorCode::ControllerTimeout,
                "the controller did not answer in time; it may have applied the request",
            )
            .response(axum::http::StatusCode::GATEWAY_TIMEOUT)
        } else {
            ApiError::new(
                ApiErrorCode::ControllerUnreachable,
                "controller unreachable",
            )
            .response(axum::http::StatusCode::BAD_GATEWAY)
        }
    }
}

#[derive(Debug)]
pub struct Answer {
    pub status: reqwest::StatusCode,
    pub headers: reqwest::header::HeaderMap,
    pub body: Bytes,
}

impl Answer {
    async fn read(response: reqwest::Response) -> Result<Self, reqwest::Error> {
        let status = response.status();
        let headers = response.headers().clone();
        Ok(Self {
            status,
            headers,
            body: response.bytes().await?,
        })
    }

    fn is_not_leader(&self) -> bool {
        self.status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            && serde_json::from_slice::<ApiError>(&self.body)
                .is_ok_and(|error| error.code == ApiErrorCode::NotLeader)
    }
}

pub struct Controllers {
    http: reqwest::Client,
    urls: Vec<String>,
    leader: AtomicUsize,
}

impl Controllers {
    pub fn new(list: &str) -> Result<Self, ControllersError> {
        Self::with_request_timeout(list, REQUEST_TIMEOUT)
    }

    pub fn with_request_timeout(list: &str, timeout: Duration) -> Result<Self, ControllersError> {
        let urls: Vec<String> = list
            .split(',')
            .map(|url| url.trim().trim_end_matches('/'))
            .filter(|url| !url.is_empty())
            .map(str::to_string)
            .collect();
        if urls.is_empty() {
            return Err(ControllersError::Empty);
        }
        if let Some(url) = urls.iter().find(|url| {
            reqwest::Url::parse(url).map_or(true, |parsed| {
                !matches!(parsed.scheme(), "http" | "https") || parsed.host().is_none()
            })
        }) {
            return Err(ControllersError::InvalidUrl { url: url.clone() });
        }
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(timeout)
            .build()
            .map_err(ControllersError::Client)?;
        Ok(Self {
            http,
            urls,
            leader: AtomicUsize::new(0),
        })
    }

    pub fn urls(&self) -> &[String] {
        &self.urls
    }

    pub async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        build: impl Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
    ) -> Result<Answer, Unanswered> {
        let first = self.leader.load(Ordering::Relaxed);
        let mut standby = None;
        let mut unanswered = None;
        for offset in 0..self.urls.len() {
            let index = (first + offset) % self.urls.len();
            let url = format!("{}{path}", self.urls[index]);
            let answer = match build(self.http.request(method.clone(), &url)).send().await {
                Ok(response) => Answer::read(response).await,
                Err(source) => Err(source),
            };
            let answer = match answer {
                Ok(answer) => answer,
                Err(source) if source.is_connect() || method == reqwest::Method::GET => {
                    tracing::debug!(error = %ErrorChain(&source), %url, "controller did not answer; trying the next");
                    unanswered = Some(Unanswered { url, source });
                    continue;
                }
                Err(source) => {
                    self.leader
                        .store((index + 1) % self.urls.len(), Ordering::Relaxed);
                    return Err(Unanswered { url, source });
                }
            };
            if answer.is_not_leader() {
                tracing::debug!(%url, "controller does not lead; trying the next");
                standby = Some(answer);
                continue;
            }
            self.leader.store(index, Ordering::Relaxed);
            return Ok(answer);
        }
        match (standby, unanswered) {
            (Some(answer), _) => Ok(answer),
            (None, Some(unanswered)) => Err(unanswered),
            (None, None) => unreachable!("a controller list is never empty"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::Router;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    use super::*;

    struct Stub {
        url: String,
        hits: Arc<AtomicUsize>,
    }

    impl Stub {
        fn hits(&self) -> usize {
            self.hits.load(Ordering::SeqCst)
        }
    }

    async fn stub(status: StatusCode, code: Option<ApiErrorCode>) -> Stub {
        let hits = Arc::new(AtomicUsize::new(0));
        let app = Router::new().fallback({
            let hits = hits.clone();
            move || {
                hits.fetch_add(1, Ordering::SeqCst);
                async move {
                    match code {
                        Some(code) => ApiError::new(code, "stub").response(status),
                        None => (status, "{}").into_response(),
                    }
                }
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        Stub { url, hits }
    }

    async fn refusing() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        format!("http://{}", listener.local_addr().unwrap())
    }

    fn controllers(urls: &[&str]) -> Controllers {
        Controllers::new(&urls.join(",")).unwrap()
    }

    async fn post(controllers: &Controllers) -> Result<Answer, Unanswered> {
        controllers
            .send(reqwest::Method::POST, "/streams", |request| {
                request.body("{}")
            })
            .await
    }

    fn not_leader() -> Option<ApiErrorCode> {
        Some(ApiErrorCode::NotLeader)
    }

    async fn frozen() -> (tokio::net::TcpListener, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        (listener, url)
    }

    fn impatient(urls: &[&str]) -> Controllers {
        Controllers::with_request_timeout(&urls.join(","), Duration::from_millis(300)).unwrap()
    }

    #[tokio::test]
    async fn a_get_moves_past_a_controller_that_never_answers() {
        let (_listener, frozen) = frozen().await;
        let leader = stub(StatusCode::OK, None).await;
        let controllers = impatient(&[&frozen, &leader.url]);
        let get = || controllers.send(reqwest::Method::GET, "/status", |request| request);

        assert_eq!(get().await.unwrap().status, StatusCode::OK);
        let started = std::time::Instant::now();
        assert_eq!(get().await.unwrap().status, StatusCode::OK);
        assert!(
            started.elapsed() < Duration::from_millis(300),
            "it went to the leader first"
        );
        assert_eq!(leader.hits(), 2);
    }

    #[tokio::test]
    async fn a_write_that_times_out_is_not_sent_again() {
        let (_listener, frozen) = frozen().await;
        let leader = stub(StatusCode::ACCEPTED, None).await;
        let controllers = impatient(&[&frozen, &leader.url]);

        let unanswered = post(&controllers).await.unwrap_err();
        assert!(unanswered.source.is_timeout(), "{unanswered}");
        assert_eq!(unanswered.response().status(), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(leader.hits(), 0, "the write went to one controller only");
        assert_eq!(
            post(&controllers).await.unwrap().status,
            StatusCode::ACCEPTED
        );
        assert_eq!(leader.hits(), 1, "the next request went to the other first");
    }

    #[test]
    fn every_entry_must_be_an_http_url_with_a_host() {
        for list in [
            "controller-a:8082,http://b:8082",
            "http://a:8082,b:8082",
            "ftp://a:8082",
            "http://",
            "a b",
        ] {
            assert!(
                matches!(
                    Controllers::new(list),
                    Err(ControllersError::InvalidUrl { .. })
                ),
                "{list}"
            );
        }
        assert!(Controllers::new("https://a.example,http://10.0.0.1:8082").is_ok());
    }

    #[test]
    fn the_list_is_comma_separated_and_never_empty() {
        let controllers = Controllers::new(" http://a:8082/, ,http://b:8082 ").unwrap();
        assert_eq!(controllers.urls(), ["http://a:8082", "http://b:8082"]);
        assert!(matches!(
            Controllers::new(" , "),
            Err(ControllersError::Empty)
        ));
    }

    #[tokio::test]
    async fn a_request_moves_past_a_controller_it_cannot_connect_to() {
        let leader = stub(StatusCode::ACCEPTED, None).await;
        let controllers = controllers(&[&refusing().await, &leader.url]);

        assert_eq!(
            post(&controllers).await.unwrap().status,
            StatusCode::ACCEPTED
        );
        assert_eq!(leader.hits(), 1);
    }

    #[tokio::test]
    async fn a_request_moves_past_a_standby_and_goes_to_the_leader_first_after() {
        let standby = stub(StatusCode::SERVICE_UNAVAILABLE, not_leader()).await;
        let leader = stub(StatusCode::ACCEPTED, None).await;
        let controllers = controllers(&[&standby.url, &leader.url]);

        assert_eq!(
            post(&controllers).await.unwrap().status,
            StatusCode::ACCEPTED
        );
        assert_eq!(
            post(&controllers).await.unwrap().status,
            StatusCode::ACCEPTED
        );
        assert_eq!((standby.hits(), leader.hits()), (1, 2));
    }

    #[tokio::test]
    async fn any_other_answer_is_returned_without_trying_the_next() {
        for (status, code) in [
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Some(ApiErrorCode::PersistenceFailed),
            ),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Some(ApiErrorCode::StreamNotReady),
            ),
            (StatusCode::SERVICE_UNAVAILABLE, None),
            (
                StatusCode::PRECONDITION_FAILED,
                Some(ApiErrorCode::PreconditionFailed),
            ),
        ] {
            let first = stub(status, code).await;
            let second = stub(StatusCode::ACCEPTED, None).await;
            let controllers = controllers(&[&first.url, &second.url]);

            assert_eq!(post(&controllers).await.unwrap().status, status, "{code:?}");
            assert_eq!((first.hits(), second.hits()), (1, 0), "{status} {code:?}");
        }
    }

    #[tokio::test]
    async fn with_no_leader_the_standby_answer_is_returned() {
        let standby = stub(StatusCode::SERVICE_UNAVAILABLE, not_leader()).await;
        let controllers = controllers(&[&refusing().await, &standby.url]);

        let answer = post(&controllers).await.unwrap();
        assert_eq!(answer.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            serde_json::from_slice::<ApiError>(&answer.body)
                .unwrap()
                .code,
            ApiErrorCode::NotLeader
        );
    }

    #[tokio::test]
    async fn with_no_controller_reachable_the_last_one_tried_is_named() {
        let last = refusing().await;
        let controllers = controllers(&[&refusing().await, &last]);

        let unanswered = post(&controllers).await.unwrap_err();
        assert_eq!(unanswered.url, format!("{last}/streams"));
        assert!(unanswered.source.is_connect());
        assert_eq!(unanswered.response().status(), StatusCode::BAD_GATEWAY);
    }
}
