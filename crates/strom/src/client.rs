use std::time::Duration;

use reqwest::{Method, RequestBuilder, Response, StatusCode};
use serde::Deserialize;
use serde_json::Value;
use weave_core::auth::Token;
use weave_core::logging::{ErrorChain, excerpt};

use crate::flow::{FlowListResponse, StromFlow};
use crate::spec::{FlowSpec, MappingError};

pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
pub enum StromError {
    #[error("strom request failed")]
    Request(#[from] reqwest::Error),
    #[error("strom answered {status} for url ({url}){}", quoted(.body))]
    Status {
        status: StatusCode,
        url: String,

        body: String,
    },
    #[error(transparent)]
    Mapping(#[from] MappingError),
}

async fn send(request: RequestBuilder) -> Result<Response, StromError> {
    let response = request.send().await?;
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let url = response.url().to_string();
    let body = match response.text().await {
        Ok(body) => excerpt(&body),
        Err(error) => format!("<failed to read body: {}>", ErrorChain(&error)),
    };
    Err(StromError::Status { status, url, body })
}

fn quoted(body: &str) -> String {
    if body.is_empty() {
        String::new()
    } else {
        format!(": {body}")
    }
}

#[derive(Clone)]
pub struct StromClient {
    base_url: String,
    http: reqwest::Client,
    token: Option<Token>,
}

impl StromClient {
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self::with_timeout(base_url, REQUEST_TIMEOUT)
    }

    #[must_use]
    pub fn with_timeout(base_url: impl Into<String>, timeout: Duration) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(timeout)
            .build()
            .expect("building the HTTP client");
        Self::with_client(base_url, http)
    }

    #[must_use]
    pub fn with_client(base_url: impl Into<String>, http: reqwest::Client) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
            token: None,
        }
    }

    #[must_use]
    pub fn with_token(mut self, token: Option<Token>) -> Self {
        self.token = token;
        self
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        let request = self
            .http
            .request(method, format!("{}{path}", self.base_url));
        match &self.token {
            Some(token) => request.header(reqwest::header::AUTHORIZATION, token.header_value()),
            None => request,
        }
    }

    pub async fn list_flows(&self) -> Result<Vec<StromFlow>, StromError> {
        let response = send(self.request(Method::GET, "/api/flows")).await?;
        Ok(response.json::<FlowListResponse>().await?.flows)
    }

    pub async fn create_flow(&self, spec: &FlowSpec) -> Result<String, StromError> {
        let response = send(self.request(Method::POST, "/api/flows").json(spec)).await?;
        Ok(response.json::<CreateFlowResponse>().await?.flow.id)
    }

    pub async fn start_flow(&self, id: &str) -> Result<(), StromError> {
        send(self.request(Method::POST, &format!("/api/flows/{id}/start"))).await?;
        Ok(())
    }

    pub async fn stop_flow(&self, id: &str) -> Result<(), StromError> {
        send(self.request(Method::POST, &format!("/api/flows/{id}/stop"))).await?;
        Ok(())
    }

    pub async fn delete_flow(&self, id: &str) -> Result<(), StromError> {
        send(self.request(Method::DELETE, &format!("/api/flows/{id}"))).await?;
        Ok(())
    }

    pub async fn version(&self) -> Result<String, StromError> {
        let response = send(self.request(Method::GET, "/api/version")).await?;
        Ok(response.json::<VersionResponse>().await?.version)
    }

    pub async fn srt_stats(&self, id: &str) -> Result<Value, StromError> {
        self.flow_stats(id, "srt-stats").await
    }

    pub async fn webrtc_stats(&self, id: &str) -> Result<Value, StromError> {
        self.flow_stats(id, "webrtc-stats").await
    }

    async fn flow_stats(&self, id: &str, kind: &str) -> Result<Value, StromError> {
        let response = send(self.request(Method::GET, &format!("/api/flows/{id}/{kind}"))).await?;
        Ok(response.json::<Value>().await?)
    }
}

#[derive(Debug, Deserialize)]
struct VersionResponse {
    version: String,
}

#[derive(Debug, Deserialize)]
struct CreateFlowResponse {
    flow: CreatedFlow,
}

#[derive(Debug, Deserialize)]
struct CreatedFlow {
    id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    async fn stub_strom() -> (String, Arc<Mutex<Option<String>>>) {
        use axum::extract::{Request, State};
        use axum::response::{IntoResponse, Response};
        use axum::{Json, Router};

        let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

        async fn record(
            State(captured): State<Arc<Mutex<Option<String>>>>,
            request: Request,
        ) -> Response {
            *captured.lock().unwrap() = request
                .headers()
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .map(str::to_string);
            Json(serde_json::json!({ "flows": [] })).into_response()
        }

        let app = Router::new().fallback(record).with_state(captured.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{addr}"), captured)
    }

    #[tokio::test]
    async fn presents_the_bearer_token_when_one_is_set() {
        let (url, captured) = stub_strom().await;
        let client = StromClient::new(url).with_token(Token::new("strom-secret"));

        client.list_flows().await.expect("list flows");

        assert_eq!(
            captured.lock().unwrap().as_deref(),
            Some("Bearer strom-secret")
        );
    }

    #[tokio::test]
    async fn reads_the_version_field_of_api_version() {
        use axum::{Json, Router, routing::get};

        let app = Router::new().route(
            "/api/version",
            get(|| async {
                Json(serde_json::json!({ "version": "0.6.10", "git_hash": "abc", "in_docker": true }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let version = StromClient::new(url).version().await.expect("version");

        assert_eq!(version, "0.6.10");
    }

    #[tokio::test]
    async fn a_strom_that_never_answers_times_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let client = StromClient::with_timeout(url, Duration::from_millis(200));

        let error = client.list_flows().await.expect_err("no answer");

        assert!(matches!(error, StromError::Request(source) if source.is_timeout()));
        drop(listener);
    }

    #[tokio::test]
    async fn sends_no_authorization_header_without_a_token() {
        let (url, captured) = stub_strom().await;
        let client = StromClient::new(url);

        client.list_flows().await.expect("list flows");

        assert_eq!(captured.lock().unwrap().as_deref(), None);
    }

    async fn strom_answering(status: u16, body: String) -> StromClient {
        let status = axum::http::StatusCode::from_u16(status).unwrap();
        let app = axum::Router::new().fallback(move || async move { (status, body) });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        StromClient::new(url)
    }

    #[tokio::test]
    async fn a_refused_request_names_the_status_and_what_strom_answered() {
        let body = "{\n  \"error\": \"Flow not found\",\n  \"details\": \"no flow abc\"\n}\n";
        let client = strom_answering(404, body.to_string()).await;

        let error = client.start_flow("abc").await.expect_err("refused");

        let text = ErrorChain(&error).to_string();
        assert!(text.contains("404 Not Found"), "{text}");
        assert!(text.contains(r#""details": "no flow abc""#), "{text}");
        assert!(!text.contains('\n'), "{text}");
    }

    #[tokio::test]
    async fn a_long_answer_is_cut_short() {
        let body = "x".repeat(100_000);
        let client = strom_answering(500, body).await;

        let error = client.list_flows().await.expect_err("refused");

        let text = ErrorChain(&error).to_string();
        assert!(text.contains("500 Internal Server Error"), "{text}");
        assert!(text.contains(&"x".repeat(500)), "{text}");
        assert!(text.len() < 2_000, "{} bytes", text.len());
    }
}
