use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::header::{
    CONNECTION, CONTENT_LENGTH, CONTENT_TYPE, HOST, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION, TE,
    TRAILER, TRANSFER_ENCODING, UPGRADE,
};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use http_body_util::LengthLimitError;
use serde::de::DeserializeOwned;
use serde_json::json;
use tracing::warn;

use crate::app::AppState;
use crate::config;
use crate::rln::RlnErrorBody;
use crate::store::NodeState;

const KEEP_ALIVE: HeaderName = HeaderName::from_static("keep-alive");
const HOP_BY_HOP: [HeaderName; 8] = [
    CONNECTION,
    KEEP_ALIVE,
    PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION,
    TE,
    TRAILER,
    TRANSFER_ENCODING,
    UPGRADE,
];

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("upstream unreachable")]
    Unreachable(reqwest::Error),
    #[error("upstream timeout")]
    Timeout,
    #[error("request body too large")]
    PayloadTooLarge,
    #[error("request body: {0}")]
    Body(String),
    #[error("invalid request path")]
    Path,
}

impl IntoResponse for ProxyError {
    fn into_response(self) -> Response {
        let (status, name) = match &self {
            ProxyError::Unreachable(_) => (StatusCode::BAD_GATEWAY, "UpstreamUnreachable"),
            ProxyError::Timeout => (StatusCode::GATEWAY_TIMEOUT, "UpstreamTimeout"),
            ProxyError::PayloadTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "PayloadTooLarge"),
            ProxyError::Body(_) | ProxyError::Path => (StatusCode::BAD_REQUEST, "InvalidRequest"),
        };
        error_body(status, name, &self.to_string())
    }
}

pub fn error_body(status: StatusCode, name: &str, message: &str) -> Response {
    let body = json!({"error": message, "code": status.as_u16(), "name": name});
    (
        status,
        [(CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

pub struct ProxyResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl ProxyResponse {
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(&self.body)
    }

    pub fn into_response(self) -> Response {
        (self.status, self.headers, self.body).into_response()
    }
}

pub struct Proxy {
    client: reqwest::Client,
    base_url: String,
    max_body: usize,
}

impl Proxy {
    pub fn new(cfg: &config::Rln) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(cfg.proxy_timeout_secs))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let max_body = usize::try_from(cfg.proxy_max_body_mb.saturating_mul(1024 * 1024))
            .unwrap_or(usize::MAX);
        Ok(Self {
            client,
            base_url: cfg.base_url.trim_end_matches('/').to_string(),
            max_body,
        })
    }

    pub async fn forward(&self, req: Request) -> Result<ProxyResponse, ProxyError> {
        let (parts, body) = req.into_parts();
        let body = self.read_body(body).await?;
        self.forward_parts(parts, body).await
    }

    pub async fn read_body(&self, body: Body) -> Result<Bytes, ProxyError> {
        axum::body::to_bytes(body, self.max_body)
            .await
            .map_err(body_error)
    }

    pub async fn forward_parts(
        &self,
        parts: Parts,
        body: Bytes,
    ) -> Result<ProxyResponse, ProxyError> {
        check_path(parts.uri.path())?;
        let path_and_query = parts
            .uri
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("/");
        let url = format!("{}{path_and_query}", self.base_url);
        let mut headers = strip_hop_by_hop(parts.headers);
        headers.remove(HOST);
        headers.remove(CONTENT_LENGTH);

        let resp = self
            .client
            .request(parts.method, url)
            .headers(headers)
            .body(body)
            .send()
            .await
            .map_err(classify)?;
        let status = resp.status();
        let mut headers = strip_hop_by_hop(resp.headers().clone());
        headers.remove(CONTENT_LENGTH);
        let body = resp.bytes().await.map_err(classify)?;
        Ok(ProxyResponse {
            status,
            headers,
            body,
        })
    }
}

/// axum routes literally but reqwest normalizes `.`/`..`/empty segments; `%` is refused
/// since encoded dots would bypass this check and no RLN route needs encoding.
fn check_path(path: &str) -> Result<(), ProxyError> {
    let ok = !path.contains('%')
        && path
            .strip_prefix('/')
            .unwrap_or(path)
            .split('/')
            .all(|s| !matches!(s, "" | "." | ".."));
    if ok {
        Ok(())
    } else {
        Err(ProxyError::Path)
    }
}

fn body_error(e: axum::Error) -> ProxyError {
    if e.into_inner().is::<LengthLimitError>() {
        ProxyError::PayloadTooLarge
    } else {
        ProxyError::Body("failed to read request body".to_string())
    }
}

fn classify(e: reqwest::Error) -> ProxyError {
    tracing::warn!(error = ?e, "proxy upstream error");
    if e.is_timeout() {
        ProxyError::Timeout
    } else {
        ProxyError::Unreachable(e)
    }
}

fn strip_hop_by_hop(mut headers: HeaderMap) -> HeaderMap {
    for name in HOP_BY_HOP {
        headers.remove(name);
    }
    headers
}

pub async fn note_locked(state: &AppState, resp: &ProxyResponse) {
    if resp.status != StatusCode::FORBIDDEN {
        return;
    }
    let Ok(err) = resp.json::<RlnErrorBody>() else {
        return;
    };
    let node_state = match err.name.as_str() {
        "LockedNode" | "ChangingState" => NodeState::Locked,
        "UnlockedNode" | "AlreadyUnlocked" => NodeState::Unlocked,
        _ => return,
    };
    if let Err(e) = state.engine.set_node_state(node_state).await {
        warn!(error = %e, "node state update failed");
    }
}

pub async fn fallback(State(state): State<Arc<AppState>>, req: Request) -> Response {
    match state.proxy.forward(req).await {
        Ok(resp) => {
            note_locked(&state, &resp).await;
            resp.into_response()
        }
        Err(e) => e.into_response(),
    }
}

pub async fn openapi(State(state): State<Arc<AppState>>) -> Response {
    match &state.openapi {
        Some(spec) => (
            StatusCode::OK,
            [(CONTENT_TYPE, "application/yaml")],
            Body::from(spec.clone()),
        )
            .into_response(),
        None => error_body(
            StatusCode::NOT_FOUND,
            "NotFound",
            "openapi spec not available",
        ),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use axum::Router;
    use serde_json::json;
    use tower::ServiceExt;
    use wiremock::matchers::{body_bytes, body_json, header, method, path, query_param};
    use wiremock::{Match, Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::app::test_state;
    use crate::config;
    use crate::rln::test_support::MockRln;

    struct NoHeaders(&'static [&'static str]);

    impl Match for NoHeaders {
        fn matches(&self, req: &wiremock::Request) -> bool {
            self.0.iter().all(|h| !req.headers.contains_key(*h))
        }
    }

    fn cfg(base_url: &str) -> config::Rln {
        config::Rln {
            base_url: base_url.to_string(),
            proxy_timeout_secs: 5,
            ..Default::default()
        }
    }

    async fn state(cfg: config::Rln, spec: Option<Bytes>) -> Arc<AppState> {
        let mut state = test_state(Proxy::new(&cfg).unwrap(), Arc::new(MockRln::default())).await;
        state.openapi = spec;
        Arc::new(state)
    }

    fn router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/companion/openapi.yaml", get(openapi))
            .fallback(fallback)
            .with_state(state)
    }

    async fn app(cfg: config::Rln, spec: Option<Bytes>) -> Router {
        router(state(cfg, spec).await)
    }

    async fn call(app: Router, req: Request<Body>) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
        let resp = app.oneshot(req).await.unwrap();
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, headers, body.to_vec())
    }

    fn closed_port_url() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        format!("http://127.0.0.1:{port}")
    }

    fn get_req(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn forwards_method_path_query_body_and_auth() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/listtransfers"))
            .and(query_param("x", "1"))
            .and(header("authorization", "Bearer t"))
            .and(header("content-type", "application/json"))
            .and(body_json(json!({"asset_id": "a"})))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(b"{\"transfers\":[]}", "application/json"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let req = Request::builder()
            .method("POST")
            .uri("/listtransfers?x=1")
            .header("authorization", "Bearer t")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"asset_id":"a"}"#))
            .unwrap();
        let (status, headers, body) =
            call(app(cfg(&format!("{}/", server.uri())), None).await, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"{\"transfers\":[]}");
        assert_eq!(headers["content-type"], "application/json");
    }

    #[tokio::test]
    async fn proxied_locked_403_updates_node_state() {
        let server = MockServer::start().await;
        let err = json!({"error": "Node is locked", "code": 403, "name": "LockedNode"});
        Mock::given(method("GET"))
            .and(path("/nodeinfo"))
            .respond_with(ResponseTemplate::new(403).set_body_json(err.clone()))
            .mount(&server)
            .await;
        let state = state(cfg(&server.uri()), None).await;
        assert_eq!(state.store.node_state().await.unwrap(), NodeState::Unknown);

        let (status, _, body) = call(router(state.clone()), get_req("/nodeinfo")).await;

        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            err
        );
        assert_eq!(state.store.node_state().await.unwrap(), NodeState::Locked);
    }

    #[tokio::test]
    async fn proxied_unlocked_403_updates_node_state() {
        for name in ["UnlockedNode", "AlreadyUnlocked"] {
            let server = MockServer::start().await;
            let err = json!({"error": "Node is unlocked", "code": 403, "name": name});
            Mock::given(method("POST"))
                .and(path("/unlock"))
                .respond_with(ResponseTemplate::new(403).set_body_json(err))
                .mount(&server)
                .await;
            let state = state(cfg(&server.uri()), None).await;

            let req = Request::builder()
                .method("POST")
                .uri("/unlock")
                .body(Body::from("{}"))
                .unwrap();
            let (status, _, _) = call(router(state.clone()), req).await;

            assert_eq!(status, StatusCode::FORBIDDEN);
            assert_eq!(state.store.node_state().await.unwrap(), NodeState::Unlocked);
        }
    }

    #[tokio::test]
    async fn passes_3xx_through() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nodeinfo"))
            .respond_with(
                ResponseTemplate::new(302).insert_header("location", "http://example.invalid/x"),
            )
            .mount(&server)
            .await;

        let (status, headers, _) =
            call(app(cfg(&server.uri()), None).await, get_req("/nodeinfo")).await;
        assert_eq!(status, StatusCode::FOUND);
        assert_eq!(headers["location"], "http://example.invalid/x");
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn does_not_add_auth_when_client_sent_none() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nodeinfo"))
            .and(NoHeaders(&["authorization"]))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let (status, _, _) = call(app(cfg(&server.uri()), None).await, get_req("/nodeinfo")).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn strips_hop_by_hop_headers() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/nodeinfo"))
            .and(header("content-type", "application/json"))
            .and(NoHeaders(&["connection", "keep-alive", "te"]))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({}))
                    .insert_header("transfer-encoding", "chunked")
                    .insert_header("connection", "keep-alive")
                    .insert_header("x-rln-test", "1"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let req = Request::builder()
            .method("POST")
            .uri("/nodeinfo")
            .header("content-type", "application/json")
            .header("connection", "keep-alive")
            .header("keep-alive", "timeout=5")
            .header("te", "trailers")
            .body(Body::from("{}"))
            .unwrap();
        let (status, headers, _) = call(app(cfg(&server.uri()), None).await, req).await;
        assert_eq!(status, StatusCode::OK);
        assert!(!headers.contains_key("transfer-encoding"));
        assert!(!headers.contains_key("connection"));
        assert_eq!(headers["x-rln-test"], "1");
    }

    #[tokio::test]
    async fn binary_body_round_trips() {
        let server = MockServer::start().await;
        let up: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        let down: Vec<u8> = (0..=255u8).rev().cycle().take(777).collect();
        Mock::given(method("POST"))
            .and(path("/blob"))
            .and(header("content-type", "application/octet-stream"))
            .and(body_bytes(up.clone()))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(down.clone(), "application/octet-stream"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let req = Request::builder()
            .method("POST")
            .uri("/blob")
            .header("content-type", "application/octet-stream")
            .body(Body::from(up))
            .unwrap();
        let (status, headers, body) = call(app(cfg(&server.uri()), None).await, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["content-type"], "application/octet-stream");
        assert_eq!(body, down);
    }

    #[tokio::test]
    async fn oversized_body_is_413() {
        let server = MockServer::start().await;
        let mut c = cfg(&server.uri());
        c.proxy_max_body_mb = 1;

        let req = Request::builder()
            .method("POST")
            .uri("/blob")
            .body(Body::from(vec![0u8; 1024 * 1024 + 1]))
            .unwrap();
        let (status, _, body) = call(app(c, None).await, req).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], 413);
        assert_eq!(body["name"], "PayloadTooLarge");
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn upstream_unreachable_is_502() {
        let (status, headers, body) = call(
            app(cfg(&closed_port_url()), None).await,
            get_req("/nodeinfo"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(headers["content-type"], "application/json");
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], 502);
        assert_eq!(body["name"], "UpstreamUnreachable");
        let error = body["error"].as_str().unwrap();
        assert!(!error.is_empty());
        assert!(!error.contains("http"), "{error}");
    }

    #[tokio::test]
    async fn upstream_timeout_is_504() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nodeinfo"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(2)))
            .mount(&server)
            .await;

        let mut c = cfg(&server.uri());
        c.proxy_timeout_secs = 1;
        let (status, _, body) = call(app(c, None).await, get_req("/nodeinfo")).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], 504);
        assert_eq!(body["name"], "UpstreamTimeout");
        assert!(!body["error"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dot_segments_are_rejected() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        for uri in [
            "/./nodeinfo",
            "/companion/../listtransfers",
            "//nodeinfo",
            "/nodeinfo/..",
            "/nodeinfo/.",
            "/a//b",
            "/%2e/rgbinvoice",
            "/%2E%2E/nodeinfo",
            "/rgbinvoice%2f..",
        ] {
            let (status, _, body) = call(app(cfg(&server.uri()), None).await, get_req(uri)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
            let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["name"], "InvalidRequest", "{uri}");
        }
        assert!(server.received_requests().await.unwrap().is_empty());

        let (status, _, _) = call(
            app(cfg(&server.uri()), None).await,
            get_req("/node.info/x.y"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn serves_openapi() {
        let server = MockServer::start().await;
        let spec = Bytes::from_static(b"openapi: 3.0.0\n");

        let (status, headers, body) = call(
            app(cfg(&server.uri()), Some(spec.clone())).await,
            get_req("/companion/openapi.yaml"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers["content-type"], "application/yaml");
        assert_eq!(body, spec);

        let (status, _, body) = call(
            app(cfg(&server.uri()), None).await,
            get_req("/companion/openapi.yaml"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], 404);
        assert_eq!(body["name"], "NotFound");
        assert!(!body["error"].as_str().unwrap().is_empty());

        assert!(server.received_requests().await.unwrap().is_empty());
    }
}
