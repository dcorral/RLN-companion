use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::Request;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::Router;
use bytes::Bytes;
use tokio::task::JoinHandle;
use tracing::info;

use crate::config::{Config, ConfigError};
use crate::engine::{Engine, EngineError};
use crate::proxy::{self, Proxy};
use crate::rln::{HttpRlnClient, RlnApi, RlnError};
use crate::store::{Store, StoreError};
use crate::webhook::Dispatcher;
use crate::{api, intercept};

pub struct AppState {
    pub proxy: Proxy,
    pub store: Store,
    pub engine: Arc<Engine<dyn RlnApi>>,
    pub openapi: Option<Bytes>,
    pub auth_token: Option<String>,
    pub webhook_max_attempts: u32,
}

pub struct App {
    pub state: Arc<AppState>,
    pub engine: Arc<Engine<dyn RlnApi>>,
    pub dispatcher: Arc<Dispatcher>,
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Rln(#[from] RlnError),
    #[error(transparent)]
    Engine(#[from] EngineError),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error("read {0}: {1}")]
    Openapi(PathBuf, std::io::Error),
    #[error("bind port {0}: {1}")]
    Bind(u16, std::io::Error),
    #[error("serve: {0}")]
    Serve(std::io::Error),
}

pub async fn build_app(cfg: &Config, openapi: Option<Bytes>) -> Result<App, AppError> {
    let store = Store::open(&cfg.database.path).await?;
    let rln: Arc<dyn RlnApi> = Arc::new(HttpRlnClient::new(&cfg.rln)?);
    let engine = Arc::new(Engine::new(
        rln,
        store.clone(),
        cfg.engine.clone(),
        cfg.sync.clone(),
        cfg.rln.network.clone(),
    ));
    let proxy = Proxy::new(&cfg.rln)?;
    let dispatcher = Arc::new(Dispatcher::new(store.clone(), cfg.webhook.clone())?);
    let state = Arc::new(AppState {
        proxy,
        store,
        engine: engine.clone(),
        openapi,
        auth_token: cfg.service.auth_token.clone(),
        webhook_max_attempts: cfg.webhook.max_attempts,
    });
    Ok(App {
        state,
        engine,
        dispatcher,
    })
}

pub fn build_router(state: Arc<AppState>) -> Router {
    api::routes(state.clone())
        .merge(intercept::interceptor_routes())
        .fallback(proxy::fallback)
        .layer(middleware::from_fn(log_request))
        .with_state(state)
}

async fn log_request(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let started = Instant::now();
    let resp = next.run(req).await;
    info!(
        %method,
        %path,
        status = resp.status().as_u16(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "request"
    );
    resp
}

pub fn spawn_background(app: &App) -> Vec<JoinHandle<()>> {
    vec![
        tokio::spawn(app.engine.clone().run_refresh_loop()),
        tokio::spawn(app.engine.clone().run_reaper()),
        tokio::spawn(app.engine.clone().run_full_sync_timer()),
        tokio::spawn(app.dispatcher.clone().run()),
    ]
}

#[cfg(test)]
pub fn test_rln_cfg(base_url: &str) -> crate::config::Rln {
    crate::config::Rln {
        base_url: base_url.to_string(),
        proxy_timeout_secs: 5,
        ..Default::default()
    }
}

#[cfg(test)]
pub async fn test_state_at(base_url: &str) -> (Arc<crate::rln::test_support::MockRln>, AppState) {
    #![allow(clippy::unwrap_used)]
    let rln = Arc::new(crate::rln::test_support::MockRln::default());
    let state = test_state(Proxy::new(&test_rln_cfg(base_url)).unwrap(), rln.clone()).await;
    (rln, state)
}

#[cfg(test)]
pub async fn test_state(proxy: Proxy, rln: Arc<crate::rln::test_support::MockRln>) -> AppState {
    #![allow(clippy::unwrap_used)]
    use crate::config;

    let store = Store::open_in_memory().await.unwrap();
    let rln: Arc<dyn RlnApi> = rln;
    let engine = Arc::new(Engine::new(
        rln,
        store.clone(),
        config::Engine::default(),
        config::Sync::default(),
        None,
    ));
    AppState {
        proxy,
        store,
        engine,
        openapi: None,
        auth_token: None,
        webhook_max_attempts: config::Webhook::default().max_attempts,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use axum::body::Body;
    use axum::http::StatusCode;
    use serde_json::{json, Value};
    use tower::ServiceExt;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::store::NodeState;

    async fn call(router: Router, req: Request<Body>) -> (StatusCode, Value) {
        let resp = router.oneshot(req).await.unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    #[tokio::test]
    async fn build_router_serves_all_layers() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/rgbinvoice"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "recipient_id": "r1",
                "invoice": "rgb:inv",
                "expiration_timestamp": 1700000000,
                "batch_transfer_idx": 7
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/nodeinfo"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"pubkey": "p"})))
            .expect(1)
            .mount(&server)
            .await;
        let state = Arc::new(test_state_at(&server.uri()).await.1);
        let router = build_router(state.clone());

        let req = Request::builder()
            .uri("/companion/health")
            .body(Body::empty())
            .unwrap();
        let (status, body) = call(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["node"], "unknown");

        let req = Request::builder()
            .method("POST")
            .uri("/rgbinvoice")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({"asset_id": "A", "min_confirmations": 1, "witness": false}).to_string(),
            ))
            .unwrap();
        let (status, body) = call(router.clone(), req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["recipient_id"], "r1");
        let rows = state.store.list_transfers(None, None, 10).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].recipient_id.as_deref(), Some("r1"));

        let req = Request::builder()
            .uri("/nodeinfo")
            .body(Body::empty())
            .unwrap();
        let (status, body) = call(router, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["pubkey"], "p");
    }

    #[tokio::test]
    async fn build_app_opens_file_store() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.database.path = dir
            .path()
            .join("companion.sqlite")
            .to_string_lossy()
            .into_owned();
        cfg.webhook.url = "http://127.0.0.1:9000/hook".into();
        cfg.webhook.secret = "s".into();
        cfg.validate().unwrap();

        let app = build_app(&cfg, None).await.unwrap();

        assert_eq!(
            app.state.store.node_state().await.unwrap(),
            NodeState::Unknown
        );
        assert!(dir.path().join("companion.sqlite").exists());
        let handles = spawn_background(&app);
        assert_eq!(handles.len(), 4);
        tokio::task::yield_now().await;
        for h in &handles {
            assert!(!h.is_finished());
            h.abort();
        }
    }
}
