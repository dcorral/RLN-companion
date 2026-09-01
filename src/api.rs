use std::sync::Arc;

use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tracing::error;

use crate::app::AppState;
use crate::proxy::{self, error_body};
use crate::store::{NodeState, StoreError, Transfer, TransferStatus};

const DEFAULT_LIMIT: u32 = 100;
const MAX_LIMIT: u32 = 1000;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    InvalidRequest(String),
    #[error("missing or invalid bearer token")]
    Unauthorized,
    #[error("transfer not found")]
    NotFound,
    #[error(transparent)]
    Store(#[from] StoreError),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, name) = match &self {
            ApiError::InvalidRequest(_) => (StatusCode::BAD_REQUEST, "InvalidRequest"),
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "Unauthorized"),
            ApiError::NotFound => (StatusCode::NOT_FOUND, "NotFound"),
            ApiError::Store(e) => {
                error!(error = %e, "api store error");
                return error_body(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal",
                    "internal error",
                );
            }
        };
        error_body(status, name, &self.to_string())
    }
}

pub fn routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    let protected = Router::new()
        .route("/companion/transfers", get(list_transfers))
        .route("/companion/transfers/:id", get(get_transfer))
        .route("/companion/openapi.yaml", get(proxy::openapi))
        .route_layer(middleware::from_fn_with_state(state, require_token));
    Router::new()
        .route("/companion/health", get(health))
        .route("/companion/*rest", any(|| async { ApiError::NotFound }))
        .merge(protected)
}

async fn require_token(State(state): State<Arc<AppState>>, req: Request, next: Next) -> Response {
    let Some(expected) = &state.auth_token else {
        return next.run(req).await;
    };
    let presented = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(bearer_token);
    match presented {
        Some(t) if bool::from(t.as_bytes().ct_eq(expected.as_bytes())) => next.run(req).await,
        _ => ApiError::Unauthorized.into_response(),
    }
}

fn bearer_token(header: &str) -> Option<&str> {
    let (scheme, token) = header.trim().split_once(char::is_whitespace)?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim())
        .filter(|t| !t.is_empty())
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    node: &'static str,
    pending_transfers: usize,
    parked_events: u64,
    last_full_sync_at: Option<i64>,
}

fn node_name(s: NodeState) -> &'static str {
    match s {
        NodeState::Unknown => "unknown",
        NodeState::Locked => "locked",
        NodeState::Unlocked => "unlocked",
        NodeState::Down => "down",
        NodeState::Misconfigured => "misconfigured",
    }
}

async fn health(State(state): State<Arc<AppState>>) -> Result<Json<Health>, ApiError> {
    let node = state.store.node_state().await?;
    let pending_transfers = state.store.pending_transfers().await?.len();
    let parked_events = state
        .store
        .parked_events_count(state.webhook_max_attempts)
        .await?;
    let last_full_sync_at = state.store.last_full_sync_at().await?;
    let status = if node == NodeState::Unlocked && parked_events == 0 {
        "ok"
    } else {
        "degraded"
    };
    Ok(Json(Health {
        status,
        node: node_name(node),
        pending_transfers,
        parked_events,
        last_full_sync_at,
    }))
}

#[derive(Deserialize)]
struct ListQuery {
    status: Option<String>,
    asset_id: Option<String>,
    limit: Option<u32>,
}

#[derive(Serialize)]
struct TransferList {
    transfers: Vec<Transfer>,
}

async fn list_transfers(
    State(state): State<Arc<AppState>>,
    query: Result<Query<ListQuery>, QueryRejection>,
) -> Result<Json<TransferList>, ApiError> {
    let Query(q) = query.map_err(|e| ApiError::InvalidRequest(e.body_text()))?;
    let status = q
        .status
        .map(|s| {
            serde_json::from_value::<TransferStatus>(serde_json::Value::String(s.clone()))
                .map_err(|_| ApiError::InvalidRequest(format!("invalid status: {s}")))
        })
        .transpose()?;
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let transfers = state
        .store
        .list_transfers(status, q.asset_id.as_deref(), limit)
        .await?;
    Ok(Json(TransferList { transfers }))
}

async fn get_transfer(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Transfer>, ApiError> {
    let transfer = state.store.get_transfer(&id).await?;
    transfer.map(Json).ok_or(ApiError::NotFound)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use bytes::Bytes;
    use serde_json::{json, Value};
    use tower::ServiceExt;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::app::{test_state_at, AppState};
    use crate::intercept::interceptor_routes;
    use crate::proxy;
    use crate::store::{NewTransfer, NodeState, TransferStatus};
    use TransferStatus::*;

    async fn state(base_url: &str) -> AppState {
        test_state_at(base_url).await.1
    }

    fn app(state: Arc<AppState>) -> Router {
        routes(state.clone())
            .merge(interceptor_routes())
            .fallback(proxy::fallback)
            .with_state(state)
    }

    fn get(uri: &str, bearer: Option<&str>) -> Request<Body> {
        let mut b = Request::builder().uri(uri);
        if let Some(t) = bearer {
            b = b.header("authorization", format!("Bearer {t}"));
        }
        b.body(Body::empty()).unwrap()
    }

    fn post_listtransfers() -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/listtransfers")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"asset_id":"A"}"#))
            .unwrap()
    }

    async fn call(app: Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
        let resp = app.oneshot(req).await.unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, body.to_vec())
    }

    async fn call_json(app: Router, req: Request<Body>) -> (StatusCode, Value) {
        let (status, body) = call(app, req).await;
        (status, serde_json::from_slice(&body).unwrap())
    }

    async fn insert(state: &AppState, status: TransferStatus, asset: &str, at: i64) -> String {
        state
            .store
            .insert_transfer(
                &NewTransfer {
                    asset_id: Some(asset.into()),
                    ..NewTransfer::with_status(status)
                },
                at,
            )
            .await
            .unwrap()
            .id
    }

    async fn park_event(state: &AppState) -> String {
        let id = insert(state, Initiated, "A", 1).await;
        state
            .store
            .apply_transition(
                &id,
                Initiated,
                Failed,
                Some(("ev1", "transfer.failed", "{}")),
                1,
            )
            .await
            .unwrap();
        state.store.record_attempt("ev1", 1).await.unwrap();
        "ev1".into()
    }

    fn assert_error(body: &Value, code: u16, name: &str) {
        assert_eq!(body["code"], code);
        assert_eq!(body["name"], name);
        assert!(!body["error"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn health_reports_node_state_and_counts() {
        let server = MockServer::start().await;
        let mut state = state(&server.uri()).await;
        state.webhook_max_attempts = 1;
        let state = Arc::new(state);
        state
            .store
            .set_node_state(NodeState::Unlocked, 1)
            .await
            .unwrap();
        insert(&state, WaitingCounterparty, "A", 1).await;
        insert(&state, WaitingConfirmations, "A", 2).await;
        insert(&state, Settled, "A", 3).await;
        let ev = park_event(&state).await;

        let (status, body) = call_json(app(state.clone()), get("/companion/health", None)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({
                "status": "degraded",
                "node": "unlocked",
                "pending_transfers": 2,
                "parked_events": 1,
                "last_full_sync_at": null
            })
        );

        state.store.mark_delivered(&ev, 5).await.unwrap();
        state.store.set_last_full_sync_at(42).await.unwrap();
        let (status, body) = call_json(app(state.clone()), get("/companion/health", None)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
        assert_eq!(body["node"], "unlocked");
        assert_eq!(body["parked_events"], 0);
        assert_eq!(body["last_full_sync_at"], 42);

        for (node, name) in [
            (NodeState::Locked, "locked"),
            (NodeState::Unknown, "unknown"),
            (NodeState::Down, "down"),
            (NodeState::Misconfigured, "misconfigured"),
        ] {
            state.store.set_node_state(node, 1).await.unwrap();
            let (status, body) =
                call_json(app(state.clone()), get("/companion/health", None)).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["status"], "degraded", "{name}");
            assert_eq!(body["node"], name);
        }
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    fn ids(body: &Value) -> Vec<&str> {
        body["transfers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["id"].as_str().unwrap())
            .collect()
    }

    #[tokio::test]
    async fn list_transfers_newest_first_with_filters() {
        let server = MockServer::start().await;
        let state = Arc::new(state(&server.uri()).await);
        let a1 = insert(&state, WaitingCounterparty, "A", 1).await;
        let b2 = insert(&state, Settled, "B", 2).await;
        let a3 = insert(&state, Settled, "A", 3).await;

        let (status, body) = call_json(app(state.clone()), get("/companion/transfers", None)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ids(&body), vec![a3.as_str(), b2.as_str(), a1.as_str()]);
        assert_eq!(body["transfers"][0]["status"], "Settled");
        assert_eq!(body["transfers"][0]["asset_id"], "A");
        assert_eq!(body["transfers"][0]["created_at"], 3);

        let (status, body) = call_json(
            app(state.clone()),
            get("/companion/transfers?status=Settled", None),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ids(&body), vec![a3.as_str(), b2.as_str()]);

        let (status, body) = call_json(
            app(state.clone()),
            get("/companion/transfers?asset_id=A", None),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ids(&body), vec![a3.as_str(), a1.as_str()]);

        let (status, body) = call_json(
            app(state.clone()),
            get("/companion/transfers?limit=1", None),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ids(&body), vec![a3.as_str()]);

        let (status, body) = call_json(
            app(state.clone()),
            get("/companion/transfers?status=Settled&asset_id=A", None),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ids(&body), vec![a3.as_str()]);

        let over_cap = format!("/companion/transfers?limit={}", MAX_LIMIT + 4000);
        let (status, body) = call_json(app(state.clone()), get(&over_cap, None)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ids(&body).len(), 3);

        let (status, body) = call_json(
            app(state.clone()),
            get("/companion/transfers?limit=0", None),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(ids(&body).is_empty());

        let (status, body) = call_json(
            app(state.clone()),
            get("/companion/transfers?status=Bogus", None),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_error(&body, 400, "InvalidRequest");

        let (status, body) = call_json(
            app(state.clone()),
            get("/companion/transfers?limit=abc", None),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_error(&body, 400, "InvalidRequest");
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_transfer_by_id_and_404() {
        let server = MockServer::start().await;
        let state = Arc::new(state(&server.uri()).await);
        let id = insert(&state, WaitingCounterparty, "A", 1).await;
        let row = state.store.get_transfer(&id).await.unwrap().unwrap();

        let (status, body) = call_json(
            app(state.clone()),
            get(&format!("/companion/transfers/{id}"), None),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::to_value(&row).unwrap());

        let (status, body) = call_json(
            app(state.clone()),
            get("/companion/transfers/missing", None),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_error(&body, 404, "NotFound");
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn auth_token_enforced_except_health() {
        let server = MockServer::start().await;
        let mut state = state(&server.uri()).await;
        state.auth_token = Some("tok".into());
        state.openapi = Some(Bytes::from_static(b"openapi: 3.0.0\n"));
        let state = Arc::new(state);

        for uri in ["/companion/transfers", "/companion/openapi.yaml"] {
            let (status, body) = call_json(app(state.clone()), get(uri, None)).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}");
            assert_error(&body, 401, "Unauthorized");

            let (status, body) = call_json(app(state.clone()), get(uri, Some("nope"))).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}");
            assert_error(&body, 401, "Unauthorized");

            let (status, _) = call(app(state.clone()), get(uri, Some("tok"))).await;
            assert_eq!(status, StatusCode::OK, "{uri}");

            for raw in ["bearer tok", "Bearer  tok ", "BEARER tok"] {
                let req = Request::builder()
                    .uri(uri)
                    .header("authorization", raw)
                    .body(Body::empty())
                    .unwrap();
                let (status, _) = call(app(state.clone()), req).await;
                assert_eq!(status, StatusCode::OK, "{uri} {raw:?}");
            }
            let (status, _) = call(app(state.clone()), get(uri, Some("tok x"))).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}");
        }
        let (status, body) = call_json(app(state.clone()), get("/companion/health", None)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["node"], "unknown");

        let mut open = self::state(&server.uri()).await;
        open.openapi = Some(Bytes::from_static(b"openapi: 3.0.0\n"));
        let open = Arc::new(open);
        for uri in [
            "/companion/transfers",
            "/companion/openapi.yaml",
            "/companion/health",
        ] {
            let (status, _) = call(app(open.clone()), get(uri, None)).await;
            assert_eq!(status, StatusCode::OK, "{uri}");
        }
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn auth_token_does_not_apply_to_proxied_routes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/listtransfers"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(b"{\"transfers\":[]}", "application/json"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let mut state = state(&server.uri()).await;
        state.auth_token = Some("tok".into());
        let state = Arc::new(state);

        let (status, body) = call(app(state), post_listtransfers()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"{\"transfers\":[]}");
    }

    #[tokio::test]
    async fn unknown_companion_path_is_404_not_proxied() {
        let server = MockServer::start().await;
        let state = Arc::new(state(&server.uri()).await);

        for uri in ["/companion/nope", "/companion/transfers/a/b"] {
            let (status, body) = call_json(app(state.clone()), get(uri, None)).await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
            assert_error(&body, 404, "NotFound");
        }
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn store_error_is_not_echoed() {
        let resp = ApiError::Store(StoreError::Corrupt("secret".into())).into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["name"], "Internal");
        assert_eq!(body["error"], "internal error");
    }

    #[tokio::test]
    async fn native_routes_do_not_shadow_rln() {
        let server = MockServer::start().await;
        let upstream: &[u8] = br#"{"transfers":[{"idx":1,"status":"Settled"}]}"#;
        Mock::given(method("POST"))
            .and(path("/listtransfers"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(upstream, "application/json"))
            .expect(1)
            .mount(&server)
            .await;
        let state = Arc::new(state(&server.uri()).await);
        insert(&state, Settled, "A", 1).await;

        let (status, body) = call(app(state), post_listtransfers()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, upstream);
    }
}
