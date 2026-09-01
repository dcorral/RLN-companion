use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tracing::warn;

use crate::app::AppState;
use crate::engine::EngineError;
use crate::now;
use crate::proxy::note_locked;
use crate::store::{NewTransfer, NodeState, TransferStatus};

pub fn interceptor_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/rgbinvoice", post(rgbinvoice))
        .route("/sendrgb", post(sendrgb))
        .route("/issueassetnia", post(|State(s), r| issue(s, r, "NIA")))
        .route("/issueassetcfa", post(|State(s), r| issue(s, r, "CFA")))
        .route("/issueassetuda", post(|State(s), r| issue(s, r, "UDA")))
        .route("/issueassetifa", post(|State(s), r| issue(s, r, "IFA")))
        .route("/inflate", post(inflate))
        .route("/assetlink", post(assetlink))
        .route("/refreshtransfers", post(sync_pending))
        .route("/failtransfers", post(sync_pending))
        .route("/init", post(lock))
        .route("/unlock", post(unlock))
        .route("/restore", post(lock))
        .route("/lock", post(lock))
        .route("/shutdown", post(shutdown))
}

struct Exchange {
    req: Bytes,
    resp: Bytes,
}

impl Exchange {
    fn parse_req<T: DeserializeOwned>(&self) -> Result<T, HookError> {
        serde_json::from_slice(&self.req).map_err(HookError::Request)
    }

    fn parse_resp<T: DeserializeOwned>(&self) -> Result<T, HookError> {
        serde_json::from_slice(&self.resp).map_err(HookError::Response)
    }
}

#[derive(Debug, thiserror::Error)]
enum HookError {
    #[error("request body: {0}")]
    Request(serde_json::Error),
    #[error("response body: {0}")]
    Response(serde_json::Error),
    #[error(transparent)]
    Engine(#[from] EngineError),
}

impl From<crate::store::StoreError> for HookError {
    fn from(e: crate::store::StoreError) -> Self {
        Self::Engine(e.into())
    }
}

async fn forward_then<F, Fut>(state: &Arc<AppState>, req: Request, on_success: F) -> Response
where
    F: FnOnce(Arc<AppState>, Exchange) -> Fut,
    Fut: Future<Output = Result<(), HookError>>,
{
    let path = req.uri().path().to_string();
    let (parts, body) = req.into_parts();
    let body = match state.proxy.read_body(body).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let resp = match state.proxy.forward_parts(parts, body.clone()).await {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    if resp.status.is_success() {
        let ex = Exchange {
            req: body,
            resp: resp.body.clone(),
        };
        if let Err(e) = on_success(state.clone(), ex).await {
            warn!(path = %path, error = %e, "interceptor hook failed");
        }
    } else {
        note_locked(state, &resp).await;
    }
    resp.into_response()
}

#[derive(Deserialize)]
struct InvoiceRequest {
    asset_id: Option<String>,
    witness: bool,
}

#[derive(Deserialize)]
struct InvoiceResponse {
    recipient_id: String,
    invoice: String,
    expiration_timestamp: Option<u64>,
    batch_transfer_idx: i32,
}

async fn rgbinvoice(State(state): State<Arc<AppState>>, req: Request) -> Response {
    forward_then(&state, req, |state, ex| async move {
        let req: InvoiceRequest = ex.parse_req()?;
        let resp: InvoiceResponse = ex.parse_resp()?;
        let kind = if req.witness {
            "ReceiveWitness"
        } else {
            "ReceiveBlind"
        };
        let row = NewTransfer {
            asset_id: req.asset_id,
            kind: Some(kind.to_string()),
            status: TransferStatus::WaitingCounterparty,
            recipient_id: Some(resp.recipient_id),
            txid: None,
            batch_transfer_idx: Some(resp.batch_transfer_idx),
            invoice: Some(resp.invoice),
            expiration_timestamp: resp.expiration_timestamp,
        };
        state.store.insert_transfer(&row, now()).await?;
        state.engine.wake();
        Ok(())
    })
    .await
}

#[derive(Deserialize)]
struct SendRequest {
    donation: bool,
    expiration_timestamp: Option<u64>,
    recipient_map: HashMap<String, serde_json::Value>,
}

#[derive(Deserialize)]
struct SendResponse {
    txid: String,
}

async fn sendrgb(State(state): State<Arc<AppState>>, req: Request) -> Response {
    forward_then(&state, req, |state, ex| async move {
        let req: SendRequest = ex.parse_req()?;
        let resp: SendResponse = ex.parse_resp()?;
        let status = if req.donation {
            TransferStatus::WaitingConfirmations
        } else {
            TransferStatus::WaitingCounterparty
        };
        let assets: Vec<String> = req.recipient_map.into_keys().collect();
        for asset_id in &assets {
            let row = NewTransfer {
                asset_id: Some(asset_id.clone()),
                kind: Some("Send".to_string()),
                status,
                recipient_id: None,
                txid: Some(resp.txid.clone()),
                batch_transfer_idx: None,
                invoice: None,
                expiration_timestamp: req.expiration_timestamp,
            };
            state.store.insert_transfer(&row, now()).await?;
        }
        spawn_asset_sync(&state, assets);
        state.engine.wake();
        Ok(())
    })
    .await
}

fn spawn_asset_sync(state: &AppState, ids: Vec<String>) {
    let engine = state.engine.clone();
    tokio::spawn(async move {
        let _g = engine.lock().await;
        if let Err(e) = engine.sync_assets_locked(ids, now()).await {
            warn!(error = %e, "asset sync failed");
        }
    });
}

#[derive(Deserialize)]
struct IssueResponse {
    asset: AssetIdField,
}

#[derive(Deserialize)]
struct AssetIdField {
    asset_id: String,
}

async fn issue(state: Arc<AppState>, req: Request, schema: &'static str) -> Response {
    forward_then(&state, req, |state, ex| async move {
        let resp: IssueResponse = ex.parse_resp()?;
        let asset_id = resp.asset.asset_id;
        state
            .store
            .upsert_assets(&[(asset_id.clone(), schema.to_string())], now())
            .await?;
        spawn_asset_sync(&state, vec![asset_id]);
        Ok(())
    })
    .await
}

async fn inflate(State(state): State<Arc<AppState>>, req: Request) -> Response {
    forward_then(&state, req, |state, ex| async move {
        let req: AssetIdField = ex.parse_req()?;
        spawn_asset_sync(&state, vec![req.asset_id]);
        Ok(())
    })
    .await
}

#[derive(Deserialize)]
struct AssetLinkRequest {
    parent_asset_id: String,
    child_asset_id: String,
}

async fn assetlink(State(state): State<Arc<AppState>>, req: Request) -> Response {
    forward_then(&state, req, |state, ex| async move {
        let req: AssetLinkRequest = ex.parse_req()?;
        spawn_asset_sync(&state, vec![req.child_asset_id, req.parent_asset_id]);
        Ok(())
    })
    .await
}

async fn sync_pending(State(state): State<Arc<AppState>>, req: Request) -> Response {
    let _g = state.engine.lock().await;
    forward_then(&state, req, |state, _| async move {
        state.engine.sync_pending_locked(now()).await?;
        Ok(())
    })
    .await
}

async fn unlock(State(state): State<Arc<AppState>>, req: Request) -> Response {
    forward_then(&state, req, |state, _| async move {
        state.engine.set_node_state(NodeState::Unlocked).await?;
        let engine = state.engine.clone();
        tokio::spawn(async move {
            match engine.reconcile().await {
                Ok(true) => {}
                Ok(false) => warn!("reconcile after unlock gave up"),
                Err(e) => warn!(error = %e, "reconcile after unlock failed"),
            }
        });
        Ok(())
    })
    .await
}

/// Also serves /init and /restore: rln requires and leaves the node locked.
async fn lock(State(state): State<Arc<AppState>>, req: Request) -> Response {
    forward_then(&state, req, |state, _| async move {
        Ok(state.engine.set_node_state(NodeState::Locked).await?)
    })
    .await
}

async fn shutdown(State(state): State<Arc<AppState>>, req: Request) -> Response {
    forward_then(&state, req, |state, _| async move {
        Ok(state.engine.set_node_state(NodeState::Down).await?)
    })
    .await
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::Router;
    use serde_json::json;
    use tower::ServiceExt;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::app::test_state_at;
    use crate::proxy;
    use crate::rln::test_support::{wait_until, MockRln};
    use crate::store::{NewTransfer, NodeState, Transfer, TransferStatus};
    use TransferStatus::*;

    async fn harness(base_url: &str) -> (Arc<MockRln>, Arc<AppState>, Router) {
        let (rln, state) = test_state_at(base_url).await;
        let state = Arc::new(state);
        let app = interceptor_routes()
            .fallback(proxy::fallback)
            .with_state(state.clone());
        (rln, state, app)
    }

    async fn mount(server: &MockServer, route: &str, status: u16, body: &'static [u8]) {
        Mock::given(method("POST"))
            .and(path(route))
            .respond_with(ResponseTemplate::new(status).set_body_raw(body, "application/json"))
            .mount(server)
            .await;
    }

    fn post(uri: &str, body: serde_json::Value) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
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

    async fn rows(state: &AppState) -> Vec<Transfer> {
        state.store.list_transfers(None, None, 10).await.unwrap()
    }

    async fn node_state(state: &AppState) -> NodeState {
        state.store.node_state().await.unwrap()
    }

    const LOCKED: &[u8] = br#"{"error":"Node is locked","code":403,"name":"LockedNode"}"#;
    const INVOICE: &[u8] = br#"{"recipient_id":"r1","invoice":"rgb:inv","expiration_timestamp":1700000000,"batch_transfer_idx":7}"#;

    fn invoice_req(asset: Option<&str>, witness: bool) -> Request<Body> {
        post(
            "/rgbinvoice",
            json!({"asset_id": asset, "min_confirmations": 1, "witness": witness}),
        )
    }

    #[tokio::test]
    async fn rgbinvoice_inserts_row_and_wakes() {
        let server = MockServer::start().await;
        mount(&server, "/rgbinvoice", 200, INVOICE).await;
        let (_, state, app) = harness(&server.uri()).await;

        let (status, body) = call(app, invoice_req(Some("A"), false)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, INVOICE);
        let rows = rows(&state).await;
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.status, WaitingCounterparty);
        assert_eq!(row.recipient_id.as_deref(), Some("r1"));
        assert_eq!(row.batch_transfer_idx, Some(7));
        assert_eq!(row.asset_id.as_deref(), Some("A"));
        assert_eq!(row.invoice.as_deref(), Some("rgb:inv"));
        assert_eq!(row.expiration_timestamp, Some(1700000000));
        assert_eq!(row.kind.as_deref(), Some("ReceiveBlind"));
        assert_eq!(state.engine.wake_count(), 1);
    }

    #[tokio::test]
    async fn rgbinvoice_with_null_asset_inserts_agnostic_row() {
        let server = MockServer::start().await;
        mount(&server, "/rgbinvoice", 200, INVOICE).await;
        let (_, state, app) = harness(&server.uri()).await;

        let (status, _) = call(app, invoice_req(None, true)).await;

        assert_eq!(status, StatusCode::OK);
        let rows = rows(&state).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].asset_id, None);
        assert_eq!(rows[0].kind.as_deref(), Some("ReceiveWitness"));
        assert_eq!(rows[0].recipient_id.as_deref(), Some("r1"));
    }

    #[tokio::test]
    async fn rgbinvoice_upstream_error_inserts_nothing() {
        let server = MockServer::start().await;
        mount(&server, "/rgbinvoice", 403, LOCKED).await;
        let (_, state, app) = harness(&server.uri()).await;

        let (status, body) = call(app, invoice_req(Some("A"), false)).await;

        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body, LOCKED);
        assert!(rows(&state).await.is_empty());
        assert_eq!(node_state(&state).await, NodeState::Locked);
        assert_eq!(state.engine.wake_count(), 0);
    }

    fn send_req(donation: bool) -> Request<Body> {
        let recipient = json!({
            "recipient_id": "rcpt",
            "witness_data": null,
            "assignment": {"type": "Fungible", "value": 1},
            "transport_endpoints": ["rpc://x"]
        });
        post(
            "/sendrgb",
            json!({
                "donation": donation,
                "fee_rate": 5,
                "min_confirmations": 1,
                "expiration_timestamp": 1700000000,
                "recipient_map": {"A": [recipient.clone()], "B": [recipient]}
            }),
        )
    }

    async fn send_rows(donation: bool) -> (Arc<MockRln>, Arc<AppState>, Vec<Transfer>) {
        let server = MockServer::start().await;
        mount(&server, "/sendrgb", 200, br#"{"txid":"t1"}"#).await;
        let (rln, state, app) = harness(&server.uri()).await;

        let (status, body) = call(app, send_req(donation)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, br#"{"txid":"t1"}"#);
        let mut rows = rows(&state).await;
        rows.sort_by(|a, b| a.asset_id.cmp(&b.asset_id));
        (rln, state, rows)
    }

    #[tokio::test]
    async fn sendrgb_inserts_row_per_asset_and_syncs() {
        let (rln, state, rows) = send_rows(false).await;

        assert_eq!(rows.len(), 2);
        for (row, asset) in rows.iter().zip(["A", "B"]) {
            assert_eq!(row.asset_id.as_deref(), Some(asset));
            assert_eq!(row.status, WaitingCounterparty);
            assert_eq!(row.txid.as_deref(), Some("t1"));
            assert_eq!(row.kind.as_deref(), Some("Send"));
            assert_eq!(row.expiration_timestamp, Some(1700000000));
        }
        wait_until(|| rln.calls().len() == 2).await;
        assert_eq!(rln.calls(), vec!["list_transfers:A", "list_transfers:B"]);
        wait_until(|| state.engine.wake_count() == 1).await;
    }

    #[tokio::test]
    async fn sendrgb_donation_is_waiting_confirmations() {
        let (_, _, rows) = send_rows(true).await;

        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.status == WaitingConfirmations));
    }

    #[tokio::test]
    async fn issue_syncs_asset() {
        let server = MockServer::start().await;
        let body = br#"{"asset":{"asset_id":"X","ticker":"T","name":"n","precision":0,"issued_supply":1}}"#;
        mount(&server, "/issueassetnia", 200, body).await;
        let (rln, state, app) = harness(&server.uri()).await;

        let req = post(
            "/issueassetnia",
            json!({"amounts": [1], "ticker": "T", "name": "n", "precision": 0}),
        );
        let (status, resp) = call(app, req).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(resp, body);
        assert_eq!(
            state.store.list_assets().await.unwrap(),
            vec![("X".to_string(), "NIA".to_string())]
        );
        wait_until(|| !rln.calls().is_empty()).await;
        assert_eq!(rln.calls(), vec!["list_transfers:X"]);
    }

    #[tokio::test]
    async fn inflate_and_assetlink_sync_asset() {
        let server = MockServer::start().await;
        mount(&server, "/inflate", 200, br#"{"txid":"t"}"#).await;
        mount(
            &server,
            "/assetlink",
            200,
            br#"{"parent_asset_id":"P","child_asset_id":"C","created_at":1,"txid":"t"}"#,
        )
        .await;
        let (rln, _, app) = harness(&server.uri()).await;

        let req = post(
            "/inflate",
            json!({"asset_id": "X", "inflation_amounts": [1], "fee_rate": 5, "min_confirmations": 1}),
        );
        let (status, _) = call(app.clone(), req).await;
        assert_eq!(status, StatusCode::OK);
        wait_until(|| !rln.calls().is_empty()).await;
        assert_eq!(rln.calls(), vec!["list_transfers:X"]);

        let req = post(
            "/assetlink",
            json!({"parent_asset_id": "P", "child_asset_id": "C", "min_confirmations": 1}),
        );
        let (status, _) = call(app, req).await;
        assert_eq!(status, StatusCode::OK);
        wait_until(|| rln.calls().len() == 3).await;
        assert_eq!(
            rln.calls(),
            vec!["list_transfers:X", "list_transfers:C", "list_transfers:P"]
        );
    }

    async fn seed_pending(state: &AppState, asset: &str) {
        state
            .store
            .insert_transfer(
                &NewTransfer {
                    asset_id: Some(asset.into()),
                    recipient_id: Some("r".into()),
                    batch_transfer_idx: Some(7),
                    ..NewTransfer::with_status(WaitingCounterparty)
                },
                1,
            )
            .await
            .unwrap();
    }

    async fn call_under_held_lock(
        server: &MockServer,
        state: &Arc<AppState>,
        app: Router,
        req: Request<Body>,
    ) -> (StatusCode, Vec<u8>) {
        let guard = state.engine.lock().await;
        let task = tokio::spawn(call(app, req));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(server.received_requests().await.unwrap().is_empty());
        drop(guard);
        task.await.unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn refreshtransfers_holds_lock_and_syncs_pending() {
        let server = MockServer::start().await;
        mount(&server, "/refreshtransfers", 200, b"{}").await;
        let (rln, state, app) = harness(&server.uri()).await;
        seed_pending(&state, "A").await;

        let req = post(
            "/refreshtransfers",
            json!({"asset_id": null, "filter": [], "skip_sync": false}),
        );
        let (status, body) = call_under_held_lock(&server, &state, app, req).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"{}");
        assert_eq!(rln.calls(), vec!["list_transfers:A"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failtransfers_holds_lock_and_syncs() {
        let server = MockServer::start().await;
        mount(
            &server,
            "/failtransfers",
            200,
            br#"{"transfers_changed":true}"#,
        )
        .await;
        let (rln, state, app) = harness(&server.uri()).await;
        seed_pending(&state, "A").await;

        let req = post(
            "/failtransfers",
            json!({"batch_transfer_idx": 7, "no_asset_only": false, "skip_sync": false}),
        );
        let (status, body) = call_under_held_lock(&server, &state, app, req).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, br#"{"transfers_changed":true}"#);
        assert_eq!(rln.calls(), vec!["list_transfers:A"]);
    }

    async fn lifecycle(
        route: &str,
        req: serde_json::Value,
        upstream: &'static [u8],
    ) -> (Arc<MockRln>, Arc<AppState>, Vec<u8>) {
        let server = MockServer::start().await;
        mount(&server, route, 200, upstream).await;
        let (rln, state, app) = harness(&server.uri()).await;
        rln.add_asset("A", "NIA", vec![]);

        let (status, body) = call(app, post(route, req)).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, upstream);
        (rln, state, body)
    }

    #[tokio::test]
    async fn unlock_sets_state_and_reconciles() {
        let (rln, state, _) = lifecycle("/unlock", json!({"password": "p"}), b"{}").await;

        assert_eq!(node_state(&state).await, NodeState::Unlocked);
        wait_until(|| rln.calls().contains(&"list_transfers:A".to_string())).await;
        let calls = rln.calls();
        assert!(calls.contains(&"list_assets".to_string()), "{calls:?}");
    }

    #[tokio::test]
    async fn init_restore_lock_shutdown_set_node_state_without_syncing() {
        let cases: [(&str, serde_json::Value, &[u8], NodeState); 4] = [
            (
                "/init",
                json!({"password": "p"}),
                br#"{"mnemonic":"a b c"}"#,
                NodeState::Locked,
            ),
            (
                "/restore",
                json!({"backup_path": "/b", "password": "p"}),
                b"{}",
                NodeState::Locked,
            ),
            ("/lock", json!({}), b"{}", NodeState::Locked),
            ("/shutdown", json!({}), b"{}", NodeState::Down),
        ];
        for (route, req, upstream, expected) in cases {
            let (rln, state, _) = lifecycle(route, req, upstream).await;
            assert_eq!(node_state(&state).await, expected, "{route}");
            assert!(rln.calls().is_empty(), "{route}");
        }
    }

    #[tokio::test]
    async fn unlock_failure_leaves_state() {
        let server = MockServer::start().await;
        let err = br#"{"error":"Invalid password","code":403,"name":"InvalidPassword"}"#;
        mount(&server, "/unlock", 403, err).await;
        let (rln, state, app) = harness(&server.uri()).await;

        let (status, body) = call(app, post("/unlock", json!({"password": "p"}))).await;

        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body, err);
        assert_eq!(node_state(&state).await, NodeState::Unknown);
        assert!(rln.calls().is_empty());
    }

    #[tokio::test]
    async fn dot_segment_does_not_bypass_interceptor() {
        let server = MockServer::start().await;
        mount(&server, "/rgbinvoice", 200, INVOICE).await;
        let (_, state, app) = harness(&server.uri()).await;

        let req = post(
            "/./rgbinvoice",
            json!({"asset_id": "A", "min_confirmations": 1, "witness": false}),
        );
        let (status, body) = call(app, req).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["name"], "InvalidRequest");
        assert!(rows(&state).await.is_empty());
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn interceptor_upstream_unreachable_is_502() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let (_, state, app) = harness(&format!("http://127.0.0.1:{port}")).await;

        let (status, body) = call(app, invoice_req(Some("A"), false)).await;

        assert_eq!(status, StatusCode::BAD_GATEWAY);
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["name"], "UpstreamUnreachable");
        assert!(rows(&state).await.is_empty());
    }
}
