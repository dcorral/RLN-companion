#![allow(clippy::unwrap_used, clippy::expect_used, dead_code)]

use std::future::Future;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use reqwest::Method;
use rln_companion::app::{build_app, build_router, spawn_background};
use rln_companion::config::Config;
use rln_companion::webhook::signature;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing_subscriber::EnvFilter;

pub const SECRET: &str = "e2e-secret";
pub const PASSWORD: &str = "e2e-password-1234";
pub const FEE_RATE: u64 = 7;

pub fn indexer_url() -> String {
    std::env::var("E2E_INDEXER").unwrap_or_else(|_| "127.0.0.1:50001".into())
}

pub fn proxy_endpoint() -> String {
    std::env::var("E2E_PROXY").unwrap_or_else(|_| "rpc://127.0.0.1:3000/json-rpc".into())
}

pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

pub fn rln_url(n: u8) -> String {
    std::env::var(format!("E2E_RLN{n}"))
        .unwrap_or_else(|_| panic!("E2E_RLN{n} not set: run this suite through ./e2e/run.sh"))
}

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    while !cond().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub async fn retry<T, F, Fut>(what: &str, timeout: Duration, mut op: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        match op().await {
            Ok(v) => return v,
            Err(e) if Instant::now() < deadline => {
                eprintln!("{what}: {e}; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => panic!("{what}: gave up: {e}"),
        }
    }
}

pub struct Companion {
    pub base_url: String,
    handles: Vec<JoinHandle<()>>,
    _dir: tempfile::TempDir,
}

impl Companion {
    pub async fn start(
        rln_url: &str,
        webhook_url: &str,
        extra: impl FnOnce(&mut Config),
    ) -> Companion {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.rln.base_url = rln_url.to_string();
        cfg.webhook.url = webhook_url.to_string();
        cfg.webhook.secret = SECRET.to_string();
        cfg.webhook.dispatch_interval_secs = 1;
        cfg.database.path = dir
            .path()
            .join("companion.sqlite")
            .to_string_lossy()
            .into_owned();
        cfg.engine.refresh_interval_secs = 2;
        cfg.engine.reap_interval_secs = 2;
        cfg.engine.reconcile_backoff_secs = 1;
        cfg.engine.reconcile_max_wait_secs = 180;
        cfg.sync.full_interval_secs = 30;
        extra(&mut cfg);
        cfg.validate().unwrap();

        let app = build_app(&cfg, None).await.unwrap();
        app.engine.probe().await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let mut handles = spawn_background(&app);
        let engine = app.engine.clone();
        handles.push(tokio::spawn(async move {
            match engine.reconcile().await {
                Ok(true) => {}
                Ok(false) => tracing::warn!("reconcile gave up"),
                Err(e) => tracing::error!(error = %e, "reconcile failed"),
            }
        }));
        let router = build_router(app.state.clone());
        handles.push(tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, router).await {
                tracing::error!(error = %e, "companion serve failed");
            }
        }));
        let base_url = format!("http://127.0.0.1:{port}");
        tracing::info!(%base_url, rln_url, "companion started");
        Companion {
            base_url,
            handles,
            _dir: dir,
        }
    }

    pub fn stop(&mut self) {
        for h in self.handles.drain(..) {
            h.abort();
        }
    }
}

impl Drop for Companion {
    fn drop(&mut self) {
        self.stop();
    }
}

type Delivery = Result<Value, String>;

pub struct WebhookSink {
    pub url: String,
    rx: mpsc::UnboundedReceiver<Delivery>,
    handle: JoinHandle<()>,
}

async fn hook(
    State(tx): State<mpsc::UnboundedSender<Delivery>>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let presented = headers
        .get("x-companion-signature")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let delivery = if presented != signature(SECRET, &body) {
        Err(format!("bad signature {presented:?} on {body:?}"))
    } else {
        serde_json::from_slice(&body).map_err(|e| format!("bad payload: {e}"))
    };
    let _ = tx.send(delivery);
    StatusCode::OK
}

impl WebhookSink {
    pub async fn start() -> WebhookSink {
        let (tx, rx) = mpsc::unbounded_channel();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let router = Router::new().route("/hook", post(hook)).with_state(tx);
        let handle = tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, router).await {
                tracing::error!(error = %e, "sink serve failed");
            }
        });
        WebhookSink {
            url: format!("http://127.0.0.1:{port}/hook"),
            rx,
            handle,
        }
    }

    pub async fn next(&mut self, timeout: Duration) -> Option<Value> {
        match tokio::time::timeout(timeout, self.rx.recv()).await {
            Ok(Some(Ok(v))) => Some(v),
            Ok(Some(Err(e))) => panic!("webhook sink: {e}"),
            Ok(None) => panic!("webhook sink closed"),
            Err(_) => None,
        }
    }

    pub async fn next_of(&mut self, event_type: &str, timeout: Duration) -> Option<Value> {
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let ev = self.next(remaining).await?;
            if ev["event_type"] == event_type {
                return Some(ev);
            }
            eprintln!(
                "skipping event {} ({})",
                ev["event_type"], ev["transfer"]["id"]
            );
        }
    }
}

impl Drop for WebhookSink {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

pub struct Rln {
    pub base_url: String,
    client: reqwest::Client,
}

impl Rln {
    pub fn new(base_url: &str) -> Rln {
        Rln {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(600))
                .build()
                .unwrap(),
        }
    }

    async fn send(&self, method: Method, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        let mut req = self
            .client
            .request(method, format!("{}{path}", self.base_url));
        if let Some(b) = body {
            req = req.json(&b);
        }
        let resp = req.send().await.unwrap_or_else(|e| panic!("{path}: {e}"));
        let status = resp.status();
        let text = resp.text().await.unwrap();
        let value = serde_json::from_str(&text).unwrap_or(Value::String(text));
        (status, value)
    }

    async fn try_post(&self, path: &str, body: Option<Value>) -> Result<Value, String> {
        let (status, value) = self.send(Method::POST, path, body).await;
        if status.is_success() {
            Ok(value)
        } else {
            Err(format!("POST {path} -> {status}: {value}"))
        }
    }

    async fn post(&self, path: &str, body: Value) -> Value {
        self.try_post(path, Some(body))
            .await
            .unwrap_or_else(|e| panic!("{e}"))
    }

    async fn get(&self, path: &str) -> Value {
        let (status, value) = self.send(Method::GET, path, None).await;
        assert!(status.is_success(), "GET {path} -> {status}: {value}");
        value
    }

    async fn post_tolerating(&self, path: &str, body: Option<Value>, ok_names: &[&str]) {
        let (status, value) = self.send(Method::POST, path, body).await;
        if status.is_success() {
            return;
        }
        let name = value["name"].as_str().unwrap_or("");
        assert!(ok_names.contains(&name), "POST {path} -> {status}: {value}");
        eprintln!("POST {path}: {name}, treating as done");
    }

    pub async fn init(&self) {
        let body = json!({"password": PASSWORD, "mnemonic": null});
        self.post_tolerating("/init", Some(body), &["AlreadyInitialized", "UnlockedNode"])
            .await;
    }

    pub async fn unlock(&self) {
        let body = json!({
            "password": PASSWORD,
            "ldk_chain_sync": {"mode": "TransactionSync", "config": {"indexer_url": indexer_url()}},
            "indexer_url": indexer_url(),
            "proxy_endpoint": proxy_endpoint(),
            "announce_addresses": [],
            "announce_alias": "RLN_alias",
            "gossip_source": null
        });
        self.post_tolerating("/unlock", Some(body), &["AlreadyUnlocked", "UnlockedNode"])
            .await;
    }

    pub async fn lock(&self) {
        self.post_tolerating("/lock", None, &["LockedNode"]).await;
    }

    pub async fn address(&self) -> String {
        self.try_post("/address", None)
            .await
            .unwrap_or_else(|e| panic!("{e}"))["address"]
            .as_str()
            .unwrap()
            .to_string()
    }

    pub async fn try_create_utxos(&self, num: u8, size: u32) -> Result<Value, String> {
        let body = json!({
            "up_to": false,
            "num": num,
            "size": size,
            "fee_rate": FEE_RATE,
            "skip_sync": false
        });
        self.try_post("/createutxos", Some(body)).await
    }

    pub async fn create_utxos(&self, num: u8, size: u32) {
        self.try_create_utxos(num, size)
            .await
            .unwrap_or_else(|e| panic!("{e}"));
    }

    pub async fn issue_nia(&self, ticker: &str, amount: u64) -> String {
        let body = json!({
            "amounts": [amount],
            "ticker": ticker,
            "name": ticker,
            "precision": 0
        });
        let resp = retry("issueassetnia", Duration::from_secs(60), || {
            self.try_post("/issueassetnia", Some(body.clone()))
        })
        .await;
        resp["asset"]["asset_id"].as_str().unwrap().to_string()
    }

    pub async fn rgb_invoice(
        &self,
        asset_id: Option<&str>,
        expiration: Option<u64>,
    ) -> (String, String, i32) {
        let mut body = json!({
            "asset_id": asset_id,
            "assignment": null,
            "min_confirmations": 1,
            "witness": false,
            "transport_endpoints": [proxy_endpoint()]
        });
        if let Some(e) = expiration {
            body["expiration_timestamp"] = e.into();
        }
        let resp = self.post("/rgbinvoice", body).await;
        (
            resp["recipient_id"].as_str().unwrap().to_string(),
            resp["invoice"].as_str().unwrap().to_string(),
            resp["batch_transfer_idx"].as_i64().unwrap() as i32,
        )
    }

    pub async fn send_rgb(
        &self,
        asset_id: &str,
        recipient_id: &str,
        amount: u64,
        donation: bool,
    ) -> String {
        let body = json!({
            "donation": donation,
            "fee_rate": FEE_RATE,
            "min_confirmations": 1,
            "recipient_map": {
                asset_id: [{
                    "recipient_id": recipient_id,
                    "witness_data": null,
                    "assignment": {"type": "Fungible", "value": amount},
                    "transport_endpoints": [proxy_endpoint()]
                }]
            }
        });
        self.post("/sendrgb", body).await["txid"]
            .as_str()
            .unwrap()
            .to_string()
    }

    pub async fn asset_balance(&self, asset_id: &str) -> u64 {
        self.post("/assetbalance", json!({"asset_id": asset_id}))
            .await["spendable"]
            .as_u64()
            .unwrap()
    }

    pub async fn list_transfers(&self, asset_id: &str) -> Vec<Value> {
        let body = json!({
            "asset_filter": {"type": "Id", "value": asset_id},
            "txid": null,
            "index_offset": null,
            "max_transfers": null,
            "status": null,
            "created_after": null,
            "created_before": null
        });
        self.post("/listtransfers", body).await["transfers"]
            .as_array()
            .unwrap()
            .clone()
    }

    pub async fn health(&self) -> Value {
        self.get("/companion/health").await
    }

    pub async fn companion_transfers(&self) -> Vec<Value> {
        self.get("/companion/transfers").await["transfers"]
            .as_array()
            .unwrap()
            .clone()
    }

    pub async fn wait_synced(&self, timeout: Duration) {
        wait_until("companion unlocked and synced", timeout, || async {
            let h = self.health().await;
            h["node"] == "unlocked" && !h["last_full_sync_at"].is_null()
        })
        .await;
    }
}

pub struct Bitcoind;

impl Bitcoind {
    async fn cli(args: &[&str]) -> String {
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        tokio::task::spawn_blocking(move || {
            let out = Command::new("docker")
                .args([
                    "compose",
                    "-f",
                    "e2e/compose.yaml",
                    "exec",
                    "-T",
                    "-u",
                    "blits",
                ])
                .args(["bitcoind", "bitcoin-cli", "-regtest"])
                .args(&args)
                .output()
                .expect("docker compose exec");
            assert!(
                out.status.success(),
                "bitcoin-cli {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        })
        .await
        .unwrap()
    }

    pub async fn send_to(addr: &str, btc: f64) -> String {
        Self::cli(&["-rpcwallet=miner", "sendtoaddress", addr, &btc.to_string()]).await
    }

    pub async fn block_count() -> u64 {
        Self::cli(&["getblockcount"]).await.parse().unwrap()
    }

    pub async fn mine(n: u32) {
        Self::cli(&["-rpcwallet=miner", "-generate", &n.to_string()]).await;
        let height = Self::block_count().await;
        wait_until(
            &format!("electrs to index height {height}"),
            Duration::from_secs(60),
            || electrs_has("blockchain.block.header", json!([height])),
        )
        .await;
    }

    pub async fn mempool() -> Vec<String> {
        serde_json::from_str(&Self::cli(&["getrawmempool"]).await).unwrap()
    }
}

async fn electrs_has(method: &'static str, params: Value) -> bool {
    tokio::task::spawn_blocking(move || electrs_query(method, &params))
        .await
        .unwrap()
}

fn electrs_query(method: &str, params: &Value) -> bool {
    let addr = indexer_url().parse().unwrap();
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_secs(2)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let req = json!({"id": 0, "method": method, "params": params});
    if stream.write_all(format!("{req}\n").as_bytes()).is_err() {
        return false;
    }
    let mut line = String::new();
    if BufReader::new(stream).read_line(&mut line).is_err() {
        return false;
    }
    serde_json::from_str::<Value>(&line).is_ok_and(|v| v["result"].is_string())
}

pub async fn mine_and_index(txid: &str) {
    Bitcoind::mine(1).await;
    wait_until(
        &format!("electrs to serve tx {txid}"),
        Duration::from_secs(60),
        || electrs_has("blockchain.transaction.get", json!([txid])),
    )
    .await;
}

pub async fn fund(rln: &Rln) {
    let addr = rln.address().await;
    Bitcoind::send_to(&addr, 1.0).await;
    Bitcoind::mine(1).await;
    retry("createutxos", Duration::from_secs(60), || {
        rln.try_create_utxos(10, 32_000)
    })
    .await;
    Bitcoind::mine(1).await;
}
