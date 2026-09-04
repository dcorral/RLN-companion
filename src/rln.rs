use reqwest::RequestBuilder;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::config;
use crate::store::{PaymentDirection, PaymentStatus, TransferStatus};

#[derive(Serialize)]
pub struct RefreshRequest {
    pub asset_id: Option<String>,
    pub filter: Vec<()>,
    pub skip_sync: bool,
}

#[derive(Clone, Serialize)]
#[serde(tag = "type", content = "value")]
pub enum AssetFilter {
    None,
    Id(String),
}

#[derive(Serialize)]
pub struct ListTransfersRequest {
    pub asset_filter: AssetFilter,
    pub txid: Option<String>,
    pub index_offset: Option<u64>,
    pub max_transfers: Option<u64>,
}

#[derive(Deserialize)]
pub struct ListTransfersResponse {
    pub transfers: Vec<RlnTransfer>,
    pub first_index_offset: u64,
    pub last_index_offset: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RlnTransfer {
    pub idx: i32,
    pub status: TransferStatus,
    pub kind: String,
    pub recipient_id: Option<String>,
    pub txid: Option<String>,
    pub expiration_timestamp: Option<u64>,
}

#[derive(Deserialize)]
pub struct ListPaymentsResponse {
    pub payments: Vec<RlnPayment>,
    pub first_index_offset: u64,
    pub last_index_offset: u64,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub enum RlnPaymentType {
    Outbound,
    InboundAutoClaim,
    InboundHodl,
}

impl RlnPaymentType {
    pub fn direction(self) -> PaymentDirection {
        match self {
            Self::Outbound => PaymentDirection::Outbound,
            Self::InboundAutoClaim | Self::InboundHodl => PaymentDirection::Inbound,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RlnPayment {
    pub amt_msat: Option<u64>,
    pub asset_amount: Option<u64>,
    pub asset_id: Option<String>,
    pub payment_hash: String,
    pub payment_type: RlnPaymentType,
    pub status: PaymentStatus,
    pub payee_pubkey: String,
}

#[derive(Serialize)]
pub struct DecodeLNInvoiceRequest {
    pub invoice: String,
}

#[derive(Deserialize)]
pub struct DecodeLNInvoiceResponse {
    pub payment_hash: String,
}

#[derive(Serialize)]
pub struct ListAssetsRequest {
    pub filter_asset_schemas: Vec<String>,
}

#[derive(Deserialize)]
pub struct ListAssetsResponse {
    pub nia: Option<Vec<AssetId>>,
    pub uda: Option<Vec<AssetId>>,
    pub cfa: Option<Vec<AssetId>>,
    pub ifa: Option<Vec<AssetId>>,
}

#[derive(Deserialize)]
pub struct AssetId {
    pub asset_id: String,
}

#[derive(Serialize)]
pub struct FailTransfersRequest {
    pub batch_transfer_idx: Option<i32>,
    pub no_asset_only: bool,
    pub skip_sync: bool,
}

#[derive(Deserialize)]
pub struct FailTransfersResponse {
    pub transfers_changed: bool,
}

#[derive(Deserialize)]
pub struct NetworkInfoResponse {
    pub network: String,
}

#[derive(Deserialize)]
pub struct RlnErrorBody {
    pub error: String,
    pub code: u16,
    pub name: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RlnError {
    #[error("node locked or changing state")]
    Locked,
    #[error("batch transfer cannot be failed")]
    CannotFail,
    #[error("rln api error {code} {name}: {message}")]
    Api {
        code: u16,
        name: String,
        message: String,
    },
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("decode: {0}")]
    Decode(String),
}

#[async_trait::async_trait]
pub trait RlnApi: Send + Sync {
    async fn refresh(&self, skip_sync: bool) -> Result<(), RlnError>;
    async fn list_transfers(
        &self,
        asset_id: &str,
        page_size: u64,
    ) -> Result<Vec<RlnTransfer>, RlnError>;
    async fn list_assetless_transfers(&self, page_size: u64) -> Result<Vec<RlnTransfer>, RlnError>;
    async fn list_payments(&self, page_size: u64) -> Result<Vec<RlnPayment>, RlnError>;
    async fn decode_ln_invoice(&self, invoice: &str) -> Result<String, RlnError>;
    /// (asset_id, schema) with schema one of NIA, UDA, CFA, IFA.
    async fn list_assets(&self) -> Result<Vec<(String, String)>, RlnError>;
    async fn fail_transfer(&self, batch_transfer_idx: i32) -> Result<(), RlnError>;
    async fn node_info(&self) -> Result<serde_json::Value, RlnError>;
    async fn network(&self) -> Result<String, RlnError>;
}

pub struct HttpRlnClient {
    base_url: String,
    client: reqwest::Client,
    token: Option<String>,
}

impl HttpRlnClient {
    pub fn new(cfg: &config::Rln) -> Result<Self, RlnError> {
        let client = crate::http_client(
            cfg.request_timeout_secs,
            reqwest::redirect::Policy::default(),
        )?;
        Ok(Self {
            base_url: cfg.base_url.trim_end_matches('/').to_string(),
            client,
            token: cfg.token.clone(),
        })
    }

    async fn post<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &impl Serialize,
    ) -> Result<T, RlnError> {
        let req = self.client.post(self.url(path)).json(body);
        self.send(req).await
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, RlnError> {
        self.send(self.client.get(self.url(path))).await
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    async fn send<T: DeserializeOwned>(&self, req: RequestBuilder) -> Result<T, RlnError> {
        let req = match &self.token {
            Some(token) => req.bearer_auth(token),
            None => req,
        };
        let resp = req.send().await?;
        let status = resp.status().as_u16();
        if resp.status().is_success() {
            let bytes = resp.bytes().await?;
            return serde_json::from_slice(&bytes).map_err(|e| RlnError::Decode(e.to_string()));
        }
        Err(classify_error(status, &resp.text().await?))
    }

    async fn list(
        &self,
        asset_filter: AssetFilter,
        page_size: u64,
    ) -> Result<Vec<RlnTransfer>, RlnError> {
        let page_size = page_size.max(1);
        let mut out = Vec::new();
        let mut index_offset = None;
        loop {
            let body = ListTransfersRequest {
                asset_filter: asset_filter.clone(),
                txid: None,
                index_offset,
                max_transfers: Some(page_size),
            };
            let page: ListTransfersResponse = self.post("/listtransfers", &body).await?;
            let count = page.transfers.len() as u64;
            out.extend(page.transfers);
            if count < page_size || index_offset == Some(page.last_index_offset) {
                return Ok(out);
            }
            index_offset = Some(page.last_index_offset);
        }
    }
}

fn classify_error(status: u16, body: &str) -> RlnError {
    match serde_json::from_str::<RlnErrorBody>(body) {
        Ok(err) => match (status, err.name.as_str()) {
            (403, "LockedNode" | "ChangingState") => RlnError::Locked,
            (403, "CannotFailBatchTransfer") => RlnError::CannotFail,
            _ => RlnError::Api {
                code: err.code,
                name: err.name,
                message: err.error,
            },
        },
        Err(_) => RlnError::Api {
            code: status,
            name: "Unknown".to_string(),
            message: body.chars().take(200).collect(),
        },
    }
}

#[async_trait::async_trait]
impl RlnApi for HttpRlnClient {
    async fn refresh(&self, skip_sync: bool) -> Result<(), RlnError> {
        let body = RefreshRequest {
            asset_id: None,
            filter: vec![],
            skip_sync,
        };
        let _: serde_json::Value = self.post("/refreshtransfers", &body).await?;
        Ok(())
    }

    async fn list_transfers(
        &self,
        asset_id: &str,
        page_size: u64,
    ) -> Result<Vec<RlnTransfer>, RlnError> {
        self.list(AssetFilter::Id(asset_id.to_string()), page_size)
            .await
    }

    async fn list_assetless_transfers(&self, page_size: u64) -> Result<Vec<RlnTransfer>, RlnError> {
        self.list(AssetFilter::None, page_size).await
    }

    async fn list_payments(&self, page_size: u64) -> Result<Vec<RlnPayment>, RlnError> {
        let page_size = page_size.max(1);
        let mut out = Vec::new();
        let mut index_offset: Option<u64> = None;
        loop {
            let mut req = self
                .client
                .get(self.url("/listpayments"))
                .query(&[("max_payments", page_size)]);
            if let Some(offset) = index_offset {
                req = req.query(&[("index_offset", offset)]);
            }
            let page: ListPaymentsResponse = self.send(req).await?;
            let count = page.payments.len() as u64;
            out.extend(page.payments);
            if count < page_size || index_offset == Some(page.last_index_offset) {
                return Ok(out);
            }
            index_offset = Some(page.last_index_offset);
        }
    }

    async fn decode_ln_invoice(&self, invoice: &str) -> Result<String, RlnError> {
        let body = DecodeLNInvoiceRequest {
            invoice: invoice.to_string(),
        };
        let resp: DecodeLNInvoiceResponse = self.post("/decodelninvoice", &body).await?;
        Ok(resp.payment_hash)
    }

    async fn list_assets(&self) -> Result<Vec<(String, String)>, RlnError> {
        let body = ListAssetsRequest {
            filter_asset_schemas: vec![],
        };
        let resp: ListAssetsResponse = self.post("/listassets", &body).await?;
        let mut out = Vec::new();
        for (schema, assets) in [
            ("NIA", resp.nia),
            ("UDA", resp.uda),
            ("CFA", resp.cfa),
            ("IFA", resp.ifa),
        ] {
            out.extend(
                assets
                    .unwrap_or_default()
                    .into_iter()
                    .map(|a| (a.asset_id, schema.to_string())),
            );
        }
        Ok(out)
    }

    async fn fail_transfer(&self, batch_transfer_idx: i32) -> Result<(), RlnError> {
        let body = FailTransfersRequest {
            batch_transfer_idx: Some(batch_transfer_idx),
            no_asset_only: false,
            skip_sync: false,
        };
        let _: FailTransfersResponse = self.post("/failtransfers", &body).await?;
        Ok(())
    }

    async fn node_info(&self) -> Result<serde_json::Value, RlnError> {
        self.get("/nodeinfo").await
    }

    async fn network(&self) -> Result<String, RlnError> {
        let resp: NetworkInfoResponse = self.get("/networkinfo").await?;
        Ok(resp.network)
    }
}

#[cfg(test)]
pub mod test_support {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::*;

    #[derive(Debug, Clone, Copy)]
    pub enum MockFailure {
        Locked,
        Transport,
        CannotFail,
        Api,
        Unauthorized,
        Forbidden,
    }

    impl MockFailure {
        fn into_error(self) -> RlnError {
            let api = |code, name: &str| RlnError::Api {
                code,
                name: name.into(),
                message: "boom".into(),
            };
            match self {
                Self::Locked => RlnError::Locked,
                Self::CannotFail => RlnError::CannotFail,
                Self::Api => api(500, "Internal"),
                Self::Unauthorized => api(401, "Unauthorized"),
                Self::Forbidden => api(403, "Forbidden"),
                Self::Transport => RlnError::Transport(
                    reqwest::Client::new()
                        .get("::not a url::")
                        .build()
                        .unwrap_err(),
                ),
            }
        }
    }

    pub async fn wait_until(mut done: impl FnMut() -> bool) {
        for _ in 0..200 {
            if done() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        panic!("condition not met within 1s");
    }

    pub struct MockRln {
        pub transfers: Mutex<HashMap<String, Vec<RlnTransfer>>>,
        pub assets: Mutex<Vec<(String, String)>>,
        pub payments: Mutex<Vec<RlnPayment>>,
        pub invoices: Mutex<HashMap<String, String>>,
        pub fail_with: Mutex<Option<MockFailure>>,
        pub fail_call: Mutex<Option<(String, MockFailure)>>,
        pub calls: Mutex<Vec<String>>,
        pub page_sizes: Mutex<Vec<u64>>,
        pub network: Mutex<String>,
    }

    impl Default for MockRln {
        fn default() -> Self {
            Self {
                transfers: Mutex::default(),
                assets: Mutex::default(),
                payments: Mutex::default(),
                invoices: Mutex::default(),
                fail_with: Mutex::default(),
                fail_call: Mutex::default(),
                calls: Mutex::default(),
                page_sizes: Mutex::default(),
                network: Mutex::new("Regtest".into()),
            }
        }
    }

    impl MockRln {
        pub fn payment(
            hash: &str,
            status: crate::store::PaymentStatus,
            payment_type: RlnPaymentType,
        ) -> RlnPayment {
            RlnPayment {
                amt_msat: Some(3000000),
                asset_amount: Some(42),
                asset_id: Some("rgb:asset".into()),
                payment_hash: hash.into(),
                payment_type,
                status,
                payee_pubkey: "02aa".into(),
            }
        }

        pub fn set_payments(&self, payments: Vec<RlnPayment>) {
            *self.payments.lock().unwrap() = payments;
        }

        pub fn add_invoice(&self, invoice: &str, payment_hash: &str) {
            self.invoices
                .lock()
                .unwrap()
                .insert(invoice.into(), payment_hash.into());
        }
        pub fn transfer(idx: i32, status: TransferStatus, recipient: Option<&str>) -> RlnTransfer {
            RlnTransfer {
                idx,
                status,
                kind: "ReceiveBlind".into(),
                recipient_id: recipient.map(str::to_string),
                txid: None,
                expiration_timestamp: None,
            }
        }

        pub fn add_asset(&self, asset_id: &str, schema: &str, transfers: Vec<RlnTransfer>) {
            self.assets
                .lock()
                .unwrap()
                .push((asset_id.into(), schema.into()));
            self.transfers
                .lock()
                .unwrap()
                .insert(asset_id.into(), transfers);
        }

        pub fn set_assetless(&self, transfers: Vec<RlnTransfer>) {
            self.transfers
                .lock()
                .unwrap()
                .insert(String::new(), transfers);
        }

        pub fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }

        fn record(&self, call: String) -> Result<(), RlnError> {
            let scoped = match &*self.fail_call.lock().unwrap() {
                Some((name, f)) if *name == call => Some(*f),
                _ => None,
            };
            self.calls.lock().unwrap().push(call);
            match scoped.or(*self.fail_with.lock().unwrap()) {
                Some(f) => Err(f.into_error()),
                None => Ok(()),
            }
        }
    }

    #[async_trait::async_trait]
    impl RlnApi for MockRln {
        async fn refresh(&self, _skip_sync: bool) -> Result<(), RlnError> {
            self.record("refresh".into())
        }

        async fn list_transfers(
            &self,
            asset_id: &str,
            page_size: u64,
        ) -> Result<Vec<RlnTransfer>, RlnError> {
            self.record(format!("list_transfers:{asset_id}"))?;
            self.page_sizes.lock().unwrap().push(page_size);
            Ok(self
                .transfers
                .lock()
                .unwrap()
                .get(asset_id)
                .cloned()
                .unwrap_or_default())
        }

        async fn list_assetless_transfers(
            &self,
            _page_size: u64,
        ) -> Result<Vec<RlnTransfer>, RlnError> {
            self.record("list_assetless_transfers".into())?;
            Ok(self
                .transfers
                .lock()
                .unwrap()
                .get("")
                .cloned()
                .unwrap_or_default())
        }

        async fn list_payments(&self, page_size: u64) -> Result<Vec<RlnPayment>, RlnError> {
            self.record("list_payments".into())?;
            self.page_sizes.lock().unwrap().push(page_size);
            Ok(self.payments.lock().unwrap().clone())
        }

        async fn decode_ln_invoice(&self, invoice: &str) -> Result<String, RlnError> {
            self.record(format!("decode_ln_invoice:{invoice}"))?;
            self.invoices
                .lock()
                .unwrap()
                .get(invoice)
                .cloned()
                .ok_or_else(|| RlnError::Decode(format!("unknown invoice {invoice}")))
        }

        async fn list_assets(&self) -> Result<Vec<(String, String)>, RlnError> {
            self.record("list_assets".into())?;
            Ok(self.assets.lock().unwrap().clone())
        }

        async fn fail_transfer(&self, batch_transfer_idx: i32) -> Result<(), RlnError> {
            self.record(format!("fail_transfer:{batch_transfer_idx}"))
        }

        async fn node_info(&self) -> Result<serde_json::Value, RlnError> {
            self.record("node_info".into())?;
            Ok(serde_json::json!({}))
        }

        async fn network(&self) -> Result<String, RlnError> {
            self.record("network".into())?;
            Ok(self.network.lock().unwrap().clone())
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::config;
    use crate::store::{PaymentDirection, PaymentStatus};
    use serde_json::json;
    use wiremock::matchers::{
        body_json, header, method, path, query_param, query_param_is_missing,
    };
    use wiremock::{Match, Mock, MockServer, Request, ResponseTemplate};

    struct NoAuthHeader;

    impl Match for NoAuthHeader {
        fn matches(&self, req: &Request) -> bool {
            !req.headers.contains_key("authorization")
        }
    }

    fn client(server: &MockServer, token: Option<&str>) -> HttpRlnClient {
        let cfg = config::Rln {
            base_url: format!("{}/", server.uri()),
            token: token.map(str::to_string),
            ..Default::default()
        };
        HttpRlnClient::new(&cfg).unwrap()
    }

    fn transfer(idx: i32) -> serde_json::Value {
        json!({
            "idx": idx,
            "created_at": 1,
            "updated_at": 1,
            "status": "WaitingCounterparty",
            "requested_assignment": null,
            "assignments": [],
            "kind": "ReceiveBlind",
            "txid": null,
            "recipient_id": format!("rcpt{idx}"),
            "proxy_recipient_id": null,
            "receive_utxo": null,
            "change_utxo": null,
            "expiration_timestamp": 123,
            "transport_endpoints": []
        })
    }

    fn list_request(index_offset: Option<u64>, max: u64) -> serde_json::Value {
        json!({
            "asset_filter": {"type": "Id", "value": "asset"},
            "txid": null,
            "index_offset": index_offset,
            "max_transfers": max
        })
    }

    #[tokio::test]
    async fn refresh_sends_unfiltered_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/refreshtransfers"))
            .and(body_json(json!({
                "asset_id": null,
                "filter": [],
                "skip_sync": true
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;

        client(&server, None).refresh(true).await.unwrap();
    }

    #[tokio::test]
    async fn list_transfers_follows_pagination() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/listtransfers"))
            .and(body_json(list_request(None, 2)))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "transfers": [transfer(9), transfer(5)],
                "first_index_offset": 9,
                "last_index_offset": 5
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/listtransfers"))
            .and(body_json(list_request(Some(5), 2)))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "transfers": [transfer(2)],
                "first_index_offset": 2,
                "last_index_offset": 2
            })))
            .expect(1)
            .mount(&server)
            .await;

        let transfers = client(&server, None)
            .list_transfers("asset", 2)
            .await
            .unwrap();

        let idxs: Vec<i32> = transfers.iter().map(|t| t.idx).collect();
        assert_eq!(idxs, vec![9, 5, 2]);
        assert_eq!(transfers[0].status, TransferStatus::WaitingCounterparty);
        assert_eq!(transfers[0].kind, "ReceiveBlind");
        assert_eq!(transfers[0].recipient_id.as_deref(), Some("rcpt9"));
        assert_eq!(transfers[0].expiration_timestamp, Some(123));
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    fn payment(hash: &str) -> serde_json::Value {
        json!({
            "amt_msat": 3000000,
            "asset_amount": 42,
            "asset_id": "rgb:asset",
            "payment_hash": hash,
            "payment_type": "Outbound",
            "status": "Pending",
            "created_at": 1,
            "updated_at": 2,
            "payee_pubkey": "02aa",
            "preimage": null,
            "description": null,
            "description_hash": null
        })
    }

    #[tokio::test]
    async fn list_payments_follows_query_pagination() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/listpayments"))
            .and(query_param("max_payments", "2"))
            .and(query_param_is_missing("index_offset"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "payments": [payment("aa"), payment("bb")],
                "first_index_offset": 9,
                "last_index_offset": 5
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/listpayments"))
            .and(query_param("max_payments", "2"))
            .and(query_param("index_offset", "5"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "payments": [payment("cc")],
                "first_index_offset": 2,
                "last_index_offset": 2
            })))
            .expect(1)
            .mount(&server)
            .await;

        let payments = client(&server, None).list_payments(2).await.unwrap();

        let hashes: Vec<&str> = payments.iter().map(|p| p.payment_hash.as_str()).collect();
        assert_eq!(hashes, vec!["aa", "bb", "cc"]);
        assert_eq!(payments[0].status, PaymentStatus::Pending);
        assert_eq!(
            payments[0].payment_type.direction(),
            PaymentDirection::Outbound
        );
        assert_eq!(payments[0].amt_msat, Some(3000000));
        assert_eq!(payments[0].asset_amount, Some(42));
        assert_eq!(payments[0].asset_id.as_deref(), Some("rgb:asset"));
        assert_eq!(payments[0].payee_pubkey, "02aa");
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn decode_ln_invoice_returns_payment_hash() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/decodelninvoice"))
            .and(body_json(json!({ "invoice": "lnbcrt1..." })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "amt_msat": 3000000,
                "expiry_sec": 420,
                "timestamp": 1,
                "asset_id": null,
                "asset_amount": null,
                "description": null,
                "description_hash": null,
                "payment_hash": "cafe",
                "payment_secret": "s",
                "payee_pubkey": "02aa",
                "min_final_cltv_expiry_delta": 40,
                "network": "Regtest"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let hash = client(&server, None)
            .decode_ln_invoice("lnbcrt1...")
            .await
            .unwrap();
        assert_eq!(hash, "cafe");
    }

    #[tokio::test]
    async fn list_assetless_transfers_sends_none_filter() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/listtransfers"))
            .and(body_json(json!({
                "asset_filter": {"type": "None"},
                "txid": null,
                "index_offset": null,
                "max_transfers": 10
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "transfers": [transfer(4)],
                "first_index_offset": 4,
                "last_index_offset": 4
            })))
            .expect(1)
            .mount(&server)
            .await;

        let transfers = client(&server, None)
            .list_assetless_transfers(10)
            .await
            .unwrap();

        assert_eq!(transfers.len(), 1);
        assert_eq!(transfers[0].idx, 4);
    }

    #[tokio::test]
    async fn list_assets_flattens_schemas() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/listassets"))
            .and(body_json(json!({ "filter_asset_schemas": [] })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "nia": [{ "asset_id": "rgb:nia", "ticker": "T", "name": "n" }],
                "uda": null,
                "cfa": [{ "asset_id": "rgb:cfa", "name": "c" }],
                "ifa": null
            })))
            .expect(1)
            .mount(&server)
            .await;

        let assets = client(&server, None).list_assets().await.unwrap();

        assert_eq!(
            assets,
            vec![
                ("rgb:nia".to_string(), "NIA".to_string()),
                ("rgb:cfa".to_string(), "CFA".to_string()),
            ]
        );
    }

    async fn mount_error(server: &MockServer, route: &str, status: u16, body: serde_json::Value) {
        Mock::given(path(route))
            .respond_with(ResponseTemplate::new(status).set_body_json(body))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn locked_and_changing_state_403_map_to_locked() {
        for name in ["LockedNode", "ChangingState"] {
            let server = MockServer::start().await;
            mount_error(
                &server,
                "/nodeinfo",
                403,
                json!({"error": "node busy", "code": 403, "name": name}),
            )
            .await;

            let err = client(&server, None).node_info().await.unwrap_err();
            assert!(matches!(err, RlnError::Locked), "{name}: {err:?}");
        }
    }

    #[tokio::test]
    async fn cannot_fail_403_maps() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/failtransfers"))
            .and(body_json(json!({
                "batch_transfer_idx": 7,
                "no_asset_only": false,
                "skip_sync": false
            })))
            .respond_with(ResponseTemplate::new(403).set_body_json(json!({
                "error": "Batch transfer cannot be set to failed status",
                "code": 403,
                "name": "CannotFailBatchTransfer"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let err = client(&server, None).fail_transfer(7).await.unwrap_err();
        assert!(matches!(err, RlnError::CannotFail), "{err:?}");
    }

    #[tokio::test]
    async fn other_error_maps_to_api() {
        let server = MockServer::start().await;
        mount_error(
            &server,
            "/nodeinfo",
            400,
            json!({
                "error": "Invalid request: bad",
                "code": 400,
                "name": "InvalidRequest"
            }),
        )
        .await;

        let err = client(&server, None).node_info().await.unwrap_err();
        match err {
            RlnError::Api {
                code,
                name,
                message,
            } => {
                assert_eq!(code, 400);
                assert_eq!(name, "InvalidRequest");
                assert_eq!(message, "Invalid request: bad");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn non_json_error_maps_to_api_unknown() {
        let server = MockServer::start().await;
        let body = "x".repeat(300);
        Mock::given(path("/nodeinfo"))
            .respond_with(ResponseTemplate::new(500).set_body_string(body))
            .mount(&server)
            .await;

        let err = client(&server, None).node_info().await.unwrap_err();
        match err {
            RlnError::Api {
                code,
                name,
                message,
            } => {
                assert_eq!(code, 500);
                assert_eq!(name, "Unknown");
                assert_eq!(message, "x".repeat(200));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn success_non_json_body_maps_to_decode() {
        let server = MockServer::start().await;
        Mock::given(path("/nodeinfo"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&server)
            .await;

        let err = client(&server, None).node_info().await.unwrap_err();
        assert!(matches!(err, RlnError::Decode(_)), "{err:?}");
    }

    #[tokio::test]
    async fn bearer_header_sent_when_configured() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nodeinfo"))
            .and(header("authorization", "Bearer tok"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "pubkey": "02ab" })))
            .expect(1)
            .mount(&server)
            .await;

        let info = client(&server, Some("tok")).node_info().await.unwrap();
        assert_eq!(info["pubkey"], "02ab");
    }

    #[tokio::test]
    async fn no_bearer_header_when_unconfigured() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nodeinfo"))
            .and(NoAuthHeader)
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;

        client(&server, None).node_info().await.unwrap();
    }

    #[tokio::test]
    async fn network_reads_networkinfo() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/networkinfo"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "network": "Regtest", "height": 12 })),
            )
            .expect(1)
            .mount(&server)
            .await;

        assert_eq!(client(&server, None).network().await.unwrap(), "Regtest");
    }

    #[tokio::test]
    async fn transport_error_maps() {
        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };
        let cfg = config::Rln {
            base_url: format!("http://127.0.0.1:{port}"),
            ..Default::default()
        };

        let err = HttpRlnClient::new(&cfg)
            .unwrap()
            .node_info()
            .await
            .unwrap_err();
        assert!(matches!(err, RlnError::Transport(_)), "{err:?}");
    }
}
