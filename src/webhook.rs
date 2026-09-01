use std::sync::Arc;
use std::time::Duration;

use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::config;
use crate::now;
use crate::store::{OutboxEvent, Store, StoreError};

// HMAC accepts keys of any length, so key init cannot fail.
#[allow(clippy::expect_used)]
pub fn signature(secret: &str, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac accepts any key length");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

pub fn next_attempt_delay(attempts_after: u32, cfg: &config::Webhook) -> u64 {
    let exp = attempts_after.saturating_sub(1);
    let factor = 1u64.checked_shl(exp).unwrap_or(u64::MAX);
    cfg.backoff_cap_secs
        .min(cfg.backoff_base_secs.saturating_mul(factor))
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct DispatchReport {
    pub delivered: usize,
    pub failed: usize,
}

pub struct Dispatcher {
    store: Store,
    client: reqwest::Client,
    cfg: config::Webhook,
}

impl Dispatcher {
    pub fn new(store: Store, cfg: config::Webhook) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self { store, client, cfg })
    }

    async fn post(&self, ev: &OutboxEvent) -> Result<(), String> {
        let res = self
            .client
            .post(&self.cfg.url)
            .header("content-type", "application/json")
            .header("x-companion-event-id", &ev.id)
            .header(
                "x-companion-signature",
                signature(&self.cfg.secret, ev.payload.as_bytes()),
            )
            .body(ev.payload.clone())
            .send()
            .await
            .map_err(|e| e.without_url().to_string())?;
        let status = res.status();
        if status.is_success() {
            Ok(())
        } else {
            Err(format!("status {status}"))
        }
    }

    pub async fn dispatch_due(&self, now: i64) -> Result<DispatchReport, StoreError> {
        let mut report = DispatchReport::default();
        for ev in self.store.undelivered_events(self.cfg.max_attempts).await? {
            if ev.next_attempt_at > now {
                break;
            }
            match self.post(&ev).await {
                Ok(()) => {
                    self.store.mark_delivered(&ev.id, now).await?;
                    report.delivered += 1;
                }
                Err(e) => {
                    let attempts = ev.attempts + 1;
                    let delay = next_attempt_delay(attempts, &self.cfg);
                    let next = now.saturating_add(i64::try_from(delay).unwrap_or(i64::MAX));
                    self.store.record_attempt(&ev.id, next).await?;
                    report.failed += 1;
                    warn!(
                        event_id = %ev.id,
                        event_type = %ev.event_type,
                        attempts,
                        max_attempts = self.cfg.max_attempts,
                        retry_in_secs = delay,
                        error = %e,
                        "webhook delivery failed"
                    );
                    break;
                }
            }
        }
        Ok(report)
    }

    pub async fn run(self: Arc<Self>) {
        let interval = Duration::from_secs(self.cfg.dispatch_interval_secs);
        loop {
            sleep(interval).await;
            match self.dispatch_due(now()).await {
                Ok(report) if report != DispatchReport::default() => {
                    debug!(?report, "webhook dispatch")
                }
                Ok(_) => {}
                Err(e) => warn!(error = %e, "webhook dispatch failed"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::store::{NewTransfer, OutboxEvent, Store, TransferStatus};

    const NOW: i64 = 1_000;

    fn hmac_hex(key: &str, msg: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).unwrap();
        mac.update(msg);
        hex::encode(mac.finalize().into_bytes())
    }

    fn cfg(url: String) -> config::Webhook {
        config::Webhook {
            url,
            secret: "secret".into(),
            max_attempts: 10,
            backoff_base_secs: 2,
            backoff_cap_secs: 300,
            dispatch_interval_secs: 1,
        }
    }

    async fn seed(store: &Store, event_id: &str, payload: &str) {
        let t = store
            .insert_transfer(
                &NewTransfer {
                    asset_id: None,
                    kind: None,
                    status: TransferStatus::Initiated,
                    recipient_id: None,
                    txid: None,
                    batch_transfer_idx: None,
                    invoice: None,
                    expiration_timestamp: None,
                },
                NOW,
            )
            .await
            .unwrap();
        assert!(store
            .apply_transition(
                &t.id,
                TransferStatus::Initiated,
                TransferStatus::Failed,
                Some((event_id, "transfer.failed", payload)),
                NOW,
            )
            .await
            .unwrap());
    }

    async fn harness(url: String) -> (Store, Dispatcher) {
        let store = Store::open_in_memory().await.unwrap();
        let d = Dispatcher::new(store.clone(), cfg(url)).unwrap();
        (store, d)
    }

    async fn undelivered(store: &Store) -> Vec<OutboxEvent> {
        store.undelivered_events(1000).await.unwrap()
    }

    fn received_ids(reqs: &[wiremock::Request]) -> Vec<String> {
        reqs.iter()
            .map(|r| {
                r.headers
                    .get("x-companion-event-id")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string()
            })
            .collect()
    }

    #[test]
    fn signature_matches_known_vector() {
        assert_eq!(
            signature("key", b"The quick brown fox jumps over the lazy dog"),
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn next_attempt_delay_schedule() {
        let c = cfg(String::new());
        assert_eq!(next_attempt_delay(1, &c), 2);
        assert_eq!(next_attempt_delay(2, &c), 4);
        assert_eq!(next_attempt_delay(10, &c), 300);
        assert_eq!(next_attempt_delay(200, &c), 300);
    }

    #[tokio::test]
    async fn delivers_due_event_with_headers_and_marks_delivered() {
        let server = MockServer::start().await;
        let payload = r#"{"event_id":"ev1","event_type":"transfer.failed","n":1}"#;
        Mock::given(method("POST"))
            .and(path("/hook"))
            .and(header("content-type", "application/json"))
            .and(header("x-companion-event-id", "ev1"))
            .and(header(
                "x-companion-signature",
                hmac_hex("secret", payload.as_bytes()).as_str(),
            ))
            .and(body_json(
                serde_json::from_str::<serde_json::Value>(payload).unwrap(),
            ))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let (store, d) = harness(format!("{}/hook", server.uri())).await;
        seed(&store, "ev1", payload).await;

        let report = d.dispatch_due(NOW).await.unwrap();

        assert_eq!(
            report,
            DispatchReport {
                delivered: 1,
                failed: 0
            }
        );
        assert!(undelivered(&store).await.is_empty());
    }

    #[tokio::test]
    async fn retries_with_backoff_on_5xx() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let (store, d) = harness(format!("{}/hook", server.uri())).await;
        seed(&store, "ev1", "{}").await;

        let report = d.dispatch_due(NOW).await.unwrap();
        assert_eq!(
            report,
            DispatchReport {
                delivered: 0,
                failed: 1
            }
        );
        let ev = undelivered(&store).await;
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].attempts, 1);
        assert_eq!(ev[0].next_attempt_at, NOW + 2);

        assert_eq!(
            d.dispatch_due(NOW + 1).await.unwrap(),
            DispatchReport::default()
        );
        let report = d.dispatch_due(NOW + 2).await.unwrap();
        assert_eq!(report.failed, 1);
        let ev = undelivered(&store).await;
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].attempts, 2);
        assert_eq!(ev[0].next_attempt_at, NOW + 2 + 4);
    }

    #[tokio::test]
    async fn parks_after_max_attempts() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let store = Store::open_in_memory().await.unwrap();
        let mut c = cfg(format!("{}/hook", server.uri()));
        c.max_attempts = 2;
        c.backoff_base_secs = 1;
        let d = Dispatcher::new(store.clone(), c).unwrap();
        seed(&store, "ev1", "{}").await;

        assert_eq!(d.dispatch_due(NOW).await.unwrap().failed, 1);
        assert_eq!(d.dispatch_due(NOW + 1).await.unwrap().failed, 1);
        assert_eq!(
            d.dispatch_due(NOW + 100).await.unwrap(),
            DispatchReport::default()
        );

        assert_eq!(server.received_requests().await.unwrap().len(), 2);
        assert!(store.undelivered_events(2).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn transport_error_counts_as_failed_attempt() {
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let (store, d) = harness(format!("http://127.0.0.1:{port}/hook")).await;
        seed(&store, "ev1", "{}").await;

        let report = d.dispatch_due(NOW).await.unwrap();

        assert_eq!(
            report,
            DispatchReport {
                delivered: 0,
                failed: 1
            }
        );
        let ev = undelivered(&store).await;
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].attempts, 1);
    }

    #[tokio::test]
    async fn stops_on_first_failure_to_keep_order() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let (store, d) = harness(format!("{}/hook", server.uri())).await;
        seed(&store, "ev1", "{}").await;
        seed(&store, "ev2", "{}").await;

        let report = d.dispatch_due(NOW).await.unwrap();

        assert_eq!(
            report,
            DispatchReport {
                delivered: 0,
                failed: 1
            }
        );
        let reqs = server.received_requests().await.unwrap();
        assert_eq!(received_ids(&reqs), vec!["ev1"]);
        let ev = undelivered(&store).await;
        assert_eq!(ev.len(), 2);
        assert_eq!(ev[0].id, "ev1");
        assert_eq!(ev[0].attempts, 1);
        assert_eq!(ev[1].id, "ev2");
        assert_eq!(ev[1].attempts, 0);
    }

    #[tokio::test]
    async fn retry_keeps_fifo_across_passes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let (store, d) = harness(format!("{}/hook", server.uri())).await;
        seed(&store, "ev1", "{}").await;
        seed(&store, "ev2", "{}").await;

        assert_eq!(d.dispatch_due(NOW).await.unwrap().failed, 1);
        assert_eq!(
            d.dispatch_due(NOW + 1).await.unwrap(),
            DispatchReport::default()
        );
        assert_eq!(
            d.dispatch_due(NOW + 2).await.unwrap(),
            DispatchReport {
                delivered: 2,
                failed: 0
            }
        );

        let reqs = server.received_requests().await.unwrap();
        assert_eq!(received_ids(&reqs), vec!["ev1", "ev1", "ev2"]);
        assert!(undelivered(&store).await.is_empty());
    }

    #[tokio::test]
    async fn redirect_status_is_failure() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(302).insert_header("location", "/elsewhere"))
            .mount(&server)
            .await;
        let (store, d) = harness(format!("{}/hook", server.uri())).await;
        seed(&store, "ev1", "{}").await;

        let report = d.dispatch_due(NOW).await.unwrap();

        assert_eq!(
            report,
            DispatchReport {
                delivered: 0,
                failed: 1
            }
        );
        let ev = undelivered(&store).await;
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].attempts, 1);
        assert_eq!(server.received_requests().await.unwrap().len(), 1);
    }
}
