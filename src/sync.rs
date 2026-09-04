use std::collections::BTreeSet;

use serde_json::json;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config;
use crate::rln::{RlnApi, RlnError, RlnTransfer};
use crate::store::{
    NewPayment, Observed, Payment, PaymentStatus, Store, StoreError, Transfer, TransferStatus,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    ConfirmedPending,
    Settled,
    Failed,
}

impl EventType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ConfirmedPending => "transfer.confirmed_pending",
            Self::Settled => "transfer.settled",
            Self::Failed => "transfer.failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition {
    pub transfer_id: String,
    pub from: TransferStatus,
    pub to: TransferStatus,
    pub event: Option<EventType>,
}

fn rank(s: TransferStatus) -> u8 {
    match s {
        TransferStatus::Initiated => 0,
        TransferStatus::WaitingCounterparty => 1,
        TransferStatus::WaitingSafeHeight => 2,
        TransferStatus::WaitingBroadcast => 3,
        TransferStatus::WaitingConfirmations => 4,
        TransferStatus::Settled | TransferStatus::Failed => 5,
    }
}

pub fn plan_transition(stored: &Transfer, observed: TransferStatus) -> Option<Transition> {
    let from = stored.status;
    if from.terminal() || rank(observed) <= rank(from) {
        return None;
    }
    let event = match observed {
        TransferStatus::WaitingConfirmations => Some(EventType::ConfirmedPending),
        TransferStatus::Settled => Some(EventType::Settled),
        TransferStatus::Failed => Some(EventType::Failed),
        _ => None,
    };
    Some(Transition {
        transfer_id: stored.id.clone(),
        from,
        to: observed,
        event,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymentEventType {
    Settled,
    Failed,
}

impl PaymentEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Settled => "payment.settled",
            Self::Failed => "payment.failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymentTransition {
    pub payment_hash: String,
    pub from: PaymentStatus,
    pub to: PaymentStatus,
    pub event: Option<PaymentEventType>,
}

fn payment_rank(s: PaymentStatus) -> u8 {
    match s {
        PaymentStatus::Pending => 0,
        PaymentStatus::Claimable => 1,
        PaymentStatus::Claiming => 2,
        PaymentStatus::Succeeded | PaymentStatus::Failed | PaymentStatus::Cancelled => 3,
    }
}

pub fn plan_payment_transition(
    stored: &Payment,
    observed: PaymentStatus,
) -> Option<PaymentTransition> {
    let from = stored.status;
    if from.terminal() {
        // RLN reuses the hash of a failed/cancelled payment on retry: restart silently.
        // A Succeeded hash is never reset, so it stays final.
        if observed != PaymentStatus::Pending
            || !matches!(from, PaymentStatus::Failed | PaymentStatus::Cancelled)
        {
            return None;
        }
        return Some(PaymentTransition {
            payment_hash: stored.payment_hash.clone(),
            from,
            to: observed,
            event: None,
        });
    }
    if payment_rank(observed) <= payment_rank(from) {
        return None;
    }
    let event = match observed {
        PaymentStatus::Succeeded => Some(PaymentEventType::Settled),
        PaymentStatus::Failed | PaymentStatus::Cancelled => Some(PaymentEventType::Failed),
        _ => None,
    };
    Some(PaymentTransition {
        payment_hash: stored.payment_hash.clone(),
        from,
        to: observed,
        event,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    Full,
    Assets(Vec<String>),
    Pending,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SyncReport {
    pub assets: usize,
    pub transfers: usize,
    pub transitions: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error(transparent)]
    Rln(#[from] RlnError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("payload: {0}")]
    Payload(#[from] serde_json::Error),
}

const FULL_MIN_PAGE_SIZE: u64 = 1000;

pub async fn sync<R: RlnApi + ?Sized>(
    rln: &R,
    store: &Store,
    cfg: &config::Sync,
    scope: Scope,
    now: i64,
) -> Result<SyncReport, SyncError> {
    let page_size = match scope {
        Scope::Full => cfg.page_size.max(FULL_MIN_PAGE_SIZE),
        _ => cfg.page_size,
    };
    let (asset_ids, assetless): (BTreeSet<String>, bool) = match scope {
        Scope::Full => (
            fetch_assets(rln, store, now).await?.into_iter().collect(),
            true,
        ),
        Scope::Assets(ids) => (ids.into_iter().collect(), false),
        Scope::Pending => {
            let (mut ids, agnostic) = store.pending_asset_ids().await?;
            if agnostic {
                ids.extend(fetch_assets(rln, store, now).await?);
            }
            (ids.into_iter().collect(), agnostic)
        }
    };
    let mut report = SyncReport {
        assets: asset_ids.len(),
        ..SyncReport::default()
    };
    if assetless {
        let transfers = rln
            .list_assetless_transfers(page_size)
            .await
            .inspect_err(|e| warn!(error = %e, "list_assetless_transfers failed"))?;
        for t in transfers {
            observe(store, &mut report, None, t, now).await?;
        }
    }
    for asset_id in &asset_ids {
        let transfers = rln
            .list_transfers(asset_id, page_size)
            .await
            .inspect_err(|e| warn!(asset_id = %asset_id, error = %e, "list_transfers failed"))?;
        for t in transfers {
            observe(store, &mut report, Some(asset_id.clone()), t, now).await?;
        }
    }
    debug!(?report, "sync done");
    Ok(report)
}

async fn observe(
    store: &Store,
    report: &mut SyncReport,
    asset_id: Option<String>,
    t: RlnTransfer,
    now: i64,
) -> Result<(), SyncError> {
    let observed = Observed {
        rln_idx: t.idx,
        asset_id,
        kind: t.kind,
        status: t.status,
        recipient_id: t.recipient_id,
        txid: t.txid,
        expiration_timestamp: t.expiration_timestamp,
    };
    let row = store.upsert_observed(&observed, now).await?;
    report.transfers += 1;
    if apply_observed(store, &row, t.status, now).await? {
        report.transitions += 1;
    }
    Ok(())
}

pub async fn apply_observed(
    store: &Store,
    row: &Transfer,
    observed: TransferStatus,
    now: i64,
) -> Result<bool, SyncError> {
    let Some(tr) = plan_transition(row, observed) else {
        return Ok(false);
    };
    let event_id = Uuid::new_v4().to_string();
    let payload = tr
        .event
        .map(|e| event_payload(&event_id, e, row.clone(), &tr, now))
        .transpose()?;
    let event = tr
        .event
        .zip(payload.as_deref())
        .map(|(e, p)| (event_id.as_str(), e.as_str(), p));
    let applied = store
        .apply_transition(&tr.transfer_id, tr.from, tr.to, event, now)
        .await?;
    if applied {
        info!(
            transfer_id = %tr.transfer_id,
            from = ?tr.from,
            to = ?tr.to,
            event = ?tr.event,
            "transfer transition"
        );
    }
    Ok(applied)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PaymentSyncReport {
    pub payments: usize,
    pub transitions: usize,
}

pub async fn sync_payments<R: RlnApi + ?Sized>(
    rln: &R,
    store: &Store,
    cfg: &config::Sync,
    now: i64,
) -> Result<PaymentSyncReport, SyncError> {
    let payments = rln
        .list_payments(cfg.page_size)
        .await
        .inspect_err(|e| warn!(error = %e, "list_payments failed"))?;
    let baseline = store.payments_baseline_at().await?;
    let mut report = PaymentSyncReport::default();
    for p in payments {
        // Before the baseline everything backfills silently; after it, a payment first
        // seen already terminal inserts as Pending so the terminal transition emits.
        let insert_status = if baseline.is_some() && p.status.terminal() {
            PaymentStatus::Pending
        } else {
            p.status
        };
        let observed = NewPayment {
            payment_hash: p.payment_hash,
            direction: p.payment_type.direction(),
            status: insert_status,
            asset_id: p.asset_id,
            asset_amount: p.asset_amount,
            amt_msat: p.amt_msat,
            payee_pubkey: Some(p.payee_pubkey),
        };
        let row = store.upsert_payment_observed(&observed, now).await?;
        report.payments += 1;
        if apply_payment_observed(store, &row, p.status, now).await? {
            report.transitions += 1;
        }
    }
    if baseline.is_none() {
        store.set_payments_baseline_at(now).await?;
    }
    debug!(?report, "payments sync done");
    Ok(report)
}

pub async fn apply_payment_observed(
    store: &Store,
    row: &Payment,
    observed: PaymentStatus,
    now: i64,
) -> Result<bool, SyncError> {
    let Some(tr) = plan_payment_transition(row, observed) else {
        return Ok(false);
    };
    let event_id = Uuid::new_v4().to_string();
    let payload = tr
        .event
        .map(|e| payment_event_payload(&event_id, e, row.clone(), &tr, now))
        .transpose()?;
    let event = tr
        .event
        .zip(payload.as_deref())
        .map(|(e, p)| (event_id.as_str(), e.as_str(), p));
    let applied = store
        .apply_payment_transition(&tr.payment_hash, tr.from, tr.to, event, now)
        .await?;
    if applied {
        info!(
            payment_hash = %tr.payment_hash,
            from = ?tr.from,
            to = ?tr.to,
            event = ?tr.event,
            "payment transition"
        );
    }
    Ok(applied)
}

fn payment_event_payload(
    event_id: &str,
    event: PaymentEventType,
    mut row: Payment,
    tr: &PaymentTransition,
    now: i64,
) -> Result<String, serde_json::Error> {
    row.status = tr.to;
    row.updated_at = now;
    serde_json::to_string(&json!({
        "event_id": event_id,
        "event_type": event.as_str(),
        "payment": row,
        "previous_status": tr.from,
        "new_status": tr.to,
        "timestamp": now,
    }))
}

async fn fetch_assets<R: RlnApi + ?Sized>(
    rln: &R,
    store: &Store,
    now: i64,
) -> Result<Vec<String>, SyncError> {
    let assets = rln.list_assets().await?;
    store.upsert_assets(&assets, now).await?;
    Ok(assets.into_iter().map(|(id, _)| id).collect())
}

fn event_payload(
    event_id: &str,
    event: EventType,
    mut row: Transfer,
    tr: &Transition,
    now: i64,
) -> Result<String, serde_json::Error> {
    row.status = tr.to;
    row.updated_at = now;
    if tr.to == TransferStatus::Settled {
        row.settled_at = Some(now);
    }
    serde_json::to_string(&json!({
        "event_id": event_id,
        "event_type": event.as_str(),
        "transfer": row,
        "previous_status": tr.from,
        "new_status": tr.to,
        "timestamp": now,
    }))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use TransferStatus::*;

    fn stored(status: TransferStatus) -> Transfer {
        Transfer {
            id: "t1".into(),
            rln_idx: Some(1),
            asset_id: Some("assetA".into()),
            kind: Some("Send".into()),
            status,
            recipient_id: None,
            txid: None,
            batch_transfer_idx: None,
            invoice: None,
            expiration_timestamp: None,
            created_at: 1,
            updated_at: 1,
            last_seen_at: None,
            settled_at: None,
        }
    }

    fn plan(from: TransferStatus, to: TransferStatus) -> Option<Transition> {
        plan_transition(&stored(from), to)
    }

    #[test]
    fn forward_moves_plan_transition_with_event_for_stage_reached() {
        for (from, to, event) in [
            (Initiated, WaitingCounterparty, None),
            (WaitingCounterparty, WaitingSafeHeight, None),
            (WaitingCounterparty, WaitingBroadcast, None),
            (
                WaitingBroadcast,
                WaitingConfirmations,
                Some(EventType::ConfirmedPending),
            ),
            (
                WaitingCounterparty,
                WaitingConfirmations,
                Some(EventType::ConfirmedPending),
            ),
            (
                WaitingSafeHeight,
                WaitingConfirmations,
                Some(EventType::ConfirmedPending),
            ),
            (WaitingConfirmations, Settled, Some(EventType::Settled)),
            (WaitingCounterparty, Settled, Some(EventType::Settled)),
            (WaitingCounterparty, Failed, Some(EventType::Failed)),
        ] {
            assert_eq!(
                plan(from, to),
                Some(Transition {
                    transfer_id: "t1".into(),
                    from,
                    to,
                    event
                }),
                "{from:?} -> {to:?}"
            );
        }
    }

    #[test]
    fn same_backwards_or_terminal_is_noop() {
        for (from, to) in [
            (WaitingCounterparty, WaitingCounterparty),
            (WaitingConfirmations, WaitingBroadcast),
            (WaitingConfirmations, WaitingCounterparty),
            (Failed, Settled),
        ] {
            assert_eq!(plan(from, to), None, "{from:?} -> {to:?}");
        }
        for observed in [
            Initiated,
            WaitingCounterparty,
            WaitingSafeHeight,
            WaitingBroadcast,
            WaitingConfirmations,
            Settled,
            Failed,
        ] {
            assert_eq!(plan(Settled, observed), None, "{observed:?}");
        }
    }

    mod payments {
        use super::*;
        use crate::rln::test_support::{MockFailure, MockRln};
        use crate::rln::RlnPaymentType;
        use crate::store::{NewPayment, PaymentDirection, PaymentStatus};

        fn cfg() -> config::Sync {
            config::Sync {
                full_interval_secs: 600,
                page_size: 100,
            }
        }

        async fn store() -> Store {
            Store::open_in_memory().await.unwrap()
        }

        fn stored_payment(status: PaymentStatus) -> Payment {
            Payment {
                payment_hash: "h1".into(),
                direction: PaymentDirection::Inbound,
                status,
                asset_id: None,
                asset_amount: None,
                amt_msat: None,
                payee_pubkey: None,
                created_at: 1,
                updated_at: 1,
                last_seen_at: None,
            }
        }

        fn plan(from: PaymentStatus, to: PaymentStatus) -> Option<PaymentTransition> {
            plan_payment_transition(&stored_payment(from), to)
        }

        #[test]
        fn payment_plan_moves_forward_with_event_on_terminal() {
            use PaymentStatus::*;
            for (from, to, event) in [
                (Pending, Claimable, None),
                (Pending, Claiming, None),
                (Claimable, Claiming, None),
                (Pending, Succeeded, Some(PaymentEventType::Settled)),
                (Claiming, Succeeded, Some(PaymentEventType::Settled)),
                (Pending, Failed, Some(PaymentEventType::Failed)),
                (Claimable, Cancelled, Some(PaymentEventType::Failed)),
                (Failed, Pending, None),
                (Cancelled, Pending, None),
            ] {
                assert_eq!(
                    plan(from, to),
                    Some(PaymentTransition {
                        payment_hash: "h1".into(),
                        from,
                        to,
                        event
                    }),
                    "{from:?} -> {to:?}"
                );
            }
            for (from, to) in [
                (Pending, Pending),
                (Claiming, Claimable),
                (Claiming, Pending),
                (Succeeded, Failed),
                (Succeeded, Pending),
                (Failed, Succeeded),
                (Failed, Failed),
                (Failed, Claiming),
                (Cancelled, Claimable),
            ] {
                assert_eq!(plan(from, to), None, "{from:?} -> {to:?}");
            }
        }

        async fn seed_pending(store: &Store, hash: &str) {
            store
                .insert_pending_payment(
                    &NewPayment {
                        payment_hash: hash.into(),
                        direction: PaymentDirection::Outbound,
                        status: PaymentStatus::Pending,
                        asset_id: None,
                        asset_amount: None,
                        amt_msat: Some(1000),
                        payee_pubkey: None,
                    },
                    1,
                )
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn settle_emits_payment_settled_with_payload() {
            let rln = MockRln::default();
            rln.set_payments(vec![MockRln::payment(
                "h1",
                PaymentStatus::Succeeded,
                RlnPaymentType::Outbound,
            )]);
            let store = store().await;
            seed_pending(&store, "h1").await;

            let report = sync_payments(&rln, &store, &cfg(), 42).await.unwrap();

            assert_eq!(
                report,
                PaymentSyncReport {
                    payments: 1,
                    transitions: 1
                }
            );
            assert_eq!(rln.calls(), vec!["list_payments"]);
            let row = store.get_payment("h1").await.unwrap().unwrap();
            assert_eq!(row.status, PaymentStatus::Succeeded);
            assert_eq!(row.updated_at, 42);
            assert_eq!(row.last_seen_at, Some(42));
            assert_eq!(row.asset_id.as_deref(), Some("rgb:asset"));
            let events = store.undelivered_events(10).await.unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event_type, "payment.settled");
            let payload: serde_json::Value = serde_json::from_str(&events[0].payload).unwrap();
            assert_eq!(payload["event_id"], events[0].id);
            assert_eq!(payload["event_type"], "payment.settled");
            assert_eq!(payload["previous_status"], "Pending");
            assert_eq!(payload["new_status"], "Succeeded");
            assert_eq!(payload["timestamp"], 42);
            assert_eq!(payload["payment"]["payment_hash"], "h1");
            assert_eq!(payload["payment"]["status"], "Succeeded");
            assert_eq!(payload["payment"]["direction"], "Outbound");
            assert_eq!(payload["payment"]["asset_id"], "rgb:asset");
            assert_eq!(payload["payment"]["asset_amount"], 42);
            assert_eq!(payload["payment"]["amt_msat"], 3000000);
            assert_eq!(payload["payment"]["payee_pubkey"], "02aa");
            assert_eq!(payload["payment"]["updated_at"], 42);
        }

        #[tokio::test]
        async fn second_run_is_idempotent() {
            let rln = MockRln::default();
            rln.set_payments(vec![MockRln::payment(
                "h1",
                PaymentStatus::Succeeded,
                RlnPaymentType::Outbound,
            )]);
            let store = store().await;
            seed_pending(&store, "h1").await;

            sync_payments(&rln, &store, &cfg(), 2).await.unwrap();
            let report = sync_payments(&rln, &store, &cfg(), 3).await.unwrap();

            assert_eq!(report.transitions, 0);
            assert_eq!(store.undelivered_events(10).await.unwrap().len(), 1);
            assert_eq!(store.list_payments(None, 10).await.unwrap().len(), 1);
        }

        #[tokio::test]
        async fn failed_and_cancelled_emit_payment_failed() {
            for observed in [PaymentStatus::Failed, PaymentStatus::Cancelled] {
                let rln = MockRln::default();
                rln.set_payments(vec![MockRln::payment(
                    "h1",
                    observed,
                    RlnPaymentType::InboundAutoClaim,
                )]);
                let store = store().await;
                seed_pending(&store, "h1").await;

                let report = sync_payments(&rln, &store, &cfg(), 5).await.unwrap();

                assert_eq!(report.transitions, 1, "{observed:?}");
                let row = store.get_payment("h1").await.unwrap().unwrap();
                assert_eq!(row.status, observed);
                let events = store.undelivered_events(10).await.unwrap();
                assert_eq!(events.len(), 1, "{observed:?}");
                assert_eq!(events[0].event_type, "payment.failed");
                let payload: serde_json::Value = serde_json::from_str(&events[0].payload).unwrap();
                assert_eq!(payload["event_type"], "payment.failed");
                assert_eq!(payload["payment"]["direction"], "Inbound");
            }
        }

        #[tokio::test]
        async fn pending_and_claim_stages_stay_silent() {
            let rln = MockRln::default();
            rln.set_payments(vec![MockRln::payment(
                "h1",
                PaymentStatus::Pending,
                RlnPaymentType::InboundHodl,
            )]);
            let store = store().await;

            let report = sync_payments(&rln, &store, &cfg(), 2).await.unwrap();

            assert_eq!(
                report,
                PaymentSyncReport {
                    payments: 1,
                    transitions: 0
                }
            );
            let row = store.get_payment("h1").await.unwrap().unwrap();
            assert_eq!(row.status, PaymentStatus::Pending);
            assert_eq!(row.direction, PaymentDirection::Inbound);
            assert!(store.undelivered_events(10).await.unwrap().is_empty());

            rln.set_payments(vec![MockRln::payment(
                "h1",
                PaymentStatus::Claimable,
                RlnPaymentType::InboundHodl,
            )]);
            let report = sync_payments(&rln, &store, &cfg(), 3).await.unwrap();

            assert_eq!(report.transitions, 1);
            let row = store.get_payment("h1").await.unwrap().unwrap();
            assert_eq!(row.status, PaymentStatus::Claimable);
            assert!(store.undelivered_events(10).await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn first_sync_backfills_terminal_silently_and_sets_baseline() {
            let rln = MockRln::default();
            rln.set_payments(vec![MockRln::payment(
                "h1",
                PaymentStatus::Succeeded,
                RlnPaymentType::Outbound,
            )]);
            let store = store().await;
            assert_eq!(store.payments_baseline_at().await.unwrap(), None);

            let report = sync_payments(&rln, &store, &cfg(), 2).await.unwrap();

            assert_eq!(report.transitions, 0);
            let row = store.get_payment("h1").await.unwrap().unwrap();
            assert_eq!(row.status, PaymentStatus::Succeeded);
            assert!(store.undelivered_events(10).await.unwrap().is_empty());
            assert_eq!(store.payments_baseline_at().await.unwrap(), Some(2));
        }

        #[tokio::test]
        async fn terminal_discovered_after_baseline_emits() {
            let rln = MockRln::default();
            let store = store().await;
            sync_payments(&rln, &store, &cfg(), 1).await.unwrap();
            assert_eq!(store.payments_baseline_at().await.unwrap(), Some(1));

            rln.set_payments(vec![MockRln::payment(
                "h1",
                PaymentStatus::Succeeded,
                RlnPaymentType::InboundAutoClaim,
            )]);
            let report = sync_payments(&rln, &store, &cfg(), 5).await.unwrap();

            assert_eq!(report.transitions, 1);
            let row = store.get_payment("h1").await.unwrap().unwrap();
            assert_eq!(row.status, PaymentStatus::Succeeded);
            assert_eq!(row.created_at, 5);
            let events = store.undelivered_events(10).await.unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event_type, "payment.settled");
            let payload: serde_json::Value = serde_json::from_str(&events[0].payload).unwrap();
            assert_eq!(payload["previous_status"], "Pending");
            assert_eq!(payload["new_status"], "Succeeded");
            assert_eq!(payload["payment"]["direction"], "Inbound");
            assert_eq!(store.payments_baseline_at().await.unwrap(), Some(1));
        }

        #[tokio::test]
        async fn failed_retry_restarts_silently_then_settles() {
            let rln = MockRln::default();
            rln.set_payments(vec![MockRln::payment(
                "h1",
                PaymentStatus::Failed,
                RlnPaymentType::Outbound,
            )]);
            let store = store().await;
            sync_payments(&rln, &store, &cfg(), 1).await.unwrap();
            assert!(store.undelivered_events(10).await.unwrap().is_empty());

            rln.set_payments(vec![MockRln::payment(
                "h1",
                PaymentStatus::Pending,
                RlnPaymentType::Outbound,
            )]);
            let report = sync_payments(&rln, &store, &cfg(), 2).await.unwrap();

            assert_eq!(report.transitions, 1);
            let row = store.get_payment("h1").await.unwrap().unwrap();
            assert_eq!(row.status, PaymentStatus::Pending);
            assert!(store.undelivered_events(10).await.unwrap().is_empty());

            rln.set_payments(vec![MockRln::payment(
                "h1",
                PaymentStatus::Succeeded,
                RlnPaymentType::Outbound,
            )]);
            let report = sync_payments(&rln, &store, &cfg(), 3).await.unwrap();

            assert_eq!(report.transitions, 1);
            let row = store.get_payment("h1").await.unwrap().unwrap();
            assert_eq!(row.status, PaymentStatus::Succeeded);
            let events = store.undelivered_events(10).await.unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event_type, "payment.settled");
            let payload: serde_json::Value = serde_json::from_str(&events[0].payload).unwrap();
            assert_eq!(payload["previous_status"], "Pending");
            assert_eq!(payload["new_status"], "Succeeded");
        }

        #[tokio::test]
        async fn locked_propagates() {
            let rln = MockRln::default();
            *rln.fail_with.lock().unwrap() = Some(MockFailure::Locked);
            let store = store().await;

            let err = sync_payments(&rln, &store, &cfg(), 2).await.unwrap_err();

            assert!(matches!(err, SyncError::Rln(RlnError::Locked)), "{err:?}");
            assert!(store.list_payments(None, 10).await.unwrap().is_empty());
            assert_eq!(store.payments_baseline_at().await.unwrap(), None);
        }
    }

    mod sync {
        use super::*;
        use crate::rln::test_support::{MockFailure, MockRln};
        use crate::store::{NewTransfer, NodeState};

        fn cfg() -> config::Sync {
            config::Sync {
                full_interval_secs: 600,
                page_size: 100,
            }
        }

        fn new_transfer(
            status: TransferStatus,
            asset_id: Option<&str>,
            recipient_id: Option<&str>,
        ) -> NewTransfer {
            NewTransfer {
                asset_id: asset_id.map(str::to_string),
                recipient_id: recipient_id.map(str::to_string),
                ..NewTransfer::with_status(status)
            }
        }

        async fn store() -> Store {
            Store::open_in_memory().await.unwrap()
        }

        #[tokio::test]
        async fn full_sync_upserts_assets_and_transfers() {
            let rln = MockRln::default();
            rln.add_asset(
                "A",
                "NIA",
                vec![
                    MockRln::transfer(1, WaitingCounterparty, Some("r1")),
                    MockRln::transfer(2, Settled, None),
                ],
            );
            rln.add_asset("B", "CFA", vec![MockRln::transfer(3, Settled, None)]);
            let store = store().await;

            let report = sync(&rln, &store, &cfg(), Scope::Full, 10).await.unwrap();

            assert_eq!(
                report,
                SyncReport {
                    assets: 2,
                    transfers: 3,
                    transitions: 0
                }
            );
            assert_eq!(
                store.list_assets().await.unwrap(),
                vec![
                    ("A".to_string(), "NIA".to_string()),
                    ("B".to_string(), "CFA".to_string())
                ]
            );
            let mut rows = store.list_transfers(None, None, 10).await.unwrap();
            rows.sort_by_key(|t| t.rln_idx);
            let summary: Vec<_> = rows
                .iter()
                .map(|t| (t.rln_idx, t.asset_id.clone(), t.status, t.last_seen_at))
                .collect();
            assert_eq!(
                summary,
                vec![
                    (Some(1), Some("A".into()), WaitingCounterparty, Some(10)),
                    (Some(2), Some("A".into()), Settled, Some(10)),
                    (Some(3), Some("B".into()), Settled, Some(10)),
                ]
            );
            assert_eq!(
                rln.calls(),
                vec![
                    "list_assets",
                    "list_assetless_transfers",
                    "list_transfers:A",
                    "list_transfers:B"
                ]
            );
            assert!(store.undelivered_events(10).await.unwrap().is_empty());
            assert_eq!(store.node_state().await.unwrap(), NodeState::Unknown);
        }

        #[tokio::test]
        async fn full_sync_picks_up_assetless_pending_then_settles_by_asset() {
            let rln = MockRln::default();
            rln.set_assetless(vec![MockRln::transfer(5, WaitingCounterparty, Some("r"))]);
            let store = store().await;

            let report = sync(&rln, &store, &cfg(), Scope::Full, 1).await.unwrap();

            assert_eq!(
                report,
                SyncReport {
                    assets: 0,
                    transfers: 1,
                    transitions: 0
                }
            );
            let rows = store.list_transfers(None, None, 10).await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].asset_id, None);
            assert_eq!(rows[0].rln_idx, Some(5));
            assert_eq!(rows[0].status, WaitingCounterparty);
            assert_eq!(rln.calls(), vec!["list_assets", "list_assetless_transfers"]);

            rln.set_assetless(vec![]);
            rln.add_asset("A", "NIA", vec![MockRln::transfer(5, Settled, Some("r"))]);
            let report = sync(&rln, &store, &cfg(), Scope::Full, 2).await.unwrap();

            assert_eq!(report.transitions, 1);
            let rows = store.list_transfers(None, None, 10).await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].asset_id.as_deref(), Some("A"));
            assert_eq!(rows[0].status, Settled);
            let events = store.undelivered_events(10).await.unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event_type, "transfer.settled");
        }

        #[tokio::test]
        async fn transition_without_event_still_counts() {
            let rln = MockRln::default();
            rln.add_asset(
                "A",
                "NIA",
                vec![MockRln::transfer(5, WaitingSafeHeight, Some("r"))],
            );
            let store = store().await;
            let inserted = store
                .insert_transfer(&new_transfer(WaitingCounterparty, None, Some("r")), 1)
                .await
                .unwrap();

            let report = sync(&rln, &store, &cfg(), Scope::Full, 2).await.unwrap();

            assert_eq!(report.transitions, 1);
            let row = store.get_transfer(&inserted.id).await.unwrap().unwrap();
            assert_eq!(row.status, WaitingSafeHeight);
            assert!(store.undelivered_events(10).await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn failure_on_second_asset_keeps_first_assets_rows() {
            let rln = MockRln::default();
            rln.add_asset("A", "NIA", vec![MockRln::transfer(1, Settled, None)]);
            rln.add_asset("B", "CFA", vec![MockRln::transfer(2, Settled, None)]);
            *rln.fail_call.lock().unwrap() =
                Some(("list_transfers:B".into(), MockFailure::Transport));
            let store = store().await;

            let err = sync(&rln, &store, &cfg(), Scope::Full, 2)
                .await
                .unwrap_err();

            assert!(
                matches!(err, SyncError::Rln(RlnError::Transport(_))),
                "{err:?}"
            );
            let rows = store.list_transfers(None, None, 10).await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].rln_idx, Some(1));
            assert_eq!(rows[0].asset_id.as_deref(), Some("A"));
            assert_eq!(
                rln.calls(),
                vec![
                    "list_assets",
                    "list_assetless_transfers",
                    "list_transfers:A",
                    "list_transfers:B"
                ]
            );
        }

        #[tokio::test]
        async fn asset_scope_dedups_and_sorts_ids() {
            let rln = MockRln::default();
            rln.add_asset("A", "NIA", vec![]);
            rln.add_asset("B", "CFA", vec![]);
            let store = store().await;

            let scope = Scope::Assets(vec!["B".into(), "A".into(), "A".into()]);
            let report = sync(&rln, &store, &cfg(), scope, 2).await.unwrap();

            assert_eq!(report.assets, 2);
            assert_eq!(rln.calls(), vec!["list_transfers:A", "list_transfers:B"]);
        }

        #[tokio::test]
        async fn full_sync_merges_intercepted_invoice_row() {
            let rln = MockRln::default();
            rln.add_asset(
                "A",
                "NIA",
                vec![MockRln::transfer(5, WaitingCounterparty, Some("r"))],
            );
            let store = store().await;
            let inserted = store
                .insert_transfer(&new_transfer(WaitingCounterparty, None, Some("r")), 1)
                .await
                .unwrap();

            let report = sync(&rln, &store, &cfg(), Scope::Full, 2).await.unwrap();

            assert_eq!(report.transfers, 1);
            assert_eq!(report.transitions, 0);
            let rows = store.list_transfers(None, None, 10).await.unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].id, inserted.id);
            assert_eq!(rows[0].rln_idx, Some(5));
            assert_eq!(rows[0].asset_id.as_deref(), Some("A"));
            assert_eq!(rows[0].status, WaitingCounterparty);
        }

        #[tokio::test]
        async fn sync_applies_transitions_and_enqueues_events() {
            let rln = MockRln::default();
            rln.add_asset("A", "NIA", vec![MockRln::transfer(5, Settled, Some("r"))]);
            let store = store().await;
            let inserted = store
                .insert_transfer(&new_transfer(WaitingCounterparty, None, Some("r")), 1)
                .await
                .unwrap();

            let report = sync(&rln, &store, &cfg(), Scope::Full, 42).await.unwrap();

            assert_eq!(report.transitions, 1);
            let row = store.get_transfer(&inserted.id).await.unwrap().unwrap();
            assert_eq!(row.status, Settled);
            assert_eq!(row.settled_at, Some(42));
            let events = store.undelivered_events(10).await.unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event_type, "transfer.settled");
            let payload: serde_json::Value = serde_json::from_str(&events[0].payload).unwrap();
            assert_eq!(payload["event_id"], events[0].id);
            assert_eq!(payload["event_type"], "transfer.settled");
            assert_eq!(payload["previous_status"], "WaitingCounterparty");
            assert_eq!(payload["new_status"], "Settled");
            assert_eq!(payload["timestamp"], 42);
            assert_eq!(payload["transfer"]["id"], inserted.id);
            assert_eq!(payload["transfer"]["status"], "Settled");
            assert_eq!(payload["transfer"]["rln_idx"], 5);
        }

        #[tokio::test]
        async fn sync_is_idempotent() {
            let rln = MockRln::default();
            rln.add_asset("A", "NIA", vec![MockRln::transfer(5, Settled, Some("r"))]);
            let store = store().await;
            store
                .insert_transfer(&new_transfer(WaitingCounterparty, None, Some("r")), 1)
                .await
                .unwrap();

            sync(&rln, &store, &cfg(), Scope::Full, 2).await.unwrap();
            let report = sync(&rln, &store, &cfg(), Scope::Full, 3).await.unwrap();

            assert_eq!(report.transitions, 0);
            assert_eq!(store.undelivered_events(10).await.unwrap().len(), 1);
            assert_eq!(store.list_transfers(None, None, 10).await.unwrap().len(), 1);
        }

        #[tokio::test]
        async fn pending_scope_only_queries_pending_assets() {
            let rln = MockRln::default();
            rln.add_asset(
                "A",
                "NIA",
                vec![MockRln::transfer(1, WaitingCounterparty, None)],
            );
            rln.add_asset("B", "CFA", vec![]);
            let store = store().await;
            store
                .upsert_assets(&[("A".into(), "NIA".into()), ("B".into(), "CFA".into())], 1)
                .await
                .unwrap();
            store
                .insert_transfer(&new_transfer(WaitingCounterparty, Some("A"), None), 1)
                .await
                .unwrap();
            store
                .insert_transfer(&new_transfer(Settled, Some("B"), None), 1)
                .await
                .unwrap();

            let report = sync(&rln, &store, &cfg(), Scope::Pending, 2).await.unwrap();

            assert_eq!(report.assets, 1);
            assert_eq!(rln.calls(), vec!["list_transfers:A"]);
        }

        #[tokio::test]
        async fn pending_scope_lists_assets_when_agnostic_rows_exist() {
            let rln = MockRln::default();
            rln.add_asset("A", "NIA", vec![]);
            rln.add_asset("B", "CFA", vec![]);
            let store = store().await;
            store
                .insert_transfer(&new_transfer(WaitingCounterparty, None, Some("r")), 1)
                .await
                .unwrap();

            let report = sync(&rln, &store, &cfg(), Scope::Pending, 2).await.unwrap();

            assert_eq!(report.assets, 2);
            assert_eq!(
                rln.calls(),
                vec![
                    "list_assets",
                    "list_assetless_transfers",
                    "list_transfers:A",
                    "list_transfers:B"
                ]
            );
            assert_eq!(store.list_assets().await.unwrap().len(), 2);
        }

        #[tokio::test]
        async fn pending_scope_with_nothing_pending_makes_no_calls() {
            let rln = MockRln::default();
            rln.add_asset("A", "NIA", vec![]);
            let store = store().await;
            store
                .insert_transfer(&new_transfer(Settled, Some("A"), None), 1)
                .await
                .unwrap();

            let report = sync(&rln, &store, &cfg(), Scope::Pending, 2).await.unwrap();

            assert_eq!(report, SyncReport::default());
            assert!(rln.calls().is_empty());
        }

        #[tokio::test]
        async fn asset_scope_queries_given_assets_only() {
            let rln = MockRln::default();
            rln.add_asset("A", "NIA", vec![]);
            rln.add_asset("B", "CFA", vec![MockRln::transfer(3, Settled, None)]);
            let store = store().await;

            let report = sync(&rln, &store, &cfg(), Scope::Assets(vec!["B".into()]), 2)
                .await
                .unwrap();

            assert_eq!(report.assets, 1);
            assert_eq!(report.transfers, 1);
            assert_eq!(rln.calls(), vec!["list_transfers:B"]);
            assert!(store.list_assets().await.unwrap().is_empty());
        }

        #[tokio::test]
        async fn locked_propagates() {
            let rln = MockRln::default();
            rln.add_asset("A", "NIA", vec![MockRln::transfer(1, Settled, None)]);
            *rln.fail_with.lock().unwrap() = Some(MockFailure::Locked);
            let store = store().await;

            let err = sync(&rln, &store, &cfg(), Scope::Full, 2)
                .await
                .unwrap_err();

            assert!(matches!(err, SyncError::Rln(RlnError::Locked)), "{err:?}");
            assert!(store.list_assets().await.unwrap().is_empty());
            assert!(store
                .list_transfers(None, None, 10)
                .await
                .unwrap()
                .is_empty());
        }

        #[tokio::test]
        async fn full_scope_uses_large_page_size() {
            let rln = MockRln::default();
            rln.add_asset("A", "NIA", vec![]);
            let store = store().await;
            store
                .insert_transfer(&new_transfer(WaitingCounterparty, Some("A"), None), 1)
                .await
                .unwrap();

            sync(&rln, &store, &cfg(), Scope::Full, 2).await.unwrap();
            assert_eq!(rln.page_sizes.lock().unwrap().clone(), vec![1000]);

            rln.page_sizes.lock().unwrap().clear();
            sync(&rln, &store, &cfg(), Scope::Pending, 3).await.unwrap();
            assert_eq!(rln.page_sizes.lock().unwrap().clone(), vec![100]);
        }
    }
}
