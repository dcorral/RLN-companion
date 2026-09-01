use std::collections::BTreeSet;

use serde_json::json;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config;
use crate::rln::{RlnApi, RlnError, RlnTransfer};
use crate::store::{Observed, Store, StoreError, Transfer, TransferStatus};

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
