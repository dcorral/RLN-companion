use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, MutexGuard, Notify};
use tokio::time::{sleep, Instant};
use tracing::{debug, error, info, warn};

use crate::config;
use crate::now;
use crate::rln::{RlnApi, RlnError};
use crate::store::{NodeState, Store, StoreError, TransferStatus};
use crate::sync::{apply_observed, sync, Scope, SyncError, SyncReport};

#[derive(Debug, PartialEq, Eq)]
pub enum TickOutcome {
    Idle,
    Paused,
    Refreshed(SyncReport),
    Locked,
    Unreachable,
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    Sync(#[from] SyncError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Rln(#[from] RlnError),
    #[error("node misconfigured: {0}")]
    Misconfigured(String),
}

fn auth_fault(e: RlnError) -> EngineError {
    match e {
        RlnError::Api { code: 401, .. } => {
            EngineError::Misconfigured("node requires a Biscuit token: set rln.token".into())
        }
        RlnError::Api { code: 403, ref name, .. } if name == "Forbidden" => {
            EngineError::Misconfigured("rln.token lacks the rights the companion needs (admin, or custom with /refreshtransfers, /failtransfers, /listtransfers, /listassets, /nodeinfo, /networkinfo)".into())
        }
        e => e.into(),
    }
}

enum Fault {
    Locked,
    Unreachable,
}

fn fault(e: &EngineError) -> Option<Fault> {
    let rln = match e {
        EngineError::Rln(r) | EngineError::Sync(SyncError::Rln(r)) => r,
        _ => return None,
    };
    match rln {
        RlnError::Locked => Some(Fault::Locked),
        RlnError::Transport(_) => Some(Fault::Unreachable),
        _ => None,
    }
}

pub struct Engine<R: RlnApi + ?Sized> {
    rln: Arc<R>,
    store: Store,
    cfg: config::Engine,
    sync_cfg: config::Sync,
    expected_network: Option<String>,
    lock: Mutex<()>,
    wake: Notify,
    #[cfg(test)]
    wake_count: std::sync::atomic::AtomicU64,
}

impl<R: RlnApi + ?Sized> Engine<R> {
    pub fn new(
        rln: Arc<R>,
        store: Store,
        cfg: config::Engine,
        sync_cfg: config::Sync,
        expected_network: Option<String>,
    ) -> Self {
        Self {
            rln,
            store,
            cfg,
            sync_cfg,
            expected_network,
            lock: Mutex::new(()),
            wake: Notify::new(),
            #[cfg(test)]
            wake_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn wake(&self) {
        #[cfg(test)]
        self.wake_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.wake.notify_one();
    }

    #[cfg(test)]
    pub fn wake_count(&self) -> u64 {
        self.wake_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Not reentrant: never call tick/reap/reconcile/full sync while holding the guard.
    pub async fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.lock().await
    }

    #[cfg(test)]
    pub fn store(&self) -> &Store {
        &self.store
    }

    pub async fn set_node_state(&self, state: NodeState) -> Result<(), EngineError> {
        if self.store.node_state().await? != state {
            info!(?state, "node state");
        }
        self.store.set_node_state(state, now()).await?;
        Ok(())
    }

    async fn is_unlocked(&self) -> bool {
        match self.store.node_state().await {
            Ok(s) => s == NodeState::Unlocked,
            Err(e) => {
                warn!(error = %e, "node_state read failed");
                false
            }
        }
    }

    pub async fn probe(&self) -> Result<NodeState, EngineError> {
        let checked = match self.rln.node_info().await {
            Ok(_) => self.check_network().await,
            Err(e) => Err(auth_fault(e)),
        };
        let state = match checked {
            Ok(()) => NodeState::Unlocked,
            Err(e) => match fault(&e) {
                Some(Fault::Locked) => NodeState::Locked,
                Some(Fault::Unreachable) => {
                    warn!(error = %e, "rln unreachable");
                    NodeState::Down
                }
                None => {
                    if matches!(e, EngineError::Misconfigured(_)) {
                        self.set_node_state(NodeState::Misconfigured).await?;
                    }
                    return Err(e);
                }
            },
        };
        self.set_node_state(state).await?;
        Ok(state)
    }

    async fn check_network(&self) -> Result<(), EngineError> {
        let Some(want) = &self.expected_network else {
            return Ok(());
        };
        let got = self.rln.network().await.map_err(auth_fault)?;
        if got.eq_ignore_ascii_case(want) {
            return Ok(());
        }
        Err(EngineError::Misconfigured(format!(
            "node network is {got}, rln.network expects {want}"
        )))
    }

    async fn classify(&self, e: EngineError) -> Result<Fault, EngineError> {
        match fault(&e) {
            Some(Fault::Locked) => {
                self.set_node_state(NodeState::Locked).await?;
                Ok(Fault::Locked)
            }
            Some(Fault::Unreachable) => {
                warn!(error = %e, "rln unreachable");
                Ok(Fault::Unreachable)
            }
            None => Err(e),
        }
    }

    async fn refresh_and_sync(&self, now: i64) -> Result<SyncReport, EngineError> {
        self.rln.refresh(self.cfg.skip_sync).await?;
        Ok(sync(&*self.rln, &self.store, &self.sync_cfg, Scope::Pending, now).await?)
    }

    async fn sync_locked(&self, scope: Scope, now: i64) -> Result<SyncReport, EngineError> {
        match sync(&*self.rln, &self.store, &self.sync_cfg, scope, now).await {
            Ok(report) => Ok(report),
            Err(e) => {
                if matches!(e, SyncError::Rln(RlnError::Locked)) {
                    self.set_node_state(NodeState::Locked).await?;
                }
                Err(e.into())
            }
        }
    }

    pub async fn sync_pending_locked(&self, now: i64) -> Result<SyncReport, EngineError> {
        self.sync_locked(Scope::Pending, now).await
    }

    pub async fn sync_assets_locked(
        &self,
        ids: Vec<String>,
        now: i64,
    ) -> Result<SyncReport, EngineError> {
        self.sync_locked(Scope::Assets(ids), now).await
    }

    pub async fn tick(&self, now: i64) -> Result<TickOutcome, EngineError> {
        if self.store.node_state().await? != NodeState::Unlocked {
            return Ok(TickOutcome::Paused);
        }
        if self.store.pending_transfers().await?.is_empty() {
            return Ok(TickOutcome::Idle);
        }
        let _g = self.lock().await;
        match self.refresh_and_sync(now).await {
            Ok(report) => Ok(TickOutcome::Refreshed(report)),
            Err(e) => Ok(match self.classify(e).await? {
                Fault::Locked => TickOutcome::Locked,
                Fault::Unreachable => TickOutcome::Unreachable,
            }),
        }
    }

    pub async fn reap(&self, now: i64) -> Result<usize, EngineError> {
        let _g = self.lock().await;
        let mut failed = 0;
        for t in self.store.expired_fallible(now).await? {
            let Some(idx) = t.batch_transfer_idx else {
                continue;
            };
            match self.rln.fail_transfer(idx).await {
                Ok(()) => {
                    info!(transfer_id = %t.id, batch_transfer_idx = idx, "failed expired transfer");
                    apply_observed(&self.store, &t, TransferStatus::Failed, now).await?;
                    failed += 1;
                }
                Err(RlnError::CannotFail) => {
                    debug!(transfer_id = %t.id, batch_transfer_idx = idx, "cannot fail");
                }
                Err(RlnError::Locked) => {
                    self.set_node_state(NodeState::Locked).await?;
                    break;
                }
                Err(RlnError::Transport(e)) => {
                    warn!(error = %e, "rln unreachable");
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        if failed > 0 {
            self.wake();
        }
        Ok(failed)
    }

    async fn full_sync(&self) -> Result<SyncReport, EngineError> {
        let _g = self.lock().await;
        let now = now();
        let report = sync(&*self.rln, &self.store, &self.sync_cfg, Scope::Full, now).await?;
        self.store.set_last_full_sync_at(now).await?;
        Ok(report)
    }

    pub async fn reconcile(&self) -> Result<bool, EngineError> {
        let started = Instant::now();
        loop {
            let synced = match self.full_sync().await {
                Ok(report) => self.check_network().await.map(|()| report),
                Err(e) => Err(e),
            };
            match synced {
                Ok(report) => {
                    info!(?report, "reconciled");
                    self.set_node_state(NodeState::Unlocked).await?;
                    return Ok(true);
                }
                Err(e @ EngineError::Misconfigured(_)) => {
                    error!(error = %e, "reconcile paused");
                    self.set_node_state(NodeState::Misconfigured).await?;
                    return Ok(false);
                }
                Err(e) => match fault(&e) {
                    Some(Fault::Locked) => {
                        info!("rln locked, waiting");
                        self.set_node_state(NodeState::Locked).await?;
                    }
                    Some(Fault::Unreachable) => {
                        warn!(error = %e, "rln unreachable");
                        self.set_node_state(NodeState::Down).await?;
                    }
                    None => return Err(e),
                },
            }
            if started.elapsed().as_secs() >= self.cfg.reconcile_max_wait_secs {
                warn!("reconcile gave up waiting for rln");
                return Ok(false);
            }
            sleep(Duration::from_secs(self.cfg.reconcile_backoff_secs)).await;
        }
    }

    async fn backoff_probe(&self) {
        sleep(Duration::from_secs(self.cfg.reconcile_backoff_secs)).await;
        match self.probe().await {
            Ok(_) => {}
            Err(e @ EngineError::Misconfigured(_)) => error!(error = %e, "probe failed"),
            Err(e) => warn!(error = %e, "probe failed"),
        }
    }

    pub async fn run_refresh_loop(self: Arc<Self>) {
        let interval = Duration::from_secs(self.cfg.refresh_interval_secs);
        loop {
            tokio::select! {
                _ = sleep(interval) => {}
                _ = self.wake.notified() => {}
            }
            match self.tick(now()).await {
                Ok(TickOutcome::Paused | TickOutcome::Locked | TickOutcome::Unreachable) => {
                    self.backoff_probe().await;
                }
                Ok(outcome) => debug!(?outcome, "tick"),
                Err(e) => warn!(error = %e, "tick failed"),
            }
        }
    }

    pub async fn run_reaper(self: Arc<Self>) {
        let interval = Duration::from_secs(self.cfg.reap_interval_secs);
        loop {
            sleep(interval).await;
            if !self.is_unlocked().await {
                continue;
            }
            match self.reap(now()).await {
                Ok(n) => debug!(failed = n, "reap"),
                Err(e) => warn!(error = %e, "reap failed"),
            }
        }
    }

    pub async fn run_full_sync_timer(self: Arc<Self>) {
        let interval = Duration::from_secs(self.sync_cfg.full_interval_secs);
        loop {
            sleep(interval).await;
            if !self.is_unlocked().await {
                continue;
            }
            match self.full_sync().await {
                Ok(report) => debug!(?report, "full sync"),
                Err(e) => match self.classify(e).await {
                    Ok(Fault::Unreachable) => self.backoff_probe().await,
                    Ok(Fault::Locked) => {}
                    Err(e) => warn!(error = %e, "full sync failed"),
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::config;
    use crate::rln::test_support::{wait_until, MockFailure, MockRln};
    use crate::store::{NewTransfer, NodeState, Store, TransferStatus};
    use TransferStatus::*;

    async fn build(
        cfg: config::Engine,
        network: Option<&str>,
    ) -> (Arc<MockRln>, Arc<Engine<MockRln>>) {
        let rln = Arc::new(MockRln::default());
        let store = Store::open_in_memory().await.unwrap();
        let engine = Arc::new(Engine::new(
            rln.clone(),
            store,
            cfg,
            config::Sync::default(),
            network.map(str::to_string),
        ));
        (rln, engine)
    }

    async fn harness(cfg: config::Engine) -> (Arc<MockRln>, Arc<Engine<MockRln>>) {
        build(cfg, None).await
    }

    async fn unlocked() -> (Arc<MockRln>, Arc<Engine<MockRln>>) {
        let (rln, engine) = harness(config::Engine::default()).await;
        engine.set_node_state(NodeState::Unlocked).await.unwrap();
        (rln, engine)
    }

    fn row(
        status: TransferStatus,
        asset: Option<&str>,
        recipient: Option<&str>,
        idx: Option<i32>,
        expiration: Option<u64>,
    ) -> NewTransfer {
        NewTransfer {
            asset_id: asset.map(str::to_string),
            recipient_id: recipient.map(str::to_string),
            batch_transfer_idx: idx,
            expiration_timestamp: expiration,
            ..NewTransfer::with_status(status)
        }
    }

    async fn pending_row(rln: &MockRln, engine: &Engine<MockRln>) -> String {
        rln.add_asset("A", "NIA", vec![MockRln::transfer(1, Settled, Some("r"))]);
        engine
            .store()
            .insert_transfer(
                &row(WaitingCounterparty, Some("A"), Some("r"), None, None),
                1,
            )
            .await
            .unwrap()
            .id
    }

    fn set_failure(rln: &MockRln, f: Option<MockFailure>) {
        *rln.fail_with.lock().unwrap() = f;
    }

    async fn state(engine: &Engine<MockRln>) -> NodeState {
        engine.store().node_state().await.unwrap()
    }

    #[tokio::test]
    async fn tick_idle_when_no_pending() {
        let (rln, engine) = unlocked().await;
        assert_eq!(engine.tick(2).await.unwrap(), TickOutcome::Idle);
        assert!(rln.calls().is_empty());
    }

    #[tokio::test]
    async fn tick_refreshes_then_syncs_pending() {
        let (rln, engine) = unlocked().await;
        let id = pending_row(&rln, &engine).await;

        let outcome = engine.tick(2).await.unwrap();

        let TickOutcome::Refreshed(report) = outcome else {
            panic!("unexpected {outcome:?}");
        };
        assert_eq!(report.transitions, 1);
        assert_eq!(rln.calls(), vec!["refresh", "list_transfers:A"]);
        let store = engine.store();
        assert_eq!(
            store.get_transfer(&id).await.unwrap().unwrap().status,
            Settled
        );
        assert_eq!(store.undelivered_events(10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn tick_paused_when_node_not_unlocked() {
        let (rln, engine) = harness(config::Engine::default()).await;
        engine.set_node_state(NodeState::Locked).await.unwrap();
        pending_row(&rln, &engine).await;

        assert_eq!(engine.tick(2).await.unwrap(), TickOutcome::Paused);
        assert!(rln.calls().is_empty());
    }

    #[tokio::test]
    async fn tick_locked_response_sets_node_state() {
        let (rln, engine) = unlocked().await;
        pending_row(&rln, &engine).await;
        set_failure(&rln, Some(MockFailure::Locked));

        assert_eq!(engine.tick(2).await.unwrap(), TickOutcome::Locked);
        assert_eq!(state(&engine).await, NodeState::Locked);
    }

    #[tokio::test]
    async fn tick_transport_error_is_unreachable() {
        let (rln, engine) = unlocked().await;
        let id = pending_row(&rln, &engine).await;
        set_failure(&rln, Some(MockFailure::Transport));

        assert_eq!(engine.tick(2).await.unwrap(), TickOutcome::Unreachable);
        assert_eq!(state(&engine).await, NodeState::Unlocked);
        let store = engine.store();
        let row = store.get_transfer(&id).await.unwrap().unwrap();
        assert_eq!(row.status, WaitingCounterparty);
        assert_eq!(row.updated_at, 1);
        assert!(store.undelivered_events(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn tick_api_error_is_err() {
        let (rln, engine) = unlocked().await;
        pending_row(&rln, &engine).await;
        set_failure(&rln, Some(MockFailure::Api));

        let err = engine.tick(2).await.unwrap_err();

        assert!(
            matches!(err, EngineError::Rln(RlnError::Api { .. })),
            "{err:?}"
        );
        assert_eq!(state(&engine).await, NodeState::Unlocked);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_lock_serializes() {
        let (rln, engine) = unlocked().await;
        pending_row(&rln, &engine).await;

        let guard = engine.lock().await;
        let task = tokio::spawn({
            let engine = engine.clone();
            async move { engine.tick(2).await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(rln.calls().is_empty());
        drop(guard);
        task.await.unwrap().unwrap();

        assert_eq!(rln.calls().iter().filter(|c| *c == "refresh").count(), 1);
    }

    async fn reap_rows(engine: &Engine<MockRln>) -> Vec<(TransferStatus, i64)> {
        let store = engine.store();
        for r in [
            row(WaitingCounterparty, None, None, Some(7), Some(50)),
            row(WaitingCounterparty, None, None, None, Some(50)),
            row(Settled, None, None, Some(8), Some(50)),
            row(WaitingCounterparty, None, None, Some(9), Some(200)),
        ] {
            store.insert_transfer(&r, 1).await.unwrap();
        }
        snapshot(engine).await
    }

    async fn snapshot(engine: &Engine<MockRln>) -> Vec<(TransferStatus, i64)> {
        engine
            .store()
            .list_transfers(None, None, 10)
            .await
            .unwrap()
            .into_iter()
            .map(|t| (t.status, t.updated_at))
            .collect()
    }

    #[tokio::test]
    async fn reap_fails_expired_fallible_with_idx() {
        let (rln, engine) = unlocked().await;
        let mut expected = reap_rows(&engine).await;

        assert_eq!(engine.reap(100).await.unwrap(), 1);

        assert_eq!(rln.calls(), vec!["fail_transfer:7"]);
        let after = snapshot(&engine).await;
        let pos = after.iter().position(|r| r.0 == Failed).unwrap();
        expected[pos] = (Failed, 100);
        assert_eq!(after, expected);
        let events = engine.store().undelivered_events(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "transfer.failed");
        let payload: serde_json::Value = serde_json::from_str(&events[0].payload).unwrap();
        assert_eq!(payload["transfer"]["batch_transfer_idx"], 7);
        assert_eq!(payload["previous_status"], "WaitingCounterparty");
    }

    #[tokio::test]
    async fn reap_ignores_cannot_fail() {
        let (rln, engine) = unlocked().await;
        let before = reap_rows(&engine).await;
        set_failure(&rln, Some(MockFailure::CannotFail));

        assert_eq!(engine.reap(100).await.unwrap(), 0);
        assert_eq!(rln.calls(), vec!["fail_transfer:7"]);
        assert_eq!(snapshot(&engine).await, before);
        assert!(engine
            .store()
            .undelivered_events(10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn reap_locked_sets_node_state() {
        let (rln, engine) = unlocked().await;
        reap_rows(&engine).await;
        set_failure(&rln, Some(MockFailure::Locked));

        assert_eq!(engine.reap(100).await.unwrap(), 0);
        assert_eq!(state(&engine).await, NodeState::Locked);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reconcile_retries_until_unlocked() {
        let cfg = config::Engine {
            reconcile_backoff_secs: 0,
            ..Default::default()
        };
        let (rln, engine) = harness(cfg).await;
        rln.add_asset("A", "NIA", vec![]);
        set_failure(&rln, Some(MockFailure::Locked));

        let task = tokio::spawn({
            let engine = engine.clone();
            async move { engine.reconcile().await }
        });
        wait_until(|| rln.calls().len() >= 2).await;
        assert_eq!(state(&engine).await, NodeState::Locked);
        set_failure(&rln, None);

        assert!(task.await.unwrap().unwrap());
        assert_eq!(state(&engine).await, NodeState::Unlocked);
        let calls = rln.calls();
        assert!(calls.contains(&"list_assets".to_string()), "{calls:?}");
        assert!(calls.contains(&"list_transfers:A".to_string()), "{calls:?}");
    }

    #[tokio::test]
    async fn full_sync_stamps_last_full_sync_at() {
        let (rln, engine) = harness(config::Engine::default()).await;
        rln.add_asset("A", "NIA", vec![]);
        assert_eq!(engine.store.last_full_sync_at().await.unwrap(), None);

        assert!(engine.reconcile().await.unwrap());

        let stamped = engine.store.last_full_sync_at().await.unwrap().unwrap();
        assert!(stamped > 0);
    }

    #[tokio::test]
    async fn reconcile_gives_up_after_max_wait() {
        let cfg = config::Engine {
            reconcile_backoff_secs: 0,
            reconcile_max_wait_secs: 0,
            ..Default::default()
        };
        let (rln, engine) = harness(cfg).await;
        set_failure(&rln, Some(MockFailure::Locked));

        assert!(!engine.reconcile().await.unwrap());
        assert_eq!(state(&engine).await, NodeState::Locked);
    }

    #[tokio::test]
    async fn probe_updates_node_state() {
        let (rln, engine) = harness(config::Engine::default()).await;

        assert_eq!(engine.probe().await.unwrap(), NodeState::Unlocked);
        assert_eq!(state(&engine).await, NodeState::Unlocked);

        set_failure(&rln, Some(MockFailure::Locked));
        assert_eq!(engine.probe().await.unwrap(), NodeState::Locked);
        assert_eq!(state(&engine).await, NodeState::Locked);

        set_failure(&rln, Some(MockFailure::Transport));
        assert_eq!(engine.probe().await.unwrap(), NodeState::Down);
        assert_eq!(state(&engine).await, NodeState::Down);
    }

    async fn harness_with_network(expected: Option<&str>) -> (Arc<MockRln>, Arc<Engine<MockRln>>) {
        build(config::Engine::default(), expected).await
    }

    fn misconfigured(err: EngineError) -> String {
        match err {
            EngineError::Misconfigured(msg) => msg,
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn probe_reports_missing_token() {
        let (rln, engine) = harness(config::Engine::default()).await;
        set_failure(&rln, Some(MockFailure::Unauthorized));

        let msg = misconfigured(engine.probe().await.unwrap_err());
        assert!(msg.contains("rln.token"), "{msg}");
        assert_eq!(state(&engine).await, NodeState::Misconfigured);
    }

    #[tokio::test]
    async fn probe_reports_forbidden_token() {
        let (rln, engine) = harness(config::Engine::default()).await;
        set_failure(&rln, Some(MockFailure::Forbidden));

        let msg = misconfigured(engine.probe().await.unwrap_err());
        assert!(msg.contains("rln.token"), "{msg}");
        assert!(msg.contains("/networkinfo"), "{msg}");
        assert_eq!(state(&engine).await, NodeState::Misconfigured);
    }

    #[tokio::test]
    async fn probe_checks_expected_network() {
        let (_, engine) = harness_with_network(Some("testnet")).await;
        let msg = misconfigured(engine.probe().await.unwrap_err());
        assert!(msg.contains("Regtest") && msg.contains("testnet"), "{msg}");
        assert_eq!(state(&engine).await, NodeState::Misconfigured);

        let (rln, engine) = harness_with_network(Some("regtest")).await;
        assert_eq!(engine.probe().await.unwrap(), NodeState::Unlocked);
        assert_eq!(rln.calls(), vec!["node_info", "network"]);
    }

    #[tokio::test]
    async fn probe_folds_network_call_faults() {
        for (failure, expected) in [
            (MockFailure::Locked, NodeState::Locked),
            (MockFailure::Transport, NodeState::Down),
        ] {
            let (rln, engine) = harness_with_network(Some("regtest")).await;
            *rln.fail_call.lock().unwrap() = Some(("network".into(), failure));

            assert_eq!(engine.probe().await.unwrap(), expected);
            assert_eq!(state(&engine).await, expected);
            assert_eq!(rln.calls(), vec!["node_info", "network"]);
        }
    }

    #[tokio::test]
    async fn probe_recovers_from_misconfigured() {
        let (rln, engine) = harness_with_network(Some("regtest")).await;
        engine
            .set_node_state(NodeState::Misconfigured)
            .await
            .unwrap();

        assert_eq!(engine.probe().await.unwrap(), NodeState::Unlocked);
        assert_eq!(state(&engine).await, NodeState::Unlocked);
        assert_eq!(rln.calls(), vec!["node_info", "network"]);
    }

    #[tokio::test]
    async fn reconcile_with_wrong_network_pauses_as_misconfigured() {
        let (rln, engine) = harness_with_network(Some("testnet")).await;
        rln.add_asset("A", "NIA", vec![]);

        assert!(!engine.reconcile().await.unwrap());

        assert_eq!(state(&engine).await, NodeState::Misconfigured);
        let calls = rln.calls();
        assert!(calls.contains(&"network".to_string()), "{calls:?}");
    }

    #[tokio::test]
    async fn probe_skips_network_check_when_unset() {
        let (rln, engine) = harness_with_network(None).await;
        assert_eq!(engine.probe().await.unwrap(), NodeState::Unlocked);
        assert_eq!(rln.calls(), vec!["node_info"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wake_triggers_tick_without_waiting_interval() {
        let cfg = config::Engine {
            refresh_interval_secs: 3600,
            ..Default::default()
        };
        let (rln, engine) = harness(cfg).await;
        engine.set_node_state(NodeState::Unlocked).await.unwrap();
        pending_row(&rln, &engine).await;

        let task = tokio::spawn(engine.clone().run_refresh_loop());
        engine.wake();
        wait_until(|| rln.calls().contains(&"refresh".to_string())).await;
        task.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn paused_loop_probes_and_recovers() {
        let cfg = config::Engine {
            refresh_interval_secs: 3600,
            reconcile_backoff_secs: 0,
            ..Default::default()
        };
        let (rln, engine) = harness(cfg).await;
        engine.set_node_state(NodeState::Down).await.unwrap();

        let task = tokio::spawn(engine.clone().run_refresh_loop());
        engine.wake();
        wait_until(|| rln.calls().contains(&"node_info".to_string())).await;
        for _ in 0..200 {
            if state(&engine).await == NodeState::Unlocked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        task.abort();

        assert_eq!(state(&engine).await, NodeState::Unlocked);
    }

    #[tokio::test]
    async fn sync_pending_locked_syncs_without_refresh() {
        let (rln, engine) = unlocked().await;
        let id = pending_row(&rln, &engine).await;

        let guard = engine.lock().await;
        let report = engine.sync_pending_locked(2).await.unwrap();
        drop(guard);

        assert_eq!(report.transitions, 1);
        assert_eq!(rln.calls(), vec!["list_transfers:A"]);
        let row = engine.store().get_transfer(&id).await.unwrap().unwrap();
        assert_eq!(row.status, Settled);
    }

    #[tokio::test]
    async fn sync_assets_locked_classifies_locked() {
        let (rln, engine) = unlocked().await;
        rln.add_asset("A", "NIA", vec![]);
        set_failure(&rln, Some(MockFailure::Locked));

        let guard = engine.lock().await;
        let err = engine
            .sync_assets_locked(vec!["A".into()], 2)
            .await
            .unwrap_err();
        drop(guard);

        assert!(
            matches!(err, EngineError::Sync(SyncError::Rln(RlnError::Locked))),
            "{err:?}"
        );
        assert_eq!(rln.calls(), vec!["list_transfers:A"]);
        assert_eq!(state(&engine).await, NodeState::Locked);
    }
}
