use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("db error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("corrupt store: {0}")]
    Corrupt(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferStatus {
    Initiated,
    WaitingCounterparty,
    WaitingSafeHeight,
    WaitingBroadcast,
    WaitingConfirmations,
    Settled,
    Failed,
}

impl TransferStatus {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Settled | Self::Failed)
    }

    pub fn waiting(self) -> bool {
        matches!(
            self,
            Self::WaitingCounterparty
                | Self::WaitingSafeHeight
                | Self::WaitingBroadcast
                | Self::WaitingConfirmations
        )
    }

    pub fn fallible(self) -> bool {
        matches!(
            self,
            Self::Initiated
                | Self::WaitingCounterparty
                | Self::WaitingSafeHeight
                | Self::WaitingBroadcast
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PaymentStatus {
    Pending,
    Succeeded,
    Failed,
    Claimable,
    Claiming,
    Cancelled,
}

impl PaymentStatus {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PaymentDirection {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Unknown,
    Locked,
    Unlocked,
    Down,
    Misconfigured,
}

#[derive(Debug, Clone, Serialize)]
pub struct Transfer {
    pub id: String,
    pub rln_idx: Option<i32>,
    pub asset_id: Option<String>,
    pub kind: Option<String>,
    pub status: TransferStatus,
    pub recipient_id: Option<String>,
    pub txid: Option<String>,
    pub batch_transfer_idx: Option<i32>,
    pub invoice: Option<String>,
    pub expiration_timestamp: Option<u64>,
    pub created_at: i64,
    pub updated_at: i64,
    pub last_seen_at: Option<i64>,
    pub settled_at: Option<i64>,
}

pub struct NewTransfer {
    pub asset_id: Option<String>,
    pub kind: Option<String>,
    pub status: TransferStatus,
    pub recipient_id: Option<String>,
    pub txid: Option<String>,
    pub batch_transfer_idx: Option<i32>,
    pub invoice: Option<String>,
    pub expiration_timestamp: Option<u64>,
}

#[cfg(test)]
impl NewTransfer {
    pub fn with_status(status: TransferStatus) -> Self {
        Self {
            asset_id: None,
            kind: None,
            status,
            recipient_id: None,
            txid: None,
            batch_transfer_idx: None,
            invoice: None,
            expiration_timestamp: None,
        }
    }
}

pub struct Observed {
    pub rln_idx: i32,
    pub asset_id: Option<String>,
    pub kind: String,
    pub status: TransferStatus,
    pub recipient_id: Option<String>,
    pub txid: Option<String>,
    pub expiration_timestamp: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Payment {
    pub payment_hash: String,
    pub direction: PaymentDirection,
    pub status: PaymentStatus,
    pub asset_id: Option<String>,
    pub asset_amount: Option<u64>,
    pub amt_msat: Option<u64>,
    pub payee_pubkey: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub last_seen_at: Option<i64>,
}

pub struct NewPayment {
    pub payment_hash: String,
    pub direction: PaymentDirection,
    pub status: PaymentStatus,
    pub asset_id: Option<String>,
    pub asset_amount: Option<u64>,
    pub amt_msat: Option<u64>,
    pub payee_pubkey: Option<String>,
}

#[derive(sqlx::FromRow)]
pub struct OutboxEvent {
    pub id: String,
    pub event_type: String,
    pub payload: String,
    pub attempts: u32,
    pub next_attempt_at: i64,
}

const ALL_STATUSES: [TransferStatus; 7] = [
    TransferStatus::Initiated,
    TransferStatus::WaitingCounterparty,
    TransferStatus::WaitingSafeHeight,
    TransferStatus::WaitingBroadcast,
    TransferStatus::WaitingConfirmations,
    TransferStatus::Settled,
    TransferStatus::Failed,
];

const SELECT_TRANSFER: &str = "SELECT id, rln_idx, asset_id, kind, status, recipient_id, txid, \
    batch_transfer_idx, invoice, expiration_timestamp, created_at, updated_at, last_seen_at, \
    settled_at FROM transfers";

#[derive(sqlx::FromRow)]
struct TransferRow {
    id: String,
    rln_idx: Option<i32>,
    asset_id: Option<String>,
    kind: Option<String>,
    status: String,
    recipient_id: Option<String>,
    txid: Option<String>,
    batch_transfer_idx: Option<i32>,
    invoice: Option<String>,
    expiration_timestamp: Option<i64>,
    created_at: i64,
    updated_at: i64,
    last_seen_at: Option<i64>,
    settled_at: Option<i64>,
}

impl TryFrom<TransferRow> for Transfer {
    type Error = StoreError;

    fn try_from(r: TransferRow) -> Result<Self, StoreError> {
        let expiration_timestamp = r
            .expiration_timestamp
            .map(u64::try_from)
            .transpose()
            .map_err(|_| StoreError::Corrupt(format!("negative expiration on {}", r.id)))?;
        Ok(Transfer {
            id: r.id,
            rln_idx: r.rln_idx,
            asset_id: r.asset_id,
            kind: r.kind,
            status: enum_from_str(&r.status)?,
            recipient_id: r.recipient_id,
            txid: r.txid,
            batch_transfer_idx: r.batch_transfer_idx,
            invoice: r.invoice,
            expiration_timestamp,
            created_at: r.created_at,
            updated_at: r.updated_at,
            last_seen_at: r.last_seen_at,
            settled_at: r.settled_at,
        })
    }
}

const ALL_PAYMENT_STATUSES: [PaymentStatus; 6] = [
    PaymentStatus::Pending,
    PaymentStatus::Succeeded,
    PaymentStatus::Failed,
    PaymentStatus::Claimable,
    PaymentStatus::Claiming,
    PaymentStatus::Cancelled,
];

const SELECT_PAYMENT: &str = "SELECT payment_hash, direction, status, asset_id, asset_amount, \
    amt_msat, payee_pubkey, created_at, updated_at, last_seen_at FROM payments";

#[derive(sqlx::FromRow)]
struct PaymentRow {
    payment_hash: String,
    direction: String,
    status: String,
    asset_id: Option<String>,
    asset_amount: Option<i64>,
    amt_msat: Option<i64>,
    payee_pubkey: Option<String>,
    created_at: i64,
    updated_at: i64,
    last_seen_at: Option<i64>,
}

impl TryFrom<PaymentRow> for Payment {
    type Error = StoreError;

    fn try_from(r: PaymentRow) -> Result<Self, StoreError> {
        let amount = |v: Option<i64>| {
            v.map(u64::try_from)
                .transpose()
                .map_err(|_| StoreError::Corrupt(format!("negative amount on {}", r.payment_hash)))
        };
        Ok(Payment {
            direction: enum_from_str(&r.direction)?,
            status: enum_from_str(&r.status)?,
            asset_id: r.asset_id,
            asset_amount: amount(r.asset_amount)?,
            amt_msat: amount(r.amt_msat)?,
            payee_pubkey: r.payee_pubkey,
            created_at: r.created_at,
            updated_at: r.updated_at,
            last_seen_at: r.last_seen_at,
            payment_hash: r.payment_hash,
        })
    }
}

fn enum_to_str<T: Serialize>(v: &T) -> Result<String, StoreError> {
    match serde_json::to_value(v) {
        Ok(Value::String(s)) => Ok(s),
        other => Err(StoreError::Corrupt(format!("non-string enum: {other:?}"))),
    }
}

fn enum_from_str<T: DeserializeOwned>(s: &str) -> Result<T, StoreError> {
    serde_json::from_value(Value::String(s.to_string()))
        .map_err(|e| StoreError::Corrupt(format!("{s}: {e}")))
}

fn ts_to_db(v: Option<u64>) -> Result<Option<i64>, StoreError> {
    v.map(i64::try_from)
        .transpose()
        .map_err(|_| StoreError::Corrupt("expiration_timestamp out of range".into()))
}

fn amt_to_db(v: Option<u64>) -> Result<Option<i64>, StoreError> {
    v.map(i64::try_from)
        .transpose()
        .map_err(|_| StoreError::Corrupt("amount out of range".into()))
}

fn status_in(pred: fn(TransferStatus) -> bool) -> Result<(String, Vec<String>), StoreError> {
    let names = ALL_STATUSES
        .iter()
        .filter(|s| pred(**s))
        .map(enum_to_str)
        .collect::<Result<Vec<_>, _>>()?;
    let placeholders = vec!["?"; names.len()].join(", ");
    Ok((format!("status IN ({placeholders})"), names))
}

fn payment_status_in(pred: fn(PaymentStatus) -> bool) -> Result<(String, Vec<String>), StoreError> {
    let names = ALL_PAYMENT_STATUSES
        .iter()
        .filter(|s| pred(**s))
        .map(enum_to_str)
        .collect::<Result<Vec<_>, _>>()?;
    let placeholders = vec!["?"; names.len()].join(", ");
    Ok((format!("status IN ({placeholders})"), names))
}

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
}

impl Store {
    pub async fn open(path: &str) -> Result<Store, StoreError> {
        let opts = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(opts)
            .await?;
        let store = Self::init(pool).await?;
        #[cfg(unix)]
        restrict_permissions(path);
        Ok(store)
    }

    #[cfg(test)]
    pub async fn open_in_memory() -> Result<Store, StoreError> {
        let opts = SqliteConnectOptions::new().in_memory(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(opts)
            .await?;
        Self::init(pool).await
    }

    async fn init(pool: SqlitePool) -> Result<Store, StoreError> {
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Store { pool })
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }

    pub async fn insert_transfer(&self, t: &NewTransfer, now: i64) -> Result<Transfer, StoreError> {
        let id = sqlx::query_scalar::<_, String>(
            "INSERT INTO transfers (id, asset_id, kind, status, recipient_id, txid, \
             batch_transfer_idx, invoice, expiration_timestamp, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (recipient_id) DO UPDATE SET invoice = excluded.invoice, \
             batch_transfer_idx = excluded.batch_transfer_idx, \
             expiration_timestamp = COALESCE(excluded.expiration_timestamp, expiration_timestamp), \
             updated_at = excluded.updated_at \
             RETURNING id",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&t.asset_id)
        .bind(&t.kind)
        .bind(enum_to_str(&t.status)?)
        .bind(&t.recipient_id)
        .bind(&t.txid)
        .bind(t.batch_transfer_idx)
        .bind(&t.invoice)
        .bind(ts_to_db(t.expiration_timestamp)?)
        .bind(now)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;
        fetch_transfer(&self.pool, &id).await
    }

    pub async fn get_transfer(&self, id: &str) -> Result<Option<Transfer>, StoreError> {
        sqlx::query_as::<_, TransferRow>(&format!("{SELECT_TRANSFER} WHERE id = ?"))
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .map(Transfer::try_from)
            .transpose()
    }

    pub async fn list_transfers(
        &self,
        status: Option<TransferStatus>,
        asset_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<Transfer>, StoreError> {
        let status = status.map(|s| enum_to_str(&s)).transpose()?;
        let rows = sqlx::query_as::<_, TransferRow>(&format!(
            "{SELECT_TRANSFER} WHERE (?1 IS NULL OR status = ?1) \
             AND (?2 IS NULL OR asset_id = ?2) ORDER BY created_at DESC, rowid DESC LIMIT ?3"
        ))
        .bind(status)
        .bind(asset_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Transfer::try_from).collect()
    }

    pub async fn pending_transfers(&self) -> Result<Vec<Transfer>, StoreError> {
        let (clause, names) = status_in(TransferStatus::waiting)?;
        let sql = format!("{SELECT_TRANSFER} WHERE {clause} ORDER BY created_at, rowid");
        let mut q = sqlx::query_as::<_, TransferRow>(&sql);
        for n in names {
            q = q.bind(n);
        }
        let rows = q.fetch_all(&self.pool).await?;
        rows.into_iter().map(Transfer::try_from).collect()
    }

    pub async fn pending_asset_ids(&self) -> Result<(Vec<String>, bool), StoreError> {
        let (clause, names) = status_in(TransferStatus::waiting)?;
        let sql =
            format!("SELECT DISTINCT asset_id FROM transfers WHERE {clause} ORDER BY asset_id");
        let mut q = sqlx::query_scalar::<_, Option<String>>(&sql);
        for n in names {
            q = q.bind(n);
        }
        let rows = q.fetch_all(&self.pool).await?;
        let agnostic = rows.iter().any(Option::is_none);
        Ok((rows.into_iter().flatten().collect(), agnostic))
    }

    pub async fn upsert_observed(&self, o: &Observed, now: i64) -> Result<Transfer, StoreError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let existing = sqlx::query_scalar::<_, String>(
            "SELECT id FROM transfers WHERE rln_idx = ?1 \
             OR (rln_idx IS NULL AND (recipient_id = ?2 OR (txid = ?3 AND asset_id = ?4))) \
             ORDER BY CASE WHEN rln_idx = ?1 THEN 0 WHEN recipient_id = ?2 THEN 1 ELSE 2 END, \
             rowid LIMIT 1",
        )
        .bind(o.rln_idx)
        .bind(&o.recipient_id)
        .bind(&o.txid)
        .bind(&o.asset_id)
        .fetch_optional(&mut *tx)
        .await?;
        let id = match existing {
            Some(id) => {
                sqlx::query(
                    "UPDATE transfers SET rln_idx = ?, asset_id = COALESCE(?, asset_id), kind = ?, \
                     txid = COALESCE(?, txid), \
                     expiration_timestamp = COALESCE(?, expiration_timestamp), \
                     last_seen_at = ? WHERE id = ?",
                )
                .bind(o.rln_idx)
                .bind(&o.asset_id)
                .bind(&o.kind)
                .bind(&o.txid)
                .bind(ts_to_db(o.expiration_timestamp)?)
                .bind(now)
                .bind(&id)
                .execute(&mut *tx)
                .await?;
                id
            }
            None => {
                let id = Uuid::new_v4().to_string();
                sqlx::query(
                    "INSERT INTO transfers (id, rln_idx, asset_id, kind, status, recipient_id, \
                     txid, expiration_timestamp, created_at, updated_at, last_seen_at) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(&id)
                .bind(o.rln_idx)
                .bind(&o.asset_id)
                .bind(&o.kind)
                .bind(enum_to_str(&o.status)?)
                .bind(&o.recipient_id)
                .bind(&o.txid)
                .bind(ts_to_db(o.expiration_timestamp)?)
                .bind(now)
                .bind(now)
                .bind(now)
                .execute(&mut *tx)
                .await?;
                id
            }
        };
        let transfer = fetch_transfer(&mut *tx, &id).await?;
        tx.commit().await?;
        Ok(transfer)
    }

    pub async fn apply_transition(
        &self,
        id: &str,
        from: TransferStatus,
        to: TransferStatus,
        event: Option<(&str, &str, &str)>,
        now: i64,
    ) -> Result<bool, StoreError> {
        if from == to {
            return Ok(false);
        }
        let settled_at = (to == TransferStatus::Settled).then_some(now);
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let affected = sqlx::query(
            "UPDATE transfers SET status = ?1, updated_at = ?2, \
             settled_at = COALESCE(?4, settled_at) \
             WHERE id = ?3 AND status = ?5",
        )
        .bind(enum_to_str(&to)?)
        .bind(now)
        .bind(id)
        .bind(settled_at)
        .bind(enum_to_str(&from)?)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if affected == 0 {
            return Ok(false);
        }
        if let Some((event_id, event_type, payload)) = event {
            sqlx::query(
                "INSERT INTO webhook_outbox (id, event_type, payload, next_attempt_at) \
                 VALUES (?, ?, ?, ?)",
            )
            .bind(event_id)
            .bind(event_type)
            .bind(payload)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(true)
    }

    async fn upsert_payment(
        &self,
        p: &NewPayment,
        last_seen_at: Option<i64>,
        now: i64,
    ) -> Result<Payment, StoreError> {
        sqlx::query_as::<_, PaymentRow>(
            "INSERT INTO payments (payment_hash, direction, status, asset_id, asset_amount, \
             amt_msat, payee_pubkey, created_at, updated_at, last_seen_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
             ON CONFLICT (payment_hash) DO UPDATE SET direction = excluded.direction, \
             asset_id = COALESCE(excluded.asset_id, asset_id), \
             asset_amount = COALESCE(excluded.asset_amount, asset_amount), \
             amt_msat = COALESCE(excluded.amt_msat, amt_msat), \
             payee_pubkey = COALESCE(excluded.payee_pubkey, payee_pubkey), \
             last_seen_at = COALESCE(excluded.last_seen_at, last_seen_at) \
             RETURNING payment_hash, direction, status, asset_id, asset_amount, amt_msat, \
             payee_pubkey, created_at, updated_at, last_seen_at",
        )
        .bind(&p.payment_hash)
        .bind(enum_to_str(&p.direction)?)
        .bind(enum_to_str(&p.status)?)
        .bind(&p.asset_id)
        .bind(amt_to_db(p.asset_amount)?)
        .bind(amt_to_db(p.amt_msat)?)
        .bind(&p.payee_pubkey)
        .bind(now)
        .bind(now)
        .bind(last_seen_at)
        .fetch_one(&self.pool)
        .await?
        .try_into()
    }

    pub async fn upsert_payment_observed(
        &self,
        p: &NewPayment,
        now: i64,
    ) -> Result<Payment, StoreError> {
        self.upsert_payment(p, Some(now), now).await
    }

    pub async fn insert_pending_payment(
        &self,
        p: &NewPayment,
        now: i64,
    ) -> Result<Payment, StoreError> {
        self.upsert_payment(p, None, now).await
    }

    pub async fn apply_payment_transition(
        &self,
        payment_hash: &str,
        from: PaymentStatus,
        to: PaymentStatus,
        event: Option<(&str, &str, &str)>,
        now: i64,
    ) -> Result<bool, StoreError> {
        if from == to {
            return Ok(false);
        }
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let affected = sqlx::query(
            "UPDATE payments SET status = ?, updated_at = ? WHERE payment_hash = ? AND status = ?",
        )
        .bind(enum_to_str(&to)?)
        .bind(now)
        .bind(payment_hash)
        .bind(enum_to_str(&from)?)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if affected == 0 {
            return Ok(false);
        }
        if let Some((event_id, event_type, payload)) = event {
            sqlx::query(
                "INSERT INTO webhook_outbox (id, event_type, payload, next_attempt_at) \
                 VALUES (?, ?, ?, ?)",
            )
            .bind(event_id)
            .bind(event_type)
            .bind(payload)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(true)
    }

    pub async fn pending_payments(&self) -> Result<Vec<Payment>, StoreError> {
        let (clause, names) = payment_status_in(|s| !s.terminal())?;
        let sql = format!("{SELECT_PAYMENT} WHERE {clause} ORDER BY created_at, rowid");
        let mut q = sqlx::query_as::<_, PaymentRow>(&sql);
        for n in names {
            q = q.bind(n);
        }
        let rows = q.fetch_all(&self.pool).await?;
        rows.into_iter().map(Payment::try_from).collect()
    }

    pub async fn list_payments(
        &self,
        status: Option<PaymentStatus>,
        limit: u32,
    ) -> Result<Vec<Payment>, StoreError> {
        let status = status.map(|s| enum_to_str(&s)).transpose()?;
        let rows = sqlx::query_as::<_, PaymentRow>(&format!(
            "{SELECT_PAYMENT} WHERE (?1 IS NULL OR status = ?1) \
             ORDER BY created_at DESC, rowid DESC LIMIT ?2"
        ))
        .bind(status)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Payment::try_from).collect()
    }

    pub async fn get_payment(&self, payment_hash: &str) -> Result<Option<Payment>, StoreError> {
        sqlx::query_as::<_, PaymentRow>(&format!("{SELECT_PAYMENT} WHERE payment_hash = ?"))
            .bind(payment_hash)
            .fetch_optional(&self.pool)
            .await?
            .map(Payment::try_from)
            .transpose()
    }

    pub async fn upsert_assets(
        &self,
        assets: &[(String, String)],
        now: i64,
    ) -> Result<(), StoreError> {
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        for (asset_id, schema) in assets {
            sqlx::query(
                "INSERT INTO assets (asset_id, schema, last_synced_at) VALUES (?, ?, ?) \
                 ON CONFLICT (asset_id) DO UPDATE SET schema = excluded.schema, \
                 last_synced_at = excluded.last_synced_at",
            )
            .bind(asset_id)
            .bind(schema)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    #[cfg(test)]
    pub async fn list_assets(&self) -> Result<Vec<(String, String)>, StoreError> {
        Ok(sqlx::query_as::<_, (String, String)>(
            "SELECT asset_id, schema FROM assets ORDER BY asset_id",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn expired_fallible(&self, now: i64) -> Result<Vec<Transfer>, StoreError> {
        let (clause, names) = status_in(TransferStatus::fallible)?;
        let sql = format!(
            "{SELECT_TRANSFER} WHERE expiration_timestamp IS NOT NULL \
             AND expiration_timestamp < ? AND {clause} ORDER BY created_at, rowid"
        );
        let mut q = sqlx::query_as::<_, TransferRow>(&sql).bind(now);
        for n in names {
            q = q.bind(n);
        }
        let rows = q.fetch_all(&self.pool).await?;
        rows.into_iter().map(Transfer::try_from).collect()
    }

    pub async fn node_state(&self) -> Result<NodeState, StoreError> {
        let state = sqlx::query_scalar::<_, String>("SELECT state FROM node_state WHERE id = 1")
            .fetch_one(&self.pool)
            .await?;
        enum_from_str(&state)
    }

    pub async fn set_node_state(&self, state: NodeState, now: i64) -> Result<(), StoreError> {
        sqlx::query("UPDATE node_state SET state = ?, updated_at = ? WHERE id = 1")
            .bind(enum_to_str(&state)?)
            .bind(now)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn last_full_sync_at(&self) -> Result<Option<i64>, StoreError> {
        Ok(sqlx::query_scalar::<_, Option<i64>>(
            "SELECT last_full_sync_at FROM node_state WHERE id = 1",
        )
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn set_last_full_sync_at(&self, now: i64) -> Result<(), StoreError> {
        sqlx::query("UPDATE node_state SET last_full_sync_at = ? WHERE id = 1")
            .bind(now)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn payments_baseline_at(&self) -> Result<Option<i64>, StoreError> {
        Ok(sqlx::query_scalar::<_, Option<i64>>(
            "SELECT payments_baseline_at FROM node_state WHERE id = 1",
        )
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn set_payments_baseline_at(&self, now: i64) -> Result<(), StoreError> {
        sqlx::query("UPDATE node_state SET payments_baseline_at = ? WHERE id = 1")
            .bind(now)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn undelivered_events(
        &self,
        max_attempts: u32,
    ) -> Result<Vec<OutboxEvent>, StoreError> {
        Ok(sqlx::query_as::<_, OutboxEvent>(
            "SELECT id, event_type, payload, attempts, next_attempt_at FROM webhook_outbox \
             WHERE delivered_at IS NULL AND attempts < ? ORDER BY rowid LIMIT 100",
        )
        .bind(max_attempts)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn parked_events_count(&self, max_attempts: u32) -> Result<u64, StoreError> {
        let n = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM webhook_outbox WHERE delivered_at IS NULL AND attempts >= ?",
        )
        .bind(max_attempts)
        .fetch_one(&self.pool)
        .await?;
        Ok(u64::try_from(n).unwrap_or(0))
    }

    pub async fn record_attempt(&self, id: &str, next_attempt_at: i64) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE webhook_outbox SET attempts = attempts + 1, next_attempt_at = ? WHERE id = ?",
        )
        .bind(next_attempt_at)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn mark_delivered(&self, id: &str, now: i64) -> Result<(), StoreError> {
        sqlx::query("UPDATE webhook_outbox SET delivered_at = ? WHERE id = ?")
            .bind(now)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

#[cfg(unix)]
fn restrict_permissions(path: &str) {
    use std::os::unix::fs::PermissionsExt;
    for p in [
        path.to_string(),
        format!("{path}-wal"),
        format!("{path}-shm"),
    ] {
        if let Err(e) = std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o600)) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %p, error = %e, "chmod 0600 failed");
            }
        }
    }
}

async fn fetch_transfer<'e, E>(ex: E, id: &str) -> Result<Transfer, StoreError>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    sqlx::query_as::<_, TransferRow>(&format!("{SELECT_TRANSFER} WHERE id = ?"))
        .bind(id)
        .fetch_optional(ex)
        .await?
        .ok_or_else(|| StoreError::Corrupt(format!("transfer {id} vanished")))?
        .try_into()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn new_transfer(status: TransferStatus) -> NewTransfer {
        NewTransfer::with_status(status)
    }

    fn observed(rln_idx: i32) -> Observed {
        Observed {
            rln_idx,
            asset_id: Some("assetA".into()),
            kind: "ReceiveBlind".into(),
            status: TransferStatus::WaitingCounterparty,
            recipient_id: None,
            txid: None,
            expiration_timestamp: None,
        }
    }

    async fn count_transfers(store: &Store) -> usize {
        store.list_transfers(None, None, 100).await.unwrap().len()
    }

    #[tokio::test]
    async fn open_creates_file_and_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("companion.sqlite");
        let path = path.to_str().unwrap();
        let store = Store::open(path).await.unwrap();
        let t = store
            .insert_transfer(&new_transfer(TransferStatus::Initiated), 1)
            .await
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |p: &str| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(path), 0o600);
            let wal = format!("{path}-wal");
            if std::fs::metadata(&wal).is_ok() {
                assert_eq!(mode(&wal), 0o600);
            }
        }
        drop(store);
        let reopened = Store::open(path).await.unwrap();
        assert!(reopened.get_transfer(&t.id).await.unwrap().is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_file_backed_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.sqlite");
        let store = Store::open(path.to_str().unwrap()).await.unwrap();
        let seed = store
            .insert_transfer(&new_transfer(TransferStatus::Initiated), 0)
            .await
            .unwrap();
        store
            .apply_transition(
                &seed.id,
                TransferStatus::Initiated,
                TransferStatus::Failed,
                Some(("seed", "transfer.failed", "{}")),
                0,
            )
            .await
            .unwrap();
        let event_id = store.undelivered_events(1).await.unwrap()[0].id.clone();
        let mut handles = Vec::new();
        for w in 0..8 {
            let s = store.clone();
            let event_id = event_id.clone();
            handles.push(tokio::spawn(async move {
                let mut errs = Vec::new();
                for i in 0..100 {
                    let idx = w * 1000 + i;
                    let now = i as i64;
                    match s.upsert_observed(&observed(idx), now).await {
                        Ok(t) => {
                            if let Err(e) = s
                                .apply_transition(
                                    &t.id,
                                    TransferStatus::WaitingCounterparty,
                                    TransferStatus::Settled,
                                    Some((&format!("ev{idx}"), "transfer.settled", "{}")),
                                    now,
                                )
                                .await
                            {
                                errs.push(format!("transition {idx}: {e}"));
                            }
                        }
                        Err(e) => errs.push(format!("upsert {idx}: {e}")),
                    }
                    if let Err(e) = s.record_attempt(&event_id, now).await {
                        errs.push(format!("attempt {idx}: {e}"));
                    }
                    if let Err(e) = s.set_node_state(NodeState::Unlocked, now).await {
                        errs.push(format!("state {idx}: {e}"));
                    }
                }
                errs
            }));
        }
        let mut all = Vec::new();
        for h in handles {
            all.extend(h.await.unwrap());
        }
        assert!(
            all.is_empty(),
            "{} errors, first: {:?}",
            all.len(),
            all.first()
        );
        assert_eq!(
            store
                .list_transfers(None, None, 10_000)
                .await
                .unwrap()
                .len(),
            801
        );
        assert_eq!(
            store
                .list_transfers(Some(TransferStatus::Settled), None, 10_000)
                .await
                .unwrap()
                .len(),
            800
        );
    }

    #[tokio::test]
    async fn upsert_observed_does_not_rebind_bound_row() {
        let store = Store::open_in_memory().await.unwrap();
        let mut a = observed(10);
        a.txid = Some("tx".into());
        let first = store.upsert_observed(&a, 1).await.unwrap();
        let mut b = observed(11);
        b.txid = Some("tx".into());
        let second = store.upsert_observed(&b, 2).await.unwrap();
        assert_ne!(second.id, first.id);
        assert_eq!(second.rln_idx, Some(11));
        let first = store.get_transfer(&first.id).await.unwrap().unwrap();
        assert_eq!(first.rln_idx, Some(10));
        assert_eq!(count_transfers(&store).await, 2);
    }

    #[tokio::test]
    async fn insert_transfer_merges_into_sync_born_row() {
        let store = Store::open_in_memory().await.unwrap();
        let mut o = observed(5);
        o.recipient_id = Some("r".into());
        o.status = TransferStatus::WaitingConfirmations;
        let born = store.upsert_observed(&o, 1).await.unwrap();
        let mut n = new_transfer(TransferStatus::WaitingCounterparty);
        n.recipient_id = Some("r".into());
        n.batch_transfer_idx = Some(7);
        n.invoice = Some("inv".into());
        n.expiration_timestamp = Some(500);
        let merged = store.insert_transfer(&n, 2).await.unwrap();
        assert_eq!(merged.id, born.id);
        assert_eq!(merged.status, TransferStatus::WaitingConfirmations);
        assert_eq!(merged.batch_transfer_idx, Some(7));
        assert_eq!(merged.invoice.as_deref(), Some("inv"));
        assert_eq!(merged.expiration_timestamp, Some(500));
        assert_eq!(merged.rln_idx, Some(5));
        assert_eq!(merged.updated_at, 2);
        assert_eq!(count_transfers(&store).await, 1);
    }

    #[tokio::test]
    async fn list_transfers_filters_order_and_limit() {
        let store = Store::open_in_memory().await.unwrap();
        let mut rows = Vec::new();
        for (status, asset, at) in [
            (TransferStatus::Initiated, "a", 1),
            (TransferStatus::Settled, "a", 2),
            (TransferStatus::Settled, "b", 2),
            (TransferStatus::Initiated, "b", 3),
        ] {
            let mut n = new_transfer(status);
            n.asset_id = Some(asset.into());
            rows.push(store.insert_transfer(&n, at).await.unwrap().id);
        }
        let ids = |v: Vec<Transfer>| v.into_iter().map(|t| t.id).collect::<Vec<_>>();
        let all = ids(store.list_transfers(None, None, 10).await.unwrap());
        assert_eq!(
            all,
            vec![
                rows[3].clone(),
                rows[2].clone(),
                rows[1].clone(),
                rows[0].clone()
            ]
        );
        let settled = ids(store
            .list_transfers(Some(TransferStatus::Settled), None, 10)
            .await
            .unwrap());
        assert_eq!(settled, vec![rows[2].clone(), rows[1].clone()]);
        let asset_a = ids(store.list_transfers(None, Some("a"), 10).await.unwrap());
        assert_eq!(asset_a, vec![rows[1].clone(), rows[0].clone()]);
        let both = ids(store
            .list_transfers(Some(TransferStatus::Initiated), Some("b"), 10)
            .await
            .unwrap());
        assert_eq!(both, vec![rows[3].clone()]);
        let limited = ids(store.list_transfers(None, None, 2).await.unwrap());
        assert_eq!(limited, vec![rows[3].clone(), rows[2].clone()]);
    }

    #[tokio::test]
    async fn insert_and_get_roundtrip() {
        let store = Store::open_in_memory().await.unwrap();
        let t = store
            .insert_transfer(
                &NewTransfer {
                    asset_id: Some("assetA".into()),
                    kind: Some("Send".into()),
                    status: TransferStatus::Initiated,
                    recipient_id: Some("rid".into()),
                    txid: Some("tx".into()),
                    batch_transfer_idx: Some(7),
                    invoice: Some("inv".into()),
                    expiration_timestamp: Some(1234),
                },
                100,
            )
            .await
            .unwrap();
        let got = store.get_transfer(&t.id).await.unwrap().unwrap();
        assert_eq!(got.id, t.id);
        assert_eq!(got.rln_idx, None);
        assert_eq!(got.asset_id.as_deref(), Some("assetA"));
        assert_eq!(got.kind.as_deref(), Some("Send"));
        assert_eq!(got.status, TransferStatus::Initiated);
        assert_eq!(got.recipient_id.as_deref(), Some("rid"));
        assert_eq!(got.txid.as_deref(), Some("tx"));
        assert_eq!(got.batch_transfer_idx, Some(7));
        assert_eq!(got.invoice.as_deref(), Some("inv"));
        assert_eq!(got.expiration_timestamp, Some(1234));
        assert_eq!(got.created_at, 100);
        assert_eq!(got.updated_at, 100);
        assert_eq!(got.last_seen_at, None);
        assert_eq!(got.settled_at, None);
        assert!(store.get_transfer("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pending_returns_only_waiting() {
        let store = Store::open_in_memory().await.unwrap();
        for s in [
            TransferStatus::Initiated,
            TransferStatus::WaitingCounterparty,
            TransferStatus::Settled,
        ] {
            store.insert_transfer(&new_transfer(s), 1).await.unwrap();
        }
        let pending = store.pending_transfers().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].status, TransferStatus::WaitingCounterparty);
    }

    #[tokio::test]
    async fn pending_asset_ids_reports_agnostic_rows() {
        let store = Store::open_in_memory().await.unwrap();
        let mut a = new_transfer(TransferStatus::WaitingCounterparty);
        a.asset_id = Some("assetA".into());
        store.insert_transfer(&a, 1).await.unwrap();
        let (ids, agnostic) = store.pending_asset_ids().await.unwrap();
        assert_eq!(ids, vec!["assetA".to_string()]);
        assert!(!agnostic);
        store
            .insert_transfer(&new_transfer(TransferStatus::WaitingSafeHeight), 1)
            .await
            .unwrap();
        let (ids, agnostic) = store.pending_asset_ids().await.unwrap();
        assert_eq!(ids, vec!["assetA".to_string()]);
        assert!(agnostic);
    }

    #[tokio::test]
    async fn upsert_observed_merges_by_rln_idx() {
        let store = Store::open_in_memory().await.unwrap();
        let first = store.upsert_observed(&observed(5), 10).await.unwrap();
        let mut o = observed(5);
        o.txid = Some("tx".into());
        o.expiration_timestamp = Some(999);
        let second = store.upsert_observed(&o, 20).await.unwrap();
        assert_eq!(first.id, second.id);
        assert_eq!(second.last_seen_at, Some(20));
        assert_eq!(second.txid.as_deref(), Some("tx"));
        assert_eq!(second.expiration_timestamp, Some(999));
        assert_eq!(second.created_at, 10);
        assert_eq!(count_transfers(&store).await, 1);
    }

    #[tokio::test]
    async fn upsert_observed_merges_by_recipient_id() {
        let store = Store::open_in_memory().await.unwrap();
        let mut n = new_transfer(TransferStatus::WaitingCounterparty);
        n.recipient_id = Some("rid".into());
        n.invoice = Some("inv".into());
        n.batch_transfer_idx = Some(3);
        let inserted = store.insert_transfer(&n, 1).await.unwrap();
        let mut o = observed(9);
        o.recipient_id = Some("rid".into());
        let merged = store.upsert_observed(&o, 2).await.unwrap();
        assert_eq!(merged.id, inserted.id);
        assert_eq!(merged.rln_idx, Some(9));
        assert_eq!(merged.asset_id.as_deref(), Some("assetA"));
        assert_eq!(merged.kind.as_deref(), Some("ReceiveBlind"));
        assert_eq!(merged.invoice.as_deref(), Some("inv"));
        assert_eq!(merged.batch_transfer_idx, Some(3));
        assert_eq!(count_transfers(&store).await, 1);
    }

    #[tokio::test]
    async fn upsert_observed_merges_by_txid_and_asset() {
        let store = Store::open_in_memory().await.unwrap();
        let mut n = new_transfer(TransferStatus::Initiated);
        n.asset_id = Some("assetA".into());
        n.txid = Some("tx".into());
        let inserted = store.insert_transfer(&n, 1).await.unwrap();
        let mut other = observed(1);
        other.asset_id = Some("assetB".into());
        other.txid = Some("tx".into());
        let unrelated = store.upsert_observed(&other, 2).await.unwrap();
        assert_ne!(unrelated.id, inserted.id);
        let mut o = observed(2);
        o.txid = Some("tx".into());
        let merged = store.upsert_observed(&o, 3).await.unwrap();
        assert_eq!(merged.id, inserted.id);
        assert_eq!(merged.rln_idx, Some(2));
        assert_eq!(count_transfers(&store).await, 2);
    }

    #[tokio::test]
    async fn upsert_observed_assetless_keeps_known_asset() {
        let store = Store::open_in_memory().await.unwrap();
        store.upsert_observed(&observed(5), 1).await.unwrap();
        let mut o = observed(5);
        o.asset_id = None;
        let t = store.upsert_observed(&o, 2).await.unwrap();
        assert_eq!(t.asset_id.as_deref(), Some("assetA"));
        let mut n = observed(6);
        n.asset_id = None;
        let t = store.upsert_observed(&n, 3).await.unwrap();
        assert_eq!(t.asset_id, None);
        assert_eq!(count_transfers(&store).await, 2);
    }

    #[tokio::test]
    async fn upsert_observed_inserts_unknown() {
        let store = Store::open_in_memory().await.unwrap();
        let mut o = observed(4);
        o.status = TransferStatus::Settled;
        let t = store.upsert_observed(&o, 50).await.unwrap();
        assert_eq!(t.status, TransferStatus::Settled);
        assert_eq!(t.rln_idx, Some(4));
        assert_eq!(t.created_at, 50);
        assert_eq!(t.updated_at, 50);
        assert_eq!(t.last_seen_at, Some(50));
        assert_eq!(count_transfers(&store).await, 1);
    }

    #[tokio::test]
    async fn upsert_observed_does_not_change_status_of_existing() {
        let store = Store::open_in_memory().await.unwrap();
        store.upsert_observed(&observed(6), 1).await.unwrap();
        let mut o = observed(6);
        o.status = TransferStatus::Settled;
        let t = store.upsert_observed(&o, 2).await.unwrap();
        assert_eq!(t.status, TransferStatus::WaitingCounterparty);
    }

    #[tokio::test]
    async fn transition_writes_outbox_event() {
        let store = Store::open_in_memory().await.unwrap();
        let t = store
            .insert_transfer(&new_transfer(TransferStatus::WaitingConfirmations), 1)
            .await
            .unwrap();
        let changed = store
            .apply_transition(
                &t.id,
                TransferStatus::WaitingConfirmations,
                TransferStatus::Settled,
                Some(("evt-1", "transfer.settled", "{\"id\":1}")),
                42,
            )
            .await
            .unwrap();
        assert!(changed);
        let got = store.get_transfer(&t.id).await.unwrap().unwrap();
        assert_eq!(got.status, TransferStatus::Settled);
        assert_eq!(got.settled_at, Some(42));
        assert_eq!(got.updated_at, 42);
        let events = store.undelivered_events(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, "evt-1");
        assert_eq!(events[0].event_type, "transfer.settled");
        assert_eq!(events[0].payload, "{\"id\":1}");
        assert_eq!(events[0].next_attempt_at, 42);
        assert_eq!(events[0].attempts, 0);
    }

    #[tokio::test]
    async fn transition_is_idempotent() {
        let store = Store::open_in_memory().await.unwrap();
        let t = store
            .insert_transfer(&new_transfer(TransferStatus::WaitingConfirmations), 1)
            .await
            .unwrap();
        let ev = Some(("evt-1", "transfer.settled", "{}"));
        let (from, to) = (
            TransferStatus::WaitingConfirmations,
            TransferStatus::Settled,
        );
        assert!(store
            .apply_transition(&t.id, from, to, ev, 5)
            .await
            .unwrap());
        assert!(!store
            .apply_transition(&t.id, from, to, ev, 6)
            .await
            .unwrap());
        assert_eq!(store.undelivered_events(10).await.unwrap().len(), 1);
        let got = store.get_transfer(&t.id).await.unwrap().unwrap();
        assert_eq!(got.settled_at, Some(5));
    }

    #[tokio::test]
    async fn transition_without_event_writes_no_outbox() {
        let store = Store::open_in_memory().await.unwrap();
        let t = store
            .insert_transfer(&new_transfer(TransferStatus::Initiated), 1)
            .await
            .unwrap();
        assert!(store
            .apply_transition(
                &t.id,
                TransferStatus::Initiated,
                TransferStatus::WaitingCounterparty,
                None,
                2
            )
            .await
            .unwrap());
        let got = store.get_transfer(&t.id).await.unwrap().unwrap();
        assert_eq!(got.status, TransferStatus::WaitingCounterparty);
        assert_eq!(got.settled_at, None);
        assert!(store.undelivered_events(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn transition_with_stale_from_is_noop() {
        let store = Store::open_in_memory().await.unwrap();
        let t = store
            .insert_transfer(&new_transfer(TransferStatus::Settled), 1)
            .await
            .unwrap();
        let changed = store
            .apply_transition(
                &t.id,
                TransferStatus::WaitingCounterparty,
                TransferStatus::WaitingConfirmations,
                Some(("evt-1", "transfer.confirmed_pending", "{}")),
                2,
            )
            .await
            .unwrap();
        assert!(!changed);
        let got = store.get_transfer(&t.id).await.unwrap().unwrap();
        assert_eq!(got.status, TransferStatus::Settled);
        assert_eq!(got.updated_at, 1);
        assert!(store.undelivered_events(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn assets_upsert_and_list() {
        let store = Store::open_in_memory().await.unwrap();
        store
            .upsert_assets(&[("a1".into(), "Nia".into())], 1)
            .await
            .unwrap();
        store
            .upsert_assets(
                &[("a1".into(), "Cfa".into()), ("a2".into(), "Uda".into())],
                2,
            )
            .await
            .unwrap();
        let assets = store.list_assets().await.unwrap();
        assert_eq!(
            assets,
            vec![
                ("a1".to_string(), "Cfa".to_string()),
                ("a2".to_string(), "Uda".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn expired_fallible_filters_correctly() {
        let store = Store::open_in_memory().await.unwrap();
        let mut hit = observed(1);
        hit.expiration_timestamp = Some(50);
        let hit = store.upsert_observed(&hit, 1).await.unwrap();
        let mut settled = observed(2);
        settled.status = TransferStatus::Settled;
        settled.expiration_timestamp = Some(50);
        store.upsert_observed(&settled, 1).await.unwrap();
        let mut fresh = observed(3);
        fresh.expiration_timestamp = Some(200);
        store.upsert_observed(&fresh, 1).await.unwrap();
        store.upsert_observed(&observed(4), 1).await.unwrap();
        let expired = store.expired_fallible(100).await.unwrap();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].id, hit.id);
    }

    #[tokio::test]
    async fn node_state_roundtrip() {
        let store = Store::open_in_memory().await.unwrap();
        assert_eq!(store.node_state().await.unwrap(), NodeState::Unknown);
        store.set_node_state(NodeState::Unlocked, 7).await.unwrap();
        assert_eq!(store.node_state().await.unwrap(), NodeState::Unlocked);
    }

    #[tokio::test]
    async fn last_full_sync_at_roundtrip() {
        let store = Store::open_in_memory().await.unwrap();
        assert_eq!(store.last_full_sync_at().await.unwrap(), None);
        store.set_last_full_sync_at(7).await.unwrap();
        assert_eq!(store.last_full_sync_at().await.unwrap(), Some(7));
        store.set_last_full_sync_at(9).await.unwrap();
        assert_eq!(store.last_full_sync_at().await.unwrap(), Some(9));
        store.set_node_state(NodeState::Locked, 10).await.unwrap();
        assert_eq!(store.last_full_sync_at().await.unwrap(), Some(9));
    }

    #[tokio::test]
    async fn parked_events_count_uses_max_attempts() {
        let store = Store::open_in_memory().await.unwrap();
        let t = store
            .insert_transfer(&new_transfer(TransferStatus::Initiated), 1)
            .await
            .unwrap();
        store
            .apply_transition(
                &t.id,
                TransferStatus::Initiated,
                TransferStatus::Failed,
                Some(("evt-1", "transfer.failed", "{}")),
                1,
            )
            .await
            .unwrap();
        assert_eq!(store.parked_events_count(1).await.unwrap(), 0);
        store.record_attempt("evt-1", 2).await.unwrap();
        assert_eq!(store.parked_events_count(1).await.unwrap(), 1);
        assert_eq!(store.parked_events_count(2).await.unwrap(), 0);
        store.mark_delivered("evt-1", 3).await.unwrap();
        assert_eq!(store.parked_events_count(1).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn undelivered_events_filters_attempts_and_delivered() {
        let store = Store::open_in_memory().await.unwrap();
        let t = store
            .insert_transfer(&new_transfer(TransferStatus::Initiated), 1)
            .await
            .unwrap();
        store
            .apply_transition(
                &t.id,
                TransferStatus::Initiated,
                TransferStatus::Failed,
                Some(("evt-1", "transfer.failed", "{}")),
                10,
            )
            .await
            .unwrap();
        let due = store.undelivered_events(3).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].next_attempt_at, 10);
        let id = due[0].id.clone();

        store.record_attempt(&id, 20).await.unwrap();
        let due = store.undelivered_events(3).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].attempts, 1);
        assert_eq!(due[0].next_attempt_at, 20);

        store.record_attempt(&id, 20).await.unwrap();
        store.record_attempt(&id, 20).await.unwrap();
        assert!(store.undelivered_events(3).await.unwrap().is_empty());
        assert_eq!(store.undelivered_events(4).await.unwrap().len(), 1);

        store.mark_delivered(&id, 30).await.unwrap();
        assert!(store.undelivered_events(100).await.unwrap().is_empty());
    }

    fn new_payment(hash: &str, status: PaymentStatus) -> NewPayment {
        NewPayment {
            payment_hash: hash.into(),
            direction: PaymentDirection::Outbound,
            status,
            asset_id: Some("assetA".into()),
            asset_amount: Some(10),
            amt_msat: Some(3000),
            payee_pubkey: Some("02aa".into()),
        }
    }

    #[tokio::test]
    async fn payment_insert_and_get_roundtrip() {
        let store = Store::open_in_memory().await.unwrap();
        let p = store
            .insert_pending_payment(&new_payment("h1", PaymentStatus::Pending), 100)
            .await
            .unwrap();
        assert_eq!(p.payment_hash, "h1");
        assert_eq!(p.direction, PaymentDirection::Outbound);
        assert_eq!(p.status, PaymentStatus::Pending);
        assert_eq!(p.asset_id.as_deref(), Some("assetA"));
        assert_eq!(p.asset_amount, Some(10));
        assert_eq!(p.amt_msat, Some(3000));
        assert_eq!(p.payee_pubkey.as_deref(), Some("02aa"));
        assert_eq!(p.created_at, 100);
        assert_eq!(p.updated_at, 100);
        assert_eq!(p.last_seen_at, None);
        let got = store.get_payment("h1").await.unwrap().unwrap();
        assert_eq!(got.status, PaymentStatus::Pending);
        assert_eq!(got.amt_msat, Some(3000));
        assert!(store.get_payment("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn insert_pending_payment_conflict_keeps_status() {
        let store = Store::open_in_memory().await.unwrap();
        store
            .insert_pending_payment(&new_payment("h1", PaymentStatus::Pending), 1)
            .await
            .unwrap();
        let mut again = new_payment("h1", PaymentStatus::Succeeded);
        again.amt_msat = Some(4000);
        again.asset_id = None;
        let p = store.insert_pending_payment(&again, 2).await.unwrap();
        assert_eq!(p.status, PaymentStatus::Pending);
        assert_eq!(p.amt_msat, Some(4000));
        assert_eq!(p.asset_id.as_deref(), Some("assetA"));
        assert_eq!(p.created_at, 1);
        assert_eq!(store.list_payments(None, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn upsert_payment_observed_inserts_and_merges_without_status() {
        let store = Store::open_in_memory().await.unwrap();
        let p = store
            .upsert_payment_observed(&new_payment("h1", PaymentStatus::Pending), 5)
            .await
            .unwrap();
        assert_eq!(p.last_seen_at, Some(5));
        assert_eq!(p.created_at, 5);
        let mut o = new_payment("h1", PaymentStatus::Succeeded);
        o.amt_msat = None;
        o.asset_id = None;
        let p = store.upsert_payment_observed(&o, 9).await.unwrap();
        assert_eq!(p.status, PaymentStatus::Pending);
        assert_eq!(p.last_seen_at, Some(9));
        assert_eq!(p.amt_msat, Some(3000));
        assert_eq!(p.asset_id.as_deref(), Some("assetA"));
        assert_eq!(p.created_at, 5);
        assert_eq!(store.list_payments(None, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn upsert_payment_observed_merges_into_interceptor_row() {
        let store = Store::open_in_memory().await.unwrap();
        store
            .insert_pending_payment(&new_payment("h1", PaymentStatus::Pending), 1)
            .await
            .unwrap();
        let p = store
            .upsert_payment_observed(&new_payment("h1", PaymentStatus::Succeeded), 2)
            .await
            .unwrap();
        assert_eq!(p.status, PaymentStatus::Pending);
        assert_eq!(p.last_seen_at, Some(2));
        assert_eq!(p.created_at, 1);
        assert_eq!(store.list_payments(None, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn payment_transition_writes_outbox_event() {
        let store = Store::open_in_memory().await.unwrap();
        store
            .insert_pending_payment(&new_payment("h1", PaymentStatus::Pending), 1)
            .await
            .unwrap();
        let changed = store
            .apply_payment_transition(
                "h1",
                PaymentStatus::Pending,
                PaymentStatus::Succeeded,
                Some(("pev-1", "payment.settled", "{\"x\":1}")),
                42,
            )
            .await
            .unwrap();
        assert!(changed);
        let p = store.get_payment("h1").await.unwrap().unwrap();
        assert_eq!(p.status, PaymentStatus::Succeeded);
        assert_eq!(p.updated_at, 42);
        let events = store.undelivered_events(10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, "pev-1");
        assert_eq!(events[0].event_type, "payment.settled");
        assert_eq!(events[0].payload, "{\"x\":1}");
        assert_eq!(events[0].next_attempt_at, 42);
    }

    #[tokio::test]
    async fn payment_transition_idempotent_and_stale_from_noop() {
        let store = Store::open_in_memory().await.unwrap();
        store
            .insert_pending_payment(&new_payment("h1", PaymentStatus::Pending), 1)
            .await
            .unwrap();
        let ev = Some(("pev-1", "payment.settled", "{}"));
        assert!(store
            .apply_payment_transition(
                "h1",
                PaymentStatus::Pending,
                PaymentStatus::Succeeded,
                ev,
                5
            )
            .await
            .unwrap());
        assert!(!store
            .apply_payment_transition(
                "h1",
                PaymentStatus::Pending,
                PaymentStatus::Succeeded,
                ev,
                6
            )
            .await
            .unwrap());
        assert!(!store
            .apply_payment_transition(
                "h1",
                PaymentStatus::Claimable,
                PaymentStatus::Failed,
                Some(("pev-2", "payment.failed", "{}")),
                7
            )
            .await
            .unwrap());
        assert_eq!(store.undelivered_events(10).await.unwrap().len(), 1);
        let p = store.get_payment("h1").await.unwrap().unwrap();
        assert_eq!(p.status, PaymentStatus::Succeeded);
        assert_eq!(p.updated_at, 5);
    }

    #[tokio::test]
    async fn pending_payments_returns_only_non_terminal() {
        let store = Store::open_in_memory().await.unwrap();
        for (hash, status) in [
            ("h1", PaymentStatus::Pending),
            ("h2", PaymentStatus::Claimable),
            ("h3", PaymentStatus::Claiming),
            ("h4", PaymentStatus::Succeeded),
            ("h5", PaymentStatus::Failed),
            ("h6", PaymentStatus::Cancelled),
        ] {
            store
                .insert_pending_payment(&new_payment(hash, status), 1)
                .await
                .unwrap();
        }
        let hashes: Vec<String> = store
            .pending_payments()
            .await
            .unwrap()
            .into_iter()
            .map(|p| p.payment_hash)
            .collect();
        assert_eq!(hashes, vec!["h1", "h2", "h3"]);
    }

    #[tokio::test]
    async fn payments_baseline_roundtrip_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("companion.sqlite");
        let path = path.to_str().unwrap();
        let store = Store::open(path).await.unwrap();
        assert_eq!(store.payments_baseline_at().await.unwrap(), None);
        store.set_payments_baseline_at(7).await.unwrap();
        assert_eq!(store.payments_baseline_at().await.unwrap(), Some(7));
        store.close().await;
        let reopened = Store::open(path).await.unwrap();
        assert_eq!(reopened.payments_baseline_at().await.unwrap(), Some(7));
        reopened.set_payments_baseline_at(9).await.unwrap();
        assert_eq!(reopened.payments_baseline_at().await.unwrap(), Some(9));
    }

    #[tokio::test]
    async fn list_payments_newest_first_filter_and_limit() {
        let store = Store::open_in_memory().await.unwrap();
        for (hash, status, at) in [
            ("h1", PaymentStatus::Pending, 1),
            ("h2", PaymentStatus::Succeeded, 2),
            ("h3", PaymentStatus::Pending, 3),
        ] {
            store
                .insert_pending_payment(&new_payment(hash, status), at)
                .await
                .unwrap();
        }
        let hashes = |v: Vec<Payment>| v.into_iter().map(|p| p.payment_hash).collect::<Vec<_>>();
        assert_eq!(
            hashes(store.list_payments(None, 10).await.unwrap()),
            vec!["h3", "h2", "h1"]
        );
        assert_eq!(
            hashes(
                store
                    .list_payments(Some(PaymentStatus::Pending), 10)
                    .await
                    .unwrap()
            ),
            vec!["h3", "h1"]
        );
        assert_eq!(
            hashes(store.list_payments(None, 2).await.unwrap()),
            vec!["h3", "h2"]
        );
    }

    #[tokio::test]
    async fn undelivered_events_keeps_insertion_order_and_limits() {
        let store = Store::open_in_memory().await.unwrap();
        for i in 0..101 {
            let t = store
                .insert_transfer(&new_transfer(TransferStatus::Initiated), 1)
                .await
                .unwrap();
            store
                .apply_transition(
                    &t.id,
                    TransferStatus::Initiated,
                    TransferStatus::Failed,
                    Some((&format!("evt-{i}"), "transfer.failed", "{}")),
                    10,
                )
                .await
                .unwrap();
        }
        let due = store.undelivered_events(10).await.unwrap();
        assert_eq!(due.len(), 100);
        let ids: Vec<String> = due.iter().map(|e| e.id.clone()).collect();
        assert_eq!(ids[0], "evt-0");
        assert_eq!(ids[99], "evt-99");
        store.record_attempt(&ids[2], 5).await.unwrap();
        store.record_attempt(&ids[0], 500).await.unwrap();
        let again: Vec<String> = store
            .undelivered_events(10)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.id)
            .collect();
        assert_eq!(again, ids);
    }
}
