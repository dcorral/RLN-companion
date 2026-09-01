CREATE TABLE transfers (
    id TEXT PRIMARY KEY,
    rln_idx INTEGER UNIQUE,
    asset_id TEXT,
    kind TEXT,
    status TEXT NOT NULL,
    recipient_id TEXT UNIQUE,
    txid TEXT,
    batch_transfer_idx INTEGER,
    invoice TEXT,
    expiration_timestamp INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    last_seen_at INTEGER,
    settled_at INTEGER
);
CREATE INDEX idx_transfers_status ON transfers (status);
CREATE INDEX idx_transfers_txid ON transfers (txid, asset_id);

CREATE TABLE assets (
    asset_id TEXT PRIMARY KEY,
    schema TEXT NOT NULL,
    last_synced_at INTEGER
);

CREATE TABLE node_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    state TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    last_full_sync_at INTEGER
);
INSERT INTO node_state (id, state, updated_at) VALUES (1, 'Unknown', 0);

CREATE TABLE webhook_outbox (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL,
    delivered_at INTEGER
);
CREATE INDEX idx_outbox_due ON webhook_outbox (delivered_at, next_attempt_at);
