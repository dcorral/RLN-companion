CREATE TABLE payments (
    payment_hash TEXT PRIMARY KEY,
    direction TEXT NOT NULL,
    status TEXT NOT NULL,
    asset_id TEXT,
    asset_amount INTEGER,
    amt_msat INTEGER,
    payee_pubkey TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    last_seen_at INTEGER
);
CREATE INDEX idx_payments_status ON payments (status);

ALTER TABLE node_state ADD COLUMN payments_baseline_at INTEGER;
