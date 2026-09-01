# Operations

## Startup

The companion probes the node once (a rejected `rln.token` is fatal; a locked or unreachable node is not), binds and serves right away, and runs a reconcile in the background: a full sync of assets and transfers, retried while the node is locked or unreachable for up to `engine.reconcile_max_wait_secs`. A fresh node can be initialised and unlocked through the companion while that wait is going on.

If the reconcile gives up, the mirror stays stale until the periodic full sync (`sync.full_interval_secs`) runs against an unlocked node, or `/unlock` goes through the companion, which triggers a reconcile immediately. The refresh loop itself only re-probes node state and syncs pending rows.

## Node state

The companion tracks whether the node is `unknown`, `locked`, `unlocked` or `down` from the lifecycle routes it proxies and from 403 answers that reveal the state. While the node is not `unlocked`, all background work pauses and the node is re-probed on every refresh interval, so an outage of any length is recovered automatically once the node is back.

## The single-flight lock

Every call that changes transfer state on the node goes through one lock: the background refresh, the reaper, the full sync, and an operator's own `/refreshtransfers` or `/failtransfers`. RLN itself serializes refresh behind a global mutex and does not cancel it when the client disconnects, so this is what keeps the companion from piling refreshes onto the node. The cost: a hung refresh holds the lock for up to `rln.proxy_timeout_secs` and delays the reaper, the full sync and other refreshes. `/companion/health` and mirror reads are never affected.

## Shutdown

SIGINT and SIGTERM stop the server gracefully, abort the background tasks and close the database (WAL checkpoint). `docker compose stop` takes well under a second.

## Database

SQLite, WAL mode, file created with mode 0600. Migration files are immutable once a version has been deployed; editing one makes existing databases fail sqlx's checksum on startup.

## Known windows

- A `/rgbinvoice` row can only be inserted after RLN answered (the identifiers come from the response). If the companion dies in between, the next full sync recovers the transfer but without `batch_transfer_idx`, so the reaper cannot fail it if it expires.
- Two identical `/sendrgb` calls for the same asset and txid create two mirror rows; only one is ever bound to RLN's transfer.
