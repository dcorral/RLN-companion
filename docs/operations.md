# Operations

## Startup

The companion probes the node once (a misconfigured node is fatal; a locked or unreachable node is not), binds and serves right away, and runs a reconcile in the background: a full sync of assets and transfers, retried while the node is locked or unreachable for up to `engine.reconcile_max_wait_secs`. A fresh node can be initialised and unlocked through the companion while that wait is going on.

The probe calls `/nodeinfo` and, when `rln.network` is set and the node is unlocked, `/networkinfo`. It validates that the node accepts the companion's token and that the node runs the expected network. Fatal lines an operator can hit at startup, all prefixed with `node misconfigured: `:

- `node requires a Biscuit token: set rln.token` -> the node answered 401; it runs with Biscuit auth and `rln.token` is unset or invalid.
- `rln.token lacks the rights the companion needs (admin, or custom with /refreshtransfers, /failtransfers, /listtransfers, /listassets, /nodeinfo, /networkinfo)` -> the node answered 403 Forbidden; the token is valid but its rights do not cover the companion's own calls.
- `node network is Regtest, rln.network expects Testnet` -> the node reports a different network than configured; fix whichever side is wrong before the companion mirrors transfers from the wrong chain.

A locked node cannot report its network, so the network mismatch is only fatal when the companion starts against a node that is already unlocked. In the common case (node locked at start, unlocked later through the companion) the first reconcile after the unlock runs the same check: on a mismatch it logs the line above at `error` level, marks the node `misconfigured` (`/companion/health` reports `node: misconfigured` and `status: degraded`) and returns without syncing. Every runtime re-probe repeats the check and keeps the state at `misconfigured` while the mismatch lasts. All background work pauses in that state, exactly as for a locked node. The companion does not exit; once the node or `rln.network` is fixed and the node reports the expected network again, the next re-probe flips the state back to `unlocked`.

If the reconcile gives up, the mirror stays stale until the periodic full sync (`sync.full_interval_secs`) runs against an unlocked node, or `/unlock` goes through the companion, which triggers a reconcile immediately. The refresh loop itself only re-probes node state and syncs pending rows.

## Node state

The companion tracks whether the node is `unknown`, `locked`, `unlocked`, `down` or `misconfigured` from the lifecycle routes it proxies, from 403 answers that reveal the state and from the `rln.network` check. While the node is not `unlocked`, all background work pauses and the node is re-probed on every refresh interval, so an outage of any length is recovered automatically once the node is back.

## The single-flight lock

Every call that changes transfer state on the node goes through one lock: the background refresh, the reaper, the full sync, and an operator's own `/refreshtransfers` or `/failtransfers`. RLN itself serializes refresh behind a global mutex and does not cancel it when the client disconnects, so this is what keeps the companion from piling refreshes onto the node. The cost: a hung refresh holds the lock for up to `rln.proxy_timeout_secs` and delays the reaper, the full sync and other refreshes. `/companion/health` and mirror reads are never affected.

## Shutdown

SIGINT and SIGTERM stop the server gracefully, abort the background tasks and close the database (WAL checkpoint). `docker compose stop` takes well under a second.

## Database

SQLite, WAL mode, file created with mode 0600. Migration files are immutable once a version has been deployed; editing one makes existing databases fail sqlx's checksum on startup.

## Known windows

- A `/rgbinvoice` row can only be inserted after RLN answered (the identifiers come from the response). If the companion dies in between, the next full sync recovers the transfer but without `batch_transfer_idx`, so the reaper cannot fail it if it expires.
- Two identical `/sendrgb` calls for the same asset and txid create two mirror rows; only one is ever bound to RLN's transfer.
