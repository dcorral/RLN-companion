# Configuration

Config is a TOML file loaded with `--config <path>`. Every key has a default (`sample-config.toml` lists them all and must stay equal to the defaults, a unit test enforces it) and unknown keys are rejected. `webhook.url` and `webhook.secret` are required.

| Key | Default | Meaning |
| --- | --- | --- |
| `service.listen_port` | `3101` | Port the companion API listens on (all interfaces) |
| `service.auth_token` | unset | Bearer token required on `/companion/*` calls except health; unset means no auth |
| `rln.base_url` | `http://127.0.0.1:3001` | Base URL of the RLN node API; must be private unless `allow_public_url` is set |
| `rln.token` | unset | Bearer token the companion sends on its own RLN calls; unset means none |
| `rln.network` | unset | Bitcoin network the node must report on `/networkinfo` (`Mainnet`, `Testnet`, `Testnet4`, `Signet`, `Regtest`, compared case-insensitively); checked on every successful probe, a mismatch is fatal at startup; unset means no check |
| `rln.request_timeout_secs` | `120` | Timeout for companion-initiated RLN calls |
| `rln.proxy_timeout_secs` | `600` | Timeout for proxied client calls to RLN |
| `rln.proxy_max_body_mb` | `64` | Maximum proxied request body size in MiB; larger bodies get 413 |
| `rln.allow_public_url` | `false` | Allow a non-private RLN host |
| `engine.refresh_interval_secs` | `10` | How often `/refreshtransfers` is driven while transfers are pending |
| `engine.skip_sync` | `false` | `skip_sync` value sent on the companion's `/refreshtransfers` calls; keep `false` when witness receives are used |
| `engine.reap_interval_secs` | `300` | How often expired transfers are failed through `/failtransfers` |
| `engine.payments_poll_interval_secs` | `3` | How often `/listpayments` is polled to mirror LN payment state; inbound payments arrive over P2P at any moment, so the poll runs constantly while the node is unlocked (it is a cheap read on RLN) |
| `engine.reconcile_backoff_secs` | `5` | Wait between reconcile attempts and re-probes while the node is locked or unreachable |
| `engine.reconcile_max_wait_secs` | `600` | Maximum time the startup reconcile waits for the node before giving up |
| `sync.full_interval_secs` | `600` | How often a full transfer sync from RLN runs |
| `sync.page_size` | `100` | Page size for RLN list calls (full syncs use at least 1000) |
| `webhook.url` | required | Operator endpoint receiving transfer events (`http` or `https`) |
| `webhook.secret` | required | HMAC secret used to sign webhook deliveries |
| `webhook.max_attempts` | `10` | Delivery attempts before an event is parked |
| `webhook.backoff_base_secs` | `2` | Base of the exponential retry backoff |
| `webhook.backoff_cap_secs` | `300` | Cap of the exponential retry backoff |
| `webhook.dispatch_interval_secs` | `2` | How often pending webhooks are dispatched |
| `database.path` | `companion.sqlite` | SQLite database file (created with mode 0600) |

Validation at startup rejects: an empty webhook url or secret, a webhook url that is not `http`/`https`, a listen port of 0, any interval set to 0, an RLN url that is not `http`/`https`, and an RLN host that is not loopback, a private range, `localhost` or a bare hostname (Docker service names count as private) unless `rln.allow_public_url = true`.

## CLI

Flags override the file:

| Flag | Overrides |
| --- | --- |
| `--config <path>` | Which file to load |
| `--listen-port <port>` | `service.listen_port` |
| `--rln-url <url>` | `rln.base_url` |
| `--db-path <path>` | `database.path` |
| `--openapi <path>` | RLN `openapi.yaml` to serve at `/companion/openapi.yaml` |

Logging uses `RUST_LOG` (default `info`). Request logs carry method, path, status and elapsed time only; never bodies or headers.
