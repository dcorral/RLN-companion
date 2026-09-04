# Companion endpoints

Everything under `/companion/*` is answered by the companion itself from its mirror database; it never touches the node. Any other `/companion/*` path is 404 and is never proxied.

## `GET /companion/health`

```sh
curl -s 127.0.0.1:3101/companion/health
{"status":"ok","node":"unlocked","pending_transfers":1,"pending_payments":0,"parked_events":0,"last_full_sync_at":1756640000}
```

| Field | Meaning |
| --- | --- |
| `status` | `ok` only when `node` is `unlocked` and `parked_events` is 0, otherwise `degraded` |
| `node` | `unknown`, `locked`, `unlocked`, `down` or `misconfigured` (node reachable and unlocked but failing the `rln.network` check; background work is paused), as last observed |
| `pending_transfers` | Mirrored rows in a `Waiting*` status |
| `pending_payments` | Mirrored LN payments in a non-terminal status (`Pending`, `Claimable`, `Claiming`) |
| `parked_events` | Webhook events that exhausted `webhook.max_attempts` and are no longer retried |
| `last_full_sync_at` | Unix time of the last completed full sync, or `null` |

Health always answers 200 while the companion is serving and never requires a token. For an orchestrator, a 200 means "companion up", not "node ready": read `node`.

## `GET /companion/transfers`

Lists mirrored transfers, newest first, as `{"transfers":[...]}` using the same transfer object as webhook payloads.

| Query | Values |
| --- | --- |
| `status` | `Initiated`, `WaitingCounterparty`, `WaitingSafeHeight`, `WaitingBroadcast`, `WaitingConfirmations`, `Settled`, `Failed` |
| `asset_id` | Exact asset id |
| `limit` | Default 100, capped at 1000 |

Invalid values give 400.

## `GET /companion/transfers/{id}`

One transfer by companion id, or 404.

## `GET /companion/payments`

Lists mirrored LN payments, newest first, as `{"payments":[...]}` using the same payment object as webhook payloads.

| Query | Values |
| --- | --- |
| `status` | `Pending`, `Claimable`, `Claiming`, `Succeeded`, `Failed`, `Cancelled` |
| `limit` | Default 100, capped at 1000 |

Invalid values give 400.

## `GET /companion/openapi.yaml`

The RLN spec passed with `--openapi`, or 404 when none was given.

## Auth

When `service.auth_token` is set, `/companion/transfers`, `/companion/transfers/{id}`, `/companion/payments` and `/companion/openapi.yaml` require `Authorization: Bearer <token>` (scheme case-insensitive) and answer 401 otherwise. It guards `/companion/*` only, never the proxied RLN routes, which stay under RLN's own auth.

## Errors

Errors from the companion itself use RLN's error shape: `{"error":"...","code":<http status>,"name":"<ErrorName>"}`. Internal failures are reported as `internal error`; the cause goes to the companion's log.
