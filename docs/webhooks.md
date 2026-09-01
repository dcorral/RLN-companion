# Webhooks

Every transfer transition that carries an event is written to an outbox in the same database transaction as the transition, then delivered by a dispatcher that runs every `webhook.dispatch_interval_secs`.

## Delivery

`POST webhook.url` with:

| Header | Value |
| --- | --- |
| `content-type` | `application/json` |
| `x-companion-event-id` | The event id (same as `event_id` in the body) |
| `x-companion-signature` | Lowercase hex HMAC-SHA256 of the raw request body, keyed with `webhook.secret` |

Redirects are not followed. Any non-2xx status or transport error counts as a failed attempt.

## Payload

```json
{
  "event_id": "5c0f2c6e-1a4b-4f2d-9c3e-8b7a6d5e4f30",
  "event_type": "transfer.settled",
  "transfer": {
    "id": "2f9d8c1b-7e6a-4d5c-b3a2-1f0e9d8c7b6a",
    "rln_idx": 3,
    "asset_id": "rgb:2dkSTbr-jFhznbPmo-TQafzswCN-av4gTsJjX-ttx6CNou5-M98k8Zd",
    "kind": "ReceiveBlind",
    "status": "Settled",
    "recipient_id": "utxob:2FZsSxN-...",
    "txid": "9f0c3b1e...",
    "batch_transfer_idx": 3,
    "invoice": "rgb:~/~/utxob:2FZsSxN-...",
    "expiration_timestamp": 1756643600,
    "created_at": 1756640000,
    "updated_at": 1756640420,
    "last_seen_at": 1756640420,
    "settled_at": 1756640420
  },
  "previous_status": "WaitingConfirmations",
  "new_status": "Settled",
  "timestamp": 1756640420
}
```

`transfer` is the mirrored row after the transition; nullable fields are `null` when unknown. Timestamps are unix seconds.

## Event types

| Event | When |
| --- | --- |
| `transfer.confirmed_pending` | The transfer entered `WaitingConfirmations`: the transaction is broadcast and waiting for confirmations |
| `transfer.settled` | The transfer entered `Settled` |
| `transfer.failed` | The transfer entered `Failed`, either reported by RLN or because the companion failed an expired invoice through `/failtransfers` |

Transitions only move forward: an observation that is the same status or an earlier one is ignored, and a `Settled` or `Failed` row never changes again. Hops that skip a stage still fire the event for the stage reached, so a transfer can go straight from `WaitingCounterparty` to `transfer.settled` without a `transfer.confirmed_pending`.

## Guarantees

- **At-least-once.** A delivery that succeeded but whose acknowledgement was lost is retried, so dedupe on `event_id`.
- **Strict FIFO.** Events are sent in creation order and a failing event blocks everything behind it: the dispatcher retries it with exponential backoff (`backoff_base_secs * 2^(attempts-1)`, capped at `backoff_cap_secs`) until it succeeds or reaches `max_attempts`, at which point it is parked and the queue moves on.
- **Parked events are never retried automatically.** They are counted as `parked_events` in `/companion/health` and turn `status` to `degraded`; use `/companion/transfers` to recover the state they carried.

## Verifying a delivery

```
body      = raw request bytes, before any JSON parsing
expected  = hex(HMAC_SHA256(key = webhook.secret, message = body))
if not constant_time_equal(expected, header["x-companion-signature"]): reject
if header["x-companion-event-id"] already processed: acknowledge and skip
process(json(body)); remember event id; respond 2xx
```

Answer 2xx quickly and do slow work afterwards; the dispatcher waits up to 30 seconds per delivery.
