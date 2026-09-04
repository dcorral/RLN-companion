# Webhooks

Every transfer or payment transition that carries an event is written to an outbox in the same database transaction as the transition, then delivered by a dispatcher that runs every `webhook.dispatch_interval_secs`.

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

## Payment events

Lightning payments settle through the channel state machine, so the companion mirrors them by polling `/listpayments` (see `engine.payments_poll_interval_secs`) and fires an event when a mirrored payment reaches a terminal status:

| Event | When |
| --- | --- |
| `payment.settled` | The payment entered `Succeeded` |
| `payment.failed` | The payment entered `Failed` or `Cancelled` |

The payload has the same envelope with a `payment` object instead of `transfer`:

```json
{
  "event_id": "7a1e9d4c-2b5f-4e8a-bc3d-0f6e5a4b3c21",
  "event_type": "payment.settled",
  "payment": {
    "payment_hash": "9f0c3b1e...",
    "direction": "Inbound",
    "status": "Succeeded",
    "asset_id": "rgb:2dkSTbr-jFhznbPmo-TQafzswCN-av4gTsJjX-ttx6CNou5-M98k8Zd",
    "asset_amount": 42,
    "amt_msat": 3000000,
    "payee_pubkey": "02ab...",
    "created_at": 1756640000,
    "updated_at": 1756640420,
    "last_seen_at": 1756640420
  },
  "previous_status": "Pending",
  "new_status": "Succeeded",
  "timestamp": 1756640420
}
```

Payment statuses move forward too (`Pending` -> `Claimable` -> `Claiming` -> terminal); the intermediate hodl-invoice stages fire no event. RLN reuses the payment hash when a failed or cancelled payment is retried, so a `Failed` or `Cancelled` row observed `Pending` again restarts silently and the next terminal status emits its event again; a `Succeeded` row never changes. Only the companion's very first payments sync backfills silently: everything already on the node is mirrored without events and a baseline is stored. After that baseline, a payment discovered already terminal still emits its event, so an inbound keysend that is auto-claimed in milliseconds (faster than the poll ever sees it pending) is not missed. A payment hash used in both directions (a circular payment back to yourself) collapses to one mirrored row whose direction follows the latest observation.

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
