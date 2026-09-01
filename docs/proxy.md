# Proxy and intercepted routes

Any RLN route works through the companion. Most are proxied untouched; a fixed list is intercepted so the companion can keep its mirror and node state up to date.

## Transparent proxy

Method, path, query, headers and body go to RLN; its status, headers and body come back. Details:

- Hop-by-hop headers are stripped and `Host` is rewritten.
- `Authorization` is passed through as-is. RLN enforces its own auth; the companion never adds a token on the client's behalf.
- Redirects are not followed.
- A proxied 403 whose error name reveals the node state (`LockedNode`, `ChangingState`, `UnlockedNode`, `AlreadyUnlocked`) updates the companion's view of the node.
- Paths with empty, `.` or `..` segments, or any percent-encoding, are rejected with 400 before forwarding. RLN's HTTP client would normalize them after the router matched them literally, which would let a call slip past the interceptors; no RLN route needs encoding.
- Request bodies above `rln.proxy_max_body_mb` get 413. An unreachable node gives 502, a timeout 504 (`rln.proxy_timeout_secs`).

## Intercepted routes

The request is forwarded exactly like the proxy does. Only when RLN answers 2xx the companion reads the request and response bodies to update its mirror. The response reaches the client unchanged either way; a failing hook is logged and never alters it.

| Route | Effect |
| --- | --- |
| `POST /rgbinvoice` | Row inserted as `WaitingCounterparty` with kind `ReceiveBlind` or `ReceiveWitness`, the recipient id, invoice, batch transfer idx and expiration; refresh loop woken |
| `POST /sendrgb` | One row per asset in `recipient_map`, kind `Send`, status `WaitingConfirmations` for donations and `WaitingCounterparty` otherwise, with the txid; background sync of those assets; refresh loop woken |
| `POST /issueassetnia`, `/issueassetcfa`, `/issueassetuda`, `/issueassetifa` | Asset recorded with its schema; background sync of that asset |
| `POST /inflate` | Background sync of the asset |
| `POST /assetlink` | Background sync of the parent and child assets |
| `POST /refreshtransfers`, `POST /failtransfers` | Serialized with the refresh loop (the engine lock is held across the RLN call), then the pending transfers are re-synced from RLN |
| `POST /init`, `/restore`, `/lock` | Node state set to `locked` (RLN requires a locked node for init and restore and leaves it locked) |
| `POST /unlock` | Node state set to `unlocked`, then a full reconcile runs in the background |
| `POST /shutdown` | Node state set to `down` |

## Keeping the mirror right

RLN's `/refreshtransfers` answers with nothing useful about what changed, so the companion never trusts it: after every refresh it re-reads the pending transfers from `/listtransfers` (per asset, plus the asset-less listing for receives whose asset is not known yet) and diffs statuses. Status only moves forward; entering `WaitingConfirmations`, `Settled` or `Failed` produces a webhook event. A full sync of all assets and transfers runs at startup, after `/unlock`, and every `sync.full_interval_secs`.
