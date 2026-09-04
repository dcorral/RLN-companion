# Testing

## Unit and integration

`cargo test` runs everything against mocked RLN servers; no external services are needed. Clippy runs with `unwrap_used` and `expect_used` denied outside tests.

## Regtest end-to-end

`./e2e/run.sh` runs the regtest suite against real RLN nodes. It needs docker (compose v2), `curl`, and a debug build of RLN at `RLN_BIN` (default `../rgb-lightning-node/dev/target/debug/rgb-lightning-node`). It brings up bitcoind, electrs and the RGB proxy from `e2e/compose.yaml`, starts two RLN nodes on the host, runs `cargo test --test e2e -- --ignored --test-threads=1` with two in-process companions in front of them, and tears everything down. Extra arguments are passed to `cargo test`, so `./e2e/run.sh receive_flow` runs one scenario. Node logs land in `e2e-tmp/node<n>.out`.

Host ports can be moved with `E2E_BITCOIND_PORT`, `E2E_ELECTRS_PORT`, `E2E_PROXY_PORT`, `E2E_NODE1_PORT`, `E2E_NODE2_PORT`, `E2E_PEER1_PORT` and `E2E_PEER2_PORT`. Use `127.0.0.1` everywhere; RLN's e2e is known to break on `localhost` resolving to `::1`.

The harness only talks to the companions (one direct RLN call exists, in the parity test, by design). The six scenarios prove:

| Scenario | Proves |
| --- | --- |
| `receive_flow_settles_with_webhooks` | A donation send between the two nodes yields `transfer.confirmed_pending` then `transfer.settled` on the receiver and `transfer.settled` on the sender; the mirror row matches RLN's own `/listtransfers` entry and the asset balance is credited |
| `non_donation_send_is_broadcast_by_sender_companion` | A non-donation send is not broadcast by `/sendrgb` itself; the sender's companion drives `/refreshtransfers` until the transaction appears in the mempool, and both sides settle after mining |
| `expired_invoice_is_reaped` | An invoice whose expiration passes is failed by the reaper through `/failtransfers` and produces `transfer.failed` |
| `companion_started_against_locked_node_recovers` | A companion started against a locked node reports `locked`/`degraded`, reconciles after `/unlock` through it, reports `ok`, and settles a transfer created before it existed |
| `lightning_payment_webhooks` | A colored channel opened through the companion settles its funding transfer with webhooks; an RGB keysend and an LN invoice paid back over it produce `payment.settled` in both directions on both companions, `/lninvoice` pre-tracks the inbound row as `Pending` before it is paid, and `pending_payments` drains to 0 |
| `pass_through_parity` | `/nodeinfo`, `/listassets`, `/btcbalance` and `/listunspents` return byte-identical bodies through the companion and directly from RLN |

## CI

`.github/workflows/ci.yaml` runs three jobs on every push and pull request: fmt, clippy and `cargo test`; the e2e suite against a pinned RLN commit; and a smoke test of `deploy/compose.yaml` (health answers, node port not published). The two docker jobs build RLN from source and are slow on a cold cache.
