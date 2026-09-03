# Manual vs companion demo

Two RLN nodes on regtest, side by side. Node1 has no companion: its operator must drive `/refreshtransfers` by hand. Node2 sits behind rln-companion: refreshes and signed webhooks happen on their own.

The payment is a non-donation `/sendrgb`, which RLN does not broadcast until the sender refreshes. The audience sees the sender stuck (empty mempool, `WaitingCounterparty`) until a manual `curl`, while the receiver side settles automatically and the webhook pane prints `transfer.confirmed_pending` -> `transfer.settled`, HMAC verified.

Flow: setup unlocks both nodes (node1 direct, node2 through the companion), then the driver shows the proxy in action -> funds wallets -> issues 1000 DEMO -> blind invoice via the companion -> `sendrgb` with `donation: false` -> manual refresh #1 broadcasts -> mine 1 block -> node2 settles alone (webhooks) -> manual refresh #2 settles node1 -> on-chain takeaway.

Act 2, Lightning: node2 opens a 300 DEMO channel to node1 through the companion -> mine 6, the funding transfer settles itself and the companion still webhooks it -> keysend 100 DEMO node2 -> node1 (`payment.settled` Outbound in the sink, node1 polls `/listpayments` by hand) -> LN invoice for 25 DEMO paid back from node1 (`payment.settled` Inbound) -> final takeaway.

## Prerequisites

- docker compose, tmux, jq, curl, python3, cargo
- RLN binary at `../../rgb-lightning-node/dev/target/debug/rgb-lightning-node` (override with `RLN_BIN`)
- free ports: 18463, 50021, 3020, 3021, 3022, 3121, 9921, 9821, 9822

## Run

```
./run.sh              # interactive: attaches to the tmux session, driver waits for [enter]
                      # setup also inits + unlocks both nodes: ~1 extra minute before the session appears
DEMO_AUTO=1 ./run.sh  # unattended: session created detached, driver paces itself
./stop.sh             # teardown: tmux session, nodes, companion, containers, tmp/
```

## Pane map

```
+---------------------+------------------------------+
| DRIVER              | WEBHOOKS (node2, automatic)  |
| narrated steps      | signed events from companion |
+---------------------+------------------------------+
| NODE1 no companion  | NODE2 behind companion       |
| transfers + mempool | health + transfer mirror     |
+---------------------+------------------------------+
```

Runtime state lives in `tmp/` (gitignored): node data dirs, logs, companion sqlite, bitcoind/electrs volumes.

Set `NO_COLOR=1` to disable the ANSI styling in every pane.
