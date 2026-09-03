#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

COMPOSE="docker compose -f compose.yaml"
BITCOIN_CLI="$COMPOSE exec -T -u blits bitcoind bitcoin-cli -regtest"
RLN_BIN="${RLN_BIN:-../../rgb-lightning-node/dev/target/debug/rgb-lightning-node}"
COMPANION_BIN=../target/release/rln-companion
SESSION=rln-demo
TIMEOUT=120

[ -x "$RLN_BIN" ] || { echo "rln binary not found at $RLN_BIN (set RLN_BIN)"; exit 1; }

tmux kill-session -t "$SESSION" 2>/dev/null || true
pkill -f 'rgb-lightning-nod[e].*demo/tmp' 2>/dev/null || true
pkill -f 'rln-companio[n] --config companion.toml' 2>/dev/null || true
$COMPOSE down -v --remove-orphans 2>/dev/null || true
rm -rf tmp
mkdir -p tmp/datacore tmp/dataindex

wait_for() {
    local name=$1 deadline=$(( $(date +%s) + $2 ))
    shift 2
    until "$@" >/dev/null 2>&1; do
        if [ "$(date +%s)" -gt "$deadline" ]; then
            echo "timeout waiting for $name"
            return 1
        fi
        sleep 1
    done
}
bitcoind_ready() { $BITCOIN_CLI getblockchaininfo; }
electrs_ready() { $COMPOSE logs electrs 2>/dev/null | grep -q 'finished full compaction'; }
proxy_ready() { curl -s http://127.0.0.1:3020; }
node_ready() { curl -s -o /dev/null "http://127.0.0.1:$1/nodeinfo"; }
companion_ready() { curl -s -o /dev/null http://127.0.0.1:3121/companion/health; }

$COMPOSE up -d bitcoind
wait_for bitcoind $TIMEOUT bitcoind_ready
$BITCOIN_CLI createwallet miner >/dev/null
$BITCOIN_CLI -rpcwallet=miner -generate 103 >/dev/null
$COMPOSE up -d
wait_for electrs $TIMEOUT electrs_ready
wait_for proxy $TIMEOUT proxy_ready

start_node() {
    local n=$1 port=$2 peer=$3
    mkdir -p "tmp/node$n"
    "$RLN_BIN" "$PWD/tmp/node$n" \
        --daemon-listening-port "$port" --ldk-peer-listening-port "$peer" \
        --network regtest --disable-authentication >"tmp/node$n.out" 2>&1 &
    wait_for "node$n" $TIMEOUT node_ready "$port"
}
start_node 1 3021 9821
start_node 2 3022 9822

[ -x "$COMPANION_BIN" ] || (cd .. && cargo build --release)
"$COMPANION_BIN" --config companion.toml >tmp/companion.out 2>&1 &
wait_for companion $TIMEOUT companion_ready

INDEXER=127.0.0.1:50021
PROXY=rpc://127.0.0.1:3020/json-rpc
PASSWORD=demo-password-12
init_unlock() {
    local base=$1
    curl -s -X POST "$base/init" -H 'content-type: application/json' \
        -d "{\"password\":\"$PASSWORD\",\"mnemonic\":null}" >/dev/null
    curl -s -X POST "$base/unlock" -H 'content-type: application/json' \
        -d "{\"password\":\"$PASSWORD\",\"ldk_chain_sync\":{\"mode\":\"TransactionSync\",\"config\":{\"indexer_url\":\"$INDEXER\"}},\"indexer_url\":\"$INDEXER\",\"proxy_endpoint\":\"$PROXY\",\"announce_addresses\":[],\"announce_alias\":\"RLN_alias\",\"gossip_source\":null}" >/dev/null
}
node_unlocked() { curl -sf -o /dev/null "http://127.0.0.1:$1/nodeinfo"; }
companion_unlocked() { [ "$(curl -s http://127.0.0.1:3121/companion/health | jq -r .node)" = unlocked ]; }
echo "unlocking nodes, ~1 min... (node1 direct, node2 through the companion)"
init_unlock http://127.0.0.1:3021 &
U1=$!
init_unlock http://127.0.0.1:3121 &
U2=$!
wait "$U1" "$U2"
wait_for "node1 unlocked" 180 node_unlocked 3021
wait_for "node2 unlocked" 180 node_unlocked 3022
wait_for "companion sees node unlocked" 180 companion_unlocked
echo "both nodes unlocked"

tmux new-session -d -s "$SESSION" -n demo -x 220 -y 50 -c "$PWD"
tmux set-option -w -t "$SESSION:demo" pane-border-status top
P0=$(tmux display-message -p -t "$SESSION:demo.0" '#{pane_id}')
P1=$(tmux split-window -h -t "$P0" -c "$PWD" -P -F '#{pane_id}')
P2=$(tmux split-window -v -t "$P0" -c "$PWD" -P -F '#{pane_id}')
P3=$(tmux split-window -v -t "$P1" -c "$PWD" -P -F '#{pane_id}')
tmux select-pane -t "$P0" -T "DRIVER"
tmux select-pane -t "$P1" -T "WEBHOOKS (node2, automatic)"
tmux select-pane -t "$P2" -T "NODE1 no companion (manual)"
tmux select-pane -t "$P3" -T "NODE2 behind companion"
tmux send-keys -t "$P1" 'python3 webhook_sink.py' C-m
tmux send-keys -t "$P2" 'watch -n1 -c -t ./status_node1.sh' C-m
tmux send-keys -t "$P3" 'watch -n1 -c -t ./status_node2.sh' C-m
tmux send-keys -t "$P0" "DEMO_AUTO=${DEMO_AUTO:-0} ./driver.sh" C-m
tmux select-pane -t "$P0"

echo "demo session ready: tmux attach -t $SESSION (teardown: ./stop.sh)"
if [ "${DEMO_AUTO:-0}" != 1 ] && [ -t 0 ]; then
    tmux attach -t "$SESSION"
fi
