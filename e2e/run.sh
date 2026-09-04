#!/usr/bin/env bash
# Regtest e2e: bitcoind + electrs + proxy in docker, two RLN nodes on the host, tests via companions.
set -euo pipefail
cd "$(dirname "$0")/.."

export E2E_BITCOIND_PORT="${E2E_BITCOIND_PORT:-18443}"
export E2E_ELECTRS_PORT="${E2E_ELECTRS_PORT:-50001}"
export E2E_PROXY_PORT="${E2E_PROXY_PORT:-3000}"
NODE1_PORT="${E2E_NODE1_PORT:-3001}"
NODE2_PORT="${E2E_NODE2_PORT:-3002}"
export E2E_PEER1_PORT="${E2E_PEER1_PORT:-9801}"
export E2E_PEER2_PORT="${E2E_PEER2_PORT:-9802}"

COMPOSE="docker compose -f e2e/compose.yaml"
BITCOIN_CLI="$COMPOSE exec -T -u blits bitcoind bitcoin-cli -regtest"
RLN_BIN="${RLN_BIN:-../rgb-lightning-node/dev/target/debug/rgb-lightning-node}"
TIMEOUT=120
NODE_PIDS=()

cleanup() {
    for pid in "${NODE_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    $COMPOSE down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

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
proxy_ready() { curl -s "http://127.0.0.1:$E2E_PROXY_PORT"; }
node_ready() { curl -s -o /dev/null "http://127.0.0.1:$1/nodeinfo"; }

start_node() {
    local n=$1 port=$2 peer=$3
    mkdir -p "e2e-tmp/node$n"
    "$RLN_BIN" "e2e-tmp/node$n" \
        --daemon-listening-port "$port" --ldk-peer-listening-port "$peer" \
        --network regtest --disable-authentication >"e2e-tmp/node$n.out" 2>&1 &
    NODE_PIDS+=("$!")
    wait_for "node$n" $TIMEOUT node_ready "$port"
}

[ -x "$RLN_BIN" ] || { echo "rln binary not found at $RLN_BIN (set RLN_BIN)"; exit 1; }

$COMPOSE down -v --remove-orphans
rm -rf e2e-tmp e2e/datacore e2e/dataindex
mkdir -p e2e-tmp e2e/datacore e2e/dataindex
$COMPOSE up -d bitcoind
wait_for bitcoind $TIMEOUT bitcoind_ready
$BITCOIN_CLI createwallet miner >/dev/null
$BITCOIN_CLI -rpcwallet=miner -generate 103 >/dev/null
$COMPOSE up -d
wait_for electrs $TIMEOUT electrs_ready
wait_for proxy $TIMEOUT proxy_ready

start_node 1 "$NODE1_PORT" "$E2E_PEER1_PORT"
start_node 2 "$NODE2_PORT" "$E2E_PEER2_PORT"
export E2E_RLN1="http://127.0.0.1:$NODE1_PORT" E2E_RLN2="http://127.0.0.1:$NODE2_PORT"
export E2E_INDEXER="127.0.0.1:$E2E_ELECTRS_PORT"
export E2E_PROXY="rpc://127.0.0.1:$E2E_PROXY_PORT/json-rpc"
export RUST_LOG="${RUST_LOG:-info}"

set +e
cargo test --test e2e -- --ignored --test-threads=1 --nocapture "$@"
rc=$?
set -e
if [ $rc -ne 0 ]; then
    for n in 1 2; do
        echo "== e2e-tmp/node$n.out (tail)"
        tail -n 100 "e2e-tmp/node$n.out" 2>/dev/null || true
    done
fi
exit $rc
