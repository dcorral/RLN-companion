#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

NODE1=http://127.0.0.1:3021
COMPANION=http://127.0.0.1:3121
PROXY=rpc://127.0.0.1:3020/json-rpc
BCLI="docker compose -f compose.yaml exec -T -u blits bitcoind bitcoin-cli -regtest"
TOTAL_STEPS=14
STEP_N=0

if [ -n "${NO_COLOR:-}" ]; then
    R="" BOLD="" DIM="" CYAN="" GREEN="" YELLOW="" RED="" BAR=""
else
    R=$'\033[0m' BOLD=$'\033[1m' DIM=$'\033[2m' CYAN=$'\033[36m'
    GREEN=$'\033[1;32m' YELLOW=$'\033[1;33m' RED=$'\033[1;31m' BAR=$'\033[7;1;36m'
fi

step() {
    STEP_N=$((STEP_N + 1))
    printf '\n\n%s  STEP %d/%d   %s  %s\n\n' "$BAR" "$STEP_N" "$TOTAL_STEPS" "$*" "$R"
}
say() { printf '%s%s%s\n' "$BOLD" "$*" "$R"; }
good() { printf '\n%s  %s%s\n\n' "$GREEN" "$*" "$R"; }
warn() { printf '\n%s  %s%s\n\n' "$YELLOW" "$*" "$R"; }
bad() { printf '\n%s  %s%s\n\n' "$RED" "$*" "$R"; }
show() { printf '  %s$%s %s%s%s\n' "$DIM" "$R" "$CYAN" "$*" "$R"; }
pause() {
    if [ "${DEMO_AUTO:-0}" = 1 ]; then sleep 2; else read -r -p "  [enter] "; fi
}

post() {
    RESP=$(curl -s -X POST "$1" -H 'content-type: application/json' ${2:+-d "$2"})
}
mine() { $BCLI -rpcwallet=miner -generate "$1" >/dev/null; }
mempool_count() { $BCLI getrawmempool | jq length; }

retry_post() {
    local what=$1 tries=$2 url=$3 body=$4
    local i
    for ((i = 1; i <= tries; i++)); do
        if RESP=$(curl -s -X POST "$url" -H 'content-type: application/json' -d "$body") \
            && ! echo "$RESP" | jq -e '.error? // empty' >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    echo "gave up on $what: $RESP" >&2
    return 1
}

listtransfers_body() {
    cat <<EOF
{"asset_filter":{"type":"Id","value":"$asset_id"},"txid":null,"index_offset":null,"max_transfers":null,"status":null,"created_after":null,"created_before":null}
EOF
}
send_status() {
    curl -s -X POST "$NODE1/listtransfers" -H 'content-type: application/json' \
        -d "$(listtransfers_body)" \
        | jq -r '.transfers[] | select(.kind == "Send") | .status'
}

step "same API, through the companion"
say "both nodes are already unlocked; node2's unlock went THROUGH the companion during setup."
show "curl $COMPANION/nodeinfo"
pubkey=$(curl -s "$COMPANION/nodeinfo" | jq -r .pubkey)
say "pubkey ${pubkey:0:12}..."
say "this is a plain RLN route answered through the companion, untouched."
show "curl $COMPANION/companion/health"
h=$(curl -s "$COMPANION/companion/health")
say "health $(echo "$h" | jq -r .status)  node $(echo "$h" | jq -r .node)"
say "and /companion/* is the companion itself."
good "one base URL for everything"
pause

step "fund wallets"
addr1=$(curl -s -X POST "$NODE1/address" | jq -r .address)
addr2=$(curl -s -X POST "$COMPANION/address" | jq -r .address)
$BCLI -rpcwallet=miner sendtoaddress "$addr1" 1 >/dev/null
$BCLI -rpcwallet=miner sendtoaddress "$addr2" 1 >/dev/null
mine 1
utxos_body='{"up_to":false,"num":10,"size":32000,"fee_rate":7,"skip_sync":false}'
show "curl -X POST $NODE1/createutxos -d '{...}'"
retry_post "createutxos node1" 30 "$NODE1/createutxos" "$utxos_body"
show "curl -X POST $COMPANION/createutxos -d '{...}'"
retry_post "createutxos node2" 30 "$COMPANION/createutxos" "$utxos_body"
mine 1
good "colored UTXOs ready on both nodes"
pause

step "issue asset"
show "curl -X POST $NODE1/issueassetnia -d '{...}'"
say "node1 issues 1000 DEMO"
retry_post issueassetnia 30 "$NODE1/issueassetnia" '{"amounts":[1000],"ticker":"DEMO","name":"DEMO","precision":0}'
asset_id=$(echo "$RESP" | jq -r .asset.asset_id)
echo "$asset_id" >tmp/asset_id
good "asset ${asset_id:0:20}..."
pause

step "blind invoice (via companion)"
say "the companion sees the invoice and tracks the receive on its own."
show "curl -X POST $COMPANION/rgbinvoice -d '{...}'"
post "$COMPANION/rgbinvoice" "{\"asset_id\":null,\"assignment\":null,\"min_confirmations\":1,\"witness\":false,\"transport_endpoints\":[\"$PROXY\"]}"
recipient_id=$(echo "$RESP" | jq -r .recipient_id)
good "invoice ready -> bottom-right pane: WaitingCounterparty"
pause

step "send 400 DEMO"
show "curl -X POST $NODE1/sendrgb -d '{...}'"
say "donation: false -> RLN will NOT broadcast"
post "$NODE1/sendrgb" "{\"donation\":false,\"fee_rate\":7,\"min_confirmations\":1,\"recipient_map\":{\"$asset_id\":[{\"recipient_id\":\"$recipient_id\",\"witness_data\":null,\"assignment\":{\"type\":\"Fungible\",\"value\":400},\"transport_endpoints\":[\"$PROXY\"]}]}}"
txid=$(echo "$RESP" | jq -r .txid)
say "txid ${txid:0:16}..."
warn "mempool EMPTY   status $(send_status)"
bad "node1 is STUCK: nothing moves on its own"
pause

step "manual refresh #1"
say "no companion on node1: YOU must run this."
show "curl -X POST $NODE1/refreshtransfers -d '{\"asset_id\":null,\"filter\":[],\"skip_sync\":false}'"
pause
for i in $(seq 1 30); do
    curl -s -X POST "$NODE1/refreshtransfers" -H 'content-type: application/json' -d '{"asset_id":null,"filter":[],"skip_sync":false}' >/dev/null
    [ "$(mempool_count)" -ge 1 ] && break
    [ "$i" = 30 ] && { echo "tx never reached the mempool" >&2; exit 1; }
    sleep 2
done
good "mempool 1 tx -> broadcast happened"
pause

step "mine 1 block"
mine 1
say "watch the RIGHT panes: the companion settles node2 alone."
for i in $(seq 1 60); do
    settled=$(curl -s "$COMPANION/companion/transfers?status=Settled" | jq -r --arg rid "$recipient_id" '.transfers[] | select(.recipient_id == $rid) | .status' | head -1)
    [ "$settled" = Settled ] && break
    [ "$i" = 60 ] && { echo "receiver never settled" >&2; exit 1; }
    sleep 2
done
good "node2 Settled -> no human touched it"
pause

step "manual refresh #2"
warn "node1 still $(send_status)"
say "the sender must refresh AGAIN to notice the confirmation."
show "curl -X POST $NODE1/refreshtransfers -d '{\"asset_id\":null,\"filter\":[],\"skip_sync\":false}'"
pause
for i in $(seq 1 30); do
    curl -s -X POST "$NODE1/refreshtransfers" -H 'content-type: application/json' -d '{"asset_id":null,"filter":[],"skip_sync":false}' >/dev/null
    st=$(send_status)
    [ "$st" = Settled ] && break
    [ "$i" = 30 ] && { echo "sender never settled (last: $st)" >&2; exit 1; }
    sleep 2
done
good "node1 Settled -> but a human refreshed TWICE"
pause

step "on-chain takeaway"
bad "no companion: 2 manual refreshes for ONE payment"
good "companion: 0 manual steps + signed webhooks"
say "now let's move the asset into a Lightning channel."
pause

wait_payment() {
    local hash=$1 i st
    for i in $(seq 1 60); do
        st=$(curl -s "$COMPANION/companion/payments" | jq -r --arg h "$hash" '[.payments[] | select(.payment_hash == $h)][0].status // empty')
        [ "$st" = Succeeded ] && return 0
        if [ "$st" = Failed ] || [ "$st" = Cancelled ]; then
            echo "payment $hash: $st" >&2
            return 1
        fi
        [ "$i" = 60 ] && { echo "payment $hash never settled" >&2; return 1; }
        sleep 1
    done
}
wait_ready() {
    local who=$1 url=$2 i
    for i in $(seq 1 60); do
        [ "$(curl -s "$url" | jq -r --arg a "$asset_id" '[.channels[] | select(.asset_id == $a) | .ready][0]')" = true ] && return 0
        [ "$i" = 60 ] && { echo "channel never ready on $who" >&2; return 1; }
        sleep 2
    done
}

step "open an RGB channel (via companion)"
say "node2 commits 300 of its 400 DEMO to a channel to node1."
node1_pubkey=$(curl -s "$NODE1/nodeinfo" | jq -r .pubkey)
tip=$($BCLI getblockcount)
for i in $(seq 1 30); do
    [ "$(curl -s "$COMPANION/networkinfo" | jq -r '.height // 0')" -ge "$tip" ] && break
    [ "$i" = 30 ] && { echo "node2 never synced height $tip" >&2; exit 1; }
    sleep 2
done
show "curl -X POST $COMPANION/openchannel -d '{...}'"
retry_post openchannel 45 "$COMPANION/openchannel" "{\"peer_pubkey_and_opt_addr\":\"$node1_pubkey@127.0.0.1:9821\",\"capacity_sat\":100000,\"push_msat\":0,\"asset_amount\":300,\"asset_id\":\"$asset_id\",\"push_asset_amount\":null,\"public\":true,\"with_anchors\":true,\"fee_base_msat\":null,\"fee_proportional_millionths\":null,\"temporary_channel_id\":null,\"virtual_open_mode\":null}"
tmp_chan=$(echo "$RESP" | jq -r .temporary_channel_id)
say "temporary_channel_id ${tmp_chan:0:16}..."
funding=""
for i in $(seq 1 60); do
    funding=$(curl -s "$COMPANION/listchannels" | jq -r --arg a "$asset_id" '[.channels[] | select(.asset_id == $a) | .funding_txid // empty][0] // empty')
    if [ -n "$funding" ] && $BCLI getrawmempool | jq -e --arg t "$funding" 'index($t) != null' >/dev/null; then break; fi
    funding=""
    [ "$i" = 60 ] && { echo "funding tx never reached the mempool" >&2; exit 1; }
    sleep 2
done
good "funding tx ${funding:0:16}... in the mempool"
pause

step "mine 6, watch the right panes"
mine 6
say "funding is the ONE transfer RLN refreshes itself (at ChannelReady),"
say "but the companion still mirrors it and webhooks transfer.settled (kind Send)."
wait_ready node1 "$NODE1/listchannels"
wait_ready node2 "$COMPANION/listchannels"
for i in $(seq 1 30); do
    [ "$(curl -s "$COMPANION/companion/transfers?status=Settled" | jq -r --arg t "$funding" '[.transfers[] | select(.txid == $t)] | length')" -ge 1 ] && break
    [ "$i" = 30 ] && { echo "funding transfer never mirrored settled" >&2; exit 1; }
    sleep 2
done
good "channel ready on both sides -> funding transfer.settled fired by itself"
pause

step "pay over lightning, instantly"
say "keysend node2 -> node1: 100 DEMO"
show "curl -X POST $COMPANION/keysend -d '{...}'"
post "$COMPANION/keysend" "{\"dest_pubkey\":\"$node1_pubkey\",\"amt_msat\":10000000,\"asset_id\":\"$asset_id\",\"asset_amount\":100}"
ks_hash=$(echo "$RESP" | jq -r .payment_hash)
say "payment_hash ${ks_hash:0:16}..."
wait_payment "$ks_hash"
good "payment.settled (Outbound) in the webhook pane -> milliseconds, no mining"
say "node1 has NO companion: its operator must poll to even notice the money."
show "curl $NODE1/listpayments"
lp=""
for i in $(seq 1 15); do
    lp=$(curl -s "$NODE1/listpayments")
    st=$(echo "$lp" | jq -r '[.payments[] | select(.payment_type != "Outbound")][0].status // empty')
    [ "$st" = Succeeded ] && break
    [ "$i" = 15 ] && { echo "node1 never saw the inbound payment" >&2; exit 1; }
    sleep 2
done
n=$(echo "$lp" | jq '.payments | length')
amt=$(echo "$lp" | jq -r '[.payments[] | select(.payment_type != "Outbound")][0].asset_amount')
say "$n payment: Succeeded, $amt DEMO in"
pause

step "and back"
say "node2 issues an LN invoice for 25 DEMO, through the companion."
show "curl -X POST $COMPANION/lninvoice -d '{...}'"
post "$COMPANION/lninvoice" "{\"amt_msat\":3000000,\"expiry_sec\":900,\"asset_id\":\"$asset_id\",\"asset_amount\":25,\"payment_hash\":null,\"description\":null,\"description_hash\":null,\"min_final_cltv_expiry_delta\":null}"
ln_invoice=$(echo "$RESP" | jq -r .invoice)
say "invoice ${ln_invoice:0:24}..."
show "curl $COMPANION/companion/payments"
pre=$(curl -s "$COMPANION/companion/payments" | jq -r '[.payments[] | select(.status == "Pending" and .direction == "Inbound")][0] // empty | "\(.direction) \(.status), \(.asset_amount) DEMO"')
[ -n "$pre" ] && say "pre-tracked: $pre (the interceptor saw the invoice)"
say "node1 pays it DIRECTLY:"
show "curl -X POST $NODE1/sendpayment -d '{...}'"
post "$NODE1/sendpayment" "{\"invoice\":\"$ln_invoice\",\"amt_msat\":null,\"asset_id\":null,\"asset_amount\":null}"
pay_hash=$(echo "$RESP" | jq -r .payment_hash)
say "payment_hash ${pay_hash:0:16}..."
wait_payment "$pay_hash"
good "payment.settled (Inbound) -> the companion announced node2 got paid"
say "node1's operator polls AGAIN for their outbound:"
show "curl $NODE1/listpayments"
for i in $(seq 1 15); do
    st=$(curl -s "$NODE1/listpayments" | jq -r --arg h "$pay_hash" '[.payments[] | select(.payment_type == "Outbound" and .payment_hash == $h)][0].status // empty')
    [ "$st" = Succeeded ] && break
    [ "$i" = 15 ] && { echo "node1 outbound never succeeded" >&2; exit 1; }
    sleep 2
done
say "outbound Succeeded, 25 DEMO out"
pause

step "takeaway"
h=$(curl -s "$COMPANION/companion/health")
say "companion health: $(echo "$h" | jq -r .status), node $(echo "$h" | jq -r .node), pending transfers $(echo "$h" | jq -r .pending_transfers), pending payments $(echo "$h" | jq -r .pending_payments)"
bad "on-chain, no companion: 2 manual refreshes for ONE payment"
good "on-chain, companion: babysat every state change, signed webhooks"
good "lightning: settles in milliseconds, companion webhooks BOTH directions"
bad "manual node1: still running curls just to know what happened"
good "DEMO COMPLETE"
