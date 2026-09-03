#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")"

if [ -n "${NO_COLOR:-}" ]; then
    R="" BOLD="" DIM="" YELLOW="" GREEN=""
else
    R=$'\033[0m' BOLD=$'\033[1m' DIM=$'\033[2m' YELLOW=$'\033[1;33m' GREEN=$'\033[1;32m'
fi

printf '%sNODE1%s  %s(no companion)%s\n' "$BOLD" "$R" "$DIM" "$R"
printf '%snothing moves unless YOU refresh%s\n\n' "$YELLOW" "$R"

asset=$(cat tmp/asset_id 2>/dev/null || true)
if [ -z "$asset" ]; then
    printf '%sno asset yet%s\n' "$DIM" "$R"
else
    curl -sf -X POST http://127.0.0.1:3021/listtransfers \
        -H 'content-type: application/json' \
        -d "{\"asset_filter\":{\"type\":\"Id\",\"value\":\"$asset\"},\"txid\":null,\"index_offset\":null,\"max_transfers\":null,\"status\":null,\"created_after\":null,\"created_before\":null}" 2>/dev/null \
        | jq -r '.transfers[] | [.kind, .status] | @tsv' 2>/dev/null \
        | while IFS=$'\t' read -r kind status; do
            if [ "$status" = Settled ]; then c=$GREEN; else c=$YELLOW; fi
            printf '  %-12s %s%s%s\n\n' "$kind" "$c" "$status" "$R"
        done
fi

count=$(docker compose -f compose.yaml exec -T -u blits bitcoind bitcoin-cli -regtest getrawmempool 2>/dev/null | jq length 2>/dev/null || echo "?")
printf '\n%smempool%s   %s%s%s\n' "$BOLD" "$R" "$BOLD" "$count" "$R"
pcount=$(curl -sf http://127.0.0.1:3021/listpayments 2>/dev/null | jq '.payments | length' 2>/dev/null || true)
[ -n "$pcount" ] && printf '%spayments%s  %s%s%s\n' "$BOLD" "$R" "$BOLD" "$pcount" "$R"
