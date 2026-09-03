#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")"

if [ -n "${NO_COLOR:-}" ]; then
    R="" BOLD="" DIM="" GREEN="" YELLOW=""
else
    R=$'\033[0m' BOLD=$'\033[1m' DIM=$'\033[2m' GREEN=$'\033[1;32m' YELLOW=$'\033[1;33m'
fi

printf '%sNODE2%s  %s(behind companion)%s\n' "$BOLD" "$R" "$DIM" "$R"
printf '%swatch it move by itself%s\n\n' "$GREEN" "$R"

health=$(curl -sf http://127.0.0.1:3121/companion/health 2>/dev/null)
if [ -z "$health" ]; then
    printf '%scompanion not answering%s\n' "$DIM" "$R"
    exit 0
fi
printf '%shealth%s    %s     %snode%s  %s\n\n' "$BOLD" "$R" \
    "$(echo "$health" | jq -r .status)" "$BOLD" "$R" "$(echo "$health" | jq -r .node)"

curl -sf 'http://127.0.0.1:3121/companion/transfers?limit=5' 2>/dev/null \
    | jq -r '.transfers[] | [(.kind // "-"), (.status // "-")] | @tsv' 2>/dev/null \
    | while IFS=$'\t' read -r kind status; do
        if [ "$status" = Settled ]; then c=$GREEN; else c=$YELLOW; fi
        printf '  %-14s %s%s%s\n\n' "$kind" "$c" "$status" "$R"
    done

rows=$(curl -sf 'http://127.0.0.1:3121/companion/payments?limit=4' 2>/dev/null \
    | jq -r '.payments[] | [.direction, .status] | @tsv' 2>/dev/null)
if [ -n "$rows" ]; then
    printf '%spayments%s\n' "$BOLD" "$R"
    while IFS=$'\t' read -r dir status; do
        if [ "$status" = Succeeded ]; then c=$GREEN; else c=$YELLOW; fi
        printf '  %-14s %s%s%s\n\n' "$dir" "$c" "$status" "$R"
    done <<<"$rows"
fi
