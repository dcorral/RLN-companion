#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")"

tmux kill-session -t rln-demo 2>/dev/null || true
pkill -f 'rgb-lightning-nod[e].*demo/tmp' 2>/dev/null || true
pkill -f 'rln-companio[n] --config companion.toml' 2>/dev/null || true
docker compose -f compose.yaml down -v --remove-orphans 2>/dev/null || true
rm -rf tmp
echo "demo stopped"
