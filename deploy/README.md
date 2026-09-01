# Deploying RLN with rln-companion

This compose file runs an RGB Lightning Node and its companion together. It is the reference way to run the companion: the node's API port only exists on the internal compose network, so every call from outside goes through the companion.

## Supported setups

There are exactly two supported ways to run a node: RLN alone, or RLN behind rln-companion with every call going through the companion. Mixing them is not supported: the companion's database is only correct if it observes every operation. Using the companion is the only secure way to interact with a node that has one. Make sure the node's API port is reachable only from the companion.

## Run

```sh
cd deploy
cp companion.toml.example companion.toml
# fill [webhook] url and secret
docker compose up --build --wait
```

Both images are built from source; the RLN build takes a while the first time. `RLN_SRC` points at the RLN checkout used as build context (default `../../rgb-lightning-node/dev`); that checkout must have its `rust-lightning` submodule populated (`git clone --recurse-submodules` or `git submodule update --init`), otherwise the image build fails on the path patches. `healthy` means the companion is serving, not that the node is unlocked: read `node` in the health body. `COMPANION_BIND` and `COMPANION_PORT` change the host address and port the companion is published on (default `127.0.0.1:3101`).

The node reads `rln.toml`, mounted read-only into the container and passed with `--config`. It is RLN's own config format, so any key from RLN's `sample-config.toml` can be added there (network, ports, auth, default indexer and proxy, channel limits). The file is committed as a template and meant to be edited in place: it ships with `network = "Testnet"`, which must match `[rln] network` in `companion.toml`. On a mismatch the companion refuses to start if the node is already unlocked; otherwise the first reconcile after `/unlock` marks the node `misconfigured` in `/companion/health` (`status` turns `degraded`) and all background work pauses until the mismatch is fixed.

Then talk to the companion for everything: RLN routes are proxied 1:1 and companion-native routes live under `/companion/*`.

```sh
curl -s 127.0.0.1:3101/companion/health
curl -s -X POST 127.0.0.1:3101/init -H 'content-type: application/json' -d '{"password":"..."}'
curl -s -X POST 127.0.0.1:3101/unlock -H 'content-type: application/json' -d '{...}'
```

## Ports

Published on the host:

- `127.0.0.1:3101` -> companion API (RLN proxy plus `/companion/*`)
- `9735` on all interfaces -> node Lightning peer port

Not published: the node's API port `3001`. It is reachable only as `http://rln:3001` from containers on the `internal` network. Do not add a port mapping for it; the isolation rule holds by construction.

## Data

- `rln-data` volume -> node storage (`/data` in the container)
- `companion-data` volume -> companion SQLite mirror (`/var/lib/rln-companion`)

`docker compose down` keeps both volumes; `docker compose down -v` deletes them, including the node's wallet data.

## Biscuit authentication

The default `rln.toml` sets `disable_authentication = true` and publishes the companion on `127.0.0.1` only, so anyone who can reach the host's loopback can drive the node. `[service] auth_token` protects `/companion/*` only, not the proxied RLN routes. To expose the companion beyond loopback (`COMPANION_BIND=0.0.0.0`), switch the node to Biscuit auth:

1. In `rln.toml` set `disable_authentication = false` and `root_public_key = "<hex>"` under `[auth]`.
2. Set `[rln] token` in `companion.toml` to a Biscuit the engine can use for its own calls (`/nodeinfo`, `/networkinfo`, `/listtransfers`, `/listassets`, `/refreshtransfers`, `/failtransfers`).
3. Operators keep sending their own Biscuit in the `Authorization` header; proxied calls forward it to the node unchanged and the companion never adds one on their behalf.

If `rln.token` is missing or lacks the rights the engine needs, the companion exits at startup (its probe of the node fails with `node misconfigured: ...`) and `restart: unless-stopped` restarts it in a loop; check `docker compose logs companion`.

## Webhooks

- Delivery is at-least-once: dedupe on the `x-companion-event-id` header.
- Events are delivered strictly in order. A failing delivery blocks the ones behind it until it succeeds or is parked after `[webhook] max_attempts` attempts.
- Parked events are counted as `parked_events` in `/companion/health`, and `status` turns `degraded` while any are parked.
