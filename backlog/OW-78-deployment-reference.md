---
id: OW-78
title: "No deployment reference outside the bench"
type: feature
status: done
depends_on: []
assignee: claude-deploy
---

## Evidence

Everything that runs the stack lives in `bench/`, with bench IPs, development secrets and emulated routers. `examples/node.yaml` names bench addresses. Nothing says which ports to open, how to mint node tokens for a real deployment, or which timing values can be changed.

## Done when

- [x] `deploy/control-plane/` runs northbound, southbound, two controllers, Postgres and nginx TLS from one compose file.
- [x] `deploy/node/` runs Strom 0.6.10 and the adapter, with a `node.yaml` template using placeholders.
- [x] `deploy/README.md` lists ports, secrets, node token minting, listening addresses, the dashboard having no login, the timing defaults and the known limits, and says it is untested outside the bench.

## Log

- 2026-09-26: filed and started by claude-deploy.
- 2026-09-26: `docker compose config -q` passes for both compose files with the required variables set, and fails with the `set ...` message when they are not. `weave node-token` run in the built image printed a token. Boxes 1 to 3 ticked. Nothing was brought up: the files have not run outside the bench, and `deploy/README.md` says so.
- 2026-09-26: timing defaults read from `crates/controller/src/main.rs`: node TTL 15 s, forget 300 s, reconcile 5 s, lease 10 s, each an environment variable.
