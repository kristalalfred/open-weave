---
id: OW-82
title: "The node kit exposes Strom's API, link keys included"
type: bug
status: done
depends_on: []
assignee: claude-rc
---

## Evidence

`deploy/node/node.yaml` advertises WHIP and WHEP at `<node-public-address>:8080`,
and `deploy/README.md` tells operators to open 8080 for them. That port is also
Strom's own API, and `deploy/node/docker-compose.yml` starts Strom without an API
key. On 2026-10-15 a node kit brought up from these files on colima answered an
unauthenticated `GET /api/flows` on node 2 with both inter-node SRT links'
`passphrase=` values. The same API creates, starts and deletes flows.

Strom 0.6.10 (`backend/src/lib.rs`) puts every `/api` route except login, auth
status, the WHIP/WHEP endpoint lists, client log, ICE servers and MCP (which
checks the key itself) behind `STROM_API_KEY`. The `/whip` and `/whep` routers
sit outside it. Strom binds `0.0.0.0` with no option to bind elsewhere
(`backend/src/main.rs`), and the kit uses host networking.

The bench also runs Strom without a key, so nothing exercises the adapter's
`WEAVE_STROM_TOKEN` path outside the Strom client's unit tests.

## Done when

- [x] The node kit refuses to start without `STROM_API_KEY`, and gives it to
      Strom and, as `WEAVE_STROM_TOKEN`, to the adapter.
- [x] With the kit running, Strom's `/api/flows` answers 401 without the key, a
      WHIP route does not ask for it, and media flows between two kit nodes.
- [x] The bench runs every Strom with an API key, its recipes and runner present
      it, and `weave-bench run` passes.
- [x] `deploy/README.md` and the README describe the key, and no doc still calls
      the kit's Strom API unauthenticated.

## Log

- 2026-10-15: filed and started by claude-rc from the 0.1 release review.
- 2026-10-15: `deploy/node/docker-compose.yml` sets `STROM_API_KEY` on Strom
  and passes the same value to the adapter as `WEAVE_STROM_TOKEN`, both with
  `:?`. `docker compose config` with `STROM_API_KEY=` empty exits 1. Box 1
  ticked.
- 2026-10-15: two node kits and the control-plane kit on one colima host, the
  second node's ports moved, image built from f5491cf: without the key Strom's
  `/api/flows` answered 401, with it 200, and `OPTIONS /whip/probe` 200 with no
  key. The adapters registered through nginx TLS and an SRT stream from node 1
  to node 2 reached `flowing`; a receiver decoded 1779 frames in 60 s. The
  same check at 8e4d161 got 200 without a key. Not bench. Box 2 ticked.
- 2026-10-15: every bench Strom runs with `STROM_API_KEY`, each adapter gets
  `WEAVE_STROM_TOKEN`, the `bench/justfile` recipes send it, and `weave-bench`
  presents it through `StromClient` for its flow checks and telemetry. On the
  rebuilt bench strom-1 and strom-2 answered `/api/flows` 401 without the key
  and 200 with it. `weave-bench run` passed 11 of 11, once on the OW-82 change
  and again at f5491cf. Bench. Box 3 ticked.
- 2026-10-15: `deploy/README.md` has a "Strom's API key" section, its node step
  and ports table name the key, and the README's Strom token and key-exposure
  paragraphs say Strom with `STROM_API_KEY` requires it. A grep of the three
  READMEs finds no claim that the kit's Strom API is unauthenticated. Box 4
  ticked.
