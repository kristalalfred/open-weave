---
id: OW-82
title: "The node kit exposes Strom's API, link keys included"
type: bug
status: in-progress
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

- [ ] The node kit refuses to start without `STROM_API_KEY`, and gives it to
      Strom and, as `WEAVE_STROM_TOKEN`, to the adapter.
- [ ] With the kit running, Strom's `/api/flows` answers 401 without the key, a
      WHIP route does not ask for it, and media flows between two kit nodes.
- [ ] The bench runs every Strom with an API key, its recipes and runner present
      it, and `weave-bench run` passes.
- [ ] `deploy/README.md` and the README describe the key, and no doc still calls
      the kit's Strom API unauthenticated.

## Log

- 2026-10-15: filed and started by claude-rc from the 0.1 release review.
