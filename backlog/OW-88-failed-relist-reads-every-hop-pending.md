---
id: OW-88
title: "A failed re-list after provisioning reports every hop pending, and logs nothing"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

After a poll that creates, starts or deletes flows, `reconcile`
(`crates/adapter-strom/src/main.rs`) lists Strom's flows again with
`flow_api.list_flows().await.unwrap_or_default()`. If that request fails, the
list reads empty, so every desired hop is reported `pending` to southbound for
that poll, and no log line says why. Found by reading the code while working
OW-87; not reproduced.

## Done when

- [ ] A failed re-list is logged with its cause.
- [ ] The hops reported after a failed re-list come from what the adapter last
      knew of Strom's flows, not from an empty list; a test covers it.

## Log

- 2026-10-15: filed by claude-rc from OW-87's findings.
