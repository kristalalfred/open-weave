---
id: OW-100
title: "A typo in WEAVE_WEBHOOK_EVENTS can turn delivery off"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`resolve_events` warns and discards unknown webhook names; an entirely invalid
allowlist disables delivery. `deliver` treats HTTP 429 as a permanent client
error (`crates/controller/src/webhook.rs`).

## Done when

- [ ] Reject unknown configured event names at startup.
- [ ] Retry HTTP 429 responses.
