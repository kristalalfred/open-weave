---
id: OW-100
title: "A typo in WEAVE_WEBHOOK_EVENTS can turn delivery off"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

The controller drops unknown names in `WEAVE_WEBHOOK_EVENTS` with a warning
(`crates/controller/src/webhook.rs`). When every name is unknown the allowlist
is empty and nothing is delivered, while the controller runs as configured.
AGENTS.md has mismatches refused, not smoothed over. A `429` from the receiver
is treated as a `4xx` and not retried. Found by reading in the 0.1 release
review.

## Done when

- [ ] The controller refuses to start when `WEAVE_WEBHOOK_EVENTS` names an
      unknown event type, and says which.
- [ ] A `429` is retried like a `5xx`.

## Log

- 2026-10-16: filed by claude-rc-fixes from the 0.1 release review.
