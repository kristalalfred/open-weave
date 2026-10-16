---
id: OW-101
title: "Two controllers started together can repeat webhook event ids"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`docs/webhooks.md` says no controller reuses an `event_id`, across restarts and
takeovers. The sequence starts at the process's wall-clock microseconds and
adds one per event (`crates/controller/src/webhook.rs`). Two controllers started
δ µs apart, as the deploy compose file starts both, reuse ids once the first has
sent more than δ events; clock skew between hosts adds to it. The id also
carries the subject, so a repeat also needs the same node or stream. Found by
reading in the 0.1 release review; not reproduced.

## Done when

- [ ] Event ids from two controllers sharing one database cannot repeat; a test
      covers two controllers started at the same instant.

## Log

- 2026-10-16: filed by claude-rc-fixes from the 0.1 release review.
