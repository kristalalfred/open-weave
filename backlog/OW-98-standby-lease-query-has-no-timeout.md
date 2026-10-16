---
id: OW-98
title: "A standby's lease query has no timeout"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`wait_for_lease` (`crates/controller/src/main.rs`) awaits `acquire_lease`
without a bound. The pool in `crates/controller/src/store.rs` sets only
`max_connections(5)`, and no statement timeout is set; sqlx's acquire timeout
bounds getting a connection, not the query. Renewal is bounded. If Postgres
stops answering with the socket left open, every standby waits until TCP gives
up, and no controller leads even after Postgres returns. Found by reading in the
0.1 release review; not reproduced.

## Done when

- [ ] A lease query that gets no answer fails within a bound and is retried; a
      test against a stalled connection covers it.

## Log

- 2026-10-16: filed by claude-rc-fixes from the 0.1 release review.
