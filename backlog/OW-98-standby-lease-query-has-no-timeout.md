---
id: OW-98
title: "A standby's lease query has no timeout"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`wait_for_lease` awaits `PgStore::acquire_lease` without a query deadline.
Lease renewal has a deadline; acquisition does not
(`crates/controller/src/main.rs`, `store.rs`).

## Done when

- [ ] Bound lease acquisition and retry after a stalled query. Verify against a
  stalled connection.
