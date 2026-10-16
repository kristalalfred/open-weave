---
id: OW-99
title: "A commit that succeeds but reports an error leaves memory behind Postgres"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

Stream and stream-set handlers update memory only after the store returns
success. A commit whose acknowledgement is lost can leave memory behind
committed Postgres state; the error paths do not reload
(`crates/controller/src/main.rs`, `store.rs`). This failure has not been reproduced.

## Done when

- [ ] Inject an ambiguous commit result and verify that subsequent reads and
  reconciliation agree with Postgres.
