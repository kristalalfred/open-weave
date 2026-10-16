---
id: OW-99
title: "A commit that succeeds but reports an error leaves memory behind Postgres"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

Stream create, update and delete, and stream-set writes, update the
controller's memory only when the store returns `Ok`
(`crates/controller/src/main.rs`). A Postgres `COMMIT` can take effect and still
return an error to the client (`crates/controller/src/store.rs`). Postgres then
holds the new revision while memory holds the old one: `GET` serves the old
ETag, or `404` after a create, every conditional write gets `412`, and the tick
plans the old spec until a takeover or restart reloads from Postgres. Found by
reading in the 0.1 release review; not reproduced.

## Done when

- [ ] After a store error on a write, the controller's view of that stream
      matches Postgres before it serves the stream again; a test covers it.

## Log

- 2026-10-16: filed by claude-rc-fixes from the 0.1 release review.
