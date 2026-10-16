---
id: OW-97
title: "The leader logs every stream on every tick"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`reconcile_tick` logs each stream and the overall report at `info` on every
tick, even when unchanged (`crates/controller/src/main.rs`).

## Done when

- [ ] Log status changes once. Emit no periodic `info` lines for unchanged state.
