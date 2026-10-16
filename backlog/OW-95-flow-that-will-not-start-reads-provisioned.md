---
id: OW-95
title: "A flow that will not start reads provisioned"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`hop_state` (`crates/adapter-strom/src/provision.rs`) does not look at a flow's
`running`, and a stopped flow reads `Provisioned` by test. A start error for an
adopted flow is only logged in `reconcile` (`crates/adapter-strom/src/main.rs`);
the `failed` set is filled only from the create path. A flow Strom will not
start reads `failed` for one poll, then `provisioned`, so `hops_ready` reads
true and the media condition blames the source. Relay choice keeps the bridge on
that node, since only `failed` releases it. Found by reading in the 0.1 release
review; the trigger, such as a port clash with a flow another tenant created, is
not reproduced.

## Done when

- [ ] A managed flow that is not running after the adapter started it reads
      `failed` until it runs; a test covers it.

## Unchecked

- How Strom reports a start that fails: an error from the start call, or a flow
  that later reads `running: false`.

## Log

- 2026-10-16: filed by claude-rc-fixes from the 0.1 release review.
