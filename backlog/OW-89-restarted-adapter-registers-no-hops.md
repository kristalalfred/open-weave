---
id: OW-89
title: "A restarted Strom adapter registers with no hops, and two streams' ports can swap"
type: bug
status: in-progress
depends_on: []
assignee: claude-rc-fixes
---

## Evidence

A new adapter process starts with an empty `last_desired`
(`crates/adapter-strom/src/main.rs`, `run`), and its first `sync_once` registers
with `hop_statuses` over that empty list. The registration carries
`hop_status: []` while Strom still runs the node's managed flows.
`register_node` in the controller replaces the whole registration, and
`HopReports::from_reports` (`crates/controller/src/path.rs`) holds a port only
when a report names it. The next tick plans that node's hops as if it ran
nothing. `docs/api.md` says both shipped nodes "keep reporting the hops they
run", and the adapter's own `hop_status` comment says an empty list must never
stand in for hops the node may still run.

Reproduced on 2026-10-16 with the CI release binaries for aarch64 macOS, a
Postgres store and two `eyevinntechnology/strom:0.6.10` containers, outside the
bench. Nodes `source` and `studio`, SRT port range 20000-20099. Apply stream
`feed` (destination `b` on `studio`), then stream `a47` (destination `x`). `feed`
listens on 20088 and `a47` on 20089. Stop `studio`'s adapter with SIGTERM and
start it again: `feed` moves to 20089 and `a47` to 20088, and both receiver
flows are deleted and created again. A consumer still dialling 20088 for `feed`
receives `a47`.

Variant: when the first Strom listing of a poll fails, `pending_statuses`
reports every hop with `resolved: None`, and `from_reports` skips unresolved
sockets, so that node's ports are not held for the tick. Found by reading and by
a unit probe; not reproduced on Strom.

No bench case restarts an adapter while its Strom keeps running. Case 08 stops
an adapter together with its Strom, so its empty report is true there.

## Done when

- [x] A restarted adapter's first registration reports the hops its Strom runs,
      with their listener addresses; a unit test covers it.
- [x] With Strom unlisted, the reported hops keep their listener addresses.
- [x] In the `feed` and `a47` reproduction on two Strom nodes, restarting
      `studio`'s adapter moves no port and recreates no flow.
- [ ] A bench case restarts an adapter while its Strom keeps running, and
      checks that flows are unchanged and media keeps decoding.

## Easy to break

- The first registration is the version handshake. Nothing is created or
  deleted before southbound accepts it.

## Log

- 2026-10-16: filed and started by claude-rc-fixes from the 0.1 release review.
- 2026-10-16: before its first registration the adapter now asks southbound
  for the node's desired hops (`desired_before_registering`), so the
  registration reports each flow Strom runs, with its listener address. A
  `5xx` or no answer fails the poll and registration waits; a `404` or hops
  that do not decode leave the registration to the handshake. With Strom
  unlisted, `pending_statuses` now reports each socket's address from the
  desired hop, as `hop_statuses` already did. Unit test
  `a_restarted_adapter_registers_the_hops_its_strom_runs` failed before the
  change (`left: []`) and passes after it; the added assertion in
  `a_missed_desired_fetch_or_strom_listing_still_reports_the_last_desired_hops`
  failed with `left: None` and passes. Boxes 1 and 2 ticked.
- 2026-10-16: the `feed` and `a47` reproduction, scripted, on two Strom 0.6.10
  containers outside the bench: with the CI release binaries, restarting
  `studio`'s adapter moved `feed` to 20089 and `a47` to 20088 and recreated both
  receiver flows; with binaries built from this change, every port and flow id
  was unchanged after the restart. Box 3 ticked.
- 2026-10-16: added bench case `12-adapter-restart`: the NAT transit stream
  moves its bridge to strom-node-2, then adapter-2 restarts while strom-2 keeps
  running. On the bench's existing images, built from the code before this
  change, it failed: the stream read `pending (hops_pending)`, the bridge moved
  to strom-node-1, the sender and receiver flows were recreated and the
  receiver reconnected. Not yet run on images with the fix: rebuilding the
  shared bench was refused in this session.
