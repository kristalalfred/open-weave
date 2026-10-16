---
id: OW-96
title: "One failed stats call reads as the link going down"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`hop_statuses` maps failed SRT and WebRTC statistics requests to absent
statistics. Socket conditions then report idle or connecting instead of an
unavailable observation (`crates/adapter-strom/src/main.rs`).

## Done when

- [ ] Represent unavailable observations without reporting a measured link failure.
  Test a statistics request failure after media was flowing.
