---
id: OW-95
title: "A flow that will not start reads provisioned"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`hop_state` reports any existing flow as `Provisioned`, even when `running`
is false. Start failures for adopted flows are logged but excluded from the
failed-hop set (`crates/adapter-strom/src/provision.rs`, `main.rs::reconcile`).

## Done when

- [ ] Report a managed flow that fails to start as failed until it runs. Cover
  repeated polls.
