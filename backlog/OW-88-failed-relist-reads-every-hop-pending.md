---
id: OW-88
title: "A failed re-list after provisioning reports every hop pending, and logs nothing"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

After provisioning changes, `reconcile` replaces a failed flow-list request
with an empty list. Desired hops then report `pending`, without a warning
(`crates/adapter-strom/src/main.rs`).

## Done when

- [ ] Log the refresh failure with its cause and preserve the last known flow
  inventory. Test a failed refresh.
