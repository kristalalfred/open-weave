---
id: OW-101
title: "Webhook event IDs rely on timestamps for uniqueness"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

Event IDs contain the subject ID and an incrementing timestamp-seeded counter.
HA leadership resets the counter from the database lease timestamp. The format
contains no process or lease identity, so timestamp reuse or regression can
repeat IDs (`webhook.rs::emit`, `count_from`; `main.rs::run_with_lease` in
`crates/controller/src`). No collision has been reproduced.

## Done when

- [ ] Ensure distinct controller runs and leadership terms cannot reuse IDs for
  the same subject. Test identical timestamps.
