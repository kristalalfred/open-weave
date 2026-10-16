---
id: OW-103
title: "The adapter protocol carries fields nothing reads"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`AdapterKind` (`crates/core/src/lib.rs`) has seven values and only `strom` is
sent; nothing reads `capabilities.adapters`. `EndpointKind` has six values and
only `flow` is sent. `EndpointDescriptor.metadata` and `transports` feed no
planning, and southbound's `/endpoints` and `/state` have no consumer in the
repo. `NodeDescriptor.endpoint` is required but never dialled
(`docs/nodes.md`); the browser node sends `browser://<id>`, and the node
config's `public_endpoint` exists only to fill it. A third-party adapter has to
fill fields that do nothing. Found by reading in the 0.1 release review.

## Done when

- [ ] Each field and value the protocol carries is read by the controller or
      removed, with `PROTOCOL_VERSION` raised for the change.

## Log

- 2026-10-16: filed by claude-rc-fixes from the 0.1 release review.
