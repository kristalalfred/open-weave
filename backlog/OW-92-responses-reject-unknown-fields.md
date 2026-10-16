---
id: OW-92
title: "Responses and webhook payloads reject unknown fields and values"
type: bug
status: in-progress
depends_on: []
assignee: claude-rc-fixes
---

## Evidence

Every wire type in `weave-core` carries `#[serde(deny_unknown_fields)]`,
including `StreamResource`, `StreamAccepted`, `StreamPlan` and the status types
in `crates/core/src/api.rs`. In the 0.1 release review, a stub northbound
returning a `StreamResource` with one extra field made `weave -o json get
stream`, `weave apply` and `weave delete` fail with `unknown field`, exit `1`.
The JSON Schemas in `contracts/` carry `additionalProperties: false` on
responses and webhook events, and condition reasons, error codes and webhook
event types are closed enums, so a new value fails the same way. Any field or
value a server adds breaks the shipped CLI and every client built on the
published contract.

Inputs are not uniform either: `PortRange` accepts unknown fields, so
`port_range: { start, end, step: 2 }` in a node config is accepted silently, and
`NodeRegistration`, `NodeDescriptor`, `NodeHeartbeat` and `HopStatus` accept
unknown fields while the types inside them reject them.

## Done when

- [ ] The CLI reads a response that carries a field and an enum value it does
      not know, and prints it; a test covers it.
- [ ] Manifests, stream sets and node configs still refuse an unknown field
      with its path, at every depth, `port_range` included.
- [ ] Northbound response and webhook schemas in `contracts/` allow
      additional properties and new enum values; request schemas and southbound
      schemas do not.
- [ ] `docs/api.md` says clients must ignore unknown fields and accept new enum
      values.

## Log

- 2026-10-16: filed and started by claude-rc-fixes from the 0.1 release review.
- 2026-10-16: box 3 narrowed from every response schema to northbound responses
  and webhooks. An adapter acts on desired hops, and `PROTOCOL_VERSION` already
  pins the southbound shape, so a southbound payload that ignored a field would
  build a flow other than the one planned. Two design sketches reached this
  independently.
