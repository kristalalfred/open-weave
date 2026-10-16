---
id: OW-92
title: "Responses and webhook payloads reject unknown fields and values"
type: bug
status: done
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

- [x] The CLI reads a response that carries a field and an enum value it does
      not know, and prints it; a test covers it.
- [x] Manifests, stream sets and node configs still refuse an unknown field
      with its path, at every depth, `port_range` included.
- [x] Northbound response and webhook schemas in `contracts/` allow
      additional properties and new enum values; request schemas and southbound
      schemas do not.
- [x] `docs/api.md` says clients must ignore unknown fields and accept new enum
      values.

## Log

- 2026-10-16: filed and started by claude-rc-fixes from the 0.1 release review.
- 2026-10-16: box 3 narrowed from every response schema to northbound responses
  and webhooks. An adapter acts on desired hops, and `PROTOCOL_VERSION` already
  pins the southbound shape, so a southbound payload that ignored a field would
  build a flow other than the one planned. Two design sketches reached this
  independently.
- 2026-10-16: every serde struct in weave-core refuses unknown fields. The
  test `the_model_refuses_unknown_fields_at_every_depth` walks the request
  schema of every contract payload and of `NodeConfig`; before the attributes
  it failed with 66 open objects, among them
  `topology.attachments[].listeners.srt.port_range`, `node-registration.json`'s
  root and `hop_status[]`. A node config with `port_range: { start, end, step: 2 }`
  now stops the adapter binary with `...srt.port_range: unknown field `step`,
  expected `start` or `end` at line 37 column 53`. The controller reads a
  registration's sender and version before its shape (403, then 409, then a
  400 that names the path), and answers an unknown node's heartbeat `404`
  before decoding it; two tests failed before with `400`. Box 2 ticked.
- 2026-10-16: `weave_core::read` holds what clients read: 26 reader types
  naming only the fields the CLI and the bench use, with every enumeration as
  `Open<E>`, whose unknown case only deserialization can build. A test checks
  each reader against the schema the server serializes. The CLI renders tables
  from them and prints the server's body for `-o json` and `-o yaml`, in the
  server's field order (serde_json `preserve_order`); `apply` and `delete` read
  only the ETag. Test `a_newer_servers_stream_is_read_printed_applied_and_deleted`
  serves a stream with extra fields, a `rist` destination, status `preempted`
  and reason `source_preempted`: before, `get stream` failed with ``unknown
  field `labels` `` and `get streams`, `-o json`, `apply` and `delete` failed
  the same way; after, all succeed and the table prints `preempted` and
  `source_preempted` as sent. Box 1 ticked.
- 2026-10-16: contracts render per reader. Requests and southbound payloads use
  the closed schemas; northbound responses and webhook events use the
  serialize contract with the `OpenToClients` transform, which drops
  `additionalProperties: false`, adds a string branch to every enumeration and
  an object branch to every union. `stream-resource.json` has no
  `additionalProperties: false`; `node-registration.json` and
  `desired-hops.json` close every object. A test fails when the transform is
  removed. Box 3 ticked.
- 2026-10-16: `docs/api.md` has a Compatibility section saying clients ignore
  unknown fields and accept new enum values and union kinds, and requests refuse
  unknown fields at any depth; `docs/webhooks.md` says the same for events.
  Real binaries with auth off: apply, get, `-o json`, `-o yaml` and delete
  against a controller and northbound; a Strom adapter and a browser page
  registered and heartbeated against the strict southbound. Box 4 ticked.
