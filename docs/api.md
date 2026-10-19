# HTTP API

Northbound serves the operator contract and southbound the adapter contract.
Both are stateless proxies onto the controller, which serves both.

| Contract | Served by | Routes |
|---|---|---|
| operator | northbound, controller | `/streams`, `/streams/{name}`, `/streams/{name}/endpoints`, `/stream-sets`, `/stream-sets/{owner}`, `/stream-plans`, `/nodes`, `/status` |
| adapter | southbound, controller | `/nodes/register`, `/nodes/{id}/heartbeat`, `/nodes/{id}/desired`, `/nodes`, `/endpoints`, `/state` |

Routes carry no version before a stable release. Routes, payloads and error
shapes can change between pre-release versions without aliases or a deprecation
window. A prefixed route such as `/v1/status` returns `404 route_not_found`.
Northbound and southbound check the token first, so with authentication on, a
request without a valid token gets `401` instead.

Outside the contracts:

- `/health` on every service and the Strom adapter, for healthchecks and load
  balancers. Its JSON body has `status: "ok"` and `version` set to the running binary's
  version.
- `/`, `/ui` and `/view` on the controller: the dashboard and its data. `/view`
  has no stability guarantee and can change in any release. Script against
  `/status`.

Authentication is covered in [Security](security.md).

## Machine-readable contracts

[`contracts/openapi/`](../contracts/openapi/) has OpenAPI 3.1 documents for
northbound and southbound. [`contracts/json-schema/`](../contracts/json-schema/)
has JSON Schema 2020-12 documents for every request and response payload, and
`webhook-event.json` for a webhook delivery. Both are generated from the Rust
wire types by `just contracts`, and a test fails when the committed files drift.

The schemas carry id patterns, required lists and enum values. Rules that depend
on where a shared type appears, such as `format` being source-only, are enforced
only by the validator at runtime. `/health` and the dashboard routes are not in
OpenAPI.

Each schema is rendered for the side that reads the payload:

| Payload | Read by | Schema |
|---|---|---|
| Requests: streams, stream sets, registrations, heartbeats | the controller | What the controller accepts. Every object refuses other properties. |
| Southbound responses | an adapter | What the controller sends. Every object refuses other properties. |
| Northbound responses, webhook events | a client | What the controller sends, open to additions. Objects accept other properties, each enumeration is an `anyOf` with `{"type": "string"}`, and each union is an `anyOf` with `{"type": "object"}`. |

A response schema's `required` list names the fields every response carries.

## Compatibility

A request, northbound or southbound, with a field the API does not define at any
depth, or with a value an enumeration does not name, gets `400 invalid_json`,
and the detail names the path.

A release can add fields to northbound responses and webhook events, values to
their enumerations, such as a condition `reason`, a status or an error `code`,
and kinds to their unions, such as a destination transport. A client must ignore
a field it does not know and must accept a value or a kind it does not know.
`weave` prints an unknown value as it arrives, and `-o json` and `-o yaml` print
the response body as returned. A Rust client reads responses with the types in
`weave_core::read`, which ignore unknown fields and keep unknown values. The
other `weave_core` types refuse anything they do not name.

Southbound payloads are exact for one protocol version. See
[Registration](#registration).

## Errors

Every error is JSON with `code` and `message`. A validation error lists every
known field issue in `details`, which is absent when the error has no field
context. Clients branch on `code`, not `message`.

```json
{
  "code": "invalid_request",
  "message": "stream validation failed",
  "details": [{
    "field": "destinations[0].srt.node",
    "code": "invalid_characters",
    "message": "node id must contain only lowercase ASCII letters, digits, or hyphens, and must start and end with a letter or digit"
  }]
}
```

## Identifiers

Stream names, node ids, destination ids, profile ids, attachment ids, network
ids and every node reference (`node`, `via`) are 1 to 63 characters matching
`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`. They are not trimmed or normalized. An
invalid id in a request path returns `400` before a proxy forwards it. A stored
node registration with an invalid id is dropped, and the node must register
again.

## Streams

`GET /streams` lists stream resources. `GET /streams/{name}` returns one, or
`404 stream_not_found`. A resource holds the definition under `spec`, its
`generation`, and an `owner` when a stream set owns it. A single resource also
carries an opaque `ETag`.

Writes are conditional:

| Request | Header |
|---|---|
| `POST /streams`, create | `If-None-Match: *` |
| `POST /streams`, replace | `If-Match: <etag>` |
| `DELETE /streams/{name}` | `If-Match: <etag>` |

A missing header returns `428 precondition_required` and a stale one
`412 precondition_failed`. The server does not retry or merge a conflicting
write. Reapplying an identical spec returns `changed: false` and keeps the
generation and ETag.

### Validation

The controller validates every write. Northbound runs the same validator first
for early feedback, and a client posting to the controller directly gets the
same rules. An invalid stream returns `400` with every issue and is not stored.
A stream with a field or value the API does not define is refused as
[Compatibility](#compatibility) describes.

When a stored stream no longer passes validation, the controller names the
stream, field and error and does not serve. With `DATABASE_URL` it gives up the
lease, stands by, and tries again every second. A load that fails for another
reason, such as Postgres going away, is handled the same way.

### Hop id conflicts

Hop ids join names with `-`, so two valid streams can spell the same id: the
sender of stream `x-receiver-a` and the receiver of stream `x` for destination
`a-sender` are both `weave-x-receiver-a-sender`. A write that stores a stream
able to plan an id another stream can plan, on any nodes, returns
`409 hop_id_conflict`. The message names both streams and the detail names the
id. The stream being replaced, and streams the same stream-set write replaces or
prunes, do not count. Another stream in the same write does.

An unchanged stream is not checked, so streams stored before this check that
already collide still reapply as no-ops. Of those, the later one by name stays
unplaced with reason `placement_failed`, and the detail names the id and the
stream holding it.

### Generations and revisions

A stream's `generation` starts at 1, increases when the spec changes, and stays
the same on an identical reapply. Its revision is opaque, exposed only as the
ETag, and never reused, so deleting and recreating a name starts generation 1
again with a new ETag. An old `If-Match` cannot match the new stream.

### Conditions

A stream status carries `generation`, and `observed_generation`: the generation
the last completed reconcile used, or `null` before one completes. It always
carries five conditions:

| Type | Says |
|---|---|
| `placement_ready` | The stream has a plan. |
| `nodes_available` | Every node it names is registered and online. |
| `hops_ready` | Every desired hop is provisioned. |
| `format_compatible` | The source, every selected hop and every destination establish unchanged, acceptable media by declaration. |
| `media_flowing` | Media reaches every destination. |

Each condition has a `status` of `true`, `false` or `unknown`, a stable `reason`
code, a `detail` string and an RFC 3339 `last_transition_time`. The time
changes only when the status changes.

Format reasons are `format_compatible` (`true`), `format_mismatch` (`false`), or
`source_format_unknown`, `destination_accepts_unknown`, `hop_accepts_unknown`,
`media_behavior_unknown`, `format_route_unknown` (`unknown`). Known mismatches
win; ties use destination-id and route order. Details name the boundary and
missing declaration or incompatible values. A strict format failure sets
`placement_ready` to `false` with the blocking format reason. Topology failures
retain their placement reasons. These declarations do not certify decoded media.

## Stream sets

A stream set lets one owner, such as an application, manage a group of streams
in one write.

`GET /stream-sets` lists sets. `GET /stream-sets/{owner}` returns one with its
streams and a set ETag. `PUT /stream-sets/{owner}` applies its `streams` list
atomically, creating with `If-None-Match: *` and updating with the current set
`If-Match`. The owner follows the identifier rule.

- With `prune: false`, owned streams missing from the request stay as they are.
- With `prune: true`, they are deleted. Pruning never deletes an unowned stream
  or one another set owns.
- A requested name that exists outside the set returns `409 ownership_conflict`.
  A set does not adopt existing streams.
- An owned stream cannot be changed or deleted through `/streams`, which returns
  `409 stream_owned`.
- The set and all member writes commit together. An identical apply returns
  `changed: false` and keeps the set ETag and every member's generation and
  ETag.
- An empty `streams` list is accepted only with `prune: true`. The empty set
  stays addressable for later conditional writes.

`weave apply-set OWNER -f FILE` applies a YAML file with a required `streams`
list and an optional `prune`, which defaults to `false`. Unknown fields are
rejected. The CLI reads the set, then creates it or updates it with its current
ETag. It does not retry a stale write. See
[`examples/stream-set.yaml`](../examples/stream-set.yaml).

## Plans

`POST /stream-plans` takes a stream definition and changes no state. It
validates the definition, plans it alongside the stored streams against the
current nodes and the hops they report running, and returns `placed`,
`unplaced` or `disabled` with the resolved nodes, desired hops, endpoints and
any placement reason. It includes `format_policy`, stream conditions and
per-destination results. Strict format failures return `unplaced` with no
executable hops or endpoints. Passphrases are removed from the returned hops.
`weave plan -f FILE` calls it.

## Status and endpoints

`GET /status` is the scriptable rollup: one entry per stream with its status,
`format_policy` and conditions, and one entry per destination with its own status, nodes,
conditions and endpoint. Its overall `status` is `idle` with no enabled streams,
`degraded` if any stream is failed or degraded, `converging` if any is pending,
and `converged` otherwise, including streams awaiting input.

| Status | When |
|---|---|
| `idle` | The stream is disabled (`enabled: false`). |
| `pending` | The stream is not placed, for example because a node it names has not registered. |
| `failed` | A hop failed to provision. |
| `pending` | A hop is still provisioning or has not reported. |
| `degraded` | A socket that carried media is stalled. |
| `awaiting_input` | Every hop is provisioned and no media enters at the source. |
| `degraded` | Media enters but does not reach every branch. |
| `flowing` | Media flows across every hop to every destination. |

The first row that matches wins. A placed stream that names an offline node, or
whose [formats](streams.md#formats) mismatch, reads `degraded` whatever its
hops report.

`GET /streams/{name}/endpoints` returns `ingress` and a `destinations` list of
`{id, endpoint}`. An endpoint carries the `node` and the `url` a peer dials. An
SRT endpoint also carries `host` and `port`. A WHIP or WHEP endpoint's `url` is
its signalling URL. A device end has a null endpoint.

## Adapter contract

Third-party adapters bind to these routes. See [Nodes](nodes.md) for what a
registration declares.

### Registration

`POST /nodes/register` carries `protocol_version`, which adapters set from
`weave_core::PROTOCOL_VERSION`:

```json
{ "protocol_version": 7, "node": { "id": "strom-node-1", "...": "..." } }
```

The controller accepts only the version it speaks. Any other value, including an
absent field, which reads as `0`, gets `409` and a `warn` log line naming the
node. Nothing about the node is recorded.

The controller reads only `protocol_version` and `node.id` before it answers
`403` for another node's token or `409` for another version, so neither answer
depends on the rest of the body. It then checks the body against this version's
shape and answers `400 invalid_json` naming the first field that does not fit.
A heartbeat from a node the controller does not know gets `404 node_not_found`
before its body is checked, and both shipped nodes then register again.

```json
{
  "code": "incompatible_protocol_version",
  "message": "incompatible southbound protocol version",
  "details": [{
    "field": "protocol_version",
    "code": "unsupported",
    "message": "reported 6; supported 7"
  }]
}
```

`PROTOCOL_VERSION` changes when adapter behavior or payload meaning becomes
incompatible. The controller and the Strom adapter refuse a southbound field or
value they do not define, so any change to a southbound payload is incompatible.

### Desired hops

`GET /nodes/{id}/desired` returns every hop the last reconcile tick computed for
that node.

- A node the tick did not cover, because it is unknown or registered after the
  tick, gets `404 node_not_found`.
- A covered node with nothing to run gets `200 []`, and its adapter removes every
  hop it manages.
- Only a `2xx` is an answer. On any other response, or when southbound is
  unreachable, an adapter keeps what it runs and asks again on its next poll.
  Both shipped nodes do this and keep reporting the hops they run. The
  controller reads an empty `hop_status` as a node running nothing.

The controller runs its first tick before serving, so after a restart with
`DATABASE_URL` it serves the stored hops from the first request. The in-memory
store starts empty, so after a restart without it each node is told to run
nothing once it has registered again.

```json
{
  "id": "weave-cam1-to-studio-sender",
  "node_id": "strom-node-1",
  "profile_id": "srt-forward",
  "role": "sender",
  "ingress": { "transport": "srt", "role": "listen", "port": 20000 },
  "egresses": [
    { "branch_id": "studio", "transport": "srt", "role": "connect", "host": "172.27.0.10", "port": 20000 },
    { "branch_id": "preview", "transport": "srt", "role": "connect", "host": "203.0.113.7", "port": 20001 }
  ]
}
```

Hop ids are `weave-{stream}-sender`, `weave-{stream}-receiver-{destination}` and
`weave-{stream}-bridge-{destination}-{position}`. A hop id is the flow name on
its node. Each destination id is the `branch_id` on every hop that carries it.
Reordering destinations in the manifest changes no hop id, port or desired hop.

### Hop status

A heartbeat reports each hop's ingress, and one entry per desired branch,
matched by `branch_id` in any order. A missing, duplicate or unknown branch id
makes the report incomplete, and the hop stays `pending`, so one healthy branch
cannot hide another. A hop with a `merge_ingress` reports it in a
`merge_ingress` field shaped like `ingress`, and is incomplete without it.
Each report carries `revision`, the lowercase SHA-256 of `open-weave hop` plus
a zero byte followed by the hop's JSON with object keys sorted recursively.
Array order is preserved and JSON has no whitespace. A report for another
revision cannot confirm the current hop. The Strom adapter leaves the revision
empty for an existing flow whose settings do not match the desired hop.

### Inventory

`GET /nodes`, `GET /endpoints` and `GET /state` list every node, endpoint and
hop status, and no node's desired hops. On southbound they accept any node's
token. The controller's `GET /nodes` also accepts the northbound token. Nodes
the controller has not heard from read `offline`, then drop out (see
[Nodes](nodes.md#lifecycle)).
