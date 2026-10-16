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

- `/health` on every service, for healthchecks and load balancers.
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

The schemas carry id patterns, required lists, unknown-field rejection and enum
values. Rules that depend on where a shared type appears, such as `format` being
source-only, are enforced only by the validator at runtime. `/health` and the
dashboard routes are not in OpenAPI.

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
Top-level and endpoint payloads reject unknown fields.

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
| `format_compatible` | Declared formats satisfy every `accepts`. |
| `media_flowing` | Media reaches every destination. |

Each condition has a `status` of `true`, `false` or `unknown`, a stable `reason`
code, a `detail` string and an RFC 3339 `last_transition_time`. The time
changes only when the status changes.

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
any placement reason. Passphrases are removed from the returned hops. `weave
plan -f FILE` calls it.

## Status and endpoints

`GET /status` is the scriptable rollup: one entry per stream with its status
and conditions, and one entry per destination with its own status, nodes,
conditions and endpoint.

```json
{
  "name": "cam1-to-studio",
  "status": "degraded",
  "conditions": [],
  "destinations": [
    {
      "id": "studio",
      "status": "flowing",
      "nodes": ["strom-node-1", "strom-node-2"],
      "conditions": [],
      "endpoint": {
        "node": "strom-node-2",
        "host": "172.27.0.10",
        "port": 20001,
        "url": "srt://172.27.0.10:20001"
      }
    }
  ]
}
```

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
{ "protocol_version": 5, "node": { "id": "strom-node-1", "...": "..." } }
```

The controller accepts only the version it speaks. Any other value, including an
absent field, which reads as `0`, gets `409` and a `warn` log line naming the
node. Nothing about the node is recorded.

```json
{
  "code": "incompatible_protocol_version",
  "message": "incompatible southbound protocol version",
  "details": [{
    "field": "protocol_version",
    "code": "unsupported",
    "message": "reported 4; supported 5"
  }]
}
```

`PROTOCOL_VERSION` changes when adapter behavior or payload meaning becomes
incompatible. Version 5 adds, over 4:

- SRT socket `params` with `passphrase` and `pbkeylen` (see
  [SRT encryption](security.md#srt-encryption)). An adapter at 4 would build its
  end of every keyed link in the clear, and the other end would refuse it.
- A hop profile's `merge`, a desired hop's `merge_ingress`, and the hop status
  that reports it.
- A hop profile's `accepts`, and the `vp8` video codec.
- The `rist` transport and socket.
- A capture device's `tracks`, and a desired hop's `tracks`.

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

```json
{
  "id": "weave-cam1-to-studio-sender",
  "node_id": "strom-node-1",
  "state": "provisioned",
  "ingress": {
    "condition": "flowing",
    "resolved": { "host": "172.26.0.10", "port": 20000 },
    "stats": { "connections": 1, "rate_mbps": 8.1 }
  },
  "egresses": [
    {
      "branch_id": "studio",
      "condition": "flowing",
      "resolved": { "host": "172.27.0.10", "port": 20000 },
      "stats": { "connections": 1, "rate_mbps": 8.0 }
    },
    {
      "branch_id": "preview",
      "condition": "connecting",
      "resolved": { "host": "203.0.113.7", "port": 20001 },
      "stats": { "connections": 0, "rate_mbps": 0.0 }
    }
  ]
}
```

Here `preview` is still connecting, so the stream reads `degraded`.

### Inventory

`GET /nodes`, `GET /endpoints` and `GET /state` list every node, endpoint and
hop status, and no node's desired hops. On southbound they accept any node's
token. The controller's `GET /nodes` also accepts the northbound token. Nodes
the controller has not heard from read `offline`, then drop out (see
[Nodes](nodes.md#lifecycle)).
