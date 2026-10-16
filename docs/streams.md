# Streams

A stream carries one source to one or more destinations. Its manifest names
nodes for managed endpoints. External SRT destinations name a remote address.
The controller chooses links between nodes from their declarations (see
[Nodes](nodes.md)).

```yaml
name: cam1-to-studio
source:
  srt: { node: strom-node-1 }
destinations:
  - id: studio
    srt: { node: strom-node-2 }
  - id: preview
    srt: { node: strom-node-2, network: internet }
```

[`examples/stream.yaml`](../examples/stream.yaml) is a complete example, and
[`contracts/json-schema/`](../contracts/json-schema/) has every field. Names and
ids follow the [identifier rule](api.md#identifiers). `weave apply -f` stores a
stream, and `weave plan -f` previews its placement without storing it.

## Endpoints

| Endpoint | Used as | Meaning |
|---|---|---|
| `srt: { node }` | source, destination | A producer or consumer outside open-weave dials an SRT listener on `node`. |
| `srt: { remote: { host, port, network } }` | destination | The stream dials an SRT listener outside open-weave. No node hosts it. |
| `whip: { node }` | source | A WHIP sender outside open-weave, such as an encoder, publishes to `node`. |
| `whep: { node }` | destination | A WHEP player outside open-weave plays from `node`. |
| `device: { node }` | source, destination | The node's own capture device or display. Only the browser node has one. |

`network` pins the shared network a peer reaches the node on. Without it, the
controller takes the node's listener on the lowest network id, then the lowest
attachment id. A `remote` listener must name its network.
`GET /streams/{name}/endpoints` (`weave get endpoints NAME`) reports the address
or URL each peer dials.

### WHIP and WHEP peers

A `whip` source puts the stream's sender hop on its node, with the ingress at
that node's `whip` base URL. A `whep` destination puts the receiver hop on its
node, with the last egress at the `whep` base URL. The URLs end in the hop id:

- `{base}/weave-{stream}-sender`
- `{base}/weave-{stream}-receiver-{destination}`

A node that declares no base URL for the transport cannot take the peer, and the
stream stays unplaced. On Strom the sender runs `whip-to-srt` and the receiver
`srt-to-whep`, so a WHIP source and a WHEP destination on one node are two hops
joined over SRT. Strom's `whip_input` takes H264 video, Opus audio and one
session per endpoint. Strom serves `/whip` and `/whep` without authentication
(see [Security](security.md#strom)).

## Relays

The controller inserts a relay on its own when no direct link works (see
[Nodes](nodes.md#relays)). `via` pins the relays an SRT destination passes
through, upstream first. Each becomes a bridge hop, and the chain is used even
where a direct link would work.

```yaml
destinations:
  - id: studio
    srt: { node: studio-node, via: [edge-relay] }
```

## Redundant paths

```yaml
destinations:
  - id: studio
    paths: 2
    srt: { node: studio-node }
```

`paths` is 1 or 2 and defaults to 1. A `remote` destination and one that pins
`via` cannot ask for two.

The controller places every destination's first path as it would with
`paths: 1`, then each second path. The second path passes through no relay the first one uses. At the sender and
the receiver it uses no attachment the first one may carry its link on. A
dialing socket names no local address, so where an end dials, every dialing
attachment on that network counts as used, and two paths that both dial out of
one node leave over different networks.

A second path is placed only when the receiver has a hop profile with
`merge: true` (see [Nodes](nodes.md#hop-profiles)). No shipped node advertises
`merge`, and the Strom adapter refuses a hop with a `merge_ingress`.

When the second path cannot be placed, the stream is placed with one.
`placement_ready` on the stream and on the destination stays `true` with reason
`single_path`, and the detail names the destination and why. `POST
/stream-plans` returns the same detail as its `reason`.

A running second path is left alone where it can be. A first path avoids the
relays and ports its own running second path holds while another relay has
room. When none has, the second path moves, or the
destination drops to one path, rather than the stream going unplaced.

The second path's branch id is `{destination}.2` and its bridges are
`weave-{stream}-bridge-{destination}.2-{position}`. Destination ids cannot
contain `.`, so these never equal another destination's ids. Adding a second
path changes none of the first path's hop ids, ports or branch ids. The
destination's status covers both paths, so one path down reads `degraded`.

## Encryption

SRT links between nodes are always encrypted with keys the controller derives
(see [Security](security.md#srt-encryption)). RIST links are not (see
[Cleartext links](#cleartext-links)). A socket that something outside
open-weave dials, or that dials out to it, takes its key from `passphrase` on
the `srt` endpoint: the source ingress a producer dials, the output a consumer
dials, or a `remote` listener.

```yaml
source:
  srt: { node: strom-node-1, passphrase: producer-shared-passphrase }
destinations:
  - id: uplink
    srt:
      remote: { host: 198.51.100.5, port: 9000, network: internet }
      passphrase: far-end-shared-passphrase
```

A passphrase is 10 to 80 printable ASCII characters, space to `~`. The peer
must use the same passphrase. Its key length may differ, since SRT settles on
one at connect. A socket without a passphrase runs in the clear.

### Cleartext links

RIST links carry no encryption. The controller plans a RIST link between nodes
only for a stream that sets `allow_cleartext_links: true`, and only where SRT,
WHIP and WHEP cannot carry the link. The field defaults to `false` and is
left out of returned specs when false. Without it,
a stream that only RIST can carry stays unplaced: `placement_ready` is `false`
with reason `cleartext_not_allowed`, and the detail names the node the link runs
into. SRT links are keyed either way.

```yaml
name: rist
allow_cleartext_links: true
source:
  srt: { node: strom-node-1 }
destinations:
  - id: output
    srt: { node: strom-node-2 }
```

## Formats

A source may declare the `format` its producer sends, and a destination what it
`accepts`. `srt` and `whip` sources take `format`; `srt` and `whep` destinations
take `accepts`.

```yaml
source:
  srt:
    node: strom-node-1
    format:
      container: mpeg_ts
      video:
        codec: h264
        width: 1920
        height: 1080
        framerate: { numerator: 25, denominator: 1 }
        chroma_subsampling: yuv422
      audio: { codec: aac, sample_rate: 48000, channels: 1 }
destinations:
  - id: studio
    srt:
      node: studio-node
      accepts:
        audio: { sample_rate: [44100] }
```

A source declares the prepared feed after any processing outside open-weave.
A present `format` fixes the container and tracks. Omitted tracks are absent;
an absent `format` is unknown. A destination's omitted `accepts` is unknown,
while `accepts: {}` explicitly imposes no modeled restrictions. Supplied
constraints must all match; a track constraint requires that track to exist.

`format_policy` defaults to `report`. Omission and explicit `report` are equal
for no-op writes, generations and revisions. `strict` accepts desired state but
admits a route only when every selected path to every destination is compatible.
One failing destination blocks the whole stream. Stream-set members use the same
policy. Disabled streams create no hops.

The controller checks each selected profile's input constraints in route order.
A [`preserve` profile](nodes.md#hop-profiles) carries the known format forward;
an `unknown` profile makes its output format unknown. Missing profile constraints
make compatibility unknown without erasing a known format. A known mismatch
takes precedence over unknown declarations, including at a later boundary.
Destination requirements are compared with the delivered format.

Strict routing considers eligible alternatives before committing ports. When
format checking blocks placement, the stream reads `pending`, has no desired
hops or published endpoints, and retains per-destination format conditions.
Changes to declarations, policy, capabilities or topology trigger reevaluation.
An active route that becomes blocked is removed through desired state, including
when switching from `report` to `strict`; there is no old-route fallback.
Removal is asynchronous. A disconnected adapter can retain flows until it polls.
Strict admission is not an immediate cutoff or a security boundary.

Under `report`, known mismatches read `degraded` without preventing routing.
Unknown compatibility does not independently degrade a stream, so `flowing` can
coexist with unknown declarations. The controller does not inspect media or build
processing pipelines. Declared compatibility does not prove decoding, delivery,
or correctness of declarations. Nothing converts or negotiates an output format.
