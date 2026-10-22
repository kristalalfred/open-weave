# Nodes

A node registers with southbound and declares the hop shapes its adapter can
build (capabilities), where it can dial and be reached (topology), and the
inputs and outputs it offers. The controller plans links from those
declarations and from the hops each node reports running.

The Strom adapter's capabilities are built in, the Strom profiles under
[Shipped profiles](#shipped-profiles). Its topology comes from `node.topology`
in its config; see [`examples/node.yaml`](../examples/node.yaml).
The config takes no `capabilities` key, and the adapter refuses to start on a
config that has one.

## Hop profiles

A registration carries capabilities in this shape:

```yaml
capabilities:
  adapters: []
  hop_profiles:
    - id: srt-forward
      ingress: { transport: srt, roles: [listen, connect] }
      egress: { transport: srt, roles: [listen, connect] }
      media_behavior: preserve
      accepts: {}
    - id: camera-to-whip
      ingress: input
      egress: { transport: whip, roles: [connect] }
      max_egresses: 1
```

| Field | Meaning |
|---|---|
| `ingress` | One transport (`srt`, `rist`, `whip`, `whep`) with the socket `roles` it supports, or `input`: one of the node's [inputs](#inputs-and-outputs). |
| `egress` | One transport, shared by every egress of the hop, or `output`: one of the node's [outputs](#inputs-and-outputs). |
| `max_egresses` | Optional limit on egresses per hop. |
| `accepts` | Optional. The media the ingress takes, in the shape of a destination's [`accepts`](streams.md#formats). |
| `media_behavior` | `preserve` or `unknown` (default). Never inferred from transport or profile id. |
| `timed_delivery` | Optional protocol, roles, latency limit, scheduling bound, clock domain and origin epoch. See [Timed delivery](timed-delivery.md). |
| `merge` | Optional. The hop takes a second ingress of the `ingress` class, `merge_ingress`, and merges two [paths](streams.md#redundant-paths). |

`preserve` declares that the complete application media payload, including
container, tracks and embedded metadata, passes unchanged. Network packet
boundaries and transport encryption may change. It does not promise lossless or
uninterrupted delivery. Remuxing, transcoding, track filtering and payload
rebuilding do not qualify. Strict routing also requires explicit input `accepts`;
`{}` imposes no modeled restrictions. Missing `accepts` remains unknown.

The controller writes the chosen `profile_id` into every desired hop, and the
adapter dispatches on it.

The controller copies the source's tracks onto every hop of the stream, and the
Strom adapter builds only those: a video-only sender gets a flow with no audio
pads. An input source's tracks are the ones its [input](#inputs-and-outputs)
declares. An outside WHIP sender's tracks are the ones its declared `format`
carries. Absent means unknown, and the hops carry both.

### Shipped profiles

| Node | Profiles |
|---|---|
| Native SRT | `srt-forward`, `srt-timed-origin`, `srt-timed-forward`, `srt-timed-receiver` |
| Strom | `srt-forward`, `whip-to-srt`, `srt-to-whep`, `whip-to-whep`, `srt-to-rist`, `rist-to-srt` |
| Browser | `camera-to-whip`, `whep-to-display`, each with one egress |
| Spaghetti | `app-audio-to-srt` with up to eight egresses, `srt-to-speaker` with one |

All native SRT profiles and Strom's `srt-forward` advertise `preserve`, with
`accepts: {}`. Only the native timed receiver advertises merge support. All other
Strom profiles, both browser profiles and both spaghetti profiles advertise
`unknown`. `srt-to-speaker` accepts AAC audio in MPEG-TS. RIST packetization
and WebRTC gateways do not establish output compatibility. Stored profiles
without this metadata remain unknown until registration supplies it.

A stream from one page to another is relayed through a Strom node's
`whip-to-whep`. Strom's `whip-to-srt` and `whip-to-whep` accept H264 video and
Opus audio. Strom has no profile that fans out over mixed transports, and none
with RIST on both sides.

## Inputs and outputs

An input is media that exists on the node independently of any stream, such as
a camera or an application's audio. An output presents media on the node, such
as a video element or a set of speakers. A stream names an input as its
[source](streams.md#endpoints) and outputs as destinations.

```yaml
inputs:
  - { id: music, label: Music, tracks: [audio] }
outputs:
  - { id: macbook-pro-speakers, label: MacBook Pro Speakers }
```

| Field | Meaning |
|---|---|
| `id` | Follows the [identifier rule](api.md#identifiers). Unique among the node's inputs, and among its outputs. |
| `label` | Display text. Must not be blank. |
| `tracks` | Inputs only. The tracks the input sends, `audio` and `video`, each at most once. |

A registration declares `node.inputs` and `node.outputs`. Each heartbeat
carries the full current `inputs` and `outputs`, which replace the declared
ones, so a node can add and remove them while it runs. A heartbeat whose inputs
or outputs fail these rules gets `400` and changes nothing.

A stream endpoint picks an input or output by `id`. Without an `id`, the node
must declare exactly one of that kind. The controller plans the endpoint only
when the node also has a hop profile whose `ingress` is `input` or whose
`egress` is `output`.

| Case | Placement |
|---|---|
| The named `id` is not declared | `placement_ready` is `false` with reason `input_missing` or `output_missing`. |
| No `id`, and the node declares none | The same reasons. |
| No `id`, and the node declares several | `placement_failed`; the detail lists the ids. |
| No hop profile takes an input or feeds an output | `placement_failed`. |

When an input or output disappears from a heartbeat, the next reconcile tick
removes the stream's hops from desired state. When it returns, the stream is
placed again.

The browser node declares the input `camera` and the output `player`. The
spaghetti adapter declares each running app under `/Applications`,
`/System/Applications` or `~/Applications` as an audio input, and each output
device as an output, with ids made from their names. The native SRT node and
the Strom adapter declare none.

## Topology

```yaml
topology:
  attachments:
    - id: wan
      network: internet
      dial: true
      listeners:
        srt:
          host: 203.0.113.10
          port_range: { start: 20000, end: 20100 }
        whip: { base_url: https://media.example/whip }
    - id: production
      network: studio-lan
      dial: true
      listeners:
        srt:
          host: 10.20.0.10
          port_range: { start: 21000, end: 21100 }
```

Attachment ids are local to the node. Network ids name routing domains shared
between nodes. `dial: true` means the node can dial out from that attachment.
`listeners` are what peers can reach: `srt` and `rist` take a `host` and a
`port_range`, and `whip` and `whep` a `base_url`. An attachment that dials and
has no listeners is a node behind NAT.

RIST is the simple profile: the receiver binds an even port for RTP and the next
one for RTCP, and the sender pushes to both. SRT and RIST can share a port
range. An SRT socket takes half of a free RIST pair only when no other port in
the range is free.

## Planning

A link is possible when one end offers `connect` and has a dialing attachment,
and the other offers `listen` with a listener on the same network. Which
transports a node supports comes only from its hop profiles. Of the possible
links, the controller takes the lowest transport, network, attachment, address,
URL and port. RIST comes after SRT, WHIP and WHEP, and only for a stream that
[allows cleartext links](streams.md#cleartext-links). Only the downstream end of
a RIST link can listen, so a receiver behind NAT cannot take RIST.

Streams are planned in name order, and a stream's destinations in id order.

### Relays

When no direct link works, the controller tries one online transit node whose
profile supports the needed ingress-to-egress shape and whose attachments carry
both halves. Of those, it takes the lowest id that has a free port for each SRT
listener the bridge would host, so a fan-out that fills one relay's port range
moves on to the next. When every candidate is out of ports, the stream stays
unplaced and the reason names the lowest-id one. Any node with a fitting
profile can relay. There is no `relay` flag.

A bridge stays on the relay that reports running it while that relay is online,
still qualifies, has the ports, and does not report the bridge `failed`. An
earlier relay coming back does not move a working stream.

### Ports

A listening socket keeps the SRT or RIST port its node reports it running on,
as long as the report is not `failed` and the port is still in range. No other
socket is given that port. A new stream or destination takes other ports, or the
next relay when the held ones were a relay's last, and a hop that starts or
fails moves no other hop's port.

A node reports a listener at its own listener host or at `0.0.0.0`. A socket
reported at another host is taken as a caller and holds no port.

## Lifecycle

[Operations](operations.md#configuration) lists the heartbeat TTL and forget interval.

A node that a stored stream names as its source, a destination or a `via` relay
is never removed and stays listed as `offline`. A removed node's next heartbeat
gets `404 node_not_found`, and shipped nodes then register again. For a
node loaded from the store, both intervals count from the controller's start, or
from the moment it took the lease.

A placed stream stays placed when a node it names goes offline. Its hops stay
desired, and the stream reads `degraded` until the node heartbeats again.

The controller never dials a node's registered endpoint. A browser node
registers `browser://<id>` as a placeholder. See
[`nodes/browser/README.md`](../nodes/browser/README.md) for the browser node and
[Strom adapter](strom.md) for Strom.

The native runtime starts with `just run-node --config examples/native-node.yaml`.
It uses `node` topology and control settings, plus an explicit clock provider.
The example uses a shared host clock for local testing. Separate hosts need
externally supplied synchronization evidence as described in
[Timed delivery](timed-delivery.md#clock-configuration).
