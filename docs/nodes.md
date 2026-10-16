# Nodes

A node registers with southbound and declares two things: the hop shapes its
adapter can build (capabilities), and where it can dial and be reached
(topology). The controller plans links from those declarations and from the
hops each node reports running.

The Strom adapter's capabilities are built in, the Strom profiles under
[Shipped profiles](#shipped-profiles). Its topology comes from `node.topology`
in its config, and [`examples/node.yaml`](../examples/node.yaml) is annotated.
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
    - id: camera-to-whip
      ingress: { device: capture, tracks: [video] }
      egress: { transport: whip, roles: [connect] }
      max_egresses: 1
```

| Field | Meaning |
|---|---|
| `ingress` | One transport (`srt`, `rist`, `whip`, `whep`) with the socket `roles` it supports, or one `device` (`capture`, `display`). |
| `egress` | The same, shared by every egress of the hop. |
| `max_egresses` | Optional limit on egresses per hop. |
| `accepts` | Optional. The media the ingress takes, in the shape of a destination's [`accepts`](streams.md#formats). |
| `merge` | Optional. The hop takes a second ingress of the `ingress` class, `merge_ingress`, and merges two [paths](streams.md#redundant-paths). |

The controller writes the chosen `profile_id` into every desired hop, and the
adapter dispatches on it.

A `capture` device may list the `tracks` it sends, `audio` and `video`, each at
most once. The controller copies the source's tracks onto every hop of the
stream, and the Strom adapter builds only those: a video-only sender gets a flow
with no audio pads. An outside WHIP sender's tracks are the ones its declared
`format` carries. Absent means unknown, and the hops carry both.

### Shipped profiles

| Node | Profiles |
|---|---|
| Strom | `srt-forward`, `whip-to-srt`, `srt-to-whep`, `whip-to-whep`, `srt-to-rist`, `rist-to-srt` |
| Browser | `camera-to-whip`, `whep-to-display`, each with one egress |

A stream from one page to another is relayed through a Strom node's
`whip-to-whep`. Strom's `whip-to-srt` and `whip-to-whep` accept H264 video and
Opus audio. Strom has no profile that fans out over mixed transports, and none
with RIST on both sides.

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

| Variable | Default | Meaning |
|---|---|---|
| `WEAVE_NODE_TTL_SECS` | `15` | Seconds without a heartbeat before a node reads `offline`. |
| `WEAVE_NODE_FORGET_SECS` | `300` | Seconds without a heartbeat before an offline node is removed. |

A node that a stored stream names as its source, a destination or a `via` relay
is never removed and stays listed as `offline`. A removed node's next heartbeat
gets `404 node_not_found`, and both shipped nodes then register again. For a
node loaded from the store, both intervals count from the controller's start, or
from the moment it took the lease.

A placed stream stays placed when a node it names goes offline. Its hops stay
desired, and the stream reads `degraded` until the node heartbeats again.

The controller never dials a node's registered endpoint. A browser node
registers `browser://<id>` as a placeholder. See
[`nodes/browser/README.md`](../nodes/browser/README.md) for the browser node and
[Strom adapter](strom.md) for Strom.
