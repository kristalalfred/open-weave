# Strom adapter

`weave-adapter-strom` runs beside one [Strom](https://github.com/Eyevinn/strom)
instance. It registers the node with southbound, pulls its desired hops,
creates, starts and deletes Strom flows to match, and reports what Strom runs
from polling `/api/flows`.

```sh
weave-adapter-strom --config examples/node.yaml
```

The config comes from `--config` or `WEAVE_NODE_CONFIG`.
[`examples/node.yaml`](../examples/node.yaml) is annotated. The node token comes
from `WEAVE_SOUTHBOUND_TOKEN` or `node.southbound_token`, and Strom's API key
from `WEAVE_STROM_TOKEN` or `strom.token` (see [Security](security.md)).

## Startup and shutdown

- The adapter needs Strom 0.6.9 or later. It reads `GET /api/version` and exits
  when the version is lower. A Strom that does not answer is asked again on the
  next poll, and a version string the adapter cannot read does not stop it. The
  bench and the node kit run 0.6.10.
- Before it registers, it asks southbound for its node's desired hops, so a
  restarted adapter reports the flows Strom still runs and keeps their ports. A
  `5xx`, or no answer, delays registration to the next poll.
- It creates and deletes no Strom flow until its first registration is accepted.
- A `409` protocol-version mismatch or a `403` at registration is fatal, and the
  adapter exits.
- SIGTERM or Ctrl-C stops the adapter and leaves Strom's flows running.

## Requests

Requests to Strom and to southbound connect within 2 s and fail after 10 s.
`strom.request_timeout_secs` changes the 10 s for Strom, and
`strom.poll_interval_secs` sets the poll interval, default 5 s. A Strom that
does not answer counts as a failed poll: the node reads `degraded` and the
adapter tries again after the poll interval. When it cannot list Strom's flows,
it reports the hops it last fetched as `pending`.

## Drift

Edits made to Strom outside open-weave are drift, as are direct edits to objects
Kubernetes manages. Desired state is the source of truth. The adapter deletes
only flows whose names start with `weave-`.

A flow whose SRT address, latency or key differs from its desired hop, or whose
WHIP or WHEP endpoint id or tracks differ, is deleted and created again. The
adapter writes each SRT socket's latency and key into its `srt://` URI, since
setting an SRT element's `uri` resets both and Strom sets element properties in
no fixed order.

## Socket conditions

The adapter reads an SRT socket's condition from Strom's `srt-stats`, and a
WHIP or WHEP socket's from `webrtc-stats`, which counts each session's RTP
bytes. A WHIP or WHEP socket with no session carrying RTP is `idle`. With one,
it is `flowing` while its bytes advance between polls and `connected` while they
do not. Bytes are followed per session, so a session that ends beside a live
one does not read as a drop.

On either transport, a socket that carried media and whose bytes have not moved
for three polls is `stalled`, whether or not its peer is still connected, and
the stream reads `degraded`.

Strom reports no RIST statistics, so a RIST socket's condition comes from the
SRT side of the same hop. A sender's RIST egress reads `flowing` whenever its
SRT ingress advances, whether or not anything receives what it pushes. A
receiver's RIST ingress reads `flowing` only while its SRT output has a consumer
pulling bytes.

## Restarting stuck callers

A running flow whose SRT caller ingress has not finished a handshake for six
polls in a row (about 30 s) is stopped and started again. Each further restart
waits twice as long, up to 96 polls (about 8 minutes). An `srtsrc` caller that
its listener refused once, as for a wrong passphrase, does not dial again by
itself while Strom keeps reporting the flow running.

A restart stops the whole flow, so every other socket of the hop drops too: a
consumer on its output listener and the peers of its egress callers lose their
connection and dial again. A caller that finished its handshake but carries no
media, because its peer has nothing to send yet, is not restarted. The adapter
reads the handshake from the negotiated latency, which libsrt reports once the
handshake completes. Strom's `connected` says only that data has flowed.
