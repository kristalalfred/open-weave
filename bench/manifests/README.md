# Bench manifests

Apply and drive an SRT manifest by its filename stem:

```sh
just bench stream-up basic
just bench stream-down basic
```

`stream` applies without media; `stream-rm` deletes. Interactive media endpoints
are shared, so driving another stream disconnects the previous one.

| Manifest | Purpose |
|---|---|
| `basic` | listener node-1 → node-2 (canonical contribution) |
| `reverse` | listener node-2 → node-1 (matrix directionality) |
| `same-node` | source and destination both on node-1 |
| `fanout` | one source, two destinations |
| `unplaceable` | `source.node: strom-node-404` (never registers) |
| `disabled` | `enabled: false` |
| `srt-latency` | non-default SRT latency (120ms / 2000ms) |
| `encrypted` | `basic` with a passphrase on the ingress and the output |
| `rist` | `basic` with `allow_cleartext_links: true`, under `just bench topology rist`: the link goes over RIST |
| `via` | pinned transit: node-1 → bridge on node-2 → node-1 |
| `nat-egress` | NAT'd node-3 contributes out to node-1 |
| `nat-ingress` | node-1 delivers into NAT'd node-3 (link reverses) |
| `nat-relay` | both ends on NAT'd node-3; bridged via node-1 |
| `nat-transit` | NAT'd node-3 → NAT'd node-4, no `via`; the controller bridges via node-1 |
| `format-ok` | declared source format the destination accepts |
| `format-mismatch` | 48 kHz source into a 44.1 kHz-only destination |
| `browser-cam` | page camera → node-1 over WHIP, consumer pulls SRT |
| `browser-cam-host` | camera of a page on the docker host → node-1 via the `docker-host` network |
| `browser-return` | producer → node-1 → page screen over WHEP |
| `browser-b2b` | one page's camera → node-1 → another page's screen, WHIP in and WHEP out |

`rist` requires `just bench topology rist`. Restore with
`just bench topology default`; changing topology affects all streams on node 2.

Browser manifests contain placeholder node IDs. Use `just bench browser-stream`,
`just bench browser-b2b` or `just bench host-cam` to fill them. The host camera
manifest selects the `docker-host` network.

The [automated cases](../cases/) define repeatable checks. This table describes
inputs, not measured outcomes.
