# open-weave

open-weave is a control plane for live media contribution. You declare the
streams you want in a manifest. The controller plans the SRT, WHIP, WHEP and
RIST links between your media nodes, and an adapter on each node builds them.
No media passes through open-weave.

```yaml
name: cam1-to-studio
source:
  srt: { node: remote-site }
destinations:
  - id: studio
    srt: { node: studio }
```

`weave apply -f` that file, and `weave get stream cam1-to-studio` reads
`flowing` once both nodes have registered, a sender feeds the source, and
something pulls from the studio destination.

## Features

- Manifests name nodes, never addresses. The controller picks which end dials,
  and on which network and port, from what each node declares about its own
  reachability.
- When neither end of a link can reach the other, the controller routes it
  through a relay node.
- SRT links between nodes are encrypted with keys the controller derives.
- Fan-out to many destinations, each with its own status.
- Declared media formats are checked against what each destination accepts. A
  mismatch is reported, and nothing converts.
- Stream status in the CLI, a dashboard at `/ui`, and webhooks.
- Conditional writes, atomic stream sets and dry-run plans for applications
  driving the API.
- Several controllers can share one Postgres. One leads and the others stand by.
- Runtimes: [Strom](https://github.com/Eyevinn/strom), and a browser node.

open-weave routes. It does not carry, convert or schedule media. What to route
and when belongs to an application driving northbound, and media handling
belongs to the runtimes behind the adapters.

## Status

Version `0.1.0`. HTTP routes and payloads can change without a deprecation
window. The features above are verified on the docker-compose bench in
`bench/` and nowhere else, except stream sets and plans, which only unit tests
cover. Redundant paths, which no shipped node can merge, and WHIP senders and
WHEP players outside open-weave are also built and covered only by unit tests.
The deploy kit has run on one Docker host.

Not built: TLS served by the services themselves (put a proxy in front),
Postgres HA, format conversion, and adapters other than Strom. Known gaps are
tracked in [`backlog/`](backlog/).

## Quickstart

The bench runs the control plane, Postgres and four Strom nodes behind
emulated routers. It needs Docker with Compose v2, `just`, a Rust toolchain,
`jq`, `curl` and `bash`.

```sh
just bench up
just bench stream-up basic     # applies a stream, starts ffmpeg, ends with "basic: flowing"
just bench down
```

To run on your own hosts, see [`deploy/`](deploy/README.md).

To run the control plane from source, start each service in its own terminal
with authentication off (local only; [Security](docs/security.md) covers the
tokens):

```sh
export WEAVE_AUTH_DISABLED=1
just run-north                 # 127.0.0.1:9080
just run-south                 # 127.0.0.1:8081
just run-controller            # 127.0.0.1:8082, dashboard at /ui
just apply                     # examples/stream.yaml
just get-stream cam1-to-studio
```

The stream reads `pending` until its nodes register. For each node, run
`just run-strom-adapter --config <file>` against a Strom instance.
[`examples/node.yaml`](examples/node.yaml) is an annotated config for
`strom-node-1` on this machine, with Strom at `http://127.0.0.1:8080`.
`strom-node-2` needs a copy with its own id, listen address, Strom URL and SRT
port range.

`weave --help` lists every command. Output defaults to tables, and
`weave get stream NAME` adds the stream's status, conditions and destinations.
`-o yaml` or `-o json` prints the API response as returned.

## How it works

```text
weave CLI, apps ──▶ northbound ──▶ controller ◀── southbound ◀── adapter ──▶ Strom
```

- **Northbound** (`weave-northbound`) takes desired state from operators and
  applications. The `weave` CLI is one client.
- **Controller** (`weave-controller`) owns all state, plans each stream's hops on
  every reconcile tick, and serves the dashboard.
- **Southbound** (`weave-southbound`) is where adapters register, heartbeat and
  pull their node's desired hops.
- **Adapters** translate hops into a runtime's own API.
  [`weave-adapter-strom`](crates/adapter-strom) drives Strom.
  [`nodes/browser`](nodes/browser/README.md) is a node in a web page.

Northbound and southbound are stateless proxies onto the controller. The
controller makes no outbound calls except an optional webhook.

## Documentation

- [Streams](docs/streams.md): the manifest, endpoints, relays, redundant paths,
  encryption and formats.
- [Nodes](docs/nodes.md): capabilities, topology, and how links, relays and ports
  are chosen.
- [HTTP API](docs/api.md): routes, conditional writes, stream sets, status and
  the adapter contract. OpenAPI and JSON Schema are in [`contracts/`](contracts/).
- [Security](docs/security.md): tokens, TLS and SRT keys.
- [Webhooks](docs/webhooks.md): node and stream events.
- [Operations](docs/operations.md): configuration, controller failover, logging
  and releases.
- [Strom adapter](docs/strom.md): how hops become Strom flows.
- [Bench](bench/README.md): the docker-compose test stack.
- [Deploy](deploy/README.md): compose files for the control plane and a media
  node.
- [Browser node](nodes/browser/README.md): a media node in a web page.

## Development

```sh
just build
just test        # workspace tests and the browser-node check
just lint
just fmt-check
```

## License

MIT. See [`LICENSE`](LICENSE).
