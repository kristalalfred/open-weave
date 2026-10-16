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

HTTP routes and payloads may change between pre-release versions without a
deprecation window. See [open issues](BACKLOG.md) and the
[media test cases](bench/cases/).

## Quickstart

The bench runs the control plane, Postgres and four Strom nodes behind
emulated routers. It needs Docker with Compose v2, `just`, a Rust toolchain,
`jq`, `curl` and `bash`.

```sh
just bench up
just bench stream-up basic
just bench down
```

To run on your own hosts, see [`deploy/`](deploy/README.md).

To run the control plane from source, start each service in its own terminal
with authentication off (local only; [Security](docs/security.md) covers the
tokens):

```sh
export WEAVE_AUTH_DISABLED=1
just run-north
just run-south
just run-controller
just apply
just get-stream cam1-to-studio
```

The stream reads `pending` until its nodes register. For each node, run
`just run-strom-adapter --config <file>` against a Strom instance.
[`examples/node.yaml`](examples/node.yaml) is a config for
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
controller makes no outbound HTTP calls except configured webhooks.

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
just test
just lint
just fmt-check
```

## License

MIT. See [`LICENSE`](LICENSE).
