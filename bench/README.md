# Media bench

Docker Compose runs northbound, southbound, two controllers, Postgres and four
Strom 0.6.10 nodes behind routers with `netem`. The stack starts without streams.

Requires Docker with Compose v2, Rust, `just`, `jq`, `curl`, `bash` and Python 3.
Run recipes from the repository root:

```sh
just bench up
just bench stream-up basic
just bench status
just bench stream-down basic
just bench down
```

`stream-up` applies the manifest, starts an ffmpeg producer and consumers, and
waits for `flowing`. `stream-down` removes them. `down` removes containers,
networks and volumes. `just bench` lists the recipes.

`just bench leader` identifies the active dashboard at
<http://localhost:29082/ui> or <http://localhost:29083/ui>.

## Automated media tests

Start an empty bench, then run the corpus:

```sh
just bench up
just bench e2e list
just bench e2e check
just bench e2e run
just bench e2e run 02-fanout 08-relay-recovery
just bench e2e schema
```

Interactive terminals show a live panel for the active case. It shows the
current steps, elapsed time and timeout or observation window, stream and node
status, receiver progress, retries, and cleanup activity. The panel collapses
to one result line when the case finishes. `V` counts decoded video frames and
`A` counts decoded audio samples; idle ages are video/audio milliseconds since
the last reported progress.

Redirected stderr and CI logs show step transitions and a heartbeat every ten
seconds, without cursor controls. `run --progress plain` forces that output;
`run --progress off` prints result lines only. Detailed JSONL diagnostics are
recorded in every mode.

The Rust runner reads YAML cases in `cases/`. It gives each stream its own
producer and one consumer per destination, so several streams can carry media
at once. Peers on the core network reach nodes through their routers. Peers at
NAT sites run inside that site's network. The route manager installs their
routes from bench labels, without adding routes into NAT sites.

Each consumer decodes video and audio. `await` requires media decoded after
the step began when the requested status is `flowing`. `observe` checks video
and audio progress throughout its window, including while its `during` actions
execute.
It fails on gaps above `max_gap_ms` or receiver restarts above `max_restarts`.
Defaults are 3000 ms and zero restarts. These are sampled decoder measurements.
Each feed has a distinct grayscale video marker and audio tone. Consumers check
both identities after decoding. A mismatch can indicate routing errors or
damaged media. These synthetic checks do not measure packet
loss, picture quality, end-to-end latency, or A/V synchronization.

Cases use these actions:

| Action | Inputs and check |
|---|---|
| `apply` | `file` relative to the case; `stream` is a case-local alias. `condition` is `current` (default), `absent`, `missing`, or `{snapshot: name}`; `expect` defaults to HTTP 202. |
| `drive` | Start a producer and all destination consumers for `stream`. |
| `undrive` | Remove a stream's producer and consumers without deleting its desired state. |
| `runtime` | `stop` or `delete` a stream's managed flows on `node` 1 through 4. |
| `await_control` | Require `/status` to answer HTTP `status` within `timeout_secs`. |
| `producer`, `consumer` | Set `running` for a stream's producer or a `receiver` such as `main/output`. |
| `await` | Wait for `status`, optional `destination`, `nodes` and `format_reason`, within `timeout_secs` (default 90). |
| `flows` | Require running hop counts in `nodes`; omitted nodes must have zero. |
| `observe` | Watch `receivers` for at least `seconds`; also watch until every `during` action finishes. |
| `service` | `stop`, `start`, `restart`, `kill`, `pause`, or `unpause` a bench control service, Postgres, adapter, or Strom; `leader` selects the current controller leader. `start` and `restart` wait until the container runs and passes its healthcheck. |
| `netem`, `clear_netem` | Apply individual `tc` tokens in `args`, or clear them, on `node1` through `node4`. |
| `snapshot`, `unchanged` | Capture a stream's ETag, generation, and runtime flow identities with `as_name`; compare using `snapshot`. |
| `delete` | Remove endpoints and the stream; wait for its managed flows to disappear. |
| `wait` | Wait `seconds` within a sequence or observation. |

Copy an existing case and change its steps to add a test. `schema` prints the
JSON Schema for the case format. `check` validates cases and their manifests
without needing Docker. Unknown actions and fields are errors.

A run requires the bench services to be running, all four media nodes ready,
no streams or managed flows, and no existing router impairment. Runs are
serialized with `bench/.e2e.lock`. The runner removes its streams and endpoint
containers, clears its impairments, and starts services it touched after
success, failure, Ctrl-C, or SIGTERM. A failed cleanup makes the run fail.
SIGKILL cannot run cleanup; check for leftover resources before removing a stale
lock.

Results go to `bench/results/<run>/`: each case has a timestamped JSONL timeline,
a result, its YAML, and service logs. The run has `summary.json`. The timeline
includes API observations, Strom statistics, and decoder progress. SRT
passphrases are redacted. The media image is pinned by digest, and its identity
is recorded; `--media-image` overrides it. The runner pulls it when absent.
Installed service image IDs are recorded separately.

The corpus covers direct links, fan-out, concurrent feeds, controller
failover, source reconnects, delay/loss, site partitions, relay replacement,
encrypted endpoints, southbound outages, a media runtime restart, and strict
declared compatibility admission, blocking, removal and recovery. The local
[declared-media evidence](evidence/declared-media.json) records decoded media
and control-plane results separately from unit tests. It is not independent
equipment verification. Its
external peers use SRT and require both video and audio. Browser, external
WHIP/WHEP, and RIST-specific cases are not in this corpus. Router impairment
affects control traffic as well as media. The Media bench CI job runs the corpus
and uploads results, including on failure.

## Interactive streams

[Manifests](manifests/README.md) cover SRT, RIST, browser, relay and format cases.
`just bench stream-ls` lists names. `just bench stream NAME` applies without
media; `just bench stream-rm NAME` deletes it.

The interactive producer and two consumers are singletons. Driving another
stream takes them from the previous one. Addresses come from northbound's
endpoint discovery. NAT endpoints run inside the corresponding node subnet.
The automated runner creates separate endpoints for each stream.

## Topology and impairment

| Network | Subnet | Reachability |
|---|---|---|
| `net_core` | `10.97.25.0/24` | Control plane and external test peers |
| `net_node1` | `10.97.26.0/24` | Routed through router 1 |
| `net_node2` | `10.97.27.0/24` | Routed through router 2 |
| `net_node3` | `10.97.29.0/24` | Outbound NAT through router 3 |
| `net_node4` | `10.97.30.0/24` | Outbound NAT through router 4 |

The route manager restores routes after container restarts. It adds no inbound
routes to nodes 3 and 4. Router impairment affects control traffic and media.

```sh
just bench netem node1 delay 200ms loss 5%
just bench netem-clear node1
```

## Browser nodes

The `browser` profile runs Chromium with fake media devices inside `net_core`.

```sh
just bench browser-up
just bench browser-stream
just bench browser-2-up
just bench browser-b2b
just bench browser-down
```

`browser-up video` captures video only. `browser-stream` fills the camera and
return templates with the page's node ID and attaches SRT test peers.
`browser-b2b` connects the two pages through Strom's WHIP/WHEP profile.
Video-only restart recovery on the pinned Strom needs
[reverification](../backlog/OW-46-video-only-page-restart-waits.md).

For a browser on the Docker host:

```sh
just bench page 8000 guest-1
just bench host-cam guest-1
just bench host-cam-down
```

Open the URL printed by `page`. The `docker-host` attachment uses signalling at
`localhost:28080`; SRT endpoints remain on the node subnet. See the
[browser node](../nodes/browser/README.md) for fragment options and camera access.

## Webhooks

`just bench hook-sink` listens on host port 29099 and prints deliveries and their
Authorization header. Start it before `just bench up`. Both controllers default
to `http://host.docker.internal:29099` with token `bench-webhook-token`.
Export `WEAVE_WEBHOOK_URL=` before starting the stack to disable delivery.
See [Webhooks](../docs/webhooks.md) for payloads and delivery limitations.

## Host ports

| Port | Service |
|------|---------|
| 29443 | northbound over TLS, which the recipes use (`SSL_CERT_FILE=bench/tls/public/ca.pem WEAVE_NORTHBOUND_URL=https://localhost:29443 weave ...`) |
| 29444 | southbound over TLS, which the recipes use |
| 29080 | northbound, plaintext; `scripts/endpoints.sh` uses it |
| 29081 | southbound, plaintext (`/nodes` shows registered capabilities; `just bench page` uses it) |
| 29082 | controller: dashboard at `/ui`, `/view`; API at `/status`, `/streams/{name}/endpoints` |
| 29083 | controller-2, the same routes; whichever does not lead answers `503 not_leader` |
| 28080 | strom-1 API; `/api` routes need `WEAVE_STROM_TOKEN` |
| 28081 | strom-2 API |
| 28082 | strom-3 API (host→container; grants no route into net_node3) |
| 28083 | strom-4 API (host→container; grants no route into net_node4) |
| 29099 | `just bench hook-sink` on this host, where the controller delivers webhooks |

## Authentication and TLS

The bench enables authentication with development credentials:

| Variable | Default | Used by |
|---|---|---|
| `WEAVE_NORTHBOUND_TOKEN` | `bench-northbound-token` | northbound, both controllers, CLI, `endpoints.sh` |
| `WEAVE_SOUTHBOUND_KEY` | `bench-southbound-key-for-local-use-only` | southbound, both controllers, `just bench node-token` |
| `WEAVE_ADAPTER_{1,2,3,4}_TOKEN` | `strom-node-{1,2,3,4}`'s epoch-0 token under the default key | adapter-1 to adapter-4; the recipes present node 1's for southbound reads |
| `WEAVE_BROWSER_TOKEN` | `browser-bench`'s epoch-0 token under the default key | the in-bench browser page, which takes its node id from it |
| `WEAVE_BROWSER_2_TOKEN` | `browser-bench-2`'s epoch-0 token under the default key | the second in-bench page (`browser-2`) |
| `WEAVE_SRT_KEY_SECRET` | `bench-srt-key-secret-for-local-use-only` | both controllers, to derive the keys of SRT links between nodes |
| `WEAVE_SOUTHBOUND_MIN_EPOCHS` | unset | southbound, both controllers: `<id>=<epoch>` pairs that revoke a node's older tokens |
| `WEAVE_STROM_TOKEN` | `bench-strom-key-for-local-use-only` | every Strom as `STROM_API_KEY`, every adapter, the recipes and `weave-bench`; `/whip` and `/whep` need no key |

If you change `WEAVE_SOUTHBOUND_KEY`, also set the four adapter and two browser
token variables. `just bench node-token ID` derives a token under the current
key. Recipes supply authentication headers; direct API calls must supply them.

`tls-certs` creates a private CA and certificate in the ignored `bench/tls/`
directory. nginx terminates TLS. Adapters trust the CA through `SSL_CERT_FILE`;
the browser container imports it into Chromium and uses `NODE_EXTRA_CA_CERTS`
for its Node.js requests. CLI recipes use `SSL_CERT_FILE`, and curl recipes use
`--cacert`. `just bench auth-check` and `just bench tls-check` exercise both.

The endpoint-discovery script and host browser recipe use the published plain
HTTP ports. Controller dashboards are unauthenticated and the bench publishes
ports on all interfaces. Keep this stack off public networks.
