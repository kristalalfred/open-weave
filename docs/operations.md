# Operations

## Configuration

Secrets and tokens are listed in [Security](security.md), and webhook settings
in [Webhooks](webhooks.md).

| Variable | Used by | Default | Meaning |
|---|---|---|---|
| `WEAVE_NORTHBOUND_ADDR` | northbound | `127.0.0.1:9080` | Listen address, or `--listen`. |
| `WEAVE_SOUTHBOUND_ADDR` | southbound | `127.0.0.1:8081` | Listen address, or `--listen`. |
| `WEAVE_CONTROLLER_ADDR` | controller | `127.0.0.1:8082` | Listen address, or `--listen`. |
| `WEAVE_CONTROLLER_URL` | northbound, southbound | `http://127.0.0.1:8082` | Comma-separated controller URLs, or `--controller-url`. |
| `DATABASE_URL` | controller | unset | Postgres URL. Unset keeps state in memory, and a restart loses it. |
| `WEAVE_RECONCILE_INTERVAL_SECS` | controller | `5` | Seconds between reconcile ticks. |
| `WEAVE_NODE_TTL_SECS` | controller | `15` | Seconds without a heartbeat before a node reads `offline`. |
| `WEAVE_NODE_FORGET_SECS` | controller | `300` | Seconds without a heartbeat before an offline node is removed. |
| `WEAVE_LEASE_TTL_SECS` | controller | `10` | Seconds the lease lasts without a renewal. |
| `WEAVE_STUN_SERVERS` | controller | `stun.cloudflare.com:3478,stun.l.google.com:19302` | Comma-separated `host:port` STUN servers put in every WebRTC link. |
| `WEAVE_TURN_SERVERS` | controller | unset | Comma-separated `host:port` UDP TURN servers put in every WebRTC link. Needs `WEAVE_TURN_SECRET`, or `WEAVE_TURN_USERNAME` and `WEAVE_TURN_CREDENTIAL`, and not both. |
| `WEAVE_TURN_SECRET` | controller | unset | The TURN server's shared secret. Each link gets its own time-limited login. |
| `WEAVE_TURN_CREDENTIAL_TTL_SECS` | controller | `86400` | Lifetime of a login minted from `WEAVE_TURN_SECRET`, above 600. |
| `WEAVE_TURN_USERNAME`, `WEAVE_TURN_CREDENTIAL` | controller | unset | One static TURN login given to every link. It never expires. |
| `WEAVE_NODE_CONFIG` | Strom adapter | none | Node config path, or `--config`. The adapter does not start without one. |
| `WEAVE_NODE_CONFIG` | native SRT node | none | Native node config path, or `--config`. |
| `WEAVE_NORTHBOUND_URL` | `weave` | `http://127.0.0.1:9080` | Northbound base URL, or `--url`. |
| `SSL_CERT_FILE` | `weave`, services, Strom adapter | unset | PEM file that replaces the platform certificate store for every outbound HTTPS request. |
| `RUST_LOG` | services, Strom adapter | `info` | Log level. |
| `WEAVE_LOG_FORMAT` | services, Strom adapter | text | `json` writes one JSON object per line. |

## Logging

The services and the adapter log to standard output. The `weave` CLI logs
nothing. It prints its result to standard output, in the format `-o` selects,
and errors to standard error.

## Controller failover

With `DATABASE_URL` set, a controller serves only while it holds a lease, a row
in Postgres. Any number of controllers can share one database: one leads and the
others stand by. Without `DATABASE_URL` there is no lease and one controller.

- A standby answers `503 not_leader` on every route except `/health`, `/` and
  `/ui`. It runs no reconcile tick and generates no new webhook events. `/health` answers `200`
  on both, and `/status` tells them apart.
- The leader renews the lease every third of the TTL. When no renewal has
  succeeded for two thirds of the TTL, it stops serving and writing. The lease
  runs out on the Postgres clock a third of the TTL later, so no standby takes
  over while the old leader still serves.
- A standby tries for the lease every second, or every third of the TTL if that
  is shorter. On getting it, it loads the stored state, runs one reconcile tick,
  then serves. It plans from the streams, registrations and node reports in
  Postgres, so it serves the hops the old leader served and adapters keep their
  flows.
- Every write checks the lease in its own transaction and fails with
  `503 not_leader` unless the writer holds it. A takeover waits for a write
  already past that check, and the new leader loads it.
- On SIGTERM or SIGINT the leader gives up the lease, and a standby takes over
  on its next try. A leader that crashes holds the lease until it runs out, so a
  lone controller restarting after a crash answers `503 not_leader` until then.
- Every controller needs the same `WEAVE_SRT_KEY_SECRET`, `WEAVE_SOUTHBOUND_KEY`,
  `WEAVE_SOUTHBOUND_MIN_EPOCHS` and STUN and TURN settings. With another SRT key
  secret, a new leader rekeys every link and adapters recreate their flows. With
  another southbound key, it refuses every node's token. With other minimum
  epochs, it accepts tokens the old leader refused, or the reverse. With other
  STUN or TURN settings, adapters rebuild every WebRTC link.
- Postgres is one instance. While no controller can reach it, none leads.

### What survives a takeover

Streams, stream sets, node registrations, generations, revisions and ETags live
in Postgres, as do the hops nodes report and the conditions the last tick
computed. A heartbeat writes the node's registration when its status, a hop's
state, a socket's condition or timed-delivery readiness changes. Rates,
addresses and delivery counters alone write nothing.
A tick writes each node it marks `offline` and each stream whose conditions
changed. A failed write is tried again on the next tick.

The new leader's first tick reads all of it, so a bridge stays on its relay, a
flowing stream keeps reading `flowing`, and each condition keeps its transition
time. Heartbeat times are not stored, so the new leader counts every stored node
as heard at the takeover. A restart with Postgres works the same way.

### Schema version

The `schema_version` table records version `2`. Empty and unversioned databases
are initialized at that version. Version `1` migrates transactionally by adding
`streams.source_revision`, seeded from each stream's current revision. Existing
stream definitions, generations and ETags are preserved. Other versions are
refused without changing the database. Back up the database before upgrading;
older controllers refuse version `2`.

### Proxies

Northbound and southbound take every controller in `WEAVE_CONTROLLER_URL`. Each
entry needs an `http` or `https` scheme and a host, or the proxy refuses to
start.

- A request goes first to the controller that answered last.
- The proxy moves to the next controller when it cannot connect within two
  seconds or gets `503 not_leader`. Neither has acted on the request, so a write
  is never sent twice.
- Each controller attempt has a ten-second timeout. A `GET` that gets no answer, or
  fails after connecting, moves on to the next controller. Any other request is
  not sent again, since the controller may have applied it. A timeout returns
  `504 controller_timeout`; other connection or response failures return
  `502 controller_unreachable`. The next request tries another controller first.
- Every other answer is passed back unchanged. When no controller leads, the
  proxy returns `503 not_leader`, and when none can be reached,
  `502 controller_unreachable`.

While no controller leads, the shipped nodes get `503` or
`502` from southbound, keep every hop they run, and register again once a
leader answers. Each registration sends `node.registered`.

## Releases

Each `v*` tag publishes the image `ghcr.io/kristalalfred/open-weave` for
linux/amd64 and linux/arm64, and a GitHub release with the six binaries for
x86_64 and aarch64 Linux and aarch64 macOS. The tag must be `v` followed by the
workspace version. A tag with a pre-release part, such as `v0.1.0-rc.1`, makes a
pre-release and moves neither `latest` nor `0.1`. The image and binary builds also run, without publishing, on every push to
`main` and every pull request.

[`deploy/README.md`](../deploy/README.md) covers running the image.
