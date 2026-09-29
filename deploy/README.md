# Deploying open-weave

A reference for running open-weave outside the bench. **Nothing here has been
run end to end outside the bench.** The compose files pass `docker compose
config` and the image builds and starts; a control plane and node brought up
from these files, with real nodes registering, has not been tried.

- `control-plane/`: northbound, southbound, two controllers, Postgres and an
  nginx that terminates TLS.
- `node/`: Strom, the Strom adapter and a `node.yaml` template, for each media
  node.

Both use the image `ghcr.io/kristalalfred/open-weave`, published on each `v*`
tag. To build it yourself, run `docker build -t open-weave:local .` at the
repository root and set `WEAVE_IMAGE=open-weave:local`. The image holds `weave`,
`weave-northbound`, `weave-southbound`, `weave-controller` and
`weave-adapter-strom`, runs as uid 10001 and starts under `tini`.

## Control plane

1. Copy `control-plane/env.example` to `control-plane/.env` and fill in the four
   secrets, each from `openssl rand -hex 32`. `WEAVE_SOUTHBOUND_KEY` and
   `WEAVE_SRT_KEY_SECRET` must be at least 32 characters. Every controller needs
   the same values; the compose file gives both the same.
2. Put a certificate and key in `control-plane/tls/` as `server.pem` and
   `server.key`. The certificate has to cover the name nodes and operators dial.
3. `docker compose up -d` in `control-plane/`.

### Ports

| Port | Service | Open to |
|---|---|---|
| 443 | nginx to northbound | operators |
| 8443 | nginx to southbound | every node |
| 8082, 8083 | controller dashboards at `/ui` | published on `127.0.0.1` only |

Postgres and the plain HTTP ports of northbound (9080), southbound (8081) and the
controllers (8082) are not published. They are reachable on the compose network.

### Secrets

| Variable | Held by | Notes |
|---|---|---|
| `WEAVE_NORTHBOUND_TOKEN` | operators, northbound, controllers | The one shared operator token. The CLI reads it from its environment. |
| `WEAVE_SOUTHBOUND_KEY` | southbound, controllers | Node tokens derive from it. Never sent as a token. |
| `WEAVE_SRT_KEY_SECRET` | controllers | SRT link keys derive from it. Changing it rekeys every link. |
| `POSTGRES_PASSWORD` | Postgres, controllers | |
| `WEAVE_SOUTHBOUND_MIN_EPOCHS` | southbound, controllers | Optional. Raises a node's minimum token epoch, which revokes its older tokens. See Authentication in the main README. |
| `WEAVE_SOUTHBOUND_CORS_ORIGIN` | southbound | Optional. Only a browser node needs it. |

### Minting node tokens

A node's token is derived from `WEAVE_SOUTHBOUND_KEY`. With the key in the
environment, no service is called:

```sh
docker run --rm -e WEAVE_SOUTHBOUND_KEY ghcr.io/kristalalfred/open-weave:latest \
  weave node-token strom-node-1
```

`weave node-token <id> --epoch <n>` mints at another epoch. Put the token in the
node's `WEAVE_SOUTHBOUND_TOKEN`. Adding a node needs no control-plane restart.

### Talking to it

```sh
export WEAVE_NORTHBOUND_TOKEN=...
weave --url https://<control-plane-host> get nodes
```

`--url` (or `WEAVE_NORTHBOUND_URL`) defaults to `http://127.0.0.1:9080`.

### Listening addresses

Every service binds to `127.0.0.1` unless told otherwise. The compose files set:

| Service | Variable | Default | Set to |
|---|---|---|---|
| northbound | `WEAVE_NORTHBOUND_ADDR` | `127.0.0.1:9080` | `0.0.0.0:9080` |
| southbound | `WEAVE_SOUTHBOUND_ADDR` | `127.0.0.1:8081` | `0.0.0.0:8081` |
| controller | `WEAVE_CONTROLLER_ADDR` | `127.0.0.1:8082` | `0.0.0.0:8082` |
| adapter | `node.listen` in `node.yaml` | none | `0.0.0.0:8091` |

Northbound and southbound reach the controllers through `WEAVE_CONTROLLER_URL`, a
comma-separated list of every controller.

### The dashboard has no login

The controller serves `/`, `/ui` and `/view` without authentication, and `/view`
shows topology and allocated ports. The compose file publishes the controller
ports on `127.0.0.1`. Reach them over an SSH tunnel or a private network, and do
not publish them on a public address.

### Timing

Verified in `crates/controller/src/main.rs`. All are controller settings and take
effect on restart.

| Variable | Default | Meaning |
|---|---|---|
| `WEAVE_NODE_TTL_SECS` | 15 | A node reads `offline` after this long without a heartbeat. |
| `WEAVE_NODE_FORGET_SECS` | 300 | An offline node is removed after this long. A node a stored stream names stays. |
| `WEAVE_RECONCILE_INTERVAL_SECS` | 5 | Time between reconcile ticks. |
| `WEAVE_LEASE_TTL_SECS` | 10 | With Postgres, how long the leader's lease lasts without a renewal. A standby takes over at most this long after the leader stops. |

The adapter heartbeats and polls Strom every `strom.poll_interval_secs` from
`node.yaml`.

### Shutdown

The image starts every service under `tini`, so SIGTERM reaches it. The
controller handles SIGTERM and gives up its lease. Northbound and southbound
install no signal handler, and the adapter handles SIGINT only, so on SIGTERM
they exit through the default action. `docker stop` of a test container running
`weave-northbound` returned in under a second with exit code 143.

## Node kit

Run on each media node host.

1. Copy `node/node.yaml` and replace every `<placeholder>`. `node.id` must match
   the id the token was minted for. `southbound_url` is the control plane's 8443.
   The `listeners` addresses are what other nodes dial to reach this node's Strom.
2. Copy `node/env.example` to `node/.env` and set `WEAVE_SOUTHBOUND_TOKEN`.
3. `docker compose up -d` in `node/`.

Strom runs `eyevinntechnology/strom:0.6.10`, the version the bench runs. Both
containers use host networking, so the SRT port range in `node.yaml` is the
host's own. Host networking does not work on Docker Desktop.

If southbound's certificate is signed by a private CA, mount the CA as a PEM file
in the adapter container and set `SSL_CERT_FILE` to it.

### Node ports

| Port | Protocol | Open to |
|---|---|---|
| 20000-20999 | UDP | Other nodes and external peers, the `port_range` in `node.yaml`. Change both together. |
| 8080 | TCP | Only if the node serves WHIP or WHEP. This is also Strom's own API, which has no authentication. |
| 8091 | TCP | The adapter's `/health`. Nothing needs it beyond your own monitoring. |

The node dials out to the control plane on 8443.

## Known limits

- Routes are unversioned before a stable release. Payloads and errors can change
  between versions without a deprecation window.
- Strom is the only runtime with an adapter. Pin Strom 0.6.10; other versions
  are untested against the adapter.
- The services serve no TLS themselves. The nginx here is the only TLS, and
  there is no mTLS.
- Postgres is one instance. While no controller can reach it, none leads.
- OW-46: a browser page that sends video only and restarts waits about 10 s
  before Strom accepts its new WHIP session, and gets `503` until then. Pages
  that send audio and video are not affected. Blocked on Strom.
- Everything else `backlog/` lists as open applies.
