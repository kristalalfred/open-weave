# Security

## Tokens

Requests carry `Authorization: Bearer <token>`. A missing or invalid token gets
`401` with a `WWW-Authenticate: Bearer` challenge. Tokens are compared in
constant time and never logged.

| Variable | Held by | Accepted by |
|---|---|---|
| `WEAVE_NORTHBOUND_TOKEN` | operators, the `weave` CLI (`--token`), northbound | northbound and the controller, on every operator route except the controller's `/status` |
| `WEAVE_SOUTHBOUND_KEY` | southbound, the controller | nothing; node tokens are derived from it |
| `WEAVE_SOUTHBOUND_TOKEN` | each adapter and media node, holding its own node token | southbound, the controller |
| `WEAVE_SOUTHBOUND_MIN_EPOCHS` | southbound, the controller | nothing; the lowest token epoch each node it names may present |

The northbound token is one shared secret. Northbound presents it again to the
controller. The controller requires the credential of the surface a route
belongs to, so a node token cannot create streams.

### Node tokens

Each node has its own token, at an epoch that is a non-negative integer:

```
<node id>.<epoch>.<lowercase hex HMAC-SHA256(WEAVE_SOUTHBOUND_KEY, "<node id>.<epoch>")>
```

Southbound and the controller recompute the MAC and read the node id from the
token. They keep no list of nodes, so adding a node needs a new token and no
restart. The key must be at least 32 characters, for example
`openssl rand -hex 32`, because a leaked token is enough to test guesses of a
short key offline. Both services refuse to start with a shorter key, and
`weave node-token` refuses one.

`weave node-token <id>` prints a node's token at epoch 0, and `--epoch <n>` at
another. It reads the key from `WEAVE_SOUTHBOUND_KEY` only, which keeps it off
the command line, and calls no service.

An adapter takes its token from `WEAVE_SOUTHBOUND_TOKEN`, or from
`node.southbound_token` in its config, which takes precedence.

A node token acts only for its own node. A registration whose `node.id` is
another node's, a heartbeat or desired-hops request whose path id is, and a
registration or heartbeat carrying an endpoint whose `node_id` is, all get
`403`:

```json
{ "code": "forbidden", "message": "token does not belong to node strom-node-2" }
```

Southbound forwards the node's own `Authorization` header to the controller, and
both check it. Nothing about a refused registration is recorded.
`weave-adapter-strom` exits on a `403` at registration.

The inventory reads `GET /nodes`, `GET /endpoints` and `GET /state` accept any
node's token, and the controller's `GET /nodes` also accepts the northbound
token. They list every node, endpoint and hop status, but no node's desired
hops. With a node token, `GET /nodes` leaves out each node's `site`, which names
its subnet and public addresses. The observation a node reports is served on no
route.

### Revoking a node

Mint the node a token at a higher epoch and raise its minimum in
`WEAVE_SOUTHBOUND_MIN_EPOCHS`, given to southbound and the controller alike as
comma-separated `<node id>=<epoch>` pairs, such as `strom-node-1=2,guest-1=1`. A
token below its node's minimum gets `401`. A node the variable does not name
accepts every epoch. Both services read the variable at startup and refuse to
start when it does not parse. Rotating `WEAVE_SOUTHBOUND_KEY` replaces every
node's token.

### Failing closed

Each service refuses to start when a secret it needs is unset or empty:

| Service | Secrets |
|---|---|
| northbound | `WEAVE_NORTHBOUND_TOKEN` |
| southbound | `WEAVE_SOUTHBOUND_KEY` |
| controller | `WEAVE_NORTHBOUND_TOKEN`, `WEAVE_SOUTHBOUND_KEY`, `WEAVE_SRT_KEY_SECRET` |
| adapter | its node token |

`WEAVE_SOUTHBOUND_KEY` and `WEAVE_SRT_KEY_SECRET` must also be at least 32
characters.

`WEAVE_AUTH_DISABLED=1` or `true` turns authentication off for local work. Any
other value, including `0`, leaves it on.

### Unauthenticated routes

- `/health` on every service.
- The controller's dashboard (`/`, `/ui`, `/view`) and its `/status`, which the
  dashboard shares data with. The dashboard is loaded in a browser and polls
  `/view` without a token.

`/view` exposes topology and allocated ports. Keep the controller port on a
private network or behind a reverse proxy. The controller's `/streams` and
`/nodes` require a token, so an exposed port leaks read-only dashboard data and
no write access. Northbound's `/status` and `/streams/{name}/endpoints` require
the northbound token, so operators can read a stream's addresses through
northbound with the controller port closed.

### Browser nodes

A browser node presents its node token on every southbound call. The token is
passed in the URL fragment so it never reaches a server log. Without `#node=`,
the page takes its node id from the token. A `#node=` naming another id is shown
as rejected and never registers.

A page on another origin needs `WEAVE_SOUTHBOUND_CORS_ORIGIN` on southbound: an
exact origin such as `https://studio.example`, or `*` for development. When it
is set, southbound sends CORS headers on its API routes and answers the
preflight before the bearer check, allowing `Authorization` and `Content-Type`.
When it is unset, cross-origin browser requests cannot register.

### Strom

When Strom requires a bearer token (`STROM_API_KEY`), the Strom adapter
presents `WEAVE_STROM_TOKEN`, or `strom.token` from its config, which takes
precedence. With neither set it sends no `Authorization` header.

Strom serves `/whip` and `/whep` without authentication. Anyone who can reach
the node and knows an endpoint id can publish to or play from it, and hop ids
are predictable.

## TLS

The services do not serve TLS. Terminate it at a reverse proxy in front of
northbound and southbound. The bench does this with nginx (see "TLS" in
[`bench/README.md`](../bench/README.md)).

The `weave` CLI, `weave-adapter-strom`, northbound and southbound calling an
`https` controller URL, and the controller calling an `https` webhook URL trust
the Mozilla roots built into them and the platform's certificate store.
`SSL_CERT_FILE` set to a PEM file replaces the platform store with that file,
which is how they trust a private CA. A browser node trusts what its
browser trusts. There is no mTLS.

## SRT encryption

Every SRT link between two nodes is encrypted. The controller derives its key as
HMAC-SHA256 of `WEAVE_SRT_KEY_SECRET` over the id of the hop the link feeds and
the ids of the two nodes it joins, as 64 hex characters, and puts it with
`pbkeylen: 32` (AES-256) in both ends' desired hops.

- The same secret and topology give the same key on every tick. No key is
  stored, and a replan or a link changing direction does not rekey it.
- A link that moves to another node, such as a swapped `via` relay or a
  destination moved to another node, gets a new key. The node it left holds no
  valid key for it.
- Changing the secret rekeys every link once, and each adapter rebuilds its
  keyed flows.

`WEAVE_SRT_KEY_SECRET` must be at least 32 characters. Without it the controller
refuses to start. With `WEAVE_AUTH_DISABLED=1` and no secret, it generates a
random one and logs a warning, and every link key changes when the controller
restarts.

Sockets that peers outside open-weave dial, or that dial out to them, take a
[passphrase from the manifest](streams.md#encryption) instead.

### Where keys appear

- Adapters get keys in `GET /nodes/{id}/desired`, and nothing else serves them.
  A node token reads only its own node's desired hops, so each node learns only
  its own sockets' keys. Whoever holds `WEAVE_SOUTHBOUND_KEY` can mint any node's
  token, and so read every key.
- `/view`, `/status` and `/streams/{name}/endpoints` carry no keys.
  `POST /stream-plans` removes every passphrase from the hops it returns and
  keeps `pbkeylen`, which marks a keyed socket.
- `GET /streams`, `/streams/{name}` and the stream-set routes return the
  manifest as written, so a holder of the northbound token can read manifest
  passphrases. Derived keys never reach northbound.
- The controller stores manifest passphrases in Postgres in the clear, as part
  of the stream spec.
- A RIST link between nodes carries no key (see
  [Cleartext links](streams.md#cleartext-links)).
- Strom returns keys in its own `GET /api/flows`, and its SRT blocks log them at
  `INFO`. Strom started with `STROM_API_KEY` requires that key on the route. The
  node kit in [`deploy/node`](../deploy/node) and the bench both set it.
