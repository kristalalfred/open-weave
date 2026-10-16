# Browser node

A static HTML/JavaScript page with two hop profiles: camera to WHIP, and WHEP to
a video element. Each supports one egress. The page registers a dial-only
attachment, heartbeats every 5 seconds and polls desired hops every 2 seconds.

Serve this directory and open:

```text
http://localhost:8000/#southbound=http://localhost:8081&token=<node-token>
```

Camera capture requires HTTPS or a localhost secure context. Cross-origin
southbound calls require `WEAVE_SOUTHBOUND_CORS_ORIGIN` to allow the page's
origin. See [Security](../../docs/security.md#browser-nodes) for tokens and CORS.

| Fragment option | Meaning |
|---|---|
| `southbound` | Southbound base URL. |
| `token` | Node token, created with `weave node-token ID`. |
| `node` | Explicit node ID; must match the token if one is supplied. |
| `network` | Routing domain; defaults to `internet`. |
| `media=video` | Capture video without audio. Default captures both. |
| `ice` | URL-encoded JSON array of `RTCIceServer` objects. Default has no STUN or TURN servers. |

Fragments are not sent to the static server. The token supplies the default
node ID. With neither a token nor an explicit ID, the page generates an ID and
keeps it in session storage across reloads; southbound must then have auth
disabled. Invalid IDs, token mismatches, and registration responses `400`, `403`
and `409` stop registration retries.

## Browser check

From this directory:

```sh
pnpm install
pnpm exec playwright install --no-shell chromium
node check.mjs --southbound http://127.0.0.1:8081 --token "$(weave node-token browser-1)" --serve 8000
```

The check launches Chromium with fake devices and waits for this page's accepted
registration and its appearance in `/nodes`.

| Option | Meaning |
|---|---|
| `--serve PORT` | Host this directory on loopback. |
| `--page URL` | Open another page URL. |
| `--stay` | Keep the browser running after registration. |
| `--headed` | Show the browser window. |
| `--video-only` | Capture video only; also `WEAVE_BROWSER_MEDIA=video`. |
| `--node ID`, `--ice JSON` | Set the corresponding fragment option. |
| `--executable PATH` | Use another Chromium binary; also `WEAVE_BROWSER_EXECUTABLE`. |

With a private CA, Node.js polling needs `NODE_EXTRA_CA_CERTS` and Chromium
needs the CA in its trust store. The [bench](../../bench/README.md) configures both.

`just browser-check` checks protocol constants and hop-status shape without a
browser, southbound or installed packages. `just test` includes it.
