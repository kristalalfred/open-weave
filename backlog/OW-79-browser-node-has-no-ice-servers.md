---
id: OW-79
title: "The browser node cannot be given STUN or TURN servers"
type: feature
status: done
depends_on: []
assignee: claude-browser
---

## Evidence

`nodes/browser/node.js` created `new RTCPeerConnection()` with no
configuration, so the page gathered host candidates only and had no way to
cross NAT to a WHIP or WHEP peer on another network. The page is configured
through the URL fragment (`southbound`, `token`, `media`, `node`, `network`);
none of them named an ICE server.

## Done when

- [x] The page reads ICE servers from the fragment and passes them to every
      `RTCPeerConnection`; without the parameter the configuration is empty.
- [x] `check.mjs --check-only` covers parsing and the hand-off.
- [x] `nodes/browser/README.md` documents the parameter.

## Unchecked

- Nothing here connected through a real STUN or TURN server. The change is
  verified as far as the configuration reaching the constructor.

## Log

- 2026-09-29: filed and started by claude-browser. The claim held: read
  `node.js`, the constructor took no argument.
- 2026-09-29: added `&ice=<JSON array of RTCIceServer>`, parsed by
  `parseIceServers`; an unparseable value sets `fatal`, like an invalid node id.
  `check.mjs --check-only` now runs `parseIceServers` on empty, valid, invalid
  and non-array input and checks the constructor call. All boxes ticked on that
  run and on `just fmt-check`, `just lint` and `just test` passing. Static
  check only; no browser or bench run.
