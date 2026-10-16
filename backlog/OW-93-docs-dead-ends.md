---
id: OW-93
title: "Docs and examples that send a new operator to a dead end"
type: bug
status: in-progress
depends_on: []
assignee: claude-rc-fixes
---

## Evidence

Found in the 0.1 release review by reading the docs against the code:

- `deploy/README.md` mints node tokens with
  `ghcr.io/kristalalfred/open-weave:latest`, which does not exist before a
  tag, never says to set `WEAVE_IMAGE` for a self-built image, and never says
  how to get the `weave` CLI or that it needs `SSL_CERT_FILE` with a private
  CA. It links to an Authentication section the README no longer has, and
  leaves `/status` out of the unauthenticated routes.
- `docs/nodes.md` says the Strom adapter's capabilities come from the node
  config. They are built in, and a `capabilities` key in `node.yaml` stops the
  adapter from starting.
- `examples/node.yaml`, offered for running from source, carries bench
  container addresses, so the adapter never reaches `just run-south`.
- `bench/README.md` says `WEAVE_AUTH_DISABLED=1` turns authentication off on the
  bench, that `consumer-up` takes an output index, and gives a passphrase that
  does not match `bench/manifests/encrypted.yaml`, among other stale lines.
- `nodes/browser/README.md` has no step that installs Playwright's browser.
- Nothing parses the example configs, so they drift from the types unseen.

## Done when

- [ ] Each statement above matches the code.
- [ ] A test parses every file in `examples/` and `deploy/node/node.yaml` the
      way the CLI and the adapter do.

## Log

- 2026-10-16: filed and started by claude-rc-fixes from the 0.1 release review.
