---
id: OW-93
title: "Docs and examples that send a new operator to a dead end"
type: bug
status: done
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

- [x] Each statement above matches the code.
- [x] A test parses every file in `examples/` and `deploy/node/node.yaml` the
      way the CLI and the adapter do.

## Log

- 2026-10-16: filed and started by claude-rc-fixes from the 0.1 release review.
- 2026-10-16: fixed against the code. `deploy/README.md` says to build the image
  and set `WEAVE_IMAGE` in every `.env` until a tag exists, mints node tokens
  with `docker compose exec southbound weave node-token` (read, not run), names
  three ways to get `weave` (`cargo install --locked --path crates/cli` was run
  and gave a working CLI), says when the CLI needs `SSL_CERT_FILE`, links
  [Revoking a node](../docs/security.md#revoking-a-node), and lists `/status` as
  unauthenticated, which a running controller confirmed. `docs/nodes.md` says
  the Strom adapter's capabilities are built in. `examples/node.yaml` targets
  `just run-south` on 127.0.0.1; an adapter started with it registered.
  `bench/README.md` matches its recipes, published ports, auth defaults, relay
  choice and hop model, and the stale comments in `bench/justfile` and on
  `NodeConfig.public_endpoint` are fixed. `nodes/browser/README.md` installs
  Playwright's Chromium with `pnpm exec playwright install --no-shell
  chromium`; `check.mjs` then registered a page against a southbound built from
  source. `docs/api.md`'s prefixed-route answer was run: `401` without a token,
  `404` with one. Box 1 ticked.
- 2026-10-16: `example_manifests_parse_and_validate` (weave-cli) reads
  `examples/stream.yaml`, `contribution.yaml` and `stream-set.yaml` through the
  CLI's own readers and validates each stream; `example_node_config_loads` and
  `deploy_node_template_loads_once_filled_in` (weave-adapter-strom) load
  `examples/node.yaml` and the filled-in `deploy/node/node.yaml` through
  `AdapterConfig::load`. Each failed on a broken copy: an unknown key, an
  invalid destination id, a set member without destinations, a `capabilities`
  block and a leftover placeholder. Box 2 ticked.
