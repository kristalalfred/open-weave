---
id: OW-91
title: "Northbound and southbound have no --help or --version, and no service reports its version"
type: bug
status: done
depends_on: []
assignee: claude-rc-fixes
---

## Evidence

`weave-northbound` and `weave-southbound` read only environment variables
(`main` in `crates/northbound/src/main.rs` and `crates/southbound/src/main.rs`).
On 2026-10-16 the release binaries answered `--help` with
`Error: WEAVE_NORTHBOUND_TOKEN is unset or empty` and
`Error: WEAVE_SOUTHBOUND_KEY is unset or empty`; with a token set,
`weave-northbound --version` starts the server. The controller and the adapter
parse arguments with clap and print help and version. Every `/health` answers
`{"status":"ok"}`, and no route or header carries the version, so an
integrator cannot tell which release a server runs.

## Done when

- [x] `--help` and `--version` work on every binary with an empty environment
      and exit `0`.
- [x] `/health` on every service and the adapter carries the version.

## Log

- 2026-10-16: filed and started by claude-rc-fixes from the 0.1 release review.
- 2026-10-16: weave-northbound and weave-southbound parse flags with clap:
  `--listen` and `--controller-url` read the env vars they read before, with
  the same defaults, and southbound's `--cors-origin` reads
  `WEAVE_SOUTHBOUND_CORS_ORIGIN`. Arguments are parsed before any secret is
  read, and `--help` names the secrets each service needs. `weave-bench` gained
  `version`. With `env -i PATH=$PATH`, `--version` on weave-northbound,
  weave-southbound, weave-controller, weave-adapter-strom, weave and weave-bench
  built from `main` prints `<name> 0.1.0` and exits `0`. Without flags or
  secrets, northbound, southbound and the controller still exit `1` with the
  missing-secret error. Box 1 ticked.
- 2026-10-16: `weave_core::Health::OK` is the one `/health` body:
  `{"status":"ok","version":"0.1.0"}`, from weave-core's `CARGO_PKG_VERSION`,
  which every crate shares through `version.workspace`. Northbound, southbound,
  the controller's leader and standby routers, and the adapter answer it. The
  health tests failed before with `left: {"status":"ok"}` and pass. Four
  services started locally with auth off each answered the body to `curl`.
  Box 2 ticked.
