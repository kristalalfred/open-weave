---
id: OW-91
title: "Northbound and southbound have no --help or --version, and no service reports its version"
type: bug
status: in-progress
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

- [ ] `--help` and `--version` work on every binary with an empty environment
      and exit `0`.
- [ ] `/health` on every service and the adapter carries the version.

## Log

- 2026-10-16: filed and started by claude-rc-fixes from the 0.1 release review.
