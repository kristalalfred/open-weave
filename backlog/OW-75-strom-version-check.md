---
id: OW-75
title: "The adapter does not check the Strom version it drives"
type: feature
status: done
depends_on: []
assignee: claude-alpha
---

## Evidence

The adapter reads a WHIP flow without a session as idle because Strom 0.6.9 and
later report it as `Playing` (`crates/adapter-strom/src/main.rs`, test
`a_playing_whip_flow_without_a_session_is_idle`), and OW-46 names Strom's
session displacement as 0.6.9 and later. The bench pins 0.6.10
(`bench/docker-compose.yml`). Nothing read the version, so an older Strom would
be driven with wrong readings and no message.

Strom serves `GET /api/version`: `backend/src/api/version.rs` in a local Strom
checkout (its `Cargo.toml` says 0.6.8, so it is not the bench release), which
returns `SystemInfo` with a string field `version`. `bench/justfile` also polls
that path on the 0.6.10 containers as its readiness check.

## Done when

- [x] The adapter reads Strom's version and exits, with the found and the
      minimum version in the message, when it is below 0.6.9.
- [x] A Strom that cannot be asked, or a version string that cannot be read,
      does not stop the adapter.

## Easy to break

- The check refuses instead of warning, to match the rule that protocol
  mismatches are refused. A version that cannot be parsed is treated as unknown,
  not as too old, and is let through with a warning.

## Unchecked

- The `version` field was read from Strom's source, not from a running Strom.
  Whether the 0.6.10 image answers `/api/version` with that field, and whether
  it needs the bearer token, was not run. If the request fails the adapter keeps
  working and logs a warning each poll until it succeeds.
- 0.6.9 as the minimum comes from the two references above, not from a test
  against 0.6.8.

## Log

- 2026-09-29: filed and started from an alpha-readiness review.
- 2026-09-29: `StromClient::version`, `weave_strom::is_supported` and
  `MIN_VERSION` (0.6.9), and `check_strom_version` in the sync loop, which runs
  until it gets an answer and returns `UnsupportedStrom` to exit. Tests:
  `version::tests` (ordering, prefix and suffix, unreadable),
  `client::tests::reads_the_version_field_of_api_version`, and three
  `check_strom_version` tests against a stub Strom (0.6.8 refused; 0.6.9,
  0.6.10 and an unreadable string pass; an unreachable Strom is not a refusal).
  Ran the adapter locally against a stub answering `{"version":"0.6.8"}`: it
  logged the error and exited 1. README updated. Both boxes ticked;
  `just fmt-check`, `just lint` and `just test` pass. Unit tests and a local
  stub; not bench.
