---
id: OW-76
title: "Logs have no machine-readable format"
type: feature
status: done
depends_on: []
assignee: claude-alpha
---

## Evidence

Each binary (`weave`, controller, northbound, southbound, adapter-strom) built
its own `tracing_subscriber::fmt()` in `main`, all with the text format. A log
collector could not parse fields without a regex.

## Done when

- [x] One init function in `weave-core` serves every binary.
- [x] `WEAVE_LOG_FORMAT=json` switches to one JSON object per line; the default
      output is unchanged.
- [x] The README documents the variable.

## Log

- 2026-09-29: filed and started from an alpha-readiness review.
- 2026-09-29: `weave_core::logging::init` replaces the five `tracing_subscriber::fmt()` blocks. `logging::tests::json_is_opt_in` covers the parse. Ran `weave-northbound` with and without `WEAVE_LOG_FORMAT=json`: JSON objects with `timestamp`, `level`, `fields`, `target` in one, the old text lines in the other. README section "Logging" added. All three boxes ticked; `just fmt-check`, `just lint` and `just test` pass. Unit tests and a local run; not bench.
