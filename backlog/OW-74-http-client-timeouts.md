---
id: OW-74
title: "The adapter's HTTP clients have no timeout"
type: bug
status: done
depends_on: []
assignee: claude-alpha
---

## Evidence

`StromClient::new` (`crates/strom/src/client.rs`) built `reqwest::Client::new()`,
and `Southbound::new` (`crates/adapter-strom/src/main.rs`) did the same. Neither
sets a connect or request timeout, so a Strom or southbound that accepts a
connection and never answers stalls `sync_loop` for good.

The other clients already have timeouts: `Controllers`
(`crates/core/src/upstream.rs`, used by northbound and southbound) connects
within 2 s and fails after 10 s, and the webhook emitter
(`crates/controller/src/webhook.rs`) sets `config.timeout`. The controller makes
no other outbound calls.

## Done when

- [x] `StromClient` connects within 2 s and fails a request after a timeout that
      the adapter config sets, 10 s by default.
- [x] The adapter's southbound client has a connect and a request timeout.
- [x] A test shows a Strom that never answers ends the request.

## Unchecked

- The `weave` CLI builds `reqwest::Client::new()` in each command
  (`crates/cli/src/main.rs`) and has no timeout. An operator can interrupt it,
  so it was left as it is.

## Log

- 2026-09-29: filed and started from an alpha-readiness review. Read the client
  construction in `strom`, `adapter-strom`, `core::upstream`, `controller::webhook`
  and the CLI to fill in Evidence.
- 2026-09-29: `StromClient::with_timeout` sets 2 s connect and the given request
  timeout; `new` uses `REQUEST_TIMEOUT` (10 s). The adapter reads
  `strom.request_timeout_secs` (default 10, `examples/node.yaml`, README). The
  adapter's `Southbound` uses the same 2 s and 10 s. Test
  `client::tests::a_strom_that_never_answers_times_out` (a listener that never
  accepts, 200 ms timeout) passes. All boxes ticked; `just fmt-check`,
  `just lint` and `just test` pass. Unit tests only; not bench.
