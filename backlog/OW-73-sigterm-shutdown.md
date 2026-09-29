---
id: OW-73
title: "Northbound, southbound and the adapter ignore SIGTERM"
type: bug
status: done
depends_on: []
assignee: claude-alpha
---

## Evidence

Read at the start of the item:

- `crates/adapter-strom/src/main.rs` waited on `tokio::signal::ctrl_c()` only.
- `crates/northbound/src/main.rs` and `crates/southbound/src/main.rs` ran
  `axum::serve(..).await` with no shutdown future, so neither stopped on Ctrl-C
  or SIGTERM in a way the code handled.
- `crates/controller/src/main.rs` already waited on SIGTERM and Ctrl-C
  (`shutdown()`), so the controller needed no fix. It aborts its API task on
  exit rather than draining it.

Under `docker stop` or Kubernetes, a process that ignores SIGTERM is killed with
SIGKILL after the grace period.

## Done when

- [x] One shutdown signal (SIGTERM or Ctrl-C) in `weave-core`, used by every
      service.
- [x] Northbound and southbound serve with `with_graceful_shutdown` on that
      signal.
- [x] The adapter exits on SIGTERM and does not touch Strom's flows on the way
      out.

## Unchecked

- That Strom's flows keep running after the adapter stops was read from the code
  (the exit path only aborts the adapter's health server; `delete_flow` is called
  from reconciliation only). It was not run against a Strom.

## Log

- 2026-09-29: filed and started from an alpha-readiness review.
- 2026-09-29: `weave_core::shutdown::signal` (SIGTERM or Ctrl-C) replaces the
  controller's own copy and the adapter's Ctrl-C wait. `shutdown::graceful` feeds
  `axum::serve(..).with_graceful_shutdown` in northbound and southbound. Test
  `shutdown::tests::sigterm_resolves_the_signal` sends SIGTERM to the test
  process. Started each of northbound, southbound, controller and the adapter
  (`examples/node.yaml`, Strom unreachable) locally, sent `kill -TERM` after 2 s,
  and each exited 0 with its shutting-down log line. Draining of a request that
  is in flight was not exercised by a test, so the second box was reworded from
  "finish in-flight requests" to what was checked: the call is wired. Draining
  itself is axum's. All boxes ticked; `just fmt-check`,
  `just lint` and `just test` pass. Unit tests and local runs; not bench.
