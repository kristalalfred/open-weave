---
id: OW-86
title: "`weave -o json` prints log lines on stdout"
type: bug
status: done
depends_on: []
assignee: claude-rc
---

## Evidence

On 2026-10-15, `weave -o json apply -f examples/stream.yaml 2>/dev/null` and
`weave -o json delete stream NAME 2>/dev/null` printed an ANSI-coloured
`INFO weave: stream applied` (or `stream deleted`) line before the JSON, so the
output does not parse. The CLI installs the services' subscriber
(`weave_core::logging::init`), which writes to stdout, and `apply_stream` and
`delete_stream` (`crates/cli/src/main.rs`) log what `emit` then prints.

## Done when

- [x] Every `weave` command's `-o json` and `-o yaml` output parses, with
      nothing else on stdout.
- [x] The README's Logging section says where the CLI writes.

## Log

- 2026-10-15: filed and started by claude-rc from the 0.1 release review.
- 2026-10-15: the CLI no longer installs a log subscriber and no longer depends
  on `tracing`, so no command can write a log line; the two `info!` calls that
  repeated `emit`'s output are gone. Errors still leave through `main`'s
  `anyhow::Result` on stderr. Checked with the real `weave` binary against the
  `deploy/` kit on one colima host: at 8e4d161, `weave -o json apply` printed an
  ANSI `INFO weave: stream applied` line before the JSON; at f5491cf the stdout
  of `-o json apply`, `-o json delete stream` and `-o yaml get status` parses
  with nothing else on it. Not bench. Box 1 ticked.
- 2026-10-15: the README's Logging section says the services and the adapter
  log to stdout and the CLI logs nothing, printing its result to stdout and
  errors to stderr. Box 2 ticked.
