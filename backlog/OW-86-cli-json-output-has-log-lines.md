---
id: OW-86
title: "`weave -o json` prints log lines on stdout"
type: bug
status: in-progress
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

- [ ] Every `weave` command's `-o json` and `-o yaml` output parses, with
      nothing else on stdout.
- [ ] The README's Logging section says where the CLI writes.

## Log

- 2026-10-15: filed and started by claude-rc from the 0.1 release review.
