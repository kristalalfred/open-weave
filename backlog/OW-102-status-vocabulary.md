---
id: OW-102
title: "A disabled stream reads idle, and /status declares a shape it never serves"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

Stream status `idle` means the stream is disabled (`crates/core/src/lib.rs`),
while `LinkCondition::idle` means a healthy listener that is waiting, and a plan
says `disabled`. On a disabled stream, `media_flowing` has reason `media_idle`
while the other conditions say `disabled`. The rollup values
`idle | converging | converged | degraded` appear nowhere in `docs/`, and an
unplaced stream keeps the rollup at `converging`. `StatusResponse`
(`crates/core/src/api.rs`) is an untagged union of `{status: "starting"}` and
the running shape, but the controller serves its router only after its first
tick, which always sets the report, so the starting shape is never sent. Found
by reading in the 0.1 release review.

## Done when

- [ ] A disabled stream reads `disabled` in status, conditions and plans.
- [ ] `/status` has one shape, and its rollup values are documented.

## Log

- 2026-10-16: filed by claude-rc-fixes from the 0.1 release review.
