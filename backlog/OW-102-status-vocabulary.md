---
id: OW-102
title: "A disabled stream reads idle, and /status declares a shape it never serves"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

Disabled streams report `idle`, while plans report `disabled`; their media
condition uses `media_idle`. `StatusResponse` also declares a starting response,
but the production router is created after the first tick populates its report
(`crates/core/src/api.rs`, `crates/controller/src/main.rs`).

## Done when

- [ ] Use consistent disabled vocabulary in status, conditions and plans.
- [ ] Give `/status` one reachable response shape and document its rollup values.
