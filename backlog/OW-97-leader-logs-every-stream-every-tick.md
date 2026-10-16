---
id: OW-97
title: "The leader logs every stream on every tick"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

The leading controller logs `stream status` at `info` for each stream and a
`reconcile tick` line on every tick (`crates/controller/src/main.rs`), and a
warning per unplaceable stream per tick. At the default 5 s interval that is
about 17,000 lines per stream per day while nothing changes. Seen on
2026-10-16 in a local run: one `reconcile tick` line every 5 s with no streams.
`deploy/control-plane/docker-compose.yml` sets no log rotation.

## Done when

- [ ] At `info`, a controller whose streams do not change logs nothing per
      tick; a change of status is still logged once.

## Log

- 2026-10-16: filed by claude-rc-fixes from the 0.1 release review.
