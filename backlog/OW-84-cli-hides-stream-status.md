---
id: OW-84
title: "`weave get streams` shows no status, and the quickstart's direct path ends nowhere"
type: bug
status: in-progress
depends_on: []
assignee: claude-rc
---

## Evidence

README.md's opening example says `weave get streams` reports a stream
`flowing`. `render_streams` (`crates/cli/src/main.rs`) prints NAME, GENERATION,
OWNER and ENABLED, and `weave get stream NAME` prints the same row. A stream's
status, its conditions and each destination's status are only in
`weave get status`, and the human form of that shows no reason or detail. In
the 2026-10-15 review a degraded stream could only be explained from
`weave -o json get status`.

The README quickstart's "run the services directly" path registers one node
from `examples/node.yaml`, which points at bench addresses, while every example
stream names two nodes, and nothing says how to send media in. It says the
stream "stays `Pending`" where the CLI prints `pending`.

## Done when

- [ ] `weave get streams` shows each stream's status.
- [ ] `weave get stream NAME` shows the stream's status, its conditions with
      reason and detail, and each destination's status and endpoint.
- [ ] Every path the README quickstart describes ends in a step the reader can
      check, and the opening example's claim is true.
- [ ] Checked against a running control plane with a degraded and a flowing
      stream.

## Log

- 2026-10-15: filed and started by claude-rc from the 0.1 release review.
