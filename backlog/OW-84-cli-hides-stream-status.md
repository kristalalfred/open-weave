---
id: OW-84
title: "`weave get streams` shows no status, and the quickstart's direct path ends nowhere"
type: bug
status: done
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

- [x] `weave get streams` shows each stream's status.
- [x] `weave get stream NAME` shows the stream's status, its conditions with
      reason and detail, and each destination's status and endpoint.
- [x] Every path the README quickstart describes ends in a step the reader can
      check, and the opening example's claim is true.
- [x] Checked against a running control plane with a degraded and a flowing
      stream.

## Log

- 2026-10-15: filed and started by claude-rc from the 0.1 release review.
- 2026-10-15: in human output `weave get streams` and `weave get stream-set`
  read `/status` and add a STATUS column; `weave get stream NAME` adds a
  conditions table and a destinations table whose REASON lists the reasons of
  the destination's false conditions. `-o json` and `-o yaml` still print the
  API response unchanged. A refactor first named every state through its serde
  name (`wire_name`), pinned by `human_tables_name_states_as_the_api_does`,
  which passed before and after. Tests: `human_stream_views_show_status_and_why`,
  `a_stream_the_controller_has_not_reported_reads_no_status`,
  `human_stream_views_read_the_status_and_json_does_not`. Boxes 1 and 2 ticked.
- 2026-10-15: the quickstart is three paths. The bench path was run as written
  at f5491cf: `just bench stream-up basic` ended with `basic: flowing` and
  `weave get stream basic` showed `media_flowing` true and the `output`
  destination flowing. The `deploy/` path's
  commands (apply, get endpoints, ffmpeg into the ingress, a receiver on a
  destination, get stream) are what the kit check runs, on one colima host.
  The from-source path, run with `WEAVE_AUTH_DISABLED=1`, left the stream
  `pending` with `node strom-node-1 is not registered`, as the text says. The
  opening example now says media must reach the destinations. Box 3 ticked.
- 2026-10-15: on the `deploy/` kit at f5491cf, `weave get stream` showed the
  stream `pending` with `hops_pending` before the hops ran, `degraded` with
  both destinations `media_degraded` while the producer ran and nothing pulled,
  and `flowing` once two receivers pulled. Not bench. Box 4 ticked.
