---
id: OW-46
title: "A restarted video-only page waits out Strom's inactivity timeout"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

The bench pins Strom 0.6.10. A recorded video-only browser restart test received
`503` until the old WHIP session expired. Recovery on a newer Strom has not
been verified. [Strom #811](https://github.com/Eyevinn/strom/pull/811), which
exposes the session URL across origins, has merged.

## Done when

- [ ] Test a video-only restart with a Strom build containing #811. Verify recovery
  without a `503` window or identify the remaining upstream defect.
