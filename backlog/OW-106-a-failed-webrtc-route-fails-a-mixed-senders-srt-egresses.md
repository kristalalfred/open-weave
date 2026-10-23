---
id: OW-106
title: "A failed WebRTC route on a mixed spaghetti sender reports its healthy SRT egresses failed"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

The spaghetti adapter keeps one state per hop. A sender hop with an SRT egress
and a WebRTC egress counts as built only when every route in its plan exists
(`crates/adapter-spaghetti/src/hops.rs:493`). When one route fails to build, for
example `CreateRoute` refusing a WebRTC route while the Mac changes networks:

- the hop reads `failed` while its retry is pending (`hops.rs:496`);
- every egress, including the running SRT one, reports `idle`
  (`hops.rs:539`), so the SRT destination's path reads `failed` while it plays;
- the adapter skips the hop's other actions until the retry is due
  (`crates/adapter-spaghetti/src/main.rs:443`).

The coupling predates WebRTC. Mixed SRT and WebRTC fan-out makes a single
failing route common. Found in review; not reproduced on a Mac.

## Done when

- [ ] A sender whose WebRTC route fails keeps reporting its running SRT egress
  with that route's condition.
- [ ] The failing route's retry does not hold back the hop's other routes.
