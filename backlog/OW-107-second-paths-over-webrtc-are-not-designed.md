---
id: OW-107
title: "Second paths (merge) over WebRTC links are not designed"
type: feature
status: todo
depends_on: []
assignee:
---

## Evidence

`place_second_path` plans the link into a merge receiver with the same
`relay_before` and `plan_link` as a first path
(`crates/controller/src/path.rs:1346`, `:1409`). Between two nodes that offer
WebRTC it can therefore plan a WebRTC `merge_ingress`, which
`DesiredHop::peer_ends` treats as an answering end. The route record that
`relay_before` returns for it is dropped, so the placement detail never names
it. Nothing tests the case.

It is unreachable today: the only merging profile is the native timed receiver,
which takes no WebRTC, and spaghetti declares no merge profile, so a spaghetti
receiver gets `NoMergeProfile` and a single path.

## Done when

- [ ] A second path over a WebRTC link is either planned, signalled and named in
  the placement detail, or refused with a reason.
- [ ] A test covers a two-path destination between nodes that offer WebRTC.
