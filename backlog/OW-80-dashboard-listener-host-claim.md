---
id: OW-80
title: "Dashboard shows the first attachment's host for every listener"
type: bug
status: dropped
depends_on: []
assignee: claude-browser
---

## Evidence

Reported, not observed: the controller dashboard shows the first attachment's
host for every listener, and planning is not affected.

## Done when

- [ ] A failing unit test shows the wrong host in the view.

## Log

- 2026-09-29: filed and started by claude-browser.
- 2026-09-29: dropped, the claim did not reproduce. `ui.html` renders a node's
  attachments as id, network and listener kinds and prints no listener host.
  Addresses in `GET /view` come from `endpoint_addr` in `path.rs`, which picks
  the SRT listener on the endpoint's named network, not the first attachment.
  `view_addresses_use_the_listener_of_the_named_network` in
  `crates/controller/src/main.rs` puts two attachments with different hosts on
  each node, names the second network on the source and destination, and
  passes: the view shows the second attachment's host for both. It stays as a
  regression test. Unit tests only. Not checked: the `remote` and RIST address
  paths, and a node with several listeners of different kinds on one attachment.
