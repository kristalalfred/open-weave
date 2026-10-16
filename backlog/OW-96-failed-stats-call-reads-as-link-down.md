---
id: OW-96
title: "One failed stats call reads as the link going down"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

`hop_statuses` (`crates/adapter-strom/src/main.rs`) turns a failed `srt-stats`
call into `None` and logs it at debug. No stats reads as not connected, so the
socket reads idle or connecting. One timed-out call flips a flowing stream to
`awaiting_input` or `degraded` for that poll, and the controller stores the
change and sends `stream.changed`. An application acting on
`media_flowing=false` gets a false alarm. Found by reading in the 0.1 release
review; not reproduced.

## Done when

- [ ] A poll whose stats call fails reports the socket as unknown or as last
      seen, not as down; a test covers it.

## Log

- 2026-10-16: filed by claude-rc-fixes from the 0.1 release review.
