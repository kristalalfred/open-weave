---
id: OW-90
title: "`weave-controller --help` prints the Postgres password and the webhook token"
type: bug
status: in-progress
depends_on: []
assignee: claude-rc-fixes
---

## Evidence

On 2026-10-16, the CI release binary run with
`DATABASE_URL=postgres://weave:SECRETPW@db/weave` and
`WEAVE_WEBHOOK_TOKEN=SECRETTOKEN` printed
`[env: DATABASE_URL=postgres://weave:SECRETPW@db/weave]` and
`[env: WEAVE_WEBHOOK_TOKEN=SECRETTOKEN]` in `--help`. clap shows an env
var's current value unless the argument sets `hide_env_values`; the CLI sets it
for its token, the controller sets it for neither.

## Done when

- [ ] No binary's `--help` prints the value of an environment variable.
- [ ] A test fails when an argument that reads an environment variable shows
      its value.

## Log

- 2026-10-16: filed and started by claude-rc-fixes from the 0.1 release review.
