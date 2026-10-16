---
id: OW-90
title: "`weave-controller --help` prints the Postgres password and the webhook token"
type: bug
status: done
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

- [x] No binary's `--help` prints the value of an environment variable.
- [x] A test fails when an argument that reads an environment variable shows
      its value.

## Log

- 2026-10-16: filed and started by claude-rc-fixes from the 0.1 release review.
- 2026-10-16: every clap argument that reads an environment variable in
  weave-controller, weave-adapter-strom, weave, weave-bench, weave-northbound
  and weave-southbound sets `hide_env_values`. Each binary has
  `help_hides_every_env_value`, which walks its `Args::command()` and its
  subcommands and lists every argument with an env var whose value help would
  show. Before the attributes it failed with all nine controller arguments,
  `config` for the adapter and `url` for weave and weave-bench. With
  `DATABASE_URL=postgres://u:SECRETPW@h/d` and `WEAVE_WEBHOOK_TOKEN=SECRETTOKEN`
  set, `weave-controller --help` built from `main` shows `[env: DATABASE_URL]`
  and no `SECRET` string. Unit tests and a local run. Both boxes ticked.
