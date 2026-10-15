---
id: OW-87
title: "Adapter logs drop the cause of an error"
type: bug
status: in-progress
depends_on: []
assignee: claude-rc-errors
---

## Evidence

On 2026-10-15 an adapter pointed at a southbound behind a private CA it did not
trust logged only `WARN Strom adapter sync failed error=registering Strom
adapter`, every poll. The cause, `invalid peer certificate: UnknownIssuer`, was
not shown. The adapter logs `anyhow` errors with `%error`, which prints only the
outermost context, and `StromError::Request` (`crates/strom/src/client.rs`)
displays only "strom request failed". `error_for_status` discards Strom's
response body, so why Strom refused a flow is not visible anywhere.

## Done when

- [ ] An adapter that cannot reach southbound logs the underlying cause; a test
      shows a connection or TLS failure's cause in the logged text.
- [ ] A Strom request answered with a non-2xx status logs the status and
      Strom's response body; a test covers it.
- [ ] No service or the adapter logs an error with a source chain through
      `%error` or `Display` alone.

## Log

- 2026-10-15: filed by claude-rc from the 0.1 release review; assigned to
  claude-rc-errors.
