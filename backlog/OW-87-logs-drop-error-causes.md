---
id: OW-87
title: "Adapter logs drop the cause of an error"
type: bug
status: done
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

- [x] An adapter that cannot reach southbound logs the underlying cause; a test
      shows a connection or TLS failure's cause in the logged text.
- [x] A Strom request answered with a non-2xx status logs the status and
      Strom's response body; a test covers it.
- [x] No service or the adapter logs an error with a source chain through
      `%error` or `Display` alone.

## Log

- 2026-10-15: filed by claude-rc from the 0.1 release review; assigned to
  claude-rc-errors.
- 2026-10-15: every error log site in core, controller, northbound and the
  adapter (36, found by grepping `%err`, `%error`, `%source` and `err = %`)
  now logs `error = %ErrorChain(..)`, which renders the error and each source
  on one line; southbound and the CLI log no errors. `postgres not ready`
  now logs the error it dropped, and the adapter's `<failed to read body>`
  text renders its chain. `Unanswered` no longer repeats its source in its
  own message. Unit tests: `logging::tests::an_error_chain_names_every_cause_on_one_line`;
  `a_southbound_that_cannot_be_reached_is_logged_with_its_cause` runs the
  adapter's sync loop against a closed port and failed before the change
  with the logged error `registering Strom adapter`; after it the text
  includes `Connection refused`. Ran the adapter binary against
  `openssl s_server` with an untrusted certificate: before, `error=registering
  Strom adapter`; after, `error=registering Strom adapter: error sending
  request for url (https://127.0.0.1:28443/nodes/register): client error
  (Connect): invalid peer certificate: UnknownIssuer`, and with
  `WEAVE_LOG_FORMAT=json` the same text in one JSON object per line. Boxes 1
  and 3 ticked. Unit tests and a local run; not bench.
- 2026-10-15: a Strom request answered other than `2xx` fails with
  `StromError::Status`, which says `strom answered <status> for url (<url>):
  <body>`; the body is quoted through `logging::excerpt`, on one line and cut
  at 1024 bytes. The adapter quotes southbound answers the same way, and a
  refused desired fetch now carries southbound's answer too. Tests
  `client::tests::a_refused_request_names_the_status_and_what_strom_answered`
  and `a_long_answer_is_cut_short` (weave-strom) and
  `a_flow_strom_refuses_is_logged_with_what_strom_answered` (adapter, reads
  the logged `provisioning hop failed` line) failed before the change with
  `HTTP status client error (400 Bad Request) for url (...)` and no body; they
  pass after it. Ran the adapter binary against a stub Strom answering
  `/api/flows` with `500` and a multi-line JSON body: before,
  `error=strom request failed`; after, `error=strom answered 500 Internal
  Server Error for url (http://127.0.0.1:28080/api/flows): { "error": "Failed
  to list flows", "details": "storage locked" }`. Box 2 ticked; all boxes
  ticked. Unit tests and a local run; not bench.
