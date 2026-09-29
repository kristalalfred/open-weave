---
id: OW-81
title: "The Postgres store has no schema version"
type: bug
status: done
depends_on: []
assignee: claude-store
---

## Evidence

`PgStore::ensure_schema` (`crates/controller/src/store.rs`) runs
`CREATE TABLE IF NOT EXISTS` and `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
statements on every connect and records no version. A controller cannot tell
whether the database was written by an older or a newer release, so a newer
schema is not refused and an upgrade between releases has nothing to key a
migration on. Found by reading the store code.

## Done when

- [x] A fresh database is initialised at the current schema version.
- [x] A database from the current code, with no version record, is stamped at
      version 1 and keeps its data.
- [x] A database at a newer or unknown version is refused at startup with an
      error naming both versions, and is not changed.
- [x] Tests cover the three cases above, and the README describes the version.

## Unchecked

- No migration exists, because no release has changed the schema since
  version 1. A database at an older known version has nothing to migrate to
  until the first schema change adds a step.

## Log

- 2026-09-29: filed and started by claude-store.
- 2026-09-29: added the `schema_version` table and check to `ensure_schema`,
  under the existing advisory lock. Three ignored Postgres tests in `store.rs`:
  `a_fresh_database_is_stamped_at_the_current_version`,
  `an_unversioned_database_is_stamped_and_keeps_its_data` (drops the table to
  imitate current code, checks a stream and a node survive) and
  `a_newer_or_unknown_schema_version_is_refused` (versions 2 and 0). Ran the
  controller store tests with `--include-ignored` against a throwaway
  `postgres:16` container: 24 passed. Container removed afterwards. Boxes
  ticked on that run and on `just fmt-check`, `just lint` and `just test`
  passing. Unit tests against a real Postgres only, not `bench/`.
