---
id: OW-85
title: "A reordered no-op apply bumps the generation on Postgres"
type: bug
status: in-progress
depends_on: []
assignee: claude-rc-store
---

## Evidence

On 2026-10-15, against the `deploy/control-plane` compose (Postgres), applying
`examples/stream.yaml` and then the same stream with its two destinations in the
other order returned `changed: false` both times, and the generation went from
1 to 2. BACKLOG.md's scope guards and the README say a semantic no-op keeps the
generation and the ETag.

Read, not yet run: `PgStore::update_stream` (`crates/controller/src/store.rs`)
decides "unchanged" with JSONB `definition = $2`, which is order-sensitive for
arrays, while the controller and `MemStore` use `StreamDefinition`'s `PartialEq`,
which ignores destination order. The stream-set write may compare the same way.

CI runs no Postgres test: the 15 Postgres tests are `#[ignore]` and
`.github/workflows/ci.yml` has no database.

## Done when

- [ ] A Postgres test applies a stream, then the same stream with its
      destinations reordered, and finds the generation and ETag unchanged. It
      fails before the fix.
- [x] The same holds for a stream-set write, with a test.
- [ ] Postgres and `MemStore` decide "unchanged" by one rule.
- [ ] CI runs the Postgres tests against a Postgres service.

## Log

- 2026-10-15: filed by claude-rc from the 0.1 release review; assigned to
  claude-rc-store.
- 2026-10-15: reproduced in unit tests against a `postgres:16` container, not
  on `bench/`. `a_reordered_reapply_on_postgres_keeps_the_generation_and_etag`
  fails: the reordered reapply answers generation 2 with `changed: false`. Its
  MemStore twin passes. `psql` shows JSONB `=` is false for reordered arrays and
  true for reordered object keys, so `definition = $2` in
  `PgStore::update_stream` is the split.
- 2026-10-15: `a_reordered_stream_set_reapply_on_postgres_keeps_generations_and_etags`
  passes before any fix, because `PgStore::write_stream_set` decodes each stored
  definition and compares with `StreamDefinition`'s `PartialEq`. With that
  comparison swapped for serialized-JSON equality the test fails, so it covers
  this bug.
