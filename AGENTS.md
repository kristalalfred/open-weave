# AGENTS.md

Compatibility: breaking-allowed

## Checks

Run `just build`, `just fmt-check`, `just lint` and `just test` before completing
changes. Tests live beside the code in `#[cfg(test)]` modules.

With a test Postgres database, also run:

```sh
DATABASE_URL=postgres://... cargo test --workspace --bin weave-controller -- --ignored --test-threads=1
```

Run `just contracts` after changing routes or wire types. Tests check the
committed OpenAPI and JSON Schema artifacts for drift.

## Boundaries

- The controller owns state and plans routes. Northbound and southbound are
  stateless proxies. Media runs in the nodes, outside the control plane.
- Keep planning side-effect free. Plan against the full candidate stream set.
- Southbound protocol changes require updating `PROTOCOL_VERSION` and every
  adapter, including `nodes/browser/`.
- Formats are declarations. Report mismatches without inferring or converting
  media.

## Repository text

Comments are forbidden. Preserve executable directives such as shebangs and
Dockerfile syntax directives. Put CLI help in explicit argument metadata.

Keep documentation concise and check claims against code. Remove obsolete or
unsupported prose and completed backlog items. Do not treat backlog claims as
proof. Distinguish unit-test coverage from media verified on the bench.

`README.md` holds the quickstart and links. `docs/` holds contracts,
`bench/README.md` the local media test setup, and `deploy/README.md` the Compose
setup. `just board` lists open work.
