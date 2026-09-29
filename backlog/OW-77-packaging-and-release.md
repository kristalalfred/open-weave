---
id: OW-77
title: "No release build, production image or release workflow"
type: feature
status: done
depends_on: []
assignee: claude-deploy
---

## Evidence

The workspace had no `[profile.release]`, and the only Dockerfile, `bench/Dockerfile`, builds debug binaries for the bench. `.github/workflows/ci.yml` runs fmt, clippy and tests and publishes nothing.

## Done when

- [x] `Cargo.toml` has a `[profile.release]`.
- [x] A root `Dockerfile` builds the release binaries of northbound, southbound, controller, adapter-strom and the `weave` CLI into a non-root image that starts them under `tini`.
- [x] A workflow builds and pushes the image to ghcr.io and attaches binaries to a GitHub release on a `v*` tag.

## Log

- 2026-09-26: filed and started by claude-deploy.
- 2026-09-26: `[profile.release]` sets `lto = "thin"`, `codegen-units = 4` and `strip = "symbols"`. Box 1 ticked.
- 2026-09-26: `docker build -t open-weave:local .` succeeded in 73 s. The container runs as uid 10001, `weave --version` prints `0.1.0`, and `docker stop` of a running `weave-northbound` returned in under a second with exit 143, so SIGTERM reaches the process. Image size 48 MB. Box 2 ticked. Rests on a local docker build only, not the bench.
- 2026-09-26: `.github/workflows/release.yml` written. It has not run, since it triggers on a tag and no tag was created. Box 3 is ticked on reading the file alone, so the first `v*` tag is its first test.
