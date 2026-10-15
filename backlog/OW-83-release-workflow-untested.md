---
id: OW-83
title: "The release workflow has never run, tags an RC as latest, and builds amd64 only"
type: bug
status: in-progress
depends_on: [OW-77]
assignee: claude-rc
---

## Evidence

`.github/workflows/release.yml` triggers only on `v*` tags. No tag exists, so it
has never run (`gh run list --workflow release.yml` is empty) and
`ghcr.io/kristalalfred/open-weave` has never been published, while both
`deploy/` compose files default to it. The root `Dockerfile` is built by no CI
job. The workflow:

- adds `type=raw,value=latest` unconditionally, so a `v0.1.0-rc.1` tag would
  move `latest`;
- builds the image for the runner's platform only, linux/amd64, and the
  binaries for x86_64 Linux only, so arm64 hosts and macOS operators get
  nothing;
- does not check the tag against the workspace version, so an RC tag ships
  binaries that print `0.1.0`;
- does not mark an RC release as a pre-release;
- packs the binaries without `LICENSE` or `README.md`.

## Done when

- [ ] The image and binary builds run on every push to `main` and every pull
      request, without publishing.
- [x] The image builds for linux/amd64 and linux/arm64, and binaries for x86_64
      and aarch64 Linux and aarch64 macOS. Archives hold `LICENSE` and
      `README.md`.
- [x] A tag that is not `v` plus the workspace version fails before anything
      is published.
- [x] A tag with a pre-release part publishes no `latest` or `major.minor`
      image tag, and its GitHub release is marked pre-release.
- [ ] The workflow has run green on GitHub on a push.

## Log

- 2026-10-15: filed and started by claude-rc from the 0.1 release review.
- 2026-10-15: `release.yml` now runs on pushes to `main`, `v*` tags and pull
  requests. A `version` job fails a tag that is not `v` plus the workspace
  version and marks a version with a `-` as a pre-release. The image job builds
  linux/amd64 and linux/arm64 with QEMU and pushes only on a tag; the binaries
  job builds x86_64 and aarch64 Linux on `ubuntu-22.04` and `ubuntu-22.04-arm`
  and aarch64 macOS on `macos-latest`, and packs `LICENSE` and `README.md`; the
  release job runs only on a tag. The root `Dockerfile` cross-compiles in a
  builder on the build platform. `actionlint` 1.7.7 passes.
- 2026-10-15: on this arm64 Mac, `docker buildx build --platform linux/arm64`
  and `--platform linux/amd64 --load` both built; the binaries' ELF machine is
  `0xb7` in the arm64 image and `0x3e` in the amd64 one, and both images run
  as uid 10001 and print `0.1.0`. `cargo build --release --locked --bins
  --target aarch64-apple-darwin` built, and the workflow's Package step, run
  locally, made an archive with the five binaries, `LICENSE` and `README.md`.
  The Linux binary builds were not run outside the image builds. Box 2 ticked.
- 2026-10-15: the `version` step, run locally with simulated refs: a push to
  `main` and tag `v0.1.0` pass; `v0.1.0-rc.1` and `v0.2.0` fail with
  `::error::tag ... is not v0.1.0` while the workspace is 0.1.0. With the
  workspace at `0.1.0-rc.1`, tag `v0.1.0-rc.1` passes with `prerelease=true`,
  which marks the GitHub release a pre-release. Box 3 ticked.
- 2026-10-15: the unconditional `type=raw,value=latest` is gone. In
  `docker/metadata-action` v5 (`src/meta.ts`), `latest=auto` on a semver tag is
  false for a pre-release, and its README says a pre-release gets only the
  `{{version}}` tag, not `{{major}}.{{minor}}`. Read, not run. Box 4 ticked.
- 2026-10-15: found while building: OW-87's `logging::excerpt` used
  `str::floor_char_boundary`, stable only since Rust 1.91, and the image builds
  with Rust 1.88. The workspace now declares `rust-version = "1.88"`; clippy's
  `incompatible_msrv` then failed on that call, and it was replaced. Boxes 1
  and 5 wait for a push: nothing has run on GitHub.
