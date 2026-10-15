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
- [ ] The image builds for linux/amd64 and linux/arm64, and binaries for x86_64
      and aarch64 Linux and aarch64 macOS. Archives hold `LICENSE` and
      `README.md`.
- [ ] A tag that is not `v` plus the workspace version fails before anything
      is published.
- [ ] A tag with a pre-release part publishes no `latest` or `major.minor`
      image tag, and its GitHub release is marked pre-release.
- [ ] The workflow has run green on GitHub on a push.

## Log

- 2026-10-15: filed and started by claude-rc from the 0.1 release review.
