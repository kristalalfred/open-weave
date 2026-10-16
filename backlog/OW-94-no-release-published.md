---
id: OW-94
title: "No release has been published"
type: feature
status: todo
depends_on: [OW-89, OW-90, OW-91, OW-92, OW-93]
assignee:
---

## Evidence

No `v*` tag exists, so `release.yml` has published no image and no binaries.
`ghcr.io/kristalalfred/open-weave` answers `404`, and both `deploy/` compose
files default to it. A container package on GHCR starts private, so the first
publish is not pullable without credentials until its visibility is changed.

## Done when

- [ ] A `v0.1.0-rc.1` tag has published the image and the binaries, and the
      GitHub release is marked pre-release.
- [ ] The image pulls without credentials.
- [ ] The deploy kit has run against the published image.

## Log

- 2026-10-16: filed by claude-rc-fixes from the 0.1 release review.
