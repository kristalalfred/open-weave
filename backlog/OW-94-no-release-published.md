---
id: OW-94
title: "No release has been published"
type: feature
status: in-progress
depends_on: [OW-89, OW-90, OW-91, OW-92, OW-93]
assignee: claude-rc-fixes
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
- 2026-10-16: started by claude-rc-fixes. OW-89 to OW-93 are done. On `main` at
  `0c2e901`, the full bench corpus passed 12 of 12 locally with the bench
  rebuilt from it, and CI and Release passed on GitHub. An earlier local run
  failed `adapter-restart` at its relay move while this Mac idle-slept: the
  power log shows sleep from 18:24 to 18:39, matching an 899 s gap in both
  controllers' logs, so that run is discounted. The workspace version is now
  `0.1.0-rc.1`.
