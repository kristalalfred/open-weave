---
id: OW-94
title: "No release has been published"
type: feature
status: in-progress
depends_on: []
assignee: claude-rc-fixes
---

## Evidence

The release audit on 2026-10-16 found no GitHub releases or local tags. The
deployment Compose files default to a release image that has not been published.

## Done when

- [ ] A `v0.1.0-rc.1` tag has published the image and the binaries, and the
      GitHub release is marked pre-release.
- [ ] The image pulls without credentials.
- [ ] The deploy kit has run against the published image.
