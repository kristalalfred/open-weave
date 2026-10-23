---
id: OW-104
title: "The clock preemption test failed once in a full test run"
type: bug
status: todo
depends_on: []
assignee:
---

## Evidence

On 2026-10-22 one `cargo test --workspace` run failed
`weave-media clock::tests::a_sample_widened_by_preemption_does_not_make_the_next_one_a_step`
at `crates/media/src/clock.rs:214` (`assert!(clock.sample().is_ok())`). The same
test passed on two reruns, and the code under test had not changed. Other
worktrees were building on the machine at the time. On 2026-10-23 it failed at
the same line in two of about four full runs in another worktree, again with
other builds running, and passed on rerun.

The test takes a wall-clock sample, plants a previous sample 3 ms ahead with
3.05 ms uncertainty and the first sample's `monotonic_ns`, and samples again.
`Clock::sample` allows the two uncertainties summed, about 3.1 ms plus half the
second sample's read time, so the assertion holds by about 100 µs. A slow first
sample shifts its monotonic midpoint without widening the planted uncertainty.
That cause is not reproduced.

## Done when

- [ ] Reproduce the failure, for example by stalling between the first sample's
  clock reads.
- [ ] The test passes regardless of how long either sample takes to read.
