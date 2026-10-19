# Timed delivery and redundant paths

Status: proposed. This specification is not implemented. The current API rejects
the `delivery` manifest field. Current behavior is documented in
[Streams](streams.md#redundant-paths), [Nodes](nodes.md#hop-profiles) and
[the Strom adapter](strom.md).

## Contract

A destination can request a fixed delay between the managed sender accepting
media and the managed receiver releasing it. The receiver holds early media,
releases one copy on schedule and discards copies that miss their deadline.
Two paths carry copies of the same encoded feed. They protect delivery when at
least one copy of each record arrives in time.

For a requested delay `B` and tolerance `T`, a successful release has an actual
delay in `[B - T, B + T]`. The receiver targets `B`; tolerance accounts for
clock uncertainty and scheduling error. This is a conditional timing contract,
not an unconditional guarantee of delivery over an arbitrary network or a
hard real-time guarantee from a general-purpose operating system. Violations
must be observable. The runtime must not report compliance when it cannot
establish the timing bounds.

The boundary starts immediately after the sender reads a payload from its
external ingress, before internal queueing, framing or duplication. It ends
when the receiver successfully submits the unwrapped payload to its final
output transport's bounded send queue. Capture, encoding, source-side transport
buffering before the read, the final output transport, decoding and display
are outside the contract. A 500 ms request does not promise 500 ms from camera
to screen.

## Manifest

```yaml
name: venue-to-studio
source:
  srt:
    node: venue
destinations:
  - id: studio
    paths: 2
    delivery:
      latency_ms: 500
      tolerance_ms: 5
    srt:
      node: studio
      latency: 100
```

| Field | Meaning |
|---|---|
| `delivery` | Optional destination object. Absence retains current delivery behavior. |
| `delivery.latency_ms` | Required positive `u32`, in milliseconds. Fixed target delay `B` across the managed boundary. |
| `delivery.tolerance_ms` | Positive `u32`, in milliseconds; defaults to 5. Must be less than `latency_ms`. Allowed error on either side of `B`. |
| `paths` | Existing desired path count, 1 or 2; defaults to 1. Timed release works with either count. |
| `srt.latency` | Existing per-link SRT buffering setting, in milliseconds. It is separate from the fixed delivery delay. |

`delivery` accepts no other fields. Omitted tolerance and explicit `5` are
equal for no-op writes, generations and ETags. Stream sets apply the same
validation and equality rules. A fixed delay never grows automatically.

The first implementation supports a managed SRT source and managed SRT
destinations. Reject `delivery` on a remote, WHEP or device destination, or on
a stream with a WHIP or device source, with `400 invalid_request` and a field
detail. RIST and WebRTC cannot carry timed internal links in this version.
Other destinations in the same SRT stream can remain untimed. Declared formats
and `format_policy` retain their existing meaning.

Each destination schedules independently. A studio can request 500 ms and a
monitor 1,500 ms from the same origin timestamps. Equal budgets align release
at managed receivers within their tolerances; they do not synchronize external
decoders or displays.

## Source identity and internal media protocol

SRT sequence numbers belong to transport connections. Native SRT broadcast
bonding synchronizes them within a socket group; it does not establish a shared
sequence space across independently terminated relay connections. See
[SRT socket groups](https://github.com/Haivision/srt/blob/master/docs/features/socket-groups.md).
Timed routes therefore carry a versioned internal envelope, `weave_timed_v1`,
inside the existing encrypted SRT links.

The sender assigns identity and time once, before duplicating a record. Relays
forward complete envelopes unchanged. They must not restamp, renumber, remux,
decode or re-encode them. The final receiver removes the envelope. External
producers and consumers continue to exchange their original media payload.
Application payload byte order is preserved; SRT message boundaries are not a
media contract.

Format declarations describe the unwrapped media. Envelope insertion and
removal do not infer or change its declared container or tracks. A profile may
advertise `media_behavior: preserve` only when it preserves those payload bytes;
strict admission still requires its explicit input constraints.

Every envelope has this 64-byte header, followed by 1 to 1,024 payload bytes.
Integers use unsigned network byte order. Each envelope occupies one SRT
message. Both ends configure a receive capacity of at least 1,088 bytes and
reject truncation. Larger ingress payloads are split; every part inherits the
timestamp of the ingress read that supplied it.

| Header field | Bytes | Value |
|---|---:|---|
| `magic` | 4 | ASCII `WVTD`. |
| `version` | 2 | `1`. |
| `flags` | 2 | `0`; other values are invalid. |
| `delivery_id` | 16 | UUID identifying this source stream incarnation. |
| `epoch` | 8 | Authorized origin runtime epoch. |
| `sequence` | 8 | Starts at 0 and increases by 1 for every record in an epoch. |
| `origin_tai_ns` | 8 | Sender ingress time, nanoseconds since 1970-01-01 00:00:00 TAI. |
| `origin_uncertainty_ns` | 4 | Bound on the source timestamp error at the read. |
| `payload_length` | 4 | Length of the following payload. |
| `reserved` | 8 | `0`; other values are invalid. |

All timed branches of a source use the same identity, sequence, timestamp and
payload for a record. Untimed branches receive the original payload without
the envelope. Sequence numbers must not wrap. Timestamp values must be
nondecreasing within an epoch; equal values are allowed for a burst.

The controller allocates and persists `delivery_id` when timed delivery is
first enabled for a source, and replaces it when the source changes or the
stream is deleted and recreated. It allocates a monotonically increasing
`epoch` when the origin runtime loses its sequence state. Allocation happens
in desired-state transactions, never in the side-effect-free planner. The
origin reports a fresh runtime incarnation identifier when it needs an epoch;
the controller handles repeated reports of that identifier idempotently.
An origin cannot restart with a reused epoch or generate a new authorized
epoch while the controller is unavailable.

Desired state authorizes the identity and epoch at every timed hop. Receivers
never adopt a different epoch from an incoming record alone. Updating that
authorization flushes the previous epoch and primes a new schedule. Delayed
records from an older epoch are discarded. Restarting the source is a
discontinuity; redundant network paths do not protect source state loss.
Changing routes, adding a path, changing a destination's budget and controller
takeover do not allocate a new origin identity or epoch.

Validate headers and lengths before allocation. Reject unauthorized identities,
versions and epochs. Detect conflicting copies with the same identity and
sequence but different content while they remain in the merge window; stop
that destination with `record_conflict`. Ignore records older than the release
watermark without allocating a buffer slot. These checks do not make an
untrusted media node trustworthy; nodes remain in the existing trust boundary.

## Clocks and scheduling

Sender and receiver need a common time reference with known error bounds. The
first version uses TAI timestamps. Merely reading a wall clock or observing an
NTP process is not evidence of a bound. An integration must supply a current
clock uncertainty bound, its expiry and a monotonic-to-TAI mapping. It may
obtain these from an externally managed synchronization service. Weave does
not run a clock synchronization protocol.

Let `Es` be the sender's timestamp uncertainty, `Er` the receiver's current
clock uncertainty, and `J` the receiver's declared release scheduling bound.
Timed operation requires `Es + Er + J <= T`. The bounds must include timestamp
sampling, mapping error and drift until the release deadline. A missing,
expired or unbounded clock estimate cannot satisfy the contract. Source
uncertainty that cannot fit the header also makes timing unavailable.

For each record, the target is `origin_tai_ns + B`. Project that target onto
the receiver's monotonic clock and use monotonic deadlines. Monitor changes to
the mapping; a clock step or loss of the required bound suspends timed output
and flushes pending records. Never shift queued media silently to a new clock
mapping. Resume with fresh, still-timely records after valid bounds return.
The epoch need not change if the origin retains valid sequence and timestamp
state; otherwise obtain a new epoch.

The runtime must attempt release at the target, without deliberately using
tolerance as extra jitter buffer capacity. Before release, the possible actual
age interval, including clock uncertainty, must fit inside `[B - T, B + T]`.
Record the completion time of successful output submission and detect any
scheduling violation. Ordinary scheduler pauses can still violate the bound;
they must increment a violation counter and make timing noncompliant.

TAI avoids UTC leap-second discontinuities, but clock steps and invalid TAI
offset configuration still need detection. Monotonic scheduling alone does
not establish clock agreement between hosts. See
[Linux timekeeping](https://www.kernel.org/doc/html/latest/core-api/timekeeping.html)
and [NTP synchronization distance](https://www.rfc-editor.org/rfc/rfc5905.html#section-11.2).

## Merge and deadline behavior

Maintain one bounded ordered buffer per destination and active epoch, keyed by
sequence. Store the first valid copy and discard later matching copies. Each
path has an independent input queue; loss recovery or backpressure on one must
not block reception from another. Sending on a slow path must likewise not
block the origin's other branches. Origin and relay output queues are bounded
per branch; exhaustion drops records on that branch and reports the drop.

Release each record once, in sequence order, at its target. A missing earlier
record must not hold a later record past its deadline. Finalize known sequence
gaps when advancing the release watermark. Discard records that arrive too
late or cannot be submitted within their allowed interval. Never replay,
stretch the schedule, emit a backlog burst or synthesize replacement media.
Loss may leave an undecodable gap in the original media.

If neither path provides a record in time, delivery for that record has failed.
Continue the original schedule with later records. Count only known sequence
gaps; silence alone cannot establish how many records were lost. A paused
source creates no media and no synthetic deadline failures.

Startup does not wait for both paths. The first valid record establishes a
sequence watermark and is scheduled from its original timestamp. A late first
record is discarded until timely media arrives. Starting in the middle of an
encoded feed does not promise a decodable picture before the next independently
decodable media boundary.

An absent output consumer is `output_unavailable`. Discard expired records;
never accumulate a replay buffer. A full output queue is `output_backpressure`.
A receiver whose bounded merge buffer is exhausted drops records it cannot
hold and reports `buffer_exhausted`. Limits cover bytes, record count and
sequence distance, including duplicate input overhead. Source timestamps and
sequence values cannot authorize unbounded allocation. Node configuration sets
these limits; lack of a declared bitrate prevents an advance memory guarantee.

## Transport delay and route admission

The budget covers all internal transport buffering, relays, queueing and the
receiver's remaining wait. Do not apply the full budget to each hop or restart
it at a relay. SRT's buffering remains a per-connection mechanism; its actual
receiver latency is negotiated. See
[SRT receiver latency](https://github.com/Haivision/srt/blob/master/docs/API/API-socket-options.md#srto_rcvlatency).

In this version, destination `srt.latency` keeps its current meaning for
internal links and its current default of 1,000 ms. The controller does not
invent a path-delay estimate from network names, RTT or topology. For each
candidate path, it sums the configured internal SRT receiver delays. Reject a
path when that sum already reaches `B - T`; no headroom remains. Try eligible
alternatives before committing ports. Runtime reports include negotiated
latencies; a larger negotiated value must trigger the same feasibility check.
Passing it establishes only that configured buffers fit, not that the network
can meet the deadline. The example sets 100 ms per link explicitly so a
500 ms budget can admit a two-link relay path.

Planning requires explicit origin, forwarding and scheduled receiver support
for `weave_timed_v1` on every selected hop. A profile that merely declares
`merge: true` is insufficient. All internal links must use encrypted SRT and
forward the envelope unchanged. A receiver for two paths must also advertise
merge support. Missing support makes timed placement fail; never fall back to
ordinary unscheduled forwarding.

A timed receiver profile cannot satisfy a raw two-input merge unless it
separately implements and advertises that behavior. `paths: 2` without
`delivery` does not implicitly enable framing or timed release.

`paths: 2` retains the existing preference for a second path and the existing
attachment and relay exclusion rules. If only one eligible path exists, keep
the timed first path and report `single_path` with redundancy not ready. The
latency contract still applies. Distinct declarations do not prove physical
carrier or failure-domain independence. Sources and receivers remain shared
failure points.

The planner remains deterministic and side-effect free, considers the full
candidate stream set, and respects existing port and relay affinity. Add or
replace an optional second path without interrupting the surviving origin
branch, receiver schedule, sequence watermark or output connection. A runtime
must support that update before advertising this feature. The controller may
repair a failed path later; choosing records from already active paths never
depends on a reconcile tick.

## Capabilities and southbound contract

Add an optional `timed_delivery` capability to hop profiles. It declares
`protocol: weave_timed_v1`, supported `roles` from `origin`, `forward` and
`receiver`, and, for a receiver, `max_latency_ms` and
`scheduling_error_bound_us`. An origin can stamp and fan out raw ingress;
`forward` preserves envelopes; a receiver removes envelopes and schedules raw
output. Roles must fit the profile's ingress and egress shapes. Advertise only
implemented roles. Absence means unsupported.

Desired hops must distinguish `raw` and `weave_timed_v1` payloads per SRT
socket. Origin and forwarding egresses name their payload kind; receiver
ingresses must agree, including `merge_ingress`. Origin desired state carries
the authorized identity and epoch. Receiver desired state also carries `B` and
`T`. These values and payload kinds participate in hop revisions. Plans expose
the resolved timing settings and configured delay sums per path.

Node reports carry clock validity, uncertainty, sample age and expiry. Origin
hop reports include the runtime incarnation identifier needed for epoch
allocation. Receiver hop reports carry current timing and redundancy readiness,
per-path observations and the counters below. Unknown clock values are explicit,
not zero. Reports must match the current hop revision before counting toward
current status. A node must stop timed release locally when a prerequisite
expires; waiting for controller polling is insufficient.

Increment `PROTOCOL_VERSION` and update Rust wire types, validation, tolerant
northbound readers, CLI output, OpenAPI, JSON Schema and every adapter,
including the browser node. Browser profiles continue to advertise no timed
roles. The Strom adapter must not advertise support until its media runtime
can implement the origin, envelope forwarding and scheduled merge behavior.
No media or synchronization traffic passes through the controller or proxies.

## Status and failure behavior

Add these conditions at destination level and aggregate them at stream level:

| Condition | True | False or unknown |
|---|---|---|
| `timing_ready` | Current clock bounds and current runtime observations establish the release contract. | Clock, capability, configuration or runtime failure; unknown before current evidence. |
| `redundancy_ready` | Every requested path is provisioned and has supplied timely media in the current observation window. | `single_path`, missing, late or failed path; unknown before media. |

For untimed destinations, `timing_ready` is true with reason `not_requested`.
One requested path makes `redundancy_ready` true with reason `not_requested`.
Aggregate false before unknown before true, breaking reason ties by destination
id. An unknown condition cannot be replaced by a stale successful report.
Configuration feasibility belongs in `placement_ready`; it cannot establish
`timing_ready`. A timed destination must not read `flowing` while timing is
false or unknown. A missing redundant path reads `degraded` even when every
output record remains timely. A clock fault may leave hops provisioned while
output is suspended. An unused, ready stream still reads `awaiting_input`.

Report these stable failure reasons where applicable:
`timed_delivery_unsupported`, `latency_budget_infeasible`, `clock_unavailable`,
`clock_uncertainty_exceeded`, `clock_discontinuity`, `deadline_missed`,
`scheduling_violation`, `record_conflict`, `output_unavailable`,
`output_backpressure` and `buffer_exhausted`. Details name the destination,
affected path or node and measured versus requested values.

Receiver reports must expose cumulative counters per epoch for records released,
duplicates discarded, late arrivals discarded, known unrecovered records,
scheduling violations and buffer drops. Report current buffer bytes and records,
clock uncertainty, observed release delay bounds, and each path's latest age
and late-record count. Preserve uncertainty in latency observations; a local
clock subtraction alone is not an independently verified measurement.

Evaluate readiness over the last 5 seconds, using monotonic time. A timing
violation keeps `timing_ready` false for at least that window; restore it only
after fresh compliant releases and valid prerequisites. Each requested path
must contribute timely media during that window to establish redundancy.
A late redundant copy does not make timing false when another copy was released
on time. When no source records are produced, retain counters and do not
manufacture new violations. With no recent releases, timing and per-path media
readiness are unknown; an otherwise ready stream without source media reads
`awaiting_input`. Clock expiry and transport failures still invalidate readiness.

| Event | Required behavior |
|---|---|
| One active path fails | Keep the same release schedule from the other path. Report reduced redundancy. |
| A path exceeds the deadline | Discard its late copies; never increase the delay. |
| Both paths miss media | Report known gaps and continue with timely later records. |
| Source clock becomes invalid | Stop stamping timed records; untimed branches may continue. |
| Receiver clock becomes invalid | Suspend timed output, flush pending records and report the fault. |
| Controller or adapter control connection is lost | Existing media continues while local prerequisites remain valid. |
| Source or receiver runtime loses state | Re-prime with a reported discontinuity; obtain a new origin epoch if sequence state was lost. |
| Budget changes | Apply a new receiver revision, flush pending records and re-prime without replaying released sequences. Continuity is not promised for configuration changes. |

Retain the release watermark when changing a budget within an epoch. Decreasing
the budget can discard newly stale records; increasing it can introduce a
pause. Do not restart the origin or unrelated destinations for a receiver-only
budget change. Endpoint publication and desired-state removal remain asynchronous
as in the current API; an unplaced plan is not an instantaneous media cutoff.

## Verification and acceptance

The implementation is complete only when the installed media runtime and both
adapters match the new protocol, contract artifacts are regenerated, and the
following checks pass. Unit tests alone do not establish timed media delivery.

| Layer | Required evidence |
|---|---|
| Validation and contracts | Field limits, defaults, equality, unsupported endpoints, stream sets, schemas and protocol rejection. |
| Planning | Full candidate sets, capability filtering, per-path delay feasibility, alternative selection, affinity, encryption and explicit single-path fallback. |
| Protocol | Cross-runtime golden byte vectors, framing limits, fragmentation of ingress payloads, epoch authorization, duplicates, reordering, conflicts and malformed input. |
| Scheduler | Fake-clock tests for release bounds, uncertainty, drift, clock steps, expiry, pauses, startup, budget changes and bounded queues. |
| Runtime lifecycle | Controller failover preserves epochs and schedules; source restart cannot reuse identity; path repair preserves the surviving flow and output connection. |
| Media bench | Decoded media identity and continuity, record byte equality under single-path loss, per-path delay and jitter, retransmissions, both-path deadline misses, consumer backpressure and recovery. |
| Timing bench | Independent observations of ingress and output submission with stated measurement uncertainty, separate from decoder progress and controller status. |

The timing bench must test a direct route and independently relayed routes,
one- and two-path destinations, concurrent feeds and two different destination
budgets. Include asymmetric paths, loss of the faster path, a surviving path
near the deadline, both paths too late, source and receiver clock faults, CPU
contention and buffer limits. Measure the full managed boundary so per-hop
buffering cannot be mistaken for the requested total delay.

Container tests sharing a host clock verify merge behavior but do not establish
clock agreement across hosts. Verify clock bounds and timing on separate hosts
with real synchronization and the installed configuration. State observation
duration, load, sample count, measurement uncertainty, maximum observed error
and every missed deadline. Zero observed failures is bounded test evidence,
not proof that every future network and scheduler condition meets the budget.

Run `just build`, `just fmt-check`, `just lint`, `just test` and `just contracts`.
Run the database integration tests with a test Postgres instance, the extended
media corpus and the separate-host timing bench. Rebuild and reinstall affected
nodes and control-plane components, apply configuration changes and verify
the installed system. A spec-only change requires no runtime migration.
