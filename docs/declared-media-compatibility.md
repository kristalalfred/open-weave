# Declared media compatibility

Proposed contract. This feature is not implemented.

open-weave routes prepared feeds between media nodes. A feed may carry any
supported media format; different streams need not share one format. Optional
strict checking permits a route only when its declarations establish that the
feed reaches every destination unchanged and satisfies each destination's
requirements.

Compatibility describes declarations, not inspected media or successful decoding.
The controller never probes a source or constructs a media-processing pipeline.

## Scope

An operator or application prepares the feed outside open-weave. For example,
a media runtime can ingest an RTSP camera and expose its output over SRT.
The stream declares the format of that exposed output, after any preparation.
The camera's RTSP URL, credentials and processing configuration remain outside
the routing contract.

This change uses the existing source and destination endpoints. It does not add
source registration, a named-feed inventory, RTSP, NDI, SDI, new transports,
automatic conversion, output-format negotiation or mixed-transport fan-out.
Existing gateway and browser routes remain available under the default policy.

The responsibilities are:

| Component | Responsibility |
|---|---|
| Operator or application | Declare the prepared source format and destination requirements. |
| Media runtime | Prepare, forward and consume media; report operational failures through its adapter. |
| Adapter | Advertise operations its runtime implements, including their input constraints and whether they preserve the payload. |
| Controller | Match declarations, choose eligible routes and report compatibility separately from media activity. |

## Current behavior

[Streams](streams.md#formats) defines optional source `format` and destination
`accepts`. A known mismatch is reported without preventing placement. The
controller does not inspect the payload. An absent source format is unknown.

[Hop profiles](nodes.md#hop-profiles) pair an ingress with an egress and may
declare `accepts`. They do not describe output formats or payload preservation.
The current format checks therefore do not establish compatibility through an
arbitrary sequence of gateway profiles.

## Stream policy

Add `format_policy` to the stream definition:

| Value | Behavior |
|---|---|
| `report` | Default. Report compatible, incompatible or unknown declarations. Format checks do not prevent routing. |
| `strict` | Permit routing only when the complete selected route is compatible by declaration. |

Omission means `report`. An explicit `report` and an omitted policy are
semantically identical for no-op applies, generations and revisions. Unknown
policy values are validation errors. The field applies equally to standalone
streams and stream-set members.

Strict checking is per stream and covers every destination and every selected
path. A failing destination blocks the whole stream. Operators needing independent
admission can use separate streams. Requesting a second path retains the existing
redundancy rules; every path actually selected must pass the format check.

## Declarations

Keep the existing `MediaFormat` and `FormatConstraint` vocabulary. This change
does not require a universal codec, container or deployment-wide media standard.

A present source `format` declares a fixed container and the tracks present.
An omitted audio or video track in that declaration means that track is absent.
An entirely absent `format` means the source format is unknown. A present track
must include the fields required by its existing format type.

A destination's `accepts` has these meanings:

- Absent: requirements are undeclared, so compatibility is unknown.
- `{}`: an explicit declaration of no restrictions within the modeled format
  vocabulary.
- Nonempty: the source must satisfy every supplied constraint. Omitted fields
  impose no constraint. A constraint on a track requires that track to exist.

Add `media_behavior` to `HopProfile`, with values `preserve` and `unknown`.
Omission means `unknown`. Never infer preservation from matching transport names
or from a profile id.

`preserve` declares that the operation forwards the complete application media
payload unchanged, including its container, tracks and embedded metadata. Changes
to network packet boundaries and transport encryption do not change the media
payload. The declaration does not promise delivery without loss or interruption.
An operation that remuxes, transcodes, filters tracks or interprets and rebuilds
the payload must not advertise `preserve` under this contract.

A preserving profile must also explicitly declare its input `accepts`. `{}`
means it can forward payloads without imposing modeled format restrictions.
An omitted `accepts` remains unknown. For other profiles, `accepts` can still
establish an input mismatch, but does not establish the output format.

Initially, the Strom adapter advertises `preserve` and `accepts: {}` for
`srt-forward`. Its other profiles and both browser profiles advertise `unknown`.
In particular, the RIST packetization and WebRTC gateway profiles do not qualify
merely because they expose familiar transports. Additional preservation claims
require a defined payload boundary, accurate constraints and runtime verification.

## Compatibility evaluation

Evaluate the same declarations for preview and reconciliation:

1. Begin with the prepared source's declared format, or unknown.
2. Check the selected sender, relay and receiver profiles in route order.
   Check each profile's input constraints against the format reaching that hop.
3. A `preserve` profile carries that format forward. An `unknown` profile makes
   its output format unknown; do not compare the original source format with
   downstream requirements as though the operation preserved it.
4. Check the format delivered to each destination against its `accepts`.

An undeclared input constraint makes the compatibility result unknown even if
the profile preserves a known format. This does not erase the known format:
later checks may still establish a mismatch. A verified mismatch at a boundary
with known input remains incompatible even if another boundary is unknown.

| Result | Meaning |
|---|---|
| Compatible | Source format, every hop's constraints and preservation behavior, and every destination's requirements are declared and satisfied. |
| Incompatible | At least one comparison with a known input violates a declared constraint. |
| Unknown | No known mismatch, but at least one required declaration or route segment cannot be evaluated. |

The stream aggregate is incompatible if any destination is incompatible,
otherwise unknown if any is unknown, otherwise compatible. Constraints and
claims cover only the fields represented by the model. They do not certify
decoder support for unmodeled codec details or the correctness of declarations.

Strict policy makes these requirements part of route eligibility, before
choosing profiles and reserving ports. Consider eligible alternatives instead
of rejecting the stream solely because the first otherwise reachable route
contains an unknown profile. Preserve existing route preferences among eligible
routes. A blocked stream must not consume ports needed by another stream.

## Apply and runtime behavior

A syntactically valid strict stream is accepted as desired state even when its
current declarations or topology prevent routing. This follows the existing
behavior for missing nodes. Strict policy rejects route placement, not the API
write. Stream-set writes retain their existing atomicity.

If no compatible complete route exists, the stream has no desired hops or
published endpoints. It reads `pending`, with `placement_ready: false` and a
format reason when format checks caused the failure. Endpoint discovery returns
the existing unplaced response. A topology failure remains a topology failure;
do not invent a format mismatch when no candidate route can be evaluated.

Reevaluate on source or destination edits, policy changes, node capability
changes and route changes. A blocked stream becomes eligible without another
apply when its declarations and topology permit a compatible route.

If an active stream becomes blocked, reconciliation removes its desired hops
and endpoint discovery stops publishing its endpoints. Adapters remove managed
flows through the existing desired-state mechanism. This can interrupt every
destination, including after a change from `report` to `strict`. Do not keep an
incompatible old route as a fallback.

Removal is asynchronous. A disconnected adapter may keep its old flows until
it receives the new desired state. Strict policy is a routing admission rule,
not an immediate media cutoff or a security boundary. It also cannot detect a
producer changing its actual format without updating the declaration.

## Status and interfaces

Use the existing three-valued `format_compatible` condition:

| Status | Reason |
|---|---|
| `true` | `format_compatible` |
| `false` | `format_mismatch` |
| `unknown` | `source_format_unknown`, `destination_accepts_unknown`, `hop_accepts_unknown`, `media_behavior_unknown` or `format_route_unknown` |

Details identify the destination, node and profile where applicable, and the
missing field or incompatible values. Choose aggregate reasons deterministically
in destination-id and route order, with known mismatches taking precedence.
Keep the existing condition transition-time semantics.

A strict format failure sets `placement_ready` to false with the same reason
as its blocking format condition. Per-destination format results remain visible
even though the whole stream is unplaced. Disabled streams stay disabled and
create no hops; enabling them invokes the policy.

Under `report`, known mismatches retain the existing degraded behavior. Unknown
compatibility does not independently degrade or block a stream. Such a stream
may read `flowing` while `format_compatible` reads `unknown`. Neither `flowing`
nor declared compatibility means the destination has decoded usable media.

Expose the policy and compatibility results consistently in plan responses,
stream and destination status, the CLI, dashboard and existing webhook payloads.
Preview returns `unplaced` for a strict format failure and includes its reasons
without returning executable hops or endpoints. Evaluate against the same full
candidate stream set used by apply. Persist the policy with the stream.

The profile contract requires a southbound protocol-version increment and updates
to every shipped adapter. Regenerate JSON Schema and OpenAPI artifacts. Stored
streams without a policy retain `report`; stored profiles without preservation
metadata remain unknown until adapters register the new declaration. No legacy
profile may qualify for strict routing through a default or migration guess.

## Example

This proposed manifest routes an already prepared feed. The new `format_policy`
field is not accepted by the current release.

```yaml
name: camera-to-studio
format_policy: strict
source:
  srt:
    node: venue
    format:
      container: mpeg_ts
      video:
        codec: h264
        width: 1920
        height: 1080
        framerate: { numerator: 25, denominator: 1 }
        chroma_subsampling: yuv420
      audio: { codec: aac, sample_rate: 48000, channels: 2 }
destinations:
  - id: studio
    srt:
      node: studio
      accepts:
        container: [mpeg_ts]
        video: { codec: [h264] }
        audio: { codec: [aac], sample_rate: [48000], channels: [2] }
```

This stream can route through declared preserving SRT profiles. Another stream
may carry H.265 along the same nodes if its destination accepts H.265. Changing
this source declaration to H.265 blocks this stream because the studio requires
H.264. Removing its format or the studio's `accepts` makes compatibility unknown
and also blocks strict routing. Setting `format_policy: report` permits the route
while retaining the incompatible or unknown condition.

## Acceptance criteria

- Existing manifests default to `report` and retain their routing behavior.
  Omitted and explicit defaults preserve no-op write semantics.
- Different declared media formats route concurrently through preserving nodes
  to destinations whose requirements they satisfy.
- Matching, mismatching and missing declarations produce distinct results.
  Explicit empty constraints, missing tracks and partially constrained formats
  follow the rules above.
- Every selected hop and path is checked. Unknown output behavior prevents
  claiming end-to-end compatibility, including for existing gateway profiles.
- Strict planning finds eligible alternatives, blocks the entire stream when
  required, releases candidate ports, and never inserts a conversion.
- Preview, reconciliation and stream-set behavior agree. Changes to policy,
  capabilities or topology cause blocking and recovery as specified. Other
  streams and their ports remain unaffected.
- All status surfaces distinguish declared compatibility from reported media
  activity. Persistence, restart and controller takeover preserve the policy.
- Bench verification drives a compatible strict SRT stream and checks decoded
  media, then demonstrates mismatch blocking, unknown blocking, removal of an
  active blocked route and recovery. It also verifies that `report` still routes
  a deliberately mismatching declaration. Record bench evidence separately from
  unit tests and do not describe it as independent equipment verification.
