# Webhooks

The controller POSTs a JSON event to one receiver when a node registers, goes
offline or comes back, and when a stream's conditions change.

| Variable | Meaning |
|---|---|
| `WEAVE_WEBHOOK_URL` | Absolute URL receiving events. Unset or blank turns webhooks off. |
| `WEAVE_WEBHOOK_TOKEN` | Optional. Sent to the receiver as `Authorization: Bearer <token>`. |
| `WEAVE_WEBHOOK_EVENTS` | Comma-separated event types to deliver. Unset or empty delivers all of them. |

## Events

| Event | When |
|---|---|
| `node.registered` | Every accepted registration, including a node that is already online registering again, as a page reload does. |
| `node.online` | A node heartbeats after being marked offline. |
| `node.offline` | A node passes `WEAVE_NODE_TTL_SECS` without a heartbeat. |
| `node.forgotten` | An offline node passes `WEAVE_NODE_FORGET_SECS` without a heartbeat and is removed. A node a stored stream names is never removed. |
| `stream.changed` | A reconcile tick computes a stream's conditions for the first time, or computes conditions that differ from the previous tick's in `type`, `status` or `reason`, on the stream or any destination. |

`WEAVE_WEBHOOK_EVENTS=node.registered,node.online,node.offline,node.forgotten`
delivers node events only. The full payload schema is
[`contracts/json-schema/webhook-event.json`](../contracts/json-schema/webhook-event.json).

Receivers must accept unknown fields and event types; see
[API compatibility](api.md#compatibility).

Node events carry the node descriptor, with a resolved [site](nodes.md#sites)
among its attachments, and without endpoints or hop status. Stream
events carry generations, status and conditions for the stream and its
destinations. The schema above defines both payloads.

A tick sends at most one event per stream. Changes to `detail` alone and stream
deletion send no event. With Postgres, the first tick after startup or takeover
compares conditions against the stored ones. An in-memory restart loses streams
and conditions.

## Delivery

Events use a bounded in-memory queue. One worker delivers them in order, with
up to four attempts. HTTP 4xx responses other than 408 and 429 are not
retried. Queue overflow,
restart and exhausted retries can lose events; retries can deliver duplicates.
Retries preserve `event_id`, but IDs are timestamp-based and are not guaranteed
unique across controller runs. A former leader continues delivering queued
events, so deliveries from different controllers can interleave.

Read `GET /nodes` and `/status` to reconcile state. Delivery has no maximum delay.
The optional bearer token authenticates the sender. Body signatures are not
implemented.
