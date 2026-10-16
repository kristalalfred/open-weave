# Webhooks

The controller POSTs a JSON event to one receiver when a node registers, goes
offline or comes back, and when a stream's conditions change. A service hosting
guest pages can use them to declare a stream when a page registers and to hear
when it flows, without polling `/nodes` and `/status`.

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

### Node events

`node` is the node's descriptor, without `endpoints` or `hop_status`. This one
is abbreviated:

```json
{
  "event_id": "guest-1-1788520800000003",
  "event_type": "node.registered",
  "occurred_at": "2026-09-04T11:22:33.123456789Z",
  "node": {
    "id": "guest-1",
    "status": "ready",
    "endpoint": "browser://guest-1",
    "capabilities": {
      "adapters": [],
      "hop_profiles": [{
        "id": "camera-to-whip",
        "ingress": { "device": "capture" },
        "egress": { "transport": "whip", "roles": ["connect"] },
        "max_egresses": 1
      }]
    },
    "topology": {
      "attachments": [{ "id": "client", "network": "internet", "dial": true }]
    }
  }
}
```

### Stream events

`stream` has the stream's `name`, `generation`, `observed_generation` and
`status`, and every condition on the stream and on each destination. It leaves
out the stream's nodes and addresses, which `/status` and
`/streams/{name}/endpoints` carry. This one is abbreviated; a stream and each
destination carry all five conditions:

```json
{
  "event_id": "basic-1790330400000007",
  "event_type": "stream.changed",
  "occurred_at": "2026-09-25T10:15:02.412345678Z",
  "stream": {
    "name": "basic",
    "generation": 2,
    "observed_generation": 2,
    "status": "degraded",
    "conditions": [{
      "type": "nodes_available",
      "status": "false",
      "reason": "node_offline",
      "detail": "node strom-node-2 is offline",
      "last_transition_time": "2026-09-25T10:15:02.410123456Z"
    }],
    "destinations": [{
      "id": "studio",
      "status": "degraded",
      "conditions": [{
        "type": "media_flowing",
        "status": "false",
        "reason": "media_degraded",
        "detail": "media is not flowing across every branch",
        "last_transition_time": "2026-09-25T10:15:02.410123456Z"
      }]
    }]
  }
}
```

A tick sends at most one event per stream. A change of `detail` alone sends
nothing, and deleting a stream sends nothing. An event follows the change it
reports by up to one reconcile interval plus an adapter poll.

The controller stores a stream's conditions whenever a tick changes them, and
the first tick after a start or takeover compares against the stored ones. With
the in-memory store a restart starts empty, so its first tick sends an event for
every stream.

## Delivery

Emitting never blocks a registration or a reconcile tick. One background worker
delivers queued events in order. It retries a connect error or `5xx` with
backoff and gives up after four attempts. A `4xx` is not retried. A full queue
drops events, and the queue is in memory, so events do not survive a restart.
A controller that loses the lease keeps delivering what it queued, so around a
takeover a receiver can get both controllers' events interleaved.

`event_id` is the node id or stream name followed by a number. It is the same
across retries of one delivery, so a receiver can deduplicate, and no controller
reuses one, across restarts and takeovers.

Delivery is at least once and can miss events. A receiver should read
`GET /nodes` and `/status` when it starts and treat events as hints.

The bearer token authenticates the controller to the receiver. It does not
prove an event genuine to a receiver that cannot trust everyone holding the
token. Body signatures are not implemented.
