use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::api::{StreamCondition, StreamStatus};
use crate::{NodeCapabilities, NodeDescriptor, NodeStatus, NodeTopology, PathStatus};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub event_id: String,
    pub event_type: EventType,

    pub occurred_at: String,

    #[serde(flatten)]
    pub subject: Subject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Subject {
    Node(NodeSummary),
    Stream(StreamSummary),
}

impl Subject {
    #[must_use]
    pub fn id(&self) -> &str {
        match self {
            Self::Node(node) => &node.id,
            Self::Stream(stream) => &stream.name,
        }
    }
}

impl From<NodeSummary> for Subject {
    fn from(node: NodeSummary) -> Self {
        Self::Node(node)
    }
}

impl From<StreamSummary> for Subject {
    fn from(stream: StreamSummary) -> Self {
        Self::Stream(stream)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum EventType {
    #[serde(rename = "node.registered")]
    NodeRegistered,
    #[serde(rename = "node.online")]
    NodeOnline,
    #[serde(rename = "node.offline")]
    NodeOffline,
    #[serde(rename = "node.forgotten")]
    NodeForgotten,
    #[serde(rename = "stream.changed")]
    StreamChanged,
}

impl EventType {
    pub const ALL: [Self; 5] = [
        Self::NodeRegistered,
        Self::NodeOnline,
        Self::NodeOffline,
        Self::NodeForgotten,
        Self::StreamChanged,
    ];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NodeRegistered => "node.registered",
            Self::NodeOnline => "node.online",
            Self::NodeOffline => "node.offline",
            Self::NodeForgotten => "node.forgotten",
            Self::StreamChanged => "stream.changed",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeSummary {
    pub id: String,
    pub status: NodeStatus,
    pub endpoint: String,
    #[serde(default)]
    pub capabilities: NodeCapabilities,
    pub topology: NodeTopology,
}

impl From<&NodeDescriptor> for NodeSummary {
    fn from(node: &NodeDescriptor) -> Self {
        Self {
            id: node.id.clone(),
            status: node.status,
            endpoint: node.endpoint.clone(),
            capabilities: node.capabilities.clone(),
            topology: node.topology.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamSummary {
    pub format_policy: crate::FormatPolicy,
    pub name: String,
    pub generation: u64,
    pub observed_generation: Option<u64>,
    pub status: PathStatus,
    pub conditions: Vec<StreamCondition>,
    pub destinations: Vec<DestinationSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DestinationSummary {
    pub id: String,
    pub status: PathStatus,
    pub conditions: Vec<StreamCondition>,
}

impl From<&StreamStatus> for StreamSummary {
    fn from(stream: &StreamStatus) -> Self {
        Self {
            format_policy: stream.format_policy,
            name: stream.name.clone(),
            generation: stream.generation,
            observed_generation: stream.observed_generation,
            status: stream.status,
            conditions: stream.conditions.clone(),
            destinations: stream
                .destinations
                .iter()
                .map(|destination| DestinationSummary {
                    id: destination.id.clone(),
                    status: destination.status,
                    conditions: destination.conditions.clone(),
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        HopEndpointClass, HopProfile, LocalEnd, RoleSet, SocketRole, Transport, TransportClass,
    };

    fn node() -> NodeDescriptor {
        NodeDescriptor {
            inputs: Vec::new(),
            outputs: Vec::new(),
            id: "guest-1".to_string(),
            endpoint: "http://guest-1:8080".to_string(),
            status: NodeStatus::Ready,
            capabilities: NodeCapabilities {
                adapters: Vec::new(),
                hop_profiles: vec![HopProfile {
                    timed_delivery: None,
                    id: "camera-to-whip".to_string(),
                    ingress: HopEndpointClass::Local(LocalEnd::Input),
                    egress: HopEndpointClass::Transport(TransportClass {
                        transport: Transport::Whip,
                        roles: RoleSet::only(SocketRole::Connect),
                    }),
                    max_egresses: Some(1),
                    media_behavior: crate::MediaBehavior::Unknown,
                    merge: false,
                    accepts: None,
                }],
            },
            topology: NodeTopology::default(),
        }
    }

    #[test]
    fn summary_carries_the_node_and_nothing_else() {
        let summary = NodeSummary::from(&node());

        assert_eq!(summary.id, "guest-1");
        assert_eq!(summary.status, NodeStatus::Ready);
        assert_eq!(summary.endpoint, "http://guest-1:8080");
        assert!(summary.capabilities.offers_local(LocalEnd::Input));

        let body = serde_json::to_value(&summary).unwrap();
        assert_eq!(
            keys(&body),
            ["capabilities", "endpoint", "id", "status", "topology"]
        );
    }

    fn endpoint() -> crate::EndpointAddr {
        crate::EndpointAddr {
            node: "strom-node-2".to_string(),
            host: Some("172.27.0.10".to_string()),
            port: Some(20001),
            url: "srt://172.27.0.10:20001".to_string(),
        }
    }

    fn stream_status() -> StreamStatus {
        let condition = StreamCondition {
            condition_type: crate::api::StreamConditionType::MediaFlowing,
            status: crate::api::StreamConditionStatus::False,
            reason: crate::api::StreamConditionReason::AwaitingInput,
            detail: "the source is not providing media".to_string(),
            last_transition_time: "2026-09-25T10:00:00Z".to_string(),
        };
        StreamStatus {
            format_policy: crate::FormatPolicy::Report,
            name: "basic".to_string(),
            generation: 2,
            observed_generation: Some(2),
            status: PathStatus::AwaitingInput,
            nodes: vec!["strom-node-1".to_string(), "strom-node-2".to_string()],
            conditions: vec![condition.clone()],
            ingress: Some(endpoint()),
            destinations: vec![crate::api::StreamDestinationStatus {
                delivery: None,
                id: "studio".to_string(),
                status: PathStatus::AwaitingInput,
                nodes: vec!["strom-node-2".to_string()],
                conditions: vec![condition],
                endpoint: Some(endpoint()),
            }],
        }
    }

    fn keys(value: &serde_json::Value) -> Vec<&str> {
        let mut keys: Vec<&str> = value
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        keys
    }

    #[test]
    fn a_stream_summary_carries_status_and_conditions_but_no_addresses() {
        let status = stream_status();
        let summary = StreamSummary::from(&status);

        assert_eq!(summary.name, "basic");
        assert_eq!(summary.generation, 2);
        assert_eq!(summary.observed_generation, Some(2));
        assert_eq!(summary.conditions, status.conditions);
        assert_eq!(
            summary.destinations[0].conditions,
            status.destinations[0].conditions
        );

        let body = serde_json::to_value(&summary).unwrap();
        assert_eq!(
            keys(&body),
            [
                "conditions",
                "destinations",
                "format_policy",
                "generation",
                "name",
                "observed_generation",
                "status"
            ]
        );
        assert_eq!(
            keys(&body["destinations"][0]),
            ["conditions", "id", "status"]
        );
        assert!(!body.to_string().contains("172.27.0.10"));
    }

    #[test]
    fn a_node_event_keeps_its_node_field() {
        let event = Event {
            event_id: "guest-1-0".to_string(),
            event_type: EventType::NodeRegistered,
            occurred_at: "2026-09-25T10:00:00Z".to_string(),
            subject: Subject::Node(NodeSummary::from(&node())),
        };
        let body = serde_json::to_value(&event).unwrap();
        assert_eq!(
            keys(&body),
            ["event_id", "event_type", "node", "occurred_at"]
        );
        assert_eq!(body["node"]["id"], "guest-1");
        assert_eq!(serde_json::from_value::<Event>(body).unwrap(), event);
    }

    #[test]
    fn a_stream_event_round_trips_with_a_stream_field() {
        let event = Event {
            event_id: "basic-0".to_string(),
            event_type: EventType::StreamChanged,
            occurred_at: "2026-09-25T10:00:00Z".to_string(),
            subject: Subject::Stream(StreamSummary::from(&stream_status())),
        };
        let body = serde_json::to_value(&event).unwrap();
        assert_eq!(
            keys(&body),
            ["event_id", "event_type", "occurred_at", "stream"]
        );
        assert_eq!(body["stream"]["conditions"][0]["reason"], "awaiting_input");
        assert_eq!(serde_json::from_value::<Event>(body).unwrap(), event);
    }

    #[test]
    fn event_types_round_trip_through_their_wire_names() {
        for kind in EventType::ALL {
            assert_eq!(EventType::parse(kind.as_str()), Some(kind));
            assert_eq!(
                serde_json::to_value(kind).unwrap(),
                serde_json::json!(kind.as_str())
            );
        }
        assert_eq!(EventType::parse("node.exploded"), None);
    }
}
