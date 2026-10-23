pub mod api;
pub mod auth;
pub mod contracts;
pub mod delivery;
pub mod logging;
pub mod media;
pub mod peer;
pub mod read;
pub mod shutdown;
#[cfg(feature = "server")]
pub mod upstream;
pub mod validation;
pub mod webhook;

use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, SocketAddrV4},
    ops::Deref,
};

use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};

pub use delivery::{
    DeliveryCapability, DeliveryCounters, DeliveryPathReport, DeliveryPolicy, DeliveryReason,
    DeliveryReport, DeliveryRole, DeliveryState, PayloadKind, TimedHop,
};

pub use api::{
    AcceptedState, ApiError, ApiErrorCode, Health, NodeAccepted, NodeResource, PlanStatus,
    RunningStatus, SiteState, StartingState, StartingStatus, StatusResponse, StreamAccepted,
    StreamCondition, StreamConditionReason, StreamConditionStatus, StreamConditionType,
    StreamDestinationStatus, StreamPlan, StreamResource, StreamSetAccepted, StreamSetAction,
    StreamSetApply, StreamSetMemberResult, StreamSetResource, StreamStatus,
};
pub use media::{
    AudioCodec, AudioConstraint, AudioFormat, ChromaSubsampling, Container, FormatConstraint,
    Framerate, MediaFormat, Mismatch, VideoCodec, VideoConstraint, VideoFormat,
};
pub use peer::{
    LinkDescription, PeerAttempt, PeerLinkId, PeerReport, PeerRole, PeerSocket,
    ROUTE_NODE_SIGNALLING, SessionDescription, SignallingExchange, SignallingReply, TurnCredential,
    TurnServer, WEBRTC_TRANSPORT, WebrtcClass, WebrtcTransport,
};
pub use validation::{
    RESOURCE_ID_MAX_LEN, ResourceIdError, ValidationIssue, resource_id_issue, validate_local_io,
    validate_node, validate_observation, validate_resource_id, validate_stream,
};

pub const ROUTE_ENDPOINTS: &str = "/endpoints";
pub const ROUTE_NODES: &str = "/nodes";
pub const ROUTE_NODE_DESIRED: &str = "/nodes/{node_id}/desired";
pub const ROUTE_NODE_HEARTBEAT: &str = "/nodes/{node_id}/heartbeat";
pub const ROUTE_NODE_REGISTER: &str = "/nodes/register";
pub const ROUTE_STATE: &str = "/state";
pub const ROUTE_STATUS: &str = "/status";
pub const ROUTE_STREAM: &str = "/streams/{name}";
pub const ROUTE_STREAM_ENDPOINTS: &str = "/streams/{name}/endpoints";
pub const ROUTE_STREAM_PLANS: &str = "/stream-plans";
pub const ROUTE_STREAM_SET: &str = "/stream-sets/{owner}";
pub const ROUTE_STREAM_SETS: &str = "/stream-sets";
pub const ROUTE_STREAMS: &str = "/streams";

pub const PROTOCOL_VERSION: u32 = 11;

#[must_use]
pub fn protocol_compatible(version: u32) -> bool {
    version == PROTOCOL_VERSION
}

#[cfg(test)]
mod contract_tests {
    use super::*;

    fn endpoint(node: &str) -> StreamTransport {
        StreamTransport::Srt(SrtEndpoint {
            node: Some(node.to_string()),
            remote: None,
            via: Vec::new(),
            network: None,
            latency: None,
            passphrase: None,
            format: None,
            accepts: None,
        })
    }

    fn destination(id: &str, node: &str) -> StreamDestination {
        StreamDestination {
            delivery: None,
            id: id.to_string(),
            paths: 1,
            endpoint: endpoint(node),
        }
    }

    #[test]
    fn destination_order_is_not_semantic() {
        let left = StreamDefinition {
            name: "feed".to_string(),
            enabled: true,
            format_policy: crate::FormatPolicy::Report,
            allow_cleartext_links: false,
            source: endpoint("source"),
            destinations: vec![destination("studio", "a"), destination("preview", "b")],
        };
        let mut right = left.clone();
        right.destinations.reverse();
        assert_eq!(left, right);
    }

    #[test]
    fn duplicate_destination_ids_are_rejected() {
        let stream = StreamDefinition {
            name: "feed".to_string(),
            enabled: true,
            format_policy: crate::FormatPolicy::Report,
            allow_cleartext_links: false,
            source: endpoint("source"),
            destinations: vec![destination("studio", "a"), destination("studio", "b")],
        };
        assert!(
            validate_stream(&stream)
                .iter()
                .any(|issue| { issue.field == "destinations[1].id" && issue.code == "duplicate" })
        );
    }

    #[test]
    fn destination_endpoint_is_flattened() {
        let value = serde_json::to_value(destination("studio", "studio-node")).unwrap();
        assert_eq!(value["id"], "studio");
        assert_eq!(value["srt"]["node"], "studio-node");
        assert!(value.get("endpoint").is_none());
    }

    #[test]
    fn a_keyed_socket_round_trips_and_debug_never_prints_the_key() {
        let socket = SocketSpec::Srt(SrtSocket::Listen {
            port: 7001,
            params: SrtParams {
                payload: crate::PayloadKind::Raw,
                latency: Some(200),
                passphrase: Some(Passphrase::new("correct horse battery")),
                pbkeylen: Some(32),
            },
        });
        let value = serde_json::to_value(&socket).unwrap();
        assert_eq!(value["params"]["passphrase"], "correct horse battery");
        assert_eq!(value["params"]["pbkeylen"], 32);
        assert_eq!(serde_json::from_value::<SocketSpec>(value).unwrap(), socket);
        assert!(!format!("{socket:?}").contains("correct horse"));
    }

    #[test]
    fn a_rist_socket_round_trips_and_carries_no_params() {
        for socket in [
            SocketSpec::Rist(RistSocket::Listen { port: 7100 }),
            SocketSpec::Rist(RistSocket::Connect {
                host: "10.0.0.2".to_string(),
                port: 7100,
            }),
        ] {
            let value = serde_json::to_value(&socket).unwrap();
            assert_eq!(value["transport"], "rist");
            assert_eq!(serde_json::from_value::<SocketSpec>(value).unwrap(), socket);
        }
        let keyed = serde_json::from_value::<SocketSpec>(serde_json::json!({
            "transport": "rist",
            "role": "listen",
            "port": 7100,
            "params": { "passphrase": "0123456789" }
        }));
        assert!(keyed.is_err(), "a rist socket carries no params");
    }

    #[test]
    fn rist_pairs_are_even_ports_whose_next_port_is_in_range() {
        let pairs = |start, end| PortRange { start, end }.rist_pairs().collect::<Vec<_>>();
        assert_eq!(pairs(20_000, 20_005), vec![20_000, 20_002, 20_004]);
        assert_eq!(pairs(20_001, 20_004), vec![20_002]);
        assert_eq!(pairs(20_000, 20_000), Vec::<u16>::new());
        assert_eq!(pairs(65_534, 65_535), vec![65_534]);
    }

    #[test]
    fn a_rist_listener_needs_a_host_and_a_port_pair() {
        let node = |host: &str, start, end| NodeDescriptor {
            inputs: Vec::new(),
            outputs: Vec::new(),
            id: "studio".to_string(),
            endpoint: "http://studio".to_string(),
            status: NodeStatus::Ready,
            capabilities: NodeCapabilities::default(),
            topology: DeclaredTopology {
                site: None,
                attachments: vec![NetworkAttachment {
                    id: "wan".to_string(),
                    network: "internet".to_string(),
                    dial: false,
                    listeners: NetworkListeners {
                        rist: Some(RistListener {
                            host: host.to_string(),
                            port_range: PortRange { start, end },
                        }),
                        ..NetworkListeners::default()
                    },
                }],
            },
        };
        assert!(validate_node(&node("10.0.0.2", 21_000, 21_001)).is_empty());
        let codes = |node| {
            validate_node(&node)
                .into_iter()
                .map(|issue| (issue.field, issue.code))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            codes(node("10.0.0.2", 21_001, 21_002)),
            vec![(
                "node.topology.attachments[0].listeners.rist.port_range".to_string(),
                "invalid_range".to_string()
            )]
        );
        assert_eq!(
            codes(node(" ", 21_000, 21_001)),
            vec![(
                "node.topology.attachments[0].listeners.rist.host".to_string(),
                "blank".to_string()
            )]
        );
    }

    #[test]
    fn a_socket_without_a_key_decodes_unkeyed() {
        let socket: SocketSpec = serde_json::from_value(serde_json::json!({
            "transport": "srt",
            "role": "listen",
            "port": 7001,
            "params": { "latency": 200 }
        }))
        .unwrap();
        let SocketSpec::Srt(socket) = socket else {
            panic!("expected an SRT socket");
        };
        assert_eq!(socket.params().passphrase, None);
        assert_eq!(socket.params().pbkeylen, None);
    }

    #[test]
    fn destination_paths_default_to_one_and_serialize_only_when_two() {
        let parsed: StreamDestination = serde_json::from_value(serde_json::json!({
            "id": "studio",
            "srt": { "node": "studio-node" }
        }))
        .unwrap();
        assert_eq!(parsed.paths, 1);
        assert!(
            serde_json::to_value(&parsed)
                .unwrap()
                .get("paths")
                .is_none()
        );

        let two: StreamDestination = serde_json::from_value(serde_json::json!({
            "id": "studio",
            "paths": 2,
            "srt": { "node": "studio-node" }
        }))
        .unwrap();
        assert_eq!(two.paths, 2);
        assert_eq!(serde_json::to_value(&two).unwrap()["paths"], 2);
        assert!(
            serde_json::from_value::<StreamDestination>(serde_json::json!({
                "id": "studio",
                "path": 2,
                "srt": { "node": "studio-node" }
            }))
            .is_err()
        );
    }

    #[test]
    fn a_flattened_payload_refuses_a_field_neither_part_names() {
        let egress = serde_json::json!({ "branch_id": "studio", "condition": "flowing" });
        assert!(serde_json::from_value::<EgressStatus>(egress.clone()).is_ok());
        let mut extra = egress;
        extra["bogus"] = serde_json::json!(1);
        let error = serde_json::from_value::<EgressStatus>(extra).unwrap_err();
        assert!(
            error.to_string().contains("unknown field `bogus`"),
            "{error}"
        );

        let event = serde_json::json!({
            "event_id": "guest-1-0",
            "event_type": "node.offline",
            "occurred_at": "2026-10-16T10:00:00Z",
            "node": {
                "id": "guest-1",
                "status": "offline",
                "endpoint": "browser://guest-1",
                "topology": { "attachments": [] }
            }
        });
        assert!(serde_json::from_value::<webhook::Event>(event.clone()).is_ok());
        let mut extra = event;
        extra["bogus"] = serde_json::json!(1);
        let error = serde_json::from_value::<webhook::Event>(extra).unwrap_err();
        assert!(
            error.to_string().contains("unknown field `bogus`"),
            "{error}"
        );
    }

    #[test]
    fn superseded_capability_fields_are_rejected() {
        let result = serde_json::from_value::<NodeCapabilities>(serde_json::json!({
            "transports": ["srt"],
            "relay": true
        }));
        assert!(result.is_err());
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FormatPolicy {
    #[default]
    Report,
    Strict,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MediaBehavior {
    Preserve,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamDefinition {
    #[serde(default)]
    pub format_policy: FormatPolicy,
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub name: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_cleartext_links: bool,
    pub source: StreamTransport,
    #[schemars(length(min = 1))]
    pub destinations: Vec<StreamDestination>,
}

impl PartialEq for StreamDefinition {
    fn eq(&self, other: &Self) -> bool {
        if self.name != other.name
            || self.format_policy != other.format_policy
            || self.enabled != other.enabled
            || self.allow_cleartext_links != other.allow_cleartext_links
            || self.source != other.source
        {
            return false;
        }
        let mut left: Vec<_> = self.destinations.iter().collect();
        let mut right: Vec<_> = other.destinations.iter().collect();
        left.sort_by(|a, b| a.id.cmp(&b.id));
        right.sort_by(|a, b| a.id.cmp(&b.id));
        left == right
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamDestination {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,

    #[serde(default = "one_path", skip_serializing_if = "is_one_path")]
    #[schemars(range(min = 1, max = 2))]
    pub paths: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<DeliveryPolicy>,
    #[serde(flatten)]
    pub endpoint: StreamTransport,
}

fn one_path() -> u8 {
    1
}

fn is_one_path(paths: &u8) -> bool {
    *paths == 1
}

impl Deref for StreamDestination {
    type Target = StreamTransport;

    fn deref(&self) -> &Self::Target {
        &self.endpoint
    }
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum StreamTransport {
    Srt(SrtEndpoint),

    Input(LocalEndpoint),

    Output(LocalEndpoint),

    Whip(SignallingEndpoint),

    Whep(SignallingEndpoint),
}

impl StreamTransport {
    #[must_use]
    pub fn node(&self) -> Option<&str> {
        match self {
            Self::Srt(endpoint) => endpoint.node.as_deref(),
            Self::Input(endpoint) | Self::Output(endpoint) => Some(&endpoint.node),
            Self::Whip(endpoint) | Self::Whep(endpoint) => Some(&endpoint.node),
        }
    }

    #[must_use]
    pub fn network(&self) -> Option<&str> {
        match self {
            Self::Srt(endpoint) => endpoint.network.as_deref(),
            Self::Input(endpoint) | Self::Output(endpoint) => endpoint.network.as_deref(),
            Self::Whip(endpoint) | Self::Whep(endpoint) => endpoint.network.as_deref(),
        }
    }

    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Srt(_) => Transport::Srt.name(),
            Self::Input(_) => LocalEnd::Input.name(),
            Self::Output(_) => LocalEnd::Output.name(),
            Self::Whip(_) => Transport::Whip.name(),
            Self::Whep(_) => Transport::Whep.name(),
        }
    }

    #[must_use]
    pub fn format(&self) -> Option<&MediaFormat> {
        match self {
            Self::Srt(endpoint) => endpoint.format.as_ref(),
            Self::Whip(endpoint) | Self::Whep(endpoint) => endpoint.format.as_ref(),
            Self::Input(_) | Self::Output(_) => None,
        }
    }

    #[must_use]
    pub fn accepts(&self) -> Option<&FormatConstraint> {
        match self {
            Self::Srt(endpoint) => endpoint.accepts.as_ref(),
            Self::Whip(endpoint) | Self::Whep(endpoint) => endpoint.accepts.as_ref(),
            Self::Input(_) | Self::Output(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SignallingEndpoint {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub node: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<MediaFormat>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepts: Option<FormatConstraint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LocalEndpoint {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub node: String,

    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SrtEndpoint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub node: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<RemoteAddr>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(inner(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    ))]
    pub via: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        length(min = Passphrase::MIN_LEN, max = Passphrase::MAX_LEN),
        regex(pattern = Passphrase::PATTERN)
    )]
    pub passphrase: Option<Passphrase>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<MediaFormat>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepts: Option<FormatConstraint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RemoteAddr {
    pub host: String,
    pub port: u16,
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub network: String,
}

#[must_use]
pub fn stream_format_conflicts(stream: &StreamDefinition) -> Vec<media::FormatConflict> {
    let Some(format) = stream.source.format() else {
        return Vec::new();
    };

    stream
        .destinations
        .iter()
        .filter_map(|destination| {
            let mismatches = destination.endpoint.accepts()?.mismatches(format);
            (!mismatches.is_empty()).then_some(media::FormatConflict {
                destination: destination.id.clone(),
                mismatches,
            })
        })
        .collect()
}

pub const HOP_ID_PREFIX: &str = "weave-";

#[must_use]
pub fn is_managed_hop_id(name: &str) -> bool {
    name.starts_with(HOP_ID_PREFIX)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Path {
    pub stream: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub hops: Vec<DesiredHop>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DesiredHop {
    pub id: String,
    pub node_id: String,
    pub profile_id: String,
    pub role: HopRole,
    pub ingress: SocketSpec,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_ingress: Option<SocketSpec>,
    pub egresses: Vec<DesiredEgress>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracks: Option<Vec<Track>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<TimedHop>,
}

impl DesiredHop {
    pub fn revision(&self) -> String {
        use sha2::{Digest, Sha256};
        let mut value = serde_json::to_value(self).expect("desired hops serialize as JSON");
        value.sort_all_objects();
        let mut hash = Sha256::new();
        hash.update(b"open-weave hop\0");
        hash.update(serde_json::to_vec(&value).expect("JSON values serialize"));
        format!("{:x}", hash.finalize())
    }

    pub fn sockets(&self) -> impl Iterator<Item = &SocketSpec> {
        let Self {
            id: _,
            node_id: _,
            profile_id: _,
            role: _,
            ingress,
            merge_ingress,
            egresses,
            tracks: _,
            delivery: _,
        } = self;
        std::iter::once(ingress)
            .chain(merge_ingress)
            .chain(egresses.iter().map(|egress| &egress.socket))
    }

    pub fn sockets_mut(&mut self) -> impl Iterator<Item = &mut SocketSpec> {
        let Self {
            id: _,
            node_id: _,
            profile_id: _,
            role: _,
            ingress,
            merge_ingress,
            egresses,
            tracks: _,
            delivery: _,
        } = self;
        std::iter::once(ingress)
            .chain(merge_ingress)
            .chain(egresses.iter_mut().map(|egress| &mut egress.socket))
    }

    pub fn peer_ends(&self) -> impl Iterator<Item = (PeerRole, &PeerSocket)> {
        let answers = std::iter::once(&self.ingress)
            .chain(&self.merge_ingress)
            .map(|socket| (PeerRole::Answer, socket));
        let offers = self
            .egresses
            .iter()
            .map(|egress| (PeerRole::Offer, &egress.socket));
        answers
            .chain(offers)
            .filter_map(|(role, socket)| match socket {
                SocketSpec::Webrtc(peer) => Some((role, peer)),
                _ => None,
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DesiredEgress {
    pub branch_id: String,
    #[serde(flatten)]
    pub socket: SocketSpec,
}

impl Deref for DesiredEgress {
    type Target = SocketSpec;

    fn deref(&self) -> &Self::Target {
        &self.socket
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HopRole {
    Sender,
    Bridge,
    Receiver,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SocketSpec {
    Srt(SrtSocket),

    Whip(SignallingSocket),

    Whep(SignallingSocket),

    Rist(RistSocket),

    Webrtc(PeerSocket),

    Local(LocalSocket),
}

impl JsonSchema for SocketSpec {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "SocketSpec".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::SocketSpec").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        generator.subschema_for::<SocketRepr>()
    }
}

impl SocketSpec {
    #[must_use]
    pub fn input(id: impl Into<String>) -> Self {
        Self::Local(LocalSocket {
            end: LocalEnd::Input,
            id: id.into(),
        })
    }

    #[must_use]
    pub fn output(id: impl Into<String>) -> Self {
        Self::Local(LocalSocket {
            end: LocalEnd::Output,
            id: id.into(),
        })
    }

    #[must_use]
    pub fn srt_listen(port: u16, latency: u32) -> Self {
        Self::Srt(SrtSocket::Listen {
            port,
            params: SrtParams {
                latency: Some(latency),
                ..SrtParams::default()
            },
        })
    }

    #[must_use]
    pub fn srt_connect(host: impl Into<String>, port: u16, latency: u32) -> Self {
        Self::Srt(SrtSocket::Connect {
            host: host.into(),
            port,
            params: SrtParams {
                latency: Some(latency),
                ..SrtParams::default()
            },
        })
    }

    #[must_use]
    pub fn signalling(
        transport: SignallingTransport,
        role: SocketRole,
        base: &str,
        endpoint_id: &str,
    ) -> Self {
        let socket = SignallingSocket::new(role, base, endpoint_id);
        match transport {
            SignallingTransport::Whip => Self::Whip(socket),
            SignallingTransport::Whep => Self::Whep(socket),
        }
    }

    #[must_use]
    pub fn end_name(&self) -> &'static str {
        match self {
            Self::Srt(socket) => socket.role().name(),
            Self::Whip(socket) | Self::Whep(socket) => socket.role.name(),
            Self::Rist(socket) => socket.role().name(),
            Self::Webrtc(_) => "link",
            Self::Local(socket) => socket.end.name(),
        }
    }
}

impl std::fmt::Display for SocketSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Srt(_) => f.write_str(Transport::Srt.name()),
            Self::Whip(_) => f.write_str(Transport::Whip.name()),
            Self::Whep(_) => f.write_str(Transport::Whep.name()),
            Self::Rist(_) => f.write_str(Transport::Rist.name()),
            Self::Webrtc(_) => f.write_str(WEBRTC_TRANSPORT),
            Self::Local(socket) => write!(f, "{} {}", socket.end, socket.id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SrtSocket {
    Listen {
        port: u16,
        params: SrtParams,
    },
    Connect {
        host: String,
        port: u16,
        params: SrtParams,
    },
}

impl SrtSocket {
    #[must_use]
    pub fn role(&self) -> SocketRole {
        match self {
            Self::Listen { .. } => SocketRole::Listen,
            Self::Connect { .. } => SocketRole::Connect,
        }
    }

    #[must_use]
    pub fn port(&self) -> u16 {
        match self {
            Self::Listen { port, .. } | Self::Connect { port, .. } => *port,
        }
    }

    #[must_use]
    pub fn params(&self) -> &SrtParams {
        match self {
            Self::Listen { params, .. } | Self::Connect { params, .. } => params,
        }
    }

    pub fn params_mut(&mut self) -> &mut SrtParams {
        match self {
            Self::Listen { params, .. } | Self::Connect { params, .. } => params,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RistSocket {
    Listen { port: u16 },
    Connect { host: String, port: u16 },
}

impl RistSocket {
    #[must_use]
    pub fn role(&self) -> SocketRole {
        match self {
            Self::Listen { .. } => SocketRole::Listen,
            Self::Connect { .. } => SocketRole::Connect,
        }
    }

    #[must_use]
    pub fn port(&self) -> u16 {
        match self {
            Self::Listen { port } | Self::Connect { port, .. } => *port,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignallingSocket {
    pub role: SocketRole,
    pub url: String,

    pub endpoint_id: String,
}

impl SignallingSocket {
    #[must_use]
    pub fn new(role: SocketRole, base: &str, endpoint_id: impl Into<String>) -> Self {
        let endpoint_id = endpoint_id.into();
        let url = format!("{}/{endpoint_id}", base.trim_end_matches('/'));
        Self {
            role,
            url,
            endpoint_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalSocket {
    pub end: LocalEnd,
    pub id: String,
}

#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct SocketRepr {
    transport: SocketTransport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    role: Option<SocketRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    endpoint_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    params: Option<SrtParams>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    link: Option<PeerLinkId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    generation: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stun: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    turn: Option<Vec<TurnServer>>,
}

#[derive(Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum SocketTransport {
    Srt,
    Whip,
    Whep,
    Rist,
    Webrtc,
    Input,
    Output,
}

fn reject_extra_fields<const N: usize>(
    socket: &str,
    fields: [(&str, bool); N],
) -> Result<(), String> {
    match fields.into_iter().find(|(_, present)| *present) {
        Some((field, _)) => Err(format!("a {socket} socket carries no {field}")),
        None => Ok(()),
    }
}

fn link_role(socket: &str, role: Option<SocketRole>) -> Result<SocketRole, String> {
    role.ok_or_else(|| format!("a {socket} socket needs a role"))
}

impl From<&SocketSpec> for SocketRepr {
    fn from(spec: &SocketSpec) -> Self {
        let bare = |transport, role| Self {
            transport,
            role,
            id: None,
            host: None,
            port: None,
            url: None,
            endpoint_id: None,
            params: None,
            link: None,
            generation: None,
            stun: None,
            turn: None,
        };
        match spec {
            SocketSpec::Srt(SrtSocket::Listen { port, params }) => Self {
                port: Some(*port),
                params: Some(params.clone()),
                ..bare(SocketTransport::Srt, Some(SocketRole::Listen))
            },
            SocketSpec::Srt(SrtSocket::Connect { host, port, params }) => Self {
                host: Some(host.clone()),
                port: Some(*port),
                params: Some(params.clone()),
                ..bare(SocketTransport::Srt, Some(SocketRole::Connect))
            },
            SocketSpec::Whip(socket) => Self {
                url: Some(socket.url.clone()),
                endpoint_id: Some(socket.endpoint_id.clone()),
                ..bare(SocketTransport::Whip, Some(socket.role))
            },
            SocketSpec::Whep(socket) => Self {
                url: Some(socket.url.clone()),
                endpoint_id: Some(socket.endpoint_id.clone()),
                ..bare(SocketTransport::Whep, Some(socket.role))
            },
            SocketSpec::Rist(RistSocket::Listen { port }) => Self {
                port: Some(*port),
                ..bare(SocketTransport::Rist, Some(SocketRole::Listen))
            },
            SocketSpec::Rist(RistSocket::Connect { host, port }) => Self {
                host: Some(host.clone()),
                port: Some(*port),
                ..bare(SocketTransport::Rist, Some(SocketRole::Connect))
            },
            SocketSpec::Webrtc(socket) => Self {
                link: Some(socket.attempt.link.clone()),
                generation: Some(socket.attempt.generation),
                stun: Some(socket.stun.clone()),
                turn: Some(socket.turn.clone()),
                ..bare(SocketTransport::Webrtc, None)
            },
            SocketSpec::Local(socket) => Self {
                id: Some(socket.id.clone()),
                ..bare(
                    match socket.end {
                        LocalEnd::Input => SocketTransport::Input,
                        LocalEnd::Output => SocketTransport::Output,
                    },
                    None,
                )
            },
        }
    }
}

impl TryFrom<SocketRepr> for SocketSpec {
    type Error = String;

    fn try_from(repr: SocketRepr) -> Result<Self, Self::Error> {
        let SocketRepr {
            transport,
            role,
            id,
            host,
            port,
            url,
            endpoint_id,
            params,
            link,
            generation,
            stun,
            turn,
        } = repr;
        let webrtc_fields = [
            ("link", link.is_some()),
            ("generation", generation.is_some()),
            ("stun", stun.is_some()),
            ("turn", turn.is_some()),
        ];

        match transport {
            SocketTransport::Srt => {
                reject_extra_fields(
                    "srt",
                    [
                        ("id", id.is_some()),
                        ("url", url.is_some()),
                        ("endpoint_id", endpoint_id.is_some()),
                    ],
                )?;
                reject_extra_fields("srt", webrtc_fields)?;
                let port = port.ok_or_else(|| "an srt socket needs a port".to_string())?;
                let params = params.unwrap_or_default();
                match link_role("srt", role)? {
                    SocketRole::Listen => {
                        reject_extra_fields("listening srt", [("host", host.is_some())])?;
                        Ok(Self::Srt(SrtSocket::Listen { port, params }))
                    }
                    SocketRole::Connect => {
                        let host =
                            host.ok_or_else(|| "a calling srt socket needs a host".to_string())?;
                        Ok(Self::Srt(SrtSocket::Connect { host, port, params }))
                    }
                }
            }
            SocketTransport::Whip | SocketTransport::Whep => {
                let whip = matches!(transport, SocketTransport::Whip);
                let name = if whip {
                    Transport::Whip
                } else {
                    Transport::Whep
                }
                .name();
                reject_extra_fields(
                    name,
                    [
                        ("id", id.is_some()),
                        ("host", host.is_some()),
                        ("port", port.is_some()),
                        ("params", params.is_some()),
                    ],
                )?;
                reject_extra_fields(name, webrtc_fields)?;
                let socket = SignallingSocket {
                    role: link_role(name, role)?,
                    url: url.ok_or_else(|| format!("a {name} socket needs a url"))?,
                    endpoint_id: endpoint_id
                        .ok_or_else(|| format!("a {name} socket needs an endpoint_id"))?,
                };
                Ok(if whip {
                    Self::Whip(socket)
                } else {
                    Self::Whep(socket)
                })
            }
            SocketTransport::Rist => {
                reject_extra_fields(
                    "rist",
                    [
                        ("id", id.is_some()),
                        ("url", url.is_some()),
                        ("endpoint_id", endpoint_id.is_some()),
                        ("params", params.is_some()),
                    ],
                )?;
                reject_extra_fields("rist", webrtc_fields)?;
                let port = port.ok_or_else(|| "a rist socket needs a port".to_string())?;
                match link_role("rist", role)? {
                    SocketRole::Listen => {
                        reject_extra_fields("listening rist", [("host", host.is_some())])?;
                        Ok(Self::Rist(RistSocket::Listen { port }))
                    }
                    SocketRole::Connect => {
                        let host =
                            host.ok_or_else(|| "a sending rist socket needs a host".to_string())?;
                        Ok(Self::Rist(RistSocket::Connect { host, port }))
                    }
                }
            }
            SocketTransport::Input | SocketTransport::Output => {
                let end = if matches!(transport, SocketTransport::Input) {
                    LocalEnd::Input
                } else {
                    LocalEnd::Output
                };
                reject_extra_fields(
                    end.name(),
                    [
                        ("role", role.is_some()),
                        ("host", host.is_some()),
                        ("port", port.is_some()),
                        ("url", url.is_some()),
                        ("endpoint_id", endpoint_id.is_some()),
                        ("params", params.is_some()),
                    ],
                )?;
                reject_extra_fields(end.name(), webrtc_fields)?;
                let id = id.ok_or_else(|| format!("an {} socket needs an id", end.name()))?;
                Ok(Self::Local(LocalSocket { end, id }))
            }
            SocketTransport::Webrtc => {
                reject_extra_fields(
                    WEBRTC_TRANSPORT,
                    [
                        ("role", role.is_some()),
                        ("id", id.is_some()),
                        ("host", host.is_some()),
                        ("port", port.is_some()),
                        ("url", url.is_some()),
                        ("endpoint_id", endpoint_id.is_some()),
                        ("params", params.is_some()),
                    ],
                )?;
                Ok(Self::Webrtc(PeerSocket {
                    attempt: PeerAttempt {
                        link: link.ok_or_else(|| "a webrtc socket needs a link".to_string())?,
                        generation: generation
                            .ok_or_else(|| "a webrtc socket needs a generation".to_string())?,
                    },
                    stun: stun.unwrap_or_default(),
                    turn: turn.unwrap_or_default(),
                }))
            }
        }
    }
}

impl Serialize for SocketSpec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        SocketRepr::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SocketSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::try_from(SocketRepr::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SocketRole {
    Listen,
    Connect,
}

impl SocketRole {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Listen => "listen",
            Self::Connect => "connect",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    Srt,

    Whip,

    Whep,

    Rist,
}

impl Transport {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Srt => "srt",
            Self::Whip => "whip",
            Self::Whep => "whep",
            Self::Rist => "rist",
        }
    }

    #[must_use]
    pub fn signalling(self) -> Option<SignallingTransport> {
        match self {
            Self::Srt | Self::Rist => None,
            Self::Whip => Some(SignallingTransport::Whip),
            Self::Whep => Some(SignallingTransport::Whep),
        }
    }
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SignallingTransport {
    Whip,
    Whep,
}

impl SignallingTransport {
    #[must_use]
    pub fn transport(self) -> Transport {
        match self {
            Self::Whip => Transport::Whip,
            Self::Whep => Transport::Whep,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LocalEnd {
    Input,
    Output,
}

pub const INPUT_TRANSPORT: &str = "input";
pub const OUTPUT_TRANSPORT: &str = "output";

impl LocalEnd {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Input => INPUT_TRANSPORT,
            Self::Output => OUTPUT_TRANSPORT,
        }
    }
}

impl std::fmt::Display for LocalEnd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct SrtParams {
    #[serde(default, skip_serializing_if = "PayloadKind::is_raw")]
    pub payload: PayloadKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency: Option<u32>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        length(min = Passphrase::MIN_LEN, max = Passphrase::MAX_LEN),
        regex(pattern = Passphrase::PATTERN)
    )]
    pub passphrase: Option<Passphrase>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("enum" = [16, 24, 32]))]
    pub pbkeylen: Option<u8>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct Passphrase(String);

impl Passphrase {
    pub const MIN_LEN: usize = 10;

    pub const MAX_LEN: usize = 80;

    pub const PATTERN: &str = "^[ -~]*$";

    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Passphrase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Passphrase(<redacted>)")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HopStatus {
    pub id: String,
    pub node_id: String,
    pub revision: String,
    pub state: HopState,
    pub ingress: SocketStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_ingress: Option<SocketStatus>,
    #[serde(default)]
    pub egresses: Vec<EgressStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<DeliveryReport>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SocketStatus {
    #[serde(default)]
    pub condition: LinkCondition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved: Option<ResolvedAddr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<LinkStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer: Option<PeerReport>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EgressStatus {
    pub branch_id: String,
    #[serde(flatten)]
    pub status: SocketStatus,
}

impl HopStatus {
    #[must_use]
    pub fn conditions(&self, desired: &DesiredHop) -> Option<HopConditions> {
        if self.revision != desired.revision() {
            return None;
        }
        let merge_ingress = match (&desired.merge_ingress, &self.merge_ingress) {
            (Some(_), Some(status)) => Some(status.condition),
            (None, None) => None,
            _ => return None,
        };
        let mut observed = BTreeMap::new();
        for egress in &self.egresses {
            if observed
                .insert(&egress.branch_id, egress.status.condition)
                .is_some()
            {
                return None;
            }
        }

        let egresses = desired
            .egresses
            .iter()
            .map(|egress| observed.remove(&egress.branch_id))
            .collect::<Option<Vec<_>>>()?;
        if !observed.is_empty() {
            return None;
        }

        Some(HopConditions {
            state: self.state,
            ingress: self.ingress.condition,
            merge_ingress,
            egresses,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HopState {
    Pending,
    Provisioned,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum LinkCondition {
    #[default]
    Idle,

    Connecting,

    Connected,

    Flowing,

    Stalled,

    Unreachable,

    Lost,
}

impl LinkCondition {
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Unreachable | Self::Lost)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HopConditions {
    pub state: HopState,
    pub ingress: LinkCondition,
    pub merge_ingress: Option<LinkCondition>,
    pub egresses: Vec<LinkCondition>,
}

impl HopConditions {
    fn ingresses(&self) -> impl Iterator<Item = LinkCondition> + '_ {
        std::iter::once(self.ingress).chain(self.merge_ingress)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PathStatus {
    Idle,

    Failed,

    Pending,

    AwaitingInput,

    Degraded,

    Flowing,
}

#[must_use]
pub fn roll_up_path(enabled: bool, hops: &[Option<HopConditions>]) -> PathStatus {
    if !enabled {
        return PathStatus::Idle;
    }
    if hops.iter().flatten().any(|h| {
        h.state == HopState::Failed
            || h.ingresses().any(LinkCondition::is_terminal)
            || h.egresses.iter().copied().any(LinkCondition::is_terminal)
    }) {
        return PathStatus::Failed;
    }
    if hops
        .iter()
        .any(|hop| hop.as_ref().is_none_or(|h| h.state == HopState::Pending))
    {
        return PathStatus::Pending;
    }
    if hops.iter().flatten().any(|h| {
        h.ingresses()
            .any(|condition| condition == LinkCondition::Stalled)
            || h.egresses.contains(&LinkCondition::Stalled)
    }) {
        return PathStatus::Degraded;
    }

    let source_flowing = hops.first().and_then(|hop| hop.as_ref().map(|h| h.ingress))
        == Some(LinkCondition::Flowing);
    if !source_flowing {
        return PathStatus::AwaitingInput;
    }

    let all_flowing = hops.iter().flatten().all(|h| {
        h.ingresses()
            .all(|condition| condition == LinkCondition::Flowing)
            && h.egresses
                .iter()
                .all(|condition| *condition == LinkCondition::Flowing)
    });
    if all_flowing {
        PathStatus::Flowing
    } else {
        PathStatus::Degraded
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResolvedAddr {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct LinkStats {
    #[serde(default)]
    pub connections: usize,
    #[serde(default)]
    pub rate_mbps: f64,
    #[serde(default)]
    pub packets_sent_lost: i64,
    #[serde(default)]
    pub packets_retransmitted: i64,
    #[serde(default)]
    pub packets_received_lost: i64,
    #[serde(default)]
    pub packets_received_retransmitted: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeDescriptor<T = NodeTopology> {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    pub endpoint: String,
    pub status: NodeStatus,
    #[serde(default)]
    pub capabilities: NodeCapabilities,
    pub topology: T,
    #[serde(default)]
    pub inputs: Vec<NodeInput>,
    #[serde(default)]
    pub outputs: Vec<NodeOutput>,
}

pub type DeclaredNode = NodeDescriptor<DeclaredTopology>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeInput {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    pub label: String,
    #[schemars(length(min = 1))]
    pub tracks: Vec<Track>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeOutput {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    pub label: String,
}

impl<T> NodeDescriptor<T> {
    #[must_use]
    pub fn with_topology<U>(self, topology: U) -> NodeDescriptor<U> {
        NodeDescriptor {
            id: self.id,
            endpoint: self.endpoint,
            status: self.status,
            capabilities: self.capabilities,
            topology,
            inputs: self.inputs,
            outputs: self.outputs,
        }
    }

    #[must_use]
    pub fn local_ids(&self, end: LocalEnd) -> Vec<&str> {
        match end {
            LocalEnd::Input => self.inputs.iter().map(|input| input.id.as_str()).collect(),
            LocalEnd::Output => self
                .outputs
                .iter()
                .map(|output| output.id.as_str())
                .collect(),
        }
    }

    #[must_use]
    pub fn input(&self, id: &str) -> Option<&NodeInput> {
        self.inputs.iter().find(|input| input.id == id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct NodeCapabilities {
    #[serde(default)]
    pub adapters: Vec<AdapterDescriptor>,
    #[serde(default)]
    pub hop_profiles: Vec<HopProfile>,
}

impl NodeCapabilities {
    #[must_use]
    pub fn offers_ingress(&self, transport: Transport, role: SocketRole) -> bool {
        self.hop_profiles
            .iter()
            .any(|profile| profile.ingress.offers_transport(transport, role))
    }

    #[must_use]
    pub fn offers_egress(&self, transport: Transport, role: SocketRole) -> bool {
        self.hop_profiles
            .iter()
            .any(|profile| profile.egress.offers_transport(transport, role))
    }

    #[must_use]
    pub fn offers_webrtc_ingress(&self) -> bool {
        self.hop_profiles
            .iter()
            .any(|profile| profile.ingress.offers_webrtc())
    }

    #[must_use]
    pub fn offers_webrtc_egress(&self) -> bool {
        self.hop_profiles
            .iter()
            .any(|profile| profile.egress.offers_webrtc())
    }

    #[must_use]
    pub fn offers_local(&self, end: LocalEnd) -> bool {
        self.hop_profiles.iter().any(|profile| {
            let class = match end {
                LocalEnd::Input => &profile.ingress,
                LocalEnd::Output => &profile.egress,
            };
            *class == HopEndpointClass::Local(end)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HopProfile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timed_delivery: Option<DeliveryCapability>,
    #[serde(default)]
    pub media_behavior: MediaBehavior,
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    pub ingress: HopEndpointClass,
    pub egress: HopEndpointClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub max_egresses: Option<usize>,

    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub merge: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accepts: Option<FormatConstraint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum HopEndpointClass {
    Transport(TransportClass),
    Webrtc(WebrtcClass),
    Local(LocalEnd),
    AnyOf(Vec<LinkClass>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum LinkClass {
    Transport(TransportClass),
    Webrtc(WebrtcClass),
}

impl LinkClass {
    pub const WEBRTC: Self = Self::Webrtc(WebrtcClass {
        transport: WebrtcTransport::Webrtc,
    });
}

impl HopEndpointClass {
    pub const WEBRTC: Self = Self::Webrtc(WebrtcClass {
        transport: WebrtcTransport::Webrtc,
    });

    #[must_use]
    pub fn offers_transport(&self, transport: Transport, role: SocketRole) -> bool {
        match self {
            Self::Transport(class) => class.offers(transport, role),
            Self::AnyOf(classes) => classes.iter().any(|class| {
                matches!(class, LinkClass::Transport(class) if class.offers(transport, role))
            }),
            Self::Webrtc(_) | Self::Local(_) => false,
        }
    }

    #[must_use]
    pub fn offers_webrtc(&self) -> bool {
        match self {
            Self::Webrtc(_) => true,
            Self::AnyOf(classes) => classes
                .iter()
                .any(|class| matches!(class, LinkClass::Webrtc(_))),
            Self::Transport(_) | Self::Local(_) => false,
        }
    }

    #[must_use]
    pub fn matches_socket(&self, socket: &SocketSpec) -> bool {
        match self {
            Self::Transport(class) => class.matches_socket(socket),
            Self::Webrtc(_) => matches!(socket, SocketSpec::Webrtc(_)),
            Self::Local(end) => matches!(socket, SocketSpec::Local(socket) if socket.end == *end),
            Self::AnyOf(classes) => classes.iter().any(|class| match class {
                LinkClass::Transport(class) => class.matches_socket(socket),
                LinkClass::Webrtc(_) => matches!(socket, SocketSpec::Webrtc(_)),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TransportClass {
    pub transport: Transport,
    pub roles: RoleSet,
}

impl TransportClass {
    fn offers(&self, transport: Transport, role: SocketRole) -> bool {
        self.transport == transport && self.roles.contains(role)
    }

    fn matches_socket(&self, socket: &SocketSpec) -> bool {
        let (transport, role) = match socket {
            SocketSpec::Srt(socket) => (Transport::Srt, socket.role()),
            SocketSpec::Whip(socket) => (Transport::Whip, socket.role),
            SocketSpec::Whep(socket) => (Transport::Whep, socket.role),
            SocketSpec::Rist(socket) => (Transport::Rist, socket.role()),
            SocketSpec::Webrtc(_) | SocketSpec::Local(_) => return false,
        };
        self.offers(transport, role)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Track {
    Audio,
    Video,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct NodeTopology {
    #[serde(default)]
    pub attachments: Vec<NetworkAttachment>,
}

pub const SITE_NETWORK_PREFIX: &str = "site-";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(deny_unknown_fields)]
pub struct DeclaredTopology {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub site: Option<SiteAttachment>,
    #[serde(default)]
    pub attachments: Vec<NetworkAttachment>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SiteAttachment {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    #[serde(default)]
    pub listeners: SiteListeners,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SiteListeners {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub srt: Option<SiteListener>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SiteListener {
    pub port_range: PortRange,
}

impl DeclaredTopology {
    pub fn attachment_ids(&self) -> impl Iterator<Item = &str> {
        self.site.iter().map(|site| site.id.as_str()).chain(
            self.attachments
                .iter()
                .map(|attachment| attachment.id.as_str()),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeObservation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uplink: Option<Uplink>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Uplink {
    pub interface: String,
    pub address: Ipv4Addr,
    #[schemars(range(max = 32))]
    pub prefix_len: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway: Option<HardwareAddress>,
    #[serde(default)]
    pub reflexive: Vec<ReflexiveAddress>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReflexiveAddress {
    pub server: SocketAddrV4,
    pub mapped: SocketAddrV4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HardwareAddress(pub [u8; 6]);

impl std::fmt::Display for HardwareAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let [a, b, c, d, e, g] = self.0;
        write!(f, "{a:02x}:{b:02x}:{c:02x}:{d:02x}:{e:02x}:{g:02x}")
    }
}

impl std::str::FromStr for HardwareAddress {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut bytes = [0_u8; 6];
        let mut parts = text.split(':');
        for byte in &mut bytes {
            let part = parts
                .next()
                .filter(|part| part.len() == 2)
                .ok_or_else(|| format!("{text:?} is not six colon-separated hex bytes"))?;
            *byte = u8::from_str_radix(part, 16).map_err(|error| format!("{text:?}: {error}"))?;
        }
        if parts.next().is_some() {
            return Err(format!("{text:?} is not six colon-separated hex bytes"));
        }
        Ok(Self(bytes))
    }
}

impl Serialize for HardwareAddress {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for HardwareAddress {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for HardwareAddress {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "HardwareAddress".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::HardwareAddress").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "string",
            "pattern": "^[0-9a-f]{2}(:[0-9a-f]{2}){5}$"
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Subnet {
    address: Ipv4Addr,
    prefix_len: u8,
}

impl Subnet {
    #[must_use]
    pub fn containing(address: Ipv4Addr, prefix_len: u8) -> Option<Self> {
        let host_bits = 32_u32.checked_sub(u32::from(prefix_len))?;
        let mask = u32::MAX.checked_shl(host_bits).unwrap_or(0);
        Some(Self {
            address: Ipv4Addr::from(u32::from(address) & mask),
            prefix_len,
        })
    }
}

impl std::fmt::Display for Subnet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.address, self.prefix_len)
    }
}

impl std::str::FromStr for Subnet {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || format!("{text:?} is not an IPv4 subnet such as 192.168.3.0/24");
        let (address, prefix_len) = text.split_once('/').ok_or_else(invalid)?;
        let address: Ipv4Addr = address.parse().map_err(|_| invalid())?;
        let prefix_len: u8 = prefix_len.parse().map_err(|_| invalid())?;
        let subnet = Self::containing(address, prefix_len).ok_or_else(invalid)?;
        if subnet.address != address {
            return Err(format!(
                "{text:?} has host bits set; the subnet is {subnet}"
            ));
        }
        Ok(subnet)
    }
}

impl Serialize for Subnet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Subnet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for Subnet {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Subnet".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Subnet").into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "string",
            "pattern": r"^(\d{1,3}\.){3}\d{1,3}/(3[0-2]|[12]?\d)$"
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NetworkAttachment {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub network: String,
    #[serde(default)]
    pub dial: bool,
    #[serde(default)]
    pub listeners: NetworkListeners,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NetworkListeners {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub srt: Option<SrtListener>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub whip: Option<SignallingListener>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub whep: Option<SignallingListener>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rist: Option<RistListener>,
}

impl NetworkListeners {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.srt.is_none() && self.whip.is_none() && self.whep.is_none() && self.rist.is_none()
    }

    #[must_use]
    pub fn signalling(&self, transport: SignallingTransport) -> Option<&str> {
        match transport {
            SignallingTransport::Whip => self.whip.as_ref().map(|v| v.base_url.as_str()),
            SignallingTransport::Whep => self.whep.as_ref().map(|v| v.base_url.as_str()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SrtListener {
    pub host: String,
    pub port_range: PortRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RistListener {
    pub host: String,
    pub port_range: PortRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SignallingListener {
    pub base_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PortRange {
    pub start: u16,
    pub end: u16,
}

impl PortRange {
    #[must_use]
    pub fn span(self) -> u16 {
        self.end.saturating_sub(self.start)
    }

    pub fn rist_pairs(self) -> impl Iterator<Item = u16> {
        let first = self.start.saturating_add(self.start % 2);
        (first..self.end).step_by(2)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    pub southbound_url: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub southbound_token: Option<String>,
    pub listen: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_endpoint: Option<String>,
    pub topology: DeclaredTopology,
}

impl NodeConfig {
    #[must_use]
    pub fn public_endpoint(&self) -> String {
        self.public_endpoint
            .clone()
            .unwrap_or_else(|| format!("http://{}", self.listen))
    }

    pub fn resolve_southbound_token(&self) -> Result<Option<auth::Token>, auth::AuthError> {
        if auth::auth_disabled() {
            return Ok(None);
        }
        self.southbound_token
            .as_deref()
            .and_then(auth::Token::new)
            .or_else(|| auth::Token::from_env(auth::SOUTHBOUND_TOKEN_VAR))
            .map(Some)
            .ok_or_else(|| auth::AuthError::MissingToken {
                var: auth::SOUTHBOUND_TOKEN_VAR.to_string(),
            })
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_resource_id(&self.id).map_err(ConfigError::InvalidId)?;
        let mut ids = std::collections::HashSet::new();
        for id in self.topology.attachment_ids() {
            validate_resource_id(id).map_err(ConfigError::InvalidAttachmentId)?;
            if !ids.insert(id) {
                return Err(ConfigError::DuplicateAttachment(id.to_string()));
            }
        }
        if let Some(listener) = self
            .topology
            .site
            .as_ref()
            .and_then(|site| site.listeners.srt.as_ref())
            && listener.port_range.start > listener.port_range.end
        {
            return Err(ConfigError::InvalidPortRange {
                start: listener.port_range.start,
                end: listener.port_range.end,
            });
        }
        for attachment in &self.topology.attachments {
            validate_resource_id(&attachment.network).map_err(ConfigError::InvalidNetworkId)?;
            if attachment.network.starts_with(SITE_NETWORK_PREFIX) {
                return Err(ConfigError::ReservedNetwork(attachment.network.clone()));
            }
            let port_listeners = attachment
                .listeners
                .srt
                .iter()
                .map(|listener| (&listener.host, listener.port_range))
                .chain(
                    attachment
                        .listeners
                        .rist
                        .iter()
                        .map(|listener| (&listener.host, listener.port_range)),
                );
            for (host, range) in port_listeners {
                if host.trim().is_empty() {
                    return Err(ConfigError::BlankListener);
                }
                if range.start > range.end {
                    return Err(ConfigError::InvalidPortRange {
                        start: range.start,
                        end: range.end,
                    });
                }
            }
            if let Some(listener) = &attachment.listeners.rist
                && listener.port_range.rist_pairs().next().is_none()
            {
                return Err(ConfigError::NoRistPortPair {
                    start: listener.port_range.start,
                    end: listener.port_range.end,
                });
            }
            if attachment
                .listeners
                .whip
                .as_ref()
                .into_iter()
                .chain(attachment.listeners.whep.as_ref())
                .any(|listener| listener.base_url.trim().is_empty())
            {
                return Err(ConfigError::BlankListener);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("node id {0}")]
    InvalidId(ResourceIdError),
    #[error("attachment id {0}")]
    InvalidAttachmentId(ResourceIdError),
    #[error("network id {0}")]
    InvalidNetworkId(ResourceIdError),
    #[error(
        "network {0} starts with the reserved prefix {SITE_NETWORK_PREFIX}; rename it, or declare topology.site for the controller to derive the site network"
    )]
    ReservedNetwork(String),
    #[error("duplicate attachment id {0}")]
    DuplicateAttachment(String),
    #[error("listener host or base_url must not be blank")]
    BlankListener,
    #[error("port_range start {start} exceeds end {end}")]
    InvalidPortRange { start: u16, end: u16 },
    #[error("rist port_range {start}..={end} holds no even port with the port after it")]
    NoRistPortPair { start: u16, end: u16 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AdapterDescriptor {
    pub name: String,
    pub kind: AdapterKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AdapterKind {
    NativeSrt,
    Strom,
    Spaghetti,
    Nmos,
    MxlDomain,
    MxlK8s,
    Mcm,
    Vendor,
    Custom,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleSet {
    listen: bool,
    connect: bool,
}

impl JsonSchema for RoleSet {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "RoleSet".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::RoleSet").into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let mut schema = generator.subschema_for::<Vec<SocketRole>>();
        schema.insert("minItems".to_string(), 1.into());
        schema
    }
}

impl RoleSet {
    #[must_use]
    pub fn both() -> Self {
        Self {
            listen: true,
            connect: true,
        }
    }

    #[must_use]
    pub fn only(role: SocketRole) -> Self {
        Self {
            listen: matches!(role, SocketRole::Listen),
            connect: matches!(role, SocketRole::Connect),
        }
    }

    #[must_use]
    pub fn contains(self, role: SocketRole) -> bool {
        match role {
            SocketRole::Listen => self.listen,
            SocketRole::Connect => self.connect,
        }
    }

    fn roles(self) -> impl Iterator<Item = SocketRole> {
        [
            self.listen.then_some(SocketRole::Listen),
            self.connect.then_some(SocketRole::Connect),
        ]
        .into_iter()
        .flatten()
    }
}

impl Serialize for RoleSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.roles())
    }
}

impl<'de> Deserialize<'de> for RoleSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut set = Self {
            listen: false,
            connect: false,
        };
        for role in Vec::<SocketRole>::deserialize(deserializer)? {
            match role {
                SocketRole::Listen => set.listen = true,
                SocketRole::Connect => set.connect = true,
            }
        }
        if !set.listen && !set.connect {
            return Err(serde::de::Error::custom("a transport offered in no role"));
        }
        Ok(set)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EndpointDescriptor {
    pub id: String,
    pub label: String,
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub node_id: Option<String>,
    pub kind: EndpointKind,

    #[serde(default)]
    pub transports: Vec<String>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EndpointKind {
    Source,
    Destination,
    Bidirectional,
    Flow,
    Gateway,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeRegistration {
    #[serde(default)]
    pub protocol_version: u32,
    pub node: DeclaredNode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation: Option<NodeObservation>,
    #[serde(default)]
    pub endpoints: Vec<EndpointDescriptor>,
    #[serde(default)]
    pub hop_status: Vec<HopStatus>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NodeHeartbeat {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub node_id: String,
    pub status: NodeStatus,
    pub inputs: Vec<NodeInput>,
    pub outputs: Vec<NodeOutput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observation: Option<NodeObservation>,
    #[serde(default)]
    pub endpoints: Vec<EndpointDescriptor>,
    #[serde(default)]
    pub hop_status: Vec<HopStatus>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObservedState {
    #[serde(default)]
    pub nodes: Vec<NodeDescriptor>,
    #[serde(default)]
    pub endpoints: Vec<EndpointDescriptor>,
    #[serde(default)]
    pub hops: Vec<HopStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReconcileReport {
    pub status: ReconcileStatus,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StreamEndpoints {
    pub ingress: Option<EndpointAddr>,
    pub destinations: Vec<DestinationEndpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DestinationEndpoint {
    #[schemars(
        length(min = 1, max = 63),
        regex(pattern = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    )]
    pub id: String,
    pub endpoint: Option<EndpointAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EndpointAddr {
    pub node: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,

    pub url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileStatus {
    Idle,
    Converging,
    Converged,
    Degraded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Unknown,
    Ready,
    Degraded,
    Offline,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_json_uses_snake_case_transport_tag_and_defaults_enabled() {
        let json = serde_json::json!({
            "name": "cam1-to-studio",
            "source": { "srt": { "node": "strom-node-1", "latency": 200 } },
            "destinations": [
                { "id": "studio", "srt": { "node": "strom-node-2", "network": "wan" } }
            ]
        });

        let stream: StreamDefinition = serde_json::from_value(json).expect("parse stream");

        assert!(stream.enabled, "enabled defaults to true when omitted");
        assert_eq!(
            stream.source,
            StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-1".to_string()),
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: None,
                latency: Some(200),
                passphrase: None,
            })
        );
        assert_eq!(stream.destinations[0].id, "studio");
        assert_eq!(
            stream.destinations[0].endpoint,
            StreamTransport::Srt(SrtEndpoint {
                node: Some("strom-node-2".to_string()),
                remote: None,
                via: Vec::new(),
                format: None,
                accepts: None,
                network: Some("wan".to_string()),
                latency: None,
                passphrase: None,
            })
        );

        let round_trip: StreamDefinition =
            serde_json::from_str(&serde_json::to_string(&stream).unwrap()).unwrap();
        assert_eq!(stream, round_trip);
        assert_eq!(
            serde_json::to_value(&stream).unwrap()["source"]
                .as_object()
                .unwrap()
                .keys()
                .next()
                .unwrap(),
            "srt"
        );
    }

    fn sample_hop() -> DesiredHop {
        DesiredHop {
            delivery: None,
            id: "weave-contribution-sender".to_string(),
            node_id: "strom-node-1".to_string(),
            profile_id: "srt-forward".to_string(),
            role: HopRole::Sender,
            ingress: SocketSpec::srt_listen(7001, 200),
            merge_ingress: None,
            egresses: vec![DesiredEgress {
                branch_id: "studio".to_string(),
                socket: SocketSpec::srt_connect("172.31.0.10", 7002, 1000),
            }],
            tracks: None,
        }
    }

    #[test]
    fn path_round_trips_and_defaults_enabled_and_hops() {
        let path = Path {
            stream: "contribution".to_string(),
            enabled: true,
            hops: vec![sample_hop()],
        };
        let round_trip: Path =
            serde_json::from_str(&serde_json::to_string(&path).unwrap()).unwrap();
        assert_eq!(path, round_trip);

        let minimal: Path = serde_json::from_value(serde_json::json!({
            "stream": "contribution"
        }))
        .expect("parse minimal path");
        assert!(minimal.enabled, "enabled defaults to true");
        assert!(minimal.hops.is_empty(), "hops defaults to empty");
    }

    #[test]
    fn hop_with_multiple_egresses_round_trips() {
        let mut hop = sample_hop();
        hop.egresses.push(DesiredEgress {
            branch_id: "preview".to_string(),
            socket: SocketSpec::srt_connect("172.31.0.10", 7003, 1000),
        });
        assert_eq!(hop.egresses.len(), 2);

        let value = serde_json::to_value(&hop).unwrap();
        assert_eq!(value["profile_id"], "srt-forward");
        assert_eq!(value["egresses"][1]["branch_id"], "preview");
        assert_eq!(value["egresses"][1]["transport"], "srt");
        let round_trip: DesiredHop = serde_json::from_value(value).unwrap();
        assert_eq!(hop, round_trip);
    }

    #[test]
    fn a_listening_srt_socket_carries_no_host() {
        let value = serde_json::to_value(SocketSpec::srt_listen(7001, 200)).unwrap();
        assert!(value.get("host").is_none(), "a listener has no host");
        assert_eq!(value["role"], "listen");
        assert_eq!(value["transport"], "srt");
    }

    #[test]
    fn hop_status_round_trips_with_optional_fields_absent() {
        let status = HopStatus {
            delivery: None,
            id: "weave-contribution-sender".to_string(),
            node_id: "strom-node-1".to_string(),
            revision: String::new(),
            state: HopState::Provisioned,
            ingress: SocketStatus {
                peer: None,
                condition: LinkCondition::Flowing,
                resolved: Some(ResolvedAddr {
                    host: "0.0.0.0".to_string(),
                    port: 7001,
                }),
                stats: Some(LinkStats {
                    connections: 1,
                    rate_mbps: 4.5,
                    ..LinkStats::default()
                }),
            },
            merge_ingress: None,
            egresses: vec![EgressStatus {
                branch_id: "studio".to_string(),
                status: SocketStatus {
                    peer: None,
                    condition: LinkCondition::Connected,
                    resolved: None,
                    stats: None,
                },
            }],
        };
        let round_trip: HopStatus =
            serde_json::from_str(&serde_json::to_string(&status).unwrap()).unwrap();
        assert_eq!(status, round_trip);
    }

    #[test]
    fn heartbeat_parses_without_hop_status_field() {
        let heartbeat: NodeHeartbeat = serde_json::from_value(serde_json::json!({
            "node_id": "strom-node-1",
            "status": "ready",
            "inputs": [],
            "outputs": []
        }))
        .expect("parse heartbeat");
        assert!(heartbeat.hop_status.is_empty());
    }

    #[test]
    fn a_heartbeat_must_say_what_inputs_and_outputs_the_node_has() {
        let missing: Result<NodeHeartbeat, _> = serde_json::from_value(serde_json::json!({
            "node_id": "strom-node-1",
            "status": "ready",
            "outputs": []
        }));
        assert!(
            missing.is_err(),
            "an absent inputs list would clear the node's inputs"
        );
    }

    #[test]
    fn registration_without_protocol_version_reads_as_incompatible_zero() {
        let registration: NodeRegistration = serde_json::from_value(serde_json::json!({
            "node": {
                "id": "strom-node-1",
                "endpoint": "http://strom-node-1:8091",
                "status": "ready",
                "topology": {}
            }
        }))
        .expect("parse pre-handshake registration");

        assert_eq!(registration.protocol_version, 0);
        assert!(
            !protocol_compatible(registration.protocol_version),
            "an adapter that declares no version is not compatible"
        );
    }

    #[test]
    fn protocol_compatible_accepts_only_the_supported_version() {
        assert!(protocol_compatible(PROTOCOL_VERSION));
        assert!(!protocol_compatible(PROTOCOL_VERSION + 1));
        assert!(!protocol_compatible(0));
    }

    #[test]
    fn is_managed_hop_id_matches_only_prefixed_names() {
        assert!(is_managed_hop_id("weave-contribution-sender"));
        assert!(!is_managed_hop_id("contribution"));
        assert!(!is_managed_hop_id("contribution-recv"));
    }

    fn hc(state: HopState, ingress: LinkCondition, egress: LinkCondition) -> Option<HopConditions> {
        Some(HopConditions {
            state,
            ingress,
            merge_ingress: None,
            egresses: vec![egress],
        })
    }

    #[test]
    fn rollup_disabled_is_idle() {
        let hops = [hc(
            HopState::Provisioned,
            LinkCondition::Flowing,
            LinkCondition::Flowing,
        )];
        assert_eq!(roll_up_path(false, &hops), PathStatus::Idle);
    }

    #[test]
    fn rollup_failed_hop_wins_over_flowing() {
        let hops = [
            hc(
                HopState::Provisioned,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ),
            hc(
                HopState::Failed,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ),
        ];
        assert_eq!(roll_up_path(true, &hops), PathStatus::Failed);
    }

    #[test]
    fn rollup_pending_or_missing_hop_is_pending() {
        let pending = [hc(
            HopState::Pending,
            LinkCondition::Idle,
            LinkCondition::Idle,
        )];
        assert_eq!(roll_up_path(true, &pending), PathStatus::Pending);

        let missing = [
            hc(
                HopState::Provisioned,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ),
            None,
        ];
        assert_eq!(roll_up_path(true, &missing), PathStatus::Pending);
    }

    #[test]
    fn rollup_source_not_receiving_is_awaiting_input() {
        let idle = [hc(
            HopState::Provisioned,
            LinkCondition::Idle,
            LinkCondition::Idle,
        )];
        assert_eq!(roll_up_path(true, &idle), PathStatus::AwaitingInput);

        let silent = [hc(
            HopState::Provisioned,
            LinkCondition::Connected,
            LinkCondition::Connected,
        )];
        assert_eq!(roll_up_path(true, &silent), PathStatus::AwaitingInput);
    }

    #[test]
    fn rollup_flowing_source_with_stalled_downstream_is_degraded() {
        let hops = [
            hc(
                HopState::Provisioned,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ),
            hc(
                HopState::Provisioned,
                LinkCondition::Connected,
                LinkCondition::Connected,
            ),
        ];
        assert_eq!(roll_up_path(true, &hops), PathStatus::Degraded);
    }

    #[test]
    fn rollup_stalled_source_is_degraded_not_awaiting_input() {
        let stalled_source = [hc(
            HopState::Provisioned,
            LinkCondition::Stalled,
            LinkCondition::Idle,
        )];
        assert_eq!(roll_up_path(true, &stalled_source), PathStatus::Degraded);
    }

    #[test]
    fn rollup_stalled_downstream_hop_is_degraded() {
        let hops = [
            hc(
                HopState::Provisioned,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ),
            hc(
                HopState::Provisioned,
                LinkCondition::Stalled,
                LinkCondition::Idle,
            ),
        ];
        assert_eq!(roll_up_path(true, &hops), PathStatus::Degraded);
    }

    #[test]
    fn rollup_all_flowing_is_flowing() {
        let hops = [
            hc(
                HopState::Provisioned,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ),
            hc(
                HopState::Provisioned,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ),
        ];
        assert_eq!(roll_up_path(true, &hops), PathStatus::Flowing);
    }

    #[test]
    fn rollup_checks_every_fanout_branch() {
        let hops = [Some(HopConditions {
            state: HopState::Provisioned,
            ingress: LinkCondition::Flowing,
            merge_ingress: None,
            egresses: vec![LinkCondition::Flowing, LinkCondition::Connecting],
        })];
        assert_eq!(roll_up_path(true, &hops), PathStatus::Degraded);
    }

    #[test]
    fn hop_status_conditions_match_desired_branches_by_id() {
        let mut desired = sample_hop();
        desired.egresses.push(DesiredEgress {
            branch_id: "preview".to_string(),
            socket: SocketSpec::srt_connect("172.31.0.11", 7003, 1000),
        });
        let status = HopStatus {
            delivery: None,
            id: "weave-a".to_string(),
            node_id: "n1".to_string(),
            revision: desired.revision(),
            state: HopState::Provisioned,
            ingress: SocketStatus {
                peer: None,
                condition: LinkCondition::Flowing,
                resolved: None,
                stats: None,
            },
            merge_ingress: None,
            egresses: vec![
                EgressStatus {
                    branch_id: "preview".to_string(),
                    status: SocketStatus {
                        peer: None,
                        condition: LinkCondition::Connected,
                        resolved: None,
                        stats: None,
                    },
                },
                EgressStatus {
                    branch_id: "studio".to_string(),
                    status: SocketStatus {
                        peer: None,
                        condition: LinkCondition::Flowing,
                        resolved: None,
                        stats: None,
                    },
                },
            ],
        };
        assert_eq!(
            status.conditions(&desired),
            Some(HopConditions {
                state: HopState::Provisioned,
                ingress: LinkCondition::Flowing,
                merge_ingress: None,
                egresses: vec![LinkCondition::Flowing, LinkCondition::Connected],
            })
        );
    }

    #[test]
    fn hop_status_conditions_reject_missing_duplicate_and_extra_branches() {
        let desired = sample_hop();
        let socket = SocketStatus {
            peer: None,
            condition: LinkCondition::Flowing,
            resolved: None,
            stats: None,
        };
        let mut status = HopStatus {
            delivery: None,
            id: desired.id.clone(),
            node_id: desired.node_id.clone(),
            revision: desired.revision(),
            state: HopState::Provisioned,
            ingress: socket.clone(),
            merge_ingress: None,
            egresses: Vec::new(),
        };
        assert_eq!(status.conditions(&desired), None);

        status.egresses = vec![
            EgressStatus {
                branch_id: "studio".to_string(),
                status: socket.clone(),
            },
            EgressStatus {
                branch_id: "studio".to_string(),
                status: socket.clone(),
            },
        ];
        assert_eq!(status.conditions(&desired), None);

        status.egresses = vec![EgressStatus {
            branch_id: "preview".to_string(),
            status: socket,
        }];
        assert_eq!(status.conditions(&desired), None);
    }

    #[test]
    fn srt_endpoint_serde_allows_node_or_remote_and_denies_unknown_fields() {
        let bare: SrtEndpoint =
            serde_json::from_value(serde_json::json!({ "network": "wan" })).expect("parse bare");
        assert_eq!(bare.node, None);
        assert_eq!(bare.remote, None);

        let via: SrtEndpoint = serde_json::from_value(serde_json::json!({
            "node": "strom-node-2",
            "via": ["edge-relay"]
        }))
        .expect("parse via");
        assert_eq!(via.via, vec!["edge-relay".to_string()]);
        assert!(
            serde_json::to_value(&bare).unwrap().get("via").is_none(),
            "an empty via is not serialized"
        );

        let remote: SrtEndpoint = serde_json::from_value(serde_json::json!({
            "remote": { "host": "198.51.100.5", "port": 9000, "network": "internet" }
        }))
        .expect("parse remote");
        assert_eq!(
            remote.remote,
            Some(RemoteAddr {
                host: "198.51.100.5".to_string(),
                port: 9000,
                network: "internet".to_string(),
            })
        );
        let round_trip: SrtEndpoint =
            serde_json::from_str(&serde_json::to_string(&remote).unwrap()).unwrap();
        assert_eq!(remote, round_trip);

        let bogus: Result<SrtEndpoint, _> =
            serde_json::from_value(serde_json::json!({ "node": "n", "bogus": true }));
        assert!(bogus.is_err(), "deny_unknown_fields rejects typos");
    }

    fn node_config() -> NodeConfig {
        NodeConfig {
            id: "strom-node-1".to_string(),
            southbound_url: "http://127.0.0.1:8081".to_string(),
            southbound_token: None,
            listen: "0.0.0.0:8091".to_string(),
            public_endpoint: None,
            topology: DeclaredTopology {
                site: None,
                attachments: vec![NetworkAttachment {
                    id: "lan".to_string(),
                    network: "studio-lan".to_string(),
                    dial: true,
                    listeners: NetworkListeners {
                        srt: Some(SrtListener {
                            host: "172.26.0.10".to_string(),
                            port_range: PortRange {
                                start: 20000,
                                end: 20999,
                            },
                        }),
                        whip: None,
                        whep: None,
                        rist: None,
                    },
                }],
            },
        }
    }

    #[test]
    fn node_config_public_endpoint_defaults_to_listen() {
        let config = node_config();
        assert_eq!(config.public_endpoint(), "http://0.0.0.0:8091");

        let mut pinned = node_config();
        pinned.public_endpoint = Some("http://172.25.0.21:8091".to_string());
        assert_eq!(pinned.public_endpoint(), "http://172.25.0.21:8091");
    }

    #[test]
    fn node_config_validate_accepts_valid_config() {
        assert_eq!(node_config().validate(), Ok(()));
    }

    #[test]
    fn node_config_validate_rejects_invariant_violations() {
        let mut empty_id = node_config();
        empty_id.id = "  ".to_string();
        assert_eq!(
            empty_id.validate(),
            Err(ConfigError::InvalidId(ResourceIdError::InvalidCharacters))
        );

        let mut long_id = node_config();
        long_id.id = "a".repeat(RESOURCE_ID_MAX_LEN + 1);
        assert_eq!(
            long_id.validate(),
            Err(ConfigError::InvalidId(ResourceIdError::InvalidLength))
        );

        let mut blank_host = node_config();
        blank_host.topology.attachments[0]
            .listeners
            .srt
            .as_mut()
            .unwrap()
            .host = String::new();
        assert_eq!(blank_host.validate(), Err(ConfigError::BlankListener));

        let mut bad_range = node_config();
        bad_range.topology.attachments[0]
            .listeners
            .srt
            .as_mut()
            .unwrap()
            .port_range = PortRange {
            start: 21000,
            end: 20000,
        };
        assert_eq!(
            bad_range.validate(),
            Err(ConfigError::InvalidPortRange {
                start: 21000,
                end: 20000,
            })
        );
    }

    #[test]
    fn a_site_attachment_shares_the_attachment_id_space() {
        let mut config = node_config();
        config.topology.site = Some(SiteAttachment {
            id: "lan".to_string(),
            listeners: SiteListeners::default(),
        });
        assert_eq!(
            config.validate(),
            Err(ConfigError::DuplicateAttachment("lan".to_string()))
        );
    }

    #[test]
    fn a_declared_network_cannot_take_the_site_prefix() {
        let mut config = node_config();
        config.topology.attachments[0].network = "site-3f9a2c1d7e4b".to_string();
        let error = config.validate().unwrap_err();
        assert_eq!(
            error,
            ConfigError::ReservedNetwork("site-3f9a2c1d7e4b".to_string())
        );
        assert!(
            error.to_string().contains("declare topology.site"),
            "{error}"
        );
    }

    #[test]
    fn a_subnet_is_written_as_its_network_address_and_prefix() {
        let subnet = Subnet::containing("192.168.3.20".parse().unwrap(), 24).unwrap();
        assert_eq!(subnet.to_string(), "192.168.3.0/24");
        assert_eq!("192.168.3.0/24".parse::<Subnet>(), Ok(subnet));
        assert_eq!(
            serde_json::to_value(subnet).unwrap(),
            serde_json::json!("192.168.3.0/24")
        );
        assert_eq!(
            Subnet::containing(Ipv4Addr::new(10, 1, 2, 3), 0)
                .unwrap()
                .to_string(),
            "0.0.0.0/0"
        );
        assert_eq!(Subnet::containing(Ipv4Addr::new(10, 1, 2, 3), 33), None);
        assert!("192.168.3.20/24".parse::<Subnet>().is_err());
        assert!("192.168.3.0/33".parse::<Subnet>().is_err());
        assert!("192.168.3.0".parse::<Subnet>().is_err());
    }

    #[test]
    fn a_hardware_address_round_trips_as_lowercase_hex() {
        let address: HardwareAddress = "A4:83:E7:0B:1C:2D".parse().unwrap();
        assert_eq!(address.to_string(), "a4:83:e7:0b:1c:2d");
        assert!("a4:83:e7:0b:1c".parse::<HardwareAddress>().is_err());
        assert!("a4:83:e7:0b:1c:2d:00".parse::<HardwareAddress>().is_err());
    }

    fn stream_with_formats(
        format: Option<MediaFormat>,
        accepts: Option<FormatConstraint>,
    ) -> StreamDefinition {
        let mut source = SrtEndpoint {
            node: Some("strom-node-1".to_string()),
            remote: None,
            via: Vec::new(),
            network: None,
            latency: None,
            passphrase: None,
            format: None,
            accepts: None,
        };
        let mut destination = source.clone();
        source.format = format;
        destination.node = Some("strom-node-2".to_string());
        destination.accepts = accepts;

        StreamDefinition {
            name: "formats".to_string(),
            enabled: true,
            format_policy: crate::FormatPolicy::Report,
            allow_cleartext_links: false,
            source: StreamTransport::Srt(source),
            destinations: vec![StreamDestination {
                delivery: None,
                id: "studio".to_string(),
                paths: 1,
                endpoint: StreamTransport::Srt(destination),
            }],
        }
    }

    fn aac_48k() -> MediaFormat {
        MediaFormat {
            container: Container::MpegTs,
            video: None,
            audio: Some(AudioFormat {
                codec: AudioCodec::Aac,
                sample_rate: 48_000,
                channels: 2,
            }),
        }
    }

    fn wants_44k() -> FormatConstraint {
        FormatConstraint {
            audio: Some(AudioConstraint {
                sample_rate: Some(vec![44_100]),
                ..AudioConstraint::default()
            }),
            ..FormatConstraint::default()
        }
    }

    #[test]
    fn a_destination_that_cannot_accept_the_source_format_is_reported() {
        let stream = stream_with_formats(Some(aac_48k()), Some(wants_44k()));
        let conflicts = stream_format_conflicts(&stream);

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].destination, "studio");
        assert_eq!(
            conflicts[0].to_string(),
            "destination studio cannot accept the source format: \
             audio.sample_rate is 48000 but accepts 44100"
        );
    }

    #[test]
    fn an_undeclared_format_or_constraint_conflicts_with_nothing() {
        assert!(stream_format_conflicts(&stream_with_formats(None, Some(wants_44k()))).is_empty());
        assert!(stream_format_conflicts(&stream_with_formats(Some(aac_48k()), None)).is_empty());
        assert!(stream_format_conflicts(&stream_with_formats(None, None)).is_empty());
    }

    #[test]
    fn only_the_destinations_that_conflict_are_reported() {
        let mut stream = stream_with_formats(Some(aac_48k()), None);
        let StreamTransport::Srt(base) = &stream.destinations[0].endpoint else {
            unreachable!("fixture destination is srt");
        };

        let mut fussy = base.clone();
        fussy.accepts = Some(wants_44k());
        let mut relaxed = base.clone();
        relaxed.accepts = Some(FormatConstraint::default());

        let destination = |id: &str, endpoint: &SrtEndpoint| StreamDestination {
            delivery: None,
            id: id.to_string(),
            paths: 1,
            endpoint: StreamTransport::Srt(endpoint.clone()),
        };
        stream.destinations = vec![
            destination("relaxed", &relaxed),
            destination("fussy-a", &fussy),
            destination("fussy-b", &fussy),
        ];

        let offenders: Vec<String> = stream_format_conflicts(&stream)
            .into_iter()
            .map(|c| c.destination)
            .collect();
        assert_eq!(offenders, vec!["fussy-a", "fussy-b"], "in manifest order");
    }

    #[test]
    fn a_whep_player_that_cannot_accept_a_whip_senders_format_is_reported() {
        let mut stream = stream_with_formats(None, None);
        stream.source = StreamTransport::Whip(SignallingEndpoint {
            node: "strom-node-1".to_string(),
            network: None,
            format: Some(aac_48k()),
            accepts: None,
        });
        stream.destinations[0].endpoint = StreamTransport::Whep(SignallingEndpoint {
            node: "strom-node-2".to_string(),
            network: None,
            format: None,
            accepts: Some(wants_44k()),
        });
        let conflicts = stream_format_conflicts(&stream);
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].destination, "studio");
    }

    #[test]
    fn network_attachment_rejects_a_misspelled_field() {
        let attachment: NetworkAttachment = serde_json::from_value(serde_json::json!({
            "id": "client",
            "network": "internet",
            "dial": true
        }))
        .expect("parse attachment");
        assert!(attachment.dial);
        assert!(attachment.listeners.is_empty());

        let result: Result<NetworkAttachment, _> = serde_json::from_value(serde_json::json!({
            "id": "client",
            "network": "internet",
            "dail": true
        }));
        assert!(result.is_err(), "a typo must not read as dial-less");
    }

    #[test]
    fn node_config_rejects_unknown_fields() {
        let mut value = serde_json::json!({
            "id": "n1",
            "southbound_url": "http://127.0.0.1:8081",
            "listen": "0.0.0.0:8091",
            "topology": { "attachments": [] }
        });
        assert!(serde_json::from_value::<NodeConfig>(value.clone()).is_ok());

        value["bogus"] = serde_json::json!(true);
        let result: Result<NodeConfig, _> = serde_json::from_value(value);
        assert!(result.is_err(), "deny_unknown_fields rejects typos");
    }

    #[test]
    fn input_and_output_endpoints_round_trip_and_deny_unknown_fields() {
        let json = serde_json::json!({
            "name": "alice-cam",
            "source": { "input": { "node": "browser-a1b2", "id": "camera" } },
            "destinations": [ { "id": "desk", "output": { "node": "filips-mac", "id": "speakers" } } ]
        });
        let stream: StreamDefinition = serde_json::from_value(json.clone()).expect("parse stream");
        assert_eq!(
            stream.source,
            StreamTransport::Input(LocalEndpoint {
                node: "browser-a1b2".to_string(),
                id: "camera".to_string(),
                network: None,
            })
        );
        assert_eq!(
            stream.destinations[0].endpoint,
            StreamTransport::Output(LocalEndpoint {
                node: "filips-mac".to_string(),
                id: "speakers".to_string(),
                network: None,
            })
        );
        assert_eq!(stream.source.node(), Some("browser-a1b2"));
        assert_eq!(stream.source.kind(), "input");
        assert_eq!(stream.destinations[0].kind(), "output");
        assert_eq!(
            serde_json::to_value(&stream.source).unwrap(),
            json["source"]
        );

        let round_trip: StreamDefinition =
            serde_json::from_str(&serde_json::to_string(&stream).unwrap()).unwrap();
        assert_eq!(stream, round_trip);

        let with_latency: Result<StreamTransport, _> = serde_json::from_value(serde_json::json!({
            "input": { "node": "browser-a1b2", "id": "camera", "latency": 200 }
        }));
        assert!(with_latency.is_err(), "an input endpoint has no SRT fields");
        let unnamed: Result<StreamTransport, _> = serde_json::from_value(serde_json::json!({
            "output": { "node": "filips-mac" }
        }));
        assert!(unnamed.is_err(), "an output endpoint names its output");
    }

    #[test]
    fn cleartext_links_are_off_unless_a_stream_allows_them() {
        let mut json = serde_json::json!({
            "name": "feed",
            "source": { "srt": { "node": "strom-node-1" } },
            "destinations": [ { "id": "studio", "srt": { "node": "strom-node-2" } } ]
        });
        let stream: StreamDefinition = serde_json::from_value(json.clone()).unwrap();
        assert!(!stream.allow_cleartext_links);
        assert!(
            serde_json::to_value(&stream).unwrap()["allow_cleartext_links"].is_null(),
            "omitted when false"
        );

        json["allow_cleartext_links"] = serde_json::json!(true);
        let allowing: StreamDefinition = serde_json::from_value(json).unwrap();
        assert!(allowing.allow_cleartext_links);
        assert_eq!(
            serde_json::to_value(&allowing).unwrap()["allow_cleartext_links"],
            true
        );
        assert_ne!(stream, allowing, "the field is part of the spec");
    }

    #[test]
    fn input_source_declares_no_format_so_nothing_conflicts() {
        let stream = StreamDefinition {
            name: "alice-cam".to_string(),
            enabled: true,
            format_policy: crate::FormatPolicy::Report,
            allow_cleartext_links: false,
            source: StreamTransport::Input(LocalEndpoint {
                node: "browser-a1b2".to_string(),
                id: "camera".to_string(),
                network: None,
            }),
            destinations: vec![StreamDestination {
                delivery: None,
                id: "studio".to_string(),
                paths: 1,
                endpoint: StreamTransport::Srt(SrtEndpoint {
                    node: Some("strom-node-2".to_string()),
                    remote: None,
                    via: Vec::new(),
                    network: None,
                    latency: None,
                    passphrase: None,
                    format: None,
                    accepts: Some(wants_44k()),
                }),
            }],
        };
        assert!(stream_format_conflicts(&stream).is_empty());
    }

    #[test]
    fn every_socket_variant_round_trips_through_its_flat_wire_form() {
        let cases = [
            (
                SocketSpec::srt_listen(7001, 200),
                serde_json::json!({
                    "transport": "srt", "role": "listen", "port": 7001,
                    "params": { "latency": 200 }
                }),
            ),
            (
                SocketSpec::srt_connect("10.0.0.2", 7002, 1000),
                serde_json::json!({
                    "transport": "srt", "role": "connect", "host": "10.0.0.2", "port": 7002,
                    "params": { "latency": 1000 }
                }),
            ),
            (
                SocketSpec::signalling(
                    SignallingTransport::Whip,
                    SocketRole::Listen,
                    "http://172.26.0.10:8080/whip",
                    "weave-x",
                ),
                serde_json::json!({
                    "transport": "whip", "role": "listen",
                    "url": "http://172.26.0.10:8080/whip/weave-x", "endpoint_id": "weave-x"
                }),
            ),
            (
                SocketSpec::signalling(
                    SignallingTransport::Whep,
                    SocketRole::Connect,
                    "http://172.26.0.10:8080/whep",
                    "weave-x",
                ),
                serde_json::json!({
                    "transport": "whep", "role": "connect",
                    "url": "http://172.26.0.10:8080/whep/weave-x", "endpoint_id": "weave-x"
                }),
            ),
            (
                SocketSpec::input("camera"),
                serde_json::json!({ "transport": "input", "id": "camera" }),
            ),
            (
                SocketSpec::output("player"),
                serde_json::json!({ "transport": "output", "id": "player" }),
            ),
            (
                SocketSpec::Webrtc(PeerSocket {
                    attempt: PeerAttempt {
                        link: PeerLinkId::new("weave-music-receiver-filip"),
                        generation: 1_761_300_000,
                    },
                    stun: vec!["stun.example:3478".to_string()],
                    turn: vec![TurnServer {
                        address: "turn.example:3478".to_string(),
                        username: "1761386400:weave-music-receiver-filip".to_string(),
                        credential: TurnCredential::new("c2VjcmV0"),
                    }],
                }),
                serde_json::json!({
                    "transport": "webrtc", "link": "weave-music-receiver-filip",
                    "generation": 1_761_300_000_u32, "stun": ["stun.example:3478"],
                    "turn": [{
                        "address": "turn.example:3478",
                        "username": "1761386400:weave-music-receiver-filip",
                        "credential": "c2VjcmV0"
                    }]
                }),
            ),
        ];

        for (socket, wire) in cases {
            assert_eq!(serde_json::to_value(&socket).unwrap(), wire);
            let parsed: SocketSpec = serde_json::from_value(wire).unwrap();
            assert_eq!(parsed, socket);
        }
    }

    #[test]
    fn an_srt_socket_parses_with_empty_params() {
        let stored: SocketSpec = serde_json::from_value(serde_json::json!({
            "transport": "srt", "role": "listen", "port": 7001, "params": {}
        }))
        .expect("hydrate");
        assert_eq!(
            stored,
            SocketSpec::Srt(SrtSocket::Listen {
                port: 7001,
                params: SrtParams::default(),
            })
        );
    }

    #[test]
    fn a_socket_that_mixes_up_its_transport_is_rejected() {
        let rejected = [
            serde_json::json!({ "transport": "srt", "role": "listen", "params": {} }),
            serde_json::json!({ "transport": "srt", "role": "connect", "port": 7002 }),
            serde_json::json!({
                "transport": "srt", "role": "listen", "port": 7001, "host": "10.0.0.2"
            }),
            serde_json::json!({
                "transport": "srt", "role": "listen", "port": 7001, "url": "http://x/whip/y"
            }),
            serde_json::json!({ "transport": "whip", "role": "listen" }),
            serde_json::json!({
                "transport": "whip", "role": "listen", "url": "http://x/whip/y"
            }),
            serde_json::json!({
                "transport": "whip", "role": "listen", "url": "http://x/whip/y", "port": 7001,
                "endpoint_id": "y"
            }),
            serde_json::json!({
                "transport": "whep", "role": "connect", "url": "http://x/whep/y",
                "endpoint_id": "y", "params": {}
            }),
            serde_json::json!({ "transport": "input" }),
            serde_json::json!({ "transport": "input", "role": "connect", "id": "camera" }),
            serde_json::json!({ "transport": "srt", "port": 7001 }),
            serde_json::json!({ "transport": "srt", "role": "listen", "port": 7001, "id": "x" }),
            serde_json::json!({ "transport": "output", "id": "player", "port": 7001 }),
            serde_json::json!({ "transport": "srt", "role": "listen", "port": 7001, "bogus": 1 }),
            serde_json::json!({
                "transport": "srt", "role": "listen", "port": 7001, "endpoint_id": "y"
            }),
            serde_json::json!({ "transport": "input", "id": "camera", "endpoint_id": "y" }),
            serde_json::json!({ "transport": "webrtc", "generation": 1 }),
            serde_json::json!({ "transport": "webrtc", "link": "x" }),
            serde_json::json!({ "transport": "webrtc", "link": "x", "generation": 1, "role": "listen" }),
            serde_json::json!({ "transport": "webrtc", "link": "x", "generation": 1, "port": 7001 }),
            serde_json::json!({ "transport": "srt", "role": "listen", "port": 7001, "link": "x" }),
            serde_json::json!({ "transport": "output", "id": "player", "turn": [] }),
        ];

        for wire in rejected {
            let result: Result<SocketSpec, _> = serde_json::from_value(wire.clone());
            assert!(result.is_err(), "{wire} must not parse");
        }
    }

    #[test]
    fn socket_display_names_the_transport_or_the_local_end() {
        assert_eq!(SocketSpec::srt_listen(7001, 200).to_string(), "srt");
        assert_eq!(
            SocketSpec::signalling(
                SignallingTransport::Whip,
                SocketRole::Listen,
                "http://x/whip",
                "y"
            )
            .to_string(),
            "whip"
        );
        assert_eq!(
            SocketSpec::signalling(
                SignallingTransport::Whep,
                SocketRole::Connect,
                "http://x/whep",
                "y"
            )
            .to_string(),
            "whep"
        );
        assert_eq!(SocketSpec::input("camera").to_string(), "input camera");
        assert_eq!(SocketSpec::output("player").to_string(), "output player");
    }

    #[test]
    fn a_transport_offered_in_no_role_or_under_no_known_name_is_rejected() {
        let no_role: Result<TransportClass, _> =
            serde_json::from_value(serde_json::json!({ "transport": "whip", "roles": [] }));
        assert!(no_role.is_err(), "a transport offered in no role");

        let unknown: Result<TransportClass, _> =
            serde_json::from_value(serde_json::json!({ "transport": "zixi", "roles": ["listen"] }));
        assert!(unknown.is_err(), "an unknown transport name is an error");
    }

    #[test]
    fn hop_profiles_decide_which_transports_roles_and_local_ends_a_node_offers() {
        let capabilities: NodeCapabilities = serde_json::from_value(serde_json::json!({
            "hop_profiles": [{
                "id": "camera-to-whip",
                "ingress": "input",
                "egress": { "transport": "whip", "roles": ["connect"] },
                "max_egresses": 1
            }]
        }))
        .expect("parse capabilities");

        assert!(capabilities.offers_local(LocalEnd::Input));
        assert!(!capabilities.offers_local(LocalEnd::Output));
        assert!(capabilities.offers_egress(Transport::Whip, SocketRole::Connect));
        assert!(!capabilities.offers_egress(Transport::Whip, SocketRole::Listen));
        assert!(!capabilities.offers_ingress(Transport::Whip, SocketRole::Connect));
        assert!(!capabilities.offers_ingress(Transport::Srt, SocketRole::Listen));

        let round_trip: NodeCapabilities =
            serde_json::from_str(&serde_json::to_string(&capabilities).unwrap()).unwrap();
        assert_eq!(capabilities, round_trip);

        let bare = NodeCapabilities::default();
        assert!(!bare.offers_ingress(Transport::Srt, SocketRole::Listen));
        assert!(!bare.offers_egress(Transport::Srt, SocketRole::Connect));
        assert!(!bare.offers_local(LocalEnd::Input));
    }

    #[test]
    fn network_listeners_carry_signalling_bases_per_webrtc_transport() {
        let listeners: NetworkListeners = serde_json::from_value(serde_json::json!({
            "whip": { "base_url": "http://172.26.0.10:8080/whip" },
            "whep": { "base_url": "http://172.26.0.10:8080/whep" }
        }))
        .expect("parse");
        assert!(!listeners.is_empty());
        assert_eq!(
            listeners.signalling(SignallingTransport::Whip),
            Some("http://172.26.0.10:8080/whip")
        );
        assert_eq!(
            listeners.signalling(SignallingTransport::Whep),
            Some("http://172.26.0.10:8080/whep")
        );
        let round_trip: NetworkListeners =
            serde_json::from_str(&serde_json::to_string(&listeners).unwrap()).unwrap();
        assert_eq!(listeners, round_trip);

        let srt_only: NetworkListeners = serde_json::from_value(serde_json::json!({
            "srt": { "host": "172.26.0.10", "port_range": { "start": 20000, "end": 20999 } }
        }))
        .expect("parse srt listener");
        assert_eq!(srt_only.signalling(SignallingTransport::Whip), None);
        assert_eq!(
            serde_json::to_value(&srt_only).unwrap(),
            serde_json::json!({
                "srt": { "host": "172.26.0.10", "port_range": { "start": 20000, "end": 20999 } }
            }),
            "a listener set hosting no signalling serializes none"
        );
    }

    #[test]
    fn signalling_socket_joins_base_and_endpoint_id() {
        let trimmed = SignallingSocket::new(SocketRole::Listen, "http://x:8080/whip/", "weave-a");
        let bare = SignallingSocket::new(SocketRole::Listen, "http://x:8080/whip", "weave-a");
        assert_eq!(trimmed.url, "http://x:8080/whip/weave-a");
        assert_eq!(trimmed.endpoint_id, "weave-a");
        assert_eq!(
            trimmed, bare,
            "a trailing slash on the base changes nothing"
        );
    }

    #[test]
    fn signalling_transport_maps_both_ways_with_transport() {
        assert_eq!(Transport::Srt.signalling(), None);
        assert_eq!(
            Transport::Whip.signalling(),
            Some(SignallingTransport::Whip)
        );
        assert_eq!(
            Transport::Whep.signalling(),
            Some(SignallingTransport::Whep)
        );
        assert_eq!(SignallingTransport::Whip.transport(), Transport::Whip);
        assert_eq!(SignallingTransport::Whep.transport(), Transport::Whep);
    }

    #[test]
    fn transport_and_local_end_names_match_their_wire_form() {
        for transport in [Transport::Srt, Transport::Whip, Transport::Whep] {
            assert_eq!(
                serde_json::to_value(transport).unwrap(),
                serde_json::json!(transport.name())
            );
            assert_eq!(transport.to_string(), transport.name());
        }
        for end in [LocalEnd::Input, LocalEnd::Output] {
            assert_eq!(
                serde_json::to_value(end).unwrap(),
                serde_json::json!(end.name())
            );
            assert_eq!(end.to_string(), end.name());
        }
    }

    #[test]
    fn a_report_for_an_old_spec_cannot_confirm_a_new_spec() {
        let mut desired = sample_hop();
        let socket = SocketStatus {
            peer: None,
            condition: LinkCondition::Flowing,
            resolved: None,
            stats: None,
        };
        let status = HopStatus {
            delivery: None,
            id: desired.id.clone(),
            node_id: desired.node_id.clone(),
            revision: desired.revision(),
            state: HopState::Provisioned,
            ingress: socket.clone(),
            merge_ingress: None,
            egresses: desired
                .egresses
                .iter()
                .map(|egress| EgressStatus {
                    branch_id: egress.branch_id.clone(),
                    status: socket.clone(),
                })
                .collect(),
        };
        assert!(status.conditions(&desired).is_some());
        desired.ingress = SocketSpec::srt_listen(7001, 400);
        assert_eq!(status.conditions(&desired), None);
    }

    #[test]
    fn hop_revisions_match_the_browser_canonical_json_vector() {
        let hop: DesiredHop = serde_json::from_value(serde_json::json!({
            "id": "hop-a", "node_id": "node-a", "profile_id": "local", "role": "sender",
            "ingress": { "transport": "input", "id": "camera" },
            "egresses": [{ "branch_id": "studio", "transport": "output", "id": "player" }]
        }))
        .unwrap();
        assert_eq!(
            hop.revision(),
            "3863eff83ea855efb81128098118c8f26b03294081b15e0b02441ec7a66e516d"
        );
    }

    #[test]
    fn a_merge_ingress_is_reported_exactly_when_desired() {
        let socket = |condition| SocketStatus {
            peer: None,
            condition,
            resolved: None,
            stats: None,
        };
        let mut desired = sample_hop();
        desired.merge_ingress = Some(SocketSpec::srt_listen(7004, 1000));
        let mut status = HopStatus {
            delivery: None,
            id: desired.id.clone(),
            node_id: desired.node_id.clone(),
            revision: desired.revision(),
            state: HopState::Provisioned,
            ingress: socket(LinkCondition::Flowing),
            merge_ingress: None,
            egresses: vec![EgressStatus {
                branch_id: "studio".to_string(),
                status: socket(LinkCondition::Flowing),
            }],
        };
        assert_eq!(status.conditions(&desired), None);

        status.merge_ingress = Some(socket(LinkCondition::Idle));
        let conditions = status.conditions(&desired).unwrap();
        assert_eq!(conditions.merge_ingress, Some(LinkCondition::Idle));
        assert_eq!(status.conditions(&sample_hop()), None);

        let sender = hc(
            HopState::Provisioned,
            LinkCondition::Flowing,
            LinkCondition::Flowing,
        );
        assert_eq!(
            roll_up_path(true, &[sender.clone(), Some(conditions.clone())]),
            PathStatus::Degraded
        );
        let flowing = HopConditions {
            merge_ingress: Some(LinkCondition::Flowing),
            ..conditions.clone()
        };
        assert_eq!(
            roll_up_path(true, &[sender.clone(), Some(flowing)]),
            PathStatus::Flowing
        );
        let stalled = HopConditions {
            merge_ingress: Some(LinkCondition::Stalled),
            ..conditions
        };
        assert_eq!(
            roll_up_path(true, &[sender, Some(stalled)]),
            PathStatus::Degraded
        );
    }

    #[test]
    fn hop_profile_merge_defaults_off_and_is_omitted_when_off() {
        let profile: HopProfile = serde_json::from_value(serde_json::json!({
            "id": "srt-merge",
            "ingress": { "transport": "srt", "roles": ["listen", "connect"] },
            "egress": { "transport": "srt", "roles": ["listen"] }
        }))
        .unwrap();
        assert!(!profile.merge);
        assert!(
            serde_json::to_value(&profile)
                .unwrap()
                .get("merge")
                .is_none()
        );
        let merging = HopProfile {
            media_behavior: crate::MediaBehavior::Unknown,
            merge: true,
            ..profile
        };
        assert_eq!(serde_json::to_value(&merging).unwrap()["merge"], true);
    }
}

#[cfg(test)]
mod declared_media_tests {
    use super::*;

    #[test]
    fn policy_defaults_round_trip_and_reject_unknown_values() {
        let mut value = serde_json::json!({
            "name": "feed",
            "source": {"srt": {"node": "source"}},
            "destinations": [{"id": "output", "srt": {"node": "receiver"}}]
        });
        let omitted: StreamDefinition = serde_json::from_value(value.clone()).unwrap();
        value["format_policy"] = serde_json::json!("report");
        let explicit: StreamDefinition = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(omitted, explicit);
        value["format_policy"] = serde_json::json!("strict");
        let strict: StreamDefinition = serde_json::from_value(value.clone()).unwrap();
        assert_ne!(strict, omitted);
        assert_eq!(
            serde_json::from_value::<StreamDefinition>(serde_json::to_value(&strict).unwrap())
                .unwrap(),
            strict
        );
        value["format_policy"] = serde_json::json!("guess");
        assert!(serde_json::from_value::<StreamDefinition>(value).is_err());
    }

    #[test]
    fn old_profile_metadata_never_implies_preservation() {
        let value = serde_json::json!({
            "id": "srt-forward",
            "ingress": {"transport": "srt", "roles": ["listen", "connect"]},
            "egress": {"transport": "srt", "roles": ["listen", "connect"]},
            "accepts": {}
        });
        let profile: HopProfile = serde_json::from_value(value).unwrap();
        assert_eq!(profile.media_behavior, MediaBehavior::Unknown);
    }
}
