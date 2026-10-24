use std::fmt;

use serde::de::{DeserializeOwned, IntoDeserializer};
use serde::{Deserialize, Deserializer, Serialize};

use crate::webhook::EventType;
use crate::{
    ApiErrorCode, NodeStatus, PathStatus, PlanStatus, ReconcileStatus, StartingState,
    StreamConditionReason, StreamConditionStatus, StreamConditionType, StreamSetAction, Track,
};

fn report_policy() -> Open<crate::FormatPolicy> {
    Open::Known(crate::FormatPolicy::Report)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Open<E> {
    Known(E),
    Unknown(Unrecognized),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Unrecognized(String);

impl Unrecognized {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<E: PartialEq> PartialEq<E> for Open<E> {
    fn eq(&self, other: &E) -> bool {
        matches!(self, Self::Known(known) if known == other)
    }
}

impl<'de, E: DeserializeOwned> Deserialize<'de> for Open<E> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        let known: Result<E, serde::de::value::Error> =
            E::deserialize(name.as_str().into_deserializer());
        Ok(match known {
            Ok(known) => Self::Known(known),
            Err(_) => Self::Unknown(Unrecognized(name)),
        })
    }
}

impl<E: Serialize> fmt::Display for Open<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Known(known) => match serde_json::to_value(known) {
                Ok(serde_json::Value::String(name)) => f.write_str(&name),
                _ => Err(fmt::Error),
            },
            Self::Unknown(unrecognized) => f.write_str(unrecognized.as_str()),
        }
    }
}

#[cfg(test)]
impl<E: schemars::JsonSchema> schemars::JsonSchema for Open<E> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        format!("Open_{}", E::schema_name()).into()
    }

    fn inline_schema() -> bool {
        true
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "anyOf": [generator.subschema_for::<E>(), { "type": "string" }] })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct ApiError {
    pub code: Open<ApiErrorCode>,
    pub message: String,
    #[serde(default)]
    pub details: Vec<ValidationIssue>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct ValidationIssue {
    pub field: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct StreamResource {
    pub generation: u64,
    pub owner: Option<String>,
    pub spec: StreamSpec,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct StreamSpec {
    #[serde(default = "report_policy")]
    pub format_policy: Open<crate::FormatPolicy>,
    pub name: String,
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct StreamSetResource {
    pub owner: String,
    pub streams: Vec<StreamResource>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct StreamAccepted {
    pub name: String,
    pub generation: u64,
    pub changed: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct StreamSetAccepted {
    pub owner: String,
    pub changed: bool,
    pub streams: Vec<StreamSetMemberResult>,
    pub pruned: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct StreamSetMemberResult {
    pub name: String,
    pub generation: u64,
    pub action: Open<StreamSetAction>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct StreamPlan {
    #[serde(default)]
    pub conditions: Vec<StreamCondition>,
    #[serde(default)]
    pub destinations: Vec<StreamDestinationStatus>,
    #[serde(default = "report_policy")]
    pub format_policy: Open<crate::FormatPolicy>,
    pub name: String,
    pub status: Open<PlanStatus>,
    pub nodes: Vec<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct StreamEndpoints {
    pub ingress: Option<EndpointAddr>,
    pub destinations: Vec<DestinationEndpoint>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct DestinationEndpoint {
    pub id: String,
    pub endpoint: Option<EndpointAddr>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct EndpointAddr {
    pub node: String,
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum StatusResponse {
    Starting(StartingStatus),
    Running(RunningStatus),
}

impl StatusResponse {
    #[must_use]
    pub fn streams(&self) -> &[StreamStatus] {
        match self {
            Self::Starting(_) => &[],
            Self::Running(status) => &status.streams,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct StartingStatus {
    pub status: StartingState,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct RunningStatus {
    pub status: Open<ReconcileStatus>,
    pub summary: String,
    pub streams: Vec<StreamStatus>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct StreamStatus {
    #[serde(default = "report_policy")]
    pub format_policy: Open<crate::FormatPolicy>,
    pub name: String,
    pub generation: u64,
    pub observed_generation: Option<u64>,
    pub status: Open<PathStatus>,
    pub nodes: Vec<String>,
    pub conditions: Vec<StreamCondition>,
    pub destinations: Vec<StreamDestinationStatus>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct StreamDestinationStatus {
    #[serde(default)]
    pub delivery: Option<serde_json::Value>,
    pub id: String,
    pub status: Open<PathStatus>,
    pub nodes: Vec<String>,
    pub conditions: Vec<StreamCondition>,
    pub endpoint: Option<EndpointAddr>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct StreamCondition {
    #[serde(rename = "type")]
    pub condition_type: Open<StreamConditionType>,
    pub status: Open<StreamConditionStatus>,
    pub reason: Open<StreamConditionReason>,
    pub detail: String,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct NodeResource {
    pub node: NodeDescriptor,
    #[serde(default)]
    pub site: Option<SiteState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct SiteState {
    pub state: String,
    #[serde(default)]
    pub subnet: Option<String>,
    #[serde(default)]
    pub public: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct NodeDescriptor {
    pub id: String,
    pub status: Open<NodeStatus>,
    pub endpoint: String,
    pub capabilities: NodeCapabilities,
    pub topology: NodeTopology,
    pub inputs: Vec<NodeInput>,
    pub outputs: Vec<NodeOutput>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct NodeInput {
    pub id: String,
    pub label: String,
    pub tracks: Vec<Open<Track>>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct NodeOutput {
    pub id: String,
    pub label: String,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct NodeCapabilities {
    pub hop_profiles: Vec<HopProfile>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct HopProfile {
    pub id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct NodeTopology {
    pub attachments: Vec<NetworkAttachment>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct NetworkAttachment {
    pub network: String,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct Event {
    pub event_id: String,
    pub event_type: Open<EventType>,
    pub occurred_at: String,
    pub node: Option<NodeSummary>,
    pub stream: Option<StreamSummary>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct NodeSummary {
    pub id: String,
    pub status: Open<NodeStatus>,
}

#[derive(Debug, Clone, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct StreamSummary {
    #[serde(default = "report_policy")]
    pub format_policy: Open<crate::FormatPolicy>,
    pub name: String,
    pub generation: u64,
    pub status: Open<PathStatus>,
    pub conditions: Vec<StreamCondition>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use schemars::JsonSchema;
    use schemars::generate::SchemaSettings;
    use serde_json::{Value, json};

    fn rendered<T: JsonSchema>(settings: SchemaSettings) -> Value {
        let settings = settings.with(|settings| settings.inline_subschemas = true);
        serde_json::to_value(settings.into_generator().into_root_schema_for::<T>()).unwrap()
    }

    fn misreads_of<R: JsonSchema, S: JsonSchema>() -> Vec<String> {
        let read = rendered::<R>(SchemaSettings::draft2020_12().for_deserialize());
        let sent = rendered::<S>(SchemaSettings::draft2020_12().for_serialize());
        let mut misreads = Vec::new();
        misread(&read, &sent, &S::schema_name(), &mut misreads);
        misreads
    }

    fn union(schema: &Value) -> Option<&Vec<Value>> {
        let branches = schema
            .get("anyOf")
            .or_else(|| schema.get("oneOf"))?
            .as_array()?;
        (!branches.iter().all(|branch| branch.get("const").is_some())).then_some(branches)
    }

    fn values(schema: &Value) -> Option<Vec<&Value>> {
        if let Some(values) = schema.get("enum").and_then(Value::as_array) {
            return Some(values.iter().collect());
        }
        if let Some(value) = schema.get("const") {
            return Some(vec![value]);
        }
        let branches = schema
            .get("oneOf")
            .or_else(|| schema.get("anyOf"))?
            .as_array()?;
        branches.iter().map(|branch| branch.get("const")).collect()
    }

    fn types(schema: &Value) -> Vec<&str> {
        match schema.get("type") {
            Some(Value::String(kind)) => vec![kind.as_str()],
            Some(Value::Array(kinds)) => kinds.iter().filter_map(Value::as_str).collect(),
            _ => Vec::new(),
        }
    }

    fn names(schema: &Value, key: &str) -> Vec<String> {
        schema
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    }

    fn variants(schema: &Value) -> Vec<&Value> {
        match union(schema) {
            Some(branches) if schema.get("properties").is_none() => {
                branches.iter().flat_map(variants).collect()
            }
            _ => vec![schema],
        }
    }

    fn sent_property<'a>(sent: &'a Value, name: &str) -> Option<&'a Value> {
        sent.get("properties")
            .and_then(|properties| properties.get(name))
            .or_else(|| {
                union(sent).into_iter().flatten().find_map(|branch| {
                    branch
                        .get("properties")
                        .and_then(|properties| properties.get(name))
                })
            })
    }

    fn misread(read: &Value, sent: &Value, at: &str, misreads: &mut Vec<String>) {
        if sent.get("properties").is_none() && union(sent).is_some() {
            let (objects, others): (Vec<&Value>, Vec<&Value>) = variants(sent)
                .into_iter()
                .partition(|branch| branch.get("properties").is_some());
            for branch in others {
                misread(read, branch, at, misreads);
            }
            if union(read).is_some() || read.get("properties").is_none() {
                for branch in objects {
                    misread(read, branch, at, misreads);
                }
            } else if !objects.is_empty() {
                misread_properties(read, &objects, at, misreads);
            }
            return;
        }
        if let Some(branches) = union(read) {
            let fits = branches.iter().any(|branch| {
                let mut found = Vec::new();
                misread(branch, sent, at, &mut found);
                found.is_empty()
            });
            if !fits {
                misreads.push(format!("{at}: no reader branch takes what is sent"));
            }
            return;
        }
        let (read_types, sent_types) = (types(read), types(sent));
        if !read_types.is_empty()
            && sent_types.iter().any(|kind| {
                !read_types.contains(kind)
                    && !(*kind == "integer" && read_types.contains(&"number"))
            })
        {
            misreads.push(format!("{at}: reads {read_types:?}, sent {sent_types:?}"));
            return;
        }
        if let Some(taken) = values(read) {
            match values(sent) {
                Some(sent_values) => {
                    for value in sent_values
                        .into_iter()
                        .filter(|value| !taken.contains(value))
                    {
                        misreads.push(format!("{at}: sent value {value} is not read"));
                    }
                }
                None => misreads.push(format!("{at}: reads a closed set, sent any value")),
            }
        }
        if sent_types != ["null"] {
            misread_properties(read, &[sent], at, misreads);
        }
        if let (Some(read_items), Some(sent_items)) = (read.get("items"), sent.get("items")) {
            misread(read_items, sent_items, &format!("{at}[]"), misreads);
        }
    }

    fn misread_properties(read: &Value, variants: &[&Value], at: &str, misreads: &mut Vec<String>) {
        let read_required = names(read, "required");
        for (name, property) in read
            .get("properties")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            let child = format!("{at}.{name}");
            let sent: Vec<Option<&Value>> = variants
                .iter()
                .map(|variant| sent_property(variant, name))
                .collect();
            if sent.iter().all(Option::is_none) {
                misreads.push(format!("{child}: read but never sent"));
                continue;
            }
            if read_required.contains(name)
                && variants.iter().zip(&sent).any(|(variant, property)| {
                    property.is_none() || !names(variant, "required").contains(name)
                })
            {
                misreads.push(format!("{child}: read as required, sent only sometimes"));
            }
            for sent_property in sent.into_iter().flatten() {
                misread(property, sent_property, &child, misreads);
            }
        }
    }

    fn closed_enumerations_of<R: JsonSchema>() -> Vec<String> {
        let mut closed = Vec::new();
        closed_enumerations(
            &rendered::<R>(SchemaSettings::draft2020_12().for_deserialize()),
            &R::schema_name(),
            &mut closed,
        );
        closed
    }

    fn closed_enumerations(schema: &Value, at: &str, closed: &mut Vec<String>) {
        let any_string = json!({ "type": "string" });
        if union(schema).is_some_and(|branches| branches.contains(&any_string)) {
            return;
        }
        if values(schema).is_some() {
            closed.push(at.to_string());
            return;
        }
        for (name, property) in schema
            .get("properties")
            .and_then(Value::as_object)
            .into_iter()
            .flatten()
        {
            closed_enumerations(property, &format!("{at}.{name}"), closed);
        }
        if let Some(items) = schema.get("items") {
            closed_enumerations(items, &format!("{at}[]"), closed);
        }
        for branch in union(schema).into_iter().flatten() {
            closed_enumerations(branch, at, closed);
        }
    }

    fn check<R: JsonSchema, S: JsonSchema>() -> Vec<String> {
        let mut problems = misreads_of::<R, S>();
        problems.extend(
            closed_enumerations_of::<R>()
                .into_iter()
                .map(|at| format!("{at}: a closed enumeration")),
        );
        problems
    }

    #[test]
    fn every_reader_takes_whatever_its_payload_sends() {
        let problems = [
            check::<ApiError, crate::ApiError>(),
            check::<StreamResource, crate::StreamResource>(),
            check::<StreamSetResource, crate::StreamSetResource>(),
            check::<StreamAccepted, crate::StreamAccepted>(),
            check::<StreamSetAccepted, crate::StreamSetAccepted>(),
            check::<StreamPlan, crate::StreamPlan>(),
            check::<StreamEndpoints, crate::StreamEndpoints>(),
            check::<StatusResponse, crate::StatusResponse>(),
            check::<Vec<NodeResource>, Vec<crate::NodeResource>>(),
            check::<Event, crate::webhook::Event>(),
        ]
        .concat();
        assert_eq!(
            problems,
            ["StatusResponse.status: a closed enumeration"],
            "only `starting` may be closed: it tells the two untagged status shapes apart"
        );
    }

    #[test]
    fn a_reader_that_drifts_from_its_payload_is_caught() {
        #[derive(Deserialize, JsonSchema)]
        #[allow(dead_code)]
        struct Renamed {
            generation: u64,
            owner_id: Option<String>,
        }
        #[derive(Deserialize, JsonSchema)]
        #[allow(dead_code)]
        struct AlwaysOwned {
            owner: String,
        }
        #[derive(Deserialize, JsonSchema)]
        #[allow(dead_code)]
        struct ClosedStatus {
            status: StartingState,
        }

        assert_eq!(
            misreads_of::<Renamed, crate::StreamResource>(),
            ["StreamResource.owner_id: read but never sent"]
        );
        assert_eq!(
            misreads_of::<AlwaysOwned, crate::StreamResource>(),
            [
                "StreamResource.owner: read as required, sent only sometimes",
                r#"StreamResource.owner: reads ["string"], sent ["string", "null"]"#
            ]
        );
        assert!(
            misreads_of::<ClosedStatus, crate::RunningStatus>()
                .contains(&r#"RunningStatus.status: sent value "idle" is not read"#.to_string())
        );

        #[derive(Deserialize, JsonSchema)]
        #[allow(dead_code)]
        struct RenamedSite {
            state: String,
            subnets: Option<String>,
        }
        #[derive(Deserialize, JsonSchema)]
        #[allow(dead_code)]
        struct AlwaysLocated {
            state: String,
            subnet: String,
        }

        assert_eq!(
            misreads_of::<RenamedSite, crate::SiteState>(),
            ["SiteState.subnets: read but never sent"]
        );
        assert_eq!(
            misreads_of::<AlwaysLocated, crate::SiteState>(),
            ["SiteState.subnet: read as required, sent only sometimes"]
        );
    }

    #[test]
    fn a_newer_servers_payloads_decode() {
        let stream: StreamResource = serde_json::from_value(json!({
            "generation": 4,
            "labels": { "site": "north" },
            "spec": {
                "name": "camera",
                "enabled": true,
                "source": { "srt": { "node": "a" } },
                "destinations": [{ "id": "studio", "rist": { "node": "b" } }]
            }
        }))
        .unwrap();
        assert_eq!(
            (stream.spec.name.as_str(), stream.generation),
            ("camera", 4)
        );

        let status: StatusResponse = serde_json::from_value(json!({
            "status": "rebalancing",
            "summary": "0/1 stream(s) flowing across 2 node(s)",
            "streams": [{
                "name": "camera",
                "generation": 4,
                "observed_generation": null,
                "status": "preempted",
                "nodes": [],
                "conditions": [{
                    "type": "media_flowing",
                    "status": "false",
                    "reason": "source_preempted",
                    "detail": "another source holds the slot",
                    "severity": "warning"
                }],
                "destinations": []
            }]
        }))
        .unwrap();
        let StatusResponse::Running(running) = &status else {
            panic!("a running status: {status:?}");
        };
        assert_eq!(running.status.to_string(), "rebalancing");
        let stream = &status.streams()[0];
        assert_eq!(stream.status.to_string(), "preempted");
        assert!(stream.status != PathStatus::Failed);
        let condition = &stream.conditions[0];
        assert_eq!(condition.reason.to_string(), "source_preempted");
        assert!(
            matches!(&condition.reason, Open::Unknown(reason) if reason.as_str() == "source_preempted")
        );
        assert!(condition.status == StreamConditionStatus::False);
        assert_eq!(condition.condition_type.to_string(), "media_flowing");

        let error: ApiError = serde_json::from_value(json!({
            "code": "quota_exceeded",
            "message": "too many streams",
            "retry_after_secs": 30
        }))
        .unwrap();
        assert_eq!(error.code.to_string(), "quota_exceeded");
        assert!(error.details.is_empty());

        let deleted: Event = serde_json::from_value(json!({
            "event_id": "camera-1",
            "event_type": "stream.deleted",
            "occurred_at": "2026-10-16T10:00:00Z",
            "route": { "id": "camera-north" }
        }))
        .unwrap();
        assert_eq!(deleted.event_type.to_string(), "stream.deleted");
        assert!(deleted.node.is_none() && deleted.stream.is_none());

        let online: Event = serde_json::from_value(json!({
            "event_id": "guest-1-2",
            "event_type": "node.online",
            "occurred_at": "2026-10-16T10:00:00Z",
            "node": { "id": "guest-1", "status": "ready", "region": "eu" }
        }))
        .unwrap();
        assert_eq!(online.event_type, Open::Known(EventType::NodeOnline));
        assert_eq!(online.event_type.to_string(), "node.online");
        assert!(online.node.unwrap().status == NodeStatus::Ready);

        let node = |site: Value| {
            json!({
                "node": {
                    "id": "alfreds-mac",
                    "endpoint": "http://127.0.0.1:8092",
                    "status": "ready",
                    "capabilities": { "hop_profiles": [] },
                    "topology": { "attachments": [] },
                    "inputs": [],
                    "outputs": []
                },
                "site": site
            })
        };
        let nodes: Vec<NodeResource> = serde_json::from_value(json!([
            node(json!({
                "state": "resolved",
                "network": "site-3f9a2c1d7e4b",
                "subnet": "192.168.3.0/24",
                "public": ["92.244.18.125"],
                "mapping": "endpoint_independent"
            })),
            node(json!({ "state": "behind_cgnat", "carrier": "100.64.0.0/10" })),
        ]))
        .unwrap();
        assert_eq!(
            nodes[0].site,
            Some(SiteState {
                state: "resolved".to_string(),
                subnet: Some("192.168.3.0/24".to_string()),
                public: vec!["92.244.18.125".to_string()],
            })
        );
        assert_eq!(
            nodes[1].site,
            Some(SiteState {
                state: "behind_cgnat".to_string(),
                subnet: None,
                public: Vec::new(),
            })
        );
    }
}
