use schemars::transform::{Transform, transform_subschemas};
use schemars::{JsonSchema, Schema, generate::SchemaSettings, json_schema};
use serde_json::{Map, Value, json};

use crate::{
    ApiError, DesiredHop, EndpointDescriptor, NodeAccepted, NodeHeartbeat, NodeRegistration,
    NodeResource, ObservedState, ROUTE_ENDPOINTS, ROUTE_NODE_DESIRED, ROUTE_NODE_HEARTBEAT,
    ROUTE_NODE_REGISTER, ROUTE_NODES, ROUTE_STATE, ROUTE_STATUS, ROUTE_STREAM,
    ROUTE_STREAM_ENDPOINTS, ROUTE_STREAM_PLANS, ROUTE_STREAM_SET, ROUTE_STREAM_SETS, ROUTE_STREAMS,
    StatusResponse, StreamAccepted, StreamDefinition, StreamEndpoints, StreamPlan, StreamResource,
    StreamSetAccepted, StreamSetApply, StreamSetResource, webhook,
};

pub struct ContractArtifact {
    pub path: &'static str,
    pub content: String,
}

#[must_use]
pub fn artifacts() -> Vec<ContractArtifact> {
    let mut artifacts = vec![
        artifact("contracts/openapi/northbound.json", northbound_openapi()),
        artifact("contracts/openapi/southbound.json", southbound_openapi()),
    ];
    artifacts.extend(
        payloads()
            .into_iter()
            .map(|payload| artifact(payload.path, (payload.schema)(payload.reader))),
    );
    artifacts
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reader {
    Controller,

    Adapter,

    Client,
}

struct Payload {
    path: &'static str,
    reader: Reader,
    schema: fn(Reader) -> Value,
}

fn payload<T: JsonSchema>(path: &'static str, reader: Reader) -> Payload {
    Payload {
        path,
        reader,
        schema: schema::<T>,
    }
}

fn payloads() -> [Payload; 16] {
    use Reader::{Adapter, Client, Controller};
    [
        payload::<ApiError>("contracts/json-schema/api-error.json", Client),
        payload::<StatusResponse>("contracts/json-schema/status-response.json", Client),
        payload::<StreamAccepted>("contracts/json-schema/stream-accepted.json", Client),
        payload::<StreamDefinition>("contracts/json-schema/stream-definition.json", Controller),
        payload::<StreamEndpoints>("contracts/json-schema/stream-endpoints.json", Client),
        payload::<StreamPlan>("contracts/json-schema/stream-plan.json", Client),
        payload::<StreamResource>("contracts/json-schema/stream-resource.json", Client),
        payload::<StreamSetAccepted>("contracts/json-schema/stream-set-accepted.json", Client),
        payload::<StreamSetApply>("contracts/json-schema/stream-set-apply.json", Controller),
        payload::<StreamSetResource>("contracts/json-schema/stream-set-resource.json", Client),
        payload::<NodeAccepted>("contracts/json-schema/node-accepted.json", Adapter),
        payload::<NodeHeartbeat>("contracts/json-schema/node-heartbeat.json", Controller),
        payload::<NodeRegistration>("contracts/json-schema/node-registration.json", Controller),
        payload::<ObservedState>("contracts/json-schema/observed-state.json", Adapter),
        payload::<Vec<DesiredHop>>("contracts/json-schema/desired-hops.json", Adapter),
        payload::<webhook::Event>("contracts/json-schema/webhook-event.json", Client),
    ]
}

fn artifact(path: &'static str, value: Value) -> ContractArtifact {
    ContractArtifact {
        path,
        content: format!("{}\n", serde_json::to_string_pretty(&value).unwrap()),
    }
}

fn schema<T: JsonSchema>(reader: Reader) -> Value {
    let base = SchemaSettings::draft2020_12().with(|settings| settings.inline_subschemas = true);
    let settings = match reader {
        Reader::Controller => base.for_deserialize(),
        Reader::Adapter => base.for_serialize(),
        Reader::Client => base.for_serialize().with_transform(OpenToClients),
    };
    serde_json::to_value(settings.into_generator().into_root_schema_for::<T>()).unwrap()
}

#[derive(Clone)]
struct OpenToClients;

impl Transform for OpenToClients {
    fn transform(&mut self, schema: &mut Schema) {
        if is_enumeration(schema.as_value()) {
            let closed = std::mem::take(schema);
            *schema = json_schema!({ "anyOf": [closed, { "type": "string" }] });
            return;
        }
        transform_subschemas(self, schema);
        for key in ["additionalProperties", "unevaluatedProperties"] {
            if schema.get(key) == Some(&Value::Bool(false)) {
                schema.remove(key);
            }
        }
        let union = ["oneOf", "anyOf"]
            .into_iter()
            .find(|key| schema.get(*key).is_some_and(is_object_union));
        if let Some(key) = union
            && let Some(Value::Array(mut branches)) = schema.remove(key)
        {
            branches.push(json!({ "type": "object" }));
            schema.insert("anyOf".to_string(), Value::Array(branches));
        }
    }
}

fn is_enumeration(schema: &Value) -> bool {
    let string_or_null = |value: &Value| value.is_string() || value.is_null();
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return values.iter().all(string_or_null);
    }
    if let Some(value) = schema.get("const") {
        return value.is_string();
    }
    schema
        .get("oneOf")
        .and_then(Value::as_array)
        .is_some_and(|branches| {
            branches
                .iter()
                .all(|branch| branch.get("const").is_some_and(Value::is_string))
        })
}

fn is_object_union(branches: &Value) -> bool {
    branches.as_array().is_some_and(|branches| {
        branches.len() > 1
            && branches
                .iter()
                .all(|branch| branch.get("type") == Some(&json!("object")))
    })
}

fn components(entries: &[(&str, Value)]) -> Value {
    let schemas: Map<String, Value> = entries
        .iter()
        .map(|(name, schema)| ((*name).to_string(), schema.clone()))
        .collect();
    json!({
        "schemas": schemas,
        "securitySchemes": {
            "bearerAuth": { "type": "http", "scheme": "bearer" }
        }
    })
}

fn schema_ref(name: &str) -> Value {
    json!({ "$ref": format!("#/components/schemas/{name}") })
}

fn json_content(schema: Value) -> Value {
    json!({ "application/json": { "schema": schema } })
}

fn response(description: &str, schema: Option<Value>) -> Value {
    match schema {
        Some(schema) => json!({ "description": description, "content": json_content(schema) }),
        None => json!({ "description": description }),
    }
}

fn response_with_etag(description: &str, schema: Value) -> Value {
    json!({
        "description": description,
        "headers": {
            "ETag": {
                "description": "Opaque resource revision for conditional mutations",
                "schema": { "type": "string" }
            }
        },
        "content": json_content(schema)
    })
}

fn request_body(schema_name: &str) -> Value {
    json!({
        "required": true,
        "content": json_content(schema_ref(schema_name))
    })
}

fn path_parameter(name: &str) -> Value {
    json!({
        "name": name,
        "in": "path",
        "required": true,
        "schema": {
            "type": "string",
            "minLength": 1,
            "maxLength": 63,
            "pattern": "^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
        }
    })
}

fn header_parameter(name: &str, description: &str, required: bool) -> Value {
    json!({
        "name": name,
        "in": "header",
        "description": description,
        "required": required,
        "schema": { "type": "string" }
    })
}

fn error_response(description: &str) -> Value {
    response(description, Some(schema_ref("ApiError")))
}

fn common_document(title: &str, paths: Value, components: Value) -> Value {
    json!({
        "openapi": "3.1.0",
        "info": { "title": title, "version": env!("CARGO_PKG_VERSION") },
        "paths": paths,
        "components": components,
        "security": [{ "bearerAuth": [] }]
    })
}

#[must_use]
pub fn northbound_openapi() -> Value {
    use Reader::{Client, Controller};
    common_document(
        "open-weave northbound API",
        json!({
            ROUTE_NODES: {
                "get": {
                    "operationId": "listNodes",
                    "responses": {
                        "200": response("Registered nodes", Some(schema_ref("NodeList"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_STREAMS: {
                "get": {
                    "operationId": "listStreams",
                    "responses": {
                        "200": response("Desired streams", Some(schema_ref("StreamList"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                },
                "post": {
                    "operationId": "applyStream",
                    "description": "Exactly one of If-Match or If-None-Match is required",
                    "parameters": [
                        header_parameter(
                            "If-Match",
                            "Current ETag when replacing an existing stream",
                            false,
                        ),
                        header_parameter(
                            "If-None-Match",
                            "Use * when creating a stream that must not already exist",
                            false,
                        )
                    ],
                    "requestBody": request_body("StreamDefinition"),
                    "responses": {
                        "202": response_with_etag("Stream accepted", schema_ref("StreamAccepted")),
                        "400": error_response("Invalid stream"),
                        "401": error_response("Authentication failed"),
                        "409": error_response("Stream is owned by a stream set, or can plan a hop id a stored stream can"),
                        "412": error_response("Revision precondition failed"),
                        "428": error_response("A revision precondition is required"),
                        "500": error_response("Persistence or encoding failed"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_STREAM: {
                "get": {
                    "operationId": "getStream",
                    "parameters": [path_parameter("name")],
                    "responses": {
                        "200": response_with_etag("Desired stream", schema_ref("StreamResource")),
                        "400": error_response("Invalid stream name"),
                        "401": error_response("Authentication failed"),
                        "404": error_response("Stream not found"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                },
                "delete": {
                    "operationId": "deleteStream",
                    "parameters": [
                        path_parameter("name"),
                        header_parameter(
                            "If-Match",
                            "Current ETag of the stream to delete",
                            true,
                        )
                    ],
                    "responses": {
                        "204": response("Stream deleted", None),
                        "400": error_response("Invalid stream name"),
                        "401": error_response("Authentication failed"),
                        "409": error_response("Stream is owned by a stream set"),
                        "412": error_response("Revision precondition failed"),
                        "428": error_response("A revision precondition is required"),
                        "500": error_response("Persistence failed"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_STREAM_ENDPOINTS: {
                "get": {
                    "operationId": "getStreamEndpoints",
                    "parameters": [path_parameter("name")],
                    "responses": {
                        "200": response("Placed stream endpoints", Some(schema_ref("StreamEndpoints"))),
                        "400": error_response("Invalid stream name"),
                        "401": error_response("Authentication failed"),
                        "404": error_response("Stream not found"),
                        "503": error_response("Stream not placed, or no controller holds the lease"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request")
                    }
                }
            },
            ROUTE_STREAM_SETS: {
                "get": {
                    "operationId": "listStreamSets",
                    "responses": {
                        "200": response("Stream ownership sets", Some(schema_ref("StreamSetList"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_STREAM_SET: {
                "get": {
                    "operationId": "getStreamSet",
                    "parameters": [path_parameter("owner")],
                    "responses": {
                        "200": response_with_etag("Stream ownership set", schema_ref("StreamSetResource")),
                        "400": error_response("Invalid owner"),
                        "401": error_response("Authentication failed"),
                        "404": error_response("Stream set not found"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                },
                "put": {
                    "operationId": "applyStreamSet",
                    "description": "Atomically applies the streams and optionally prunes omitted streams owned by this set. Exactly one of If-Match or If-None-Match is required",
                    "parameters": [
                        path_parameter("owner"),
                        header_parameter(
                            "If-Match",
                            "Current ETag when updating an existing stream set",
                            false,
                        ),
                        header_parameter(
                            "If-None-Match",
                            "Use * when creating a stream set that must not already exist",
                            false,
                        )
                    ],
                    "requestBody": request_body("StreamSetApply"),
                    "responses": {
                        "202": response_with_etag("Stream set accepted", schema_ref("StreamSetAccepted")),
                        "400": error_response("Invalid stream set"),
                        "401": error_response("Authentication failed"),
                        "409": error_response("Stream ownership conflict, or a stream can plan a hop id another can"),
                        "412": error_response("Revision precondition failed"),
                        "428": error_response("A revision precondition is required"),
                        "500": error_response("Persistence or encoding failed"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_STREAM_PLANS: {
                "post": {
                    "operationId": "planStream",
                    "requestBody": request_body("StreamDefinition"),
                    "responses": {
                        "200": response("Stream placement plan", Some(schema_ref("StreamPlan"))),
                        "400": error_response("Invalid stream"),
                        "401": error_response("Authentication failed"),
                        "500": error_response("Encoding failed"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_STATUS: {
                "get": {
                    "operationId": "getStatus",
                    "responses": {
                        "200": response("Control-plane status", Some(schema_ref("StatusResponse"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            }
        }),
        components(&[
            ("ApiError", schema::<ApiError>(Client)),
            ("NodeList", schema::<Vec<NodeResource>>(Client)),
            ("StatusResponse", schema::<StatusResponse>(Client)),
            ("StreamAccepted", schema::<StreamAccepted>(Client)),
            ("StreamDefinition", schema::<StreamDefinition>(Controller)),
            ("StreamEndpoints", schema::<StreamEndpoints>(Client)),
            ("StreamPlan", schema::<StreamPlan>(Client)),
            ("StreamResource", schema::<StreamResource>(Client)),
            ("StreamList", schema::<Vec<StreamResource>>(Client)),
            ("StreamSetAccepted", schema::<StreamSetAccepted>(Client)),
            ("StreamSetApply", schema::<StreamSetApply>(Controller)),
            ("StreamSetResource", schema::<StreamSetResource>(Client)),
            ("StreamSetList", schema::<Vec<StreamSetResource>>(Client)),
        ]),
    )
}

#[must_use]
pub fn southbound_openapi() -> Value {
    use Reader::{Adapter, Controller};
    common_document(
        "open-weave southbound API",
        json!({
            ROUTE_NODES: {
                "get": {
                    "operationId": "listNodes",
                    "responses": {
                        "200": response("Registered nodes", Some(schema_ref("NodeList"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_NODE_REGISTER: {
                "post": {
                    "operationId": "registerNode",
                    "requestBody": request_body("NodeRegistration"),
                    "responses": {
                        "202": response("Node accepted", Some(schema_ref("NodeAccepted"))),
                        "400": error_response("Invalid registration"),
                        "401": error_response("Authentication failed"),
                        "403": error_response("Token belongs to another node"),
                        "409": error_response("Incompatible protocol version"),
                        "500": error_response("Persistence failed"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_NODE_HEARTBEAT: {
                "post": {
                    "operationId": "heartbeatNode",
                    "parameters": [path_parameter("node_id")],
                    "requestBody": request_body("NodeHeartbeat"),
                    "responses": {
                        "202": response("Heartbeat accepted", Some(schema_ref("NodeAccepted"))),
                        "400": error_response("Invalid heartbeat"),
                        "401": error_response("Authentication failed"),
                        "403": error_response("Token belongs to another node"),
                        "404": error_response("Node not found"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_NODE_DESIRED: {
                "get": {
                    "operationId": "getDesiredHops",
                    "parameters": [path_parameter("node_id")],
                    "responses": {
                        "200": response("Desired hops", Some(schema_ref("DesiredHopList"))),
                        "400": error_response("Invalid node id"),
                        "401": error_response("Authentication failed"),
                        "403": error_response("Token belongs to another node"),
                        "404": error_response("Node not reconciled yet"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_ENDPOINTS: {
                "get": {
                    "operationId": "listEndpoints",
                    "responses": {
                        "200": response("Discovered endpoints", Some(schema_ref("EndpointList"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            },
            ROUTE_STATE: {
                "get": {
                    "operationId": "getObservedState",
                    "responses": {
                        "200": response("Observed state", Some(schema_ref("ObservedState"))),
                        "401": error_response("Authentication failed"),
                        "502": error_response("Controller unavailable"),
                        "504": error_response("Controller did not answer in time; it may have applied the request"),
                        "503": error_response("No controller holds the lease")
                    }
                }
            }
        }),
        components(&[
            ("ApiError", schema::<ApiError>(Adapter)),
            ("DesiredHopList", schema::<Vec<DesiredHop>>(Adapter)),
            ("EndpointList", schema::<Vec<EndpointDescriptor>>(Adapter)),
            ("NodeAccepted", schema::<NodeAccepted>(Adapter)),
            ("NodeHeartbeat", schema::<NodeHeartbeat>(Controller)),
            ("NodeList", schema::<Vec<NodeResource>>(Adapter)),
            ("NodeRegistration", schema::<NodeRegistration>(Controller)),
            ("ObservedState", schema::<ObservedState>(Adapter)),
        ]),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeConfig;
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::Path;

    #[test]
    fn committed_artifacts_match_generated_contracts() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        for artifact in artifacts() {
            let actual = std::fs::read_to_string(root.join(artifact.path)).unwrap_or_default();
            assert_eq!(actual, artifact.content, "{} is stale", artifact.path);
        }
    }

    #[test]
    fn the_model_refuses_unknown_fields_at_every_depth() {
        let mut gaps = Vec::new();
        for payload in payloads() {
            gaps.extend(gaps_in(&(payload.schema)(Reader::Controller), payload.path));
        }
        gaps.extend(gaps_in(
            &schema::<NodeConfig>(Reader::Controller),
            "node config",
        ));
        assert!(
            gaps.is_empty(),
            "these objects accept fields they do not name; give each one's type #[serde(deny_unknown_fields)]:\n{}",
            gaps.join("\n")
        );
    }

    fn requests(document: &Value) -> BTreeSet<&str> {
        document["paths"]
            .as_object()
            .unwrap()
            .values()
            .flat_map(|operations| operations.as_object().unwrap().values())
            .filter_map(|operation| {
                operation["requestBody"]["content"]["application/json"]["schema"]["$ref"].as_str()
            })
            .map(|reference| reference.trim_start_matches("#/components/schemas/"))
            .collect()
    }

    #[test]
    fn northbound_responses_and_webhooks_are_open_and_the_rest_closed() {
        let mut problems = Vec::new();
        for payload in payloads() {
            let schema = (payload.schema)(payload.reader);
            problems.extend(match payload.reader {
                Reader::Client => refusals_in(&schema, payload.path),
                Reader::Controller | Reader::Adapter => gaps_in(&schema, payload.path),
            });
        }

        let north = northbound_openapi();
        let south = southbound_openapi();
        assert_eq!(
            requests(&north),
            BTreeSet::from(["StreamDefinition", "StreamSetApply"])
        );
        assert_eq!(
            requests(&south),
            BTreeSet::from(["NodeHeartbeat", "NodeRegistration"])
        );
        for (name, document, responses_open) in
            [("northbound", &north, true), ("southbound", &south, false)]
        {
            let requests = requests(document);
            for (component, schema) in document["components"]["schemas"].as_object().unwrap() {
                let at = format!("{name} {component}");
                problems.extend(
                    if responses_open && !requests.contains(component.as_str()) {
                        refusals_in(schema, &at)
                    } else {
                        gaps_in(schema, &at)
                    },
                );
            }
        }
        assert!(problems.is_empty(), "{}", problems.join("\n"));
    }

    fn refusals_in(schema: &Value, root: &str) -> Vec<String> {
        let mut refusals = Vec::new();
        refused(schema, root, "", &mut refusals);
        refusals
    }

    fn refused(schema: &Value, root: &str, at: &str, refusals: &mut Vec<String>) {
        let Some(object) = schema.as_object() else {
            return;
        };
        let mut refuse = |what: &str| {
            refusals.push(format!(
                "{root}: {}: {what}",
                if at.is_empty() { "." } else { at }
            ));
        };
        for key in ["additionalProperties", "unevaluatedProperties"] {
            if object.get(key) == Some(&Value::Bool(false)) {
                refuse(&format!("{key} is false"));
            }
        }
        if is_enumeration(schema) {
            refuse("a closed enumeration");
            return;
        }
        if object.get("oneOf").is_some_and(is_object_union) {
            refuse("a closed union");
        }
        let branches = object.get("anyOf").and_then(Value::as_array);
        let any_string = json!({ "type": "string" });
        let any_object = json!({ "type": "object" });
        let open_enumeration = branches.is_some_and(|branches| branches.contains(&any_string));
        if let Some(branches) = branches
            && is_object_union(&object["anyOf"])
            && branches
                .iter()
                .filter(|branch| **branch == any_object)
                .count()
                != 1
        {
            refuse("a union without exactly one branch for another object");
        }
        let child = |name: &str| {
            if at.is_empty() {
                name.to_string()
            } else {
                format!("{at}.{name}")
            }
        };
        for (key, value) in object {
            match key.as_str() {
                "properties" => {
                    for (name, property) in value.as_object().into_iter().flatten() {
                        refused(property, root, &child(name), refusals);
                    }
                }
                "items" => refused(value, root, &format!("{at}[]"), refusals),
                "additionalProperties" => refused(value, root, &child("*"), refusals),
                "anyOf" | "oneOf" | "allOf" => {
                    for branch in value.as_array().into_iter().flatten() {
                        if !(open_enumeration && is_enumeration(branch)) {
                            refused(branch, root, at, refusals);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn gaps_in(schema: &Value, root: &str) -> Vec<String> {
        let mut gaps = Vec::new();
        unclosed(schema, root, "", false, &mut gaps);
        gaps
    }

    fn unclosed(schema: &Value, root: &str, at: &str, closed: bool, gaps: &mut Vec<String>) {
        let Some(object) = schema.as_object() else {
            return;
        };
        let refuses = |key: &str| object.get(key) == Some(&Value::Bool(false));
        if object.contains_key("properties")
            && !closed
            && !refuses("additionalProperties")
            && !refuses("unevaluatedProperties")
        {
            gaps.push(format!("{root}: {}", if at.is_empty() { "." } else { at }));
        }
        let child = |name: &str| {
            if at.is_empty() {
                name.to_string()
            } else {
                format!("{at}.{name}")
            }
        };
        for (key, value) in object {
            match key.as_str() {
                "properties" => {
                    for (name, property) in value.as_object().into_iter().flatten() {
                        unclosed(property, root, &child(name), false, gaps);
                    }
                }
                "items" => unclosed(value, root, &format!("{at}[]"), false, gaps),
                "additionalProperties" => unclosed(value, root, &child("*"), false, gaps),
                "anyOf" | "oneOf" | "allOf" => {
                    for branch in value.as_array().into_iter().flatten() {
                        unclosed(branch, root, at, refuses("unevaluatedProperties"), gaps);
                    }
                }
                _ => {}
            }
        }
    }

    #[test]
    fn openapi_documents_cover_the_api_routes() {
        let north = northbound_openapi();
        let south = southbound_openapi();
        assert_eq!(north["openapi"], "3.1.0");
        assert_eq!(south["openapi"], "3.1.0");
        assert_eq!(north["security"][0]["bearerAuth"], json!([]));
        assert_eq!(south["security"][0]["bearerAuth"], json!([]));
        let mut stream_methods: Vec<&str> = north["paths"]["/streams/{name}"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        stream_methods.sort_unstable();
        assert_eq!(stream_methods, ["delete", "get"]);

        let north_paths: BTreeMap<_, _> = north["paths"].as_object().unwrap().iter().collect();
        assert_eq!(
            north_paths.keys().copied().collect::<Vec<_>>(),
            [
                "/nodes",
                "/status",
                "/stream-plans",
                "/stream-sets",
                "/stream-sets/{owner}",
                "/streams",
                "/streams/{name}",
                "/streams/{name}/endpoints"
            ]
        );

        let south_paths: BTreeMap<_, _> = south["paths"].as_object().unwrap().iter().collect();
        assert_eq!(
            south_paths.keys().copied().collect::<Vec<_>>(),
            [
                "/endpoints",
                "/nodes",
                "/nodes/register",
                "/nodes/{node_id}/desired",
                "/nodes/{node_id}/heartbeat",
                "/state"
            ]
        );
    }

    #[test]
    fn every_proxied_operation_can_answer_that_no_controller_leads() {
        for document in [northbound_openapi(), southbound_openapi()] {
            for (path, operations) in document["paths"].as_object().unwrap() {
                for (method, operation) in operations.as_object().unwrap() {
                    assert!(
                        operation["responses"]["503"].is_object(),
                        "{method} {path} lists no 503"
                    );
                    assert!(
                        operation["responses"]["504"].is_object(),
                        "{method} {path} lists no 504"
                    );
                }
            }
        }
    }

    #[test]
    fn stream_schema_carries_the_resource_id_rule() {
        let schema =
            serde_json::to_string(&schema::<StreamDefinition>(Reader::Controller)).unwrap();
        assert!(schema.contains("maxLength"));
        assert!(schema.contains("^[a-z0-9]"));
    }

    #[test]
    fn northbound_contract_exposes_node_inventory() {
        let document = northbound_openapi();
        let nodes = &document["paths"]["/nodes"]["get"];

        assert_eq!(nodes["operationId"], "listNodes");
        assert_eq!(
            nodes["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/NodeList"
        );
        assert!(nodes["responses"]["401"].is_object());
        assert!(nodes["responses"]["502"].is_object());
        assert!(document["components"]["schemas"]["NodeList"].is_object());
    }

    #[test]
    fn northbound_contract_exposes_resource_revisions() {
        let document = northbound_openapi();
        let apply = &document["paths"]["/streams"]["post"];
        let stream = &document["paths"]["/streams/{name}"];

        assert_eq!(
            stream["get"]["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/StreamResource"
        );
        assert!(stream["get"]["responses"]["200"]["headers"]["ETag"].is_object());
        assert!(apply["responses"]["202"]["headers"]["ETag"].is_object());
        assert!(apply["responses"]["412"].is_object());
        assert!(apply["responses"]["428"].is_object());
        assert_eq!(stream["delete"]["parameters"][1]["name"], "If-Match");
        assert_eq!(stream["delete"]["parameters"][1]["required"], true);
    }

    #[test]
    fn northbound_contract_exposes_atomic_stream_sets() {
        let document = northbound_openapi();
        let sets = &document["paths"]["/stream-sets"];
        let set = &document["paths"]["/stream-sets/{owner}"];

        assert!(sets["get"].is_object());
        assert_eq!(set["get"]["parameters"][0]["name"], "owner");
        assert!(set["get"]["responses"]["200"]["headers"]["ETag"].is_object());
        assert_eq!(
            set["put"]["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/StreamSetApply"
        );
        assert!(set["put"]["responses"]["202"]["headers"]["ETag"].is_object());
        assert!(set["put"]["responses"]["409"].is_object());
        assert!(set["put"]["responses"]["412"].is_object());
        assert!(set["put"]["responses"]["428"].is_object());
    }

    #[test]
    fn condition_transition_time_is_an_rfc3339_schema_string() {
        let schema = serde_json::to_value(schema::<StatusResponse>(Reader::Client)).unwrap();
        let rendered = serde_json::to_string(&schema).unwrap();
        assert!(rendered.contains("last_transition_time"));
        assert!(rendered.contains("date-time"));
    }
}
