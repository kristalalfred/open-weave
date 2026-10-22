use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use weave_core::auth::{Guard, NodeGuard, NodeKey, Token};
use weave_core::webhook::Subject;
use weave_core::{NodeRegistration, PROTOCOL_VERSION};

use crate::keys::LinkKeys;
use crate::store::{MemStore, StateStore};
use crate::webhook::{self, tests::sink};
use crate::{AppState, reconcile_tick, router};

const NORTH: &str = "north-test-token";
const KEY: &str = "south-test-key-0123456789abcdef0123";

fn observation(address: &str, public: &str, port: u16) -> Value {
    json!({
        "uplink": {
            "interface": "en0",
            "address": address,
            "prefix_len": 24,
            "gateway": "02:00:00:00:00:01",
            "reflexive": [{ "server": "192.0.2.1:3478", "mapped": format!("{public}:{port}") }]
        }
    })
}

fn registration(id: &str, observation: Option<Value>) -> Value {
    let mut registration = json!({
        "protocol_version": PROTOCOL_VERSION,
        "node": {
            "id": id,
            "endpoint": format!("http://{id}:8092"),
            "status": "ready",
            "capabilities": {
                "adapters": [],
                "hop_profiles": [{
                    "id": "srt-forward",
                    "ingress": { "transport": "srt", "roles": ["listen", "connect"] },
                    "egress": { "transport": "srt", "roles": ["listen", "connect"] }
                }]
            },
            "topology": {
                "site": {
                    "id": "lan",
                    "listeners": { "srt": { "port_range": { "start": 23000, "end": 23099 } } }
                }
            }
        },
        "endpoints": [],
        "hop_status": []
    });
    if let Some(observation) = observation {
        registration["observation"] = observation;
    }
    registration
}

fn heartbeat(id: &str, observation: Option<Value>) -> Value {
    let mut heartbeat = json!({
        "node_id": id,
        "status": "ready",
        "inputs": [],
        "outputs": [],
        "endpoints": [],
        "hop_status": []
    });
    if let Some(observation) = observation {
        heartbeat["observation"] = observation;
    }
    heartbeat
}

async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    authorization: Option<&str>,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(authorization) = authorization {
        request = request.header(header::AUTHORIZATION, authorization);
    }
    if method == "POST" && uri == "/streams" {
        request = request.header(header::IF_NONE_MATCH, "*");
    }
    let request = request
        .body(body.map_or_else(Body::empty, |body| Body::from(body.to_string())))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn hydrated(store: Arc<MemStore>, webhooks: Option<Arc<webhook::Emitter>>) -> AppState {
    AppState::hydrate(
        store,
        Duration::from_secs(15),
        Duration::from_secs(300),
        webhooks,
        LinkKeys::for_tests(),
    )
    .await
    .unwrap()
}

fn open(state: &AppState) -> Router {
    router(state.clone(), Guard::Disabled, NodeGuard::Disabled)
}

async fn accepted(app: &Router, uri: &str, body: Value) {
    let (status, body) = send(app, "POST", uri, None, Some(body)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{uri}: {body}");
}

async fn listed(app: &Router, id: &str) -> Value {
    let (status, nodes) = send(app, "GET", "/nodes", None, None).await;
    assert_eq!(status, StatusCode::OK);
    nodes
        .as_array()
        .unwrap()
        .iter()
        .find(|resource| resource["node"]["id"] == id)
        .cloned()
        .unwrap_or_else(|| panic!("{id} in {nodes}"))
}

fn networks(node: &Value) -> Vec<&str> {
    node["topology"]["attachments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|attachment| attachment["network"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn a_heartbeat_without_an_observation_clears_the_site() {
    let state = hydrated(Arc::new(MemStore::new()), None).await;
    let app = open(&state);
    accepted(
        &app,
        "/nodes/register",
        registration(
            "mac-a",
            Some(observation("192.168.1.20", "203.0.113.7", 61_000)),
        ),
    )
    .await;
    let resolved = listed(&app, "mac-a").await;
    assert_eq!(resolved["site"]["state"], "resolved");
    assert_eq!(resolved["site"]["subnet"], "192.168.1.0/24");
    assert_eq!(resolved["site"]["public"], json!(["203.0.113.7"]));
    assert_eq!(networks(&resolved["node"]), [&resolved["site"]["network"]]);

    accepted(&app, "/nodes/mac-a/heartbeat", heartbeat("mac-a", None)).await;

    let cleared = listed(&app, "mac-a").await;
    assert_eq!(cleared["site"], json!({ "state": "not_observed" }));
    assert!(networks(&cleared["node"]).is_empty());
}

#[tokio::test]
async fn a_moved_uplink_moves_the_host_its_peer_dials() {
    let state = hydrated(Arc::new(MemStore::new()), None).await;
    let app = open(&state);
    for (id, address) in [("mac-a", "192.168.1.20"), ("mac-b", "192.168.1.31")] {
        accepted(
            &app,
            "/nodes/register",
            registration(id, Some(observation(address, "203.0.113.7", 61_000))),
        )
        .await;
    }
    accepted(
        &app,
        "/streams",
        json!({
            "name": "music",
            "source": { "srt": { "node": "mac-a" } },
            "destinations": [{ "id": "studio", "srt": { "node": "mac-b" } }]
        }),
    )
    .await;
    assert_eq!(dialled_host(&state, &app).await, "192.168.1.31");

    accepted(
        &app,
        "/nodes/mac-b/heartbeat",
        heartbeat(
            "mac-b",
            Some(observation("192.168.1.44", "203.0.113.7", 61_000)),
        ),
    )
    .await;

    assert_eq!(dialled_host(&state, &app).await, "192.168.1.44");
}

async fn dialled_host(state: &AppState, app: &Router) -> String {
    reconcile_tick(state).await;
    let (status, hops) = send(app, "GET", "/nodes/mac-a/desired", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let egress = &hops[0]["egresses"][0];
    assert_eq!(egress["role"], "connect", "{hops}");
    egress["host"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn nodes_state_view_and_webhooks_carry_one_site_network() {
    let mut sink = sink(StatusCode::OK).await;
    let emitter = webhook::Emitter::new(webhook::Config {
        url: Some(sink.url.clone()),
        ..webhook::Config::default()
    })
    .map(Arc::new);
    let state = hydrated(Arc::new(MemStore::new()), emitter).await;
    let app = open(&state);
    accepted(
        &app,
        "/nodes/register",
        registration(
            "mac-a",
            Some(observation("192.168.1.20", "203.0.113.7", 61_000)),
        ),
    )
    .await;
    reconcile_tick(&state).await;

    let listed = listed(&app, "mac-a").await;
    let network = listed["site"]["network"].as_str().unwrap().to_string();
    assert!(network.starts_with("site-"), "{network}");
    let (_, observed) = send(&app, "GET", "/state", None, None).await;
    let (_, view) = send(&app, "GET", "/view", None, None).await;
    let Subject::Node(summary) = sink.next().await.event.subject else {
        panic!("a node event");
    };
    let summary = serde_json::to_value(summary).unwrap();

    for node in [
        &listed["node"],
        &observed["nodes"][0],
        &view["nodes"][0],
        &summary,
    ] {
        assert_eq!(networks(node), [network.as_str()], "{node}");
        assert_eq!(
            node["topology"]["attachments"][0]["listeners"]["srt"]["host"],
            "192.168.1.20"
        );
    }
    for served in [&observed, &view] {
        assert!(!served.to_string().contains("203.0.113.7"), "{served}");
    }
}

#[tokio::test]
async fn a_site_change_is_stored_and_a_new_mapped_port_is_not() {
    let store = Arc::new(MemStore::new());
    let state = hydrated(store.clone(), None).await;
    let app = open(&state);
    accepted(
        &app,
        "/nodes/register",
        registration(
            "mac-a",
            Some(observation("192.168.1.20", "203.0.113.7", 61_000)),
        ),
    )
    .await;
    let first = listed(&app, "mac-a").await["site"]["network"].clone();
    assert_eq!(store.upsert_node_calls(), 1);

    accepted(
        &app,
        "/nodes/mac-a/heartbeat",
        heartbeat(
            "mac-a",
            Some(observation("192.168.1.20", "203.0.113.7", 62_000)),
        ),
    )
    .await;
    assert_eq!(store.upsert_node_calls(), 1);
    assert_eq!(listed(&app, "mac-a").await["site"]["network"], first);

    accepted(
        &app,
        "/nodes/mac-a/heartbeat",
        heartbeat(
            "mac-a",
            Some(observation("192.168.1.20", "198.51.100.4", 61_000)),
        ),
    )
    .await;
    assert_eq!(store.upsert_node_calls(), 2);
    let moved = listed(&app, "mac-a").await["site"]["network"].clone();
    assert_ne!(moved, first);

    let restarted = open(&hydrated(store, None).await);
    let after_restart = listed(&restarted, "mac-a").await;
    assert_eq!(after_restart["site"]["network"], moved);
    assert_eq!(after_restart["site"]["public"], json!(["198.51.100.4"]));
}

#[tokio::test]
async fn a_declared_network_cannot_take_the_site_prefix() {
    let state = hydrated(Arc::new(MemStore::new()), None).await;
    let app = open(&state);
    let mut body = registration("strom-node-1", None);
    body["node"]["topology"] = json!({
        "attachments": [{ "id": "wan", "network": "site-3f9a2c1d7e4b", "dial": true }]
    });

    let (status, error) = send(&app, "POST", "/nodes/register", None, Some(body)).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error["details"][0]["field"],
        "node.topology.attachments[0].network"
    );
    assert_eq!(error["details"][0]["code"], "reserved");
    let (_, nodes) = send(&app, "GET", "/nodes", None, None).await;
    assert_eq!(nodes, json!([]));
}

#[tokio::test]
async fn an_impossible_observation_is_refused_at_every_entry() {
    let store = Arc::new(MemStore::new());
    let state = hydrated(store.clone(), None).await;
    let app = open(&state);
    let loopback = observation("127.0.0.1", "203.0.113.7", 61_000);

    let (status, error) = send(
        &app,
        "POST",
        "/nodes/register",
        None,
        Some(registration("mac-a", Some(loopback.clone()))),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["details"][0]["field"], "observation.uplink.address");

    accepted(&app, "/nodes/register", registration("mac-a", None)).await;
    let (status, error) = send(
        &app,
        "POST",
        "/nodes/mac-a/heartbeat",
        None,
        Some(heartbeat("mac-a", Some(loopback.clone()))),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(error["details"][0]["field"], "observation.uplink.address");

    let stored: NodeRegistration =
        serde_json::from_value(registration("mac-b", Some(loopback))).unwrap();
    store.upsert_node(&stored).await.unwrap();
    let restarted = open(&hydrated(store, None).await);
    let (_, nodes) = send(&restarted, "GET", "/nodes", None, None).await;
    assert_eq!(
        nodes
            .as_array()
            .unwrap()
            .iter()
            .map(|resource| resource["node"]["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["mac-a"]
    );
}

#[tokio::test]
async fn node_tokens_read_nodes_without_sites() {
    let state = hydrated(Arc::new(MemStore::new()), None).await;
    let key = NodeKey::new(KEY).unwrap();
    let app = router(
        state,
        Guard::Required(Token::new(NORTH).unwrap()),
        NodeGuard::Required(key.clone()),
    );
    let node_bearer = |id: &str| format!("Bearer {}", key.token_for(id, 0));
    let (status, _) = send(
        &app,
        "POST",
        "/nodes/register",
        Some(&node_bearer("mac-a")),
        Some(registration(
            "mac-a",
            Some(observation("192.168.1.20", "203.0.113.7", 61_000)),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let (_, operator) = send(
        &app,
        "GET",
        "/nodes",
        Some(&format!("Bearer {NORTH}")),
        None,
    )
    .await;
    let (_, page) = send(
        &app,
        "GET",
        "/nodes",
        Some(&node_bearer("browser-a1b2")),
        None,
    )
    .await;

    assert_eq!(operator[0]["site"]["public"], json!(["203.0.113.7"]));
    assert!(page[0].get("site").is_none(), "{page}");
    assert_eq!(page[0]["node"], operator[0]["node"]);
    assert!(!page.to_string().contains("203.0.113.7"), "{page}");
}

fn spaghetti_mac(id: &str, address: &str, public: &str) -> Value {
    let mut mac = registration(id, Some(observation(address, public, 61_000)));
    mac["node"]["capabilities"]["hop_profiles"] = json!([
        {
            "id": "app-audio-to-srt",
            "ingress": "input",
            "egress": { "transport": "srt", "roles": ["listen", "connect"] },
            "max_egresses": 8
        },
        {
            "id": "srt-to-speaker",
            "ingress": { "transport": "srt", "roles": ["listen", "connect"] },
            "egress": "output",
            "max_egresses": 1,
            "accepts": { "container": ["mpeg_ts"], "audio": { "codec": ["aac"] } }
        }
    ]);
    mac["node"]["inputs"] = json!([{ "id": "music", "label": "Music", "tracks": ["audio"] }]);
    mac["node"]["outputs"] = json!([{ "id": "speakers", "label": "Speakers" }]);
    mac
}

fn strom_relay() -> Value {
    let srt = json!({ "transport": "srt", "roles": ["listen", "connect"] });
    json!({
        "protocol_version": PROTOCOL_VERSION,
        "node": {
            "id": "strom-node-1",
            "endpoint": "http://10.97.26.11:8091",
            "status": "ready",
            "capabilities": {
                "adapters": [{ "name": "strom", "kind": "strom" }],
                "hop_profiles": [{
                    "id": "srt-forward",
                    "ingress": srt,
                    "egress": srt,
                    "media_behavior": "preserve",
                    "accepts": {}
                }]
            },
            "topology": {
                "attachments": [{
                    "id": "a-routed",
                    "network": "internet",
                    "dial": true,
                    "listeners": {
                        "srt": { "host": "10.97.26.10", "port_range": { "start": 20000, "end": 20999 } }
                    }
                }]
            }
        },
        "endpoints": [],
        "hop_status": []
    })
}

async fn music_to_filip(public_a: &str, public_b: &str) -> (AppState, Router) {
    let state = hydrated(Arc::new(MemStore::new()), None).await;
    let app = open(&state);
    for node in [
        spaghetti_mac("alfreds-mac", "192.168.1.20", public_a),
        spaghetti_mac("filips-mac", "192.168.1.31", public_b),
        strom_relay(),
    ] {
        accepted(&app, "/nodes/register", node).await;
    }
    accepted(
        &app,
        "/streams",
        json!({
            "name": "music-to-filip",
            "source": { "input": { "node": "alfreds-mac", "id": "music" } },
            "destinations": [{
                "id": "filip",
                "output": { "node": "filips-mac", "id": "speakers" }
            }]
        }),
    )
    .await;
    reconcile_tick(&state).await;
    (state, app)
}

async fn placement(app: &Router) -> Value {
    let (status, body) = send(app, "GET", "/status", None, None).await;
    assert_eq!(status, StatusCode::OK);
    body["streams"][0]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == "placement_ready")
        .cloned()
        .unwrap_or_else(|| panic!("a placement condition in {body}"))
}

async fn desired(app: &Router, node: &str) -> Value {
    let (status, hops) = send(app, "GET", &format!("/nodes/{node}/desired"), None, None).await;
    assert_eq!(status, StatusCode::OK);
    hops
}

#[tokio::test]
async fn sites_behind_different_public_addresses_are_not_bridged_through_a_relay() {
    let (_state, app) = music_to_filip("203.0.113.7", "198.51.100.4").await;

    let placement = placement(&app).await;
    assert_eq!(placement["status"], "false", "{placement}");
    assert_eq!(placement["reason"], "placement_failed", "{placement}");
    for node in ["alfreds-mac", "filips-mac", "strom-node-1"] {
        assert_eq!(desired(&app, node).await, json!([]), "{node}");
    }
}

#[tokio::test]
async fn sites_behind_one_public_address_are_linked_directly() {
    let (_state, app) = music_to_filip("203.0.113.7", "203.0.113.7").await;

    let placement = placement(&app).await;
    assert_eq!(placement["status"], "true", "{placement}");
    let sender = desired(&app, "alfreds-mac").await;
    let egress = &sender[0]["egresses"][0];
    assert_eq!(egress["role"], "connect", "{sender}");
    assert_eq!(egress["host"], "192.168.1.31", "{sender}");
    assert_eq!(
        desired(&app, "filips-mac").await[0]["egresses"][0]["transport"],
        "output"
    );
    assert_eq!(desired(&app, "strom-node-1").await, json!([]));
}
