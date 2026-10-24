use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use weave_core::auth::{Guard, NodeGuard};
use weave_core::{DesiredHop, PROTOCOL_VERSION};

use crate::keys::{LinkKeys, TURN_CREDENTIAL_VAR, TURN_USERNAME_VAR};
use crate::store::MemStore;
use crate::{AppState, reconcile_tick, router};

const OFFICE: &str = "203.0.113.7";
const HOME: &str = "198.51.100.4";
const CAFE: &str = "192.0.2.77";
const RELAY_HOST: &str = "192.168.1.10";
const TURN: &str = "192.0.2.50:3478";
const LINK: &str = "weave-music-to-filip-receiver-filip";
const OFFER: &str = "v=0\r\na=candidate:1 1 udp 2130706431 192.168.1.20 50000 typ host\r\na=candidate:2 1 udp 16777215 192.0.2.50 3478 typ relay raddr 203.0.113.7 rport 61000\r\n";
const OTHER_OFFER: &str = "v=0\r\na=candidate:1 1 udp 2130706431 192.168.1.20 50002 typ host\r\na=candidate:2 1 udp 16777215 192.0.2.50 3478 typ relay raddr 203.0.113.7 rport 61002\r\n";
const OFFER_WITHOUT_TURN: &str =
    "v=0\r\na=candidate:1 1 udp 2130706431 192.168.1.20 50000 typ host\r\n";
const ANSWER: &str = "v=0\r\na=candidate:1 1 udp 2130706431 192.168.1.31 50100 typ host\r\na=candidate:2 1 udp 16777215 192.0.2.50 3478 typ relay raddr 198.51.100.4 rport 62000\r\n";
const DAY: u64 = 24 * 60 * 60;

struct Mac {
    id: &'static str,
    address: &'static str,
    public: &'static str,
}

const ALFRED: Mac = Mac {
    id: "alfreds-mac",
    address: "192.168.1.20",
    public: OFFICE,
};
const ANNA: Mac = Mac {
    id: "annas-mac",
    address: "192.168.1.21",
    public: OFFICE,
};
const FILIP: Mac = Mac {
    id: "filips-mac",
    address: "192.168.1.31",
    public: HOME,
};

async fn send(app: &Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
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

async fn accepted(app: &Router, uri: &str, body: Value) {
    let (status, body) = send(app, "POST", uri, Some(body)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{uri}: {body}");
}

async fn controller(store: Arc<MemStore>, keys: LinkKeys) -> (AppState, Router) {
    let state = AppState::hydrate(
        store,
        Duration::from_secs(15),
        Duration::from_secs(300),
        None,
        keys,
    )
    .await
    .unwrap();
    let app = router(state.clone(), Guard::Disabled, NodeGuard::Disabled);
    (state, app)
}

fn observation(address: &str, public: &str) -> Value {
    json!({
        "uplink": {
            "interface": "en0",
            "address": address,
            "prefix_len": 24,
            "gateway": "02:00:00:00:00:01",
            "reflexive": [{ "server": "192.0.2.1:3478", "mapped": format!("{public}:61000") }]
        }
    })
}

fn registration(mac: &Mac) -> Value {
    let srt = json!({ "transport": "srt", "roles": ["listen", "connect"] });
    json!({
        "protocol_version": PROTOCOL_VERSION,
        "node": {
            "id": mac.id,
            "endpoint": format!("http://{}:8092", mac.id),
            "status": "ready",
            "capabilities": {
                "hop_profiles": [
                    {
                        "id": "app-audio-out",
                        "ingress": "input",
                        "egress": [srt, { "transport": "webrtc" }],
                        "max_egresses": 8
                    },
                    {
                        "id": "webrtc-to-speaker",
                        "ingress": { "transport": "webrtc" },
                        "egress": "output",
                        "max_egresses": 1,
                        "accepts": { "container": ["rtp"], "audio": { "codec": ["opus"] } }
                    },
                    {
                        "id": "srt-to-speaker",
                        "ingress": srt,
                        "egress": "output",
                        "max_egresses": 1,
                        "accepts": { "container": ["mpeg_ts"], "audio": { "codec": ["aac"] } }
                    }
                ]
            },
            "topology": {
                "site": {
                    "id": "lan",
                    "listeners": { "srt": { "port_range": { "start": 23000, "end": 23099 } } }
                },
                "attachments": [{ "id": "relay", "network": "bench-lan", "dial": true }]
            },
            "inputs": [{ "id": "music", "label": "Music", "tracks": ["audio"] }],
            "outputs": [{ "id": "speakers", "label": "Speakers" }]
        },
        "observation": observation(mac.address, mac.public),
        "endpoints": [],
        "hop_status": []
    })
}

fn relay() -> Value {
    let srt = json!({ "transport": "srt", "roles": ["listen", "connect"] });
    json!({
        "protocol_version": PROTOCOL_VERSION,
        "node": {
            "id": "weave-relay",
            "endpoint": "http://weave-relay:8091",
            "status": "ready",
            "capabilities": {
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
                    "id": "lan",
                    "network": "bench-lan",
                    "dial": true,
                    "listeners": {
                        "srt": { "host": RELAY_HOST, "port_range": { "start": 24000, "end": 24099 } }
                    }
                }]
            }
        },
        "endpoints": [],
        "hop_status": []
    })
}

fn music_to_filip() -> Value {
    json!({
        "name": "music-to-filip",
        "source": { "input": { "node": "alfreds-mac", "id": "music" } },
        "destinations": [{ "id": "filip", "output": { "node": "filips-mac", "id": "speakers" } }]
    })
}

async fn bench(store: Arc<MemStore>, keys: LinkKeys, with_relay: bool) -> (AppState, Router) {
    let (state, app) = controller(store, keys).await;
    accepted(&app, "/nodes/register", registration(&ALFRED)).await;
    accepted(&app, "/nodes/register", registration(&FILIP)).await;
    if with_relay {
        accepted(&app, "/nodes/register", relay()).await;
    }
    accepted(&app, "/streams", music_to_filip()).await;
    reconcile_tick(&state).await;
    (state, app)
}

async fn desired(app: &Router, node: &str) -> Value {
    let (status, hops) = send(app, "GET", &format!("/nodes/{node}/desired"), None).await;
    assert_eq!(status, StatusCode::OK, "{hops}");
    hops
}

async fn hop(app: &Router, node: &str) -> Value {
    desired(app, node).await[0].clone()
}

fn link_socket(hop: &Value) -> Value {
    if hop["role"] == "sender" {
        hop["egresses"]
            .as_array()
            .unwrap()
            .iter()
            .find(|egress| egress["branch_id"] == "filip")
            .cloned()
            .unwrap_or_else(|| panic!("a filip egress in {hop}"))
    } else {
        hop["ingress"].clone()
    }
}

async fn sent(app: &Router) -> Value {
    link_socket(&hop(app, "alfreds-mac").await)
}

fn generation(socket: &Value) -> u64 {
    socket["generation"]
        .as_u64()
        .unwrap_or_else(|| panic!("a generation in {socket}"))
}

fn at_generation(hop: &Value, generation: u64) -> Value {
    let mut hop = hop.clone();
    if hop["role"] == "sender" {
        for egress in hop["egresses"].as_array_mut().unwrap() {
            if egress["branch_id"] == "filip" {
                egress["generation"] = json!(generation);
            }
        }
    } else {
        hop["ingress"]["generation"] = json!(generation);
    }
    hop
}

async fn placement(app: &Router) -> (Value, String) {
    let (_, body) = send(app, "GET", "/status", None).await;
    let condition = body["streams"][0]["conditions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|condition| condition["type"] == "placement_ready")
        .cloned()
        .unwrap_or_else(|| panic!("a placement condition in {body}"));
    let detail = condition["detail"].as_str().unwrap().to_string();
    (condition, detail)
}

async fn detail(app: &Router) -> String {
    placement(app).await.1
}

async fn signal(app: &Router, node: &str, local: &[(u64, &str)]) -> Value {
    let local: Vec<Value> = local
        .iter()
        .map(|(generation, sdp)| json!({ "link": LINK, "generation": generation, "description": sdp }))
        .collect();
    let (status, reply) = send(
        app,
        "POST",
        &format!("/nodes/{node}/signalling"),
        Some(json!({ "local": local })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    reply
}

fn reply(remote: &[(u64, &str)], refused: &[u64]) -> Value {
    json!({
        "remote": remote
            .iter()
            .map(|(generation, sdp)| json!({ "link": LINK, "generation": generation, "description": sdp }))
            .collect::<Vec<_>>(),
        "refused": refused
            .iter()
            .map(|generation| json!({ "link": LINK, "generation": generation }))
            .collect::<Vec<_>>()
    })
}

async fn signal_both(app: &Router, generation: u64) {
    signal(app, "alfreds-mac", &[(generation, OFFER)]).await;
    signal(app, "filips-mac", &[]).await;
    signal(app, "filips-mac", &[(generation, ANSWER)]).await;
    assert_eq!(
        signal(app, "alfreds-mac", &[(generation, OFFER)]).await,
        reply(&[(generation, ANSWER)], &[])
    );
}

struct Path {
    remote: Option<&'static str>,
    relayed: bool,
}

const UNKNOWN: Path = Path {
    remote: None,
    relayed: false,
};

async fn report(app: &Router, mac: &Mac, runs: &Value, condition: &str, path: Path) {
    let revision = serde_json::from_value::<DesiredHop>(runs.clone())
        .unwrap()
        .revision();
    let socket = link_socket(runs);
    let mut status = json!({
        "condition": condition,
        "peer": { "link": socket["link"], "generation": socket["generation"], "relayed": path.relayed }
    });
    if let Some(remote) = path.remote {
        let (host, port) = remote.split_once(':').unwrap();
        status["resolved"] = json!({ "host": host, "port": port.parse::<u16>().unwrap() });
    }
    let (ingress, mut egress) = if runs["role"] == "sender" {
        (json!({ "condition": "flowing" }), status)
    } else {
        (status, json!({ "condition": "flowing" }))
    };
    egress["branch_id"] = json!("filip");
    heartbeat(
        app,
        mac,
        mac.public,
        json!([{
            "id": runs["id"],
            "node_id": mac.id,
            "revision": revision,
            "state": "provisioned",
            "ingress": ingress,
            "egresses": [egress]
        }]),
    )
    .await;
}

async fn heartbeat(app: &Router, mac: &Mac, public: &str, hop_status: Value) {
    accepted(
        app,
        &format!("/nodes/{}/heartbeat", mac.id),
        json!({
            "node_id": mac.id,
            "status": "ready",
            "inputs": [{ "id": "music", "label": "Music", "tracks": ["audio"] }],
            "outputs": [{ "id": "speakers", "label": "Speakers" }],
            "observation": observation(mac.address, public),
            "endpoints": [],
            "hop_status": hop_status
        }),
    )
    .await;
}

async fn both_report(app: &Router, sender: &Value, receiver: &Value, condition: &str) {
    report(app, &ALFRED, sender, condition, UNKNOWN).await;
    report(app, &FILIP, receiver, condition, UNKNOWN).await;
}

#[tokio::test]
async fn sites_link_over_webrtc_with_the_descriptions_relayed_through_the_signalling_route() {
    let (state, app) = bench(Arc::new(MemStore::new()), LinkKeys::for_tests(), true).await;

    let sender = hop(&app, "alfreds-mac").await;
    let receiver = hop(&app, "filips-mac").await;
    let socket = link_socket(&sender);
    let g = generation(&socket);
    assert_eq!(socket["transport"], "webrtc", "{socket}");
    assert_eq!(socket["link"], LINK, "{socket}");
    assert_eq!(socket["stun"], json!(["192.0.2.60:3478"]), "{socket}");
    assert_eq!(socket["turn"][0]["address"], TURN, "{socket}");
    assert_eq!(socket["turn"][0]["username"], format!("{}:{LINK}", g + DAY));
    let mut same = receiver["ingress"].clone();
    same["branch_id"] = json!("filip");
    assert_eq!(same, socket);
    assert_eq!(sender["profile_id"], "app-audio-out");
    assert_eq!(receiver["profile_id"], "webrtc-to-speaker");
    assert_eq!(desired(&app, "weave-relay").await, json!([]));
    assert!(
        detail(&app)
            .await
            .contains("webrtc link alfreds-mac to filips-mac, connecting"),
        "{}",
        detail(&app).await
    );

    assert_eq!(
        signal(&app, "alfreds-mac", &[(g, OFFER)]).await,
        reply(&[], &[])
    );
    assert_eq!(
        signal(&app, "filips-mac", &[]).await,
        reply(&[(g, OFFER)], &[])
    );
    assert_eq!(
        signal(&app, "filips-mac", &[(g, ANSWER)]).await,
        reply(&[(g, OFFER)], &[])
    );
    assert_eq!(
        signal(&app, "alfreds-mac", &[(g, OFFER)]).await,
        reply(&[(g, ANSWER)], &[])
    );

    report(
        &app,
        &ALFRED,
        &sender,
        "flowing",
        Path {
            remote: Some("198.51.100.4:62000"),
            relayed: false,
        },
    )
    .await;
    report(
        &app,
        &FILIP,
        &receiver,
        "flowing",
        Path {
            remote: Some("203.0.113.7:61000"),
            relayed: false,
        },
    )
    .await;
    reconcile_tick(&state).await;
    assert!(
        detail(&app)
            .await
            .contains("media direct from 198.51.100.4:62000"),
        "{}",
        detail(&app).await
    );
    assert_eq!(hop(&app, "alfreds-mac").await, sender);

    report(
        &app,
        &FILIP,
        &receiver,
        "flowing",
        Path {
            remote: Some(TURN),
            relayed: true,
        },
    )
    .await;
    reconcile_tick(&state).await;
    assert!(
        detail(&app).await.contains("media relayed through TURN"),
        "{}",
        detail(&app).await
    );
    assert_eq!(hop(&app, "alfreds-mac").await, sender);
}

#[tokio::test]
async fn descriptions_reach_only_the_two_ends_and_no_shared_route() {
    let (state, app) = bench(Arc::new(MemStore::new()), LinkKeys::for_tests(), true).await;
    let g = generation(&sent(&app).await);
    let credential = sent(&app).await["turn"][0]["credential"]
        .as_str()
        .unwrap()
        .to_string();

    assert_eq!(
        signal(&app, "weave-relay", &[(g, "v=0 forged")]).await,
        reply(&[], &[])
    );
    assert_eq!(signal(&app, "filips-mac", &[]).await, reply(&[], &[]));
    signal_both(&app, g).await;
    assert_eq!(signal(&app, "weave-relay", &[]).await, reply(&[], &[]));

    reconcile_tick(&state).await;
    let (_, plan) = send(&app, "POST", "/stream-plans", Some(music_to_filip())).await;
    assert_eq!(plan["hops"][0]["egresses"][0]["link"], LINK, "{plan}");
    assert_eq!(plan["hops"][0]["egresses"][0]["turn"], json!([]), "{plan}");
    let view = send(&app, "GET", "/view", None).await.1;
    assert!(view.to_string().contains(LINK), "{view}");
    for (route, body) in [
        ("/view", view),
        ("/state", send(&app, "GET", "/state", None).await.1),
        ("/nodes", send(&app, "GET", "/nodes", None).await.1),
        ("/status", send(&app, "GET", "/status", None).await.1),
        ("/stream-plans", plan),
    ] {
        let text = body.to_string();
        for secret in [OFFER, ANSWER, &credential, "forged"] {
            let shown = serde_json::to_string(secret).unwrap();
            let shown = shown.trim_matches('"');
            assert!(!text.contains(shown), "{route} shows {secret}: {text}");
        }
    }
}

#[tokio::test]
async fn a_conflicting_description_is_refused_without_the_answer_and_restarts_the_generation() {
    let (state, app) = bench(Arc::new(MemStore::new()), LinkKeys::for_tests(), true).await;
    let sender = hop(&app, "alfreds-mac").await;
    let g = generation(&link_socket(&sender));
    signal_both(&app, g).await;

    assert_eq!(
        signal(&app, "alfreds-mac", &[(g, OTHER_OFFER)]).await,
        reply(&[], &[g])
    );
    assert_eq!(
        signal(&app, "filips-mac", &[]).await,
        reply(&[(g, OFFER)], &[])
    );

    report(&app, &ALFRED, &sender, "lost", UNKNOWN).await;
    reconcile_tick(&state).await;
    let next = generation(&sent(&app).await);
    assert!(next > g, "{next} after {g}");
    assert_eq!(
        generation(&link_socket(&hop(&app, "filips-mac").await)),
        next
    );
    assert_eq!(
        signal(&app, "alfreds-mac", &[(g, OTHER_OFFER)]).await,
        reply(&[], &[])
    );
    assert_eq!(
        signal(&app, "alfreds-mac", &[(next, OTHER_OFFER)]).await,
        reply(&[], &[])
    );
    assert_eq!(
        signal(&app, "filips-mac", &[]).await,
        reply(&[(next, OTHER_OFFER)], &[])
    );
}

#[tokio::test]
async fn an_offerer_gets_the_answer_only_alongside_the_offer_it_was_answered_for() {
    let (_, app) = bench(Arc::new(MemStore::new()), LinkKeys::for_tests(), true).await;
    let g = generation(&sent(&app).await);
    signal_both(&app, g).await;

    assert_eq!(signal(&app, "alfreds-mac", &[]).await, reply(&[], &[]));
    assert_eq!(
        signal(&app, "alfreds-mac", &[(g, OTHER_OFFER)]).await,
        reply(&[], &[g])
    );
    assert_eq!(
        signal(&app, "alfreds-mac", &[(g, OFFER)]).await,
        reply(&[(g, ANSWER)], &[])
    );
    assert_eq!(
        signal(&app, "filips-mac", &[]).await,
        reply(&[(g, OFFER)], &[])
    );
}

#[tokio::test]
async fn an_unreachable_link_falls_back_to_the_srt_relay() {
    let (state, app) = bench(Arc::new(MemStore::new()), LinkKeys::for_tests(), true).await;
    let sender = hop(&app, "alfreds-mac").await;
    signal_both(&app, generation(&link_socket(&sender))).await;

    report(&app, &ALFRED, &sender, "unreachable", UNKNOWN).await;
    reconcile_tick(&state).await;

    assert_eq!(
        desired(&app, "weave-relay").await.as_array().unwrap().len(),
        1
    );
    let socket = sent(&app).await;
    assert_eq!(socket["transport"], "srt", "{socket}");
    assert_eq!(socket["host"], RELAY_HOST, "{socket}");
    let receiver = hop(&app, "filips-mac").await;
    assert_eq!(receiver["ingress"]["transport"], "srt");
    assert_eq!(receiver["profile_id"], "srt-to-speaker");
    assert_eq!(
        detail(&app).await,
        "the stream has a complete path; destination filip: relayed through weave-relay: the webrtc link alfreds-mac to filips-mac connected neither directly nor through TURN within 10 s of the answer"
    );
    assert_eq!(signal(&app, "filips-mac", &[]).await, reply(&[], &[]));
}

#[tokio::test]
async fn without_a_relay_a_failed_link_fails_placement_naming_both_causes() {
    let (state, app) = bench(Arc::new(MemStore::new()), LinkKeys::for_tests(), false).await;
    let sender = hop(&app, "alfreds-mac").await;
    signal_both(&app, generation(&link_socket(&sender))).await;

    report(&app, &ALFRED, &sender, "unreachable", UNKNOWN).await;
    reconcile_tick(&state).await;

    let (condition, detail) = placement(&app).await;
    assert_eq!(condition["status"], "false", "{condition}");
    assert_eq!(condition["reason"], "placement_failed", "{condition}");
    assert_eq!(
        detail,
        "no route from alfreds-mac to filips-mac: the webrtc link connected neither directly nor through TURN within 10 s of the answer, and no relay node is available"
    );
    assert_eq!(desired(&app, "alfreds-mac").await, json!([]));
}

#[tokio::test]
async fn a_lost_link_is_reoffered_and_a_second_loss_goes_to_the_relay() {
    let (state, app) = bench(Arc::new(MemStore::new()), LinkKeys::for_tests(), true).await;
    let sender = hop(&app, "alfreds-mac").await;
    let receiver = hop(&app, "filips-mac").await;
    let first = generation(&link_socket(&sender));
    both_report(&app, &sender, &receiver, "flowing").await;
    reconcile_tick(&state).await;

    both_report(&app, &sender, &receiver, "lost").await;
    reconcile_tick(&state).await;
    let sender = hop(&app, "alfreds-mac").await;
    let receiver = hop(&app, "filips-mac").await;
    let second = generation(&link_socket(&sender));
    assert!(second > first, "{second} after {first}");
    assert_eq!(link_socket(&sender)["transport"], "webrtc");
    assert_eq!(generation(&link_socket(&receiver)), second);
    assert!(
        detail(&app)
            .await
            .contains("webrtc link alfreds-mac to filips-mac, connecting")
    );

    both_report(&app, &sender, &receiver, "flowing").await;
    reconcile_tick(&state).await;
    report(&app, &FILIP, &receiver, "lost", UNKNOWN).await;
    reconcile_tick(&state).await;

    assert_eq!(sent(&app).await["transport"], "srt");
    assert!(
        detail(&app).await.contains(
            "relayed through weave-relay: the webrtc link alfreds-mac to filips-mac was lost twice within 120 s"
        ),
        "{}",
        detail(&app).await
    );
}

#[tokio::test]
async fn a_new_leader_keeps_the_generation_both_ends_run_and_recovers_the_descriptions() {
    let store = Arc::new(MemStore::new());
    let (_, app) = bench(Arc::clone(&store), LinkKeys::for_tests(), true).await;
    let sender = hop(&app, "alfreds-mac").await;
    let receiver = hop(&app, "filips-mac").await;
    let g = generation(&link_socket(&sender));
    signal_both(&app, g).await;
    both_report(&app, &sender, &receiver, "flowing").await;

    let (successor, next) = controller(store, LinkKeys::for_tests()).await;
    reconcile_tick(&successor).await;

    assert_eq!(hop(&next, "alfreds-mac").await, sender);
    assert_eq!(hop(&next, "filips-mac").await, receiver);
    assert_eq!(
        signal(&next, "filips-mac", &[(g, ANSWER)]).await,
        reply(&[], &[])
    );
    assert_eq!(
        signal(&next, "alfreds-mac", &[(g, OFFER)]).await,
        reply(&[(g, ANSWER)], &[])
    );
    assert_eq!(
        signal(&next, "filips-mac", &[(g, ANSWER)]).await,
        reply(&[(g, OFFER)], &[])
    );
    reconcile_tick(&successor).await;
    assert_eq!(hop(&next, "alfreds-mac").await, sender);
}

async fn aged_link(keys: impl Fn() -> LinkKeys, relayed: bool) -> (u64, u64) {
    let store = Arc::new(MemStore::new());
    let (_, app) = bench(Arc::clone(&store), keys(), true).await;
    let now = generation(&sent(&app).await);
    let aged = now - DAY + 5 * 60;
    let sender = at_generation(&hop(&app, "alfreds-mac").await, aged);
    let receiver = at_generation(&hop(&app, "filips-mac").await, aged);
    report(&app, &ALFRED, &sender, "flowing", UNKNOWN).await;
    report(
        &app,
        &FILIP,
        &receiver,
        "flowing",
        Path {
            remote: Some(TURN),
            relayed,
        },
    )
    .await;

    let (successor, next) = controller(store, keys()).await;
    reconcile_tick(&successor).await;
    (aged, generation(&sent(&next).await))
}

#[tokio::test]
async fn only_a_minted_login_on_a_relayed_link_is_renewed_before_it_expires() {
    let (aged, kept) = aged_link(LinkKeys::for_tests, false).await;
    assert_eq!(kept, aged);
    let (aged, renewed) = aged_link(LinkKeys::for_tests, true).await;
    assert!(renewed >= aged + DAY - 5 * 60, "{renewed} after {aged}");
}

#[tokio::test]
async fn a_static_login_is_never_renewed() {
    let fixed = || {
        LinkKeys::with_test_turn(&[
            (TURN_USERNAME_VAR, "weave"),
            (TURN_CREDENTIAL_VAR, "static-password"),
        ])
    };
    let (aged, kept) = aged_link(fixed, true).await;
    assert_eq!(kept, aged);
    let (_, app) = bench(Arc::new(MemStore::new()), fixed(), true).await;
    let socket = sent(&app).await;
    assert_eq!(socket["turn"][0]["username"], "weave");
    assert_eq!(socket["turn"][0]["credential"], "static-password");
}

#[tokio::test]
async fn a_new_generation_holds_while_both_ends_report_the_old_one_until_its_deadline() {
    let (state, app) = bench(Arc::new(MemStore::new()), LinkKeys::for_tests(), true).await;
    let sender = hop(&app, "alfreds-mac").await;
    let receiver = hop(&app, "filips-mac").await;
    let first = generation(&link_socket(&sender));

    both_report(&app, &sender, &receiver, "lost").await;
    reconcile_tick(&state).await;
    let bumped = hop(&app, "alfreds-mac").await;
    assert!(generation(&link_socket(&bumped)) > first);
    tokio::time::sleep(Duration::from_millis(2100)).await;
    for _ in 0..2 {
        both_report(&app, &sender, &receiver, "lost").await;
        reconcile_tick(&state).await;
        assert_eq!(hop(&app, "alfreds-mac").await, bumped);
    }

    state
        .peers()
        .rewind(crate::peers::DESCRIPTION_DEADLINE + Duration::from_secs(1));
    both_report(&app, &sender, &receiver, "lost").await;
    reconcile_tick(&state).await;
    assert_eq!(sent(&app).await["transport"], "srt");
    assert!(
        detail(&app).await.contains(
            "relayed through weave-relay: the webrtc link alfreds-mac to filips-mac got no offer from alfreds-mac within 20 s"
        ),
        "{}",
        detail(&app).await
    );
}

#[tokio::test]
async fn an_offer_without_a_turn_candidate_is_named_and_its_failure_claims_no_turn_attempt() {
    let (state, app) = bench(Arc::new(MemStore::new()), LinkKeys::for_tests(), true).await;
    let sender = hop(&app, "alfreds-mac").await;
    let g = generation(&link_socket(&sender));
    signal(&app, "alfreds-mac", &[(g, OFFER_WITHOUT_TURN)]).await;
    signal(&app, "filips-mac", &[(g, ANSWER)]).await;
    reconcile_tick(&state).await;
    assert!(
        detail(&app).await.contains(
            "webrtc link alfreds-mac to filips-mac, connecting, alfreds-mac's offer has no TURN candidate"
        ),
        "{}",
        detail(&app).await
    );

    report(&app, &ALFRED, &sender, "unreachable", UNKNOWN).await;
    reconcile_tick(&state).await;
    let detail = detail(&app).await;
    assert!(
        detail.contains(
            "the webrtc link alfreds-mac to filips-mac found no direct path within 10 s of the answer, and alfreds-mac's offer has no TURN candidate"
        ),
        "{detail}"
    );
    assert!(!detail.contains("through TURN"), "{detail}");
}

#[tokio::test]
async fn a_mac_that_moves_network_gets_a_fresh_webrtc_attempt() {
    let (state, app) = bench(Arc::new(MemStore::new()), LinkKeys::for_tests(), true).await;
    let sender = hop(&app, "alfreds-mac").await;
    signal_both(&app, generation(&link_socket(&sender))).await;
    report(&app, &ALFRED, &sender, "unreachable", UNKNOWN).await;
    reconcile_tick(&state).await;
    assert_eq!(sent(&app).await["transport"], "srt");

    heartbeat(&app, &FILIP, FILIP.public, json!([])).await;
    reconcile_tick(&state).await;
    assert_eq!(sent(&app).await["transport"], "srt");

    heartbeat(&app, &FILIP, CAFE, json!([])).await;
    reconcile_tick(&state).await;
    assert_eq!(sent(&app).await["transport"], "webrtc");
    assert_eq!(
        hop(&app, "filips-mac").await["ingress"]["transport"],
        "webrtc"
    );
}

#[tokio::test]
async fn a_webrtc_peer_address_holds_no_port_on_the_node_that_reports_it() {
    let (state, app) = controller(Arc::new(MemStore::new()), LinkKeys::for_tests()).await;
    for mac in [&ALFRED, &ANNA, &FILIP] {
        accepted(&app, "/nodes/register", registration(mac)).await;
    }
    accepted(&app, "/streams", music_to_filip()).await;
    accepted(
        &app,
        "/streams",
        json!({
            "name": "anna-to-alfred",
            "source": { "input": { "node": "annas-mac", "id": "music" } },
            "destinations": [{ "id": "alfred", "output": { "node": "alfreds-mac", "id": "speakers" } }]
        }),
    )
    .await;
    reconcile_tick(&state).await;
    let on_alfred = |hops: Value, id: &str| {
        hops.as_array()
            .unwrap()
            .iter()
            .find(|hop| hop["id"] == id)
            .cloned()
            .unwrap_or_else(|| panic!("{id} in {hops}"))
    };
    let receiver = "weave-anna-to-alfred-receiver-alfred";
    let port = on_alfred(desired(&app, "alfreds-mac").await, receiver)["ingress"]["port"].clone();
    assert!(port.is_u64(), "{port}");

    let sender = on_alfred(
        desired(&app, "alfreds-mac").await,
        "weave-music-to-filip-sender",
    );
    let socket = link_socket(&sender);
    heartbeat(
        &app,
        &ALFRED,
        ALFRED.public,
        json!([{
            "id": sender["id"],
            "node_id": ALFRED.id,
            "revision": serde_json::from_value::<DesiredHop>(sender.clone()).unwrap().revision(),
            "state": "provisioned",
            "ingress": { "condition": "flowing" },
            "egresses": [{
                "branch_id": "filip",
                "condition": "flowing",
                "resolved": { "host": ALFRED.address, "port": port },
                "peer": { "link": socket["link"], "generation": socket["generation"], "relayed": false }
            }]
        }]),
    )
    .await;
    reconcile_tick(&state).await;

    assert_eq!(
        on_alfred(desired(&app, "alfreds-mac").await, receiver)["ingress"]["port"],
        port
    );
}

#[tokio::test]
async fn one_stream_fans_out_over_srt_on_its_site_and_webrtc_across_sites() {
    let (state, app) = controller(Arc::new(MemStore::new()), LinkKeys::for_tests()).await;
    for mac in [&ALFRED, &ANNA, &FILIP] {
        accepted(&app, "/nodes/register", registration(mac)).await;
    }
    accepted(
        &app,
        "/streams",
        json!({
            "name": "music",
            "source": { "input": { "node": "alfreds-mac", "id": "music" } },
            "destinations": [
                { "id": "anna", "output": { "node": "annas-mac", "id": "speakers" } },
                { "id": "filip", "output": { "node": "filips-mac", "id": "speakers" } }
            ]
        }),
    )
    .await;
    reconcile_tick(&state).await;

    let sender = hop(&app, "alfreds-mac").await;
    assert_eq!(sender["profile_id"], "app-audio-out", "{sender}");
    let egresses = sender["egresses"].as_array().unwrap();
    assert_eq!(egresses.len(), 2, "{sender}");
    assert_eq!(egresses[0]["branch_id"], "anna");
    assert_eq!(egresses[0]["transport"], "srt");
    assert_eq!(egresses[0]["host"], ANNA.address);
    assert_eq!(egresses[1]["branch_id"], "filip");
    assert_eq!(egresses[1]["transport"], "webrtc");
    assert_eq!(egresses[1]["link"], "weave-music-receiver-filip");
    let anna = hop(&app, "annas-mac").await;
    assert_eq!(anna["profile_id"], "srt-to-speaker");
    let filip = hop(&app, "filips-mac").await;
    assert_eq!(filip["profile_id"], "webrtc-to-speaker");
    assert_eq!(placement(&app).await.0["status"], "true");
}
