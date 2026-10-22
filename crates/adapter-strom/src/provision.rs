use std::collections::{HashMap, HashSet};

use serde_json::Value;
use weave_core::{
    DesiredHop, HopState, LinkCondition, ResolvedAddr, RistSocket, SocketRole, SocketSpec,
    SrtSocket, is_managed_hop_id,
};
use weave_strom::{FlowSpec, SrtUri, StromFlow, flow_spec_from_hop, hop_srt_uris};

const STALL_POLLS: u32 = 3;

#[derive(Debug, Clone, Copy)]
pub struct SideObservation {
    pub bytes: Option<i64>,

    pub running: bool,

    pub gst_paused: bool,
}

#[derive(Debug, Default)]
struct HopProgress {
    last_bytes: Option<i64>,
    stale_polls: u32,
    ever_flowed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Ingress,
    Egress(usize),
}

const REDIAL_POLLS: u32 = 6;
const MAX_REDIAL_POLLS: u32 = 96;

#[derive(Debug)]
struct CallerWait {
    polls: u32,
    due_at: u32,
}

#[derive(Debug, Default)]
pub struct StallTracker {
    hops: HashMap<(String, Side), HopProgress>,
    unconnected_callers: HashMap<String, CallerWait>,
    sessions: HashMap<(String, Side), SessionTotal>,
}

#[derive(Debug, Default)]
struct SessionTotal {
    last: HashMap<String, i64>,
    total: i64,
}

impl StallTracker {
    pub fn observe(&mut self, hop_id: &str, side: Side, obs: SideObservation) -> bool {
        let progress = self.hops.entry((hop_id.to_string(), side)).or_default();

        if let Some(bytes) = obs.bytes {
            match progress.last_bytes {
                None => {}
                Some(prev) if bytes > prev => {
                    progress.ever_flowed = true;
                    progress.stale_polls = 0;
                }

                Some(_) => progress.stale_polls = progress.stale_polls.saturating_add(1),
            }
            progress.last_bytes = Some(bytes);
        }

        progress.ever_flowed
            && obs.running
            && !obs.gst_paused
            && progress.stale_polls >= STALL_POLLS
    }

    #[must_use]
    pub fn advanced(&self, hop_id: &str, side: Side) -> bool {
        self.hops
            .get(&(hop_id.to_string(), side))
            .is_some_and(|progress| progress.ever_flowed && progress.stale_polls == 0)
    }

    pub fn session_total(&mut self, hop_id: &str, side: Side, sessions: &[(&str, i64)]) -> i64 {
        let entry = self.sessions.entry((hop_id.to_string(), side)).or_default();
        let mut last = HashMap::with_capacity(sessions.len());
        for (key, bytes) in sessions {
            let before = entry.last.get(*key).copied().unwrap_or(0);
            entry.total += (bytes - before).max(0);
            last.insert((*key).to_string(), *bytes);
        }
        entry.last = last;
        entry.total
    }

    pub fn retain(&mut self, desired: &HashSet<&str>) {
        self.hops.retain(|(id, _), _| desired.contains(id.as_str()));
        self.sessions
            .retain(|(id, _), _| desired.contains(id.as_str()));
        self.unconnected_callers
            .retain(|id, _| desired.contains(id.as_str()));
    }

    pub fn note_caller(&mut self, hop_id: &str, handshaken: bool) {
        if handshaken {
            self.unconnected_callers.remove(hop_id);
        } else {
            self.unconnected_callers
                .entry(hop_id.to_string())
                .or_insert(CallerWait {
                    polls: 0,
                    due_at: REDIAL_POLLS,
                })
                .polls += 1;
        }
    }

    pub fn caller_due_restart(&mut self, hop_id: &str) -> bool {
        match self.unconnected_callers.get_mut(hop_id) {
            Some(wait) if wait.polls >= wait.due_at => {
                wait.polls = 0;
                wait.due_at = (wait.due_at * 2).min(MAX_REDIAL_POLLS);
                true
            }
            _ => false,
        }
    }
}

#[derive(Debug, Default, PartialEq)]
pub struct HopPlan {
    pub create: Vec<DesiredHop>,

    pub start: Vec<String>,

    pub delete: Vec<String>,
}

impl HopPlan {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.create.is_empty() && self.start.is_empty() && self.delete.is_empty()
    }
}

#[must_use]
pub fn diff_hops(desired: &[DesiredHop], flows: &[StromFlow]) -> HopPlan {
    let desired_by_id: HashMap<&str, &DesiredHop> =
        desired.iter().map(|h| (h.id.as_str(), h)).collect();
    let flow_by_name: HashMap<&str, &StromFlow> =
        flows.iter().map(|f| (f.name.as_str(), f)).collect();

    let mut create = Vec::new();
    let mut start = Vec::new();
    let mut delete = Vec::new();

    for flow in flows {
        match desired_by_id.get(flow.name.as_str()) {
            Some(hop) if flow_drifted(flow, hop) => delete.push(flow.id.clone()),
            Some(_) if !flow.running => start.push(flow.id.clone()),
            Some(_) => {}
            None if is_managed_hop_id(&flow.name) => delete.push(flow.id.clone()),
            None => {}
        }
    }

    for hop in desired {
        match flow_by_name.get(hop.id.as_str()) {
            Some(flow) if !flow_drifted(flow, hop) => {}
            _ => create.push(hop.clone()),
        }
    }

    HopPlan {
        create,
        start,
        delete,
    }
}

pub(crate) fn flow_drifted(flow: &StromFlow, hop: &DesiredHop) -> bool {
    let actual_srt = sorted(flow_srt_uris(flow));
    let actual_rist = sorted(flow_rist_endpoints(flow));
    let actual_ids = sorted(flow_endpoint_ids(flow));
    if actual_srt.is_empty() && actual_rist.is_empty() && actual_ids.is_empty() {
        return false;
    }
    actual_srt != sorted(hop_srt_uris(hop))
        || actual_rist != sorted(hop_rist_endpoints(hop))
        || actual_ids != sorted(hop_endpoint_ids(hop))
        || flow_spec_from_hop(hop)
            .is_ok_and(|spec| flow_track_settings(flow) != spec_track_settings(&spec))
}

const TRACK_PROPERTIES: [&str; 3] = ["mode", "num_video_tracks", "num_audio_tracks"];

type TrackSetting = (String, &'static str, String);

fn flow_track_settings(flow: &StromFlow) -> Vec<TrackSetting> {
    sorted(
        flow.blocks
            .iter()
            .flat_map(|block| track_values(&block.id, |key| block.properties.get(key)))
            .collect(),
    )
}

fn spec_track_settings(spec: &FlowSpec) -> Vec<TrackSetting> {
    sorted(
        spec.blocks
            .iter()
            .flat_map(|block| track_values(&block.id, |key| block.properties.get(key)))
            .collect(),
    )
}

fn track_values<'a>(block: &str, get: impl Fn(&str) -> Option<&'a Value>) -> Vec<TrackSetting> {
    TRACK_PROPERTIES
        .into_iter()
        .filter_map(|key| get(key).map(|value| (block.to_string(), key, value.to_string())))
        .collect()
}

fn sorted<T: Ord>(mut values: Vec<T>) -> Vec<T> {
    values.sort();
    values
}

fn flow_srt_uris(flow: &StromFlow) -> Vec<SrtUri> {
    let element_uris = flow
        .elements
        .iter()
        .filter_map(|element| element.properties.get("uri"));
    let block_uris = flow
        .blocks
        .iter()
        .filter_map(|block| block.properties.get("srt_uri"));
    element_uris
        .chain(block_uris)
        .filter_map(Value::as_str)
        .filter_map(SrtUri::parse)
        .collect()
}

fn flow_rist_endpoints(flow: &StromFlow) -> Vec<(String, u16)> {
    flow.elements
        .iter()
        .filter_map(|element| {
            let port = element.properties.get("port")?.as_u64()?;
            let host = match element.element_type.as_str() {
                "ristsrc" => String::new(),
                "ristsink" => element.properties.get("address")?.as_str()?.to_string(),
                _ => return None,
            };
            Some((host, u16::try_from(port).ok()?))
        })
        .collect()
}

fn hop_rist_endpoints(hop: &DesiredHop) -> Vec<(String, u16)> {
    hop_sockets(hop)
        .filter_map(|spec| match spec {
            SocketSpec::Rist(RistSocket::Listen { port }) => Some((String::new(), *port)),
            SocketSpec::Rist(RistSocket::Connect { host, port }) => Some((host.clone(), *port)),
            _ => None,
        })
        .collect()
}

fn flow_endpoint_ids(flow: &StromFlow) -> Vec<String> {
    flow.blocks
        .iter()
        .filter_map(|block| block.properties.get("endpoint_id"))
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

fn hop_sockets(hop: &DesiredHop) -> impl Iterator<Item = &SocketSpec> {
    std::iter::once(&hop.ingress).chain(hop.egresses.iter().map(|egress| &egress.socket))
}

fn hop_endpoint_ids(hop: &DesiredHop) -> Vec<String> {
    hop_sockets(hop)
        .filter_map(|spec| match spec {
            SocketSpec::Whip(socket) | SocketSpec::Whep(socket) => Some(socket.endpoint_id.clone()),
            SocketSpec::Srt(_) | SocketSpec::Rist(_) | SocketSpec::Local(_) => None,
        })
        .collect()
}

#[must_use]
pub fn hop_state(flow: Option<&StromFlow>, failed: bool) -> HopState {
    if failed {
        return HopState::Failed;
    }
    match flow {
        None => HopState::Pending,
        Some(_) => HopState::Provisioned,
    }
}

#[must_use]
pub fn socket_condition(
    role: SocketRole,
    connected: bool,
    rate_mbps: f64,
    stalled: bool,
) -> LinkCondition {
    if stalled {
        return LinkCondition::Stalled;
    }
    match (connected, role) {
        (false, SocketRole::Listen) => LinkCondition::Idle,
        (false, SocketRole::Connect) => LinkCondition::Connecting,
        (true, _) if rate_mbps > 0.0 => LinkCondition::Flowing,
        (true, _) => LinkCondition::Connected,
    }
}

#[must_use]
pub fn webrtc_condition(
    role: SocketRole,
    in_session: bool,
    advanced: bool,
    stalled: bool,
) -> LinkCondition {
    if stalled {
        return LinkCondition::Stalled;
    }
    match (in_session, role) {
        (false, SocketRole::Listen) => LinkCondition::Idle,
        (false, SocketRole::Connect) => LinkCondition::Connecting,
        (true, _) if advanced => LinkCondition::Flowing,
        (true, _) => LinkCondition::Connected,
    }
}

#[must_use]
pub fn rist_condition(
    role: SocketRole,
    srt_side_connected: bool,
    advanced: bool,
    stalled: bool,
) -> LinkCondition {
    match (srt_side_connected, role) {
        (false, SocketRole::Listen) => LinkCondition::Idle,
        (false, SocketRole::Connect) => LinkCondition::Connecting,
        (true, _) if stalled => LinkCondition::Stalled,
        (true, _) if advanced => LinkCondition::Flowing,
        (true, _) => LinkCondition::Connected,
    }
}

#[must_use]
pub fn resolved_addr(spec: &SocketSpec, listener_host: Option<&str>) -> Option<ResolvedAddr> {
    let (host, port) = match spec {
        SocketSpec::Srt(SrtSocket::Listen { port, .. }) => {
            (listener_host.unwrap_or("0.0.0.0").to_string(), *port)
        }
        SocketSpec::Srt(SrtSocket::Connect { host, port, .. })
        | SocketSpec::Rist(RistSocket::Connect { host, port }) => (host.clone(), *port),
        SocketSpec::Rist(RistSocket::Listen { port }) => {
            (listener_host.unwrap_or("0.0.0.0").to_string(), *port)
        }
        SocketSpec::Whip(_) | SocketSpec::Whep(_) | SocketSpec::Local(_) => return None,
    };
    Some(ResolvedAddr { host, port })
}

#[cfg(test)]
mod tests {
    use super::*;
    use weave_core::{DesiredEgress, HopRole, SignallingTransport, Track};

    fn hop(id: &str) -> DesiredHop {
        DesiredHop {
            delivery: None,
            id: id.to_string(),
            node_id: "strom-node-1".to_string(),
            profile_id: "srt-forward".to_string(),
            role: HopRole::Sender,
            ingress: SocketSpec::srt_listen(7001, 200),
            merge_ingress: None,
            egresses: vec![DesiredEgress {
                branch_id: "studio".to_string(),
                socket: SocketSpec::srt_connect("10.0.0.2", 7002, 1000),
            }],
            tracks: None,
        }
    }

    fn flow(name: &str, id: &str, running: bool) -> StromFlow {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "name": name,
            "running": running,
        }))
        .unwrap()
    }

    #[test]
    fn creates_desired_hops_without_a_matching_flow() {
        let desired = vec![hop("weave-a"), hop("weave-b")];
        let flows = vec![flow("weave-a", "id-a", true)];

        let plan = diff_hops(&desired, &flows);

        let created: Vec<_> = plan.create.iter().map(|h| h.id.as_str()).collect();
        assert_eq!(created, vec!["weave-b"]);
        assert!(plan.delete.is_empty());
    }

    #[test]
    fn deletes_only_managed_flows_no_longer_desired() {
        let desired = vec![hop("weave-a")];
        let flows = vec![
            flow("weave-a", "id-a", true),
            flow("weave-stale", "id-stale", true),
            flow("operator-manual", "id-manual", true),
        ];

        let plan = diff_hops(&desired, &flows);

        assert_eq!(plan.delete, vec!["id-stale".to_string()]);
        assert!(
            plan.create.is_empty(),
            "adopted-by-name flow is not recreated"
        );
    }

    #[test]
    fn never_deletes_unmanaged_flows_even_when_not_desired() {
        let desired: Vec<DesiredHop> = Vec::new();
        let flows = vec![
            flow("contribution", "id-1", true),
            flow("contribution-recv", "id-2", true),
        ];

        let plan = diff_hops(&desired, &flows);

        assert!(plan.is_empty(), "old controller flows are left untouched");
    }

    fn flow_with_uris(name: &str, id: &str, ingress: &str, egress: &str) -> StromFlow {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "name": name,
            "running": true,
            "elements": [
                { "element_type": "srtsrc", "properties": { "uri": ingress } },
                { "element_type": "srtsink", "properties": { "uri": egress } },
            ],
        }))
        .unwrap()
    }

    #[test]
    fn drifted_flow_is_deleted_and_recreated() {
        let desired = vec![hop("weave-a")];
        let flows = vec![flow_with_uris(
            "weave-a",
            "id-a",
            "srt://:9999?mode=listener",
            "srt://10.0.0.2:7002?mode=caller",
        )];

        let plan = diff_hops(&desired, &flows);

        assert_eq!(plan.delete, vec!["id-a".to_string()]);
        let created: Vec<_> = plan.create.iter().map(|h| h.id.as_str()).collect();
        assert_eq!(created, vec!["weave-a"]);
        assert!(
            plan.start.is_empty(),
            "recreated flow is not also started in place"
        );
    }

    #[test]
    fn a_flow_whose_srt_mode_was_edited_is_recreated() {
        let desired = vec![hop("weave-a")];
        for (ingress, egress) in [
            (
                "srt://:7001?mode=rendezvous&latency=200",
                "srt://10.0.0.2:7002?mode=caller&latency=1000",
            ),
            (
                "srt://:7001?mode=listener&latency=200",
                "srt://10.0.0.2:7002?mode=rendezvous&latency=1000",
            ),
            (
                "srt://:7001?latency=200",
                "srt://10.0.0.2:7002?mode=caller&latency=1000",
            ),
        ] {
            let flows = vec![flow_with_uris("weave-a", "id-a", ingress, egress)];
            let plan = diff_hops(&desired, &flows);
            assert_eq!(plan.delete, vec!["id-a".to_string()], "{ingress} {egress}");
            assert_eq!(plan.create.len(), 1, "{ingress} {egress}");
        }
    }

    #[test]
    fn matching_flow_uris_are_adopted_not_recreated() {
        let desired = vec![hop("weave-a")];
        let flows = vec![flow_with_uris(
            "weave-a",
            "id-a",
            "srt://:7001?mode=listener&latency=200",
            "srt://10.0.0.2:7002?mode=caller&latency=1000",
        )];

        let plan = diff_hops(&desired, &flows);

        assert!(plan.is_empty(), "unchanged flow is adopted as-is: {plan:?}");
    }

    #[test]
    fn a_flow_whose_latency_differs_is_recreated() {
        let desired = vec![hop("weave-a")];
        for (ingress, egress) in [
            (
                "srt://:7001?mode=listener&latency=120",
                "srt://10.0.0.2:7002?mode=caller&latency=1000",
            ),
            (
                "srt://:7001?mode=listener&latency=200",
                "srt://10.0.0.2:7002?mode=caller&latency=2000",
            ),
            (
                "srt://:7001?mode=listener",
                "srt://10.0.0.2:7002?mode=caller",
            ),
        ] {
            let flows = vec![flow_with_uris("weave-a", "id-a", ingress, egress)];
            let plan = diff_hops(&desired, &flows);
            assert_eq!(plan.delete, vec!["id-a".to_string()], "{ingress} {egress}");
            assert_eq!(plan.create.len(), 1, "{ingress} {egress}");
        }
    }

    fn rist_sender(address: &str, port: u16) -> StromFlow {
        serde_json::from_value(serde_json::json!({
            "id": "id-a",
            "name": "weave-a",
            "running": true,
            "elements": [
                { "element_type": "srtsrc",
                  "properties": { "uri": "srt://:7001?mode=listener&latency=200" } },
                { "element_type": "ristsink",
                  "properties": { "address": address, "port": port } },
            ],
        }))
        .unwrap()
    }

    #[test]
    fn a_rist_flow_is_adopted_until_its_address_changes() {
        let mut sender = hop("weave-a");
        sender.profile_id = "srt-to-rist".to_string();
        sender.egresses[0].socket = SocketSpec::Rist(RistSocket::Connect {
            host: "10.0.0.2".to_string(),
            port: 7100,
        });
        let desired = vec![sender];
        assert!(diff_hops(&desired, &[rist_sender("10.0.0.2", 7100)]).is_empty());
        for flow in [rist_sender("10.0.0.3", 7100), rist_sender("10.0.0.2", 7102)] {
            let plan = diff_hops(&desired, &[flow]);
            assert_eq!(plan.delete, vec!["id-a".to_string()]);
            assert_eq!(plan.create.len(), 1);
        }
    }

    #[test]
    fn rist_condition_follows_the_srt_side() {
        assert_eq!(
            rist_condition(SocketRole::Listen, false, false, false),
            LinkCondition::Idle,
            "a receiver with no consumer on its SRT output waits"
        );
        assert_eq!(
            rist_condition(SocketRole::Connect, false, false, false),
            LinkCondition::Connecting,
            "a sender with no producer on its SRT input has nothing to push"
        );
        assert_eq!(
            rist_condition(SocketRole::Connect, true, true, false),
            LinkCondition::Flowing
        );
        assert_eq!(
            rist_condition(SocketRole::Listen, true, false, false),
            LinkCondition::Connected
        );
        assert_eq!(
            rist_condition(SocketRole::Listen, true, false, true),
            LinkCondition::Stalled
        );
    }

    fn keyed(socket: SocketSpec, passphrase: &str) -> SocketSpec {
        let SocketSpec::Srt(mut socket) = socket else {
            panic!("expected an SRT socket");
        };
        let params = socket.params_mut();
        params.passphrase = Some(weave_core::Passphrase::new(passphrase));
        params.pbkeylen = Some(32);
        SocketSpec::Srt(socket)
    }

    #[test]
    fn a_flow_whose_key_differs_is_recreated() {
        let mut keyed_hop = hop("weave-a");
        keyed_hop.egresses[0].socket = keyed(keyed_hop.egresses[0].socket.clone(), "new-link-key");
        let desired = vec![keyed_hop];
        for egress in [
            "srt://10.0.0.2:7002?mode=caller&latency=1000",
            "srt://10.0.0.2:7002?mode=caller&latency=1000&passphrase=old%2Dlink%2Dkey&pbkeylen=32",
            "srt://10.0.0.2:7002?mode=caller&latency=1000&passphrase=new%2Dlink%2Dkey&pbkeylen=16",
        ] {
            let flows = vec![flow_with_uris(
                "weave-a",
                "id-a",
                "srt://:7001?mode=listener&latency=200",
                egress,
            )];
            let plan = diff_hops(&desired, &flows);
            assert_eq!(plan.delete, vec!["id-a".to_string()], "{egress}");
            assert_eq!(plan.create.len(), 1, "{egress}");
        }

        let flows = vec![flow_with_uris(
            "weave-a",
            "id-a",
            "srt://:7001?mode=listener&latency=200",
            "srt://10.0.0.2:7002?mode=caller&latency=1000&passphrase=new%2Dlink%2Dkey&pbkeylen=32",
        )];
        assert!(diff_hops(&desired, &flows).is_empty());
    }

    #[test]
    fn starts_desired_flow_that_is_present_but_stopped() {
        let desired = vec![hop("weave-a")];
        let flows = vec![flow("weave-a", "id-a", false)];

        let plan = diff_hops(&desired, &flows);

        assert_eq!(plan.start, vec!["id-a".to_string()]);
        assert!(plan.create.is_empty());
    }

    #[test]
    fn hop_state_reports_lifecycle_only() {
        let running = flow("weave-a", "id-a", true);
        let stopped = flow("weave-a", "id-a", false);

        assert_eq!(hop_state(None, false), HopState::Pending);
        assert_eq!(hop_state(Some(&stopped), false), HopState::Provisioned);
        assert_eq!(hop_state(Some(&running), false), HopState::Provisioned);
        assert_eq!(hop_state(Some(&running), true), HopState::Failed);
        assert_eq!(hop_state(None, true), HopState::Failed);
    }

    #[test]
    fn socket_condition_distinguishes_listener_and_caller_when_down() {
        assert_eq!(
            socket_condition(SocketRole::Listen, false, 0.0, false),
            LinkCondition::Idle
        );
        assert_eq!(
            socket_condition(SocketRole::Connect, false, 0.0, false),
            LinkCondition::Connecting
        );
    }

    #[test]
    fn socket_condition_flows_only_when_connected_with_rate() {
        assert_eq!(
            socket_condition(SocketRole::Listen, true, 0.0, false),
            LinkCondition::Connected
        );
        assert_eq!(
            socket_condition(SocketRole::Connect, true, 3.2, false),
            LinkCondition::Flowing
        );
        assert_eq!(
            socket_condition(SocketRole::Listen, true, 3.2, false),
            LinkCondition::Flowing
        );
    }

    #[test]
    fn socket_condition_stalled_overrides_connected_state() {
        assert_eq!(
            socket_condition(SocketRole::Listen, true, 0.0, true),
            LinkCondition::Stalled
        );
    }

    fn flowing(bytes: i64) -> SideObservation {
        SideObservation {
            bytes: Some(bytes),
            running: true,
            gst_paused: false,
        }
    }

    #[test]
    fn a_caller_without_a_handshake_is_restarted_with_backoff() {
        let mut tracker = StallTracker::default();
        let mut poll = |handshaken| {
            tracker.note_caller("weave-a", handshaken);
            tracker.caller_due_restart("weave-a")
        };
        let restarts_at: Vec<u32> = (1..=400).filter(|_| poll(false)).collect();
        assert_eq!(restarts_at, vec![6, 18, 42, 90, 186, 282, 378]);

        assert!(!poll(true), "a handshake is not due a restart");
        let restarts_at: Vec<u32> = (1..=6).filter(|_| poll(false)).collect();
        assert_eq!(restarts_at, vec![6], "a handshake resets the backoff");
    }

    #[test]
    fn a_caller_that_shook_hands_but_carries_nothing_is_never_restarted() {
        let mut tracker = StallTracker::default();
        for _ in 0..200 {
            tracker.note_caller("weave-a", true);
            assert!(!tracker.caller_due_restart("weave-a"));
        }
    }

    #[test]
    fn never_flowed_hop_never_stalls() {
        let mut tracker = StallTracker::default();
        for _ in 0..6 {
            assert!(!tracker.observe("weave-a", Side::Ingress, flowing(0)));
        }
    }

    #[test]
    fn flowed_then_frozen_stalls_after_three_polls() {
        let mut tracker = StallTracker::default();
        assert!(!tracker.observe("weave-a", Side::Ingress, flowing(1000)));
        assert!(!tracker.observe("weave-a", Side::Ingress, flowing(2000)));
        assert!(
            !tracker.observe("weave-a", Side::Ingress, flowing(2000)),
            "1 frozen poll"
        );
        assert!(
            !tracker.observe("weave-a", Side::Ingress, flowing(2000)),
            "2 frozen polls"
        );
        assert!(
            tracker.observe("weave-a", Side::Ingress, flowing(2000)),
            "3 frozen polls"
        );
    }

    #[test]
    fn byte_progress_clears_a_stall() {
        let mut tracker = StallTracker::default();
        tracker.observe("weave-a", Side::Ingress, flowing(1000));
        for _ in 0..3 {
            tracker.observe("weave-a", Side::Ingress, flowing(2000));
        }
        assert!(
            tracker.observe("weave-a", Side::Ingress, flowing(2000)),
            "stalled"
        );
        assert!(
            !tracker.observe("weave-a", Side::Ingress, flowing(3000)),
            "recovers when bytes advance"
        );
    }

    #[test]
    fn paused_flow_is_never_stalled() {
        let mut tracker = StallTracker::default();
        tracker.observe("weave-a", Side::Ingress, flowing(1000));
        tracker.observe("weave-a", Side::Ingress, flowing(2000));
        for _ in 0..4 {
            let obs = SideObservation {
                bytes: Some(2000),
                running: true,
                gst_paused: true,
            };
            assert!(!tracker.observe("weave-a", Side::Ingress, obs));
        }
    }

    #[test]
    fn stopped_flow_is_never_stalled() {
        let mut tracker = StallTracker::default();
        tracker.observe("weave-a", Side::Ingress, flowing(1000));
        tracker.observe("weave-a", Side::Ingress, flowing(2000));
        for _ in 0..4 {
            let obs = SideObservation {
                bytes: Some(2000),
                running: false,
                gst_paused: false,
            };
            assert!(!tracker.observe("weave-a", Side::Ingress, obs));
        }
    }

    #[test]
    fn counter_settling_lower_then_freezing_still_stalls() {
        let mut tracker = StallTracker::default();
        tracker.observe("weave-a", Side::Ingress, flowing(71_416_276));
        tracker.observe("weave-a", Side::Ingress, flowing(72_000_000));
        assert!(
            !tracker.observe("weave-a", Side::Ingress, flowing(68_958_964)),
            "settle-down poll"
        );
        assert!(!tracker.observe("weave-a", Side::Ingress, flowing(68_958_964)));
        assert!(
            tracker.observe("weave-a", Side::Ingress, flowing(68_958_964)),
            "frozen after settle -> stalled"
        );
    }

    #[test]
    fn frozen_from_first_observation_reads_never_flowed() {
        let mut tracker = StallTracker::default();
        for _ in 0..6 {
            assert!(!tracker.observe("weave-a", Side::Ingress, flowing(9_299_932)));
        }
    }

    #[test]
    fn missing_stats_do_not_advance_a_stall() {
        let mut tracker = StallTracker::default();
        tracker.observe("weave-a", Side::Ingress, flowing(1000));
        tracker.observe("weave-a", Side::Ingress, flowing(2000));
        let missing = SideObservation {
            bytes: None,
            running: true,
            gst_paused: false,
        };
        for _ in 0..5 {
            assert!(!tracker.observe("weave-a", Side::Ingress, missing));
        }
    }

    #[test]
    fn retain_drops_undesired_hops() {
        let mut tracker = StallTracker::default();
        tracker.observe("weave-a", Side::Ingress, flowing(1000));
        tracker.observe("weave-a", Side::Egress(0), flowing(1000));
        tracker.observe("weave-b", Side::Ingress, flowing(1000));
        let desired: HashSet<&str> = ["weave-a"].into_iter().collect();
        tracker.retain(&desired);
        assert!(
            tracker
                .hops
                .contains_key(&("weave-a".to_string(), Side::Ingress))
        );
        assert!(
            tracker
                .hops
                .contains_key(&("weave-a".to_string(), Side::Egress(0)))
        );
        assert!(
            !tracker
                .hops
                .contains_key(&("weave-b".to_string(), Side::Ingress))
        );
    }

    #[test]
    fn advanced_reads_only_the_most_recent_poll() {
        let mut tracker = StallTracker::default();
        assert!(!tracker.advanced("weave-a", Side::Egress(0)), "never seen");
        tracker.observe("weave-a", Side::Egress(0), flowing(1000));
        assert!(
            !tracker.advanced("weave-a", Side::Egress(0)),
            "a baseline is not progress"
        );
        tracker.observe("weave-a", Side::Egress(0), flowing(2000));
        assert!(tracker.advanced("weave-a", Side::Egress(0)));
        tracker.observe("weave-a", Side::Egress(0), flowing(2000));
        assert!(
            !tracker.advanced("weave-a", Side::Egress(0)),
            "frozen this poll"
        );
        assert!(
            !tracker.advanced("weave-a", Side::Ingress),
            "sides are independent"
        );
    }

    #[test]
    fn egress_branches_track_progress_independently() {
        let mut tracker = StallTracker::default();
        tracker.observe("weave-a", Side::Egress(0), flowing(1000));
        tracker.observe("weave-a", Side::Egress(1), flowing(1000));
        tracker.observe("weave-a", Side::Egress(0), flowing(2000));
        tracker.observe("weave-a", Side::Egress(1), flowing(1000));

        assert!(tracker.advanced("weave-a", Side::Egress(0)));
        assert!(!tracker.advanced("weave-a", Side::Egress(1)));
    }

    #[test]
    fn webrtc_condition_waits_without_a_session_and_flows_on_byte_progress() {
        assert_eq!(
            webrtc_condition(SocketRole::Listen, false, false, false),
            LinkCondition::Idle
        );
        assert_eq!(
            webrtc_condition(SocketRole::Connect, false, false, false),
            LinkCondition::Connecting
        );
        assert_eq!(
            webrtc_condition(SocketRole::Listen, true, false, false),
            LinkCondition::Connected
        );
        assert_eq!(
            webrtc_condition(SocketRole::Listen, true, true, false),
            LinkCondition::Flowing
        );
    }

    #[test]
    fn webrtc_condition_reports_a_stall_whether_or_not_the_peer_is_in_session() {
        assert_eq!(
            webrtc_condition(SocketRole::Listen, true, false, true),
            LinkCondition::Stalled
        );
        assert_eq!(
            webrtc_condition(SocketRole::Listen, false, false, true),
            LinkCondition::Stalled,
            "a peer that carried media and left is a stall, as over SRT"
        );
        assert_eq!(
            webrtc_condition(SocketRole::Connect, false, false, true),
            LinkCondition::Stalled
        );
    }

    #[test]
    fn session_total_never_falls_when_a_session_ends() {
        let mut tracker = StallTracker::default();
        let mut total =
            |sessions: &[(&str, i64)]| tracker.session_total("weave-a", Side::Egress(0), sessions);
        assert_eq!(total(&[("old", 5000), ("new", 100)]), 5100);
        assert_eq!(total(&[("old", 5000), ("new", 600)]), 5600);
        assert_eq!(
            total(&[("new", 1100)]),
            6100,
            "the old session takes nothing away"
        );
        assert_eq!(total(&[]), 6100, "no session carries nothing");
        assert_eq!(total(&[("next", 300)]), 6400);
    }

    fn whip_gateway_hop() -> DesiredHop {
        DesiredHop {
            delivery: None,
            id: "weave-alice-cam-receiver-studio".to_string(),
            node_id: "strom-node-2".to_string(),
            profile_id: "whip-to-srt".to_string(),
            role: HopRole::Receiver,
            ingress: SocketSpec::signalling(
                SignallingTransport::Whip,
                SocketRole::Listen,
                "http://172.27.0.10:8080/whip",
                "weave-alice-cam-receiver-studio",
            ),
            merge_ingress: None,
            egresses: vec![DesiredEgress {
                branch_id: "studio".to_string(),
                socket: SocketSpec::srt_listen(7003, 200),
            }],
            tracks: None,
        }
    }

    fn block_flow(name: &str, endpoint_id: &str, srt_uri: &str) -> StromFlow {
        serde_json::from_value(serde_json::json!({
            "id": "id-a",
            "name": name,
            "running": true,
            "blocks": [
                { "id": "whip_in", "block_definition_id": "builtin.whip_input",
                  "properties": { "endpoint_id": endpoint_id, "mode": "audio_video" } },
                { "id": "srt_out_0", "block_definition_id": "builtin.mpegtssrt_output",
                  "properties": { "srt_uri": srt_uri } },
            ],
        }))
        .unwrap()
    }

    #[test]
    fn block_flow_with_matching_sockets_is_adopted() {
        let desired = vec![whip_gateway_hop()];
        let flows = vec![block_flow(
            "weave-alice-cam-receiver-studio",
            "weave-alice-cam-receiver-studio",
            "srt://:7003?mode=listener&latency=200",
        )];
        assert!(diff_hops(&desired, &flows).is_empty());
    }

    #[test]
    fn block_flow_with_a_changed_endpoint_id_or_srt_uri_drifts() {
        let desired = vec![whip_gateway_hop()];
        for (endpoint_id, srt_uri) in [
            (
                "weave-alice-cam-receiver-preview",
                "srt://:7003?mode=listener&latency=200",
            ),
            (
                "weave-alice-cam-receiver-studio",
                "srt://:7004?mode=listener&latency=200",
            ),
        ] {
            let flows = vec![block_flow(
                "weave-alice-cam-receiver-studio",
                endpoint_id,
                srt_uri,
            )];
            let plan = diff_hops(&desired, &flows);
            assert_eq!(
                plan.delete,
                vec!["id-a".to_string()],
                "{endpoint_id} {srt_uri}"
            );
            assert_eq!(plan.create.len(), 1);
        }
    }

    #[test]
    fn a_flow_built_for_other_tracks_drifts() {
        let mut video_only = whip_gateway_hop();
        video_only.tracks = Some(vec![Track::Video]);
        let both = block_flow(
            "weave-alice-cam-receiver-studio",
            "weave-alice-cam-receiver-studio",
            "srt://:7003?mode=listener&latency=200",
        );
        let plan = diff_hops(std::slice::from_ref(&video_only), &[both]);
        assert_eq!(plan.delete, vec!["id-a".to_string()]);

        let mut built: StromFlow = serde_json::from_value(serde_json::json!({
            "id": "id-a",
            "name": "weave-alice-cam-receiver-studio",
            "running": true,
            "blocks": [
                { "id": "whip_in", "properties": {
                    "endpoint_id": "weave-alice-cam-receiver-studio", "mode": "video" } },
                { "id": "srt_out_0", "properties": {
                    "srt_uri": "srt://:7003?mode=listener&latency=200", "num_audio_tracks": 0 } },
            ],
        }))
        .unwrap();
        assert!(
            diff_hops(
                std::slice::from_ref(&video_only),
                std::slice::from_ref(&built)
            )
            .is_empty()
        );

        built.blocks[1].properties.remove("num_audio_tracks");
        let plan = diff_hops(&[video_only], &[built]);
        assert_eq!(
            plan.delete,
            vec!["id-a".to_string()],
            "the muxer still waits on audio"
        );
    }

    #[test]
    fn resolved_addr_uses_listener_host_for_listener_else_wildcard() {
        let listen = SocketSpec::srt_listen(7001, 200);
        assert_eq!(
            resolved_addr(&listen, Some("172.26.0.10")),
            Some(ResolvedAddr {
                host: "172.26.0.10".to_string(),
                port: 7001
            })
        );
        assert_eq!(
            resolved_addr(&listen, None),
            Some(ResolvedAddr {
                host: "0.0.0.0".to_string(),
                port: 7001
            })
        );

        let signalling = SocketSpec::signalling(
            SignallingTransport::Whip,
            SocketRole::Listen,
            "http://172.26.0.10:8080/whip",
            "x",
        );
        assert_eq!(
            resolved_addr(&signalling, Some("172.26.0.10")),
            None,
            "a socket with no address of its own resolves to nothing"
        );
    }
}
