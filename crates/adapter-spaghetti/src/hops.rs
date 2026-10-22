use std::collections::BTreeMap;
use std::time::Instant;

use weave_core::{
    DesiredHop, EgressStatus, HopState, HopStatus, LinkCondition, LinkStats, LocalEnd, LocalSocket,
    PayloadKind, ResolvedAddr, SocketSpec, SocketStatus, SrtSocket as WeaveSrt,
};
use weave_spaghetti::{
    Destination, FlowState, FlowStatus, RouteInfo, SourceInfo, SourceSpec, SrtMode, SrtSocket,
    TapMode,
};

use crate::inventory::Inventory;

pub const SENDER_PROFILE: &str = "app-audio-to-srt";
pub const RECEIVER_PROFILE: &str = "srt-to-speaker";
pub const MAX_SENDER_EGRESSES: usize = 8;

const TAG_PREFIX: &str = "weave:";
const DEFAULT_LATENCY_MS: u32 = 200;
const RATE_WINDOW_SECS: f64 = 0.2;

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub source: SourceSpec,
    pub routes: Vec<(String, Destination)>,
}

#[must_use]
pub fn tag(node_id: &str, hop_id: &str) -> String {
    format!("{TAG_PREFIX}{node_id}/{hop_id}")
}

pub fn plan(hop: &DesiredHop, inventory: &Inventory) -> Result<Plan, String> {
    if hop.merge_ingress.is_some() {
        return Err("this node does not merge two paths".into());
    }
    if hop.delivery.is_some() {
        return Err("this node does not run timed delivery".into());
    }
    match hop.profile_id.as_str() {
        SENDER_PROFILE => {
            let SocketSpec::Local(LocalSocket {
                end: LocalEnd::Input,
                id,
            }) = &hop.ingress
            else {
                return Err(format!(
                    "{SENDER_PROFILE} takes an input, not {}",
                    hop.ingress
                ));
            };
            let pids = inventory
                .pids(id)
                .ok_or_else(|| format!("input {id} is not running"))?;
            let routes = hop
                .egresses
                .iter()
                .map(|egress| {
                    Ok((
                        egress.branch_id.clone(),
                        Destination::Srt(srt(&egress.socket)?),
                    ))
                })
                .collect::<Result<_, String>>()?;
            Ok(Plan {
                source: SourceSpec::Tap {
                    pids: pids.to_vec(),
                    mode: TapMode::Mirror,
                },
                routes,
            })
        }
        RECEIVER_PROFILE => {
            let source = SourceSpec::Srt(srt(&hop.ingress)?);
            let routes = hop
                .egresses
                .iter()
                .map(|egress| match &egress.socket {
                    SocketSpec::Local(LocalSocket {
                        end: LocalEnd::Output,
                        id,
                    }) => inventory
                        .device(id)
                        .map(|uid| {
                            (
                                egress.branch_id.clone(),
                                Destination::Device {
                                    uid: uid.to_string(),
                                },
                            )
                        })
                        .ok_or_else(|| format!("output {id} is not connected")),
                    other => Err(format!("{RECEIVER_PROFILE} feeds an output, not {other}")),
                })
                .collect::<Result<_, String>>()?;
            Ok(Plan { source, routes })
        }
        other => Err(format!("this node offers no hop profile {other}")),
    }
}

fn srt(socket: &SocketSpec) -> Result<SrtSocket, String> {
    let SocketSpec::Srt(socket) = socket else {
        return Err(format!("expected an srt socket, not {socket}"));
    };
    let params = socket.params();
    if params.payload != PayloadKind::Raw {
        return Err("this node carries only raw SRT payloads".into());
    }
    let mode = match socket {
        WeaveSrt::Listen { port, .. } => SrtMode::Listen { port: *port },
        WeaveSrt::Connect { host, port, .. } => SrtMode::Call {
            host: host.clone(),
            port: *port,
        },
    };
    Ok(SrtSocket {
        mode,
        latency_ms: params.latency.unwrap_or(DEFAULT_LATENCY_MS),
        passphrase: params
            .passphrase
            .as_ref()
            .map(|phrase| phrase.expose().to_string()),
        key_size: params.pbkeylen,
    })
}

#[derive(Debug)]
pub struct Applied<'a> {
    pub source: &'a SourceInfo,
    pub routes: Vec<&'a RouteInfo>,
}

impl Applied<'_> {
    #[must_use]
    pub fn matches(&self, plan: &Plan) -> bool {
        self.source.spec == plan.source
            && self.routes.len() == plan.routes.len()
            && plan
                .routes
                .iter()
                .all(|(_, destination)| self.route(destination).is_some())
    }

    fn route(&self, destination: &Destination) -> Option<&RouteInfo> {
        self.routes
            .iter()
            .copied()
            .find(|route| &route.destination == destination)
    }
}

#[must_use]
pub fn applied<'a>(
    node_id: &str,
    sources: &'a [SourceInfo],
    routes: &'a [RouteInfo],
) -> (BTreeMap<&'a str, Applied<'a>>, Vec<u32>) {
    let prefix = tag(node_id, "");
    let mut by_hop = BTreeMap::new();
    let mut duplicates = Vec::new();
    for source in sources {
        let Some(hop_id) = source
            .tag
            .as_deref()
            .and_then(|tag| tag.strip_prefix(prefix.as_str()))
        else {
            continue;
        };
        if by_hop.contains_key(hop_id) {
            duplicates.push(source.source_id);
            continue;
        }
        by_hop.insert(
            hop_id,
            Applied {
                source,
                routes: routes
                    .iter()
                    .filter(|route| route.source_id == source.source_id)
                    .collect(),
            },
        );
    }
    (by_hop, duplicates)
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Destroy { source_id: u32 },
    Build { hop_id: String, plan: Plan },
}

#[must_use]
pub fn actions(
    node_id: &str,
    plans: &BTreeMap<String, Result<Plan, String>>,
    sources: &[SourceInfo],
    routes: &[RouteInfo],
) -> Vec<Action> {
    let (applied, duplicates) = applied(node_id, sources, routes);
    let mut actions: Vec<Action> = duplicates
        .into_iter()
        .map(|source_id| Action::Destroy { source_id })
        .collect();
    for (hop_id, built) in &applied {
        let keep = matches!(plans.get(*hop_id), Some(Ok(plan)) if built.matches(plan));
        if !keep {
            actions.push(Action::Destroy {
                source_id: built.source.source_id,
            });
        }
    }
    for (hop_id, plan) in plans {
        let Ok(plan) = plan else { continue };
        let built = applied
            .get(hop_id.as_str())
            .is_some_and(|built| built.matches(plan));
        if !built {
            actions.push(Action::Build {
                hop_id: hop_id.clone(),
                plan: plan.clone(),
            });
        }
    }
    actions
}

#[derive(Debug, Default)]
pub struct Rates(BTreeMap<String, (u64, Instant, f64)>);

impl Rates {
    pub fn sample(&mut self, key: String, bytes: u64, now: Instant) -> f64 {
        match self.0.get_mut(&key) {
            Some((last_bytes, at, rate)) => {
                let elapsed = now.duration_since(*at).as_secs_f64();
                if bytes < *last_bytes {
                    *last_bytes = bytes;
                    *at = now;
                    *rate = 0.0;
                } else if elapsed >= RATE_WINDOW_SECS {
                    *rate = (bytes - *last_bytes) as f64 * 8.0 / elapsed / 1_000_000.0;
                    *last_bytes = bytes;
                    *at = now;
                }
                *rate
            }
            None => {
                self.0.insert(key, (bytes, now, 0.0));
                0.0
            }
        }
    }

    pub fn retain(&mut self, hop_ids: &[&str]) {
        self.0
            .retain(|key, _| hop_ids.iter().any(|id| key.starts_with(&format!("{id}/"))));
    }
}

#[must_use]
pub fn hop_status(
    hop: &DesiredHop,
    plan: &Result<Plan, String>,
    built: Option<&Applied>,
    failed: bool,
    rates: &mut Rates,
    now: Instant,
) -> HopStatus {
    let applied = match (plan, built) {
        (Ok(plan), Some(built)) if built.matches(plan) => Some((plan, built)),
        _ => None,
    };
    let state = if plan.is_err() || failed {
        HopState::Failed
    } else if applied.is_some() {
        HopState::Provisioned
    } else {
        HopState::Pending
    };
    let ingress = match applied {
        Some((_, built)) => socket_status(
            &built.source.status,
            &hop.ingress,
            rates.sample(
                format!("{}/ingress", hop.id),
                built.source.status.bytes,
                now,
            ),
            Direction::Receiving,
        ),
        None => idle(&hop.ingress),
    };
    let egresses = hop
        .egresses
        .iter()
        .map(|egress| {
            let route = applied.and_then(|(plan, built)| {
                plan.routes
                    .iter()
                    .find(|(branch, _)| branch == &egress.branch_id)
                    .and_then(|(_, destination)| built.route(destination))
            });
            let status = match route {
                Some(route) => socket_status(
                    &route.status,
                    &egress.socket,
                    rates.sample(
                        format!("{}/egress/{}", hop.id, egress.branch_id),
                        route.status.bytes,
                        now,
                    ),
                    Direction::Sending,
                ),
                None => idle(&egress.socket),
            };
            EgressStatus {
                branch_id: egress.branch_id.clone(),
                status,
            }
        })
        .collect();
    HopStatus {
        id: hop.id.clone(),
        node_id: hop.node_id.clone(),
        revision: hop.revision(),
        state,
        ingress,
        merge_ingress: None,
        egresses,
        delivery: None,
    }
}

#[derive(Clone, Copy)]
enum Direction {
    Sending,
    Receiving,
}

fn resolved(socket: &SocketSpec) -> Option<ResolvedAddr> {
    match socket {
        SocketSpec::Srt(WeaveSrt::Listen { port, .. }) => Some(ResolvedAddr {
            host: "0.0.0.0".into(),
            port: *port,
        }),
        SocketSpec::Srt(WeaveSrt::Connect { host, port, .. }) => Some(ResolvedAddr {
            host: host.clone(),
            port: *port,
        }),
        _ => None,
    }
}

fn idle(socket: &SocketSpec) -> SocketStatus {
    SocketStatus {
        condition: LinkCondition::Idle,
        resolved: resolved(socket),
        stats: None,
    }
}

fn socket_status(
    flow: &FlowStatus,
    socket: &SocketSpec,
    rate_mbps: f64,
    direction: Direction,
) -> SocketStatus {
    let condition = match flow.state {
        FlowState::Idle => LinkCondition::Idle,
        FlowState::Connecting => LinkCondition::Connecting,
        FlowState::Connected => LinkCondition::Connected,
        FlowState::Flowing => LinkCondition::Flowing,
        FlowState::Stalled => LinkCondition::Stalled,
    };
    let lost = i64::try_from(flow.packets_lost).unwrap_or(i64::MAX);
    let retransmitted = i64::try_from(flow.packets_retransmitted).unwrap_or(i64::MAX);
    let mut stats = LinkStats {
        connections: usize::from(flow.peer.is_some()),
        rate_mbps,
        ..LinkStats::default()
    };
    match direction {
        Direction::Sending => {
            stats.packets_sent_lost = lost;
            stats.packets_retransmitted = retransmitted;
        }
        Direction::Receiving => {
            stats.packets_received_lost = lost;
            stats.packets_received_retransmitted = retransmitted;
        }
    }
    SocketStatus {
        condition,
        resolved: resolved(socket),
        stats: Some(stats),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weave_core::{DesiredEgress, HopRole, Passphrase, SrtParams, Track};
    use weave_spaghetti::{AppInfo, SystemDevice};

    fn inventory() -> Inventory {
        Inventory::detect(
            &[AppInfo {
                name: "Music".into(),
                bundle_id: Some("com.apple.Music".into()),
                bundle_path: Some("/System/Applications/Music.app".into()),
                pids: vec![731],
            }],
            &[SystemDevice {
                uid: "BuiltInSpeakerDevice".into(),
                name: "MacBook Pro Speakers".into(),
                channels: 2,
                is_output: true,
            }],
            "/Users/alfred",
        )
    }

    fn srt_connect(host: &str, port: u16) -> SocketSpec {
        SocketSpec::Srt(WeaveSrt::Connect {
            host: host.into(),
            port,
            params: SrtParams {
                latency: Some(120),
                passphrase: Some(Passphrase::new("controller-derived-key")),
                pbkeylen: Some(32),
                ..SrtParams::default()
            },
        })
    }

    fn sender() -> DesiredHop {
        DesiredHop {
            id: "weave-music-sender".into(),
            node_id: "alfreds-mac".into(),
            profile_id: SENDER_PROFILE.into(),
            role: HopRole::Sender,
            ingress: SocketSpec::input("music"),
            merge_ingress: None,
            egresses: vec![DesiredEgress {
                branch_id: "filip".into(),
                socket: srt_connect("192.168.1.30", 23000),
            }],
            tracks: Some(vec![Track::Audio]),
            delivery: None,
        }
    }

    fn receiver() -> DesiredHop {
        DesiredHop {
            id: "weave-music-receiver-filip".into(),
            node_id: "filips-mac".into(),
            profile_id: RECEIVER_PROFILE.into(),
            role: HopRole::Receiver,
            ingress: SocketSpec::srt_listen(23000, 120),
            merge_ingress: None,
            egresses: vec![DesiredEgress {
                branch_id: "filip".into(),
                socket: SocketSpec::output("macbook-pro-speakers"),
            }],
            tracks: Some(vec![Track::Audio]),
            delivery: None,
        }
    }

    fn flowing(bytes: u64) -> FlowStatus {
        FlowStatus {
            state: FlowState::Flowing,
            bytes,
            peer: Some("192.168.1.30:23000".into()),
            packets_lost: 2,
            packets_retransmitted: 5,
        }
    }

    fn built(
        hop: &DesiredHop,
        plan: &Plan,
        source_id: u32,
        bytes: u64,
    ) -> (SourceInfo, Vec<RouteInfo>) {
        let source = SourceInfo {
            source_id,
            spec: plan.source.clone(),
            tag: Some(tag(&hop.node_id, &hop.id)),
            label: "x".into(),
            status: flowing(bytes),
        };
        let routes = plan
            .routes
            .iter()
            .enumerate()
            .map(|(n, (_, destination))| RouteInfo {
                route_id: source_id * 10 + n as u32,
                source_id,
                destination: destination.clone(),
                volume: 1.0,
                status: flowing(bytes),
            })
            .collect();
        (source, routes)
    }

    #[test]
    fn a_sender_taps_the_inputs_app_and_calls_each_egress_over_srt() {
        let plan = plan(&sender(), &inventory()).unwrap();
        assert_eq!(
            plan,
            Plan {
                source: SourceSpec::Tap {
                    pids: vec![731],
                    mode: TapMode::Mirror
                },
                routes: vec![(
                    "filip".into(),
                    Destination::Srt(SrtSocket {
                        mode: SrtMode::Call {
                            host: "192.168.1.30".into(),
                            port: 23000
                        },
                        latency_ms: 120,
                        passphrase: Some("controller-derived-key".into()),
                        key_size: Some(32),
                    })
                )],
            }
        );
    }

    #[test]
    fn a_receiver_listens_over_srt_and_plays_on_the_outputs_device() {
        let plan = plan(&receiver(), &inventory()).unwrap();
        assert_eq!(
            plan.source,
            SourceSpec::Srt(SrtSocket {
                mode: SrtMode::Listen { port: 23000 },
                latency_ms: 120,
                passphrase: None,
                key_size: None,
            })
        );
        assert_eq!(
            plan.routes,
            [(
                "filip".to_string(),
                Destination::Device {
                    uid: "BuiltInSpeakerDevice".into()
                }
            )]
        );
    }

    #[test]
    fn hops_this_node_cannot_run_are_refused() {
        let mut gone = sender();
        gone.ingress = SocketSpec::input("spotify");
        assert_eq!(
            plan(&gone, &inventory()),
            Err("input spotify is not running".into())
        );
        let mut foreign = sender();
        foreign.profile_id = "srt-forward".into();
        assert!(plan(&foreign, &inventory()).is_err());
        let mut timed = receiver();
        timed.ingress = SocketSpec::Srt(WeaveSrt::Listen {
            port: 23000,
            params: SrtParams {
                payload: PayloadKind::WeaveTimedV1,
                ..SrtParams::default()
            },
        });
        assert!(plan(&timed, &inventory()).is_err());
    }

    #[test]
    fn actions_build_what_is_missing_and_destroy_what_is_stale_or_unwanted() {
        let hop = sender();
        let wanted = plan(&hop, &inventory()).unwrap();
        let (current, current_routes) = built(&hop, &wanted, 1, 0);
        let second = DesiredHop {
            id: "weave-music-2-sender".into(),
            ..sender()
        };
        let mut stale_plan = wanted.clone();
        stale_plan.source = SourceSpec::Tap {
            pids: vec![999],
            mode: TapMode::Mirror,
        };
        let (stale, stale_routes) = built(&second, &stale_plan, 2, 0);
        let (unwanted, _) = built(
            &DesiredHop {
                id: "weave-old-sender".into(),
                ..sender()
            },
            &wanted,
            3,
            0,
        );
        let untagged = SourceInfo {
            tag: None,
            ..built(&hop, &wanted, 4, 0).0
        };
        let (other_node, _) = built(&receiver(), &wanted, 5, 0);
        let sources = [current, stale, unwanted, untagged, other_node];
        let routes: Vec<RouteInfo> = current_routes.into_iter().chain(stale_routes).collect();

        let plans = BTreeMap::from([
            (hop.id.clone(), Ok(wanted.clone())),
            (second.id.clone(), Ok(wanted.clone())),
        ]);
        let mut actions = actions(&hop.node_id, &plans, &sources, &routes);
        actions.sort_by_key(|action| format!("{action:?}"));
        assert_eq!(
            actions,
            [
                Action::Build {
                    hop_id: second.id.clone(),
                    plan: wanted
                },
                Action::Destroy { source_id: 2 },
                Action::Destroy { source_id: 3 },
            ]
        );
    }

    #[test]
    fn status_reports_each_branch_from_its_route_and_rates_from_byte_deltas() {
        let hop = sender();
        let wanted = plan(&hop, &inventory()).unwrap();
        let mut rates = Rates::default();
        let start = Instant::now();
        let (source, routes) = built(&hop, &wanted, 1, 1_000);
        let (current, _) = applied(&hop.node_id, std::slice::from_ref(&source), &routes);
        let first = hop_status(
            &hop,
            &Ok(wanted.clone()),
            current.get(hop.id.as_str()),
            false,
            &mut rates,
            start,
        );
        assert_eq!(first.state, HopState::Provisioned);
        assert_eq!(first.revision, hop.revision());
        assert_eq!(first.ingress.condition, LinkCondition::Flowing);
        assert_eq!(first.egresses[0].branch_id, "filip");
        assert_eq!(
            first.egresses[0].status.resolved,
            Some(ResolvedAddr {
                host: "192.168.1.30".into(),
                port: 23000
            })
        );
        let stats = first.egresses[0].status.stats.unwrap();
        assert_eq!(
            (stats.packets_sent_lost, stats.packets_retransmitted),
            (2, 5)
        );

        let (source, routes) = built(&hop, &wanted, 1, 1_000 + 125_000);
        let (current, _) = applied(&hop.node_id, std::slice::from_ref(&source), &routes);
        let later = start + std::time::Duration::from_secs(1);
        let second = hop_status(
            &hop,
            &Ok(wanted),
            current.get(hop.id.as_str()),
            false,
            &mut rates,
            later,
        );
        let rate = second.egresses[0].status.stats.unwrap().rate_mbps;
        assert!((rate - 1.0).abs() < 1e-9, "{rate}");
    }

    #[test]
    fn an_unbuilt_or_refused_hop_reports_every_branch_idle() {
        let hop = receiver();
        let mut rates = Rates::default();
        let pending = hop_status(
            &hop,
            &plan(&hop, &inventory()),
            None,
            false,
            &mut rates,
            Instant::now(),
        );
        assert_eq!(pending.state, HopState::Pending);
        assert_eq!(pending.ingress.condition, LinkCondition::Idle);
        assert_eq!(
            pending.ingress.resolved,
            Some(ResolvedAddr {
                host: "0.0.0.0".into(),
                port: 23000
            })
        );
        assert_eq!(pending.egresses.len(), 1);
        let refused = hop_status(
            &hop,
            &Err("no".into()),
            None,
            false,
            &mut rates,
            Instant::now(),
        );
        assert_eq!(refused.state, HopState::Failed);
    }
}
