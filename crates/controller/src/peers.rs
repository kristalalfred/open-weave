use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use weave_core::{
    DesiredHop, HopStatus, LinkCondition, LinkDescription, NodeDescriptor, NodeStatus, PeerAttempt,
    PeerLinkId, PeerRole, SITE_NETWORK_PREFIX, SessionDescription, SignallingReply,
};

pub const DESCRIPTION_DEADLINE: Duration = Duration::from_secs(20);
pub const CONNECT_BACKSTOP: Duration = Duration::from_secs(60);
pub const FAILED_PAIR_MEMORY: Duration = Duration::from_secs(120);

#[derive(Debug, Default)]
pub struct Peers {
    attempts: BTreeMap<PeerLinkId, Attempt>,
    failed: BTreeMap<PairKey, FailedPair>,
    lost: BTreeMap<PairKey, Loss>,
}

#[derive(Debug)]
struct Attempt {
    generation: u32,
    pair: PairKey,
    turn_offered: bool,
    offer: Side,
    answer: Side,
    observed_at: Instant,
    described_at: Option<Instant>,
    waited: Duration,
    connecting: Duration,
    settled: bool,
}

#[derive(Debug)]
struct Side {
    node: String,
    description: Option<SessionDescription>,
}

#[derive(Debug, Clone)]
struct FailedPair {
    failure: PeerFailure,
    at: Instant,
}

#[derive(Debug, Clone)]
struct Loss {
    attempt: PeerAttempt,
    at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PairKey([Vantage; 2]);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Vantage {
    node: String,
    site: Option<String>,
}

impl PairKey {
    #[must_use]
    pub fn between(upstream: &NodeDescriptor, downstream: &NodeDescriptor) -> Self {
        let mut ends = [Vantage::of(upstream), Vantage::of(downstream)];
        ends.sort();
        Self(ends)
    }
}

impl Vantage {
    fn of(node: &NodeDescriptor) -> Self {
        Self {
            node: node.id.clone(),
            site: node
                .topology
                .attachments
                .iter()
                .map(|attachment| &attachment.network)
                .find(|network| network.starts_with(SITE_NETWORK_PREFIX))
                .cloned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingTurn {
    pub node: String,
    pub role: PeerRole,
}

impl std::fmt::Display for MissingTurn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}'s {} has no TURN candidate",
            self.node,
            self.role.description_name()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnUse {
    Unconfigured,
    Offered,
    Missing(Vec<MissingTurn>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerFailure {
    Unreachable { turn: TurnUse },
    LostTwice,
    NoDescription { node: String, role: PeerRole },
    NoConnection { node: String },
}

impl std::fmt::Display for PeerFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreachable {
                turn: TurnUse::Offered,
            } => {
                f.write_str("connected neither directly nor through TURN within 10 s of the answer")
            }
            Self::Unreachable {
                turn: TurnUse::Unconfigured,
            } => f.write_str(
                "found no direct path within 10 s of the answer, and no TURN server is configured",
            ),
            Self::Unreachable {
                turn: TurnUse::Missing(missing),
            } => {
                f.write_str("found no direct path within 10 s of the answer, and ")?;
                f.write_str(&joined(missing))
            }
            Self::LostTwice => write!(
                f,
                "was lost twice within {} s",
                FAILED_PAIR_MEMORY.as_secs()
            ),
            Self::NoDescription { node, role } => write!(
                f,
                "got no {} from {node} within {} s",
                role.description_name(),
                DESCRIPTION_DEADLINE.as_secs()
            ),
            Self::NoConnection { node } => write!(
                f,
                "had no result from {node} within {} s of both descriptions",
                CONNECT_BACKSTOP.as_secs()
            ),
        }
    }
}

#[must_use]
pub fn joined(missing: &[MissingTurn]) -> String {
    missing
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" and ")
}

#[derive(Debug, Clone, Default)]
pub struct PeerPlanning {
    failed: BTreeMap<PairKey, PeerFailure>,
    served: BTreeMap<PeerLinkId, u32>,
    missing_turn: BTreeMap<PeerLinkId, Vec<MissingTurn>>,
    now_unix: u64,
}

impl PeerPlanning {
    #[must_use]
    pub fn failure(
        &self,
        upstream: &NodeDescriptor,
        downstream: &NodeDescriptor,
    ) -> Option<&PeerFailure> {
        self.failed.get(&PairKey::between(upstream, downstream))
    }

    #[must_use]
    pub fn served(&self, link: &PeerLinkId) -> Option<u32> {
        self.served.get(link).copied()
    }

    #[must_use]
    pub fn missing_turn(&self, link: &PeerLinkId) -> &[MissingTurn] {
        self.missing_turn.get(link).map_or(&[], Vec::as_slice)
    }

    #[must_use]
    pub fn next_generation(&self, after: Option<u32>) -> u32 {
        let now = u32::try_from(self.now_unix).unwrap_or(u32::MAX);
        after.map_or(now, |after| now.max(after.saturating_add(1)))
    }

    #[must_use]
    pub fn now_unix(&self) -> u64 {
        self.now_unix
    }
}

impl Attempt {
    fn side(&mut self, node: &str) -> Option<&mut Side> {
        if self.offer.node == node {
            Some(&mut self.offer)
        } else if self.answer.node == node {
            Some(&mut self.answer)
        } else {
            None
        }
    }

    fn remote_of(&self, node: &str) -> Option<&Side> {
        if self.offer.node == node {
            Some(&self.answer)
        } else if self.answer.node == node {
            Some(&self.offer)
        } else {
            None
        }
    }

    fn sides(&self) -> [(PeerRole, &Side); 2] {
        [
            (PeerRole::Offer, &self.offer),
            (PeerRole::Answer, &self.answer),
        ]
    }

    fn missing_turn(&self) -> Vec<MissingTurn> {
        if !self.turn_offered {
            return Vec::new();
        }
        self.sides()
            .into_iter()
            .filter(|(_, side)| {
                side.description
                    .as_ref()
                    .is_some_and(|description| !description.has_relayed_candidate())
            })
            .map(|(role, side)| MissingTurn {
                node: side.node.clone(),
                role,
            })
            .collect()
    }

    fn turn_use(&self) -> TurnUse {
        let missing = self.missing_turn();
        if !self.turn_offered {
            TurnUse::Unconfigured
        } else if missing.is_empty() {
            TurnUse::Offered
        } else {
            TurnUse::Missing(missing)
        }
    }

    fn tick(&mut self, now: Instant, online: bool) {
        if online {
            self.waited += now.saturating_duration_since(self.observed_at);
            if let Some(described) = self.described_at {
                self.connecting += now.saturating_duration_since(described.max(self.observed_at));
            }
        }
        self.observed_at = now;
    }

    fn overdue(&self) -> Option<PeerFailure> {
        if self.settled {
            return None;
        }
        if self.waited >= DESCRIPTION_DEADLINE
            && let Some((role, side)) = self
                .sides()
                .into_iter()
                .find(|(_, side)| side.description.is_none())
        {
            return Some(PeerFailure::NoDescription {
                node: side.node.clone(),
                role,
            });
        }
        (self.connecting >= CONNECT_BACKSTOP).then(|| PeerFailure::NoConnection {
            node: self.offer.node.clone(),
        })
    }
}

impl Peers {
    pub fn observe(&mut self, reports: &[HopStatus], nodes: &[NodeDescriptor], now: Instant) {
        let Self {
            attempts,
            failed,
            lost,
        } = self;
        let mut seen: BTreeMap<(&PeerLinkId, u32), Vec<LinkCondition>> = BTreeMap::new();
        for report in reports {
            let sockets = std::iter::once(&report.ingress)
                .chain(&report.merge_ingress)
                .chain(report.egresses.iter().map(|egress| &egress.status));
            for status in sockets {
                if let Some(peer) = &status.peer {
                    seen.entry((&peer.link, peer.generation))
                        .or_default()
                        .push(status.condition);
                }
            }
        }
        let online = |id: &str| {
            nodes
                .iter()
                .any(|node| node.id == id && node.status != NodeStatus::Offline)
        };
        let mut failures = Vec::new();
        for (link, attempt) in attempts.iter_mut() {
            attempt.tick(
                now,
                online(&attempt.offer.node) && online(&attempt.answer.node),
            );
            let conditions = seen
                .get(&(link, attempt.generation))
                .map_or(&[][..], Vec::as_slice);
            let failure = if conditions.contains(&LinkCondition::Unreachable) {
                Some(PeerFailure::Unreachable {
                    turn: attempt.turn_use(),
                })
            } else if conditions.contains(&LinkCondition::Lost) {
                let this = PeerAttempt {
                    link: link.clone(),
                    generation: attempt.generation,
                };
                match lost.get(&attempt.pair) {
                    Some(loss) if loss.attempt == this => None,
                    Some(loss) if now.duration_since(loss.at) < FAILED_PAIR_MEMORY => {
                        Some(PeerFailure::LostTwice)
                    }
                    _ => {
                        lost.insert(
                            attempt.pair.clone(),
                            Loss {
                                attempt: this,
                                at: now,
                            },
                        );
                        None
                    }
                }
            } else if conditions.iter().any(|condition| {
                matches!(
                    condition,
                    LinkCondition::Connected | LinkCondition::Flowing | LinkCondition::Stalled
                )
            }) {
                attempt.settled = true;
                None
            } else {
                attempt.overdue()
            };
            if let Some(failure) = failure {
                failures.push((attempt.pair.clone(), failure));
            }
        }
        for (pair, failure) in failures {
            failed
                .entry(pair)
                .or_insert(FailedPair { failure, at: now });
        }
        failed.retain(|_, failed| now.duration_since(failed.at) < FAILED_PAIR_MEMORY);
        lost.retain(|_, loss| now.duration_since(loss.at) < FAILED_PAIR_MEMORY);
    }

    #[must_use]
    pub fn planning(&self, now: Instant, now_unix: u64) -> PeerPlanning {
        PeerPlanning {
            failed: self
                .failed
                .iter()
                .filter(|(_, failed)| now.duration_since(failed.at) < FAILED_PAIR_MEMORY)
                .map(|(pair, failed)| (pair.clone(), failed.failure.clone()))
                .collect(),
            served: self
                .attempts
                .iter()
                .map(|(link, attempt)| (link.clone(), attempt.generation))
                .collect(),
            missing_turn: self
                .attempts
                .iter()
                .map(|(link, attempt)| (link.clone(), attempt.missing_turn()))
                .filter(|(_, missing)| !missing.is_empty())
                .collect(),
            now_unix,
        }
    }

    pub fn serve<'h>(
        &mut self,
        hops: impl IntoIterator<Item = &'h DesiredHop>,
        nodes: &[NodeDescriptor],
        now: Instant,
    ) {
        let served = served_links(hops);
        self.attempts.retain(|link, attempt| {
            served.get(link).is_some_and(|served| {
                served.generation == attempt.generation
                    && served.offerer == attempt.offer.node
                    && served.answerer == attempt.answer.node
            })
        });
        for (link, served) in served {
            if self.attempts.contains_key(&link) {
                continue;
            }
            let node = |id: &str| nodes.iter().find(|node| node.id == id);
            let (Some(offerer), Some(answerer)) = (node(&served.offerer), node(&served.answerer))
            else {
                continue;
            };
            self.attempts.insert(
                link,
                Attempt {
                    generation: served.generation,
                    pair: PairKey::between(offerer, answerer),
                    turn_offered: served.turn_offered,
                    offer: Side {
                        node: served.offerer,
                        description: None,
                    },
                    answer: Side {
                        node: served.answerer,
                        description: None,
                    },
                    observed_at: now,
                    described_at: None,
                    waited: Duration::ZERO,
                    connecting: Duration::ZERO,
                    settled: false,
                },
            );
        }
    }

    pub fn exchange(
        &mut self,
        node: &str,
        local: Vec<LinkDescription>,
        now: Instant,
    ) -> SignallingReply {
        let mut posted: BTreeMap<PeerLinkId, bool> = BTreeMap::new();
        for description in local {
            let Some(attempt) = self
                .attempts
                .get_mut(&description.link)
                .filter(|attempt| attempt.generation == description.generation)
            else {
                continue;
            };
            let Some(side) = attempt.side(node) else {
                continue;
            };
            let stored = side
                .description
                .get_or_insert_with(|| description.description.clone());
            let kept = *stored == description.description;
            posted.insert(description.link, kept);
            if kept
                && attempt.described_at.is_none()
                && attempt.offer.description.is_some()
                && attempt.answer.description.is_some()
            {
                attempt.described_at = Some(now);
            }
        }
        let refused = posted
            .iter()
            .filter(|(_, kept)| !**kept)
            .map(|(link, _)| PeerAttempt {
                link: link.clone(),
                generation: self.attempts[link].generation,
            })
            .collect();
        let remote = self
            .attempts
            .iter()
            .filter(|(link, attempt)| {
                let posted = posted.get(*link).copied();
                if attempt.offer.node == node {
                    posted == Some(true)
                } else {
                    posted != Some(false)
                }
            })
            .filter_map(|(link, attempt)| {
                Some(LinkDescription {
                    link: link.clone(),
                    generation: attempt.generation,
                    description: attempt.remote_of(node)?.description.clone()?,
                })
            })
            .collect();
        SignallingReply { remote, refused }
    }

    #[cfg(test)]
    pub fn rewind(&mut self, by: Duration) {
        let back = |at: &mut Instant| *at = at.checked_sub(by).expect("a rewindable clock");
        for attempt in self.attempts.values_mut() {
            back(&mut attempt.observed_at);
            if let Some(described) = &mut attempt.described_at {
                back(described);
            }
        }
        for failed in self.failed.values_mut() {
            back(&mut failed.at);
        }
        for loss in self.lost.values_mut() {
            back(&mut loss.at);
        }
    }
}

struct ServedLink {
    generation: u32,
    turn_offered: bool,
    offerer: String,
    answerer: String,
}

fn served_links<'h>(
    hops: impl IntoIterator<Item = &'h DesiredHop>,
) -> BTreeMap<PeerLinkId, ServedLink> {
    let mut offers = BTreeMap::new();
    let mut answers = BTreeMap::new();
    for hop in hops {
        for (role, socket) in hop.peer_ends() {
            let ends = match role {
                PeerRole::Offer => &mut offers,
                PeerRole::Answer => &mut answers,
            };
            ends.insert(
                socket.attempt.link.clone(),
                (
                    socket.attempt.generation,
                    !socket.turn.is_empty(),
                    hop.node_id.clone(),
                ),
            );
        }
    }
    offers
        .into_iter()
        .filter_map(|(link, (generation, turn_offered, offerer))| {
            let (answered, _, answerer) = answers.remove(&link)?;
            (answered == generation).then_some((
                link,
                ServedLink {
                    generation,
                    turn_offered,
                    offerer,
                    answerer,
                },
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    const LINK: &str = "weave-music-receiver-filip";

    fn node(id: &str, status: &str) -> NodeDescriptor {
        serde_json::from_value(json!({
            "id": id,
            "endpoint": format!("http://{id}:8092"),
            "status": status,
            "capabilities": {},
            "topology": { "attachments": [] }
        }))
        .unwrap()
    }

    fn hops() -> Vec<DesiredHop> {
        let socket =
            json!({ "transport": "webrtc", "link": LINK, "generation": 7, "stun": [], "turn": [] });
        let mut egress = socket.clone();
        egress["branch_id"] = json!("filip");
        serde_json::from_value(json!([
            {
                "id": "weave-music-sender", "node_id": "alfreds-mac", "profile_id": "app-audio-out",
                "role": "sender", "ingress": { "transport": "input", "id": "music" },
                "egresses": [egress]
            },
            {
                "id": LINK, "node_id": "filips-mac", "profile_id": "webrtc-to-speaker",
                "role": "receiver", "ingress": socket,
                "egresses": [{ "branch_id": "filip", "transport": "output", "id": "speakers" }]
            }
        ]))
        .unwrap()
    }

    fn report(condition: &str) -> Vec<HopStatus> {
        serde_json::from_value(json!([{
            "id": "weave-music-sender", "node_id": "alfreds-mac", "revision": "r",
            "state": "provisioned", "ingress": { "condition": "flowing" },
            "egresses": [{
                "branch_id": "filip", "condition": condition,
                "peer": { "link": LINK, "generation": 7, "relayed": false }
            }]
        }]))
        .unwrap()
    }

    fn described(peers: &mut Peers, at: Instant) {
        let description = |sdp: &str| {
            vec![LinkDescription {
                link: PeerLinkId::new(LINK),
                generation: 7,
                description: SessionDescription::new(sdp),
            }]
        };
        peers.exchange("alfreds-mac", description("v=0 offer"), at);
        peers.exchange("filips-mac", description("v=0 answer"), at);
    }

    fn failure(peers: &Peers, at: Instant) -> Option<PeerFailure> {
        peers
            .planning(at, 0)
            .failure(&node("alfreds-mac", "ready"), &node("filips-mac", "ready"))
            .cloned()
    }

    #[test]
    fn the_description_deadline_runs_only_while_both_ends_are_online() {
        let start = Instant::now();
        let after = |secs| start + Duration::from_secs(secs);
        let online = [node("alfreds-mac", "ready"), node("filips-mac", "ready")];
        let away = [node("alfreds-mac", "ready"), node("filips-mac", "offline")];
        let mut peers = Peers::default();
        peers.serve(&hops(), &online, start);

        peers.observe(&[], &away, after(15));
        peers.observe(&[], &online, after(30));
        assert_eq!(failure(&peers, after(30)), None);
        peers.observe(&[], &online, after(36));
        assert_eq!(
            failure(&peers, after(36)),
            Some(PeerFailure::NoDescription {
                node: "alfreds-mac".to_string(),
                role: PeerRole::Offer,
            })
        );
    }

    #[test]
    fn an_offerer_that_never_reports_a_result_meets_the_backstop() {
        let start = Instant::now();
        let after = |secs| start + Duration::from_secs(secs);
        let online = [node("alfreds-mac", "ready"), node("filips-mac", "ready")];
        let mut peers = Peers::default();
        peers.serve(&hops(), &online, start);
        described(&mut peers, after(5));

        peers.observe(&report("connecting"), &online, after(40));
        peers.observe(&report("connecting"), &online, after(64));
        assert_eq!(failure(&peers, after(64)), None);
        peers.observe(&report("connecting"), &online, after(66));
        assert_eq!(
            failure(&peers, after(66)),
            Some(PeerFailure::NoConnection {
                node: "alfreds-mac".to_string(),
            })
        );
    }

    #[test]
    fn a_connected_link_has_no_deadline_and_a_failed_pair_is_forgotten_after_two_minutes() {
        let start = Instant::now();
        let after = |secs| start + Duration::from_secs(secs);
        let online = [node("alfreds-mac", "ready"), node("filips-mac", "ready")];
        let mut peers = Peers::default();
        peers.serve(&hops(), &online, start);
        described(&mut peers, after(1));
        peers.observe(&report("flowing"), &online, after(5));
        peers.observe(&report("connected"), &online, after(200));
        assert_eq!(failure(&peers, after(200)), None);

        peers.observe(&report("unreachable"), &online, after(210));
        assert_eq!(
            failure(&peers, after(210)),
            Some(PeerFailure::Unreachable {
                turn: TurnUse::Unconfigured,
            })
        );
        assert!(failure(&peers, after(329)).is_some());
        peers.serve(std::iter::empty(), &online, after(330));
        peers.observe(&[], &online, after(330));
        assert_eq!(failure(&peers, after(330)), None);
    }

    #[test]
    fn a_failure_belongs_to_both_ends_in_either_order_and_to_no_other_pair() {
        let start = Instant::now();
        let online = [node("alfreds-mac", "ready"), node("filips-mac", "ready")];
        let mut peers = Peers::default();
        peers.serve(&hops(), &online, start);
        peers.observe(&report("unreachable"), &online, start);
        let planning = peers.planning(start, 0);
        let [alfred, filip] = online;
        let anna = node("annas-mac", "ready");
        assert!(planning.failure(&filip, &alfred).is_some());
        assert!(planning.failure(&alfred, &anna).is_none());
        assert!(planning.failure(&anna, &filip).is_none());
    }
}
