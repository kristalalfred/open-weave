mod stun;

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio::task::JoinHandle;
use weave_core::{HardwareAddress, NodeObservation, ReflexiveAddress, Uplink};

pub const STUN_SERVERS: [&str; 2] = ["stun.cloudflare.com:3478", "stun.l.google.com:19302"];

const CHECK_EVERY: Duration = Duration::from_secs(2);
const LOST_CHECK_EVERY: Duration = Duration::from_millis(250);
const REFRESH_EVERY: Duration = Duration::from_secs(60);
const STUN_TIMEOUT: Duration = Duration::from_millis(1500);
const GRACE_MISSES: u8 = 10;
const ANSWER_LIFETIME: Duration = Duration::from_secs(300);

pub struct Reach {
    latest: watch::Receiver<NodeObservation>,
    task: JoinHandle<()>,
}

impl Reach {
    pub async fn start(servers: &[&str]) -> Self {
        let servers: Vec<String> = servers.iter().map(ToString::to_string).collect();
        let state = advance(&servers, UplinkState::Unknown).await;
        let (sender, latest) = watch::channel(state.observation());
        let task = tokio::spawn(follow(servers, state, sender));
        Self { latest, task }
    }

    #[must_use]
    pub fn latest(&self) -> NodeObservation {
        self.latest.borrow().clone()
    }
}

impl Drop for Reach {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn follow(
    servers: Vec<String>,
    mut state: UplinkState,
    sender: watch::Sender<NodeObservation>,
) {
    loop {
        tokio::time::sleep(state.check_every()).await;
        state = advance(&servers, state).await;
        sender.send_replace(state.observation());
    }
}

async fn advance(servers: &[String], state: UplinkState) -> UplinkState {
    match check(state, route().await, Instant::now()) {
        Check::Settled(state) => state,
        Check::Stun { route, previous } => {
            let answers = stun::bindings(route.address, servers, STUN_TIMEOUT).await;
            UplinkState::Seen {
                sighting: carry_forward(previous, route, answers, Instant::now()),
                misses: 0,
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum UplinkState {
    Unknown,
    Seen { sighting: Sighting, misses: u8 },
    Lost(Sighting),
}

impl UplinkState {
    fn observation(&self) -> NodeObservation {
        let uplink = match self {
            Self::Seen { sighting, .. } => Some(sighting.uplink()),
            Self::Unknown | Self::Lost(_) => None,
        };
        NodeObservation { uplink }
    }

    fn check_every(&self) -> Duration {
        match self {
            Self::Lost(_) => LOST_CHECK_EVERY,
            Self::Unknown | Self::Seen { .. } => CHECK_EVERY,
        }
    }
}

enum Check {
    Settled(UplinkState),
    Stun {
        route: Route,
        previous: Option<Sighting>,
    },
}

fn check(state: UplinkState, read: Option<Route>, now: Instant) -> Check {
    match (state, read) {
        (UplinkState::Unknown, None) => Check::Settled(UplinkState::Unknown),
        (UplinkState::Unknown, Some(route)) => Check::Stun {
            route,
            previous: None,
        },
        (UplinkState::Seen { sighting, misses }, None) if misses < GRACE_MISSES => {
            Check::Settled(UplinkState::Seen {
                sighting,
                misses: misses + 1,
            })
        }
        (UplinkState::Seen { sighting, .. } | UplinkState::Lost(sighting), None) => {
            Check::Settled(UplinkState::Lost(sighting))
        }
        (UplinkState::Seen { sighting, .. }, Some(read)) => {
            let route = read.settled(&sighting.route);
            if route == sighting.route
                && !sighting.answers.is_empty()
                && now.duration_since(sighting.refreshed) < REFRESH_EVERY
            {
                Check::Settled(UplinkState::Seen {
                    sighting,
                    misses: 0,
                })
            } else {
                Check::Stun {
                    route,
                    previous: Some(sighting),
                }
            }
        }
        (UplinkState::Lost(last), Some(read)) => {
            let route = read.settled(&last.route);
            if route == last.route {
                Check::Settled(UplinkState::Seen {
                    sighting: last,
                    misses: 0,
                })
            } else {
                Check::Stun {
                    route,
                    previous: None,
                }
            }
        }
    }
}

fn carry_forward(
    previous: Option<Sighting>,
    route: Route,
    answers: BTreeMap<String, ReflexiveAddress>,
    now: Instant,
) -> Sighting {
    let mut kept = previous
        .filter(|previous| previous.route == route)
        .map(|previous| previous.answers)
        .unwrap_or_default();
    let fresh: BTreeMap<String, Answer> = answers
        .into_iter()
        .filter(|(_, reflexive)| publishable(reflexive))
        .map(|(server, reflexive)| {
            (
                server,
                Answer {
                    reflexive,
                    received: now,
                },
            )
        })
        .collect();
    if !fresh.is_empty() {
        kept.retain(|_, answer| now.duration_since(answer.received) <= ANSWER_LIFETIME);
    }
    kept.extend(fresh);
    Sighting {
        route,
        answers: kept,
        refreshed: now,
    }
}

fn publishable(answer: &ReflexiveAddress) -> bool {
    let server = answer.server.ip();
    let unicast = !(server.is_unspecified()
        || server.is_loopback()
        || server.is_multicast()
        || server.is_broadcast());
    unicast && !answer.mapped.ip().is_unspecified() && answer.mapped.port() != 0
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Sighting {
    route: Route,
    answers: BTreeMap<String, Answer>,
    refreshed: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Answer {
    reflexive: ReflexiveAddress,
    received: Instant,
}

impl Sighting {
    fn uplink(&self) -> Uplink {
        Uplink {
            interface: self.route.interface.clone(),
            address: self.route.address,
            prefix_len: self.route.prefix_len,
            gateway: self.route.gateway,
            reflexive: self
                .answers
                .values()
                .map(|answer| answer.reflexive)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Route {
    interface: String,
    address: Ipv4Addr,
    prefix_len: u8,
    gateway: Option<HardwareAddress>,
}

impl Route {
    fn of(interface: &netdev::Interface) -> Option<Self> {
        let gateway = interface.gateway.as_ref();
        let gateway_addresses = gateway.map_or(&[][..], |gateway| &gateway.ipv4);
        let network = interface
            .ipv4
            .iter()
            .find(|network| {
                gateway_addresses
                    .iter()
                    .any(|address| network.contains(address))
            })
            .or_else(|| interface.ipv4.first())?;
        Some(Self {
            interface: interface.name.clone(),
            address: network.addr(),
            prefix_len: network.prefix_len(),
            gateway: gateway
                .map(|gateway| gateway.mac_addr.octets())
                .filter(|octets| *octets != [0; 6] && *octets != [0xff; 6])
                .map(HardwareAddress),
        })
    }

    fn settled(self, previous: &Route) -> Self {
        if self.gateway.is_none()
            && self.interface == previous.interface
            && self.address == previous.address
            && self.prefix_len == previous.prefix_len
        {
            previous.clone()
        } else {
            self
        }
    }
}

async fn route() -> Option<Route> {
    match tokio::task::spawn_blocking(netdev::get_default_interface).await {
        Ok(interface) => interface.ok().as_ref().and_then(Route::of),
        Err(error) => {
            tracing::warn!(%error, "cannot read this node's uplink");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CLOUDFLARE: &str = "stun.cloudflare.com:3478";
    const GOOGLE: &str = "stun.l.google.com:19302";
    const MINUTE: Duration = Duration::from_secs(60);

    fn lan(address: &str) -> Route {
        Route {
            interface: "en0".to_string(),
            address: address.parse().unwrap(),
            prefix_len: 24,
            gateway: Some("02:00:00:00:00:01".parse().unwrap()),
        }
    }

    fn answer(server: &str, mapped: &str) -> ReflexiveAddress {
        ReflexiveAddress {
            server: server.parse().unwrap(),
            mapped: mapped.parse().unwrap(),
        }
    }

    fn public() -> ReflexiveAddress {
        answer("162.159.207.0:3478", "203.0.113.7:61000")
    }

    fn round(
        previous: Option<Sighting>,
        route: Route,
        entries: &[(&str, ReflexiveAddress)],
        at: Instant,
    ) -> Sighting {
        let answers = entries
            .iter()
            .map(|(configured, answer)| (configured.to_string(), *answer))
            .collect();
        carry_forward(previous, route, answers, at)
    }

    fn reflexive(sighting: &Sighting) -> Vec<ReflexiveAddress> {
        sighting.uplink().reflexive
    }

    fn seen(sighting: Sighting) -> UplinkState {
        UplinkState::Seen {
            sighting,
            misses: 0,
        }
    }

    fn reachable(start: Instant) -> UplinkState {
        seen(round(
            None,
            lan("192.168.1.20"),
            &[(CLOUDFLARE, public())],
            start,
        ))
    }

    fn without_stun(check: Check) -> UplinkState {
        match check {
            Check::Settled(state) => state,
            Check::Stun { route, .. } => panic!("unexpected STUN round on {route:?}"),
        }
    }

    fn after_misses(mut state: UplinkState, misses: u32, start: Instant) -> UplinkState {
        for miss in 1..=misses {
            state = without_stun(check(state, None, start + CHECK_EVERY * miss));
        }
        state
    }

    fn grace() -> u32 {
        u32::from(GRACE_MISSES)
    }

    fn lost(start: Instant) -> UplinkState {
        after_misses(reachable(start), grace() + 1, start)
    }

    #[test]
    fn a_failed_stun_round_keeps_the_last_public_address_while_the_route_holds() {
        let start = Instant::now();
        let last = public();
        let first = round(None, lan("192.168.1.20"), &[(CLOUDFLARE, last)], start);

        let next = round(Some(first), lan("192.168.1.20"), &[], start + MINUTE);

        assert_eq!(reflexive(&next), vec![last]);
    }

    #[test]
    fn a_moved_route_drops_the_last_public_address() {
        let start = Instant::now();
        let first = round(None, lan("192.168.1.20"), &[(CLOUDFLARE, public())], start);

        let next = round(Some(first), lan("10.0.0.5"), &[], start + MINUTE);

        assert_eq!(
            next.uplink().address,
            "10.0.0.5".parse::<Ipv4Addr>().unwrap()
        );
        assert_eq!(reflexive(&next), vec![]);
    }

    #[test]
    fn a_fresh_public_address_replaces_the_last_one() {
        let start = Instant::now();
        let fresh = answer("162.159.207.0:3478", "198.51.100.4:61000");
        let first = round(None, lan("192.168.1.20"), &[(CLOUDFLARE, public())], start);

        let next = round(
            Some(first),
            lan("192.168.1.20"),
            &[(CLOUDFLARE, fresh)],
            start + MINUTE,
        );

        assert_eq!(reflexive(&next), vec![fresh]);
    }

    #[test]
    fn a_server_that_misses_a_round_keeps_its_last_answer_while_the_route_holds() {
        let start = Instant::now();
        let google_last = answer("74.125.250.129:19302", "203.0.113.7:61000");
        let cloudflare_fresh = answer("162.159.207.0:3478", "203.0.113.7:61001");
        let first = round(
            None,
            lan("192.168.1.20"),
            &[(CLOUDFLARE, public()), (GOOGLE, google_last)],
            start,
        );

        let next = round(
            Some(first),
            lan("192.168.1.20"),
            &[(CLOUDFLARE, cloudflare_fresh)],
            start + MINUTE,
        );

        assert_eq!(reflexive(&next), vec![cloudflare_fresh, google_last]);
    }

    #[test]
    fn a_server_that_resolves_to_a_new_address_replaces_its_last_answer() {
        let start = Instant::now();
        let last = answer("74.125.250.129:19302", "203.0.113.7:61000");
        let rotated = answer("142.250.82.95:19302", "203.0.113.7:61000");
        let first = round(None, lan("192.168.1.20"), &[(GOOGLE, last)], start);

        let next = round(
            Some(first),
            lan("192.168.1.20"),
            &[(GOOGLE, rotated)],
            start + MINUTE,
        );

        assert_eq!(reflexive(&next), vec![rotated]);
    }

    #[test]
    fn a_moved_route_keeps_only_this_rounds_answers() {
        let start = Instant::now();
        let google_last = answer("74.125.250.129:19302", "203.0.113.7:61000");
        let cloudflare_fresh = answer("162.159.207.0:3478", "198.51.100.4:61000");
        let first = round(
            None,
            lan("192.168.1.20"),
            &[(CLOUDFLARE, public()), (GOOGLE, google_last)],
            start,
        );

        let next = round(
            Some(first),
            lan("10.0.0.5"),
            &[(CLOUDFLARE, cloudflare_fresh)],
            start + MINUTE,
        );

        assert_eq!(reflexive(&next), vec![cloudflare_fresh]);
    }

    #[test]
    fn a_silent_servers_answer_expires_five_minutes_after_it_was_received() {
        let start = Instant::now();
        let google_last = answer("74.125.250.129:19302", "203.0.113.7:61000");
        let cloudflare_fresh = answer("162.159.207.0:3478", "203.0.113.7:61001");
        let first = round(
            None,
            lan("192.168.1.20"),
            &[(CLOUDFLARE, public()), (GOOGLE, google_last)],
            start,
        );

        let at_five_minutes = round(
            Some(first.clone()),
            lan("192.168.1.20"),
            &[(CLOUDFLARE, cloudflare_fresh)],
            start + 5 * MINUTE,
        );
        let past_five_minutes = round(
            Some(first),
            lan("192.168.1.20"),
            &[(CLOUDFLARE, cloudflare_fresh)],
            start + 5 * MINUTE + Duration::from_secs(1),
        );

        assert_eq!(
            reflexive(&at_five_minutes),
            vec![cloudflare_fresh, google_last]
        );
        assert_eq!(reflexive(&past_five_minutes), vec![cloudflare_fresh]);
    }

    #[test]
    fn a_total_stun_outage_keeps_every_carried_answer_while_the_route_holds() {
        let start = Instant::now();
        let google_last = answer("74.125.250.129:19302", "203.0.113.7:61000");
        let mut sighting = round(
            None,
            lan("192.168.1.20"),
            &[(CLOUDFLARE, public()), (GOOGLE, google_last)],
            start,
        );

        for minute in 1..=10 {
            sighting = round(
                Some(sighting),
                lan("192.168.1.20"),
                &[],
                start + MINUTE * minute,
            );
        }

        assert_eq!(reflexive(&sighting), vec![public(), google_last]);
    }

    #[test]
    fn a_round_with_only_rejected_answers_expires_nothing() {
        let start = Instant::now();
        let google_last = answer("74.125.250.129:19302", "203.0.113.7:61000");
        let first = round(
            None,
            lan("192.168.1.20"),
            &[(CLOUDFLARE, public()), (GOOGLE, google_last)],
            start,
        );

        let next = round(
            Some(first),
            lan("192.168.1.20"),
            &[(CLOUDFLARE, answer("162.159.207.0:3478", "0.0.0.0:0"))],
            start + 10 * MINUTE,
        );

        assert_eq!(reflexive(&next), vec![public(), google_last]);
    }

    #[test]
    fn an_answer_the_controller_rejects_is_never_published() {
        for rejected in [
            answer("162.159.207.0:3478", "0.0.0.0:61000"),
            answer("162.159.207.0:3478", "203.0.113.7:0"),
            answer("0.0.0.0:3478", "203.0.113.7:61000"),
            answer("127.0.0.1:3478", "203.0.113.7:61000"),
            answer("224.0.0.1:3478", "203.0.113.7:61000"),
            answer("255.255.255.255:3478", "203.0.113.7:61000"),
        ] {
            let next = round(
                None,
                lan("192.168.1.20"),
                &[(CLOUDFLARE, rejected)],
                Instant::now(),
            );

            assert_eq!(reflexive(&next), vec![], "{rejected:?}");
        }
    }

    #[test]
    fn a_rejected_answer_keeps_the_servers_last_publishable_one() {
        let start = Instant::now();
        let last = public();
        let first = round(None, lan("192.168.1.20"), &[(CLOUDFLARE, last)], start);

        let next = round(
            Some(first),
            lan("192.168.1.20"),
            &[(CLOUDFLARE, answer("162.159.207.0:3478", "0.0.0.0:0"))],
            start + MINUTE,
        );

        assert_eq!(reflexive(&next), vec![last]);
    }

    #[test]
    fn a_lost_route_keeps_the_uplink_through_the_grace_and_clears_it_on_the_next_miss() {
        let start = Instant::now();
        let mut state = reachable(start);
        let published = state.observation();

        for miss in 1..=grace() {
            state = without_stun(check(state, None, start + CHECK_EVERY * miss));
            assert_eq!(state.observation(), published, "miss {miss}");
        }
        state = without_stun(check(state, None, start + CHECK_EVERY * (grace() + 1)));

        assert_eq!(state.observation(), NodeObservation::default());
    }

    #[test]
    fn a_good_read_restarts_the_grace() {
        let start = Instant::now();
        let mut state = reachable(start);
        let published = state.observation();

        for _ in 1..grace() {
            state = without_stun(check(state, None, start));
        }
        state = without_stun(check(state, Some(lan("192.168.1.20")), start));
        for _ in 0..grace() {
            state = without_stun(check(state, None, start));
        }
        assert_eq!(state.observation(), published);

        state = without_stun(check(state, None, start));
        assert_eq!(state.observation(), NodeObservation::default());
    }

    #[test]
    fn a_route_that_returns_after_the_uplink_was_lost_republishes_it_on_the_same_check() {
        let start = Instant::now();
        let mut state = reachable(start);
        let published = state.observation();

        for miss in 1..=grace() + 2 {
            state = without_stun(check(state, None, start + CHECK_EVERY * miss));
            let expected = if miss <= grace() {
                published.clone()
            } else {
                NodeObservation::default()
            };
            assert_eq!(state.observation(), expected, "miss {miss}");
        }
        state = without_stun(check(
            state,
            Some(lan("192.168.1.20")),
            start + CHECK_EVERY * (grace() + 2) + LOST_CHECK_EVERY,
        ));

        assert_eq!(state.observation(), published);
    }

    #[test]
    fn a_route_that_returns_with_an_unresolved_gateway_republishes_the_last_uplink() {
        let start = Instant::now();
        let published = reachable(start).observation();
        let unresolved = Route {
            gateway: None,
            ..lan("192.168.1.20")
        };

        let back = without_stun(check(lost(start), Some(unresolved), start + 2 * MINUTE));

        assert_eq!(back.observation(), published);
    }

    #[test]
    fn a_lost_uplink_stays_lost_through_further_misses_and_still_returns() {
        let start = Instant::now();
        let published = reachable(start).observation();
        let mut state = lost(start);

        for miss in grace() + 2..=grace() + 30 {
            state = without_stun(check(state, None, start + CHECK_EVERY * miss));
            assert_eq!(
                state.observation(),
                NodeObservation::default(),
                "miss {miss}"
            );
        }
        state = without_stun(check(
            state,
            Some(lan("192.168.1.20")),
            start + CHECK_EVERY * (grace() + 31),
        ));

        assert_eq!(state.observation(), published);
    }

    #[test]
    fn only_a_lost_uplink_is_read_every_quarter_second() {
        let start = Instant::now();
        let in_grace = after_misses(reachable(start), grace(), start);

        assert_eq!(lost(start).check_every(), Duration::from_millis(250));
        for state in [UplinkState::Unknown, reachable(start), in_grace] {
            assert_eq!(state.check_every(), Duration::from_secs(2), "{state:?}");
        }
    }

    #[test]
    fn an_overdue_refresh_after_the_uplink_returns_runs_on_the_next_check_and_carries_its_answers()
    {
        let start = Instant::now();
        let lost = lost(start);
        let back_at = start + 2 * MINUTE;
        let back = without_stun(check(lost, Some(lan("192.168.1.20")), back_at));

        let Check::Stun { route, previous } =
            check(back, Some(lan("192.168.1.20")), back_at + CHECK_EVERY)
        else {
            panic!("an overdue refresh must run STUN");
        };
        let refreshed = carry_forward(previous, route, BTreeMap::new(), back_at + CHECK_EVERY);

        assert_eq!(reflexive(&refreshed), vec![public()]);
    }

    #[test]
    fn a_different_route_after_the_uplink_was_lost_runs_a_fresh_stun_round() {
        let start = Instant::now();
        let lost = lost(start);

        let Check::Stun { route, previous } = check(lost, Some(lan("10.0.0.5")), start + MINUTE)
        else {
            panic!("a different route must run STUN");
        };
        let fresh = carry_forward(previous, route, BTreeMap::new(), start + MINUTE);

        assert_eq!(
            fresh.uplink().address,
            "10.0.0.5".parse::<Ipv4Addr>().unwrap()
        );
        assert_eq!(reflexive(&fresh), vec![]);
    }

    #[test]
    fn a_route_without_a_current_uplink_runs_stun() {
        let Check::Stun { route, previous } = check(
            UplinkState::Unknown,
            Some(lan("192.168.1.20")),
            Instant::now(),
        ) else {
            panic!("a new route must run STUN");
        };

        assert_eq!((route, previous), (lan("192.168.1.20"), None));
    }

    #[test]
    fn an_unknown_uplink_publishes_nothing_until_a_route_is_read() {
        let start = Instant::now();
        let state = after_misses(UplinkState::Unknown, 10, start);

        assert_eq!(state.observation(), NodeObservation::default());
        assert!(matches!(
            check(state, Some(lan("192.168.1.20")), start + MINUTE),
            Check::Stun { previous: None, .. }
        ));
    }

    #[test]
    fn a_sighting_with_an_answer_runs_stun_again_once_a_minute() {
        let start = Instant::now();
        let state = reachable(start);

        let next = check(
            state.clone(),
            Some(lan("192.168.1.20")),
            start + CHECK_EVERY,
        );
        let before = check(
            state.clone(),
            Some(lan("192.168.1.20")),
            start + REFRESH_EVERY - CHECK_EVERY,
        );
        let due = check(state, Some(lan("192.168.1.20")), start + REFRESH_EVERY);

        assert!(matches!(next, Check::Settled(UplinkState::Seen { .. })));
        assert!(matches!(before, Check::Settled(UplinkState::Seen { .. })));
        assert!(matches!(due, Check::Stun { .. }));
    }

    #[test]
    fn a_sighting_without_answers_runs_stun_again_on_the_next_check() {
        let start = Instant::now();
        let empty = seen(round(None, lan("192.168.1.20"), &[], start));

        let next = check(empty, Some(lan("192.168.1.20")), start + CHECK_EVERY);

        assert!(matches!(next, Check::Stun { .. }));
    }

    #[test]
    fn an_unresolved_gateway_keeps_the_last_hardware_address_without_a_stun_round() {
        let start = Instant::now();
        let state = reachable(start);
        let published = state.observation();
        let unresolved = Route {
            gateway: None,
            ..lan("192.168.1.20")
        };

        let next = without_stun(check(state, Some(unresolved), start + CHECK_EVERY));

        assert_eq!(next.observation(), published);
    }

    #[test]
    fn a_changed_gateway_address_prefix_or_interface_is_a_moved_route() {
        let start = Instant::now();
        let held = lan("192.168.1.20");
        for moved in [
            Route {
                gateway: Some("02:00:00:00:00:02".parse().unwrap()),
                ..held.clone()
            },
            Route {
                address: "192.168.1.21".parse().unwrap(),
                ..held.clone()
            },
            Route {
                address: "192.168.1.21".parse().unwrap(),
                gateway: None,
                ..held.clone()
            },
            Route {
                prefix_len: 16,
                ..held.clone()
            },
            Route {
                interface: "en1".to_string(),
                ..held.clone()
            },
        ] {
            let state = seen(round(None, held.clone(), &[(CLOUDFLARE, public())], start));

            let Check::Stun { route, .. } = check(state, Some(moved.clone()), start + CHECK_EVERY)
            else {
                panic!("{moved:?} must be a moved route");
            };
            assert_eq!(route, moved);
        }
    }

    fn interface(networks: &[&str], gateway: Option<([u8; 6], &str)>) -> netdev::Interface {
        let mut interface = netdev::Interface::dummy();
        interface.name = "en0".to_string();
        interface.ipv4 = networks
            .iter()
            .map(|network| network.parse().unwrap())
            .collect();
        interface.gateway = gateway.map(|(mac, address)| netdev::NetworkDevice {
            mac_addr: netdev::MacAddr::from_octets(mac),
            ipv4: vec![address.parse().unwrap()],
            ipv6: Vec::new(),
        });
        interface
    }

    const ROUTER: [u8; 6] = [0xb8, 0x27, 0xc5, 0x47, 0x89, 0x6f];

    #[test]
    fn the_uplink_is_the_address_on_the_gateways_subnet() {
        let en0 = interface(
            &["169.254.10.2/16", "192.168.3.121/24"],
            Some((ROUTER, "192.168.3.1")),
        );

        assert_eq!(
            Route::of(&en0),
            Some(Route {
                interface: "en0".to_string(),
                address: "192.168.3.121".parse().unwrap(),
                prefix_len: 24,
                gateway: Some(HardwareAddress(ROUTER)),
            })
        );
    }

    #[test]
    fn without_a_gateway_the_uplink_is_the_first_ipv4_address() {
        let en0 = interface(&["10.1.2.3/8", "192.168.3.121/24"], None);

        let route = Route::of(&en0).unwrap();

        assert_eq!(
            (route.address, route.prefix_len, route.gateway),
            ("10.1.2.3".parse().unwrap(), 8, None)
        );
    }

    #[test]
    fn an_unresolved_or_broadcast_gateway_hardware_address_is_unknown() {
        for mac in [[0; 6], [0xff; 6]] {
            let en0 = interface(&["192.168.3.121/24"], Some((mac, "192.168.3.1")));

            assert_eq!(Route::of(&en0).unwrap().gateway, None);
        }
    }

    #[test]
    fn an_interface_without_ipv4_is_no_uplink() {
        let en0 = interface(&[], Some((ROUTER, "192.168.3.1")));

        assert_eq!(Route::of(&en0), None);
    }
}
