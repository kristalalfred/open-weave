use sha2::{Digest, Sha256};
use weave_core::{
    DeliveryReport, DeliveryRole, DeliveryState, DesiredHop, HopProfile, HopRole, NodeDescriptor,
    PathStatus, PayloadKind, SocketSpec, StreamCondition, StreamConditionReason,
    StreamConditionStatus, StreamConditionType, StreamDefinition, StreamDestination, TimedHop,
};

use crate::path::PlacementError;

pub fn origin(
    stream: &StreamDefinition,
    nodes: &[NodeDescriptor],
) -> Result<Option<TimedHop>, PlacementError> {
    if !stream
        .destinations
        .iter()
        .any(|destination| destination.delivery.is_some())
    {
        return Ok(None);
    }
    let source = stream.source.node().unwrap_or_default();
    let node = nodes.iter().find(|node| node.id == source).ok_or_else(|| {
        PlacementError::NodeNotRegistered {
            node: source.into(),
        }
    })?;
    let mut profiles: Vec<_> = node.capabilities.hop_profiles.iter().collect();
    profiles.sort_by(|a, b| a.id.cmp(&b.id));
    let capability = profiles
        .iter()
        .filter_map(|profile| profile.timed_delivery.as_ref())
        .find(|capability| {
            capability.protocol == weave_core::delivery::TIMED_PROTOCOL
                && capability.roles.contains(&DeliveryRole::Origin)
        })
        .ok_or_else(|| PlacementError::TimedUnsupported {
            node: source.into(),
        })?;
    Ok(Some(TimedHop {
        delivery_id: [0; 16],
        epoch: capability.origin_epoch,
        clock_domain: capability.clock_domain.clone(),
        role: DeliveryRole::Origin,
        policy: None,
        paths: 1,
    }))
}

pub fn eligible(
    nodes: &[NodeDescriptor],
    source: &str,
    destination: &StreamDestination,
    origin: &TimedHop,
) -> Vec<NodeDescriptor> {
    let receiver = destination.node().unwrap_or_default();
    let maximum = destination
        .delivery
        .as_ref()
        .map_or(0, |policy| policy.maximum_ms());
    nodes
        .iter()
        .cloned()
        .map(|mut node| {
            let is_source = node.id == source;
            let is_receiver = node.id == receiver;
            node.capabilities.hop_profiles.retain(|profile| {
                profile.timed_delivery.as_ref().is_some_and(|capability| {
                    let roles = &capability.roles;
                    capability.protocol == weave_core::delivery::TIMED_PROTOCOL
                        && capability.max_latency_ms >= maximum
                        && ((is_source
                            && roles.contains(&DeliveryRole::Origin)
                            && capability.clock_domain == origin.clock_domain)
                            || (is_receiver
                                && roles.contains(&DeliveryRole::Receiver)
                                && capability.clock_domain == origin.clock_domain)
                            || (!is_source
                                && !is_receiver
                                && roles.contains(&DeliveryRole::Forward)))
                })
            });
            node
        })
        .collect()
}

pub fn decorate(hop: &mut DesiredHop, origin: &TimedHop, destination: Option<&StreamDestination>) {
    let mut timing = origin.clone();
    timing.role = match hop.role {
        HopRole::Sender => DeliveryRole::Origin,
        HopRole::Bridge => DeliveryRole::Forward,
        HopRole::Receiver => DeliveryRole::Receiver,
    };
    timing.policy = None;
    timing.paths = 1;
    if timing.role == DeliveryRole::Receiver
        && let Some(destination) = destination
    {
        timing.policy.clone_from(&destination.delivery);
        timing.paths = destination.paths;
    }
    if timing.role != DeliveryRole::Origin {
        set_payload(&mut hop.ingress, PayloadKind::WeaveTimedV1);
        if let Some(merge) = &mut hop.merge_ingress {
            set_payload(merge, PayloadKind::WeaveTimedV1);
        }
    }
    if timing.role == DeliveryRole::Forward {
        for egress in &mut hop.egresses {
            set_payload(&mut egress.socket, PayloadKind::WeaveTimedV1);
        }
    }
    hop.delivery = Some(timing);
}

pub fn set_payload(socket: &mut SocketSpec, payload: PayloadKind) {
    if let SocketSpec::Srt(socket) = socket {
        socket.params_mut().payload = payload;
    }
}

pub fn profile_matches(hop: &DesiredHop, profile: &HopProfile) -> bool {
    match (&hop.delivery, &profile.timed_delivery) {
        (None, None) => true,
        (Some(timing), Some(capability)) => {
            capability.protocol == weave_core::delivery::TIMED_PROTOCOL
                && capability.roles.contains(&timing.role)
                && (timing.role == DeliveryRole::Forward
                    || capability.clock_domain == timing.clock_domain)
                && timing.policy.as_ref().is_none_or(|policy| {
                    capability.max_latency_ms >= policy.maximum_ms()
                        && u64::from(capability.scheduling_error_bound_us)
                            < u64::from(policy.tolerance_ms()) * 1_000
                })
        }
        _ => false,
    }
}

pub fn check_budget(
    destination: &StreamDestination,
    links: usize,
    latency_ms: u32,
) -> Result<(), PlacementError> {
    if let Some(policy) = &destination.delivery {
        let configured = links as u64 * u64::from(latency_ms);
        if configured >= u64::from(policy.maximum_ms().saturating_sub(policy.tolerance_ms())) {
            return Err(PlacementError::LatencyBudget {
                destination: destination.id.clone(),
                configured_ms: configured,
                budget_ms: policy.maximum_ms(),
            });
        }
    }
    Ok(())
}

pub fn bind(hops: &mut [DesiredHop], stream: &StreamDefinition, source_revision: u64) {
    let mut hash = Sha256::new();
    hash.update(b"weave timed source\0");
    hash.update(source_revision.to_be_bytes());
    hash.update(stream.name.as_bytes());
    hash.update(serde_json::to_vec(&stream.source).expect("stream source serializes"));
    let id: [u8; 16] = hash.finalize()[..16].try_into().unwrap();
    for hop in hops {
        if let Some(timing) = &mut hop.delivery {
            timing.delivery_id = id;
        }
    }
}

pub fn status(
    base: PathStatus,
    report: Option<&DeliveryReport>,
    requested_paths: u8,
) -> PathStatus {
    if matches!(base, PathStatus::Idle | PathStatus::Failed) {
        return base;
    }
    let Some(report) = report else {
        return if base == PathStatus::AwaitingInput {
            base
        } else {
            PathStatus::Pending
        };
    };
    match report.state {
        DeliveryState::Failed => PathStatus::Failed,
        DeliveryState::Calibrating => PathStatus::Pending,
        DeliveryState::AwaitingInput if base == PathStatus::AwaitingInput => {
            PathStatus::AwaitingInput
        }
        DeliveryState::AwaitingInput => PathStatus::Pending,
        _ if !report.timing_ready || (requested_paths > 1 && !report.redundancy_ready) => {
            PathStatus::Degraded
        }
        _ => base,
    }
}

pub fn effective_report(
    receiver: Option<&DeliveryReport>,
    origin: Option<&DeliveryReport>,
) -> Option<DeliveryReport> {
    if let Some(origin) = origin.filter(|report| {
        matches!(
            report.state,
            DeliveryState::Degraded | DeliveryState::Failed
        )
    }) {
        let mut report = receiver.cloned().unwrap_or_default();
        report.state = origin.state;
        report.reason = origin.reason;
        report.timing_ready = false;
        report.redundancy_ready = false;
        return Some(report);
    }
    receiver.cloned()
}

pub fn conditions(report: Option<&DeliveryReport>, requested_paths: u8) -> Vec<StreamCondition> {
    let mut timing = crate::stream_condition(
        StreamConditionType::TimingReady,
        StreamConditionStatus::Unknown,
        StreamConditionReason::HopsPending,
        "waiting for current timed receiver evidence",
    );
    let mut redundancy = crate::stream_condition(
        StreamConditionType::RedundancyReady,
        StreamConditionStatus::Unknown,
        StreamConditionReason::HopsPending,
        "waiting for current redundant-path evidence",
    );
    if let Some(report) = report {
        timing = if report.timing_ready {
            crate::stream_condition(
                StreamConditionType::TimingReady,
                StreamConditionStatus::True,
                StreamConditionReason::TimingReady,
                format!(
                    "scheduled release at {} ms",
                    report.selected_latency_ms.unwrap_or_default()
                ),
            )
        } else if report.state == DeliveryState::Calibrating {
            crate::stream_condition(
                StreamConditionType::TimingReady,
                StreamConditionStatus::Unknown,
                StreamConditionReason::TimingCalibrating,
                "measuring each available path before choosing a fixed release delay",
            )
        } else if report.state == DeliveryState::AwaitingInput {
            crate::stream_condition(
                StreamConditionType::TimingReady,
                StreamConditionStatus::Unknown,
                StreamConditionReason::AwaitingInput,
                "waiting for timely scheduled releases",
            )
        } else {
            crate::stream_condition(
                StreamConditionType::TimingReady,
                StreamConditionStatus::False,
                StreamConditionReason::TimingDegraded,
                format!("timed receiver reports {:?}", report.reason),
            )
        };
        redundancy = crate::stream_condition(
            StreamConditionType::RedundancyReady,
            if report.redundancy_ready {
                StreamConditionStatus::True
            } else {
                StreamConditionStatus::False
            },
            if report.redundancy_ready {
                StreamConditionReason::RedundancyReady
            } else {
                StreamConditionReason::RedundancyDegraded
            },
            format!(
                "{} of {} requested paths have recent timely media",
                report.paths.iter().filter(|path| path.qualified).count(),
                requested_paths
            ),
        );
    }
    if requested_paths == 1 {
        redundancy = crate::stream_condition(
            StreamConditionType::RedundancyReady,
            StreamConditionStatus::True,
            StreamConditionReason::NotRequested,
            "one path requested",
        );
    }
    vec![timing, redundancy]
}

pub fn aggregate(destinations: &[weave_core::StreamDestinationStatus]) -> Vec<StreamCondition> {
    let mut result = Vec::new();
    for kind in [
        StreamConditionType::TimingReady,
        StreamConditionType::RedundancyReady,
    ] {
        let selected = destinations
            .iter()
            .flat_map(|destination| {
                destination
                    .conditions
                    .iter()
                    .filter(move |condition| condition.condition_type == kind)
                    .map(move |condition| (destination, condition))
            })
            .min_by_key(|(destination, condition)| {
                (
                    match condition.status {
                        StreamConditionStatus::False => 0,
                        StreamConditionStatus::Unknown => 1,
                        StreamConditionStatus::True => 2,
                    },
                    &destination.id,
                )
            });
        if let Some((destination, condition)) = selected {
            let mut condition = condition.clone();
            condition.detail = format!("destination {}: {}", destination.id, condition.detail);
            result.push(condition);
        }
    }
    result
}
