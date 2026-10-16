use weave_core::{Container, FormatConstraint, FormatPolicy, MediaBehavior, MediaFormat};

fn declared(name: &str) -> StreamDefinition {
    let mut stream = stream(name);
    stream.format_policy = FormatPolicy::Strict;
    source(&mut stream).format = Some(MediaFormat {
        container: Container::MpegTs,
        video: None,
        audio: None,
    });
    destination(&mut stream).accepts = Some(FormatConstraint::default());
    stream
}

fn source(stream: &mut StreamDefinition) -> &mut weave_core::SrtEndpoint {
    let StreamTransport::Srt(endpoint) = &mut stream.source else {
        unreachable!()
    };
    endpoint
}

fn destination(stream: &mut StreamDefinition) -> &mut weave_core::SrtEndpoint {
    let StreamTransport::Srt(endpoint) = &mut stream.destinations[0].endpoint else {
        unreachable!()
    };
    endpoint
}

fn observed_nodes() -> ObservedState {
    observed_state(&BTreeMap::from([
        (
            "strom-node-1".into(),
            node_registration("strom-node-1", "172.26.0.10"),
        ),
        (
            "strom-node-2".into(),
            node_registration("strom-node-2", "172.27.0.10"),
        ),
    ]))
}

fn condition(conditions: &[StreamCondition], kind: StreamConditionType) -> &StreamCondition {
    conditions
        .iter()
        .find(|condition| condition.condition_type == kind)
        .unwrap()
}

fn format(outcome: &ReconcileOutcome) -> &StreamCondition {
    condition(
        &outcome.streams[0].conditions,
        StreamConditionType::FormatCompatible,
    )
}

fn run(stream: StreamDefinition, observed: &ObservedState) -> ReconcileOutcome {
    reconcile(vec![stream], observed, &LinkKeys::for_tests())
}

#[test]
fn strict_declarations_distinguish_missing_empty_and_mismatching_requirements() {
    let observed = observed_nodes();
    let stream = declared("feed");
    let compatible = run(stream.clone(), &observed);
    assert_eq!(format(&compatible).status, StreamConditionStatus::True);
    assert!(compatible.endpoints.contains_key("feed"));
    for (change, reason) in [
        (0, StreamConditionReason::SourceFormatUnknown),
        (1, StreamConditionReason::DestinationAcceptsUnknown),
        (2, StreamConditionReason::FormatMismatch),
        (3, StreamConditionReason::FormatMismatch),
    ] {
        let mut candidate = stream.clone();
        match change {
            0 => source(&mut candidate).format = None,
            1 => destination(&mut candidate).accepts = None,
            2 => {
                destination(&mut candidate)
                    .accepts
                    .as_mut()
                    .unwrap()
                    .container = Some(vec![Container::Rtp])
            }
            _ => {
                destination(&mut candidate).accepts.as_mut().unwrap().audio =
                    Some(Default::default())
            }
        }
        let blocked = run(candidate, &observed);
        assert_eq!(blocked.streams[0].status, PathStatus::Pending);
        assert_eq!(format(&blocked).reason, reason);
        assert_eq!(
            condition(
                &blocked.streams[0].conditions,
                StreamConditionType::PlacementReady
            )
            .reason,
            reason
        );
        assert!(blocked.desired_by_node.values().all(Vec::is_empty));
        assert!(blocked.endpoints.is_empty());
        assert!(blocked.streams[0].destinations[0].endpoint.is_none());
    }
}

#[test]
fn unknown_behavior_erases_output_but_missing_accepts_does_not() {
    let mut observed = observed_nodes();
    let mut stream = declared("feed");
    stream.format_policy = FormatPolicy::Report;
    destination(&mut stream).accepts.as_mut().unwrap().container = Some(vec![Container::Rtp]);
    observed.nodes[0].capabilities.hop_profiles[0].accepts = None;
    assert_eq!(
        format(&run(stream.clone(), &observed)).reason,
        StreamConditionReason::FormatMismatch
    );
    observed.nodes[0].capabilities.hop_profiles[0].accepts = Some(Default::default());
    observed.nodes[0].capabilities.hop_profiles[0].media_behavior = MediaBehavior::Unknown;
    let unknown = run(stream.clone(), &observed);
    assert_eq!(
        format(&unknown).reason,
        StreamConditionReason::MediaBehaviorUnknown
    );
    assert_ne!(unknown.streams[0].status, PathStatus::Degraded);
    observed.nodes[0].capabilities.hop_profiles[0]
        .accepts
        .as_mut()
        .unwrap()
        .container = Some(vec![Container::Rtp]);
    assert_eq!(
        format(&run(stream, &observed)).reason,
        StreamConditionReason::FormatMismatch
    );
}

#[test]
fn strict_selects_eligible_profiles_and_relays_before_reserving_ports() {
    let mut observed = observed_nodes();
    for node in &mut observed.nodes {
        let mut unknown = node.capabilities.hop_profiles[0].clone();
        unknown.id = "a-unknown".into();
        unknown.media_behavior = MediaBehavior::Unknown;
        node.capabilities.hop_profiles.push(unknown);
    }
    let compatible = run(declared("feed"), &observed);
    assert_eq!(format(&compatible).status, StreamConditionStatus::True);
    assert!(
        compatible.hops_by_stream["feed"]
            .iter()
            .all(|hop| hop.profile_id == "srt-forward")
    );
    let mut nat = observed_state(&BTreeMap::from([
        (
            "strom-node-1".into(),
            nat_registration("strom-node-1", "192.168.1.10"),
        ),
        (
            "strom-node-2".into(),
            nat_registration("strom-node-2", "192.168.2.10"),
        ),
        (
            "relay-a".into(),
            node_registration("relay-a", "198.51.100.1"),
        ),
        (
            "relay-b".into(),
            node_registration("relay-b", "198.51.100.2"),
        ),
    ]));
    nat.nodes
        .iter_mut()
        .find(|node| node.id == "relay-a")
        .unwrap()
        .capabilities
        .hop_profiles[0]
        .media_behavior = MediaBehavior::Unknown;
    let compatible = run(declared("feed"), &nat);
    assert_eq!(format(&compatible).status, StreamConditionStatus::True);
    assert!(compatible.streams[0].nodes.contains(&"relay-b".into()));
    assert!(!compatible.streams[0].nodes.contains(&"relay-a".into()));
    let mut constrained = observed_nodes();
    for node in &mut constrained.nodes {
        node.topology.attachments[0]
            .listeners
            .srt
            .as_mut()
            .unwrap()
            .port_range = PortRange {
            start: 20000,
            end: 20001,
        };
    }
    let mut blocked = declared("a-blocked");
    destination(&mut blocked).accepts = None;
    let alone = run(declared("z-eligible"), &constrained);
    let together = reconcile(
        vec![blocked, declared("z-eligible")],
        &constrained,
        &LinkKeys::for_tests(),
    );
    assert_eq!(
        alone.hops_by_stream["z-eligible"],
        together.hops_by_stream["z-eligible"]
    );
}

#[test]
fn aggregate_mismatch_wins_and_blocks_every_destination() {
    let observed = observed_nodes();
    let mut stream = declared("feed");
    let mut mismatch = stream.destinations[0].clone();
    mismatch.id = "z-mismatch".into();
    let StreamTransport::Srt(endpoint) = &mut mismatch.endpoint else {
        unreachable!()
    };
    endpoint.accepts.as_mut().unwrap().container = Some(vec![Container::Rtp]);
    destination(&mut stream).accepts = None;
    stream.destinations.push(mismatch);
    let blocked = run(stream.clone(), &observed);
    assert_eq!(
        format(&blocked).reason,
        StreamConditionReason::FormatMismatch
    );
    assert!(format(&blocked).detail.contains("z-mismatch"));
    assert_eq!(
        condition(
            &blocked.streams[0].destinations[0].conditions,
            StreamConditionType::FormatCompatible
        )
        .reason,
        StreamConditionReason::DestinationAcceptsUnknown
    );
    assert!(blocked.hops_by_stream.is_empty());
    stream.destinations.reverse();
    assert_eq!(format(&blocked), format(&run(stream, &observed)));
}

#[test]
fn topology_failures_and_disabled_streams_are_not_format_rejections() {
    let mut observed = observed_nodes();
    observed.nodes.clear();
    let mut stream = declared("feed");
    source(&mut stream).format = None;
    let missing = run(stream.clone(), &observed);
    assert_eq!(
        condition(
            &missing.streams[0].conditions,
            StreamConditionType::PlacementReady
        )
        .reason,
        StreamConditionReason::PlacementFailed
    );
    stream.enabled = false;
    let disabled = run(stream, &observed);
    assert_eq!(disabled.streams[0].status, PathStatus::Idle);
    assert!(disabled.hops_by_stream.is_empty());
}

#[tokio::test]
async fn preview_apply_capability_changes_and_policy_removal_agree() {
    let (state, mem) = mem_state();
    let app = open_router(state.clone());
    for node in [
        node_registration("strom-node-1", "172.26.0.10"),
        node_registration("strom-node-2", "172.27.0.10"),
    ] {
        assert_eq!(
            send(
                &app,
                "POST",
                "/nodes/register",
                Some(serde_json::to_value(node).unwrap())
            )
            .await
            .0,
            StatusCode::ACCEPTED
        );
    }
    let mut stream = declared("feed");
    let (status, _) = send(
        &app,
        "POST",
        "/streams",
        Some(serde_json::to_value(&stream).unwrap()),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    reconcile_tick(&state).await;
    assert_eq!(
        send(&app, "GET", "/streams/feed/endpoints", None).await.0,
        StatusCode::OK
    );
    state
        .nodes
        .write()
        .await
        .get_mut("strom-node-2")
        .unwrap()
        .node
        .capabilities
        .hop_profiles[0]
        .media_behavior = MediaBehavior::Unknown;
    let (_, body) = send(
        &app,
        "POST",
        "/stream-plans",
        Some(serde_json::to_value(&stream).unwrap()),
    )
    .await;
    let plan: StreamPlan = serde_json::from_value(body).unwrap();
    assert_eq!(plan.status, PlanStatus::Unplaced);
    assert!(plan.hops.is_empty());
    assert!(plan.endpoints.is_none());
    reconcile_tick(&state).await;
    let current = state.view.read().await.streams[0].clone();
    assert_eq!(
        condition(&plan.conditions, StreamConditionType::FormatCompatible).reason,
        condition(&current.conditions, StreamConditionType::FormatCompatible).reason
    );
    assert!(
        desired_hops(&app, "strom-node-1")
            .await
            .1
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_ne!(
        send(&app, "GET", "/streams/feed/endpoints", None).await.0,
        StatusCode::OK
    );
    state
        .nodes
        .write()
        .await
        .get_mut("strom-node-2")
        .unwrap()
        .node
        .capabilities
        .hop_profiles[0]
        .media_behavior = MediaBehavior::Preserve;
    reconcile_tick(&state).await;
    assert_eq!(
        send(&app, "GET", "/streams/feed/endpoints", None).await.0,
        StatusCode::OK
    );
    stream.format_policy = FormatPolicy::Report;
    destination(&mut stream).accepts.as_mut().unwrap().container = Some(vec![Container::Rtp]);
    for policy in [FormatPolicy::Report, FormatPolicy::Strict] {
        stream.format_policy = policy;
        let (_, headers, _) = send_with_headers(&app, "GET", "/streams/feed", None, &[]).await;
        let (status, _, _) = send_with_headers(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(&stream).unwrap()),
            &[("if-match", &response_etag(&headers))],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        reconcile_tick(&state).await;
        let endpoint_status = send(&app, "GET", "/streams/feed/endpoints", None).await.0;
        assert_eq!(
            endpoint_status == StatusCode::OK,
            policy == FormatPolicy::Report
        );
    }
    assert_eq!(
        mem.load_streams().await.unwrap()[0].spec.format_policy,
        FormatPolicy::Strict
    );
}

#[tokio::test]
async fn omitted_and_explicit_report_are_noops_and_strict_sets_accept_blocked_members() {
    let (state, _) = mem_state();
    let app = open_router(state.clone());
    let mut omitted = serde_json::to_value(stream("feed")).unwrap();
    omitted.as_object_mut().unwrap().remove("format_policy");
    let (_, headers, _) = send_with_headers(
        &app,
        "POST",
        "/streams",
        Some(omitted.clone()),
        &[("if-none-match", "*")],
    )
    .await;
    let etag = response_etag(&headers);
    omitted["format_policy"] = json!("report");
    let (_, headers, body) = send_with_headers(
        &app,
        "POST",
        "/streams",
        Some(omitted),
        &[("if-match", &etag)],
    )
    .await;
    assert_eq!(response_etag(&headers), etag);
    assert_eq!(body["changed"], false);
    assert_eq!(body["generation"], 1);
    let apply = json!({"streams": [declared("set-feed")], "prune": false});
    let (status, _, _) = send_with_headers(
        &app,
        "PUT",
        "/stream-sets/strict",
        Some(apply),
        &[("if-none-match", "*")],
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    reconcile_tick(&state).await;
    assert!(
        state
            .view
            .read()
            .await
            .streams
            .iter()
            .all(|stream| stream.status == PathStatus::Pending)
    );
}

#[test]
fn different_formats_route_concurrently_without_conversion() {
    let mut streams = vec![declared("h264"), declared("h265")];
    for (stream, codec) in streams.iter_mut().zip(["h264", "h265"]) {
        source(stream).format = Some(serde_json::from_value(json!({
            "container": "mpeg_ts",
            "video": {"codec": codec, "width": 1920, "height": 1080, "framerate": {"numerator": 25, "denominator": 1}, "chroma_subsampling": "yuv420"}
        })).unwrap());
        destination(stream).accepts =
            Some(serde_json::from_value(json!({"video": {"codec": [codec]}})).unwrap());
    }
    let outcome = reconcile(streams, &observed_nodes(), &LinkKeys::for_tests());
    assert_eq!(outcome.endpoints.len(), 2);
    assert!(outcome.streams.iter().all(|stream| {
        condition(&stream.conditions, StreamConditionType::FormatCompatible).status
            == StreamConditionStatus::True
    }));
    assert!(
        outcome
            .hops_by_stream
            .values()
            .flatten()
            .all(|hop| hop.profile_id == "srt-forward")
    );
}

#[tokio::test]
#[ignore = "requires a running Postgres via DATABASE_URL"]
async fn postgres_persists_strict_policy_and_default_noops_across_hydration() {
    let url = store::tests::fresh_database().await;
    let store = Arc::new(store::tests::leading_pg_store(&url).await);
    let spec = declared("strict");
    store.create_stream(&spec).await.unwrap();
    store
        .create_stream_set("owner", &[declared("member")], false)
        .await
        .unwrap();
    let report = stream("report");
    let stored = store.create_stream(&report).await.unwrap();
    let unchanged = store.update_stream(&report, stored.revision).await.unwrap();
    assert_eq!(stored, unchanged);
    let reconnected = Arc::new(store::PgStore::connect(&url).await.unwrap());
    let loaded = reconnected.load_streams().await.unwrap();
    assert_eq!(
        loaded
            .iter()
            .find(|stream| stream.spec.name == "strict")
            .unwrap()
            .spec,
        spec
    );
    assert_eq!(
        reconnected.load_stream_sets().await.unwrap()[0].streams[0]
            .spec
            .format_policy,
        FormatPolicy::Strict
    );
    let state = AppState::hydrate(
        reconnected,
        Duration::from_secs(60),
        Duration::from_secs(3600),
        None,
        LinkKeys::for_tests(),
    )
    .await
    .unwrap();
    assert_eq!(
        state.streams.read().await["strict"].spec.format_policy,
        FormatPolicy::Strict
    );
}
