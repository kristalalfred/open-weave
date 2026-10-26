mod compatibility;
mod desired;
#[cfg(test)]
mod hop_id_tests;
mod keys;
#[cfg(test)]
mod outside_peer_tests;
mod path;
#[cfg(test)]
mod peer_link_tests;
mod peers;
#[cfg(test)]
mod port_hold_tests;
#[cfg(test)]
mod redundant_paths_tests;
#[cfg(test)]
mod relay_choice_tests;
#[cfg(test)]
mod rist_tests;
#[cfg(test)]
mod scale_tests;
#[cfg(test)]
mod second_path_tests;
#[cfg(test)]
mod site_tests;
mod sites;
mod store;
mod timed;
#[cfg(test)]
mod timed_tests;
mod webhook;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::{
    Extension, Json, Router,
    extract::{Path, State, rejection::JsonRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use clap::Parser;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tower::ServiceExt;
use weave_core::auth::{
    self, Guard, NodeCaller, NodeGuard, refuse_other_node, require_bearer, require_node_token,
    unauthorized,
};
use weave_core::logging::ErrorChain;
use weave_core::webhook::{EventType, NodeSummary, StreamSummary, Subject};
use weave_core::{
    AcceptedState, ApiError, ApiErrorCode, DesiredHop, EndpointDescriptor, Health, LocalEnd,
    NodeAccepted, NodeHeartbeat, NodeRegistration, NodeResource, NodeStatus, ObservedState,
    PROTOCOL_VERSION, PathStatus, PlanStatus, ROUTE_ENDPOINTS, ROUTE_NODE_DESIRED,
    ROUTE_NODE_HEARTBEAT, ROUTE_NODE_REGISTER, ROUTE_NODES, ROUTE_STATE, ROUTE_STATUS,
    ROUTE_STREAM, ROUTE_STREAM_ENDPOINTS, ROUTE_STREAM_PLANS, ROUTE_STREAM_SET, ROUTE_STREAM_SETS,
    ROUTE_STREAMS, ReconcileReport, ReconcileStatus, RunningStatus, StartingState, StartingStatus,
    StatusResponse, StreamAccepted, StreamCondition, StreamConditionReason, StreamConditionStatus,
    StreamConditionType, StreamDefinition, StreamDestinationStatus, StreamEndpoints, StreamPlan,
    StreamResource, StreamSetAccepted, StreamSetAction, StreamSetApply, StreamSetMemberResult,
    StreamSetResource, StreamStatus, ValidationIssue, protocol_compatible, resource_id_issue,
    validate_local_io, validate_node, validate_observation, validate_resource_id, validate_stream,
};

use keys::{LinkKeys, SecretSource};
use path::{
    HopIds, HopReports, PlacementError, PortAllocator, SinglePath, derive_stream,
    destination_nodes, destination_path_status, path_reports, path_status, reports_by_hop,
    shared_hop_id, stream_endpoints,
};
use store::{
    LeaseTerm, MemStore, PgStore, StateStore, StoreError, StoredStream, StreamSetMemberAction,
    StreamSetWrite,
};

#[derive(Debug, Parser)]
#[command(name = "weave-controller", version, about = "open-weave reconciler")]
struct Args {
    #[arg(
        long,
        env = "WEAVE_CONTROLLER_ADDR",
        hide_env_values = true,
        default_value = "127.0.0.1:8082"
    )]
    listen: String,
    #[arg(
        long,
        env = "WEAVE_RECONCILE_INTERVAL_SECS",
        hide_env_values = true,
        default_value_t = 5
    )]
    interval_secs: u64,
    #[arg(help = "Seconds without a heartbeat before a node is marked offline")]
    #[arg(
        long,
        env = "WEAVE_NODE_TTL_SECS",
        hide_env_values = true,
        default_value_t = 15
    )]
    node_ttl_secs: u64,
    #[arg(
        help = "Seconds without a heartbeat before an offline node is removed, unless a stream names it"
    )]
    #[arg(
        long,
        env = "WEAVE_NODE_FORGET_SECS",
        hide_env_values = true,
        default_value_t = 300
    )]
    node_forget_secs: u64,
    #[arg(help = "Postgres URL; unset keeps state in memory only")]
    #[arg(long, env = "DATABASE_URL", hide_env_values = true)]
    database_url: Option<String>,
    #[arg(help = "Seconds before the Postgres controller lease expires without renewal")]
    #[arg(
        long,
        env = "WEAVE_LEASE_TTL_SECS",
        hide_env_values = true,
        default_value_t = 10,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    lease_ttl_secs: u64,
    #[arg(help = "Absolute URL that receives node and stream events. Webhooks are off when unset")]
    #[arg(long, env = "WEAVE_WEBHOOK_URL", hide_env_values = true)]
    webhook_url: Option<String>,
    #[arg(help = "Presented to the receiver as `Authorization: Bearer <token>`")]
    #[arg(long, env = "WEAVE_WEBHOOK_TOKEN", hide_env_values = true)]
    webhook_token: Option<String>,
    #[arg(help = "Event types to deliver, comma-separated. Defaults to all of them")]
    #[arg(
        long,
        env = "WEAVE_WEBHOOK_EVENTS",
        hide_env_values = true,
        value_delimiter = ','
    )]
    webhook_events: Vec<String>,
}

#[derive(Default)]
struct ControllerView {
    report: Option<ReconcileReport>,
    streams: Vec<StreamStatus>,
    endpoints: BTreeMap<String, StreamEndpoints>,

    hops: BTreeMap<String, Vec<DesiredHop>>,
}

#[derive(Clone)]
struct AppState {
    store: Arc<dyn StateStore>,
    streams: Arc<RwLock<BTreeMap<String, StoredStream>>>,
    stream_sets: Arc<RwLock<BTreeMap<String, u64>>>,
    nodes: Arc<RwLock<BTreeMap<String, NodeRegistration>>>,

    last_seen: Arc<RwLock<BTreeMap<String, Instant>>>,
    node_ttl: Duration,
    node_forget: Duration,
    desired: Arc<RwLock<BTreeMap<String, desired::DesiredSnapshot>>>,
    view: Arc<RwLock<ControllerView>>,

    webhooks: Option<Arc<webhook::Emitter>>,
    keys: LinkKeys,
    unsaved: Arc<std::sync::Mutex<Unsaved>>,
    peers: Arc<std::sync::Mutex<peers::Peers>>,
}

#[derive(Default)]
struct Unsaved {
    nodes: BTreeSet<String>,
    streams: BTreeSet<String>,
}

impl AppState {
    fn unsaved(&self) -> std::sync::MutexGuard<'_, Unsaved> {
        self.unsaved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn peers(&self) -> std::sync::MutexGuard<'_, peers::Peers> {
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Clone)]
struct SharedReadGuards {
    north: Guard,
    south: NodeGuard,
}

impl AppState {
    async fn hydrate(
        store: Arc<dyn StateStore>,
        node_ttl: Duration,
        node_forget: Duration,
        webhooks: Option<Arc<webhook::Emitter>>,
        keys: LinkKeys,
    ) -> Result<Self> {
        let loaded_streams = store.load_streams().await.context("hydrating streams")?;
        for stream in &loaded_streams {
            if let Some(issue) = validate_stream(&stream.spec).into_iter().next() {
                anyhow::bail!(
                    "stored stream {:?} is invalid at {}: {}",
                    stream.spec.name,
                    issue.field,
                    issue.message
                );
            }
            if let Some(owner) = stream.owner.as_deref()
                && let Err(error) = validate_resource_id(owner)
            {
                anyhow::bail!(
                    "stored stream {:?} has invalid owner: {error}",
                    stream.spec.name
                );
            }
        }
        let streams: BTreeMap<String, StoredStream> = loaded_streams
            .into_iter()
            .map(|stream| (stream.spec.name.clone(), stream))
            .collect();
        let stream_sets = store
            .load_stream_sets()
            .await
            .context("hydrating stream sets")?
            .into_iter()
            .map(|stream_set| {
                validate_resource_id(&stream_set.owner).map_err(|error| {
                    anyhow::anyhow!(
                        "stored stream set {:?} has an invalid owner: {error}",
                        stream_set.owner
                    )
                })?;
                Ok((stream_set.owner, stream_set.revision))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let nodes = store
            .load_nodes()
            .await
            .context("hydrating nodes")?
            .into_iter()
            .filter(|registration| {
                let mut issues = validate_node(&registration.node);
                issues.extend(
                    registration
                        .observation
                        .iter()
                        .flat_map(validate_observation),
                );
                if !issues.is_empty() {
                    tracing::warn!(
                        node_id = %registration.node.id,
                        ?issues,
                        "dropping a stored registration with an invalid node descriptor; the node must re-register"
                    );
                    false
                } else {
                    true
                }
            })
            .map(|registration| (registration.node.id.clone(), registration))
            .collect::<BTreeMap<String, NodeRegistration>>();
        let statuses = store
            .load_stream_statuses()
            .await
            .context("hydrating stream statuses")?
            .into_iter()
            .filter(|status| streams.contains_key(&status.name))
            .collect();
        let boot = Instant::now();
        let last_seen = nodes.keys().map(|id| (id.clone(), boot)).collect();
        Ok(Self {
            store,
            streams: Arc::new(RwLock::new(streams)),
            stream_sets: Arc::new(RwLock::new(stream_sets)),
            nodes: Arc::new(RwLock::new(nodes)),
            last_seen: Arc::new(RwLock::new(last_seen)),
            node_ttl,
            node_forget,
            desired: Arc::new(RwLock::new(BTreeMap::new())),
            view: Arc::new(RwLock::new(ControllerView {
                streams: statuses,
                ..ControllerView::default()
            })),
            webhooks,
            keys,
            unsaved: Arc::default(),
            peers: Arc::default(),
        })
    }

    fn emit(&self, event_type: EventType, subject: impl Into<Subject>) {
        if let Some(emitter) = &self.webhooks {
            emitter.emit(event_type, subject);
        }
    }
}

#[derive(Debug, Serialize)]
struct SystemView {
    #[serde(skip_serializing_if = "Option::is_none")]
    report: Option<ReconcileReport>,
    nodes: Vec<NodeView>,
    streams: Vec<StreamView>,
}

#[derive(Debug, Serialize)]
struct NodeView {
    id: String,
    status: NodeStatus,
    endpoint: String,
    topology: weave_core::NodeTopology,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_seen_secs: Option<u64>,
}

#[derive(Debug, Serialize)]
struct StreamView {
    format_policy: weave_core::FormatPolicy,
    conditions: Vec<StreamCondition>,
    destinations: Vec<StreamDestinationStatus>,
    name: String,
    status: PathStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    nodes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoints: Option<StreamEndpoints>,
    hops: Vec<HopView>,
}

#[derive(Debug, Serialize)]
struct HopView {
    id: String,
    node: String,
    profile_id: String,
    role: weave_core::HopRole,
    ingress: IngressView,
    #[serde(skip_serializing_if = "Option::is_none")]
    merge_ingress: Option<IngressView>,
    egresses: Vec<EgressView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<weave_core::HopState>,
}

#[derive(Debug, Serialize)]
struct IngressView {
    #[serde(flatten)]
    socket: SocketView,
    #[serde(skip_serializing_if = "Option::is_none")]
    condition: Option<weave_core::LinkCondition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<weave_core::LinkStats>,
}

impl IngressView {
    fn new(socket: &weave_core::SocketSpec, status: Option<&weave_core::SocketStatus>) -> Self {
        Self {
            socket: SocketView::from(socket),
            condition: status.map(|status| status.condition),
            stats: status.and_then(|status| status.stats),
        }
    }
}

#[derive(Debug, Serialize)]
struct EgressView {
    branch_id: String,
    #[serde(flatten)]
    socket: SocketView,
    #[serde(skip_serializing_if = "Option::is_none")]
    condition: Option<weave_core::LinkCondition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resolved: Option<weave_core::ResolvedAddr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<weave_core::LinkStats>,
}

#[derive(Debug, Serialize)]
struct SocketView {
    transport: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
}

impl From<&weave_core::SocketSpec> for SocketView {
    fn from(spec: &weave_core::SocketSpec) -> Self {
        use weave_core::{RistSocket, SocketSpec, SrtSocket, Transport};

        let bare = |transport, mode| Self {
            transport,
            mode,
            id: None,
            host: None,
            port: None,
            url: None,
        };
        match spec {
            SocketSpec::Srt(socket) => Self {
                host: match socket {
                    SrtSocket::Connect { host, .. } => Some(host.clone()),
                    SrtSocket::Listen { .. } => None,
                },
                port: Some(socket.port()),
                ..bare(Transport::Srt.name(), Some(socket.role().name()))
            },
            SocketSpec::Whip(socket) => Self {
                url: Some(socket.url.clone()),
                ..bare(Transport::Whip.name(), Some(socket.role.name()))
            },
            SocketSpec::Whep(socket) => Self {
                url: Some(socket.url.clone()),
                ..bare(Transport::Whep.name(), Some(socket.role.name()))
            },
            SocketSpec::Rist(socket) => Self {
                host: match socket {
                    RistSocket::Connect { host, .. } => Some(host.clone()),
                    RistSocket::Listen { .. } => None,
                },
                port: Some(socket.port()),
                ..bare(Transport::Rist.name(), Some(socket.role().name()))
            },
            SocketSpec::Webrtc(socket) => Self {
                id: Some(socket.attempt.link.to_string()),
                ..bare(weave_core::WEBRTC_TRANSPORT, None)
            },
            SocketSpec::Local(socket) => Self {
                id: Some(socket.id.clone()),
                ..bare(socket.end.name(), None)
            },
        }
    }
}

struct ReconcileOutcome {
    report: ReconcileReport,
    streams: Vec<StreamStatus>,
    endpoints: BTreeMap<String, StreamEndpoints>,
    hops_by_stream: BTreeMap<String, Vec<DesiredHop>>,
    desired_by_node: BTreeMap<String, Vec<DesiredHop>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    weave_core::logging::init();

    let args = Args::parse();

    let north = Guard::from_env(auth::NORTHBOUND_TOKEN_VAR)?;
    let south = NodeGuard::from_env(auth::SOUTHBOUND_KEY_VAR)?;
    if north.is_disabled() {
        tracing::warn!(
            "{}=1: controller serves its API without authentication",
            auth::AUTH_DISABLED_VAR
        );
    }
    let (keys, secret_source) = LinkKeys::from_env()?;
    let keys = keys.with_ice(keys::Ice::from_env()?);
    if secret_source == SecretSource::Generated {
        tracing::warn!(
            "{} unset: generated a random one, so every SRT link key changes when the controller restarts or another controller takes over",
            keys::SECRET_VAR
        );
    }

    let webhooks = webhook::Emitter::new(webhook::Config {
        url: args.webhook_url.clone(),
        token: args.webhook_token.clone(),
        events: webhook::parse_events(&args.webhook_events)?,
        ..webhook::Config::default()
    })
    .map(Arc::new);

    let pg = match &args.database_url {
        Some(url) => {
            tracing::info!("connecting controller store to postgres");
            Some(Arc::new(
                PgStore::connect(url)
                    .await
                    .context("opening postgres store")?,
            ))
        }
        None => {
            tracing::warn!("DATABASE_URL unset; using in-memory store (state is not persisted)");
            None
        }
    };
    let controller = Controller {
        node_ttl: Duration::from_secs(args.node_ttl_secs),
        node_forget: Duration::from_secs(args.node_forget_secs),
        webhooks,
        keys,
        north,
        south,
        interval: Duration::from_secs(args.interval_secs),
    };
    tracing::info!(interval_secs = args.interval_secs, "controller starting");

    match pg {
        Some(pg) => {
            let leadership = Leadership::default();
            let api = spawn_api_server(
                bind(&args.listen).await?,
                leadership_router(leadership.clone()),
            );
            let timing = LeaseTiming::new(Duration::from_secs(args.lease_ttl_secs));
            let result = run_with_lease(pg, &controller, &leadership, timing, shutdown()).await;
            api.abort();
            result
        }
        None => {
            let (state, app) = controller.lead(Arc::new(MemStore::new())).await?;
            let api = spawn_api_server(bind(&args.listen).await?, app);
            let result = tokio::select! {
                result = shutdown() => result,
                never = tick_every(&state, controller.interval) => match never {},
            };
            api.abort();
            result
        }
    }
}

struct Controller {
    node_ttl: Duration,
    node_forget: Duration,
    webhooks: Option<Arc<webhook::Emitter>>,
    keys: LinkKeys,
    north: Guard,
    south: NodeGuard,
    interval: Duration,
}

impl Controller {
    async fn lead(&self, store: Arc<dyn StateStore>) -> Result<(AppState, Router)> {
        let state = AppState::hydrate(
            store,
            self.node_ttl,
            self.node_forget,
            self.webhooks.clone(),
            self.keys.clone(),
        )
        .await?;
        let app = router_after_first_tick(&state, self.north.clone(), self.south.clone()).await;
        Ok((state, app))
    }
}

async fn tick_every(state: &AppState, interval: Duration) -> Infallible {
    loop {
        tokio::time::sleep(interval).await;
        reconcile_tick(state).await;
    }
}

async fn shutdown() -> Result<()> {
    weave_core::shutdown::signal()
        .await
        .context("waiting for shutdown signal")?;
    tracing::info!("controller shutting down");
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct LeaseTiming {
    ttl: Duration,
    renew_every: Duration,
    hold_for: Duration,
    retry_every: Duration,
}

impl LeaseTiming {
    fn new(ttl: Duration) -> Self {
        let third = ttl / 3;
        Self {
            ttl,
            renew_every: third,
            hold_for: ttl - third,
            retry_every: third.min(Duration::from_secs(1)),
        }
    }
}

#[derive(Clone, Default)]
struct Leadership(Arc<std::sync::RwLock<Held>>);

#[derive(Default)]
struct Held {
    role: Role,
    until: Option<Instant>,
}

#[derive(Default)]
enum Role {
    #[default]
    Standby,
    Taking,
    Leading(Router),
}

impl Leadership {
    fn held(&self) -> std::sync::RwLockWriteGuard<'_, Held> {
        self.0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn taking(&self, until: Instant) {
        *self.held() = Held {
            role: Role::Taking,
            until: Some(until),
        };
    }

    fn hold_until(&self, until: Instant) {
        let mut held = self.held();
        if !matches!(held.role, Role::Standby) {
            held.until = Some(until);
        }
    }

    fn lead(&self, app: Router) -> bool {
        let mut held = self.held();
        if !matches!(held.role, Role::Taking) {
            return false;
        }
        held.role = Role::Leading(app);
        true
    }

    fn stand_by(&self) {
        *self.held() = Held::default();
    }

    fn router(&self) -> Option<Router> {
        let held = self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &held.role {
            Role::Leading(app) if held.until.is_some_and(|until| Instant::now() < until) => {
                Some(app.clone())
            }
            Role::Leading(_) | Role::Standby | Role::Taking => None,
        }
    }
}

fn leadership_router(leadership: Leadership) -> Router {
    Router::new()
        .route("/", get(ui))
        .route("/ui", get(ui))
        .route("/health", get(health))
        .fallback(serve_if_leading)
        .with_state(leadership)
}

async fn serve_if_leading(
    State(leadership): State<Leadership>,
    request: axum::extract::Request,
) -> Response {
    match leadership.router() {
        Some(app) => match app.oneshot(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        },
        None => not_leader(),
    }
}

fn not_leader() -> Response {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        ApiErrorCode::NotLeader,
        "this controller does not hold the lease; another controller leads",
    )
}

async fn run_with_lease(
    pg: Arc<PgStore>,
    controller: &Controller,
    leadership: &Leadership,
    timing: LeaseTiming,
    shutdown: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
    tokio::pin!(shutdown);
    let holder = holder_id()?;
    tracing::info!(%holder, "standing by for the controller lease");
    loop {
        let (term, asked) = tokio::select! {
            taken = wait_for_lease(&pg, &holder, timing) => taken,
            result = &mut shutdown => return result,
        };
        tracing::info!(
            epoch = term.epoch,
            "took the controller lease; loading state"
        );
        leadership.taking(asked + timing.hold_for);
        let mut hold = tokio::spawn(hold_lease(pg.clone(), leadership.clone(), asked, timing));
        let led = tokio::select! {
            led = controller.lead(Arc::new(pg.for_term(term.epoch))) => led,
            lost = &mut hold => {
                tracing::warn!(reason = lost.unwrap_or("renewal task failed"), "lost the controller lease while loading state; standing by");
                continue;
            }
            result = &mut shutdown => {
                step_down(&pg, leadership, hold).await;
                return result;
            }
        };
        let state = match led {
            Ok((state, app)) => {
                if !leadership.lead(app) {
                    let lost = hold.await;
                    tracing::warn!(
                        reason = lost.unwrap_or("renewal task failed"),
                        "lost the controller lease while loading state; standing by"
                    );
                    continue;
                }
                state
            }
            Err(err) => {
                step_down(&pg, leadership, hold).await;
                tracing::error!(error = %ErrorChain(err.as_ref()), "loading state after taking the controller lease failed; standing by");
                tokio::select! {
                    () = tokio::time::sleep(timing.retry_every) => continue,
                    result = &mut shutdown => return result,
                }
            }
        };
        tracing::info!(epoch = term.epoch, "leading");
        let lost = tokio::select! {
            lost = &mut hold => lost,
            result = &mut shutdown => {
                step_down(&pg, leadership, hold).await;
                return result;
            }
            never = tick_every(&state, controller.interval) => match never {},
        };
        tracing::warn!(
            epoch = term.epoch,
            reason = lost.unwrap_or("renewal task failed"),
            "lost the controller lease; standing by"
        );
    }
}

async fn step_down(pg: &PgStore, leadership: &Leadership, hold: JoinHandle<&'static str>) {
    hold.abort();
    leadership.stand_by();
    if let Err(err) = pg.release_lease().await {
        tracing::warn!(error = %ErrorChain(&err), "releasing the controller lease failed; it expires on its own");
    }
}

fn holder_id() -> Result<String> {
    let mut id = [0u8; 8];
    getrandom::fill(&mut id)
        .map_err(|err| anyhow::anyhow!("generating a lease holder id: {err}"))?;
    Ok(id.iter().map(|byte| format!("{byte:02x}")).collect())
}

async fn wait_for_lease(pg: &PgStore, holder: &str, timing: LeaseTiming) -> (LeaseTerm, Instant) {
    let mut held_elsewhere = false;
    loop {
        let asked = Instant::now();
        match pg.acquire_lease(holder, timing.ttl).await {
            Ok(Some(term)) => return (term, asked),
            Ok(None) if !held_elsewhere => {
                tracing::info!("another controller holds the lease");
                held_elsewhere = true;
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(error = %ErrorChain(&err), "taking the controller lease failed; retrying")
            }
        }
        tokio::time::sleep(timing.retry_every).await;
    }
}

async fn hold_lease(
    pg: Arc<PgStore>,
    leadership: Leadership,
    asked: Instant,
    timing: LeaseTiming,
) -> &'static str {
    let mut deadline = asked + timing.hold_for;
    let mut wait = timing.renew_every;
    let reason = loop {
        tokio::time::sleep_until((Instant::now() + wait).min(deadline).into()).await;
        let sent = Instant::now();
        if sent >= deadline {
            break "no renewal succeeded in time";
        }
        match tokio::time::timeout_at(deadline.into(), pg.renew_lease(timing.ttl)).await {
            Ok(Ok(true)) => {
                deadline = sent + timing.hold_for;
                leadership.hold_until(deadline);
                wait = timing.renew_every;
            }
            Ok(Ok(false)) => break "the lease expired or another controller took it",
            Ok(Err(err)) => {
                tracing::warn!(error = %ErrorChain(&err), "renewing the controller lease failed; retrying");
                wait = timing.retry_every;
            }
            Err(_) => break "no renewal succeeded in time",
        }
    };
    leadership.stand_by();
    pg.drop_lease();
    reason
}

async fn bind(addr: &str) -> Result<tokio::net::TcpListener> {
    tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding controller listener on {addr}"))
}

async fn router_after_first_tick(state: &AppState, north: Guard, south: NodeGuard) -> Router {
    reconcile_tick(state).await;
    router(state.clone(), north, south)
}

async fn reconcile_tick(state: &AppState) {
    let streams = state.streams.read().await;
    let definitions = streams.values().map(|stream| stream.spec.clone()).collect();
    let (observed, went_offline, forgotten) = {
        let mut nodes = state.nodes.write().await;
        let mut last_seen = state.last_seen.write().await;
        let now = Instant::now();
        let transitioned = mark_offline(&mut nodes, &last_seen, now, state.node_ttl);
        let went_offline: Vec<NodeSummary> = transitioned
            .iter()
            .filter_map(|id| nodes.get(id))
            .map(|registration| NodeSummary::from(&sites::resolve(registration).node))
            .collect();
        let mut to_store = std::mem::take(&mut state.unsaved().nodes);
        to_store.extend(transitioned);
        for registration in to_store.iter().filter_map(|id| nodes.get(id)) {
            if let Err(err) = state.store.upsert_node(registration).await {
                tracing::warn!(error = %ErrorChain(&err), node_id = %registration.node.id, "storing a node failed; retrying next tick");
                state.unsaved().nodes.insert(registration.node.id.clone());
            }
        }
        let named = named_nodes(streams.values().map(|stream| &stream.spec));
        let mut forgotten = Vec::new();
        for id in forgettable(&nodes, &last_seen, &named, now, state.node_forget) {
            if let Err(err) = state.store.delete_node(&id).await {
                tracing::warn!(error = %ErrorChain(&err), node_id = %id, "forgetting an offline node failed; retrying next tick");
                continue;
            }
            last_seen.remove(&id);
            if let Some(registration) = nodes.remove(&id) {
                tracing::info!(node_id = %id, "forgot an offline node that no stream names");
                forgotten.push(NodeSummary::from(&sites::resolve(&registration).node));
            }
        }
        (observed_state(&nodes), went_offline, forgotten)
    };
    for node in went_offline {
        state.emit(EventType::NodeOffline, node);
    }
    for node in forgotten {
        state.emit(EventType::NodeForgotten, node);
    }
    let source_revisions = streams
        .iter()
        .map(|(name, stream)| (name.clone(), stream.source_revision))
        .collect();
    let now = Instant::now();
    let planning = {
        let mut peers = state.peers();
        peers.observe(&observed.hops, &observed.nodes, now);
        peers.planning(now, unix_now())
    };
    let mut outcome = reconcile_with_revisions(
        definitions,
        &observed,
        &state.keys,
        &planning,
        &source_revisions,
    );
    state.peers().serve(
        outcome.desired_by_node.values().flatten(),
        &observed.nodes,
        now,
    );
    for status in &mut outcome.streams {
        let stored = streams
            .get(&status.name)
            .expect("reconcile returns every submitted stream");
        status.generation = stored.generation;
        status.observed_generation = Some(stored.generation);
    }

    for stream in &outcome.streams {
        tracing::info!(stream = %stream.name, status = ?stream.status, "stream status");
    }
    tracing::info!(
        status = ?outcome.report.status,
        summary = %outcome.report.summary,
        "reconcile tick"
    );

    let desired = desired::snapshots(outcome.desired_by_node);
    for (node_id, snapshot) in &desired {
        tracing::debug!(%node_id, revision = %snapshot.revision, hops = snapshot.hops.len(), "desired snapshot");
    }
    *state.desired.write().await = desired;
    let mut view = state.view.write().await;
    stamp_condition_transition_times(&mut outcome.streams, &view.streams, &now_rfc3339());
    let changed = changed_streams(&outcome.streams, &view.streams);
    let mut to_store = std::mem::take(&mut state.unsaved().streams);
    to_store.extend(changed.iter().map(|stream| stream.name.clone()));
    let statuses: Vec<StreamStatus> = outcome
        .streams
        .iter()
        .filter(|stream| to_store.contains(&stream.name))
        .cloned()
        .collect();
    view.report = Some(outcome.report);
    view.streams = outcome.streams;
    view.endpoints = outcome.endpoints;
    view.hops = outcome.hops_by_stream;
    drop(view);
    drop(streams);
    if !statuses.is_empty()
        && let Err(err) = state.store.save_stream_statuses(&statuses).await
    {
        tracing::warn!(error = %ErrorChain(&err), "storing stream statuses failed; retrying next tick");
        state
            .unsaved()
            .streams
            .extend(statuses.into_iter().map(|stream| stream.name));
    }
    for stream in &changed {
        state.emit(EventType::StreamChanged, StreamSummary::from(stream));
    }
}

fn changed_streams(current: &[StreamStatus], previous: &[StreamStatus]) -> Vec<StreamStatus> {
    current
        .iter()
        .filter(|stream| {
            previous
                .iter()
                .find(|candidate| {
                    candidate.name == stream.name && candidate.observed_generation.is_some()
                })
                .is_none_or(|previous| condition_keys(previous) != condition_keys(stream))
        })
        .cloned()
        .collect()
}

type ConditionKey<'a> = (
    Option<&'a str>,
    StreamConditionType,
    StreamConditionStatus,
    StreamConditionReason,
);

fn condition_keys(stream: &StreamStatus) -> Vec<ConditionKey<'_>> {
    std::iter::once((None, &stream.conditions))
        .chain(
            stream
                .destinations
                .iter()
                .map(|destination| (Some(destination.id.as_str()), &destination.conditions)),
        )
        .flat_map(|(destination, conditions)| {
            conditions.iter().map(move |condition| {
                (
                    destination,
                    condition.condition_type,
                    condition.status,
                    condition.reason,
                )
            })
        })
        .collect()
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

fn unix_now() -> u64 {
    u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp()).unwrap_or_default()
}

fn stamp_condition_transition_times(
    current: &mut [StreamStatus],
    previous: &[StreamStatus],
    now: &str,
) {
    for stream in current {
        let previous = previous
            .iter()
            .find(|candidate| candidate.name == stream.name);
        stamp_conditions(
            &mut stream.conditions,
            previous.map(|stream| stream.conditions.as_slice()),
            now,
        );
        for destination in &mut stream.destinations {
            let previous_conditions = previous
                .and_then(|stream| {
                    stream
                        .destinations
                        .iter()
                        .find(|candidate| candidate.id == destination.id)
                })
                .map(|destination| destination.conditions.as_slice());
            stamp_conditions(&mut destination.conditions, previous_conditions, now);
        }
    }
}

fn stamp_conditions(
    current: &mut [StreamCondition],
    previous: Option<&[StreamCondition]>,
    now: &str,
) {
    for condition in current {
        condition.last_transition_time = previous
            .and_then(|conditions| {
                conditions.iter().find(|candidate| {
                    candidate.condition_type == condition.condition_type
                        && candidate.status == condition.status
                })
            })
            .map_or_else(
                || now.to_string(),
                |condition| condition.last_transition_time.clone(),
            );
    }
}

fn unreconciled_stream_status(stored: &StoredStream) -> StreamStatus {
    let now = now_rfc3339();
    let conditions: Vec<StreamCondition> = [
        StreamConditionType::PlacementReady,
        StreamConditionType::NodesAvailable,
        StreamConditionType::HopsReady,
        StreamConditionType::FormatCompatible,
        StreamConditionType::MediaFlowing,
    ]
    .into_iter()
    .map(|condition_type| StreamCondition {
        condition_type,
        status: StreamConditionStatus::Unknown,
        reason: StreamConditionReason::NotReady,
        detail: "the controller has not reconciled this generation".to_string(),
        last_transition_time: now.clone(),
    })
    .collect();
    let mut status = StreamStatus {
        format_policy: stored.spec.format_policy,
        name: stored.spec.name.clone(),
        generation: stored.generation,
        observed_generation: None,
        status: PathStatus::Pending,
        nodes: Vec::new(),
        destinations: stored
            .spec
            .destinations
            .iter()
            .map(|destination| StreamDestinationStatus {
                delivery: None,
                id: destination.id.clone(),
                status: PathStatus::Pending,
                nodes: Vec::new(),
                conditions: conditions.clone(),
                endpoint: None,
            })
            .collect(),
        conditions,
        ingress: None,
    };
    status
        .destinations
        .sort_by(|left, right| left.id.cmp(&right.id));
    status
}

fn update_view_generation(view: &mut ControllerView, stored: &StoredStream) {
    if let Some(status) = view
        .streams
        .iter_mut()
        .find(|status| status.name == stored.spec.name)
    {
        status.generation = stored.generation;
    } else {
        view.streams.push(unreconciled_stream_status(stored));
    }
}

async fn update_pending_generation(state: &AppState, stored: &StoredStream) {
    let mut view = state.view.write().await;
    update_view_generation(&mut view, stored);
    view.streams
        .sort_by(|left, right| left.name.cmp(&right.name));
}

async fn update_stream_set_view(state: &AppState, write: &StreamSetWrite) {
    let mut view = state.view.write().await;
    for name in &write.deleted {
        view.streams.retain(|status| status.name != *name);
        view.endpoints.remove(name);
        view.hops.remove(name);
    }
    for stream in &write.stream_set.streams {
        update_view_generation(&mut view, stream);
    }
    view.streams
        .sort_by(|left, right| left.name.cmp(&right.name));
}

async fn remove_stream_view(state: &AppState, name: &str) {
    let mut view = state.view.write().await;
    view.streams.retain(|status| status.name != name);
    view.endpoints.remove(name);
    view.hops.remove(name);
}

fn mark_offline(
    nodes: &mut BTreeMap<String, NodeRegistration>,
    last_seen: &BTreeMap<String, Instant>,
    now: Instant,
    ttl: Duration,
) -> Vec<String> {
    let mut transitioned = Vec::new();
    for (id, registration) in nodes.iter_mut() {
        if registration.node.status != NodeStatus::Offline
            && let Some(seen) = last_seen.get(id)
            && now.saturating_duration_since(*seen) > ttl
        {
            registration.node.status = NodeStatus::Offline;
            transitioned.push(id.clone());
        }
    }
    transitioned
}

fn forgettable(
    nodes: &BTreeMap<String, NodeRegistration>,
    last_seen: &BTreeMap<String, Instant>,
    named: &BTreeSet<&str>,
    now: Instant,
    after: Duration,
) -> Vec<String> {
    nodes
        .iter()
        .filter(|(id, registration)| {
            registration.node.status == NodeStatus::Offline
                && !named.contains(id.as_str())
                && last_seen
                    .get(*id)
                    .is_some_and(|seen| now.saturating_duration_since(*seen) > after)
        })
        .map(|(id, _)| id.clone())
        .collect()
}

fn named_nodes<'a>(streams: impl IntoIterator<Item = &'a StreamDefinition>) -> BTreeSet<&'a str> {
    let mut named = BTreeSet::new();
    for stream in streams {
        let endpoints = std::iter::once(&stream.source).chain(
            stream
                .destinations
                .iter()
                .map(|destination| &destination.endpoint),
        );
        for endpoint in endpoints {
            named.extend(endpoint.node());
            if let weave_core::StreamTransport::Srt(srt) = endpoint {
                named.extend(srt.via.iter().map(String::as_str));
            }
        }
    }
    named
}

fn observed_state(nodes: &BTreeMap<String, NodeRegistration>) -> ObservedState {
    ObservedState {
        nodes: nodes.values().map(|r| sites::resolve(r).node).collect(),
        endpoints: nodes.values().flat_map(|r| r.endpoints.clone()).collect(),
        hops: nodes.values().flat_map(|r| r.hop_status.clone()).collect(),
    }
}

fn router(state: AppState, north: Guard, south: NodeGuard) -> Router {
    let shared_reads = Router::new().route(ROUTE_NODES, get(list_nodes)).layer(
        axum::middleware::from_fn_with_state(
            SharedReadGuards {
                north: north.clone(),
                south: south.clone(),
            },
            require_shared_read_bearer,
        ),
    );

    let streams = Router::new()
        .route(ROUTE_STREAMS, get(list_streams).post(submit_stream))
        .route(ROUTE_STREAM, get(get_stream).delete(delete_stream))
        .route(ROUTE_STREAM_ENDPOINTS, get(get_endpoints))
        .route(ROUTE_STREAM_PLANS, post(plan_stream))
        .route(ROUTE_STREAM_SETS, get(list_stream_sets))
        .route(ROUTE_STREAM_SET, get(get_stream_set).put(put_stream_set))
        .layer(axum::middleware::from_fn_with_state(north, require_bearer));

    let nodes = Router::new()
        .route(ROUTE_NODE_REGISTER, post(register_node))
        .route(ROUTE_NODE_HEARTBEAT, post(node_heartbeat))
        .route(ROUTE_NODE_DESIRED, get(get_desired))
        .route(
            weave_core::ROUTE_NODE_SIGNALLING,
            post(exchange_descriptions),
        )
        .route(ROUTE_ENDPOINTS, get(list_endpoints))
        .route(ROUTE_STATE, get(get_state))
        .layer(axum::middleware::from_fn_with_state(
            south,
            require_node_token,
        ));

    let api = Router::new()
        .route(ROUTE_STATUS, get(get_status))
        .merge(shared_reads)
        .merge(streams)
        .merge(nodes)
        .fallback(api_route_not_found)
        .method_not_allowed_fallback(api_method_not_allowed);

    Router::new()
        .route("/", get(ui))
        .route("/ui", get(ui))
        .route("/health", get(health))
        .route("/view", get(get_view))
        .merge(api)
        .with_state(state)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SharedReader {
    Operator,
    Node,
}

async fn require_shared_read_bearer(
    State(guards): State<SharedReadGuards>,
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let authorization = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    let reader = if guards.north.token().is_none_or(|token| {
        authorization.is_some_and(|authorization| token.matches_header(authorization))
    }) {
        SharedReader::Operator
    } else if guards.south.caller(authorization).is_some() {
        SharedReader::Node
    } else {
        return unauthorized();
    };
    request.extensions_mut().insert(reader);
    next.run(request).await
}

fn spawn_api_server(listener: tokio::net::TcpListener, app: Router) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        if let Ok(addr) = listener.local_addr() {
            tracing::info!(%addr, "controller API listening");
        }
        axum::serve(listener, app)
            .await
            .context("controller server error")?;
        Ok(())
    })
}

async fn health() -> Json<Health> {
    Json(Health::OK)
}

async fn api_route_not_found() -> Response {
    error(
        StatusCode::NOT_FOUND,
        ApiErrorCode::RouteNotFound,
        "API route not found",
    )
}

async fn api_method_not_allowed() -> Response {
    error(
        StatusCode::METHOD_NOT_ALLOWED,
        ApiErrorCode::MethodNotAllowed,
        "method not allowed",
    )
}

async fn get_status(State(state): State<AppState>) -> Json<StatusResponse> {
    let view = state.view.read().await;
    Json(match &view.report {
        Some(report) => StatusResponse::Running(RunningStatus {
            status: report.status,
            summary: report.summary.clone(),
            streams: view.streams.clone(),
        }),
        None => StatusResponse::Starting(StartingStatus {
            status: StartingState::Starting,
        }),
    })
}

async fn get_view(State(state): State<AppState>) -> Json<SystemView> {
    let nodes = state.nodes.read().await;
    let last_seen = state.last_seen.read().await;
    let view = state.view.read().await;
    let now = Instant::now();

    let observed = reports_by_hop(nodes.values().flat_map(|r| &r.hop_status));

    let node_views = nodes
        .values()
        .map(|r| NodeView {
            id: r.node.id.clone(),
            status: r.node.status,
            endpoint: r.node.endpoint.clone(),
            topology: sites::resolve(r).node.topology,
            last_seen_secs: last_seen
                .get(&r.node.id)
                .map(|seen| now.saturating_duration_since(*seen).as_secs()),
        })
        .collect();

    let streams = view
        .streams
        .iter()
        .map(|stream| StreamView {
            format_policy: stream.format_policy,
            conditions: stream.conditions.clone(),
            destinations: stream.destinations.clone(),
            name: stream.name.clone(),
            status: stream.status,
            reason: stream
                .conditions
                .iter()
                .find(|condition| condition.status == StreamConditionStatus::False)
                .map(|condition| condition.detail.clone()),
            nodes: stream.nodes.clone(),
            endpoints: view.endpoints.get(&stream.name).cloned(),
            hops: view
                .hops
                .get(&stream.name)
                .into_iter()
                .flatten()
                .map(|hop| {
                    let status = observed
                        .get(&(hop.id.as_str(), hop.node_id.as_str()))
                        .copied()
                        .filter(|status| status.revision == hop.revision());
                    let mut reported_egresses = HashMap::new();
                    for reported in status.into_iter().flat_map(|status| &status.egresses) {
                        reported_egresses
                            .entry(reported.branch_id.as_str())
                            .or_insert(&reported.status);
                    }
                    HopView {
                        id: hop.id.clone(),
                        node: hop.node_id.clone(),
                        profile_id: hop.profile_id.clone(),
                        role: hop.role,
                        ingress: IngressView::new(&hop.ingress, status.map(|s| &s.ingress)),
                        merge_ingress: hop.merge_ingress.as_ref().map(|socket| {
                            IngressView::new(socket, status.and_then(|s| s.merge_ingress.as_ref()))
                        }),
                        egresses: hop
                            .egresses
                            .iter()
                            .map(|egress| {
                                let observed =
                                    reported_egresses.get(egress.branch_id.as_str()).copied();
                                EgressView {
                                    branch_id: egress.branch_id.clone(),
                                    socket: SocketView::from(&egress.socket),
                                    condition: observed.map(|reported| reported.condition),
                                    resolved: observed
                                        .and_then(|reported| reported.resolved.clone()),
                                    stats: observed.and_then(|reported| reported.stats),
                                }
                            })
                            .collect(),
                        state: status.map(|s| s.state),
                    }
                })
                .collect(),
        })
        .collect();

    Json(SystemView {
        report: view.report.clone(),
        nodes: node_views,
        streams,
    })
}

async fn ui() -> axum::response::Html<&'static str> {
    axum::response::Html(include_str!("ui.html"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamWritePrecondition {
    Absent,
    Revision(u64),
}

struct RequestError {
    status: StatusCode,
    code: ApiErrorCode,
    message: &'static str,
}

impl RequestError {
    fn response(self) -> Response {
        error(self.status, self.code, self.message)
    }
}

fn stream_resource(stream: &StoredStream) -> StreamResource {
    StreamResource {
        generation: stream.generation,
        owner: stream.owner.clone(),
        spec: stream.spec.clone(),
    }
}

fn revision_etag(revision: u64) -> HeaderValue {
    HeaderValue::from_str(&format!("\"revision-{revision}\""))
        .expect("numeric revision always forms a valid ETag")
}

fn stream_set_etag(revision: u64) -> HeaderValue {
    HeaderValue::from_str(&format!("\"set-revision-{revision}\""))
        .expect("numeric revision always forms a valid ETag")
}

fn with_etag(mut response: Response, revision: u64) -> Response {
    response
        .headers_mut()
        .insert(header::ETAG, revision_etag(revision));
    response
}

fn with_stream_set_etag(mut response: Response, revision: u64) -> Response {
    response
        .headers_mut()
        .insert(header::ETAG, stream_set_etag(revision));
    response
}

fn parse_revision(value: &HeaderValue) -> Option<u64> {
    value
        .to_str()
        .ok()?
        .strip_prefix("\"revision-")?
        .strip_suffix('"')?
        .parse()
        .ok()
}

fn parse_stream_set_revision(value: &HeaderValue) -> Option<u64> {
    value
        .to_str()
        .ok()?
        .strip_prefix("\"set-revision-")?
        .strip_suffix('"')?
        .parse()
        .ok()
}

fn required_revision(headers: &HeaderMap) -> Result<u64, RequestError> {
    let Some(value) = headers.get(header::IF_MATCH) else {
        return Err(RequestError {
            status: StatusCode::PRECONDITION_REQUIRED,
            code: ApiErrorCode::PreconditionRequired,
            message: "If-Match is required",
        });
    };
    parse_revision(value).ok_or(RequestError {
        status: StatusCode::BAD_REQUEST,
        code: ApiErrorCode::InvalidRequest,
        message: "If-Match must contain one current stream ETag",
    })
}

fn stream_write_precondition(headers: &HeaderMap) -> Result<StreamWritePrecondition, RequestError> {
    match (
        headers.get(header::IF_MATCH),
        headers.get(header::IF_NONE_MATCH),
    ) {
        (None, None) => Err(RequestError {
            status: StatusCode::PRECONDITION_REQUIRED,
            code: ApiErrorCode::PreconditionRequired,
            message: "If-Match or If-None-Match is required",
        }),
        (Some(_), Some(_)) => Err(RequestError {
            status: StatusCode::BAD_REQUEST,
            code: ApiErrorCode::InvalidRequest,
            message: "send either If-Match or If-None-Match, not both",
        }),
        (Some(value), None) => parse_revision(value)
            .map(StreamWritePrecondition::Revision)
            .ok_or(RequestError {
                status: StatusCode::BAD_REQUEST,
                code: ApiErrorCode::InvalidRequest,
                message: "If-Match must contain one current stream ETag",
            }),
        (None, Some(value)) if value == "*" => Ok(StreamWritePrecondition::Absent),
        (None, Some(_)) => Err(RequestError {
            status: StatusCode::BAD_REQUEST,
            code: ApiErrorCode::InvalidRequest,
            message: "If-None-Match must be * when creating a stream",
        }),
    }
}

fn stream_set_write_precondition(
    headers: &HeaderMap,
) -> Result<StreamWritePrecondition, RequestError> {
    match (
        headers.get(header::IF_MATCH),
        headers.get(header::IF_NONE_MATCH),
    ) {
        (None, None) => Err(RequestError {
            status: StatusCode::PRECONDITION_REQUIRED,
            code: ApiErrorCode::PreconditionRequired,
            message: "If-Match or If-None-Match is required",
        }),
        (Some(_), Some(_)) => Err(RequestError {
            status: StatusCode::BAD_REQUEST,
            code: ApiErrorCode::InvalidRequest,
            message: "send either If-Match or If-None-Match, not both",
        }),
        (Some(value), None) => parse_stream_set_revision(value)
            .map(StreamWritePrecondition::Revision)
            .ok_or(RequestError {
                status: StatusCode::BAD_REQUEST,
                code: ApiErrorCode::InvalidRequest,
                message: "If-Match must contain one current stream-set ETag",
            }),
        (None, Some(value)) if value == "*" => Ok(StreamWritePrecondition::Absent),
        (None, Some(_)) => Err(RequestError {
            status: StatusCode::BAD_REQUEST,
            code: ApiErrorCode::InvalidRequest,
            message: "If-None-Match must be * when creating a stream set",
        }),
    }
}

async fn list_streams(State(state): State<AppState>) -> Json<Vec<StreamResource>> {
    Json(
        state
            .streams
            .read()
            .await
            .values()
            .map(stream_resource)
            .collect(),
    )
}

async fn get_stream(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(reason) = validate_resource_id(&name) {
        return invalid_request(
            "stream name is invalid",
            vec![resource_id_issue("name", "stream name", reason)],
        );
    }
    match state.streams.read().await.get(&name).cloned() {
        Some(stream) => with_etag(
            Json(stream_resource(&stream)).into_response(),
            stream.revision,
        ),
        None => error(
            StatusCode::NOT_FOUND,
            ApiErrorCode::StreamNotFound,
            "stream not found",
        ),
    }
}

fn stream_set_resource(owner: &str, streams: &BTreeMap<String, StoredStream>) -> StreamSetResource {
    StreamSetResource {
        owner: owner.to_string(),
        streams: streams
            .values()
            .filter(|stream| stream.owner.as_deref() == Some(owner))
            .map(stream_resource)
            .collect(),
    }
}

async fn list_stream_sets(State(state): State<AppState>) -> Json<Vec<StreamSetResource>> {
    let streams = state.streams.read().await;
    let stream_sets = state.stream_sets.read().await;
    Json(
        stream_sets
            .keys()
            .map(|owner| stream_set_resource(owner, &streams))
            .collect(),
    )
}

async fn get_stream_set(State(state): State<AppState>, Path(owner): Path<String>) -> Response {
    if let Err(reason) = validate_resource_id(&owner) {
        return invalid_request(
            "stream-set owner is invalid",
            vec![resource_id_issue("owner", "stream-set owner", reason)],
        );
    }
    let streams = state.streams.read().await;
    let stream_sets = state.stream_sets.read().await;
    match stream_sets.get(&owner).copied() {
        Some(revision) => with_stream_set_etag(
            Json(stream_set_resource(&owner, &streams)).into_response(),
            revision,
        ),
        None => error(
            StatusCode::NOT_FOUND,
            ApiErrorCode::StreamSetNotFound,
            "stream set not found",
        ),
    }
}

fn validate_stream_set(owner: &str, apply: &StreamSetApply) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    if let Err(reason) = validate_resource_id(owner) {
        issues.push(resource_id_issue("owner", "stream-set owner", reason));
    }
    if apply.streams.is_empty() && !apply.prune {
        issues.push(ValidationIssue::new(
            "streams",
            "required",
            "streams must not be empty unless prune is true",
        ));
    }
    let mut names = BTreeSet::new();
    for (index, stream) in apply.streams.iter().enumerate() {
        for mut issue in validate_stream(stream) {
            issue.field = format!("streams[{index}].{}", issue.field);
            issues.push(issue);
        }
        if !names.insert(stream.name.as_str()) {
            issues.push(ValidationIssue::new(
                format!("streams[{index}].name"),
                "duplicate",
                format!("stream {:?} appears more than once", stream.name),
            ));
        }
    }
    issues
}

fn stream_set_action(action: StreamSetMemberAction) -> StreamSetAction {
    match action {
        StreamSetMemberAction::Created => StreamSetAction::Created,
        StreamSetMemberAction::Updated => StreamSetAction::Updated,
        StreamSetMemberAction::Unchanged => StreamSetAction::Unchanged,
    }
}

async fn put_stream_set(
    State(state): State<AppState>,
    Path(owner): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<StreamSetApply>, JsonRejection>,
) -> Response {
    let Json(apply) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return invalid_json(rejection.body_text()),
    };
    let issues = validate_stream_set(&owner, &apply);
    if !issues.is_empty() {
        return invalid_request("stream-set validation failed", issues);
    }
    let precondition = match stream_set_write_precondition(&headers) {
        Ok(precondition) => precondition,
        Err(error) => return error.response(),
    };

    let write = {
        let mut streams = state.streams.write().await;
        if let Some(conflict) = stream_set_hop_id_conflict(&owner, &apply, &streams) {
            return conflict;
        }
        let mut stream_sets = state.stream_sets.write().await;
        let result = match precondition {
            StreamWritePrecondition::Absent => {
                state
                    .store
                    .create_stream_set(&owner, &apply.streams, apply.prune)
                    .await
            }
            StreamWritePrecondition::Revision(revision) => {
                state
                    .store
                    .update_stream_set(&owner, &apply.streams, apply.prune, revision)
                    .await
            }
        };
        let write = match result {
            Ok(write) => write,
            Err(StoreError::PreconditionFailed) => {
                return error(
                    StatusCode::PRECONDITION_FAILED,
                    ApiErrorCode::PreconditionFailed,
                    "stream set changed or the requested owner already exists",
                );
            }
            Err(StoreError::OwnershipConflict { .. }) => {
                return error(
                    StatusCode::CONFLICT,
                    ApiErrorCode::OwnershipConflict,
                    "a stream name belongs to another workflow",
                );
            }
            Err(StoreError::DuplicateStreamName { .. }) => {
                return invalid_request("stream-set validation failed", Vec::new());
            }
            Err(StoreError::NotLeader) => return not_leader(),
            Err(err) => {
                tracing::error!(error = %ErrorChain(&err), %owner, "persisting stream set failed");
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ApiErrorCode::PersistenceFailed,
                    "failed to persist stream set",
                );
            }
        };
        streams.retain(|_, stream| stream.owner.as_deref() != Some(owner.as_str()));
        for stream in &write.stream_set.streams {
            streams.insert(stream.spec.name.clone(), stream.clone());
        }
        stream_sets.insert(owner.clone(), write.stream_set.revision);
        update_stream_set_view(&state, &write).await;
        write
    };

    let response = StreamSetAccepted {
        status: AcceptedState::Accepted,
        owner: owner.clone(),
        changed: write.changed,
        streams: write
            .stream_set
            .streams
            .iter()
            .map(|stream| StreamSetMemberResult {
                name: stream.spec.name.clone(),
                generation: stream.generation,
                action: stream_set_action(write.actions[&stream.spec.name]),
            })
            .collect(),
        pruned: write.deleted.clone(),
    };
    tracing::info!(%owner, changed = write.changed, "stream set accepted");
    with_stream_set_etag(
        (StatusCode::ACCEPTED, Json(response)).into_response(),
        write.stream_set.revision,
    )
}

fn first_shared_hop_id<'a>(
    stream: &StreamDefinition,
    others: impl IntoIterator<Item = &'a StreamDefinition>,
) -> Option<(&'a str, String)> {
    others
        .into_iter()
        .find_map(|other| shared_hop_id(stream, other).map(|hop| (other.name.as_str(), hop)))
}

fn stream_set_hop_id_conflict(
    owner: &str,
    apply: &StreamSetApply,
    stored: &BTreeMap<String, StoredStream>,
) -> Option<Response> {
    let written: BTreeSet<&str> = apply
        .streams
        .iter()
        .map(|stream| stream.name.as_str())
        .collect();
    let kept: Vec<&StreamDefinition> = stored
        .values()
        .filter(|stream| !written.contains(stream.spec.name.as_str()))
        .filter(|stream| !(apply.prune && stream.owner.as_deref() == Some(owner)))
        .map(|stream| &stream.spec)
        .collect();
    apply
        .streams
        .iter()
        .enumerate()
        .filter(|(_, stream)| {
            stored
                .get(&stream.name)
                .is_none_or(|current| current.spec != **stream)
        })
        .find_map(|(index, stream)| {
            let others = kept.iter().copied().chain(
                apply
                    .streams
                    .iter()
                    .filter(|other| other.name != stream.name),
            );
            first_shared_hop_id(stream, others).map(|(other, hop)| {
                hop_id_conflict(&format!("streams[{index}].name"), &stream.name, other, &hop)
            })
        })
}

fn hop_id_conflict(field: &str, stream: &str, other: &str, hop: &str) -> Response {
    let message =
        format!("stream {stream} can plan hop id {hop}, which stream {other} can also plan");
    ApiError::with_details(
        ApiErrorCode::HopIdConflict,
        format!("stream {stream} and stream {other} can plan the same hop id"),
        vec![ValidationIssue::new(field, "hop_id_conflict", message)],
    )
    .response(StatusCode::CONFLICT)
}

async fn submit_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: Result<Json<StreamDefinition>, JsonRejection>,
) -> Response {
    let Json(stream) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return invalid_json(rejection.body_text()),
    };
    let issues = validate_stream(&stream);
    if !issues.is_empty() {
        return invalid_request("stream validation failed", issues);
    }
    let precondition = match stream_write_precondition(&headers) {
        Ok(precondition) => precondition,
        Err(error) => return error.response(),
    };
    let name = stream.name.clone();
    let (stored, changed) = {
        let mut streams = state.streams.write().await;
        let current = streams.get(&name);
        let changed = current.is_none_or(|current| current.spec != stream);
        let owned = current.is_some_and(|current| current.owner.is_some());
        if changed
            && !owned
            && let Some((other, hop)) = first_shared_hop_id(
                &stream,
                streams
                    .values()
                    .map(|stored| &stored.spec)
                    .filter(|stored| stored.name != name),
            )
        {
            return hop_id_conflict("name", &name, other, &hop);
        }
        let result = match precondition {
            StreamWritePrecondition::Absent => state.store.create_stream(&stream).await,
            StreamWritePrecondition::Revision(revision) => {
                state.store.update_stream(&stream, revision).await
            }
        };
        let stored = match result {
            Ok(stored) => stored,
            Err(StoreError::PreconditionFailed) => {
                return error(
                    StatusCode::PRECONDITION_FAILED,
                    ApiErrorCode::PreconditionFailed,
                    "stream changed or the requested create name already exists",
                );
            }
            Err(StoreError::OwnershipConflict { .. }) => {
                return error(
                    StatusCode::CONFLICT,
                    ApiErrorCode::StreamOwned,
                    "stream belongs to a stream set",
                );
            }
            Err(StoreError::NotLeader) => return not_leader(),
            Err(err) => {
                tracing::error!(error = %ErrorChain(&err), %name, "persisting stream failed");
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ApiErrorCode::PersistenceFailed,
                    "failed to persist stream",
                );
            }
        };
        streams.insert(name.clone(), stored.clone());
        update_pending_generation(&state, &stored).await;
        (stored, changed)
    };
    tracing::info!(%name, "stream accepted");
    with_etag(
        (
            StatusCode::ACCEPTED,
            Json(StreamAccepted {
                status: AcceptedState::Accepted,
                name,
                generation: stored.generation,
                changed,
            }),
        )
            .into_response(),
        stored.revision,
    )
}

async fn plan_stream(
    State(state): State<AppState>,
    payload: Result<Json<StreamDefinition>, JsonRejection>,
) -> Response {
    let Json(stream) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return invalid_json(rejection.body_text()),
    };
    let issues = validate_stream(&stream);
    if !issues.is_empty() {
        return invalid_request("stream validation failed", issues);
    }

    let stored = state.streams.read().await;
    let source_revisions = stored
        .iter()
        .map(|(name, stored)| {
            let revision = if name != &stream.name || stored.spec.source == stream.source {
                stored.source_revision
            } else {
                0
            };
            (name.clone(), revision)
        })
        .collect();
    let mut streams: BTreeMap<String, StreamDefinition> = stored
        .iter()
        .map(|(name, stream)| (name.clone(), stream.spec.clone()))
        .collect();
    drop(stored);
    streams.insert(stream.name.clone(), stream.clone());
    let nodes = state.nodes.read().await;
    let observed = observed_state(&nodes);
    drop(nodes);
    let planning = state.peers().planning(Instant::now(), unix_now());
    let mut outcome = reconcile_with_revisions(
        streams.into_values().collect(),
        &observed,
        &state.keys,
        &planning,
        &source_revisions,
    );
    let endpoints = outcome.endpoints.remove(&stream.name);
    let planned = outcome
        .streams
        .into_iter()
        .find(|status| status.name == stream.name)
        .expect("candidate stream is included in plan");
    let mut hops = outcome
        .hops_by_stream
        .remove(&stream.name)
        .unwrap_or_default();
    withhold_secrets(&mut hops);
    let status = if !stream.enabled {
        PlanStatus::Disabled
    } else if hops.is_empty() {
        PlanStatus::Unplaced
    } else {
        PlanStatus::Placed
    };

    Json(StreamPlan {
        format_policy: stream.format_policy,
        destinations: planned.destinations,
        name: planned.name,
        status,
        nodes: planned.nodes,
        hops,
        endpoints,
        reason: planned
            .conditions
            .iter()
            .find(|condition| condition.condition_type == StreamConditionType::PlacementReady)
            .filter(|condition| {
                status == PlanStatus::Unplaced
                    || condition.reason == StreamConditionReason::SinglePath
            })
            .map(|condition| condition.detail.clone()),
        conditions: planned.conditions,
    })
    .into_response()
}

fn withhold_secrets(hops: &mut [DesiredHop]) {
    for socket in hops.iter_mut().flat_map(DesiredHop::sockets_mut) {
        match socket {
            weave_core::SocketSpec::Srt(socket) => socket.params_mut().passphrase = None,
            weave_core::SocketSpec::Webrtc(socket) => socket.turn.clear(),
            _ => {}
        }
    }
}

async fn delete_stream(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Err(reason) = validate_resource_id(&name) {
        return invalid_request(
            "stream name is invalid",
            vec![resource_id_issue("name", "stream name", reason)],
        );
    }
    let revision = match required_revision(&headers) {
        Ok(revision) => revision,
        Err(error) => return error.response(),
    };
    let mut streams = state.streams.write().await;
    match state.store.delete_stream(&name, revision).await {
        Ok(()) => {}
        Err(StoreError::PreconditionFailed) => {
            return error(
                StatusCode::PRECONDITION_FAILED,
                ApiErrorCode::PreconditionFailed,
                "stream changed or no longer exists",
            );
        }
        Err(StoreError::OwnershipConflict { .. }) => {
            return error(
                StatusCode::CONFLICT,
                ApiErrorCode::StreamOwned,
                "stream belongs to a stream set",
            );
        }
        Err(StoreError::NotLeader) => return not_leader(),
        Err(err) => {
            tracing::error!(error = %ErrorChain(&err), %name, "deleting stream failed");
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                ApiErrorCode::PersistenceFailed,
                "failed to delete stream",
            );
        }
    }
    streams.remove(&name);
    remove_stream_view(&state, &name).await;
    drop(streams);
    tracing::info!(%name, "stream deleted");
    StatusCode::NO_CONTENT.into_response()
}

async fn get_endpoints(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    if let Err(reason) = validate_resource_id(&name) {
        return invalid_request(
            "stream name is invalid",
            vec![resource_id_issue("name", "stream name", reason)],
        );
    }
    let view = state.view.read().await;
    if let Some(endpoints) = view.endpoints.get(&name) {
        (StatusCode::OK, Json(endpoints)).into_response()
    } else if view.streams.iter().any(|s| s.name == name) {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new(
                ApiErrorCode::StreamNotReady,
                format!("stream {name} is not placed"),
            )),
        )
            .into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(ApiError::new(
                ApiErrorCode::StreamNotFound,
                "stream not found",
            )),
        )
            .into_response()
    }
}

async fn list_nodes(
    State(state): State<AppState>,
    Extension(reader): Extension<SharedReader>,
) -> Json<Vec<NodeResource>> {
    Json(
        state
            .nodes
            .read()
            .await
            .values()
            .map(|registration| {
                let resource = sites::resolve(registration);
                match reader {
                    SharedReader::Operator => resource,
                    SharedReader::Node => NodeResource {
                        site: None,
                        ..resource
                    },
                }
            })
            .collect(),
    )
}

async fn list_endpoints(State(state): State<AppState>) -> Json<Vec<EndpointDescriptor>> {
    Json(
        state
            .nodes
            .read()
            .await
            .values()
            .flat_map(|r| r.endpoints.clone())
            .collect(),
    )
}

async fn get_state(State(state): State<AppState>) -> Json<ObservedState> {
    Json(observed_state(&*state.nodes.read().await))
}

#[derive(Deserialize)]
struct Handshake {
    #[serde(default)]
    protocol_version: u32,
    node: HandshakeNode,
}

#[derive(Deserialize)]
struct HandshakeNode {
    id: String,
}

async fn register_node(
    State(state): State<AppState>,
    Extension(caller): Extension<NodeCaller>,
    payload: Result<Json<Box<RawValue>>, JsonRejection>,
) -> Response {
    let Json(body) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return invalid_json(rejection.body_text()),
    };
    let handshake: Handshake = match decode(&body) {
        Ok(handshake) => handshake,
        Err(detail) => return invalid_json(detail),
    };
    if let Some(response) = refuse_other_node(&caller, &handshake.node.id) {
        return response;
    }
    if !protocol_compatible(handshake.protocol_version) {
        tracing::warn!(
            node_id = %handshake.node.id,
            reported = handshake.protocol_version,
            supported = PROTOCOL_VERSION,
            "rejecting node registration: incompatible southbound protocol version"
        );
        return (
            StatusCode::CONFLICT,
            Json(ApiError::with_details(
                ApiErrorCode::IncompatibleProtocolVersion,
                "incompatible southbound protocol version",
                vec![ValidationIssue::new(
                    "protocol_version",
                    "unsupported",
                    format!(
                        "reported {}; supported {}",
                        handshake.protocol_version, PROTOCOL_VERSION
                    ),
                )],
            )),
        )
            .into_response();
    }
    let registration: NodeRegistration = match decode(&body) {
        Ok(registration) => registration,
        Err(detail) => return invalid_json(detail),
    };
    let node_id = registration.node.id.clone();
    let endpoint_count = registration.endpoints.len();
    let node_issues = validate_node(&registration.node);
    if !node_issues.is_empty() {
        return invalid_request("node descriptor is invalid", node_issues);
    }
    let observation_issues: Vec<ValidationIssue> = registration
        .observation
        .iter()
        .flat_map(validate_observation)
        .collect();
    if !observation_issues.is_empty() {
        return invalid_request("node observation is invalid", observation_issues);
    }
    if let Err(issue) = validate_endpoint_node_ids(&registration.endpoints) {
        return invalid_request("endpoint node id is invalid", vec![issue]);
    }
    if let Some(response) = refuse_other_nodes_endpoints(&caller, &registration.endpoints) {
        return response;
    }
    if registration
        .hop_status
        .iter()
        .any(|status| status.node_id != node_id)
    {
        return invalid_request(
            "hop status node id does not match registration node id",
            vec![ValidationIssue::new(
                "hop_status",
                "node_id_mismatch",
                "every hop status node id must match node.id",
            )],
        );
    }

    let summary = NodeSummary::from(&sites::resolve(&registration).node);
    {
        let mut nodes = state.nodes.write().await;
        match state.store.upsert_node(&registration).await {
            Ok(()) => {}
            Err(StoreError::NotLeader) => return not_leader(),
            Err(err) => {
                tracing::error!(error = %ErrorChain(&err), %node_id, "persisting node registration failed");
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ApiErrorCode::PersistenceFailed,
                    "failed to persist node registration",
                );
            }
        }
        nodes.insert(node_id.clone(), registration);
        state
            .last_seen
            .write()
            .await
            .insert(node_id.clone(), Instant::now());
    }
    tracing::info!(%node_id, endpoint_count, "node registered");
    state.emit(EventType::NodeRegistered, summary);
    (
        StatusCode::ACCEPTED,
        Json(NodeAccepted {
            status: AcceptedState::Accepted,
            node_id,
        }),
    )
        .into_response()
}

async fn node_heartbeat(
    State(state): State<AppState>,
    Extension(caller): Extension<NodeCaller>,
    Path(node_id): Path<String>,
    payload: Result<Json<Box<RawValue>>, JsonRejection>,
) -> Response {
    if let Some(response) = refuse_other_node(&caller, &node_id) {
        return response;
    }
    if let Err(reason) = validate_resource_id(&node_id) {
        return invalid_request(
            "node id is invalid",
            vec![resource_id_issue("node_id", "node id", reason)],
        );
    }
    let mut nodes = state.nodes.write().await;
    let Some(registration) = nodes.get_mut(&node_id) else {
        return error(
            StatusCode::NOT_FOUND,
            ApiErrorCode::NodeNotFound,
            "unknown node",
        );
    };
    let decoded = payload
        .map_err(|rejection| rejection.body_text())
        .and_then(|Json(body)| decode(&body));
    let heartbeat: NodeHeartbeat = match decoded {
        Ok(heartbeat) => heartbeat,
        Err(detail) => return invalid_json(detail),
    };
    if node_id != heartbeat.node_id {
        return invalid_request(
            "node id mismatch",
            vec![ValidationIssue::new(
                "node_id",
                "path_mismatch",
                "body node_id must match the path node id",
            )],
        );
    }
    if let Err(issue) = validate_endpoint_node_ids(&heartbeat.endpoints) {
        return invalid_request("endpoint node id is invalid", vec![issue]);
    }
    let io_issues = validate_local_io(&heartbeat.inputs, &heartbeat.outputs, "");
    if !io_issues.is_empty() {
        return invalid_request("heartbeat inputs or outputs are invalid", io_issues);
    }
    let observation_issues: Vec<ValidationIssue> = heartbeat
        .observation
        .iter()
        .flat_map(validate_observation)
        .collect();
    if !observation_issues.is_empty() {
        return invalid_request("heartbeat observation is invalid", observation_issues);
    }
    if let Some(response) = refuse_other_nodes_endpoints(&caller, &heartbeat.endpoints) {
        return response;
    }
    if heartbeat
        .hop_status
        .iter()
        .any(|status| status.node_id != node_id)
    {
        return invalid_request(
            "hop status node id does not match heartbeat node id",
            vec![ValidationIssue::new(
                "hop_status",
                "node_id_mismatch",
                "every hop status node id must match node_id",
            )],
        );
    }
    let was_offline = registration.node.status == NodeStatus::Offline;
    let mut updated = registration.clone();
    updated.node.status = heartbeat.status;
    updated.node.inputs = heartbeat.inputs;
    updated.node.outputs = heartbeat.outputs;
    updated.observation = heartbeat.observation;
    updated.endpoints = heartbeat.endpoints;
    updated.hop_status = heartbeat.hop_status;
    if reported(&updated) != reported(registration) {
        match state.store.upsert_node(&updated).await {
            Ok(()) => {}
            Err(StoreError::NotLeader) => return not_leader(),
            Err(err) => {
                tracing::warn!(error = %ErrorChain(&err), %node_id, "storing a changed node report failed; retrying next tick");
                state.unsaved().nodes.insert(node_id.clone());
            }
        }
    }
    *registration = updated;
    let status = registration.node.status;
    let recovered = (was_offline && status != NodeStatus::Offline)
        .then(|| NodeSummary::from(&sites::resolve(registration).node));
    state
        .last_seen
        .write()
        .await
        .insert(node_id.clone(), Instant::now());
    drop(nodes);
    if let Some(summary) = recovered {
        state.emit(EventType::NodeOnline, summary);
    }

    tracing::debug!(%node_id, ?status, "node heartbeat");
    (
        StatusCode::ACCEPTED,
        Json(NodeAccepted {
            status: AcceptedState::Accepted,
            node_id,
        }),
    )
        .into_response()
}

type HopReport<'a> = (
    &'a str,
    &'a str,
    weave_core::HopState,
    weave_core::LinkCondition,
    Option<weave_core::LinkCondition>,
    Vec<(&'a str, weave_core::LinkCondition)>,
    Option<DeliveryKey>,
);

type DeliveryKey = (
    weave_core::DeliveryState,
    weave_core::DeliveryReason,
    Option<u32>,
    bool,
    bool,
);

type NodeReport<'a> = (
    NodeStatus,
    &'a [weave_core::NodeInput],
    &'a [weave_core::NodeOutput],
    weave_core::NodeTopology,
    Vec<HopReport<'a>>,
);

fn reported(registration: &NodeRegistration) -> NodeReport<'_> {
    (
        registration.node.status,
        &registration.node.inputs,
        &registration.node.outputs,
        sites::resolve(registration).node.topology,
        registration
            .hop_status
            .iter()
            .map(|hop| {
                (
                    hop.id.as_str(),
                    hop.revision.as_str(),
                    hop.state,
                    hop.ingress.condition,
                    hop.merge_ingress.as_ref().map(|socket| socket.condition),
                    hop.egresses
                        .iter()
                        .map(|egress| (egress.branch_id.as_str(), egress.status.condition))
                        .collect(),
                    hop.delivery.as_ref().map(|report| {
                        (
                            report.state,
                            report.reason,
                            report.selected_latency_ms,
                            report.timing_ready,
                            report.redundancy_ready,
                        )
                    }),
                )
            })
            .collect(),
    )
}

async fn get_desired(
    State(state): State<AppState>,
    Extension(caller): Extension<NodeCaller>,
    Path(node_id): Path<String>,
) -> Response {
    if let Some(response) = refuse_other_node(&caller, &node_id) {
        return response;
    }
    if let Err(reason) = validate_resource_id(&node_id) {
        return invalid_request(
            "node id is invalid",
            vec![resource_id_issue("node_id", "node id", reason)],
        );
    }
    match state.desired.read().await.get(&node_id) {
        Some(snapshot) => Json(snapshot.hops.clone()).into_response(),
        None => error(
            StatusCode::NOT_FOUND,
            ApiErrorCode::NodeNotFound,
            "no desired state for this node yet",
        ),
    }
}

async fn exchange_descriptions(
    State(state): State<AppState>,
    Extension(caller): Extension<NodeCaller>,
    Path(node_id): Path<String>,
    payload: Result<Json<weave_core::SignallingExchange>, JsonRejection>,
) -> Response {
    if let Some(response) = refuse_other_node(&caller, &node_id) {
        return response;
    }
    if let Err(reason) = validate_resource_id(&node_id) {
        return invalid_request(
            "node id is invalid",
            vec![resource_id_issue("node_id", "node id", reason)],
        );
    }
    let Json(exchange) = match payload {
        Ok(payload) => payload,
        Err(rejection) => return invalid_json(rejection.body_text()),
    };
    Json(
        state
            .peers()
            .exchange(&node_id, exchange.local, Instant::now()),
    )
    .into_response()
}

fn error(status: StatusCode, code: ApiErrorCode, message: &str) -> Response {
    ApiError::new(code, message).response(status)
}

fn invalid_request(message: &str, details: Vec<ValidationIssue>) -> Response {
    ApiError::with_details(ApiErrorCode::InvalidRequest, message, details)
        .response(StatusCode::BAD_REQUEST)
}

fn invalid_json(detail: String) -> Response {
    ApiError::with_details(
        ApiErrorCode::InvalidJson,
        "request body is not valid for this endpoint",
        vec![ValidationIssue::new("body", "invalid_json", detail)],
    )
    .response(StatusCode::BAD_REQUEST)
}

fn decode<T: DeserializeOwned>(body: &RawValue) -> Result<T, String> {
    serde_path_to_error::deserialize(&mut serde_json::Deserializer::from_str(body.get()))
        .map_err(|error| error.to_string())
}

fn validate_endpoint_node_ids(endpoints: &[EndpointDescriptor]) -> Result<(), ValidationIssue> {
    for (index, endpoint) in endpoints.iter().enumerate() {
        if let Some(node_id) = endpoint.node_id.as_deref()
            && let Err(error) = validate_resource_id(node_id)
        {
            return Err(resource_id_issue(
                &format!("endpoints[{index}].node_id"),
                "node id",
                error,
            ));
        }
    }
    Ok(())
}

fn refuse_other_nodes_endpoints(
    caller: &NodeCaller,
    endpoints: &[EndpointDescriptor],
) -> Option<Response> {
    endpoints
        .iter()
        .filter_map(|endpoint| endpoint.node_id.as_deref())
        .find_map(|node_id| refuse_other_node(caller, node_id))
}

fn stream_condition(
    condition_type: StreamConditionType,
    status: StreamConditionStatus,
    reason: StreamConditionReason,
    detail: impl Into<String>,
) -> StreamCondition {
    StreamCondition {
        condition_type,
        status,
        reason,
        detail: detail.into(),
        last_transition_time: String::new(),
    }
}

fn media_condition(status: PathStatus) -> StreamCondition {
    let (condition_status, reason, detail) = match status {
        PathStatus::Idle => (
            StreamConditionStatus::False,
            StreamConditionReason::MediaIdle,
            "stream is disabled",
        ),
        PathStatus::Failed => (
            StreamConditionStatus::False,
            StreamConditionReason::MediaFailed,
            "a hop failed",
        ),
        PathStatus::Pending => (
            StreamConditionStatus::Unknown,
            StreamConditionReason::NotReady,
            "media status is not known until the path is ready",
        ),
        PathStatus::AwaitingInput => (
            StreamConditionStatus::False,
            StreamConditionReason::AwaitingInput,
            "the source is not providing media",
        ),
        PathStatus::Degraded => (
            StreamConditionStatus::False,
            StreamConditionReason::MediaDegraded,
            "media is not flowing across every branch",
        ),
        PathStatus::Flowing => (
            StreamConditionStatus::True,
            StreamConditionReason::MediaFlowing,
            "media is flowing across every branch",
        ),
    };
    stream_condition(
        StreamConditionType::MediaFlowing,
        condition_status,
        reason,
        detail,
    )
}

fn disabled_conditions(stream: &StreamDefinition) -> Vec<StreamCondition> {
    vec![
        stream_condition(
            StreamConditionType::PlacementReady,
            StreamConditionStatus::False,
            StreamConditionReason::Disabled,
            "stream is disabled",
        ),
        stream_condition(
            StreamConditionType::NodesAvailable,
            StreamConditionStatus::Unknown,
            StreamConditionReason::Disabled,
            "stream is disabled",
        ),
        stream_condition(
            StreamConditionType::HopsReady,
            StreamConditionStatus::Unknown,
            StreamConditionReason::Disabled,
            "stream is disabled",
        ),
        compatibility::evaluate(stream, None, &[]).aggregate(),
        media_condition(PathStatus::Idle),
    ]
}

fn placement_failed_conditions(
    stream: &StreamDefinition,
    error: &PlacementError,
) -> Vec<StreamCondition> {
    let detail = error.to_string();
    let (node_status, node_reason) = if matches!(error, PlacementError::NodeNotRegistered { .. }) {
        (
            StreamConditionStatus::False,
            StreamConditionReason::NodeMissing,
        )
    } else {
        (
            StreamConditionStatus::Unknown,
            StreamConditionReason::NotReady,
        )
    };
    let format = match error {
        PlacementError::FormatBlocked { compatibility, .. } => compatibility.for_stream(stream),
        _ => compatibility::evaluate(stream, None, &[]).aggregate(),
    };
    let placement_reason = if let PlacementError::FormatBlocked { compatibility, .. } = error {
        compatibility.aggregate().reason
    } else if matches!(error, PlacementError::CleartextLink { .. }) {
        StreamConditionReason::CleartextNotAllowed
    } else if matches!(error, PlacementError::TimedUnsupported { .. }) {
        StreamConditionReason::TimedDeliveryUnsupported
    } else if matches!(error, PlacementError::LatencyBudget { .. }) {
        StreamConditionReason::LatencyBudgetInfeasible
    } else if let PlacementError::LocalMissing { end, .. } = error {
        match end {
            LocalEnd::Input => StreamConditionReason::InputMissing,
            LocalEnd::Output => StreamConditionReason::OutputMissing,
        }
    } else {
        StreamConditionReason::PlacementFailed
    };
    vec![
        stream_condition(
            StreamConditionType::PlacementReady,
            StreamConditionStatus::False,
            placement_reason,
            detail.clone(),
        ),
        stream_condition(
            StreamConditionType::NodesAvailable,
            node_status,
            node_reason,
            detail,
        ),
        stream_condition(
            StreamConditionType::HopsReady,
            StreamConditionStatus::Unknown,
            StreamConditionReason::NotReady,
            "no desired hops exist until placement succeeds",
        ),
        format,
        media_condition(PathStatus::Pending),
    ]
}

fn single_path_detail<'a>(shortfalls: impl IntoIterator<Item = &'a SinglePath>) -> Option<String> {
    let details: Vec<String> = shortfalls
        .into_iter()
        .map(|shortfall| {
            format!(
                "destination {} has one path of two: {}",
                shortfall.destination, shortfall.reason
            )
        })
        .collect();
    (!details.is_empty()).then(|| details.join("; "))
}

fn peer_route_detail<'a>(routes: impl IntoIterator<Item = &'a path::PeerRoute>) -> Option<String> {
    let details: Vec<String> = routes.into_iter().map(ToString::to_string).collect();
    (!details.is_empty()).then(|| details.join("; "))
}

fn placed_conditions(
    format: StreamCondition,
    path_status: PathStatus,
    offline_node: Option<&str>,
    single_path: Option<&str>,
    peer_routes: Option<&str>,
) -> Vec<StreamCondition> {
    let nodes = match offline_node {
        Some(node) => stream_condition(
            StreamConditionType::NodesAvailable,
            StreamConditionStatus::False,
            StreamConditionReason::NodeOffline,
            format!("node {node} is offline"),
        ),
        None => stream_condition(
            StreamConditionType::NodesAvailable,
            StreamConditionStatus::True,
            StreamConditionReason::NodesAvailable,
            "every placed node is available",
        ),
    };
    let hops = match path_status {
        PathStatus::Pending => stream_condition(
            StreamConditionType::HopsReady,
            StreamConditionStatus::False,
            StreamConditionReason::HopsPending,
            "one or more desired hops have not reported ready",
        ),
        PathStatus::Failed => stream_condition(
            StreamConditionType::HopsReady,
            StreamConditionStatus::False,
            StreamConditionReason::HopFailed,
            "one or more desired hops failed",
        ),
        _ => stream_condition(
            StreamConditionType::HopsReady,
            StreamConditionStatus::True,
            StreamConditionReason::HopsReady,
            "every desired hop is provisioned",
        ),
    };
    let with_routes = |detail: &str| match peer_routes {
        Some(routes) => format!("{detail}; {routes}"),
        None => detail.to_string(),
    };
    let placement = match single_path {
        Some(detail) => stream_condition(
            StreamConditionType::PlacementReady,
            StreamConditionStatus::True,
            StreamConditionReason::SinglePath,
            with_routes(detail),
        ),
        None => stream_condition(
            StreamConditionType::PlacementReady,
            StreamConditionStatus::True,
            StreamConditionReason::Placed,
            with_routes("the stream has a complete path"),
        ),
    };
    vec![
        placement,
        nodes,
        hops,
        format,
        media_condition(if offline_node.is_some() {
            PathStatus::Degraded
        } else {
            path_status
        }),
    ]
}

fn destination_stream(stream: &StreamDefinition, id: &str) -> StreamDefinition {
    let destination = stream
        .destinations
        .iter()
        .find(|destination| destination.id == id)
        .expect("destination status is built from the stream")
        .clone();
    StreamDefinition {
        name: stream.name.clone(),
        enabled: stream.enabled,
        format_policy: stream.format_policy,
        allow_cleartext_links: stream.allow_cleartext_links,
        source: stream.source.clone(),
        destinations: vec![destination],
    }
}

#[cfg(test)]
fn reconcile(
    streams: Vec<StreamDefinition>,
    observed: &ObservedState,
    keys: &LinkKeys,
) -> ReconcileOutcome {
    reconcile_with_revisions(
        streams,
        observed,
        keys,
        &peers::PeerPlanning::default(),
        &BTreeMap::new(),
    )
}

fn reconcile_with_revisions(
    mut streams: Vec<StreamDefinition>,
    observed: &ObservedState,
    keys: &LinkKeys,
    peers: &peers::PeerPlanning,
    source_revisions: &BTreeMap<String, u64>,
) -> ReconcileOutcome {
    streams.sort_by(|a, b| a.name.cmp(&b.name));

    let mut desired_by_node: BTreeMap<String, Vec<DesiredHop>> = observed
        .nodes
        .iter()
        .map(|node| (node.id.clone(), Vec::new()))
        .collect();

    let offline: BTreeSet<&str> = observed
        .nodes
        .iter()
        .filter(|node| node.status == NodeStatus::Offline)
        .map(|node| node.id.as_str())
        .collect();

    let mut stream_statuses = Vec::with_capacity(streams.len());
    let mut endpoints_by_stream: BTreeMap<String, StreamEndpoints> = BTreeMap::new();
    let mut hops_by_stream: BTreeMap<String, Vec<DesiredHop>> = BTreeMap::new();
    let mut enabled = 0usize;
    let mut flowing = 0usize;
    let mut ports =
        PortAllocator::holding(HopReports::from_reports(&observed.hops, &observed.nodes));
    let reports = reports_by_hop(&observed.hops);
    let mut hop_ids = HopIds::default();

    for stream in &streams {
        if !stream.enabled {
            let conditions = disabled_conditions(stream);
            stream_statuses.push(StreamStatus {
                format_policy: stream.format_policy,
                name: stream.name.clone(),
                generation: 0,
                observed_generation: None,
                status: PathStatus::Idle,
                nodes: Vec::new(),
                destinations: stream
                    .destinations
                    .iter()
                    .map(|destination| StreamDestinationStatus {
                        delivery: None,
                        id: destination.id.clone(),
                        status: PathStatus::Idle,
                        nodes: Vec::new(),
                        conditions: disabled_conditions(&destination_stream(
                            stream,
                            &destination.id,
                        )),
                        endpoint: None,
                    })
                    .collect(),
                conditions,
                ingress: None,
            });
            continue;
        }
        enabled += 1;

        let mut stream_ports = ports.clone();
        let placed = derive_stream(stream, &observed.nodes, peers, &mut stream_ports, keys)
            .and_then(|mut planned| {
                timed::bind(
                    &mut planned.path.hops,
                    stream,
                    source_revisions.get(&stream.name).copied().unwrap_or(0),
                );
                hop_ids.claim(&planned.path).map(|()| planned)
            });
        let status = match placed {
            Ok(planned) => {
                ports = stream_ports;
                let path = planned.path;
                hops_by_stream.insert(stream.name.clone(), path.hops.clone());
                let mut nodes = Vec::new();
                for hop in &path.hops {
                    if !nodes.contains(&hop.node_id) {
                        nodes.push(hop.node_id.clone());
                    }
                    desired_by_node
                        .entry(hop.node_id.clone())
                        .or_default()
                        .push(hop.clone());
                }
                let reported = path_reports(&path, &reports);
                let path_status = path_status(&path, &reported);
                let offline_node = nodes
                    .iter()
                    .find(|id| offline.contains(id.as_str()))
                    .cloned();
                let compatibility = compatibility::evaluate(stream, Some(&path), &observed.nodes);
                let mut status = if offline_node.is_some()
                    || compatibility.aggregate().status == StreamConditionStatus::False
                {
                    PathStatus::Degraded
                } else {
                    path_status
                };
                let endpoints = match stream_endpoints(stream, &path, &observed.nodes) {
                    Ok(endpoints) => {
                        endpoints_by_stream.insert(stream.name.clone(), endpoints.clone());
                        Some(endpoints)
                    }
                    Err(error) => {
                        tracing::warn!(stream = %stream.name, error = %ErrorChain(&error), "cannot resolve stream endpoints");
                        None
                    }
                };
                let destination_statuses: Vec<StreamDestinationStatus> = stream
                    .destinations
                    .iter()
                    .map(|destination| {
                        let nodes = destination_nodes(&path, &destination.id);
                        let branch_status =
                            destination_path_status(&path, &destination.id, &reported);
                        let offline_node = nodes
                            .iter()
                            .find(|id| offline.contains(id.as_str()))
                            .cloned();
                        let destination_stream = destination_stream(stream, &destination.id);
                        let status = if offline_node.is_some()
                            || compatibility.for_stream(&destination_stream).status
                                == StreamConditionStatus::False
                        {
                            PathStatus::Degraded
                        } else {
                            branch_status
                        };
                        let single_path = single_path_detail(
                            planned
                                .single_path
                                .iter()
                                .filter(|shortfall| shortfall.destination == destination.id),
                        );
                        let delivery = destination.delivery.as_ref().and_then(|_| {
                            let receiver = path.hops.iter().find(|hop| {
                                hop.id == path::receiver_hop_id(&stream.name, &destination.id)
                            })?;
                            let current = |hop: &DesiredHop| {
                                reports
                                    .get(&(hop.id.as_str(), hop.node_id.as_str()))
                                    .filter(|report| report.revision == hop.revision())
                                    .and_then(|report| report.delivery.as_ref())
                            };
                            let origin = path.hops.iter().find(|hop| {
                                hop.delivery.as_ref().is_some_and(|timing| {
                                    timing.role == weave_core::DeliveryRole::Origin
                                })
                            });
                            timed::effective_report(current(receiver), origin.and_then(current))
                        });
                        let mut conditions = placed_conditions(
                            compatibility.for_stream(&destination_stream),
                            branch_status,
                            offline_node.as_deref(),
                            single_path.as_deref(),
                            peer_route_detail(
                                planned
                                    .peer_routes
                                    .iter()
                                    .filter(|route| route.destination == destination.id),
                            )
                            .as_deref(),
                        );
                        let status = if destination.delivery.is_some() {
                            conditions
                                .extend(timed::conditions(delivery.as_ref(), destination.paths));
                            timed::status(status, delivery.as_ref(), destination.paths)
                        } else {
                            status
                        };
                        StreamDestinationStatus {
                            delivery,
                            id: destination.id.clone(),
                            status,
                            nodes,
                            conditions,
                            endpoint: endpoints.as_ref().and_then(|endpoints| {
                                endpoints
                                    .destinations
                                    .iter()
                                    .find(|candidate| candidate.id == destination.id)
                                    .and_then(|candidate| candidate.endpoint.clone())
                            }),
                        }
                    })
                    .collect();
                for candidate in [
                    PathStatus::Failed,
                    PathStatus::Degraded,
                    PathStatus::Pending,
                ] {
                    if destination_statuses
                        .iter()
                        .any(|destination| destination.status == candidate)
                    {
                        status = candidate;
                        break;
                    }
                }
                let mut conditions = placed_conditions(
                    compatibility.aggregate(),
                    path_status,
                    offline_node.as_deref(),
                    single_path_detail(&planned.single_path).as_deref(),
                    peer_route_detail(&planned.peer_routes).as_deref(),
                );
                conditions.extend(timed::aggregate(&destination_statuses));
                stream_statuses.push(StreamStatus {
                    format_policy: stream.format_policy,
                    name: stream.name.clone(),
                    generation: 0,
                    observed_generation: None,
                    status,
                    nodes,
                    ingress: endpoints.as_ref().and_then(|value| value.ingress.clone()),
                    destinations: destination_statuses,
                    conditions,
                });
                status
            }
            Err(error) => {
                tracing::warn!(stream = %stream.name, error = %ErrorChain(&error), "cannot place stream; retrying next tick");
                let conditions = placement_failed_conditions(stream, &error);
                stream_statuses.push(StreamStatus {
                    format_policy: stream.format_policy,
                    name: stream.name.clone(),
                    generation: 0,
                    observed_generation: None,
                    status: PathStatus::Pending,
                    nodes: Vec::new(),
                    destinations: stream
                        .destinations
                        .iter()
                        .map(|destination| StreamDestinationStatus {
                            delivery: None,
                            id: destination.id.clone(),
                            status: PathStatus::Pending,
                            nodes: Vec::new(),
                            conditions: placement_failed_conditions(
                                &destination_stream(stream, &destination.id),
                                &error,
                            ),
                            endpoint: None,
                        })
                        .collect(),
                    conditions,
                    ingress: None,
                });
                PathStatus::Pending
            }
        };
        if status == PathStatus::Flowing {
            flowing += 1;
        }
    }

    for stream in &mut stream_statuses {
        stream
            .destinations
            .sort_by(|left, right| left.id.cmp(&right.id));
    }

    let status = if enabled == 0 {
        ReconcileStatus::Idle
    } else if stream_statuses
        .iter()
        .any(|s| matches!(s.status, PathStatus::Failed | PathStatus::Degraded))
    {
        ReconcileStatus::Degraded
    } else if stream_statuses
        .iter()
        .any(|s| s.status == PathStatus::Pending)
    {
        ReconcileStatus::Converging
    } else {
        ReconcileStatus::Converged
    };
    let report = ReconcileReport {
        status,
        summary: format!(
            "{flowing}/{enabled} stream(s) flowing across {} node(s)",
            desired_by_node.len()
        ),
    };

    ReconcileOutcome {
        report,
        streams: stream_statuses,
        endpoints: endpoints_by_stream,
        hops_by_stream,
        desired_by_node,
    }
}

#[cfg(test)]
mod tests {
    mod formats {
        use super::*;
        include!("format_tests.rs");
    }
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tower::ServiceExt;
    use weave_core::{
        DeclaredTopology, HopEndpointClass, HopProfile, HopStatus, NetworkAttachment,
        NetworkListeners, NodeCapabilities, NodeDescriptor, PortRange, RoleSet, SrtEndpoint,
        SrtListener, StreamDestination, StreamTransport, Transport, TransportClass,
    };

    use crate::webhook::tests::{Sink, sink};

    fn open_router(state: AppState) -> Router {
        router(state, Guard::Disabled, NodeGuard::Disabled)
    }

    fn mem_state() -> (AppState, Arc<MemStore>) {
        let mem = Arc::new(MemStore::new());
        let state = AppState {
            store: mem.clone(),
            streams: Arc::new(RwLock::new(BTreeMap::new())),
            stream_sets: Arc::new(RwLock::new(BTreeMap::new())),
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
            last_seen: Arc::new(RwLock::new(BTreeMap::new())),
            node_ttl: Duration::from_secs(15),
            node_forget: Duration::from_secs(300),
            desired: Arc::new(RwLock::new(BTreeMap::new())),
            view: Arc::new(RwLock::new(ControllerView::default())),
            webhooks: None,
            keys: LinkKeys::for_tests(),
            unsaved: Arc::default(),
            peers: Arc::default(),
        };
        (state, mem)
    }

    fn srt_forward() -> HopProfile {
        let srt = HopEndpointClass::Transport(TransportClass {
            transport: Transport::Srt,
            roles: RoleSet::both(),
        });
        HopProfile {
            timed_delivery: None,
            id: "srt-forward".to_string(),
            ingress: srt.clone(),
            egress: srt,
            max_egresses: None,
            media_behavior: weave_core::MediaBehavior::Preserve,
            merge: false,
            accepts: Some(weave_core::FormatConstraint::default()),
        }
    }

    fn node_registration(id: &str, host: &str) -> NodeRegistration {
        NodeRegistration {
            protocol_version: PROTOCOL_VERSION,
            node: NodeDescriptor {
                inputs: Vec::new(),
                outputs: Vec::new(),
                id: id.to_string(),
                endpoint: format!("http://{id}:8080"),
                status: NodeStatus::Ready,
                capabilities: NodeCapabilities {
                    hop_profiles: vec![srt_forward()],
                },
                topology: DeclaredTopology {
                    site: None,
                    attachments: vec![NetworkAttachment {
                        id: "wan".to_string(),
                        network: "internet".to_string(),
                        dial: true,
                        listeners: NetworkListeners {
                            srt: Some(SrtListener {
                                host: host.to_string(),
                                port_range: PortRange {
                                    start: 7000,
                                    end: 7999,
                                },
                            }),
                            whip: None,
                            whep: None,
                            rist: None,
                        },
                    }],
                },
            },
            observation: None,
            endpoints: Vec::new(),
            hop_status: Vec::new(),
        }
    }

    fn nat_registration(id: &str, host: &str) -> NodeRegistration {
        let mut registration = node_registration(id, host);
        let attachments = &mut registration.node.topology.attachments;
        let mut site = attachments[0].clone();
        site.id = "site".to_string();
        site.network = format!("{id}-local");
        attachments[0].listeners.srt = None;
        attachments.push(site);
        registration
    }

    fn srt_endpoint(node: &str, latency: u32) -> StreamTransport {
        StreamTransport::Srt(SrtEndpoint {
            node: Some(node.to_string()),
            remote: None,
            via: Vec::new(),
            format: None,
            accepts: None,
            network: None,
            latency: Some(latency),
            passphrase: None,
        })
    }

    fn stream_between(name: &str, source: &str, destination: &str) -> StreamDefinition {
        StreamDefinition {
            name: name.to_string(),
            enabled: true,
            format_policy: weave_core::FormatPolicy::Report,
            allow_cleartext_links: false,
            source: srt_endpoint(source, 200),
            destinations: vec![StreamDestination {
                delivery: None,
                id: "studio".to_string(),
                paths: 1,
                endpoint: srt_endpoint(destination, 1000),
            }],
        }
    }

    fn stream(name: &str) -> StreamDefinition {
        stream_between(name, "strom-node-1", "strom-node-2")
    }

    fn stored_stream(spec: StreamDefinition) -> StoredStream {
        StoredStream {
            source_revision: 1,
            spec,
            generation: 1,
            revision: 1,
            owner: None,
        }
    }

    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        if method == "POST" && uri == "/streams" {
            request = request.header("if-none-match", "*");
        }
        if method == "DELETE" && uri.starts_with("/streams/") {
            request = request.header("if-match", "\"revision-1\"");
        }
        let request = request
            .body(body.map_or(Body::empty(), |v| {
                Body::from(serde_json::to_vec(&v).unwrap())
            }))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, value)
    }

    async fn send_with_headers(
        app: &Router,
        method: &str,
        uri: &str,
        body: Option<Value>,
        headers: &[(&str, &str)],
    ) -> (StatusCode, HeaderMap, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let response = app
            .clone()
            .oneshot(
                request
                    .body(body.map_or(Body::empty(), |value| {
                        Body::from(serde_json::to_vec(&value).unwrap())
                    }))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, headers, value)
    }

    fn response_etag(headers: &HeaderMap) -> String {
        headers
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    async fn desired_hops(app: &Router, node_id: &str) -> (StatusCode, Value) {
        send(app, "GET", &format!("/nodes/{node_id}/desired"), None).await
    }

    async fn two_nodes_and_basic(state: &AppState) {
        let mut nodes = state.nodes.write().await;
        nodes.insert(
            "strom-node-1".to_string(),
            node_registration("strom-node-1", "172.26.0.10"),
        );
        nodes.insert(
            "strom-node-2".to_string(),
            node_registration("strom-node-2", "172.27.0.10"),
        );
        state
            .streams
            .write()
            .await
            .insert("basic".to_string(), stored_stream(stream("basic")));
    }

    #[tokio::test]
    async fn desired_reflects_computed_hops_after_a_reconcile_tick() {
        let (state, _mem) = mem_state();
        two_nodes_and_basic(&state).await;

        reconcile_tick(&state).await;

        let app = open_router(state);
        let (status, body) = desired_hops(&app, "strom-node-1").await;
        assert_eq!(status, StatusCode::OK);
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(hops.len(), 1, "sender hop placed on node 1");
        assert_eq!(hops[0].id, "weave-basic-sender");

        let (_, body) = desired_hops(&app, "strom-node-2").await;
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(hops.len(), 1, "receiver hop placed on node 2");
        assert_eq!(hops[0].id, "weave-basic-receiver-studio");
    }

    #[tokio::test]
    async fn post_stream_then_get_returns_it_and_writes_through() {
        let (state, mem) = mem_state();
        let app = open_router(state);

        let (status, _) = send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("basic")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let (status, body) = send(&app, "GET", "/streams", None).await;
        assert_eq!(status, StatusCode::OK);
        let listed: Vec<StreamResource> = serde_json::from_value(body).unwrap();
        assert_eq!(listed[0].generation, 1);
        assert_eq!(listed[0].spec, stream("basic"));

        let (status, body) = send(&app, "GET", "/streams/basic", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            serde_json::from_value::<StreamResource>(body).unwrap(),
            StreamResource {
                generation: 1,
                owner: None,
                spec: stream("basic")
            }
        );

        let (status, body) = send(&app, "GET", "/streams/missing", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "stream_not_found");

        assert_eq!(
            mem.upsert_stream_calls(),
            1,
            "stream was written through the store"
        );
        assert_eq!(mem.load_streams().await.unwrap()[0].spec, stream("basic"));
    }

    #[tokio::test]
    async fn stream_writes_require_and_enforce_etag_preconditions() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        let original = stream("basic");

        let (status, _, body) = send_with_headers(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(&original).unwrap()),
            &[],
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_REQUIRED);
        assert_eq!(body["code"], "precondition_required");

        let (status, headers, body) = send_with_headers(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(&original).unwrap()),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let first_etag = response_etag(&headers);
        let accepted: StreamAccepted = serde_json::from_value(body).unwrap();
        assert_eq!(accepted.generation, 1);
        assert!(accepted.changed);

        let (status, headers, body) = send_with_headers(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(&original).unwrap()),
            &[("if-match", &first_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(response_etag(&headers), first_etag);
        let accepted: StreamAccepted = serde_json::from_value(body).unwrap();
        assert_eq!(accepted.generation, 1);
        assert!(!accepted.changed);

        let mut changed = original.clone();
        changed.enabled = false;
        let (status, headers, body) = send_with_headers(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(&changed).unwrap()),
            &[("if-match", &first_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let second_etag = response_etag(&headers);
        assert_ne!(second_etag, first_etag);
        let accepted: StreamAccepted = serde_json::from_value(body).unwrap();
        assert_eq!(accepted.generation, 2);
        assert!(accepted.changed);

        let (status, _, body) = send_with_headers(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(&original).unwrap()),
            &[("if-match", &first_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        assert_eq!(body["code"], "precondition_failed");

        let (status, headers, body) =
            send_with_headers(&app, "GET", "/streams/basic", None, &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response_etag(&headers), second_etag);
        let resource: StreamResource = serde_json::from_value(body).unwrap();
        assert_eq!(resource.generation, 2);
        assert_eq!(resource.spec, changed);
    }

    #[tokio::test]
    async fn stream_set_apply_retains_noops_and_prunes_atomically() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        let alpha = stream("alpha");
        let beta = stream("beta");
        let create = StreamSetApply {
            streams: vec![alpha.clone(), beta.clone()],
            prune: false,
        };
        let (status, headers, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(serde_json::to_value(create).unwrap()),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let first_etag = response_etag(&headers);
        let accepted: StreamSetAccepted = serde_json::from_value(body).unwrap();
        assert!(accepted.changed);
        assert_eq!(accepted.streams.len(), 2);
        assert!(
            accepted
                .streams
                .iter()
                .all(|stream| stream.action == StreamSetAction::Created)
        );

        let mut changed_alpha = alpha;
        changed_alpha.enabled = false;
        let gamma = stream("gamma");
        let update = StreamSetApply {
            streams: vec![changed_alpha.clone(), gamma.clone()],
            prune: false,
        };
        let (status, headers, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(serde_json::to_value(&update).unwrap()),
            &[("if-match", &first_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let second_etag = response_etag(&headers);
        assert_ne!(second_etag, first_etag);
        let accepted: StreamSetAccepted = serde_json::from_value(body).unwrap();
        assert_eq!(
            accepted
                .streams
                .iter()
                .map(|stream| (stream.name.as_str(), stream.generation, stream.action))
                .collect::<Vec<_>>(),
            [
                ("alpha", 2, StreamSetAction::Updated),
                ("beta", 1, StreamSetAction::Unchanged),
                ("gamma", 1, StreamSetAction::Created),
            ]
        );

        let (status, headers, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(serde_json::to_value(&update).unwrap()),
            &[("if-match", &second_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(response_etag(&headers), second_etag);
        let accepted: StreamSetAccepted = serde_json::from_value(body).unwrap();
        assert!(!accepted.changed);
        assert!(
            accepted
                .streams
                .iter()
                .all(|stream| stream.action == StreamSetAction::Unchanged)
        );

        let prune = StreamSetApply {
            streams: vec![changed_alpha.clone()],
            prune: true,
        };
        let (status, headers, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(serde_json::to_value(prune).unwrap()),
            &[("if-match", &second_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let third_etag = response_etag(&headers);
        let accepted: StreamSetAccepted = serde_json::from_value(body).unwrap();
        assert_eq!(accepted.pruned, ["beta", "gamma"]);

        let mut stale = changed_alpha;
        stale.enabled = true;
        let (status, _, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(
                serde_json::to_value(StreamSetApply {
                    streams: vec![stale],
                    prune: true,
                })
                .unwrap(),
            ),
            &[("if-match", &second_etag)],
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        assert_eq!(body["code"], "precondition_failed");

        let (status, headers, body) =
            send_with_headers(&app, "GET", "/stream-sets/studio-a", None, &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response_etag(&headers), third_etag);
        let resource: StreamSetResource = serde_json::from_value(body).unwrap();
        assert_eq!(resource.streams.len(), 1);
        assert_eq!(resource.streams[0].owner.as_deref(), Some("studio-a"));
        assert_eq!(resource.streams[0].spec.name, "alpha");
    }

    fn two_destination_stream(name: &str) -> StreamDefinition {
        let mut stream = stream(name);
        stream.destinations.push(StreamDestination {
            delivery: None,
            id: "preview".to_string(),
            paths: 1,
            endpoint: srt_endpoint("strom-node-3", 1000),
        });
        stream
    }

    fn with_destinations_reversed(mut stream: StreamDefinition) -> StreamDefinition {
        stream.destinations.reverse();
        stream
    }

    async fn controller_on(store: Arc<dyn StateStore>) -> Router {
        open_router(
            AppState::hydrate(
                store,
                Duration::from_secs(15),
                Duration::from_secs(300),
                None,
                LinkKeys::for_tests(),
            )
            .await
            .unwrap(),
        )
    }

    async fn etag_and_body(app: &Router, uri: &str) -> (String, Value) {
        let (status, headers, body) = send_with_headers(app, "GET", uri, None, &[]).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        (response_etag(&headers), body)
    }

    async fn assert_a_reordered_reapply_is_a_noop(store: Arc<dyn StateStore>) {
        let app = controller_on(store.clone()).await;
        let original = two_destination_stream("fan-out");
        let (status, headers, body) = send_with_headers(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(&original).unwrap()),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let etag = response_etag(&headers);
        let before = etag_and_body(&app, "/streams/fan-out").await;

        let (status, headers, body) = send_with_headers(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(with_destinations_reversed(original)).unwrap()),
            &[("if-match", &etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let accepted: StreamAccepted = serde_json::from_value(body).unwrap();
        assert_eq!((accepted.generation, accepted.changed), (1, false));
        assert_eq!(response_etag(&headers), etag);

        let restarted = controller_on(store).await;
        assert_eq!(etag_and_body(&restarted, "/streams/fan-out").await, before);

        let mut changed = two_destination_stream("fan-out");
        changed.enabled = false;
        let (status, headers, body) = send_with_headers(
            &restarted,
            "POST",
            "/streams",
            Some(serde_json::to_value(&changed).unwrap()),
            &[("if-match", &etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let accepted: StreamAccepted = serde_json::from_value(body).unwrap();
        assert_eq!((accepted.generation, accepted.changed), (2, true));
        assert_ne!(response_etag(&headers), etag);
    }

    #[tokio::test]
    async fn a_reordered_reapply_keeps_the_generation_and_etag() {
        assert_a_reordered_reapply_is_a_noop(Arc::new(MemStore::new())).await;
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn a_reordered_reapply_on_postgres_keeps_the_generation_and_etag() {
        let url = store::tests::fresh_database().await;
        assert_a_reordered_reapply_is_a_noop(Arc::new(store::tests::leading_pg_store(&url).await))
            .await;
    }

    async fn assert_a_reordered_stream_set_reapply_is_a_noop(store: Arc<dyn StateStore>) {
        let app = controller_on(store.clone()).await;
        let original = two_destination_stream("fan-out");
        let apply = |stream: StreamDefinition| {
            Some(
                serde_json::to_value(StreamSetApply {
                    streams: vec![stream],
                    prune: true,
                })
                .unwrap(),
            )
        };
        let (status, headers, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            apply(original.clone()),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let etag = response_etag(&headers);
        let set_before = etag_and_body(&app, "/stream-sets/studio-a").await;
        let member_before = etag_and_body(&app, "/streams/fan-out").await;

        let (status, headers, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            apply(with_destinations_reversed(original)),
            &[("if-match", &etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let accepted: StreamSetAccepted = serde_json::from_value(body).unwrap();
        assert!(!accepted.changed);
        assert_eq!(
            accepted
                .streams
                .iter()
                .map(|stream| (stream.name.as_str(), stream.generation, stream.action))
                .collect::<Vec<_>>(),
            [("fan-out", 1, StreamSetAction::Unchanged)]
        );
        assert_eq!(response_etag(&headers), etag);

        let restarted = controller_on(store).await;
        assert_eq!(
            etag_and_body(&restarted, "/stream-sets/studio-a").await,
            set_before
        );
        assert_eq!(
            etag_and_body(&restarted, "/streams/fan-out").await,
            member_before
        );
    }

    #[tokio::test]
    async fn a_reordered_stream_set_reapply_keeps_generations_and_etags() {
        assert_a_reordered_stream_set_reapply_is_a_noop(Arc::new(MemStore::new())).await;
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn a_reordered_stream_set_reapply_on_postgres_keeps_generations_and_etags() {
        let url = store::tests::fresh_database().await;
        assert_a_reordered_stream_set_reapply_is_a_noop(Arc::new(
            store::tests::leading_pg_store(&url).await,
        ))
        .await;
    }

    #[tokio::test]
    async fn stream_set_conflicts_do_not_partially_apply() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        let (status, _) = send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("taken")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let (status, _, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(
                serde_json::to_value(StreamSetApply {
                    streams: vec![stream("new"), stream("taken")],
                    prune: false,
                })
                .unwrap(),
            ),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["code"], "ownership_conflict");
        let (status, _) = send(&app, "GET", "/streams/new", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = send(&app, "GET", "/stream-sets/studio-a", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    fn stream_x_to_a_sender() -> StreamDefinition {
        let mut stream = stream("x");
        stream.destinations[0].id = "a-sender".to_string();
        stream
    }

    const X_CONFLICT: &str = "stream x can plan hop id weave-x-receiver-a-sender, \
                              which stream x-receiver-a can also plan";

    fn assert_hop_id_conflict(body: &Value, field: &str) {
        assert_eq!(body["code"], "hop_id_conflict");
        assert_eq!(
            body["message"],
            "stream x and stream x-receiver-a can plan the same hop id"
        );
        assert_eq!(body["details"][0]["field"], field);
        assert_eq!(body["details"][0]["code"], "hop_id_conflict");
        assert_eq!(body["details"][0]["message"], X_CONFLICT);
    }

    async fn register_both_nodes(state: &AppState) {
        let mut nodes = state.nodes.write().await;
        for (id, host) in [
            ("strom-node-1", "172.26.0.10"),
            ("strom-node-2", "172.27.0.10"),
        ] {
            nodes.insert(id.to_string(), node_registration(id, host));
        }
    }

    #[tokio::test]
    async fn a_stream_that_can_plan_a_stored_streams_hop_id_is_refused() {
        let (state, mem) = mem_state();
        register_both_nodes(&state).await;
        let app = open_router(state.clone());
        let (status, _) = send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("x-receiver-a")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        reconcile_tick(&state).await;
        assert!(state.view.read().await.hops.contains_key("x-receiver-a"));

        let (status, body) = send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream_x_to_a_sender()).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_hop_id_conflict(&body, "name");
        assert_eq!(
            mem.upsert_stream_calls(),
            1,
            "the refused stream is not stored"
        );

        reconcile_tick(&state).await;
        let view = state.view.read().await;
        assert!(
            view.hops.contains_key("x-receiver-a"),
            "the stored stream stays placed"
        );
        assert!(!view.streams.iter().any(|status| status.name == "x"));
    }

    #[tokio::test]
    async fn a_stream_set_write_that_can_plan_a_kept_streams_hop_id_writes_nothing() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("x-receiver-a")).unwrap()),
        )
        .await;

        let (status, _, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(
                serde_json::to_value(StreamSetApply {
                    streams: vec![stream("alpha"), stream_x_to_a_sender()],
                    prune: false,
                })
                .unwrap(),
            ),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_hop_id_conflict(&body, "streams[1].name");
        let (status, _) = send(&app, "GET", "/stream-sets/studio-a", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = send(&app, "GET", "/streams/alpha", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn two_streams_in_one_set_write_that_can_plan_the_same_hop_id_are_refused() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        let (status, _, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(
                serde_json::to_value(StreamSetApply {
                    streams: vec![stream_x_to_a_sender(), stream("x-receiver-a")],
                    prune: false,
                })
                .unwrap(),
            ),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_hop_id_conflict(&body, "streams[0].name");
    }

    #[tokio::test]
    async fn a_set_write_may_prune_the_stream_it_would_share_a_hop_id_with() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        let apply =
            |streams, prune| Some(serde_json::to_value(StreamSetApply { streams, prune }).unwrap());
        let (status, headers, _) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            apply(vec![stream("x-receiver-a")], false),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let etag = response_etag(&headers);

        let (status, _, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            apply(vec![stream_x_to_a_sender()], false),
            &[("if-match", &etag)],
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "without prune the member stays"
        );
        assert_hop_id_conflict(&body, "streams[0].name");

        let (status, _, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            apply(vec![stream_x_to_a_sender()], true),
            &[("if-match", &etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let accepted: StreamSetAccepted = serde_json::from_value(body).unwrap();
        assert_eq!(accepted.pruned, ["x-receiver-a"]);
    }

    #[tokio::test]
    async fn streams_that_already_collide_reapply_unchanged_and_plan_in_name_order() {
        let (state, mem) = mem_state();
        register_both_nodes(&state).await;
        let loose = mem.create_stream(&stream("x-receiver-a")).await.unwrap();
        state
            .streams
            .write()
            .await
            .insert(loose.spec.name.clone(), loose);
        let set = mem
            .create_stream_set("studio-a", &[stream_x_to_a_sender()], false)
            .await
            .unwrap();
        for stored in &set.stream_set.streams {
            state
                .streams
                .write()
                .await
                .insert(stored.spec.name.clone(), stored.clone());
        }
        state
            .stream_sets
            .write()
            .await
            .insert("studio-a".to_string(), set.stream_set.revision);
        let app = open_router(state.clone());

        let (_, headers, _) =
            send_with_headers(&app, "GET", "/streams/x-receiver-a", None, &[]).await;
        let etag = response_etag(&headers);
        let (status, headers, body) = send_with_headers(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("x-receiver-a")).unwrap()),
            &[("if-match", &etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(response_etag(&headers), etag);
        assert_eq!(body["changed"], false);

        let (_, headers, _) =
            send_with_headers(&app, "GET", "/stream-sets/studio-a", None, &[]).await;
        let etag = response_etag(&headers);
        let (status, headers, body) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(
                serde_json::to_value(StreamSetApply {
                    streams: vec![stream_x_to_a_sender()],
                    prune: false,
                })
                .unwrap(),
            ),
            &[("if-match", &etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(response_etag(&headers), etag);
        assert_eq!(body["changed"], false);
        assert_eq!(body["streams"][0]["generation"], 1);

        reconcile_tick(&state).await;
        let view = state.view.read().await;
        assert!(view.hops.contains_key("x"));
        let later = view
            .streams
            .iter()
            .find(|status| status.name == "x-receiver-a")
            .unwrap();
        assert_eq!(later.status, PathStatus::Pending);
        assert!(later.conditions.iter().any(|condition| condition.detail
            == "hop id weave-x-receiver-a-sender is already planned for stream x"));
    }

    #[tokio::test]
    async fn owned_streams_reject_single_resource_mutations() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        let (status, _, _) = send_with_headers(
            &app,
            "PUT",
            "/stream-sets/studio-a",
            Some(
                serde_json::to_value(StreamSetApply {
                    streams: vec![stream("alpha")],
                    prune: false,
                })
                .unwrap(),
            ),
            &[("if-none-match", "*")],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let (_, headers, _) = send_with_headers(&app, "GET", "/streams/alpha", None, &[]).await;
        let stream_etag = response_etag(&headers);

        for (method, body) in [
            ("POST", Some(serde_json::to_value(stream("alpha")).unwrap())),
            ("DELETE", None),
        ] {
            let (status, _, body) = send_with_headers(
                &app,
                method,
                if method == "POST" {
                    "/streams"
                } else {
                    "/streams/alpha"
                },
                body,
                &[("if-match", &stream_etag)],
            )
            .await;
            assert_eq!(status, StatusCode::CONFLICT);
            assert_eq!(body["code"], "stream_owned");
        }
    }

    #[tokio::test]
    async fn delete_stream_removes_and_writes_through() {
        let (state, mem) = mem_state();
        let app = open_router(state);

        send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("basic")).unwrap()),
        )
        .await;
        let (status, _) = send(&app, "DELETE", "/streams/basic", None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(mem.delete_stream_calls(), 1);

        let (_, body) = send(&app, "GET", "/streams", None).await;
        let listed: Vec<StreamResource> = serde_json::from_value(body).unwrap();
        assert!(listed.is_empty());

        let (status, _) = send(&app, "DELETE", "/streams/basic", None).await;
        assert_eq!(
            status,
            StatusCode::PRECONDITION_FAILED,
            "second delete fails its revision precondition"
        );
    }

    #[tokio::test]
    async fn endpoints_route_pending_then_placed() {
        let (state, _mem) = mem_state();
        {
            let mut view = state.view.write().await;
            view.streams = vec![StreamStatus {
                format_policy: weave_core::FormatPolicy::Report,
                name: "basic".to_string(),
                generation: 1,
                observed_generation: None,
                status: PathStatus::Pending,
                nodes: Vec::new(),
                conditions: Vec::new(),
                ingress: None,
                destinations: Vec::new(),
            }];
        }
        let app = open_router(state);
        let (status, _) = send(&app, "GET", "/streams/basic/endpoints", None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

        let (status, _) = send(&app, "GET", "/streams/nope/endpoints", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn view_joins_desired_hops_with_reported_status() {
        let (state, _mem) = mem_state();
        {
            let mut nodes = state.nodes.write().await;
            nodes.insert(
                "strom-node-1".to_string(),
                node_registration("strom-node-1", "172.26.0.10"),
            );
            nodes.insert(
                "strom-node-2".to_string(),
                node_registration("strom-node-2", "172.27.0.10"),
            );
            let mut definition = stream("basic");
            let mut preview = definition.destinations[0].clone();
            preview.id = "preview".to_string();
            definition.destinations.push(preview);
            state
                .streams
                .write()
                .await
                .insert("basic".to_string(), stored_stream(definition));
            let now = Instant::now();
            let mut seen = state.last_seen.write().await;
            seen.insert("strom-node-1".to_string(), now);
            seen.insert("strom-node-2".to_string(), now);
        }

        reconcile_tick(&state).await;
        let revision = state.desired.read().await["strom-node-1"].hops[0].revision();

        state
            .nodes
            .write()
            .await
            .get_mut("strom-node-1")
            .unwrap()
            .hop_status = vec![weave_core::HopStatus {
            delivery: None,
            id: "weave-basic-sender".to_string(),
            node_id: "strom-node-1".to_string(),
            revision,
            state: weave_core::HopState::Provisioned,
            ingress: weave_core::SocketStatus {
                peer: None,
                condition: weave_core::LinkCondition::Flowing,
                resolved: None,
                stats: Some(weave_core::LinkStats {
                    rate_mbps: 3.2,
                    ..weave_core::LinkStats::default()
                }),
            },
            merge_ingress: None,
            egresses: vec![
                weave_core::EgressStatus {
                    branch_id: "preview".to_string(),
                    status: weave_core::SocketStatus {
                        peer: None,
                        condition: weave_core::LinkCondition::Flowing,
                        resolved: None,
                        stats: Some(weave_core::LinkStats {
                            rate_mbps: 3.1,
                            ..weave_core::LinkStats::default()
                        }),
                    },
                },
                weave_core::EgressStatus {
                    branch_id: "studio".to_string(),
                    status: weave_core::SocketStatus {
                        peer: None,
                        condition: weave_core::LinkCondition::Connecting,
                        resolved: Some(weave_core::ResolvedAddr {
                            host: "172.27.0.10".to_string(),
                            port: 7555,
                        }),
                        stats: Some(weave_core::LinkStats {
                            rate_mbps: 0.0,
                            ..weave_core::LinkStats::default()
                        }),
                    },
                },
            ],
        }];

        let app = open_router(state);
        let (status, body) = send(&app, "GET", "/view", None).await;
        assert_eq!(status, StatusCode::OK);

        assert_eq!(body["report"]["status"], "converging");
        assert_eq!(body["nodes"].as_array().unwrap().len(), 2);
        assert_eq!(body["nodes"][0]["last_seen_secs"], 0);

        let basic = &body["streams"][0];
        assert_eq!(basic["name"], "basic");
        let hops = basic["hops"].as_array().unwrap();
        assert_eq!(hops.len(), 3, "sender + two receivers");

        let sender = &hops[0];
        assert_eq!(sender["id"], "weave-basic-sender");
        assert_eq!(sender["node"], "strom-node-1");
        assert_eq!(sender["state"], "provisioned");
        assert_eq!(sender["ingress"]["condition"], "flowing");
        assert_eq!(sender["ingress"]["stats"]["rate_mbps"], 3.2);
        assert_eq!(sender["ingress"]["mode"], "listen");
        assert_eq!(sender["egresses"][0]["branch_id"], "preview");
        assert_eq!(sender["egresses"][0]["condition"], "flowing");
        assert_eq!(sender["egresses"][0]["stats"]["rate_mbps"], 3.1);
        assert_eq!(sender["egresses"][0]["mode"], "connect");
        assert_eq!(sender["egresses"][0]["host"], "172.27.0.10");
        assert_eq!(sender["egresses"][1]["branch_id"], "studio");
        assert_eq!(sender["egresses"][1]["condition"], "connecting");
        assert_eq!(sender["egresses"][1]["resolved"]["port"], 7555);

        let receiver = &hops[1];
        assert_eq!(receiver["node"], "strom-node-2");
        assert!(
            receiver.get("state").is_none(),
            "unreported hop carries no observed fields"
        );
    }

    #[tokio::test]
    async fn view_shows_the_first_report_of_a_branch_reported_twice() {
        let (state, _mem) = mem_state();
        {
            let mut nodes = state.nodes.write().await;
            nodes.insert(
                "strom-node-1".to_string(),
                node_registration("strom-node-1", "172.26.0.10"),
            );
            nodes.insert(
                "strom-node-2".to_string(),
                node_registration("strom-node-2", "172.27.0.10"),
            );
            state
                .streams
                .write()
                .await
                .insert("basic".to_string(), stored_stream(stream("basic")));
        }
        reconcile_tick(&state).await;
        let revision = state.desired.read().await["strom-node-1"].hops[0].revision();
        let egress = |condition| weave_core::EgressStatus {
            branch_id: "studio".to_string(),
            status: weave_core::SocketStatus {
                peer: None,
                condition,
                resolved: None,
                stats: None,
            },
        };
        state
            .nodes
            .write()
            .await
            .get_mut("strom-node-1")
            .unwrap()
            .hop_status = vec![HopStatus {
            delivery: None,
            id: "weave-basic-sender".to_string(),
            node_id: "strom-node-1".to_string(),
            revision,
            state: weave_core::HopState::Provisioned,
            ingress: weave_core::SocketStatus {
                peer: None,
                condition: weave_core::LinkCondition::Flowing,
                resolved: None,
                stats: None,
            },
            merge_ingress: None,
            egresses: vec![
                egress(weave_core::LinkCondition::Connecting),
                egress(weave_core::LinkCondition::Flowing),
            ],
        }];

        let (status, body) = send(&open_router(state), "GET", "/view", None).await;
        assert_eq!(status, StatusCode::OK);
        let sender = &body["streams"][0]["hops"][0];
        assert_eq!(sender["id"], "weave-basic-sender");
        assert_eq!(sender["egresses"].as_array().unwrap().len(), 1);
        assert_eq!(sender["egresses"][0]["condition"], "connecting");
    }

    #[tokio::test]
    async fn view_addresses_use_the_listener_of_the_named_network() {
        let (state, _mem) = mem_state();
        let on_lan = |id: &str, wan: &str, lan: &str| {
            let mut registration = node_registration(id, wan);
            let mut second = registration.node.topology.attachments[0].clone();
            second.id = "lan".to_string();
            second.network = "lan".to_string();
            second.listeners.srt.as_mut().unwrap().host = lan.to_string();
            registration.node.topology.attachments.push(second);
            registration
        };
        {
            let mut nodes = state.nodes.write().await;
            nodes.insert(
                "strom-node-1".to_string(),
                on_lan("strom-node-1", "172.26.0.10", "10.1.0.10"),
            );
            nodes.insert(
                "strom-node-2".to_string(),
                on_lan("strom-node-2", "172.27.0.10", "10.1.0.11"),
            );
            let mut definition = stream("basic");
            for endpoint in [
                &mut definition.source,
                &mut definition.destinations[0].endpoint,
            ] {
                if let StreamTransport::Srt(endpoint) = endpoint {
                    endpoint.network = Some("lan".to_string());
                }
            }
            state
                .streams
                .write()
                .await
                .insert("basic".to_string(), stored_stream(definition));
        }

        reconcile_tick(&state).await;

        let (_, body) = send(&open_router(state), "GET", "/view", None).await;
        let url = body["streams"][0]["endpoints"]["ingress"]["url"]
            .as_str()
            .unwrap_or_else(|| panic!("no ingress url: {body}"));
        assert!(url.starts_with("srt://10.1.0.10:"), "{url}");
        let output = body["streams"][0]["endpoints"]["destinations"][0]["endpoint"]["url"]
            .as_str()
            .unwrap_or_else(|| panic!("no output url: {body}"));
        assert!(output.starts_with("srt://10.1.0.11:"), "{output}");
    }

    #[tokio::test]
    async fn view_before_first_tick_has_no_report() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        let (status, body) = send(&app, "GET", "/view", None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("report").is_none());
        assert_eq!(body["streams"], json!([]));
    }

    #[tokio::test]
    async fn invalid_json_has_a_structured_error() {
        let (state, mem) = mem_state();
        let request = Request::builder()
            .method("POST")
            .uri("/streams")
            .header("content-type", "application/json")
            .body(Body::from("{"))
            .unwrap();

        let response = open_router(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let error: ApiError = serde_json::from_slice(&body).unwrap();
        assert_eq!(error.code, ApiErrorCode::InvalidJson);
        assert_eq!(error.details[0].field, "body");
        assert_eq!(mem.upsert_stream_calls(), 0);
    }

    #[tokio::test]
    async fn invalid_resource_paths_are_rejected() {
        let (state, mem) = mem_state();
        let app = open_router(state);

        for (method, uri, body) in [
            ("GET", "/streams/foo%3Fignored", None),
            ("DELETE", "/streams/foo%3Fignored", None),
            ("GET", "/streams/foo%3Fignored/endpoints", None),
            (
                "POST",
                "/nodes/foo%3Fignored/heartbeat",
                Some(json!({ "node_id": "foo?ignored", "status": "ready" })),
            ),
            ("GET", "/nodes/foo%3Fignored/desired", None),
        ] {
            let (status, _) = send(&app, method, uri, body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {uri}");
        }

        assert_eq!(mem.delete_stream_calls(), 0);
    }

    #[tokio::test]
    async fn view_reports_each_merge_input_and_ignores_stale_revisions() {
        let (state, _mem) = mem_state();
        two_nodes_and_basic(&state).await;
        reconcile_tick(&state).await;
        let receiver = {
            let mut view = state.view.write().await;
            let receiver = &mut view.hops.get_mut("basic").unwrap()[1];
            receiver.merge_ingress = Some(weave_core::SocketSpec::srt_listen(7100, 60));
            receiver.clone()
        };
        let report = serde_json::from_value(json!({
            "id": receiver.id,
            "node_id": receiver.node_id,
            "revision": receiver.revision(),
            "state": "provisioned",
            "ingress": {"condition": "stalled", "stats": {"rate_mbps": 0.0}},
            "merge_ingress": {"condition": "flowing", "stats": {"rate_mbps": 2.3}}
        }))
        .unwrap();
        state
            .nodes
            .write()
            .await
            .get_mut(&receiver.node_id)
            .unwrap()
            .hop_status = vec![report];
        let app = open_router(state.clone());

        let (status, body) = send(&app, "GET", "/view", None).await;
        assert_eq!(status, StatusCode::OK);
        let hop = &body["streams"][0]["hops"][1];
        assert_eq!(hop["ingress"]["condition"], "stalled");
        assert_eq!(hop["ingress"]["stats"]["rate_mbps"], 0.0);
        assert_eq!(hop["merge_ingress"]["condition"], "flowing");
        assert_eq!(hop["merge_ingress"]["stats"]["rate_mbps"], 2.3);
        assert_eq!(hop["merge_ingress"]["port"], 7100);

        state
            .nodes
            .write()
            .await
            .get_mut(&receiver.node_id)
            .unwrap()
            .hop_status[0]
            .revision = "stale".into();
        let (_, body) = send(&app, "GET", "/view", None).await;
        let hop = &body["streams"][0]["hops"][1];
        for input in ["ingress", "merge_ingress"] {
            assert!(hop[input].get("condition").is_none());
            assert!(hop[input].get("stats").is_none());
        }
    }

    #[tokio::test]
    async fn ui_is_served_at_root_and_ui() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        for uri in ["/", "/ui"] {
            let request = Request::builder()
                .method("GET")
                .uri(uri)
                .body(Body::empty())
                .unwrap();
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let content_type = response.headers()["content-type"].to_str().unwrap();
            assert!(content_type.starts_with("text/html"), "{content_type}");
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            assert!(String::from_utf8_lossy(&bytes).contains("open-weave"));
        }
    }

    pub(super) const NORTH_ROUTES: [(&str, &str); 9] = [
        ("GET", "/streams"),
        ("POST", "/streams"),
        ("POST", "/stream-plans"),
        ("GET", "/streams/basic"),
        ("DELETE", "/streams/basic"),
        ("GET", "/streams/basic/endpoints"),
        ("GET", "/stream-sets"),
        ("GET", "/stream-sets/studio-a"),
        ("PUT", "/stream-sets/studio-a"),
    ];

    pub(super) const SOUTH_ROUTES: [(&str, &str); 5] = [
        ("POST", "/nodes/register"),
        ("POST", "/nodes/strom-node-1/heartbeat"),
        ("GET", "/nodes/strom-node-1/desired"),
        ("GET", "/endpoints"),
        ("GET", "/state"),
    ];

    pub(super) const SHARED_READ_ROUTES: [(&str, &str); 1] = [("GET", "/nodes")];

    #[tokio::test]
    async fn versioned_api_paths_are_not_served() {
        let (state, _mem) = mem_state();
        let app = open_router(state);
        for retired_prefix in ["/v1", "/v2", "/v3", "/v4", "/v5", "/v6"] {
            for (method, uri) in NORTH_ROUTES
                .iter()
                .chain(&SOUTH_ROUTES)
                .chain(&SHARED_READ_ROUTES)
            {
                let retired = format!("{retired_prefix}{uri}");
                let (status, _) = send(&app, method, &retired, None).await;
                assert_eq!(status, StatusCode::NOT_FOUND, "{retired} must stay retired");
            }
            let (status, _) = send(&app, "GET", &format!("{retired_prefix}/status"), None).await;
            assert_eq!(status, StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn status_distinguishes_current_and_observed_generations() {
        let (state, _mem) = mem_state();
        let app = open_router(state.clone());
        let definition = stream("basic");
        let (_, headers, _) = send_with_headers(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(&definition).unwrap()),
            &[("if-none-match", "*")],
        )
        .await;
        let etag = response_etag(&headers);
        reconcile_tick(&state).await;
        {
            let view = state.view.read().await;
            assert_eq!(view.streams[0].generation, 1);
            assert_eq!(view.streams[0].observed_generation, Some(1));
            assert_eq!(view.streams[0].conditions.len(), 5);
            for condition in &view.streams[0].conditions {
                time::OffsetDateTime::parse(
                    &condition.last_transition_time,
                    &time::format_description::well_known::Rfc3339,
                )
                .unwrap();
            }
        }

        let mut changed = definition;
        changed.enabled = false;
        let (status, _, _) = send_with_headers(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(&changed).unwrap()),
            &[("if-match", &etag)],
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        {
            let view = state.view.read().await;
            assert_eq!(view.streams[0].generation, 2);
            assert_eq!(view.streams[0].observed_generation, Some(1));
        }

        reconcile_tick(&state).await;
        let view = state.view.read().await;
        assert_eq!(view.streams[0].generation, 2);
        assert_eq!(view.streams[0].observed_generation, Some(2));
        assert_eq!(
            view.streams[0].conditions[0].reason,
            StreamConditionReason::Disabled
        );
    }

    #[test]
    fn condition_transition_time_changes_only_when_status_changes() {
        let mut previous = StreamStatus {
            format_policy: weave_core::FormatPolicy::Report,
            name: "basic".to_string(),
            generation: 1,
            observed_generation: Some(1),
            status: PathStatus::Pending,
            nodes: Vec::new(),
            conditions: vec![stream_condition(
                StreamConditionType::PlacementReady,
                StreamConditionStatus::False,
                StreamConditionReason::PlacementFailed,
                "missing node",
            )],
            ingress: None,
            destinations: Vec::new(),
        };
        previous.conditions[0].last_transition_time = "2026-09-18T10:00:00Z".to_string();
        let mut current = previous.clone();
        current.conditions[0].reason = StreamConditionReason::Disabled;
        stamp_condition_transition_times(
            std::slice::from_mut(&mut current),
            std::slice::from_ref(&previous),
            "2026-09-18T10:01:00Z",
        );
        assert_eq!(
            current.conditions[0].last_transition_time,
            "2026-09-18T10:00:00Z"
        );

        current.conditions[0].status = StreamConditionStatus::True;
        stamp_condition_transition_times(
            std::slice::from_mut(&mut current),
            std::slice::from_ref(&previous),
            "2026-09-18T10:02:00Z",
        );
        assert_eq!(
            current.conditions[0].last_transition_time,
            "2026-09-18T10:02:00Z"
        );
    }

    #[tokio::test]
    async fn invalid_stream_is_rejected_before_persistence() {
        let (state, mem) = mem_state();
        let app = open_router(state);
        let mut invalid = stream("invalid");
        let StreamTransport::Srt(destination) = &mut invalid.destinations[0].endpoint else {
            unreachable!()
        };
        destination.remote = Some(weave_core::RemoteAddr {
            host: "example.test".to_string(),
            port: 9000,
            network: "internet".to_string(),
        });

        let (status, body) = send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(&invalid).unwrap()),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_request");
        assert_eq!(body["details"][0]["field"], "destinations[0].srt");
        assert_eq!(body["details"][0]["code"], "mutually_exclusive");
        assert_eq!(mem.upsert_stream_calls(), 0);

        let (status, body) = send(
            &app,
            "POST",
            "/stream-plans",
            Some(serde_json::to_value(invalid).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_request");
        assert_eq!(mem.upsert_stream_calls(), 0);

        let (_, body) = send(&app, "GET", "/streams", None).await;
        let listed: Vec<StreamResource> = serde_json::from_value(body).unwrap();
        assert!(listed.is_empty());
    }

    fn with_port_range(
        mut registration: NodeRegistration,
        start: u16,
        end: u16,
    ) -> NodeRegistration {
        registration.node.topology.attachments[0]
            .listeners
            .srt
            .as_mut()
            .expect("an SRT listener")
            .port_range = PortRange { start, end };
        registration
    }

    #[tokio::test]
    async fn plan_places_without_changing_desired_state() {
        let (state, mem) = mem_state();
        let app = open_router(state.clone());
        for registration in [
            node_registration("strom-node-1", "172.26.0.10"),
            node_registration("strom-node-2", "172.27.0.10"),
        ] {
            let (status, _) = send(
                &app,
                "POST",
                "/nodes/register",
                Some(serde_json::to_value(registration).unwrap()),
            )
            .await;
            assert_eq!(status, StatusCode::ACCEPTED);
        }

        let (status, body) = send(
            &app,
            "POST",
            "/stream-plans",
            Some(serde_json::to_value(stream("preview")).unwrap()),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let plan: StreamPlan = serde_json::from_value(body).unwrap();
        assert_eq!(plan.status, PlanStatus::Placed);
        assert_eq!(plan.nodes, ["strom-node-1", "strom-node-2"]);
        assert_eq!(plan.hops.len(), 2);
        assert!(plan.endpoints.is_some());
        assert_eq!(mem.upsert_stream_calls(), 0);
        assert!(state.streams.read().await.is_empty());
        assert!(state.desired.read().await.is_empty());
        assert!(state.view.read().await.streams.is_empty());
    }

    #[tokio::test]
    async fn plan_distinguishes_unplaced_and_disabled_streams() {
        let (state, mem) = mem_state();
        let app = open_router(state);

        let (status, body) = send(
            &app,
            "POST",
            "/stream-plans",
            Some(serde_json::to_value(stream("unplaced")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let plan: StreamPlan = serde_json::from_value(body).unwrap();
        assert_eq!(plan.status, PlanStatus::Unplaced);
        assert!(plan.hops.is_empty());
        assert!(plan.reason.unwrap().contains("not registered"));

        let mut disabled = stream("disabled");
        disabled.enabled = false;
        let (status, body) = send(
            &app,
            "POST",
            "/stream-plans",
            Some(serde_json::to_value(disabled).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let plan: StreamPlan = serde_json::from_value(body).unwrap();
        assert_eq!(plan.status, PlanStatus::Disabled);
        assert!(plan.hops.is_empty());
        assert!(plan.reason.is_none());
        assert_eq!(mem.upsert_stream_calls(), 0);
    }

    #[tokio::test]
    async fn a_relay_that_misses_one_desired_fetch_keeps_its_bridge() {
        let bridge = "weave-feed-bridge-studio-0";
        let (state, _mem) = mem_state();
        let app = open_router(state.clone());
        let register = |registration: NodeRegistration| {
            let app = app.clone();
            async move {
                let (status, _) = send(
                    &app,
                    "POST",
                    "/nodes/register",
                    Some(serde_json::to_value(registration).unwrap()),
                )
                .await;
                assert_eq!(status, StatusCode::ACCEPTED);
            }
        };
        register(nat_registration("nat-1", "192.168.0.10")).await;
        register(nat_registration("nat-2", "192.168.0.20")).await;
        register(node_registration("relay-b", "198.51.100.20")).await;
        state.streams.write().await.insert(
            "feed".to_string(),
            stored_stream(stream_between("feed", "nat-1", "nat-2")),
        );
        reconcile_tick(&state).await;
        let (_, body) = desired_hops(&app, "relay-b").await;
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(hops[0].id, bridge);
        register(node_registration("relay-a", "198.51.100.10")).await;

        let heartbeat = |hop_status: Vec<weave_core::HopStatus>| NodeHeartbeat {
            inputs: Vec::new(),
            outputs: Vec::new(),
            node_id: "relay-b".to_string(),
            status: NodeStatus::Ready,
            observation: None,
            endpoints: Vec::new(),
            hop_status,
        };
        let running = hops
            .iter()
            .map(|hop| weave_core::HopStatus {
                delivery: None,
                id: hop.id.clone(),
                node_id: hop.node_id.clone(),
                revision: hop.revision(),
                state: weave_core::HopState::Provisioned,
                ingress: weave_core::SocketStatus {
                    peer: None,
                    condition: weave_core::LinkCondition::Flowing,
                    resolved: None,
                    stats: None,
                },
                merge_ingress: None,
                egresses: hop
                    .egresses
                    .iter()
                    .map(|egress| weave_core::EgressStatus {
                        branch_id: egress.branch_id.clone(),
                        status: weave_core::SocketStatus {
                            peer: None,
                            condition: weave_core::LinkCondition::Flowing,
                            resolved: None,
                            stats: None,
                        },
                    })
                    .collect(),
            })
            .collect();
        let relay_of_bridge = |state: AppState| async move {
            reconcile_tick(&state).await;
            state.view.read().await.hops["feed"]
                .iter()
                .find(|hop| hop.id == bridge)
                .unwrap()
                .node_id
                .clone()
        };

        for (hop_status, relay) in [(running, "relay-b"), (Vec::new(), "relay-a")] {
            let (status, _) = send(
                &app,
                "POST",
                "/nodes/relay-b/heartbeat",
                Some(serde_json::to_value(heartbeat(hop_status)).unwrap()),
            )
            .await;
            assert_eq!(status, StatusCode::ACCEPTED);
            assert_eq!(relay_of_bridge(state.clone()).await, relay);
        }
    }

    #[tokio::test]
    async fn a_plan_keeps_a_bridge_on_the_relay_that_reports_running_it() {
        let bridge = "weave-feed-bridge-studio-0";
        let plan_relay = |relay_b: NodeRegistration| async move {
            let (state, _mem) = mem_state();
            let app = open_router(state);
            for registration in [
                nat_registration("nat-1", "192.168.0.10"),
                nat_registration("nat-2", "192.168.0.20"),
                node_registration("relay-a", "198.51.100.10"),
                relay_b,
            ] {
                let (status, _) = send(
                    &app,
                    "POST",
                    "/nodes/register",
                    Some(serde_json::to_value(registration).unwrap()),
                )
                .await;
                assert_eq!(status, StatusCode::ACCEPTED);
            }
            let (_, body) = send(
                &app,
                "POST",
                "/stream-plans",
                Some(serde_json::to_value(stream_between("feed", "nat-1", "nat-2")).unwrap()),
            )
            .await;
            let plan: StreamPlan = serde_json::from_value(body).unwrap();
            plan.hops
                .iter()
                .find(|hop| hop.id == bridge)
                .map(|hop| hop.node_id.clone())
        };

        let idle = node_registration("relay-b", "198.51.100.20");
        assert_eq!(plan_relay(idle.clone()).await.as_deref(), Some("relay-a"));

        let mut running = idle;
        running.hop_status = vec![weave_core::HopStatus {
            delivery: None,
            id: bridge.to_string(),
            node_id: "relay-b".to_string(),
            revision: String::new(),
            state: weave_core::HopState::Provisioned,
            ingress: weave_core::SocketStatus {
                peer: None,
                condition: weave_core::LinkCondition::Flowing,
                resolved: None,
                stats: None,
            },
            merge_ingress: None,
            egresses: Vec::new(),
        }];
        assert_eq!(plan_relay(running).await.as_deref(), Some("relay-b"));
    }

    #[tokio::test]
    async fn plan_allocates_ports_alongside_existing_streams() {
        let (state, mem) = mem_state();
        let app = open_router(state);

        for registration in [
            with_port_range(node_registration("strom-node-1", "172.26.0.10"), 7000, 7000),
            with_port_range(node_registration("strom-node-2", "172.27.0.10"), 7000, 7001),
        ] {
            send(
                &app,
                "POST",
                "/nodes/register",
                Some(serde_json::to_value(registration).unwrap()),
            )
            .await;
        }
        let (_, body) = send(
            &app,
            "POST",
            "/stream-plans",
            Some(serde_json::to_value(stream("preview")).unwrap()),
        )
        .await;
        let plan: StreamPlan = serde_json::from_value(body).unwrap();
        assert_eq!(plan.status, PlanStatus::Placed, "alone, the preview fits");

        send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("existing")).unwrap()),
        )
        .await;

        let (status, body) = send(
            &app,
            "POST",
            "/stream-plans",
            Some(serde_json::to_value(stream("preview")).unwrap()),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let plan: StreamPlan = serde_json::from_value(body).unwrap();
        assert_eq!(plan.status, PlanStatus::Unplaced);
        assert!(plan.reason.unwrap().contains("no free port"));
        assert_eq!(mem.upsert_stream_calls(), 1, "plan did not write a stream");
        let (_, body) = send(&app, "GET", "/streams", None).await;
        let streams: Vec<StreamResource> = serde_json::from_value(body).unwrap();
        assert_eq!(streams[0].spec, stream("existing"));
    }

    #[tokio::test]
    async fn hydration_refuses_invalid_persisted_streams() {
        let mem = Arc::new(MemStore::new());
        let mut invalid = stream("broken");
        invalid.destinations.clear();
        mem.create_stream(&invalid).await.unwrap();

        let error = AppState::hydrate(
            mem,
            Duration::from_secs(15),
            Duration::from_secs(300),
            None,
            LinkKeys::for_tests(),
        )
        .await
        .err()
        .expect("invalid stored stream must prevent startup");

        assert_eq!(
            error.to_string(),
            "stored stream \"broken\" is invalid at destinations: stream must have at least one destination"
        );
    }

    #[tokio::test]
    async fn hydration_drops_persisted_nodes_with_invalid_ids() {
        let mem = Arc::new(MemStore::new());
        let registration = node_registration("node/one", "172.26.0.10");
        mem.upsert_node(&registration).await.unwrap();

        let state = AppState::hydrate(
            mem,
            Duration::from_secs(15),
            Duration::from_secs(300),
            None,
            LinkKeys::for_tests(),
        )
        .await
        .expect("invalid cached nodes must not prevent startup");

        assert!(state.nodes.read().await.is_empty());
    }

    #[tokio::test]
    async fn a_heartbeat_writes_the_store_only_when_its_reports_change() {
        let (state, mem) = mem_state();
        let app = open_router(state);
        send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(node_registration("strom-node-1", "172.26.0.10")).unwrap()),
        )
        .await;
        assert_eq!(mem.upsert_node_calls(), 1);

        let socket = |condition, rate_mbps| weave_core::SocketStatus {
            peer: None,
            condition,
            resolved: None,
            stats: Some(weave_core::LinkStats {
                rate_mbps,
                ..weave_core::LinkStats::default()
            }),
        };
        let hop = |condition, rate_mbps| HopStatus {
            delivery: None,
            id: "weave-basic-sender".to_string(),
            node_id: "strom-node-1".to_string(),
            revision: String::new(),
            state: weave_core::HopState::Provisioned,
            ingress: socket(condition, rate_mbps),
            merge_ingress: None,
            egresses: vec![weave_core::EgressStatus {
                branch_id: "studio".to_string(),
                status: socket(condition, rate_mbps),
            }],
        };
        let heartbeat = |status, hop_status| NodeHeartbeat {
            inputs: Vec::new(),
            outputs: Vec::new(),
            node_id: "strom-node-1".to_string(),
            status,
            observation: None,
            endpoints: Vec::new(),
            hop_status,
        };
        for (step, beat, writes) in [
            ("unchanged", heartbeat(NodeStatus::Ready, Vec::new()), 1),
            ("new status", heartbeat(NodeStatus::Degraded, Vec::new()), 2),
            (
                "new hop",
                heartbeat(
                    NodeStatus::Degraded,
                    vec![hop(weave_core::LinkCondition::Flowing, 2.5)],
                ),
                3,
            ),
            (
                "new rate only",
                heartbeat(
                    NodeStatus::Degraded,
                    vec![hop(weave_core::LinkCondition::Flowing, 2.7)],
                ),
                3,
            ),
            (
                "new condition",
                heartbeat(
                    NodeStatus::Degraded,
                    vec![hop(weave_core::LinkCondition::Stalled, 0.0)],
                ),
                4,
            ),
            (
                "new revision only",
                heartbeat(
                    NodeStatus::Degraded,
                    vec![HopStatus {
                        revision: "new-revision".to_string(),
                        ..hop(weave_core::LinkCondition::Stalled, 0.0)
                    }],
                ),
                5,
            ),
        ] {
            let (status, _) = send(
                &app,
                "POST",
                "/nodes/strom-node-1/heartbeat",
                Some(serde_json::to_value(&beat).unwrap()),
            )
            .await;
            assert_eq!(status, StatusCode::ACCEPTED, "{step}");
            assert_eq!(mem.upsert_node_calls(), writes, "{step}");
        }

        let stored = mem.load_nodes().await.unwrap();
        assert_eq!(stored[0].hop_status[0].revision, "new-revision");
        assert_eq!(stored[0].node.status, NodeStatus::Degraded);
        assert_eq!(
            stored[0].hop_status[0].ingress.condition,
            weave_core::LinkCondition::Stalled
        );
        let (_, body) = send(&app, "GET", "/state", None).await;
        let observed: ObservedState = serde_json::from_value(body).unwrap();
        assert_eq!(
            observed.hops[0].ingress.condition,
            weave_core::LinkCondition::Stalled
        );
    }

    #[tokio::test]
    async fn registration_with_an_incompatible_protocol_version_is_rejected() {
        let (state, mem) = mem_state();
        let app = open_router(state);

        for reported in [0, PROTOCOL_VERSION + 1] {
            let mut registration = node_registration("strom-node-1", "172.26.0.10");
            registration.protocol_version = reported;

            let (status, body) = send(
                &app,
                "POST",
                "/nodes/register",
                Some(serde_json::to_value(&registration).unwrap()),
            )
            .await;

            assert_eq!(status, StatusCode::CONFLICT, "reported version {reported}");
            assert_eq!(body["code"], "incompatible_protocol_version");
            assert_eq!(body["details"][0]["field"], "protocol_version");
            assert_eq!(
                body["details"][0]["message"],
                format!("reported {reported}; supported {PROTOCOL_VERSION}")
            );
        }

        assert_eq!(
            mem.upsert_node_calls(),
            0,
            "a rejected node is never persisted"
        );
        let (_, body) = send(&app, "GET", "/nodes", None).await;
        assert_eq!(body, json!([]), "a rejected node is never registered");
    }

    #[tokio::test]
    async fn registration_with_an_invalid_node_id_is_rejected() {
        let (state, mem) = mem_state();
        let app = open_router(state);
        let registration = node_registration("node/one", "172.26.0.10");

        let (status, body) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(&registration).unwrap()),
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_request");
        assert_eq!(body["details"][0]["field"], "node.id");
        assert_eq!(body["details"][0]["code"], "invalid_characters");
        assert_eq!(mem.upsert_node_calls(), 0);
        let (_, body) = send(&app, "GET", "/nodes", None).await;
        assert_eq!(body, json!([]));

        let mut registration = node_registration("node-one", "172.26.0.10");
        registration.endpoints.push(EndpointDescriptor {
            id: "capture-1".to_string(),
            label: "Capture".to_string(),
            node_id: Some("node/one".to_string()),
            kind: weave_core::EndpointKind::Source,
            transports: Vec::new(),
            metadata: Value::Null,
        });
        let (status, body) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(&registration).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "invalid_request");
        assert_eq!(body["details"][0]["field"], "endpoints[0].node_id");
        assert_eq!(mem.upsert_node_calls(), 0);
    }

    #[tokio::test]
    async fn node_cannot_report_another_nodes_hop_status() {
        let (state, mem) = mem_state();
        let app = open_router(state);
        let status_for = |node_id: &str| weave_core::HopStatus {
            delivery: None,
            id: "weave-basic-sender".to_string(),
            node_id: node_id.to_string(),
            revision: String::new(),
            state: weave_core::HopState::Provisioned,
            ingress: weave_core::SocketStatus {
                peer: None,
                condition: weave_core::LinkCondition::Flowing,
                resolved: None,
                stats: None,
            },
            merge_ingress: None,
            egresses: Vec::new(),
        };

        let mut registration = node_registration("strom-node-1", "172.26.0.10");
        registration.hop_status = vec![status_for("strom-node-2")];
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(&registration).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(mem.upsert_node_calls(), 0);

        registration.hop_status.clear();
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(&registration).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let heartbeat = NodeHeartbeat {
            inputs: Vec::new(),
            outputs: Vec::new(),
            node_id: "strom-node-1".to_string(),
            status: NodeStatus::Ready,
            observation: None,
            endpoints: Vec::new(),
            hop_status: vec![status_for("strom-node-2")],
        };
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/strom-node-1/heartbeat",
            Some(serde_json::to_value(&heartbeat).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn a_browser_endpoint_is_stored_verbatim_and_never_dialled() {
        let (state, _mem) = mem_state();
        let app = open_router(state.clone());

        let mut browser = node_registration("browser-a1b2", "browser");
        browser.node.endpoint = "browser://browser-a1b2".to_string();
        browser.node.inputs = vec![weave_core::NodeInput {
            id: "camera".to_string(),
            label: "Camera".to_string(),
            tracks: vec![weave_core::Track::Audio, weave_core::Track::Video],
        }];
        browser.node.capabilities.hop_profiles = vec![HopProfile {
            timed_delivery: None,
            id: "camera-to-whip".to_string(),
            ingress: HopEndpointClass::Local(LocalEnd::Input),
            egress: HopEndpointClass::Transport(TransportClass {
                transport: Transport::Whip,
                roles: RoleSet::only(weave_core::SocketRole::Connect),
            }),
            max_egresses: Some(1),
            media_behavior: weave_core::MediaBehavior::Unknown,
            merge: false,
            accepts: None,
        }];
        browser.node.topology.attachments[0].listeners = NetworkListeners::default();
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(&browser).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let mut strom = node_registration("strom-node-2", "172.27.0.10");
        strom.node.capabilities.hop_profiles.push(HopProfile {
            timed_delivery: None,
            id: "whip-to-srt".to_string(),
            ingress: HopEndpointClass::Transport(TransportClass {
                transport: Transport::Whip,
                roles: RoleSet::only(weave_core::SocketRole::Listen),
            }),
            egress: HopEndpointClass::Transport(TransportClass {
                transport: Transport::Srt,
                roles: RoleSet::both(),
            }),
            max_egresses: None,
            media_behavior: weave_core::MediaBehavior::Unknown,
            merge: false,
            accepts: None,
        });
        strom.node.topology.attachments[0].listeners.whip = Some(weave_core::SignallingListener {
            base_url: "http://172.27.0.10:8080/whip".to_string(),
        });
        send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(&strom).unwrap()),
        )
        .await;

        let mut cam = stream("alice-cam");
        cam.source = StreamTransport::Input(weave_core::LocalEndpoint {
            node: "browser-a1b2".to_string(),
            id: "camera".to_string(),
            network: None,
        });
        state
            .streams
            .write()
            .await
            .insert(cam.name.clone(), stored_stream(cam));

        reconcile_tick(&state).await;

        let (_, body) = send(&app, "GET", "/nodes", None).await;
        let nodes: Vec<NodeResource> = serde_json::from_value(body).unwrap();
        let page = nodes.iter().find(|n| n.node.id == "browser-a1b2").unwrap();
        assert_eq!(page.node.endpoint, "browser://browser-a1b2");

        let (status, body) = send(&app, "GET", "/nodes/browser-a1b2/desired", None).await;
        assert_eq!(status, StatusCode::OK);
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert_eq!(hops.len(), 1);
        assert_eq!(
            hops[0].ingress,
            weave_core::SocketSpec::input("camera"),
            "the page's own camera feeds the hop"
        );
        assert!(
            matches!(hops[0].egresses[0].socket, weave_core::SocketSpec::Whip(_)),
            "the page pushes to the Strom's ingest: {:?}",
            hops[0].egresses[0]
        );
    }

    fn local_node(inputs: Value) -> Value {
        let mut registration =
            serde_json::to_value(node_registration("alfreds-mac", "127.0.0.1")).unwrap();
        registration["node"]["capabilities"]["hop_profiles"] = json!([
            {
                "id": "app-audio-to-srt",
                "ingress": "input",
                "egress": { "transport": "srt", "roles": ["listen", "connect"] }
            },
            {
                "id": "srt-to-speaker",
                "ingress": { "transport": "srt", "roles": ["listen", "connect"] },
                "egress": "output"
            }
        ]);
        registration["node"]["inputs"] = inputs;
        registration["node"]["outputs"] = json!([{ "id": "speakers", "label": "Speakers" }]);
        registration
    }

    fn music() -> Value {
        json!([{ "id": "music", "label": "Music", "tracks": ["audio"] }])
    }

    async fn local_heartbeat(app: &Router, inputs: Value) -> StatusCode {
        send(
            app,
            "POST",
            "/nodes/alfreds-mac/heartbeat",
            Some(json!({
                "node_id": "alfreds-mac",
                "status": "ready",
                "inputs": inputs,
                "outputs": [{ "id": "speakers", "label": "Speakers" }],
                "endpoints": [],
                "hop_status": []
            })),
        )
        .await
        .0
    }

    async fn placement(app: &Router) -> (Value, Vec<DesiredHop>) {
        let status = get_ok(app, "/status").await;
        let condition = status["streams"][0]["conditions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|condition| condition["type"] == "placement_ready")
            .cloned()
            .unwrap();
        let (_, body) = send(app, "GET", "/nodes/alfreds-mac/desired", None).await;
        (condition, serde_json::from_value(body).unwrap())
    }

    #[tokio::test]
    async fn a_stream_follows_its_input_as_heartbeats_add_and_remove_it() {
        let (state, _mem) = mem_state();
        let app = open_router(state.clone());
        let (status, _) = send(&app, "POST", "/nodes/register", Some(local_node(music()))).await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let mut spec = stream("music-to-speakers");
        spec.source = StreamTransport::Input(weave_core::LocalEndpoint {
            node: "alfreds-mac".to_string(),
            id: "music".to_string(),
            network: None,
        });
        spec.destinations[0].endpoint = StreamTransport::Output(weave_core::LocalEndpoint {
            node: "alfreds-mac".to_string(),
            id: "speakers".to_string(),
            network: None,
        });
        state
            .streams
            .write()
            .await
            .insert(spec.name.clone(), stored_stream(spec));

        reconcile_tick(&state).await;
        let (condition, hops) = placement(&app).await;
        assert_eq!(condition["status"], "true");
        assert_eq!(hops.len(), 2);
        assert_eq!(hops[0].ingress, weave_core::SocketSpec::input("music"));
        assert_eq!(hops[0].tracks, Some(vec![weave_core::Track::Audio]));
        assert_eq!(
            hops[1].egresses[0].socket,
            weave_core::SocketSpec::output("speakers")
        );

        assert_eq!(local_heartbeat(&app, json!([])).await, StatusCode::ACCEPTED);
        reconcile_tick(&state).await;
        let (condition, hops) = placement(&app).await;
        assert_eq!(condition["status"], "false");
        assert_eq!(condition["reason"], "input_missing");
        assert_eq!(
            condition["detail"],
            "node alfreds-mac declares no input music"
        );
        assert!(
            hops.is_empty(),
            "the hops leave desired state with the input"
        );

        assert_eq!(local_heartbeat(&app, music()).await, StatusCode::ACCEPTED);
        reconcile_tick(&state).await;
        let (condition, hops) = placement(&app).await;
        assert_eq!(condition["status"], "true");
        assert_eq!(hops.len(), 2);
    }

    #[tokio::test]
    async fn a_heartbeat_with_invalid_inputs_is_refused_and_changes_nothing() {
        let (state, _mem) = mem_state();
        let app = open_router(state.clone());
        send(&app, "POST", "/nodes/register", Some(local_node(music()))).await;

        let invalid = json!([{ "id": "Music", "label": " ", "tracks": [] }]);
        assert_eq!(
            local_heartbeat(&app, invalid).await,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            state.nodes.read().await["alfreds-mac"].node.inputs[0].id,
            "music"
        );
    }

    #[test]
    fn mark_offline_marks_stale_preserves_fresh_and_spares_exact_ttl() {
        let mut nodes = BTreeMap::from([
            (
                "stale".to_string(),
                node_registration("stale", "172.26.0.10"),
            ),
            (
                "fresh".to_string(),
                node_registration("fresh", "172.27.0.10"),
            ),
            ("edge".to_string(), node_registration("edge", "172.28.0.10")),
        ]);
        nodes.get_mut("fresh").unwrap().node.status = NodeStatus::Degraded;

        let ttl = Duration::from_secs(15);
        let now = Instant::now();
        let last_seen = BTreeMap::from([
            ("stale".to_string(), now - Duration::from_secs(20)),
            ("fresh".to_string(), now - Duration::from_secs(5)),
            ("edge".to_string(), now - ttl),
        ]);

        mark_offline(&mut nodes, &last_seen, now, ttl);

        assert_eq!(nodes["stale"].node.status, NodeStatus::Offline);
        assert_eq!(
            nodes["fresh"].node.status,
            NodeStatus::Degraded,
            "fresh node keeps its reported status"
        );
        assert_eq!(
            nodes["edge"].node.status,
            NodeStatus::Ready,
            "age == ttl is not yet offline"
        );
    }

    #[test]
    fn mark_offline_reports_only_the_nodes_it_transitioned() {
        let mut nodes = BTreeMap::from([
            (
                "stale".to_string(),
                node_registration("stale", "172.26.0.10"),
            ),
            (
                "fresh".to_string(),
                node_registration("fresh", "172.27.0.10"),
            ),
        ]);

        let ttl = Duration::from_secs(15);
        let now = Instant::now();
        let last_seen = BTreeMap::from([
            ("stale".to_string(), now - Duration::from_secs(20)),
            ("fresh".to_string(), now - Duration::from_secs(5)),
        ]);

        assert_eq!(
            mark_offline(&mut nodes, &last_seen, now, ttl),
            vec!["stale".to_string()]
        );
        assert!(
            mark_offline(&mut nodes, &last_seen, now, ttl).is_empty(),
            "a node already offline does not transition again"
        );
    }

    fn webhook_state(sink: &Sink) -> AppState {
        let (mut state, _mem) = mem_state();
        state.webhooks = webhook::Emitter::new(webhook::Config {
            url: Some(sink.url.clone()),
            ..webhook::Config::default()
        })
        .map(Arc::new);
        state
    }

    #[tokio::test]
    async fn registering_a_node_emits_node_registered() {
        let mut sink = sink(StatusCode::OK).await;
        let app = open_router(webhook_state(&sink));

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(node_registration("guest-1", "172.26.0.10")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let event = sink.next().await.event;
        assert_eq!(event.event_type, EventType::NodeRegistered);
        let Subject::Node(node) = event.subject else {
            panic!("expected a node event");
        };
        assert_eq!(node.id, "guest-1");
        assert_eq!(node.endpoint, "http://guest-1:8080");
        assert_eq!(node.status, NodeStatus::Ready);
    }

    #[tokio::test]
    async fn a_node_past_its_ttl_emits_node_offline_once() {
        let mut sink = sink(StatusCode::OK).await;
        let state = webhook_state(&sink);
        state.nodes.write().await.insert(
            "guest-1".to_string(),
            node_registration("guest-1", "172.26.0.10"),
        );
        state.last_seen.write().await.insert(
            "guest-1".to_string(),
            Instant::now() - Duration::from_secs(60),
        );

        reconcile_tick(&state).await;
        reconcile_tick(&state).await;

        let event = sink.next().await.event;
        assert_eq!(event.event_type, EventType::NodeOffline);
        let Subject::Node(node) = event.subject else {
            panic!("expected a node event");
        };
        assert_eq!(node.id, "guest-1");
        assert_eq!(node.status, NodeStatus::Offline);
        sink.expect_idle().await;
    }

    #[tokio::test]
    async fn a_heartbeat_from_an_offline_node_emits_node_online() {
        let mut sink = sink(StatusCode::OK).await;
        let state = webhook_state(&sink);
        let mut registration = node_registration("guest-1", "172.26.0.10");
        registration.node.status = NodeStatus::Offline;
        state
            .nodes
            .write()
            .await
            .insert("guest-1".to_string(), registration);
        let app = open_router(state);

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/guest-1/heartbeat",
            Some(json!({ "node_id": "guest-1", "status": "ready", "inputs": [], "outputs": [] })),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let event = sink.next().await.event;
        assert_eq!(event.event_type, EventType::NodeOnline);
        let Subject::Node(node) = event.subject else {
            panic!("expected a node event");
        };
        assert_eq!(node.status, NodeStatus::Ready);
    }

    #[tokio::test]
    async fn a_heartbeat_from_a_live_node_emits_nothing() {
        let mut sink = sink(StatusCode::OK).await;
        let state = webhook_state(&sink);
        state.nodes.write().await.insert(
            "guest-1".to_string(),
            node_registration("guest-1", "172.26.0.10"),
        );
        let app = open_router(state);

        send(
            &app,
            "POST",
            "/nodes/guest-1/heartbeat",
            Some(json!({ "node_id": "guest-1", "status": "ready", "inputs": [], "outputs": [] })),
        )
        .await;

        sink.expect_idle().await;
    }

    #[tokio::test]
    async fn registration_is_accepted_while_the_receiver_refuses_connections() {
        let (mut state, _mem) = mem_state();
        state.webhooks = webhook::Emitter::new(webhook::Config {
            url: Some("http://192.0.2.1:1/hook".to_string()),
            timeout: Duration::from_millis(50),
            ..webhook::Config::default()
        })
        .map(Arc::new);
        let app = open_router(state);

        let accepted = tokio::time::timeout(
            Duration::from_secs(2),
            send(
                &app,
                "POST",
                "/nodes/register",
                Some(serde_json::to_value(node_registration("guest-1", "172.26.0.10")).unwrap()),
            ),
        )
        .await
        .expect("registration must not wait on the webhook receiver");

        assert_eq!(accepted.0, StatusCode::ACCEPTED);
    }

    #[test]
    fn reconcile_degrades_stream_when_a_hop_node_is_offline() {
        let mut nodes = BTreeMap::from([
            (
                "strom-node-1".to_string(),
                node_registration("strom-node-1", "172.26.0.10"),
            ),
            (
                "strom-node-2".to_string(),
                node_registration("strom-node-2", "172.27.0.10"),
            ),
        ]);
        nodes.get_mut("strom-node-1").unwrap().node.status = NodeStatus::Offline;
        let observed = observed_state(&nodes);

        let mut definition = stream("basic");
        let StreamTransport::Srt(source) = &mut definition.source else {
            unreachable!()
        };
        source.format = Some(weave_core::MediaFormat {
            container: weave_core::Container::MpegTs,
            video: None,
            audio: None,
        });
        let StreamTransport::Srt(destination) = &mut definition.destinations[0].endpoint else {
            unreachable!()
        };
        destination.accepts = Some(weave_core::FormatConstraint {
            container: Some(vec![weave_core::Container::Rtp]),
            ..Default::default()
        });
        let outcome = reconcile(vec![definition], &observed, &LinkKeys::for_tests());

        let basic = outcome.streams.iter().find(|s| s.name == "basic").unwrap();
        assert_eq!(basic.status, PathStatus::Degraded);
        let nodes = basic
            .conditions
            .iter()
            .find(|condition| condition.condition_type == StreamConditionType::NodesAvailable)
            .unwrap();
        assert_eq!(nodes.reason, StreamConditionReason::NodeOffline);
        assert_eq!(nodes.detail, "node strom-node-1 is offline");
        let format = basic
            .conditions
            .iter()
            .find(|condition| condition.condition_type == StreamConditionType::FormatCompatible)
            .unwrap();
        assert_eq!(format.status, StreamConditionStatus::False);
        assert_eq!(format.reason, StreamConditionReason::FormatMismatch);
        assert!(
            !outcome.desired_by_node["strom-node-1"].is_empty(),
            "desired hops for the offline node are still computed"
        );
    }

    #[test]
    fn reconcile_reports_why_a_stream_is_pending() {
        let nodes = BTreeMap::from([(
            "strom-node-1".to_string(),
            node_registration("strom-node-1", "172.26.0.10"),
        )]);
        let observed = observed_state(&nodes);

        let outcome = reconcile(vec![stream("basic")], &observed, &LinkKeys::for_tests());

        let basic = outcome.streams.iter().find(|s| s.name == "basic").unwrap();
        assert_eq!(basic.status, PathStatus::Pending);
        let placement = basic
            .conditions
            .iter()
            .find(|condition| condition.condition_type == StreamConditionType::PlacementReady)
            .unwrap();
        assert_eq!(placement.reason, StreamConditionReason::PlacementFailed);
        assert_eq!(placement.detail, "node strom-node-2 is not registered");
    }

    #[test]
    fn reconcile_replans_a_stream_off_an_offline_relay() {
        let mut nodes = BTreeMap::from([
            (
                "strom-node-1".to_string(),
                nat_registration("strom-node-1", "172.26.0.10"),
            ),
            (
                "strom-node-2".to_string(),
                nat_registration("strom-node-2", "172.27.0.10"),
            ),
            (
                "relay-a".to_string(),
                node_registration("relay-a", "198.51.100.10"),
            ),
            (
                "relay-b".to_string(),
                node_registration("relay-b", "198.51.100.20"),
            ),
        ]);
        let observed = observed_state(&nodes);
        let outcome = reconcile(vec![stream("basic")], &observed, &LinkKeys::for_tests());
        let basic = outcome.streams.iter().find(|s| s.name == "basic").unwrap();
        assert!(
            basic.nodes.contains(&"relay-a".to_string()),
            "the lowest-id relay carries the stream while it is online: {:?}",
            basic.nodes
        );

        nodes.get_mut("relay-a").unwrap().node.status = NodeStatus::Offline;
        let observed = observed_state(&nodes);
        let outcome = reconcile(vec![stream("basic")], &observed, &LinkKeys::for_tests());

        let basic = outcome.streams.iter().find(|s| s.name == "basic").unwrap();
        assert!(basic.nodes.contains(&"relay-b".to_string()));
        assert!(!basic.nodes.contains(&"relay-a".to_string()));
        assert_ne!(basic.status, PathStatus::Degraded);
        assert!(basic.conditions.iter().all(|condition| {
            condition.condition_type != StreamConditionType::NodesAvailable
                || condition.status == StreamConditionStatus::True
        }));
    }

    #[tokio::test]
    async fn tick_marks_offline_node_but_still_serves_its_desired_hops() {
        let (state, _mem) = mem_state();
        two_nodes_and_basic(&state).await;
        {
            let now = Instant::now();
            let mut seen = state.last_seen.write().await;
            seen.insert("strom-node-1".to_string(), now - Duration::from_secs(60));
            seen.insert("strom-node-2".to_string(), now);
        }

        reconcile_tick(&state).await;

        let app = open_router(state);
        let (status, body) = send(&app, "GET", "/nodes", None).await;
        assert_eq!(status, StatusCode::OK);
        let nodes: Vec<NodeResource> = serde_json::from_value(body).unwrap();
        let node1 = nodes.iter().find(|n| n.node.id == "strom-node-1").unwrap();
        assert_eq!(node1.node.status, NodeStatus::Offline);

        let (status, body) = desired_hops(&app, "strom-node-1").await;
        assert_eq!(status, StatusCode::OK);
        let hops: Vec<DesiredHop> = serde_json::from_value(body).unwrap();
        assert!(
            !hops.is_empty(),
            "offline node still receives its desired hops"
        );
    }

    async fn seed_restart_case(store: &dyn StateStore) {
        if let Some(existing) = store
            .load_streams()
            .await
            .unwrap()
            .into_iter()
            .find(|stored| stored.spec.name == "restart")
        {
            store
                .delete_stream("restart", existing.revision)
                .await
                .unwrap();
        }
        store
            .create_stream(&stream_between(
                "restart",
                "restart-node-1",
                "restart-node-2",
            ))
            .await
            .unwrap();
        for (id, host) in [
            ("restart-node-1", "172.26.0.10"),
            ("restart-node-2", "172.27.0.10"),
        ] {
            store
                .upsert_node(&node_registration(id, host))
                .await
                .unwrap();
        }
    }

    async fn assert_a_restart_serves_the_stored_hops(store: Arc<dyn StateStore>) {
        seed_restart_case(store.as_ref()).await;
        let before = AppState::hydrate(
            store.clone(),
            Duration::from_secs(15),
            Duration::from_secs(300),
            None,
            LinkKeys::for_tests(),
        )
        .await
        .unwrap();
        let before = router_after_first_tick(&before, Guard::Disabled, NodeGuard::Disabled).await;
        let after = AppState::hydrate(
            store,
            Duration::from_secs(15),
            Duration::from_secs(300),
            None,
            LinkKeys::for_tests(),
        )
        .await
        .unwrap();
        let after = router_after_first_tick(&after, Guard::Disabled, NodeGuard::Disabled).await;

        for node in ["restart-node-1", "restart-node-2"] {
            let (status, served) = desired_hops(&before, node).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(served.as_array().map(Vec::len), Some(1), "{node}: {served}");
            assert_eq!(
                desired_hops(&after, node).await,
                (StatusCode::OK, served),
                "{node} gets the same hops from the restarted controller"
            );
        }
    }

    #[tokio::test]
    async fn a_restart_serves_the_stored_hops_on_its_first_request() {
        assert_a_restart_serves_the_stored_hops(Arc::new(MemStore::new())).await;
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn a_restart_on_postgres_serves_the_stored_hops_on_its_first_request() {
        let url = store::tests::fresh_database().await;
        let store: Arc<dyn StateStore> = Arc::new(store::tests::leading_pg_store(&url).await);
        assert_a_restart_serves_the_stored_hops(store).await;
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn postgres_hydration_keeps_a_webrtc_report_and_drops_a_protocol_10_registration() {
        let url = store::tests::fresh_database().await;
        let store = store::tests::leading_pg_store(&url).await;
        let node = |id: &str, profile: Value| {
            json!({
                "id": id,
                "endpoint": format!("http://{id}:8092"),
                "status": "ready",
                "capabilities": { "hop_profiles": [profile] },
                "topology": { "attachments": [] },
                "inputs": [{ "id": "music", "label": "Music", "tracks": ["audio"] }]
            })
        };
        let current: NodeRegistration = serde_json::from_value(json!({
            "protocol_version": PROTOCOL_VERSION,
            "node": node("alfreds-mac", json!({
                "id": "app-audio-out",
                "ingress": "input",
                "egress": [{ "transport": "srt", "roles": ["listen", "connect"] }, { "transport": "webrtc" }],
                "max_egresses": 8
            })),
            "hop_status": [{
                "id": "weave-music-sender", "node_id": "alfreds-mac", "revision": "r",
                "state": "provisioned", "ingress": { "condition": "flowing" },
                "egresses": [{
                    "branch_id": "filip", "condition": "unreachable",
                    "resolved": { "host": "198.51.100.4", "port": 62000 },
                    "peer": { "link": "weave-music-receiver-filip", "generation": 1_761_300_000_u32, "relayed": true }
                }]
            }]
        }))
        .unwrap();
        store.upsert_node(&current).await.unwrap();
        let earlier = json!({
            "protocol_version": PROTOCOL_VERSION - 1,
            "node": node("filips-mac", json!({
                "id": "app-audio-to-srt",
                "ingress": "input",
                "egress": { "transport": "srt", "roles": ["listen", "connect"] },
                "max_egresses": 8
            })),
            "hop_status": [{
                "id": "weave-music-sender", "node_id": "filips-mac", "revision": "r",
                "state": "provisioned", "ingress": { "condition": "flowing" },
                "egresses": [{ "branch_id": "anna", "condition": "flowing" }]
            }]
        });
        store::tests::run_sql(
            &url,
            &format!("INSERT INTO nodes (id, registration) VALUES ('filips-mac', '{earlier}')"),
        )
        .await;

        let state = AppState::hydrate(
            Arc::new(store),
            Duration::from_secs(15),
            Duration::from_secs(300),
            None,
            LinkKeys::for_tests(),
        )
        .await
        .expect("hydration survives an earlier protocol's row");
        let nodes = state.nodes.read().await;
        assert_eq!(nodes.keys().collect::<Vec<_>>(), ["alfreds-mac"]);
        assert_eq!(nodes["alfreds-mac"], current);
    }

    async fn get_ok(app: &Router, uri: &str) -> Value {
        let (status, body) = send(app, "GET", uri, None).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        body
    }

    #[tokio::test]
    async fn health_names_the_version_on_a_leader_and_a_standby() {
        let (state, _mem) = mem_state();
        for app in [open_router(state), leadership_router(Leadership::default())] {
            assert_eq!(
                get_ok(&app, "/health").await,
                json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") })
            );
        }
    }

    #[tokio::test]
    async fn a_standby_answers_not_leader_on_every_api_route() {
        let app = leadership_router(Leadership::default());
        for (method, uri) in [
            ("GET", "/status"),
            ("GET", "/streams"),
            ("POST", "/streams"),
            ("GET", "/streams/basic"),
            ("DELETE", "/streams/basic"),
            ("GET", "/streams/basic/endpoints"),
            ("POST", "/stream-plans"),
            ("GET", "/stream-sets"),
            ("PUT", "/stream-sets/production"),
            ("GET", "/nodes"),
            ("POST", "/nodes/register"),
            ("POST", "/nodes/strom-node-1/heartbeat"),
            ("GET", "/nodes/strom-node-1/desired"),
            ("POST", "/nodes/strom-node-1/signalling"),
            ("GET", "/endpoints"),
            ("GET", "/state"),
            ("GET", "/view"),
            ("GET", "/v1/status"),
        ] {
            let (status, body) = send(&app, method, uri, None).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{method} {uri}");
            assert_eq!(body["code"], "not_leader", "{method} {uri}");
        }
        assert_eq!(get_ok(&app, "/health").await["status"], "ok");
        for page in ["/", "/ui"] {
            let response = app
                .clone()
                .oneshot(Request::get(page).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{page}");
        }
    }

    #[tokio::test]
    async fn a_leader_serves_through_the_gate_until_it_stands_by() {
        let (state, _mem) = mem_state();
        two_nodes_and_basic(&state).await;
        let leadership = Leadership::default();
        let app = leadership_router(leadership.clone());

        leadership.taking(Instant::now() + Duration::from_secs(60));
        assert_eq!(
            desired_hops(&app, "strom-node-1").await.0,
            StatusCode::SERVICE_UNAVAILABLE,
            "nothing is served while the state loads"
        );
        let led = router_after_first_tick(&state, Guard::Disabled, NodeGuard::Disabled).await;
        assert!(leadership.lead(led));
        let hops = get_ok(&app, "/nodes/strom-node-1/desired").await;
        assert_eq!(hops.as_array().map(Vec::len), Some(1), "{hops}");

        leadership.stand_by();
        let (status, body) = desired_hops(&app, "strom-node-1").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "not_leader");
    }

    #[tokio::test]
    async fn a_leader_serves_nothing_past_its_hold_deadline_without_a_renewal() {
        let (state, _mem) = mem_state();
        let leadership = Leadership::default();
        let app = leadership_router(leadership.clone());
        leadership.taking(Instant::now() + Duration::from_millis(200));
        let led = router_after_first_tick(&state, Guard::Disabled, NodeGuard::Disabled).await;
        assert!(leadership.lead(led));
        assert_eq!(send(&app, "GET", "/status", None).await.0, StatusCode::OK);

        tokio::time::sleep(Duration::from_millis(250)).await;
        let (status, body) = send(&app, "GET", "/status", None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], "not_leader");
    }

    #[test]
    fn a_lease_lost_while_the_state_loads_is_never_served() {
        let (state, _mem) = mem_state();
        let leadership = Leadership::default();
        leadership.taking(Instant::now() + Duration::from_secs(60));
        leadership.stand_by();

        assert!(!leadership.lead(open_router(state)));
        assert!(leadership.router().is_none());
    }

    fn flowing_reports(hops: &[DesiredHop]) -> Vec<HopStatus> {
        let flowing = || weave_core::SocketStatus {
            peer: None,
            condition: weave_core::LinkCondition::Flowing,
            resolved: None,
            stats: None,
        };
        hops.iter()
            .map(|hop| HopStatus {
                delivery: None,
                id: hop.id.clone(),
                node_id: hop.node_id.clone(),
                revision: hop.revision(),
                state: weave_core::HopState::Provisioned,
                ingress: flowing(),
                merge_ingress: hop.merge_ingress.as_ref().map(|_| flowing()),
                egresses: hop
                    .egresses
                    .iter()
                    .map(|egress| weave_core::EgressStatus {
                        branch_id: egress.branch_id.clone(),
                        status: flowing(),
                    })
                    .collect(),
            })
            .collect()
    }

    async fn report_flowing(app: &Router, node_id: &str) {
        let hops: Vec<DesiredHop> =
            serde_json::from_value(get_ok(app, &format!("/nodes/{node_id}/desired")).await)
                .unwrap();
        let heartbeat = NodeHeartbeat {
            inputs: Vec::new(),
            outputs: Vec::new(),
            node_id: node_id.to_string(),
            status: NodeStatus::Ready,
            observation: None,
            endpoints: Vec::new(),
            hop_status: flowing_reports(&hops),
        };
        let (status, body) = send(
            app,
            "POST",
            &format!("/nodes/{node_id}/heartbeat"),
            Some(serde_json::to_value(&heartbeat).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{node_id}: {body}");
    }

    #[tokio::test]
    async fn updated_settings_wait_for_reports_of_the_new_hop_revision() {
        let (state, _) = mem_state();
        two_nodes_and_basic(&state).await;
        let app = open_router(state.clone());
        reconcile_tick(&state).await;
        for node in ["strom-node-1", "strom-node-2"] {
            report_flowing(&app, node).await;
        }
        reconcile_tick(&state).await;
        assert_eq!(
            get_ok(&app, "/status").await["streams"][0]["status"],
            "flowing"
        );
        {
            let mut streams = state.streams.write().await;
            let stored = streams.get_mut("basic").unwrap();
            stored.generation += 1;
            let StreamTransport::Srt(source) = &mut stored.spec.source else {
                unreachable!()
            };
            source.latency = Some(400);
        }
        reconcile_tick(&state).await;
        assert_eq!(
            get_ok(&app, "/status").await["streams"][0]["status"],
            "pending"
        );
        assert!(get_ok(&app, "/view").await["streams"][0]["hops"][0]["state"].is_null());
        report_flowing(&app, "strom-node-1").await;
        reconcile_tick(&state).await;
        assert_eq!(
            get_ok(&app, "/status").await["streams"][0]["status"],
            "flowing"
        );
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

    #[tokio::test]
    async fn a_restart_keeps_unchanged_conditions_and_reports_no_change() {
        let mem = Arc::new(MemStore::new());
        seed_restart_case(mem.as_ref()).await;
        let before = hydrated(mem.clone(), None).await;
        let before_app =
            router_after_first_tick(&before, Guard::Disabled, NodeGuard::Disabled).await;
        for node in ["restart-node-1", "restart-node-2"] {
            report_flowing(&before_app, node).await;
        }
        reconcile_tick(&before).await;
        let flowing = before.view.read().await.streams.clone();
        assert_eq!(flowing[0].status, PathStatus::Flowing, "{flowing:?}");
        tokio::time::sleep(Duration::from_millis(5)).await;

        let mut sink = sink(StatusCode::OK).await;
        let webhooks = webhook::Emitter::new(webhook::Config {
            url: Some(sink.url.clone()),
            ..webhook::Config::default()
        })
        .map(Arc::new);
        let restarted = hydrated(mem, webhooks).await;
        let restarted_app =
            router_after_first_tick(&restarted, Guard::Disabled, NodeGuard::Disabled).await;

        let RunningStatus { streams, .. } =
            serde_json::from_value(get_ok(&restarted_app, "/status").await).unwrap();
        assert_eq!(
            streams, flowing,
            "the same status, and every condition keeps its transition time"
        );
        assert!(
            stream_events(&mut sink).await.is_empty(),
            "nothing changed, so nothing is reported"
        );
    }

    fn hop_nodes(app_desired: &[(String, Vec<DesiredHop>)], hop_id: &str) -> Vec<String> {
        app_desired
            .iter()
            .filter(|(_, hops)| hops.iter().any(|hop| hop.id == hop_id))
            .map(|(node, _)| node.clone())
            .collect()
    }

    async fn desired_everywhere(app: &Router, nodes: &[&str]) -> Vec<(String, Vec<DesiredHop>)> {
        let mut desired = Vec::new();
        for node in nodes {
            let hops = get_ok(app, &format!("/nodes/{node}/desired")).await;
            desired.push(((*node).to_string(), serde_json::from_value(hops).unwrap()));
        }
        desired
    }

    #[tokio::test]
    async fn a_restart_keeps_a_relayed_stream_on_the_relay_that_runs_it() {
        const NODES: [&str; 4] = ["relay-a", "relay-b", "source", "studio-node"];
        let bridge = crate::path::bridge_hop_id("feed", "studio", 0);
        let mem = Arc::new(MemStore::new());
        mem.create_stream(&stream_between("feed", "source", "studio-node"))
            .await
            .unwrap();
        for registration in [
            nat_registration("source", "192.168.1.10"),
            nat_registration("studio-node", "192.168.2.10"),
            node_registration("relay-a", "198.51.100.10"),
            node_registration("relay-b", "198.51.100.20"),
        ] {
            mem.upsert_node(&registration).await.unwrap();
        }

        let before = hydrated(mem.clone(), None).await;
        let before_app =
            router_after_first_tick(&before, Guard::Disabled, NodeGuard::Disabled).await;
        assert_eq!(
            hop_nodes(&desired_everywhere(&before_app, &NODES).await, &bridge),
            ["relay-a"]
        );
        before.last_seen.write().await.insert(
            "relay-a".to_string(),
            Instant::now() - Duration::from_secs(60),
        );
        reconcile_tick(&before).await;
        let moved = desired_everywhere(&before_app, &NODES).await;
        assert_eq!(
            hop_nodes(&moved, &bridge),
            ["relay-b"],
            "relay-a went offline"
        );
        let (status, _) = send(
            &before_app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(node_registration("relay-a", "198.51.100.10")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        for node in NODES {
            report_flowing(&before_app, node).await;
        }
        reconcile_tick(&before).await;
        assert_eq!(
            desired_everywhere(&before_app, &NODES).await,
            moved,
            "relay-a is back, and the bridge stays where it runs"
        );

        let restarted = hydrated(mem, None).await;
        let restarted_app =
            router_after_first_tick(&restarted, Guard::Disabled, NodeGuard::Disabled).await;
        assert_eq!(
            desired_everywhere(&restarted_app, &NODES).await,
            moved,
            "the restarted controller keeps the bridge, and its ports, on relay-b"
        );

        restarted.last_seen.write().await.insert(
            "relay-b".to_string(),
            Instant::now() - Duration::from_secs(60),
        );
        reconcile_tick(&restarted).await;
        assert_eq!(
            hop_nodes(&desired_everywhere(&restarted_app, &NODES).await, &bridge),
            ["relay-a"],
            "a relay that stops heartbeating after the restart still loses the bridge"
        );
    }

    #[tokio::test]
    async fn a_stream_write_holds_the_streams_until_the_view_shows_it() {
        let (state, mem) = mem_state();
        let app = open_router(state.clone());
        let set = json!({ "streams": [serde_json::to_value(stream("owned")).unwrap()] });
        for (label, method, uri, body, headers, writes) in [
            (
                "create",
                "POST",
                "/streams",
                Some(serde_json::to_value(stream("basic")).unwrap()),
                vec![("if-none-match", "*")],
                1,
            ),
            (
                "stream set",
                "PUT",
                "/stream-sets/production",
                Some(set),
                vec![("if-none-match", "*")],
                1,
            ),
            (
                "delete",
                "DELETE",
                "/streams/basic",
                None,
                vec![("if-match", "\"revision-1\"")],
                1,
            ),
        ] {
            let view = state.view.write().await;
            let before = mem.upsert_stream_calls() + mem.delete_stream_calls();
            let request = tokio::spawn({
                let app = app.clone();
                async move { send_with_headers(&app, method, uri, body, &headers).await }
            });
            tokio::time::sleep(Duration::from_millis(50)).await;
            if label != "stream set" {
                assert_eq!(
                    mem.upsert_stream_calls() + mem.delete_stream_calls(),
                    before + writes,
                    "{label}: the store write has happened"
                );
            }
            assert!(
                state.streams.try_read().is_err(),
                "{label}: the streams are readable before the view shows the write"
            );
            drop(view);
            let (status, _, body) = request.await.unwrap();
            assert!(status.is_success(), "{label}: {status} {body}");
        }
    }

    #[tokio::test]
    async fn a_failed_status_save_is_retried_on_the_next_tick() {
        let mem = Arc::new(MemStore::new());
        seed_restart_case(mem.as_ref()).await;
        let state = hydrated(mem.clone(), None).await;
        mem.fail_writes(1);

        reconcile_tick(&state).await;
        assert!(mem.load_stream_statuses().await.unwrap().is_empty());
        reconcile_tick(&state).await;

        assert_eq!(
            mem.load_stream_statuses().await.unwrap(),
            state.view.read().await.streams
        );
    }

    #[tokio::test]
    async fn failed_node_writes_are_retried_on_the_next_tick() {
        let mem = Arc::new(MemStore::new());
        seed_restart_case(mem.as_ref()).await;
        let state = hydrated(mem.clone(), None).await;
        let app = router_after_first_tick(&state, Guard::Disabled, NodeGuard::Disabled).await;

        mem.fail_writes(1);
        report_flowing(&app, "restart-node-2").await;
        state.last_seen.write().await.insert(
            "restart-node-1".to_string(),
            Instant::now() - Duration::from_secs(60),
        );
        mem.fail_writes(2);
        reconcile_tick(&state).await;
        reconcile_tick(&state).await;

        let stored: BTreeMap<String, NodeRegistration> = mem
            .load_nodes()
            .await
            .unwrap()
            .into_iter()
            .map(|registration| (registration.node.id.clone(), registration))
            .collect();
        assert_eq!(stored, *state.nodes.read().await);
        assert_eq!(stored["restart-node-1"].node.status, NodeStatus::Offline);
        assert!(!stored["restart-node-2"].hop_status.is_empty());
    }

    #[tokio::test]
    async fn a_node_marked_offline_is_still_offline_after_a_restart() {
        let mem = Arc::new(MemStore::new());
        seed_restart_case(mem.as_ref()).await;
        let before = hydrated(mem.clone(), None).await;
        before.last_seen.write().await.insert(
            "restart-node-1".to_string(),
            Instant::now() - Duration::from_secs(60),
        );
        reconcile_tick(&before).await;

        let restarted = hydrated(mem, None).await;
        let app = router_after_first_tick(&restarted, Guard::Disabled, NodeGuard::Disabled).await;
        let nodes: Vec<NodeResource> =
            serde_json::from_value(get_ok(&app, "/nodes").await).unwrap();
        assert_eq!(
            nodes
                .iter()
                .map(|resource| (resource.node.id.as_str(), resource.node.status))
                .collect::<Vec<_>>(),
            [
                ("restart-node-1", NodeStatus::Offline),
                ("restart-node-2", NodeStatus::Ready),
            ]
        );
    }

    #[tokio::test]
    async fn a_takeover_counts_every_stored_node_as_heard_at_the_takeover() {
        let store = Arc::new(MemStore::new());
        seed_restart_case(store.as_ref()).await;
        store
            .upsert_node(&node_registration("spare-node", "172.28.0.10"))
            .await
            .unwrap();
        let took_over = Instant::now();
        let state = AppState::hydrate(
            store,
            Duration::from_secs(15),
            Duration::ZERO,
            None,
            LinkKeys::for_tests(),
        )
        .await
        .unwrap();
        let app = router_after_first_tick(&state, Guard::Disabled, NodeGuard::Disabled).await;

        let nodes: Vec<NodeResource> =
            serde_json::from_value(get_ok(&app, "/nodes").await).unwrap();
        assert_eq!(
            nodes
                .iter()
                .map(|resource| (resource.node.id.as_str(), resource.node.status))
                .collect::<Vec<_>>(),
            [
                ("restart-node-1", NodeStatus::Ready),
                ("restart-node-2", NodeStatus::Ready),
                ("spare-node", NodeStatus::Ready),
            ],
            "the first tick marks nobody offline and forgets nobody, with a zero forget interval"
        );
        assert!(
            state
                .last_seen
                .read()
                .await
                .values()
                .all(|seen| *seen >= took_over)
        );
    }

    async fn pg_controller(url: &str) -> (Arc<PgStore>, Controller) {
        let pg = Arc::new(PgStore::connect(url).await.expect("connect"));
        let controller = Controller {
            node_ttl: Duration::from_secs(15),
            node_forget: Duration::from_secs(300),
            webhooks: None,
            keys: LinkKeys::for_tests(),
            north: Guard::Disabled,
            south: NodeGuard::Disabled,
            interval: Duration::from_millis(200),
        };
        (pg, controller)
    }

    async fn start_controller(
        url: &str,
        timing: LeaseTiming,
    ) -> (
        Arc<PgStore>,
        Leadership,
        tokio::sync::oneshot::Sender<()>,
        JoinHandle<Result<()>>,
    ) {
        let (pg, controller) = pg_controller(url).await;
        let leadership = Leadership::default();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let run = tokio::spawn({
            let pg = pg.clone();
            let leadership = leadership.clone();
            async move {
                run_with_lease(pg, &controller, &leadership, timing, async {
                    let _ = stopped.await;
                    Ok(())
                })
                .await
            }
        });
        (pg, leadership, stop, run)
    }

    async fn until_leading(leadership: &Leadership, within: Duration) {
        let deadline = Instant::now() + within;
        while leadership.router().is_none() {
            assert!(Instant::now() < deadline, "not leading within {within:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn until_standing_by(leadership: &Leadership, within: Duration) {
        let deadline = Instant::now() + within;
        while leadership.router().is_some() {
            assert!(Instant::now() < deadline, "still leading after {within:?}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn served_state(app: &Router) -> Value {
        let (_, stream_headers, stream) =
            send_with_headers(app, "GET", "/streams/restart", None, &[]).await;
        let (_, set_headers, _) =
            send_with_headers(app, "GET", "/stream-sets/production", None, &[]).await;
        json!({
            "node-1": get_ok(app, "/nodes/restart-node-1/desired").await,
            "node-2": get_ok(app, "/nodes/restart-node-2/desired").await,
            "stream": stream,
            "stream_etag": response_etag(&stream_headers),
            "set_etag": response_etag(&set_headers),
        })
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn a_standby_on_postgres_takes_over_when_the_leader_stops() {
        let url = store::tests::fresh_database().await;
        let seed = store::tests::leading_pg_store(&url).await;
        seed_restart_case(&seed).await;
        seed.create_stream_set(
            "production",
            &[stream_between("owned", "restart-node-2", "restart-node-1")],
            true,
        )
        .await
        .unwrap();
        seed.release_lease().await.unwrap();
        let timing = LeaseTiming::new(Duration::from_secs(3));

        let (first_pg, first, stop_first, first_run) = start_controller(&url, timing).await;
        until_leading(&first, Duration::from_secs(5)).await;
        let first_app = leadership_router(first.clone());
        let before = served_state(&first_app).await;
        assert_eq!(
            before["node-1"].as_array().map(Vec::len),
            Some(2),
            "{before}"
        );

        let (_second_pg, second, _stop_second, _second_run) = start_controller(&url, timing).await;
        let second_app = leadership_router(second.clone());
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(second.router().is_none(), "the lease has one holder");
        assert_eq!(
            desired_hops(&second_app, "restart-node-1").await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            served_state(&first_app).await,
            before,
            "renewals keep it leading"
        );

        stop_first.send(()).unwrap();
        first_run.await.unwrap().unwrap();
        assert_eq!(
            desired_hops(&first_app, "restart-node-1").await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(matches!(
            first_pg
                .upsert_node(&node_registration("late", "172.28.0.10"))
                .await,
            Err(StoreError::NotLeader)
        ));

        until_leading(&second, Duration::from_secs(5)).await;
        assert_eq!(
            served_state(&second_app).await,
            before,
            "the new leader serves the same hops, generations and ETags"
        );
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn a_write_from_an_earlier_term_is_refused_after_the_lease_is_taken_again() {
        let url = store::tests::fresh_database().await;
        let seed = store::tests::leading_pg_store(&url).await;
        seed.create_stream(&stream("x")).await.unwrap();
        seed.release_lease().await.unwrap();
        let timing = LeaseTiming::new(Duration::from_secs(3));
        let (_pg, leadership, _stop, _run) = start_controller(&url, timing).await;
        until_leading(&leadership, Duration::from_secs(5)).await;
        let earlier = leadership.router().unwrap();

        let other = sqlx::PgPool::connect(&url).await.unwrap();
        sqlx::query(
            "UPDATE controller_lease SET holder = 'other', epoch = epoch + 1,
             expires_at = now() + interval '1 second'",
        )
        .execute(&other)
        .await
        .unwrap();
        until_standing_by(&leadership, timing.renew_every * 3).await;
        until_leading(&leadership, Duration::from_secs(5)).await;
        let current = leadership_router(leadership.clone());

        let (status, body) = send(&earlier, "DELETE", "/streams/x", None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        assert_eq!(body["code"], "not_leader");
        let stored: Vec<String> = sqlx::query_scalar("SELECT name FROM streams")
            .fetch_all(&other)
            .await
            .unwrap();
        assert_eq!(stored, ["x"]);
        assert_eq!(
            send(&current, "GET", "/streams/x", None).await.0,
            StatusCode::OK
        );
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn a_controller_that_cannot_load_its_state_stands_by_and_tries_again() {
        let url = store::tests::fresh_database().await;
        let seed = store::tests::leading_pg_store(&url).await;
        seed.create_stream(&stream("basic")).await.unwrap();
        seed.release_lease().await.unwrap();
        let other = sqlx::PgPool::connect(&url).await.unwrap();
        let set_name = |name: &'static str| {
            sqlx::query(
                "UPDATE streams SET definition = jsonb_set(definition, '{name}', to_jsonb($1::text))",
            )
            .bind(name)
            .execute(&other)
        };
        set_name("Not A Stream Name").await.unwrap();

        let timing = LeaseTiming::new(Duration::from_secs(3));
        let (_pg, leadership, _stop, run) = start_controller(&url, timing).await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!run.is_finished(), "the controller is still running");
        assert!(leadership.router().is_none());

        set_name("basic").await.unwrap();
        until_leading(&leadership, Duration::from_secs(8)).await;
        let app = leadership_router(leadership.clone());
        assert_eq!(
            send(&app, "GET", "/streams/basic", None).await.0,
            StatusCode::OK
        );
    }

    #[tokio::test]
    #[ignore = "requires a running Postgres via DATABASE_URL"]
    async fn a_leader_whose_lease_is_taken_stops_serving_and_stands_by() {
        let url = store::tests::fresh_database().await;
        let timing = LeaseTiming::new(Duration::from_secs(3));
        let (_pg, leadership, _stop, _run) = start_controller(&url, timing).await;
        until_leading(&leadership, Duration::from_secs(5)).await;

        let other = sqlx::PgPool::connect(&url).await.unwrap();
        sqlx::query(
            "UPDATE controller_lease SET holder = 'other', epoch = epoch + 1,
             expires_at = now() + interval '5 seconds'",
        )
        .execute(&other)
        .await
        .unwrap();
        until_standing_by(&leadership, timing.renew_every * 3).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            leadership.router().is_none(),
            "it waits while the other holder's lease lasts"
        );

        until_leading(&leadership, Duration::from_secs(8)).await;
        let (holder, epoch): (String, i64) =
            sqlx::query_as("SELECT holder, epoch FROM controller_lease")
                .fetch_one(&other)
                .await
                .unwrap();
        assert!(
            holder != "other" && epoch >= 3,
            "it took the lease again once the other one lapsed: {holder} {epoch}"
        );
    }

    #[tokio::test]
    async fn a_node_the_last_tick_did_not_cover_gets_404_for_its_desired_hops() {
        let (state, _mem) = mem_state();
        let app = router_after_first_tick(&state, Guard::Disabled, NodeGuard::Disabled).await;

        let (status, body) = desired_hops(&app, "strom-node-1").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "node_not_found");

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(node_registration("strom-node-1", "172.26.0.10")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(
            desired_hops(&app, "strom-node-1").await.0,
            StatusCode::NOT_FOUND,
            "registered after the last tick"
        );

        reconcile_tick(&state).await;
        assert_eq!(
            desired_hops(&app, "strom-node-1").await,
            (StatusCode::OK, json!([])),
            "a reconciled node with nothing to run is told so"
        );
    }

    #[tokio::test]
    async fn deleting_the_last_stream_tells_its_nodes_to_run_nothing() {
        let (state, _mem) = mem_state();
        {
            let mut nodes = state.nodes.write().await;
            for (id, host) in [
                ("strom-node-1", "172.26.0.10"),
                ("strom-node-2", "172.27.0.10"),
            ] {
                nodes.insert(id.to_string(), node_registration(id, host));
            }
        }
        let app = open_router(state.clone());
        let (status, _) = send(
            &app,
            "POST",
            "/streams",
            Some(serde_json::to_value(stream("basic")).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        reconcile_tick(&state).await;
        let (_, body) = desired_hops(&app, "strom-node-1").await;
        assert_eq!(body.as_array().map(Vec::len), Some(1));

        let (status, _) = send(&app, "DELETE", "/streams/basic", None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        reconcile_tick(&state).await;
        assert_eq!(
            desired_hops(&app, "strom-node-1").await,
            (StatusCode::OK, json!([]))
        );
    }

    fn observed_status(conditions: Vec<StreamCondition>) -> StreamStatus {
        StreamStatus {
            format_policy: weave_core::FormatPolicy::Report,
            name: "basic".to_string(),
            generation: 1,
            observed_generation: Some(1),
            status: PathStatus::AwaitingInput,
            nodes: Vec::new(),
            conditions: conditions.clone(),
            ingress: None,
            destinations: vec![StreamDestinationStatus {
                delivery: None,
                id: "studio".to_string(),
                status: PathStatus::AwaitingInput,
                nodes: Vec::new(),
                conditions,
                endpoint: None,
            }],
        }
    }

    fn media(status: StreamConditionStatus, reason: StreamConditionReason) -> StreamCondition {
        let mut condition = stream_condition(
            StreamConditionType::MediaFlowing,
            status,
            reason,
            "media detail",
        );
        condition.last_transition_time = "2026-09-25T10:00:00Z".to_string();
        condition
    }

    fn awaiting_input() -> StreamStatus {
        observed_status(vec![media(
            StreamConditionStatus::False,
            StreamConditionReason::AwaitingInput,
        )])
    }

    fn changed_names(current: &[StreamStatus], previous: &[StreamStatus]) -> Vec<String> {
        changed_streams(current, previous)
            .into_iter()
            .map(|stream| stream.name)
            .collect()
    }

    #[test]
    fn a_stream_no_tick_computed_before_has_changed() {
        let current = [awaiting_input()];
        assert_eq!(changed_names(&current, &[]), ["basic"]);

        let placeholder = unreconciled_stream_status(&stored_stream(stream("basic")));
        assert_eq!(
            changed_names(&current, std::slice::from_ref(&placeholder)),
            ["basic"],
            "a stream accepted since the last tick"
        );
    }

    #[test]
    fn unchanged_conditions_are_not_a_change() {
        let previous = [awaiting_input()];
        assert!(changed_names(&previous, &previous).is_empty());

        let mut detail = awaiting_input();
        detail.conditions[0].detail = "other words".to_string();
        assert!(
            changed_names(std::slice::from_ref(&detail), &previous).is_empty(),
            "detail is free text"
        );

        let mut generation = awaiting_input();
        generation.generation = 2;
        generation.observed_generation = Some(2);
        assert!(changed_names(std::slice::from_ref(&generation), &previous).is_empty());
    }

    #[test]
    fn a_status_reason_or_destination_change_is_one_change() {
        let previous = [awaiting_input()];

        let mut flowing = awaiting_input();
        flowing.conditions[0] = media(
            StreamConditionStatus::True,
            StreamConditionReason::MediaFlowing,
        );
        flowing.destinations[0].conditions[0] = flowing.conditions[0].clone();
        assert_eq!(
            changed_names(std::slice::from_ref(&flowing), &previous),
            ["basic"]
        );

        let mut degraded = awaiting_input();
        degraded.conditions[0].reason = StreamConditionReason::MediaDegraded;
        assert_eq!(
            changed_names(std::slice::from_ref(&degraded), &previous),
            ["basic"],
            "awaiting_input and media_degraded share a status"
        );

        let mut destination = awaiting_input();
        destination.destinations[0].conditions[0].reason = StreamConditionReason::MediaDegraded;
        assert_eq!(
            changed_names(std::slice::from_ref(&destination), &previous),
            ["basic"]
        );

        let mut added = awaiting_input();
        let mut preview = added.destinations[0].clone();
        preview.id = "preview".to_string();
        added.destinations.insert(0, preview);
        assert_eq!(
            changed_names(std::slice::from_ref(&added), &previous),
            ["basic"]
        );
    }

    #[test]
    fn a_reason_change_keeps_the_transition_time_it_reports() {
        let previous = [awaiting_input()];
        let mut current = awaiting_input();
        current.conditions[0].reason = StreamConditionReason::MediaDegraded;
        current.conditions[0].last_transition_time = String::new();
        stamp_condition_transition_times(
            std::slice::from_mut(&mut current),
            &previous,
            "2026-09-25T10:05:00Z",
        );

        let changed = changed_streams(std::slice::from_ref(&current), &previous);
        assert_eq!(
            changed[0].conditions[0].last_transition_time,
            "2026-09-25T10:00:00Z"
        );
    }

    async fn stream_events(sink: &mut Sink) -> Vec<StreamSummary> {
        let mut events = Vec::new();
        while let Ok(delivery) = tokio::time::timeout(Duration::from_millis(300), sink.next()).await
        {
            assert_eq!(delivery.event.event_type, EventType::StreamChanged);
            let Subject::Stream(stream) = delivery.event.subject else {
                panic!("expected a stream event");
            };
            events.push(stream);
        }
        events
    }

    #[tokio::test]
    async fn the_first_tick_after_a_start_reports_every_stream() {
        let mut sink = sink(StatusCode::OK).await;
        let mem = Arc::new(MemStore::new());
        mem.create_stream(&stream("basic")).await.unwrap();
        mem.create_stream(&stream("backup")).await.unwrap();
        let webhooks = webhook::Emitter::new(webhook::Config {
            url: Some(sink.url.clone()),
            ..webhook::Config::default()
        })
        .map(Arc::new);
        let state = AppState::hydrate(
            mem,
            Duration::from_secs(15),
            Duration::from_secs(300),
            webhooks,
            LinkKeys::for_tests(),
        )
        .await
        .unwrap();

        reconcile_tick(&state).await;
        reconcile_tick(&state).await;

        let events = stream_events(&mut sink).await;
        let names: Vec<&str> = events.iter().map(|stream| stream.name.as_str()).collect();
        assert_eq!(names, ["backup", "basic"], "once each, on the first tick");
        assert_eq!(events[1].generation, 1);
        assert_eq!(events[1].observed_generation, Some(1));
        assert_eq!(events[1].status, PathStatus::Pending);
        let placement = &events[1].conditions[0];
        assert_eq!(
            placement.condition_type,
            StreamConditionType::PlacementReady
        );
        assert_eq!(placement.reason, StreamConditionReason::PlacementFailed);
        assert_eq!(events[1].destinations[0].id, "studio");
    }

    #[tokio::test]
    async fn a_hop_status_change_reports_the_stream_once() {
        let mut sink = sink(StatusCode::OK).await;
        let state = webhook_state(&sink);
        two_nodes_and_basic(&state).await;
        reconcile_tick(&state).await;
        let first = stream_events(&mut sink).await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].status, PathStatus::Pending, "no hop has reported");

        let hops = state.view.read().await.hops["basic"].clone();
        {
            let mut nodes = state.nodes.write().await;
            for hop in &hops {
                nodes
                    .get_mut(&hop.node_id)
                    .unwrap()
                    .hop_status
                    .push(HopStatus {
                        delivery: None,
                        id: hop.id.clone(),
                        node_id: hop.node_id.clone(),
                        revision: hop.revision(),
                        state: weave_core::HopState::Provisioned,
                        ingress: weave_core::SocketStatus {
                            peer: None,
                            condition: weave_core::LinkCondition::Connecting,
                            resolved: None,
                            stats: None,
                        },
                        merge_ingress: None,
                        egresses: hop
                            .egresses
                            .iter()
                            .map(|egress| weave_core::EgressStatus {
                                branch_id: egress.branch_id.clone(),
                                status: weave_core::SocketStatus {
                                    peer: None,
                                    condition: weave_core::LinkCondition::Connecting,
                                    resolved: None,
                                    stats: None,
                                },
                            })
                            .collect(),
                    });
            }
        }
        reconcile_tick(&state).await;
        reconcile_tick(&state).await;

        let second = stream_events(&mut sink).await;
        assert_eq!(second.len(), 1, "{second:?}");
        assert_ne!(second[0].status, PathStatus::Pending);
        let hops_ready = second[0]
            .conditions
            .iter()
            .find(|condition| condition.condition_type == StreamConditionType::HopsReady)
            .unwrap();
        assert_eq!(hops_ready.status, StreamConditionStatus::True);
    }

    #[tokio::test]
    async fn a_tick_does_not_wait_on_an_unreachable_receiver() {
        let (mut state, _mem) = mem_state();
        state.webhooks = webhook::Emitter::new(webhook::Config {
            url: Some("http://192.0.2.1:1/hook".to_string()),
            timeout: Duration::from_secs(5),
            ..webhook::Config::default()
        })
        .map(Arc::new);
        {
            let mut streams = state.streams.write().await;
            for index in 0..300 {
                let name = format!("stream-{index}");
                streams.insert(name.clone(), stored_stream(stream(&name)));
            }
        }

        tokio::time::timeout(Duration::from_secs(2), reconcile_tick(&state))
            .await
            .expect("a tick must not wait on the webhook receiver");
    }

    #[test]
    fn only_unnamed_offline_nodes_past_the_interval_are_forgettable() {
        let offline = |id: &str| {
            let mut registration = node_registration(id, "172.26.0.10");
            registration.node.status = NodeStatus::Offline;
            (id.to_string(), registration)
        };
        let mut nodes = BTreeMap::from([
            offline("stale"),
            offline("named"),
            offline("edge"),
            offline("recent"),
            (
                "silent".to_string(),
                node_registration("silent", "172.26.0.10"),
            ),
        ]);
        nodes.get_mut("silent").unwrap().node.status = NodeStatus::Ready;
        let after = Duration::from_secs(300);
        let now = Instant::now();
        let last_seen = BTreeMap::from([
            ("stale".to_string(), now - Duration::from_secs(301)),
            ("named".to_string(), now - Duration::from_secs(301)),
            ("edge".to_string(), now - after),
            ("recent".to_string(), now - Duration::from_secs(20)),
            ("silent".to_string(), now - Duration::from_secs(301)),
        ]);
        let named = BTreeSet::from(["named"]);

        assert_eq!(
            forgettable(&nodes, &last_seen, &named, now, after),
            ["stale"]
        );
    }

    #[test]
    fn a_stream_names_its_source_destinations_and_via_relays() {
        let mut definition = stream_between("basic", "source-node", "studio-node");
        let StreamTransport::Srt(studio) = &mut definition.destinations[0].endpoint else {
            unreachable!()
        };
        studio.via = vec!["relay-node".to_string()];
        definition.destinations.push(StreamDestination {
            delivery: None,
            id: "partner".to_string(),
            paths: 1,
            endpoint: StreamTransport::Srt(SrtEndpoint {
                node: None,
                remote: Some(weave_core::RemoteAddr {
                    host: "203.0.113.7".to_string(),
                    port: 9000,
                    network: "internet".to_string(),
                }),
                via: vec!["egress-node".to_string()],
                format: None,
                accepts: None,
                network: None,
                latency: None,
                passphrase: None,
            }),
        });
        let mut disabled = stream_between("spare", "spare-node", "studio-node");
        disabled.enabled = false;

        assert_eq!(
            named_nodes([&definition, &disabled]),
            BTreeSet::from([
                "egress-node",
                "relay-node",
                "source-node",
                "spare-node",
                "studio-node"
            ])
        );
    }

    fn event_types(events: &[weave_core::webhook::Event]) -> Vec<EventType> {
        events.iter().map(|event| event.event_type).collect()
    }

    async fn deliveries(sink: &mut Sink) -> Vec<weave_core::webhook::Event> {
        let mut events = Vec::new();
        while let Ok(delivery) = tokio::time::timeout(Duration::from_millis(300), sink.next()).await
        {
            events.push(delivery.event);
        }
        events
    }

    #[tokio::test]
    async fn a_tick_forgets_an_offline_node_no_stream_names() {
        let mut sink = sink(StatusCode::OK).await;
        let (mut state, mem) = mem_state();
        state.webhooks = webhook::Emitter::new(webhook::Config {
            url: Some(sink.url.clone()),
            events: vec![EventType::NodeOffline, EventType::NodeForgotten],
            ..webhook::Config::default()
        })
        .map(Arc::new);
        two_nodes_and_basic(&state).await;
        let guest = node_registration("guest-1", "172.28.0.10");
        mem.upsert_node(&guest).await.unwrap();
        state
            .nodes
            .write()
            .await
            .insert("guest-1".to_string(), guest.clone());
        {
            let now = Instant::now();
            let mut seen = state.last_seen.write().await;
            seen.insert("strom-node-1".to_string(), now);
            seen.insert("strom-node-2".to_string(), now);
            seen.insert("guest-1".to_string(), now - Duration::from_secs(301));
        }
        let app = open_router(state.clone());
        reconcile_tick(&state).await;
        let placed = desired_hops(&app, "strom-node-1").await;

        let events = deliveries(&mut sink).await;
        assert_eq!(
            event_types(&events),
            [EventType::NodeOffline, EventType::NodeForgotten]
        );
        assert_eq!(events[1].subject.id(), "guest-1");
        let (_, body) = send(&app, "GET", "/nodes", None).await;
        let listed: Vec<NodeResource> = serde_json::from_value(body).unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|resource| resource.node.id.as_str())
                .collect::<Vec<_>>(),
            ["strom-node-1", "strom-node-2"]
        );
        assert!(!state.last_seen.read().await.contains_key("guest-1"));
        assert!(
            mem.load_nodes()
                .await
                .unwrap()
                .iter()
                .all(|registration| registration.node.id != "guest-1")
        );
        assert_eq!(desired_hops(&app, "guest-1").await.0, StatusCode::NOT_FOUND);

        reconcile_tick(&state).await;
        assert_eq!(
            desired_hops(&app, "strom-node-1").await,
            placed,
            "forgetting a node no stream names moves no hop"
        );

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/guest-1/heartbeat",
            Some(json!({ "node_id": "guest-1", "status": "ready", "inputs": [], "outputs": [] })),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(&guest).unwrap()),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(state.nodes.read().await.contains_key("guest-1"));
    }

    #[tokio::test]
    async fn a_node_a_stream_names_stays_listed_as_offline() {
        let (state, _mem) = mem_state();
        two_nodes_and_basic(&state).await;
        {
            let now = Instant::now();
            let mut seen = state.last_seen.write().await;
            seen.insert("strom-node-1".to_string(), now - Duration::from_secs(301));
            seen.insert("strom-node-2".to_string(), now);
        }

        reconcile_tick(&state).await;

        let app = open_router(state);
        let (_, body) = send(&app, "GET", "/nodes", None).await;
        let listed: Vec<NodeResource> = serde_json::from_value(body).unwrap();
        let node1 = listed
            .iter()
            .find(|resource| resource.node.id == "strom-node-1")
            .unwrap();
        assert_eq!(node1.node.status, NodeStatus::Offline);
        let (status, body) = desired_hops(&app, "strom-node-1").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
    }

    async fn offline_check_during(
        state: &AppState,
        request: impl std::future::Future<Output = (StatusCode, Value)>,
    ) -> String {
        let last_seen = state.last_seen.write().await;
        tokio::pin!(request);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut request)
                .await
                .is_err(),
            "the request waits for last_seen"
        );
        let tick_in_the_gap = state
            .nodes
            .try_write()
            .map(|mut nodes| mark_offline(&mut nodes, &last_seen, Instant::now(), state.node_ttl));
        drop(last_seen);
        assert_eq!(request.await.0, StatusCode::ACCEPTED);
        format!("{tick_in_the_gap:?}")
    }

    #[tokio::test]
    async fn a_tick_cannot_see_a_heartbeat_half_applied() {
        let (state, _mem) = mem_state();
        state.nodes.write().await.insert(
            "strom-node-1".to_string(),
            node_registration("strom-node-1", "172.26.0.10"),
        );
        state.last_seen.write().await.insert(
            "strom-node-1".to_string(),
            Instant::now() - Duration::from_secs(60),
        );
        let app = open_router(state.clone());

        let heartbeat = send(
            &app,
            "POST",
            "/nodes/strom-node-1/heartbeat",
            Some(
                json!({ "node_id": "strom-node-1", "status": "ready", "inputs": [], "outputs": [] }),
            ),
        );
        let seen = offline_check_during(&state, heartbeat).await;

        assert!(seen.starts_with("Err"), "a tick in the gap got {seen}");
        reconcile_tick(&state).await;
        assert_eq!(
            state.nodes.read().await["strom-node-1"].node.status,
            NodeStatus::Ready
        );
    }

    #[tokio::test]
    async fn a_tick_cannot_see_a_registration_half_applied() {
        let (state, _mem) = mem_state();
        let registration = node_registration("strom-node-1", "172.26.0.10");
        state
            .nodes
            .write()
            .await
            .insert("strom-node-1".to_string(), registration.clone());
        state.last_seen.write().await.insert(
            "strom-node-1".to_string(),
            Instant::now() - Duration::from_secs(60),
        );
        let app = open_router(state.clone());

        let register = send(
            &app,
            "POST",
            "/nodes/register",
            Some(serde_json::to_value(&registration).unwrap()),
        );
        let seen = offline_check_during(&state, register).await;

        assert!(seen.starts_with("Err"), "a tick in the gap got {seen}");
    }
}

#[cfg(test)]
mod node_auth_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use serde_json::{Value, json};
    use tower::ServiceExt;
    use weave_core::auth::{MinEpochs, NodeKey, Token};

    use super::tests::{NORTH_ROUTES, SHARED_READ_ROUTES, SOUTH_ROUTES};

    const NORTH: &str = "north-test-token";
    const KEY: &str = "south-test-key-0123456789abcdef0123";

    async fn app() -> (Router, AppState) {
        let state = AppState::hydrate(
            Arc::new(MemStore::new()),
            Duration::from_secs(15),
            Duration::from_secs(300),
            None,
            LinkKeys::for_tests(),
        )
        .await
        .unwrap();
        let app = router_after_first_tick(
            &state,
            Guard::Required(Token::new(NORTH).unwrap()),
            NodeGuard::Required(NodeKey::new(KEY).unwrap()),
        )
        .await;
        (app, state)
    }

    fn node_bearer(node_id: &str) -> String {
        format!(
            "Bearer {}",
            NodeKey::new(KEY).unwrap().token_for(node_id, 0)
        )
    }

    fn registration(node_id: &str) -> Value {
        json!({
            "protocol_version": PROTOCOL_VERSION,
            "node": {
                "id": node_id,
                "endpoint": format!("http://{node_id}:8091"),
                "status": "ready",
                "capabilities": { "hop_profiles": [] },
                "topology": {
                    "attachments": [{ "id": "wan", "network": "internet", "dial": true, "listeners": {} }]
                }
            },
            "endpoints": [],
            "hop_status": []
        })
    }

    fn heartbeat(node_id: &str) -> Value {
        json!({ "node_id": node_id, "status": "ready", "inputs": [], "outputs": [], "endpoints": [], "hop_status": [] })
    }

    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        authorization: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", authorization)
            .header("content-type", "application/json")
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

    async fn node_ids(app: &Router) -> Vec<String> {
        let (status, nodes) = send(app, "GET", "/nodes", &format!("Bearer {NORTH}"), None).await;
        assert_eq!(status, StatusCode::OK);
        nodes
            .as_array()
            .unwrap()
            .iter()
            .map(|resource| resource["node"]["id"].as_str().unwrap().to_string())
            .collect()
    }

    #[tokio::test]
    async fn a_node_registers_heartbeats_and_reads_desired_as_itself() {
        let (app, state) = app().await;
        let me = node_bearer("strom-node-1");

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            &me,
            Some(registration("strom-node-1")),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/strom-node-1/heartbeat",
            &me,
            Some(heartbeat("strom-node-1")),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        reconcile_tick(&state).await;
        let (status, hops) = send(&app, "GET", "/nodes/strom-node-1/desired", &me, None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(hops, json!([]));
        assert_eq!(node_ids(&app).await, ["strom-node-1"]);
    }

    #[tokio::test]
    async fn a_node_token_is_refused_for_another_node() {
        let (app, _) = app().await;
        let other = node_bearer("strom-node-2");
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            &node_bearer("strom-node-1"),
            Some(registration("strom-node-1")),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        for (method, uri, body) in [
            (
                "POST",
                "/nodes/register",
                Some(registration("strom-node-1")),
            ),
            (
                "POST",
                "/nodes/register",
                Some(registration("strom-node-3")),
            ),
            (
                "POST",
                "/nodes/strom-node-1/heartbeat",
                Some(heartbeat("strom-node-1")),
            ),
            ("GET", "/nodes/strom-node-1/desired", None),
        ] {
            let (status, error) = send(&app, method, uri, &other, body).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{method} {uri}");
            assert_eq!(error["code"], "forbidden", "{method} {uri}");
        }
        assert_eq!(
            node_ids(&app).await,
            ["strom-node-1"],
            "a forged registration is not recorded"
        );
    }

    #[tokio::test]
    async fn a_registration_is_judged_on_its_sender_and_version_before_its_shape() {
        let (app, _) = app().await;
        let me = node_bearer("strom-node-1");
        let mut body = registration("strom-node-1");
        body["node"]["capabilities"]["bogus"] = json!(true);
        let mut stale = body.clone();
        stale["protocol_version"] = json!(PROTOCOL_VERSION + 1);

        let (status, error) = send(
            &app,
            "POST",
            "/nodes/register",
            &node_bearer("strom-node-2"),
            Some(stale.clone()),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(error["code"], "forbidden");

        let (status, error) = send(&app, "POST", "/nodes/register", &me, Some(stale)).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(error["code"], "incompatible_protocol_version");

        let (status, error) = send(&app, "POST", "/nodes/register", &me, Some(body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(error["code"], "invalid_json");
        let detail = error["details"][0]["message"].as_str().unwrap();
        assert!(
            detail.starts_with("node.capabilities.bogus: unknown field `bogus`"),
            "{detail}"
        );
        assert!(node_ids(&app).await.is_empty(), "nothing is recorded");
    }

    #[tokio::test]
    async fn a_heartbeat_from_an_unknown_node_gets_404_whatever_its_body() {
        let (app, _) = app().await;
        let me = node_bearer("strom-node-1");
        let mut unknown_value = heartbeat("strom-node-1");
        unknown_value["status"] = json!("sleeping");
        let mut unknown_field = heartbeat("strom-node-1");
        unknown_field["uptime_secs"] = json!(30);
        let cases = [
            (unknown_value, "status: unknown variant `sleeping`"),
            (unknown_field, "uptime_secs: unknown field `uptime_secs`"),
        ];

        for (body, _) in &cases {
            let (status, error) = send(
                &app,
                "POST",
                "/nodes/strom-node-1/heartbeat",
                &me,
                Some(body.clone()),
            )
            .await;
            assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
            assert_eq!(error["code"], "node_not_found");
        }

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            &me,
            Some(registration("strom-node-1")),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        for (body, expected) in cases {
            let (status, error) = send(
                &app,
                "POST",
                "/nodes/strom-node-1/heartbeat",
                &me,
                Some(body),
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(error["code"], "invalid_json");
            let detail = error["details"][0]["message"].as_str().unwrap();
            assert!(detail.starts_with(expected), "{detail}");
        }
    }

    fn with_endpoints(mut body: Value, owners: &[Option<&str>]) -> Value {
        body["endpoints"] = owners
            .iter()
            .enumerate()
            .map(|(index, owner)| {
                json!({ "id": format!("cam-{index}"), "label": "cam", "node_id": owner, "kind": "source" })
            })
            .collect();
        body
    }

    async fn endpoint_owners(app: &Router) -> Vec<Option<String>> {
        let (status, endpoints) =
            send(app, "GET", "/endpoints", &node_bearer("strom-node-1"), None).await;
        assert_eq!(status, StatusCode::OK);
        endpoints
            .as_array()
            .unwrap()
            .iter()
            .map(|endpoint| endpoint["node_id"].as_str().map(str::to_string))
            .collect()
    }

    #[tokio::test]
    async fn a_node_publishes_endpoints_only_under_its_own_id() {
        let (app, _) = app().await;
        let me = node_bearer("strom-node-1");
        let foreign = [Some("strom-node-1"), Some("strom-node-2")];

        let (status, error) = send(
            &app,
            "POST",
            "/nodes/register",
            &me,
            Some(with_endpoints(registration("strom-node-1"), &foreign)),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(error["code"], "forbidden");
        assert!(node_ids(&app).await.is_empty(), "nothing is recorded");

        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            &me,
            Some(with_endpoints(
                registration("strom-node-1"),
                &[Some("strom-node-1"), None],
            )),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let (status, error) = send(
            &app,
            "POST",
            "/nodes/strom-node-1/heartbeat",
            &me,
            Some(with_endpoints(heartbeat("strom-node-1"), &foreign)),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(error["code"], "forbidden");
        assert_eq!(
            endpoint_owners(&app).await,
            [Some("strom-node-1".to_string()), None],
            "the refused heartbeat changed nothing"
        );
    }

    #[tokio::test]
    async fn without_auth_a_node_may_publish_endpoints_under_any_id() {
        let (_, state) = app().await;
        let app = router(state, Guard::Disabled, NodeGuard::Disabled);
        let foreign = [Some("strom-node-2")];
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/register",
            "",
            Some(with_endpoints(registration("strom-node-1"), &foreign)),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let (status, _) = send(
            &app,
            "POST",
            "/nodes/strom-node-1/heartbeat",
            "",
            Some(with_endpoints(heartbeat("strom-node-1"), &foreign)),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(
            endpoint_owners(&app).await,
            [Some("strom-node-2".to_string())]
        );
    }

    #[tokio::test]
    async fn a_token_below_its_nodes_minimum_epoch_gets_401_and_others_pass() {
        let (_, state) = app().await;
        let key = NodeKey::new(KEY).unwrap();
        let app = router(
            state,
            Guard::Required(Token::new(NORTH).unwrap()),
            NodeGuard::Required(
                key.clone()
                    .with_min_epochs(MinEpochs::parse("strom-node-1=1").unwrap()),
            ),
        );
        for (node_id, epoch, expected) in [
            ("strom-node-1", 0, StatusCode::UNAUTHORIZED),
            ("strom-node-1", 1, StatusCode::ACCEPTED),
            ("strom-node-2", 0, StatusCode::ACCEPTED),
        ] {
            let bearer = format!("Bearer {}", key.token_for(node_id, epoch));
            let (status, _) = send(
                &app,
                "POST",
                "/nodes/register",
                &bearer,
                Some(registration(node_id)),
            )
            .await;
            assert_eq!(status, expected, "{node_id} at epoch {epoch}");
        }
    }

    #[tokio::test]
    async fn node_inventory_accepts_the_north_token_or_any_node_token() {
        let (app, _) = app().await;
        for authorization in [format!("Bearer {NORTH}"), node_bearer("browser-a1b2")] {
            let (status, _) = send(&app, "GET", "/nodes", &authorization, None).await;
            assert_eq!(status, StatusCode::OK, "{authorization}");
        }
        for authorization in [
            "Bearer wrong".to_string(),
            format!("Bearer {KEY}"),
            "Bearer south-test-token".to_string(),
        ] {
            let (status, _) = send(&app, "GET", "/nodes", &authorization, None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{authorization}");
        }
    }

    async fn send_auth(
        app: &Router,
        method: &str,
        uri: &str,
        authorization: Option<&str>,
    ) -> (StatusCode, Option<String>) {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json");
        if let Some(authorization) = authorization {
            request = request.header("authorization", authorization);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::from("{}")).unwrap())
            .await
            .unwrap();
        let challenge = response
            .headers()
            .get(axum::http::header::WWW_AUTHENTICATE)
            .map(|value| value.to_str().unwrap().to_string());
        (response.status(), challenge)
    }

    #[tokio::test]
    async fn dashboard_and_health_stay_open() {
        let (app, _) = app().await;
        for uri in ["/", "/ui", "/health", "/view", "/status"] {
            let (status, _) = send_auth(&app, "GET", uri, None).await;
            assert_eq!(status, StatusCode::OK, "{uri} must not require a token");
        }
    }

    #[tokio::test]
    async fn api_routes_reject_missing_and_wrong_tokens() {
        let (app, _) = app().await;

        for authorization in [None, Some("Bearer wrong-token"), Some("Basic ignored")] {
            for (method, uri) in NORTH_ROUTES
                .iter()
                .chain(&SOUTH_ROUTES)
                .chain(&SHARED_READ_ROUTES)
            {
                let (status, challenge) = send_auth(&app, method, uri, authorization).await;
                assert_eq!(
                    status,
                    StatusCode::UNAUTHORIZED,
                    "{method} {uri} with authorization={authorization:?}"
                );
                assert_eq!(challenge.as_deref(), Some("Bearer"));
            }
        }
    }

    #[tokio::test]
    async fn each_surface_rejects_the_other_surfaces_token() {
        let (app, _) = app().await;
        let north = format!("Bearer {NORTH}");
        let node = node_bearer("strom-node-1");

        for (method, uri) in NORTH_ROUTES {
            let (status, _) = send_auth(&app, method, uri, Some(&node)).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} {uri} must reject a node token"
            );
        }
        for (method, uri) in SOUTH_ROUTES {
            let (status, _) = send_auth(&app, method, uri, Some(&north)).await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{method} {uri} must reject the northbound token"
            );
        }
    }

    #[tokio::test]
    async fn each_surface_accepts_its_own_token() {
        let (app, _) = app().await;

        let (status, _) =
            send_auth(&app, "GET", "/streams", Some(&format!("Bearer {NORTH}"))).await;
        assert_eq!(status, StatusCode::OK);

        let (status, _) =
            send_auth(&app, "GET", "/state", Some(&node_bearer("strom-node-1"))).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn surfaces_do_not_accept_each_others_tokens() {
        let (app, _) = app().await;
        let (status, _) = send(&app, "GET", "/streams", &node_bearer("strom-node-1"), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        for (uri, past_auth) in [
            ("/endpoints", StatusCode::OK),
            ("/state", StatusCode::OK),
            ("/nodes/strom-node-1/desired", StatusCode::NOT_FOUND),
        ] {
            let (status, _) = send(&app, "GET", uri, &format!("Bearer {NORTH}"), None).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}");
            let (status, _) = send(&app, "GET", uri, &node_bearer("strom-node-1"), None).await;
            assert_eq!(status, past_auth, "{uri}");
        }
    }
}

#[cfg(test)]
mod key_exposure_tests {
    use super::*;
    use crate::webhook::tests::sink;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use serde_json::Value;
    use tower::ServiceExt;
    use weave_core::{
        DeclaredTopology, HopEndpointClass, HopProfile, NetworkAttachment, NetworkListeners,
        NodeCapabilities, NodeDescriptor, Passphrase, PortRange, RoleSet, SocketSpec, SrtEndpoint,
        SrtListener, StreamDestination, StreamTransport, Transport, TransportClass,
    };

    const PRODUCER_KEY: &str = "producer-passphrase-1";
    const CONSUMER_KEY: &str = "consumer-passphrase-1";

    fn attachment(network: &str, host: &str) -> NetworkAttachment {
        NetworkAttachment {
            id: network.to_string(),
            network: network.to_string(),
            dial: true,
            listeners: NetworkListeners {
                srt: Some(SrtListener {
                    host: host.to_string(),
                    port_range: PortRange {
                        start: 20_000,
                        end: 20_100,
                    },
                }),
                whip: None,
                whep: None,
                rist: None,
            },
        }
    }

    fn registration(id: &str, hosts: [&str; 2], merge: bool) -> NodeRegistration {
        let srt = || {
            HopEndpointClass::Transport(TransportClass {
                transport: Transport::Srt,
                roles: RoleSet::both(),
            })
        };
        let profile = |id: &str, merge| HopProfile {
            timed_delivery: None,
            id: id.to_string(),
            ingress: srt(),
            egress: srt(),
            max_egresses: None,
            media_behavior: weave_core::MediaBehavior::Unknown,
            merge,
            accepts: None,
        };
        let mut hop_profiles = vec![profile("srt-forward", false)];
        if merge {
            hop_profiles.push(profile("srt-merge", true));
        }
        NodeRegistration {
            protocol_version: PROTOCOL_VERSION,
            node: NodeDescriptor {
                inputs: Vec::new(),
                outputs: Vec::new(),
                id: id.to_string(),
                endpoint: format!("http://{id}"),
                status: NodeStatus::Ready,
                capabilities: NodeCapabilities { hop_profiles },
                topology: DeclaredTopology {
                    site: None,
                    attachments: vec![attachment("net-a", hosts[0]), attachment("net-b", hosts[1])],
                },
            },
            observation: None,
            endpoints: Vec::new(),
            hop_status: Vec::new(),
        }
    }

    fn endpoint(node: &str, passphrase: &str) -> StreamTransport {
        StreamTransport::Srt(SrtEndpoint {
            node: Some(node.to_string()),
            remote: None,
            via: Vec::new(),
            network: None,
            latency: None,
            passphrase: Some(Passphrase::new(passphrase)),
            format: None,
            accepts: None,
        })
    }

    fn keyed_stream(paths: u8) -> StreamDefinition {
        StreamDefinition {
            name: "feed".to_string(),
            enabled: true,
            format_policy: weave_core::FormatPolicy::Report,
            allow_cleartext_links: false,
            source: endpoint("node-a", PRODUCER_KEY),
            destinations: vec![StreamDestination {
                delivery: None,
                id: "studio".to_string(),
                paths,
                endpoint: endpoint("node-b", CONSUMER_KEY),
            }],
        }
    }

    async fn send(app: &Router, request: Request<Body>) -> (StatusCode, String) {
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    fn json_request(method: &str, uri: &str, value: &impl Serialize) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::IF_NONE_MATCH, "*")
            .body(Body::from(serde_json::to_vec(value).unwrap()))
            .unwrap()
    }

    fn get(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    fn hops(body: &str) -> Vec<DesiredHop> {
        serde_json::from_str(body).unwrap_or_else(|error| panic!("{error}: {body}"))
    }

    fn passphrases(hops: &[DesiredHop]) -> Vec<String> {
        hops.iter()
            .flat_map(DesiredHop::sockets)
            .filter_map(|socket| match socket {
                SocketSpec::Srt(socket) => socket.params().passphrase.as_ref(),
                _ => None,
            })
            .map(|passphrase| passphrase.expose().to_string())
            .collect()
    }

    async fn keys_reach_desired_hops_and_no_other_route(paths: u8) {
        let mut hooks = sink(StatusCode::OK).await;
        let webhooks = webhook::Emitter::new(webhook::Config {
            url: Some(hooks.url.clone()),
            ..webhook::Config::default()
        })
        .map(Arc::new);
        let state = AppState::hydrate(
            Arc::new(MemStore::new()),
            Duration::from_secs(60),
            Duration::from_secs(300),
            webhooks,
            LinkKeys::for_tests(),
        )
        .await
        .unwrap();
        let app = router(state.clone(), Guard::Disabled, NodeGuard::Disabled);
        for node in [
            registration("node-a", ["192.0.2.1", "198.51.100.1"], false),
            registration("node-b", ["192.0.2.2", "198.51.100.2"], true),
        ] {
            let (status, _) = send(&app, json_request("POST", ROUTE_NODE_REGISTER, &node)).await;
            assert_eq!(status, StatusCode::ACCEPTED);
        }
        let (status, _) = send(
            &app,
            json_request("POST", ROUTE_STREAMS, &keyed_stream(paths)),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        reconcile_tick(&state).await;

        let sender = passphrases(&hops(&send(&app, get("/nodes/node-a/desired")).await.1));
        let receiver = passphrases(&hops(&send(&app, get("/nodes/node-b/desired")).await.1));
        assert!(sender.contains(&PRODUCER_KEY.to_string()));
        assert!(receiver.contains(&CONSUMER_KEY.to_string()));
        let mut link_keys: Vec<String> = sender
            .iter()
            .filter(|key| key.as_str() != PRODUCER_KEY)
            .cloned()
            .collect();
        link_keys.sort();
        let mut receiver_link_keys: Vec<String> = receiver
            .iter()
            .filter(|key| key.as_str() != CONSUMER_KEY)
            .cloned()
            .collect();
        receiver_link_keys.sort();
        assert_eq!(link_keys, receiver_link_keys, "both ends share each key");
        link_keys.dedup();
        assert_eq!(link_keys.len(), usize::from(paths), "one key per path");

        let (_, resource) = send(&app, get("/streams/feed")).await;
        assert!(
            resource.contains(PRODUCER_KEY),
            "the manifest reads back as written"
        );

        let (status, plan) = send(
            &app,
            json_request("POST", ROUTE_STREAM_PLANS, &keyed_stream(paths)),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let planned: Value = serde_json::from_str(&plan).unwrap();
        let planned = hops(&planned["hops"].to_string());
        assert_eq!(passphrases(&planned), Vec::<String>::new());
        assert_eq!(
            planned.iter().any(|hop| hop.merge_ingress.is_some()),
            paths == 2,
            "{plan}"
        );
        assert!(plan.contains("\"pbkeylen\":32"));

        let mut events = Vec::new();
        while let Ok(delivery) =
            tokio::time::timeout(Duration::from_millis(300), hooks.next()).await
        {
            events.push(serde_json::to_string(&delivery.event).unwrap());
        }
        assert!(!events.is_empty(), "webhooks were sent");

        let routes = [
            ("/view", send(&app, get("/view")).await.1),
            (ROUTE_STATUS, send(&app, get(ROUTE_STATUS)).await.1),
            (ROUTE_STREAMS, send(&app, get(ROUTE_STREAMS)).await.1),
            ("/streams/feed", resource),
            (
                "/streams/feed/endpoints",
                send(&app, get("/streams/feed/endpoints")).await.1,
            ),
            (ROUTE_STREAM_PLANS, plan),
        ];
        for (route, body) in &routes {
            assert!(
                body.contains("node-a"),
                "{route} describes the stream: {body}"
            );
            for secret in &link_keys {
                assert!(!body.contains(secret), "{route} leaks a link key: {body}");
            }
        }
        for (route, body) in routes
            .iter()
            .filter(|(route, _)| !matches!(*route, ROUTE_STREAMS | "/streams/feed"))
        {
            for secret in [PRODUCER_KEY, CONSUMER_KEY] {
                assert!(!body.contains(secret), "{route} leaks a key: {body}");
            }
        }
        for event in &events {
            for secret in link_keys
                .iter()
                .map(String::as_str)
                .chain([PRODUCER_KEY, CONSUMER_KEY])
            {
                assert!(!event.contains(secret), "a webhook leaks a key: {event}");
            }
        }
    }

    #[tokio::test]
    async fn one_path_keys_reach_desired_hops_and_no_other_route() {
        keys_reach_desired_hops_and_no_other_route(1).await;
    }

    #[tokio::test]
    async fn two_path_keys_reach_desired_hops_and_no_other_route() {
        keys_reach_desired_hops_and_no_other_route(2).await;
    }
}

#[cfg(test)]
mod args_tests {
    use clap::CommandFactory;

    use super::Args;

    fn env_values_shown(command: &clap::Command) -> Vec<String> {
        command
            .get_arguments()
            .filter(|arg| arg.get_env().is_some() && !arg.is_hide_env_values_set())
            .map(|arg| arg.get_id().to_string())
            .chain(command.get_subcommands().flat_map(env_values_shown))
            .collect()
    }

    #[test]
    fn help_hides_every_env_value() {
        let shown = env_values_shown(&Args::command());
        assert!(
            shown.is_empty(),
            "--help prints the environment value of {shown:?}"
        );
    }
}
