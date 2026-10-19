mod config;
mod provision;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use axum::{Json, Router, routing::get};
use clap::Parser;
use reqwest::{Client, RequestBuilder, StatusCode};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use weave_core::auth::{self, Token};
use weave_core::logging::{ErrorChain, excerpt};
use weave_core::{
    AdapterDescriptor, AdapterKind, AudioCodec, AudioConstraint, DesiredHop, EgressStatus,
    EndpointDescriptor, EndpointKind, FormatConstraint, HopEndpointClass, HopProfile, HopState,
    HopStatus, LinkCondition, LinkStats, NodeCapabilities, NodeDescriptor, NodeHeartbeat,
    NodeRegistration, NodeStatus, PROTOCOL_VERSION, RoleSet, SocketRole, SocketSpec, SocketStatus,
    SrtSocket, Transport, TransportClass, VideoCodec, VideoConstraint,
};
use weave_strom::{
    ElementStats, FlowSpec, FlowStats, SessionBytes, SessionStats, StromClient, StromError,
    StromFlow, WebRtcStats, flow_spec_from_hop, parse_flow_stats, parse_webrtc_stats,
};

use config::AdapterConfig;
use provision::{
    Side, SideObservation, StallTracker, diff_hops, hop_state, resolved_addr, rist_condition,
    socket_condition, webrtc_condition,
};

#[derive(Debug, Parser)]
#[command(
    name = "weave-adapter-strom",
    version,
    about = "open-weave southbound adapter for Strom"
)]
struct Args {
    #[arg(help = "Path to the adapter config file (YAML)")]
    #[arg(long, env = "WEAVE_NODE_CONFIG", hide_env_values = true)]
    config: PathBuf,
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

#[tokio::main]
async fn main() -> Result<()> {
    weave_core::logging::init();

    let args = Args::parse();
    let config = AdapterConfig::load(&args.config)?;
    let public_endpoint = config.node.public_endpoint();

    let token = config.node.resolve_southbound_token()?;
    if token.is_none() {
        tracing::warn!(
            "{}=1: calling southbound without authentication",
            auth::AUTH_DISABLED_VAR
        );
    }
    let southbound = Southbound::new(config.node.southbound_url.clone(), token);
    let strom_token = config.strom.resolve_token();
    let strom_auth = strom_token.is_some();
    let strom = StromClient::with_timeout(
        &config.strom.url,
        Duration::from_secs(config.strom.request_timeout_secs),
    )
    .with_token(strom_token);
    let health_server = spawn_health_server(config.node.listen.clone());

    tracing::info!(
        node_id = %config.node.id,
        strom_url = %config.strom.url,
        southbound_url = %config.node.southbound_url,
        poll_interval_secs = config.strom.poll_interval_secs,
        strom_auth,
        topology = ?config.node.topology,
        "Strom adapter starting"
    );

    tokio::select! {
        result = weave_core::shutdown::signal() => {
            result.context("waiting for shutdown signal")?;
            tracing::info!("Strom adapter shutting down");
            health_server.abort();
            Ok(())
        }
        result = sync_loop(&southbound, &strom, &config, &public_endpoint) => {
            health_server.abort();
            result
        }
    }
}

struct Southbound {
    http: Client,
    url: String,

    token: Option<Token>,
}

impl Southbound {
    fn new(url: String, token: Option<Token>) -> Self {
        Self {
            http: Client::builder()
                .connect_timeout(weave_strom::CONNECT_TIMEOUT)
                .timeout(weave_strom::REQUEST_TIMEOUT)
                .build()
                .expect("building the HTTP client"),
            url,
            token,
        }
    }

    fn get(&self, path: &str) -> RequestBuilder {
        self.authorized(self.http.get(self.join(path)))
    }

    fn post(&self, path: &str) -> RequestBuilder {
        self.authorized(self.http.post(self.join(path)))
    }

    fn authorized(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.token {
            Some(token) => request.header(reqwest::header::AUTHORIZATION, token.header_value()),
            None => request,
        }
    }

    fn join(&self, path: &str) -> String {
        format!("{}{path}", self.url.trim_end_matches('/'))
    }
}

fn spawn_health_server(addr: String) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let app = Router::new().route("/health", get(health));
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .with_context(|| format!("binding Strom adapter health listener on {addr}"))?;
        tracing::info!(%addr, "Strom adapter health API listening");
        axum::serve(listener, app)
            .await
            .context("Strom adapter health server error")?;
        Ok(())
    })
}

async fn sync_loop(
    southbound: &Southbound,
    strom: &StromClient,
    config: &AdapterConfig,
    public_endpoint: &str,
) -> Result<()> {
    let mut registered = false;
    let mut strom_checked = false;
    let mut tracker = StallTracker::default();
    let mut last_desired = Vec::new();
    let interval = Duration::from_secs(config.strom.poll_interval_secs);

    loop {
        if !strom_checked {
            match check_strom_version(strom).await {
                Ok(()) => strom_checked = true,
                Err(error) if error.is::<UnsupportedStrom>() => {
                    tracing::error!(error = %ErrorChain(error.as_ref()), "Strom adapter cannot drive this Strom; exiting");
                    return Err(error);
                }
                Err(error) => {
                    tracing::warn!(error = %ErrorChain(error.as_ref()), "Strom version check failed")
                }
            }
        }

        match sync_once(
            southbound,
            strom,
            config,
            public_endpoint,
            registered,
            &mut tracker,
            &mut last_desired,
        )
        .await
        {
            Ok(next_registered) => registered = next_registered,
            Err(error) if error.is::<RegistrationRejected>() => {
                tracing::error!(
                    node_id = %config.node.id,
                    error = %ErrorChain(error.as_ref()),
                    "Strom adapter cannot register with this control plane; exiting"
                );
                return Err(error);
            }
            Err(error) => {
                registered = false;
                tracing::warn!(error = %ErrorChain(error.as_ref()), "Strom adapter sync failed");
            }
        }

        tokio::time::sleep(interval).await;
    }
}

async fn sync_once(
    southbound: &Southbound,
    strom: &dyn FlowApi,
    config: &AdapterConfig,
    public_endpoint: &str,
    registered: bool,
    tracker: &mut StallTracker,
    last_desired: &mut Vec<DesiredHop>,
) -> Result<bool> {
    let node_id = &config.node.id;
    let (status, flows) = match strom.list_flows().await {
        Ok(flows) => (NodeStatus::Ready, Some(flows)),
        Err(error) => {
            tracing::warn!(error = %ErrorChain(&error), "Strom observation failed");
            (NodeStatus::Degraded, None)
        }
    };
    let endpoints = strom_endpoints(node_id, flows.as_deref().unwrap_or_default());
    let listener_host = config
        .node
        .topology
        .attachments
        .iter()
        .find_map(|attachment| attachment.listeners.srt.as_ref())
        .map(|listener| listener.host.as_str());

    if !registered {
        desired_before_registering(southbound, node_id, last_desired).await?;
        let hop_status = match flows.as_deref() {
            Some(flows) => {
                hop_statuses(
                    strom,
                    last_desired,
                    flows,
                    listener_host,
                    &std::collections::HashSet::new(),
                    tracker,
                )
                .await
            }
            None => pending_statuses(last_desired, listener_host),
        };
        let registration = registration(config, public_endpoint, status, endpoints, hop_status);
        register_node(southbound, &registration).await?;
        return Ok(true);
    }

    let hop_status = hop_status(
        southbound,
        strom,
        node_id,
        flows.as_deref(),
        listener_host,
        tracker,
        last_desired,
    )
    .await;

    let registration = registration(
        config,
        public_endpoint,
        status,
        endpoints.clone(),
        hop_status.clone(),
    );

    let heartbeat = NodeHeartbeat {
        node_id: node_id.clone(),
        status,
        endpoints,
        hop_status,
    };

    if heartbeat_node(southbound, &heartbeat).await? == StatusCode::NOT_FOUND {
        register_node(southbound, &registration).await?;
    }

    Ok(true)
}

#[async_trait::async_trait]
#[allow(
    clippy::double_must_use,
    reason = "async_trait emits must_use on futures"
)]
trait FlowApi {
    async fn list_flows(&self) -> Result<Vec<StromFlow>, StromError>;
    async fn create_flow(&self, spec: &FlowSpec) -> Result<String, StromError>;
    async fn start_flow(&self, id: &str) -> Result<(), StromError>;
    async fn stop_flow(&self, id: &str) -> Result<(), StromError>;
    async fn delete_flow(&self, id: &str) -> Result<(), StromError>;
    async fn srt_stats(&self, id: &str) -> Result<Value, StromError>;
    async fn webrtc_stats(&self, id: &str) -> Result<Value, StromError>;
}

#[async_trait::async_trait]
impl FlowApi for StromClient {
    async fn list_flows(&self) -> Result<Vec<StromFlow>, StromError> {
        StromClient::list_flows(self).await
    }
    async fn create_flow(&self, spec: &FlowSpec) -> Result<String, StromError> {
        StromClient::create_flow(self, spec).await
    }
    async fn start_flow(&self, id: &str) -> Result<(), StromError> {
        StromClient::start_flow(self, id).await
    }
    async fn stop_flow(&self, id: &str) -> Result<(), StromError> {
        StromClient::stop_flow(self, id).await
    }
    async fn delete_flow(&self, id: &str) -> Result<(), StromError> {
        StromClient::delete_flow(self, id).await
    }
    async fn srt_stats(&self, id: &str) -> Result<Value, StromError> {
        StromClient::srt_stats(self, id).await
    }
    async fn webrtc_stats(&self, id: &str) -> Result<Value, StromError> {
        StromClient::webrtc_stats(self, id).await
    }
}

async fn hop_status(
    southbound: &Southbound,
    strom: &dyn FlowApi,
    node_id: &str,
    flows: Option<&[StromFlow]>,
    listener_host: Option<&str>,
    tracker: &mut StallTracker,
    last_desired: &mut Vec<DesiredHop>,
) -> Vec<HopStatus> {
    let Some(flows) = flows else {
        return pending_statuses(last_desired, listener_host);
    };
    match provision(
        southbound,
        strom,
        node_id,
        flows,
        listener_host,
        tracker,
        last_desired,
    )
    .await
    {
        Ok(hop_status) => hop_status,
        Err(error) => {
            tracing::warn!(error = %ErrorChain(error.as_ref()), "provisioning desired hops failed; reporting the last desired hops");
            hop_statuses(
                strom,
                last_desired,
                flows,
                listener_host,
                &std::collections::HashSet::new(),
                tracker,
            )
            .await
        }
    }
}

fn pending_statuses(desired: &[DesiredHop], listener_host: Option<&str>) -> Vec<HopStatus> {
    let idle = |spec: &SocketSpec| SocketStatus {
        condition: LinkCondition::Idle,
        resolved: resolved_addr(spec, listener_host),
        stats: None,
    };
    desired
        .iter()
        .map(|hop| HopStatus {
            delivery: None,
            id: hop.id.clone(),
            node_id: hop.node_id.clone(),
            revision: hop.revision(),
            state: HopState::Pending,
            ingress: idle(&hop.ingress),
            merge_ingress: None,
            egresses: hop
                .egresses
                .iter()
                .map(|egress| EgressStatus {
                    branch_id: egress.branch_id.clone(),
                    status: idle(&egress.socket),
                })
                .collect(),
        })
        .collect()
}

async fn desired_before_registering(
    southbound: &Southbound,
    node_id: &str,
    last_desired: &mut Vec<DesiredHop>,
) -> Result<()> {
    let response = southbound
        .get(&format!("/nodes/{node_id}/desired"))
        .send()
        .await
        .context("fetching desired hops before registering")?;
    let status = response.status();
    if status.is_server_error() {
        let body = response_body(response).await;
        bail!("southbound desired request failed before registering: {status}: {body}");
    }
    if status.is_success() {
        match response.json::<Vec<DesiredHop>>().await {
            Ok(desired) => *last_desired = desired,
            Err(error) => {
                tracing::warn!(error = %ErrorChain(&error), "desired hops before registering did not decode");
            }
        }
    }
    Ok(())
}

async fn provision(
    southbound: &Southbound,
    strom: &dyn FlowApi,
    node_id: &str,
    flows: &[StromFlow],
    listener_host: Option<&str>,
    tracker: &mut StallTracker,
    last_desired: &mut Vec<DesiredHop>,
) -> Result<Vec<HopStatus>> {
    let desired = fetch_desired(southbound, node_id).await?;
    let hop_status = reconcile(strom, &desired, flows, listener_host, tracker).await;
    *last_desired = desired;
    Ok(hop_status)
}

async fn reconcile(
    flow_api: &dyn FlowApi,
    desired: &[DesiredHop],
    flows: &[StromFlow],
    listener_host: Option<&str>,
    tracker: &mut StallTracker,
) -> Vec<HopStatus> {
    let plan = diff_hops(desired, flows);

    for flow_id in &plan.delete {
        match flow_api.delete_flow(flow_id).await {
            Ok(()) => tracing::info!(flow_id = %flow_id, "deleted undesired managed flow"),
            Err(error) => {
                tracing::warn!(flow_id = %flow_id, error = %ErrorChain(&error), "deleting flow failed")
            }
        }
    }

    let mut failed = std::collections::HashSet::new();
    for hop in &plan.create {
        if let Err(error) = provision_hop(flow_api, hop).await {
            tracing::warn!(hop = %hop.id, error = %ErrorChain(error.as_ref()), "provisioning hop failed");
            failed.insert(hop.id.clone());
        }
    }
    for flow_id in &plan.start {
        if let Err(error) = flow_api.start_flow(flow_id).await {
            tracing::warn!(flow_id = %flow_id, error = %ErrorChain(&error), "starting adopted flow failed");
            if let Some(flow) = flows.iter().find(|flow| &flow.id == flow_id) {
                failed.insert(flow.name.clone());
            }
        }
    }

    let refreshed;
    let current: &[StromFlow] = if plan.is_empty() {
        flows
    } else {
        refreshed = flow_api.list_flows().await.unwrap_or_default();
        &refreshed
    };

    let desired_ids: std::collections::HashSet<&str> =
        desired.iter().map(|h| h.id.as_str()).collect();
    tracker.retain(&desired_ids);
    let statuses = hop_statuses(flow_api, desired, current, listener_host, &failed, tracker).await;
    redial_unconnected_callers(flow_api, desired, current, tracker).await;
    statuses
}

async fn redial_unconnected_callers(
    flow_api: &dyn FlowApi,
    desired: &[DesiredHop],
    flows: &[StromFlow],
    tracker: &mut StallTracker,
) {
    for hop in desired {
        let Some(flow) = flows
            .iter()
            .find(|flow| flow.name == hop.id && flow.running)
        else {
            continue;
        };
        if !tracker.caller_due_restart(&hop.id) {
            continue;
        }
        let restarted = match flow_api.stop_flow(&flow.id).await {
            Ok(()) => flow_api.start_flow(&flow.id).await,
            Err(error) => Err(error),
        };
        match restarted {
            Ok(()) => tracing::info!(
                hop = %hop.id,
                flow_id = %flow.id,
                "restarted a flow whose SRT caller has not finished a handshake"
            ),
            Err(error) => tracing::warn!(
                hop = %hop.id,
                flow_id = %flow.id,
                error = %ErrorChain(&error),
                "restarting a flow whose SRT caller has not finished a handshake failed"
            ),
        }
    }
}

async fn hop_statuses(
    strom: &dyn FlowApi,
    desired: &[DesiredHop],
    flows: &[StromFlow],
    listener_host: Option<&str>,
    failed: &std::collections::HashSet<String>,
    tracker: &mut StallTracker,
) -> Vec<HopStatus> {
    let mut statuses = Vec::with_capacity(desired.len());
    for hop in desired {
        let flow = flows.iter().find(|f| f.name == hop.id);
        let srt = match flow {
            Some(flow) => match strom.srt_stats(&flow.id).await {
                Ok(value) => Some(parse_flow_stats(&value)),
                Err(error) => {
                    tracing::debug!(hop = %hop.id, error = %ErrorChain(&error), "srt-stats unavailable");
                    None
                }
            },
            None => None,
        };
        let webrtc = match flow {
            Some(flow) if carries_webrtc(hop) => match strom.webrtc_stats(&flow.id).await {
                Ok(value) => Some(parse_webrtc_stats(&value)),
                Err(error) => {
                    tracing::debug!(hop = %hop.id, error = %ErrorChain(&error), "webrtc-stats unavailable");
                    None
                }
            },
            _ => None,
        };
        let running = flow.is_some_and(|f| f.running);
        let gst_paused = flow.and_then(|f| f.gst_state.as_deref()) == Some("Paused");

        let mut observe = |side: Side, spec: &SocketSpec, reading: &SocketReading| {
            let bytes = match &reading.sessions {
                Some(sessions) => {
                    let sessions: Vec<(&str, i64)> = sessions
                        .iter()
                        .map(|(key, bytes)| (key.as_str(), *bytes))
                        .collect();
                    Some(tracker.session_total(&hop.id, side, &sessions))
                }
                None => reading.bytes,
            };
            let stalled = tracker.observe(
                &hop.id,
                side,
                SideObservation {
                    bytes,
                    running,
                    gst_paused: gst_paused && matches!(spec, SocketSpec::Srt(_)),
                },
            );
            let condition = reading.condition(spec, tracker.advanced(&hop.id, side), stalled);
            SocketStatus {
                condition,
                resolved: resolved_addr(spec, listener_host),
                stats: reading.stats,
            }
        };

        let ingress_handshaken = srt
            .as_ref()
            .and_then(FlowStats::ingress)
            .is_some_and(|element| element.handshaken || element.connected);
        let ingress = SocketReading::ingress(&hop.ingress, srt.as_ref(), webrtc.as_ref());
        let ingress = observe(Side::Ingress, &hop.ingress, &ingress);
        let egresses = hop
            .egresses
            .iter()
            .enumerate()
            .map(|(index, egress)| {
                let reading =
                    SocketReading::egress(&egress.socket, index, srt.as_ref(), webrtc.as_ref());
                EgressStatus {
                    branch_id: egress.branch_id.clone(),
                    status: observe(Side::Egress(index), &egress.socket, &reading),
                }
            })
            .collect();
        if running
            && srt.is_some()
            && matches!(hop.ingress, SocketSpec::Srt(SrtSocket::Connect { .. }))
        {
            tracker.note_caller(&hop.id, ingress_handshaken);
        }

        statuses.push(HopStatus {
            delivery: None,
            id: hop.id.clone(),
            node_id: hop.node_id.clone(),
            revision: if flow.is_none_or(|flow| !provision::flow_drifted(flow, hop)) {
                hop.revision()
            } else {
                String::new()
            },
            state: hop_state(flow, failed.contains(&hop.id)),
            ingress,
            merge_ingress: None,
            egresses,
        });
    }
    statuses
}

fn carries_webrtc(hop: &DesiredHop) -> bool {
    std::iter::once(&hop.ingress)
        .chain(hop.egresses.iter().map(|egress| &egress.socket))
        .any(|spec| matches!(spec, SocketSpec::Whip(_) | SocketSpec::Whep(_)))
}

#[derive(Debug, Default)]
struct SocketReading {
    bytes: Option<i64>,

    sessions: Option<Vec<(String, i64)>>,

    connected: bool,
    rate_mbps: f64,
    stats: Option<LinkStats>,
}

impl SocketReading {
    fn ingress(spec: &SocketSpec, srt: Option<&FlowStats>, webrtc: Option<&WebRtcStats>) -> Self {
        match spec {
            SocketSpec::Srt(_) => Self::srt(srt.and_then(FlowStats::ingress), |e| e.bytes_received),
            SocketSpec::Whip(_) | SocketSpec::Whep(_) => webrtc
                .map(|stats| Self::sessions(stats.ingress(), |b| b.received))
                .unwrap_or_default(),
            SocketSpec::Rist(_) => Self::srt_egresses(srt),
            SocketSpec::Device(_) => Self::default(),
        }
    }

    fn egress(
        spec: &SocketSpec,
        index: usize,
        srt: Option<&FlowStats>,
        webrtc: Option<&WebRtcStats>,
    ) -> Self {
        match spec {
            SocketSpec::Srt(_) => Self::srt(srt.and_then(|s| s.egress_at(index)), |e| e.bytes_sent),
            SocketSpec::Whip(_) | SocketSpec::Whep(_) => webrtc
                .map(|stats| Self::sessions(stats.egress_at(index), |b| b.sent))
                .unwrap_or_default(),
            SocketSpec::Rist(_) => Self::srt_ingress(srt),
            SocketSpec::Device(_) => Self::default(),
        }
    }

    fn srt(element: Option<&ElementStats>, bytes: fn(&ElementStats) -> i64) -> Self {
        Self {
            bytes: element.map(bytes),
            connected: element.is_some_and(|e| e.connected),
            rate_mbps: element.map_or(0.0, |e| e.rate_mbps),
            stats: element.map(LinkStats::from),
            ..Self::default()
        }
    }

    fn srt_ingress(srt: Option<&FlowStats>) -> Self {
        let element = srt.and_then(FlowStats::ingress);
        Self {
            bytes: element.map(|e| e.bytes_received),
            connected: element.is_some_and(|e| e.connected),
            ..Self::default()
        }
    }

    fn srt_egresses(srt: Option<&FlowStats>) -> Self {
        let sinks: Vec<&ElementStats> = srt.map(|s| s.egresses().collect()).unwrap_or_default();
        Self {
            bytes: srt.map(|_| sinks.iter().map(|e| e.bytes_sent).sum()),
            connected: sinks.iter().any(|e| e.connected),
            ..Self::default()
        }
    }

    fn sessions(block: Option<&SessionStats>, bytes: fn(&SessionBytes) -> i64) -> Self {
        Self {
            sessions: Some(block.map_or_else(Vec::new, |block| {
                block
                    .by_session
                    .iter()
                    .map(|session| (session.key.clone(), bytes(session)))
                    .collect()
            })),
            connected: block.is_some_and(|b| b.sessions > 0),
            ..Self::default()
        }
    }

    fn condition(&self, spec: &SocketSpec, advanced: bool, stalled: bool) -> LinkCondition {
        match spec {
            SocketSpec::Srt(socket) => {
                socket_condition(socket.role(), self.connected, self.rate_mbps, stalled)
            }
            SocketSpec::Whip(socket) | SocketSpec::Whep(socket) => {
                webrtc_condition(socket.role, self.connected, advanced, stalled)
            }
            SocketSpec::Rist(socket) => {
                rist_condition(socket.role(), self.connected, advanced, stalled)
            }
            SocketSpec::Device(_) => LinkCondition::Idle,
        }
    }
}

async fn provision_hop(strom: &dyn FlowApi, hop: &DesiredHop) -> Result<()> {
    let spec = flow_spec_from_hop(hop).with_context(|| format!("mapping hop {}", hop.id))?;
    let id = strom
        .create_flow(&spec)
        .await
        .with_context(|| format!("creating hop {}", hop.id))?;
    strom
        .start_flow(&id)
        .await
        .with_context(|| format!("starting hop {}", hop.id))?;
    tracing::info!(hop = %hop.id, flow_id = %id, "provisioned hop");
    Ok(())
}

async fn fetch_desired(southbound: &Southbound, node_id: &str) -> Result<Vec<DesiredHop>> {
    let response = southbound
        .get(&format!("/nodes/{node_id}/desired"))
        .send()
        .await
        .context("fetching desired hops")?;
    ensure_success(response, "southbound desired request failed")
        .await?
        .json::<Vec<DesiredHop>>()
        .await
        .context("decoding desired hops")
}

fn registration(
    config: &AdapterConfig,
    public_endpoint: &str,
    status: NodeStatus,
    endpoints: Vec<EndpointDescriptor>,
    hop_status: Vec<HopStatus>,
) -> NodeRegistration {
    NodeRegistration {
        protocol_version: PROTOCOL_VERSION,
        node: NodeDescriptor {
            id: config.node.id.clone(),
            endpoint: public_endpoint.to_string(),
            status,
            capabilities: NodeCapabilities {
                adapters: vec![AdapterDescriptor {
                    name: "strom".to_string(),
                    kind: AdapterKind::Strom,
                }],
                hop_profiles: strom_hop_profiles(),
            },
            topology: config.node.topology.clone(),
        },
        endpoints,
        hop_status,
    }
}

fn transport_class(transport: Transport, roles: RoleSet) -> HopEndpointClass {
    HopEndpointClass::Transport(TransportClass { transport, roles })
}

fn strom_hop_profiles() -> Vec<HopProfile> {
    vec![
        HopProfile {
            timed_delivery: None,
            id: "srt-forward".to_string(),
            ingress: transport_class(Transport::Srt, RoleSet::both()),
            egress: transport_class(Transport::Srt, RoleSet::both()),
            max_egresses: None,
            media_behavior: weave_core::MediaBehavior::Preserve,
            merge: false,
            accepts: Some(FormatConstraint::default()),
        },
        HopProfile {
            timed_delivery: None,
            id: "whip-to-srt".to_string(),
            ingress: transport_class(Transport::Whip, RoleSet::only(SocketRole::Listen)),
            egress: transport_class(Transport::Srt, RoleSet::both()),
            max_egresses: None,
            media_behavior: weave_core::MediaBehavior::Unknown,
            merge: false,
            accepts: Some(whip_input_accepts()),
        },
        HopProfile {
            timed_delivery: None,
            id: "srt-to-whep".to_string(),
            ingress: transport_class(Transport::Srt, RoleSet::both()),
            egress: transport_class(Transport::Whep, RoleSet::only(SocketRole::Listen)),
            max_egresses: None,
            media_behavior: weave_core::MediaBehavior::Unknown,
            merge: false,
            accepts: None,
        },
        HopProfile {
            timed_delivery: None,
            id: "whip-to-whep".to_string(),
            ingress: transport_class(Transport::Whip, RoleSet::only(SocketRole::Listen)),
            egress: transport_class(Transport::Whep, RoleSet::only(SocketRole::Listen)),
            max_egresses: None,
            media_behavior: weave_core::MediaBehavior::Unknown,
            merge: false,
            accepts: Some(whip_input_accepts()),
        },
        HopProfile {
            timed_delivery: None,
            id: "srt-to-rist".to_string(),
            ingress: transport_class(Transport::Srt, RoleSet::both()),
            egress: transport_class(Transport::Rist, RoleSet::only(SocketRole::Connect)),
            max_egresses: None,
            media_behavior: weave_core::MediaBehavior::Unknown,
            merge: false,
            accepts: None,
        },
        HopProfile {
            timed_delivery: None,
            id: "rist-to-srt".to_string(),
            ingress: transport_class(Transport::Rist, RoleSet::only(SocketRole::Listen)),
            egress: transport_class(Transport::Srt, RoleSet::both()),
            max_egresses: None,
            media_behavior: weave_core::MediaBehavior::Unknown,
            merge: false,
            accepts: None,
        },
    ]
}

fn whip_input_accepts() -> FormatConstraint {
    FormatConstraint {
        container: None,
        video: Some(VideoConstraint {
            codec: Some(vec![VideoCodec::H264]),
            ..VideoConstraint::default()
        }),
        audio: Some(AudioConstraint {
            codec: Some(vec![AudioCodec::Opus]),
            ..AudioConstraint::default()
        }),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Strom {found} is older than {min}, the oldest release this adapter supports")]
struct UnsupportedStrom {
    found: String,
    min: String,
}

async fn check_strom_version(strom: &StromClient) -> Result<()> {
    let found = strom.version().await.context("reading Strom version")?;
    let (major, minor, patch) = weave_strom::MIN_VERSION;
    match weave_strom::is_supported(&found) {
        Some(true) => tracing::info!(strom_version = %found, "Strom version supported"),
        Some(false) => {
            return Err(UnsupportedStrom {
                found,
                min: format!("{major}.{minor}.{patch}"),
            }
            .into());
        }
        None => {
            tracing::warn!(strom_version = %found, "cannot read Strom version; not checking it")
        }
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("southbound rejected registration permanently: {status}: {body}")]
struct RegistrationRejected {
    status: StatusCode,
    body: String,
}

async fn register_node(southbound: &Southbound, registration: &NodeRegistration) -> Result<()> {
    let response = southbound
        .post("/nodes/register")
        .json(registration)
        .send()
        .await
        .context("registering Strom adapter")?;

    if matches!(
        response.status(),
        StatusCode::CONFLICT | StatusCode::FORBIDDEN
    ) {
        return Err(RegistrationRejected {
            status: response.status(),
            body: response_body(response).await,
        }
        .into());
    }

    ensure_success(response, "southbound node registration failed").await?;
    tracing::info!(
        node_id = %registration.node.id,
        protocol_version = registration.protocol_version,
        "Strom adapter registered"
    );
    Ok(())
}

async fn heartbeat_node(southbound: &Southbound, heartbeat: &NodeHeartbeat) -> Result<StatusCode> {
    let response = southbound
        .post(&format!("/nodes/{}/heartbeat", heartbeat.node_id))
        .json(heartbeat)
        .send()
        .await
        .context("sending Strom adapter heartbeat")?;

    let status = response.status();
    if status.is_success() || status == StatusCode::NOT_FOUND {
        return Ok(status);
    }

    let body = response_body(response).await;
    bail!("southbound heartbeat failed: {status}: {body}")
}

async fn ensure_success(response: reqwest::Response, context: &str) -> Result<reqwest::Response> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }

    let body = response_body(response).await;
    bail!("{context}: {status}: {body}")
}

async fn response_body(response: reqwest::Response) -> String {
    match response.text().await {
        Ok(body) => excerpt(&body),
        Err(error) => format!("<failed to read body: {}>", ErrorChain(&error)),
    }
}

fn strom_endpoints(node_id: &str, flows: &[StromFlow]) -> Vec<EndpointDescriptor> {
    flows
        .iter()
        .map(|flow| EndpointDescriptor {
            id: format!("strom:{node_id}:flow:{}", flow.id),
            label: flow.name.clone(),
            node_id: Some(node_id.to_string()),
            kind: EndpointKind::Flow,
            transports: flow_transports(flow),
            metadata: json!({
                "source": "strom",
                "strom_flow_id": flow.id,
                "running": flow.running,
            }),
        })
        .collect()
}

fn flow_transports(flow: &StromFlow) -> Vec<String> {
    let mut transports = Vec::new();

    for element in &flow.elements {
        collect_transport(&mut transports, &element.element_type);
        for value in element.properties.values() {
            collect_transport_value(&mut transports, value);
        }
    }

    for block in &flow.blocks {
        collect_transport(&mut transports, &block.block_definition_id);
        for value in block.properties.values() {
            collect_transport_value(&mut transports, value);
        }
    }

    transports
}

fn collect_transport(transports: &mut Vec<String>, text: &str) {
    let lower = text.to_ascii_lowercase();
    let transport = if lower.contains("srt") {
        Some("srt")
    } else if lower.contains("whip") || lower.contains("whep") || lower.contains("webrtc") {
        Some("webrtc")
    } else if lower.contains("aes67") {
        Some("aes67")
    } else if lower.contains("ndi") {
        Some("ndi")
    } else if lower.contains("decklink") {
        Some("decklink")
    } else if lower.contains("rtp") {
        Some("rtp")
    } else {
        None
    };

    if let Some(name) = transport {
        push_unique(transports, name);
    }
}

fn collect_transport_value(transports: &mut Vec<String>, value: &Value) {
    match value {
        Value::String(text) => collect_transport(transports, text),
        Value::Array(values) => {
            for value in values {
                collect_transport_value(transports, value);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_transport_value(transports, value);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if values.iter().any(|existing| existing == value) {
        return;
    }
    values.push(value.to_string());
}

async fn health() -> Json<weave_core::Health> {
    Json(weave_core::Health::OK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use weave_core::{DesiredEgress, HopRole, SignallingTransport};

    #[derive(Debug, Clone, PartialEq)]
    enum Op {
        Create(String),
        Start(String),
        Stop(String),
        Delete(String),
    }

    #[derive(Default)]
    struct RecordingFlowApi {
        ops: Mutex<Vec<Op>>,
        flows_after: Vec<(String, String)>,
        stats: Value,
        webrtc: Value,
    }

    impl RecordingFlowApi {
        fn listing(flows_after: &[(&str, &str)]) -> Self {
            Self {
                flows_after: flows_after
                    .iter()
                    .map(|(name, id)| ((*name).to_string(), (*id).to_string()))
                    .collect(),
                ..Self::default()
            }
        }

        fn with_stats(mut self, stats: Value) -> Self {
            self.stats = stats;
            self
        }

        fn with_webrtc(mut self, webrtc: Value) -> Self {
            self.webrtc = webrtc;
            self
        }

        fn ops(&self) -> Vec<Op> {
            self.ops.lock().expect("ops lock").clone()
        }

        fn record(&self, op: Op) {
            self.ops.lock().expect("ops lock").push(op);
        }
    }

    #[async_trait::async_trait]
    impl FlowApi for RecordingFlowApi {
        async fn list_flows(&self) -> Result<Vec<StromFlow>, StromError> {
            Ok(self
                .flows_after
                .iter()
                .map(|(name, id)| flow(name, id))
                .collect())
        }
        async fn create_flow(&self, spec: &FlowSpec) -> Result<String, StromError> {
            self.record(Op::Create(spec.name.clone()));
            Ok(format!("id-{}", spec.name))
        }
        async fn start_flow(&self, id: &str) -> Result<(), StromError> {
            self.record(Op::Start(id.to_string()));
            Ok(())
        }
        async fn stop_flow(&self, id: &str) -> Result<(), StromError> {
            self.record(Op::Stop(id.to_string()));
            Ok(())
        }
        async fn delete_flow(&self, id: &str) -> Result<(), StromError> {
            self.record(Op::Delete(id.to_string()));
            Ok(())
        }
        async fn srt_stats(&self, _id: &str) -> Result<Value, StromError> {
            Ok(self.stats.clone())
        }
        async fn webrtc_stats(&self, _id: &str) -> Result<Value, StromError> {
            Ok(self.webrtc.clone())
        }
    }

    fn flow(name: &str, id: &str) -> StromFlow {
        serde_json::from_value(json!({ "id": id, "name": name, "running": true }))
            .expect("flow fixture")
    }

    async fn southbound_answering(status: StatusCode, body: Value) -> Southbound {
        let app = Router::new().route(
            "/nodes/{node_id}/desired",
            get(move || async move { (status, Json(body)) }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Southbound::new(format!("http://{addr}"), None)
    }

    async fn strom_reporting(version: Value) -> StromClient {
        let app = Router::new().route(
            "/api/version",
            get(move || {
                let version = version.clone();
                async move { Json(json!({ "version": version })) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        StromClient::new(format!("http://{addr}"))
    }

    #[tokio::test]
    async fn a_strom_below_the_minimum_version_is_refused() {
        let error = check_strom_version(&strom_reporting(json!("0.6.8")).await)
            .await
            .expect_err("too old");

        assert!(error.is::<UnsupportedStrom>());
        assert!(error.to_string().contains("0.6.8"));
    }

    #[tokio::test]
    async fn a_supported_or_unreadable_strom_version_passes() {
        for version in ["0.6.9", "0.6.10", "not-a-version"] {
            let strom = strom_reporting(json!(version)).await;
            assert!(check_strom_version(&strom).await.is_ok(), "{version}");
        }
    }

    #[tokio::test]
    async fn a_strom_that_cannot_be_asked_is_not_a_refusal() {
        let strom = StromClient::new("http://127.0.0.1:1");

        let error = check_strom_version(&strom).await.expect_err("unreachable");

        assert!(!error.is::<UnsupportedStrom>());
    }

    #[tokio::test]
    async fn a_refused_first_registration_leaves_every_flow_alone() {
        let app = Router::new()
            .route(
                "/nodes/register",
                axum::routing::post(|| async {
                    (
                        StatusCode::CONFLICT,
                        Json(json!({ "code": "incompatible_protocol_version" })),
                    )
                }),
            )
            .route(
                "/nodes/{node_id}/desired",
                get(|| async { Json(json!([])) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let southbound = Southbound::new(format!("http://{addr}"), None);
        let config: AdapterConfig = serde_norway::from_str(&format!(
            "node:\n  id: strom-node-1\n  southbound_url: http://{addr}\n  listen: 127.0.0.1:0\n  topology: {{ attachments: [] }}\nstrom:\n  url: http://127.0.0.1:1\n"
        ))
        .unwrap();
        let strom = RecordingFlowApi::listing(&[("weave-old-sender", "id-old")]);

        let result = sync_once(
            &southbound,
            &strom,
            &config,
            "http://strom-node-1",
            false,
            &mut StallTracker::default(),
            &mut Vec::new(),
        )
        .await;

        assert!(result.is_err_and(|error| error.is::<RegistrationRejected>()));
        assert!(strom.ops().is_empty(), "{:?}", strom.ops());
    }

    #[tokio::test]
    async fn a_restarted_adapter_registers_the_hops_its_strom_runs() {
        let desired = hop("weave-feed-receiver-b", 20088);
        let registered = std::sync::Arc::new(Mutex::new(None::<NodeRegistration>));
        let seen = registered.clone();
        let answer = json!([desired]);
        let app = Router::new()
            .route(
                "/nodes/register",
                axum::routing::post(move |Json(body): Json<NodeRegistration>| async move {
                    let node_id = body.node.id.clone();
                    *seen.lock().unwrap() = Some(body);
                    Json(json!({ "status": "accepted", "node_id": node_id }))
                }),
            )
            .route(
                "/nodes/{node_id}/desired",
                get(move || async move { Json(answer) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let southbound = Southbound::new(format!("http://{addr}"), None);
        let config: AdapterConfig = serde_norway::from_str(&format!(
            "node:\n  id: strom-node-1\n  southbound_url: http://{addr}\n  listen: 127.0.0.1:0\n  topology: {{ attachments: [] }}\nstrom:\n  url: http://127.0.0.1:1\n"
        ))
        .unwrap();
        let strom = RecordingFlowApi::listing(&[(&desired.id, "id-feed")]);
        let mut last_desired = Vec::new();

        let next = sync_once(
            &southbound,
            &strom,
            &config,
            "http://strom-node-1",
            false,
            &mut StallTracker::default(),
            &mut last_desired,
        )
        .await
        .expect("registered");

        assert!(next);
        assert!(strom.ops().is_empty(), "{:?}", strom.ops());
        let registration = registered.lock().unwrap().take().expect("a registration");
        let reported: Vec<_> = registration
            .hop_status
            .iter()
            .map(|status| {
                (
                    status.id.as_str(),
                    status.state,
                    status.ingress.resolved.as_ref().map(|addr| addr.port),
                )
            })
            .collect();
        assert_eq!(
            reported,
            [(desired.id.as_str(), HopState::Provisioned, Some(20088))],
            "the first registration reports the flow Strom already runs"
        );
    }

    async fn provision_against(southbound: &Southbound, flows: &[StromFlow]) -> (bool, Vec<Op>) {
        let strom = RecordingFlowApi::default();
        let result = provision(
            southbound,
            &strom,
            "strom-node-1",
            flows,
            None,
            &mut StallTracker::default(),
            &mut Vec::new(),
        )
        .await;
        (result.is_ok(), strom.ops())
    }

    #[tokio::test]
    async fn a_desired_response_other_than_2xx_leaves_every_flow_alone() {
        let flows = [flow("weave-basic-sender", "id-managed")];
        let not_reconciled = json!({ "code": "node_not_found", "message": "no desired state" });
        for status in [
            StatusCode::NOT_FOUND,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let southbound = southbound_answering(status, not_reconciled.clone()).await;
            assert_eq!(
                provision_against(&southbound, &flows).await,
                (false, Vec::new()),
                "{status}"
            );
        }

        let unreachable = Southbound::new("http://127.0.0.1:1".to_string(), None);
        assert_eq!(
            provision_against(&unreachable, &flows).await,
            (false, Vec::new())
        );
    }

    #[tokio::test]
    async fn an_empty_desired_list_deletes_only_managed_flows() {
        let flows = [
            flow("weave-basic-sender", "id-managed"),
            flow("studio-mixer", "id-unmanaged"),
        ];
        let southbound = southbound_answering(StatusCode::OK, json!([])).await;
        assert_eq!(
            provision_against(&southbound, &flows).await,
            (true, vec![Op::Delete("id-managed".to_string())])
        );
    }

    fn hop(id: &str, port: u16) -> DesiredHop {
        DesiredHop {
            delivery: None,
            id: id.to_string(),
            node_id: "strom-node-1".to_string(),
            profile_id: "srt-forward".to_string(),
            role: HopRole::Sender,
            ingress: SocketSpec::srt_listen(port, 200),
            merge_ingress: None,
            egresses: vec![DesiredEgress {
                branch_id: "studio".to_string(),
                socket: SocketSpec::srt_connect("10.0.0.2", port + 1, 1000),
            }],
            tracks: None,
        }
    }

    #[tokio::test]
    async fn hop_status_reports_each_fanout_branch_independently() {
        let mut desired = hop("weave-fanout-sender", 7001);
        desired.egresses.push(DesiredEgress {
            branch_id: "preview".to_string(),
            socket: SocketSpec::srt_connect("10.0.0.3", 7003, 1000),
        });
        let flows = vec![flow("weave-fanout-sender", "id-fanout")];
        let fake = RecordingFlowApi::default().with_stats(json!({
            "stats": { "connections": {
                "srtsrc_0": { "connected": true, "callers": [
                    { "recv_rate_mbps": 4.5, "bytes_received": 1000 }
                ]},
                "srtsink_0": { "connected": true, "callers": [
                    { "send_rate_mbps": 4.4, "bytes_sent": 900,
                      "packets_sent_lost": 2 }
                ]},
                "srtsink_1": { "connected": false, "callers": [] }
            }}
        }));
        let mut tracker = StallTracker::default();

        let statuses = hop_statuses(
            &fake,
            &[desired],
            &flows,
            Some("10.0.0.1"),
            &std::collections::HashSet::new(),
            &mut tracker,
        )
        .await;

        let status = &statuses[0];
        assert_eq!(status.ingress.condition, LinkCondition::Flowing);
        assert_eq!(
            status.ingress.stats.as_ref().map(|stats| stats.rate_mbps),
            Some(4.5)
        );
        assert_eq!(status.egresses.len(), 2);
        assert_eq!(status.egresses[0].branch_id, "studio");
        assert_eq!(status.egresses[0].status.condition, LinkCondition::Flowing);
        assert_eq!(
            status.egresses[0]
                .status
                .resolved
                .as_ref()
                .map(|address| (address.host.as_str(), address.port)),
            Some(("10.0.0.2", 7002))
        );
        assert_eq!(
            status.egresses[0]
                .status
                .stats
                .as_ref()
                .map(|stats| (stats.rate_mbps, stats.packets_sent_lost)),
            Some((4.4, 2))
        );
        assert_eq!(status.egresses[1].branch_id, "preview");
        assert_eq!(
            status.egresses[1].status.condition,
            LinkCondition::Connecting
        );
        assert_eq!(
            status.egresses[1]
                .status
                .resolved
                .as_ref()
                .map(|address| (address.host.as_str(), address.port)),
            Some(("10.0.0.3", 7003))
        );
        assert_eq!(
            status.egresses[1]
                .status
                .stats
                .as_ref()
                .map(|stats| stats.rate_mbps),
            Some(0.0)
        );
    }

    #[tokio::test]
    async fn an_old_runtime_flow_cannot_acknowledge_new_desired_settings() {
        let mut desired = hop("weave-basic-sender", 7001);
        desired.ingress = SocketSpec::srt_listen(7001, 400);
        for latency in [200, 400] {
            let observed: StromFlow = serde_json::from_value(json!({
                "id": "flow", "name": desired.id, "running": true,
                "elements": [
                    { "element_type": "srtsrc", "properties": { "uri": format!("srt://:7001?mode=listener&latency={latency}") } },
                    { "element_type": "srtsink", "properties": { "uri": "srt://10.0.0.2:7002?mode=caller&latency=1000" } }
                ]
            })).unwrap();
            let statuses = hop_statuses(
                &RecordingFlowApi::default(),
                &[desired.clone()],
                &[observed],
                None,
                &std::collections::HashSet::new(),
                &mut StallTracker::default(),
            )
            .await;
            assert_eq!(statuses[0].revision == desired.revision(), latency == 400);
        }
    }

    #[tokio::test]
    async fn a_failed_start_of_an_adopted_flow_is_reported_as_failed() {
        let desired = hop("weave-basic-sender", 7001);
        let mut stopped = serde_json::to_value(flow_spec_from_hop(&desired).unwrap()).unwrap();
        stopped["id"] = json!("id-stopped");
        stopped["running"] = json!(false);
        let starts = std::sync::Arc::new(Mutex::new(Vec::new()));
        let seen = starts.clone();
        let app = Router::new()
            .route(
                "/api/flows",
                get(move || {
                    let stopped = stopped.clone();
                    async move { Json(json!({ "flows": [stopped] })) }
                }),
            )
            .route(
                "/api/flows/{id}/start",
                axum::routing::post(
                    move |axum::extract::Path(id): axum::extract::Path<String>| {
                        seen.lock().unwrap().push(id);
                        async {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({ "error": "injected start failure" })),
                            )
                        }
                    },
                ),
            )
            .route(
                "/api/flows/{id}/srt-stats",
                get(|| async { Json(json!({ "stats": { "connections": {} } })) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let strom = StromClient::new(format!("http://{addr}"));
        let mut tracker = StallTracker::default();
        let mut states = Vec::new();

        for _ in 0..2 {
            let flows = strom.list_flows().await.unwrap();
            assert_eq!(flows.len(), 1);
            assert!(!flows[0].running);
            let statuses = reconcile(
                &strom,
                std::slice::from_ref(&desired),
                &flows,
                None,
                &mut tracker,
            )
            .await;
            assert_eq!(statuses.len(), 1);
            assert_eq!(statuses[0].id, desired.id);
            states.push(statuses[0].state);
        }
        server.abort();
        let _ = server.await;

        assert_eq!(*starts.lock().unwrap(), ["id-stopped", "id-stopped"]);
        assert_eq!(
            states,
            [HopState::Failed, HopState::Failed],
            "a rejected start must be reported as failed while the flow remains stopped"
        );
    }

    #[tokio::test]
    async fn a_caller_ingress_left_unconnected_is_restarted_and_a_listener_is_not() {
        let unconnected = json!({
            "stats": { "connections": {
                "srtsrc_0": { "connected": false, "callers": [
                    { "bytes_received": 0 }
                ]},
                "srtsink_0": { "connected": false, "callers": [] }
            }}
        });
        let mut caller = hop("weave-feed-receiver-studio", 7002);
        caller.ingress = SocketSpec::srt_connect("10.0.0.1", 7002, 1000);
        let listener = hop("weave-feed-sender", 7000);
        let flows = vec![
            flow("weave-feed-receiver-studio", "id-caller"),
            flow("weave-feed-sender", "id-listener"),
        ];
        let fake = RecordingFlowApi::default().with_stats(unconnected);
        let mut tracker = StallTracker::default();
        let desired = vec![caller, listener];
        for _ in 0..5 {
            let _ = reconcile(&fake, &desired, &flows, None, &mut tracker).await;
        }
        assert!(fake.ops().is_empty(), "{:?}", fake.ops());
        let _ = reconcile(&fake, &desired, &flows, None, &mut tracker).await;
        assert_eq!(
            fake.ops(),
            vec![
                Op::Stop("id-caller".to_string()),
                Op::Start("id-caller".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn a_caller_that_shook_hands_but_carries_nothing_is_not_restarted() {
        let idle = json!({
            "stats": { "connections": {
                "srtsrc_0": { "mode": "caller", "connected": false, "callers": [
                    { "rtt_ms": 100.0, "negotiated_latency_ms": 1000,
                      "packets_received": 0, "bytes_received": 0 }
                ]},
                "srtsink_0": { "connected": false, "callers": [] }
            }}
        });
        let mut caller = hop("weave-feed-receiver-studio", 7002);
        caller.ingress = SocketSpec::srt_connect("10.0.0.1", 7002, 1000);
        let flows = vec![flow("weave-feed-receiver-studio", "id-caller")];
        let fake = RecordingFlowApi::default().with_stats(idle);
        let mut tracker = StallTracker::default();
        let desired = vec![caller];
        for _ in 0..50 {
            let _ = reconcile(&fake, &desired, &flows, None, &mut tracker).await;
        }
        assert!(fake.ops().is_empty(), "{:?}", fake.ops());
    }

    #[tokio::test]
    async fn reconcile_deletes_before_creating_on_same_ports() {
        let desired = vec![
            hop("weave-srt-latency-sender", 7001),
            hop("weave-srt-latency-receiver-studio", 7002),
        ];
        let flows = vec![
            flow("weave-basic-sender", "id-basic-sender"),
            flow("weave-basic-receiver-studio", "id-basic-receiver-studio"),
        ];
        let fake = RecordingFlowApi::listing(&[
            ("weave-srt-latency-sender", "id-weave-srt-latency-sender"),
            (
                "weave-srt-latency-receiver-studio",
                "id-weave-srt-latency-receiver-studio",
            ),
        ]);

        let mut tracker = StallTracker::default();
        let _ = reconcile(&fake, &desired, &flows, None, &mut tracker).await;

        let ops = fake.ops();
        let last_delete = ops
            .iter()
            .rposition(|op| matches!(op, Op::Delete(_)))
            .expect("a delete was recorded");
        let first_create = ops
            .iter()
            .position(|op| matches!(op, Op::Create(_)))
            .expect("a create was recorded");
        assert!(
            last_delete < first_create,
            "all deletes must precede all creates within one cycle: {ops:?}"
        );
    }

    fn recorded_webrtc(name: &str) -> Value {
        let text = match name {
            "poll-0" => include_str!("../../strom/src/testdata/webrtc-stats/whip-whep-poll-0.json"),
            "poll-1" => include_str!("../../strom/src/testdata/webrtc-stats/whip-whep-poll-1.json"),
            "poll-2" => include_str!("../../strom/src/testdata/webrtc-stats/whip-whep-poll-2.json"),
            "ended" => include_str!("../../strom/src/testdata/webrtc-stats/whip-whep-ended.json"),
            other => panic!("no recording {other}"),
        };
        serde_json::from_str(text).expect("recorded webrtc-stats")
    }

    fn flow_in_state(name: &str, gst_state: &str) -> StromFlow {
        serde_json::from_value(json!({
            "id": "id-a", "name": name, "running": true, "gst_state": gst_state,
        }))
        .expect("flow fixture")
    }

    fn signalling(transport: SignallingTransport, endpoint_id: &str) -> SocketSpec {
        SocketSpec::signalling(
            transport,
            SocketRole::Listen,
            "http://10.97.26.10:8080",
            endpoint_id,
        )
    }

    fn whip_to_srt_hop() -> DesiredHop {
        DesiredHop {
            delivery: None,
            id: "weave-browser-cam-receiver-output".to_string(),
            node_id: "strom-node-1".to_string(),
            profile_id: "whip-to-srt".to_string(),
            role: HopRole::Receiver,
            ingress: signalling(
                SignallingTransport::Whip,
                "weave-browser-cam-receiver-output",
            ),
            merge_ingress: None,
            egresses: vec![DesiredEgress {
                branch_id: "output".to_string(),
                socket: SocketSpec::srt_listen(7003, 1000),
            }],
            tracks: None,
        }
    }

    fn srt_to_whep_hop() -> DesiredHop {
        DesiredHop {
            delivery: None,
            id: "weave-browser-return-sender".to_string(),
            node_id: "strom-node-1".to_string(),
            profile_id: "srt-to-whep".to_string(),
            role: HopRole::Sender,
            ingress: SocketSpec::srt_listen(7001, 200),
            merge_ingress: None,
            egresses: vec![DesiredEgress {
                branch_id: "display".to_string(),
                socket: signalling(
                    SignallingTransport::Whep,
                    "weave-browser-return-receiver-display",
                ),
            }],
            tracks: None,
        }
    }

    async fn poll(
        hop: &DesiredHop,
        flow: StromFlow,
        srt: Value,
        webrtc: Value,
        tracker: &mut StallTracker,
    ) -> HopStatus {
        let fake = RecordingFlowApi::default()
            .with_stats(srt)
            .with_webrtc(webrtc);
        let mut statuses = hop_statuses(
            &fake,
            std::slice::from_ref(hop),
            &[flow],
            None,
            &std::collections::HashSet::new(),
            tracker,
        )
        .await;
        statuses.remove(0)
    }

    #[tokio::test]
    async fn a_whip_ingress_reads_its_sessions_not_the_flow_state() {
        let hop = whip_to_srt_hop();
        let mut tracker = StallTracker::default();
        let mut conditions = Vec::new();
        for recording in ["poll-0", "poll-1", "poll-2", "ended"] {
            let status = poll(
                &hop,
                flow_in_state(&hop.id, "Paused"),
                json!({}),
                recorded_webrtc(recording),
                &mut tracker,
            )
            .await;
            conditions.push(status.ingress.condition);
            assert_eq!(status.egresses[0].status.condition, LinkCondition::Idle);
        }
        assert_eq!(
            conditions,
            [
                LinkCondition::Connected,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
                LinkCondition::Idle,
            ]
        );
    }

    #[tokio::test]
    async fn a_whip_to_whep_hop_reads_each_side_from_its_own_block() {
        let mut hop = whip_to_srt_hop();
        hop.profile_id = "whip-to-whep".to_string();
        hop.egresses[0].socket =
            signalling(SignallingTransport::Whep, "weave-b2b-receiver-display");
        let mut tracker = StallTracker::default();
        let mut conditions = Vec::new();
        for recording in ["poll-0", "poll-1", "poll-2", "ended"] {
            let status = poll(
                &hop,
                flow_in_state(&hop.id, "Playing"),
                json!({}),
                recorded_webrtc(recording),
                &mut tracker,
            )
            .await;
            conditions.push((
                status.ingress.condition,
                status.egresses[0].status.condition,
            ));
        }
        assert_eq!(
            conditions,
            [
                (LinkCondition::Connected, LinkCondition::Connected),
                (LinkCondition::Flowing, LinkCondition::Flowing),
                (LinkCondition::Flowing, LinkCondition::Flowing),
                (LinkCondition::Idle, LinkCondition::Idle),
            ]
        );
    }

    fn whep_sessions(sessions: &[(&str, i64)]) -> Value {
        let connections: serde_json::Map<String, Value> = sessions
            .iter()
            .map(|(id, bytes)| {
                (
                    format!("whep_out_0:session_{id}:webrtcbin-{id}"),
                    json!({
                        "inbound_rtp": [],
                        "outbound_rtp": [{ "media_type": "video", "bytes": bytes }],
                    }),
                )
            })
            .collect();
        json!({ "flow_id": "id-a", "stats": { "connections": connections } })
    }

    #[tokio::test]
    async fn a_whep_egress_keeps_flowing_when_an_old_session_ends() {
        let hop = srt_to_whep_hop();
        let srt = json!({ "stats": { "connections": {
            "srt_in:srtsrc": { "connected": true, "callers": [
                { "recv_rate_mbps": 2.5, "bytes_received": 1000 }
            ]}
        }}});
        let mut tracker = StallTracker::default();
        let mut conditions = Vec::new();
        for sessions in [
            vec![("old", 5000), ("new", 100)],
            vec![("old", 5000), ("new", 600)],
            vec![("new", 1100)],
            vec![("new", 1600)],
        ] {
            let status = poll(
                &hop,
                flow_in_state(&hop.id, "Playing"),
                srt.clone(),
                whep_sessions(&sessions),
                &mut tracker,
            )
            .await;
            conditions.push(status.egresses[0].status.condition);
        }
        assert_eq!(
            conditions,
            [
                LinkCondition::Connected,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
            ]
        );
    }

    #[tokio::test]
    async fn a_source_that_leaves_stalls_over_whip_and_over_srt() {
        let whip = whip_to_srt_hop();
        let mut tracker = StallTracker::default();
        let mut whip_conditions = Vec::new();
        for recording in ["poll-0", "poll-1", "ended", "ended", "ended", "poll-0"] {
            let status = poll(
                &whip,
                flow_in_state(&whip.id, "Playing"),
                json!({}),
                recorded_webrtc(recording),
                &mut tracker,
            )
            .await;
            whip_conditions.push(status.ingress.condition);
        }

        let srt = hop("weave-basic-sender", 7001);
        let producer = |bytes: Option<i64>| match bytes {
            Some(bytes) => json!({ "stats": { "connections": {
                "srtsrc_0": { "connected": true, "callers": [
                    { "recv_rate_mbps": 2.5, "bytes_received": bytes }
                ]}
            }}}),
            None => json!({ "stats": { "connections": {
                "srtsrc_0": { "connected": false, "callers": [] }
            }}}),
        };
        let mut srt_conditions = Vec::new();
        for bytes in [Some(1000), Some(2000), None, None, None, Some(500)] {
            let status = poll(
                &srt,
                flow_in_state(&srt.id, "Playing"),
                producer(bytes),
                json!({}),
                &mut tracker,
            )
            .await;
            srt_conditions.push(status.ingress.condition);
        }

        use LinkCondition::{Connected, Flowing, Idle, Stalled};
        assert_eq!(
            whip_conditions,
            [Connected, Flowing, Idle, Idle, Stalled, Flowing],
            "WHIP"
        );
        assert_eq!(
            srt_conditions,
            [Flowing, Flowing, Idle, Idle, Stalled, Flowing],
            "SRT"
        );
    }

    #[tokio::test]
    async fn a_playing_whip_flow_without_a_session_is_idle() {
        let hop = whip_to_srt_hop();
        let mut tracker = StallTracker::default();
        let unfed = json!({ "flow_id": "id-a", "stats": { "connections": {} } });
        for _ in 0..4 {
            let status = poll(
                &hop,
                flow_in_state(&hop.id, "Playing"),
                json!({}),
                unfed.clone(),
                &mut tracker,
            )
            .await;
            assert_eq!(status.ingress.condition, LinkCondition::Idle);
        }
    }

    #[tokio::test]
    async fn a_whep_egress_reads_its_sessions() {
        let hop = srt_to_whep_hop();
        let srt = json!({ "stats": { "connections": {
            "srt_in:srtsrc": { "connected": true, "callers": [
                { "recv_rate_mbps": 2.5, "bytes_received": 1000 }
            ]}
        }}});
        let mut tracker = StallTracker::default();
        let mut conditions = Vec::new();
        for recording in ["poll-0", "poll-1", "poll-2", "ended"] {
            let status = poll(
                &hop,
                flow_in_state(&hop.id, "Playing"),
                srt.clone(),
                recorded_webrtc(recording),
                &mut tracker,
            )
            .await;
            conditions.push(status.egresses[0].status.condition);
        }
        assert_eq!(
            conditions,
            [
                LinkCondition::Connected,
                LinkCondition::Flowing,
                LinkCondition::Flowing,
                LinkCondition::Idle,
            ]
        );
    }

    #[tokio::test]
    async fn a_webrtc_session_with_frozen_bytes_stalls() {
        let hop = whip_to_srt_hop();
        let mut tracker = StallTracker::default();
        let mut last = LinkCondition::Idle;
        for recording in ["poll-0", "poll-1", "poll-1", "poll-1", "poll-1"] {
            last = poll(
                &hop,
                flow_in_state(&hop.id, "Playing"),
                json!({}),
                recorded_webrtc(recording),
                &mut tracker,
            )
            .await
            .ingress
            .condition;
        }
        assert_eq!(last, LinkCondition::Stalled);
    }

    #[test]
    fn srt_preserves_unrestricted_payloads_and_gateways_declare_unknown() {
        let profiles = strom_hop_profiles();
        let constrained: Vec<_> = profiles
            .iter()
            .filter(|profile| profile.accepts.is_some())
            .map(|profile| profile.id.as_str())
            .collect();
        assert_eq!(constrained, ["srt-forward", "whip-to-srt", "whip-to-whep"]);
        assert_eq!(
            profiles[0].media_behavior,
            weave_core::MediaBehavior::Preserve
        );
        assert_eq!(profiles[0].accepts, Some(FormatConstraint::default()));
        assert!(
            profiles[1..]
                .iter()
                .all(|profile| profile.media_behavior == weave_core::MediaBehavior::Unknown)
        );
        let accepts = profiles[1].accepts.as_ref().unwrap();
        let format = |video| weave_core::MediaFormat {
            container: weave_core::Container::Rtp,
            video: Some(weave_core::VideoFormat {
                codec: video,
                width: 1280,
                height: 720,
                framerate: weave_core::Framerate::new(30, 1),
                chroma_subsampling: weave_core::ChromaSubsampling::Yuv420,
            }),
            audio: Some(weave_core::AudioFormat {
                codec: AudioCodec::Opus,
                sample_rate: 48_000,
                channels: 2,
            }),
        };
        assert!(accepts.satisfied_by(&format(VideoCodec::H264)));
        assert!(!accepts.satisfied_by(&format(VideoCodec::Vp8)));
    }

    #[tokio::test]
    async fn a_missed_desired_fetch_or_strom_listing_still_reports_the_last_desired_hops() {
        let desired = hop("weave-feed-bridge-studio-0", 7001);
        let fetched = southbound_answering(StatusCode::OK, json!([desired])).await;
        let missed = southbound_answering(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "code": "not_leader", "message": "no leader" }),
        )
        .await;
        let running = [flow(&desired.id, "id-bridge")];
        let strom = RecordingFlowApi::default();
        let mut tracker = StallTracker::default();
        let mut last_desired = Vec::new();
        let states = |statuses: &[HopStatus]| {
            statuses
                .iter()
                .map(|status| {
                    (
                        status.id.clone(),
                        status.state,
                        status
                            .egresses
                            .iter()
                            .map(|egress| egress.branch_id.clone())
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>()
        };

        let first = hop_status(
            &missed,
            &strom,
            "strom-node-1",
            Some(&running),
            None,
            &mut tracker,
            &mut last_desired,
        )
        .await;
        assert!(
            first.is_empty(),
            "before any desired fetch there is nothing to report"
        );

        hop_status(
            &fetched,
            &strom,
            "strom-node-1",
            Some(&running),
            None,
            &mut tracker,
            &mut last_desired,
        )
        .await;
        assert_eq!(last_desired, std::slice::from_ref(&desired));
        let ops = strom.ops().len();

        let reported = hop_status(
            &missed,
            &strom,
            "strom-node-1",
            Some(&running),
            None,
            &mut tracker,
            &mut last_desired,
        )
        .await;
        assert_eq!(strom.ops().len(), ops, "a missed fetch touches no flow");
        assert_eq!(
            states(&reported),
            [(
                desired.id.clone(),
                HopState::Provisioned,
                vec!["studio".to_string()]
            )],
            "the hop Strom still runs is reported"
        );

        let unlisted = hop_status(
            &fetched,
            &strom,
            "strom-node-1",
            None,
            None,
            &mut tracker,
            &mut last_desired,
        )
        .await;
        assert_eq!(
            states(&unlisted),
            [(
                desired.id.clone(),
                HopState::Pending,
                vec!["studio".to_string()]
            )],
            "with Strom unlisted, the last desired hops are reported pending"
        );
        assert_eq!(
            unlisted[0].ingress.resolved.as_ref().map(|addr| addr.port),
            Some(7001),
            "with Strom unlisted, a hop keeps the listener port it holds"
        );
    }

    #[derive(Clone, Default)]
    struct LogCapture(std::sync::Arc<Mutex<Vec<std::collections::BTreeMap<String, String>>>>);

    impl LogCapture {
        fn error_of(&self, message: &str) -> Option<String> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .find(|fields| fields.get("message").map(String::as_str) == Some(message))
                .and_then(|fields| fields.get("error").cloned())
        }

        async fn error_logged_by(&self, run: impl Future, message: &str) -> String {
            let logged = async {
                while self.error_of(message).is_none() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            };
            let _ = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::select! {
                    _ = run => {}
                    () = logged => {}
                }
            })
            .await;
            self.error_of(message)
                .unwrap_or_else(|| panic!("{message:?} was not logged"))
        }
    }

    impl tracing::Subscriber for LogCapture {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target().starts_with(env!("CARGO_CRATE_NAME"))
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            let mut fields = Fields::default();
            event.record(&mut fields);
            self.0.lock().unwrap().push(fields.0);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[derive(Default)]
    struct Fields(std::collections::BTreeMap<String, String>);

    impl tracing::field::Visit for Fields {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
    }

    #[tokio::test]
    async fn a_southbound_that_cannot_be_reached_is_logged_with_its_cause() {
        let strom = strom_reporting(json!("0.6.10")).await;
        let unreachable = "http://127.0.0.1:1";
        let config: AdapterConfig = serde_norway::from_str(&format!(
            "node:\n  id: strom-node-1\n  southbound_url: {unreachable}\n  listen: 127.0.0.1:0\n  topology: {{ attachments: [] }}\nstrom:\n  url: {}\n",
            strom.base_url()
        ))
        .unwrap();
        let southbound = Southbound::new(unreachable.to_string(), None);
        let logs = LogCapture::default();
        let _logging = tracing::subscriber::set_default(logs.clone());

        let error = logs
            .error_logged_by(
                sync_loop(&southbound, &strom, &config, "http://strom-node-1"),
                "Strom adapter sync failed",
            )
            .await;

        assert!(error.contains("Connection refused"), "{error}");
    }

    #[tokio::test]
    async fn a_flow_strom_refuses_is_logged_with_what_strom_answered() {
        let app = Router::new().route(
            "/api/flows",
            axum::routing::post(|| async {
                (
                    StatusCode::BAD_REQUEST,
                    "{\n  \"error\": \"Invalid flow\",\n  \"details\": \"no such element: srtsrcx\"\n}",
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let strom = StromClient::new(format!("http://{addr}"));
        let logs = LogCapture::default();
        let _logging = tracing::subscriber::set_default(logs.clone());

        let error = logs
            .error_logged_by(
                reconcile(
                    &strom,
                    &[hop("weave-basic-sender", 7001)],
                    &[],
                    None,
                    &mut StallTracker::default(),
                ),
                "provisioning hop failed",
            )
            .await;

        assert!(error.contains("400 Bad Request"), "{error}");
        assert!(error.contains("no such element: srtsrcx"), "{error}");
        assert!(!error.contains('\n'), "{error}");
    }
}
