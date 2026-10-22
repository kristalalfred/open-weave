mod config;
mod hops;
mod inventory;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::{Json, Router, routing::get};
use clap::Parser;
use reqwest::{Client, RequestBuilder, StatusCode};
use tokio::task::JoinHandle;
use weave_core::auth::{self, Token};
use weave_core::logging::ErrorChain;
use weave_core::{
    AdapterDescriptor, AdapterKind, AudioCodec, AudioConstraint, Container, DesiredHop,
    FormatConstraint, HopEndpointClass, HopProfile, HopStatus, LocalEnd, MediaBehavior,
    NodeCapabilities, NodeDescriptor, NodeHeartbeat, NodeRegistration, NodeStatus,
    PROTOCOL_VERSION, RoleSet, Transport, TransportClass,
};
use weave_spaghetti::{SpaghettiClient, SpaghettiError};

use config::AdapterConfig;
use hops::{Action, Plan, Rates};
use inventory::Inventory;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Parser)]
#[command(
    name = "weave-adapter-spaghetti",
    version,
    about = "open-weave southbound adapter for spaghetti"
)]
struct Args {
    #[arg(help = "Path to the adapter config file (YAML)")]
    #[arg(long, env = "WEAVE_NODE_CONFIG", hide_env_values = true)]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    weave_core::logging::init();

    let args = Args::parse();
    let config = AdapterConfig::load(&args.config)?;
    let token = config.node.resolve_southbound_token()?;
    if token.is_none() {
        tracing::warn!(
            "{}=1: calling southbound without authentication",
            auth::AUTH_DISABLED_VAR
        );
    }
    let southbound = Southbound::new(config.node.southbound_url.clone(), token)?;
    let spaghetti = SpaghettiClient::new(&config.spaghetti.socket);
    let health_server = spawn_health_server(config.node.listen.clone());

    tracing::info!(
        node_id = %config.node.id,
        southbound_url = %config.node.southbound_url,
        socket = %config.spaghetti.socket.display(),
        topology = ?config.node.topology,
        "spaghetti adapter starting"
    );

    let result = tokio::select! {
        result = weave_core::shutdown::signal() => {
            result.context("waiting for shutdown signal")?;
            tracing::info!("spaghetti adapter shutting down");
            Ok(())
        }
        result = sync_loop(&southbound, &spaghetti, &config) => result,
    };
    health_server.abort();
    result
}

struct Southbound {
    http: Client,
    url: String,
    token: Option<Token>,
}

impl Southbound {
    fn new(url: String, token: Option<Token>) -> Result<Self> {
        Ok(Self {
            http: Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .timeout(REQUEST_TIMEOUT)
                .build()
                .context("building the HTTP client")?,
            url: url.trim_end_matches('/').to_string(),
            token,
        })
    }

    fn request(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.token {
            Some(token) => request.header(reqwest::header::AUTHORIZATION, token.header_value()),
            None => request,
        }
    }

    async fn register(&self, registration: &NodeRegistration) -> Result<()> {
        let response = self
            .request(self.http.post(format!("{}/nodes/register", self.url)))
            .json(registration)
            .send()
            .await
            .context("registering with southbound")?;
        let status = response.status();
        if status.is_client_error() {
            let body = response.text().await.unwrap_or_default();
            return Err(RegistrationRejected { status, body }.into());
        }
        response
            .error_for_status()
            .context("registering with southbound")?;
        Ok(())
    }

    async fn desired(&self, node_id: &str) -> Result<Option<Vec<DesiredHop>>> {
        let response = self
            .request(
                self.http
                    .get(format!("{}/nodes/{node_id}/desired", self.url)),
            )
            .send()
            .await
            .context("fetching desired hops")?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let hops: Vec<DesiredHop> = response
            .error_for_status()
            .context("fetching desired hops")?
            .json()
            .await
            .context("decoding desired hops")?;
        anyhow::ensure!(
            hops.iter().all(|hop| hop.node_id == node_id),
            "southbound returned another node's desired hops"
        );
        Ok(Some(hops))
    }

    async fn heartbeat(&self, heartbeat: &NodeHeartbeat) -> Result<bool> {
        let response = self
            .request(self.http.post(format!(
                "{}/nodes/{}/heartbeat",
                self.url, heartbeat.node_id
            )))
            .json(heartbeat)
            .send()
            .await
            .context("sending heartbeat")?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        response.error_for_status().context("sending heartbeat")?;
        Ok(true)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("southbound refused the registration with {status}: {body}")]
struct RegistrationRejected {
    status: StatusCode,
    body: String,
}

fn spawn_health_server(addr: String) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let app = Router::new().route("/health", get(health));
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .with_context(|| format!("binding spaghetti adapter health listener on {addr}"))?;
        tracing::info!(%addr, "spaghetti adapter health API listening");
        axum::serve(listener, app)
            .await
            .context("spaghetti adapter health server error")?;
        Ok(())
    })
}

async fn health() -> Json<weave_core::Health> {
    Json(weave_core::Health::OK)
}

fn profiles() -> Vec<HopProfile> {
    let srt = HopEndpointClass::Transport(TransportClass {
        transport: Transport::Srt,
        roles: RoleSet::both(),
    });
    vec![
        HopProfile {
            id: hops::SENDER_PROFILE.into(),
            ingress: HopEndpointClass::Local(LocalEnd::Input),
            egress: srt.clone(),
            max_egresses: Some(hops::MAX_SENDER_EGRESSES),
            media_behavior: MediaBehavior::Unknown,
            merge: false,
            accepts: None,
            timed_delivery: None,
        },
        HopProfile {
            id: hops::RECEIVER_PROFILE.into(),
            ingress: srt,
            egress: HopEndpointClass::Local(LocalEnd::Output),
            max_egresses: Some(1),
            media_behavior: MediaBehavior::Unknown,
            merge: false,
            accepts: Some(FormatConstraint {
                container: Some(vec![Container::MpegTs]),
                video: None,
                audio: Some(AudioConstraint {
                    codec: Some(vec![AudioCodec::Aac]),
                    sample_rate: None,
                    channels: None,
                }),
            }),
            timed_delivery: None,
        },
    ]
}

fn descriptor(config: &AdapterConfig, inventory: &Inventory, status: NodeStatus) -> NodeDescriptor {
    NodeDescriptor {
        id: config.node.id.clone(),
        endpoint: config.node.public_endpoint(),
        status,
        capabilities: NodeCapabilities {
            adapters: vec![AdapterDescriptor {
                name: "spaghetti".into(),
                kind: AdapterKind::Spaghetti,
            }],
            hop_profiles: profiles(),
        },
        topology: config.node.topology.clone(),
        inputs: inventory.inputs.clone(),
        outputs: inventory.outputs.clone(),
    }
}

struct Observation {
    inventory: Inventory,
    sources: Vec<weave_spaghetti::SourceInfo>,
    routes: Vec<weave_spaghetti::RouteInfo>,
}

async fn observe(spaghetti: &SpaghettiClient, home: &str) -> Result<Observation, SpaghettiError> {
    let apps = spaghetti.apps().await?;
    let devices = spaghetti.system_devices().await?;
    Ok(Observation {
        inventory: Inventory::detect(&apps, &devices, home),
        sources: spaghetti.sources().await?,
        routes: spaghetti.routes().await?,
    })
}

struct Node {
    registered: bool,
    version_checked: bool,
    inventory: Inventory,
    desired: Vec<DesiredHop>,
    failed: BTreeSet<String>,
    rates: Rates,
    statuses: Vec<HopStatus>,
}

async fn sync_loop(
    southbound: &Southbound,
    spaghetti: &SpaghettiClient,
    config: &AdapterConfig,
) -> Result<()> {
    let home = std::env::var("HOME").unwrap_or_default();
    let mut node = Node {
        registered: false,
        version_checked: false,
        inventory: Inventory::default(),
        desired: Vec::new(),
        failed: BTreeSet::new(),
        rates: Rates::default(),
        statuses: Vec::new(),
    };
    loop {
        match sync_once(southbound, spaghetti, config, &home, &mut node).await {
            Ok(()) => {}
            Err(error)
                if error.is::<RegistrationRejected>()
                    || matches!(
                        error.downcast_ref::<SpaghettiError>(),
                        Some(SpaghettiError::Version { .. })
                    ) =>
            {
                tracing::error!(error = %ErrorChain(error.as_ref()), "spaghetti adapter cannot run; exiting");
                return Err(error);
            }
            Err(error) => {
                node.registered = false;
                tracing::warn!(error = %ErrorChain(error.as_ref()), "spaghetti adapter sync failed");
            }
        }
        tokio::time::sleep(config.spaghetti.poll_interval()).await;
    }
}

async fn sync_once(
    southbound: &Southbound,
    spaghetti: &SpaghettiClient,
    config: &AdapterConfig,
    home: &str,
    node: &mut Node,
) -> Result<()> {
    if !node.version_checked {
        match spaghetti.check_version().await {
            Ok(_) => node.version_checked = true,
            Err(error @ SpaghettiError::Version { .. }) => return Err(error.into()),
            Err(error) => tracing::warn!(error = %error, "spaghetti daemon is not answering"),
        }
    }
    let observation = match observe(spaghetti, home).await {
        Ok(observation) => Some(observation),
        Err(error) => {
            tracing::warn!(error = %error, "cannot read the spaghetti daemon");
            node.version_checked = false;
            None
        }
    };
    let status = if observation.is_some() {
        NodeStatus::Ready
    } else {
        NodeStatus::Degraded
    };
    if let Some(observation) = &observation {
        node.inventory = observation.inventory.clone();
    }

    if !node.registered {
        southbound
            .register(&NodeRegistration {
                protocol_version: PROTOCOL_VERSION,
                node: descriptor(config, &node.inventory, status),
                endpoints: Vec::new(),
                hop_status: node.statuses.clone(),
            })
            .await?;
        node.registered = true;
        tracing::info!(
            node_id = %config.node.id,
            inputs = node.inventory.inputs.len(),
            outputs = node.inventory.outputs.len(),
            "registered with southbound"
        );
    }

    match southbound.desired(&config.node.id).await? {
        Some(desired) => node.desired = desired,
        None => {
            node.registered = false;
            return Ok(());
        }
    }

    if let Some(observation) = observation {
        let plans: BTreeMap<String, Result<Plan, String>> = node
            .desired
            .iter()
            .map(|hop| (hop.id.clone(), hops::plan(hop, &node.inventory)))
            .collect();
        for (hop_id, plan) in &plans {
            if let Err(reason) = plan {
                tracing::debug!(hop_id, reason, "cannot run desired hop");
            }
        }
        let actions = hops::actions(&plans, &observation.sources, &observation.routes);
        node.failed
            .retain(|hop_id| plans.get(hop_id).is_some_and(Result::is_ok));
        for action in actions {
            apply(spaghetti, action, &mut node.failed).await;
        }
        let sources = spaghetti.sources().await?;
        let routes = spaghetti.routes().await?;
        let (applied, _) = hops::applied(&sources, &routes);
        let now = Instant::now();
        node.statuses = node
            .desired
            .iter()
            .map(|hop| {
                hops::hop_status(
                    hop,
                    &plans[&hop.id],
                    applied.get(hop.id.as_str()),
                    node.failed.contains(&hop.id),
                    &mut node.rates,
                    now,
                )
            })
            .collect();
        let hop_ids: Vec<&str> = node.desired.iter().map(|hop| hop.id.as_str()).collect();
        node.rates.retain(&hop_ids);
    }

    let heartbeat = NodeHeartbeat {
        node_id: config.node.id.clone(),
        status,
        inputs: node.inventory.inputs.clone(),
        outputs: node.inventory.outputs.clone(),
        endpoints: Vec::new(),
        hop_status: node.statuses.clone(),
    };
    if !southbound.heartbeat(&heartbeat).await? {
        node.registered = false;
    }
    Ok(())
}

async fn apply(spaghetti: &SpaghettiClient, action: Action, failed: &mut BTreeSet<String>) {
    match action {
        Action::Destroy { source_id } => match spaghetti.destroy_source(source_id).await {
            Ok(()) => tracing::info!(source_id, "removed spaghetti source"),
            Err(error) => {
                tracing::warn!(source_id, error = %error, "cannot remove spaghetti source")
            }
        },
        Action::Build { hop_id, plan } => match build(spaghetti, &hop_id, &plan).await {
            Ok(source_id) => {
                failed.remove(&hop_id);
                tracing::info!(hop_id, source_id, routes = plan.routes.len(), "built hop");
            }
            Err(error) => {
                failed.insert(hop_id.clone());
                tracing::warn!(hop_id, error = %error, "cannot build hop");
            }
        },
    }
}

async fn build(
    spaghetti: &SpaghettiClient,
    hop_id: &str,
    plan: &Plan,
) -> Result<u32, SpaghettiError> {
    let source_id = spaghetti
        .create_source(&plan.source, &hops::tag(hop_id))
        .await?;
    for (_, destination) in &plan.routes {
        if let Err(error) = spaghetti.create_route(source_id, destination, 1.0).await {
            let _ = spaghetti.destroy_source(source_id).await;
            return Err(error);
        }
    }
    Ok(source_id)
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::*;

    #[test]
    fn help_hides_every_env_value() {
        let command = Args::command();
        let shown: Vec<String> = command
            .get_arguments()
            .filter(|arg| arg.get_env().is_some() && !arg.is_hide_env_values_set())
            .map(|arg| arg.get_id().to_string())
            .collect();
        assert!(
            shown.is_empty(),
            "--help prints the environment value of {shown:?}"
        );
    }

    #[test]
    fn the_registration_is_valid_for_the_controller() {
        let config: AdapterConfig = serde_norway::from_str(
            r"
node:
  id: alfreds-mac
  southbound_url: http://127.0.0.1:8081
  listen: 127.0.0.1:8092
  topology:
    attachments:
      - id: lan
        network: office
        dial: true
        listeners:
          srt:
            host: 192.168.1.20
            port_range: { start: 23000, end: 23099 }
",
        )
        .unwrap();
        let inventory = Inventory::detect(
            &[weave_spaghetti::AppInfo {
                name: "Music".into(),
                bundle_id: Some("com.apple.Music".into()),
                bundle_path: Some("/System/Applications/Music.app".into()),
                pids: vec![731],
            }],
            &[],
            "/Users/alfred",
        );
        let node = descriptor(&config, &inventory, NodeStatus::Ready);
        assert!(weave_core::validate_node(&node).is_empty());
        assert!(node.capabilities.offers_local(LocalEnd::Input));
        assert!(node.capabilities.offers_local(LocalEnd::Output));
        assert_eq!(node.inputs[0].id, "music");
    }
}
