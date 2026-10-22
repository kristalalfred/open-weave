use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use axum::{Json, Router, routing::get};
use reqwest::{Client, RequestBuilder};
use serde::Deserialize;
use serde_json::json;
use weave_core::auth::Token;
use weave_core::{
    AdapterDescriptor, AdapterKind, DeliveryCapability, DeliveryRole, DesiredHop, FormatConstraint,
    HopEndpointClass, HopProfile, HopStatus, MediaBehavior, NodeCapabilities, NodeConfig,
    NodeDescriptor, NodeHeartbeat, NodeRegistration, NodeStatus, PROTOCOL_VERSION, RoleSet,
    Transport, TransportClass,
};

use crate::clock::{Clock, ClockConfig};
use crate::merge::BufferLimits;
use crate::runtime::{Hop, OriginState};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub node: NodeConfig,
    pub clock: ClockConfig,
    #[serde(default)]
    pub media: MediaConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MediaConfig {
    pub listen_ip: IpAddr,
    pub max_buffer_records: usize,
    pub max_buffer_bytes: usize,
    pub scheduling_error_bound_us: u32,
}

impl Default for MediaConfig {
    fn default() -> Self {
        let limits = BufferLimits::default();
        Self {
            listen_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            max_buffer_records: limits.records,
            max_buffer_bytes: limits.bytes,
            scheduling_error_bound_us: limits.scheduling_error_us,
        }
    }
}

impl MediaConfig {
    fn limits(&self) -> BufferLimits {
        BufferLimits {
            records: self.max_buffer_records,
            bytes: self.max_buffer_bytes,
            scheduling_error_us: self.scheduling_error_bound_us,
        }
    }
}

#[must_use]
pub fn profiles(clock_domain: &str, epoch: u64, scheduling_error_bound_us: u32) -> Vec<HopProfile> {
    let class = || {
        HopEndpointClass::Transport(TransportClass {
            transport: Transport::Srt,
            roles: RoleSet::both(),
        })
    };
    let mut profiles = vec![HopProfile {
        id: "srt-forward".into(),
        ingress: class(),
        egress: class(),
        max_egresses: None,
        media_behavior: MediaBehavior::Preserve,
        merge: false,
        accepts: Some(FormatConstraint::default()),
        timed_delivery: None,
    }];
    for (id, role, merge) in [
        ("srt-timed-origin", DeliveryRole::Origin, false),
        ("srt-timed-forward", DeliveryRole::Forward, false),
        ("srt-timed-receiver", DeliveryRole::Receiver, true),
    ] {
        profiles.push(HopProfile {
            id: id.into(),
            ingress: class(),
            egress: class(),
            max_egresses: (role == DeliveryRole::Receiver).then_some(1),
            media_behavior: MediaBehavior::Preserve,
            merge,
            accepts: Some(FormatConstraint::default()),
            timed_delivery: Some(DeliveryCapability {
                protocol: weave_core::delivery::TIMED_PROTOCOL.into(),
                roles: vec![role],
                max_latency_ms: 60_000,
                scheduling_error_bound_us,
                clock_domain: clock_domain.into(),
                origin_epoch: epoch,
            }),
        });
    }
    profiles
}

struct Control {
    client: Client,
    url: String,
    token: Option<Token>,
    registered: bool,
}

impl Control {
    fn authorize(&self, builder: RequestBuilder) -> RequestBuilder {
        match &self.token {
            Some(token) => builder.header(reqwest::header::AUTHORIZATION, token.header_value()),
            None => builder,
        }
    }

    async fn synchronize(
        &mut self,
        registration: NodeRegistration,
        hops: &mut BTreeMap<String, Hop>,
        origins: &mut BTreeMap<String, Arc<Mutex<OriginState>>>,
        clock: &Clock,
        limits: BufferLimits,
        bind_ip: IpAddr,
    ) -> Result<()> {
        if !self.registered {
            self.authorize(self.client.post(format!("{}/nodes/register", self.url)))
                .json(&registration)
                .send()
                .await?
                .error_for_status()?;
            self.registered = true;
        }
        let response = self
            .authorize(self.client.get(format!(
                "{}/nodes/{}/desired",
                self.url, registration.node.id
            )))
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            self.registered = false;
            return Ok(());
        }
        let desired: Vec<DesiredHop> = response.error_for_status()?.json().await?;
        anyhow::ensure!(
            desired
                .iter()
                .all(|hop| hop.node_id == registration.node.id),
            "controller returned another node's desired hops"
        );
        hops.retain(|id, _| desired.iter().any(|hop| &hop.id == id));
        for desired in desired {
            if let Some(timing) = desired
                .delivery
                .as_ref()
                .filter(|timing| timing.role == DeliveryRole::Origin)
            {
                let authorized = registration
                    .node
                    .capabilities
                    .hop_profiles
                    .iter()
                    .filter_map(|profile| profile.timed_delivery.as_ref())
                    .any(|capability| {
                        capability.roles.contains(&DeliveryRole::Origin)
                            && capability.origin_epoch == timing.epoch
                    });
                anyhow::ensure!(
                    authorized,
                    "waiting for the controller to authorize the current origin epoch"
                );
            }
            if let Some(hop) = hops.get_mut(&desired.id) {
                hop.update(desired, clock)?;
            } else {
                let origin = origins.entry(desired.id.clone()).or_default().clone();
                hops.insert(
                    desired.id.clone(),
                    Hop::start(desired, clock.clone(), limits, origin, bind_ip)?,
                );
            }
        }
        let heartbeat = NodeHeartbeat {
            inputs: Vec::new(),
            outputs: Vec::new(),
            node_id: registration.node.id,
            status: NodeStatus::Ready,
            endpoints: vec![],
            hop_status: reports(hops),
        };
        let response = self
            .authorize(self.client.post(format!(
                "{}/nodes/{}/heartbeat",
                self.url, heartbeat.node_id
            )))
            .json(&heartbeat)
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            self.registered = false;
            return Ok(());
        }
        response.error_for_status()?;
        Ok(())
    }
}

fn reports(hops: &BTreeMap<String, Hop>) -> Vec<HopStatus> {
    hops.values()
        .map(|hop| hop.report.lock().unwrap().clone())
        .collect()
}

pub async fn run(config: Config) -> Result<()> {
    config
        .node
        .validate()
        .context("validating native node configuration")?;
    anyhow::ensure!(
        (1..=65_536).contains(&config.media.max_buffer_records),
        "buffer record limit must be between 1 and 65536"
    );
    anyhow::ensure!(
        (1_088..=256 * 1024 * 1024).contains(&config.media.max_buffer_bytes),
        "buffer byte limit must be between 1088 and 268435456"
    );
    let clock = Clock::new(config.clock.clone());
    anyhow::ensure!(!clock.domain().is_empty(), "clock domain must not be empty");
    clock.refresh().await;
    let mut epoch_bytes = [0_u8; 8];
    getrandom::fill(&mut epoch_bytes)
        .map_err(|error| anyhow::anyhow!("generating origin runtime epoch: {error}"))?;
    let epoch = u64::from_be_bytes(epoch_bytes).max(1);
    let node = NodeDescriptor {
        inputs: Vec::new(),
        outputs: Vec::new(),
        id: config.node.id.clone(),
        endpoint: config.node.public_endpoint(),
        status: NodeStatus::Ready,
        capabilities: NodeCapabilities {
            adapters: vec![AdapterDescriptor {
                name: "native-srt".into(),
                kind: AdapterKind::NativeSrt,
            }],
            hop_profiles: profiles(
                &clock.domain(),
                epoch,
                config.media.scheduling_error_bound_us,
            ),
        },
        topology: config.node.topology.clone(),
    };
    let mut control = Control {
        client: Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .build()?,
        url: config.node.southbound_url.trim_end_matches('/').to_string(),
        token: config.node.resolve_southbound_token()?,
        registered: false,
    };
    let published: Arc<Mutex<Vec<HopStatus>>> = Arc::default();
    let health_reports = published.clone();
    let app = Router::new()
        .route(
            "/health",
            get(|| async { Json(json!({"status":"ok", "version":env!("CARGO_PKG_VERSION")})) }),
        )
        .route(
            "/status",
            get(move || {
                let reports = health_reports.clone();
                async move { Json(reports.lock().unwrap().clone()) }
            }),
        );
    let listener = tokio::net::TcpListener::bind(&config.node.listen)
        .await
        .context("binding native node health API")?;
    let health = tokio::spawn(async move { axum::serve(listener, app).await });
    let refreshed = clock.clone();
    let refresh = tokio::spawn(async move {
        loop {
            refreshed.refresh().await;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    });
    let mut hops = BTreeMap::new();
    let mut origins = BTreeMap::new();
    tracing::info!(node = %node.id, epoch, clock_domain = %clock.domain(), "native SRT media node starting");
    let operation = async {
        loop {
            anyhow::ensure!(
                hops.values().all(|hop: &Hop| !hop.is_finished()),
                "media actor exited; restart the node to authorize a new origin epoch"
            );
            let registration = NodeRegistration {
                protocol_version: PROTOCOL_VERSION,
                node: node.clone(),
                endpoints: vec![],
                hop_status: reports(&hops),
            };
            if let Err(error) = control
                .synchronize(
                    registration,
                    &mut hops,
                    &mut origins,
                    &clock,
                    config.media.limits(),
                    config.media.listen_ip,
                )
                .await
            {
                if error
                    .downcast_ref::<reqwest::Error>()
                    .and_then(reqwest::Error::status)
                    .is_some_and(|status| status.is_client_error())
                {
                    return Err(error).context("control surface rejected the native node");
                }
                tracing::warn!(node = %node.id, error = %error, "native control synchronization failed; retaining media");
            }
            *published.lock().unwrap() = reports(&hops);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    };
    let result = tokio::select! {
        result = operation => result,
        result = weave_core::shutdown::signal() => result.context("waiting for native node shutdown"),
    };
    refresh.abort();
    health.abort();
    result
}
