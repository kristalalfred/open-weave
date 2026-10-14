use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use reqwest::header::{ETAG, IF_MATCH, IF_NONE_MATCH};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::process::Command;
use weave_core::{
    EndpointAddr, NodeDescriptor, NodeStatus, PathStatus, StatusResponse, StreamDefinition,
    StreamEndpoints, StreamResource, StreamTransport,
};
use weave_strom::{FlowListResponse, StromClient};

use crate::media::{self, Probe, Watch};
use crate::report::{Report, redact};
use crate::scenario::{ServiceAction, Step};

const SERVICES: &[&str] = &[
    "northbound",
    "southbound",
    "controller",
    "controller-2",
    "route-manager",
    "router-1",
    "router-2",
    "router-3",
    "router-4",
    "adapter-1",
    "adapter-2",
    "adapter-3",
    "adapter-4",
    "strom-1",
    "strom-2",
    "strom-3",
    "strom-4",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FlowIdentity {
    node: usize,
    name: String,
    id: String,
    running: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct Snapshot {
    stream: String,
    etag: String,
    generation: u64,
    flows: Vec<FlowIdentity>,
}

pub struct Bench {
    pub root: PathBuf,
    pub report: Report,
    pub http: reqwest::Client,
    pub url: String,
    pub token: String,
    pub prefix: String,
    image: String,
    streams: BTreeMap<String, StreamDefinition>,
    producers: BTreeMap<String, String>,
    pub consumers: BTreeMap<String, Probe>,
    containers: Vec<String>,
    services: BTreeSet<String>,
    impaired: BTreeSet<String>,
    stopped: BTreeSet<String>,
    snapshots: BTreeMap<String, Snapshot>,
    completed_media: Vec<Value>,
}

impl Bench {
    pub fn new(
        root: PathBuf,
        url: String,
        token: String,
        image: String,
        prefix: String,
        report: Report,
    ) -> Result<Self> {
        let parsed = reqwest::Url::parse(&url)?;
        ensure!(
            matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "[::1]")),
            "bench URL must use loopback"
        );
        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5));
        if parsed.scheme() == "https" {
            builder = builder.add_root_certificate(reqwest::Certificate::from_pem(
                &std::fs::read(root.join("tls/public/ca.pem"))
                    .context("bench CA missing; run just bench up")?,
            )?);
        }
        Ok(Self {
            root,
            report,
            http: builder.build()?,
            url,
            token,
            prefix,
            image,
            streams: BTreeMap::new(),
            producers: BTreeMap::new(),
            consumers: BTreeMap::new(),
            containers: Vec::new(),
            services: BTreeSet::new(),
            impaired: BTreeSet::new(),
            stopped: BTreeSet::new(),
            snapshots: BTreeMap::new(),
            completed_media: Vec::new(),
        })
    }

    pub async fn preflight(&self) -> Result<()> {
        for service in SERVICES {
            let container = format!("ow-{service}");
            self.report.event(
                "preflight_check",
                json!({"detail": format!("checking {container}")}),
            )?;
            let value: Value = serde_json::from_str(&docker(&["inspect", &container]).await?)?;
            ensure!(
                value[0]["State"]["Running"] == true,
                "{container} is not running"
            );
            ensure!(
                value[0]["Config"]["Labels"]["com.docker.compose.project"] == "ow-bench",
                "{container} is not owned by the bench"
            );
        }
        let deadline = Instant::now() + Duration::from_secs(45);
        self.report.event(
            "preflight_check",
            json!({"detail": "waiting for controller readiness"}),
        )?;
        loop {
            let response = self
                .http
                .get(format!("{}/status", self.url))
                .bearer_auth(&self.token)
                .send()
                .await?;
            ensure!(
                response.status() != reqwest::StatusCode::UNAUTHORIZED,
                "northbound rejected the bench token"
            );
            if response.status().is_success()
                && matches!(
                    response.json::<StatusResponse>().await?,
                    StatusResponse::Running(_)
                )
            {
                break;
            }
            ensure!(Instant::now() < deadline, "controller did not become ready");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        let streams: Vec<StreamResource> = self.get("/streams").await?;
        ensure!(
            streams.is_empty(),
            "bench has streams; remove them before running disruptive scenarios"
        );
        loop {
            let nodes: Vec<NodeDescriptor> = self.get("/nodes").await?;
            self.report.event("preflight_nodes", &nodes)?;
            if (1..=4).all(|n| {
                nodes.iter().any(|node| {
                    node.id == format!("strom-node-{n}") && node.status == NodeStatus::Ready
                })
            }) {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "all four media nodes did not become ready"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        for n in 1..=4 {
            let flows = self.flows_on(n).await?;
            ensure!(
                flows.is_empty(),
                "strom-{n} has managed flows before the scenario"
            );
        }
        for node in ["node1", "node2", "node3", "node4"] {
            let qdiscs = self.netem_command(node, &["show".into()]).await?;
            ensure!(!qdiscs.contains("netem"), "{node} already has impairment");
        }
        let inspection = match docker(&["image", "inspect", &self.image]).await {
            Ok(value) => value,
            Err(_) => {
                self.report.event(
                    "preflight_check",
                    json!({"detail": "pulling the media image"}),
                )?;
                docker(&["pull", &self.image]).await?;
                docker(&["image", "inspect", &self.image]).await?
            }
        };
        let image: Value = serde_json::from_str(&inspection)?;
        self.report.event(
            "media_image",
            json!({"image": self.image, "id": image[0]["Id"], "digests": image[0]["RepoDigests"]}),
        )?;
        Ok(())
    }

    pub async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let response = self
            .http
            .get(format!("{}{path}", self.url))
            .bearer_auth(&self.token)
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            "GET {path}: {}",
            response.status()
        );
        Ok(response.json().await?)
    }

    fn name(&self, alias: &str) -> Result<&str> {
        Ok(&self
            .streams
            .get(alias)
            .with_context(|| format!("unknown stream {alias}"))?
            .name)
    }

    pub async fn steps(&mut self, steps: &[Step], directory: &Path) -> Result<()> {
        for (index, step) in steps.iter().enumerate() {
            self.report
                .event("step_started", json!({"index": index, "step": step}))?;
            Box::pin(self.step(step, directory))
                .await
                .with_context(|| format!("step {}: {step:?}", index + 1))?;
            self.report
                .event("step_finished", json!({"index": index}))?;
        }
        Ok(())
    }

    async fn step(&mut self, step: &Step, directory: &Path) -> Result<()> {
        match step {
            Step::Apply { file, stream } => self.apply(stream, &directory.join(file)).await?,
            Step::Drive { stream } => self.drive(stream).await?,
            Step::Flows {
                stream,
                nodes,
                timeout_secs,
            } => {
                let deadline = Instant::now() + Duration::from_secs(*timeout_secs);
                loop {
                    let flows = self.flows(self.name(stream)?).await?;
                    let mut counts = BTreeMap::new();
                    for flow in &flows {
                        *counts
                            .entry(format!("strom-node-{}", flow.node))
                            .or_insert(0_usize) += 1;
                    }
                    if flows.iter().all(|flow| flow.running)
                        && (1..=4).all(|n| {
                            let node = format!("strom-node-{n}");
                            counts.get(&node).copied().unwrap_or(0)
                                == nodes.get(&node).copied().unwrap_or(0)
                        })
                    {
                        self.report.event("flows_verified", flows)?;
                        break;
                    }
                    ensure!(
                        Instant::now() < deadline,
                        "{stream} has wrong or stopped runtime flows: {flows:?}"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
            Step::Producer { stream, running } => {
                let name = self
                    .producers
                    .get(stream)
                    .context("producer has not been driven")?;
                docker(&[if *running { "start" } else { "stop" }, name]).await?;
            }
            Step::Consumer { receiver, running } => {
                if *running {
                    let (stream, destination) =
                        receiver.split_once('/').context("invalid receiver")?;
                    self.start_consumer(stream, destination).await?;
                } else {
                    let probe = self
                        .consumers
                        .get(receiver)
                        .context("consumer has not been driven")?;
                    docker(&["stop", "--time", "3", &probe.container]).await?;
                }
            }
            Step::Await {
                stream,
                status,
                destination,
                timeout_secs,
                nodes,
            } => {
                self.await_status(
                    stream,
                    *status,
                    destination.as_deref(),
                    *timeout_secs,
                    nodes.as_deref(),
                )
                .await?;
            }
            Step::Observe {
                receivers,
                seconds,
                max_gap_ms,
                max_restarts,
                during,
            } => {
                let gap = Duration::from_millis(*max_gap_ms);
                let watches = receivers
                    .iter()
                    .map(|receiver| {
                        let probe = self
                            .consumers
                            .get(receiver)
                            .with_context(|| format!("no consumer for {receiver}"))?;
                        Watch::arm(receiver.clone(), probe.progress.clone(), gap)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let deadline = tokio::time::Instant::now() + Duration::from_secs(*seconds);
                let work = async {
                    self.steps(during, directory).await?;
                    tokio::time::sleep_until(deadline).await;
                    Ok::<_, anyhow::Error>(())
                };
                tokio::select! {
                    result = media::watch(watches, gap, *max_restarts) => result?,
                    result = work => result?,
                }
            }
            Step::Service { service, action } => self.service(service, *action).await?,
            Step::Netem { node, args } => {
                self.impaired.insert(node.clone());
                self.netem_command(node, args).await?;
            }
            Step::ClearNetem { node } => {
                self.netem_command(node, &["clear".into()]).await?;
                self.impaired.remove(node);
            }
            Step::Snapshot { stream, as_name } => {
                let snapshot = self.snapshot(stream).await?;
                ensure!(
                    !snapshot.flows.is_empty(),
                    "cannot snapshot a stream without flows"
                );
                self.report.event("snapshot", &snapshot)?;
                self.snapshots.insert(as_name.clone(), snapshot);
            }
            Step::Unchanged { snapshot } => {
                let before = self.snapshots.get(snapshot).context("unknown snapshot")?;
                let after = self.snapshot(&before.stream).await?;
                ensure!(
                    *before == after,
                    "snapshot {snapshot} changed: {}",
                    serde_json::to_string(&after)?
                );
            }
            Step::Delete { stream } => self.delete(stream).await?,
            Step::Wait { seconds } => tokio::time::sleep(Duration::from_secs(*seconds)).await,
        }
        Ok(())
    }

    async fn apply(&mut self, alias: &str, file: &Path) -> Result<()> {
        let mut spec = crate::scenario::read_manifest(file)?;
        spec.name = format!("{}-{alias}", self.prefix);
        weave_core::validate_resource_id(&spec.name)?;
        let existing = self
            .http
            .get(format!("{}/streams/{}", self.url, spec.name))
            .bearer_auth(&self.token)
            .send()
            .await?;
        let request = self
            .http
            .post(format!("{}/streams", self.url))
            .bearer_auth(&self.token)
            .json(&spec);
        let request = match existing.status() {
            reqwest::StatusCode::NOT_FOUND => request.header(IF_NONE_MATCH, "*"),
            reqwest::StatusCode::OK => request.header(
                IF_MATCH,
                existing
                    .headers()
                    .get(ETAG)
                    .context("missing ETag")?
                    .clone(),
            ),
            status => bail!("looking up stream: {status}"),
        };
        self.streams.insert(alias.to_string(), spec);
        let response = request.send().await?;
        ensure!(
            response.status().is_success(),
            "applying manifest: {} {}",
            response.status(),
            redact(&response.text().await?)
        );
        Ok(())
    }

    async fn endpoints(&self, alias: &str) -> Result<StreamEndpoints> {
        let name = self.name(alias)?;
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let response = self
                .http
                .get(format!("{}/streams/{name}/endpoints", self.url))
                .bearer_auth(&self.token)
                .send()
                .await?;
            match response.status() {
                reqwest::StatusCode::OK => return Ok(response.json().await?),
                reqwest::StatusCode::SERVICE_UNAVAILABLE => (),
                status => bail!("discovery for {alias}: {status}"),
            }
            ensure!(Instant::now() < deadline, "discovery timed out for {alias}");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    async fn drive(&mut self, alias: &str) -> Result<()> {
        ensure!(
            !self.producers.contains_key(alias),
            "{alias} is already driven"
        );
        let endpoints = self.endpoints(alias).await?;
        let spec = self.streams.get(alias).unwrap();
        let url = media_url(
            endpoints.ingress.as_ref().context("no ingress")?,
            &spec.source,
        )?;
        let producer = self
            .spawn_media(&url, &format!("{alias}-producer"), media::PRODUCER)
            .await?;
        self.producers.insert(alias.to_string(), producer);
        for destination in endpoints.destinations {
            self.start_consumer(alias, &destination.id).await?;
        }
        Ok(())
    }

    async fn start_consumer(&mut self, alias: &str, destination: &str) -> Result<()> {
        let receiver = format!("{alias}/{destination}");
        if let Some(previous) = self.consumers.remove(&receiver) {
            self.completed_media
                .push(json!({"receiver": receiver, "progress": previous.summary()}));
            docker(&["rm", "-f", &previous.container]).await?;
            self.containers.retain(|name| name != &previous.container);
        }
        let endpoints = self.endpoints(alias).await?;
        let endpoint = endpoints
            .destinations
            .iter()
            .find(|d| d.id == destination)
            .and_then(|d| d.endpoint.as_ref())
            .context("destination has no endpoint")?;
        let spec = self
            .streams
            .get(alias)
            .unwrap()
            .destinations
            .iter()
            .find(|d| d.id == destination)
            .context("destination missing from manifest")?;
        let url = media_url(endpoint, &spec.endpoint)?;
        let container = self
            .spawn_media(&url, &format!("{alias}-{destination}"), media::CONSUMER)
            .await?;
        self.consumers.insert(
            receiver.clone(),
            Probe::follow(container, receiver, self.report.clone())?,
        );
        Ok(())
    }

    async fn spawn_media(&mut self, url: &str, suffix: &str, script: &str) -> Result<String> {
        let host = reqwest::Url::parse(url)?
            .host_str()
            .context("media URL missing host")?
            .to_string();
        let site = if host.starts_with("10.97.29.") {
            "node3"
        } else if host.starts_with("10.97.30.") {
            "node4"
        } else {
            "core"
        };
        let network = docker(&[
            "network",
            "ls",
            "--filter",
            "label=com.docker.compose.project=ow-bench",
            "--filter",
            &format!("label=com.docker.compose.network=net_{site}"),
            "--format",
            "{{.ID}}",
        ])
        .await?;
        ensure!(
            network.split_whitespace().count() == 1,
            "expected one bench network for {site}"
        );
        let name = format!("ow-{}-{suffix}", self.prefix);
        self.containers.push(name.clone());
        docker(&[
            "run",
            "-d",
            "--name",
            &name,
            "--network",
            network.trim(),
            "--label",
            &format!("open-weave.bench.site={site}"),
            "--label",
            &format!("open-weave.bench.run={}", self.prefix),
            "--label",
            "open-weave.bench.project=ow-bench",
            "--env",
            &format!("BENCH_URL={url}"),
            "--entrypoint",
            "/bin/bash",
            &self.image,
            "-c",
            script,
        ])
        .await?;
        self.report.event(
            "media_started",
            json!({"container": name, "site": site, "host": host}),
        )?;
        Ok(name)
    }

    async fn await_status(
        &self,
        alias: &str,
        status: PathStatus,
        destination: Option<&str>,
        seconds: u64,
        nodes: Option<&[String]>,
    ) -> Result<()> {
        let name = self.name(alias)?;
        let deadline = Instant::now() + Duration::from_secs(seconds);
        let mut last = String::new();
        loop {
            match self.get::<StatusResponse>("/status").await {
                Ok(StatusResponse::Running(response)) => {
                    if let Some(stream) = response.streams.iter().find(|s| s.name == name) {
                        last = serde_json::to_string(stream)?;
                        self.report.event("await_status", stream)?;
                        let selected = destination
                            .and_then(|id| stream.destinations.iter().find(|d| d.id == id));
                        let got = if destination.is_some() {
                            selected.map(|d| d.status)
                        } else {
                            Some(stream.status)
                        };
                        let got_nodes = selected.map_or(&stream.nodes, |d| &d.nodes);
                        let nodes_match = nodes.is_none_or(|expected| {
                            expected.iter().collect::<BTreeSet<_>>()
                                == got_nodes.iter().collect::<BTreeSet<_>>()
                        });
                        let media =
                            status != PathStatus::Flowing || self.receiving(alias, destination);
                        if got == Some(status)
                            && stream.observed_generation == Some(stream.generation)
                            && nodes_match
                            && media
                        {
                            return Ok(());
                        }
                    }
                }
                Ok(_) => last = "controller starting".into(),
                Err(error) => last = format!("{error:#}"),
            }
            ensure!(
                Instant::now() < deadline,
                "{alias} did not reach {status:?} with fresh media in {seconds}s; last: {last}"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    fn receiving(&self, alias: &str, destination: Option<&str>) -> bool {
        let spec = &self.streams[alias];
        let destinations = spec
            .destinations
            .iter()
            .filter(|d| destination.is_none_or(|id| d.id == id))
            .collect::<Vec<_>>();
        !destinations.is_empty()
            && destinations.iter().all(|d| {
                self.consumers
                    .get(&format!("{alias}/{}", d.id))
                    .is_some_and(|probe| {
                        probe
                            .progress
                            .lock()
                            .unwrap()
                            .healthy(Instant::now(), Duration::from_secs(2))
                    })
            })
    }

    async fn service(&mut self, service: &str, action: ServiceAction) -> Result<()> {
        let actual = if service == "leader" {
            let mut leader = None;
            for (name, port) in [("controller", 29082), ("controller-2", 29083)] {
                if self
                    .http
                    .get(format!("http://localhost:{port}/status"))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
                {
                    leader = Some(name.to_string());
                    break;
                }
            }
            leader.context("no leading controller")?
        } else {
            service.to_string()
        };
        self.services.insert(actual.clone());
        self.report.event(
            "service_action",
            json!({"service": actual, "action": action}),
        )?;
        let container = format!("ow-{actual}");
        let args = match action {
            ServiceAction::Stop => vec!["stop", "--time", "5", &container],
            ServiceAction::Start => vec!["start", &container],
            ServiceAction::Restart => vec!["restart", "--time", "5", &container],
            ServiceAction::Kill => vec!["kill", "--signal", "KILL", &container],
        };
        docker(&args).await?;
        match action {
            ServiceAction::Stop => {
                self.stopped.insert(actual);
            }
            ServiceAction::Start | ServiceAction::Restart | ServiceAction::Kill => {
                self.stopped.remove(&actual);
            }
        }
        Ok(())
    }

    async fn netem_command(&self, node: &str, args: &[String]) -> Result<String> {
        let script = self.root.join("scripts/netem.sh");
        let mut command = Command::new("bash");
        command.arg(script).arg(node).args(args);
        output(command).await
    }

    async fn flows_on(&self, node: usize) -> Result<Vec<FlowIdentity>> {
        let response: FlowListResponse = self
            .http
            .get(format!("http://localhost:{}/api/flows", 28079 + node))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(response
            .flows
            .into_iter()
            .filter(|f| f.name.starts_with("weave-"))
            .map(|f| FlowIdentity {
                node,
                name: f.name,
                id: f.id,
                running: f.running,
            })
            .collect())
    }

    async fn flows(&self, name: &str) -> Result<Vec<FlowIdentity>> {
        let mut flows = Vec::new();
        for node in 1..=4 {
            if self.stopped.contains(&format!("strom-{node}")) {
                continue;
            }
            flows.extend(
                self.flows_on(node)
                    .await?
                    .into_iter()
                    .filter(|f| f.name.starts_with(&format!("weave-{name}-"))),
            );
        }
        flows.sort_by(|a, b| (&a.node, &a.name).cmp(&(&b.node, &b.name)));
        Ok(flows)
    }

    async fn snapshot(&self, alias: &str) -> Result<Snapshot> {
        let name = self.name(alias)?;
        let response = self
            .http
            .get(format!("{}/streams/{name}", self.url))
            .bearer_auth(&self.token)
            .send()
            .await?
            .error_for_status()?;
        let etag = response
            .headers()
            .get(ETAG)
            .context("missing stream ETag")?
            .to_str()?
            .to_string();
        let resource: StreamResource = response.json().await?;
        Ok(Snapshot {
            stream: alias.to_string(),
            etag,
            generation: resource.generation,
            flows: self.flows(name).await?,
        })
    }

    async fn delete(&mut self, alias: &str) -> Result<()> {
        if let Some(name) = self.producers.remove(alias) {
            docker(&["rm", "-f", &name]).await?;
            self.containers.retain(|n| n != &name);
        }
        let receivers = self
            .consumers
            .keys()
            .filter(|r| r.starts_with(&format!("{alias}/")))
            .cloned()
            .collect::<Vec<_>>();
        for receiver in receivers {
            let probe = self.consumers.remove(&receiver).unwrap();
            self.completed_media
                .push(json!({"receiver": receiver, "progress": probe.summary()}));
            docker(&["rm", "-f", &probe.container]).await?;
            self.containers.retain(|n| n != &probe.container);
        }
        let name = self.name(alias)?.to_string();
        self.delete_resource(&name).await?;
        let deadline = Instant::now() + Duration::from_secs(45);
        while !self.flows(&name).await?.is_empty() {
            ensure!(
                Instant::now() < deadline,
                "{alias} left managed flows behind"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        self.streams.remove(alias);
        Ok(())
    }

    async fn delete_resource(&self, name: &str) -> Result<()> {
        let url = format!("{}/streams/{name}", self.url);
        let response = self.http.get(&url).bearer_auth(&self.token).send().await?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        response.error_for_status_ref()?;
        let etag = response
            .headers()
            .get(ETAG)
            .context("missing stream ETag")?
            .clone();
        self.http
            .delete(url)
            .bearer_auth(&self.token)
            .header(IF_MATCH, etag)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn cleanup(&mut self) -> Result<()> {
        let _ = self.report.event(
            "cleanup_started",
            json!({
                "streams": self.streams.len(),
                "endpoints": self.containers.len(),
                "impairments": self.impaired.len(),
                "services": self.services.len(),
            }),
        );
        let mut errors = Vec::new();
        for node in &self.impaired {
            let _ = self.report.event(
                "cleanup_action",
                json!({"detail": format!("clearing impairment on {node}")}),
            );
            if let Err(error) = self.netem_command(node, &["clear".into()]).await {
                errors.push(format!("clear {node}: {error:#}"));
            }
        }
        for service in &self.services {
            let _ = self.report.event(
                "cleanup_action",
                json!({"detail": format!("restoring {service}")}),
            );
            if let Err(error) = docker(&["start", &format!("ow-{service}")]).await {
                errors.push(format!("restore {service}: {error:#}"));
            }
        }
        self.stopped.clear();
        self.consumers.clear();
        for container in &self.containers {
            let _ = self.report.event(
                "cleanup_action",
                json!({"detail": format!("removing {container}")}),
            );
            if let Err(error) = docker(&["rm", "-f", container]).await {
                errors.push(format!("remove {container}: {error:#}"));
            }
        }
        let deadline = Instant::now() + Duration::from_secs(45);
        for spec in self.streams.values() {
            let _ = self.report.event(
                "cleanup_action",
                json!({"detail": format!("deleting {}", spec.name)}),
            );
            loop {
                match self.delete_resource(&spec.name).await {
                    Ok(()) => break,
                    Err(error) if Instant::now() >= deadline => {
                        errors.push(format!("delete {}: {error:#}", spec.name));
                        break;
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
                }
            }
        }
        for spec in self.streams.values() {
            let _ = self.report.event(
                "cleanup_action",
                json!({"detail": format!("waiting for {} flows to disappear", spec.name)}),
            );
            loop {
                match self.flows(&spec.name).await {
                    Ok(flows) if flows.is_empty() => break,
                    result if Instant::now() >= deadline => {
                        errors.push(format!("flows remaining for {}: {result:?}", spec.name));
                        break;
                    }
                    _ => tokio::time::sleep(Duration::from_millis(500)).await,
                }
            }
        }
        self.report.event("cleanup", &errors)?;
        ensure!(errors.is_empty(), "cleanup failed: {}", errors.join("; "));
        Ok(())
    }

    pub fn media_summary(&self) -> Vec<Value> {
        let mut summary = self.completed_media.clone();
        summary.extend(self.consumers.iter().map(|(receiver, probe)| {
            json!({
                "receiver": receiver, "progress": probe.summary(),
            })
        }));
        summary
    }
}

pub fn media_url(endpoint: &EndpointAddr, spec: &StreamTransport) -> Result<String> {
    let StreamTransport::Srt(spec) = spec else {
        bail!("bench media tools currently require SRT external endpoints")
    };
    let mut url = reqwest::Url::parse(&endpoint.url)?;
    ensure!(url.scheme() == "srt", "expected SRT endpoint");
    url.query_pairs_mut()
        .append_pair("mode", "caller")
        .append_pair("latency", &spec.latency.unwrap_or(200).to_string());
    if let Some(passphrase) = &spec.passphrase {
        url.query_pairs_mut()
            .append_pair("passphrase", passphrase.expose());
    }
    Ok(url.to_string())
}

pub async fn docker(args: &[&str]) -> Result<String> {
    let mut command = Command::new("docker");
    command.args(args);
    output(command).await
}

pub async fn logs(container: &str) -> Result<String> {
    let mut command = Command::new("docker");
    command
        .args(["logs", "--tail", "200", container])
        .kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(15), command.output()).await??;
    ensure!(output.status.success(), "cannot read logs for {container}");
    Ok(format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    ))
}

async fn output(mut command: Command) -> Result<String> {
    command.kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(120), command.output())
        .await
        .context("bench command timed out")??;
    ensure!(
        output.status.success(),
        "bench command failed: {}",
        redact(&String::from_utf8_lossy(&output.stderr))
    );
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

pub fn telemetry(bench: &Bench) -> tokio::task::JoinHandle<()> {
    let http = bench.http.clone();
    let url = bench.url.clone();
    let token = bench.token.clone();
    let report = bench.report.clone();
    let prefix = format!("weave-{}-", bench.prefix);
    tokio::spawn(async move {
        loop {
            for path in ["/status", "/nodes"] {
                let observation = async {
                    let response = http.get(format!("{url}{path}")).bearer_auth(&token).send().await?;
                    Ok::<_, reqwest::Error>(json!({"status": response.status().as_u16(), "body": response.json::<Value>().await?}))
                }.await;
                let data = match observation {
                    Ok(value) => value,
                    Err(error) => json!({"error": error.to_string()}),
                };
                let _ = report.event(path, data);
            }
            for node in 1..=4 {
                let strom = StromClient::with_timeout(
                    format!("http://localhost:{}", 28079 + node),
                    Duration::from_secs(2),
                );
                match strom.list_flows().await {
                    Ok(flows) => {
                        for flow in flows
                            .into_iter()
                            .filter(|flow| flow.name.starts_with(&prefix))
                        {
                            let stats = strom.srt_stats(&flow.id).await;
                            let _ = report.event("runtime", json!({
                                "node": node, "name": flow.name, "id": flow.id, "running": flow.running,
                                "gst_state": flow.gst_state, "srt_stats": stats.ok(),
                            }));
                        }
                    }
                    Err(error) => {
                        let _ = report.event(
                            "runtime_error",
                            json!({"node": node, "error": error.to_string()}),
                        );
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    })
}
