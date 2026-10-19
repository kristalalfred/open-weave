use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use clap::{Parser, Subcommand, ValueEnum};
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use srt_tokio::SrtSocket;
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

const NORTH: &str = "http://127.0.0.1:39080";
const STREAM: &str = "timed-demo";

#[derive(Parser)]
#[command(
    name = "weave-timed-demo",
    about = "Run an isolated local SRT timing demo with generated video, audio, and independently controllable relay paths"
)]
struct Args {
    #[command(subcommand)]
    action: Option<Action>,
}

#[derive(Subcommand)]
enum Action {
    #[command(about = "Start the local media demo")]
    Run,
    #[command(about = "Show delivery status and decoded media progress")]
    Status,
    #[command(about = "Select fixed 500 ms delivery")]
    Fixed,
    #[command(about = "Calibrate and lock delivery latency")]
    Auto,
    #[command(about = "Pause relay paths until restored or the optional timeout expires")]
    Fail {
        #[arg(value_enum, default_value_t = RelayTarget::A, help = "Relay paths to pause")]
        path: RelayTarget,
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..), help = "Restore these paths automatically after this many seconds")]
        seconds: Option<u64>,
    },
    #[command(about = "Resume relay paths")]
    Restore {
        #[arg(value_enum, default_value_t = RelayTarget::Both, help = "Relay paths to resume")]
        path: RelayTarget,
    },
    #[command(about = "Open the video and audio preview")]
    View,
    #[command(about = "Stop the local demo")]
    Stop,
}

#[derive(Clone, Copy, ValueEnum)]
enum RelayTarget {
    A,
    B,
    Both,
}

impl RelayTarget {
    fn paths(self) -> &'static [&'static str] {
        match self {
            Self::A => &["a"],
            Self::B => &["b"],
            Self::Both => &["a", "b"],
        }
    }
}

#[derive(Deserialize, Serialize)]
struct Fault {
    restore_at_unix_ms: Option<u64>,
}

fn unix_ms() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}

fn change_paths(path: RelayTarget, fail: bool, seconds: Option<u64>) -> Result<()> {
    let root = root();
    ensure!(root.join("run.lock").exists(), "timed demo is not running");
    let restore_at_unix_ms = seconds
        .map(|seconds| {
            unix_ms()?
                .checked_add(seconds.checked_mul(1_000).context("timeout is too large")?)
                .context("timeout is too large")
        })
        .transpose()?;
    for path in path.paths() {
        let target = root.join(format!("fault-{path}.json"));
        if fail {
            let temporary = target.with_extension(format!("{}.tmp", std::process::id()));
            std::fs::write(
                &temporary,
                serde_json::to_vec(&Fault { restore_at_unix_ms })?,
            )?;
            if let Err(error) = std::fs::rename(&temporary, &target) {
                let _ = std::fs::remove_file(&temporary);
                return Err(error.into());
            }
        } else {
            match std::fs::remove_file(&target) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        println!(
            "Relay {path}: {} requested",
            if fail { "pause" } else { "restore" }
        );
    }
    Ok(())
}

fn apply_faults(resources: &Resources, paused: &mut [bool; 2]) -> Result<()> {
    let now = unix_ms()?;
    for (path, paused) in RelayTarget::Both.paths().iter().zip(paused) {
        let fault = match std::fs::read(resources.root.join(format!("fault-{path}.json"))) {
            Ok(bytes) => Some(serde_json::from_slice::<Fault>(&bytes)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let requested = fault.is_some_and(|fault| {
            fault
                .restore_at_unix_ms
                .is_none_or(|deadline| now < deadline)
        });
        if requested == *paused {
            continue;
        }
        let name = format!("relay-{path}");
        let relay = &resources
            .children
            .iter()
            .find(|(id, _)| id == &name)
            .context("relay process")?
            .1;
        ensure!(
            Command::new("kill")
                .args([
                    if requested { "-STOP" } else { "-CONT" },
                    &relay.id().to_string()
                ])
                .status()?
                .success(),
            "changing relay {path} state failed"
        );
        *paused = requested;
        println!(
            "Relay {path} {}",
            if requested { "paused" } else { "restored" }
        );
    }
    Ok(())
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.demo/timed")
}

struct Resources {
    root: PathBuf,
    children: Vec<(String, Child)>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Resources {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
        for (_, child) in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_file(self.root.join("run.lock"));
        let _ = std::fs::remove_file(self.root.join("stop"));
        for path in RelayTarget::Both.paths() {
            let _ = std::fs::remove_file(self.root.join(format!("fault-{path}.json")));
        }
    }
}

impl Resources {
    fn child(&mut self, name: &str, executable: &Path, args: &[&str]) -> Result<()> {
        let log = File::create(self.root.join(format!("{name}.log")))?;
        let child = Command::new(executable)
            .args(args)
            .env("WEAVE_AUTH_DISABLED", "1")
            .env(
                "WEAVE_SRT_KEY_SECRET",
                "timed-demo-local-media-key-00000000",
            )
            .env_remove("DATABASE_URL")
            .env_remove("WEAVE_WEBHOOK_URL")
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()
            .with_context(|| format!("starting {name}"))?;
        self.children.push((name.into(), child));
        Ok(())
    }
}

async fn get(client: &Client, path: &str) -> Result<Value> {
    Ok(client
        .get(format!("{NORTH}{path}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn wait(client: &Client, path: &str, ready: impl Fn(&Value) -> bool) -> Result<Value> {
    let started = Instant::now();
    loop {
        if let Ok(value) = get(client, path).await
            && ready(&value)
        {
            return Ok(value);
        }
        ensure!(
            started.elapsed() < Duration::from_secs(45),
            "demo did not become ready at {path}; inspect {}",
            root().display()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn policy(automatic: bool) -> Value {
    if automatic {
        json!({"mode":"auto","min_latency_ms":80,"max_latency_ms":500,"tolerance_ms":10})
    } else {
        json!({"mode":"fixed","latency_ms":500,"tolerance_ms":10})
    }
}

async fn change_policy(client: &Client, automatic: bool) -> Result<()> {
    let response = client
        .get(format!("{NORTH}/streams/{STREAM}"))
        .send()
        .await?
        .error_for_status()?;
    let etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .context("stream has no ETag")?
        .clone();
    let mut resource: Value = response.json().await?;
    resource["spec"]["destinations"][0]["delivery"] = policy(automatic);
    client
        .post(format!("{NORTH}/streams"))
        .header(reqwest::header::IF_MATCH, etag)
        .json(&resource["spec"])
        .send()
        .await?
        .error_for_status()?;
    std::fs::write(
        root().join("stream.yaml"),
        serde_norway::to_string(&resource["spec"])?,
    )?;
    println!(
        "Applied {} delivery policy",
        if automatic {
            "automatic calibration"
        } else {
            "fixed 500 ms"
        }
    );
    Ok(())
}

fn attachment(id: &str, network: &str, dial: bool, listener: Option<(&str, u16, u16)>) -> Value {
    let mut value = json!({"id":id,"network":network,"dial":dial});
    if let Some((host, start, end)) = listener {
        value["listeners"] = json!({"srt":{"host":host,"port_range":{"start":start,"end":end}}});
    }
    value
}

async fn peers(source: &Value, destination: &Value) -> Result<Vec<JoinHandle<()>>> {
    let source_address = format!(
        "{}:{}",
        source["host"].as_str().context("source host")?,
        source["port"].as_u64().context("source port")?
    );
    let destination_address = format!(
        "{}:{}",
        destination["host"].as_str().context("destination host")?,
        destination["port"].as_u64().context("destination port")?
    );
    let connect = |address: String| async move {
        let started = Instant::now();
        loop {
            if let Ok(Ok(socket)) = tokio::time::timeout(
                Duration::from_secs(3),
                SrtSocket::builder()
                    .latency(Duration::from_millis(120))
                    .set(|options| {
                        options.sender.buffer_size = srt_tokio::options::ByteCount(8 * 1024 * 1024)
                    })
                    .call(address.as_str(), None),
            )
            .await
            {
                return Ok::<_, anyhow::Error>(socket);
            }
            ensure!(
                started.elapsed() < Duration::from_secs(45),
                "SRT peer did not connect to {address}"
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    let mut producer = connect(source_address).await?;
    let mut consumer = connect(destination_address).await?;
    let mut producer_stats = producer.statistics().clone();
    let mut consumer_stats = consumer.statistics().clone();
    let statistics = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(stats) = producer_stats.next() => {
                    let _ = std::fs::write(root().join("producer-srt.log"), format!("{stats:#?}"));
                },
                Some(stats) = consumer_stats.next() => {
                    let _ = std::fs::write(root().join("consumer-srt.log"), format!("{stats:#?}"));
                },
                else => return,
            }
        }
    });
    let input = UdpSocket::bind("127.0.0.1:39092").await?;
    let output = UdpSocket::bind("127.0.0.1:0").await?;
    let sending = tokio::spawn(async move {
        let mut packet = [0_u8; 65_536];
        while let Ok(length) = input.recv(&mut packet).await {
            if producer
                .send((Instant::now(), Bytes::copy_from_slice(&packet[..length])))
                .await
                .is_err()
            {
                return;
            }
        }
    });
    let receiving = tokio::spawn(async move {
        while let Some(Ok((_, payload))) = consumer.next().await {
            let _ = output.send_to(&payload, "127.0.0.1:39093").await;
            let _ = output.send_to(&payload, "127.0.0.1:39094").await;
        }
    });
    Ok(vec![sending, receiving, statistics])
}

async fn run(client: &Client) -> Result<()> {
    let root = root();
    std::fs::create_dir_all(&root)?;
    let mut lock = OpenOptions::new().write(true).create_new(true).open(root.join("run.lock"))
        .context("timed demo has a run.lock; stop the running demo or inspect its recorded process before removing a stale lock")?;
    writeln!(lock, "{}", std::process::id())?;
    let mut resources = Resources {
        root: root.clone(),
        children: vec![],
        tasks: vec![],
    };
    let _ = std::fs::remove_file(root.join("stop"));
    for path in RelayTarget::Both.paths() {
        let _ = std::fs::remove_file(root.join(format!("fault-{path}.json")));
    }
    let binaries = std::env::current_exe()?
        .parent()
        .context("demo binary directory")?
        .to_owned();
    let node_ttl = u64::MAX.to_string();
    resources.child(
        "controller",
        &binaries.join("weave-controller"),
        &[
            "--listen",
            "127.0.0.1:39082",
            "--interval-secs",
            "1",
            "--node-ttl-secs",
            &node_ttl,
        ],
    )?;
    resources.child(
        "northbound",
        &binaries.join("weave-northbound"),
        &[
            "--listen",
            "127.0.0.1:39080",
            "--controller-url",
            "http://127.0.0.1:39082",
        ],
    )?;
    resources.child(
        "southbound",
        &binaries.join("weave-southbound"),
        &[
            "--listen",
            "127.0.0.1:39081",
            "--controller-url",
            "http://127.0.0.1:39082",
        ],
    )?;
    let configurations = [
        (
            "relay-a",
            "127.0.0.1",
            39086,
            vec![attachment(
                "a",
                "a",
                true,
                Some(("127.0.0.1", 43400, 43431)),
            )],
        ),
        (
            "source",
            "127.0.0.1",
            39084,
            vec![
                attachment("in", "0-source", false, Some(("127.0.0.1", 43200, 43215))),
                attachment("a", "a", true, None),
                attachment("b", "b", true, None),
            ],
        ),
        (
            "studio",
            "127.0.0.1",
            39085,
            vec![
                attachment("out", "0-output", false, Some(("127.0.0.1", 43100, 43115))),
                attachment("a", "a", true, None),
                attachment("b", "b", true, None),
            ],
        ),
        (
            "relay-b",
            "127.0.0.1",
            39087,
            vec![attachment(
                "b",
                "b",
                true,
                Some(("127.0.0.1", 43300, 43331)),
            )],
        ),
    ];
    for (id, ip, health, attachments) in configurations {
        let configuration = json!({"node":{"id":id,"listen":format!("127.0.0.1:{health}"),"southbound_url":"http://127.0.0.1:39081","topology":{"attachments":attachments}},"clock":{"provider":"shared_host","domain":"timed-demo","tai_offset_secs":37},"media":{"listen_ip":ip}});
        let path = root.join(format!("{id}.yaml"));
        std::fs::write(&path, serde_norway::to_string(&configuration)?)?;
        resources.child(
            id,
            &binaries.join("weave-node"),
            &["--config", path.to_str().context("configuration path")?],
        )?;
    }
    wait(client, "/nodes", |value| {
        value.as_array().is_some_and(|nodes| nodes.len() == 4)
    })
    .await?;
    let media_format = json!({"container":"mpeg_ts","video":{"codec":"h264","width":640,"height":360,"framerate":{"numerator":25,"denominator":1},"chroma_subsampling":"yuv420"},"audio":{"codec":"aac","sample_rate":48000,"channels":1}});
    let manifest = json!({"name":STREAM,"format_policy":"strict","source":{"srt":{"node":"source","latency":120,"format":media_format}},"destinations":[{"id":"studio","paths":2,"delivery":policy(false),"srt":{"node":"studio","latency":60,"accepts":{"container":["mpeg_ts"],"video":{"codec":["h264"]},"audio":{"codec":["aac"],"sample_rate":[48000],"channels":[1]}}}}]});
    std::fs::write(
        root.join("stream.yaml"),
        serde_norway::to_string(&manifest)?,
    )?;
    client
        .post(format!("{NORTH}/streams"))
        .header(reqwest::header::IF_NONE_MATCH, "*")
        .json(&manifest)
        .send()
        .await?
        .error_for_status()?;
    let endpoints = wait(client, &format!("/streams/{STREAM}/endpoints"), |value| {
        value["ingress"].is_object() && value["destinations"][0]["endpoint"].is_object()
    })
    .await?;
    resources.tasks.extend(
        peers(
            &endpoints["ingress"],
            &endpoints["destinations"][0]["endpoint"],
        )
        .await?,
    );
    resources.child(
        "decoder",
        Path::new("ffmpeg"),
        &[
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "info",
            "-probesize",
            "200000",
            "-analyzeduration",
            "1000000",
            "-i",
            "udp://127.0.0.1:39093?fifo_size=4096&overrun_nonfatal=1",
            "-map",
            "0:v:0",
            "-map",
            "0:a:0",
            "-vf",
            "showinfo",
            "-af",
            "ashowinfo",
            "-progress",
            "pipe:1",
            "-f",
            "null",
            "-",
        ],
    )?;
    resources.child(
        "producer",
        Path::new("ffmpeg"),
        &[
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-re",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=640x360:rate=25",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:sample_rate=48000",
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-b:v",
            "2M",
            "-g",
            "25",
            "-c:a",
            "aac",
            "-f",
            "mpegts",
            "udp://127.0.0.1:39092?pkt_size=1316",
        ],
    )?;
    println!("Timed media demo: http://127.0.0.1:39082/ui");
    println!(
        "Commands: just timed-demo status | auto | fixed | fail [a|b|both] | restore [a|b|both] | view | stop"
    );
    println!(
        "The demo uses one shared host clock. Decoder and display latency are outside the managed timing boundary."
    );
    let mut paused = [false; 2];
    loop {
        if root.join("stop").exists() {
            return Ok(());
        }
        apply_faults(&resources, &mut paused)?;
        for (name, child) in &mut resources.children {
            if let Some(status) = child.try_wait()? {
                bail!(
                    "{name} exited with {status}; inspect {}",
                    root.join(format!("{name}.log")).display()
                );
            }
        }
        ensure!(
            resources.tasks.iter().all(|task| !task.is_finished()),
            "a demo media peer exited"
        );
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(200)) => {},
            result = weave_core::shutdown::signal() => { result?; return Ok(()); }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    weave_core::logging::init();
    let args = Args::parse();
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(5))
        .build()?;
    match args.action.unwrap_or(Action::Run) {
        Action::Run => tokio::select! {
            result = run(&client) => result,
            result = weave_core::shutdown::signal() => result.context("waiting for demo shutdown"),
        },
        Action::Auto => change_policy(&client, true).await,
        Action::Fixed => change_policy(&client, false).await,
        Action::Status => {
            println!(
                "{}",
                serde_json::to_string_pretty(&get(&client, "/status").await?)?
            );
            let log = std::fs::read_to_string(root().join("decoder.log")).unwrap_or_default();
            if let Some(frame) = log.lines().rev().find(|line| line.starts_with("frame=")) {
                println!("Decoded video: {frame}");
            }
            if let Some(audio) = log.lines().rev().find(|line| line.contains("nb_samples:")) {
                println!("Decoded audio: {audio}");
            }
            Ok(())
        }
        Action::Fail { path, seconds } => change_paths(path, true, seconds),
        Action::Restore { path } => change_paths(path, false, None),
        Action::Stop => {
            std::fs::write(root().join("stop"), "stop")?;
            Ok(())
        }
        Action::View => {
            ensure!(
                root().join("run.lock").exists(),
                "timed demo is not running"
            );
            Command::new("ffplay")
                .args([
                    "-loglevel",
                    "error",
                    "-fflags",
                    "nobuffer",
                    "-i",
                    "udp://127.0.0.1:39094?fifo_size=4096&overrun_nonfatal=1",
                ])
                .status()
                .context("opening the demo preview with ffplay")?;
            Ok(())
        }
    }
}
