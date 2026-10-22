use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::sync::{Arc, Mutex, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use clap::ValueEnum;
use console::{Term, measure_text_width, truncate_str};
use serde_json::Value;

use crate::report::{EventObserver, redact};
use crate::scenario::Step;

#[derive(Clone, Copy, Default, ValueEnum)]
pub enum ProgressMode {
    #[default]
    Auto,
    Plain,
    Off,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Live,
    Plain,
    Off,
}

#[derive(Clone)]
pub struct Progress {
    state: Arc<Mutex<State>>,
    enabled: bool,
}

pub struct CaseOutput {
    progress: Progress,
    stop: mpsc::Sender<()>,
    worker: Option<JoinHandle<io::Result<()>>>,
    term: Term,
}

struct ActiveStep {
    step: Step,
    started: Instant,
    number: usize,
}

struct Receiver {
    frames: u64,
    audio_samples: u64,
    retries: u64,
    video_idle_ms: Option<u64>,
    audio_idle_ms: Option<u64>,
    ended: bool,
    sampled: Instant,
}

struct State {
    name: String,
    prefix: String,
    index: usize,
    cases: usize,
    started: Instant,
    phase: String,
    detail: String,
    total_steps: usize,
    started_steps: usize,
    finished_steps: usize,
    active: Vec<ActiveStep>,
    receivers: BTreeMap<String, Receiver>,
    streams: BTreeMap<String, String>,
    control_error: Option<String>,
    nodes: String,
    diagnostic: String,
    failure: Option<String>,
    pending: VecDeque<String>,
}

impl CaseOutput {
    pub fn new(
        name: &str,
        prefix: &str,
        index: usize,
        cases: usize,
        steps: &[Step],
        mode: ProgressMode,
    ) -> io::Result<Self> {
        let term = Term::buffered_stderr();
        let mode = match mode {
            ProgressMode::Off => Mode::Off,
            ProgressMode::Plain => Mode::Plain,
            ProgressMode::Auto
                if term.is_term() && std::env::var("TERM").ok().as_deref() != Some("dumb") =>
            {
                Mode::Live
            }
            ProgressMode::Auto => Mode::Plain,
        };
        let state = State::new(name, prefix, index, cases, steps);
        if mode == Mode::Plain {
            term.write_line(&format!("RUN  [{index}/{cases}] {name}"))?;
            term.flush()?;
        }
        let progress = Progress {
            state: Arc::new(Mutex::new(state)),
            enabled: mode != Mode::Off,
        };
        let (stop, stopped) = mpsc::channel();
        let worker = if mode == Mode::Off {
            None
        } else {
            let state = progress.state.clone();
            let term = term.clone();
            Some(std::thread::spawn(move || {
                render(term, state, stopped, mode)
            }))
        };
        Ok(Self {
            progress,
            stop,
            worker,
            term,
        })
    }

    pub fn progress(&self) -> Progress {
        self.progress.clone()
    }

    fn stop(&mut self) -> io::Result<()> {
        let _ = self.stop.send(());
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| io::Error::other("progress renderer panicked"))??;
        }
        Ok(())
    }

    pub fn finish(&mut self, outcome: &Value) -> io::Result<()> {
        self.stop()?;
        let state = self.progress.state.lock().unwrap();
        self.term.write_line(&result_line(&state, outcome))?;
        for field in ["preflight_error", "error", "cleanup_error"] {
            if let Some(error) = outcome[field].as_str() {
                self.term
                    .write_line(&format!("  {field}: {}", clean(error)))?;
            }
        }
        if let Some(errors) = outcome["diagnostic_errors"].as_array() {
            for error in errors.iter().filter_map(Value::as_str) {
                self.term
                    .write_line(&format!("  diagnostics: {}", clean(error)))?;
            }
        }
        self.term.flush()
    }
}

impl Drop for CaseOutput {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

impl Progress {
    pub fn failure(&self, error: &str) {
        if !self.enabled {
            return;
        }
        let mut state = self.state.lock().unwrap();
        state.failure = Some(error.to_string());
        state
            .pending
            .push_back(format!("  failure: {}", failure_reason(error)));
    }

    pub fn phase(&self, phase: &str, detail: &str) {
        if !self.enabled {
            return;
        }
        let mut state = self.state.lock().unwrap();
        if state.phase != phase {
            state.pending.push_back(format!("  {}", phase));
        }
        state.phase = phase.to_string();
        state.detail = detail.to_string();
    }

    pub fn detail(&self, detail: &str) {
        if self.enabled {
            self.state.lock().unwrap().detail = detail.to_string();
        }
    }
}

impl EventObserver for Progress {
    fn event(&self, kind: &str, data: &Value) {
        if self.enabled {
            self.state.lock().unwrap().event(kind, data, Instant::now());
        }
    }
}

impl State {
    fn new(name: &str, prefix: &str, index: usize, cases: usize, steps: &[Step]) -> Self {
        Self {
            name: name.to_string(),
            prefix: format!("{prefix}-"),
            index,
            cases,
            started: Instant::now(),
            phase: "preflight".into(),
            detail: "checking the bench".into(),
            total_steps: count_steps(steps),
            started_steps: 0,
            finished_steps: 0,
            active: Vec::new(),
            receivers: BTreeMap::new(),
            streams: BTreeMap::new(),
            control_error: None,
            nodes: String::new(),
            diagnostic: String::new(),
            failure: None,
            pending: VecDeque::new(),
        }
    }

    fn event(&mut self, kind: &str, data: &Value, now: Instant) {
        match kind {
            "step_started" => {
                if let Ok(step) = serde_json::from_value::<Step>(data["step"].clone()) {
                    self.phase = "running".into();
                    self.detail.clear();
                    self.started_steps += 1;
                    self.pending.push_back(format!(
                        "  {}/{} {}",
                        self.started_steps,
                        self.total_steps,
                        step_label(&step)
                    ));
                    self.active.push(ActiveStep {
                        step,
                        started: now,
                        number: self.started_steps,
                    });
                }
            }
            "step_finished" => {
                self.finished_steps += 1;
                self.active.pop();
            }
            "receiver_progress" => {
                if let Some(receiver) = data["receiver"].as_str() {
                    self.receivers.insert(
                        receiver.to_string(),
                        Receiver {
                            frames: data["frames"].as_u64().unwrap_or(0),
                            audio_samples: data["audio_samples"].as_u64().unwrap_or(0),
                            retries: data["attempts"].as_u64().unwrap_or(0).saturating_sub(1),
                            video_idle_ms: data["video_idle_ms"].as_u64(),
                            audio_idle_ms: data["audio_idle_ms"].as_u64(),
                            ended: data["ended"].as_bool().unwrap_or(false),
                            sampled: now,
                        },
                    );
                    if let Some(error) = data["observer_error"].as_str() {
                        self.diagnostic = format!("{receiver}: {error}");
                    }
                }
            }
            "await_status" => {
                self.control_error = None;
                self.stream(data);
            }
            "/status" => {
                self.control_error = data["error"].as_str().map(str::to_string);
                if let Some(status) = data["status"].as_u64()
                    && status != 200
                {
                    self.control_error = Some(format!("HTTP {status}"));
                }
                if let Some(streams) = data["body"]["streams"].as_array() {
                    self.streams.clear();
                    for stream in streams {
                        self.stream(stream);
                    }
                }
            }
            "/nodes" | "preflight_nodes" => {
                let nodes = if kind == "/nodes" {
                    &data["body"]
                } else {
                    data
                };
                if let Some(nodes) = nodes.as_array() {
                    let mut counts = BTreeMap::<&str, usize>::new();
                    for resource in nodes {
                        *counts
                            .entry(resource["node"]["status"].as_str().unwrap_or("unknown"))
                            .or_default() += 1;
                    }
                    self.nodes = counts
                        .into_iter()
                        .map(|(status, count)| format!("{count} {status}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                }
            }
            "preflight_check" | "cleanup_action" => {
                self.detail = data["detail"].as_str().unwrap_or("").to_string();
            }
            "cleanup_started" => {
                self.phase = "cleanup".into();
                self.detail = format!(
                    "{} streams, {} endpoints, {} impairments, {} services",
                    data["streams"], data["endpoints"], data["impairments"], data["services"]
                );
                self.pending
                    .push_back(format!("  cleanup: {}", self.detail));
            }
            "media_started" => {
                self.detail = format!(
                    "media peer at {} on {}",
                    data["host"].as_str().unwrap_or("?"),
                    data["site"].as_str().unwrap_or("?")
                );
            }
            "service_action" => {
                self.detail = format!(
                    "{} {}",
                    data["action"].as_str().unwrap_or("?"),
                    data["service"].as_str().unwrap_or("?")
                );
            }
            "media_log" => {
                let line = data["line"].as_str().unwrap_or("");
                if line.contains("Error ")
                    || line.contains("failed:")
                    || line.starts_with("BENCH_EXIT")
                {
                    self.diagnostic = line.to_string();
                }
            }
            _ => (),
        }
    }

    fn stream(&mut self, data: &Value) {
        if let Some(name) = data["name"]
            .as_str()
            .and_then(|name| name.strip_prefix(&self.prefix))
        {
            let mut status = data["status"].as_str().unwrap_or("unknown").to_string();
            if let Some(condition) = data["conditions"].as_array().and_then(|conditions| {
                conditions
                    .iter()
                    .find(|condition| condition["status"] == "false")
            }) && let Some(reason) = condition["reason"].as_str()
            {
                status.push_str(&format!(" ({reason})"));
            }
            self.streams.insert(name.to_string(), status);
        }
    }

    fn control(&self) -> String {
        if let Some(error) = &self.control_error {
            format!("control unavailable: {error}")
        } else {
            self.streams
                .iter()
                .map(|(name, status)| format!("{name}: {status}"))
                .collect::<Vec<_>>()
                .join(" | ")
        }
    }

    fn lines(&self, now: Instant, tick: usize) -> Vec<String> {
        let spinner = if self.failure.is_some() {
            "!"
        } else {
            ["|", "/", "-", "\\"][tick % 4]
        };
        let mut lines = vec![format!(
            "{spinner} [{}/{}] {}  {} elapsed  {}/{} steps complete",
            self.index,
            self.cases,
            self.name,
            clock(now.duration_since(self.started)),
            self.finished_steps,
            self.total_steps
        )];
        if self.phase == "running" && !self.active.is_empty() {
            for active in self.active.iter().rev().take(2).rev() {
                lines.push(format!(
                    "  > {}/{} {}",
                    active.number,
                    self.total_steps,
                    step_label(&active.step)
                ));
                lines.push(format!("    {}", step_clock(active, now)));
            }
            if !self.detail.is_empty() {
                lines.push(format!("  {}", self.detail));
            }
        } else {
            lines.push(format!("  {}: {}", self.phase, self.detail));
        }
        let control = self.control();
        if !control.is_empty() {
            lines.push(format!("  {control}"));
        }
        if !self.nodes.is_empty() {
            lines.push(format!("  nodes: {}", self.nodes));
        }
        if self.phase == "running" {
            for (receiver, progress) in self.receivers.iter().take(4) {
                lines.push(format!("  {receiver}: {}", progress.line(now)));
            }
            if self.receivers.len() > 4 {
                lines.push(format!("  +{} more receivers", self.receivers.len() - 4));
            }
        }
        if !self.diagnostic.is_empty() {
            lines.push(format!("  last diagnostic: {}", self.diagnostic));
        }
        if let Some(error) = &self.failure {
            lines.push(format!("  failure: {}", failure_reason(error)));
        }
        lines
    }

    fn heartbeat(&self, now: Instant) -> String {
        let current = self
            .active
            .last()
            .filter(|_| self.phase == "running")
            .map_or_else(
                || format!("{}: {}", self.phase, self.detail),
                |active| format!("{} {}", step_label(&active.step), step_clock(active, now)),
            );
        let receivers = self
            .receivers
            .iter()
            .take(2)
            .map(|(receiver, progress)| format!("{receiver}: {}", progress.line(now)))
            .collect::<Vec<_>>()
            .join(" | ");
        let mut parts = vec![format!(
            "  [{}] {current}",
            clock(now.duration_since(self.started))
        )];
        if !self.control().is_empty() {
            parts.push(self.control());
        }
        if self.phase == "running" && !receivers.is_empty() {
            parts.push(receivers);
        }
        parts.join(" | ")
    }
}

impl Receiver {
    fn line(&self, now: Instant) -> String {
        let age = now.duration_since(self.sampled).as_millis();
        let gap = |value: Option<u64>| {
            value.map_or_else(
                || "waiting".into(),
                |value| format!("{}ms", u128::from(value) + age),
            )
        };
        format!(
            "V:{}f A:{} samples, idle {}/{}, retries:{}{}",
            self.frames,
            quantity(self.audio_samples),
            gap(self.video_idle_ms),
            gap(self.audio_idle_ms),
            self.retries,
            if self.ended { ", stopped" } else { "" }
        )
    }
}

fn quantity(value: u64) -> String {
    if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}k", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn count_steps(steps: &[Step]) -> usize {
    steps
        .iter()
        .map(|step| {
            1 + match step {
                Step::Observe { during, .. } => count_steps(during),
                _ => 0,
            }
        })
        .sum()
}

fn step_label(step: &Step) -> String {
    match step {
        Step::Apply {
            file,
            stream,
            expect,
            ..
        } => format!(
            "apply {stream} from {} (HTTP {expect})",
            file.file_name().unwrap_or_default().to_string_lossy()
        ),
        Step::Drive { stream } => format!("attach producer and consumers to {stream}"),
        Step::Undrive { stream } => format!("remove producer and consumers from {stream}"),
        Step::Runtime {
            stream,
            node,
            action,
        } => format!("{action:?} {stream} flows on node {node}").to_lowercase(),
        Step::AwaitControl { status, .. } => format!("await controller HTTP {status}"),
        Step::Flows { stream, nodes, .. } => format!(
            "verify {stream} runtime hops: {}",
            nodes
                .iter()
                .map(|(node, count)| format!("{node}={count}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Step::Producer { stream, running } => format!(
            "{} {stream} producer",
            if *running { "start" } else { "stop" }
        ),
        Step::Consumer { receiver, running } => format!(
            "{} {receiver} consumer",
            if *running { "start" } else { "stop" }
        ),
        Step::Await {
            stream,
            status,
            destination,
            ..
        } => format!(
            "await {stream}{} {:?}",
            destination
                .as_ref()
                .map_or(String::new(), |id| format!("/{id}")),
            status
        )
        .to_lowercase(),
        Step::Observe {
            receivers,
            seconds,
            max_gap_ms,
            ..
        } => format!(
            "observe {} for {seconds}s (gap <= {max_gap_ms}ms)",
            receivers.join(", ")
        ),
        Step::Service { service, action } => format!("{action:?} {service}").to_lowercase(),
        Step::Netem { node, args } => format!("impair {node}: {}", args.join(" ")),
        Step::ClearNetem { node } => format!("clear impairment on {node}"),
        Step::Snapshot { stream, as_name } => format!("snapshot {stream} as {as_name}"),
        Step::Unchanged { snapshot } => format!("verify snapshot {snapshot} unchanged"),
        Step::Delete { stream } => format!("delete {stream} and wait for its hops to disappear"),
        Step::Wait { seconds } => format!("wait {seconds}s"),
    }
}

fn step_clock(active: &ActiveStep, now: Instant) -> String {
    let elapsed = now.duration_since(active.started);
    let limit = match &active.step {
        Step::Await { timeout_secs, .. }
        | Step::Flows { timeout_secs, .. }
        | Step::AwaitControl { timeout_secs, .. } => Some((*timeout_secs, "timeout")),
        Step::Observe { seconds, .. } => Some((*seconds, "minimum")),
        Step::Wait { seconds } => Some((*seconds, "wait")),
        _ => None,
    };
    limit.map_or_else(
        || clock(elapsed),
        |(seconds, kind)| {
            format!(
                "{} / {} {kind}",
                clock(elapsed),
                clock(Duration::from_secs(seconds))
            )
        },
    )
}

fn clock(duration: Duration) -> String {
    format!(
        "{:02}:{:02}",
        duration.as_secs() / 60,
        duration.as_secs() % 60
    )
}

fn clean(text: &str) -> String {
    redact(text)
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect()
}

fn failure_reason(error: &str) -> &str {
    error
        .split("; last:")
        .next()
        .unwrap_or(error)
        .rsplit(": ")
        .next()
        .unwrap_or(error)
}

fn result_line(state: &State, outcome: &Value) -> String {
    let media = outcome["media"].as_array().cloned().unwrap_or_default();
    let frames: u64 = media
        .iter()
        .filter_map(|receiver| receiver["progress"]["decoded_frames"].as_u64())
        .sum();
    let audio: u64 = media
        .iter()
        .filter_map(|receiver| receiver["progress"]["decoded_audio_samples"].as_u64())
        .sum();
    let receivers = media
        .iter()
        .filter_map(|receiver| receiver["receiver"].as_str())
        .collect::<BTreeSet<_>>()
        .len();
    let elapsed = outcome["elapsed_ms"]
        .as_u64()
        .map_or(state.started.elapsed().as_secs_f64(), |ms| {
            ms as f64 / 1000.0
        });
    let detail = if receivers > 0 {
        format!(
            "; {receivers} {}, V:{frames}f A:{} samples",
            if receivers == 1 {
                "receiver"
            } else {
                "receivers"
            },
            quantity(audio)
        )
    } else {
        String::new()
    };
    format!(
        "{} [{}/{}] {} ({elapsed:.1}s{detail})",
        if outcome["passed"] == true {
            "PASS"
        } else {
            "FAIL"
        },
        state.index,
        state.cases,
        state.name
    )
}

struct Region {
    term: Term,
    lines: Vec<String>,
    enabled: bool,
}

impl Region {
    fn erase(&mut self) -> io::Result<()> {
        if self.lines.is_empty() {
            return Ok(());
        }
        let columns = usize::from(self.term.size().1).max(1);
        let rows = self
            .lines
            .iter()
            .map(|line| measure_text_width(line).max(1).div_ceil(columns))
            .sum();
        self.term.clear_last_lines(rows)?;
        self.lines.clear();
        Ok(())
    }

    fn draw(&mut self, lines: Vec<String>) -> io::Result<()> {
        self.erase()?;
        let (rows, columns) = self.term.size();
        let width = usize::from(columns).saturating_sub(1).max(1);
        for line in lines
            .into_iter()
            .take(usize::from(rows).saturating_sub(2).max(1))
        {
            let tail = if width >= 3 { "..." } else { "" };
            let line = truncate_str(&clean(&line), width, tail).into_owned();
            self.term.write_line(&line)?;
            self.lines.push(line);
        }
        self.term.flush()
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        if !self.enabled {
            return;
        }
        let _ = self.erase();
        let _ = self.term.show_cursor();
        let _ = self.term.flush();
    }
}

fn render(
    term: Term,
    state: Arc<Mutex<State>>,
    stopped: mpsc::Receiver<()>,
    mode: Mode,
) -> io::Result<()> {
    let mut region = Region {
        term: term.clone(),
        lines: Vec::new(),
        enabled: mode == Mode::Live,
    };
    if mode == Mode::Live {
        term.hide_cursor()?;
    }
    let mut last_heartbeat = Instant::now();
    let mut tick = 0;
    loop {
        let now = Instant::now();
        let (pending, lines, heartbeat) = {
            let mut state = state.lock().unwrap();
            (
                std::mem::take(&mut state.pending),
                state.lines(now, tick),
                state.heartbeat(now),
            )
        };
        if mode == Mode::Live {
            region.draw(lines)?;
        } else {
            for message in pending {
                term.write_line(&clean(&message))?;
            }
            if now.duration_since(last_heartbeat) >= Duration::from_secs(10) {
                term.write_line(&clean(&heartbeat))?;
                last_heartbeat = now;
            }
            term.flush()?;
        }
        tick += 1;
        match stopped.recv_timeout(Duration::from_millis(200)) {
            Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => (),
        }
    }
    if mode == Mode::Plain {
        for message in std::mem::take(&mut state.lock().unwrap().pending) {
            term.write_line(&clean(&message))?;
        }
        term.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn nested_steps_keep_the_observation_visible_and_count_every_action() {
        let observe = Step::Observe {
            receivers: vec!["main/output".into()],
            seconds: 20,
            max_gap_ms: 3000,
            max_restarts: 0,
            during: vec![Step::Wait { seconds: 2 }],
        };
        let mut state = State::new("test", "e2e-1", 1, 2, std::slice::from_ref(&observe));
        let now = state.started;
        state.event("step_started", &json!({"step": observe}), now);
        state.event(
            "step_started",
            &json!({"step": Step::Wait { seconds: 2 }}),
            now,
        );
        let lines = state.lines(now + Duration::from_secs(1), 0).join("\n");
        assert!(lines.contains("observe main/output"));
        assert!(lines.contains("wait 2s"));
        assert!(lines.contains("0/2 steps complete"));
        state.event("step_finished", &Value::Null, now);
        assert_eq!(state.active.len(), 1);
        state.event("step_finished", &Value::Null, now);
        assert!(state.active.is_empty());
        assert_eq!(state.finished_steps, state.total_steps);
    }

    #[test]
    fn quiet_receivers_show_growing_audio_and_video_ages() {
        let mut state = State::new("test", "e2e-1", 1, 1, &[]);
        let now = state.started;
        state.event(
            "receiver_progress",
            &json!({
                "receiver": "main/output", "frames": 120, "audio_samples": 192000,
                "attempts": 2, "video_idle_ms": 10, "audio_idle_ms": 20,
            }),
            now,
        );
        let line = state.receivers["main/output"].line(now + Duration::from_secs(3));
        assert!(line.contains("idle 3010ms/3020ms"));
        assert!(line.contains("retries:1"));
    }

    #[test]
    fn listed_nodes_are_counted_by_status() {
        let mut state = State::new("test", "e2e-1", 1, 1, &[]);
        let node = |status: &str| json!({ "node": { "id": "n", "status": status } });
        state.event(
            "/nodes",
            &json!({ "status": 200, "body": [node("ready"), node("offline"), node("ready")] }),
            state.started,
        );
        assert_eq!(state.nodes, "1 offline, 2 ready");
        state.event("preflight_nodes", &json!([node("ready")]), state.started);
        assert_eq!(state.nodes, "1 ready");
    }

    #[test]
    fn controls_and_credentials_cannot_escape_into_the_terminal() {
        assert_eq!(
            clean("bad\n\u{1b}[2J srt://a?passphrase=secret&mode=caller"),
            "bad  [2J srt://a?passphrase=[redacted]&mode=caller"
        );
    }
}
