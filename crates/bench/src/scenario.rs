use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use weave_core::{PathStatus, validate_resource_id};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub name: String,
    pub description: String,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "do", rename_all = "snake_case", deny_unknown_fields)]
pub enum Step {
    Apply {
        file: PathBuf,
        stream: String,
    },
    Drive {
        stream: String,
    },
    Flows {
        stream: String,
        nodes: BTreeMap<String, usize>,
        #[serde(default = "default_timeout")]
        timeout_secs: u64,
    },
    Producer {
        stream: String,
        running: bool,
    },
    Consumer {
        receiver: String,
        running: bool,
    },
    Await {
        stream: String,
        status: PathStatus,
        #[serde(default)]
        destination: Option<String>,
        #[serde(default = "default_timeout")]
        timeout_secs: u64,
        #[serde(default)]
        nodes: Option<Vec<String>>,
        #[serde(default)]
        format_reason: Option<weave_core::StreamConditionReason>,
    },
    Observe {
        receivers: Vec<String>,
        seconds: u64,
        #[serde(default = "default_gap")]
        max_gap_ms: u64,
        #[serde(default)]
        max_restarts: u64,
        #[serde(default)]
        during: Vec<Step>,
    },
    Service {
        service: String,
        action: ServiceAction,
    },
    Netem {
        node: String,
        args: Vec<String>,
    },
    ClearNetem {
        node: String,
    },
    Snapshot {
        stream: String,
        as_name: String,
    },
    Unchanged {
        snapshot: String,
    },
    Delete {
        stream: String,
    },
    Wait {
        seconds: u64,
    },
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ServiceAction {
    Stop,
    Start,
    Restart,
    Kill,
}

pub fn default_timeout() -> u64 {
    90
}

fn default_gap() -> u64 {
    3000
}

impl Scenario {
    pub fn read(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let scenario: Self = serde_norway::from_str(&text)
            .with_context(|| format!("decoding {}", path.display()))?;
        validate_resource_id(&scenario.name)?;
        ensure!(!scenario.steps.is_empty(), "scenario has no steps");
        let mut streams = BTreeSet::new();
        let mut snapshots = BTreeSet::new();
        validate_steps(
            &scenario.steps,
            path.parent().unwrap_or(Path::new(".")),
            &mut streams,
            &mut snapshots,
            false,
        )?;
        Ok(scenario)
    }
}

fn validate_steps(
    steps: &[Step],
    directory: &Path,
    streams: &mut BTreeSet<String>,
    snapshots: &mut BTreeSet<String>,
    observing: bool,
) -> Result<()> {
    for step in steps {
        match step {
            Step::Apply { file, stream } => {
                validate_resource_id(stream)?;
                ensure!(
                    directory.join(file).is_file(),
                    "manifest {} missing",
                    file.display()
                );
                let spec = read_manifest(&directory.join(file))?;
                ensure!(
                    weave_core::validate_stream(&spec).is_empty(),
                    "invalid manifest {}",
                    file.display()
                );
                streams.insert(stream.clone());
            }
            Step::Drive { stream } | Step::Producer { stream, .. } | Step::Delete { stream } => {
                ensure!(streams.contains(stream), "unknown stream {stream}");
            }
            Step::Flows {
                stream,
                nodes,
                timeout_secs,
            } => {
                ensure!(streams.contains(stream), "unknown stream {stream}");
                ensure!(*timeout_secs > 0, "timeout_secs must be positive");
                for node in nodes.keys() {
                    ensure!(
                        node.strip_prefix("strom-node-")
                            .is_some_and(|n| matches!(n, "1" | "2" | "3" | "4")),
                        "unknown media node {node}"
                    );
                }
            }
            Step::Await {
                stream,
                timeout_secs,
                destination,
                ..
            } => {
                ensure!(streams.contains(stream), "unknown stream {stream}");
                ensure!(*timeout_secs > 0, "timeout_secs must be positive");
                if let Some(id) = destination {
                    validate_resource_id(id)?;
                }
            }
            Step::Consumer { receiver, .. } => validate_receiver(receiver, streams)?,
            Step::Observe {
                receivers,
                seconds,
                max_gap_ms,
                during,
                ..
            } => {
                ensure!(!observing, "observe cannot be nested");
                ensure!(!receivers.is_empty(), "observe needs receivers");
                ensure!(
                    *seconds > 0 && *max_gap_ms > 0,
                    "observation limits must be positive"
                );
                let mut unique = BTreeSet::new();
                for receiver in receivers {
                    validate_receiver(receiver, streams)?;
                    ensure!(unique.insert(receiver), "duplicate receiver {receiver}");
                }
                validate_steps(during, directory, streams, snapshots, true)?;
            }
            Step::Service { service, .. } => {
                ensure!(
                    matches!(
                        service.as_str(),
                        "leader" | "controller" | "controller-2" | "northbound" | "southbound"
                    ) || ["adapter-", "strom-"].iter().any(|prefix| {
                        service
                            .strip_prefix(prefix)
                            .is_some_and(|n| matches!(n, "1" | "2" | "3" | "4"))
                    }),
                    "unsupported bench service {service}"
                );
            }
            Step::Netem { node, args } => {
                validate_node(node)?;
                ensure!(!args.is_empty(), "netem needs args");
                ensure!(
                    args.iter().all(|arg| arg
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || ".%:-".contains(c))),
                    "netem args must be individual tc tokens"
                );
            }
            Step::ClearNetem { node } => validate_node(node)?,
            Step::Snapshot { stream, as_name } => {
                ensure!(streams.contains(stream), "unknown stream {stream}");
                ensure!(
                    snapshots.insert(as_name.clone()),
                    "duplicate snapshot {as_name}"
                );
            }
            Step::Unchanged { snapshot } => {
                ensure!(snapshots.contains(snapshot), "unknown snapshot {snapshot}");
            }
            Step::Wait { seconds } => ensure!(*seconds > 0, "wait seconds must be positive"),
        }
    }
    Ok(())
}

fn validate_receiver(receiver: &str, streams: &BTreeSet<String>) -> Result<()> {
    let (stream, destination) = receiver
        .split_once('/')
        .context("receiver must be stream/destination")?;
    ensure!(streams.contains(stream), "unknown stream {stream}");
    validate_resource_id(destination)?;
    Ok(())
}

fn validate_node(node: &str) -> Result<()> {
    ensure!(
        matches!(node, "node1" | "node2" | "node3" | "node4"),
        "unknown bench node {node}"
    );
    Ok(())
}

pub fn read_manifest(path: &Path) -> Result<weave_core::StreamDefinition> {
    let value: serde_json::Value = serde_norway::from_str(&std::fs::read_to_string(path)?)?;
    Ok(serde_json::from_value(value)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_misspelled_actions_and_limits() {
        for yaml in [
            "do: observe\nreceivers: [main/output]\nseconds: 10\nmax_gap: 1000",
            "do: observ\nreceivers: [main/output]\nseconds: 10",
            "do: service\nservice: controller\naction: destroy",
        ] {
            assert!(serde_norway::from_str::<Step>(yaml).is_err());
        }
    }

    #[test]
    fn rejects_faults_outside_the_bench_and_shell_tokens() {
        for step in [
            Step::Service {
                service: "postgres".into(),
                action: ServiceAction::Stop,
            },
            Step::Netem {
                node: "node1".into(),
                args: vec!["loss".into(), "5%;true".into()],
            },
        ] {
            assert!(
                validate_steps(
                    &[step],
                    Path::new("."),
                    &mut BTreeSet::new(),
                    &mut BTreeSet::new(),
                    false
                )
                .is_err()
            );
        }
    }

    #[test]
    fn the_corpus_and_its_manifests_validate() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../bench/cases");
        let mut names = BTreeSet::new();
        for entry in std::fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path
                .extension()
                .is_some_and(|extension| extension == "yaml")
            {
                let scenario = Scenario::read(&path).unwrap();
                assert!(names.insert(scenario.name));
            }
        }
        assert!(!names.is_empty());
    }
}
