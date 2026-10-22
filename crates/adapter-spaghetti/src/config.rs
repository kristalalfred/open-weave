use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use weave_core::NodeConfig;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterConfig {
    pub node: NodeConfig,
    #[serde(default)]
    pub spaghetti: SpaghettiSection,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpaghettiSection {
    pub socket: PathBuf,
    pub poll_interval_ms: u64,
}

impl Default for SpaghettiSection {
    fn default() -> Self {
        Self {
            socket: PathBuf::from(weave_spaghetti::DEFAULT_SOCKET),
            poll_interval_ms: 1000,
        }
    }
}

impl SpaghettiSection {
    #[must_use]
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms.max(100))
    }
}

impl AdapterConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let config: Self = serde_norway::from_str(&text)
            .with_context(|| format!("parsing config {}", path.display()))?;
        config
            .node
            .validate()
            .with_context(|| format!("validating node config {}", path.display()))?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r"
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
";

    #[test]
    fn the_spaghetti_section_defaults_to_the_daemons_socket() {
        let config: AdapterConfig = serde_norway::from_str(MINIMAL).unwrap();
        assert_eq!(config.spaghetti.socket, Path::new("/tmp/spaghetti.sock"));
        assert_eq!(config.spaghetti.poll_interval(), Duration::from_secs(1));
    }

    #[test]
    fn unknown_keys_are_refused() {
        let text = format!("{MINIMAL}spaghetti:\n  sockett: /tmp/x\n");
        assert!(serde_norway::from_str::<AdapterConfig>(&text).is_err());
    }
}
