use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;
use weave_core::NodeConfig;
use weave_core::auth::Token;

pub const STROM_TOKEN_VAR: &str = "WEAVE_STROM_TOKEN";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterConfig {
    pub node: NodeConfig,
    pub strom: StromSection,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StromSection {
    pub url: String,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
}

fn default_poll_interval_secs() -> u64 {
    5
}

fn default_request_timeout_secs() -> u64 {
    weave_strom::REQUEST_TIMEOUT.as_secs()
}

impl StromSection {
    #[must_use]
    pub fn resolve_token(&self) -> Option<Token> {
        pick_token(
            self.token.as_deref(),
            std::env::var(STROM_TOKEN_VAR).ok().as_deref(),
        )
    }
}

fn pick_token(config: Option<&str>, env: Option<&str>) -> Option<Token> {
    config
        .and_then(Token::new)
        .or_else(|| env.and_then(Token::new))
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
        anyhow::ensure!(
            config.node.topology.site.is_none(),
            "{}: topology.site needs an adapter that runs beside its media, and the Strom adapter is not co-located with Strom; declare an attachment with a network and listener host instead",
            path.display()
        );
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r"
node:
  id: strom-node-1
  southbound_url: http://127.0.0.1:8081
  listen: 0.0.0.0:8091
  topology:
    attachments:
      - id: lan
        network: studio-lan
        dial: true
        listeners:
          srt:
            host: 172.26.0.10
            port_range: { start: 20000, end: 20999 }
strom:
  url: http://172.26.0.10:8080
";

    #[test]
    fn parses_valid_config_and_defaults_poll_interval() {
        let config: AdapterConfig = serde_norway::from_str(VALID).expect("parse");
        assert_eq!(config.node.id, "strom-node-1");
        let attachment = &config.node.topology.attachments[0];
        assert_eq!(attachment.network, "studio-lan");
        assert!(attachment.dial);
        assert_eq!(
            attachment
                .listeners
                .srt
                .as_ref()
                .map(|srt| srt.host.as_str()),
            Some("172.26.0.10")
        );
        assert_eq!(config.strom.url, "http://172.26.0.10:8080");
        assert_eq!(config.strom.poll_interval_secs, 5);
        assert_eq!(config.strom.request_timeout_secs, 10);
        assert_eq!(config.node.validate(), Ok(()));
    }

    #[test]
    fn accepts_an_inline_southbound_token() {
        let config: AdapterConfig = serde_norway::from_str(VALID).expect("parse");
        assert_eq!(
            config.node.southbound_token, None,
            "the field is optional; deployments may use the env var instead"
        );

        let yaml = VALID.replace(
            "southbound_url: http://127.0.0.1:8081",
            "southbound_url: http://127.0.0.1:8081\n  southbound_token: from-yaml",
        );
        let config: AdapterConfig = serde_norway::from_str(&yaml).expect("parse");
        assert_eq!(config.node.southbound_token.as_deref(), Some("from-yaml"));
    }

    #[test]
    fn strom_token_is_optional_and_parses_when_present() {
        let config: AdapterConfig = serde_norway::from_str(VALID).expect("parse");
        assert_eq!(
            config.strom.token, None,
            "an unauthenticated Strom needs no token"
        );

        let yaml = VALID.replace(
            "url: http://172.26.0.10:8080",
            "url: http://172.26.0.10:8080\n  token: from-yaml",
        );
        let config: AdapterConfig = serde_norway::from_str(&yaml).expect("parse");
        assert_eq!(config.strom.token.as_deref(), Some("from-yaml"));
    }

    #[test]
    fn config_token_wins_over_the_environment() {
        assert_eq!(
            pick_token(Some("from-yaml"), Some("from-env")),
            Token::new("from-yaml")
        );
        assert_eq!(pick_token(None, Some("from-env")), Token::new("from-env"));
        assert_eq!(pick_token(None, None), None);
        assert_eq!(
            pick_token(Some("  "), Some("from-env")),
            Token::new("from-env"),
            "a blank config value counts as absent"
        );
        assert_eq!(pick_token(Some("  "), Some("")), None);
    }

    fn repository_file(path: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join(path)
    }

    #[test]
    fn example_node_config_loads() {
        let path = repository_file("examples/node.yaml");
        AdapterConfig::load(&path).unwrap_or_else(|error| panic!("{error:#}"));
    }

    #[test]
    fn deploy_node_template_loads_once_filled_in() {
        let template = std::fs::read_to_string(repository_file("deploy/node/node.yaml"))
            .expect("reading deploy/node/node.yaml");
        let filled = template
            .replace("<node-id>", "strom-node-1")
            .replace("<control-plane-host>", "control.example")
            .replace("<node-address>", "203.0.113.10")
            .replace("<node-public-address>", "203.0.113.10");
        let unfilled: Vec<_> = filled
            .lines()
            .filter(|line| !line.trim_start().starts_with('#') && line.contains('<'))
            .collect();
        assert!(unfilled.is_empty(), "placeholders left: {unfilled:?}");

        let path =
            std::env::temp_dir().join(format!("weave-deploy-node-{}.yaml", std::process::id()));
        std::fs::write(&path, filled).expect("writing the filled-in template");
        let loaded = AdapterConfig::load(&path);
        std::fs::remove_file(&path).expect("removing the filled-in template");
        loaded.unwrap_or_else(|error| panic!("{error:#}"));
    }

    #[test]
    fn refuses_an_unknown_field_inside_a_port_range() {
        let path =
            std::env::temp_dir().join(format!("weave-port-step-{}.yaml", std::process::id()));
        std::fs::write(
            &path,
            VALID.replace(
                "port_range: { start: 20000, end: 20999 }",
                "port_range: { start: 20000, end: 20999, step: 2 }",
            ),
        )
        .expect("writing the config");
        let loaded = AdapterConfig::load(&path);
        std::fs::remove_file(&path).expect("removing the config");
        let error = format!("{:#}", loaded.expect_err("step is not a port range field"));
        assert!(error.contains("unknown field `step`"), "{error}");
        assert!(error.contains("port_range"), "{error}");
    }

    #[test]
    fn refuses_a_site_attachment() {
        let path =
            std::env::temp_dir().join(format!("weave-strom-site-{}.yaml", std::process::id()));
        std::fs::write(
            &path,
            VALID.replace(
                "  topology:\n",
                "  topology:\n    site:\n      id: office\n",
            ),
        )
        .expect("writing the config");
        let loaded = AdapterConfig::load(&path);
        std::fs::remove_file(&path).expect("removing the config");
        let error = format!(
            "{:#}",
            loaded.expect_err("a Strom node cannot observe a site")
        );
        assert!(error.contains("not co-located"), "{error}");
    }

    #[test]
    fn rejects_unknown_fields() {
        let yaml = format!("{VALID}  bogus: true\n");
        let result: Result<AdapterConfig, _> = serde_norway::from_str(&yaml);
        assert!(result.is_err(), "deny_unknown_fields rejects typos");
    }
}
