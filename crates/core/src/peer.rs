use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const ROUTE_NODE_SIGNALLING: &str = "/nodes/{node_id}/signalling";
pub const WEBRTC_TRANSPORT: &str = "webrtc";

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(transparent)]
pub struct PeerLinkId(String);

impl PeerLinkId {
    #[must_use]
    pub fn new(downstream_hop: impl Into<String>) -> Self {
        Self(downstream_hop.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PeerLinkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
#[serde(deny_unknown_fields)]
pub struct PeerAttempt {
    pub link: PeerLinkId,
    pub generation: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerSocket {
    pub attempt: PeerAttempt,
    pub stun: Vec<String>,
    pub turn: Vec<TurnServer>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TurnServer {
    pub address: String,
    pub username: String,
    pub credential: TurnCredential,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct TurnCredential(String);

impl TurnCredential {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for TurnCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TurnCredential(<redacted>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PeerReport {
    pub link: PeerLinkId,
    pub generation: u32,
    pub relayed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PeerRole {
    Offer,
    Answer,
}

impl PeerRole {
    #[must_use]
    pub fn description_name(self) -> &'static str {
        match self {
            Self::Offer => "offer",
            Self::Answer => "answer",
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct SessionDescription(String);

impl SessionDescription {
    #[must_use]
    pub fn new(sdp: impl Into<String>) -> Self {
        Self(sdp.into())
    }

    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn has_relayed_candidate(&self) -> bool {
        self.0.lines().any(|line| {
            line.starts_with("a=candidate:")
                && line
                    .split_whitespace()
                    .skip_while(|token| *token != "typ")
                    .nth(1)
                    == Some("relay")
        })
    }
}

impl std::fmt::Debug for SessionDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SessionDescription(<redacted>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LinkDescription {
    pub link: PeerLinkId,
    pub generation: u32,
    pub description: SessionDescription,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SignallingExchange {
    #[serde(default)]
    pub local: Vec<LinkDescription>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SignallingReply {
    #[serde(default)]
    pub remote: Vec<LinkDescription>,
    #[serde(default)]
    pub refused: Vec<PeerAttempt>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WebrtcTransport {
    Webrtc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WebrtcClass {
    pub transport: WebrtcTransport,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_relay_candidate_line_counts_as_relayed() {
        let sdp = |candidates: &str| {
            SessionDescription::new(format!(
                "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n{candidates}a=end-of-candidates\r\n"
            ))
        };
        let host = "a=candidate:1 1 udp 2130706431 192.168.1.20 50000 typ host\r\n";
        let reflexive = "a=candidate:2 1 udp 1694498815 203.0.113.7 61000 typ srflx raddr 192.168.1.20 rport 50000\r\n";
        let relay = "a=candidate:3 1 udp 16777215 91.197.40.98 30025 typ relay raddr 203.0.113.7 rport 61000\r\n";
        assert!(!sdp(host).has_relayed_candidate());
        assert!(!sdp(&format!("{host}{reflexive}")).has_relayed_candidate());
        assert!(sdp(&format!("{host}{relay}")).has_relayed_candidate());
        assert!(!SessionDescription::new("v=0\r\ns=typ relay\r\n").has_relayed_candidate());
    }

    #[test]
    fn descriptions_and_credentials_never_show_in_debug_output() {
        let description = SessionDescription::new("v=0 secret-ice-pwd");
        let credential = TurnCredential::new("hmac-secret");
        let shown = format!("{description:?} {credential:?}");
        assert!(!shown.contains("secret"), "{shown}");
    }
}
