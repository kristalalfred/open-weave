use std::fmt::{self, Write};
use std::time::Duration;

use base64::Engine;
use hmac::{Hmac, Mac};
use sha1::Sha1;
use sha2::Sha256;
use weave_core::{Passphrase, PeerAttempt, PeerSocket, TurnCredential, TurnServer};

pub(crate) const SECRET_VAR: &str = "WEAVE_SRT_KEY_SECRET";
pub(crate) const STUN_VAR: &str = "WEAVE_STUN_SERVERS";
pub(crate) const TURN_VAR: &str = "WEAVE_TURN_SERVERS";
pub(crate) const TURN_SECRET_VAR: &str = "WEAVE_TURN_SECRET";
pub(crate) const TURN_USERNAME_VAR: &str = "WEAVE_TURN_USERNAME";
pub(crate) const TURN_CREDENTIAL_VAR: &str = "WEAVE_TURN_CREDENTIAL";
pub(crate) const TURN_TTL_VAR: &str = "WEAVE_TURN_CREDENTIAL_TTL_SECS";
const MIN_SECRET_LEN: usize = 32;
const LINK_LABEL: &[u8] = b"open-weave srt link\0";
const DEFAULT_STUN: [&str; 2] = ["stun.cloudflare.com:3478", "stun.l.google.com:19302"];
const DEFAULT_TURN_TTL: Duration = Duration::from_secs(24 * 60 * 60);
pub(crate) const TURN_RENEW_BEFORE: Duration = Duration::from_secs(10 * 60);

#[derive(Clone)]
pub(crate) struct LinkKeys {
    secret: Vec<u8>,
    ice: Ice,
}

#[derive(Clone, Default)]
pub(crate) struct Ice {
    stun: Vec<String>,
    turn: Option<Turn>,
}

#[derive(Clone)]
struct Turn {
    servers: Vec<String>,
    auth: TurnAuth,
}

#[derive(Clone)]
enum TurnAuth {
    Minted {
        secret: Vec<u8>,
        ttl: Duration,
    },
    Static {
        username: String,
        credential: TurnCredential,
    },
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum IceError {
    #[error(
        "{TURN_VAR} is set without credentials: set {TURN_SECRET_VAR} to the secret the TURN server shares, or {TURN_USERNAME_VAR} and {TURN_CREDENTIAL_VAR} to one static login"
    )]
    NoTurnCredentials,
    #[error(
        "set either {TURN_SECRET_VAR} or {TURN_USERNAME_VAR} and {TURN_CREDENTIAL_VAR}, not both"
    )]
    TwoTurnCredentials,
    #[error("{TURN_USERNAME_VAR} and {TURN_CREDENTIAL_VAR} are set together or not at all")]
    HalfStaticLogin,
    #[error("TURN credentials are set but {TURN_VAR} is not")]
    CredentialsWithoutServers,
    #[error("{TURN_TTL_VAR} applies only to credentials minted from {TURN_SECRET_VAR}")]
    TtlWithoutSecret,
    #[error("{TURN_TTL_VAR} must be a whole number of seconds above 600")]
    InvalidTtl,
    #[error("{server} in {var} is not host:port")]
    InvalidServer { var: &'static str, server: String },
}

impl Ice {
    pub(crate) fn from_env() -> Result<Self, IceError> {
        Self::resolve(|name| std::env::var(name).ok())
    }

    fn resolve(var: impl Fn(&str) -> Option<String>) -> Result<Self, IceError> {
        let set = |name: &str| {
            var(name)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        let stun = match set(STUN_VAR) {
            Some(value) => servers(STUN_VAR, &value)?,
            None => DEFAULT_STUN.map(String::from).to_vec(),
        };
        let servers = match set(TURN_VAR) {
            Some(value) => servers(TURN_VAR, &value)?,
            None => Vec::new(),
        };
        let secret = set(TURN_SECRET_VAR);
        let login = match (set(TURN_USERNAME_VAR), set(TURN_CREDENTIAL_VAR)) {
            (Some(username), Some(credential)) => Some((username, credential)),
            (None, None) => None,
            _ => return Err(IceError::HalfStaticLogin),
        };
        let ttl = set(TURN_TTL_VAR);
        let auth = match (secret, login) {
            (Some(_), Some(_)) => return Err(IceError::TwoTurnCredentials),
            (Some(secret), None) => TurnAuth::Minted {
                secret: secret.into_bytes(),
                ttl: match ttl {
                    None => DEFAULT_TURN_TTL,
                    Some(value) => value
                        .parse::<u64>()
                        .ok()
                        .map(Duration::from_secs)
                        .filter(|ttl| *ttl > TURN_RENEW_BEFORE)
                        .ok_or(IceError::InvalidTtl)?,
                },
            },
            (None, _) if ttl.is_some() => return Err(IceError::TtlWithoutSecret),
            (None, Some((username, credential))) => TurnAuth::Static {
                username,
                credential: TurnCredential::new(credential),
            },
            (None, None) if servers.is_empty() => return Ok(Self { stun, turn: None }),
            (None, None) => return Err(IceError::NoTurnCredentials),
        };
        if servers.is_empty() {
            return Err(IceError::CredentialsWithoutServers);
        }
        Ok(Self {
            stun,
            turn: Some(Turn { servers, auth }),
        })
    }
}

fn servers(var: &'static str, value: &str) -> Result<Vec<String>, IceError> {
    value
        .split(',')
        .map(str::trim)
        .filter(|server| !server.is_empty())
        .map(|server| {
            server
                .rsplit_once(':')
                .filter(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok())
                .map(|_| server.to_string())
                .ok_or_else(|| IceError::InvalidServer {
                    var,
                    server: server.to_string(),
                })
        })
        .collect()
}

fn turn_rest_credential(secret: &[u8], username: &str) -> TurnCredential {
    let mut mac =
        Hmac::<Sha1>::new_from_slice(secret).expect("HMAC-SHA1 accepts a key of any length");
    mac.update(username.as_bytes());
    TurnCredential::new(
        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()),
    )
}

impl fmt::Debug for LinkKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LinkKeys(<redacted>)")
    }
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum SecretError {
    #[error(
        "{SECRET_VAR} is unset or empty: set it to a random value of at least {MIN_SECRET_LEN} characters, or set {}=1 to generate one per run (local development only)",
        weave_core::auth::AUTH_DISABLED_VAR
    )]
    Missing,
    #[error("{SECRET_VAR} must be at least {MIN_SECRET_LEN} characters")]
    TooShort,
    #[error("no random source for a generated {SECRET_VAR}: {0}")]
    Random(String),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SecretSource {
    Configured,
    Generated,
}

impl LinkKeys {
    pub(crate) fn new(secret: impl Into<Vec<u8>>) -> Self {
        Self {
            secret: secret.into(),
            ice: Ice::default(),
        }
    }

    pub(crate) fn with_ice(self, ice: Ice) -> Self {
        Self { ice, ..self }
    }

    pub(crate) fn peer_socket(&self, attempt: PeerAttempt) -> PeerSocket {
        let turn = self.ice.turn.as_ref().map_or_else(Vec::new, |turn| {
            let (username, credential) = match &turn.auth {
                TurnAuth::Minted { secret, ttl } => {
                    let expiry = u64::from(attempt.generation) + ttl.as_secs();
                    let username = format!("{expiry}:{}", attempt.link);
                    let credential = turn_rest_credential(secret, &username);
                    (username, credential)
                }
                TurnAuth::Static {
                    username,
                    credential,
                } => (username.clone(), credential.clone()),
            };
            turn.servers
                .iter()
                .map(|address| TurnServer {
                    address: address.clone(),
                    username: username.clone(),
                    credential: credential.clone(),
                })
                .collect()
        });
        PeerSocket {
            attempt,
            stun: self.ice.stun.clone(),
            turn,
        }
    }

    pub(crate) fn turn_renewal_due(&self, generation: u32, now_unix: u64) -> bool {
        self.ice.turn.as_ref().is_some_and(|turn| match &turn.auth {
            TurnAuth::Minted { ttl, .. } => {
                now_unix + TURN_RENEW_BEFORE.as_secs() >= u64::from(generation) + ttl.as_secs()
            }
            TurnAuth::Static { .. } => false,
        })
    }

    pub(crate) fn from_env() -> Result<(Self, SecretSource), SecretError> {
        Self::resolve(
            std::env::var(SECRET_VAR).ok().as_deref(),
            weave_core::auth::auth_disabled(),
        )
    }

    fn resolve(
        configured: Option<&str>,
        auth_disabled: bool,
    ) -> Result<(Self, SecretSource), SecretError> {
        match configured.map(str::trim).filter(|value| !value.is_empty()) {
            Some(value) if value.len() < MIN_SECRET_LEN => Err(SecretError::TooShort),
            Some(value) => Ok((Self::new(value), SecretSource::Configured)),
            None if auth_disabled => {
                let mut secret = [0u8; 32];
                getrandom::fill(&mut secret)
                    .map_err(|error| SecretError::Random(error.to_string()))?;
                Ok((Self::new(secret), SecretSource::Generated))
            }
            None => Err(SecretError::Missing),
        }
    }

    #[cfg(test)]
    pub(crate) fn for_tests() -> Self {
        Self::with_test_turn(&[(TURN_SECRET_VAR, "turn-test-secret")])
    }

    #[cfg(test)]
    pub(crate) fn with_test_turn(credentials: &[(&str, &str)]) -> Self {
        let vars: std::collections::BTreeMap<&str, &str> =
            [(STUN_VAR, "192.0.2.60:3478"), (TURN_VAR, "192.0.2.50:3478")]
                .into_iter()
                .chain(credentials.iter().copied())
                .collect();
        let ice = Ice::resolve(|name| vars.get(name).map(|value| (*value).to_string()))
            .expect("the test ICE settings are valid");
        Self::new("0123456789abcdef0123456789abcdef").with_ice(ice)
    }

    pub(crate) fn link(&self, hop_id: &str, mut ends: [&str; 2]) -> Passphrase {
        ends.sort_unstable();
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret)
            .expect("HMAC-SHA256 accepts a key of any length");
        mac.update(LINK_LABEL);
        for part in [hop_id, ends[0], ends[1]] {
            mac.update(part.as_bytes());
            mac.update(b"\0");
        }
        let mut key = String::with_capacity(64);
        for byte in mac.finalize().into_bytes() {
            write!(key, "{byte:02x}").expect("writing to a string cannot fail");
        }
        Passphrase::new(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use weave_core::PeerLinkId;

    const SECRET: &str = "0123456789abcdef0123456789abcdef";

    const HOP: &str = "weave-feed-receiver-studio";
    const ENDS: [&str; 2] = ["source", "studio-node"];

    #[test]
    fn a_link_key_is_stable_per_hop_and_distinct_across_hops_and_secrets() {
        let keys = LinkKeys::new(SECRET);
        let key = keys.link(HOP, ENDS);
        assert_eq!(key, keys.link(HOP, ENDS));
        assert_eq!(key.expose().len(), 64);
        assert!(key.expose().bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_ne!(key, keys.link("weave-feed-receiver-preview", ENDS));
        assert_ne!(key, LinkKeys::new(format!("{SECRET}x")).link(HOP, ENDS));
    }

    #[test]
    fn a_link_key_changes_with_either_end_node_but_not_their_order() {
        let keys = LinkKeys::new(SECRET);
        let key = keys.link(HOP, ENDS);
        assert_eq!(key, keys.link(HOP, ["studio-node", "source"]));
        assert_ne!(key, keys.link(HOP, ["relay", "studio-node"]));
        assert_ne!(key, keys.link(HOP, ["source", "relay"]));
        assert_ne!(
            keys.link("hop", ["a", "bc"]),
            keys.link("hop", ["ab", "c"]),
            "the ids are separated"
        );
    }

    #[test]
    fn a_missing_secret_refuses_to_start_unless_auth_is_disabled() {
        assert_eq!(
            LinkKeys::resolve(None, false).unwrap_err(),
            SecretError::Missing
        );
        assert_eq!(
            LinkKeys::resolve(Some("  "), false).unwrap_err(),
            SecretError::Missing
        );
        let (first, source) = LinkKeys::resolve(None, true).unwrap();
        assert_eq!(source, SecretSource::Generated);
        let (second, _) = LinkKeys::resolve(None, true).unwrap();
        assert_ne!(first.link(HOP, ENDS), second.link(HOP, ENDS));
    }

    #[test]
    fn a_short_secret_is_refused_even_with_auth_disabled() {
        assert_eq!(
            LinkKeys::resolve(Some("too-short"), true).unwrap_err(),
            SecretError::TooShort
        );
        let (keys, source) = LinkKeys::resolve(Some(SECRET), false).unwrap();
        assert_eq!(source, SecretSource::Configured);
        assert_eq!(keys.link(HOP, ENDS), LinkKeys::new(SECRET).link(HOP, ENDS));
    }

    #[test]
    fn debug_never_prints_the_secret() {
        assert!(!format!("{:?}", LinkKeys::new(SECRET)).contains(SECRET));
    }

    fn ice(vars: &[(&str, &str)]) -> Result<Ice, IceError> {
        Ice::resolve(|name| {
            vars.iter()
                .find(|(var, _)| *var == name)
                .map(|(_, value)| (*value).to_string())
        })
    }

    fn attempt(generation: u32) -> PeerAttempt {
        PeerAttempt {
            link: PeerLinkId::new("weave-music-receiver-filip"),
            generation,
        }
    }

    #[test]
    fn the_turn_rest_password_is_the_base64_hmac_sha1_of_the_username() {
        assert_eq!(
            turn_rest_credential(b"Jefe", "what do ya want for nothing?").expose(),
            "7/zfauXrL6LSdBbV8YTfnCWafHk="
        );
    }

    #[test]
    fn a_minted_login_expires_one_ttl_after_its_generation_and_names_the_link() {
        let keys = LinkKeys::new(SECRET).with_ice(
            ice(&[
                (TURN_VAR, "turn-a.example:3478, turn-b.example:3478"),
                (TURN_SECRET_VAR, "shared"),
                (TURN_TTL_VAR, "3600"),
            ])
            .unwrap(),
        );
        let socket = keys.peer_socket(attempt(1_761_300_000));
        let username = "1761303600:weave-music-receiver-filip";
        assert_eq!(
            socket
                .turn
                .iter()
                .map(|server| (server.address.as_str(), server.username.as_str()))
                .collect::<Vec<_>>(),
            [
                ("turn-a.example:3478", username),
                ("turn-b.example:3478", username)
            ]
        );
        assert_eq!(
            socket.turn[0].credential,
            turn_rest_credential(b"shared", username)
        );
        assert_eq!(socket.stun, DEFAULT_STUN.map(String::from));
        assert_ne!(
            keys.peer_socket(attempt(1_761_300_001)).turn[0].credential,
            socket.turn[0].credential
        );
    }

    #[test]
    fn only_a_minted_login_is_renewed_and_only_in_its_last_ten_minutes() {
        let generation = 1_761_300_000;
        let expiry = u64::from(generation) + DEFAULT_TURN_TTL.as_secs();
        let minted = LinkKeys::for_tests();
        assert!(!minted.turn_renewal_due(generation, expiry - 601));
        assert!(minted.turn_renewal_due(generation, expiry - 600));
        let fixed = LinkKeys::with_test_turn(&[
            (TURN_USERNAME_VAR, "weave"),
            (TURN_CREDENTIAL_VAR, "static-password"),
        ]);
        assert!(!fixed.turn_renewal_due(generation, expiry + 1));
        assert!(!LinkKeys::new(SECRET).turn_renewal_due(generation, expiry + 1));
        let socket = fixed.peer_socket(attempt(generation));
        assert_eq!(socket.turn[0].username, "weave");
        assert_eq!(socket.turn[0].credential.expose(), "static-password");
    }

    #[test]
    fn turn_settings_take_exactly_one_form_of_credentials() {
        let servers = (TURN_VAR, "turn.example:3478");
        let secret = (TURN_SECRET_VAR, "shared");
        let username = (TURN_USERNAME_VAR, "weave");
        let credential = (TURN_CREDENTIAL_VAR, "password");
        for (vars, error) in [
            (vec![servers], IceError::NoTurnCredentials),
            (
                vec![servers, secret, username, credential],
                IceError::TwoTurnCredentials,
            ),
            (vec![servers, username], IceError::HalfStaticLogin),
            (vec![secret], IceError::CredentialsWithoutServers),
            (
                vec![username, credential],
                IceError::CredentialsWithoutServers,
            ),
            (
                vec![servers, username, credential, (TURN_TTL_VAR, "3600")],
                IceError::TtlWithoutSecret,
            ),
            (
                vec![servers, secret, (TURN_TTL_VAR, "600")],
                IceError::InvalidTtl,
            ),
            (
                vec![(TURN_VAR, "turn.example"), secret],
                IceError::InvalidServer {
                    var: TURN_VAR,
                    server: "turn.example".to_string(),
                },
            ),
        ] {
            assert_eq!(ice(&vars).err(), Some(error), "{vars:?}");
        }
        assert!(ice(&[servers, secret]).is_ok());
        assert!(ice(&[servers, username, credential]).is_ok());
        let none = ice(&[(STUN_VAR, "stun.example:3478")]).unwrap();
        assert!(none.turn.is_none());
        assert_eq!(none.stun, ["stun.example:3478"]);
    }
}
