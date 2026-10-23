use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

pub const IPC_VERSION: u32 = 3;
pub const DEFAULT_SOCKET: &str = "/tmp/spaghetti.sock";

const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const CALL_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TapMode {
    Mirror,
    MuteSourceWhenTapped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceSpec {
    Tap { pids: Vec<u32>, mode: TapMode },
    Srt(SrtSocket),
    WebRtc(WebRtcPeer),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Destination {
    Device { uid: String },
    Srt(SrtSocket),
    WebRtc(WebRtcPeer),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebRtcPeer {
    pub link: String,
    pub generation: u32,
    pub role: PeerRole,
    pub stun: Vec<String>,
    pub turn: Vec<TurnServer>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnServer {
    pub address: String,
    pub username: String,
    pub credential: String,
}

impl std::fmt::Debug for TurnServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurnServer")
            .field("address", &self.address)
            .field("username", &self.username)
            .field("credential", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerRole {
    Offer,
    Answer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PeerRef {
    Source(u32),
    Route(u32),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SrtSocket {
    pub mode: SrtMode,
    pub latency_ms: u32,
    pub passphrase: Option<String>,
    pub key_size: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SrtMode {
    Listen { port: u16 },
    Call { host: String, port: u16 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowState {
    Idle,
    Connecting,
    Connected,
    Flowing,
    Stalled,
    Unreachable,
    Lost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowStatus {
    pub state: FlowState,
    pub bytes: u64,
    pub peer: Option<String>,
    pub relayed: bool,
    pub packets_lost: u64,
    pub packets_retransmitted: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DaemonStatus {
    pub running: bool,
    pub ipc_version: u32,
    pub source_count: u32,
    pub route_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AppInfo {
    pub name: String,
    pub bundle_id: Option<String>,
    pub bundle_path: Option<String>,
    pub pids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SourceInfo {
    pub source_id: u32,
    pub spec: SourceSpec,
    pub tag: Option<String>,
    pub label: String,
    pub status: FlowStatus,
    pub local_description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RouteInfo {
    pub route_id: u32,
    pub source_id: u32,
    pub destination: Destination,
    pub volume: f32,
    pub status: FlowStatus,
    pub local_description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SystemDevice {
    pub uid: String,
    pub name: String,
    pub channels: u32,
    pub is_output: bool,
}

#[derive(Debug, Serialize)]
enum Request<'a> {
    Status,
    ListApps,
    CreateSource {
        source: &'a SourceSpec,
        tag: Option<&'a str>,
    },
    DestroySource {
        source_id: u32,
    },
    SetTapProcesses {
        source_id: u32,
        pids: &'a [u32],
    },
    ListSources,
    CreateRoute {
        source_id: u32,
        destination: &'a Destination,
        volume: f32,
    },
    DestroyRoute {
        route_id: u32,
    },
    ListRoutes,
    ListSystemDevices,
    SetRemoteDescription {
        peer: PeerRef,
        generation: u32,
        sdp: &'a str,
    },
}

#[derive(Debug, Deserialize)]
enum Response {
    Ok,
    Error { message: String },
    Status(DaemonStatus),
    Apps(Vec<AppInfo>),
    Sources(Vec<SourceInfo>),
    Routes(Vec<RouteInfo>),
    SourceCreated { source_id: u32 },
    RouteCreated { route_id: u32 },
    SystemDevices(Vec<SystemDevice>),
}

#[derive(Debug, thiserror::Error)]
pub enum SpaghettiError {
    #[error("cannot reach the spaghetti daemon at {path}: {source}")]
    Unreachable {
        path: String,
        source: std::io::Error,
    },
    #[error("spaghetti daemon connection failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("spaghetti daemon sent an unreadable response: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("spaghetti daemon refused the request: {0}")]
    Refused(String),
    #[error("spaghetti daemon answered {request} with something else")]
    Unexpected { request: &'static str },
    #[error("spaghetti daemon did not answer within {CALL_TIMEOUT:?}")]
    Timeout,
    #[error("spaghetti daemon speaks IPC version {found}; this adapter speaks {IPC_VERSION}")]
    Version { found: u32 },
}

pub type Result<T> = std::result::Result<T, SpaghettiError>;

#[derive(Debug, Clone)]
pub struct SpaghettiClient {
    socket: PathBuf,
}

impl SpaghettiClient {
    #[must_use]
    pub fn new(socket: impl AsRef<Path>) -> Self {
        Self {
            socket: socket.as_ref().to_path_buf(),
        }
    }

    pub async fn check_version(&self) -> Result<DaemonStatus> {
        let status = self.status().await?;
        if status.ipc_version != IPC_VERSION {
            return Err(SpaghettiError::Version {
                found: status.ipc_version,
            });
        }
        Ok(status)
    }

    pub async fn status(&self) -> Result<DaemonStatus> {
        match self.call(&Request::Status).await? {
            Response::Status(status) => Ok(status),
            _ => Err(SpaghettiError::Unexpected { request: "Status" }),
        }
    }

    pub async fn apps(&self) -> Result<Vec<AppInfo>> {
        match self.call(&Request::ListApps).await? {
            Response::Apps(apps) => Ok(apps),
            _ => Err(SpaghettiError::Unexpected {
                request: "ListApps",
            }),
        }
    }

    pub async fn system_devices(&self) -> Result<Vec<SystemDevice>> {
        match self.call(&Request::ListSystemDevices).await? {
            Response::SystemDevices(devices) => Ok(devices),
            _ => Err(SpaghettiError::Unexpected {
                request: "ListSystemDevices",
            }),
        }
    }

    pub async fn sources(&self) -> Result<Vec<SourceInfo>> {
        match self.call(&Request::ListSources).await? {
            Response::Sources(sources) => Ok(sources),
            _ => Err(SpaghettiError::Unexpected {
                request: "ListSources",
            }),
        }
    }

    pub async fn routes(&self) -> Result<Vec<RouteInfo>> {
        match self.call(&Request::ListRoutes).await? {
            Response::Routes(routes) => Ok(routes),
            _ => Err(SpaghettiError::Unexpected {
                request: "ListRoutes",
            }),
        }
    }

    pub async fn create_source(&self, source: &SourceSpec, tag: &str) -> Result<u32> {
        match self
            .call(&Request::CreateSource {
                source,
                tag: Some(tag),
            })
            .await?
        {
            Response::SourceCreated { source_id } => Ok(source_id),
            _ => Err(SpaghettiError::Unexpected {
                request: "CreateSource",
            }),
        }
    }

    pub async fn destroy_source(&self, source_id: u32) -> Result<()> {
        match self.call(&Request::DestroySource { source_id }).await? {
            Response::Ok => Ok(()),
            _ => Err(SpaghettiError::Unexpected {
                request: "DestroySource",
            }),
        }
    }

    pub async fn set_tap_processes(&self, source_id: u32, pids: &[u32]) -> Result<()> {
        match self
            .call(&Request::SetTapProcesses { source_id, pids })
            .await?
        {
            Response::Ok => Ok(()),
            _ => Err(SpaghettiError::Unexpected {
                request: "SetTapProcesses",
            }),
        }
    }

    pub async fn destroy_route(&self, route_id: u32) -> Result<()> {
        match self.call(&Request::DestroyRoute { route_id }).await? {
            Response::Ok => Ok(()),
            _ => Err(SpaghettiError::Unexpected {
                request: "DestroyRoute",
            }),
        }
    }

    pub async fn create_route(
        &self,
        source_id: u32,
        destination: &Destination,
        volume: f32,
    ) -> Result<u32> {
        match self
            .call(&Request::CreateRoute {
                source_id,
                destination,
                volume,
            })
            .await?
        {
            Response::RouteCreated { route_id } => Ok(route_id),
            _ => Err(SpaghettiError::Unexpected {
                request: "CreateRoute",
            }),
        }
    }

    pub async fn set_remote_description(
        &self,
        peer: PeerRef,
        generation: u32,
        sdp: &str,
    ) -> Result<()> {
        match self
            .call(&Request::SetRemoteDescription {
                peer,
                generation,
                sdp,
            })
            .await?
        {
            Response::Ok => Ok(()),
            _ => Err(SpaghettiError::Unexpected {
                request: "SetRemoteDescription",
            }),
        }
    }

    async fn call(&self, request: &Request<'_>) -> Result<Response> {
        tokio::time::timeout(CALL_TIMEOUT, self.exchange(request))
            .await
            .map_err(|_| SpaghettiError::Timeout)?
    }

    async fn exchange(&self, request: &Request<'_>) -> Result<Response> {
        let mut stream = UnixStream::connect(&self.socket).await.map_err(|source| {
            SpaghettiError::Unreachable {
                path: self.socket.display().to_string(),
                source,
            }
        })?;
        let body = serde_json::to_vec(request)?;
        let len = u32::try_from(body.len()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "request too large")
        })?;
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(&body).await?;
        stream.flush().await?;

        let mut len = [0; 4];
        stream.read_exact(&mut len).await?;
        let len = u32::from_be_bytes(len) as usize;
        if len > MAX_RESPONSE_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("response of {len} bytes"),
            )
            .into());
        }
        let mut body = vec![0; len];
        stream.read_exact(&mut body).await?;
        match serde_json::from_slice(&body)? {
            Response::Error { message } => Err(SpaghettiError::Refused(message)),
            response => Ok(response),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use tokio::net::UnixListener;

    async fn daemon(answers: Vec<Value>) -> (SpaghettiClient, tokio::task::JoinHandle<Vec<Value>>) {
        let dir = std::env::temp_dir().join(format!(
            "weave-spaghetti-{}-{}",
            std::process::id(),
            answers.len()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{:x}.sock", rand_suffix()));
        let listener = UnixListener::bind(&path).unwrap();
        let client = SpaghettiClient::new(&path);
        let task = tokio::spawn(async move {
            let mut seen = Vec::new();
            for answer in answers {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut len = [0; 4];
                stream.read_exact(&mut len).await.unwrap();
                let mut body = vec![0; u32::from_be_bytes(len) as usize];
                stream.read_exact(&mut body).await.unwrap();
                seen.push(serde_json::from_slice(&body).unwrap());
                let reply = serde_json::to_vec(&answer).unwrap();
                stream
                    .write_all(&u32::try_from(reply.len()).unwrap().to_be_bytes())
                    .await
                    .unwrap();
                stream.write_all(&reply).await.unwrap();
            }
            let _ = std::fs::remove_file(&path);
            seen
        });
        (client, task)
    }

    fn rand_suffix() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    #[tokio::test]
    async fn requests_and_responses_use_the_daemons_wire_shapes() {
        let (client, daemon) = daemon(vec![
            json!({ "SourceCreated": { "source_id": 4 } }),
            json!({ "RouteCreated": { "route_id": 9 } }),
            json!({ "Sources": [{
                "source_id": 4,
                "spec": { "Tap": { "pids": [731], "mode": "Mirror" } },
                "tag": "weave:hop",
                "label": "Music",
                "status": { "state": "Flowing", "bytes": 64, "peer": null, "relayed": false, "packets_lost": 0, "packets_retransmitted": 0 },
                "local_description": null
            }] }),
        ])
        .await;

        let tap = SourceSpec::Tap {
            pids: vec![731],
            mode: TapMode::Mirror,
        };
        assert_eq!(client.create_source(&tap, "weave:hop").await.unwrap(), 4);
        let destination = Destination::Srt(SrtSocket {
            mode: SrtMode::Call {
                host: "10.0.0.2".into(),
                port: 23000,
            },
            latency_ms: 200,
            passphrase: Some("0123456789".into()),
            key_size: Some(32),
        });
        assert_eq!(client.create_route(4, &destination, 1.0).await.unwrap(), 9);
        let sources = client.sources().await.unwrap();
        assert_eq!(sources[0].spec, tap);
        assert_eq!(sources[0].status.state, FlowState::Flowing);

        let seen = daemon.await.unwrap();
        assert_eq!(
            seen[0],
            json!({ "CreateSource": { "source": { "Tap": { "pids": [731], "mode": "Mirror" } }, "tag": "weave:hop" } })
        );
        assert_eq!(
            seen[1],
            json!({ "CreateRoute": { "source_id": 4, "destination": { "Srt": {
                "mode": { "Call": { "host": "10.0.0.2", "port": 23000 } },
                "latency_ms": 200, "passphrase": "0123456789", "key_size": 32
            } }, "volume": 1.0 } })
        );
        assert_eq!(seen[2], json!("ListSources"));
    }

    fn golden<T: Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq>(
        value: &T,
        json: &str,
    ) {
        assert_eq!(serde_json::to_string(value).unwrap(), json);
        assert_eq!(&serde_json::from_str::<T>(json).unwrap(), value);
    }

    fn read<T: serde::de::DeserializeOwned + std::fmt::Debug + PartialEq>(json: &str, value: &T) {
        assert_eq!(&serde_json::from_str::<T>(json).unwrap(), value);
    }

    fn answering() -> WebRtcPeer {
        WebRtcPeer {
            link: "link-7".into(),
            generation: 2,
            role: PeerRole::Answer,
            stun: vec!["stun.l.google.com:19302".into(), "203.0.113.9:3478".into()],
            turn: vec![TurnServer {
                address: "turn.example.net:3478".into(),
                username: "1767225600:link-7".into(),
                credential: "c2VjcmV0".into(),
            }],
        }
    }

    fn offering() -> WebRtcPeer {
        WebRtcPeer {
            link: "link-7".into(),
            generation: 3,
            role: PeerRole::Offer,
            stun: Vec::new(),
            turn: Vec::new(),
        }
    }

    const ANSWERING: &str = r#"{"link":"link-7","generation":2,"role":"Answer","stun":["stun.l.google.com:19302","203.0.113.9:3478"],"turn":[{"address":"turn.example.net:3478","username":"1767225600:link-7","credential":"c2VjcmV0"}]}"#;
    const OFFERING: &str = r#"{"link":"link-7","generation":3,"role":"Offer","stun":[],"turn":[]}"#;

    #[test]
    fn webrtc_specs_match_the_daemons_ipc_3_shapes() {
        golden(&answering(), ANSWERING);
        golden(&offering(), OFFERING);
        golden(
            &SourceSpec::WebRtc(answering()),
            &format!(r#"{{"WebRtc":{ANSWERING}}}"#),
        );
        golden(
            &Destination::WebRtc(offering()),
            &format!(r#"{{"WebRtc":{OFFERING}}}"#),
        );
        golden(
            &answering().turn[0],
            r#"{"address":"turn.example.net:3478","username":"1767225600:link-7","credential":"c2VjcmV0"}"#,
        );
        golden(&PeerRole::Offer, r#""Offer""#);
        golden(&PeerRole::Answer, r#""Answer""#);
        golden(&PeerRef::Source(4), r#"{"Source":4}"#);
        golden(&PeerRef::Route(9), r#"{"Route":9}"#);
        golden(&FlowState::Unreachable, r#""Unreachable""#);
        golden(&FlowState::Lost, r#""Lost""#);
        let debug = format!("{:?}", answering());
        assert!(debug.contains("1767225600:link-7"), "{debug}");
        assert!(!debug.contains("c2VjcmV0"), "{debug}");
    }

    #[test]
    fn webrtc_requests_match_the_daemons_ipc_3_shapes() {
        let source = SourceSpec::WebRtc(answering());
        let destination = Destination::WebRtc(offering());
        for (request, json) in [
            (
                Request::SetRemoteDescription {
                    peer: PeerRef::Route(9),
                    generation: 3,
                    sdp: "v=0\r\no=- 1 2 IN IP4 0.0.0.0\r\n",
                },
                r#"{"SetRemoteDescription":{"peer":{"Route":9},"generation":3,"sdp":"v=0\r\no=- 1 2 IN IP4 0.0.0.0\r\n"}}"#.to_string(),
            ),
            (
                Request::CreateSource {
                    source: &source,
                    tag: Some("mic"),
                },
                format!(r#"{{"CreateSource":{{"source":{{"WebRtc":{ANSWERING}}},"tag":"mic"}}}}"#),
            ),
            (
                Request::CreateRoute {
                    source_id: 4,
                    destination: &destination,
                    volume: 1.0,
                },
                format!(r#"{{"CreateRoute":{{"source_id":4,"destination":{{"WebRtc":{OFFERING}}},"volume":1.0}}}}"#),
            ),
        ] {
            assert_eq!(serde_json::to_string(&request).unwrap(), json);
        }
    }

    #[test]
    fn listings_match_the_daemons_ipc_3_shapes() {
        read(
            &format!(
                r#"{{"source_id":4,"spec":{{"WebRtc":{ANSWERING}}},"tag":null,"label":"link-7","status":{{"state":"Connecting","bytes":0,"peer":null,"relayed":false,"packets_lost":0,"packets_retransmitted":0}},"local_description":"v=0\r\n"}}"#
            ),
            &SourceInfo {
                source_id: 4,
                spec: SourceSpec::WebRtc(answering()),
                tag: None,
                label: "link-7".into(),
                status: FlowStatus {
                    state: FlowState::Connecting,
                    bytes: 0,
                    peer: None,
                    relayed: false,
                    packets_lost: 0,
                    packets_retransmitted: 0,
                },
                local_description: Some("v=0\r\n".into()),
            },
        );
        read(
            &format!(
                r#"{{"route_id":9,"source_id":4,"destination":{{"WebRtc":{OFFERING}}},"volume":0.5,"status":{{"state":"Flowing","bytes":1200,"peer":"203.0.113.1:3478","relayed":true,"packets_lost":3,"packets_retransmitted":0}},"local_description":null}}"#
            ),
            &RouteInfo {
                route_id: 9,
                source_id: 4,
                destination: Destination::WebRtc(offering()),
                volume: 0.5,
                status: FlowStatus {
                    state: FlowState::Flowing,
                    bytes: 1200,
                    peer: Some("203.0.113.1:3478".into()),
                    relayed: true,
                    packets_lost: 3,
                    packets_retransmitted: 0,
                },
                local_description: None,
            },
        );
    }

    #[tokio::test]
    async fn a_refusal_and_a_foreign_version_are_errors() {
        let (client, daemon) = daemon(vec![
            json!({ "Error": { "message": "source 3 not found" } }),
            json!({ "Status": { "running": true, "ipc_version": 1, "source_count": 0, "route_count": 0 } }),
        ])
        .await;
        assert!(matches!(
            client.destroy_source(3).await,
            Err(SpaghettiError::Refused(message)) if message == "source 3 not found"
        ));
        assert!(matches!(
            client.check_version().await,
            Err(SpaghettiError::Version { found: 1 })
        ));
        daemon.await.unwrap();
    }
}
