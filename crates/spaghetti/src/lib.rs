use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

pub const IPC_VERSION: u32 = 2;
pub const DEFAULT_SOCKET: &str = "/tmp/spaghetti.sock";

const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TapMode {
    Mirror,
    MuteSourceWhenTapped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceSpec {
    Tap { pids: Vec<u32>, mode: TapMode },
    Srt(SrtSocket),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Destination {
    Device { uid: String },
    Srt(SrtSocket),
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowStatus {
    pub state: FlowState,
    pub bytes: u64,
    pub peer: Option<String>,
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
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RouteInfo {
    pub route_id: u32,
    pub source_id: u32,
    pub destination: Destination,
    pub volume: f32,
    pub status: FlowStatus,
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
    ListSources,
    CreateRoute {
        source_id: u32,
        destination: &'a Destination,
        volume: f32,
    },
    ListRoutes,
    ListSystemDevices,
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

    async fn call(&self, request: &Request<'_>) -> Result<Response> {
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
                "status": { "state": "Flowing", "bytes": 64, "peer": null, "packets_lost": 0, "packets_retransmitted": 0 }
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
