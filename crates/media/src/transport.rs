use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use srt_tokio::SrtSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use weave_core::{
    DeliveryReason, HopState, LinkCondition, LinkStats, ResolvedAddr, SocketStatus,
    SrtSocket as SocketSpec,
};

use crate::clock::Clock;
use crate::merge::ClockSample;

pub struct Input {
    pub path: usize,
    pub bytes: Bytes,
    pub stamp: Result<ClockSample, DeliveryReason>,
}

struct Statistics {
    bound: bool,
    invalid: bool,
    connected: bool,
    last_progress_ns: Option<u64>,
    bytes: u64,
    drops: u64,
    sample_ns: u64,
    sample_bytes: u64,
    rate_mbps: f64,
    negotiated_latency_ms: Option<u64>,
    packets_sent_lost: u64,
    packets_retransmitted: u64,
}

impl Statistics {
    fn new(spec: &SocketSpec) -> Self {
        Self {
            bound: false,
            invalid: spec
                .params()
                .latency
                .is_some_and(|latency| latency > 60_000)
                || spec
                    .params()
                    .passphrase
                    .as_ref()
                    .is_some_and(|phrase| !(10..=79).contains(&phrase.expose().len()))
                || spec
                    .params()
                    .pbkeylen
                    .is_some_and(|length| ![16, 24, 32].contains(&length)),
            connected: false,
            last_progress_ns: None,
            bytes: 0,
            drops: 0,
            sample_ns: 0,
            sample_bytes: 0,
            rate_mbps: 0.0,
            negotiated_latency_ms: None,
            packets_sent_lost: 0,
            packets_retransmitted: 0,
        }
    }
}

pub struct SocketTask {
    pub spec: SocketSpec,
    statistics: Arc<Mutex<Statistics>>,
    socket: Arc<Mutex<Option<SrtSocket>>>,
    task: JoinHandle<()>,
}

fn configured_latency(spec: &SocketSpec) -> Duration {
    Duration::from_millis(u64::from(spec.params().latency.unwrap_or(100)))
}

async fn connect(
    spec: &SocketSpec,
    bind_ip: IpAddr,
    statistics: &Mutex<Statistics>,
) -> anyhow::Result<SrtSocket> {
    anyhow::ensure!(
        !statistics.lock().unwrap().invalid,
        "unsupported native SRT parameters"
    );
    let port = match spec {
        SocketSpec::Listen { port, .. } => *port,
        SocketSpec::Connect { .. } => 0,
    };
    let udp = tokio::net::UdpSocket::bind(SocketAddr::new(bind_ip, port)).await?;
    statistics.lock().unwrap().bound = true;
    let params = spec.params();
    let mut builder = SrtSocket::builder()
        .latency(configured_latency(spec))
        .socket(udp)
        .set(|options| options.sender.buffer_size = srt_tokio::options::ByteCount(8 * 1024 * 1024));
    if let Some(passphrase) = &params.passphrase {
        let phrase: srt_tokio::options::Passphrase =
            passphrase.expose().to_owned().try_into().map_err(|_| {
                anyhow::anyhow!("native SRT passphrase must contain 10 to 79 characters")
            })?;
        builder = builder.set(|options| {
            options.encryption.passphrase = Some(phrase);
            options.encryption.key_size =
                u16::from(params.pbkeylen.unwrap_or(32)).try_into().unwrap();
        });
    }
    Ok(match spec {
        SocketSpec::Listen { port, .. } => {
            builder.listen_on(SocketAddr::new(bind_ip, *port)).await?
        }
        SocketSpec::Connect { host, port, .. } => {
            let address = tokio::net::lookup_host((host.as_str(), *port))
                .await?
                .next()
                .ok_or_else(|| anyhow::anyhow!("SRT host resolved to no addresses"))?;
            builder.call(address, None).await?
        }
    })
}

impl SocketTask {
    #[must_use]
    pub fn input(
        spec: SocketSpec,
        path: usize,
        sender: mpsc::Sender<Input>,
        clock: Clock,
        bind_ip: IpAddr,
    ) -> Self {
        let statistics = Arc::new(Mutex::new(Statistics::new(&spec)));
        let shared = statistics.clone();
        let configuration = spec.clone();
        let task = tokio::spawn(async move {
            loop {
                shared.lock().unwrap().bound = false;
                match connect(&configuration, bind_ip, &shared).await {
                    Ok(mut socket) => {
                        {
                            let mut stats = shared.lock().unwrap();
                            stats.connected = true;
                            stats.negotiated_latency_ms =
                                Some(socket.settings().recv_tsbpd_latency.as_millis() as u64);
                        }
                        while let Some(Ok((_, bytes))) = socket.next().await {
                            let stamp = clock.sample();
                            let now = clock.monotonic_ns();
                            {
                                let mut stats = shared.lock().unwrap();
                                stats.bytes = stats.bytes.saturating_add(bytes.len() as u64);
                                stats.last_progress_ns = Some(now);
                            }
                            let maximum = if configuration.params().payload
                                == weave_core::PayloadKind::WeaveTimedV1
                            {
                                crate::record::MAX_MESSAGE
                            } else {
                                65_536
                            };
                            if bytes.len() > maximum
                                || sender.try_send(Input { path, bytes, stamp }).is_err()
                            {
                                shared.lock().unwrap().drops += 1;
                            }
                        }
                        shared.lock().unwrap().connected = false;
                    }
                    Err(error) => {
                        tracing::debug!(port = configuration.port(), error = %error, "SRT input connection failed")
                    }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
        Self {
            spec,
            statistics,
            socket: Arc::new(Mutex::new(None)),
            task,
        }
    }

    #[must_use]
    pub fn output(spec: SocketSpec, bind_ip: IpAddr) -> Self {
        let statistics = Arc::new(Mutex::new(Statistics::new(&spec)));
        let socket = Arc::new(Mutex::new(None));
        let shared_socket: Arc<Mutex<Option<SrtSocket>>> = socket.clone();
        let shared = statistics.clone();
        let configuration = spec.clone();
        let task = tokio::spawn(async move {
            loop {
                shared.lock().unwrap().bound = false;
                match connect(&configuration, bind_ip, &shared).await {
                    Ok(connected) => {
                        shared.lock().unwrap().negotiated_latency_ms =
                            Some(connected.settings().send_tsbpd_latency.as_millis() as u64);
                        *shared_socket.lock().unwrap() = Some(connected);
                        shared.lock().unwrap().connected = true;
                        let mut last_sent = 0;
                        let mut last_wire_progress = Instant::now();
                        let mut last_library_drops = 0;
                        loop {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            let closed = {
                                let mut guard = shared_socket.lock().unwrap();
                                match guard.as_mut() {
                                    Some(socket) => {
                                        if let Some(Some(sample)) =
                                            socket.statistics().next().now_or_never()
                                        {
                                            if sample.tx_unique_data != last_sent {
                                                last_sent = sample.tx_unique_data;
                                                last_wire_progress = Instant::now();
                                            }
                                            if sample.tx_buffered_data == 0 {
                                                last_wire_progress = Instant::now();
                                            }
                                            let mut stats = shared.lock().unwrap();
                                            stats.drops = stats.drops.saturating_add(
                                                sample
                                                    .tx_dropped_data
                                                    .saturating_sub(last_library_drops),
                                            );
                                            stats.packets_sent_lost = sample.tx_dropped_data;
                                            stats.packets_retransmitted = sample.tx_retransmit_data;
                                            last_library_drops = sample.tx_dropped_data;
                                            if sample.tx_buffered_data > 0
                                                && last_wire_progress.elapsed()
                                                    > Duration::from_secs(3)
                                            {
                                                break;
                                            }
                                        }
                                        matches!(
                                            socket.next().now_or_never(),
                                            Some(None) | Some(Some(Err(_)))
                                        )
                                    }
                                    None => true,
                                }
                            };
                            if closed {
                                break;
                            }
                        }
                        shared_socket.lock().unwrap().take();
                        shared.lock().unwrap().connected = false;
                    }
                    Err(error) => {
                        tracing::debug!(port = configuration.port(), error = %error, "SRT output connection failed")
                    }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
        Self {
            spec,
            statistics,
            socket,
            task,
        }
    }

    pub fn try_send(&self, bytes: Bytes, now_ns: u64) -> Result<(), DeliveryReason> {
        self.try_send_at(bytes, Instant::now(), now_ns)
    }

    pub fn try_send_at(
        &self,
        bytes: Bytes,
        source_time: Instant,
        now_ns: u64,
    ) -> Result<(), DeliveryReason> {
        let mut socket = self.socket.lock().unwrap();
        let socket = socket.as_mut().ok_or(DeliveryReason::OutputUnavailable)?;
        let source_time = source_time.max(socket.settings().socket_start_time);
        if socket.try_send(source_time, bytes.clone()).is_err() {
            self.statistics.lock().unwrap().drops += 1;
            return Err(DeliveryReason::OutputBackpressure);
        }
        let mut stats = self.statistics.lock().unwrap();
        stats.bytes = stats.bytes.saturating_add(bytes.len() as u64);
        stats.last_progress_ns = Some(now_ns);
        Ok(())
    }

    #[must_use]
    pub fn status(&self, now_ns: u64) -> SocketStatus {
        let mut stats = self.statistics.lock().unwrap();
        let elapsed = now_ns.saturating_sub(stats.sample_ns);
        if elapsed >= 200_000_000 {
            stats.rate_mbps =
                stats.bytes.saturating_sub(stats.sample_bytes) as f64 * 8_000.0 / elapsed as f64;
            stats.sample_ns = now_ns;
            stats.sample_bytes = stats.bytes;
        }
        let condition = match (stats.connected, stats.last_progress_ns) {
            (_, Some(last)) if now_ns.saturating_sub(last) > 3_000_000_000 => {
                LinkCondition::Stalled
            }
            (true, Some(_)) => LinkCondition::Flowing,
            (true, None) => LinkCondition::Connected,
            (false, _) => LinkCondition::Connecting,
        };
        SocketStatus {
            condition,
            resolved: Some(ResolvedAddr {
                host: match &self.spec {
                    SocketSpec::Listen { .. } => "0.0.0.0".into(),
                    SocketSpec::Connect { host, .. } => host.clone(),
                },
                port: self.spec.port(),
            }),
            stats: Some(LinkStats {
                connections: usize::from(stats.connected),
                rate_mbps: stats.rate_mbps,
                packets_sent_lost: i64::try_from(stats.packets_sent_lost).unwrap_or(i64::MAX),
                packets_retransmitted: i64::try_from(stats.packets_retransmitted)
                    .unwrap_or(i64::MAX),
                ..LinkStats::default()
            }),
        }
    }

    #[must_use]
    pub fn take_drops(&self) -> u64 {
        std::mem::take(&mut self.statistics.lock().unwrap().drops)
    }

    #[must_use]
    pub fn state(&self) -> HopState {
        let stats = self.statistics.lock().unwrap();
        if stats.invalid {
            HopState::Failed
        } else if stats.bound {
            HopState::Provisioned
        } else {
            HopState::Pending
        }
    }

    #[must_use]
    pub fn latency(&self) -> Duration {
        self.statistics
            .lock()
            .unwrap()
            .negotiated_latency_ms
            .map_or_else(|| configured_latency(&self.spec), Duration::from_millis)
    }
}

impl Drop for SocketTask {
    fn drop(&mut self) {
        self.task.abort();
        self.socket.lock().unwrap().take();
    }
}
