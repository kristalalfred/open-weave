use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use weave_core::DeliveryReason;

use crate::merge::ClockSample;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "provider", rename_all = "snake_case", deny_unknown_fields)]
pub enum ClockConfig {
    SharedHost {
        domain: String,
        tai_offset_secs: u32,
    },
    External {
        domain: String,
        status_file: PathBuf,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalClock {
    pub domain: String,
    pub tai_offset_secs: u32,
    pub uncertainty_us: u32,
    pub observed_at_unix_ms: u64,
    pub valid_until_unix_ms: u64,
    pub max_drift_ppb: u32,
}

struct State {
    external: Option<ExternalClock>,
    previous: Option<(u64, u64)>,
}

#[derive(Clone)]
pub struct Clock {
    config: ClockConfig,
    started: Instant,
    state: Arc<Mutex<State>>,
}

impl Clock {
    #[must_use]
    pub fn new(config: ClockConfig) -> Self {
        Self {
            config,
            started: Instant::now(),
            state: Arc::new(Mutex::new(State {
                external: None,
                previous: None,
            })),
        }
    }

    #[must_use]
    pub fn domain(&self) -> String {
        match &self.config {
            ClockConfig::SharedHost { domain, .. } => format!("shared-host:{domain}"),
            ClockConfig::External { domain, .. } => domain.clone(),
        }
    }

    #[must_use]
    pub fn monotonic_ns(&self) -> u64 {
        self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
    }

    pub async fn refresh(&self) {
        if let ClockConfig::External {
            status_file,
            domain,
        } = &self.config
        {
            let status = tokio::fs::read(status_file)
                .await
                .ok()
                .and_then(|data| serde_json::from_slice::<ExternalClock>(&data).ok())
                .filter(|status| &status.domain == domain);
            self.state.lock().unwrap().external = status;
        }
    }

    pub fn sample(&self) -> Result<ClockSample, DeliveryReason> {
        let mut state = self.state.lock().unwrap();
        let before_ns = self.monotonic_ns();
        let unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| DeliveryReason::ClockUnavailable)?
            .as_nanos();
        let unix_ns = u64::try_from(unix_ns).map_err(|_| DeliveryReason::ClockUnavailable)?;
        let after_ns = self.monotonic_ns();
        let sampling_ns = after_ns.saturating_sub(before_ns);
        let monotonic_ns = before_ns.saturating_add(sampling_ns / 2);
        let (offset, uncertainty_ns) = match &self.config {
            ClockConfig::SharedHost {
                tai_offset_secs, ..
            } => (*tai_offset_secs, 50_000_u32),
            ClockConfig::External { .. } => {
                let status = state
                    .external
                    .as_ref()
                    .ok_or(DeliveryReason::ClockUnavailable)?;
                let now_ms = unix_ns / 1_000_000;
                if status.observed_at_unix_ms > now_ms || now_ms >= status.valid_until_unix_ms {
                    return Err(DeliveryReason::ClockUnavailable);
                }
                let horizon_ns = (now_ms - status.observed_at_unix_ms)
                    .saturating_add(60_000)
                    .saturating_mul(1_000_000);
                let drift_ns =
                    u128::from(horizon_ns) * u128::from(status.max_drift_ppb) / 1_000_000_000;
                let uncertainty = u128::from(status.uncertainty_us) * 1_000 + drift_ns;
                (
                    status.tai_offset_secs,
                    u32::try_from(uncertainty)
                        .map_err(|_| DeliveryReason::ClockUncertaintyExceeded)?,
                )
            }
        };
        let uncertainty_ns =
            u32::try_from(u64::from(uncertainty_ns).saturating_add(sampling_ns.div_ceil(2)))
                .map_err(|_| DeliveryReason::ClockUncertaintyExceeded)?;
        let tai_ns = unix_ns
            .checked_add(u64::from(offset) * 1_000_000_000)
            .ok_or(DeliveryReason::ClockUnavailable)?;
        if let Some((previous_tai, previous_mono)) = state.previous {
            let expected = previous_tai.saturating_add(monotonic_ns.saturating_sub(previous_mono));
            if tai_ns.abs_diff(expected)
                > u64::from(uncertainty_ns).saturating_mul(2).max(2_000_000)
            {
                state.previous = Some((tai_ns, monotonic_ns));
                return Err(DeliveryReason::ClockDiscontinuity);
            }
        }
        state.previous = Some((tai_ns, monotonic_ns));
        Ok(ClockSample {
            tai_ns,
            monotonic_ns,
            uncertainty_ns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_expired_or_mismatched_external_clock_fails_closed() {
        let clock = Clock::new(ClockConfig::External {
            domain: "tai".into(),
            status_file: PathBuf::from("absent"),
        });
        assert_eq!(
            clock.sample().unwrap_err(),
            DeliveryReason::ClockUnavailable
        );
        clock.state.lock().unwrap().external = Some(ExternalClock {
            domain: "tai".into(),
            tai_offset_secs: 37,
            uncertainty_us: 100,
            observed_at_unix_ms: 0,
            valid_until_unix_ms: 1,
            max_drift_ppb: 1,
        });
        assert_eq!(
            clock.sample().unwrap_err(),
            DeliveryReason::ClockUnavailable
        );
    }

    #[test]
    fn shared_host_clock_has_an_explicit_private_domain_and_detects_steps() {
        let clock = Clock::new(ClockConfig::SharedHost {
            domain: "demo".into(),
            tai_offset_secs: 37,
        });
        assert_eq!(clock.domain(), "shared-host:demo");
        let sample = clock.sample().unwrap();
        assert!(sample.uncertainty_ns >= 50_000);
        clock.state.lock().unwrap().previous =
            Some((sample.tai_ns + 1_000_000_000, sample.monotonic_ns));
        assert_eq!(
            clock.sample().unwrap_err(),
            DeliveryReason::ClockDiscontinuity
        );
    }
}
