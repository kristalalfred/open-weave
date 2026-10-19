use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const TIMED_PROTOCOL: &str = "weave_timed_v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum DeliveryPolicy {
    Fixed {
        #[schemars(range(min = 1, max = 60000))]
        latency_ms: u32,
        #[serde(default = "default_tolerance")]
        #[schemars(range(min = 1, max = 59999))]
        tolerance_ms: u32,
    },
    Auto {
        #[schemars(range(min = 2, max = 60000))]
        min_latency_ms: u32,
        #[schemars(range(min = 1, max = 60000))]
        max_latency_ms: u32,
        #[serde(default = "default_tolerance")]
        #[schemars(range(min = 1, max = 59999))]
        tolerance_ms: u32,
        #[serde(default = "default_calibration")]
        #[schemars(range(min = 1000, max = 60000))]
        calibration_ms: u32,
        #[serde(default = "default_late_ppm")]
        #[schemars(range(max = 100000))]
        max_late_ppm: u32,
        #[serde(default = "default_margin")]
        #[schemars(range(max = 59999))]
        margin_ms: u32,
    },
}

fn default_tolerance() -> u32 {
    5
}

fn default_calibration() -> u32 {
    5_000
}

fn default_late_ppm() -> u32 {
    1_000
}

fn default_margin() -> u32 {
    20
}

impl DeliveryPolicy {
    #[must_use]
    pub fn maximum_ms(&self) -> u32 {
        match self {
            Self::Fixed { latency_ms, .. } => *latency_ms,
            Self::Auto { max_latency_ms, .. } => *max_latency_ms,
        }
    }

    #[must_use]
    pub fn tolerance_ms(&self) -> u32 {
        match self {
            Self::Fixed { tolerance_ms, .. } | Self::Auto { tolerance_ms, .. } => *tolerance_ms,
        }
    }

    #[must_use]
    pub fn validate(&self) -> Vec<(&'static str, &'static str)> {
        let mut issues = Vec::new();
        let maximum = self.maximum_ms();
        if maximum == 0 || maximum > 60_000 {
            issues.push((
                match self {
                    Self::Fixed { .. } => "latency_ms",
                    Self::Auto { .. } => "max_latency_ms",
                },
                "latency must be between 1 and 60000 ms",
            ));
        }
        if self.tolerance_ms() == 0 || self.tolerance_ms() >= maximum {
            issues.push((
                "tolerance_ms",
                "tolerance must be positive and less than the latency",
            ));
        }
        if let Self::Auto {
            min_latency_ms,
            max_latency_ms,
            calibration_ms,
            max_late_ppm,
            margin_ms,
            ..
        } = self
        {
            if *min_latency_ms <= self.tolerance_ms() || min_latency_ms > max_latency_ms {
                issues.push((
                    "min_latency_ms",
                    "minimum must exceed the tolerance and not exceed the maximum",
                ));
            }
            if !(1_000..=60_000).contains(calibration_ms) {
                issues.push((
                    "calibration_ms",
                    "calibration must be between 1000 and 60000 ms",
                ));
            }
            if *max_late_ppm > 100_000 {
                issues.push((
                    "max_late_ppm",
                    "late-record allowance must not exceed 100000 parts per million",
                ));
            }
            if margin_ms >= max_latency_ms {
                issues.push(("margin_ms", "margin must be less than the maximum latency"));
            }
        }
        issues
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryRole {
    Origin,
    Forward,
    Receiver,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryCapability {
    pub protocol: String,
    pub roles: Vec<DeliveryRole>,
    pub max_latency_ms: u32,
    pub scheduling_error_bound_us: u32,
    pub clock_domain: String,
    pub origin_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TimedHop {
    pub delivery_id: [u8; 16],
    pub epoch: u64,
    pub clock_domain: String,
    pub role: DeliveryRole,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<DeliveryPolicy>,
    pub paths: u8,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PayloadKind {
    #[default]
    Raw,
    WeaveTimedV1,
}

impl PayloadKind {
    #[must_use]
    pub fn is_raw(&self) -> bool {
        *self == Self::Raw
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    #[default]
    AwaitingInput,
    Calibrating,
    Ready,
    Degraded,
    Failed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryReason {
    #[default]
    AwaitingInput,
    Calibrating,
    Timely,
    SinglePath,
    ClockUnavailable,
    ClockUncertaintyExceeded,
    ClockDiscontinuity,
    DeadlineMissed,
    SchedulingViolation,
    RecordConflict,
    OutputUnavailable,
    OutputBackpressure,
    BufferExhausted,
    LatencyBudgetInfeasible,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryCounters {
    pub released: u64,
    pub duplicates: u64,
    pub late_arrivals: u64,
    pub unrecovered: u64,
    pub scheduling_violations: u64,
    pub buffer_drops: u64,
    pub invalid_records: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryPathReport {
    pub received: u64,
    pub late: u64,
    pub latest_age_us: Option<u64>,
    pub qualified: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryReport {
    pub state: DeliveryState,
    pub reason: DeliveryReason,
    pub selected_latency_ms: Option<u32>,
    pub timing_ready: bool,
    pub redundancy_ready: bool,
    pub clock_uncertainty_us: Option<u32>,
    pub buffer_records: u64,
    pub buffer_bytes: u64,
    pub minimum_release_age_us: Option<u64>,
    pub maximum_release_age_us: Option<u64>,
    pub counters: DeliveryCounters,
    pub paths: Vec<DeliveryPathReport>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fixed_defaults_compare_equal_and_modes_reject_mixed_fields() {
        let implicit: DeliveryPolicy =
            serde_json::from_value(json!({"mode":"fixed", "latency_ms":500})).unwrap();
        let explicit: DeliveryPolicy =
            serde_json::from_value(json!({"mode":"fixed", "latency_ms":500, "tolerance_ms":5}))
                .unwrap();
        assert_eq!(implicit, explicit);
        assert!(
            serde_json::from_value::<DeliveryPolicy>(
                json!({"mode":"auto", "latency_ms":500, "min_latency_ms":80, "max_latency_ms":500})
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<DeliveryPolicy>(json!({"mode":"adaptive", "latency_ms":500}))
                .is_err()
        );
    }

    #[test]
    fn automatic_limits_bound_the_target_and_observation_window() {
        let valid: DeliveryPolicy = serde_json::from_value(
            json!({"mode":"auto", "min_latency_ms":80, "max_latency_ms":500}),
        )
        .unwrap();
        assert!(valid.validate().is_empty());
        let invalid: DeliveryPolicy = serde_json::from_value(json!({"mode":"auto", "min_latency_ms":500, "max_latency_ms":80, "calibration_ms":0, "max_late_ppm":100001})).unwrap();
        assert_eq!(invalid.validate().len(), 3);
    }
}
