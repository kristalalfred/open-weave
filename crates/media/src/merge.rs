use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

use weave_core::{
    DeliveryPathReport, DeliveryPolicy, DeliveryReason, DeliveryReport, DeliveryState,
};

use crate::record::{HEADER_LENGTH, Record};

const NS_PER_MS: u64 = 1_000_000;
const HEALTH_WINDOW_NS: u64 = 5_000_000_000;
const MAX_CALIBRATION_RECORDS: usize = 65_536;

#[derive(Debug, Clone, Copy)]
pub struct ClockSample {
    pub tai_ns: u64,
    pub monotonic_ns: u64,
    pub uncertainty_ns: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct BufferLimits {
    pub records: usize,
    pub bytes: usize,
    pub scheduling_error_us: u32,
}

impl Default for BufferLimits {
    fn default() -> Self {
        Self {
            records: 32_768,
            bytes: 32 * 1024 * 1024,
            scheduling_error_us: 1_000,
        }
    }
}

struct Pending {
    record: Record,
    target_monotonic_ns: u64,
}

struct Observation {
    origin_ns: u64,
    fingerprint: [u8; 32],
    ages: [Option<u64>; 2],
}

pub struct Merger {
    policy: DeliveryPolicy,
    input_paths: usize,
    requested_paths: usize,
    limits: BufferLimits,
    pending: BTreeMap<u64, Pending>,
    pending_bytes: usize,
    watermark: Option<u64>,
    calibration_started_ns: Option<u64>,
    observations: BTreeMap<u64, Observation>,
    calibration_finished: bool,
    last_timely: [Option<u64>; 2],
    last_release_ns: Option<u64>,
    last_violation_ns: Option<u64>,
    report: DeliveryReport,
}

impl Merger {
    pub fn reconfigure(
        &mut self,
        policy: DeliveryPolicy,
        input_paths: usize,
        requested_paths: usize,
    ) {
        if self.policy != policy {
            let watermark = self.watermark;
            let counters = self.report.counters.clone();
            *self = Self::new(policy, input_paths, requested_paths, self.limits);
            self.watermark = watermark;
            self.report.counters = counters;
        } else {
            self.input_paths = input_paths;
            self.requested_paths = requested_paths;
            self.report
                .paths
                .resize(input_paths, DeliveryPathReport::default());
        }
    }

    pub fn reset_path(&mut self, path: usize) {
        if path < self.last_timely.len() {
            self.last_timely[path] = None;
        }
        if let Some(report) = self.report.paths.get_mut(path) {
            report.qualified = false;
        }
        if !self.calibration_finished && self.report.selected_latency_ms.is_none() {
            self.observations.clear();
            self.calibration_started_ns = None;
        }
    }

    pub fn invalid_record(&mut self) {
        self.report.counters.invalid_records += 1;
    }

    pub fn transport_dropped(&mut self, count: u64) {
        self.report.counters.buffer_drops = self.report.counters.buffer_drops.saturating_add(count);
    }

    #[must_use]
    pub fn new(
        policy: DeliveryPolicy,
        input_paths: usize,
        requested_paths: usize,
        limits: BufferLimits,
    ) -> Self {
        assert!((1..=2).contains(&input_paths));
        let selected_latency_ms = match policy {
            DeliveryPolicy::Fixed { latency_ms, .. } => Some(latency_ms),
            DeliveryPolicy::Auto { .. } => None,
        };
        Self {
            policy,
            input_paths,
            requested_paths,
            limits,
            pending: BTreeMap::new(),
            pending_bytes: 0,
            watermark: None,
            calibration_started_ns: None,
            observations: BTreeMap::new(),
            calibration_finished: false,
            last_timely: [None; 2],
            last_release_ns: None,
            last_violation_ns: None,
            report: DeliveryReport {
                selected_latency_ms,
                paths: vec![DeliveryPathReport::default(); input_paths],
                ..DeliveryReport::default()
            },
        }
    }

    fn uncertainty_ns(&self, record: &Record, clock: ClockSample) -> u64 {
        u64::from(record.origin_uncertainty_ns)
            + u64::from(clock.uncertainty_ns)
            + u64::from(self.limits.scheduling_error_us) * 1_000
    }

    fn violate(&mut self, reason: DeliveryReason, now_ns: u64) {
        self.last_violation_ns = Some(now_ns);
        self.report.reason = reason;
        self.report.state = DeliveryState::Degraded;
        self.report.timing_ready = false;
    }

    pub fn suspend(&mut self, reason: DeliveryReason, now_ns: u64) {
        self.pending.clear();
        self.pending_bytes = 0;
        self.observations.clear();
        self.calibration_started_ns = None;
        self.last_timely = [None; 2];
        if self.report.state == DeliveryState::Failed {
            return;
        }
        self.violate(reason, now_ns);
        self.report.clock_uncertainty_us = None;
    }

    pub fn ingest(&mut self, record: Record, path: usize, clock: ClockSample) {
        if path >= self.input_paths || self.report.state == DeliveryState::Failed {
            self.report.counters.invalid_records += 1;
            return;
        }
        self.report.clock_uncertainty_us = Some(clock.uncertainty_ns.div_ceil(1_000));
        self.report.paths[path].received += 1;
        let uncertainty = self.uncertainty_ns(&record, clock);
        if uncertainty > u64::from(self.policy.tolerance_ms()) * NS_PER_MS {
            self.violate(DeliveryReason::ClockUncertaintyExceeded, clock.monotonic_ns);
            return;
        }
        if record.origin_tai_ns > clock.tai_ns.saturating_add(uncertainty) {
            self.report.counters.invalid_records += 1;
            self.violate(DeliveryReason::ClockDiscontinuity, clock.monotonic_ns);
            return;
        }
        let age_ns = clock.tai_ns.saturating_sub(record.origin_tai_ns);
        self.report.paths[path].latest_age_us = Some(age_ns / 1_000);
        if self.report.selected_latency_ms.is_none() {
            self.observe(&record, path, age_ns.saturating_add(uncertainty), clock);
            return;
        }
        let budget_ns = u64::from(self.report.selected_latency_ms.unwrap()) * NS_PER_MS;
        let tolerance_ns = u64::from(self.policy.tolerance_ms()) * NS_PER_MS;
        if age_ns.saturating_add(uncertainty) > budget_ns.saturating_add(tolerance_ns) {
            self.report.paths[path].late += 1;
            self.report.counters.late_arrivals += 1;
            if !self.pending.contains_key(&record.sequence)
                && self
                    .watermark
                    .is_none_or(|watermark| record.sequence > watermark)
            {
                self.violate(DeliveryReason::DeadlineMissed, clock.monotonic_ns);
            }
            return;
        }
        self.last_timely[path] = Some(clock.monotonic_ns);
        if self
            .watermark
            .is_some_and(|watermark| record.sequence <= watermark)
        {
            self.report.counters.duplicates += 1;
            return;
        }
        if let Some(existing) = self.pending.get(&record.sequence) {
            if existing.record != record {
                self.pending.clear();
                self.pending_bytes = 0;
                self.report.state = DeliveryState::Failed;
                self.report.reason = DeliveryReason::RecordConflict;
                self.report.timing_ready = false;
            } else {
                self.report.counters.duplicates += 1;
            }
            return;
        }
        let maximum_distance = self.limits.records as u64;
        let first = self.pending.first_key_value().map(|(seq, _)| *seq);
        let inconsistent_time = self
            .pending
            .range(..record.sequence)
            .next_back()
            .is_some_and(|(_, previous)| previous.record.origin_tai_ns > record.origin_tai_ns)
            || self
                .pending
                .range(record.sequence..)
                .next()
                .is_some_and(|(_, next)| next.record.origin_tai_ns < record.origin_tai_ns);
        if first.is_some_and(|first| record.sequence.abs_diff(first) > maximum_distance)
            || inconsistent_time
        {
            self.report.counters.invalid_records += 1;
            return;
        }
        let size = HEADER_LENGTH + record.payload.len();
        if self.pending.len() >= self.limits.records
            || self.pending_bytes.saturating_add(size) > self.limits.bytes
        {
            self.report.counters.buffer_drops += 1;
            self.violate(DeliveryReason::BufferExhausted, clock.monotonic_ns);
            return;
        }
        let target = record.origin_tai_ns.saturating_add(budget_ns);
        let target_monotonic_ns = if target >= clock.tai_ns {
            clock.monotonic_ns.saturating_add(target - clock.tai_ns)
        } else {
            clock.monotonic_ns.saturating_sub(clock.tai_ns - target)
        };
        self.pending_bytes += size;
        self.pending.insert(
            record.sequence,
            Pending {
                record,
                target_monotonic_ns,
            },
        );
    }

    fn observe(&mut self, record: &Record, path: usize, age_ns: u64, clock: ClockSample) {
        if self.calibration_finished {
            return;
        }
        self.calibration_started_ns
            .get_or_insert(clock.monotonic_ns);
        self.report.state = DeliveryState::Calibrating;
        self.report.reason = DeliveryReason::Calibrating;
        if self.observations.len() >= MAX_CALIBRATION_RECORDS
            && !self.observations.contains_key(&record.sequence)
        {
            self.report.counters.buffer_drops += 1;
            return;
        }
        let mut hash = Sha256::new();
        hash.update(record.origin_uncertainty_ns.to_be_bytes());
        hash.update(&record.payload);
        let fingerprint: [u8; 32] = hash.finalize().into();
        let observation = self
            .observations
            .entry(record.sequence)
            .or_insert_with(|| Observation {
                origin_ns: record.origin_tai_ns,
                fingerprint,
                ages: [None; 2],
            });
        if observation.origin_ns != record.origin_tai_ns || observation.fingerprint != fingerprint {
            self.report.state = DeliveryState::Failed;
            self.report.reason = DeliveryReason::RecordConflict;
            return;
        }
        observation.ages[path].get_or_insert(age_ns);
    }

    pub fn calibrate(&mut self, clock: ClockSample) {
        if self.report.state == DeliveryState::Failed {
            return;
        }
        let DeliveryPolicy::Auto {
            min_latency_ms,
            max_latency_ms,
            calibration_ms,
            max_late_ppm,
            margin_ms,
            ..
        } = self.policy
        else {
            return;
        };
        let Some(started) = self.calibration_started_ns else {
            return;
        };
        if self.calibration_finished
            || clock.monotonic_ns.saturating_sub(started) < u64::from(calibration_ms) * NS_PER_MS
        {
            return;
        }
        let cutoff = clock
            .tai_ns
            .saturating_sub(u64::from(max_latency_ms) * NS_PER_MS);
        let eligible: Vec<_> = self
            .observations
            .iter()
            .filter(|(_, observation)| observation.origin_ns <= cutoff)
            .collect();
        let required_samples = if max_late_ppm == 0 {
            1_000
        } else {
            1_000_000_u64
                .div_ceil(u64::from(max_late_ppm))
                .clamp(32, MAX_CALIBRATION_RECORDS as u64)
        };
        let count = eligible
            .first()
            .zip(eligible.last())
            .map_or(0, |((first, _), (last, _))| {
                last.saturating_sub(**first).saturating_add(1)
            });
        if count < required_samples {
            return;
        }
        self.calibration_finished = true;
        let allowed = count.saturating_mul(u64::from(max_late_ppm)) / 1_000_000;
        let mut selected_ns = u64::from(min_latency_ms) * NS_PER_MS;
        let mut qualified = true;
        for path in 0..self.input_paths {
            let mut ages: Vec<_> = eligible
                .iter()
                .filter_map(|(_, observation)| observation.ages[path])
                .collect();
            ages.sort_unstable();
            let rank = count.saturating_sub(allowed).saturating_sub(1);
            let Some(age) = usize::try_from(rank).ok().and_then(|rank| ages.get(rank)) else {
                qualified = false;
                break;
            };
            selected_ns = selected_ns.max(age.saturating_add(u64::from(margin_ms) * NS_PER_MS));
        }
        let selected_ms = selected_ns.div_ceil(NS_PER_MS);
        if qualified && selected_ms <= u64::from(max_latency_ms) {
            if let Some((&last, _)) = self.observations.last_key_value() {
                self.watermark = Some(self.watermark.map_or(last, |previous| previous.max(last)));
            }
            self.report.selected_latency_ms = Some(selected_ms as u32);
            self.report.reason = DeliveryReason::AwaitingInput;
            self.report.state = DeliveryState::AwaitingInput;
        } else {
            self.violate(DeliveryReason::LatencyBudgetInfeasible, clock.monotonic_ns);
        }
        self.observations.clear();
    }

    #[must_use]
    pub fn next_deadline_ns(&self) -> Option<u64> {
        self.pending
            .first_key_value()
            .map(|(_, pending)| pending.target_monotonic_ns)
    }

    pub fn take_due(&mut self, clock: ClockSample) -> Option<Record> {
        if self.report.state == DeliveryState::Failed {
            return None;
        }
        loop {
            let (sequence, pending) = self.pending.first_key_value()?;
            if pending.target_monotonic_ns > clock.monotonic_ns {
                return None;
            }
            let sequence = *sequence;
            let pending = self.pending.pop_first().unwrap().1;
            self.pending_bytes -= HEADER_LENGTH + pending.record.payload.len();
            if let Some(previous) = self.watermark {
                let missing = sequence.saturating_sub(previous.saturating_add(1));
                self.report.counters.unrecovered =
                    self.report.counters.unrecovered.saturating_add(missing);
                if missing != 0 {
                    self.violate(DeliveryReason::DeadlineMissed, clock.monotonic_ns);
                }
            }
            self.watermark = Some(sequence);
            let budget_ns = u64::from(self.report.selected_latency_ms.unwrap()) * NS_PER_MS;
            let tolerance_ns = u64::from(self.policy.tolerance_ms()) * NS_PER_MS;
            let age_ns = clock.tai_ns.saturating_sub(pending.record.origin_tai_ns);
            let error_ns = self.uncertainty_ns(&pending.record, clock);
            if age_ns.saturating_sub(error_ns) < budget_ns.saturating_sub(tolerance_ns)
                || age_ns.saturating_add(error_ns) > budget_ns.saturating_add(tolerance_ns)
            {
                self.report.counters.unrecovered += 1;
                self.violate(DeliveryReason::DeadlineMissed, clock.monotonic_ns);
                continue;
            }
            return Some(pending.record);
        }
    }

    pub fn released(&mut self, record: &Record, clock: ClockSample) {
        let age_us = clock.tai_ns.saturating_sub(record.origin_tai_ns) / 1_000;
        self.report.counters.released += 1;
        self.report.minimum_release_age_us = Some(
            self.report
                .minimum_release_age_us
                .map_or(age_us, |previous| previous.min(age_us)),
        );
        self.report.maximum_release_age_us = Some(
            self.report
                .maximum_release_age_us
                .map_or(age_us, |previous| previous.max(age_us)),
        );
        self.last_release_ns = Some(clock.monotonic_ns);
        let budget_ns = u64::from(self.report.selected_latency_ms.unwrap()) * NS_PER_MS;
        let tolerance_ns = u64::from(self.policy.tolerance_ms()) * NS_PER_MS;
        let error_ns = self.uncertainty_ns(record, clock);
        let age_ns = clock.tai_ns.saturating_sub(record.origin_tai_ns);
        if age_ns.saturating_add(error_ns) > budget_ns.saturating_add(tolerance_ns)
            || age_ns.saturating_sub(error_ns) < budget_ns.saturating_sub(tolerance_ns)
        {
            self.report.counters.scheduling_violations += 1;
            self.violate(DeliveryReason::SchedulingViolation, clock.monotonic_ns);
        }
    }

    pub fn output_failed(&mut self, reason: DeliveryReason, now_ns: u64) {
        self.report.counters.unrecovered += 1;
        self.violate(reason, now_ns);
    }

    #[must_use]
    pub fn report(&self, now_ns: u64) -> DeliveryReport {
        let mut report = self.report.clone();
        report.buffer_bytes = self.pending_bytes as u64;
        report.buffer_records = self.pending.len() as u64;
        let recent = |value: Option<u64>| {
            value.is_some_and(|then| now_ns.saturating_sub(then) <= HEALTH_WINDOW_NS)
        };
        for (path, last) in report.paths.iter_mut().zip(self.last_timely) {
            path.qualified = recent(last);
        }
        report.redundancy_ready = self.input_paths == self.requested_paths
            && report.paths.iter().all(|path| path.qualified);
        report.timing_ready = report.selected_latency_ms.is_some()
            && recent(self.last_release_ns)
            && !recent(self.last_violation_ns)
            && report.state != DeliveryState::Failed;
        if report.timing_ready {
            report.state = if report.redundancy_ready {
                DeliveryState::Ready
            } else {
                DeliveryState::Degraded
            };
            report.reason = if report.redundancy_ready {
                DeliveryReason::Timely
            } else {
                DeliveryReason::SinglePath
            };
        } else if report.selected_latency_ms.is_some()
            && !recent(self.last_release_ns)
            && !recent(self.last_violation_ns)
            && report.state != DeliveryState::Failed
        {
            report.state = DeliveryState::AwaitingInput;
            report.reason = DeliveryReason::AwaitingInput;
        }
        report
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn clock(ms: u64) -> ClockSample {
        ClockSample {
            tai_ns: ms * NS_PER_MS,
            monotonic_ns: ms * NS_PER_MS,
            uncertainty_ns: 0,
        }
    }

    fn record(sequence: u64, origin_ms: u64) -> Record {
        Record {
            delivery_id: [1; 16],
            epoch: 1,
            sequence,
            origin_tai_ns: origin_ms * NS_PER_MS,
            origin_uncertainty_ns: 0,
            payload: Bytes::from_static(b"media"),
        }
    }

    fn fixed() -> Merger {
        Merger::new(
            DeliveryPolicy::Fixed {
                latency_ms: 500,
                tolerance_ms: 5,
            },
            2,
            2,
            BufferLimits::default(),
        )
    }

    #[test]
    fn copies_merge_and_fast_path_failure_preserves_the_schedule() {
        let mut merger = fixed();
        merger.ingest(record(0, 0), 0, clock(40));
        merger.ingest(record(0, 0), 1, clock(140));
        assert!(merger.take_due(clock(499)).is_none());
        let first = merger.take_due(clock(500)).unwrap();
        merger.released(&first, clock(500));
        merger.ingest(record(1, 20), 1, clock(170));
        let second = merger.take_due(clock(520)).unwrap();
        assert_eq!(second.sequence, 1);
        merger.released(&second, clock(520));
        let report = merger.report(520 * NS_PER_MS);
        assert_eq!(report.counters.released, 2);
        assert_eq!(report.counters.duplicates, 1);
        assert_eq!(report.minimum_release_age_us, Some(500_000));
        assert_eq!(report.maximum_release_age_us, Some(500_000));
        assert!(report.timing_ready);
    }

    #[test]
    fn late_records_and_missing_sequences_do_not_shift_later_deadlines() {
        let mut merger = fixed();
        merger.ingest(record(0, 0), 0, clock(40));
        let first = merger.take_due(clock(500)).unwrap();
        merger.released(&first, clock(500));
        merger.ingest(record(2, 40), 1, clock(510));
        merger.ingest(record(1, 20), 0, clock(530));
        let next = merger.take_due(clock(540)).unwrap();
        assert_eq!(next.sequence, 2);
        merger.released(&next, clock(540));
        let report = merger.report(540 * NS_PER_MS);
        assert_eq!(report.counters.unrecovered, 1);
        assert_eq!(report.counters.late_arrivals, 1);
        assert!(!report.timing_ready);
    }

    #[test]
    fn scheduling_pauses_discard_stale_media_instead_of_bursting() {
        let mut merger = fixed();
        merger.ingest(record(0, 0), 0, clock(40));
        merger.ingest(record(1, 20), 0, clock(60));
        assert!(merger.take_due(clock(700)).is_none());
        assert_eq!(merger.report(700 * NS_PER_MS).counters.unrecovered, 2);
    }

    #[test]
    fn conflicting_duplicates_stop_output_and_buffers_remain_bounded() {
        let mut merger = fixed();
        merger.ingest(record(0, 0), 0, clock(40));
        let mut conflict = record(0, 0);
        conflict.payload = Bytes::from_static(b"different");
        merger.ingest(conflict, 1, clock(140));
        assert_eq!(merger.report(140 * NS_PER_MS).state, DeliveryState::Failed);
        assert!(merger.take_due(clock(500)).is_none());
        let mut merger = Merger::new(
            DeliveryPolicy::Fixed {
                latency_ms: 500,
                tolerance_ms: 5,
            },
            1,
            1,
            BufferLimits {
                records: 1,
                bytes: 128,
                ..BufferLimits::default()
            },
        );
        merger.ingest(record(0, 0), 0, clock(40));
        merger.ingest(record(1, 20), 0, clock(60));
        let report = merger.report(60 * NS_PER_MS);
        assert_eq!(report.buffer_records, 1);
        assert_eq!(report.counters.buffer_drops, 1);
    }

    #[test]
    fn auto_qualifies_each_path_and_locks_the_slowest_required_tail() {
        let policy = DeliveryPolicy::Auto {
            min_latency_ms: 80,
            max_latency_ms: 500,
            tolerance_ms: 5,
            calibration_ms: 1_000,
            max_late_ppm: 10_000,
            margin_ms: 20,
        };
        let mut merger = Merger::new(policy, 2, 2, BufferLimits::default());
        for sequence in 0..200 {
            merger.ingest(record(sequence, sequence * 2), 0, clock(sequence * 2 + 40));
            merger.ingest(record(sequence, sequence * 2), 1, clock(sequence * 2 + 250));
        }
        merger.calibrate(clock(1_400));
        assert_eq!(
            merger.report(1_400 * NS_PER_MS).selected_latency_ms,
            Some(271)
        );
        merger.ingest(record(200, 1_400), 0, clock(1_440));
        let next = merger.take_due(clock(1_671)).unwrap();
        merger.released(&next, clock(1_671));
        assert_eq!(merger.report(1_671 * NS_PER_MS).counters.unrecovered, 0);
        merger.calibrate(clock(10_000));
        assert_eq!(
            merger.report(10_000 * NS_PER_MS).selected_latency_ms,
            Some(271)
        );
    }

    #[test]
    fn calibration_cannot_hide_missing_backup_samples_or_exceed_the_ceiling() {
        for missing_backup in [true, false] {
            let policy = DeliveryPolicy::Auto {
                min_latency_ms: 80,
                max_latency_ms: 200,
                tolerance_ms: 5,
                calibration_ms: 1_000,
                max_late_ppm: 10_000,
                margin_ms: 20,
            };
            let mut merger = Merger::new(policy, 2, 2, BufferLimits::default());
            for sequence in 0..200 {
                merger.ingest(record(sequence, sequence), 0, clock(sequence + 40));
                if !missing_backup {
                    merger.ingest(record(sequence, sequence), 1, clock(sequence + 250));
                }
            }
            merger.calibrate(clock(1_500));
            let report = merger.report(1_500 * NS_PER_MS);
            assert_eq!(report.selected_latency_ms, None);
            assert_eq!(report.reason, DeliveryReason::LatencyBudgetInfeasible);
        }
    }

    #[test]
    fn clock_uncertainty_and_clock_suspension_prevent_compliant_output() {
        let mut merger = fixed();
        let mut bad = clock(40);
        bad.uncertainty_ns = 5_000_000;
        merger.ingest(record(0, 0), 0, bad);
        assert_eq!(
            merger.report(40 * NS_PER_MS).reason,
            DeliveryReason::ClockUncertaintyExceeded
        );
        merger.ingest(record(1, 20), 0, clock(60));
        merger.suspend(DeliveryReason::ClockDiscontinuity, 100 * NS_PER_MS);
        assert!(merger.take_due(clock(520)).is_none());
    }

    #[test]
    fn a_large_sequence_gap_can_recover_without_unbounded_buffering() {
        let mut merger = fixed();
        merger.ingest(record(0, 0), 0, clock(40));
        let first = merger.take_due(clock(500)).unwrap();
        merger.released(&first, clock(500));
        merger.ingest(record(100_000, 10_000), 1, clock(10_040));
        let next = merger.take_due(clock(10_500)).unwrap();
        assert_eq!(next.sequence, 100_000);
        assert_eq!(
            merger.report(10_500 * NS_PER_MS).counters.unrecovered,
            99_999
        );
    }

    #[test]
    fn calibration_conflicts_remain_failed_after_clock_recovery() {
        let policy = DeliveryPolicy::Auto {
            min_latency_ms: 80,
            max_latency_ms: 500,
            tolerance_ms: 5,
            calibration_ms: 1_000,
            max_late_ppm: 10_000,
            margin_ms: 20,
        };
        let mut merger = Merger::new(policy, 2, 2, BufferLimits::default());
        for sequence in 0..200 {
            merger.ingest(record(sequence, sequence), 0, clock(sequence + 40));
            merger.ingest(record(sequence, sequence), 1, clock(sequence + 100));
        }
        let mut conflict = record(199, 199);
        conflict.payload = Bytes::from_static(b"conflict");
        merger.ingest(conflict, 1, clock(400));
        merger.calibrate(clock(1_500));
        merger.suspend(DeliveryReason::ClockUnavailable, 1_600 * NS_PER_MS);
        assert_eq!(
            merger.report(1_600 * NS_PER_MS).reason,
            DeliveryReason::RecordConflict
        );
        assert_eq!(
            merger.report(1_600 * NS_PER_MS).state,
            DeliveryState::Failed
        );
    }
}
