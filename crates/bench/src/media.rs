use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Result, ensure};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::task::JoinHandle;

use crate::report::Report;

pub const IMAGE: &str =
    "linuxserver/ffmpeg@sha256:abb675ffda7047f42dfba02adcd0886b3abfca6dec332adc4da831de943989ed";

pub const PRODUCER: &str = r#"
trap 'kill -TERM "$child" 2>/dev/null; exit 0' TERM INT
while :; do
  echo BENCH_ATTEMPT
  ffmpeg -hide_banner -nostdin -loglevel warning -re \
    -f lavfi -i testsrc2=size=640x360:rate=30 \
    -f lavfi -i sine=frequency=1000:sample_rate=48000 \
    -c:v libx264 -preset ultrafast -tune zerolatency -b:v 1500k -pix_fmt yuv420p -g 30 \
    -c:a aac -b:a 128k -f mpegts "$BENCH_URL" 2>&1 &
  child=$!
  wait "$child"
  echo "BENCH_EXIT $?"
  sleep 1
done
"#;

pub const CONSUMER: &str = r#"
trap 'kill -TERM "$child" 2>/dev/null; exit 0' TERM INT
while :; do
  echo BENCH_ATTEMPT
  ffmpeg -hide_banner -nostdin -nostats -loglevel info -stats_period 0.25 -progress pipe:1 \
    -i "$BENCH_URL" -map 0:v:0 -map 0:a:0 -af ashowinfo -f null - 2>&1 &
  child=$!
  wait "$child"
  echo "BENCH_EXIT $?"
  sleep 1
done
"#;

#[derive(Default)]
pub struct Progress {
    pub attempts: u64,
    pub frames: u64,
    pub media_us: u64,
    pub total_frames: u64,
    pub last_advance: Option<Instant>,
    pub last_audio_advance: Option<Instant>,
    pub audio_samples: u64,
    audio_us: f64,
    pub max_gap_ms: u128,
    pub ended: bool,
    pub error: Option<String>,
    pending_frames: u64,
    pending_media_us: u64,
}

impl Progress {
    fn diagnostics(&self, receiver: &str, now: Instant) -> serde_json::Value {
        json!({
            "receiver": receiver,
            "frames": self.total_frames,
            "audio_samples": self.audio_samples,
            "attempts": self.attempts,
            "video_idle_ms": self.last_advance.map(|last| now.duration_since(last).as_millis()),
            "audio_idle_ms": self.last_audio_advance.map(|last| now.duration_since(last).as_millis()),
            "ended": self.ended,
            "observer_error": self.error,
        })
    }

    pub fn line(&mut self, line: &str, now: Instant) {
        if line == "BENCH_ATTEMPT" {
            self.attempts += 1;
            self.frames = 0;
            self.media_us = 0;
            self.pending_frames = 0;
            self.pending_media_us = 0;
            self.audio_us = -1.0;
        } else if line.contains("ashowinfo") {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            let time = fields
                .iter()
                .find_map(|field| field.strip_prefix("pts_time:"))
                .and_then(|value| value.parse::<f64>().ok());
            let samples = fields
                .iter()
                .find_map(|field| field.strip_prefix("nb_samples:"))
                .and_then(|value| value.parse::<u64>().ok());
            if let (Some(time), Some(samples)) = (time, samples)
                && time.is_finite()
                && time != self.audio_us
                && samples > 0
            {
                self.audio_us = time;
                self.audio_samples += samples;
                self.last_audio_advance = Some(now);
            }
        } else if let Some(value) = line.strip_prefix("frame=") {
            self.pending_frames = value.trim().parse().unwrap_or(0);
        } else if let Some(value) = line.strip_prefix("out_time_us=") {
            self.pending_media_us = value.trim().parse().unwrap_or(0);
        } else if line.starts_with("progress=") {
            if self.pending_frames > self.frames && self.pending_media_us > self.media_us {
                if let Some(previous) = self.last_advance {
                    self.max_gap_ms = self
                        .max_gap_ms
                        .max(now.duration_since(previous).as_millis());
                }
                self.total_frames += self.pending_frames - self.frames;
                self.last_advance = Some(now);
            }
            self.frames = self.pending_frames;
            self.media_us = self.pending_media_us;
        }
    }

    pub fn healthy(&self, now: Instant, gap: Duration) -> bool {
        !self.ended
            && self.error.is_none()
            && self.frames > 0
            && self
                .last_advance
                .is_some_and(|last| now.duration_since(last) <= gap)
            && self
                .last_audio_advance
                .is_some_and(|last| now.duration_since(last) <= gap)
    }
}

pub struct Probe {
    pub container: String,
    pub progress: Arc<Mutex<Progress>>,
    task: JoinHandle<()>,
}

impl Probe {
    pub fn follow(container: String, receiver: String, report: Report) -> Result<Self> {
        let mut child = Command::new("docker")
            .args(["logs", "--follow", &container])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()?;
        let output = child.stdout.take().unwrap();
        let progress = Arc::new(Mutex::new(Progress::default()));
        let state = progress.clone();
        let name = container.clone();
        let task = tokio::spawn(async move {
            let mut lines = BufReader::new(output).lines();
            let mut reported = Instant::now();
            let initial = state.lock().unwrap().diagnostics(&receiver, reported);
            if let Err(error) = report.event("receiver_progress", initial) {
                state.lock().unwrap().error = Some(error.to_string());
            }
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => {
                        state.lock().unwrap().line(&line, Instant::now());
                        if let Err(error) = report.log(&name, &line) {
                            state.lock().unwrap().error = Some(error.to_string());
                            break;
                        }
                        let now = Instant::now();
                        if now.duration_since(reported) >= Duration::from_millis(250) {
                            let diagnostic = state.lock().unwrap().diagnostics(&receiver, now);
                            if let Err(error) = report.event("receiver_progress", diagnostic) {
                                state.lock().unwrap().error = Some(error.to_string());
                                break;
                            }
                            reported = now;
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        state.lock().unwrap().error = Some(error.to_string());
                        break;
                    }
                }
            }
            state.lock().unwrap().ended = true;
            let final_progress = state.lock().unwrap().diagnostics(&receiver, Instant::now());
            let _ = report.event("receiver_progress", final_progress);
            let _ = child.wait().await;
        });
        Ok(Self {
            container,
            progress,
            task,
        })
    }

    pub fn summary(&self) -> serde_json::Value {
        let state = self.progress.lock().unwrap();
        json!({
            "container": self.container,
            "attempts": state.attempts,
            "decoded_frames": state.total_frames,
            "decoded_audio_samples": state.audio_samples,
            "max_progress_gap_ms": state.max_gap_ms,
            "observer_error": state.error,
        })
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct Watch {
    receiver: String,
    progress: Arc<Mutex<Progress>>,
    attempts: u64,
    frames: u64,
    last_advance: Instant,
    audio_samples: u64,
    last_audio_advance: Instant,
}

impl Watch {
    pub fn arm(
        receiver: String,
        progress: Arc<Mutex<Progress>>,
        max_gap: Duration,
    ) -> Result<Self> {
        let state = progress.lock().unwrap();
        let now = Instant::now();
        ensure!(
            state.healthy(now, max_gap),
            "{receiver} has no fresh decoded media"
        );
        let watch = Self {
            receiver,
            progress: progress.clone(),
            attempts: state.attempts,
            frames: state.total_frames,
            last_advance: state.last_advance.unwrap(),
            audio_samples: state.audio_samples,
            last_audio_advance: state.last_audio_advance.unwrap(),
        };
        Ok(watch)
    }

    pub fn check(&mut self, now: Instant, gap: Duration, restarts: u64) -> Result<()> {
        let state = self.progress.lock().unwrap();
        ensure!(
            !state.ended && state.error.is_none(),
            "{} observer stopped: {:?}",
            self.receiver,
            state.error
        );
        ensure!(
            state.attempts.saturating_sub(self.attempts) <= restarts,
            "{} receiver restarted",
            self.receiver
        );
        if state.total_frames > self.frames {
            let last = state.last_advance.unwrap();
            ensure!(
                last.duration_since(self.last_advance) <= gap,
                "{} exceeded media gap {}ms",
                self.receiver,
                gap.as_millis()
            );
            self.frames = state.total_frames;
            self.last_advance = last;
        }
        ensure!(
            now.duration_since(self.last_advance) <= gap,
            "{} no decoded progress for {}ms",
            self.receiver,
            now.duration_since(self.last_advance).as_millis()
        );
        if state.audio_samples > self.audio_samples {
            let last = state.last_audio_advance.unwrap();
            ensure!(
                last.duration_since(self.last_audio_advance) <= gap,
                "{} exceeded audio gap {}ms",
                self.receiver,
                gap.as_millis()
            );
            self.audio_samples = state.audio_samples;
            self.last_audio_advance = last;
        }
        ensure!(
            now.duration_since(self.last_audio_advance) <= gap,
            "{} no decoded audio for {}ms",
            self.receiver,
            now.duration_since(self.last_audio_advance).as_millis()
        );
        Ok(())
    }
}

pub async fn watch(mut watches: Vec<Watch>, gap: Duration, restarts: u64) -> Result<()> {
    loop {
        for watch in &mut watches {
            watch.check(Instant::now(), gap, restarts)?;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn progress(state: &mut Progress, frames: u64, time: u64, now: Instant) {
        state.line(&format!("frame={frames}"), now);
        state.line(&format!("out_time_us={time}"), now);
        state.line("progress=continue", now);
        state.line(
            &format!(
                "[Parsed_ashowinfo_0] pts_time:{} nb_samples:1024",
                time as f64 / 1_000_000.0
            ),
            now,
        );
    }

    #[test]
    fn a_receiver_that_recovers_cannot_hide_a_gap() {
        let start = Instant::now();
        let mut state = Progress::default();
        state.line("BENCH_ATTEMPT", start);
        progress(&mut state, 30, 1_000_000, start);
        let shared = Arc::new(Mutex::new(state));
        let mut watch =
            Watch::arm("main/output".into(), shared.clone(), Duration::from_secs(2)).unwrap();
        progress(
            &mut shared.lock().unwrap(),
            120,
            4_000_000,
            start + Duration::from_secs(3),
        );
        assert!(
            watch
                .check(start + Duration::from_secs(3), Duration::from_secs(2), 0)
                .is_err()
        );
    }

    #[test]
    fn frozen_frames_and_restarts_are_not_progress() {
        let start = Instant::now();
        let mut state = Progress::default();
        state.line("BENCH_ATTEMPT", start);
        progress(&mut state, 30, 1_000_000, start);
        progress(&mut state, 30, 2_000_000, start + Duration::from_secs(1));
        assert_eq!(state.last_advance, Some(start));
        let shared = Arc::new(Mutex::new(state));
        let mut watch =
            Watch::arm("main/output".into(), shared.clone(), Duration::from_secs(2)).unwrap();
        shared
            .lock()
            .unwrap()
            .line("BENCH_ATTEMPT", start + Duration::from_secs(1));
        assert!(
            watch
                .check(start + Duration::from_secs(1), Duration::from_secs(2), 0)
                .is_err()
        );
    }

    #[test]
    fn video_progress_does_not_hide_frozen_audio() {
        let start = Instant::now();
        let mut state = Progress::default();
        state.line("BENCH_ATTEMPT", start);
        progress(&mut state, 30, 1_000_000, start);
        let shared = Arc::new(Mutex::new(state));
        let mut watch =
            Watch::arm("main/output".into(), shared.clone(), Duration::from_secs(2)).unwrap();
        for second in 1..=3 {
            let now = start + Duration::from_secs(second);
            {
                let mut state = shared.lock().unwrap();
                state.line(&format!("frame={}", 30 * (second + 1)), now);
                state.line(&format!("out_time_us={}", 1_000_000 * (second + 1)), now);
                state.line("progress=continue", now);
            }
            let result = watch.check(now, Duration::from_secs(2), 0);
            if second < 3 {
                assert!(result.is_ok());
            } else {
                assert!(result.unwrap_err().to_string().contains("no decoded audio"));
            }
        }
    }
}
