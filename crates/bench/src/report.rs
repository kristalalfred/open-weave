use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Clone)]
pub struct Report {
    started: Instant,
    writer: Arc<Mutex<BufWriter<File>>>,
}

impl Report {
    pub fn new(directory: &Path) -> Result<Self> {
        std::fs::create_dir_all(directory)?;
        Ok(Self {
            started: Instant::now(),
            writer: Arc::new(Mutex::new(BufWriter::new(File::create(
                directory.join("timeline.jsonl"),
            )?))),
        })
    }

    pub fn event(&self, kind: &str, data: impl Serialize) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        serde_json::to_writer(
            &mut *writer,
            &json!({
                "elapsed_ms": self.started.elapsed().as_millis(),
                "kind": kind,
                "data": data,
            }),
        )?;
        writeln!(writer)?;
        writer.flush()?;
        Ok(())
    }

    pub fn log(&self, container: &str, line: &str) -> Result<()> {
        self.event(
            "media_log",
            json!({"container": container, "line": redact(line)}),
        )
    }
}

pub fn redact(text: &str) -> String {
    let mut result = text.to_string();
    for marker in ["passphrase=", "Authorization: Bearer "] {
        let mut offset = 0;
        while let Some(found) = result[offset..].find(marker) {
            let start = offset + found + marker.len();
            let end = result[start..]
                .find(|c: char| c.is_whitespace() || "&\"'".contains(c))
                .map_or(result.len(), |n| start + n);
            result.replace_range(start..end, "[redacted]");
            offset = start + "[redacted]".len();
        }
    }
    result
}

pub fn write_json(path: &Path, value: &Value) -> Result<()> {
    std::fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_multiple_keys_without_removing_diagnostics() {
        assert_eq!(
            redact("srt://a?passphrase=one&mode=caller srt://b?passphrase=two': refused"),
            "srt://a?passphrase=[redacted]&mode=caller srt://b?passphrase=[redacted]': refused"
        );
    }
}
