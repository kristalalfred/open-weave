//! Log output shared by every binary.

use tracing_subscriber::EnvFilter;

/// Environment variable selecting the log format: `json` for one JSON object
/// per line, anything else (or unset) for the default text format.
pub const LOG_FORMAT_VAR: &str = "WEAVE_LOG_FORMAT";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Text,
    Json,
}

impl LogFormat {
    #[must_use]
    pub fn parse(value: Option<&str>) -> Self {
        match value
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("json") => Self::Json,
            _ => Self::Text,
        }
    }

    #[must_use]
    pub fn from_env() -> Self {
        Self::parse(std::env::var(LOG_FORMAT_VAR).ok().as_deref())
    }
}

/// Install the global subscriber. The level comes from `RUST_LOG` and defaults
/// to `info`; the format from [`LOG_FORMAT_VAR`].
pub fn init() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match LogFormat::from_env() {
        LogFormat::Json => builder.json().init(),
        LogFormat::Text => builder.init(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_is_opt_in() {
        assert_eq!(LogFormat::parse(None), LogFormat::Text);
        assert_eq!(LogFormat::parse(Some("")), LogFormat::Text);
        assert_eq!(LogFormat::parse(Some("text")), LogFormat::Text);
        assert_eq!(LogFormat::parse(Some("json")), LogFormat::Json);
        assert_eq!(LogFormat::parse(Some(" JSON ")), LogFormat::Json);
    }
}
