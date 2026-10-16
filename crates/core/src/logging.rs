use std::error::Error;
use std::fmt;

use tracing_subscriber::EnvFilter;

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

pub fn init() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match LogFormat::from_env() {
        LogFormat::Json => builder.json().init(),
        LogFormat::Text => builder.init(),
    }
}

pub struct ErrorChain<'a>(pub &'a (dyn Error + 'static));

impl fmt::Display for ErrorChain<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;
        let mut source = self.0.source();
        while let Some(error) = source {
            write!(f, ": {error}")?;
            source = error.source();
        }
        Ok(())
    }
}

const EXCERPT_BYTES: usize = 1024;

#[must_use]
pub fn excerpt(text: &str) -> String {
    let mut line = String::new();
    for word in text.split_whitespace() {
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(word);
        if line.len() > EXCERPT_BYTES {
            let cut = (0..=EXCERPT_BYTES)
                .rev()
                .find(|&index| line.is_char_boundary(index))
                .unwrap_or(0);
            line.truncate(cut);
            line.push('…');
            break;
        }
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    #[error("registering the node")]
    struct Registering(#[source] Handshake);

    #[derive(Debug, thiserror::Error)]
    #[error("TLS handshake failed")]
    struct Handshake(#[source] std::io::Error);

    #[test]
    fn an_error_chain_names_every_cause_on_one_line() {
        let error = Registering(Handshake(std::io::Error::other(
            "invalid peer certificate: UnknownIssuer",
        )));

        assert_eq!(
            ErrorChain(&error).to_string(),
            "registering the node: TLS handshake failed: invalid peer certificate: UnknownIssuer"
        );
    }

    #[test]
    fn an_excerpt_never_splits_a_character() {
        let quoted = excerpt(&"€".repeat(1000));

        assert!(quoted.starts_with("€€€"), "{quoted}");
        assert!(quoted.ends_with('…'), "{quoted}");
        assert!(quoted.len() <= EXCERPT_BYTES + '…'.len_utf8());
    }

    #[test]
    fn json_is_opt_in() {
        assert_eq!(LogFormat::parse(None), LogFormat::Text);
        assert_eq!(LogFormat::parse(Some("")), LogFormat::Text);
        assert_eq!(LogFormat::parse(Some("text")), LogFormat::Text);
        assert_eq!(LogFormat::parse(Some("json")), LogFormat::Json);
        assert_eq!(LogFormat::parse(Some(" JSON ")), LogFormat::Json);
    }
}
