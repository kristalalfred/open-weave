//! The Strom releases the adapter is written for.

/// The oldest Strom release the adapter supports: `(major, minor, patch)`.
pub const MIN_VERSION: (u64, u64, u64) = (0, 6, 9);

/// Whether `version`, as Strom reports it (`0.6.10`, `v0.6.10-rc1`), is at least
/// [`MIN_VERSION`]. `None` when the string has no `major.minor.patch` prefix.
#[must_use]
pub fn is_supported(version: &str) -> Option<bool> {
    parse(version).map(|found| found >= MIN_VERSION)
}

fn parse(version: &str) -> Option<(u64, u64, u64)> {
    let core = version.trim().trim_start_matches('v');
    let core = core.split(['-', '+']).next()?;
    let mut parts = core.split('.');
    let mut next = || parts.next()?.parse::<u64>().ok();
    let parsed = (next()?, next()?, next()?);
    parts.next().is_none().then_some(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compares_by_number_not_by_text() {
        assert_eq!(is_supported("0.6.10"), Some(true));
        assert_eq!(is_supported("0.6.9"), Some(true));
        assert_eq!(is_supported("0.6.8"), Some(false));
        assert_eq!(is_supported("0.5.20"), Some(false));
        assert_eq!(is_supported("0.7.0"), Some(true));
        assert_eq!(is_supported("1.0.0"), Some(true));
    }

    #[test]
    fn ignores_a_prefix_and_a_suffix() {
        assert_eq!(is_supported("v0.6.10"), Some(true));
        assert_eq!(is_supported("0.6.8-rc1"), Some(false));
        assert_eq!(is_supported("0.6.10+abc"), Some(true));
    }

    #[test]
    fn a_string_that_is_not_a_version_is_unknown() {
        assert_eq!(is_supported(""), None);
        assert_eq!(is_supported("latest"), None);
        assert_eq!(is_supported("0.6"), None);
        assert_eq!(is_supported("0.6.9.1"), None);
    }
}
