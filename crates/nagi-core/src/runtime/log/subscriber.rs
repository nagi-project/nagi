use std::sync::Once;

use tracing_subscriber::EnvFilter;

static INIT: Once = Once::new();

/// Initializes the tracing subscriber with the given log level.
///
/// Must be called before any tracing output is expected.
/// Safe to call multiple times; only the first call takes effect.
pub fn set_log_level(level: &str) {
    INIT.call_once(|| {
        init_with_filter(EnvFilter::new(level));
    });
}

/// Fallback initialization using `NAGI_LOG_LEVEL` env var, defaulting to `warn`.
///
/// Called automatically on first use if `set_log_level` was not called.
/// Safe to call multiple times; only the first call takes effect.
pub fn init() {
    INIT.call_once(|| {
        let filter =
            EnvFilter::try_from_env("NAGI_LOG_LEVEL").unwrap_or_else(|_| EnvFilter::new("warn"));
        init_with_filter(filter);
    });
}

fn init_with_filter(filter: EnvFilter) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    // Once is process-global, so only the first of these actually initializes.
    // These tests verify that calling each function does not panic.

    #[test]
    fn set_log_level_does_not_panic() {
        set_log_level("warn");
    }

    #[test]
    fn init_does_not_panic() {
        init();
    }

    #[test]
    fn repeated_calls_do_not_panic() {
        set_log_level("error");
        init();
        set_log_level("debug");
    }
}
