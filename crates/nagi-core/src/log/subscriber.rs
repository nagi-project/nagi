use tracing_subscriber::EnvFilter;

/// Initializes the tracing subscriber writing to stderr.
///
/// Log level is controlled by the `NAGI_LOG_LEVEL` environment variable.
/// Defaults to `info` when unset.
///
/// Safe to call multiple times; subsequent calls are no-ops
/// (tracing-subscriber silently ignores duplicate global subscriber set).
pub fn init() {
    let filter =
        EnvFilter::try_from_env("NAGI_LOG_LEVEL").unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_target(false)
        .try_init();
}
