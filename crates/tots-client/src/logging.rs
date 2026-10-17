//! Инициализация tracing.

use tracing_subscriber::EnvFilter;

pub fn init(debug: bool) {
    let default = if debug { "debug" } else { "info" };
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("tots={default},tots_providers={default}")));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_line_number(debug)
        .try_init();
}
