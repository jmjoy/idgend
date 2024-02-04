use crate::args::Args;
use std::env::{self, VarError};
use tracing_subscriber::{fmt::layer, prelude::*, EnvFilter};

pub fn init_logger(args: &Args) {
    if matches!(env::var("RUST_LOG"), Err(VarError::NotPresent)) {
        env::set_var("RUST_LOG", &args.rust_log_filter);
    }

    tracing_subscriber::registry()
        .with(layer())
        .with(EnvFilter::from_default_env())
        .init();
}
