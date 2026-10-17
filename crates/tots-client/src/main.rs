//! ToTS client — точка входа.

mod cli;
mod logging;
mod runtime;
mod session;
mod transport;
mod udp;

use anyhow::Result;
use clap::Parser;

use crate::cli::Cli;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    logging::init(cli.debug);
    runtime::run(cli).await
}
