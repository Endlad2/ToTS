//! ToTS command line client.
//!
//! Usage:
//!   tots info
//!   tots transports
//!   tots run --link "tots://connect?transport=vk&payload=wg&server=HOST:PORT&password=SECRET"

use anyhow::{anyhow, Context, Result};
use tots_core::{Config, TransportKind};

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(String::as_str).unwrap_or("info");

    match cmd {
        "info" => print_info(),
        "transports" => print_transports(),
        "run" => run(&args),
        other => {
            eprintln!("unknown command `{other}`");
            eprintln!("usage: tots [info|transports|run --link <LINK>]");
            std::process::exit(2);
        }
    }
}

fn print_info() -> Result<()> {
    println!("ToTS v{}", tots_core::VERSION);
    println!("payloads: wireguard (udp), xray (tcp)");
    Ok(())
}

fn print_transports() -> Result<()> {
    println!("available transports:");
    for t in TransportKind::ALL {
        let family = if t.is_realtime() { "realtime" } else { "document" };
        println!("  {:<10} {:<9} (cf. {})", t.id(), family, upstream(t));
    }
    Ok(())
}

fn upstream(t: TransportKind) -> &'static str {
    match t {
        TransportKind::Vk => "free-turn-proxy-core",
        TransportKind::YandexTelemost | TransportKind::VkStream => "olcrtc-core",
        _ => "OpenFlux-core",
    }
}

fn run(args: &[String]) -> Result<()> {
    let mut link: Option<String> = None;
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--link" | "-l" => {
                link = args.get(i + 1).cloned();
                i += 2;
            }
            other => {
                return Err(anyhow!("unexpected argument `{other}`"));
            }
        }
    }

    let link = link.context("missing --link")?;
    let cfg = Config::from_link(&link).context("parsing tots link")?;
    println!("transport = {}", cfg.transport.id());
    println!("payload   = {:?}", cfg.payload);
    println!("server    = {}", cfg.server);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(async move {
        let mut tunnel = tots_core::Tunnel::new(cfg)?;
        tunnel.connect().await?;
        log::info!(
            "connected via {} carrying {:?}",
            tunnel.transport_kind().id(),
            tunnel.payload()
        );
        // Real packet relay is driven by the platform TUN integration; here we
        // demonstrate the encrypted round-trip with a probe datagram.
        tunnel.send(b"tots-probe").await?;
        if let Some(reply) = tunnel.recv().await? {
            log::info!("received {} bytes back", reply.len());
        }
        tunnel.close().await?;
        Ok::<(), tots_core::Error>(())
    })?;

    Ok(())
}