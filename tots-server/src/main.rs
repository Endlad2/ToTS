mod api;
mod bridge;
mod config;
mod dataplane;
mod db;
use anyhow::Result;
use clap::Parser;
use config::Args;
use tracing::info;
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("tots_server=info".parse()?),
        )
        .init();
    let args = Args::parse();
    if args.public_host.trim().is_empty() {
        anyhow::bail!("--public-host / TOTS_PUBLIC_HOST is required");
    }
    let db = db::Db::open(&args.db).await?;
    let restore_state = api::ApiState {
        db: db.clone(),
        args: args.clone(),
        rooms: std::sync::Arc::new(dashmap::DashMap::new()),
        room_changes: std::sync::Arc::new(tokio::sync::Mutex::new(())),
    };
    let listener = tokio::net::TcpListener::bind(args.api_listen).await?;
    info!(listen=%args.api_listen,"management API");
    let api_state = restore_state.clone();
    let api_task = tokio::spawn(async move { axum::serve(listener, api::router(api_state)).await });
    // The dataplane binds the authenticated ingress sockets. Start it first: a
    // restored provider bridge connects its loopback socket to those ports, and an
    // unbound port produced an ICMP port-unreachable that killed the bridge task.
    let data_args = args.clone();
    let data_task = tokio::spawn(async move { dataplane::run(data_args, db).await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    tokio::spawn(async move {
        api::restore_rooms(restore_state).await;
    });
    tokio::select! {r=api_task=>r??,r=data_task=>r??};
    Ok(())
}
