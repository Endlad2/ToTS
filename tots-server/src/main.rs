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
    let api_state = api::ApiState {
        db: db.clone(),
        args: args.clone(),
        rooms: std::sync::Arc::new(dashmap::DashMap::new()),
        room_changes: std::sync::Arc::new(tokio::sync::Mutex::new(())),
    };
    let restore = api_state.clone();
    tokio::spawn(async move {
        api::restore_rooms(restore).await;
    });
    let listener = tokio::net::TcpListener::bind(args.api_listen).await?;
    info!(listen=%args.api_listen,"management API");
    let api_task = tokio::spawn(async move { axum::serve(listener, api::router(api_state)).await });
    let data_args = args.clone();
    let data_task = tokio::spawn(async move { dataplane::run(data_args, db).await });
    tokio::select! {r=api_task=>r??,r=data_task=>r??};
    Ok(())
}
