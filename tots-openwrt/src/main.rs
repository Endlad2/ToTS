//! OpenWrt executable entrypoint. The dataplane and provider implementation are
//! shared with tots-client; this package exists to produce a dedicated router
//! binary and release artifact without forking the client implementation.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tots_client::run().await
}
