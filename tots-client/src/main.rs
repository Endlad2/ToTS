#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tots_client::run().await
}
