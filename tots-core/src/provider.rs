use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnServer {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnCredentials {
    pub username: String,
    pub password: String,
    pub servers: Vec<TurnServer>,
}

#[async_trait]
pub trait TurnProvider: Send + Sync {
    fn name(&self) -> &'static str;
    async fn get_credentials(&self) -> Result<TurnCredentials>;
}
