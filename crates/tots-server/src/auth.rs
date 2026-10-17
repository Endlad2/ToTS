//! Проверка Client ID по файлу clients.json.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::info;

#[derive(Debug, Default, Serialize, Deserialize)]
struct ClientsFile {
    clients: HashMap<String, ClientInfo>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ClientInfo {
    #[serde(default)]
    comment: String,
}

pub struct Authorizer {
    inner: RwLock<HashMap<String, ClientInfo>>,
}

impl Authorizer {
    pub fn empty() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub async fn load(path: &Path) -> std::io::Result<Self> {
        let data = tokio::fs::read(path).await?;
        let parsed: ClientsFile = serde_json::from_slice(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        info!(count = parsed.clients.len(), "loaded clients.json");
        Ok(Self {
            inner: RwLock::new(parsed.clients),
        })
    }

    pub async fn is_authorized(&self, client_id: &str) -> bool {
        self.inner.read().await.contains_key(client_id)
    }
}
