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
    #[allow(dead_code)]
    comment: String,
}

pub struct Authorizer {
    /// `None` — авторизация отключена (все допускаются).
    inner: Option<RwLock<HashMap<String, ClientInfo>>>,
}

impl Authorizer {
    /// Пустой авторизатор: все клиенты допускаются.
    pub fn empty() -> Self {
        Self { inner: None }
    }

    pub async fn load(path: &Path) -> std::io::Result<Self> {
        let data = tokio::fs::read(path).await?;
        let parsed: ClientsFile = serde_json::from_slice(&data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        info!(count = parsed.clients.len(), "loaded clients.json");
        Ok(Self {
            inner: Some(RwLock::new(parsed.clients)),
        })
    }

    pub async fn is_authorized(&self, client_id: &str) -> bool {
        match &self.inner {
            None => true,
            Some(map) => map.read().await.contains_key(client_id),
        }
    }
}
