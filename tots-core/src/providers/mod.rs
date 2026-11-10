use crate::{config::ProviderName, Error, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IceServer {
    pub urls: Vec<String>,
    pub username: Option<String>,
    pub credential: Option<String>,
}
#[derive(Clone, Debug, Default)]
pub struct ProviderBootstrap {
    pub turn: Vec<crate::transport::turn::TurnCredentials>,
    pub stun_urls: Vec<String>,
    pub signalling: Value,
}

#[async_trait]
pub trait Provider: Send + Sync {
    async fn bootstrap(&self, data: &Value) -> Result<ProviderBootstrap>;
    async fn connect_native(
        &self,
        _data: &Value,
    ) -> Result<Option<std::sync::Arc<dyn crate::transport::DatagramPath>>> {
        Ok(None)
    }
    async fn cleanup(&self, _data: &Value, _bootstrap: &ProviderBootstrap) -> Result<()> {
        Ok(())
    }
}

#[path = "../../../Providers/jazz.rs"]
pub mod jazz;
#[path = "../../../Providers/vk.rs"]
pub mod vk;

pub fn make_provider(name: &ProviderName) -> Result<Box<dyn Provider>> {
    match name {
        ProviderName::Vk => Ok(Box::new(vk::VkProvider)),
        ProviderName::Jazz => Ok(Box::new(jazz::JazzProvider)),
        ProviderName::Unknown => Err(Error::Provider("unknown provider".into())),
    }
}
