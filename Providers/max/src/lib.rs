use anyhow::{bail, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tots_core::{RelayScope, TurnCredentials, TurnProvider};

/// Placeholder for a future standards-compliant MAX call transport.
///
/// This crate intentionally does not repurpose provider-scoped TURN credentials
/// as an arbitrary UDP tunnel. A real implementation must use MAX's supported
/// WebRTC call/media semantics and terms of service.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MaxProviderData {}

pub struct MaxProvider;

impl MaxProvider {
    pub fn new(_data: MaxProviderData) -> Result<Self> {
        Ok(Self)
    }
}

#[async_trait]
impl TurnProvider for MaxProvider {
    fn name(&self) -> &'static str {
        "max"
    }

    fn relay_scope(&self) -> RelayScope {
        RelayScope::ProviderSession
    }

    async fn get_credentials(&self) -> Result<TurnCredentials> {
        bail!(
            "MAX provider-native WebRTC transport is not implemented; provider-scoped TURN credentials are not exposed as a generic relay"
        )
    }
}
