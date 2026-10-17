//! Интеграционные тесты провайдера VK.
//!
//! Тесты, ходящие в сеть, помечены `#[ignore]` и запускаются вручную:
//! `cargo test -p tots-providers -- --ignored`.

use tots_providers::vk::VkProvider;
use tots_providers::{ProviderError, TurnProvider};

#[test]
fn provider_rejects_empty_link() {
    let cfg = tots_providers::vk::client_config_from_link("");
    assert!(cfg.is_err());
}

#[tokio::test]
#[ignore = "requires network"]
async fn provider_fetches_real_credentials() {
    let link = std::env::var("TOTS_VK_LINK").expect("set TOTS_VK_LINK");
    let provider = VkProvider::new(tots_providers::vk::VkConfig::new(link)).unwrap();
    let creds = provider.get_credentials(1).await.unwrap();
    assert!(!creds.username.is_empty());
    assert!(!creds.password.is_empty());
    assert!(!creds.server_addrs.is_empty());
    assert!(creds.is_valid(std::time::Duration::from_secs(0)));
}

#[tokio::test]
#[ignore = "requires network"]
async fn provider_returns_captcha_for_bad_link() {
    let provider = VkProvider::new(tots_providers::vk::VkConfig::new(
        "https://vk.ru/call/join/aaaaaaaaaaaa",
    ))
    .unwrap();
    match provider.get_credentials(1).await {
        Err(ProviderError::CaptchaRequired { .. }) => {}
        Err(ProviderError::InvalidLink(_)) => {}
        other => panic!("expected captcha or invalid-link, got {other:?}"),
    }
}
