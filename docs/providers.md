# Провайдеры

## Принцип

`tots-providers` — единственное место, где живёт логика получения
TURN-креденшелов. Клиент и сервер зависят только от trait `TurnProvider`.

```
use tots_providers::{TurnProvider, TurnCredentials, ProviderError};

let provider = tots_providers::vk::VkProvider::new(cfg)?;
let creds: TurnCredentials = provider.get_credentials(stream_id).await?;
```

## Реализованные

### VK Calls (`vk`)

Получает TURN-креды из ссылки на звонок VK.

```
use tots_providers::vk::{VkConfig, VkProvider};

let cfg = VkConfig {
    link: "https://vk.ru/call/join/ABC123".into(),
    streams_per_cache: 12,
    restrict_to_first_client: false,
    request_timeout: Duration::from_secs(20),
};
let provider = VkProvider::new(cfg)?;
```

## Планируемые

| Провайдер ↕▾ | Источник ↕▾ | Статус ↕▾ |
|---|---|---|
| −`direct` | прямо на `--peer`, без TURN | TODO |
| −`static` | статический список в конфиге | TODO |
| −`telemost` | Yandex Telemost | TODO |
⚙

## Написание своего провайдера

```
use async_trait::async_trait;
use tots_providers::{ProviderError, TurnCredentials, TurnProvider};

struct MyProvider { /* ... */ }

#[async_trait]
impl TurnProvider for MyProvider {
    async fn get_credentials(&self, stream_id: u32)
        -> Result<TurnCredentials, ProviderError>
    {
        // ...
        # todo!()
    }
    fn name(&self) -> &'static str { "my" }
}
```

Достаточно реализовать `get_credentials`; остальные методы имеют
разумные дефолты.