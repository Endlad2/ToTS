# Архитектура ToTS

## Слои

```
┌──────────────────────────────────────────────┐
│             tots-client (bin)                │
│  CLI → runtime → локальный listener → ...    │
└──────────────┬───────────────────────────────┘
               │ TurnCredentials
               ▼
┌──────────────────────────────────────────────┐
│           tots-providers (lib)               │
│  trait TurnProvider                          │
│   ├── vk::VkProvider   (реализован)          │
│   ├── direct::Direct   (TODO)                │
│   └── static::Static   (TODO)                │
└──────────────────────────────────────────────┘
               ▲
               │ TurnCredentials
┌──────────────┴───────────────────────────────┐
│             tots-server (bin)                │
│  DTLS listener → обфускация → backend        │
└──────────────────────────────────────────────┘
```

## Контракт `TurnProvider`

```
#[async_trait]
pub trait TurnProvider: Send + Sync {
    async fn get_credentials(&self, stream_id: u32)
        -> Result<TurnCredentials, ProviderError>;
    fn is_auth_error(&self, err: &ProviderError) -> bool;
    fn handle_auth_error(&self, stream_id: u32) -> bool;
    fn reset_errors(&self, stream_id: u32);
    fn drop_credentials(&self, stream_id: u32);
    fn backoff_until_unix(&self) -> i64;
    fn name(&self) -> &'static str;
}
```

Клиент и сервер получают креды **только** через этот интерфейс.
Никакой провайдерской логики внутри `tots-client` / `tots-server`.

## Провайдер VK

Цепочка запросов:

| # ↕▾ | Метод ↕▾ | Endpoint ↕▾ | Что даёт ↕▾ |
|---|---|---|---|
| −1 | POST | `login.vk.ru/?act=get_anonym_token` | анонимный `access_token` |
| −2 | POST | `api.vk.ru/method/calls.getAnonymousToken` | токен звонка |
| −3 | POST | `calls.okcdn.ru/fb.do` (`auth.anonymLogin`) | `session_key` |
| −4 | POST | `calls.okcdn.ru/fb.do` (`vchat.joinConversationByLink`) | `turn_server.{username,credential,urls}` |
⚙

Если на шаге 2 приходит `error_code: 14` — VK требует капчу.
Провайдер возвращает `ProviderError::CaptchaRequired` с `session_token` и
`redirect_uri`; решение капчи — отдельный этап (позже).

### Кэш кредов

`CredentialStore` нарезает потоки на группы по `streams_per_cache`:
потоки `1..=N` делят один набор кредов, `N+1..=2N` — следующий и т.д.
Это снижает число обращений к VK и защищает от rate limit.

### Обработка ошибок

- `error_code: 14` → `CaptchaRequired`
- `error_code: 29` → `RateLimited`
- `error_code: 9000/9008` → `InvalidLink`
- `error_code: 5` → `Auth`
- остальное → `Malformed`

## Дорожная карта

1. ✅ Скелет workspace
2. ✅ Провайдер VK
3. ⬜ TURN-клиент (allocate / create-permission / channel-bind)
4. ⬜ DTLS-транспорт поверх TURN
5. ⬜ Обфускация rtpopus3
6. ⬜ UDP-релей (WireGuard)
7. ⬜ TCP-режим (KCP + smux)
8. ⬜ Другие провайдеры