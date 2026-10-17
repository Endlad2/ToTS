# ToTS — TURN-over-TLS Secure Tunnel

Собственный VPN-протокол на Rust, инкапсулирующий трафик поверх TURN-релеев.

## Сборка

```
# Убедитесь, что тулчейн свежий
rustup update stable
rustup default stable

# Сборка
cargo build --release
```

Бинарники:

```
target/release/tots-client
target/release/tots-server
```

## Архитектура

```
tots-providers   — получение TURN-креденшелов (VK)
tots-core        — TURN-клиент, DTLS, обфускация, Client ID
tots-wire        — yamux-мультиплексор, bond
tots-client      — клиент: локальный listener → TURN → сервер
tots-server      — сервер: TURN-приём → backend
```

## Зависимости

Ключевые крейты (все — из crates.io):

| Крейт ↕▾ | Назначение ↕▾ |
|---|---|
| −`tokio` | async runtime |
| −`reqwest` (rustls-tls) | HTTP control-plane |
| −`chacha20poly1305` | AEAD |
| −`md-5` | TURN long-term key |
| −`hmac` + `sha2` | MessageIntegrity |
| −`uuid` | device_id для OK CDN |
| −`yamux` | мультиплексор |
| −`rand_chacha` | детерминированный RNG |
| −`parking_lot` | sync примитивы |
| −`socket2` | socket options |
⚙

## Быстрый старт

```
# сервер
./target/release/tots-server \
  --listen 0.0.0.0:56000 \
  --connect 127.0.0.1:51820 \
  --mode udp \
  --obf-profile rtpopus3 \
  --obf-key $(openssl rand -hex 32) \
  --debug

# клиент
./target/release/tots-client \
  --peer <vps>:56000 \
  --provider vk \
  --link "https://vk.ru/call/join/XXXXXXXX" \
  --listen 127.0.0.1:9000 \
  --obf-profile rtpopus3 \
  --obf-key <key> \
  --client-id $(openssl rand -hex 16) \
  --debug
```

## Статус

- ✅ Workspace, все крейты собираются
- ✅ Провайдер VK (получение TURN-креденшелов)
- ✅ TURN-клиент (Allocate/CreatePermission/ChannelBind/Refresh/ChannelData)
- ✅ DTLS-подобный транспорт (X25519 + ChaCha20-Poly1305)
- ✅ Client ID с retransmit-подтверждением
- ✅ UDP-релей клиента и сервера
- ✅ Авторизация по `clients.json`
- ⬜ Bond-режим (инфраструктура готова)