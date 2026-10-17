# Быстрый старт

## Сборка

```
cargo build --release
```

Бинарники появятся в `target/release/`:

- `tots-client`
- `tots-server`

## Сервер

```
./target/release/tots-server \
  --listen 0.0.0.0:56000 \
  --connect 127.0.0.1:51820 \
  --mode udp \
  --obf-profile rtpopus3 \
  --obf-key $(openssl rand -hex 32) \
  --debug
```

| Флаг ↕▾ | Смысл ↕▾ |
|---|---|
| −`--listen` | внешний UDP-порт |
| −`--connect` | локальный backend (WG `51820`, Xray `443`) |
| −`--mode` | `udp` (WG) или `tcp` (Xray) |
| −`--obf-profile` | `none` | `rtpopus` | `rtpopus2` | `rtpopus3` |
| −`--obf-key` | 64 hex-символа, общий с клиентом |
| −`--clients-file` | JSON со списком разрешённых Client ID |
⚙

## Клиент

```
./target/release/tots-client \
  --peer <vps-ip>:56000 \
  --provider vk \
  --link "https://vk.ru/call/join/XXXXXXXX" \
  --listen 127.0.0.1:9000 \
  --streams 12 \
  --obf-profile rtpopus3 \
  --obf-key <тот-же-ключ> \
  --debug
```

| Флаг ↕▾ | Смысл ↕▾ |
|---|---|
| −`--peer` | адрес сервера на VPS |
| −`--provider` | `vk` или `direct` |
| −`--link` | ссылка на звонок (нужна для `vk`) |
| −`--listen` | локальный порт для WireGuard |
| −`--streams` | число параллельных TURN-потоков |
| −`--mode` | должен совпадать с сервером |
| −`--obf-profile` / `--obf-key` | должны совпадать с сервером |
⚙

## Текущий статус

Провайдер VK полностью рабочий: клиент получает TURN-креды и логирует их.
Транспортный слой (DTLS + обфускация + релей) — в разработке.