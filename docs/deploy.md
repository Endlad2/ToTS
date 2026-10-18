# Развёртывание сервера

## Установка скриптом (рекомендуется)

```bash
curl -fsSL https://raw.githubusercontent.com/hackdiaz-dev/tots/master/scripts/install.sh | sudo bash
```

Мастер спросит:

1. **VPN на сервере** - новый WireGuard (скрипт поднимет его сам) или уже установленный VPN (Amnezia, wg-easy, 3x-ui и т.п.: указываете его `host:port`).
2. **Запуск** - `docker` или `systemd`.
3. **Порт и обфускация.**

В конце - ссылка `tots://` для приложения (и WireGuard-конфиг, если VPN новый).

Управление:

```bash
tots                      # меню: клиенты, настройки, обновление, логи, удаление
tots client add phone     # новый клиент и его ссылка
tots client list
tots client qr phone      # показать ссылку и QR ещё раз
tots client remove phone
```

Без вопросов:

```bash
curl -fsSL https://raw.githubusercontent.com/hackdiaz-dev/tots/master/scripts/install.sh | \
  sudo bash -s -- -y --backend=new --method=docker
```

Все флаги: `tots --help`.

## Ручная установка

Ключ обфускации (одинаковый на сервере и клиенте): `openssl rand -hex 32`.

### Docker Compose

```yaml
services:
  tots:
    image: ghcr.io/hackdiaz-dev/tots:latest
    network_mode: host
    restart: unless-stopped
    environment:
      - CONNECT_ADDR=127.0.0.1:51820   # ваш VPN
      - LISTEN_ADDR=0.0.0.0:56000
      - OBF_PROFILE=rtpopus3
      - OBF_KEY=<ВАШ_КЛЮЧ>
```

| Переменная | По умолчанию | |
| --- | --- | --- |
| `CONNECT_ADDR` | обязательна | адрес вашего VPN |
| `LISTEN_ADDR` | `0.0.0.0:56000` | внешний адрес |
| `MODE` | `udp` | `udp` (WireGuard) \| `tcp` (Xray/VLESS), как на клиенте |
| `OBF_PROFILE` | `none` | `none` \| `rtpopus` \| `rtpopus2` \| `rtpopus3` |
| `OBF_KEY` | - | ключ обфускации |
| `CLIENTS_FILE` | - | список разрешённых Client ID |
| `KCP_*` | - | тюнинг `MODE=tcp`, см. `docs/flags.md` |
| `DEBUG` | `false` | подробные логи |

### systemd

```bash
sudo mkdir -p /opt/tots
sudo curl -L -o /opt/tots/server \
  https://github.com/hackdiaz-dev/tots/releases/latest/download/server-linux-amd64
sudo chmod +x /opt/tots/server
```

`/etc/systemd/system/tots.service`:

```ini
[Unit]
Description=ToTS
After=network-online.target

[Service]
ExecStart=/opt/tots/server -listen 0.0.0.0:56000 -connect 127.0.0.1:51820 -obf-profile rtpopus3 -obf-key <ВАШ_КЛЮЧ>
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now tots
```

### Доступ по Client ID

Без списка подключиться может любой, у кого есть ключ обфускации. Список:

```bash
sudo CLIENTS_FILE=/opt/tots/clients.json \
  /opt/tots/server clients add $(openssl rand -hex 16) phone
```

Запуск с `-clients-file /opt/tots/clients.json` (Docker: `CLIENTS_FILE` + volume). На клиенте - `-client-id` или ссылка `tots://`.

## Порт

Откройте внешний порт по **UDP** (при `MODE=tcp` тоже): `sudo ufw allow 56000/udp` (firewalld: `sudo firewall-cmd --permanent --add-port=56000/udp && sudo firewall-cmd --reload`). Скрипт делает это сам.
