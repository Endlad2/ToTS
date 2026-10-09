# LaLune.ToTS

**Tunnel over TURN / Socks** — VPN через TURN-серверы MAX Messenger.

## Как это работает (высокоуровнево)

```

END_NODE (клиент)                                  EXIT_NODE (сервер)
┌──────────────────────────┐                     ┌──────────────────────────┐
│ SOCKS5 UDP ASSOCIATE     │                     │ Читает handshake-группу  │
│ MAX-сессия (Playwright)  │◄─── MAX-сообщения ──►│ Расшифровывает payload   │
│ Создаёт звонки в группах │                     │ Подключается к TURN'ам   │
│ Получает TURN-креды      │                     │ Пропускает UDP в интернет│
│ Шифрует + отправляет в   │                     │ Шифрует обратно          │
│  handshake-группу MAX    │◄─── TURN (трафик) ──►│ Отправляет "Start"       │
└──────────────────────────┘                     └──────────────────────────┘

```

## Установка

```bash
dotnet restore
dotnet build -c Release
pwsh bin/Release/net8.0/playwright.ps1 install chromium
```

## Режим exit_node

Запускать от root (для `ufw`).

```
sudo lalune-tots --exit_node --password="enderunity73@" --session-path="/opt/tots/session"
```

При запуске:

1. Спросит ID handshake-группы MAX (создай её заранее).
2. Сгенерирует ToTS-ключ (`.db`, упакованный в парольный ZIP).
3. Выведет конфиг `tots://...` в консоль.

Скопируй конфиг целиком — он нужен end_node'у.

## Режим end_node

```
lalune-tots --end_node \
  --config="tots://handshake_group=...&password=...&key=..." \
  --session-path="/home/user/.tots/session" \
  --listen="127.0.0.1:1080"
```

Поднимет локальный SOCKS5 на `--listen` с поддержкой `UDP ASSOCIATE`.

## Формат конфига

```
tots://handshake_group=<ID>&password=<пароль>&key=<base64 ZIP>
```

- `handshake_group` — ID группы MAX, через которую идёт обмен (создаётся вручную).
- `password` — пароль для распаковки ZIP.
- `key` — base64 запароленного ZIP-архива, внутри — `.db` с ToTS-ключом.

## ToTS-ключ

`.db` — это SQLite с двумя таблицами:

- `every_symbol(value INTEGER)` — одно число 3..10. Это **длина** каждой шифрованной подстановки.
- `Symbols(symbol TEXT, value TEXT)` — 64 строки. Каждый символ base64-алфавита заменяется на строку фиксированной длины `every_symbol`.

Алфавит: `ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=`

**Шифрование:** payload → base64 → посимвольная замена → зашифрованная строка.
**Дешифрование:** режем строку на куски по `every_symbol` символов → обратный lookup → base64-строка → decode.

## Формат handshake-сообщения

**End_node → handshake-группа:**

Весь JSON шифруется целиком, в MAX-группу уходит **только зашифрованная строка**.

```
{
  "turn": [
    "turn:155.212.199.137:19302|turn:155.212.205.13:19302|username|credential|expires_at",
    "..."
  ],
  "uuid": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Exit_node → handshake-группа:**

```
{
  "uuid": "550e8400-e29b-41d4-a716-446655440000",
  "status": "Start"
}
```

## Текущий статус

Реализовано (Этап 1):

- ✅ CLI (end_node / exit_node)
- ✅ Разбор конфига `tots://...`
- ✅ Генерация ToTS-ключа
- ✅ Шифр ToTS (шифрование/дешифрование, round-trip)
- ✅ `.db` в ZIP с паролем
- ✅ Тест round-trip при запуске end_node

В работе (Этапы 2–6):

- ⏳ MAX-клиент через Playwright
- ⏳ Отправка/чтение handshake-сообщений
- ⏳ TURN-клиент (Allocate / ChannelBind / ChannelData)
- ⏳ SOCKS5 UDP ASSOCIATE сервер
- ⏳ HTTP-стрим логина для exit_node
- ⏳ Мост SOCKS5 ↔ TURN

## Структура проекта

```
LaLune.ToTS/
├── Program.cs                # точка входа, разбор флагов
├── Cli/
│   ├── CommandLine.cs        # DTO для флагов
│   └── ConfigParser.cs       # tots://...
├── Crypto/
│   ├── TotsCipher.cs         # супер-быстрый шифр
│   ├── TotsKeyDb.cs          # SQLite-обёртка
│   └── ZipWithPassword.cs    # ZIP с AES-256
├── Common/
│   ├── HandshakeMessage.cs   # DTO сообщения
│   └── Log.cs
├── ExitNode/
│   ├── ExitNodeRunner.cs     # точка входа
│   └── ExitNodeSetup.cs      # генератор конфига
├── EndNode/
│   └── EndNodeRunner.cs      # точка входа
├── Max/                      # (Этап 2)
├── Turn/                     # (Этап 3)
└── Socks/                    # (Этап 4)
```

## Лицензия

Проект для сообщества LaLune. Использование на свой риск.

</BDS:create_file>

<BDS:create_file fileName="LaLune.ToTS/.gitignore">

bin/
obj/
*.user
session/
*.db
*.zip

</BDS:create_file>

</BDS:LONG_WORK>

## Что реализовано (Этап 1)

✅ **Структура проекта** — один бинарник, режимы `--end_node` / `--exit_node`
✅ **Разбор конфига** `tots://handshake_group=...&password=...&key=...`
✅ **Генерация ToTS-ключа** — `every_symbol` (3..10) + 64 символа `Symbols`
✅ **SQLite-хранилище ключа** (`.db`) — таблицы `every_symbol` и `Symbols`
✅ **ZIP с AES-256-паролем** (SharpZipLib)
✅ **ToTS-шифр** — супер-быстрый (`string[]` по индексу char + обратный `Dictionary`)
✅ **DTO handshake** — `{"turn":[...], "uuid":"..."}` / `{"uuid":"...", "status":"Start"}`
✅ **exit_node** — генерит ключ, упаковывает в ZIP, печатает конфиг в консоль
✅ **end_node** — парсит конфиг, распаковывает `.db`, тестит round-trip шифра

## Что делать

```bash
cd LaLune.ToTS
dotnet restore
dotnet build -c Release
pwsh bin/Release/net8.0/playwright.ps1 install chromium

**Тест 1: exit_node**

```
dotnet run --project LaLune.ToTS -- \
  --exit_node \
  --password="testpass123" \
  --session-path="./session"
```

Он спросит ID handshake-группы (введи `-79867987848553` или любой), сгенерирует конфиг и выведет его в консоль **одной строкой**.

**Скопируй конфиг** — он нужен для теста 2.

**Тест 2: end_node** (в другом терминале)

```
dotnet run --project LaLune.ToTS -- \
  --end_node \
  --config="tots://handshake_group=-79867987848553&password=testpass123&key=..." \
  --session-path="./session"
```

**Что должно произойти:**

```
═══ LaLune.ToTS :: end_node ═══
→ Разбираю конфиг ...
  handshake_group : -79867987848553
  key zip size    : 4823 bytes
→ Распаковываю ключ ...
  .db размер : 12288 bytes
→ Инициализирую ToTS-шифр ...
  every_symbol : 5
  symbols      : 64
→ Тест шифра ...
  исходная длина : 99
  шифр длина     : 495
  round-trip OK  : ✓
```

**Ключевое:** `round-trip OK : ✓` — значит **шифр, ZIP, `.db` — всё работает**. Шифр длина = исходная × `every_symbol`.

## Скинь мне вывод обоих тестов

Как только увижу `round-trip OK ✓` — **двигаемся к Этапу 2: MAX-клиент** (Playwright + INIT/LOGIN/CALL_TOKEN + отправка/приём сообщений в группе). У нас **уже есть рабочий код** для WS-сессии из `LaLune.MaxExtractor` — я его **перенесу** и адаптирую под постоянную сессию.

**Если будут ошибки компиляции** — скинь, поправлю мгновенно.
