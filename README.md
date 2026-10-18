<div align="center">

![License](https://img.shields.io/badge/license-Happy_Bunny-ff69b4?style=flat-square&logoColor=white&labelColor=0D1117)
![Go](https://img.shields.io/badge/Go-1.26-00ADD8?style=flat-square&logo=go&logoColor=white&labelColor=0D1117)
![Docker](https://img.shields.io/badge/docker-ready-2496ED?style=flat-square&logo=docker&logoColor=white&labelColor=0D1117)
![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20Windows%20%7C%20macOS%20%7C%20Android%20%7C%20iOS-green?style=flat-square&labelColor=0D1117)
</div>

## О проекте

**ToTS** — универсальный прокси-туннель для инкапсуляции UDP/TCP трафика поверх протокола TURN. Клиент извлекает временные TURN-учётки из ссылок на WebRTC-звонки и прозрачно маршрутизирует ваш VPN-трафик (WireGuard, AmneziaWG, Xray/VLESS) до сервера на VPS, используя DTLS и механизмы маскировки пакетов.

> [!NOTE]
> **ToTS основан на проекте [ToTS](https://github.com/hackdiaz-dev/tots).**
> Оригинальная архитектура, провайдеры, wire-профили обфускации и клиентское ядро
> разработаны авторами ToTS. ToTS — это переименованная и реорганизованная ветка:
> работа с провайдерами (VK captcha и получение TURN-реквизитов) вынесена в отдельный
> каталог `providers/`, а брендинг проекта заменён. Все заслуги за исходную реализацию
> принадлежат контрибьюторам ToTS.

## Android-приложение — рекомендуемый способ использования

- Добавляйте подключения по ссылке, QR-коду или вручную.
- Устанавливайте и настраивайте сервер на VPS из приложения.
- Переключайте серверы, настраивайте раздельное туннелирование и делитесь доступом.
- Сохраняйте настройки в зашифрованную резервную копию.

[Скачать APK](https://github.com/hackdiaz-dev/turn-proxy-android/releases/latest)

Требования: Android 7.0+ и устройство ARM (arm64-v8a или armeabi-v7a). Для собственного подключения нужен VPS; в режиме Relay также нужна ссылка на звонок.

## Разработка

### Зависимости

- **Go** ≥ 1.26 — `https://go.dev/dl/`
- **Task** (runner) — `go install github.com/go-task/task/v3/cmd/task@v3.40.0` или `winget install Task.Task` / `brew install go-task`

Остальные dev-инструменты (`golangci-lint`, `govulncheck`, `goimports`, `goreleaser`) ставит сам Task:

```bash
task tools:install
```

### Структура провайдеров

Работа с источниками TURN-реквизитов вынесена в отдельный каталог `providers/`:

```
providers/
├── provider.go          # интерфейс Provider и общие типы
├── multi/               # round-robin пул провайдеров
└── vk/                  # VK provider
    ├── vk.go            # фасад провайдера
    ├── manualsolver.go  # ручное решение captcha
    └── internal/
        ├── browserprofile/  # браузерные отпечатки (UA, client hints)
        ├── captcha/         # обход VK Smart Captcha (PoW, slider, телеметрия)
        │   └── manual/      # ручной режим через локальный прокси
        ├── namegen/         # генерация имён персон
        ├── personanet/      # TLS-клиент с отпечатком браузера
        └── vkauth/          # авторизация VK, кэш TURN-реквизитов
```

### Команды

```
task                # список доступных задач
task build          # собрать tots-client + tots-server в dist/ для текущего хоста
task build:all      # кросс-сборка всех target через goreleaser snapshot
task build:ios      # сборка XCFramework для iOS в dist/Mobile.xcframework
task build:android  # сборка AAR архива для Android в dist/tots.aar
task test           # go test -race
task test:cover     # тесты + покрытие -> cover.html
task lint           # golangci-lint
task fmt            # gofmt + goimports (форматирование)
task fmt:check      # проверить форматирование (используется в CI)
task vet            # go vet
task vuln           # govulncheck
task ci             # полный набор: fmt:check + vet + lint + test + vuln
task tidy           # go mod tidy
task clean          # удалить dist/, cover.out, cover.html
```

### Миграция с ToTS

Скрипт `scripts/migrate_to_tots.py` выполняет перенос VK-провайдера в `providers/`,
патчит Go-импорты и глобально переименовывает бренд:

```
python3 scripts/migrate_to_tots.py --dry-run   # показать план
python3 scripts/migrate_to_tots.py --apply     # применить
```

## Документация

- [Быстрый старт](https://./docs/quickstart.md)
- [Режимы](https://./docs/modes.md)
- [Флаги](https://./docs/flags.md)
- [Развёртывание](https://./docs/deploy.md)
- [Мобильные устройства](https://./docs/mobile.md)
- [URI и форматы ссылок](https://./docs/uri.md)
- [Подписки (Subscriptions)](https://./docs/sub.md)
- [Провайдеры](https://./docs/providers.md)
- [Решение проблем](https://./docs/troubleshooting.md)

## Благодарности

Огромное спасибо за вклад и идеи:

- [@cacggghp](https://github.com/cacggghp)
- [@Moroka8](https://github.com/Moroka8)
- [@alxmcp](https://github.com/alxmcp)
- [@TheAirBlow](https://github.com/TheAirBlow)

И отдельная благодарность авторам исходного проекта
[ToTS](https://github.com/hackdiaz-dev/tots), на котором основан ToTS.

