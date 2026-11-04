# jazz-session

Rust-библиотека для создания гостевой встречи SaluteJazz и входа по ссылке.
Возвращает живую сессию и JSON с приглашением, TURN-кредами, полными SDP,
ICE-кандидатами и параметрами ICE/DTLS для publisher и subscriber.

Это самостоятельная реализация гостевого протокола, без браузера, Node.js,
официального SDK и ключа SDK. Камера и микрофон не открываются. Для переговоров
publisher создаётся локальная выключенная Opus-дорожка; звук с устройства не
захватывается и RTP с аудио не отправляется.

## Подключение

Распакуйте исходники рядом со своим приложением:

```toml
[dependencies]
jazz-session = { path = "../jazz-session" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde_json = "1"
```

Крейт пока не опубликован на crates.io. В Cargo.toml указан Rust 1.89;
проверки выполнены на Rust 1.99.1, Linux x86_64 и Windows x86_64 MSVC.
Cargo.lock включён для воспроизводимой сборки примеров и проверок.

## API

```rust,no_run
use jazz_session::{create_call, join_call};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let creator = create_call("Саша").await?;
    let data = creator.json().await;
    let link = data["call"]["join_url"].as_str().expect("ссылка встречи");

    let guest = match join_call(link, "Второй участник").await {
        Ok(session) => session,
        Err(error) => {
            let _ = creator.close().await;
            return Err(error.into());
        }
    };
    let guest_data = guest.json().await;
    let _json_string = serde_json::to_string(&guest_data)?;

    // Сохраните оба объекта, пока используете соответствующие медиасессии.
    let guest_close = guest.close().await;
    let creator_close = creator.close().await;
    guest_close?;
    creator_close?;
    Ok(())
}
```

- `create_call(name: &str) -> Result<CallSession>` создаёт гостевую встречу,
  входит под указанным именем и дожидается SDP обоих WebRTC-соединений и TURN-конфига.
- `join_call(link: &str, name: &str) -> Result<CallSession>` делает тот же вход
  в существующую встречу по HTTPS-приглашению с `psw`.
- `session.json().await -> serde_json::Value` возвращает текущий снимок.
- `session.close().await -> Result<()>` отправляет `leave` и закрывает
  WebSocket, publisher и subscriber. Метод потребляет объект сессии.
- `Drop` инициирует такое же завершение в фоне. Для гарантированного ожидания
  завершения вызывайте `close()` до остановки Tokio runtime.

Непустое имя ограничено 128 символами, управляющие символы запрещены.
Неподдерживаемая ссылка и некорректное имя отклоняются до обращения к сети.
Сетевые этапы ограничены таймаутами по 30 секунд; это не общий таймаут всей функции.
Доступны варианты `Error::InvalidInput`, `Http`, `WebSocket`, `Media`,
`Protocol` и `Timeout`.

## Что находится в JSON

| Путь | Содержимое |
| --- | --- |
| `schema_version` | Версия схемы, сейчас `1` |
| `status` | `ready`, после завершения фоновой задачи — `closed` или `failed` |
| `call.name`, `call.room_id` | Имя участника и код встречи |
| `call.join_url`, `call.password` | Рабочая ссылка и декодированный пароль встречи |
| `session` | Ответ `join-response`: участник, sessionId, participantId, группа и meetingId |
| `rtc_join` | Исходные параметры `rtc:join`, если сервер их прислал |
| `webrtc.ice_servers` | Исходная ICE-конфигурация: STUN/TURN URLs, username, credential |
| `webrtc.publisher`, `webrtc.subscriber` | Снимки двух настоящих PeerConnection |

В каждом PeerConnection есть `local_description`, `remote_description`
с `type` и **полным исходным `sdp`**, `local_ice_candidates`,
`remote_ice_candidates` и `connection_state`.

К каждому SDP добавлен `transport_parameters`: поля уровня `session` и массив
`media`. Для каждой медиасекции раскрыты `ice_ufrag`, `ice_password`,
`fingerprints` с алгоритмом и значением, `dtls_setup`, `mid`, `kind`,
`protocol`, `direction`, если они присутствуют в SDP. Параметры уровня сессии
наследуются медиасекциями по правилам SDP. Остальные параметры, включая
кодеки, RTP/RTCP и extensions, сохранены в исходном SDP.

При возврате функции все четыре SDP и TURN-креды уже получены. `ready` означает
завершение начальных переговоров; ICE/DTLS ещё может подключаться.
Trickle ICE продолжает поступать, поэтому повторный `json()` обновляет
кандидаты и состояния. Сохраняйте объект сессии: JSON описывает именно его
соединения и не переносит DTLS-ключи и состояние PeerConnection в другой процесс.

JSON содержит секреты подключения. Библиотека не печатает их в логи,
`Debug` сессии скрывает данные, ошибки не включают тела серверных ответов.
Сериализованный JSON передавайте только туда, где эти данные нужны.

## Примеры и проверки

```sh
cargo test --locked --all-targets
cargo test --locked --doc
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
```

Обычные тесты работают локально и не создают встречи на внешнем сервисе.
Проверяются HTTP-запросы через локальный сервер, настоящие локальные WebRTC
переговоры, SDP/ICE/TURN, ошибки, таймауты и закрытие сессий.

Интерактивный пример **явно печатает полный JSON с секретами** и держит
соединение до Ctrl+C:

```sh
cargo run --locked --example session -- create "Саша"
cargo run --locked --example session -- join "Гость" "ССЫЛКА_ПРИГЛАШЕНИЕ"
```

Живая проверка создаёт одну встречу и входит вторым участником. Печатает
только счётчики и состояния, затем закрывает обоих участников:

```sh
# Linux/macOS
JAZZ_LIVE_TEST=1 cargo run --locked --example live_check
```

```powershell
# PowerShell
$env:JAZZ_LIVE_TEST = "1"
cargo run --locked --example live_check
```

Дополнительная переменная `JAZZ_PRIVATE_OUTPUT_DIR` сохраняет полные снимки
в два новых файла `*.private.json`; без неё снимки на диск не пишутся.
На Unix файлы создаются с правами `0600`. На Windows применяются ACL каталога.

## Границы реализации

Поддерживается текущий гостевой протокол SaluteJazz. Это не официальный API
со стабильным контрактом: изменение протокола сервиса может потребовать правок.
Гостевой доступ должен быть разрешён; авторизация аккаунтом, комнаты ожидания,
старый signaling без `connectorUrl`, передача звука/видео и VPN не реализованы.
При изменении сервером ICE/TURN-конфигурации активная сессия завершится с
понятной ошибкой: для новых credentials требуется повторный `join_call`.
Автоматического переподключения нет.

Лицензия исходников — MIT. Материалы из предоставленного архива приложения
не включены в поставку.
