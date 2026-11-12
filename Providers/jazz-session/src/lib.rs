#![doc = include_str!("../README.md")]

mod carrier;
mod error;
mod http;
mod invite;
mod rtc;
mod session;
mod snapshot;

pub use carrier::MediaCarrier;
pub use error::{Error, Result};
pub use http::HttpError;
pub use session::{CallSession, JazzDataPlane};

/// Создать гостевую встречу и получить её живую медиасессию.
pub async fn create_call(name: &str) -> Result<CallSession> {
    create_call_with_carrier(name, MediaCarrier::Audio).await
}

/// Opt-in media carrier; existing callers continue to use the audio default.
pub async fn create_call_with_carrier(name: &str, carrier: MediaCarrier) -> Result<CallSession> {
    let name = validate_name(name)?;
    let timeout = std::time::Duration::from_secs(30);
    let service = http::Service::new(
        url::Url::parse("https://salutejazz.ru/").expect("fixed origin"),
        timeout,
    )?;
    let created = service.create_room(name).await?;
    let link = created
        .get("url")
        .and_then(serde_json::Value::as_str)
        .ok_or(Error::Protocol("в ответе создания отсутствует ссылка"))?;
    let invite = invite::Invite::parse(link)
        .map_err(|_| Error::Protocol("неизвестный формат ссылки сервера"))?;
    let room_id = created
        .get("roomId")
        .or_else(|| created.get("room"))
        .and_then(serde_json::Value::as_str)
        .ok_or(Error::Protocol("отсутствует код комнаты"))?;
    let password = created
        .get("password")
        .and_then(serde_json::Value::as_str)
        .ok_or(Error::Protocol("отсутствует пароль комнаты"))?;
    if invite.room_id != room_id || invite.password != password {
        return Err(Error::Protocol("ссылка не соответствует созданной комнате"));
    }
    connect_invite(name, link, invite, service, timeout, carrier).await
}

/// Присоединиться к встрече по ссылке с выбранным именем.
pub async fn join_call(link: &str, name: &str) -> Result<CallSession> {
    join_call_with_carrier(link, name, MediaCarrier::Audio).await
}

pub async fn join_call_with_carrier(
    link: &str,
    name: &str,
    carrier: MediaCarrier,
) -> Result<CallSession> {
    let name = validate_name(name)?;
    let invite =
        invite::Invite::parse(link).map_err(|_| Error::InvalidInput("некорректная ссылка Jazz"))?;
    let timeout = std::time::Duration::from_secs(30);
    let service = http::Service::new(invite.origin.clone(), timeout)?;
    connect_invite(name, link, invite, service, timeout, carrier).await
}

/// Получить адрес сигнального сервера через HTTP без входа в медиасессию.
/// Возвращаемая строка содержит секрет подключения; передавайте её только участнику встречи.
pub async fn preconnect_call(link: &str) -> Result<String> {
    let invite =
        invite::Invite::parse(link).map_err(|_| Error::InvalidInput("некорректная ссылка Jazz"))?;
    let service = http::Service::new(invite.origin.clone(), std::time::Duration::from_secs(30))?;
    Ok(preconnect_invite(&invite, service).await?.to_string())
}

/// Войти напрямую в WebSocket и WebRTC по адресу, полученному через preconnect_call().
/// HTTP-запросы не выполняются. `connector` содержит секрет подключения.
pub async fn join_preconnected(link: &str, name: &str, connector: &str) -> Result<CallSession> {
    join_preconnected_with_carrier(link, name, connector, MediaCarrier::Audio).await
}

pub async fn join_preconnected_with_carrier(
    link: &str,
    name: &str,
    connector: &str,
    carrier: MediaCarrier,
) -> Result<CallSession> {
    let name = validate_name(name)?;
    let invite =
        invite::Invite::parse(link).map_err(|_| Error::InvalidInput("некорректная ссылка Jazz"))?;
    establish_invite(
        name,
        link,
        invite,
        parse_connector(connector)?,
        std::time::Duration::from_secs(30),
        carrier,
    )
    .await
}

fn validate_name(name: &str) -> Result<&str> {
    if name.trim().is_empty() || name.chars().count() > 128 || name.chars().any(char::is_control) {
        return Err(Error::InvalidInput(
            "имя должно содержать 1–128 символов без управляющих знаков",
        ));
    }
    Ok(name.trim())
}

async fn connect_invite(
    name: &str,
    link: &str,
    invite: invite::Invite,
    service: http::Service,
    timeout: std::time::Duration,
    carrier: MediaCarrier,
) -> Result<CallSession> {
    let connector = preconnect_invite(&invite, service).await?;
    establish_invite(name, link, invite, connector, timeout, carrier).await
}

async fn preconnect_invite(invite: &invite::Invite, service: http::Service) -> Result<url::Url> {
    let response = service
        .preconnect(&invite.room_id, &invite.password)
        .await?;
    let connector = response
        .get("connectorUrl")
        .and_then(serde_json::Value::as_str)
        .ok_or(Error::Protocol(
            "сервер не выдал connectorUrl; legacy-комнаты не поддерживаются",
        ))?;
    parse_connector(connector)
}

fn parse_connector(connector: &str) -> Result<url::Url> {
    let mut connector_url =
        url::Url::parse(connector).map_err(|_| Error::Protocol("некорректный connectorUrl"))?;
    if connector_url.scheme() == "https" {
        connector_url
            .set_scheme("wss")
            .map_err(|_| Error::Protocol("некорректный connectorUrl"))?;
    }
    if connector_url.scheme() != "wss"
        || connector_url.host_str().is_none()
        || !connector_url.username().is_empty()
        || connector_url.password().is_some()
        || connector_url.fragment().is_some()
    {
        return Err(Error::Protocol("недопустимый сигнальный адрес"));
    }
    Ok(connector_url)
}

async fn establish_invite(
    name: &str,
    link: &str,
    invite: invite::Invite,
    connector_url: url::Url,
    timeout: std::time::Duration,
    carrier: MediaCarrier,
) -> Result<CallSession> {
    session::establish(
        session::CallInfo {
            name: name.to_owned(),
            room_id: invite.room_id,
            password: invite.password,
            join_url: link.to_owned(),
            connector_url,
            carrier,
        },
        timeout,
    )
    .await
}

#[cfg(test)]
mod api_tests {
    use super::*;
    const INVITE: &str = "https://salutejazz.ru/calls/tool-5g1c28?psw=OAMGUUtdChkGEwFHFEkdCgkZHA";

    #[tokio::test]
    async fn preconnected_join_rejects_invalid_names_before_networking() {
        let too_long = "я".repeat(129);
        for name in ["", "   ", "name\nspoof", "name\0", too_long.as_str()] {
            let result = join_preconnected(INVITE, name, "wss://127.0.0.1:1/?token=secret").await;
            assert!(matches!(result, Err(Error::InvalidInput(_))));
        }
    }

    #[tokio::test]
    async fn provisioning_apis_reject_invalid_invites_without_echoing_secrets() {
        let error = preconnect_call("file:///secret-password")
            .await
            .unwrap_err();
        assert!(matches!(error, Error::InvalidInput(_)));
        assert!(!format!("{error:?} {error}").contains("secret-password"));
        let error = join_preconnected(
            "file:///secret-password",
            "Имя",
            "wss://127.0.0.1:1/?token=secret",
        )
        .await
        .unwrap_err();
        assert!(matches!(error, Error::InvalidInput(_)));
        assert!(!format!("{error:?} {error}").contains("secret"));
    }

    #[tokio::test]
    async fn preconnected_join_rejects_invalid_connectors_without_echoing_secrets() {
        for connector in [
            "secret-password",
            "http://127.0.0.1:1/?token=secret",
            "ws://127.0.0.1:1/?token=secret",
            "file:///secret-password",
            "wss://user:secret@127.0.0.1:1/",
            "wss://127.0.0.1:1/#secret",
        ] {
            let error = join_preconnected(INVITE, "Имя", connector)
                .await
                .unwrap_err();
            assert!(matches!(error, Error::Protocol(_)));
            assert!(!format!("{error:?} {error}").contains("secret"));
        }
    }

    #[test]
    fn provisioned_connector_preserves_tokens_and_normalizes_https() {
        for scheme in ["https", "wss"] {
            let connector = format!("{scheme}://ws.salutejazz.ru/path?token=secret");
            assert_eq!(
                parse_connector(&connector).unwrap().as_str(),
                "wss://ws.salutejazz.ru/path?token=secret",
            );
        }
    }

    #[tokio::test]
    async fn invalid_names_are_rejected_before_any_network_operation() {
        let too_long = "я".repeat(129);
        for name in ["", "   ", "name\nspoof", "name\0", too_long.as_str()] {
            assert!(matches!(
                create_call(name).await,
                Err(Error::InvalidInput(_))
            ));
            assert!(matches!(
                join_call(
                    "https://salutejazz.ru/calls/tool-5g1c28?psw=OAMGUUtdChkGEwFHFEkdCgkZHA",
                    name
                )
                .await,
                Err(Error::InvalidInput(_))
            ));
        }
    }
    #[tokio::test]
    async fn invalid_links_are_rejected_without_echoing_the_link() {
        let error = match join_call("file:///secret-password", "Имя").await {
            Err(error) => error,
            Ok(_) => panic!("invalid link accepted"),
        };
        assert!(matches!(error, Error::InvalidInput(_)));
        assert!(!format!("{error:?}").contains("secret-password"));
    }
}
