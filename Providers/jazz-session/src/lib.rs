#![doc = include_str!("../README.md")]

mod error;
mod http;
mod invite;
mod rtc;
mod session;
mod snapshot;

pub use error::{Error, Result};
pub use http::HttpError;
pub use session::{CallSession, JazzDataPlane};

/// Создать гостевую встречу и получить её живую медиасессию.
pub async fn create_call(name: &str) -> Result<CallSession> {
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
    connect_invite(name, link, invite, service, timeout).await
}

/// Присоединиться к встрече по ссылке с выбранным именем.
pub async fn join_call(link: &str, name: &str) -> Result<CallSession> {
    let name = validate_name(name)?;
    let invite =
        invite::Invite::parse(link).map_err(|_| Error::InvalidInput("некорректная ссылка Jazz"))?;
    let timeout = std::time::Duration::from_secs(30);
    let service = http::Service::new(invite.origin.clone(), timeout)?;
    connect_invite(name, link, invite, service, timeout).await
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
) -> Result<CallSession> {
    let response = service
        .preconnect(&invite.room_id, &invite.password)
        .await?;
    let connector = response
        .get("connectorUrl")
        .and_then(serde_json::Value::as_str)
        .ok_or(Error::Protocol(
            "сервер не выдал connectorUrl; legacy-комнаты не поддерживаются",
        ))?;
    let mut connector_url =
        url::Url::parse(connector).map_err(|_| Error::Protocol("некорректный connectorUrl"))?;
    if connector_url.scheme() == "https" {
        connector_url
            .set_scheme("wss")
            .map_err(|_| Error::Protocol("некорректный connectorUrl"))?;
    }
    session::establish(
        session::CallInfo {
            name: name.to_owned(),
            room_id: invite.room_id,
            password: invite.password,
            join_url: link.to_owned(),
            connector_url,
        },
        timeout,
    )
    .await
}

#[cfg(test)]
mod api_tests {
    use super::*;
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
