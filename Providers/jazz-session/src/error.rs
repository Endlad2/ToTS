/// Ошибки не содержат SDP, TURN-пароли и ссылки приглашений.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Некорректный аргумент: {0}")]
    InvalidInput(&'static str),
    #[error(transparent)]
    Http(#[from] crate::http::HttpError),
    #[error("Сбой сигнального WebSocket")]
    WebSocket,
    #[error("Ошибка WebRTC")]
    Media,
    #[error("Ошибка протокола Jazz: {0}")]
    Protocol(&'static str),
    #[error("Истёк таймаут: {0}")]
    Timeout(&'static str),
}

impl From<crate::rtc::MediaError> for Error {
    fn from(_: crate::rtc::MediaError) -> Self {
        Self::Media
    }
}

pub type Result<T> = std::result::Result<T, Error>;
