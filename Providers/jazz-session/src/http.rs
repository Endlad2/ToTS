use std::{fmt, sync::Arc, time::Duration};

use serde_json::{json, Value};
use tokio::sync::OnceCell;
use url::Url;

#[derive(thiserror::Error)]
pub enum HttpError {
    #[error("Сетевой HTTP-запрос не выполнен")]
    Network(#[source] reqwest::Error),
    #[error("HTTP-запрос вернул статус {0}")]
    Status(u16),
    #[error("Некорректный HTTP-ответ: {0}")]
    InvalidResponse(&'static str),
}

impl fmt::Debug for HttpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Clone)]
pub(crate) struct Service {
    origin: Url,
    client: reqwest::Client,
    backend: Arc<OnceCell<Url>>,
    allow_loopback_http: bool,
}

impl Service {
    pub(crate) fn new(origin: Url, timeout: Duration) -> Result<Self, HttpError> {
        Self::build(origin, timeout, false)
    }

    #[cfg(test)]
    fn for_test(origin: Url, timeout: Duration) -> Self {
        Self::build(origin, timeout, true).unwrap()
    }

    fn build(origin: Url, timeout: Duration, allow_loopback_http: bool) -> Result<Self, HttpError> {
        validate_url(&origin, allow_loopback_http)?;
        if origin.path() != "/" || timeout.is_zero() {
            return Err(HttpError::InvalidResponse(
                "недопустимый адрес сервиса или таймаут",
            ));
        }
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "X-Client-AuthType",
            reqwest::header::HeaderValue::from_static("ANONYMOUS"),
        );
        headers.insert(
            "X-Jazz-AuthType",
            reqwest::header::HeaderValue::from_static("ANONYMOUS"),
        );
        headers.insert(
            "X-Jazz-ClientId",
            uuid::Uuid::new_v4()
                .to_string()
                .parse()
                .expect("UUID header"),
        );
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .user_agent("jazz-session/0.1")
            .timeout(timeout)
            .connect_timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .https_only(!allow_loopback_http)
            .build()
            .map_err(network_error)?;
        Ok(Self {
            origin,
            client,
            backend: Arc::new(OnceCell::new()),
            allow_loopback_http,
        })
    }

    async fn backend(&self) -> Result<&Url, HttpError> {
        self.backend
            .get_or_try_init(|| async {
                let mut last_error = HttpError::InvalidResponse("адрес сервера не найден");
                for path in [
                    "/.well-known/s2b-services.json",
                    "/.well-known/jazz-services.json",
                ] {
                    let url = self
                        .origin
                        .join(path)
                        .map_err(|_| HttpError::InvalidResponse("недопустимый адрес discovery"))?;
                    let response = match self.client.get(url).send().await {
                        Ok(response) => response,
                        Err(error) => {
                            last_error = network_error(error);
                            continue;
                        }
                    };
                    if !response.status().is_success() {
                        last_error = HttpError::Status(response.status().as_u16());
                        continue;
                    }
                    let value = response
                        .json::<Value>()
                        .await
                        .map_err(|_| HttpError::InvalidResponse("discovery не содержит JSON"))?;
                    let server = value
                        .pointer("/jazz/serverUrl")
                        .and_then(Value::as_str)
                        .ok_or(HttpError::InvalidResponse(
                            "в discovery отсутствует jazz.serverUrl",
                        ))?;
                    let url = Url::parse(server)
                        .map_err(|_| HttpError::InvalidResponse("недопустимый jazz.serverUrl"))?;
                    validate_url(&url, self.allow_loopback_http)?;
                    return Ok(url);
                }
                Err(last_error)
            })
            .await
    }

    async fn post(
        &self,
        segments: &[&str],
        body: &Value,
        create: bool,
    ) -> Result<Value, HttpError> {
        let mut url = self.backend().await?.clone();
        url.path_segments_mut()
            .map_err(|_| HttpError::InvalidResponse("недопустимый адрес сервера"))?
            .pop_if_empty()
            .extend(segments.iter().copied());
        let mut request = self.client.post(url).json(body);
        if create {
            request = request.header("X-Jazz-Token", "");
        }
        // A failed create may already have created the room; never replay this request.
        let response = request.send().await.map_err(network_error)?;
        if !response.status().is_success() {
            return Err(HttpError::Status(response.status().as_u16()));
        }
        let value = response
            .json::<Value>()
            .await
            .map_err(|_| HttpError::InvalidResponse("ответ не содержит JSON"))?;
        if !value.is_object() {
            return Err(HttpError::InvalidResponse("ожидался объект JSON"));
        }
        Ok(value)
    }

    pub(crate) async fn create_room(&self, name: &str) -> Result<Value, HttpError> {
        self.post(
            &["room", "create-meeting"],
            &json!({"title":name,"guestEnabled":true}),
            true,
        )
        .await
    }

    pub(crate) async fn preconnect(
        &self,
        room_id: &str,
        password: &str,
    ) -> Result<Value, HttpError> {
        if room_id.is_empty() || room_id == "." || room_id == ".." {
            return Err(HttpError::InvalidResponse("недопустимый код конференции"));
        }
        self.post(
            &["room", room_id, "preconnect"],
            &json!({"password":password,"jazzNextMigration":{
                "b2bBaseRoomSupport":true,"demoRoomBaseSupport":true,"demoRoomVersionSupport":2,
                "mediaWithoutAutoSubscribeSupport":true,"webinarSpeakerSupport":false,
                "webinarViewerSupport":false,"sdkRoomSupport":true,"sberclassRoomSupport":false
            }}),
            false,
        )
        .await
    }
}

fn network_error(error: reqwest::Error) -> HttpError {
    HttpError::Network(error.without_url())
}

fn validate_url(url: &Url, allow_loopback_http: bool) -> Result<(), HttpError> {
    let loopback_http = allow_loopback_http
        && url.scheme() == "http"
        && url
            .host_str()
            .and_then(|host| host.parse::<std::net::IpAddr>().ok())
            .is_some_and(|ip| ip.is_loopback());
    if (url.scheme() != "https" && !loopback_http)
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(HttpError::InvalidResponse(
            "разрешён только HTTPS без учётных данных, query и fragment",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Bytes,
        extract::State,
        http::{HeaderMap, Method, StatusCode, Uri},
        response::{IntoResponse, Response},
        routing::any,
        Json, Router,
    };
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug)]
    struct Recorded {
        method: Method,
        path: String,
        headers: HeaderMap,
        body: Value,
    }

    #[derive(Clone)]
    struct Mock {
        origin: String,
        requests: Arc<Mutex<Vec<Recorded>>>,
        mode: &'static str,
    }

    async fn handler(
        State(state): State<Mock>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        bytes: Bytes,
    ) -> Response {
        let path = uri.path();
        state.requests.lock().unwrap().push(Recorded {
            method,
            path: path.to_owned(),
            headers,
            body: serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        });
        if path.starts_with("/.well-known/") {
            if state.mode == "discovery-unsafe" {
                return Json(json!({"jazz":{"serverUrl":"http://example.com"}})).into_response();
            }
            if state.mode == "discovery-invalid" {
                return Json(json!({"jazz":{}})).into_response();
            }
            if path.ends_with("s2b-services.json") {
                return StatusCode::NOT_FOUND.into_response();
            }
            return Json(json!({"jazz":{"serverUrl":format!("{}/api", state.origin)}}))
                .into_response();
        }
        if state.mode == "timeout" {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        if state.mode == "status" {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "secret-password-and-server-response",
            )
                .into_response();
        }
        if state.mode == "invalid-json" {
            return (StatusCode::OK, "secret-password-and-server-response").into_response();
        }
        if path == "/api/room/create-meeting" {
            return Json(json!({"roomId":"abc123", "password":"secret-password", "url":"https://salutejazz.ru/calls/abc123?psw=encoded"})).into_response();
        }
        if path == "/api/room/abc123/preconnect" {
            return Json(json!({"connectorUrl":"wss://ws.salutejazz.ru/test", "preconnectPermissions":{"canShareMicrophone":true}})).into_response();
        }
        StatusCode::NOT_FOUND.into_response()
    }

    async fn mock(
        mode: &'static str,
        timeout: Duration,
    ) -> (Service, Mock, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let state = Mock {
            origin: format!("http://{}", listener.local_addr().unwrap()),
            requests: Arc::default(),
            mode,
        };
        let app = Router::new()
            .fallback(any(handler))
            .with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let service = Service::for_test(Url::parse(&state.origin).unwrap(), timeout);
        (service, state, task)
    }

    #[tokio::test]
    async fn discovers_backend_and_sends_current_anonymous_wire() {
        let (service, state, task) = mock("ok", Duration::from_secs(2)).await;
        let created = service.create_room("Rust test").await.unwrap();
        assert_eq!(created["roomId"], "abc123");
        let connected = service
            .clone()
            .preconnect("abc123", "secret-password")
            .await
            .unwrap();
        assert!(connected["connectorUrl"].is_string());
        let requests = state.requests.lock().unwrap();
        assert_eq!(requests[0].path, "/.well-known/s2b-services.json");
        assert_eq!(requests[1].path, "/.well-known/jazz-services.json");
        let create = requests
            .iter()
            .find(|r| r.path == "/api/room/create-meeting")
            .unwrap();
        let preconnect = requests
            .iter()
            .find(|r| r.path.ends_with("/preconnect"))
            .unwrap();
        assert_eq!(create.method, Method::POST);
        assert_eq!(
            create.body,
            json!({"title":"Rust test", "guestEnabled":true})
        );
        assert_eq!(
            preconnect.body,
            json!({"password":"secret-password", "jazzNextMigration":{
                "b2bBaseRoomSupport":true, "demoRoomBaseSupport":true, "demoRoomVersionSupport":2,
                "mediaWithoutAutoSubscribeSupport":true, "webinarSpeakerSupport":false,
                "webinarViewerSupport":false, "sdkRoomSupport":true, "sberclassRoomSupport":false
            }})
        );
        for request in [create, preconnect] {
            assert_eq!(request.headers["x-jazz-authtype"], "ANONYMOUS");
            assert_eq!(request.headers["x-client-authtype"], "ANONYMOUS");
            assert!(!request.headers.contains_key("authorization"));
            assert!(request.headers["content-type"]
                .to_str()
                .unwrap()
                .starts_with("application/json"));
        }
        assert_eq!(create.headers["x-jazz-token"], "");
        let client_id = create.headers["x-jazz-clientid"].to_str().unwrap();
        assert!(uuid::Uuid::parse_str(client_id).is_ok());
        assert_eq!(preconnect.headers["x-jazz-clientid"], client_id);
        task.abort();
    }

    #[tokio::test]
    async fn failed_create_is_not_retried_and_does_not_expose_response() {
        let (service, state, task) = mock("status", Duration::from_secs(2)).await;
        let error = service.create_room("private-name").await.unwrap_err();
        assert!(matches!(error, HttpError::Status(503)));
        assert!(!format!("{error:?} {error}").contains("secret"));
        assert_eq!(
            state
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.path.ends_with("create-meeting"))
                .count(),
            1
        );
        task.abort();
    }

    #[tokio::test]
    async fn timeout_is_bounded_and_create_is_not_retried() {
        let (service, state, task) = mock("timeout", Duration::from_millis(40)).await;
        let error = service.create_room("private-name").await.unwrap_err();
        assert!(matches!(error, HttpError::Network(_)));
        assert!(!format!("{error:?} {error}").contains(&state.origin));
        assert_eq!(
            state
                .requests
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r.path.ends_with("create-meeting"))
                .count(),
            1
        );
        task.abort();
    }

    #[tokio::test]
    async fn invalid_json_does_not_expose_response() {
        let (service, _, task) = mock("invalid-json", Duration::from_secs(2)).await;
        let error = service.create_room("private-name").await.unwrap_err();
        assert!(matches!(error, HttpError::InvalidResponse(_)));
        assert!(!format!("{error:?} {error}").contains("secret"));
        task.abort();
    }

    #[tokio::test]
    async fn rejects_malformed_and_insecure_discovered_backends() {
        for mode in ["discovery-invalid", "discovery-unsafe"] {
            let (service, state, task) = mock(mode, Duration::from_secs(2)).await;
            assert!(matches!(
                service.create_room("private-name").await,
                Err(HttpError::InvalidResponse(_))
            ));
            assert!(state
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|r| r.path.starts_with("/.well-known/")));
            task.abort();
        }
    }

    #[test]
    fn production_accepts_https_and_rejects_insecure_or_credential_urls() {
        assert!(Service::new(
            Url::parse("https://salutejazz.ru").unwrap(),
            Duration::from_secs(2)
        )
        .is_ok());
        for url in [
            "http://salutejazz.ru",
            "https://username:secret@salutejazz.ru",
            "https://salutejazz.ru/path?secret=value",
        ] {
            assert!(Service::new(Url::parse(url).unwrap(), Duration::from_secs(2)).is_err());
        }
        assert!(
            Service::new(Url::parse("https://salutejazz.ru").unwrap(), Duration::ZERO).is_err()
        );
    }
}
