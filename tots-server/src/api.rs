use crate::{bridge::NativeBridge, config::Args, db::Db};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tots_core::config::{ConnectionConfig, ProviderName};
use uuid::Uuid;

#[derive(Clone)]
pub struct ApiState {
    pub db: Db,
    pub args: Args,
    pub rooms: Arc<dashmap::DashMap<Uuid, NativeBridge>>,
    pub room_changes: Arc<tokio::sync::Mutex<()>>,
}
#[derive(Deserialize)]
pub struct CreateRequest {
    pub provider: String,
    pub provider_data: Value,
    pub password: String,
    pub device_id: Uuid,
    #[serde(default = "days")]
    pub valid_days: i64,
}
fn days() -> i64 {
    30
}
#[derive(Serialize)]
pub struct CreateResponse {
    pub uri: String,
    pub virtual_ip: String,
    pub expires_at: String,
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/configs", post(create))
        .route("/v1/configs/{id}", delete(revoke))
        .with_state(Arc::new(state))
}
fn auth(headers: &HeaderMap, s: &ApiState) -> Result<(), StatusCode> {
    let Some(expected) = &s.args.admin_token else {
        return Ok(());
    };
    let got = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if got == Some(expected.as_str()) {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}
async fn create(
    State(s): State<Arc<ApiState>>,
    headers: HeaderMap,
    Json(mut req): Json<CreateRequest>,
) -> Result<Json<CreateResponse>, (StatusCode, String)> {
    auth(&headers, &s).map_err(|c| (c, "unauthorized".into()))?;
    let provider = ProviderName::parse(&req.provider).map_err(bad)?;
    if req.password.len() < 8 {
        return Err((StatusCode::BAD_REQUEST, "password too short".into()));
    }
    let room = if provider == tots_core::config::ProviderName::Jazz {
        if !req.provider_data.is_object() {
            return Err((
                StatusCode::BAD_REQUEST,
                "provider_data must be an object".into(),
            ));
        }
        let room = if let Some(link) = req
            .provider_data
            .get("join_link")
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
        {
            tots_core::providers::jazz::join_room(link)
                .await
                .map_err(internal)?
        } else {
            tots_core::providers::jazz::create_room()
                .await
                .map_err(internal)?
        };
        let connector = tots_core::providers::jazz::preconnect(&room.join_link)
            .await
            .map_err(internal)?;
        req.provider_data["join_link"] = Value::String(room.join_link.clone());
        req.provider_data["connector_url"] = Value::String(connector);
        Some(room)
    } else {
        None
    };
    let cfg = ConnectionConfig {
        version: 1,
        peer: s.args.public_host.clone(),
        webrtc_port: s.args.webrtc_listen.port(),
        turn_port: s.args.turn_listen.port(),
        password: req.password.clone(),
        device_id: req.device_id,
        provider: provider.clone(),
        provider_data: req.provider_data.clone(),
        mtu: 1200,
    };
    cfg.validate().map_err(bad)?;
    let changes = s.room_changes.lock().await;
    let d =
        s.db.upsert(
            req.device_id,
            req.password,
            provider,
            req.provider_data,
            req.valid_days,
        )
        .await
        .map_err(internal)?;
    let previous = if let Some(room) = room {
        let bridge = NativeBridge::start(room.path, s.args.turn_listen)
            .await
            .map_err(internal)?;
        s.rooms.insert(req.device_id, bridge)
    } else {
        s.rooms.remove(&req.device_id).map(|(_, bridge)| bridge)
    };
    drop(changes);
    if let Some(previous) = previous {
        previous.close().await;
    }
    Ok(Json(CreateResponse {
        uri: cfg.encode_uri().map_err(bad)?,
        virtual_ip: cfg.virtual_ipv4().to_string(),
        expires_at: d.expires_at.to_rfc3339(),
    }))
}
async fn revoke(
    State(s): State<Arc<ApiState>>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, (StatusCode, String)> {
    auth(&headers, &s).map_err(|c| (c, "unauthorized".into()))?;
    let changes = s.room_changes.lock().await;
    let revoked = s.db.revoke(id).await.map_err(internal)?;
    let removed = if revoked { s.rooms.remove(&id) } else { None };
    drop(changes);
    if revoked {
        if let Some((_, bridge)) = removed {
            bridge.close().await;
        }
        Ok(StatusCode::NO_CONTENT)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}

pub async fn restore_rooms(state: ApiState) {
    match state.db.active_jazz().await {
        Ok(devices) => {
            for device in devices {
                let Some(link) = device
                    .provider_data
                    .get("join_link")
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                let restored = async {
                    let room = tots_core::providers::jazz::join_room(link).await?;
                    NativeBridge::start(room.path, state.args.turn_listen).await
                }
                .await;
                match restored {
                    Ok(bridge) => {
                        publish_restored_room(&state, device.device_id, link, bridge).await;
                    }
                    Err(error) => {
                        tracing::warn!(device_id=%device.device_id, %error, "Jazz room restore failed")
                    }
                }
            }
        }
        Err(error) => tracing::warn!(%error, "read persisted Jazz rooms failed"),
    }
    let mut timer = tokio::time::interval(std::time::Duration::from_secs(60));
    loop {
        timer.tick().await;
        let ids: Vec<_> = state.rooms.iter().map(|entry| *entry.key()).collect();
        for id in ids {
            let changes = state.room_changes.lock().await;
            let remove = match state.db.get(id).await {
                Ok(Some(device)) => {
                    device.revoked
                        || device.expires_at < chrono::Utc::now()
                        || device.provider != ProviderName::Jazz
                }
                Ok(None) => true,
                Err(_) => false,
            };
            let removed = if remove {
                state.rooms.remove(&id)
            } else {
                None
            };
            drop(changes);
            if let Some((_, bridge)) = removed {
                bridge.close().await;
            }
        }
    }
}

async fn publish_restored_room(state: &ApiState, id: Uuid, link: &str, bridge: NativeBridge) {
    let changes = state.room_changes.lock().await;
    let current = state.db.get(id).await.ok().flatten();
    let active = current.is_some_and(|current| {
        !current.revoked
            && current.provider == ProviderName::Jazz
            && current.expires_at >= chrono::Utc::now()
            && current
                .provider_data
                .get("join_link")
                .and_then(Value::as_str)
                == Some(link)
    });
    let unused = if active {
        match state.rooms.entry(id) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(bridge);
                None
            }
            dashmap::mapref::entry::Entry::Occupied(_) => Some(bridge),
        }
    } else {
        Some(bridge)
    };
    drop(changes);
    if let Some(bridge) = unused {
        bridge.close().await;
    }
}

fn bad<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, e.to_string())
}
fn internal<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{path::PathBuf, sync::Weak, time::Duration};
    use tokio::net::UdpSocket;
    use tots_core::transport::DirectUdpPath;

    const OLD_LINK: &str = "https://salutejazz.ru/calls/old-room?psw=test";
    const NEW_LINK: &str = "https://salutejazz.ru/calls/new-room?psw=test";

    struct Fixture {
        state: Arc<ApiState>,
        endpoint: UdpSocket,
        db_path: PathBuf,
    }

    impl Fixture {
        async fn new() -> Self {
            let db_path = std::env::temp_dir().join(format!("tots-api-{}.db", Uuid::new_v4()));
            let db = Db::open(&db_path).await.unwrap();
            let endpoint = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let args = Args {
                db: db_path.clone(),
                api_listen: "127.0.0.1:0".parse().unwrap(),
                webrtc_listen: "127.0.0.1:7800".parse().unwrap(),
                turn_listen: endpoint.local_addr().unwrap(),
                public_host: "203.0.113.2".into(),
                tun_name: "unused-test-tun".into(),
                admin_token: None,
                no_tun: true,
            };
            Self {
                state: Arc::new(ApiState {
                    db,
                    args,
                    rooms: Arc::new(dashmap::DashMap::new()),
                    room_changes: Arc::new(tokio::sync::Mutex::new(())),
                }),
                endpoint,
                db_path,
            }
        }

        async fn seed(&self, id: Uuid, provider: ProviderName, link: &str) {
            self.state
                .db
                .upsert(
                    id,
                    "test-password".into(),
                    provider,
                    json!({"join_link":link, "vk_call_links":["test"]}),
                    30,
                )
                .await
                .unwrap();
        }

        async fn bridge(&self) -> (NativeBridge, Weak<DirectUdpPath>) {
            let address = self.endpoint.local_addr().unwrap();
            let path = Arc::new(
                DirectUdpPath::connect("offline-test", address)
                    .await
                    .unwrap(),
            );
            let weak = Arc::downgrade(&path);
            (NativeBridge::start(path, address).await.unwrap(), weak)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let mut path = self.db_path.as_os_str().to_os_string();
                path.push(suffix);
                let _ = std::fs::remove_file(path);
            }
        }
    }

    async fn assert_released(path: Weak<DirectUdpPath>) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while path.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("closed bridge retained its transport task");
    }

    #[tokio::test]
    async fn changing_jazz_to_vk_removes_and_closes_the_old_bridge() {
        let fixture = Fixture::new().await;
        let id = Uuid::new_v4();
        fixture.seed(id, ProviderName::Jazz, OLD_LINK).await;
        let (bridge, path) = fixture.bridge().await;
        fixture.state.rooms.insert(id, bridge);

        let Json(response) = create(
            State(fixture.state.clone()),
            HeaderMap::new(),
            Json(CreateRequest {
                provider: "vk".into(),
                provider_data: json!({"vk_call_links":["test"]}),
                password: "new-password".into(),
                device_id: id,
                valid_days: 30,
            }),
        )
        .await
        .unwrap();

        assert!(fixture.state.rooms.is_empty());
        assert_eq!(
            ConnectionConfig::decode_uri(&response.uri)
                .unwrap()
                .provider,
            ProviderName::Vk
        );
        assert_eq!(
            fixture.state.db.get(id).await.unwrap().unwrap().provider,
            ProviderName::Vk
        );
        assert_released(path).await;
    }

    #[tokio::test]
    async fn revoking_jazz_removes_and_closes_its_bridge() {
        let fixture = Fixture::new().await;
        let id = Uuid::new_v4();
        fixture.seed(id, ProviderName::Jazz, OLD_LINK).await;
        let (bridge, path) = fixture.bridge().await;
        fixture.state.rooms.insert(id, bridge);

        let status = revoke(State(fixture.state.clone()), HeaderMap::new(), Path(id))
            .await
            .unwrap();

        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(fixture.state.rooms.is_empty());
        assert!(fixture.state.db.get(id).await.unwrap().unwrap().revoked);
        assert_released(path).await;
    }

    #[tokio::test]
    async fn stale_and_duplicate_restores_leave_the_current_bridge_alive() {
        let fixture = Fixture::new().await;
        let id = Uuid::new_v4();
        fixture.seed(id, ProviderName::Jazz, NEW_LINK).await;
        let (current, current_path) = fixture.bridge().await;
        fixture.state.rooms.insert(id, current);

        for link in [OLD_LINK, NEW_LINK] {
            let (restored, unused_path) = fixture.bridge().await;
            publish_restored_room(&fixture.state, id, link, restored).await;
            assert_eq!(fixture.state.rooms.len(), 1);
            assert!(current_path.upgrade().is_some());
            assert_released(unused_path).await;
        }
    }

    #[tokio::test]
    async fn stale_link_is_not_restored_even_when_no_room_is_published() {
        let fixture = Fixture::new().await;
        let id = Uuid::new_v4();
        fixture.seed(id, ProviderName::Jazz, NEW_LINK).await;
        let (restored, path) = fixture.bridge().await;

        publish_restored_room(&fixture.state, id, OLD_LINK, restored).await;

        assert!(fixture.state.rooms.is_empty());
        assert_released(path).await;
    }

    #[tokio::test]
    async fn matching_active_jazz_room_can_be_restored() {
        let fixture = Fixture::new().await;
        let id = Uuid::new_v4();
        fixture.seed(id, ProviderName::Jazz, OLD_LINK).await;
        let (restored, path) = fixture.bridge().await;

        publish_restored_room(&fixture.state, id, OLD_LINK, restored).await;

        assert!(fixture.state.rooms.contains_key(&id));
        assert!(path.upgrade().is_some());
    }

    #[tokio::test]
    async fn revoked_or_non_jazz_configs_cannot_publish_a_restored_room() {
        for provider in [ProviderName::Jazz, ProviderName::Vk] {
            let fixture = Fixture::new().await;
            let id = Uuid::new_v4();
            fixture.seed(id, provider.clone(), OLD_LINK).await;
            if provider == ProviderName::Jazz {
                fixture.state.db.revoke(id).await.unwrap();
            }
            let (restored, path) = fixture.bridge().await;

            publish_restored_room(&fixture.state, id, OLD_LINK, restored).await;

            assert!(
                fixture.state.rooms.is_empty(),
                "published inactive {provider:?} room"
            );
            assert_released(path).await;
        }
    }
}
