use crate::{bridge::NativeBridge, config::Args, db::Db};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;
use tots_core::config::{ConnectionConfig, ProviderName};
use uuid::Uuid;

#[derive(Clone)]
pub struct ApiState {
    pub db: Db,
    pub args: Args,
    pub rooms: Arc<dashmap::DashMap<Uuid, Vec<NativeBridge>>>,
    pub room_changes: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Deserialize)]
pub struct CreateRequest {
    /// Preferred provider. provider_data can contain additional fallbacks.
    pub provider: String,
    /// New layout: {"vk": {...}, "jazz": {...}}.
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
    Json(req): Json<CreateRequest>,
) -> Result<Json<CreateResponse>, (StatusCode, String)> {
    auth(&headers, &s).map_err(|c| (c, "unauthorized".into()))?;
    let provider = ProviderName::parse(&req.provider).map_err(bad)?;
    if req.password.len() < 8 {
        return Err((StatusCode::BAD_REQUEST, "password too short".into()));
    }
    let mut provider_data = normalize_provider_data(provider, req.provider_data).map_err(bad)?;

    // A separate publisher per worker avoids room-wide quadratic media fanout.
    let bridges = if let Some(jazz) = provider_data.get_mut("jazz") {
        let carrier = tots_core::providers::jazz::media_carrier(jazz).map_err(bad)?;
        let links = requested_room_links(jazz).map_err(bad)?;
        let target = s.args.turn_listen;
        let (rooms, bridges) = build_room_pool(links.len(), move |index| {
            let link = links[index].clone();
            async move {
                let room = match link {
                    Some(link) => {
                        tots_core::providers::jazz::join_room_with_carrier(&link, carrier).await?
                    }
                    None => tots_core::providers::jazz::create_room_with_carrier(carrier).await?,
                };
                let connector = match tots_core::providers::jazz::preconnect(&room.join_link).await
                {
                    Ok(connector) => connector,
                    Err(error) => {
                        let _ = room.path.close().await;
                        return Err(error.into());
                    }
                };
                let bridge = match NativeBridge::start(room.path.clone(), target).await {
                    Ok(bridge) => bridge,
                    Err(error) => {
                        let _ = room.path.close().await;
                        return Err(error);
                    }
                };
                Ok((
                    serde_json::json!({"join_link":room.join_link,"connector_url":connector}),
                    bridge,
                ))
            }
        })
        .await
        .map_err(internal)?;
        let jazz = jazz
            .as_object_mut()
            .expect("Jazz data validated before provisioning");
        jazz.insert("join_link".into(), rooms[0]["join_link"].clone());
        jazz.insert("connector_url".into(), rooms[0]["connector_url"].clone());
        jazz.insert("worker_count".into(), Value::from(rooms.len()));
        jazz.insert("worker_rooms".into(), Value::Array(rooms));
        bridges
    } else {
        Vec::new()
    };

    let cfg = ConnectionConfig {
        version: 1,
        peer: s.args.public_host.clone(),
        webrtc_port: s.args.webrtc_listen.port(),
        turn_port: s.args.turn_listen.port(),
        password: req.password.clone(),
        device_id: req.device_id,
        provider,
        provider_data: provider_data.clone(),
        mtu: 1200,
    };
    let uri = match cfg.encode_uri() {
        Ok(uri) => uri,
        Err(error) => {
            close_rooms(bridges).await;
            return Err(bad(error));
        }
    };

    let changes = s.room_changes.lock().await;
    let d = match s
        .db
        .upsert(
            req.device_id,
            req.password,
            provider,
            provider_data,
            req.valid_days,
        )
        .await
    {
        Ok(device) => device,
        Err(error) => {
            drop(changes);
            close_rooms(bridges).await;
            return Err(internal(error));
        }
    };
    let previous = if bridges.is_empty() {
        s.rooms.remove(&req.device_id).map(|(_, bridges)| bridges)
    } else {
        s.rooms.insert(req.device_id, bridges)
    };
    drop(changes);
    if let Some(previous) = previous {
        close_rooms(previous).await;
    }
    Ok(Json(CreateResponse {
        uri,
        virtual_ip: cfg.virtual_ipv4().to_string(),
        expires_at: d.expires_at.to_rfc3339(),
    }))
}

/// Drain in-flight setup on failure so every successfully opened path is explicitly closed.
async fn build_room_pool_once<F, Fut>(
    count: usize,
    make_room: F,
) -> anyhow::Result<(Vec<Value>, Vec<NativeBridge>)>
where
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = anyhow::Result<(Value, NativeBridge)>> + Send + 'static,
{
    let make_room = Arc::new(make_room);
    let mut pending = tokio::task::JoinSet::new();
    let mut completed: Vec<Option<(Value, NativeBridge)>> =
        std::iter::repeat_with(|| None).take(count).collect();
    let mut next = 0;
    let mut failure = None;
    loop {
        while failure.is_none() && next < count && pending.len() < 4 {
            let index = next;
            let make_room = make_room.clone();
            pending.spawn(async move { (index, make_room(index).await) });
            next += 1;
        }
        let Some(result) = pending.join_next().await else {
            break;
        };
        match result {
            Ok((index, Ok(room))) => completed[index] = Some(room),
            Ok((_, Err(error))) => {
                failure.get_or_insert(error);
            }
            Err(_) => {
                failure.get_or_insert_with(|| anyhow::anyhow!("Jazz room setup task failed"));
            }
        }
    }
    let (rooms, bridges) = completed.into_iter().flatten().unzip();
    if let Some(error) = failure {
        close_rooms(bridges).await;
        Err(error)
    } else {
        Ok((rooms, bridges))
    }
}

/// Each retry keeps its existing pool slot, so at most four rooms are set up at once.
async fn build_room_pool<F, Fut>(
    count: usize,
    make_room: F,
) -> anyhow::Result<(Vec<Value>, Vec<NativeBridge>)>
where
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = anyhow::Result<(Value, NativeBridge)>> + Send + 'static,
{
    let make_room = Arc::new(make_room);
    build_room_pool_once(count, move |index| {
        let make_room = make_room.clone();
        async move {
            for attempt in 1..=3 {
                match make_room(index).await {
                    Ok(room) => {
                        if attempt > 1 {
                            tracing::info!(
                                worker = index,
                                attempt,
                                "Jazz room setup retry succeeded"
                            );
                        }
                        return Ok(room);
                    }
                    Err(_) => {
                        tracing::warn!(worker = index, attempt, "Jazz room setup attempt failed")
                    }
                }
                if attempt < 3 {
                    tokio::time::sleep(std::time::Duration::from_millis(250 * attempt)).await;
                }
            }
            anyhow::bail!("Jazz room setup attempts exhausted")
        }
    })
    .await
}

async fn close_rooms(bridges: Vec<NativeBridge>) {
    let mut closing = tokio::task::JoinSet::new();
    for bridge in bridges {
        closing.spawn(bridge.close());
    }
    while closing.join_next().await.is_some() {}
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
        if let Some((_, bridges)) = removed {
            close_rooms(bridges).await;
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
                let Some(jazz) =
                    provider_entry(device.provider, &device.provider_data, ProviderName::Jazz)
                else {
                    continue;
                };
                let links = match persisted_room_links(jazz) {
                    Ok(links) => links,
                    Err(_) => {
                        tracing::warn!(device_id=%device.device_id, "invalid persisted Jazz room pool");
                        continue;
                    }
                };
                let carrier = match tots_core::providers::jazz::media_carrier(jazz) {
                    Ok(carrier) => carrier,
                    Err(_) => {
                        tracing::warn!(device_id=%device.device_id, "invalid persisted Jazz media carrier");
                        continue;
                    }
                };
                let target = state.args.turn_listen;
                let restored = build_room_pool(links.len(), move |index| {
                    let link = links[index].clone();
                    async move {
                        let room =
                            tots_core::providers::jazz::join_room_with_carrier(&link, carrier)
                                .await?;
                        match NativeBridge::start(room.path.clone(), target).await {
                            Ok(bridge) => Ok((Value::Null, bridge)),
                            Err(error) => {
                                let _ = room.path.close().await;
                                Err(error)
                            }
                        }
                    }
                })
                .await;
                match restored {
                    Ok((_, bridges)) => {
                        publish_restored_rooms(&state, device.device_id, jazz, bridges).await;
                    }
                    Err(_) => {
                        tracing::warn!(device_id=%device.device_id, "Jazz room pool restore failed")
                    }
                }
            }
        }
        Err(_) => tracing::warn!("read persisted Jazz rooms failed"),
    }
    let mut timer = tokio::time::interval(std::time::Duration::from_secs(60));
    loop {
        timer.tick().await;
        prune_rooms(&state).await;
    }
}

async fn prune_rooms(state: &ApiState) {
    let ids: Vec<_> = state.rooms.iter().map(|entry| *entry.key()).collect();
    for id in ids {
        let changes = state.room_changes.lock().await;
        let remove = match state.db.get(id).await {
            Ok(Some(device)) => {
                device.revoked
                    || device.expires_at < chrono::Utc::now()
                    || provider_entry(device.provider, &device.provider_data, ProviderName::Jazz)
                        .is_none()
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
        if let Some((_, bridges)) = removed {
            close_rooms(bridges).await;
        }
    }
}

async fn publish_restored_rooms(
    state: &ApiState,
    id: Uuid,
    expected: &Value,
    bridges: Vec<NativeBridge>,
) {
    let changes = state.room_changes.lock().await;
    let current = state.db.get(id).await.ok().flatten();
    let active = current.is_some_and(|current| {
        !current.revoked
            && current.expires_at >= chrono::Utc::now()
            && provider_entry(current.provider, &current.provider_data, ProviderName::Jazz)
                == Some(expected)
    });
    let unused = if active {
        match state.rooms.entry(id) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(bridges);
                None
            }
            dashmap::mapref::entry::Entry::Occupied(_) => Some(bridges),
        }
    } else {
        Some(bridges)
    };
    drop(changes);
    if let Some(bridges) = unused {
        close_rooms(bridges).await;
    }
}

fn persisted_room_links(jazz: &Value) -> Result<Vec<String>, String> {
    let count = tots_core::providers::jazz::worker_room_count(jazz)
        .map_err(|_| "invalid Jazz room pool")?;
    (0..count)
        .map(|index| {
            let room = tots_core::providers::jazz::worker_room_data(jazz, index)
                .map_err(|_| "invalid Jazz room pool")?;
            Ok(room["join_link"]
                .as_str()
                .expect("validated Jazz invite")
                .to_owned())
        })
        .collect()
}

fn requested_room_links(jazz: &Value) -> Result<Vec<Option<String>>, String> {
    let jazz = jazz
        .as_object()
        .ok_or("provider_data.jazz must be an object")?;
    let count = match jazz.get("worker_count") {
        Some(value) => value
            .as_u64()
            .filter(|count| {
                (1..=tots_core::providers::jazz::MAX_WORKER_ROOMS as u64).contains(count)
            })
            .ok_or("provider_data.jazz.worker_count must be 1..=50")?
            as usize,
        None => 1,
    };
    if jazz.contains_key("worker_rooms") {
        return persisted_room_links(&Value::Object(jazz.clone()))
            .map(|links| links.into_iter().map(Some).collect());
    }
    let link = match jazz.get("join_link") {
        Some(Value::String(link)) if !link.trim().is_empty() => Some(link.clone()),
        Some(Value::String(_)) | None => None,
        _ => return Err("provider_data.jazz.join_link must be a string".into()),
    };
    let mut links = vec![None; count];
    links[0] = link;
    Ok(links)
}

fn normalize_provider_data(primary: ProviderName, value: Value) -> Result<Value, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "provider_data must be an object".to_owned())?;
    let value = if object.contains_key("vk") || object.contains_key("jazz") {
        value
    } else {
        // Migration path for existing API clients using a flat provider object.
        let mut keyed = Map::new();
        keyed.insert(primary.as_str().to_owned(), value);
        Value::Object(keyed)
    };
    if let Some(jazz) = value.get("jazz") {
        requested_room_links(jazz)?;
    }
    Ok(value)
}

fn provider_entry(primary: ProviderName, data: &Value, provider: ProviderName) -> Option<&Value> {
    data.get(provider.as_str()).or_else(|| {
        if primary == provider && data.get("vk").is_none() && data.get("jazz").is_none() {
            Some(data)
        } else {
            None
        }
    })
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
    async fn revoking_a_worker_pool_closes_every_bridge() {
        let fixture = Fixture::new().await;
        let id = Uuid::new_v4();
        fixture.seed(id, ProviderName::Jazz, OLD_LINK).await;
        let mut bridges = Vec::new();
        let mut paths = Vec::new();
        for _ in 0..3 {
            let (bridge, path) = fixture.bridge().await;
            bridges.push(bridge);
            paths.push(path);
        }
        fixture.state.rooms.insert(id, bridges);
        revoke(State(fixture.state.clone()), HeaderMap::new(), Path(id))
            .await
            .unwrap();
        assert!(fixture.state.rooms.is_empty());
        for path in paths {
            assert_released(path).await;
        }
    }

    #[tokio::test]
    async fn restoring_a_stale_pool_checks_every_invite() {
        let fixture = Fixture::new().await;
        let id = Uuid::new_v4();
        let stale = json!({"join_link":OLD_LINK,"worker_count":2,"worker_rooms":[
            {"join_link":OLD_LINK,"connector_url":"a"},
            {"join_link":NEW_LINK,"connector_url":"b"}
        ]});
        let mut current = stale.clone();
        current["worker_rooms"][1]["join_link"] = json!("replacement-room");
        fixture
            .state
            .db
            .upsert(
                id,
                "test-password".into(),
                ProviderName::Jazz,
                json!({"jazz":current}),
                30,
            )
            .await
            .unwrap();
        let (one, first_path) = fixture.bridge().await;
        let (two, second_path) = fixture.bridge().await;
        publish_restored_rooms(&fixture.state, id, &stale, vec![one, two]).await;
        assert!(fixture.state.rooms.is_empty());
        assert_released(first_path).await;
        assert_released(second_path).await;
    }

    #[tokio::test]
    async fn expiry_closes_every_bridge_in_a_pool() {
        let fixture = Fixture::new().await;
        let id = Uuid::new_v4();
        fixture.seed(id, ProviderName::Jazz, OLD_LINK).await;
        let (one, first_path) = fixture.bridge().await;
        let (two, second_path) = fixture.bridge().await;
        fixture.state.rooms.insert(id, vec![one, two]);
        rusqlite::Connection::open(&fixture.db_path)
            .unwrap()
            .execute(
                "UPDATE configs SET expires_at='2000-01-01T00:00:00Z' WHERE device_id=?1",
                [id.to_string()],
            )
            .unwrap();
        prune_rooms(&fixture.state).await;
        assert!(fixture.state.rooms.is_empty());
        assert_released(first_path).await;
        assert_released(second_path).await;
    }

    #[tokio::test]
    async fn transient_restore_failure_retries_and_publishes_the_complete_ordered_pool() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let fixture = Arc::new(Fixture::new().await);
        let id = Uuid::new_v4();
        let pool = json!({"worker_count":6,"worker_rooms":(0..6).map(|index| json!({"join_link":format!("room-{index}"),"connector_url":"test"})).collect::<Vec<_>>()});
        fixture
            .state
            .db
            .upsert(
                id,
                "test-password".into(),
                ProviderName::Jazz,
                json!({"jazz":pool}),
                30,
            )
            .await
            .unwrap();
        let attempts = Arc::new((0..6).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let saved_attempts = attempts.clone();
        let room_fixture = fixture.clone();
        let (rooms, bridges) = build_room_pool(6, move |index| {
            let fixture = room_fixture.clone();
            let attempts = attempts.clone();
            async move {
                let attempt = attempts[index].fetch_add(1, Ordering::SeqCst) + 1;
                if index == 2 && attempt < 3 {
                    return Err(anyhow::anyhow!("transient test failure"));
                }
                let (bridge, _) = fixture.bridge().await;
                Ok((json!({"index":index}), bridge))
            }
        })
        .await
        .expect("transient room failure rolled back the complete pool");
        assert_eq!(saved_attempts[2].load(Ordering::SeqCst), 3);
        assert_eq!(
            rooms
                .iter()
                .map(|room| room["index"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5]
        );
        publish_restored_rooms(&fixture.state, id, &pool, bridges).await;
        assert_eq!(fixture.state.rooms.get(&id).unwrap().len(), 6);
        close_rooms(fixture.state.rooms.remove(&id).unwrap().1).await;
    }

    #[tokio::test]
    async fn persistent_restore_failure_stops_after_three_attempts_and_closes_all_rooms() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let fixture = Arc::new(Fixture::new().await);
        let failed_attempts = Arc::new(AtomicUsize::new(0));
        let saved_attempts = failed_attempts.clone();
        let paths = Arc::new(std::sync::Mutex::new(Vec::new()));
        let saved_paths = paths.clone();
        let result = build_room_pool(6, move |index| {
            let fixture = fixture.clone();
            let failed_attempts = failed_attempts.clone();
            let paths = paths.clone();
            async move {
                if index == 2 {
                    failed_attempts.fetch_add(1, Ordering::SeqCst);
                    return Err(anyhow::anyhow!("persistent test failure"));
                }
                let (bridge, path) = fixture.bridge().await;
                paths.lock().unwrap().push(path);
                Ok((Value::Null, bridge))
            }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            saved_attempts.load(Ordering::SeqCst),
            3,
            "restore did not exhaust its bounded retry budget"
        );
        let paths = std::mem::take(&mut *saved_paths.lock().unwrap());
        assert!(!paths.is_empty());
        for path in paths {
            assert_released(path).await;
        }
    }

    #[tokio::test]
    async fn failed_pool_creation_closes_all_completed_and_inflight_rooms() {
        let fixture = Arc::new(Fixture::new().await);
        let paths = Arc::new(std::sync::Mutex::new(Vec::new()));
        let saved = paths.clone();
        let result = build_room_pool(8, move |index| {
            let fixture = fixture.clone();
            let paths = paths.clone();
            async move {
                if index == 2 {
                    return Err(anyhow::anyhow!("test provisioning failure"));
                }
                tokio::task::yield_now().await;
                let (bridge, path) = fixture.bridge().await;
                paths.lock().unwrap().push(path);
                Ok((
                    json!({"join_link":format!("room-{index}"),"connector_url":"test"}),
                    bridge,
                ))
            }
        })
        .await;
        assert!(result.is_err());
        let paths = std::mem::take(&mut *saved.lock().unwrap());
        assert!(!paths.is_empty());
        for path in paths {
            assert_released(path).await;
        }
    }

    #[tokio::test]
    async fn pool_creation_is_bounded_and_preserves_worker_order() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let fixture = Arc::new(Fixture::new().await);
        let attempts = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let saved_peak = peak.clone();
        let (rooms, bridges) = build_room_pool(12, move |index| {
            let fixture = fixture.clone();
            let active = active.clone();
            let peak = peak.clone();
            let attempts = attempts.clone();
            async move {
                if index == 3 && attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(anyhow::anyhow!("transient provisioning failure"));
                }
                let n = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(n, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis((4 - index % 4) as u64)).await;
                let (bridge, _) = fixture.bridge().await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok((
                    json!({"join_link":format!("room-{index}"),"connector_url":"test"}),
                    bridge,
                ))
            }
        })
        .await
        .unwrap();
        assert!((2..=4).contains(&saved_peak.load(Ordering::SeqCst)));
        for (index, room) in rooms.iter().enumerate() {
            assert_eq!(room["join_link"], format!("room-{index}"));
        }
        close_rooms(bridges).await;
    }

    #[test]
    fn provisioning_defaults_to_one_room_and_keeps_existing_invites_separate() {
        assert_eq!(requested_room_links(&json!({})).unwrap(), vec![None]);
        let links = requested_room_links(&json!({"worker_count":20,"join_link":OLD_LINK})).unwrap();
        assert_eq!(links.len(), 20);
        assert_eq!(links[0].as_deref(), Some(OLD_LINK));
        assert!(links[1..].iter().all(Option::is_none));
        let migrated =
            normalize_provider_data(ProviderName::Jazz, json!({"join_link":OLD_LINK})).unwrap();
        assert_eq!(
            persisted_room_links(&migrated["jazz"]).unwrap(),
            vec![OLD_LINK]
        );
        let pool = json!({"worker_rooms":[
            {"join_link":OLD_LINK,"connector_url":"first"},
            {"join_link":NEW_LINK,"connector_url":"second"}
        ]});
        assert_eq!(
            persisted_room_links(&pool).unwrap(),
            vec![OLD_LINK, NEW_LINK]
        );
        assert_eq!(
            requested_room_links(&pool).unwrap(),
            vec![Some(OLD_LINK.into()), Some(NEW_LINK.into())]
        );
    }

    #[tokio::test]
    async fn matching_active_pool_restores_all_bridges() {
        let fixture = Fixture::new().await;
        let id = Uuid::new_v4();
        let pool = json!({"worker_count":2,"worker_rooms":[
            {"join_link":OLD_LINK,"connector_url":"first"},
            {"join_link":NEW_LINK,"connector_url":"second"}
        ]});
        fixture
            .state
            .db
            .upsert(
                id,
                "test-password".into(),
                ProviderName::Jazz,
                json!({"jazz":pool}),
                30,
            )
            .await
            .unwrap();
        let (one, first_path) = fixture.bridge().await;
        let (two, second_path) = fixture.bridge().await;
        publish_restored_rooms(&fixture.state, id, &pool, vec![one, two]).await;
        assert_eq!(fixture.state.rooms.get(&id).unwrap().len(), 2);
        assert!(first_path.upgrade().is_some());
        assert!(second_path.upgrade().is_some());
        let (_, bridges) = fixture.state.rooms.remove(&id).unwrap();
        close_rooms(bridges).await;
        assert_released(first_path).await;
        assert_released(second_path).await;
    }

    #[test]
    fn provisioning_rejects_invalid_worker_counts_before_creating_rooms() {
        for count in [
            json!(0),
            json!(51),
            json!(-1),
            json!("20"),
            json!(1.5),
            Value::Null,
        ] {
            assert!(
                normalize_provider_data(ProviderName::Jazz, json!({"jazz":{"worker_count":count}}))
                    .is_err(),
                "invalid worker count accepted"
            );
        }
        assert!(normalize_provider_data(ProviderName::Jazz, json!({"jazz":{}})).is_ok());
        assert!(
            normalize_provider_data(ProviderName::Jazz, json!({"jazz":{"worker_count":50}}))
                .is_ok()
        );
    }

    #[tokio::test]
    async fn changing_jazz_to_vk_removes_and_closes_the_old_bridge() {
        let fixture = Fixture::new().await;
        let id = Uuid::new_v4();
        fixture.seed(id, ProviderName::Jazz, OLD_LINK).await;
        let (bridge, path) = fixture.bridge().await;
        let (second, second_path) = fixture.bridge().await;
        fixture.state.rooms.insert(id, vec![bridge, second]);

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
        assert_released(second_path).await;
    }

    #[tokio::test]
    async fn revoking_jazz_removes_and_closes_its_bridge() {
        let fixture = Fixture::new().await;
        let id = Uuid::new_v4();
        fixture.seed(id, ProviderName::Jazz, OLD_LINK).await;
        let (bridge, path) = fixture.bridge().await;
        fixture.state.rooms.insert(id, vec![bridge]);

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
        fixture.state.rooms.insert(id, vec![current]);

        for link in [OLD_LINK, NEW_LINK] {
            let (restored, unused_path) = fixture.bridge().await;
            publish_restored_rooms(
                &fixture.state,
                id,
                &json!({"join_link":link,"vk_call_links":["test"]}),
                vec![restored],
            )
            .await;
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

        publish_restored_rooms(
            &fixture.state,
            id,
            &json!({"join_link":OLD_LINK,"vk_call_links":["test"]}),
            vec![restored],
        )
        .await;

        assert!(fixture.state.rooms.is_empty());
        assert_released(path).await;
    }

    #[tokio::test]
    async fn matching_active_jazz_room_can_be_restored() {
        let fixture = Fixture::new().await;
        let id = Uuid::new_v4();
        fixture.seed(id, ProviderName::Jazz, OLD_LINK).await;
        let (restored, path) = fixture.bridge().await;

        publish_restored_rooms(
            &fixture.state,
            id,
            &json!({"join_link":OLD_LINK,"vk_call_links":["test"]}),
            vec![restored],
        )
        .await;

        assert!(fixture.state.rooms.contains_key(&id));
        assert!(path.upgrade().is_some());
    }

    #[tokio::test]
    async fn revoked_or_non_jazz_configs_cannot_publish_a_restored_room() {
        for provider in [ProviderName::Jazz, ProviderName::Vk] {
            let fixture = Fixture::new().await;
            let id = Uuid::new_v4();
            fixture.seed(id, provider, OLD_LINK).await;
            if provider == ProviderName::Jazz {
                fixture.state.db.revoke(id).await.unwrap();
            }
            let (restored, path) = fixture.bridge().await;

            publish_restored_rooms(
                &fixture.state,
                id,
                &json!({"join_link":OLD_LINK,"vk_call_links":["test"]}),
                vec![restored],
            )
            .await;

            assert!(
                fixture.state.rooms.is_empty(),
                "published inactive {provider:?} room"
            );
            assert_released(path).await;
        }
    }
}
