use crate::{config::Args, db::Db};
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
    Json(req): Json<CreateRequest>,
) -> Result<Json<CreateResponse>, (StatusCode, String)> {
    auth(&headers, &s).map_err(|c| (c, "unauthorized".into()))?;
    let provider = ProviderName::parse(&req.provider).map_err(bad)?;
    if req.password.len() < 8 {
        return Err((StatusCode::BAD_REQUEST, "password too short".into()));
    }
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
    let d = s
        .db
        .upsert(
            req.device_id,
            req.password,
            provider,
            req.provider_data,
            req.valid_days,
        )
        .await
        .map_err(internal)?;
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
    if s.db.revoke(id).await.map_err(internal)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Ok(StatusCode::NOT_FOUND)
    }
}
fn bad<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, e.to_string())
}
fn internal<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}
