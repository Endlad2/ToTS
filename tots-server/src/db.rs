use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::task;
use tots_core::config::ProviderName;
use uuid::Uuid;

#[derive(Clone)]
pub struct Db {
    path: Arc<PathBuf>,
}
#[derive(Clone, Debug)]
pub struct Device {
    pub device_id: Uuid,
    pub password: String,
    pub provider: ProviderName,
    pub provider_data: Value,
    pub expires_at: DateTime<Utc>,
    pub revoked: bool,
}
impl Db {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(p) = path.parent() {
            tokio::fs::create_dir_all(p).await?;
        }
        let db = Self {
            path: Arc::new(path),
        };
        db.init().await?;
        Ok(db)
    }
    fn conn(path: &Path) -> Result<Connection> {
        Ok(Connection::open(path)?)
    }
    async fn init(&self) -> Result<()> {
        let p = self.path.clone();
        task::spawn_blocking(move || -> Result<()> {
            let c = Self::conn(&p)?;
            c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; CREATE TABLE IF NOT EXISTS configs(device_id TEXT PRIMARY KEY,password TEXT NOT NULL,provider TEXT NOT NULL,provider_data TEXT NOT NULL,created_at TEXT NOT NULL,expires_at TEXT NOT NULL,revoked INTEGER NOT NULL DEFAULT 0);")?;
            Ok(())
        })
        .await??;
        Ok(())
    }
    pub async fn upsert(
        &self,
        device_id: Uuid,
        password: String,
        provider: ProviderName,
        provider_data: Value,
        days: i64,
    ) -> Result<Device> {
        let p = self.path.clone();
        task::spawn_blocking(move || -> Result<Device> {
            let now = Utc::now();
            let expires = now + Duration::days(days.clamp(1, 3650));
            let c = Self::conn(&p)?;
            let provider_s = provider.as_str();
            c.execute(
                "INSERT INTO configs(device_id,password,provider,provider_data,created_at,expires_at,revoked) VALUES(?1,?2,?3,?4,?5,?6,0) ON CONFLICT(device_id) DO UPDATE SET password=excluded.password,provider=excluded.provider,provider_data=excluded.provider_data,expires_at=excluded.expires_at,revoked=0",
                params![
                    device_id.to_string(),
                    password,
                    provider_s,
                    provider_data.to_string(),
                    now.to_rfc3339(),
                    expires.to_rfc3339()
                ],
            )?;
            Ok(Device {
                device_id,
                password,
                provider,
                provider_data,
                expires_at: expires,
                revoked: false,
            })
        })
        .await?
    }
    pub async fn get(&self, id: Uuid) -> Result<Option<Device>> {
        let p = self.path.clone();
        task::spawn_blocking(move || -> Result<Option<Device>> {
            let c = Self::conn(&p)?;
            let row = c
                .query_row(
                    "SELECT password,provider,provider_data,expires_at,revoked FROM configs WHERE device_id=?1",
                    [id.to_string()],
                    |r| {
                        Ok((
                            r.get::<_, String>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, String>(3)?,
                            r.get::<_, i64>(4)?,
                        ))
                    },
                )
                .optional()?;
            let Some((password, provider_s, pd, exp, revoked)) = row else {
                return Ok(None);
            };
            let provider = ProviderName::parse(&provider_s).unwrap_or(ProviderName::Unknown);
            let provider_data = serde_json::from_str(&pd)?;
            let expires_at = DateTime::parse_from_rfc3339(&exp)?.with_timezone(&Utc);
            Ok(Some(Device {
                device_id: id,
                password,
                provider,
                provider_data,
                expires_at,
                revoked: revoked != 0,
            }))
        })
        .await?
    }
    pub async fn revoke(&self, id: Uuid) -> Result<bool> {
        let p = self.path.clone();
        task::spawn_blocking(move || -> Result<bool> {
            let c = Self::conn(&p)?;
            Ok(c.execute(
                "UPDATE configs SET revoked=1 WHERE device_id=?1",
                [id.to_string()],
            )? > 0)
        })
        .await?
    }

    pub async fn active_jazz(&self) -> Result<Vec<Device>> {
        let p = self.path.clone();
        let ids = task::spawn_blocking(move || -> Result<Vec<String>> {
            let conn = Self::conn(&p)?;
            let mut query = conn.prepare("SELECT device_id FROM configs WHERE revoked=0")?;
            let rows = query
                .query_map([], |row| row.get(0))?
                .collect::<rusqlite::Result<Vec<String>>>()?;
            Ok(rows)
        })
        .await??;
        let mut devices = Vec::new();
        for id in ids {
            if let Some(device) = self.get(Uuid::parse_str(&id)?).await? {
                let keyed = ProviderName::ALL
                    .iter()
                    .any(|provider| device.provider_data.get(provider.as_str()).is_some());
                let has_jazz = device.provider_data.get("jazz").is_some()
                    || (device.provider == ProviderName::Jazz && !keyed);
                if has_jazz && device.expires_at >= Utc::now() {
                    devices.push(device);
                }
            }
        }
        Ok(devices)
    }
}
