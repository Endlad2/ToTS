use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::{BTreeSet, HashMap},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    process::Stdio,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::{
    net::{lookup_host, TcpStream},
    process::{Child, Command},
    sync::Mutex,
};
use url::Url;

use crate::{
    config::ProviderName,
    providers::{Provider, ProviderBootstrap},
    transport::{socks5::Socks5UdpPath, DatagramPath},
    Error, Result,
};

const SIDECAR_READY_TIMEOUT: Duration = Duration::from_secs(25);
const SIDECAR_POLL: Duration = Duration::from_millis(150);

#[derive(Clone, Copy)]
pub struct OpenFluxProvider {
    kind: ProviderName,
}

impl OpenFluxProvider {
    pub const fn new(kind: ProviderName) -> Self {
        Self { kind }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct OpenFluxData {
    url: String,
    #[serde(default)]
    socks5_addr: Option<String>,
    #[serde(default)]
    target: Option<String>,
    #[serde(default = "default_auto_launch")]
    auto_launch: bool,
    #[serde(default)]
    openflux_binary: Option<String>,
    #[serde(default)]
    yandex_cookies_file: Option<String>,
    #[serde(default)]
    cookie_store: Option<String>,
    #[serde(default)]
    route_hosts: Vec<String>,
}

fn default_auto_launch() -> bool {
    true
}

struct Sidecar {
    child: Mutex<Child>,
}

static SIDECARS: OnceLock<Mutex<HashMap<String, Arc<Sidecar>>>> = OnceLock::new();

fn sidecars() -> &'static Mutex<HashMap<String, Arc<Sidecar>>> {
    SIDECARS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[async_trait]
impl Provider for OpenFluxProvider {
    async fn bootstrap(&self, _data: &Value) -> Result<ProviderBootstrap> {
        Ok(ProviderBootstrap::default())
    }

    async fn connect_native(
        &self,
        data: &Value,
        default_target: SocketAddr,
    ) -> Result<Option<Arc<dyn DatagramPath>>> {
        let parsed = parse_data(self.kind, data)?;
        let proxy = parsed
            .socks5_addr
            .as_deref()
            .unwrap_or(default_socks5_addr(self.kind))
            .parse::<SocketAddr>()
            .map_err(|_| Error::Provider("invalid OpenFlux socks5_addr".into()))?;
        let target = match parsed.target.as_deref() {
            Some(target) => target
                .parse::<SocketAddr>()
                .map_err(|_| Error::Provider("invalid OpenFlux target".into()))?,
            None => default_target,
        };

        ensure_sidecar(self.kind, &parsed, proxy).await?;
        let route_ips = resolve_route_ips(self.kind, &parsed).await;
        let path = Socks5UdpPath::connect(
            format!("openflux:{}", self.kind.as_str()),
            proxy,
            target,
            route_ips,
        )
        .await?;
        Ok(Some(Arc::new(path)))
    }
}

fn parse_data(kind: ProviderName, data: &Value) -> Result<OpenFluxData> {
    if !kind.is_openflux_carrier() {
        return Err(Error::Provider("not an OpenFlux carrier provider".into()));
    }
    let parsed: OpenFluxData = serde_json::from_value(data.clone())
        .map_err(|_| Error::Provider("invalid OpenFlux provider_data".into()))?;
    if parsed.url.trim().is_empty() {
        return Err(Error::Provider(format!(
            "provider_data.{} requires url",
            kind.as_str()
        )));
    }
    if parsed
        .openflux_binary
        .as_deref()
        .is_some_and(|path| path.trim().is_empty())
        || parsed
            .yandex_cookies_file
            .as_deref()
            .is_some_and(|path| path.trim().is_empty())
        || parsed
            .cookie_store
            .as_deref()
            .is_some_and(|path| path.trim().is_empty())
        || parsed.route_hosts.iter().any(|host| host.trim().is_empty())
    {
        return Err(Error::Provider("invalid OpenFlux runtime option".into()));
    }
    if parsed.yandex_cookies_file.is_some() && kind != ProviderName::Vyandex {
        return Err(Error::Provider(
            "yandex_cookies_file is only valid for vyandex".into(),
        ));
    }
    Ok(parsed)
}

async fn ensure_sidecar(kind: ProviderName, data: &OpenFluxData, proxy: SocketAddr) -> Result<()> {
    if proxy_ready(proxy).await {
        return Ok(());
    }
    if !data.auto_launch {
        return Err(Error::Provider(format!(
            "OpenFlux SOCKS5 sidecar is not listening on {proxy}"
        )));
    }

    let key = format!("{}|{}|{}", kind.as_str(), proxy, data.url);
    let mut registry = sidecars().lock().await;
    if let Some(existing) = registry.get(&key).cloned() {
        drop(registry);
        if wait_proxy(proxy, SIDECAR_READY_TIMEOUT).await {
            return Ok(());
        }
        sidecars().lock().await.remove(&key);
        let mut child = existing.child.lock().await;
        let _ = child.kill().await;
        let _ = child.wait().await;
        return Err(Error::Provider(
            "OpenFlux sidecar stopped responding; worker will retry and restart it".into(),
        ));
    }
    if proxy_ready(proxy).await {
        return Ok(());
    }

    let binary = data
        .openflux_binary
        .as_deref()
        .map(PathBuf::from)
        .unwrap_or_else(default_openflux_binary);
    let mut command = Command::new(&binary);
    command
        .arg("--role=client")
        .arg("--inbound=socks5")
        .arg(format!("--transport={}", kind.as_str()))
        .arg(format!("--url={}", data.url))
        .arg(format!("--socks5={proxy}"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    if kind == ProviderName::Vyandex {
        if let Some(path) = data.yandex_cookies_file.as_deref() {
            command.arg(format!("--yandex-cookies-file={path}"));
        }
    }
    if let Some(path) = data.cookie_store.as_deref() {
        command.arg(format!("--cookie-store={path}"));
    }

    let child = command.spawn().map_err(|error| {
        Error::Provider(format!(
            "failed to start OpenFlux sidecar {:?}: {error}",
            binary
        ))
    })?;
    let sidecar = Arc::new(Sidecar {
        child: Mutex::new(child),
    });
    registry.insert(key.clone(), sidecar.clone());
    drop(registry);

    let deadline = tokio::time::Instant::now() + SIDECAR_READY_TIMEOUT;
    loop {
        if proxy_ready(proxy).await {
            return Ok(());
        }
        {
            let mut child = sidecar.child.lock().await;
            if let Some(status) = child
                .try_wait()
                .map_err(|error| Error::Provider(format!("OpenFlux status check failed: {error}")))?
            {
                sidecars().lock().await.remove(&key);
                return Err(Error::Provider(format!(
                    "OpenFlux sidecar exited before SOCKS5 became ready: {status}"
                )));
            }
        }
        if tokio::time::Instant::now() >= deadline {
            sidecars().lock().await.remove(&key);
            let mut child = sidecar.child.lock().await;
            let _ = child.kill().await;
            return Err(Error::Provider(format!(
                "OpenFlux SOCKS5 sidecar on {proxy} timed out"
            )));
        }
        tokio::time::sleep(SIDECAR_POLL).await;
    }
}

async fn proxy_ready(proxy: SocketAddr) -> bool {
    tokio::time::timeout(Duration::from_millis(250), TcpStream::connect(proxy))
        .await
        .is_ok_and(|result| result.is_ok())
}

async fn wait_proxy(proxy: SocketAddr, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if proxy_ready(proxy).await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(SIDECAR_POLL).await;
    }
}

fn default_socks5_addr(kind: ProviderName) -> &'static str {
    match kind {
        ProviderName::Yandex => "127.0.0.1:11801",
        ProviderName::Vyandex => "127.0.0.1:11802",
        ProviderName::Boards => "127.0.0.1:11803",
        ProviderName::Cupsonline => "127.0.0.1:11804",
        ProviderName::Mailru => "127.0.0.1:11805",
        _ => "127.0.0.1:11800",
    }
}

fn default_openflux_binary() -> PathBuf {
    let executable = if cfg!(windows) {
        "openflux.exe"
    } else {
        "openflux"
    };
    if let Ok(mut current) = std::env::current_exe() {
        current.pop();
        current.push(executable);
        if current.is_file() {
            return current;
        }
    }
    PathBuf::from(executable)
}

async fn resolve_route_ips(kind: ProviderName, data: &OpenFluxData) -> Vec<Ipv4Addr> {
    let mut hosts = BTreeSet::new();
    hosts.extend(default_route_hosts(kind).iter().map(|host| host.to_string()));
    hosts.extend(data.route_hosts.iter().map(|host| host.trim().to_string()));
    if let Ok(url) = Url::parse(&data.url) {
        if let Some(host) = url.host_str() {
            hosts.insert(host.to_owned());
        }
    }

    let mut ips = BTreeSet::new();
    for host in hosts {
        if let Ok(ip) = host.parse::<Ipv4Addr>() {
            if !ip.is_unspecified() && !ip.is_loopback() {
                ips.insert(ip);
            }
            continue;
        }
        let Ok(Ok(addresses)) =
            tokio::time::timeout(Duration::from_secs(3), lookup_host((host.as_str(), 443))).await
        else {
            continue;
        };
        for address in addresses {
            if let IpAddr::V4(ip) = address.ip() {
                if !ip.is_unspecified() && !ip.is_loopback() {
                    ips.insert(ip);
                }
            }
        }
    }
    ips.into_iter().collect()
}

fn default_route_hosts(kind: ProviderName) -> &'static [&'static str] {
    match kind {
        ProviderName::Yandex => &["docs.yandex.ru"],
        ProviderName::Vyandex => &["disk.yandex.ru", "volga.yandex.ru", "push.yandex.ru"],
        ProviderName::Boards => &["boards.yandex.ru"],
        ProviderName::Cupsonline => &["interview.cups.online"],
        ProviderName::Mailru => &["cloud.mail.ru", "docs.datacloudmail.ru"],
        _ => &[],
    }
}

pub async fn shutdown_sidecars() {
    let sidecars = {
        let mut registry = sidecars().lock().await;
        std::mem::take(&mut *registry)
    };
    for (_, sidecar) in sidecars {
        let mut child = sidecar.child.lock().await;
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validates_carrier_data_without_echoing_url() {
        assert!(parse_data(ProviderName::Yandex, &json!({})).is_err());
        assert!(parse_data(
            ProviderName::Vyandex,
            &json!({"url":"https://docs.yandex.ru/example","yandex_cookies_file":"cookies.txt"})
        )
        .is_ok());
        assert!(parse_data(
            ProviderName::Mailru,
            &json!({"url":"AbCdEfGh1/IjKlMnOp2","yandex_cookies_file":"cookies.txt"})
        )
        .is_err());
    }
}
