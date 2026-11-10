#[cfg(unix)]
use anyhow::bail;
use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tots_core::config::ConnectionConfig;

pub enum TunHandle {
    Device(tun::AsyncDevice),
    #[cfg(unix)]
    Fd(tokio::fs::File),
}

pub fn index(tun: &TunHandle) -> Result<i32> {
    use tun::AbstractDevice;
    match tun {
        TunHandle::Device(dev) => Ok(dev.tun_index()?),
        #[cfg(unix)]
        TunHandle::Fd(_) => anyhow::bail!("automatic routes require a managed TUN device"),
    }
}

pub async fn open(cfg: &ConnectionConfig, name: &str, fd: Option<i32>) -> Result<TunHandle> {
    #[cfg(not(unix))]
    if fd.is_some() {
        anyhow::bail!("--tun-fd is supported only on Unix");
    }
    #[cfg(unix)]
    if let Some(fd) = fd {
        use std::os::fd::FromRawFd;
        if fd < 0 {
            bail!("invalid --tun-fd");
        }
        let f = unsafe { std::fs::File::from_raw_fd(fd) };
        return Ok(TunHandle::Fd(tokio::fs::File::from_std(f)));
    }
    let mut c = tun::Configuration::default();
    c.tun_name(name)
        .address(cfg.virtual_ipv4())
        .netmask((255, 255, 0, 0))
        .up();
    #[cfg(not(target_os = "windows"))]
    c.mtu(cfg.mtu as u16);
    #[cfg(target_os = "windows")]
    {
        let dll = std::env::current_exe()
            .context("locate executable for wintun.dll")?
            .with_file_name("wintun.dll");
        c.platform_config(|p| p.wintun_file(dll));
    }
    let dev = tun::create_as_async(&c).context(
        "create TUN (Windows requires Administrator and wintun.dll next to the executable)",
    )?;
    #[cfg(target_os = "windows")]
    {
        use tun::AbstractDevice;
        // Wintun's generic setter also sets IPv6, which rejects MTUs below 1280.
        let output = tokio::process::Command::new("netsh.exe")
            .args(["interface", "ipv4", "set", "subinterface"])
            .arg(dev.tun_index()?.to_string())
            .arg(format!("mtu={}", cfg.mtu))
            .arg("store=active")
            .output()
            .await
            .context("set IPv4 TUN MTU")?;
        if !output.status.success() {
            anyhow::bail!(
                "set IPv4 TUN MTU failed: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
    Ok(TunHandle::Device(dev))
}

pub async fn run(
    tun: &mut TunHandle,
    manager: std::sync::Arc<crate::workers::WorkerManager>,
    inbound: tokio::sync::mpsc::Receiver<Vec<u8>>,
) -> Result<()> {
    match tun {
        TunHandle::Device(dev) => run_io(dev, manager, inbound).await,
        #[cfg(unix)]
        TunHandle::Fd(file) => run_io(file, manager, inbound).await,
    }
}

async fn run_io<T>(
    io: T,
    manager: std::sync::Arc<crate::workers::WorkerManager>,
    mut inbound: tokio::sync::mpsc::Receiver<Vec<u8>>,
) -> Result<()>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    let (mut rd, mut wr) = tokio::io::split(io);
    let tx_manager = manager.clone();
    let tx = async move {
        let mut buf = vec![0u8; 65535];
        loop {
            let n = rd.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            tx_manager.send_ip(&buf[..n]).await?;
        }
        Ok::<_, anyhow::Error>(())
    };
    let rx = async move {
        while let Some(bytes) = inbound.recv().await {
            wr.write_all(&bytes).await?;
            manager
                .stats
                .tun_rx_bytes
                .fetch_add(bytes.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        Err::<(), anyhow::Error>(anyhow::anyhow!("worker inbound queue closed"))
    };
    tokio::select! {r=tx=>r?,r=rx=>r?};
    Ok(())
}

#[cfg(all(test, target_os = "windows"))]
mod tests {
    use super::*;
    use tun::AbstractDevice;

    #[tokio::test]
    #[ignore = "requires Administrator and official wintun.dll next to the test executable"]
    async fn windows_tun_supports_small_ipv4_mtu() {
        let mut cfg = ConnectionConfig {
            version: 1,
            peer: "203.0.113.2".into(),
            webrtc_port: 7800,
            turn_port: 7801,
            password: "test-only-password".into(),
            device_id: uuid::Uuid::new_v4(),
            provider: tots_core::config::ProviderName::Vk,
            provider_data: serde_json::json!({"vk":{"vk_call_links":["test"]}}),
            mtu: 1200,
        };
        for mtu in [1200, 576, 1400] {
            cfg.mtu = mtu;
            let TunHandle::Device(dev) = open(&cfg, "tots-mtu-test", None)
                .await
                .unwrap_or_else(|e| panic!("MTU {mtu}: {e:#}"));
            assert_eq!(dev.mtu().unwrap(), mtu as u16);
            assert_eq!(
                dev.address().unwrap(),
                std::net::IpAddr::V4(cfg.virtual_ipv4())
            );
            println!("Created IPv4 TUN with MTU {mtu}");
        }
    }
}
