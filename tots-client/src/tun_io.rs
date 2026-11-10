#[cfg(unix)]
use anyhow::bail;
use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tots_core::{
    config::ConnectionConfig,
    session::{DecodedPacket, SessionEngine},
    transport::DatagramPath,
};

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
    engine: Arc<SessionEngine>,
    path: Arc<dyn DatagramPath>,
) -> Result<()> {
    match tun {
        TunHandle::Device(dev) => run_io(dev, engine, path).await,
        #[cfg(unix)]
        TunHandle::Fd(file) => run_io(file, engine, path).await,
    }
}

async fn run_io<T>(io: T, engine: Arc<SessionEngine>, path: Arc<dyn DatagramPath>) -> Result<()>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    let (mut rd, mut wr) = tokio::io::split(io);
    let tx_engine = engine.clone();
    let tx_path = path.clone();
    let tx = async move {
        let mut buf = vec![0u8; 65535];
        loop {
            let n = rd.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            let assoc = flow_hash(&buf[..n]);
            let (_, packets) = tx_engine.encapsulate_ip(assoc, &buf[..n])?;
            for p in packets {
                tx_path.send(&p).await?;
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    let rx = async move {
        let mut buf = vec![0u8; 65535];
        loop {
            let n = path.recv(&mut buf).await?;
            if let Some(decoded) = engine.decapsulate(&buf[..n])? {
                match decoded {
                    DecodedPacket::Ip {
                        packet_id, bytes, ..
                    } => {
                        wr.write_all(&bytes).await?;
                        let ack = engine.make_ack(packet_id)?;
                        path.send(&ack).await?;
                    }
                    DecodedPacket::Probe { token, sent_micros } => {
                        path.send(&engine.make_probe_ack(token, sent_micros)?)
                            .await?;
                    }
                    _ => {}
                }
            }
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    };
    tokio::select! {r=tx=>r?,r=rx=>r?};
    Ok(())
}
fn flow_hash(packet: &[u8]) -> u16 {
    let mut x: u32 = 0x9e37_79b9;
    for b in packet.iter().take(64) {
        x = x.rotate_left(5) ^ (*b as u32);
        x = x.wrapping_mul(0x85eb_ca6b);
    }
    ((x >> 16) ^ x) as u16
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
            provider_data: serde_json::json!({"vk_call_links":["test"]}),
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
