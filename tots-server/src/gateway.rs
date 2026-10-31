use std::{
    collections::HashMap,
    net::Ipv4Addr,
    sync::Arc,
};

use anyhow::{bail, Context, Result};
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};
use tun_rs::DeviceBuilder;

#[derive(Clone)]
pub struct TunGateway {
    ingress: mpsc::Sender<Vec<u8>>,
    routes: Arc<RwLock<HashMap<Ipv4Addr, mpsc::Sender<Vec<u8>>>>>,
    network: u32,
    mask: u32,
    gateway_ip: Ipv4Addr,
}

impl TunGateway {
    pub async fn start(
        name: String,
        address: Ipv4Addr,
        prefix: u8,
        mtu: u16,
        outbound_interface: Option<String>,
    ) -> Result<Self> {
        if prefix == 0 || prefix > 32 {
            bail!("--tun-prefix must be between 1 and 32");
        }
        if mtu < 576 || mtu > 1500 {
            bail!("--tun-mtu must be between 576 and 1500");
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = (name, address, prefix, mtu, outbound_interface);
            bail!("server --tun-gateway is currently supported only on Linux");
        }

        #[cfg(target_os = "linux")]
        {
            let tun = DeviceBuilder::new()
                .name(name.clone())
                .mtu(mtu)
                .ipv4(address, prefix, None)
                .build_async()
                .with_context(|| {
                    format!(
                        "create server TUN interface '{name}'; run tots-server as root or grant CAP_NET_ADMIN"
                    )
                })?;

            setup_linux_nat(&name, address, prefix, outbound_interface.as_deref()).await?;

            let routes: Arc<RwLock<HashMap<Ipv4Addr, mpsc::Sender<Vec<u8>>>>> =
                Arc::new(RwLock::new(HashMap::new()));
            let routes_task = routes.clone();
            let (ingress, mut ingress_rx) = mpsc::channel::<Vec<u8>>(2048);
            let read_mtu = usize::from(mtu) + 256;

            tokio::spawn(async move {
                let mut buf = vec![0u8; read_mtu];
                loop {
                    tokio::select! {
                        from_client = ingress_rx.recv() => {
                            let Some(packet) = from_client else { break; };
                            if let Err(err) = tun.send(&packet).await {
                                warn!(%err, "failed to inject client packet into server TUN");
                            }
                        }
                        from_tun = tun.recv(&mut buf) => {
                            match from_tun {
                                Ok(0) => {}
                                Ok(n) => {
                                    let packet = &buf[..n];
                                    let Some(dst) = ipv4_destination(packet) else {
                                        continue;
                                    };
                                    let tx = routes_task.read().await.get(&dst).cloned();
                                    if let Some(tx) = tx {
                                        if tx.send(packet.to_vec()).await.is_err() {
                                            routes_task.write().await.remove(&dst);
                                        }
                                    }
                                }
                                Err(err) => {
                                    warn!(%err, "server TUN receive failed");
                                    break;
                                }
                            }
                        }
                    }
                }
            });

            let mask = prefix_mask(prefix);
            let network = u32::from(address) & mask;
            info!(
                tun = %name,
                address = %address,
                prefix,
                mtu,
                "server TUN gateway is ready"
            );

            Ok(Self {
                ingress,
                routes,
                network,
                mask,
                gateway_ip: address,
            })
        }
    }

    pub async fn forward_from_client(
        &self,
        packet: &[u8],
        return_path: mpsc::Sender<Vec<u8>>,
    ) -> Result<()> {
        let source = ipv4_source(packet).context("TUN gateway received a non-IPv4/invalid packet")?;
        let source_u32 = u32::from(source);
        if source_u32 & self.mask != self.network || source == self.gateway_ip {
            bail!(
                "client packet source {source} is outside the server TUN subnet or uses the gateway address"
            );
        }

        self.routes.write().await.insert(source, return_path);
        self.ingress
            .send(packet.to_vec())
            .await
            .context("server TUN gateway task stopped")?;
        Ok(())
    }
}

fn ipv4_source(packet: &[u8]) -> Option<Ipv4Addr> {
    if packet.len() < 20 || packet[0] >> 4 != 4 {
        return None;
    }
    let ihl = usize::from(packet[0] & 0x0f) * 4;
    if ihl < 20 || packet.len() < ihl {
        return None;
    }
    Some(Ipv4Addr::new(packet[12], packet[13], packet[14], packet[15]))
}

fn ipv4_destination(packet: &[u8]) -> Option<Ipv4Addr> {
    if packet.len() < 20 || packet[0] >> 4 != 4 {
        return None;
    }
    let ihl = usize::from(packet[0] & 0x0f) * 4;
    if ihl < 20 || packet.len() < ihl {
        return None;
    }
    Some(Ipv4Addr::new(packet[16], packet[17], packet[18], packet[19]))
}

fn prefix_mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - u32::from(prefix))
    }
}

#[cfg(target_os = "linux")]
async fn setup_linux_nat(
    tun_name: &str,
    address: Ipv4Addr,
    prefix: u8,
    outbound_interface: Option<&str>,
) -> Result<()> {
    use tokio::process::Command;

    let outbound = if let Some(name) = outbound_interface {
        name.to_string()
    } else {
        let output = Command::new("ip")
            .args(["-4", "route", "get", "1.1.1.1"])
            .output()
            .await
            .context("run 'ip -4 route get 1.1.1.1' to discover outbound interface")?;
        if !output.status.success() {
            bail!(
                "failed to discover VPS outbound interface: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let text = String::from_utf8_lossy(&output.stdout);
        let mut words = text.split_whitespace();
        let mut found = None;
        while let Some(word) = words.next() {
            if word == "dev" {
                found = words.next().map(ToOwned::to_owned);
                break;
            }
        }
        found.context("could not find 'dev <iface>' in 'ip route get' output")?
    };

    run_checked("sysctl", &["-w", "net.ipv4.ip_forward=1"]).await?;

    let network = Ipv4Addr::from(u32::from(address) & prefix_mask(prefix));
    let subnet = format!("{network}/{prefix}");

    ensure_iptables_rule(&[
        "-t", "nat", "POSTROUTING", "-s", &subnet, "-o", &outbound, "-j", "MASQUERADE",
    ])
    .await?;
    ensure_iptables_rule(&[
        "FORWARD", "-i", tun_name, "-o", &outbound, "-j", "ACCEPT",
    ])
    .await?;
    ensure_iptables_rule(&[
        "FORWARD", "-i", &outbound, "-o", tun_name, "-m", "conntrack", "--ctstate",
        "RELATED,ESTABLISHED", "-j", "ACCEPT",
    ])
    .await?;

    info!(
        tun = %tun_name,
        subnet = %subnet,
        outbound = %outbound,
        "enabled Linux IPv4 forwarding and NAT for ToTS TUN gateway"
    );
    Ok(())
}

#[cfg(target_os = "linux")]
async fn run_checked(program: &str, args: &[&str]) -> Result<()> {
    use tokio::process::Command;
    let output = Command::new(program)
        .args(args)
        .output()
        .await
        .with_context(|| format!("run {program}"))?;
    if !output.status.success() {
        bail!(
            "{} {} failed: {}",
            program,
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
async fn ensure_iptables_rule(rule_without_action: &[&str]) -> Result<()> {
    use tokio::process::Command;

    let mut check_args = Vec::with_capacity(rule_without_action.len() + 1);
    let mut add_args = Vec::with_capacity(rule_without_action.len() + 1);

    if rule_without_action.first() == Some(&"-t") {
        if rule_without_action.len() < 4 {
            bail!("internal invalid iptables rule");
        }
        check_args.extend_from_slice(&rule_without_action[..2]);
        check_args.push("-C");
        check_args.extend_from_slice(&rule_without_action[2..]);

        add_args.extend_from_slice(&rule_without_action[..2]);
        add_args.push("-A");
        add_args.extend_from_slice(&rule_without_action[2..]);
    } else {
        check_args.push("-C");
        check_args.extend_from_slice(rule_without_action);
        add_args.push("-A");
        add_args.extend_from_slice(rule_without_action);
    }

    let check = Command::new("iptables")
        .args(&check_args)
        .status()
        .await
        .context("run iptables rule check")?;
    if check.success() {
        return Ok(());
    }

    run_checked("iptables", &add_args).await
}
