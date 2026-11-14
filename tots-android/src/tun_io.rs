use anyhow::{bail, Context, Result};
use std::os::fd::{FromRawFd, RawFd};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct AndroidTun {
    file: tokio::fs::File,
    original_fd: RawFd,
}

impl AndroidTun {
    pub fn original_fd(&self) -> RawFd {
        self.original_fd
    }
}

/// Takes an inherited Android VpnService TUN descriptor.
///
/// The descriptor is duplicated before ToTS takes ownership, so the host keeps
/// ownership of the descriptor it passed. The host must establish/configure the
/// VpnService interface (address, MTU and routes) before starting this binary.
pub async fn open(fd: Option<i32>) -> Result<AndroidTun> {
    let fd = fd.context(
        "--tun-fd is required on Android (create the TUN with VpnService.Builder and pass its inherited fd)",
    )?;
    if fd < 0 {
        bail!("invalid --tun-fd");
    }

    let duplicated = unsafe { libc::dup(fd) };
    if duplicated < 0 {
        return Err(std::io::Error::last_os_error()).context("dup Android TUN fd");
    }

    let file = unsafe { std::fs::File::from_raw_fd(duplicated) };
    Ok(AndroidTun {
        file: tokio::fs::File::from_std(file),
        original_fd: fd,
    })
}

pub async fn run(
    tun: &mut AndroidTun,
    manager: std::sync::Arc<crate::workers::WorkerManager>,
    inbound: tokio::sync::mpsc::Receiver<Vec<u8>>,
) -> Result<()> {
    run_io(&mut tun.file, manager, inbound).await
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

    tokio::select! {
        result = tx => result?,
        result = rx => result?,
    }
    Ok(())
}
