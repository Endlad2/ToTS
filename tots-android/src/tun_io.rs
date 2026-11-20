use anyhow::{bail, Context, Result};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use tokio::io::unix::AsyncFd;

pub struct AndroidTun {
    fd: AsyncFd<OwnedFd>,
    original_fd: RawFd,
}

impl AndroidTun {
    pub fn original_fd(&self) -> RawFd {
        self.original_fd
    }

    async fn read_packet(&self, buffer: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let mut ready = self.fd.readable().await?;
            match ready.try_io(|inner| loop {
                let result = unsafe {
                    libc::read(
                        inner.get_ref().as_raw_fd(),
                        buffer.as_mut_ptr().cast(),
                        buffer.len(),
                    )
                };
                if result >= 0 {
                    return Ok(result as usize);
                }
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }

    async fn write_packet(&self, packet: &[u8]) -> std::io::Result<()> {
        if packet.is_empty() {
            return Ok(());
        }
        loop {
            let mut ready = self.fd.writable().await?;
            match ready.try_io(|inner| loop {
                let result = unsafe {
                    libc::write(
                        inner.get_ref().as_raw_fd(),
                        packet.as_ptr().cast(),
                        packet.len(),
                    )
                };
                if result >= 0 {
                    let written = result as usize;
                    if written == packet.len() {
                        return Ok(());
                    }
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        format!("partial TUN packet write: {written}/{} bytes", packet.len()),
                    ));
                }
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(error);
            }) {
                Ok(result) => return result,
                Err(_would_block) => continue,
            }
        }
    }
}

/// Takes an inherited Android VpnService TUN descriptor.
///
/// The descriptor is duplicated before ToTS takes ownership, so the host keeps
/// ownership of the descriptor it passed. Android TUN descriptors can be
/// non-blocking; AsyncFd handles EAGAIN through epoll instead of treating it as
/// a fatal Tokio file-read error. O_NONBLOCK is an open-file-description flag,
/// so the host must not concurrently read/write the descriptor after handoff.
/// The host must establish/configure the VpnService interface (address, MTU and
/// routes) before starting this binary.
pub async fn open(fd: Option<i32>) -> Result<AndroidTun> {
    let fd = fd.context(
        "--tun-fd is required on Android (create the TUN with VpnService.Builder and pass its inherited fd)",
    )?;
    if fd < 0 {
        bail!("invalid --tun-fd");
    }

    let original_flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if original_flags < 0 {
        return Err(std::io::Error::last_os_error()).context(
            "--tun-fd is not open in the Android process; ensure the VpnService fd is inherited across exec",
        );
    }

    let duplicated = unsafe { libc::dup(fd) };
    if duplicated < 0 {
        return Err(std::io::Error::last_os_error()).context("dup Android TUN fd");
    }
    let owned = unsafe { OwnedFd::from_raw_fd(duplicated) };

    let status_flags = unsafe { libc::fcntl(owned.as_raw_fd(), libc::F_GETFL) };
    if status_flags < 0 {
        return Err(std::io::Error::last_os_error()).context("read Android TUN fd flags");
    }
    if status_flags & libc::O_NONBLOCK == 0 {
        let result = unsafe {
            libc::fcntl(
                owned.as_raw_fd(),
                libc::F_SETFL,
                status_flags | libc::O_NONBLOCK,
            )
        };
        if result < 0 {
            return Err(std::io::Error::last_os_error())
                .context("set Android TUN fd non-blocking for epoll");
        }
    }

    let descriptor_flags = unsafe { libc::fcntl(owned.as_raw_fd(), libc::F_GETFD) };
    if descriptor_flags >= 0 {
        let _ = unsafe {
            libc::fcntl(
                owned.as_raw_fd(),
                libc::F_SETFD,
                descriptor_flags | libc::FD_CLOEXEC,
            )
        };
    }

    let async_fd = AsyncFd::new(owned).context("register Android TUN fd with epoll")?;
    Ok(AndroidTun {
        fd: async_fd,
        original_fd: fd,
    })
}

pub async fn run(
    tun: &mut AndroidTun,
    manager: std::sync::Arc<crate::workers::WorkerManager>,
    mut inbound: tokio::sync::mpsc::Receiver<Vec<u8>>,
) -> Result<()> {
    let tx_tun = &*tun;
    let rx_tun = &*tun;
    let tx_manager = manager.clone();
    let stats = manager.stats.clone();

    let tx = async move {
        let mut buffer = vec![0u8; 65535];
        loop {
            let length = tx_tun.read_packet(&mut buffer).await?;
            if length == 0 {
                bail!("Android TUN fd closed unexpectedly (read returned EOF)");
            }
            tx_manager.send_ip(&buffer[..length]).await?;
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    };

    let rx = async move {
        while let Some(packet) = inbound.recv().await {
            rx_tun.write_packet(&packet).await?;
            stats
                .tun_rx_bytes
                .fetch_add(packet.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        Err::<(), anyhow::Error>(anyhow::anyhow!("worker inbound queue closed"))
    };

    tokio::select! {
        result = tx => result?,
        result = rx => result?,
    }
    Ok(())
}
