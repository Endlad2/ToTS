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

/// Takes an Android VpnService TUN descriptor, preferably transferred with
/// SCM_RIGHTS over `--tun-fd-socket`. The legacy inherited `--tun-fd` path is
/// retained for launchers that guarantee descriptor inheritance.
///
/// Android TUN descriptors can be non-blocking; AsyncFd handles EAGAIN through
/// epoll instead of treating it as a fatal Tokio file-read error. O_NONBLOCK is
/// an open-file-description flag, so the host must not concurrently read/write
/// the descriptor after handoff. The host must establish/configure the
/// VpnService interface (address, MTU and routes) before starting this binary.
pub async fn open(fd: Option<i32>, fd_socket: Option<&str>) -> Result<AndroidTun> {
    let (owned, original_fd) = match (fd, fd_socket) {
        (Some(_), Some(_)) => bail!("use either --tun-fd or --tun-fd-socket, not both"),
        (Some(fd), _) => {
            if fd < 0 {
                bail!("invalid --tun-fd");
            }
            let original_flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            if original_flags < 0 {
                return Err(std::io::Error::last_os_error()).context(
                    "--tun-fd is not open in the Android process; use --tun-fd-socket when launching through Android ProcessBuilder",
                );
            }
            let duplicated = unsafe { libc::dup(fd) };
            if duplicated < 0 {
                return Err(std::io::Error::last_os_error()).context("dup Android TUN fd");
            }
            (unsafe { OwnedFd::from_raw_fd(duplicated) }, fd)
        }
        (None, Some(name)) => {
            let owned = receive_fd_from_abstract_socket(name)
                .context("receive Android VpnService TUN fd over SCM_RIGHTS")?;
            let received_fd = owned.as_raw_fd();
            (owned, received_fd)
        }
        (None, None) => bail!(
            "--tun-fd or --tun-fd-socket is required on Android (create the TUN with VpnService.Builder)"
        ),
    };

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
        original_fd,
    })
}

/// Receive a descriptor from the Android VpnService through an abstract local
/// socket. This avoids relying on Java ProcessBuilder preserving arbitrary fds
/// across exec, which is not guaranteed on Android.
#[cfg(unix)]
fn receive_fd_from_abstract_socket(name: &str) -> Result<OwnedFd> {
    use std::{mem, ptr};

    if name.is_empty() || !name.is_ascii() || name.as_bytes().contains(&0) {
        bail!("invalid Android TUN fd socket name");
    }

    let mut address: libc::sockaddr_un = unsafe { mem::zeroed() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    let name_bytes = name.as_bytes();
    if name_bytes.len() + 1 > address.sun_path.len() {
        bail!("Android TUN fd socket name is too long");
    }
    // Linux/Android abstract namespace: leading NUL, then the exact name.
    address.sun_path[0] = 0;
    for (index, byte) in name_bytes.iter().enumerate() {
        address.sun_path[index + 1] = *byte as libc::c_char;
    }

    let socket_fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
    if socket_fd < 0 {
        return Err(std::io::Error::last_os_error()).context("create Android TUN fd socket");
    }
    let socket = unsafe { OwnedFd::from_raw_fd(socket_fd) };
    let address_len = (mem::size_of::<libc::sa_family_t>() + 1 + name_bytes.len())
        as libc::socklen_t;
    let connected = unsafe {
        libc::connect(
            socket.as_raw_fd(),
            (&address as *const libc::sockaddr_un).cast::<libc::sockaddr>(),
            address_len,
        )
    };
    if connected < 0 {
        return Err(std::io::Error::last_os_error()).context("connect Android TUN fd socket");
    }

    let mut marker = 0u8;
    let mut iovec = libc::iovec {
        iov_base: (&mut marker as *mut u8).cast(),
        iov_len: 1,
    };
    let alignment = mem::size_of::<usize>();
    let header_size = (mem::size_of::<libc::cmsghdr>() + alignment - 1) & !(alignment - 1);
    let control_size = (header_size + mem::size_of::<RawFd>() + alignment - 1)
        & !(alignment - 1);
    let mut control = vec![0usize; control_size.div_ceil(alignment)];
    let mut message: libc::msghdr = unsafe { mem::zeroed() };
    message.msg_iov = &mut iovec;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = control_size;

    let received = unsafe {
        libc::recvmsg(
            socket.as_raw_fd(),
            &mut message,
            libc::MSG_CMSG_CLOEXEC,
        )
    };
    if received != 1 {
        if received < 0 {
            return Err(std::io::Error::last_os_error()).context("read Android TUN fd handoff");
        }
        bail!("Android TUN fd handoff closed without a descriptor");
    }
    if marker != 1 {
        bail!("Android TUN fd handoff contained an invalid handshake marker");
    }
    if message.msg_flags & libc::MSG_CTRUNC != 0 {
        bail!("Android TUN fd handoff control message was truncated");
    }

    let header = message.msg_control.cast::<libc::cmsghdr>();
    if header.is_null() {
        bail!("Android TUN fd handoff did not contain ancillary data");
    }
    let header_ref = unsafe { &*header };
    let minimum_len = header_size + mem::size_of::<RawFd>();
    if message.msg_controllen < minimum_len
        || header_ref.cmsg_len > message.msg_controllen
        || header_ref.cmsg_level != libc::SOL_SOCKET
        || header_ref.cmsg_type != libc::SCM_RIGHTS
        || header_ref.cmsg_len < minimum_len
    {
        bail!("Android TUN fd handoff contained an unexpected control message");
    }
    let fd_ptr = unsafe { (header.cast::<u8>().add(header_size)).cast::<RawFd>() };
    let received_fd = unsafe { ptr::read_unaligned(fd_ptr) };
    if received_fd < 0 {
        bail!("Android TUN fd handoff returned an invalid descriptor");
    }
    Ok(unsafe { OwnedFd::from_raw_fd(received_fd) })
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
