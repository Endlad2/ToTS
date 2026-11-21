# tots-android

`tots-android` is the Android process-form client for ToTS. It reuses the desktop worker/provider architecture but does not create or configure a TUN interface itself.

The APK host must create the interface with `VpnService.Builder` and configure the ToTS virtual IPv4/MTU. Prefer passing the descriptor over an abstract local socket with `SCM_RIGHTS` using `--tun-fd-socket <name>`. This is reliable with Android `ProcessBuilder`, which may close arbitrary non-standard descriptors when starting a child process. The host creates `LocalServerSocket` before starting ToTS, then sends the TUN `FileDescriptor` on the first socket write. The older `--tun-fd <number>` mode remains available only when the launcher can guarantee descriptor inheritance.

ToTS owns the received descriptor, sets `O_NONBLOCK`, and registers it with epoll; this avoids treating Android TUN `EAGAIN` as a fatal file I/O error. A zero-length read is treated as an explicit TUN-closed error rather than a silent successful exit.

Example argv:

```text
libtots.so --config tots://... --n 20 --workers_percent {"vk":50,"jazz":50} --relay-only --tun-fd-socket lalune-tots-random
```

Build arm64-v8a:

```bash
export ANDROID_NDK_HOME=/path/to/android-ndk
../scripts/build-android.sh aarch64-linux-android
```

Normal startup does not wait for all workers to pass the health probe. It starts the TUN loop immediately and queues the first outbound packet until a worker is ready; `--probe-only` is the explicit slow readiness diagnostic. The final file is `../dist/android/arm64-v8a/libtots.so`. It is a renamed PIE executable, not a JNI shared-library API.
