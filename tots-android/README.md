# tots-android

`tots-android` is the Android process-form client for ToTS. It reuses the desktop worker/provider architecture but does not create or configure a TUN interface itself.

The APK host must create the interface with `VpnService.Builder`, configure the ToTS virtual IPv4/MTU, and pass an inherited descriptor with `--tun-fd`. If the host starts ToTS with `exec`, it must ensure the duplicated TUN descriptor is inheritable (not closed by `FD_CLOEXEC`).

Example argv:

```text
libtots.so --config tots://... --n 20 --workers_percent {"vk":50,"jazz":50} --relay-only --tun-fd 42
```

Build arm64-v8a:

```bash
export ANDROID_NDK_HOME=/path/to/android-ndk
../scripts/build-android.sh aarch64-linux-android
```

The final file is `../dist/android/arm64-v8a/libtots.so`. It is a renamed PIE executable, not a JNI shared-library API.
