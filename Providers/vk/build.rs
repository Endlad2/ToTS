use std::{env, fs, path::PathBuf, process::Command};

fn go_arch(target_arch: &str) -> &'static str {
    match target_arch {
        "x86_64" => "amd64",
        "x86" | "i586" | "i686" => "386",
        "aarch64" => "arm64",
        "arm" => "arm",
        other => panic!("unsupported Rust target architecture for VK captcha helper: {other}"),
    }
}

fn go_os(target_os: &str) -> &'static str {
    match target_os {
        "windows" => "windows",
        "linux" => "linux",
        "macos" => "darwin",
        other => panic!("unsupported Rust target OS for VK captcha helper: {other}"),
    }
}

fn main() {
    println!("cargo:rerun-if-changed=captcha-helper/main.go");
    println!("cargo:rerun-if-changed=captcha-helper/go.mod");
    println!("cargo:rerun-if-changed=captcha-helper/go.sum");
    println!("cargo:rerun-if-env-changed=GO");

    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let helper_dir = manifest_dir.join("captcha-helper");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    let target_os = env::var("CARGO_CFG_TARGET_OS").expect("CARGO_CFG_TARGET_OS");
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").expect("CARGO_CFG_TARGET_ARCH");
    let exe_name = if target_os == "windows" { "vk-captcha-helper.exe" } else { "vk-captcha-helper" };
    let output = out_dir.join(exe_name);
    let go = env::var_os("GO").unwrap_or_else(|| "go".into());

    let status = Command::new(go)
        .current_dir(&helper_dir)
        .env("GOOS", go_os(&target_os))
        .env("GOARCH", go_arch(&target_arch))
        .env("CGO_ENABLED", "0")
        .arg("build")
        .arg("-buildvcs=false")
        .arg("-trimpath")
        .arg("-ldflags=-s -w")
        .arg("-o")
        .arg(&output)
        .arg(".")
        .status()
        .unwrap_or_else(|err| panic!("failed to execute Go compiler for VK captcha helper: {err}"));

    if !status.success() {
        panic!("Go build failed for VK captcha helper with status {status}");
    }
    let canonical = fs::canonicalize(&output)
        .unwrap_or_else(|err| panic!("could not resolve built VK captcha helper {}: {err}", output.display()));
    println!("cargo:rustc-env=VK_CAPTCHA_HELPER_PATH={}", canonical.display());
}
