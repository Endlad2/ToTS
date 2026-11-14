use std::{
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

const PACKAGE: &str = "tots-android";
const BINARY: &str = "tots-android";
const APK_NAME: &str = "libtots.so";

fn main() -> ExitCode {
    // Cargo executes build.rs before the final crate artifact exists, so a normal
    // Cargo build-script invocation can only stamp the Android ELF metadata.
    // The same file is also a standalone builder: scripts/build-android.sh compiles
    // and runs it outside Cargo; that mode builds the binary and then moves the
    // finished PIE executable to an APK-safe libtots.so filename.
    if env::var_os("OUT_DIR").is_some() {
        cargo_build_script_mode();
        return ExitCode::SUCCESS;
    }

    match standalone_builder_mode() {
        Ok(path) => {
            println!("{}", path.display());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("tots-android build failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn cargo_build_script_mode() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=TOTS_ANDROID_TARGET");
    println!("cargo:rerun-if-env-changed=TOTS_ANDROID_ABI");

    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("android") {
        // Android executables are PIE/ET_DYN. Give the ELF the same soname as the
        // filename used in jniLibs; scripts/build-android.sh performs the final move.
        println!("cargo:rustc-link-arg-bin={BINARY}=-Wl,-soname,{APK_NAME}");
    }
}

fn standalone_builder_mode() -> Result<PathBuf, Box<dyn Error>> {
    let root = repo_root()?;
    let target = env::var("TOTS_ANDROID_TARGET")
        .unwrap_or_else(|_| "aarch64-linux-android".to_owned());
    let abi = env::var("TOTS_ANDROID_ABI").unwrap_or_else(|_| abi_for_target(&target).to_owned());

    if !target.contains("android") {
        return Err(format!("target {target:?} is not an Android target").into());
    }

    let cargo = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = Command::new(cargo)
        .current_dir(&root)
        .args([
            "build",
            "-p",
            PACKAGE,
            "--target",
            &target,
            "--release",
        ])
        .status()?;
    if !status.success() {
        return Err(format!("cargo build exited with {status}").into());
    }

    let built = root
        .join("target")
        .join(&target)
        .join("release")
        .join(BINARY);
    if !built.is_file() {
        return Err(format!("built Android binary not found at {}", built.display()).into());
    }

    let output = env::var_os("TOTS_ANDROID_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("dist").join("android").join(&abi).join(APK_NAME));
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    if output.exists() {
        fs::remove_file(&output)?;
    }

    // "Rename" the executable into the APK native-library namespace. Preserve
    // a cross-filesystem fallback for CI/custom output directories.
    match fs::rename(&built, &output) {
        Ok(()) => {}
        Err(_) => {
            fs::copy(&built, &output)?;
            fs::remove_file(&built)?;
        }
    }

    Ok(output)
}

fn repo_root() -> Result<PathBuf, Box<dyn Error>> {
    if let Some(root) = env::var_os("TOTS_REPO_ROOT") {
        let root = PathBuf::from(root);
        ensure_repo_root(&root)?;
        return Ok(root);
    }

    let current = env::current_dir()?;
    if ensure_repo_root(&current).is_ok() {
        return Ok(current);
    }

    let mut cursor = env::current_exe()?;
    cursor.pop();
    for ancestor in cursor.ancestors() {
        if ensure_repo_root(ancestor).is_ok() {
            return Ok(ancestor.to_path_buf());
        }
    }
    Err("run the standalone builder from the ToTS repository root or set TOTS_REPO_ROOT".into())
}

fn ensure_repo_root(root: &Path) -> Result<(), Box<dyn Error>> {
    if root.join("Cargo.toml").is_file() && root.join("tots-android").join("Cargo.toml").is_file() {
        Ok(())
    } else {
        Err("not a ToTS repository root".into())
    }
}

fn abi_for_target(target: &str) -> &'static str {
    match target {
        "aarch64-linux-android" => "arm64-v8a",
        "armv7-linux-androideabi" => "armeabi-v7a",
        "x86_64-linux-android" => "x86_64",
        "i686-linux-android" => "x86",
        _ => "unknown",
    }
}
