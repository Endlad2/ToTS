//! Основная логика сервера.

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use tracing::{info, warn};

use tots_core::obf::Profile;

use crate::auth::Authorizer;
use crate::cli::{Cli, Mode, ObfProfile};
use crate::{tcp, udp};

pub async fn run(cli: Cli) -> Result<()> {
    info!(
        listen = %cli.listen,
        connect = %cli.connect,
        mode = ?cli.mode,
        obf = ?cli.obf_profile,
        "starting tots-server"
    );

    let profile = obf_profile(cli.obf_profile);
    let obf_key = decode_obf_key(profile, cli.obf_key.as_deref())?;

    if matches!(profile, Profile::None) {
        warn!(
            "running with --obf-profile=none: any client reaching {} can relay",
            cli.listen
        );
    }

    let authorizer = match cli.clients_file.as_deref() {
        Some(path) => {
            let a = Authorizer::load(path)
                .await
                .with_context(|| format!("load clients-file {}", path.display()))?;
            info!("client ID authorization enabled");
            Arc::new(a)
        }
        None => {
            warn!("no --clients-file: any client with a valid obf key is admitted");
            Arc::new(Authorizer::empty())
        }
    };

    match cli.mode {
        Mode::Udp => {
            udp::serve(
                cli.listen,
                cli.connect,
                profile,
                obf_key,
                cli.obf_timing,
                authorizer,
            )
            .await
        }
        Mode::Tcp => {
            tcp::serve(
                cli.listen,
                cli.connect,
                profile,
                obf_key,
                cli.obf_timing,
                authorizer,
            )
            .await
        }
    }
}

fn obf_profile(p: ObfProfile) -> Profile {
    match p {
        ObfProfile::None => Profile::None,
        ObfProfile::Rtpopus => Profile::Rtpopus,
        ObfProfile::Rtpopus2 => Profile::Rtpopus2,
        ObfProfile::Rtpopus3 => Profile::Rtpopus3,
    }
}

fn decode_obf_key(profile: Profile, raw: Option<&str>) -> Result<Option<Vec<u8>>> {
    if matches!(profile, Profile::None) {
        return Ok(None);
    }
    let raw = raw.context("--obf-key is required when --obf-profile != none")?;
    let bytes = hex::decode(raw).context("--obf-key: invalid hex")?;
    if bytes.len() != 32 {
        bail!("--obf-key must decode to 32 bytes, got {}", bytes.len());
    }
    Ok(Some(bytes))
}
