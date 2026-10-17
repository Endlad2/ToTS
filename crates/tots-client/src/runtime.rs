//! Основная логика клиента.

use anyhow::{bail, Context, Result};
use tots_core::obf::Profile;
use tots_providers::vk::{VkConfig, VkProvider};
use tots_providers::TurnProvider;
use tracing::info;

use crate::cli::{Cli, Mode, ObfProfile, Provider};
use crate::session;

pub async fn run(cli: Cli) -> Result<()> {
    let client_id = cli
        .client_id
        .clone()
        .unwrap_or_else(|| hex::encode(rand::random::<[u8; 16]>()));

    info!(
        peer = %cli.peer,
        listen = %cli.listen,
        mode = ?cli.mode,
        streams = cli.streams,
        client_id = %client_id,
        "starting tots-client"
    );

    let profile = obf_profile(cli.obf_profile);
    let obf_key = decode_obf_key(profile, cli.obf_key.as_deref())?;

    match cli.provider {
        Provider::Direct => {
            info!("provider=direct: connecting straight to peer");
            session::run_direct(
                cli.peer,
                cli.listen,
                cli.streams,
                profile,
                obf_key,
                cli.obf_timing,
                client_id,
                cli.mode,
                cli.debug,
            )
            .await
        }
        Provider::Vk => {
            let link = cli
                .link
                .as_deref()
                .context("--link is required for --provider vk")?;

            let mut cfg = VkConfig::new(link);
            cfg.streams_per_cache = cli.streams_per_cred;
            cfg.disable_captcha_solver = cli.no_captcha_solver;

            let provider = VkProvider::new(cfg).context("init vk provider")?;

            match provider.get_credentials(1).await {
                Ok(_) => info!("vk provider ready"),
                Err(e) => {
                    let msg = format!("{e}");
                    bail!("VK fetch failed: {msg}");
                }
            }

            session::run_vk(
                cli.peer,
                cli.listen,
                cli.streams,
                profile,
                obf_key,
                cli.obf_timing,
                client_id,
                cli.mode,
                cli.transport,
                provider,
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

#[allow(dead_code)]
fn _assert_mode_used(_: Mode) {}
