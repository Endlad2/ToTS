//! Opt-in transport diagnosis: set JAZZ_PRIVATE_URI_FILE and optionally JAZZ_WORKER_INDEX.
//! Prints only allowlisted state labels and aggregate transport counts.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};
use std::{collections::BTreeSet, net::IpAddr, time::Duration};

fn room_from_uri(uri: &str, index: usize) -> Result<(String, String), &'static str> {
    let raw = uri
        .trim_start_matches('\u{feff}')
        .trim()
        .strip_prefix("tots://")
        .ok_or("invalid private URI")?;
    let decoded = URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| "invalid private URI")?;
    let config: Value = serde_json::from_slice(&decoded).map_err(|_| "invalid private URI")?;
    let data = config.get("provider_data").ok_or("missing provider data")?;
    let jazz = data.get("jazz").unwrap_or(data);
    let room = if let Some(rooms) = jazz.get("worker_rooms") {
        rooms
            .as_array()
            .and_then(|rooms| rooms.get(index))
            .ok_or("worker room unavailable")?
    } else {
        if index != 0 {
            return Err("legacy invite has only one worker room");
        }
        jazz
    };
    let field = |key: &str| {
        room.get(key)
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .ok_or("missing worker connection field")
    };
    Ok((field("join_link")?, field("connector_url")?))
}

fn allow(value: &Value, allowed: &[&'static str]) -> &'static str {
    allowed
        .iter()
        .copied()
        .find(|candidate| Some(*candidate) == value.as_str())
        .unwrap_or("unknown")
}

fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, _, _] = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_documentation()
                && a != 0
                && a < 224
                && !(a == 100 && (64..=127).contains(&b))
                && !(a == 198 && (18..=19).contains(&b))
        }
        IpAddr::V6(ip) => {
            !ip.is_loopback()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && !ip.is_unique_local()
                && !ip.is_unicast_link_local()
                && !(ip.segments()[0] == 0x2001 && ip.segments()[1] == 0xdb8)
        }
    }
}

fn increment(counts: &mut Value, key: &str) {
    counts[key] = json!(counts[key].as_u64().unwrap_or(0) + 1);
}

fn candidate_summary(peer: &Value, side: &str) -> Value {
    let mut candidates = BTreeSet::new();
    let description = format!("{side}_description");
    let items = format!("{side}_ice_candidates");
    for line in peer[&description]["sdp"]
        .as_str()
        .unwrap_or("")
        .lines()
        .map(str::trim)
    {
        if let Some(candidate) = line.strip_prefix("a=candidate:") {
            candidates.insert(candidate.to_owned());
        }
    }
    if let Some(items) = peer[&items].as_array() {
        for item in items {
            if let Some(candidate) = item["candidate"].as_str().filter(|value| !value.is_empty()) {
                candidates.insert(
                    candidate
                        .strip_prefix("candidate:")
                        .unwrap_or(candidate)
                        .to_owned(),
                );
            }
        }
    }
    let mut counts = json!({"total":candidates.len(),"host":0,"srflx":0,"prflx":0,"relay":0,"other":0,
        "public_ip":0,"private_or_reserved_ip":0,"unparsed_ip":0});
    for candidate in candidates {
        let fields: Vec<_> = candidate.split_whitespace().collect();
        let kind = fields
            .windows(2)
            .find(|pair| pair[0] == "typ")
            .map(|pair| pair[1]);
        increment(
            &mut counts,
            match kind {
                Some("host") => "host",
                Some("srflx") => "srflx",
                Some("prflx") => "prflx",
                Some("relay") => "relay",
                _ => "other",
            },
        );
        increment(
            &mut counts,
            match fields.get(4).and_then(|ip| ip.parse::<IpAddr>().ok()) {
                Some(ip) if public_ip(ip) => "public_ip",
                Some(_) => "private_or_reserved_ip",
                None => "unparsed_ip",
            },
        );
    }
    counts
}

fn media_summary(description: &Value) -> Value {
    let mut sections: Vec<Value> = Vec::new();
    let mut session_direction = "unknown";
    let mut session_setup = "unknown";
    for line in description["sdp"]
        .as_str()
        .unwrap_or("")
        .lines()
        .map(str::trim)
    {
        if let Some(line) = line.strip_prefix("m=") {
            let fields: Vec<_> = line.split_whitespace().collect();
            let kind = match fields.first().copied() {
                Some("audio") => "audio",
                Some("video") => "video",
                Some("application") => "application",
                _ => "unknown",
            };
            let port = fields
                .get(1)
                .and_then(|value| value.split('/').next())
                .and_then(|port| port.parse::<u16>().ok());
            sections.push(json!({"kind":kind,"port":port,"direction":session_direction,"setup":session_setup}));
        } else if let Some(attribute) = line.strip_prefix("a=") {
            if ["sendrecv", "sendonly", "recvonly", "inactive"].contains(&attribute) {
                let direction = match attribute {
                    "sendrecv" => "sendrecv",
                    "sendonly" => "sendonly",
                    "recvonly" => "recvonly",
                    _ => "inactive",
                };
                if let Some(section) = sections.last_mut() {
                    section["direction"] = json!(direction);
                } else {
                    session_direction = direction;
                }
            } else if let Some(setup) = attribute.strip_prefix("setup:") {
                let setup = match setup {
                    "active" => "active",
                    "passive" => "passive",
                    "actpass" => "actpass",
                    "holdconn" => "holdconn",
                    _ => "unknown",
                };
                if let Some(section) = sections.last_mut() {
                    section["setup"] = json!(setup);
                } else {
                    session_setup = setup;
                }
            }
        }
    }
    Value::Array(sections)
}

fn configuration_summary(snapshot: &Value) -> Value {
    let mut counts = json!({"server_entries":0,"url_count":0,"udp_stun_urls":0,"udp_turn_urls":0,
        "unsupported_urls":0,"credentialed_turn_entries":0});
    if let Some(servers) = snapshot["webrtc"]["ice_servers"].as_array() {
        counts["server_entries"] = json!(servers.len());
        for server in servers {
            let mut urls = BTreeSet::new();
            for key in ["url", "urls"] {
                if let Some(value) = server[key].as_str() {
                    urls.insert(value);
                }
                if let Some(values) = server[key].as_array() {
                    urls.extend(values.iter().filter_map(Value::as_str));
                }
            }
            let mut has_turn = false;
            for url in urls {
                increment(&mut counts, "url_count");
                let url = url.to_ascii_lowercase();
                let udp = !url.contains("transport=tcp");
                let kind = if url.starts_with("turn:") && udp {
                    has_turn = true;
                    "udp_turn_urls"
                } else if url.starts_with("stun:") && udp {
                    "udp_stun_urls"
                } else {
                    "unsupported_urls"
                };
                increment(&mut counts, kind);
            }
            if has_turn
                && ["username", "credential"]
                    .iter()
                    .all(|key| server[*key].as_str().is_some_and(|value| !value.is_empty()))
            {
                increment(&mut counts, "credentialed_turn_entries");
            }
        }
    }
    counts
}

fn safe_candidate(candidate: &Value) -> Value {
    if !candidate.is_object() {
        return Value::Null;
    }
    json!({"candidate_type":allow(&candidate["candidate_type"], &["host","srflx","prflx","relay"]),
        "ip_class":allow(&candidate["ip_class"], &["public","private_or_reserved","unparsed"]),
        "port":candidate["port"].as_u64().filter(|port| *port <= u16::MAX as u64)})
}

fn safe_pair(pair: &Value) -> Value {
    if !pair.is_object() {
        return Value::Null;
    }
    json!({"local":safe_candidate(&pair["local"]),"remote":safe_candidate(&pair["remote"])})
}

fn safe_ice_diagnostics(value: &Value) -> Value {
    let mut states = json!({});
    for state in [
        "waiting",
        "in-progress",
        "succeeded",
        "failed",
        "unspecified",
    ] {
        states[state] = json!(value["pair_states"][state].as_u64().unwrap_or(0));
    }
    let nominated: Vec<_> = value["nominated_pairs"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|pair| {
            let mut summary = safe_pair(pair);
            summary["state"] = json!(allow(
                &pair["state"],
                &[
                    "waiting",
                    "in-progress",
                    "succeeded",
                    "failed",
                    "unspecified"
                ]
            ));
            summary
        })
        .collect();
    json!({"pair_states":states,"nominated_pairs":nominated})
}

fn transport_summary(snapshot: &Value) -> Value {
    let mut report = json!({"session_status":allow(&snapshot["status"], &["connecting","joining","negotiating","ready","closed","failed"]),
        "configuration":configuration_summary(snapshot)});
    for role in ["publisher", "subscriber"] {
        let peer = &snapshot["webrtc"][role];
        report[role] = json!({"connection_state":allow(&peer["connection_state"], &["new","connecting","connected","disconnected","failed","closed"]),
            "ice_state":allow(&peer["ice_state"], &["new","checking","connected","completed","disconnected","failed","closed"]),
            "gathering_state":allow(&peer["gathering_state"], &["new","gathering","complete"]),
            "selected_pair":safe_pair(&peer["selected_pair"]),"ice_diagnostics":safe_ice_diagnostics(&peer["ice_diagnostics"]),
            "local_candidates":candidate_summary(peer,"local"),"remote_candidates":candidate_summary(peer,"remote"),
            "local_media":media_summary(&peer["local_description"]),"remote_media":media_summary(&peer["remote_description"])});
    }
    report
}

async fn diagnose() -> bool {
    let input = (|| {
        let path = std::env::var_os("JAZZ_PRIVATE_URI_FILE")
            .ok_or("set JAZZ_PRIVATE_URI_FILE to opt in")?;
        let uri = std::fs::read_to_string(path).map_err(|_| "private URI file unavailable")?;
        let index = std::env::var("JAZZ_WORKER_INDEX")
            .unwrap_or_else(|_| "0".into())
            .parse::<usize>()
            .map_err(|_| "invalid worker index")?;
        room_from_uri(&uri, index)
    })();
    let (link, connector) = match input {
        Ok(input) => input,
        Err(_) => {
            let mut report = transport_summary(&Value::Null);
            report["phase"] = json!("input");
            report["success"] = json!(false);
            println!("{report}");
            return false;
        }
    };
    let session =
        match jazz_session::join_preconnected(&link, "ToTS transport diagnostic", &connector).await
        {
            Ok(session) => session,
            Err(_) => {
                let mut report = transport_summary(&Value::Null);
                report["phase"] = json!("join");
                report["success"] = json!(false);
                println!("{report}");
                return false;
            }
        };
    let ready = match session.data_plane().await {
        Ok(plane) => plane.wait_ready(Duration::from_secs(20)).await.is_ok(),
        Err(_) => false,
    };
    let snapshot = tokio::time::timeout(Duration::from_secs(5), session.json()).await;
    let available = snapshot.is_ok();
    let mut report = transport_summary(&snapshot.unwrap_or(Value::Null));
    let closed = session.close().await.is_ok();
    report["phase"] = json!("media");
    report["media_ready"] = json!(ready);
    report["snapshot_available"] = json!(available);
    report["closed"] = json!(closed);
    report["success"] = json!(ready && available && closed);
    println!("{report}");
    ready && available && closed
}

#[tokio::main]
async fn main() {
    if !diagnose().await {
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

    #[test]
    fn selects_nested_worker_room_and_rejects_legacy_overflow() {
        let config = json!({"provider_data":{"jazz":{"worker_rooms":[
            {"join_link":"first-secret","connector_url":"first-connector"},
            {"join_link":"second-secret","connector_url":"second-connector"}
        ]}}});
        let uri = format!(
            "tots://{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&config).unwrap())
        );
        assert_eq!(
            room_from_uri(&uri, 1).unwrap(),
            ("second-secret".into(), "second-connector".into())
        );
        assert!(room_from_uri(&uri, 2).is_err());
        let legacy =
            json!({"provider_data":{"join_link":"legacy-secret","connector_url":"connector"}});
        let uri = format!(
            "tots://{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&legacy).unwrap())
        );
        assert_eq!(
            room_from_uri(&uri, 0).unwrap(),
            ("legacy-secret".into(), "connector".into())
        );
        assert!(room_from_uri(&uri, 1).is_err());
    }

    #[test]
    fn summary_counts_transport_without_repeating_any_secret_strings() {
        let snapshot = json!({"status":"ready", "webrtc":{
            "ice_servers":[{"urls":["turn:relay.example:3478?transport=udp","turns:relay.example:5349?transport=tcp"],"username":"secret-user","credential":"secret-password"}],
            "publisher":{
                "connection_state":"connected","ice_state":"connected","gathering_state":"complete",
                "selected_pair":{"local":{"candidate_type":"relay","ip_class":"public","port":3478,"url":"secret-url"},"remote":{"candidate_type":"prflx","ip_class":"public","port":5000,"id":"secret-id"}},
                "ice_diagnostics":{"pair_states":{"succeeded":1,"secret-key":100},"nominated_pairs":[{"state":"succeeded","local":{"candidate_type":"relay","ip_class":"public","port":3478,"url":"secret-url"},"remote":{"candidate_type":"prflx","ip_class":"public","port":5000,"id":"secret-id"}}]},
                "local_description":{"sdp":"v=0\r\na=setup:actpass\r\na=ice-pwd:secret-password\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=sendonly\r\na=candidate:secret-foundation 1 udp 1 192.168.1.2 1234 typ host\r\n"},
                "remote_description":{"sdp":"m=audio 0 UDP/TLS/RTP/SAVPF 111\r\na=inactive\r\na=setup:passive\r\n"},
                "local_ice_candidates":[{"candidate":"candidate:secret-foundation 1 udp 1 192.168.1.2 1234 typ host"},{"candidate":"candidate:x 1 udp 1 8.8.8.8 1234 typ relay"}],
                "remote_ice_candidates":[{"candidate":"candidate:x 1 udp 1 172.17.250.101 30000 typ host"}]
            },
            "subscriber":{"connection_state":"secret-state"}
        }});
        let report = transport_summary(&snapshot);
        let text = report.to_string();
        assert!(!text.contains("secret"));
        assert!(!text.contains("relay.example"));
        assert!(!text.contains("192.168"));
        assert_eq!(report["publisher"]["local_candidates"]["host"], 1);
        assert_eq!(report["publisher"]["local_candidates"]["relay"], 1);
        assert_eq!(report["publisher"]["local_candidates"]["public_ip"], 1);
        assert_eq!(
            report["publisher"]["remote_candidates"]["private_or_reserved_ip"],
            1
        );
        assert_eq!(report["publisher"]["local_media"][0]["kind"], "audio");
        assert_eq!(report["publisher"]["local_media"][0]["setup"], "actpass");
        assert_eq!(report["publisher"]["remote_media"][0]["port"], 0);
        assert_eq!(report["configuration"]["udp_turn_urls"], 1);
        assert_eq!(
            report["publisher"]["selected_pair"]["remote"]["candidate_type"],
            "prflx"
        );
        assert_eq!(
            report["publisher"]["ice_diagnostics"]["nominated_pairs"][0]["remote"]["port"],
            5000
        );
    }
}
