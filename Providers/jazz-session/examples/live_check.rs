use jazz_session::{create_call, join_call};
use serde_json::{json, Value};
use std::time::Duration;

fn summary(data: &Value) -> Value {
    let servers = data["webrtc"]["ice_servers"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let turn = servers
        .iter()
        .filter(|server| {
            server["username"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
                && server["credential"]
                    .as_str()
                    .is_some_and(|value| !value.is_empty())
        })
        .count();
    let mut result = json!({"status":data["status"],"ice_server_entries":servers.len(),"turn_credentials_entries":turn});
    for peer in ["publisher", "subscriber"] {
        result[peer] = json!({"connection_state":data["webrtc"][peer]["connection_state"],
            "local_sdp_bytes":data["webrtc"][peer]["local_description"]["sdp"].as_str().map_or(0,str::len),
            "remote_sdp_bytes":data["webrtc"][peer]["remote_description"]["sdp"].as_str().map_or(0,str::len),
            "local_candidates":data["webrtc"][peer]["local_ice_candidates"].as_array().map_or(0,Vec::len),
            "remote_candidates":data["webrtc"][peer]["remote_ice_candidates"].as_array().map_or(0,Vec::len),
            "relay_candidates":data["webrtc"][peer]["local_ice_candidates"].as_array().map_or(0, |items| items.iter().filter(|item| item["candidate"].as_str().is_some_and(|value|value.contains(" typ relay"))).count()),
            "local_transport_parameters_complete":complete_transport(&data["webrtc"][peer]["local_description"]),
            "remote_transport_parameters_complete":complete_transport(&data["webrtc"][peer]["remote_description"])});
    }
    result
}

fn complete_transport(description: &Value) -> bool {
    description["transport_parameters"]["media"]
        .as_array()
        .is_some_and(|items| {
            !items.is_empty()
                && items.iter().all(|item| {
                    ["ice_ufrag", "ice_password", "dtls_setup"]
                        .iter()
                        .all(|key| item[key].as_str().is_some_and(|value| !value.is_empty()))
                        && item["fingerprints"]
                            .as_array()
                            .is_some_and(|values| !values.is_empty())
                })
        })
}

fn connected(data: &Value) -> bool {
    ["publisher", "subscriber"]
        .iter()
        .all(|peer| data["webrtc"][peer]["connection_state"] == "connected")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var("JAZZ_LIVE_TEST").as_deref() != Ok("1") {
        return Err("Для создания реального тестового звонка задайте JAZZ_LIVE_TEST=1".into());
    }
    eprintln!("Создание тестового звонка...");
    let first = create_call("Rust Jazz test — creator").await?;
    let initial = first.json().await;
    eprintln!("Создание и первый вход выполнены; повторный вход...");
    let link = initial["call"]["join_url"].as_str().ok_or("Нет ссылки")?;
    let second = match join_call(link, "Rust Jazz test — guest").await {
        Ok(second) => second,
        Err(error) => {
            let _ = first.close().await;
            return Err(error.into());
        }
    };
    let result = async {
        for _ in 0..80 {
            let a = first.json().await;
            let b = second.json().await;
            if connected(&a) && connected(&b) { break; }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        let a = first.json().await;
        let b = second.json().await;
        let report = json!({"create_call":summary(&a),"join_call":summary(&b),
            "same_room":a["call"]["room_id"]==b["call"]["room_id"],
            "distinct_participants":a["session"]["participant"]["participantId"]!=b["session"]["participant"]["participantId"]});
        println!("{}",serde_json::to_string_pretty(&report)?);
        if let Ok(dir) = std::env::var("JAZZ_PRIVATE_OUTPUT_DIR") {
            std::fs::create_dir_all(&dir)?;
            for (name, data) in [("creator.private.json",&a),("guest.private.json",&b)] {
                use std::io::Write;
                let mut options = std::fs::OpenOptions::new();
                options.write(true).create_new(true);
                #[cfg(unix)] { use std::os::unix::fs::OpenOptionsExt; options.mode(0o600); }
                let mut file = options.open(std::path::Path::new(&dir).join(name))?;
                file.write_all(serde_json::to_string_pretty(data)?.as_bytes())?;
            }
        }
        if report["same_room"] != true || report["distinct_participants"] != true
            || report["create_call"]["turn_credentials_entries"].as_u64().unwrap_or(0)==0
            || report["join_call"]["turn_credentials_entries"].as_u64().unwrap_or(0)==0 {
            return Err::<(),Box<dyn std::error::Error>>("Live-проверка не получила требуемые данные".into());
        }
        for item in ["create_call","join_call"] {
            for peer in ["publisher", "subscriber"] {
                let data = &report[item][peer];
                if data["connection_state"] != "connected" ||
                    data["local_transport_parameters_complete"] != true ||
                    data["remote_transport_parameters_complete"] != true ||
                    data["relay_candidates"].as_u64().unwrap_or(0) == 0 {
                    return Err("Live-проверка: transport, SDP/ICE/DTLS или TURN allocation не подтверждены".into());
                }
            }
        }
        Ok(())
    }.await;
    let close_second = second.close().await;
    let close_first = first.close().await;
    eprintln!("Обе тестовые сессии завершены.");
    result?;
    close_second?;
    close_first?;
    Ok(())
}
