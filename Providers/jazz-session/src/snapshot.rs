use serde_json::{json, Value};

pub(crate) fn sdp_parameters(sdp: &str) -> Value {
    let mut session = serde_json::Map::new();
    let mut media = Vec::<serde_json::Map<String, Value>>::new();
    for line in sdp.lines().map(str::trim) {
        if let Some(value) = line.strip_prefix("m=") {
            let parts: Vec<_> = value.split_whitespace().collect();
            let mut section = serde_json::Map::new();
            section.insert("kind".into(), json!(parts.first()));
            section.insert("protocol".into(), json!(parts.get(2)));
            media.push(section);
            continue;
        }
        let scope = media.last_mut().unwrap_or(&mut session);
        for (prefix, key) in [
            ("a=ice-ufrag:", "ice_ufrag"),
            ("a=ice-pwd:", "ice_password"),
            ("a=setup:", "dtls_setup"),
            ("a=mid:", "mid"),
        ] {
            if let Some(value) = line.strip_prefix(prefix) {
                scope.insert(key.into(), json!(value));
            }
        }
        if let Some(value) = line.strip_prefix("a=fingerprint:") {
            if let Some((algorithm, fingerprint)) = value.split_once(' ') {
                scope
                    .entry("fingerprints")
                    .or_insert_with(|| json!([]))
                    .as_array_mut()
                    .expect("fingerprint array")
                    .push(json!({"algorithm":algorithm,"value":fingerprint}));
            }
        }
        if let Some(direction) = line.strip_prefix("a=") {
            if ["sendrecv", "sendonly", "recvonly", "inactive"].contains(&direction) {
                scope.insert("direction".into(), json!(direction));
            }
        }
    }
    for section in &mut media {
        for key in [
            "ice_ufrag",
            "ice_password",
            "dtls_setup",
            "fingerprints",
            "direction",
        ] {
            if !section.contains_key(key) {
                if let Some(value) = session.get(key) {
                    section.insert(key.into(), value.clone());
                }
            }
        }
    }
    json!({"session":session,"media":media})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_parameters_respect_media_scope_and_session_inheritance() {
        let data = sdp_parameters(concat!(
            "v=0\r\na=ice-ufrag:session-user\r\na=ice-pwd:session-password\r\n",
            "a=fingerprint:sha-256 AA:BB\r\na=setup:actpass\r\n",
            "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=mid:audio\r\na=sendonly\r\n",
            "m=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:video\r\n",
            "a=ice-ufrag:video-user\r\na=ice-pwd:video-password\r\n",
            "a=fingerprint:sha-256 CC:DD\r\na=setup:active\r\n"
        ));
        assert_eq!(data["media"][0]["ice_ufrag"], "session-user");
        assert_eq!(data["media"][0]["ice_password"], "session-password");
        assert_eq!(data["media"][0]["fingerprints"][0]["value"], "AA:BB");
        assert_eq!(data["media"][0]["direction"], "sendonly");
        assert_eq!(data["media"][1]["ice_ufrag"], "video-user");
        assert_eq!(data["media"][1]["fingerprints"][0]["value"], "CC:DD");
        assert_eq!(data["media"][1]["dtls_setup"], "active");
        assert_eq!(data["media"][1]["mid"], "video");
    }
}
