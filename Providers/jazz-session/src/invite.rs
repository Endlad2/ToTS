use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use url::Url;

pub(crate) struct Invite {
    pub origin: Url,
    pub room_id: String,
    pub password: String,
}

impl Invite {
    pub fn parse(link: &str) -> Result<Self, String> {
        let outer = Url::parse(link).map_err(|_| "Некорректная ссылка Jazz")?;
        if outer.scheme() != "https"
            || outer.host_str().is_none()
            || !outer.username().is_empty()
            || outer.password().is_some()
        {
            return Err("Нужна HTTPS-ссылка Jazz без пользовательских данных в адресе".into());
        }
        let effective = if let Some(fragment) = outer.fragment() {
            if !fragment.starts_with("/calls/") || outer.query().is_some() {
                return Err("Неизвестный формат ссылки Jazz".into());
            }
            outer
                .join(fragment)
                .map_err(|_| "Некорректная ссылка Jazz")?
        } else {
            outer.clone()
        };
        let segments: Vec<_> = effective
            .path_segments()
            .ok_or("Некорректный путь звонка")?
            .filter(|part| !part.is_empty())
            .collect();
        let room_id = match segments.as_slice() {
            ["calls", id] | [id] => *id,
            _ => return Err("Неизвестный путь звонка Jazz".into()),
        };
        if room_id.len() > 128
            || room_id.is_empty()
            || !room_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err("Некорректный код звонка Jazz".into());
        }
        let passwords: Vec<_> = effective
            .query_pairs()
            .filter(|(key, _)| key == "psw")
            .map(|(_, value)| value.into_owned())
            .collect();
        if passwords.len() != 1 {
            return Err("В ссылке нужен один параметр psw".into());
        }
        let encoded = passwords[0].trim_end_matches('=');
        if encoded.len() > 512 {
            return Err("Некорректный пароль в ссылке".into());
        }
        let decoded = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| "Некорректный пароль в ссылке")?;
        let length = decoded
            .first()
            .and_then(|b| (*b as char).to_digit(36))
            .filter(|length| *length > 0)
            .ok_or("Некорректная длина пароля")? as usize;
        if decoded.len() <= length {
            return Err("Пароль в ссылке обрезан".into());
        }
        let key = b"sberdevices-prefix";
        let raw: Vec<_> = decoded[1..=length]
            .iter()
            .enumerate()
            .map(|(i, byte)| byte ^ key.get(i).copied().unwrap_or(b'-'))
            .collect();
        let password = String::from_utf8(raw).map_err(|_| "Некорректная кодировка пароля")?;
        let origin = Url::parse(&format!("{}/", outer.origin().ascii_serialization()))
            .map_err(|_| "Некорректный адрес Jazz")?;
        Ok(Self {
            origin,
            room_id: room_id.to_owned(),
            password,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_documented_invite_password() {
        let invite =
            Invite::parse("https://salutejazz.ru/calls/tool-5g1c28?psw=OAMGUUtdChkGEwFHFEkdCgkZHA")
                .unwrap();
        assert_eq!(invite.room_id, "tool-5g1c28");
        assert_eq!(invite.password, "pd499ooo");
        assert_eq!(invite.origin.as_str(), "https://salutejazz.ru/");
    }

    #[test]
    fn accepts_legacy_fragment_link() {
        let invite = Invite::parse(
            "https://jazz.sber.ru/#/calls/tool-5g1c28?psw=OAMGUUtdChkGEwFHFEkdCgkZHA",
        )
        .unwrap();
        assert_eq!(invite.room_id, "tool-5g1c28");
        assert_eq!(invite.password, "pd499ooo");
    }

    #[test]
    fn rejects_untrusted_link_shapes() {
        for link in [
            "file:///calls/abc?psw=AA",
            "http://salutejazz.ru/abc?psw=AA",
            "https://user:secret@salutejazz.ru/abc?psw=AA",
            "https://salutejazz.ru/calls/abc?psw=%%%",
            "https://salutejazz.ru/calls/abc?psw=MA",
            "https://salutejazz.ru/calls/abc?psw=OAMGUUtdChkGEwFHFEkdCgkZHA&psw=other",
            "https://salutejazz.ru/calls/abc/extra?psw=AA",
        ] {
            assert!(Invite::parse(link).is_err(), "Accepted invalid link");
        }
    }
}
