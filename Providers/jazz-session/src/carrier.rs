//! Optional envelopes around opaque ToTS bytes. VP8 mode supplies routing headers,
//! not a VP8 encoder; forwarding by a provider must be verified independently.
use serde_json::{json, Value};
use std::time::Duration;

pub(crate) const VIDEO_WIDTH: u16 = 1280;
pub(crate) const VIDEO_HEIGHT: u16 = 720;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MediaCarrier {
    #[default]
    Audio,
    AudioWallclock,
    Vp8Experimental,
}

impl MediaCarrier {
    pub(crate) fn is_video(self) -> bool {
        self == Self::Vp8Experimental
    }
    pub(crate) fn track_type(self) -> &'static str {
        if self.is_video() {
            "VIDEO"
        } else {
            "AUDIO"
        }
    }
    pub(crate) fn clock_rate(self) -> u32 {
        if self.is_video() {
            90_000
        } else {
            48_000
        }
    }
    pub(crate) fn uses_wallclock(self) -> bool {
        self != Self::Audio
    }

    pub(crate) fn wrap_fragment(
        self,
        fragment: &[u8],
        first: bool,
        frame_len: usize,
        picture: u16,
    ) -> Vec<u8> {
        if !self.is_video() {
            return fragment.to_vec();
        }
        let mut payload = Vec::with_capacity(fragment.len() + if first { 14 } else { 4 });
        payload.extend_from_slice(&[
            if first { 0x90 } else { 0x80 },
            0x80,
            0x80 | ((picture >> 8) as u8 & 0x7f),
            picture as u8,
        ]);
        if first {
            // Header shape only: keyframe tag, sync code and dimensions; opaque bytes follow.
            let tag = ((frame_len as u32) << 5) | 0x10;
            payload.extend_from_slice(&tag.to_le_bytes()[..3]);
            payload.extend_from_slice(&[0x9d, 0x01, 0x2a]);
            payload.extend_from_slice(&VIDEO_WIDTH.to_le_bytes());
            payload.extend_from_slice(&VIDEO_HEIGHT.to_le_bytes());
        }
        payload.extend_from_slice(fragment);
        payload
    }

    pub(crate) fn unwrap_fragment(self, payload: &[u8]) -> Option<&[u8]> {
        if !self.is_video() {
            return Some(payload);
        }
        let descriptor = *payload.first()?;
        if descriptor & 0x4f != 0 {
            return None;
        } // reserved bits and nonzero partition IDs
        let mut offset = 1;
        if descriptor & 0x80 != 0 {
            let flags = *payload.get(offset)?;
            offset += 1;
            if flags & 0x0f != 0 {
                return None;
            }
            if flags & 0x80 != 0 {
                let picture = *payload.get(offset)?;
                offset += 1;
                if picture & 0x80 != 0 {
                    payload.get(offset)?;
                    offset += 1;
                }
            }
            if flags & 0x40 != 0 {
                payload.get(offset)?;
                offset += 1;
            }
            if flags & 0x30 != 0 {
                payload.get(offset)?;
                offset += 1;
            }
        }
        if descriptor & 0x10 != 0 {
            let header = payload.get(offset..offset + 10)?;
            if header[0] & 0x1f != 0x10
                || header[3..6] != [0x9d, 0x01, 0x2a]
                || u16::from_le_bytes([header[6], header[7]]) & 0x3fff != VIDEO_WIDTH
                || u16::from_le_bytes([header[8], header[9]]) & 0x3fff != VIDEO_HEIGHT
            {
                return None;
            }
            offset += 10;
        }
        let fragment = payload.get(offset..)?;
        fragment.starts_with(b"TOTS\x01").then_some(fragment)
    }

    pub(crate) fn track_request(self, cid: &str) -> Value {
        if !self.is_video() {
            return json!({"cid":cid,"type":"AUDIO","source":"MICROPHONE","muted":false});
        }
        json!({"cid":cid,"type":"VIDEO","source":"CAMERA","muted":false,
            "width":VIDEO_WIDTH,"height":VIDEO_HEIGHT,
            "layers":[{"quality":"HIGH","width":VIDEO_WIDTH,"height":VIDEO_HEIGHT,"bitrate":2_000_000}],
            "simulcastCodecs":[{"codec":"vp8","cid":cid}]})
    }
}

pub(crate) fn wallclock_timestamp(elapsed: Duration, rate: u32, last: u64) -> u64 {
    let ticks = (elapsed.as_nanos().saturating_mul(rate as u128) / 1_000_000_000)
        .min(u64::MAX as u128) as u64;
    ticks.max(last.saturating_add(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn video_envelope_preserves_opaque_fragment_bytes_and_marks_frame_start() {
        let carrier = MediaCarrier::Vp8Experimental;
        let opaque = b"TOTS\x01opaque encrypted bytes";
        let first = carrier.wrap_fragment(opaque, true, opaque.len() * 2, 0x1234);
        assert_eq!(first[0], 0x90, "missing VP8 start descriptor");
        assert_eq!(&first[7..10], &[0x9d, 0x01, 0x2a]);
        assert_eq!(carrier.unwrap_fragment(&first).unwrap(), opaque);
        let last = carrier.wrap_fragment(opaque, false, opaque.len() * 2, 0x1234);
        assert_eq!(last[0], 0x80);
        assert_eq!(carrier.unwrap_fragment(&last).unwrap(), opaque);
    }
    #[test]
    fn video_envelope_rejects_malformed_headers_without_panicking() {
        let carrier = MediaCarrier::Vp8Experimental;
        for packet in [
            vec![],
            vec![0x80],
            vec![0x80, 0x80],
            vec![0x80, 0x80, 0x80],
            vec![0x90, 0x80, 0x80, 1],
            b"TOTS raw audio carrier".to_vec(),
            vec![0x90, 0xf0, 0x80, 1],
        ] {
            assert!(carrier.unwrap_fragment(&packet).is_none());
        }
        let good = carrier.wrap_fragment(b"TOTS\x01encrypted bytes", true, 20, 1);
        for len in 0..good.len() {
            if len < 19 {
                assert!(carrier.unwrap_fragment(&good[..len]).is_none());
            }
        }
        let mut corrupt = good;
        corrupt[7] ^= 1;
        assert!(carrier.unwrap_fragment(&corrupt).is_none());
    }
    #[test]
    fn audio_envelope_remains_exactly_unchanged() {
        let payload = b"TOTS\x01audio body";
        assert_eq!(
            MediaCarrier::Audio.wrap_fragment(payload, true, payload.len(), 1),
            payload
        );
        assert_eq!(
            MediaCarrier::Audio.unwrap_fragment(payload),
            Some(payload.as_slice())
        );
    }
    #[test]
    fn video_timestamps_follow_the_90khz_wallclock_and_remain_monotonic() {
        assert_eq!(
            wallclock_timestamp(Duration::from_secs(1), 90_000, 0),
            90_000
        );
        assert_eq!(
            wallclock_timestamp(Duration::from_secs(2), 90_000, 90_000),
            180_000
        );
        assert_eq!(
            wallclock_timestamp(Duration::from_secs(2), 90_000, 180_000),
            180_001
        );
        assert_eq!(
            wallclock_timestamp(Duration::from_secs(1), 48_000, 0),
            48_000
        );
        assert_eq!(MediaCarrier::AudioWallclock.clock_rate(), 48_000);
        assert!(MediaCarrier::AudioWallclock.uses_wallclock());
    }
    #[test]
    fn video_track_request_has_single_layer_codec_and_dimensions() {
        let data = MediaCarrier::Vp8Experimental.track_request("test-cid");
        assert_eq!(data["type"], "VIDEO");
        assert_eq!(data["source"], "CAMERA");
        assert_eq!(data["muted"], false);
        assert_eq!(data["width"], 1280);
        assert_eq!(data["height"], 720);
        assert_eq!(data["layers"].as_array().unwrap().len(), 1);
        assert_eq!(data["layers"][0]["quality"], "HIGH");
        assert_eq!(data["simulcastCodecs"][0]["codec"], "vp8");
    }
}
