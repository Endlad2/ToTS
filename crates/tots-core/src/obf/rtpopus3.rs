//! Профиль rtpopus3: RTP + extensions + abs-send-time + VAD + джиттер timestamp.

use std::io;
use std::time::Instant;

use chacha20poly1305::aead::{AeadInPlace, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use parking_lot::Mutex;
use rand::{Rng, RngCore};
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;

pub const KEY_LEN: usize = 32;
const RTP_HDR_LEN: usize = 12;
const RTP_EXT_LEN: usize = 16;
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;
pub const HEADER_LEN: usize = RTP_HDR_LEN + RTP_EXT_LEN + NONCE_LEN;
pub const OVERHEAD: usize = HEADER_LEN + TAG_LEN;

const RTP_VER_EXT: u8 = 0x90;
const RTP_PT: u8 = 0x6F;
const RTP_MARKER: u8 = 0x80;

const EXT_AUDIO_LEVEL: u8 = 0x10;
const EXT_TRANSPORT: u8 = 0x21;
const EXT_ABS_SEND_TIME: u8 = 0x32;

const SPEECH_MIN: u32 = 30;
const SPEECH_MAX: u32 = 200;
const SILENCE_MIN: u32 = 5;
const SILENCE_MAX: u32 = 30;

const GAP_MIN: u32 = 50;
const GAP_MAX: u32 = 150;
const GAP_SIZE_MIN: u32 = 1;
const GAP_SIZE_MAX: u32 = 3;

const TS_20MS: u32 = 960;
const TS_10MS: u32 = 480;
const TS_40MS: u32 = 1920;

#[derive(Clone, Copy, PartialEq, Eq)]
enum AudioState {
    Silence,
    Speech,
}

struct State {
    rng: ChaCha8Rng,
    counter: u64,
    seq: u16,
    timestamp: u32,
    tcc: u16,
    audio: AudioState,
    pkts_in_state: u32,
    next_state_switch: u32,
    next_gap_at: u32,
    gap_size: u32,
}

pub struct Conn {
    aead: ChaCha20Poly1305,
    session_id: [u8; 4],
    ssrc: [u8; 4],
    start: Instant,
    st: Mutex<State>,
}

impl Conn {
    pub fn new(key: &[u8], is_server: bool) -> io::Result<Self> {
        if key.len() != KEY_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rtpopus3: bad key len",
            ));
        }
        let cipher = ChaCha20Poly1305::new(Key::from_slice(key));

        let mut seed = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut seed);
        let mut rng = ChaCha8Rng::from_seed(seed);

        let next_state_switch = SPEECH_MIN + rng.gen_range(0..=(SPEECH_MAX - SPEECH_MIN));
        let next_gap_at = GAP_MIN + rng.gen_range(0..=(GAP_MAX - GAP_MIN));
        let gap_size = GAP_SIZE_MIN + rng.gen_range(0..=(GAP_SIZE_MAX - GAP_SIZE_MIN));

        let mut rnd = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut rnd);
        let mut session_id = [0u8; 4];
        let mut ssrc = [0u8; 4];
        session_id.copy_from_slice(&rnd[0..4]);
        ssrc.copy_from_slice(&rnd[4..8]);
        if is_server {
            session_id[0] |= 0x80;
        } else {
            session_id[0] &= !0x80;
        }

        let mut cb = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut cb);

        Ok(Self {
            aead: cipher,
            session_id,
            ssrc,
            start: Instant::now(),
            st: Mutex::new(State {
                rng,
                counter: u64::from_be_bytes(cb),
                seq: u16::from_be_bytes([rnd[8], rnd[9]]),
                timestamp: u32::from_be_bytes([rnd[10], rnd[11], rnd[12], rnd[13]]),
                tcc: u16::from_be_bytes([rnd[14], rnd[15]]),
                audio: AudioState::Speech,
                pkts_in_state: 0,
                next_state_switch,
                next_gap_at,
                gap_size,
            }),
        })
    }

    pub fn wrap_into_mut(&mut self, dst: &mut [u8], payload: &[u8]) -> io::Result<usize> {
        let wire_len = OVERHEAD + payload.len();
        if dst.len() < wire_len {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "dst too small"));
        }

        let (marker, level, seq, ts, tcc, ctr) = {
            let mut st = self.st.lock();
            let marker = st.pkts_in_state >= st.next_state_switch;
            if marker {
                st.pkts_in_state = 0;
                if st.audio == AudioState::Silence {
                    st.audio = AudioState::Speech;
                    st.next_state_switch =
                        SPEECH_MIN + st.rng.gen_range(0..=(SPEECH_MAX - SPEECH_MIN));
                } else {
                    st.audio = AudioState::Silence;
                    st.next_state_switch =
                        SILENCE_MIN + st.rng.gen_range(0..=(SILENCE_MAX - SILENCE_MIN));
                }
            }
            st.pkts_in_state += 1;

            let level = if st.audio == AudioState::Speech {
                0x80 | (20 + st.rng.gen_range(0..31) as u8)
            } else {
                (100 + st.rng.gen_range(0..28)) as u8
            };

            let seq = st.seq;
            st.seq = st.seq.wrapping_add(1);
            st.next_gap_at = st.next_gap_at.saturating_sub(1);
            if st.next_gap_at == 0 {
                st.seq = st.seq.wrapping_add(st.gap_size as u16);
                st.next_gap_at = GAP_MIN + st.rng.gen_range(0..=(GAP_MAX - GAP_MIN));
                st.gap_size = GAP_SIZE_MIN + st.rng.gen_range(0..=(GAP_SIZE_MAX - GAP_SIZE_MIN));
            }

            let ts = st.timestamp;
            st.timestamp = st.timestamp.wrapping_add(pick_ts_step(&mut st.rng));

            let tcc = st.tcc;
            st.tcc = st.tcc.wrapping_add(1);

            let ctr = st.counter;
            st.counter = st.counter.wrapping_add(1);

            (marker, level, seq, ts, tcc, ctr)
        };

        // 1. Заголовок.
        dst[0] = RTP_VER_EXT;
        let mut pt = RTP_PT;
        if marker {
            pt |= RTP_MARKER;
        }
        dst[1] = pt;
        dst[2..4].copy_from_slice(&seq.to_be_bytes());
        dst[4..8].copy_from_slice(&ts.to_be_bytes());
        dst[8..12].copy_from_slice(&self.ssrc);

        dst[12] = 0xBE;
        dst[13] = 0xDE;
        dst[14..16].copy_from_slice(&3u16.to_be_bytes());
        dst[16] = EXT_AUDIO_LEVEL;
        dst[17] = level;
        dst[18] = EXT_TRANSPORT;
        dst[19..21].copy_from_slice(&tcc.to_be_bytes());
        dst[21] = EXT_ABS_SEND_TIME;

        let ast = self.abs_send_time();
        dst[22] = ((ast >> 16) & 0xFF) as u8;
        dst[23] = ((ast >> 8) & 0xFF) as u8;
        dst[24] = (ast & 0xFF) as u8;
        dst[25] = 0;
        dst[26] = 0;
        dst[27] = 0;

        // 2. Nonce.
        dst[28..32].copy_from_slice(&self.session_id);
        dst[32..HEADER_LEN].copy_from_slice(&ctr.to_be_bytes());

        // 3. Копируем nonce и AAD в собственные буферы до мутации dst.
        let mut nonce_bytes = [0u8; NONCE_LEN];
        nonce_bytes.copy_from_slice(&dst[28..HEADER_LEN]);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let mut aad = [0u8; HEADER_LEN];
        aad.copy_from_slice(&dst[..HEADER_LEN]);

        // 4. Payload.
        dst[HEADER_LEN..HEADER_LEN + payload.len()].copy_from_slice(payload);

        // 5. Шифрование.
        let (ct, tag_slot) = dst[HEADER_LEN..wire_len].split_at_mut(payload.len());
        let tag = self
            .aead
            .encrypt_in_place_detached(nonce, &aad, ct)
            .map_err(|_| io::Error::other("rtpopus3: encrypt failed"))?;
        tag_slot[..TAG_LEN].copy_from_slice(&tag);

        Ok(wire_len)
    }

    pub fn unwrap_into_mut(&mut self, wire: &[u8], dst: &mut [u8]) -> io::Result<usize> {
        if wire.len() < OVERHEAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "packet too short",
            ));
        }
        let payload_len = wire.len() - OVERHEAD;
        if dst.len() < payload_len {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "dst too small"));
        }

        let mut nonce_bytes = [0u8; NONCE_LEN];
        nonce_bytes.copy_from_slice(&wire[28..HEADER_LEN]);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let mut aad = [0u8; HEADER_LEN];
        aad.copy_from_slice(&wire[..HEADER_LEN]);

        let (ct, tag) = wire[HEADER_LEN..].split_at(payload_len);
        dst[..payload_len].copy_from_slice(ct);

        self.aead
            .decrypt_in_place_detached(
                nonce,
                &aad,
                &mut dst[..payload_len],
                chacha20poly1305::Tag::from_slice(tag),
            )
            .map_err(|_| io::Error::other("rtpopus3: decrypt failed"))?;

        Ok(payload_len)
    }

    fn abs_send_time(&self) -> u32 {
        let ms = self.start.elapsed().as_millis() as u64;
        let sec = ((ms / 1000) % 64) as u32;
        let frac = ((ms % 1000) << 18) / 1000;
        (sec << 18) | (frac as u32)
    }
}

fn pick_ts_step(rng: &mut ChaCha8Rng) -> u32 {
    match rng.gen_range(0..256) {
        0..=9 => TS_10MS,
        10..=229 => TS_20MS,
        _ => TS_40MS,
    }
}
