//! Bond — распределение одного TCP-соединения по нескольким подпотокам.
//!
//! Кадр: `[type:u8][seq:u64][len:u32][payload]`.
//! `FIN` — тип 2, пустой payload, seq указывает на конец последовательности.

use std::io;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const FRAME_DATA: u8 = 1;
pub const FRAME_FIN: u8 = 2;
pub const MAX_CHUNK: usize = 16 * 1024;
pub const MAX_LANES: usize = 256;

pub const HELLO_MAGIC: &[u8; 4] = b"VLB1";

pub async fn write_hello<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    id: u64,
    index: u16,
    count: u16,
) -> io::Result<()> {
    let mut buf = Vec::with_capacity(17);
    buf.extend_from_slice(HELLO_MAGIC);
    buf.push(1);
    buf.extend_from_slice(&id.to_be_bytes());
    buf.extend_from_slice(&index.to_be_bytes());
    buf.extend_from_slice(&count.to_be_bytes());
    w.write_all(&buf).await?;
    w.flush().await
}

pub async fn read_hello<R: AsyncReadExt + Unpin>(r: &mut R) -> io::Result<(u64, u16, u16)> {
    let mut buf = [0u8; 17];
    r.read_exact(&mut buf).await?;
    if &buf[..4] != HELLO_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }
    if buf[4] != 1 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad version"));
    }
    let id = u64::from_be_bytes(buf[5..13].try_into().unwrap());
    let index = u16::from_be_bytes(buf[13..15].try_into().unwrap());
    let count = u16::from_be_bytes(buf[15..17].try_into().unwrap());
    if count == 0 || count as usize > MAX_LANES || index >= count {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad count"));
    }
    Ok((id, index, count))
}

pub async fn write_frame<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    typ: u8,
    seq: u64,
    data: &[u8],
) -> io::Result<()> {
    let mut buf = BytesMut::with_capacity(13 + data.len());
    buf.put_u8(typ);
    buf.put_u64(seq);
    buf.put_u32(data.len() as u32);
    buf.put_slice(data);
    w.write_all(&buf).await
}

pub async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> io::Result<(u8, u64, Vec<u8>)> {
    let mut head = [0u8; 13];
    r.read_exact(&mut head).await?;
    let typ = head[0];
    let seq = u64::from_be_bytes(head[1..9].try_into().unwrap());
    let len = u32::from_be_bytes(head[9..13].try_into().unwrap()) as usize;
    if (typ != FRAME_DATA && typ != FRAME_FIN) || len > MAX_CHUNK {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad frame"));
    }
    let mut data = vec![0u8; len];
    if len > 0 {
        r.read_exact(&mut data).await?;
    }
    Ok((typ, seq, data))
}
