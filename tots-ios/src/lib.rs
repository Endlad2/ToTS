use libc::{c_char, c_uchar, size_t};
use std::{
    ffi::{CStr, CString},
    ptr, slice,
};
use tots_core::{
    config::ConnectionConfig,
    session::{DecodedPacket, SessionEngine},
};

pub struct Handle(SessionEngine);

#[repr(C)]
pub struct TotsBuffer {
    pub ptr: *mut c_uchar,
    pub len: size_t,
    pub cap: size_t,
}

fn into_buffer(mut data: Vec<u8>) -> TotsBuffer {
    let out = TotsBuffer {
        ptr: data.as_mut_ptr(),
        len: data.len(),
        cap: data.capacity(),
    };
    std::mem::forget(data);
    out
}

fn empty_buffer() -> TotsBuffer {
    TotsBuffer {
        ptr: ptr::null_mut(),
        len: 0,
        cap: 0,
    }
}

#[no_mangle]
/// # Safety
/// A non-null `uri` must point to a valid NUL-terminated string for this call.
pub unsafe extern "C" fn tots_engine_new(uri: *const c_char) -> *mut Handle {
    if uri.is_null() {
        return ptr::null_mut();
    }
    let s = unsafe { CStr::from_ptr(uri) }.to_string_lossy();
    match ConnectionConfig::decode_uri(&s).and_then(SessionEngine::new) {
        Ok(e) => Box::into_raw(Box::new(Handle(e))),
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
/// # Safety
/// A non-null `h` must be an exclusively owned handle returned by `tots_engine_new`.
pub unsafe extern "C" fn tots_engine_free(h: *mut Handle) {
    if !h.is_null() {
        unsafe {
            drop(Box::from_raw(h));
        }
    }
}

#[no_mangle]
/// # Safety
/// A non-null `uri` must point to a valid NUL-terminated string for this call.
pub unsafe extern "C" fn tots_config_json(uri: *const c_char) -> *mut c_char {
    if uri.is_null() {
        return ptr::null_mut();
    }
    let s = unsafe { CStr::from_ptr(uri) }.to_string_lossy();
    match ConnectionConfig::decode_uri(&s)
        .and_then(|c| serde_json::to_string(&c).map_err(Into::into))
    {
        Ok(v) => CString::new(v)
            .map(CString::into_raw)
            .unwrap_or(ptr::null_mut()),
        Err(_) => ptr::null_mut(),
    }
}

/// Returns a length-prefixed batch: repeated `u32_be datagram_len | datagram`.
/// # Safety
/// `h` must be live and exclusively borrowed; `data` must be non-null and readable for `len` bytes.
#[no_mangle]
pub unsafe extern "C" fn tots_engine_encapsulate_ip(
    h: *mut Handle,
    assoc_id: u16,
    data: *const c_uchar,
    len: size_t,
) -> TotsBuffer {
    if h.is_null() || (data.is_null() && len != 0) {
        return empty_buffer();
    }
    let handle = unsafe { &mut *h };
    let packet = unsafe { slice::from_raw_parts(data, len) };
    let Ok((_, datagrams)) = handle.0.encapsulate_ip(assoc_id, packet) else {
        return empty_buffer();
    };
    let mut batch = Vec::new();
    for d in datagrams {
        let Ok(n) = u32::try_from(d.len()) else {
            return empty_buffer();
        };
        batch.extend_from_slice(&n.to_be_bytes());
        batch.extend_from_slice(&d);
    }
    into_buffer(batch)
}

/// Returns one completed inner IP packet, or an empty buffer for a control/incomplete fragment.
/// # Safety
/// `h` must be live and exclusively borrowed; `data` must be non-null and readable for `len` bytes.
#[no_mangle]
pub unsafe extern "C" fn tots_engine_decapsulate(
    h: *mut Handle,
    data: *const c_uchar,
    len: size_t,
) -> TotsBuffer {
    if h.is_null() || (data.is_null() && len != 0) {
        return empty_buffer();
    }
    let handle = unsafe { &mut *h };
    let datagram = unsafe { slice::from_raw_parts(data, len) };
    match handle.0.decapsulate(datagram) {
        Ok(Some(DecodedPacket::Ip { bytes, .. })) => into_buffer(bytes),
        _ => empty_buffer(),
    }
}

#[no_mangle]
/// # Safety
/// A non-empty buffer must be an unmodified, exclusively owned buffer returned by this library.
pub unsafe extern "C" fn tots_buffer_free(buffer: TotsBuffer) {
    if !buffer.ptr.is_null() {
        unsafe {
            drop(Vec::from_raw_parts(buffer.ptr, buffer.len, buffer.cap));
        }
    }
}

#[no_mangle]
/// # Safety
/// A non-null `s` must be an exclusively owned string returned by this library.
pub unsafe extern "C" fn tots_string_free(s: *mut c_char) {
    if !s.is_null() {
        unsafe {
            drop(CString::from_raw(s));
        }
    }
}
