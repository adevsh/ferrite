//! # codec
//!
//! Hand-rolled little-endian binary encode/decode primitives for on-disk formats.
//!
//! ## Role in the LSM pipeline
//! codec is a pure utility layer below the pipeline proper. It has no state and
//! no I/O — it only converts between Rust values and raw byte slices. The WAL
//! and SSTable writer call into this module to serialise record fields; the WAL
//! reader and SSTable reader call into it to deserialise.
//!
//! ## Dependencies
//! - `error` — decode functions return `Result<T>` so callers can propagate
//!   malformed-buffer errors without panicking.
//!
//! ## Used by
//! - `wal` — encodes CRC32, type byte, key_len, val_len, key, and value into WAL records.
//! - `sstable` — encodes block headers, index entries, bloom filter blocks, and footer.

#![allow(dead_code)]

use crate::error::{FerriteError, Result};

/// Serialises a `u32` to a 4-byte little-endian array.
pub fn encode_u32(val: u32) -> [u8; 4] {
    val.to_le_bytes()
}

/// Deserialises a `u32` from the first 4 bytes of `buf`.
///
/// # Errors
/// Returns `FerriteError::InvalidFormat` if `buf` is shorter than 4 bytes.
pub fn decode_u32(buf: &[u8]) -> Result<u32> {
    if buf.len() < 4 {
        return Err(FerriteError::InvalidFormat(format!(
            "need 4 bytes for u32, got {}",
            buf.len()
        )));
    }
    Ok(u32::from_le_bytes(buf[..4].try_into().unwrap()))
}

/// Serialises a `u64` to an 8-byte little-endian array.
pub fn encode_u64(val: u64) -> [u8; 8] {
    val.to_le_bytes()
}

/// Deserialises a `u64` from the first 8 bytes of `buf`.
///
/// # Errors
/// Returns `FerriteError::InvalidFormat` if `buf` is shorter than 8 bytes.
pub fn decode_u64(buf: &[u8]) -> Result<u64> {
    if buf.len() < 8 {
        return Err(FerriteError::InvalidFormat(format!(
            "need 8 bytes for u64, got {}",
            buf.len()
        )));
    }
    Ok(u64::from_le_bytes(buf[..8].try_into().unwrap()))
}

/// Serialises a key–value pair into a length-prefixed byte vector.
///
/// Layout: `[ key_len:u32 | val_len:u32 | key bytes | val bytes ]`.
/// This is the building block for WAL records and SSTable data block entries.
/// Passing an empty slice for `val` encodes a tombstone marker;
/// the caller sets the record type byte in the WAL header to distinguish tombstones.
pub fn encode_bytes(key: &[u8], val: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + key.len() + val.len());
    buf.extend_from_slice(&encode_u32(key.len() as u32));
    buf.extend_from_slice(&encode_u32(val.len() as u32));
    buf.extend_from_slice(key);
    buf.extend_from_slice(val);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trips representative u32 values through encode → decode and verifies identity.
    #[test]
    fn test_u32_roundtrip() {
        for val in [0u32, 1, 255, u16::MAX as u32, u32::MAX] {
            assert_eq!(decode_u32(&encode_u32(val)).unwrap(), val);
        }
    }

    /// Verifies that decode_u32 returns an error when given fewer than 4 bytes.
    #[test]
    fn test_u32_short_buffer() {
        assert!(decode_u32(&[0x01, 0x02, 0x03]).is_err());
        assert!(decode_u32(&[]).is_err());
    }

    /// Round-trips representative u64 values through encode → decode and verifies identity.
    #[test]
    fn test_u64_roundtrip() {
        for val in [0u64, 1, u32::MAX as u64, u64::MAX] {
            assert_eq!(decode_u64(&encode_u64(val)).unwrap(), val);
        }
    }

    /// Verifies that decode_u64 returns an error when given fewer than 8 bytes.
    #[test]
    fn test_u64_short_buffer() {
        assert!(decode_u64(&[0x00; 7]).is_err());
        assert!(decode_u64(&[]).is_err());
    }

    /// Verifies encode_bytes produces the correct layout: [key_len | val_len | key | val].
    #[test]
    fn test_encode_bytes_layout() {
        let buf = encode_bytes(b"hello", b"world");

        assert_eq!(decode_u32(&buf[..4]).unwrap(), 5);
        assert_eq!(decode_u32(&buf[4..8]).unwrap(), 5);
        assert_eq!(&buf[8..13], b"hello");
        assert_eq!(&buf[13..18], b"world");
    }

    /// Verifies encode_bytes handles an empty value, which is the tombstone encoding pattern.
    #[test]
    fn test_encode_bytes_empty_val() {
        let buf = encode_bytes(b"key", b"");

        assert_eq!(decode_u32(&buf[..4]).unwrap(), 3);
        assert_eq!(decode_u32(&buf[4..8]).unwrap(), 0);
        assert_eq!(&buf[8..11], b"key");
        assert_eq!(buf.len(), 11);
    }
}
