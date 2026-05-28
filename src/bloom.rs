//! # bloom
//!
//! Probabilistic set membership structure, hand-rolled — no external crates.
//!
//! ## Role in the LSM pipeline
//! The Bloom filter sits in front of SSTable data block I/O. When the Engine
//! searches for a key across Level-0 and lower SSTables, the SSTableReader
//! probes the filter first. A definitive `false` means the key is
//! absent from that table and no block I/O is required. A `true` means the
//! key *may* be present; a full index + block lookup follows.
//!
//! Each SSTable serialises exactly one table-level filter via `to_bytes` and
//! embeds it in the file footer. On `SSTableReader::open`, `from_bytes`
//! reconstructs the filter from those bytes so subsequent `get` calls pay no
//! deserialization cost.
//!
//! ## Dependencies
//! - `codec` — `encode_u64` / `decode_u64` serialise the header and bit words
//!   in the same little-endian format used by WAL records and SSTable blocks.
//! - `error` — `from_bytes` returns `Result<BloomFilter>` to propagate format
//!   violations up to the caller.
//!
//! ## Used by
//! - `sstable` — `SSTableWriter` builds the filter from its key
//!   stream; `SSTableReader` loads and probes it on every `get`.

use std::f64::consts::LN_2;

use crate::codec::{decode_u64, encode_u64};
use crate::error::{FerriteError, Result};

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;

/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x100000001b3;

/// djb2 initial hash value.
const DJB2_INIT: u64 = 5381;

/// A space-efficient probabilistic set supporting insertion and membership
/// queries with a tuneable false-positive rate and no false negatives.
///
/// Internally the filter is a packed bit array of `num_bits` bits stored as
/// `ceil(num_bits / 64)` `u64` words.  For each key, `num_hashes` bit
/// positions are derived via double-hashing (`h1 + i*h2 mod num_bits`) using
/// FNV-1a and djb2 as the two independent hash functions.
pub struct BloomFilter {
    /// Packed bit array; word `i` holds bits `[i*64, i*64+63]`.
    bits: Vec<u64>,
    /// Number of hash probes applied per key (`k` in the standard formula).
    num_hashes: usize,
    /// Total logical bit count (`m` in the standard formula).  Always ≥ 1.
    num_bits: usize,
}

impl BloomFilter {
    /// Constructs a new, empty filter sized for `expected_items` insertions at
    /// the requested `false_positive_rate`.
    ///
    /// Optimal bit count and hash count are derived from the standard formulas:
    /// - `m = -n * ln(p) / ln(2)²`
    /// - `k = (m / n) * ln(2)`
    ///
    /// Both `expected_items` and `false_positive_rate` are clamped to safe
    /// ranges to prevent division-by-zero and `ln(0)` in the formulas.
    pub fn new(expected_items: usize, false_positive_rate: f64) -> BloomFilter {
        // Guard against n=0 dividing in the k formula, and p=0 passing to ln.
        let n = expected_items.max(1);
        let p = false_positive_rate.clamp(1e-9, 0.5);

        let m = (-(n as f64) * p.ln() / (LN_2 * LN_2)).ceil() as usize;
        let num_bits = m.max(1);

        let k = ((m as f64 / n as f64) * LN_2).round() as usize;
        let num_hashes = k.max(1);

        BloomFilter {
            bits: vec![0u64; num_bits.div_ceil(64)],
            num_hashes,
            num_bits,
        }
    }

    /// Records `key` in the filter by setting `num_hashes` bit positions.
    ///
    /// After this call, `may_contain(key)` is guaranteed to return `true`.
    pub fn insert(&mut self, key: &[u8]) {
        let h1 = fnv1a_64(key);
        let h2 = djb2_64(key);
        for i in 0..self.num_hashes {
            let idx = probe_idx(h1, h2, i, self.num_bits);
            self.set_bit(idx);
        }
    }

    /// Returns `false` if `key` is definitely absent from the set; `true` if
    /// the key *may* have been inserted (with false-positive probability ≤ the
    /// configured rate).
    ///
    /// A `false` result is exact and can be used to skip I/O entirely. A
    /// `true` result requires further verification against the actual data.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let h1 = fnv1a_64(key);
        let h2 = djb2_64(key);
        for i in 0..self.num_hashes {
            let idx = probe_idx(h1, h2, i, self.num_bits);
            if !self.get_bit(idx) {
                return false;
            }
        }
        true
    }

    /// Serialises the filter into bytes for embedding in an SSTable footer.
    ///
    /// Layout: `[ num_bits:u64 LE | num_hashes:u64 LE | bit words:u64 LE × … ]`.
    /// The payload can be reconstructed losslessly with `from_bytes`.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16 + self.bits.len() * 8);
        out.extend_from_slice(&encode_u64(self.num_bits as u64));
        out.extend_from_slice(&encode_u64(self.num_hashes as u64));
        for &word in &self.bits {
            out.extend_from_slice(&encode_u64(word));
        }
        out
    }

    /// Deserialises a filter previously produced by `to_bytes`.
    ///
    /// # Errors
    /// Returns `FerriteError::InvalidFormat` if:
    /// - `data` is shorter than the 16-byte header.
    /// - `num_bits` or `num_hashes` decoded from the header are zero (a
    ///   zero-bit filter would let `may_contain` vacuously return `true`).
    /// - The payload length does not match `ceil(num_bits / 64) * 8`.
    pub fn from_bytes(data: &[u8]) -> Result<BloomFilter> {
        if data.len() < 16 {
            return Err(FerriteError::InvalidFormat(format!(
                "bloom filter header requires 16 bytes, got {}",
                data.len()
            )));
        }

        let num_bits = decode_u64(&data[0..8])? as usize;
        let num_hashes = decode_u64(&data[8..16])? as usize;

        // Zero parameters would break invariants: zero bits → out-of-bounds
        // on any probe; zero hashes → may_contain vacuously true.
        if num_bits == 0 || num_hashes == 0 {
            return Err(FerriteError::InvalidFormat(
                "bloom filter num_bits and num_hashes must each be > 0".into(),
            ));
        }

        let expected_words = num_bits.div_ceil(64);
        let expected_len = 16 + expected_words * 8;
        if data.len() != expected_len {
            return Err(FerriteError::InvalidFormat(format!(
                "bloom filter expected {} bytes for {num_bits} bits, got {}",
                expected_len,
                data.len()
            )));
        }

        let mut bits = Vec::with_capacity(expected_words);
        for i in 0..expected_words {
            bits.push(decode_u64(&data[16 + i * 8..])?);
        }

        Ok(BloomFilter {
            bits,
            num_hashes,
            num_bits,
        })
    }

    /// Sets the bit at position `idx` in the packed bit array.
    fn set_bit(&mut self, idx: usize) {
        self.bits[idx / 64] |= 1u64 << (idx % 64);
    }

    /// Returns `true` if the bit at position `idx` is set.
    fn get_bit(&self, idx: usize) -> bool {
        (self.bits[idx / 64] >> (idx % 64)) & 1 != 0
    }
}

/// Computes the bit index for probe `i` of a key given its two hash values.
///
/// Double-hashing formula: `(h1 + i * h2) mod num_bits`. Wrapping arithmetic
/// is required because both hash values span the full u64 range.
fn probe_idx(h1: u64, h2: u64, i: usize, num_bits: usize) -> usize {
    h1.wrapping_add((i as u64).wrapping_mul(h2)) as usize % num_bits
}

/// FNV-1a 64-bit hash — hand-rolled, no crates.
///
/// XOR-then-multiply variant (FNV-1a, not FNV-1). Wrapping multiply is
/// required: the algorithm is defined modulo 2^64.
fn fnv1a_64(key: &[u8]) -> u64 {
    let mut h = FNV_OFFSET_BASIS;
    for &byte in key {
        h ^= byte as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// djb2 64-bit hash variant — hand-rolled, no crates.
///
/// Classic `h * 33 + byte` recurrence expressed as a shift-and-add so the
/// compiler can lower it to a single LEA on x86-64. Wrapping arithmetic is
/// required to stay in u64.
fn djb2_64(key: &[u8]) -> u64 {
    let mut h = DJB2_INIT;
    for &byte in key {
        h = h.wrapping_shl(5).wrapping_add(h).wrapping_add(byte as u64);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies FNV-1a 64-bit against the canonical test vectors from
    /// https://fnv.isthe.info/ — detects any mutation of the constants or
    /// algorithm order (the XOR-then-multiply order matters; FNV-1 gives
    /// different outputs for the same inputs).
    #[test]
    fn test_fnv1a_known_vectors() {
        assert_eq!(fnv1a_64(b""), 0xcbf29ce484222325);
        assert_eq!(fnv1a_64(b"a"), 0xaf63dc4c8601ec8c);
        assert_eq!(fnv1a_64(b"foobar"), 0x85944171f73967e8);
    }

    /// Verifies the djb2 initial value — an empty input must return the seed
    /// unchanged, confirming the loop body never executes on zero bytes.
    #[test]
    fn test_djb2_empty_is_seed() {
        assert_eq!(djb2_64(b""), DJB2_INIT);
    }

    /// Verifies that the two hash functions return different values for a
    /// non-trivial key, which is a necessary (not sufficient) condition for
    /// the double-hashing scheme to explore distinct bit positions.
    #[test]
    fn test_hash_functions_differ() {
        let key = b"test-key";
        assert_ne!(fnv1a_64(key), djb2_64(key));
    }

    /// Verifies probe_idx output stays within [0, num_bits) for several
    /// (h1, h2, i) combinations, including wrapping edge cases.
    #[test]
    fn test_probe_idx_in_range() {
        let num_bits = 1000;
        for i in 0..20 {
            let idx = probe_idx(u64::MAX, u64::MAX, i, num_bits);
            assert!(idx < num_bits, "probe {i} out of range: {idx}");
        }
    }
}
