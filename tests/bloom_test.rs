//! # bloom_test
//!
//! Integration tests for the Bloom filter.
//!
//! ## Role in the LSM pipeline
//! These tests exercise `BloomFilter` in isolation — without an SSTable,
//! Engine, or any I/O — to verify the two acceptance criteria before the
//! filter is embedded in SSTable files: zero false negatives and a false-positive
//! rate that stays within the configured statistical bound.
//!
//! ## Dependencies
//! - `ferrite::bloom` — the module under test.
//! - `ferrite::error::FerriteError` — used to assert `from_bytes` error variants.
//!
//! ## Used by
//! - `cargo test` — Cargo discovers this as an integration test target because
//!   it lives directly in the `tests/` directory.

use ferrite::bloom::BloomFilter;
use ferrite::error::FerriteError;

/// Inserts 10 000 keys into a filter configured for 1% FPR, then calls
/// `may_contain` on every inserted key and asserts it returns `true`. A Bloom
/// filter that ever returns `false` for an inserted key violates its core
/// invariant and is incorrect regardless of its false-positive rate.
#[test]
fn test_no_false_negatives_on_10k_keys() {
    let mut f = BloomFilter::new(10_000, 0.01);
    for i in 0u32..10_000 {
        f.insert(format!("key{i:04}").as_bytes());
    }
    for i in 0u32..10_000 {
        assert!(
            f.may_contain(format!("key{i:04}").as_bytes()),
            "false negative at key{i:04}"
        );
    }
}

/// Queries 10 000 keys that were never inserted and counts how many the filter
/// mistakenly claims to contain. Asserts the observed false-positive rate is
/// below 2%. The filter is configured for 1%; the extra headroom accommodates
/// the ±0.3% statistical variance at this sample size.
#[test]
fn test_false_positive_rate_within_bound() {
    let mut f = BloomFilter::new(10_000, 0.01);
    for i in 0u32..10_000 {
        f.insert(format!("key{i:04}").as_bytes());
    }

    // "miss" prefix is disjoint from "key" prefix — guaranteed no overlap.
    let mut fp_count = 0u32;
    for i in 0u32..10_000 {
        if f.may_contain(format!("miss{i:04}").as_bytes()) {
            fp_count += 1;
        }
    }
    let fp_rate = fp_count as f64 / 10_000.0;
    assert!(
        fp_rate < 0.02,
        "false positive rate {fp_rate:.4} exceeded 2% bound ({fp_count}/10000 false positives)"
    );
}

/// Serialises a populated filter with `to_bytes`, deserialises with
/// `from_bytes`, and verifies that both inserted and non-inserted keys return
/// the identical `may_contain` result from the original and the restored
/// filter. Also verifies that a truncated buffer causes `from_bytes` to return
/// an `InvalidFormat` error.
#[test]
fn test_serialize_round_trip_preserves_membership() {
    let mut original = BloomFilter::new(200, 0.01);
    for i in 0u8..200 {
        original.insert(&[i]);
    }

    let bytes = original.to_bytes();
    let restored = BloomFilter::from_bytes(&bytes).expect("from_bytes must succeed on valid bytes");

    // Every inserted key must agree.
    for i in 0u8..200 {
        assert_eq!(
            original.may_contain(&[i]),
            restored.may_contain(&[i]),
            "membership mismatch for inserted key {i}"
        );
    }

    // Non-inserted keys (different byte range) must also agree — same bit
    // array means same false-positive pattern.
    for i in 200u16..=255 {
        assert_eq!(
            original.may_contain(&[i as u8]),
            restored.may_contain(&[i as u8]),
            "membership mismatch for non-inserted key {i}"
        );
    }

    // A buffer shorter than the serialised form must be rejected.
    let truncated = &bytes[..bytes.len() / 2];
    assert!(
        matches!(
            BloomFilter::from_bytes(truncated),
            Err(FerriteError::InvalidFormat(_))
        ),
        "truncated buffer must return InvalidFormat"
    );
}

/// Verifies that a freshly constructed filter with no insertions returns
/// `false` for every probed key. With all bits zero the first probe always
/// finds an unset bit, so no key can ever produce a spurious `true`.
#[test]
fn test_empty_filter_returns_false_for_all() {
    let f = BloomFilter::new(1_000, 0.01);
    for key in [
        b"hello".as_slice(),
        b"world",
        b"",
        b"\x00",
        b"some-arbitrary-key",
    ] {
        assert!(
            !f.may_contain(key),
            "empty filter must return false for {key:?}"
        );
    }
}

/// Inserts a single key into a filter built for one item and asserts that
/// `may_contain` returns `true` for that exact key. This is the minimal
/// non-trivial end-to-end test for a correct insert → probe cycle.
#[test]
fn test_single_item_round_trip() {
    let mut f = BloomFilter::new(1, 0.01);
    f.insert(b"lone-key");
    assert!(
        f.may_contain(b"lone-key"),
        "filter must contain the single inserted key"
    );
}
