//! # memtable_test
//!
//! Integration tests for the Memtable.
//!
//! ## Role in the LSM pipeline
//! These tests exercise `Memtable` in isolation — without a WAL file, Engine,
//! or SSTable — to verify correctness of the in-memory sorted write buffer
//! before higher-level phases depend on it.
//!
//! ## Dependencies
//! - `ferrite::memtable` — the module under test.
//! - `ferrite::wal::WalRecord` — used to construct replay inputs for
//!   `restore_from_wal`.
//!
//! ## Used by
//! - `cargo test` — Cargo discovers this as an integration test target because
//!   it lives directly in the `tests/` directory.

use ferrite::memtable::{MemValue, Memtable};
use ferrite::wal::WalRecord;

/// Inserts 1000 put records, then reads each key back and asserts the returned
/// value matches what was written, in every position.
#[test]
fn test_put_1000_keys_round_trip() {
    let mut mem = Memtable::new();
    for i in 0u32..1000 {
        let key = format!("key{i:04}").into_bytes();
        let val = format!("val{i:04}").into_bytes();
        mem.put(key, val);
    }
    for i in 0u32..1000 {
        let key = format!("key{i:04}").into_bytes();
        let expected_val = format!("val{i:04}").into_bytes();
        match mem.get(&key) {
            Some(MemValue::Value(v)) => assert_eq!(v, &expected_val, "key{i:04} value mismatch"),
            Some(MemValue::Tombstone) => panic!("key{i:04} unexpectedly a tombstone"),
            None => panic!("key{i:04} missing from memtable"),
        }
    }
}

/// Puts a key then deletes it and verifies that get returns `Some(Tombstone)`
/// rather than `None`, confirming that the Memtable can shadow lower SSTable
/// copies with an explicit deletion marker.
#[test]
fn test_delete_returns_tombstone_not_none() {
    let mut mem = Memtable::new();
    mem.put(b"ghost".to_vec(), b"haunted".to_vec());
    mem.delete(b"ghost".to_vec());
    assert_eq!(
        mem.get(b"ghost"),
        Some(&MemValue::Tombstone),
        "deleted key must return Some(Tombstone), not None"
    );
}

/// Inserts keys under three prefixes, scans for one prefix, and verifies the
/// result contains exactly the matching keys in ascending order with no
/// cross-prefix contamination.
#[test]
fn test_scan_prefix_filters_correctly() {
    let mut mem = Memtable::new();

    // Insert in non-sorted order to confirm BTreeMap sorts for us.
    mem.put(b"user:bob".to_vec(), b"1".to_vec());
    mem.put(b"post:xyz".to_vec(), b"2".to_vec());
    mem.put(b"user:alice".to_vec(), b"3".to_vec());
    mem.put(b"comment:abc".to_vec(), b"4".to_vec());
    mem.put(b"user:carol".to_vec(), b"5".to_vec());

    let results: Vec<(&Vec<u8>, &MemValue)> = mem.scan_prefix(b"user:").collect();

    assert_eq!(results.len(), 3, "expected exactly 3 user: keys");
    // BTreeMap guarantees ascending order.
    assert_eq!(results[0].0.as_slice(), b"user:alice");
    assert_eq!(results[1].0.as_slice(), b"user:bob");
    assert_eq!(results[2].0.as_slice(), b"user:carol");
    // No post: or comment: keys should appear.
    for (k, _) in &results {
        assert!(k.starts_with(b"user:"), "unexpected key in scan: {k:?}");
    }
}

/// Verifies the size accounting rules: size grows on every put, does not
/// shrink when a key is deleted (tombstone replaces value, bytes unchanged),
/// and grows by exactly the new value length when a key is overwritten.
#[test]
fn test_size_grows_on_put_does_not_shrink_on_delete() {
    let mut mem = Memtable::new();

    // Fresh put: size = key.len() + value.len() = 3 + 5 = 8.
    mem.put(b"foo".to_vec(), b"hello".to_vec());
    let after_put = mem.size_bytes();
    assert_eq!(after_put, 8);

    // Delete same key: size must not decrease.
    mem.delete(b"foo".to_vec());
    let after_delete = mem.size_bytes();
    assert!(
        after_delete >= after_put,
        "size must not shrink on delete: was {after_put}, now {after_delete}"
    );

    // Overwrite with a new value of length 7: size should grow by exactly 7.
    mem.put(b"foo".to_vec(), b"updated".to_vec());
    let after_overwrite = mem.size_bytes();
    assert_eq!(
        after_overwrite,
        after_delete + 7,
        "overwrite should add new value length only"
    );
}

/// Builds a Memtable via direct put/delete calls, then builds an equivalent
/// Vec<WalRecord> and calls restore_from_wal; asserts the two Memtables have
/// identical entries via iter().collect().
#[test]
fn test_restore_from_wal_matches_original() {
    let mut original = Memtable::new();
    original.put(b"alpha".to_vec(), b"one".to_vec());
    original.put(b"beta".to_vec(), b"two".to_vec());
    original.delete(b"alpha".to_vec());
    original.put(b"gamma".to_vec(), b"three".to_vec());
    original.put(b"beta".to_vec(), b"updated".to_vec());

    let records = vec![
        WalRecord::Put {
            key: b"alpha".to_vec(),
            value: b"one".to_vec(),
        },
        WalRecord::Put {
            key: b"beta".to_vec(),
            value: b"two".to_vec(),
        },
        WalRecord::Delete {
            key: b"alpha".to_vec(),
        },
        WalRecord::Put {
            key: b"gamma".to_vec(),
            value: b"three".to_vec(),
        },
        WalRecord::Put {
            key: b"beta".to_vec(),
            value: b"updated".to_vec(),
        },
    ];

    let restored = Memtable::restore_from_wal(records);

    let original_entries: Vec<_> = original.iter().collect();
    let restored_entries: Vec<_> = restored.iter().collect();
    assert_eq!(
        original_entries, restored_entries,
        "restored memtable must match original entry for entry"
    );
}

/// Confirms is_full returns false below the threshold and true once the
/// threshold is reached, using a tiny threshold to keep the test deterministic.
#[test]
fn test_is_full_threshold_boundary() {
    let mut mem = Memtable::new();
    // Each key is [b'k', i] (2 bytes) and each value is [b'v', i] (2 bytes),
    // so every new-key put adds 4 bytes. Use threshold = 20 so 4 puts (16 bytes)
    // stays below it and the 5th put (20 bytes) exactly hits it.
    let threshold = 20;

    assert!(!mem.is_full(threshold), "empty memtable must not be full");

    // 4 puts × 4 bytes = 16 < 20 → still not full.
    for i in 0u8..4 {
        mem.put(vec![b'k', i], vec![b'v', i]);
    }
    assert!(!mem.is_full(threshold), "16 bytes must be below threshold 20");

    // 5th put adds 4 more bytes → 20 >= 20 → full.
    mem.put(vec![b'k', 4], vec![b'v', 4]);
    assert!(
        mem.is_full(threshold),
        "20 bytes must satisfy threshold 20"
    );
}
