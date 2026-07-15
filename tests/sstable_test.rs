//! # sstable_test
//!
//! Integration tests for the SSTable writer and reader.
//!
//! ## Role in the LSM pipeline
//! These tests exercise `SSTableWriter` and `SSTableReader` in isolation —
//! without a WAL, Memtable, or Engine — to verify the full round-trip of
//! entries through the on-disk format before the Engine depends on the module.
//!
//! ## Dependencies
//! - `ferrite::sstable` — the modules under test.
//! - `ferrite::error::FerriteError` — used to assert error variant types.
//! - `tempfile` — provides `TempDir` for isolated, auto-cleaned test directories.
//!
//! ## Used by
//! - `cargo test` — Cargo discovers this as an integration test target because
//!   it lives directly in the `tests/` directory.

use ferrite::error::FerriteError;
use ferrite::sstable::{SSTableReader, SSTableWriter};
use tempfile::tempdir;

/// Writes 10 000 sorted entries across many 4 KiB data blocks, reopens the
/// file as a reader, and verifies that every key returns the exact value that
/// was written. This exercises the full write → index → bloom → block-read
/// pipeline across multiple blocks.
#[test]
fn test_round_trip_10k_entries() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0u32..10_000)
        .map(|i| {
            (
                format!("key{i:06}").into_bytes(),
                Some(format!("val{i:06}").into_bytes()),
            )
        })
        .collect();

    let writer = SSTableWriter::new(&path).unwrap();
    let meta = writer.write(entries.iter().cloned()).unwrap();
    assert_eq!(meta.entry_count, 10_000);
    assert_eq!(meta.smallest_key, b"key000000");
    assert_eq!(meta.largest_key, b"key009999");

    let reader = SSTableReader::open(&path).unwrap();
    for i in 0u32..10_000 {
        let key = format!("key{i:06}").into_bytes();
        let expected_val = format!("val{i:06}").into_bytes();
        match reader.get(&key).unwrap() {
            Some(Some(v)) => assert_eq!(v, expected_val, "value mismatch at key{i:06}"),
            Some(None) => panic!("key{i:06} unexpectedly a tombstone"),
            None => panic!("key{i:06} missing from SSTable"),
        }
    }
}

/// Writes 100 entries, calls get on a key that was never inserted, and
/// verifies the result is None. Also asserts that the bloom_misses counter
/// increased — confirming the bloom filter short-circuited the lookup without
/// reading any data block. With ~99% bloom rejection probability at 1% FPR,
/// at least 90 of 100 miss queries must be short-circuited.
#[test]
fn test_get_missing_key_bloom_short_circuits() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0u32..1_000)
        .map(|i| {
            (
                format!("key{i:04}").into_bytes(),
                Some(format!("val{i:04}").into_bytes()),
            )
        })
        .collect();

    let writer = SSTableWriter::new(&path).unwrap();
    writer.write(entries.into_iter()).unwrap();

    let reader = SSTableReader::open(&path).unwrap();
    assert_eq!(reader.bloom_misses(), 0, "no misses before any get");

    // Query 100 keys that were never inserted.
    for i in 0u32..100 {
        let key = format!("miss{i:04}").into_bytes();
        assert_eq!(
            reader.get(&key).unwrap(),
            None,
            "missing key must return None"
        );
    }

    // At 1% FPR and 100 queries, expected ~99 bloom misses. Require ≥ 90.
    assert!(
        reader.bloom_misses() >= 90,
        "expected ≥ 90 bloom short-circuits out of 100 miss queries; got {}",
        reader.bloom_misses()
    );
}

/// Writes a small number of known entries and verifies that get returns the
/// exact written value for each key.
#[test]
fn test_get_present_key_returns_value() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let data: &[(&[u8], &[u8])] = &[
        (b"alpha", b"one"),
        (b"beta", b"two"),
        (b"gamma", b"three"),
        (b"delta", b"four"),
        (b"epsilon", b"five"),
    ];
    // Keys must be sorted.
    let mut entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = data
        .iter()
        .map(|(k, v)| (k.to_vec(), Some(v.to_vec())))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let writer = SSTableWriter::new(&path).unwrap();
    writer.write(entries.into_iter()).unwrap();

    let reader = SSTableReader::open(&path).unwrap();
    for (key, val) in data {
        assert_eq!(
            reader.get(key).unwrap(),
            Some(Some(val.to_vec())),
            "key {:?} must return {:?}",
            key,
            val
        );
    }
}

/// Writes a tombstone entry (None value), then verifies that get returns
/// Some(None) — present as a tombstone — rather than None (absent).
/// The distinction is required for correct LSM layering: the Engine must
/// stop at a tombstone rather than falling through to a stale value in an
/// older SSTable.
#[test]
fn test_tombstone_get_returns_some_none() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let entries = vec![
        (b"alpha".to_vec(), Some(b"live".to_vec())),
        (b"ghost".to_vec(), None), // tombstone
        (b"omega".to_vec(), Some(b"also-live".to_vec())),
    ];

    let writer = SSTableWriter::new(&path).unwrap();
    writer.write(entries.into_iter()).unwrap();

    let reader = SSTableReader::open(&path).unwrap();
    // Live keys round-trip normally.
    assert_eq!(reader.get(b"alpha").unwrap(), Some(Some(b"live".to_vec())));
    assert_eq!(
        reader.get(b"omega").unwrap(),
        Some(Some(b"also-live".to_vec()))
    );
    // Tombstone: present in the table but deleted.
    assert_eq!(
        reader.get(b"ghost").unwrap(),
        Some(None),
        "tombstone key must return Some(None), not None"
    );
}

/// Writes entries under three different key prefixes, calls scan_prefix for
/// one prefix, and asserts the returned list contains only matching keys in
/// ascending order with no tombstones.
#[test]
fn test_scan_prefix_filters_and_sorts() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    // Mix of user:, comment:, and post: keys — must be written in sorted order.
    let mut entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = vec![
        (b"comment:1".to_vec(), Some(b"c1".to_vec())),
        (b"post:1".to_vec(), Some(b"p1".to_vec())),
        (b"user:alice".to_vec(), Some(b"ua".to_vec())),
        (b"user:bob".to_vec(), None), // tombstone — must NOT appear in scan
        (b"user:carol".to_vec(), Some(b"uc".to_vec())),
        (b"user:dave".to_vec(), Some(b"ud".to_vec())),
        (b"vendor:x".to_vec(), Some(b"vx".to_vec())),
    ];
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    let writer = SSTableWriter::new(&path).unwrap();
    writer.write(entries.into_iter()).unwrap();

    let reader = SSTableReader::open(&path).unwrap();
    let results = reader.scan_prefix(b"user:").unwrap();

    // Only non-tombstone user: keys must be returned.
    assert_eq!(
        results.len(),
        3,
        "expected 3 live user: results, got {:?}",
        results
    );
    assert_eq!(results[0].0, b"user:alice");
    assert_eq!(results[0].1, b"ua");
    assert_eq!(results[1].0, b"user:carol");
    assert_eq!(results[2].0, b"user:dave");

    for (k, _) in &results {
        assert!(k.starts_with(b"user:"), "unexpected key in scan: {k:?}");
    }
}

/// Writes enough entries to span at least two 4 KiB data blocks, flips a byte
/// in the first block's CRC field (byte 0 of the file), re-opens the reader,
/// and asserts that a get on a key in the first block returns
/// Err(FerriteError::Corruption). Also verifies that a key in the second
/// (unmodified) block is still readable.
#[test]
fn test_corrupt_data_block_detected() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    // 200 entries of ~23 bytes each → ~177 entries fit per 4 KiB block, so
    // entries 0-176 land in block 0 and entries 177-199 in block 1.
    let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0u32..200)
        .map(|i| {
            (
                format!("key{i:04}").into_bytes(),
                Some(format!("val{i:04}").into_bytes()),
            )
        })
        .collect();

    let writer = SSTableWriter::new(&path).unwrap();
    writer.write(entries.into_iter()).unwrap();

    // Flip byte 0 — this is the first byte of the first block's CRC field.
    // The stored CRC no longer matches the block body.
    let mut file_bytes = std::fs::read(&path).unwrap();
    file_bytes[0] ^= 0xFF;
    std::fs::write(&path, &file_bytes).unwrap();

    // open() reads the last data block (block 1, unmodified) for largest_key
    // — this must succeed.
    let reader = SSTableReader::open(&path).unwrap();

    // A key in block 0 (corrupted) must return a Corruption error.
    let result = reader.get(b"key0000");
    assert!(
        matches!(result, Err(FerriteError::Corruption(_))),
        "expected Corruption error, got {result:?}"
    );

    // A key in block 1 (unmodified) must still be readable.
    let result2 = reader.get(b"key0177");
    assert!(
        matches!(result2, Ok(Some(Some(_)))),
        "key in unmodified block must still be readable, got {result2:?}"
    );
}

/// Writes a mix of values and tombstones, collects all entries via iter(), and
/// verifies the order and tombstone visibility. Tombstones must appear as
/// (key, None) in iteration order — the Engine uses iter() for cross-level
/// scan merges where tombstone visibility matters.
#[test]
fn test_iter_yields_all_in_order_including_tombstones() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let input: Vec<(Vec<u8>, Option<Vec<u8>>)> = vec![
        (b"a".to_vec(), Some(b"1".to_vec())),
        (b"b".to_vec(), None), // tombstone
        (b"c".to_vec(), Some(b"3".to_vec())),
        (b"d".to_vec(), None), // tombstone
        (b"e".to_vec(), Some(b"5".to_vec())),
    ];

    let writer = SSTableWriter::new(&path).unwrap();
    writer.write(input.iter().cloned()).unwrap();

    let reader = SSTableReader::open(&path).unwrap();
    let output: Vec<(Vec<u8>, Option<Vec<u8>>)> = reader
        .iter()
        .map(|r| r.expect("iter must not error on valid SSTable"))
        .collect();

    assert_eq!(output.len(), 5);
    assert_eq!(
        output, input,
        "iter must yield entries in original write order"
    );
}

/// Writes known entries and verifies that the SSTableMeta returned by write()
/// reports the correct smallest_key, largest_key, and entry_count.
#[test]
fn test_meta_smallest_largest_count() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let entries = vec![
        (b"apple".to_vec(), Some(b"a".to_vec())),
        (b"banana".to_vec(), Some(b"b".to_vec())),
        (b"cherry".to_vec(), None), // tombstone counts toward entry_count
        (b"date".to_vec(), Some(b"d".to_vec())),
    ];

    let writer = SSTableWriter::new(&path).unwrap();
    let meta = writer.write(entries.into_iter()).unwrap();

    assert_eq!(meta.entry_count, 4);
    assert_eq!(meta.smallest_key, b"apple");
    assert_eq!(meta.largest_key, b"date");
    assert_eq!(meta.path, path);
}

/// Writes a valid SSTable, then overwrites the magic bytes in the footer with
/// invalid bytes, and asserts that SSTableReader::open returns
/// Err(FerriteError::InvalidFormat).
#[test]
fn test_open_invalid_magic_rejected() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");

    let entries = vec![(b"k".to_vec(), Some(b"v".to_vec()))];
    let writer = SSTableWriter::new(&path).unwrap();
    writer.write(entries.into_iter()).unwrap();

    // Footer layout: [index_offset(8) | bloom_offset(8) | magic(4) | version(4)]
    // Magic is at bytes [file_size-8 .. file_size-4].
    let mut file_bytes = std::fs::read(&path).unwrap();
    let magic_start = file_bytes.len() - 8;
    file_bytes[magic_start] ^= 0xFF; // corrupt first byte of magic
    std::fs::write(&path, &file_bytes).unwrap();

    let result = SSTableReader::open(&path);
    assert!(
        matches!(result, Err(FerriteError::InvalidFormat(_))),
        "corrupted magic must return InvalidFormat"
    );
}
