//! # wal_test
//!
//! Integration tests for the Write-Ahead Log.
//!
//! ## Role in the LSM pipeline
//! These tests exercise `Wal` in isolation — without an Engine or Memtable —
//! to verify durability, recovery, and corruption-handling guarantees before
//! higher-level phases depend on them.
//!
//! ## Dependencies
//! - `ferrite::wal` — the module under test.
//! - `tempfile` — provides `TempDir` for isolated, auto-cleaned test directories
//!   so tests can run in parallel without I/O conflicts.
//!
//! ## Used by
//! - `cargo test` — Cargo discovers this as an integration test target because
//!   it lives directly in the `tests/` directory.

use ferrite::wal::{Wal, WalRecord};
use tempfile::tempdir;

/// Appends 100 put records, drops the handle (without truncating), then
/// recovers and verifies every key/value is present and in insertion order.
#[test]
fn test_recovers_100_puts() {
    let dir = tempdir().unwrap();
    let mut wal = Wal::open(dir.path()).unwrap();
    for i in 0u32..100 {
        let key = format!("key{i:04}");
        let val = format!("val{i:04}");
        wal.append_put(key.as_bytes(), val.as_bytes()).unwrap();
    }
    drop(wal);

    let records = Wal::recover(dir.path()).unwrap();
    assert_eq!(records.len(), 100);
    for (i, record) in records.iter().enumerate() {
        match record {
            WalRecord::Put { key, value } => {
                assert_eq!(key.as_slice(), format!("key{i:04}").as_bytes());
                assert_eq!(value.as_slice(), format!("val{i:04}").as_bytes());
            }
            WalRecord::Delete { .. } => panic!("expected Put at index {i}"),
        }
    }
}

/// Appends an interleaved mix of puts and deletes, then recovers and verifies
/// the correct WalRecord variant, key, and value at each position.
#[test]
fn test_recovers_mixed_puts_and_deletes() {
    let dir = tempdir().unwrap();
    let mut wal = Wal::open(dir.path()).unwrap();

    wal.append_put(b"alpha", b"one").unwrap();
    wal.append_delete(b"alpha").unwrap();
    wal.append_put(b"beta", b"two").unwrap();
    wal.append_delete(b"beta").unwrap();
    wal.append_put(b"gamma", b"three").unwrap();
    drop(wal);

    let records = Wal::recover(dir.path()).unwrap();
    assert_eq!(records.len(), 5);
    assert!(matches!(&records[0], WalRecord::Put { key, .. } if key == b"alpha"));
    assert!(matches!(&records[1], WalRecord::Delete { key } if key == b"alpha"));
    assert!(matches!(&records[2], WalRecord::Put { key, .. } if key == b"beta"));
    assert!(matches!(&records[3], WalRecord::Delete { key } if key == b"beta"));
    assert!(matches!(&records[4], WalRecord::Put { key, .. } if key == b"gamma"));
}

/// Simulates a crash by writing records then dropping the Wal handle without
/// calling truncate. Verifies that reopening and recovering returns all records,
/// and that the file remains appendable after recovery.
#[test]
fn test_survives_simulated_crash() {
    let dir = tempdir().unwrap();

    {
        let mut wal = Wal::open(dir.path()).unwrap();
        wal.append_put(b"k1", b"v1").unwrap();
        wal.append_put(b"k2", b"v2").unwrap();
        wal.append_delete(b"k1").unwrap();
        // Drop without truncating — this is the simulated crash.
    }

    let records = Wal::recover(dir.path()).unwrap();
    assert_eq!(records.len(), 3);
    assert!(matches!(&records[0], WalRecord::Put { key, .. } if key == b"k1"));
    assert!(matches!(&records[1], WalRecord::Put { key, .. } if key == b"k2"));
    assert!(matches!(&records[2], WalRecord::Delete { key } if key == b"k1"));

    // Reopen must succeed (file is still appendable), and new records must
    // be recoverable after the pre-crash records.
    let mut wal = Wal::open(dir.path()).unwrap();
    wal.append_put(b"k3", b"v3").unwrap();
    drop(wal);

    let records = Wal::recover(dir.path()).unwrap();
    assert_eq!(records.len(), 4);
    assert!(matches!(&records[3], WalRecord::Put { key, .. } if key == b"k3"));
}

/// Writes 5 records, flips the last byte of the WAL file (corrupting the final
/// record's value), then recovers and verifies that the 4 intact records are
/// returned while the corrupt tail record is silently dropped.
#[test]
fn test_tail_corruption_is_dropped() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");

    let mut wal = Wal::open(dir.path()).unwrap();
    for i in 0u8..5 {
        // Each record: key = [b'k', i], value = [b'v', i].
        wal.append_put(&[b'k', i], &[b'v', i]).unwrap();
    }
    drop(wal);

    // Flip the last byte — it lands inside the value of the final record and
    // causes that record's CRC check to fail during recovery.
    let mut bytes = std::fs::read(&wal_path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    std::fs::write(&wal_path, &bytes).unwrap();

    let records = Wal::recover(dir.path()).unwrap();
    assert_eq!(
        records.len(),
        4,
        "corrupt tail record must be silently dropped"
    );
    // Verify the 4 surviving records are intact.
    for (i, record) in records.iter().enumerate() {
        assert!(matches!(record, WalRecord::Put { key, value }
                if key == &[b'k', i as u8] && value == &[b'v', i as u8]));
    }
}

/// Appends records, truncates the WAL, and verifies the file becomes zero bytes
/// and recovery returns an empty vec. Then writes again and confirms the new
/// records are readable from offset 0.
#[test]
fn test_truncate_resets_to_empty() {
    let dir = tempdir().unwrap();
    let mut wal = Wal::open(dir.path()).unwrap();

    wal.append_put(b"before-one", b"truncate").unwrap();
    wal.append_put(b"before-two", b"truncate").unwrap();
    wal.truncate().unwrap();

    let size = std::fs::metadata(dir.path().join("wal.log")).unwrap().len();
    assert_eq!(size, 0, "WAL file must be empty immediately after truncate");

    let records = Wal::recover(dir.path()).unwrap();
    assert!(records.is_empty(), "no records recoverable after truncate");

    // Writing after truncate on the same handle must work and be recoverable.
    wal.append_put(b"after", b"truncate").unwrap();
    drop(wal);

    let records = Wal::recover(dir.path()).unwrap();
    assert_eq!(records.len(), 1);
    assert!(matches!(&records[0], WalRecord::Put { key, value }
            if key == b"after" && value == b"truncate"));
}
