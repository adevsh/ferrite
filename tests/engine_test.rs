//! # engine_test
//!
//! Integration tests for the `Engine` LSM coordinator.
//!
//! ## Role in the LSM pipeline
//! These tests exercise the complete write/read/delete/flush/restart cycle end-
//! to-end: WAL durability, Memtable-to-SSTable flushes, tombstone propagation,
//! newest-wins deduplication across layers, prefix scans, and the 100k
//! sequential-put/get acceptance criterion from SKILL.md.
//!
//! ## Dependencies
//! - `ferrite::engine::{Engine, EngineConfig, EngineStats}` — the module under test.
//! - `ferrite::error::Result` — propagated through every test via `?`.
//! - `tempfile::tempdir` — provides an isolated, OS-managed data directory per
//!   test that is deleted automatically when the `TempDir` guard drops.
//!
//! ## Used by
//! - `cargo test` — discovered as an integration test because this file lives
//!   directly in `tests/`.

use ferrite::engine::{Engine, EngineConfig, EngineStats};
use ferrite::error::Result;
use tempfile::tempdir;

/// Opens an engine, puts five distinct key-value pairs, and verifies each
/// can be read back before any flush occurs.
#[test]
fn test_put_get_basic() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::open(EngineConfig::new(dir.path()))?;

    engine.put(b"alpha", b"1")?;
    engine.put(b"beta", b"2")?;
    engine.put(b"gamma", b"3")?;
    engine.put(b"delta", b"4")?;
    engine.put(b"epsilon", b"5")?;

    assert_eq!(engine.get(b"alpha")?, Some(b"1".to_vec()));
    assert_eq!(engine.get(b"beta")?, Some(b"2".to_vec()));
    assert_eq!(engine.get(b"gamma")?, Some(b"3".to_vec()));
    assert_eq!(engine.get(b"delta")?, Some(b"4".to_vec()));
    assert_eq!(engine.get(b"epsilon")?, Some(b"5".to_vec()));
    assert_eq!(engine.get(b"missing")?, None);
    Ok(())
}

/// Uses a 128-byte Memtable threshold so that puts across multiple keys
/// trigger several automatic flushes. Verifies every written key reads back
/// the correct value regardless of whether it lives in the Memtable or an
/// L0 SSTable.
#[test]
fn test_put_get_across_flush_boundary() -> Result<()> {
    let dir = tempdir()?;
    let mut config = EngineConfig::new(dir.path());
    // 128-byte threshold; each key+value = 8 bytes → flush every 16 entries.
    config.memtable_threshold = 128;
    let mut engine = Engine::open(config)?;

    let n = 50u32;
    for i in 0..n {
        let key = format!("k{i:03}");
        let val = format!("v{i:03}");
        engine.put(key.as_bytes(), val.as_bytes())?;
    }

    for i in 0..n {
        let key = format!("k{i:03}");
        let expected = format!("v{i:03}");
        assert_eq!(
            engine.get(key.as_bytes())?,
            Some(expected.as_bytes().to_vec()),
            "key {key} missing after flush boundary",
        );
    }
    Ok(())
}

/// Puts a key, verifies the value is present, deletes it, and verifies
/// `get` returns `None`. Then forces a flush and verifies the tombstone
/// still shadows the value — the deleted key must not reappear from the
/// L0 SSTable.
#[test]
fn test_delete_tombstone_before_and_after_flush() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::open(EngineConfig::new(dir.path()))?;

    engine.put(b"mykey", b"myval")?;
    assert_eq!(engine.get(b"mykey")?, Some(b"myval".to_vec()));

    engine.delete(b"mykey")?;
    assert_eq!(engine.get(b"mykey")?, None, "tombstone must shadow immediately");

    // Flush forces both the tombstone (now in Memtable) and an older value
    // (also in Memtable from the put above) into the same L0 SSTable entry.
    engine.flush()?;
    assert_eq!(engine.get(b"mykey")?, None, "tombstone must survive flush");
    Ok(())
}

/// Puts `key`=`v1`, flushes to L0 SSTable, then puts `key`=`v2` (which
/// lives only in the Memtable at that point). Asserts that `get` returns
/// `v2`, verifying that the Memtable layer shadows the older SSTable value.
#[test]
fn test_overwrite_after_flush_returns_newest() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::open(EngineConfig::new(dir.path()))?;

    engine.put(b"key", b"v1")?;
    engine.flush()?;

    engine.put(b"key", b"v2")?;

    assert_eq!(
        engine.get(b"key")?,
        Some(b"v2".to_vec()),
        "Memtable must shadow older SSTable value",
    );
    Ok(())
}

/// Puts 100 keys without triggering a flush (well within the default 4 MiB
/// threshold), drops the Engine so data lives only in the WAL, then reopens
/// and verifies all 100 keys recover correctly.
///
/// Exercises the `Wal::recover` → `Memtable::restore_from_wal` path.
#[test]
fn test_restart_engine_recovers_memtable_from_wal() -> Result<()> {
    let dir = tempdir()?;
    let config = EngineConfig::new(dir.path());

    {
        let mut engine = Engine::open(config.clone())?;
        for i in 0u32..100 {
            let key = format!("key{i:03}");
            let val = format!("val{i:03}");
            engine.put(key.as_bytes(), val.as_bytes())?;
            // 100 × (6+6) = 1200 bytes << 4 MiB threshold → no auto-flush.
        }
    } // Engine dropped here; data lives only in wal.log.

    let mut engine = Engine::open(config)?;
    for i in 0u32..100 {
        let key = format!("key{i:03}");
        let expected = format!("val{i:03}");
        assert_eq!(
            engine.get(key.as_bytes())?,
            Some(expected.as_bytes().to_vec()),
            "key {key} not recovered from WAL",
        );
    }
    Ok(())
}

/// Puts enough data to trigger at least one auto-flush (small threshold),
/// drops the Engine, reopens it, and verifies all keys can be read back.
///
/// Exercises the SSTable-file rescan path in `Engine::open` (the code that
/// rebuilds `self.levels` from the `L0_*.sst` files found on disk).
#[test]
fn test_restart_engine_after_flush_reads_from_sstable() -> Result<()> {
    let dir = tempdir()?;
    let mut config = EngineConfig::new(dir.path());
    // 256-byte threshold; each key+value ≈ 10 bytes → flush every ~25 entries.
    config.memtable_threshold = 256;

    let n = 80u32;

    {
        let mut engine = Engine::open(config.clone())?;
        for i in 0..n {
            let key = format!("r{i:04}");
            let val = format!("s{i:04}");
            engine.put(key.as_bytes(), val.as_bytes())?;
        }
        // Flush any remaining Memtable data before dropping.
        engine.flush()?;
    }

    let mut engine = Engine::open(config)?;
    for i in 0..n {
        let key = format!("r{i:04}");
        let expected = format!("s{i:04}");
        assert_eq!(
            engine.get(key.as_bytes())?,
            Some(expected.as_bytes().to_vec()),
            "key {key} missing after restart",
        );
    }
    Ok(())
}

/// Writes two keys to L0 via an explicit flush, then writes a new value for
/// one key and a new key to the Memtable, and deletes the second original
/// key. Verifies that `scan_prefix` returns the correct newest-wins,
/// tombstone-filtered result across both layers.
///
/// Expected output: `[(user:a, v1_new), (user:c, v3)]`
/// — `user:b` is filtered because of the Memtable tombstone.
/// — `user:a` returns `v1_new` (Memtable), not `v1` (L0 SSTable).
#[test]
fn test_scan_prefix_dedupes_across_layers() -> Result<()> {
    let dir = tempdir()?;
    let mut engine = Engine::open(EngineConfig::new(dir.path()))?;

    // L0 layer: user:a=v1, user:b=v2.
    engine.put(b"user:a", b"v1")?;
    engine.put(b"user:b", b"v2")?;
    engine.flush()?;

    // Memtable layer: user:a updated, user:b deleted, user:c added.
    engine.put(b"user:a", b"v1_new")?;
    engine.put(b"user:c", b"v3")?;
    engine.delete(b"user:b")?;

    let results = engine.scan_prefix(b"user:")?;

    assert_eq!(results.len(), 2, "user:b tombstone must be filtered");
    assert_eq!(results[0], (b"user:a".to_vec(), b"v1_new".to_vec()));
    assert_eq!(results[1], (b"user:c".to_vec(), b"v3".to_vec()));
    Ok(())
}

/// Writes 100 000 sequential key-value pairs with a 64 KiB Memtable
/// threshold (triggering ~30 flushes), then reads every key back and
/// verifies the value.
///
/// This is the explicit SKILL.md acceptance criterion for the Engine:
/// "full put/get/delete/scan lifecycle works across WAL recovery and
/// SSTable flushes."
#[test]
fn test_100k_sequential_puts_then_reads() -> Result<()> {
    let dir = tempdir()?;
    let mut config = EngineConfig::new(dir.path());
    // 64 KiB threshold; each entry ≈ 20 bytes → ~3 276 entries per SSTable.
    config.memtable_threshold = 64 * 1024;
    let mut engine = Engine::open(config)?;

    let n = 100_000u32;

    for i in 0..n {
        let key = format!("{i:08}");
        let val = format!("val:{i:08}");
        engine.put(key.as_bytes(), val.as_bytes())?;
    }

    for i in 0..n {
        let key = format!("{i:08}");
        let expected = format!("val:{i:08}");
        assert_eq!(
            engine.get(key.as_bytes())?,
            Some(expected.as_bytes().to_vec()),
            "key {key} missing at i={i}",
        );
    }
    Ok(())
}

/// Verifies that `Engine::stats()` reports a non-zero `memtable_size_bytes`
/// after inserts that have not yet triggered a flush. Confirms that stats
/// is accessible as `&self` (no `&mut self` needed).
#[test]
fn test_stats_reflects_memtable_size() -> Result<()> {
    let dir = tempdir()?;
    // Large threshold so nothing flushes; all data stays in the Memtable.
    let mut config = EngineConfig::new(dir.path());
    config.memtable_threshold = 4 * 1024 * 1024;
    let mut engine = Engine::open(config)?;

    engine.put(b"alpha", b"one")?;
    engine.put(b"beta", b"two")?;
    engine.put(b"gamma", b"three")?;

    let stats: EngineStats = engine.stats();
    assert!(
        stats.memtable_size_bytes > 0,
        "memtable size must be positive after inserts; got 0"
    );
    Ok(())
}

/// Verifies that `Engine::stats()` counts SSTables per level correctly.
/// After 4 auto-flushed puts compact L0 into one L1 file, the stats must
/// reflect L0 = 0 and L1 = 1.
#[test]
fn test_stats_counts_sstables_per_level() -> Result<()> {
    let dir = tempdir()?;
    let mut config = EngineConfig::new(dir.path());
    // threshold=1: every put triggers an immediate flush + auto-compaction check.
    config.memtable_threshold = 1;
    let mut engine = Engine::open(config)?;

    // 4 puts → 4 flushes → on the 4th, L0 reaches threshold and auto-compacts
    // into one L1 SSTable. After this: L0 = 0, L1 = 1.
    for i in 0u32..4 {
        engine.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())?;
    }

    let stats = engine.stats();
    assert!(
        !stats.sstable_count_per_level.is_empty(),
        "levels must be non-empty after flushes + compaction"
    );
    assert_eq!(
        stats.sstable_count_per_level[0],
        engine.levels()[0].len(),
        "stats L0 count must match engine.levels()[0].len()"
    );
    assert_eq!(
        stats.sstable_count_per_level[0],
        0,
        "L0 must be empty after auto-compaction"
    );
    assert_eq!(
        stats.sstable_count_per_level.get(1).copied().unwrap_or(0),
        1,
        "L1 must have exactly 1 merged SSTable"
    );
    Ok(())
}
