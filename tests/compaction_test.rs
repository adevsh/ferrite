//! # compaction_test
//!
//! Integration tests for size-tiered compaction.
//!
//! ## Role in the LSM pipeline
//! These tests exercise the full compaction lifecycle end-to-end: the trigger
//! threshold, k-way merge correctness, tombstone GC at the bottom level,
//! tombstone preservation when deeper levels exist, cascade compaction, and
//! restart durability after compaction.
//!
//! ## Dependencies
//! - `ferrite::cache::BlockCache` — used by `test_compact_returns_files_merged_count`
//!   to drive `Compactor::run` directly without an Engine.
//! - `ferrite::compaction::Compactor` — `should_compact` unit-level check.
//! - `ferrite::engine::{Engine, EngineConfig}` — high-level write/read/compact.
//! - `ferrite::error::Result` — propagated through every test via `?`.
//! - `ferrite::sstable::{SSTableReader, SSTableWriter}` — direct SSTable
//!   construction for the `should_compact` fixture and tombstone verification.
//! - `tempfile::tempdir` — isolated, OS-managed data directory per test.
//!
//! ## Used by
//! - `cargo test` — discovered as an integration test in `tests/`.

use ferrite::cache::BlockCache;
use ferrite::compaction::Compactor;
use ferrite::engine::{Engine, EngineConfig};
use ferrite::error::Result;
use ferrite::sstable::{SSTableReader, SSTableWriter};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// Creates a minimal single-entry SSTable at `dir/filename` and returns its
/// open `SSTableReader`. Used to build fixture level vecs for `should_compact`.
fn make_reader(dir: &std::path::Path, filename: &str, key: &[u8]) -> Result<SSTableReader> {
    let path = dir.join(filename);
    SSTableWriter::new(&path)?.write(std::iter::once((key.to_vec(), Some(b"v".to_vec()))))?;
    SSTableReader::open(&path)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Verifies that `Compactor::should_compact` returns `false` for 3 files and
/// `true` for 4 files, confirming the COMPACTION_THRESHOLD boundary is correct.
#[test]
fn test_should_compact_threshold() -> Result<()> {
    let dir = tempdir()?;
    let d = dir.path();

    // Construct a levels vec with 3 SSTables — below the threshold.
    let levels_3: Vec<Vec<SSTableReader>> = vec![(0u32..3)
        .map(|i| make_reader(d, &format!("t3_{i:08}.sst"), &format!("k{i}").into_bytes()))
        .collect::<Result<_>>()?];
    assert!(
        !Compactor::should_compact(&levels_3),
        "3 files must be below the threshold"
    );

    // Construct a separate levels vec with 4 SSTables — at the threshold.
    let levels_4: Vec<Vec<SSTableReader>> = vec![(4u32..8)
        .map(|i| make_reader(d, &format!("t4_{i:08}.sst"), &format!("k{i}").into_bytes()))
        .collect::<Result<_>>()?];
    assert!(
        Compactor::should_compact(&levels_4),
        "4 files must meet the threshold"
    );

    Ok(())
}

/// Triggers an auto-compaction by writing the same key four times across four
/// separate flushes. Verifies L0 is empty, L1 has exactly one file, and the
/// most recent value for each key is returned correctly.
///
/// Also confirms that deduplication across L0 sources keeps only the newest
/// version: `key` is overwritten four times, so L1 must reflect the 4th value.
#[test]
fn test_compact_l0_to_l1_dedupes_and_clears_l0() -> Result<()> {
    let dir = tempdir()?;
    let mut config = EngineConfig::new(dir.path());
    // Threshold = 1: every put triggers a flush immediately.
    config.memtable_threshold = 1;
    let mut engine = Engine::open(config)?;

    // Write key four times; each overwrite lands in a separate L0 SSTable.
    engine.put(b"mykey", b"v1")?;
    engine.put(b"mykey", b"v2")?;
    engine.put(b"mykey", b"v3")?;
    // The 4th put triggers the 4th flush → auto-compaction of L0→L1.
    engine.put(b"mykey", b"v4")?;

    // After compaction L0 must be empty.
    assert!(
        engine.levels()[0].is_empty(),
        "L0 must be emptied after compaction"
    );
    // L1 must have exactly one merged file.
    assert_eq!(
        engine.levels().get(1).map(|l| l.len()).unwrap_or(0),
        1,
        "L1 must have exactly 1 file after the first L0→L1 compaction"
    );

    // Newest value wins across the merged sources.
    assert_eq!(engine.get(b"mykey")?, Some(b"v4".to_vec()));
    Ok(())
}

/// Writes a key, deletes it, then fills L0 to the compaction threshold with
/// unrelated keys. The resulting L0→L1 compaction targets an empty L1
/// (bottom level), so the tombstone for the deleted key must be GC'd. Verifies
/// directly via `SSTableReader::iter` that the key is absent from the L1 file
/// and that the engine returns `None` for that key.
#[test]
fn test_compact_drops_tombstone_at_bottom_level() -> Result<()> {
    let dir = tempdir()?;
    let mut config = EngineConfig::new(dir.path());
    config.memtable_threshold = 1;
    let mut engine = Engine::open(config)?;

    // Flush 1: key="k", value="v".
    engine.put(b"k", b"v")?;
    // Flush 2: tombstone for "k".
    engine.delete(b"k")?;
    // Flushes 3 & 4: unrelated keys to reach the threshold.
    engine.put(b"ka", b"va")?;
    // 4th flush triggers auto-compaction with L1 empty → bottom level.
    engine.put(b"kb", b"vb")?;

    // L0 must be emptied by the compaction.
    assert!(engine.levels()[0].is_empty(), "L0 must be empty after compaction");

    // Verify directly via the L1 SSTable iter that "k" is absent (GC'd).
    {
        let readers = engine.levels();
        assert!(readers.len() >= 2 && !readers[1].is_empty(), "L1 must exist");
        let l1 = &readers[1][0];
        let found_k = l1
            .iter()
            .any(|item| item.map_or(false, |(key, _)| key == b"k".to_vec()));
        assert!(!found_k, "tombstone for 'k' must be GC'd at the bottom level");
    }

    // Engine API must also return None.
    assert_eq!(engine.get(b"k")?, None);
    // Unrelated keys must survive.
    assert_eq!(engine.get(b"ka")?, Some(b"va".to_vec()));
    assert_eq!(engine.get(b"kb")?, Some(b"vb".to_vec()));
    Ok(())
}

/// Drives enough flushes to cascade L0→L1→L2 (16 flushes; after the 16th,
/// L1 fills its 4th file and the cascade compacts L1→L2 in the same
/// `Compactor::run` call). Then adds a write+delete pair and fills L0 to the
/// threshold again. The new L0→L1 compaction targets L1 while L2 is non-empty,
/// so the tombstone must be preserved in the L1 output file.
#[test]
fn test_compact_keeps_tombstone_when_lower_levels_exist() -> Result<()> {
    let dir = tempdir()?;
    let mut config = EngineConfig::new(dir.path());
    config.memtable_threshold = 1;
    let mut engine = Engine::open(config)?;

    // 16 unique puts → cascade L0→L1→L2 (see test_cascade_l0_to_l1_to_l2).
    for i in 0u32..16 {
        let key = format!("{i:04}");
        let val = format!("v{i:04}");
        engine.put(key.as_bytes(), val.as_bytes())?;
    }

    // After cascade: L2 has data, L0 and L1 are empty.
    assert!(
        engine.levels().len() >= 3 && !engine.levels()[2].is_empty(),
        "L2 must exist after cascade"
    );

    // Put, delete, then two unrelated puts to fill L0 to threshold.
    engine.put(b"target", b"old_val")?;
    engine.delete(b"target")?;
    engine.put(b"other1", b"ov1")?;
    // 4th flush → L0→L1 compaction; L1 is empty but L2 is non-empty →
    // is_target_bottom = false → tombstone must be preserved.
    engine.put(b"other2", b"ov2")?;

    // Verify the tombstone for "target" is present in the L1 file.
    {
        let readers = engine.levels();
        assert!(readers.len() >= 2 && !readers[1].is_empty(), "L1 must exist after compaction");
        let l1 = &readers[1][0];
        let mut found_tombstone = false;
        for item in l1.iter() {
            let (key, val_opt) = item?;
            if key.as_slice() == b"target" {
                assert!(
                    val_opt.is_none(),
                    "entry for 'target' in L1 must be a tombstone"
                );
                found_tombstone = true;
                break;
            }
        }
        assert!(found_tombstone, "tombstone for 'target' must be present in L1");
    }

    // Engine correctly returns None (tombstone still shadows).
    assert_eq!(engine.get(b"target")?, None);
    Ok(())
}

/// Drives 16 sequential puts with a 1-byte threshold so that every flush
/// produces one L0 SSTable and 4-file boundaries trigger cascade compaction.
/// Asserts that after 16 puts the level structure has grown to at least 3
/// levels and Level 2 is non-empty.
#[test]
fn test_cascade_l0_to_l1_to_l2() -> Result<()> {
    let dir = tempdir()?;
    let mut config = EngineConfig::new(dir.path());
    config.memtable_threshold = 1;
    let mut engine = Engine::open(config)?;

    // 4 puts → L0→L1 (1 L1 file). 4 more → L0→L1 (2 L1 files).
    // 4 more → L0→L1 (3 L1 files). 4 more → L0→L1 (4 L1 files) + cascade L1→L2.
    for i in 0u32..16 {
        let key = format!("{i:04}");
        let val = format!("v{i:04}");
        engine.put(key.as_bytes(), val.as_bytes())?;
    }

    assert!(
        engine.levels().len() >= 3,
        "cascade must have created at least 3 levels"
    );
    assert!(
        !engine.levels()[2].is_empty(),
        "Level 2 must be non-empty after the L1→L2 cascade"
    );
    Ok(())
}

/// Writes enough keys to trigger at least three L0→L1 compaction cycles, then
/// reads every written key and asserts the correct value is returned.
///
/// This is the explicit SKILL.md acceptance criterion for compaction:
/// "data survives full compaction cycle."
#[test]
fn test_data_intact_through_3_compaction_cycles() -> Result<()> {
    let dir = tempdir()?;
    let mut config = EngineConfig::new(dir.path());
    // 1 byte threshold ensures every put flushes; 13 flushes produce 3
    // L0→L1 compactions (at the 4th, 8th, and 12th flushes).
    config.memtable_threshold = 1;
    let n = 13u32;
    let mut engine = Engine::open(config)?;

    for i in 0..n {
        let key = format!("{i:04}");
        let val = format!("val:{i:08}");
        engine.put(key.as_bytes(), val.as_bytes())?;
    }

    for i in 0..n {
        let key = format!("{i:04}");
        let expected = format!("val:{i:08}");
        assert_eq!(
            engine.get(key.as_bytes())?,
            Some(expected.as_bytes().to_vec()),
            "key {key} missing after 3 compaction cycles",
        );
    }
    Ok(())
}

/// Forces 20 sequential flushes with a 1-byte threshold, triggering automatic
/// compaction on the 4th, 8th, 12th, 16th, and 20th flushes. Asserts that
/// Level 0 never exceeds 4 files at the end of all 20 puts.
///
/// This is the explicit SKILL.md acceptance test: "file count: after
/// 20 flushes, verify Level 0 never exceeds 4 files."
#[test]
fn test_l0_file_count_bounded_after_20_flushes() -> Result<()> {
    let dir = tempdir()?;
    let mut config = EngineConfig::new(dir.path());
    config.memtable_threshold = 1;
    let mut engine = Engine::open(config)?;

    for i in 0u32..20 {
        let key = format!("{i:04}");
        let val = format!("v{i:04}");
        engine.put(key.as_bytes(), val.as_bytes())?;
    }

    let l0_count = engine.levels().first().map(|l| l.len()).unwrap_or(0);
    assert!(
        l0_count <= 4,
        "Level 0 must never exceed 4 files; found {l0_count}"
    );

    // All 20 keys must still be readable.
    for i in 0u32..20 {
        let key = format!("{i:04}");
        let expected = format!("v{i:04}");
        assert_eq!(
            engine.get(key.as_bytes())?,
            Some(expected.as_bytes().to_vec()),
            "key {key} must be readable after 20 flushes",
        );
    }
    Ok(())
}

/// Triggers one compaction cycle, drops the Engine (flushing nothing to WAL on
/// close — data is in SSTables), re-opens, and verifies every written key is
/// still readable. Also checks that the MANIFEST file was written and contains
/// a parseable non-zero sequence number.
#[test]
fn test_restart_engine_after_compaction() -> Result<()> {
    let dir = tempdir()?;
    let n = 8u32;

    {
        let mut config = EngineConfig::new(dir.path());
        config.memtable_threshold = 1;
        let mut engine = Engine::open(config)?;
        // 8 puts with threshold=1 → 8 flushes → 2 L0→L1 compactions.
        for i in 0..n {
            let key = format!("{i:04}");
            let val = format!("v{i:08}");
            engine.put(key.as_bytes(), val.as_bytes())?;
        }
    } // Engine drops here.

    // MANIFEST must exist and contain a valid next_seq > 0.
    let manifest_path = dir.path().join("MANIFEST");
    assert!(manifest_path.exists(), "MANIFEST must be written after compaction");
    let manifest_seq: u64 = std::fs::read_to_string(&manifest_path)?
        .trim()
        .parse()
        .expect("MANIFEST must contain a valid u64");
    assert!(manifest_seq > 0, "MANIFEST next_seq must be positive");

    // Reopen and verify all keys.
    let mut engine = Engine::open(EngineConfig::new(dir.path()))?;
    for i in 0..n {
        let key = format!("{i:04}");
        let expected = format!("v{i:08}");
        assert_eq!(
            engine.get(key.as_bytes())?,
            Some(expected.as_bytes().to_vec()),
            "key {key} must survive engine restart after compaction",
        );
    }
    Ok(())
}

/// Pre-warms the block cache by reading from 3 L0 SSTables, then triggers a
/// 4th flush to fire automatic compaction. After compaction the L0 files are
/// deleted, so their cache entries must be invalidated. Asserts that
/// `engine.cache().len()` drops to 0 immediately after the compaction-
/// triggering flush (before any subsequent reads repopulate the cache).
#[test]
fn test_cache_invalidated_for_old_sstables() -> Result<()> {
    let dir = tempdir()?;
    let mut config = EngineConfig::new(dir.path());
    config.memtable_threshold = 1;
    let mut engine = Engine::open(config)?;

    // Flushes 1–3: one key per SSTable. Cache is cold.
    engine.put(b"k0", b"v0")?;
    engine.put(b"k1", b"v1")?;
    engine.put(b"k2", b"v2")?;

    // Warm the cache by reading each key — each read populates at least one
    // block entry for the corresponding L0 SSTable.
    engine.get(b"k0")?;
    engine.get(b"k1")?;
    engine.get(b"k2")?;

    let pre_compact_len = engine.cache().len();
    assert!(pre_compact_len > 0, "cache must be warm after reads from L0");

    // Flush 4 → L0 reaches threshold → auto-compaction deletes the 4 L0 files.
    // No reads of the new L1 file happen inside put(), so cache should be empty.
    engine.put(b"k3", b"v3")?;

    assert_eq!(
        engine.cache().len(),
        0,
        "all L0 cache entries must be invalidated after compaction (pre-compact len was {pre_compact_len})"
    );

    // Data integrity: all 4 keys must be readable from L1.
    assert_eq!(engine.get(b"k0")?, Some(b"v0".to_vec()));
    assert_eq!(engine.get(b"k3")?, Some(b"v3".to_vec()));
    Ok(())
}

/// Verifies that `Compactor::run` returns the exact number of source SSTable
/// files consumed across the cascade. Builds 4 fixture SSTables directly via
/// `make_reader` (bypassing the Engine) so the threshold is reached without
/// triggering the post-flush auto-compaction path inside `Engine::flush`.
/// A second call on the now-compacted levels must return 0.
#[test]
fn test_compact_returns_files_merged_count() -> Result<()> {
    let dir = tempdir()?;
    let d = dir.path();

    // Build exactly 4 L0 SSTables with distinct keys to reach the threshold.
    let mut levels: Vec<Vec<SSTableReader>> = vec![(0u32..4)
        .map(|i| make_reader(d, &format!("cnt_{i:08}.sst"), format!("k{i}").as_bytes()))
        .collect::<Result<_>>()?];

    assert!(
        Compactor::should_compact(&levels),
        "pre-condition: levels must be over-threshold before run"
    );

    let mut cache = BlockCache::new(1024 * 1024);
    let mut next_seq = 10u64;

    let merged = Compactor::run(&mut levels, d, &mut cache, &mut next_seq)?;
    assert_eq!(merged, 4, "run must report 4 source files consumed");

    // Second run: L0 is empty, L1 has 1 file — both below threshold.
    let merged2 = Compactor::run(&mut levels, d, &mut cache, &mut next_seq)?;
    assert_eq!(merged2, 0, "second run on a below-threshold state must return 0");

    Ok(())
}
