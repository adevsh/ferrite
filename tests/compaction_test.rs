//! # compaction_test
//!
//! Integration tests for leveled compaction.
//!
//! ## Role in the LSM pipeline
//! These tests exercise the compaction lifecycle end-to-end: the trigger
//! threshold, overlap-based L0→L1 and L1→L2 rewrites, tombstone GC, tombstone
//! preservation when deeper overlap exists, cache invalidation for rewritten
//! files, and restart durability after compaction.

use std::path::Path;

use ferrite::cache::BlockCache;
use ferrite::compaction::Compactor;
use ferrite::engine::{Engine, EngineConfig};
use ferrite::error::Result;
use ferrite::sstable::{SSTableReader, SSTableWriter};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_reader(dir: &Path, filename: &str, key: &[u8]) -> Result<SSTableReader> {
    make_reader_entries(dir, filename, vec![(key.to_vec(), Some(b"v".to_vec()))])
}

fn make_reader_entries(
    dir: &Path,
    filename: &str,
    entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
) -> Result<SSTableReader> {
    let path = dir.join(filename);
    SSTableWriter::new(&path)?.write(entries.into_iter())?;
    SSTableReader::open(&path)
}

fn find_value_in_reader(reader: &SSTableReader, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
    for item in reader.iter() {
        let (entry_key, entry_val) = item?;
        if entry_key.as_slice() == key {
            return Ok(Some(entry_val));
        }
    }
    Ok(None)
}

fn find_value_in_level(level: &[SSTableReader], key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
    for reader in level {
        if let Some(value) = find_value_in_reader(reader, key)? {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn ranges_overlap(
    lhs_smallest: &[u8],
    lhs_largest: &[u8],
    rhs_smallest: &[u8],
    rhs_largest: &[u8],
) -> bool {
    !(lhs_largest < rhs_smallest || rhs_largest < lhs_smallest)
}

fn assert_level_non_overlapping(level: &[SSTableReader]) {
    for (idx, lhs) in level.iter().enumerate() {
        for rhs in &level[idx + 1..] {
            assert!(
                !ranges_overlap(
                    lhs.smallest_key(),
                    lhs.largest_key(),
                    rhs.smallest_key(),
                    rhs.largest_key(),
                ),
                "level contains overlapping ranges: {:?}-{:?} with {:?}-{:?}",
                lhs.smallest_key(),
                lhs.largest_key(),
                rhs.smallest_key(),
                rhs.largest_key(),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_should_compact_threshold() -> Result<()> {
    let dir = tempdir()?;
    let d = dir.path();

    let levels_3: Vec<Vec<SSTableReader>> = vec![(0u32..3)
        .map(|i| make_reader(d, &format!("t3_{i:08}.sst"), &format!("k{i}").into_bytes()))
        .collect::<Result<_>>()?];
    assert!(!Compactor::should_compact(&levels_3));

    let levels_4: Vec<Vec<SSTableReader>> = vec![(4u32..8)
        .map(|i| make_reader(d, &format!("t4_{i:08}.sst"), &format!("k{i}").into_bytes()))
        .collect::<Result<_>>()?];
    assert!(Compactor::should_compact(&levels_4));

    Ok(())
}

#[test]
fn test_compact_l0_to_l1_dedupes_and_clears_l0() -> Result<()> {
    let dir = tempdir()?;
    let mut config = EngineConfig::new(dir.path());
    config.memtable_threshold = 1;
    let mut engine = Engine::open(config)?;

    engine.put(b"mykey", b"v1")?;
    engine.put(b"mykey", b"v2")?;
    engine.put(b"mykey", b"v3")?;
    engine.put(b"mykey", b"v4")?;
    engine.flush()?;

    assert!(
        engine.levels()[0].is_empty(),
        "all overlapping L0 files must be rewritten"
    );
    assert_eq!(engine.levels().get(1).map_or(0, Vec::len), 1);
    assert_eq!(engine.get(b"mykey")?, Some(b"v4".to_vec()));
    assert_level_non_overlapping(&engine.levels()[1]);
    Ok(())
}

#[test]
fn test_compact_drops_tombstone_at_bottom_level() -> Result<()> {
    let dir = tempdir()?;
    let d = dir.path();

    let old_k = make_reader_entries(
        d,
        "L0_00000001.sst",
        vec![(b"k".to_vec(), Some(b"v".to_vec()))],
    )?;
    let old_k_path = old_k.path.clone();
    let tombstone_k = make_reader_entries(d, "L0_00000002.sst", vec![(b"k".to_vec(), None)])?;
    let tombstone_k_path = tombstone_k.path.clone();
    let ka = make_reader_entries(
        d,
        "L0_00000003.sst",
        vec![(b"ka".to_vec(), Some(b"va".to_vec()))],
    )?;
    let kb = make_reader_entries(
        d,
        "L0_00000004.sst",
        vec![(b"kb".to_vec(), Some(b"vb".to_vec()))],
    )?;

    let mut levels = vec![vec![old_k, tombstone_k, ka, kb]];
    let mut cache = BlockCache::new(1024 * 1024);
    let mut next_seq = 10u64;

    let merged = Compactor::run(&mut levels, d, &mut cache, &mut next_seq)?;
    assert_eq!(
        merged, 2,
        "only the overlapping k-range inputs should be rewritten"
    );
    assert_eq!(
        levels[0].len(),
        2,
        "non-overlapping L0 files should stay in place"
    );
    assert!(levels.get(1).is_some_and(|level| level.is_empty()));
    assert_eq!(
        find_value_in_level(&levels[0], b"ka")?,
        Some(Some(b"va".to_vec()))
    );
    assert_eq!(
        find_value_in_level(&levels[0], b"kb")?,
        Some(Some(b"vb".to_vec()))
    );
    assert!(!old_k_path.exists());
    assert!(!tombstone_k_path.exists());

    Ok(())
}

#[test]
fn test_compact_keeps_tombstone_when_deeper_overlap_exists() -> Result<()> {
    let dir = tempdir()?;
    let d = dir.path();

    let tombstone = make_reader_entries(d, "L0_00000001.sst", vec![(b"k".to_vec(), None)])?;
    let other1 = make_reader_entries(
        d,
        "L0_00000002.sst",
        vec![(b"x".to_vec(), Some(b"vx".to_vec()))],
    )?;
    let other2 = make_reader_entries(
        d,
        "L0_00000003.sst",
        vec![(b"y".to_vec(), Some(b"vy".to_vec()))],
    )?;
    let other3 = make_reader_entries(
        d,
        "L0_00000004.sst",
        vec![(b"z".to_vec(), Some(b"vz".to_vec()))],
    )?;
    let deeper = make_reader_entries(
        d,
        "L2_00000005.sst",
        vec![(b"k".to_vec(), Some(b"old".to_vec()))],
    )?;

    let mut levels = vec![
        vec![tombstone, other1, other2, other3],
        vec![],
        vec![deeper],
    ];
    let mut cache = BlockCache::new(1024 * 1024);
    let mut next_seq = 10u64;

    let merged = Compactor::run(&mut levels, d, &mut cache, &mut next_seq)?;
    assert_eq!(
        merged, 1,
        "only the tombstone file overlaps the rewrite range"
    );
    assert_eq!(levels[0].len(), 3);
    assert_eq!(levels[1].len(), 1);
    assert_eq!(levels[2].len(), 1);
    assert_eq!(find_value_in_level(&levels[1], b"k")?, Some(None));
    assert_level_non_overlapping(&levels[1]);
    assert_level_non_overlapping(&levels[2]);

    Ok(())
}

#[test]
fn test_l0_compaction_rewrites_only_overlapping_l1_files() -> Result<()> {
    let dir = tempdir()?;
    let d = dir.path();

    let l0_b = make_reader_entries(
        d,
        "L0_00000001.sst",
        vec![(b"b".to_vec(), Some(b"new-b".to_vec()))],
    )?;
    let l0_x = make_reader_entries(
        d,
        "L0_00000002.sst",
        vec![(b"x".to_vec(), Some(b"vx".to_vec()))],
    )?;
    let l0_y = make_reader_entries(
        d,
        "L0_00000003.sst",
        vec![(b"y".to_vec(), Some(b"vy".to_vec()))],
    )?;
    let l0_z = make_reader_entries(
        d,
        "L0_00000004.sst",
        vec![(b"z".to_vec(), Some(b"vz".to_vec()))],
    )?;

    let l1_a = make_reader_entries(
        d,
        "L1_00000005.sst",
        vec![(b"a".to_vec(), Some(b"old-a".to_vec()))],
    )?;
    let l1_b = make_reader_entries(
        d,
        "L1_00000006.sst",
        vec![(b"b".to_vec(), Some(b"old-b".to_vec()))],
    )?;
    let l1_b_path = l1_b.path.clone();
    let l1_d = make_reader_entries(
        d,
        "L1_00000007.sst",
        vec![(b"d".to_vec(), Some(b"old-d".to_vec()))],
    )?;
    let l1_d_path = l1_d.path.clone();

    let mut levels = vec![vec![l0_b, l0_x, l0_y, l0_z], vec![l1_a, l1_b, l1_d]];
    let mut cache = BlockCache::new(1024 * 1024);
    let mut next_seq = 10u64;

    let merged = Compactor::run(&mut levels, d, &mut cache, &mut next_seq)?;
    assert_eq!(
        merged, 2,
        "rewrite should consume the selected L0 file and the overlapping L1 file"
    );
    assert_eq!(levels[0].len(), 3);
    assert_eq!(levels[1].len(), 3);
    assert_eq!(
        find_value_in_level(&levels[1], b"a")?,
        Some(Some(b"old-a".to_vec()))
    );
    assert_eq!(
        find_value_in_level(&levels[1], b"b")?,
        Some(Some(b"new-b".to_vec()))
    );
    assert_eq!(
        find_value_in_level(&levels[1], b"d")?,
        Some(Some(b"old-d".to_vec()))
    );
    assert!(
        !l1_b_path.exists(),
        "overlapping target file should be rewritten away"
    );
    assert!(
        l1_d_path.exists(),
        "non-overlapping target file should stay on disk"
    );
    assert_level_non_overlapping(&levels[1]);

    Ok(())
}

#[test]
fn test_l1_to_l2_rewrites_overlapping_slice_and_keeps_l2_non_overlapping() -> Result<()> {
    let dir = tempdir()?;
    let d = dir.path();

    let l1_c = make_reader_entries(
        d,
        "L1_00000001.sst",
        vec![(b"c".to_vec(), Some(b"new-c".to_vec()))],
    )?;
    let l1_e = make_reader_entries(
        d,
        "L1_00000002.sst",
        vec![(b"e".to_vec(), Some(b"ve".to_vec()))],
    )?;
    let l1_g = make_reader_entries(
        d,
        "L1_00000003.sst",
        vec![(b"g".to_vec(), Some(b"vg".to_vec()))],
    )?;
    let l1_i = make_reader_entries(
        d,
        "L1_00000004.sst",
        vec![(b"i".to_vec(), Some(b"vi".to_vec()))],
    )?;

    let l2_a = make_reader_entries(
        d,
        "L2_00000005.sst",
        vec![(b"a".to_vec(), Some(b"va".to_vec()))],
    )?;
    let l2_cd = make_reader_entries(
        d,
        "L2_00000006.sst",
        vec![
            (b"c".to_vec(), Some(b"old-c".to_vec())),
            (b"d".to_vec(), Some(b"vd".to_vec())),
        ],
    )?;
    let l2_k = make_reader_entries(
        d,
        "L2_00000007.sst",
        vec![(b"k".to_vec(), Some(b"vk".to_vec()))],
    )?;

    let mut levels = vec![
        vec![],
        vec![l1_c, l1_e, l1_g, l1_i],
        vec![l2_a, l2_cd, l2_k],
    ];
    let mut cache = BlockCache::new(1024 * 1024);
    let mut next_seq = 20u64;

    let merged = Compactor::run(&mut levels, d, &mut cache, &mut next_seq)?;
    assert_eq!(
        merged, 2,
        "rewrite should consume one L1 file and its overlapping L2 file"
    );
    assert_eq!(levels[1].len(), 3);
    assert_eq!(levels[2].len(), 3);
    assert_eq!(
        find_value_in_level(&levels[2], b"a")?,
        Some(Some(b"va".to_vec()))
    );
    assert_eq!(
        find_value_in_level(&levels[2], b"c")?,
        Some(Some(b"new-c".to_vec()))
    );
    assert_eq!(
        find_value_in_level(&levels[2], b"d")?,
        Some(Some(b"vd".to_vec()))
    );
    assert_eq!(
        find_value_in_level(&levels[2], b"k")?,
        Some(Some(b"vk".to_vec()))
    );
    assert_level_non_overlapping(&levels[2]);

    Ok(())
}

#[test]
fn test_data_intact_through_3_compaction_cycles() -> Result<()> {
    let dir = tempdir()?;
    let mut config = EngineConfig::new(dir.path());
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
            Some(expected.as_bytes().to_vec())
        );
    }

    for level in engine.levels().iter().skip(1) {
        assert_level_non_overlapping(level);
    }

    Ok(())
}

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

    let l0_count = engine.levels().first().map_or(0, Vec::len);
    assert!(
        l0_count < 4,
        "Level 0 should be reduced below the trigger threshold; found {l0_count}"
    );

    for level in engine.levels().iter().skip(1) {
        assert_level_non_overlapping(level);
    }

    for i in 0u32..20 {
        let key = format!("{i:04}");
        let expected = format!("v{i:04}");
        assert_eq!(
            engine.get(key.as_bytes())?,
            Some(expected.as_bytes().to_vec())
        );
    }

    Ok(())
}

#[test]
fn test_restart_engine_after_compaction() -> Result<()> {
    let dir = tempdir()?;
    let n = 8u32;

    {
        let mut config = EngineConfig::new(dir.path());
        config.memtable_threshold = 1;
        let mut engine = Engine::open(config)?;
        for i in 0..n {
            let key = format!("{i:04}");
            let val = format!("v{i:08}");
            engine.put(key.as_bytes(), val.as_bytes())?;
        }
        engine.flush()?;
    }

    let manifest_path = dir.path().join("MANIFEST");
    assert!(manifest_path.exists());
    let manifest_seq: u64 = std::fs::read_to_string(&manifest_path)?
        .trim()
        .parse()
        .expect("MANIFEST must contain a valid u64");
    assert!(manifest_seq > 0);

    let mut engine = Engine::open(EngineConfig::new(dir.path()))?;
    for i in 0..n {
        let key = format!("{i:04}");
        let expected = format!("v{i:08}");
        assert_eq!(
            engine.get(key.as_bytes())?,
            Some(expected.as_bytes().to_vec())
        );
    }

    for level in engine.levels().iter().skip(1) {
        assert_level_non_overlapping(level);
    }

    Ok(())
}

#[test]
fn test_cache_invalidated_for_rewritten_sstables() -> Result<()> {
    let dir = tempdir()?;
    let d = dir.path();

    let l0_b = make_reader_entries(
        d,
        "L0_00000001.sst",
        vec![(b"b".to_vec(), Some(b"new-b".to_vec()))],
    )?;
    let l1_b = make_reader_entries(
        d,
        "L1_00000002.sst",
        vec![(b"b".to_vec(), Some(b"old-b".to_vec()))],
    )?;
    let l1_d = make_reader_entries(
        d,
        "L1_00000003.sst",
        vec![(b"d".to_vec(), Some(b"old-d".to_vec()))],
    )?;
    let l0_x = make_reader_entries(
        d,
        "L0_00000004.sst",
        vec![(b"x".to_vec(), Some(b"vx".to_vec()))],
    )?;
    let l0_y = make_reader_entries(
        d,
        "L0_00000005.sst",
        vec![(b"y".to_vec(), Some(b"vy".to_vec()))],
    )?;
    let l0_z = make_reader_entries(
        d,
        "L0_00000006.sst",
        vec![(b"z".to_vec(), Some(b"vz".to_vec()))],
    )?;

    let mut levels = vec![vec![l0_b, l0_x, l0_y, l0_z], vec![l1_b, l1_d]];
    let mut cache = BlockCache::new(1024 * 1024);
    let mut next_seq = 10u64;

    levels[0][0].get_with_cache(b"b", &mut cache)?;
    levels[1][0].get_with_cache(b"b", &mut cache)?;
    levels[1][1].get_with_cache(b"d", &mut cache)?;
    assert_eq!(
        cache.len(),
        3,
        "cache should contain one block per warmed file"
    );

    Compactor::run(&mut levels, d, &mut cache, &mut next_seq)?;
    assert_eq!(
        cache.len(),
        1,
        "rewritten source and target files should be invalidated"
    );
    assert_eq!(
        find_value_in_level(&levels[1], b"d")?,
        Some(Some(b"old-d".to_vec()))
    );

    Ok(())
}

#[test]
fn test_compact_returns_files_merged_count() -> Result<()> {
    let dir = tempdir()?;
    let d = dir.path();

    let mut levels: Vec<Vec<SSTableReader>> = vec![(0u32..4)
        .map(|i| {
            make_reader_entries(
                d,
                &format!("cnt_{i:08}.sst"),
                vec![(b"same-key".to_vec(), Some(format!("v{i}").into_bytes()))],
            )
        })
        .collect::<Result<_>>()?];

    let mut cache = BlockCache::new(1024 * 1024);
    let mut next_seq = 10u64;

    let merged = Compactor::run(&mut levels, d, &mut cache, &mut next_seq)?;
    assert_eq!(merged, 4);

    let merged2 = Compactor::run(&mut levels, d, &mut cache, &mut next_seq)?;
    assert_eq!(merged2, 0);

    Ok(())
}
