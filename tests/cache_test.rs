//! # cache_test
//!
//! Integration tests for the `BlockCache` LRU implementation.
//!
//! ## Role in the LSM pipeline
//! These tests exercise `BlockCache` in isolation — no SSTable files, no
//! filesystem I/O — to verify LRU eviction ordering, capacity management,
//! path-based invalidation, correct `Drop` behaviour, and the SKILL.md
//! acceptance criterion (hit ratio > 80 %) before the cache is wired into
//! the Engine.
//!
//! ## Dependencies
//! - `ferrite::cache::BlockCache` — the module under test.
//!
//! ## Used by
//! - `cargo test` — discovered as an integration test target because this
//!   file lives directly in `tests/`.

use std::path::PathBuf;

use ferrite::cache::BlockCache;

/// Constructs a `CacheKey` from a string path and block offset.
fn key(path: &str, offset: u64) -> (PathBuf, u64) {
    (PathBuf::from(path), offset)
}

/// Returns a `Vec<u8>` filled with `byte` repeated `len` times.
fn block(byte: u8, len: usize) -> Vec<u8> {
    vec![byte; len]
}

/// Inserts three distinct blocks into a cache with ample capacity, then
/// retrieves each one and verifies the returned bytes, hit/miss counters,
/// and the `size_bytes` total.
#[test]
fn test_basic_insert_and_get() {
    let mut cache = BlockCache::new(1024);

    cache.insert(key("/a.sst", 0), block(0x01, 4));
    cache.insert(key("/a.sst", 100), block(0x02, 8));
    cache.insert(key("/b.sst", 0), block(0x03, 16));

    assert_eq!(cache.get(&key("/a.sst", 0)).cloned(), Some(block(0x01, 4)));
    assert_eq!(
        cache.get(&key("/a.sst", 100)).cloned(),
        Some(block(0x02, 8))
    );
    assert_eq!(cache.get(&key("/b.sst", 0)).cloned(), Some(block(0x03, 16)));

    assert_eq!(
        cache.hit_count(),
        3,
        "every get on a present key must be a hit"
    );
    assert_eq!(cache.miss_count(), 0, "no misses when all keys are present");
    assert_eq!(cache.size_bytes(), 4 + 8 + 16);
    assert_eq!(cache.len(), 3);
}

/// Fills a 30-byte cache with three 10-byte blocks (k1, k2, k3), then inserts
/// a fourth 10-byte block (k4). Verifies that k1 — the LRU entry — was
/// evicted to make room, while k2, k3, and k4 remain.
///
/// Insert order k1→k2→k3 pushes each entry to the head, leaving k1 at the
/// tail (LRU position). k4's insertion raises total size to 40 bytes;
/// eviction brings it back to 30 by removing k1.
#[test]
fn test_capacity_eviction_lru_order() {
    let mut cache = BlockCache::new(30);

    cache.insert(key("/s.sst", 0), block(1, 10)); // k1 — tail after k2/k3 inserted
    cache.insert(key("/s.sst", 100), block(2, 10)); // k2
    cache.insert(key("/s.sst", 200), block(3, 10)); // k3 — head

    // After k1/k2/k3 the list is k3(head)→k2→k1(tail), size=30.
    // Inserting k4 pushes size to 40 → k1 evicted → size=30.
    cache.insert(key("/s.sst", 300), block(4, 10)); // k4

    assert_eq!(cache.len(), 3);
    assert_eq!(cache.size_bytes(), 30);

    assert_eq!(
        cache.get(&key("/s.sst", 0)),
        None,
        "k1 must have been evicted"
    );
    assert!(cache.get(&key("/s.sst", 100)).is_some(), "k2 must survive");
    assert!(cache.get(&key("/s.sst", 200)).is_some(), "k3 must survive");
    assert!(cache.get(&key("/s.sst", 300)).is_some(), "k4 must survive");
}

/// Verifies that accessing a block via `get` promotes it past blocks that
/// were inserted more recently, so it survives the next eviction.
///
/// After inserting k1, k2, k3 the LRU order is k3(head)→k2→k1(tail).
/// Calling `get(k1)` promotes k1 to head, making the order k1→k3→k2(tail).
/// When k4 is inserted, k2 (the new LRU) is evicted — not k1 or k3.
#[test]
fn test_access_pattern_promotes_node() {
    let mut cache = BlockCache::new(30);

    cache.insert(key("/p.sst", 0), block(1, 10)); // k1
    cache.insert(key("/p.sst", 100), block(2, 10)); // k2
    cache.insert(key("/p.sst", 200), block(3, 10)); // k3

    // get(k1) promotes k1 to head: order becomes k1→k3→k2(tail).
    assert!(cache.get(&key("/p.sst", 0)).is_some());

    // k4 insertion evicts k2 (the current tail).
    cache.insert(key("/p.sst", 300), block(4, 10)); // k4

    assert_eq!(cache.len(), 3);
    assert_eq!(cache.get(&key("/p.sst", 100)), None, "k2 must be evicted");
    assert!(
        cache.get(&key("/p.sst", 0)).is_some(),
        "k1 must survive (promoted)"
    );
    assert!(cache.get(&key("/p.sst", 200)).is_some(), "k3 must survive");
    assert!(cache.get(&key("/p.sst", 300)).is_some(), "k4 must survive");
}

/// Inserts a key with a 10-byte value and then re-inserts the same key with
/// a 5-byte value. Verifies that `size_bytes` reflects the shorter value and
/// that `get` returns the new bytes with no duplicate entry created.
#[test]
fn test_insert_overwrite_updates_value_and_size() {
    let mut cache = BlockCache::new(1024);

    cache.insert(key("/o.sst", 0), vec![0xAA; 10]);
    assert_eq!(cache.size_bytes(), 10);
    assert_eq!(cache.len(), 1);

    cache.insert(key("/o.sst", 0), vec![0xBB; 5]);
    assert_eq!(
        cache.size_bytes(),
        5,
        "size_bytes must reflect the new value length"
    );
    assert_eq!(
        cache.len(),
        1,
        "overwrite must not create a duplicate entry"
    );
    assert_eq!(
        cache.get(&key("/o.sst", 0)).cloned(),
        Some(vec![0xBB; 5]),
        "get must return the replacement value"
    );
}

/// Inserts blocks under two different SSTable paths, calls `invalidate` on
/// the first path, and verifies that only the second path's entries remain.
#[test]
fn test_invalidate_removes_only_matching_path() {
    let mut cache = BlockCache::new(4096);
    let path_a = PathBuf::from("/a.sst");
    let path_b = PathBuf::from("/b.sst");

    // Three 10-byte blocks under path_a, two 20-byte blocks under path_b.
    cache.insert((path_a.clone(), 0), block(0xA0, 10));
    cache.insert((path_a.clone(), 100), block(0xA1, 10));
    cache.insert((path_a.clone(), 200), block(0xA2, 10));
    cache.insert((path_b.clone(), 0), block(0xB0, 20));
    cache.insert((path_b.clone(), 100), block(0xB1, 20));

    cache.invalidate(&path_a);

    assert_eq!(cache.len(), 2, "only path_b entries must remain");
    assert_eq!(
        cache.size_bytes(),
        40,
        "size_bytes must reflect only path_b blocks"
    );

    assert_eq!(cache.get(&(path_a.clone(), 0)), None);
    assert_eq!(cache.get(&(path_a.clone(), 100)), None);
    assert_eq!(cache.get(&(path_a.clone(), 200)), None);
    assert!(cache.get(&(path_b.clone(), 0)).is_some());
    assert!(cache.get(&(path_b.clone(), 100)).is_some());
}

/// Populates a cache with 100 entries inside a nested scope and lets the
/// `BlockCache` drop at the end of that scope. The test passes if execution
/// completes without panicking, which would indicate a double-free or
/// use-after-free in the `Drop` implementation.
#[test]
fn test_drop_releases_all_nodes() {
    {
        let mut cache = BlockCache::new(1024 * 1024);
        for i in 0u64..100 {
            cache.insert(key("/d.sst", i * 4096), vec![i as u8; 100]);
        }
        assert_eq!(cache.len(), 100);
        // Cache drops here; all 100 nodes must be freed exactly once.
    }
    // Reaching this line without panic confirms Drop ran cleanly.
}

/// Verifies the SKILL.md acceptance criterion: cache hit ratio > 80 % on
/// repeated reads of the same keys.
///
/// Method: get each of 100 keys once before inserting (100 misses), then
/// insert all 100 keys, then get each key 9 more times (900 hits).
/// Total reads = 1000, hits = 900, ratio = 0.90 — deterministically above
/// the 80 % threshold without relying on random access patterns.
#[test]
fn test_hit_ratio_above_80_percent_acceptance() {
    // Capacity fits all 100 × 64-byte blocks exactly; no eviction during reads.
    let mut cache = BlockCache::new(100 * 64);

    // First pass: probe before insert → 100 misses.
    for i in 0u64..100 {
        assert_eq!(cache.get(&key("/r.sst", i * 4096)), None);
        cache.insert(key("/r.sst", i * 4096), vec![i as u8; 64]);
    }
    assert_eq!(cache.miss_count(), 100);

    // Nine more passes: every get is a hit → 9 × 100 = 900 hits.
    for _ in 0..9 {
        for i in 0u64..100 {
            assert!(cache.get(&key("/r.sst", i * 4096)).is_some());
        }
    }
    assert_eq!(cache.hit_count(), 900);

    let total = (cache.hit_count() + cache.miss_count()) as f64;
    let ratio = cache.hit_count() as f64 / total;
    assert!(
        ratio > 0.80,
        "expected hit ratio > 0.80; got {ratio:.2} ({}/{total})",
        cache.hit_count()
    );
}

/// Queries a key that was never inserted and verifies that `get` returns
/// `None` with the miss counter incremented by exactly one.
#[test]
fn test_get_missing_key_returns_none_and_increments_miss() {
    let mut cache = BlockCache::new(1024);
    assert_eq!(cache.get(&key("/missing.sst", 0)), None);
    assert_eq!(cache.miss_count(), 1);
    assert_eq!(cache.hit_count(), 0);
}
