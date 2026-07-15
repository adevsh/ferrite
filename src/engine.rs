//! # engine
//!
//! Top-level coordinator that wires WAL, Memtable, SSTable files, and the
//! block cache together into a single read/write interface.
//!
//! ## Role in the LSM pipeline
//! The Engine is the public-facing storage API. Every write goes WAL-first
//! (durability) then into the active Memtable (fast reads); when the active
//! Memtable reaches its threshold, the Engine rotates it into a frozen state
//! and continues writing into a fresh active Memtable. `flush` persists the
//! frozen Memtable first, then the active Memtable, and truncates the WAL only
//! after all in-memory state has reached SSTables. Reads probe the active
//! Memtable, then the frozen Memtable, then walk Level-0 SSTables
//! newest-to-oldest, using bloom-filter short-circuiting and the block cache
//! to minimise disk I/O. On `open`, the Engine replays the WAL into a fresh
//! active Memtable and scans the data directory to rebuild the in-memory SSTable
//! index so no durably written data is lost.
//!
//! ## Dependencies
//! - `cache` — `BlockCache` for hot data blocks.
//! - `error` — `Result<T>` alias.
//! - `memtable` — `Memtable`, `MemValue`, `DEFAULT_MEMTABLE_THRESHOLD`.
//! - `sstable` — `SSTableWriter` for flushes, `SSTableReader` for reads.
//! - `wal` — `Wal` for durable write logging; `Wal::recover` for startup replay.
//! - `std::collections::BTreeMap` — sorted accumulator for `scan_prefix` dedup.
//! - `std::fs` — directory creation and scanning at open time.
//!
//! ## Used by
//! - `main` (binary) — each CLI command maps to one Engine call.
//! - `tests/engine_test.rs` — integration tests for the full write/read/flush/recover path.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};

use crate::cache::BlockCache;
use crate::compaction::{Compactor, COMPACTION_THRESHOLD};
use crate::error::{FerriteError, Result};
use crate::memtable::{MemValue, Memtable, DEFAULT_MEMTABLE_THRESHOLD};
use crate::sstable::{SSTableReader, SSTableWriter};
use crate::wal::{Wal, WalRecord};

/// File extension for all SSTable files written by this engine.
const SSTABLE_EXT: &str = "sst";

/// Filename of the MANIFEST file that persists the next sequence counter.
///
/// Written atomically (via MANIFEST.tmp + rename) after every operation that
/// increments `next_seq`. On `open`, the MANIFEST value is combined with a
/// filename rescan via `max` so the engine recovers correctly even if MANIFEST
/// is absent (first-run), stale, or corrupted.
const MANIFEST_FILENAME: &str = "MANIFEST";

/// Default block cache byte capacity (8 MiB).
///
/// Sized to hold roughly 2 048 × 4 KiB data blocks warm — enough for a hot
/// working set in small-to-medium embedded workloads. Overridable via
/// `EngineConfig::cache_capacity`.
const DEFAULT_CACHE_CAPACITY: usize = 8 * 1024 * 1024;

/// Digit-width of the zero-padded sequence number in SSTable filenames.
///
/// A width of 8 produces names like `L0_00000001.sst`, supporting up to
/// 99_999_999 SSTables before overflow — sufficient for many years of
/// operation before compaction starts recycling sequence numbers.
const FLUSH_FILENAME_WIDTH: usize = 8;

// --- EngineConfig ------------------------------------------------------------

/// Runtime configuration for an `Engine` instance.
///
/// Construct via `EngineConfig::new` for production defaults. Override
/// individual fields (e.g. `memtable_threshold`) in tests to control flush
/// and cache behaviour precisely.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Path to the directory that holds `wal.log` and all SSTable files.
    /// Created by `Engine::open` if it does not already exist.
    pub data_dir: PathBuf,
    /// Byte threshold at which a Memtable flush is triggered after each write.
    /// Defaults to `DEFAULT_MEMTABLE_THRESHOLD` (4 MiB).
    pub memtable_threshold: usize,
    /// Byte capacity of the block cache shared across all open SSTables.
    /// Defaults to `DEFAULT_CACHE_CAPACITY` (8 MiB).
    pub cache_capacity: usize,
}

impl EngineConfig {
    /// Returns a config with `data_dir` set and both thresholds at their
    /// production defaults: 4 MiB Memtable, 8 MiB cache.
    pub fn new(data_dir: impl Into<PathBuf>) -> EngineConfig {
        EngineConfig {
            data_dir: data_dir.into(),
            memtable_threshold: DEFAULT_MEMTABLE_THRESHOLD,
            cache_capacity: DEFAULT_CACHE_CAPACITY,
        }
    }
}

// --- EngineStats -------------------------------------------------------------

/// Snapshot of engine internals collected by `Engine::stats`.
///
/// All counters are point-in-time readings; calling `stats()` twice in
/// succession may return different values if concurrent writes are in flight.
/// The engine is single-threaded, so readings are stable within
/// a single call.
pub struct EngineStats {
    /// Current in-memory Memtable footprint in bytes across both the active
    /// Memtable and any frozen Memtable (monotonic; tombstones are counted).
    pub memtable_size_bytes: usize,
    /// Current frozen Memtable footprint in bytes.
    pub frozen_memtable_size_bytes: usize,
    /// Number of SSTable files at each level.
    /// `sstable_count_per_level[i]` is the count of files in level `i`.
    /// Empty if no SSTables have been flushed yet.
    pub sstable_count_per_level: Vec<usize>,
    /// Total `BlockCache::get` calls that returned `Some` since this `Engine`
    /// was opened.
    pub cache_hit_count: u64,
    /// Total `BlockCache::get` calls that returned `None` since this `Engine`
    /// was opened.
    pub cache_miss_count: u64,
    /// Whether a frozen Memtable is waiting to be flushed.
    pub has_pending_flush: bool,
    /// Whether a background flush worker is currently responsible for the
    /// frozen Memtable's SSTable write.
    pub flush_in_flight: bool,
}

/// Background SSTable write launched after the active Memtable rotates.
struct BackgroundFlush {
    /// Path where the frozen Memtable image is being written.
    output_path: PathBuf,
    /// Worker thread that persists the frozen image to disk.
    handle: JoinHandle<Result<()>>,
}

// --- Engine ------------------------------------------------------------------

/// Ferrite LSM storage engine.
///
/// Coordinates the WAL → Memtable → SSTable write path and the
/// Memtable → Level 0 → Level 1 → … read path under a single mutable owner.
///
/// # Mutability note
/// `get` and `scan_prefix` take `&mut self` rather than `&self` because
/// `BlockCache::get` must update the LRU order and hit/miss counters.
/// This is a deliberate deviation from the SKILL.md `&self` spec, chosen
/// to avoid `RefCell` indirection.
pub struct Engine {
    /// Configuration snapshot; referenced for the data path and thresholds.
    config: EngineConfig,
    /// Append-only durability log; fsynced before every Memtable mutation.
    wal: Wal,
    /// In-memory sorted write buffer that accepts all new writes.
    active_memtable: Memtable,
    /// Previous full Memtable held stable for a later flush while writes
    /// continue in `active_memtable`.
    frozen_memtable: Option<Memtable>,
    /// Background worker currently persisting `frozen_memtable` to a new L0
    /// SSTable. Completion is polled at the start of engine operations.
    background_flush: Option<BackgroundFlush>,
    /// SSTable readers grouped by level. `levels[i]` is sorted by sequence
    /// number ascending; `.iter().rev()` yields newest-first within a level.
    levels: Vec<Vec<SSTableReader>>,
    /// Shared block cache used by all `SSTableReader::get_with_cache` calls.
    cache: BlockCache,
    /// Sequence number assigned to the next `flush` call's output file.
    next_seq: u64,
}

impl Engine {
    /// Returns a read-only view of the SSTable level structure.
    ///
    /// `levels()[i]` is the slice of SSTables at level `i`, sorted by sequence
    /// number ascending (`.iter().rev()` = newest first). Exposed so that
    /// integration tests and the `compact` CLI path can inspect compaction state
    /// without going through individual `get` calls.
    pub fn levels(&self) -> &[Vec<SSTableReader>] {
        &self.levels
    }

    /// Returns a read-only reference to the shared block cache.
    ///
    /// Exposed for integration tests that need to verify cache state (e.g.
    /// checking that entries are invalidated after compaction deletes source
    /// SSTable files).
    pub fn cache(&self) -> &BlockCache {
        &self.cache
    }

    /// Returns a point-in-time snapshot of engine internal counters and
    /// structure for display by the `stats` CLI subcommand.
    pub fn stats(&self) -> EngineStats {
        let frozen_memtable_size_bytes = self
            .frozen_memtable
            .as_ref()
            .map_or(0, Memtable::size_bytes);
        EngineStats {
            memtable_size_bytes: self.active_memtable.size_bytes() + frozen_memtable_size_bytes,
            frozen_memtable_size_bytes,
            sstable_count_per_level: self.levels.iter().map(|l| l.len()).collect(),
            cache_hit_count: self.cache.hit_count(),
            cache_miss_count: self.cache.miss_count(),
            has_pending_flush: self.frozen_memtable.is_some(),
            flush_in_flight: self.background_flush.is_some(),
        }
    }

    /// Returns whether a frozen Memtable is currently waiting to be flushed.
    pub fn has_frozen_memtable(&self) -> bool {
        self.frozen_memtable.is_some()
    }

    /// Opens or creates an Engine rooted at `config.data_dir`.
    ///
    /// Steps on startup:
    /// 1. Creates the data directory (no-op if it exists).
    /// 2. Recovers WAL records into a fresh Memtable via `Wal::recover` +
    ///    `Memtable::restore_from_wal`.
    /// 3. Opens the WAL in append mode on top of the recovered file.
    ///    (`Wal::open` does not truncate, so previously recovered records stay
    ///    intact until the next successful flush.)
    /// 4. Scans the data directory for `L{level}_{seq:08}.sst` files, opens
    ///    each as an `SSTableReader`, and groups them into `self.levels`.
    /// 5. Reads MANIFEST if present; sets `next_seq` to
    ///    `max(manifest_seq, filename_rescan_seq)` so the engine recovers
    ///    correctly even when MANIFEST is missing or stale.
    /// 6. Initialises the block cache.
    ///
    /// # Errors
    /// Returns `FerriteError::Io` if the directory cannot be created or any
    /// file cannot be opened. Returns `FerriteError::InvalidFormat` or
    /// `FerriteError::Corruption` if a recovered SSTable is malformed.
    pub fn open(config: EngineConfig) -> Result<Engine> {
        std::fs::create_dir_all(&config.data_dir)?;

        // Recover WAL before opening it for append so the replay sees the
        // complete on-disk state rather than starting from the append cursor.
        let wal_records = Wal::recover(&config.data_dir)?;
        let active_memtable = Memtable::restore_from_wal(wal_records);
        let wal = Wal::open(&config.data_dir)?;

        // Scan the directory for SSTable files. Unrecognised names (wal.log,
        // future MANIFEST, temp files) are silently ignored via
        // parse_sstable_filename returning None.
        let mut sstable_infos: Vec<(u32, u64, SSTableReader)> = Vec::new();
        for entry in std::fs::read_dir(&config.data_dir)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let name_str = file_name.to_string_lossy();
            if let Some((level, seq)) = parse_sstable_filename(&name_str) {
                match SSTableReader::open(&entry.path()) {
                    Ok(reader) => sstable_infos.push((level, seq, reader)),
                    // Background flush failures or crashes can leave behind a
                    // reserved `.sst` filename with incomplete bytes. Skip any
                    // unreadable candidate so startup rebuilds only published
                    // SSTables and does not advance `next_seq` past junk.
                    Err(FerriteError::InvalidFormat(_)) | Err(FerriteError::Corruption(_)) => {}
                    Err(err) => return Err(err),
                }
            }
        }

        // Combine the MANIFEST value (if present) with a filename rescan via
        // max so that MANIFEST being absent, stale, or partially written never
        // causes a sequence number regression and potential SSTable name collision.
        let manifest_seq = read_manifest(&config.data_dir).unwrap_or(0);
        let rescan_seq: u64 = sstable_infos
            .iter()
            .map(|(_, seq, _)| *seq)
            .max()
            .map(|m| m + 1)
            .unwrap_or(1);
        let next_seq = std::cmp::max(manifest_seq, rescan_seq);

        // Sort by (level, seq) ascending so that within each level the Vec is
        // ordered oldest→newest; .iter().rev() then gives newest-first reads.
        sstable_infos.sort_by_key(|(level, seq, _)| (*level, *seq));

        let mut levels: Vec<Vec<SSTableReader>> = Vec::new();
        for (level, _seq, reader) in sstable_infos {
            let level_idx = level as usize;
            while levels.len() <= level_idx {
                levels.push(Vec::new());
            }
            levels[level_idx].push(reader);
        }

        let cache = BlockCache::new(config.cache_capacity);

        Ok(Engine {
            config,
            wal,
            active_memtable,
            frozen_memtable: None,
            background_flush: None,
            levels,
            cache,
            next_seq,
        })
    }

    /// Appends `key`=`value` to the WAL, inserts into the active Memtable, and
    /// rotates the full active Memtable into a frozen state once the threshold
    /// is reached.
    ///
    /// WAL is fsynced before the Memtable is mutated so the write survives
    /// a crash between the two operations.
    ///
    /// If a frozen Memtable is already present when the active Memtable fills,
    /// the write path first persists the older frozen Memtable so the single
    /// frozen slot can be reused. Task 2 replaces that synchronous fallback
    /// with real background flush coordination.
    ///
    /// # Errors
    /// Returns `FerriteError::Io` if the WAL write or any required flush fails.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.poll_background_flush_completion()?;
        self.wal.append_put(key, value)?;
        self.active_memtable.put(key.to_vec(), value.to_vec());
        self.rotate_or_flush_if_needed()?;
        Ok(())
    }

    /// Appends a tombstone for `key` to the WAL, inserts into the active
    /// Memtable, and rotates once the Memtable threshold is reached.
    ///
    /// A Memtable tombstone shadows any older value for `key` in lower-level
    /// SSTables (LSM invariant 3). The tombstone persists in Level-0 SSTables
    /// until compaction reaches the bottom level.
    ///
    /// # Errors
    /// Returns `FerriteError::Io` if the WAL write or any required flush fails.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.poll_background_flush_completion()?;
        self.wal.append_delete(key)?;
        self.active_memtable.delete(key.to_vec());
        self.rotate_or_flush_if_needed()?;
        Ok(())
    }

    /// Returns the most recent value for `key`, or `None` if the key is
    /// absent or has been deleted.
    ///
    /// Search order: active Memtable → frozen Memtable → Level-0 SSTables
    /// newest-to-oldest → Level-1 → …
    /// A tombstone anywhere in the search path returns `Ok(None)` immediately
    /// without probing lower layers (LSM invariant 3).
    ///
    /// `self.levels` and `self.cache` are disjoint fields; Rust's field-
    /// splitting borrow rules allow the shared borrow on `levels` (via the
    /// loop variables) and the mutable borrow on `cache` to coexist.
    ///
    /// # Errors
    /// Returns `FerriteError::Corruption` or `FerriteError::Io` if a data
    /// block read or CRC check fails.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.poll_background_flush_completion()?;
        if let Some(result) = Self::lookup_memtable(&self.active_memtable, key) {
            return Ok(result);
        }
        if let Some(frozen_memtable) = &self.frozen_memtable {
            if let Some(result) = Self::lookup_memtable(frozen_memtable, key) {
                return Ok(result);
            }
        }

        for level in &self.levels {
            for sstable in level.iter().rev() {
                match sstable.get_with_cache(key, &mut self.cache)? {
                    Some(Some(v)) => return Ok(Some(v)),
                    Some(None) => return Ok(None),
                    None => {}
                }
            }
        }

        Ok(None)
    }

    /// Returns all live key-value pairs whose key starts with `prefix`, in
    /// ascending key order, deduplicated across all layers with newest-wins
    /// semantics.
    ///
    /// Tombstones from any layer suppress older values for the same key.
    /// The active Memtable always wins over the frozen Memtable and SSTable
    /// values; the frozen Memtable wins over SSTables; within SSTables,
    /// Level-0 newest wins over older Level-0, which wins over Level-1, and
    /// so on.
    ///
    /// Uses `SSTableReader::iter()` for the cross-SSTable scan — O(N)
    /// per table — because implementing a cache-aware prefix scan would add
    /// complexity not yet justified by profiling.
    ///
    /// # Errors
    /// Returns `FerriteError::Corruption` or `FerriteError::Io` if a data
    /// block read fails.
    pub fn scan_prefix(&mut self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.poll_background_flush_completion()?;
        let mut acc: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

        // Active Memtable is unconditionally newest; insert its matching
        // entries first so later layers cannot overwrite them.
        for (k, v) in self.active_memtable.scan_prefix(prefix) {
            let val_opt = match v {
                MemValue::Value(b) => Some(b.clone()),
                MemValue::Tombstone => None,
            };
            acc.insert(k.clone(), val_opt);
        }

        // Frozen Memtable is older than the active Memtable but newer than any
        // SSTable. Preserve active entries already present in `acc`.
        if let Some(frozen_memtable) = &self.frozen_memtable {
            for (k, v) in frozen_memtable.scan_prefix(prefix) {
                let val_opt = match v {
                    MemValue::Value(b) => Some(b.clone()),
                    MemValue::Tombstone => None,
                };
                acc.entry(k.clone()).or_insert(val_opt);
            }
        }

        // SSTables visited newest-to-oldest; or_insert preserves the first
        // (newest) value seen for each key across all SSTable layers.
        for level in &self.levels {
            for sstable in level.iter().rev() {
                for item in sstable.iter() {
                    let (key, val_opt) = item?;
                    if key.starts_with(prefix) {
                        acc.entry(key).or_insert(val_opt);
                    } else if key.as_slice() > prefix {
                        // SSTable entries are sorted; nothing past this point
                        // can start with prefix.
                        break;
                    }
                }
            }
        }

        // Filter tombstones; BTreeMap iteration order is already sorted by key.
        Ok(acc
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect())
    }

    /// Runs a full cascade compaction via `Compactor::run`, then persists the
    /// updated sequence counter to MANIFEST.
    ///
    /// Finds the lowest over-threshold level (≥ 4 SSTables), merges all its
    /// files into one new SSTable at the next level, and repeats until no level
    /// is over the threshold. If all levels are below the threshold on entry,
    /// no SSTable files are touched (but MANIFEST is still written, which is
    /// harmless).
    ///
    /// Returns the total number of source SSTable files consumed across the
    /// full cascade (0 when already below threshold). The caller can surface
    /// this as `"Compaction complete. N files merged."`.
    ///
    /// Also callable from the CLI `compact` subcommand for deterministic
    /// compaction outside the automatic post-flush path.
    ///
    /// # Errors
    /// Returns `FerriteError::Io` or `FerriteError::Corruption` if any
    /// SSTable write, block read, or disk operation fails.
    pub fn compact(&mut self) -> Result<usize> {
        self.finish_background_flush()?;
        let merged = Compactor::run(
            &mut self.levels,
            &self.config.data_dir,
            &mut self.cache,
            &mut self.next_seq,
        )?;
        write_manifest(&self.config.data_dir, self.next_seq)?;
        Ok(merged)
    }

    /// Drains the frozen Memtable first, then the active Memtable, into new
    /// Level-0 SSTables and truncates the WAL.
    ///
    /// No-op when both Memtables are empty. Otherwise:
    /// 1. Serialises all frozen Memtable entries, if present, into
    ///    `L0_{seq:08}.sst`.
    /// 2. Serialises all active Memtable entries, if present, into the next
    ///    `L0_{seq:08}.sst`.
    /// 3. Opens each written file as an `SSTableReader` and appends it to
    ///    `self.levels[0]`.
    /// 4. Truncates the WAL — only **after** every in-memory Memtable has
    ///    reached SSTables. This keeps WAL cleanup conservative until Task 2
    ///    introduces per-flush WAL coordination.
    /// 5. Resets both Memtables to empty.
    /// 6. Runs `Compactor::run` to cascade-compact any over-threshold level.
    /// 7. Writes MANIFEST to persist the updated `next_seq`.
    ///
    /// The entries are collected into a `Vec` before writing so that borrows
    /// on the Memtables end before the Engine mutates `self.levels`.
    ///
    /// # Errors
    /// Returns `FerriteError::Io` if the SSTable write, fsync, or WAL
    /// truncation fails.
    pub fn flush(&mut self) -> Result<()> {
        self.finish_background_flush()?;

        if self.frozen_memtable.is_none() && self.active_memtable.size_bytes() == 0 {
            return Ok(());
        }

        if self.frozen_memtable.is_some() {
            self.flush_frozen_memtable_sync()?;
        }

        if self.active_memtable.size_bytes() > 0 {
            self.rotate_active_memtable_to_frozen();
            self.flush_frozen_memtable_sync()?;
        }

        Ok(())
    }

    /// Returns the current value for `key` from `memtable`, if present.
    fn lookup_memtable(memtable: &Memtable, key: &[u8]) -> Option<Option<Vec<u8>>> {
        match memtable.get(key) {
            Some(MemValue::Value(v)) => Some(Some(v.clone())),
            Some(MemValue::Tombstone) => Some(None),
            None => None,
        }
    }

    /// Rotates the active Memtable into the frozen slot on the first threshold
    /// crossing. If a frozen Memtable is already waiting, persist that older
    /// Memtable first so the slot can be reused.
    fn rotate_or_flush_if_needed(&mut self) -> Result<()> {
        if !self.active_memtable.is_full(self.config.memtable_threshold) {
            return Ok(());
        }

        if self.frozen_memtable.is_some() {
            self.finish_background_flush()?;
        }
        if self.frozen_memtable.is_some() {
            self.flush_frozen_memtable_sync()?;
        }
        self.rotate_active_memtable_to_frozen();
        self.start_background_flush();
        self.poll_background_flush_completion()?;
        if self.background_flush.is_some()
            && self.levels.first().map_or(0, Vec::len) + 1 >= COMPACTION_THRESHOLD
        {
            self.finish_background_flush()?;
        }

        Ok(())
    }

    /// Moves the active Memtable into the frozen slot and replaces it with a
    /// fresh active Memtable.
    fn rotate_active_memtable_to_frozen(&mut self) {
        if self.active_memtable.size_bytes() == 0 || self.frozen_memtable.is_some() {
            return;
        }
        self.frozen_memtable = Some(std::mem::take(&mut self.active_memtable));
    }

    /// Collects all Memtable entries into the `(key, value-or-tombstone)`
    /// form expected by `SSTableWriter`.
    fn collect_memtable_entries(memtable: &Memtable) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        memtable
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    match v {
                        MemValue::Value(b) => Some(b.clone()),
                        MemValue::Tombstone => None,
                    },
                )
            })
            .collect()
    }

    /// Persists the current frozen Memtable into a new Level-0 SSTable on the
    /// caller thread, then runs the same completion path used by the
    /// background worker.
    fn flush_frozen_memtable_sync(&mut self) -> Result<()> {
        let Some(frozen_memtable) = self.frozen_memtable.as_ref() else {
            return Ok(());
        };

        let entries = Self::collect_memtable_entries(frozen_memtable);
        if entries.is_empty() {
            self.rewrite_wal_from_live_memtables(false)?;
            self.frozen_memtable = None;
            write_manifest(&self.config.data_dir, self.next_seq)?;
            return Ok(());
        }

        let path = self.reserve_next_l0_path();
        Self::write_sstable_file(&path, entries)?;
        self.complete_frozen_flush(path)
    }

    /// Launches a background worker that writes the current frozen Memtable to
    /// its reserved Level-0 SSTable path.
    fn start_background_flush(&mut self) {
        if self.background_flush.is_some() {
            return;
        }

        let Some(frozen_memtable) = self.frozen_memtable.as_ref() else {
            return;
        };
        let entries = Self::collect_memtable_entries(frozen_memtable);
        if entries.is_empty() {
            return;
        }

        let output_path = self.reserve_next_l0_path();
        let worker_path = output_path.clone();
        let handle = thread::spawn(move || Self::write_sstable_file(&worker_path, entries));
        self.background_flush = Some(BackgroundFlush {
            output_path,
            handle,
        });
    }

    /// Completes a finished background flush if its worker has already exited.
    fn poll_background_flush_completion(&mut self) -> Result<()> {
        let Some(background_flush) = self.background_flush.as_ref() else {
            return Ok(());
        };
        for _ in 0..64 {
            if background_flush.handle.is_finished() {
                return self.finish_background_flush();
            }
            thread::yield_now();
        }
        Ok(())
    }

    /// Waits for any in-flight background flush to finish and integrates its
    /// completed SSTable into the engine state.
    fn finish_background_flush(&mut self) -> Result<()> {
        let Some(background_flush) = self.background_flush.take() else {
            return Ok(());
        };

        match background_flush.handle.join() {
            Ok(Ok(())) => self.complete_frozen_flush(background_flush.output_path),
            Ok(Err(err)) => Err(err),
            Err(panic_payload) => Err(FerriteError::Io(std::io::Error::other(format!(
                "background flush thread panicked: {}",
                join_panic_message(&panic_payload)
            )))),
        }
    }

    /// Finalizes a flushed frozen Memtable by cleaning up the WAL, publishing
    /// the new SSTable into L0, and running post-flush compaction.
    fn complete_frozen_flush(&mut self, output_path: PathBuf) -> Result<()> {
        let reader = SSTableReader::open(&output_path)?;
        self.rewrite_wal_from_live_memtables(false)?;

        if self.levels.is_empty() {
            self.levels.push(Vec::new());
        }
        self.levels[0].push(reader);
        self.frozen_memtable = None;

        let _ = Compactor::run(
            &mut self.levels,
            &self.config.data_dir,
            &mut self.cache,
            &mut self.next_seq,
        )?;
        write_manifest(&self.config.data_dir, self.next_seq)?;
        Ok(())
    }

    /// Rewrites the WAL to contain only Memtable state that is still resident
    /// in memory after a flush completes.
    fn rewrite_wal_from_live_memtables(&mut self, include_frozen: bool) -> Result<()> {
        let mut records = Vec::new();

        if include_frozen {
            if let Some(frozen_memtable) = &self.frozen_memtable {
                records.extend(Self::collect_wal_records(frozen_memtable));
            }
        }
        records.extend(Self::collect_wal_records(&self.active_memtable));

        self.wal.rewrite(records)
    }

    /// Collects the current Memtable image as WAL records in key order.
    fn collect_wal_records(memtable: &Memtable) -> Vec<WalRecord> {
        memtable
            .iter()
            .map(|(key, value)| match value {
                MemValue::Value(bytes) => WalRecord::Put {
                    key: key.clone(),
                    value: bytes.clone(),
                },
                MemValue::Tombstone => WalRecord::Delete { key: key.clone() },
            })
            .collect()
    }

    /// Reserves the next Level-0 SSTable output path and advances `next_seq`.
    fn reserve_next_l0_path(&mut self) -> PathBuf {
        let seq = self.next_seq;
        self.next_seq += 1;

        let filename = format!(
            "L0_{seq:0>width$}.{SSTABLE_EXT}",
            width = FLUSH_FILENAME_WIDTH
        );
        self.config.data_dir.join(filename)
    }

    /// Writes one Memtable image to `path`, removing any partial output if the
    /// SSTable write fails mid-stream.
    fn write_sstable_file(path: &Path, entries: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> Result<()> {
        match SSTableWriter::new(path)?.write(entries.into_iter()) {
            Ok(_) => Ok(()),
            Err(err) => {
                let _ = std::fs::remove_file(path);
                Err(err)
            }
        }
    }
}

// --- private helpers ---------------------------------------------------------

/// Formats a thread panic payload into a short string for error reporting.
fn join_panic_message(payload: &(dyn std::any::Any + Send + 'static)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// Atomically writes `next_seq` to the MANIFEST file.
///
/// Writes to `MANIFEST.tmp` then renames over `MANIFEST`. `rename` is atomic
/// on the same filesystem on macOS/Linux, so a crash between write and rename
/// leaves at most a stale `.tmp` file — `read_manifest` ignores it because
/// it only reads `MANIFEST`.
fn write_manifest(data_dir: &Path, next_seq: u64) -> Result<()> {
    let manifest = data_dir.join(MANIFEST_FILENAME);
    let tmp = data_dir.join("MANIFEST.tmp");
    std::fs::write(&tmp, format!("{next_seq}\n"))?;
    std::fs::rename(&tmp, &manifest)?;
    Ok(())
}

/// Reads the current `next_seq` from the MANIFEST file, returning `None` on
/// any failure (file absent, unreadable, or unparseable).
///
/// Returns `Option` rather than `Result` so callers can silently fall back to
/// the filename-rescan value without threading a non-fatal I/O error upward.
fn read_manifest(data_dir: &Path) -> Option<u64> {
    let content = std::fs::read_to_string(data_dir.join(MANIFEST_FILENAME)).ok()?;
    content.trim().parse::<u64>().ok()
}

/// Parses the standard Ferrite SSTable filename `L{level}_{seq}.sst`.
///
/// Returns `Some((level, seq))` on a valid match and `None` for any other
/// filename. Silently skips `wal.log`, future `MANIFEST` files, temp files,
/// and anything else that does not follow the convention.
///
/// Parsing rules:
/// - Must end with `.sst`.
/// - Stem must contain exactly one `_` separator.
/// - Left side must start with `L` followed by a valid `u32` level.
/// - Right side must be a valid `u64` sequence number (zero-padding is a
///   writer convention; any digit string is accepted on read).
fn parse_sstable_filename(name: &str) -> Option<(u32, u64)> {
    let stem = name.strip_suffix(".sst")?;
    let (level_part, seq_part) = stem.split_once('_')?;
    let level_str = level_part.strip_prefix('L')?;
    let level: u32 = level_str.parse().ok()?;
    let seq: u64 = seq_part.parse().ok()?;
    Some((level, seq))
}
