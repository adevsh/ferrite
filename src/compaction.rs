//! # compaction
//!
//! Size-tiered compaction for the Ferrite LSM engine.
//!
//! ## Role in the LSM pipeline
//! Compaction sits above the SSTable layer and is called by the Engine after
//! each flush and from the CLI `compact` command. When any level accumulates
//! ≥ 4 SSTables, `Compactor::run` drains that level, k-way merges all its
//! files into one new SSTable at the next level, then re-checks for further
//! over-threshold levels and cascades until stable. This bounds L0 file count,
//! limits read amplification, and reclaims space from tombstone-deleted keys.
//!
//! ## Merge algorithm
//! A `BinaryHeap<HeapEntry>` drives a k-way streaming merge over `SSTableIter`
//! instances. Entries with the same key are deduplicated by keeping the version
//! from the source with the highest index in the input slice (highest index =
//! newest, because `levels[N]` is sorted by sequence number ascending). Tombstones
//! are garbage-collected only at the bottom level — i.e., only when the merge
//! target level and every level deeper are currently empty.
//!
//! ## Dependencies
//! - `cache`  — `BlockCache::invalidate` removes stale entries after source
//!   files are deleted from disk.
//! - `error`  — all public methods propagate `Result<T>`.
//! - `sstable`— `SSTableReader::iter` for streaming merge inputs;
//!   `SSTableWriter` for the merged output.
//! - `std::collections::BinaryHeap` — k-way merge heap; explicitly allowed by
//!   SKILL.md constraints ("Merge sort | `std::collections::BinaryHeap`").
//!
//! ## Used by
//! - `engine::Engine::flush`   — automatic post-flush compaction check.
//! - `engine::Engine::compact` — manual compaction trigger from the CLI.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};

use crate::cache::BlockCache;
use crate::error::Result;
use crate::sstable::{SSTableReader, SSTableWriter};

/// Number of SSTables at a single level that triggers compaction.
///
/// All levels use the same threshold so the cascade logic is uniform.
/// Level 0 is bounded to this many flush-produced files; Level 1+ accumulates
/// one merged file per L0 compaction until it too reaches this threshold.
const COMPACTION_THRESHOLD: usize = 4;

/// Width of the zero-padded sequence number in compacted output filenames.
///
/// Must match `engine::FLUSH_FILENAME_WIDTH` so all SSTable names sort
/// lexicographically by sequence when listed by `read_dir`.
const FILENAME_WIDTH: usize = 8;

// --- HeapEntry ---------------------------------------------------------------

/// One entry in the k-way merge heap: a decoded SSTable item tagged with its
/// source iterator index.
///
/// `source_idx` encodes relative freshness: the `sources` slice passed to
/// `merge_sources` is sorted by sequence number ascending, so a higher index
/// corresponds to a newer source SSTable. When multiple sources carry the same
/// key, the version with the highest `source_idx` wins.
struct HeapEntry {
    /// The entry's key bytes.
    key: Vec<u8>,
    /// `Some(bytes)` for a live value; `None` for a tombstone.
    val_opt: Option<Vec<u8>>,
    /// Index into the `iters` vector. Higher = newer source.
    source_idx: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    /// Min-heap on key; max on `source_idx` for equal keys.
    ///
    /// `BinaryHeap` is a max-heap. Reversing the key comparison makes the
    /// smallest key pop first. When keys are equal, the highest `source_idx`
    /// (newest source) pops first, so the first `pop` for any given key always
    /// yields the authoritative version without needing a separate lookup.
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then(self.source_idx.cmp(&other.source_idx))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// --- Compactor ---------------------------------------------------------------

/// Stateless namespace for size-tiered compaction operations.
///
/// All mutable state flows through the parameters of `run` and
/// `should_compact`; `Compactor` itself holds no data.
pub struct Compactor;

impl Compactor {
    /// Returns `true` if any level currently holds ≥ `COMPACTION_THRESHOLD` files.
    ///
    /// Exposed so the Engine and integration tests can inspect the trigger
    /// condition without having to call the full `run` path.
    pub fn should_compact(levels: &[Vec<SSTableReader>]) -> bool {
        levels.iter().any(|l| l.len() >= COMPACTION_THRESHOLD)
    }

    /// Runs size-tiered compaction until every level is below the threshold.
    ///
    /// On each iteration, finds the lowest over-threshold level and compacts it
    /// by merging all its SSTables into one new file at the next level
    /// (`compact_level`). After each merge the loop re-checks from Level 0
    /// upward so that a cascade (L0→L1→L2→…) is handled within a single call.
    ///
    /// Returns the total number of source SSTable files consumed across all
    /// compaction iterations in this call (e.g. 4 L0 files merging into one
    /// L1 file = 4). Returns `0` when every level is already below the
    /// threshold and no work is performed.
    ///
    /// # Errors
    /// Returns `FerriteError::Io` if a new SSTable cannot be written or a
    /// source file cannot be deleted from disk.
    /// Returns `FerriteError::Corruption` if a source data block fails CRC.
    pub fn run(
        levels: &mut Vec<Vec<SSTableReader>>,
        data_dir: &Path,
        cache: &mut BlockCache,
        next_seq: &mut u64,
    ) -> Result<usize> {
        let mut total_merged = 0;
        loop {
            // Always compact the lowest (L0-first) over-threshold level so that
            // the freshest data is promoted before older data is reorganised.
            let source_level =
                match levels.iter().position(|l| l.len() >= COMPACTION_THRESHOLD) {
                    Some(i) => i,
                    None => return Ok(total_merged),
                };
            total_merged += compact_level(levels, data_dir, cache, next_seq, source_level)?;
        }
    }
}

// --- private helpers ---------------------------------------------------------

/// Merges all SSTables at `source_level` into one new file at `source_level + 1`.
///
/// Steps in order:
/// 1. Determine tombstone GC eligibility (bottom-level check, before mutation).
/// 2. Drain `levels[source_level]` via `std::mem::take` — owns the readers.
/// 3. K-way merge all sources into a materialised `Vec`.
/// 4. If the merge produced no entries (all tombstones were GC'd): clean up
///    source files and return — no new SSTable is written.
/// 5. Write the merged output at Level `target_level`; open its reader; push
///    into `levels[target_level]`.
/// 6. Drop source readers (closes file handles), invalidate cache entries for
///    each source path, then remove the source files from disk.
///
/// Returns the number of source files consumed (always `sources.len()` from
/// step 2, regardless of whether a new SSTable was emitted in step 5).
///
/// # Errors
/// Same as `Compactor::run`.
fn compact_level(
    levels: &mut Vec<Vec<SSTableReader>>,
    data_dir: &Path,
    cache: &mut BlockCache,
    next_seq: &mut u64,
    source_level: usize,
) -> Result<usize> {
    let target_level = source_level + 1;

    // Tombstones may only be dropped when the target level and everything
    // below it is currently empty — otherwise an existing SSTable at a deeper
    // level might contain a live value that the tombstone is supposed to shadow.
    let is_target_bottom = levels
        .iter()
        .skip(target_level)
        .all(|l| l.is_empty());

    // Take ownership of the source readers; leaves levels[source_level] empty.
    let sources: Vec<SSTableReader> = std::mem::take(&mut levels[source_level]);
    let source_count = sources.len();

    // Materialise the merge into a Vec so we can detect the empty-output case
    // before calling SSTableWriter::new — the writer rejects empty iterators
    // with FerriteError::InvalidFormat.
    let merged = merge_sources(&sources, is_target_bottom)?;

    if merged.is_empty() {
        // Every entry was an eligible tombstone; no data to write.
        // Clean up sources without allocating a sequence number.
        let old_paths: Vec<PathBuf> = sources.iter().map(|r| r.path.clone()).collect();
        drop(sources);
        for path in &old_paths {
            cache.invalidate(path);
            std::fs::remove_file(path)?;
        }
        return Ok(source_count);
    }

    let new_seq = *next_seq;
    *next_seq += 1;
    let filename = format!("L{}_{:0>width$}.sst", target_level, new_seq, width = FILENAME_WIDTH);
    let new_path = data_dir.join(&filename);

    // Write merged output and fsync; only after this is durable do we delete
    // the sources (analogous to LSM invariant 5: files are immutable once
    // written, replaced only by an atomic write-then-delete).
    SSTableWriter::new(&new_path)?.write(merged.into_iter())?;
    let new_reader = SSTableReader::open(&new_path)?;

    // Ensure levels[target_level] exists before pushing.
    while levels.len() <= target_level {
        levels.push(Vec::new());
    }
    levels[target_level].push(new_reader);

    // Collect paths before dropping the readers so the paths are still
    // available after the file handles (inside the readers) are closed.
    let old_paths: Vec<PathBuf> = sources.iter().map(|r| r.path.clone()).collect();
    // Close file handles first so the OS can fully release the file resources.
    drop(sources);

    for path in &old_paths {
        // Invalidate any cached blocks that referenced the now-deleted file.
        cache.invalidate(path);
        std::fs::remove_file(path)?;
    }

    Ok(source_count)
}

/// K-way merges `sources` into a sorted, deduplicated `Vec` of entries.
///
/// Uses a `BinaryHeap<HeapEntry>` seeded with the first entry from each source
/// iterator. On each `pop`, the globally smallest key (highest `source_idx` for
/// ties) is yielded as the authoritative version; all subsequent heap entries
/// with the same key are drained and discarded, advancing their iterators so
/// they stay in sync.
///
/// When `drop_tombstones` is `true`, tombstone entries (`val_opt == None`) are
/// omitted — this is only safe when the caller has confirmed no older data
/// exists below the merge target (bottom-level GC, enforced by `compact_level`).
///
/// The result is materialised rather than streamed so that `compact_level` can
/// check for the empty-output case before creating a file.
///
/// # Errors
/// Returns `FerriteError::Corruption` if a source block fails CRC.
/// Returns `FerriteError::Io` on a disk read failure.
#[allow(clippy::type_complexity)]
fn merge_sources(
    sources: &[SSTableReader],
    drop_tombstones: bool,
) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    let mut iters: Vec<_> = sources.iter().map(|r| r.iter()).collect();
    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();

    // Seed the heap with the first entry from every source.
    for (idx, iter) in iters.iter_mut().enumerate() {
        if let Some(item) = iter.next() {
            let (key, val_opt) = item?;
            heap.push(HeapEntry { key, val_opt, source_idx: idx });
        }
    }

    let mut output: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();

    while let Some(top) = heap.pop() {
        let HeapEntry { key, val_opt, source_idx } = top;

        // Advance the iterator that produced this (authoritative) entry so the
        // heap always has at most one live entry per source.
        if let Some(item) = iters[source_idx].next() {
            let (k, v) = item?;
            heap.push(HeapEntry { key: k, val_opt: v, source_idx });
        }

        // Drain all remaining heap entries for the same key — they are stale
        // (older-source) duplicates. Their iterators must still be advanced so
        // subsequent entries from those sources continue to participate.
        loop {
            let is_dup = heap.peek().is_some_and(|e| e.key == key);
            if !is_dup {
                break;
            }
            let dup = heap.pop().unwrap();
            if let Some(item) = iters[dup.source_idx].next() {
                let (k, v) = item?;
                heap.push(HeapEntry { key: k, val_opt: v, source_idx: dup.source_idx });
            }
        }

        // Tombstones at the bottom level are safe to discard because no older
        // data can exist below to be accidentally resurrected.
        if drop_tombstones && val_opt.is_none() {
            continue;
        }

        output.push((key, val_opt));
    }

    Ok(output)
}
