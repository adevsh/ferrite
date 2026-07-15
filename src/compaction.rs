//! # compaction
//!
//! Leveled compaction for the Ferrite LSM engine.
//!
//! ## Role in the LSM pipeline
//! Compaction sits above the SSTable layer and is called by the Engine after
//! each flush and from the CLI `compact` command. Level 0 remains an
//! overlapping landing zone for flushes; once a level reaches the compaction
//! threshold, `Compactor::run` rewrites an overlapping slice into the next
//! level, then re-checks for further over-threshold levels until stable. This
//! bounds L0 file count, keeps L1+ non-overlapping by key range, and reclaims
//! space from tombstone-deleted keys when no older overlapping data remains.
//!
//! ## Merge algorithm
//! A `BinaryHeap<HeapEntry>` drives a k-way streaming merge over `SSTableIter`
//! instances. Entries with the same key are deduplicated by keeping the version
//! source with the highest index in the input slice (highest index = newest).
//! For leveled rewrites, overlapping target-level files are fed into the merge
//! before source-level files so newer data from the source level wins. Tombstones
//! are garbage-collected only when no deeper level overlaps the rewrite range.
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
/// All levels use the same threshold so the trigger logic stays uniform.
/// Level 0 is bounded to this many flush-produced files; Level 1+ are rewritten
/// in overlapping slices once they accumulate this many files.
pub(crate) const COMPACTION_THRESHOLD: usize = 4;

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

#[derive(Debug, Clone)]
struct KeyRange {
    smallest: Vec<u8>,
    largest: Vec<u8>,
}

impl KeyRange {
    fn from_reader(reader: &SSTableReader) -> KeyRange {
        KeyRange {
            smallest: reader.smallest_key().to_vec(),
            largest: reader.largest_key().to_vec(),
        }
    }

    fn include_reader(&mut self, reader: &SSTableReader) {
        if reader.smallest_key() < self.smallest.as_slice() {
            self.smallest = reader.smallest_key().to_vec();
        }
        if reader.largest_key() > self.largest.as_slice() {
            self.largest = reader.largest_key().to_vec();
        }
    }

    fn overlaps_reader(&self, reader: &SSTableReader) -> bool {
        ranges_overlap(
            self.smallest.as_slice(),
            self.largest.as_slice(),
            reader.smallest_key(),
            reader.largest_key(),
        )
    }
}

#[derive(Debug)]
struct CompactionPlan {
    source_level: usize,
    target_level: usize,
    source_indices: Vec<usize>,
    target_indices: Vec<usize>,
    key_range: KeyRange,
}

/// Stateless namespace for leveled compaction operations.
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

    /// Runs leveled compaction until every level is below the threshold.
    ///
    /// On each iteration, finds the lowest over-threshold level and compacts an
    /// overlapping slice into the next level (`compact_level`). L0 starts from
    /// its oldest file and expands to include overlapping L0 and L1 files;
    /// L1+ rewrite one source file plus all overlapping target files. After
    /// each rewrite the loop re-checks from Level 0 upward so a cascade
    /// (L0→L1→L2→…) is handled within a single call.
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
            let plan = match pick_compaction(levels) {
                Some(plan) => plan,
                None => return Ok(total_merged),
            };
            total_merged += compact_level(levels, data_dir, cache, next_seq, plan)?;
        }
    }
}

// --- private helpers ---------------------------------------------------------

/// Rewrites one overlapping compaction slice into `target_level`.
///
/// Steps in order:
/// 1. Determine tombstone GC eligibility from the rewrite range.
/// 2. Remove the selected readers from the source and target levels.
/// 3. K-way merge the selected readers into a materialised `Vec`.
/// 4. If the merge produced no entries (all tombstones were GC'd): clean up
///    source files and return — no new SSTable is written.
/// 5. Write the merged output at Level `target_level`; open its reader; insert
///    it back into that level.
/// 6. Drop rewritten readers (closes file handles), invalidate cache entries
///    for every deleted source path, then remove those files from disk.
///
/// Returns the number of input files consumed (selected source + selected
/// target files), regardless of whether a new SSTable was emitted in step 5.
///
/// # Errors
/// Same as `Compactor::run`.
fn compact_level(
    levels: &mut Vec<Vec<SSTableReader>>,
    data_dir: &Path,
    cache: &mut BlockCache,
    next_seq: &mut u64,
    plan: CompactionPlan,
) -> Result<usize> {
    let CompactionPlan {
        source_level,
        target_level,
        source_indices,
        target_indices,
        key_range,
    } = plan;

    let drop_tombstones = levels.iter().skip(target_level + 1).all(|level| {
        level
            .iter()
            .all(|reader| !key_range.overlaps_reader(reader))
    });

    while levels.len() <= target_level {
        levels.push(Vec::new());
    }

    let source_level_readers = std::mem::take(&mut levels[source_level]);
    let (selected_sources, remaining_sources) =
        take_selected_readers(source_level_readers, &source_indices);

    let target_level_readers = std::mem::take(&mut levels[target_level]);
    let (selected_targets, mut remaining_targets) =
        take_selected_readers(target_level_readers, &target_indices);

    let source_count = selected_sources.len() + selected_targets.len();

    let mut merge_inputs: Vec<&SSTableReader> =
        Vec::with_capacity(selected_targets.len() + selected_sources.len());
    merge_inputs.extend(selected_targets.iter());
    merge_inputs.extend(selected_sources.iter());

    // Materialise the merge into a Vec so we can detect the empty-output case
    // before calling SSTableWriter::new — the writer rejects empty iterators
    // with FerriteError::InvalidFormat.
    let merged = merge_sources(&merge_inputs, drop_tombstones)?;

    levels[source_level] = remaining_sources;

    if merged.is_empty() {
        // Every entry was an eligible tombstone; no data to write.
        // Clean up sources without allocating a sequence number.
        let old_paths = collect_paths(&selected_sources, &selected_targets);
        levels[target_level] = remaining_targets;
        drop(selected_sources);
        drop(selected_targets);
        for path in &old_paths {
            cache.invalidate(path);
            std::fs::remove_file(path)?;
        }
        return Ok(source_count);
    }

    let new_seq = *next_seq;
    *next_seq += 1;
    let filename = format!(
        "L{}_{:0>width$}.sst",
        target_level,
        new_seq,
        width = FILENAME_WIDTH
    );
    let new_path = data_dir.join(&filename);

    // Write merged output and fsync; only after this is durable do we delete
    // the sources (analogous to LSM invariant 5: files are immutable once
    // written, replaced only by an atomic write-then-delete).
    SSTableWriter::new(&new_path)?.write(merged.into_iter())?;
    let new_reader = SSTableReader::open(&new_path)?;

    remaining_targets.push(new_reader);
    if target_level > 0 {
        sort_level_by_range(&mut remaining_targets);
    }
    levels[target_level] = remaining_targets;

    // Collect paths before dropping the readers so the paths are still
    // available after the file handles (inside the readers) are closed.
    let old_paths = collect_paths(&selected_sources, &selected_targets);
    // Close file handles first so the OS can fully release the file resources.
    drop(selected_sources);
    drop(selected_targets);

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
    sources: &[&SSTableReader],
    drop_tombstones: bool,
) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    let mut iters: Vec<_> = sources.iter().map(|r| r.iter()).collect();
    let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();

    // Seed the heap with the first entry from every source.
    for (idx, iter) in iters.iter_mut().enumerate() {
        if let Some(item) = iter.next() {
            let (key, val_opt) = item?;
            heap.push(HeapEntry {
                key,
                val_opt,
                source_idx: idx,
            });
        }
    }

    let mut output: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();

    while let Some(top) = heap.pop() {
        let HeapEntry {
            key,
            val_opt,
            source_idx,
        } = top;

        // Advance the iterator that produced this (authoritative) entry so the
        // heap always has at most one live entry per source.
        if let Some(item) = iters[source_idx].next() {
            let (k, v) = item?;
            heap.push(HeapEntry {
                key: k,
                val_opt: v,
                source_idx,
            });
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
                heap.push(HeapEntry {
                    key: k,
                    val_opt: v,
                    source_idx: dup.source_idx,
                });
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

fn pick_compaction(levels: &[Vec<SSTableReader>]) -> Option<CompactionPlan> {
    let source_level = levels
        .iter()
        .position(|l| l.len() >= COMPACTION_THRESHOLD)?;
    let target_level = source_level + 1;

    if source_level == 0 {
        let level0 = levels.first()?;
        let mut source_indices = vec![0usize];
        let mut source_selected = vec![false; level0.len()];
        source_selected[0] = true;

        let target_len = levels.get(target_level).map_or(0, Vec::len);
        let mut target_indices = Vec::new();
        let mut target_selected = vec![false; target_len];
        let mut key_range = KeyRange::from_reader(&level0[0]);

        loop {
            let mut changed = false;

            for (idx, reader) in level0.iter().enumerate() {
                if !source_selected[idx] && key_range.overlaps_reader(reader) {
                    source_selected[idx] = true;
                    source_indices.push(idx);
                    key_range.include_reader(reader);
                    changed = true;
                }
            }

            if let Some(target_level_readers) = levels.get(target_level) {
                for (idx, reader) in target_level_readers.iter().enumerate() {
                    if !target_selected[idx] && key_range.overlaps_reader(reader) {
                        target_selected[idx] = true;
                        target_indices.push(idx);
                        key_range.include_reader(reader);
                        changed = true;
                    }
                }
            }

            if !changed {
                break;
            }
        }

        return Some(CompactionPlan {
            source_level,
            target_level,
            source_indices,
            target_indices,
            key_range,
        });
    }

    let (source_idx, source_reader) = levels[source_level]
        .iter()
        .enumerate()
        .min_by(|(_, lhs), (_, rhs)| compare_readers_by_range(lhs, rhs))?;
    let mut target_indices = Vec::new();
    let mut target_selected = vec![false; levels.get(target_level).map_or(0, Vec::len)];
    let mut key_range = KeyRange::from_reader(source_reader);

    loop {
        let mut changed = false;
        if let Some(target_level_readers) = levels.get(target_level) {
            for (idx, reader) in target_level_readers.iter().enumerate() {
                if !target_selected[idx] && key_range.overlaps_reader(reader) {
                    target_selected[idx] = true;
                    target_indices.push(idx);
                    key_range.include_reader(reader);
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }

    Some(CompactionPlan {
        source_level,
        target_level,
        source_indices: vec![source_idx],
        target_indices,
        key_range,
    })
}

fn take_selected_readers(
    readers: Vec<SSTableReader>,
    selected_indices: &[usize],
) -> (Vec<SSTableReader>, Vec<SSTableReader>) {
    let mut selected = vec![false; readers.len()];
    for &idx in selected_indices {
        selected[idx] = true;
    }

    let mut chosen = Vec::with_capacity(selected_indices.len());
    let mut remaining = Vec::with_capacity(readers.len().saturating_sub(selected_indices.len()));

    for (idx, reader) in readers.into_iter().enumerate() {
        if selected[idx] {
            chosen.push(reader);
        } else {
            remaining.push(reader);
        }
    }

    (chosen, remaining)
}

fn collect_paths(sources: &[SSTableReader], targets: &[SSTableReader]) -> Vec<PathBuf> {
    sources
        .iter()
        .chain(targets.iter())
        .map(|reader| reader.path.clone())
        .collect()
}

fn sort_level_by_range(level: &mut [SSTableReader]) {
    level.sort_by(compare_readers_by_range);
}

fn compare_readers_by_range(lhs: &SSTableReader, rhs: &SSTableReader) -> Ordering {
    lhs.smallest_key()
        .cmp(rhs.smallest_key())
        .then(lhs.largest_key().cmp(rhs.largest_key()))
        .then(lhs.path.cmp(&rhs.path))
}

fn ranges_overlap(
    lhs_smallest: &[u8],
    lhs_largest: &[u8],
    rhs_smallest: &[u8],
    rhs_largest: &[u8],
) -> bool {
    !(lhs_largest < rhs_smallest || rhs_largest < lhs_smallest)
}
