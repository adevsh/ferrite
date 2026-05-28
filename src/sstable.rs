//! # sstable
//!
//! SSTable (Sorted String Table) — the immutable on-disk sorted file format.
//!
//! ## Role in the LSM pipeline
//! SSTables are the permanent storage layer. The Engine flushes the
//! Memtable into a new Level-0 SSTable when the Memtable reaches its size
//! threshold. Reads first check the in-memory Memtable, then walk SSTables
//! newest-to-oldest, short-circuiting on bloom-filter misses. Compaction
//! Compaction merges multiple SSTables into a larger one at the next level.
//!
//! ## On-disk layout
//! ```text
//! [ Data Block 0 | Data Block 1 | ... | Index Block | Bloom Bytes | Footer ]
//!
//! Data Block  : [ CRC32(4) | entry_count(4) | entries... ]
//! Entry       : [ type(1) | key_len(4) | val_len(4) | key | value ]
//! Index Block : [ entry_count(4) | { key_len(4) | key | block_offset(8) }... ]
//! Footer      : [ index_offset(8) | bloom_offset(8) | magic(4) | version(4) ]
//! ```
//! The per-entry `type` byte (0x01 = value, 0x02 = tombstone) and per-block
//! CRC32 are additions over the bare SKILL.md format: the type byte is
//! required to distinguish an empty-value put from a tombstone without
//! conflating them; the CRC is required for the "corrupt data block" test.
//!
//! ## Dependencies
//! - `bloom`  — `BloomFilter` embedded in the file for O(1) miss detection.
//! - `codec`  — little-endian `encode_u32`/`decode_u32`/`encode_u64`/`decode_u64`.
//! - `error`  — all public methods return `Result<T>`.
//! - `crc32fast` — per-block CRC32, same algorithm as the WAL.
//! - `std::os::unix::fs::FileExt` — `read_at` (pread) so reads are cursor-free
//!   and callable via `&self`. Unix-only; macOS/Linux supported.
//!
//! ## Used by
//! - `engine` — `SSTableWriter` for Memtable flushes; `SSTableReader`
//!   for `get`/`scan_prefix`/startup scan.
//! - `compactor` — reads via `iter()` and writes a merged output.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::bloom::BloomFilter;
use crate::cache::{BlockCache, CacheKey};
use crate::codec::{decode_u32, decode_u64, encode_u32, encode_u64};
use crate::error::{FerriteError, Result};

// --- constants ---------------------------------------------------------------

/// Magic number in the SSTable footer that identifies this file format.
const MAGIC: u32 = 0x00FE771E;

/// On-disk format version; bump on incompatible layout changes.
const VERSION: u32 = 1;

/// Fixed byte size of the footer at the tail of every SSTable file.
const FOOTER_SIZE: usize = 24;

/// Maximum on-disk size of a single data block in bytes (CRC + header + entries).
const MAX_BLOCK_SIZE: usize = 4096;

/// Byte overhead of the per-block header: CRC32 (4) + entry_count (4).
const DATA_BLOCK_HEADER_SIZE: usize = 8;

/// Parsed entries from one data block: key + optional value (None = tombstone).
type BlockEntries = Vec<(Vec<u8>, Option<Vec<u8>>)>;

/// Type byte for a live value entry.
const TYPE_VALUE: u8 = 0x01;

/// Type byte for a tombstone entry; no value bytes follow.
const TYPE_TOMBSTONE: u8 = 0x02;

/// Bloom filter false-positive rate used when building the table-level filter.
const BLOOM_FALSE_POSITIVE_RATE: f64 = 0.01;

// --- SSTableMeta -------------------------------------------------------------

/// Summary returned by `SSTableWriter::write` after a successful flush.
#[derive(Debug)]
pub struct SSTableMeta {
    /// Absolute path to the written `.sst` file.
    pub path: PathBuf,
    /// The smallest key written (first entry in the first data block).
    pub smallest_key: Vec<u8>,
    /// The largest key written (last entry in the last data block).
    pub largest_key: Vec<u8>,
    /// Total number of entries (values + tombstones).
    pub entry_count: usize,
}

// --- SSTableWriter -----------------------------------------------------------

/// One-shot writer that serialises a sorted entry iterator into an SSTable file.
///
/// Construct with `new`, then call `write` exactly once. `write` consumes
/// `self` to enforce single-use semantics — SSTable files are immutable once
/// written (LSM invariant 5).
pub struct SSTableWriter {
    /// File handle opened for sequential writes.
    file: File,
    /// Destination path; moved into `SSTableMeta` on success.
    path: PathBuf,
}

impl SSTableWriter {
    /// Opens a new, empty SSTable file at `path`.
    ///
    /// Uses `create_new(true)` so the call fails if the file already exists,
    /// preventing silent overwrites of existing SSTables.
    ///
    /// # Errors
    /// Returns `FerriteError::Io` if the parent directory does not exist or
    /// the file cannot be created.
    pub fn new(path: &Path) -> Result<SSTableWriter> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)?;
        Ok(SSTableWriter {
            file,
            path: path.to_path_buf(),
        })
    }

    /// Serialises `entries` into a complete SSTable, syncs to disk, and returns
    /// `SSTableMeta`.
    ///
    /// `entries` must arrive in **ascending key order**; the writer does not
    /// sort them. Passing an empty iterator returns `FerriteError::InvalidFormat`.
    ///
    /// The file is fsynced before returning so the Engine may immediately
    /// truncate the WAL after the meta is received (LSM invariant 2).
    ///
    /// # Errors
    /// Returns `FerriteError::InvalidFormat` if `entries` is empty.
    /// Returns `FerriteError::Io` if any write or fsync fails.
    pub fn write(
        mut self,
        entries: impl Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
    ) -> Result<SSTableMeta> {
        let mut file_offset: u64 = 0;
        let mut index_entries: Vec<(Vec<u8>, u64)> = Vec::new();
        // Buffer keys for bloom construction (bounded by memtable size ≈ 4 MiB).
        let mut all_keys: Vec<Vec<u8>> = Vec::new();
        let mut smallest_key: Option<Vec<u8>> = None;
        let mut largest_key: Vec<u8> = Vec::new();
        let mut entry_count: usize = 0;

        // Current data block state.
        let mut block_payload: Vec<u8> = Vec::new();
        let mut block_entry_count: u32 = 0;
        let mut block_first_key: Option<Vec<u8>> = None;

        for (key, opt_val) in entries {
            let val_len = opt_val.as_ref().map(|v| v.len()).unwrap_or(0);
            // type(1) + key_len(4) + val_len(4) + key + value
            let entry_size = 9 + key.len() + val_len;
            let projected = DATA_BLOCK_HEADER_SIZE + block_payload.len() + entry_size;

            // Flush current block if this entry would push it past the limit.
            // Never flush an empty block: a single oversized entry is written
            // as its own block rather than silently discarded.
            if projected > MAX_BLOCK_SIZE && block_entry_count > 0 {
                file_offset = write_data_block(
                    &mut self.file,
                    file_offset,
                    &block_payload,
                    block_entry_count,
                    block_first_key.take().unwrap(),
                    &mut index_entries,
                )?;
                block_payload.clear();
                block_entry_count = 0;
            }

            if block_first_key.is_none() {
                block_first_key = Some(key.clone());
            }

            // Serialize entry into block_payload.
            if let Some(ref val) = opt_val {
                block_payload.push(TYPE_VALUE);
                block_payload.extend_from_slice(&encode_u32(key.len() as u32));
                block_payload.extend_from_slice(&encode_u32(val.len() as u32));
                block_payload.extend_from_slice(&key);
                block_payload.extend_from_slice(val);
            } else {
                block_payload.push(TYPE_TOMBSTONE);
                block_payload.extend_from_slice(&encode_u32(key.len() as u32));
                block_payload.extend_from_slice(&encode_u32(0u32));
                block_payload.extend_from_slice(&key);
            }

            block_entry_count += 1;
            entry_count += 1;

            if smallest_key.is_none() {
                smallest_key = Some(key.clone());
            }
            largest_key.clone_from(&key);
            all_keys.push(key);
        }

        // Flush the final (possibly only) pending block.
        if block_entry_count > 0 {
            file_offset = write_data_block(
                &mut self.file,
                file_offset,
                &block_payload,
                block_entry_count,
                block_first_key.take().unwrap(),
                &mut index_entries,
            )?;
        }

        if entry_count == 0 {
            return Err(FerriteError::InvalidFormat(
                "cannot write an empty SSTable".into(),
            ));
        }

        let smallest_key = smallest_key.unwrap();

        // --- Index block -----------------------------------------------------
        let index_offset = file_offset;
        let mut index_buf = Vec::new();
        index_buf.extend_from_slice(&encode_u32(index_entries.len() as u32));
        for (first_key, block_off) in &index_entries {
            index_buf.extend_from_slice(&encode_u32(first_key.len() as u32));
            index_buf.extend_from_slice(first_key);
            index_buf.extend_from_slice(&encode_u64(*block_off));
        }
        self.file.write_all(&index_buf)?;
        file_offset += index_buf.len() as u64;

        // --- Bloom filter ----------------------------------------------------
        let bloom_offset = file_offset;
        let mut bloom = BloomFilter::new(all_keys.len().max(1), BLOOM_FALSE_POSITIVE_RATE);
        for key in &all_keys {
            bloom.insert(key);
        }
        let bloom_bytes = bloom.to_bytes();
        self.file.write_all(&bloom_bytes)?;

        // --- Footer ----------------------------------------------------------
        let mut footer = Vec::with_capacity(FOOTER_SIZE);
        footer.extend_from_slice(&encode_u64(index_offset));
        footer.extend_from_slice(&encode_u64(bloom_offset));
        footer.extend_from_slice(&encode_u32(MAGIC));
        footer.extend_from_slice(&encode_u32(VERSION));
        self.file.write_all(&footer)?;

        // fsync before returning — the Engine may truncate the WAL next.
        self.file.sync_all()?;

        Ok(SSTableMeta {
            path: self.path,
            smallest_key,
            largest_key,
            entry_count,
        })
    }
}

/// Serialises one data block to `file` and records its starting offset in `index_entries`.
///
/// Block layout: `[ CRC32(4) | entry_count(4) | payload ]`. CRC covers
/// `entry_count_bytes ++ payload`, matching the WAL's CRC convention
/// (`wal::append_record`): the checksum field is excluded from its own hash.
///
/// Returns the updated `file_offset` (= old offset + block byte length).
fn write_data_block(
    file: &mut File,
    file_offset: u64,
    payload: &[u8],
    entry_count: u32,
    first_key: Vec<u8>,
    index_entries: &mut Vec<(Vec<u8>, u64)>,
) -> Result<u64> {
    let count_bytes = encode_u32(entry_count);

    // CRC covers everything after itself: entry_count field + payload.
    let mut crc_body = Vec::with_capacity(4 + payload.len());
    crc_body.extend_from_slice(&count_bytes);
    crc_body.extend_from_slice(payload);
    let crc = crc32fast::hash(&crc_body);

    let mut block = Vec::with_capacity(DATA_BLOCK_HEADER_SIZE + payload.len());
    block.extend_from_slice(&encode_u32(crc));
    block.extend_from_slice(&count_bytes);
    block.extend_from_slice(payload);

    file.write_all(&block)?;

    // The block starts at file_offset (all writes are sequential).
    index_entries.push((first_key, file_offset));

    Ok(file_offset + block.len() as u64)
}

// --- SSTableReader -----------------------------------------------------------

/// Read-only handle to a complete SSTable file.
///
/// On `open`, the footer is validated, the index is loaded into a sorted
/// `Vec` in memory, and the bloom filter is deserialised. Data blocks are
/// read on demand via `get`/`scan_prefix`/`iter` using `read_at` (pread) so
/// the file cursor is never advanced and `&self` is sufficient.
pub struct SSTableReader {
    /// Absolute path; used as a cache key by the Block Cache.
    pub path: PathBuf,
    /// Open file handle; reads use `FileExt::read_at` — cursor-free.
    file: File,
    /// In-memory index: one `(first_key, block_offset)` per data block.
    index: Vec<(Vec<u8>, u64)>,
    /// Byte offset where the index block starts; also = end of data blocks.
    index_offset: u64,
    /// Deserialised bloom filter; probed before every data block read.
    bloom: BloomFilter,
    /// Count of `get` calls short-circuited by the bloom filter.
    bloom_misses: AtomicU64,
    /// Smallest key in the table (first key of the first data block).
    smallest_key: Vec<u8>,
    /// Largest key in the table (last key in the last data block).
    largest_key: Vec<u8>,
}

impl SSTableReader {
    /// Opens an existing SSTable and loads its index and bloom filter into memory.
    ///
    /// Reads the footer to locate the index and bloom regions, parses the index,
    /// deserialises the bloom filter, and reads the last data block once to
    /// extract `largest_key`.
    ///
    /// # Errors
    /// Returns `FerriteError::Io` if the file cannot be opened or read.
    /// Returns `FerriteError::InvalidFormat` if the footer magic/version are
    /// wrong, if the file is too small, or if any structural field is malformed.
    /// Returns `FerriteError::Corruption` if the last data block fails its
    /// CRC check.
    pub fn open(path: &Path) -> Result<SSTableReader> {
        let file = OpenOptions::new().read(true).open(path)?;
        let file_size = file.metadata()?.len();

        if file_size < FOOTER_SIZE as u64 {
            return Err(FerriteError::InvalidFormat(format!(
                "SSTable {path:?} too small: {file_size} bytes (footer needs {FOOTER_SIZE})"
            )));
        }

        // Read and validate footer.
        let footer = read_exact_at(&file, file_size - FOOTER_SIZE as u64, FOOTER_SIZE)?;
        let index_offset = decode_u64(&footer[0..8])?;
        let bloom_offset = decode_u64(&footer[8..16])?;
        let magic = decode_u32(&footer[16..20])?;
        let version = decode_u32(&footer[20..24])?;

        if magic != MAGIC {
            return Err(FerriteError::InvalidFormat(format!(
                "SSTable {path:?} bad magic: 0x{magic:X} (expected 0x{MAGIC:X})"
            )));
        }
        if version != VERSION {
            return Err(FerriteError::InvalidFormat(format!(
                "SSTable {path:?} unsupported version: {version} (expected {VERSION})"
            )));
        }

        // Read and parse index block.
        let index_len = (bloom_offset - index_offset) as usize;
        let index_bytes = read_exact_at(&file, index_offset, index_len)?;
        let entry_count = decode_u32(&index_bytes[0..4])? as usize;
        let mut index: Vec<(Vec<u8>, u64)> = Vec::with_capacity(entry_count);
        let mut pos = 4usize;
        for _ in 0..entry_count {
            let key_len = decode_u32(&index_bytes[pos..pos + 4])? as usize;
            pos += 4;
            let key = index_bytes[pos..pos + key_len].to_vec();
            pos += key_len;
            let block_off = decode_u64(&index_bytes[pos..pos + 8])?;
            pos += 8;
            index.push((key, block_off));
        }

        // Read and deserialise bloom filter.
        let bloom_len = (file_size - FOOTER_SIZE as u64 - bloom_offset) as usize;
        let bloom_bytes = read_exact_at(&file, bloom_offset, bloom_len)?;
        let bloom = BloomFilter::from_bytes(&bloom_bytes)?;

        // smallest_key = first key of first data block.
        let smallest_key = index.first().map(|(k, _)| k.clone()).unwrap_or_default();

        // largest_key = last key in last data block (requires one block read).
        let largest_key = if index.is_empty() {
            Vec::new()
        } else {
            let last = index.len() - 1;
            let blk_start = index[last].1;
            let blk_end = index_offset;
            let blk_bytes = read_exact_at(&file, blk_start, (blk_end - blk_start) as usize)?;
            let entries = parse_block_entries(&blk_bytes, blk_start)?;
            entries.into_iter().last().map(|(k, _)| k).unwrap_or_default()
        };

        Ok(SSTableReader {
            path: path.to_path_buf(),
            file,
            index,
            index_offset,
            bloom,
            bloom_misses: AtomicU64::new(0),
            smallest_key,
            largest_key,
        })
    }

    /// Looks up `key` and returns a three-state result.
    ///
    /// - `Ok(None)` — key is absent from this SSTable (bloom miss, or not
    ///   found in the candidate block).
    /// - `Ok(Some(Some(v)))` — key is present with value `v`.
    /// - `Ok(Some(None))` — key is present as a tombstone.
    ///
    /// The three-state shape (deliberate extension to SKILL.md's
    /// `Result<Option<Vec<u8>>>`) is required for correct LSM layering: the
    /// Engine must stop at a tombstone hit in a newer SSTable rather than
    /// falling through to a stale value in an older one. Collapsing tombstone
    /// and absent into one `None` would violate invariant 3 ("tombstones
    /// propagate through all layers until bottom-level compaction").
    ///
    /// # Errors
    /// Returns `FerriteError::Corruption` if the candidate data block fails
    /// its CRC check.
    /// Returns `FerriteError::Io` if the block read fails.
    pub fn get(&self, key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
        if !self.bloom.may_contain(key) {
            // Bloom says definitely absent — skip all block I/O.
            self.bloom_misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }

        // Binary search: rightmost block whose first_key <= key.
        let pp = self
            .index
            .partition_point(|(fk, _)| fk.as_slice() <= key);
        if pp == 0 {
            // Key is smaller than the first block's first key.
            return Ok(None);
        }
        let block_idx = pp - 1;
        let blk_start = self.index[block_idx].1;
        let blk_end = if block_idx + 1 < self.index.len() {
            self.index[block_idx + 1].1
        } else {
            self.index_offset
        };

        let blk_bytes = read_exact_at(&self.file, blk_start, (blk_end - blk_start) as usize)?;
        let entries = parse_block_entries(&blk_bytes, blk_start)?;

        for (entry_key, entry_val) in entries {
            if entry_key.as_slice() == key {
                return Ok(Some(entry_val));
            }
            // Entries are sorted — once we've passed the target, no point continuing.
            if entry_key.as_slice() > key {
                break;
            }
        }
        Ok(None)
    }

    /// Looks up `key` with the same three-state semantics as [`get`], but
    /// consults `cache` for the candidate data block before issuing a disk read.
    ///
    /// On a cache hit the block bytes are used directly, avoiding the `pread`
    /// syscall. On a cache miss the block is read from disk, inserted into
    /// `cache` for future callers, then searched. Bloom-filter short-circuiting
    /// still applies — a bloom miss returns `Ok(None)` without touching the
    /// cache at all.
    ///
    /// The `&mut` on `cache` is required because a hit promotes the node to
    /// MRU and a miss inserts a new entry.
    ///
    /// # Errors
    /// Same as [`get`]: `FerriteError::Corruption` on CRC failure,
    /// `FerriteError::Io` on read failure.
    pub fn get_with_cache(
        &self,
        key: &[u8],
        cache: &mut BlockCache,
    ) -> Result<Option<Option<Vec<u8>>>> {
        if !self.bloom.may_contain(key) {
            // Bloom says definitely absent — skip cache and all block I/O.
            self.bloom_misses.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        }

        let pp = self
            .index
            .partition_point(|(fk, _)| fk.as_slice() <= key);
        if pp == 0 {
            return Ok(None);
        }
        let block_idx = pp - 1;
        let blk_start = self.index[block_idx].1;
        let blk_end = if block_idx + 1 < self.index.len() {
            self.index[block_idx + 1].1
        } else {
            self.index_offset
        };

        let cache_key: CacheKey = (self.path.clone(), blk_start);
        // 2-stage borrow pattern: `.cloned()` ends the shared borrow on `cache`
        // before we may need to call `cache.insert` on a miss, which requires
        // &mut. The clone copies at most MAX_BLOCK_SIZE (4 KiB) bytes.
        let block_bytes: Vec<u8> = match cache.get(&cache_key).cloned() {
            Some(b) => b,
            None => {
                let bytes =
                    read_exact_at(&self.file, blk_start, (blk_end - blk_start) as usize)?;
                cache.insert(cache_key, bytes.clone());
                bytes
            }
        };

        let entries = parse_block_entries(&block_bytes, blk_start)?;
        for (entry_key, entry_val) in entries {
            if entry_key.as_slice() == key {
                return Ok(Some(entry_val));
            }
            if entry_key.as_slice() > key {
                break;
            }
        }
        Ok(None)
    }

    /// Returns all live (non-tombstone) entries whose key starts with `prefix`,
    /// in ascending key order.
    ///
    /// Tombstones within the prefix range are silently filtered. The Engine
    /// uses `iter()` when it needs tombstone visibility for cross-level scans.
    ///
    /// # Errors
    /// Returns `FerriteError::Corruption` or `FerriteError::Io` if a data
    /// block along the scan path cannot be read or verified.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if self.index.is_empty() {
            return Ok(Vec::new());
        }

        // Start one block before the first block whose first_key > prefix to
        // avoid missing keys whose block started before the prefix but whose
        // individual entries fall within the prefix range.
        let pp = self
            .index
            .partition_point(|(fk, _)| fk.as_slice() <= prefix);
        let start_block = if pp == 0 { 0 } else { pp - 1 };

        let mut results = Vec::new();
        for block_idx in start_block..self.index.len() {
            let blk_start = self.index[block_idx].1;
            let blk_end = if block_idx + 1 < self.index.len() {
                self.index[block_idx + 1].1
            } else {
                self.index_offset
            };
            let blk_bytes =
                read_exact_at(&self.file, blk_start, (blk_end - blk_start) as usize)?;
            let entries = parse_block_entries(&blk_bytes, blk_start)?;

            let mut done = false;
            for (key, val_opt) in entries {
                if key.starts_with(prefix) {
                    if let Some(val) = val_opt {
                        results.push((key, val));
                    }
                    // Tombstones in the prefix range are skipped.
                } else if key.as_slice() > prefix {
                    // Lexicographically past the prefix — no further matches.
                    done = true;
                    break;
                }
                // Keys before the prefix are skipped silently.
            }
            if done {
                break;
            }
        }
        Ok(results)
    }

    /// Returns a lazy sequential iterator over all entries (values and tombstones).
    ///
    /// Each `next` call yields `Ok((key, Some(value)))` for live entries or
    /// `Ok((key, None))` for tombstones. Yields `Err(...)` on block I/O or
    /// CRC failure, after which the iterator state is unspecified.
    pub fn iter(&self) -> SSTableIter<'_> {
        SSTableIter {
            reader: self,
            block_idx: 0,
            current_block: Vec::new(),
            entry_idx: 0,
        }
    }

    /// Returns the number of `get` calls short-circuited by the bloom filter
    /// without reading any data block.
    ///
    /// Exposed for the bloom-short-circuit integration test; not used in
    /// production paths.
    pub fn bloom_misses(&self) -> u64 {
        self.bloom_misses.load(Ordering::Relaxed)
    }

    /// Returns the smallest key in this SSTable.
    pub fn smallest_key(&self) -> &[u8] {
        &self.smallest_key
    }

    /// Returns the largest key in this SSTable.
    pub fn largest_key(&self) -> &[u8] {
        &self.largest_key
    }
}

// --- SSTableIter -------------------------------------------------------------

/// Lazy sequential iterator over all entries in an SSTable, block by block.
pub struct SSTableIter<'a> {
    /// Borrowed reader; provides file handle and index.
    reader: &'a SSTableReader,
    /// Index of the next block to load when `current_block` is exhausted.
    block_idx: usize,
    /// Decoded entries from the currently loaded data block.
    current_block: BlockEntries,
    /// Cursor within `current_block`.
    entry_idx: usize,
}

impl<'a> Iterator for SSTableIter<'a> {
    type Item = Result<(Vec<u8>, Option<Vec<u8>>)>;

    /// Returns the next entry or `None` when all blocks are exhausted.
    ///
    /// `opt_val = None` in the yielded tuple signals a tombstone.
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Serve from the in-memory current block if entries remain.
            if self.entry_idx < self.current_block.len() {
                let entry = self.current_block[self.entry_idx].clone();
                self.entry_idx += 1;
                return Some(Ok(entry));
            }

            // All blocks exhausted.
            if self.block_idx >= self.reader.index.len() {
                return None;
            }

            // Load the next block.
            let blk_start = self.reader.index[self.block_idx].1;
            let blk_end = if self.block_idx + 1 < self.reader.index.len() {
                self.reader.index[self.block_idx + 1].1
            } else {
                self.reader.index_offset
            };
            self.block_idx += 1;

            let blk_bytes = match read_exact_at(
                &self.reader.file,
                blk_start,
                (blk_end - blk_start) as usize,
            ) {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };

            match parse_block_entries(&blk_bytes, blk_start) {
                Ok(entries) => {
                    self.current_block = entries;
                    self.entry_idx = 0;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// --- private helpers ---------------------------------------------------------

/// Reads exactly `len` bytes from `file` at `offset` using `pread`.
///
/// `FileExt::read_at` does not move the file cursor, which allows multiple
/// concurrent reads via `&self` references to the same `File`. Returns
/// `FerriteError::InvalidFormat` on a short read (truncated file or offset
/// past EOF).
fn read_exact_at(file: &File, offset: u64, len: usize) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    let n = file.read_at(&mut buf, offset)?;
    if n != len {
        return Err(FerriteError::InvalidFormat(format!(
            "short read at offset {offset}: expected {len} bytes, got {n}"
        )));
    }
    Ok(buf)
}

/// Parses and CRC-verifies a raw data block byte slice.
///
/// Block layout: `[ CRC32(4) | entry_count(4) | entries... ]`. The stored
/// CRC is compared against `crc32fast::hash(&block[4..])` — covering the
/// entry_count field and all entry bytes, but not the CRC field itself
/// (mirrors the WAL pattern). Returns `FerriteError::Corruption` on mismatch
/// so callers can distinguish data corruption from ordinary API errors.
///
/// `block_offset` is included in error messages only.
fn parse_block_entries(
    block: &[u8],
    block_offset: u64,
) -> Result<BlockEntries> {
    if block.len() < DATA_BLOCK_HEADER_SIZE {
        return Err(FerriteError::InvalidFormat(format!(
            "data block at {block_offset} is only {} bytes (need at least {DATA_BLOCK_HEADER_SIZE})",
            block.len()
        )));
    }

    let stored_crc = decode_u32(&block[0..4])?;
    let computed_crc = crc32fast::hash(&block[4..]);
    if stored_crc != computed_crc {
        return Err(FerriteError::Corruption(format!(
            "data block CRC mismatch at offset {block_offset}: \
             stored {stored_crc:#010x} vs computed {computed_crc:#010x}"
        )));
    }

    let entry_count = decode_u32(&block[4..8])? as usize;
    let mut pos = 8usize;
    let mut entries = Vec::with_capacity(entry_count);

    for i in 0..entry_count {
        if pos >= block.len() {
            return Err(FerriteError::InvalidFormat(format!(
                "block at {block_offset}: entry {i} starts past end of block"
            )));
        }
        let entry_type = block[pos];
        pos += 1;

        let key_len = decode_u32(&block[pos..pos + 4])? as usize;
        pos += 4;
        let val_len = decode_u32(&block[pos..pos + 4])? as usize;
        pos += 4;

        let key = block[pos..pos + key_len].to_vec();
        pos += key_len;

        let val_opt = match entry_type {
            TYPE_VALUE => {
                let v = block[pos..pos + val_len].to_vec();
                pos += val_len;
                Some(v)
            }
            TYPE_TOMBSTONE => None,
            other => {
                return Err(FerriteError::InvalidFormat(format!(
                    "unknown entry type 0x{other:02X} in block at {block_offset}"
                )));
            }
        };

        entries.push((key, val_opt));
    }

    Ok(entries)
}
