//! # memtable
//!
//! In-memory sorted write buffer backed by the Write-Ahead Log.
//!
//! ## Role in the LSM pipeline
//! The Memtable is the first read target and the destination for every write
//! after the WAL has fsynced the record. It holds the most recent version of
//! every key in sorted order. When `size_bytes` reaches the configured
//! threshold the Engine flushes it to an immutable Level-0 SSTable, then
//! resets the Memtable and truncates the WAL. On startup `restore_from_wal`
//! replays the WAL into a fresh Memtable so no acknowledged write is lost.
//!
//! ## Dependencies
//! - `wal` — `WalRecord` is the unit of replay during `restore_from_wal`.
//!
//! ## Used by
//! - `engine` — calls `put`/`delete` on every write, `get` and
//!   `scan_prefix` on every read, `iter` when flushing to an SSTable, and
//!   `restore_from_wal` on startup.

use std::collections::BTreeMap;

use crate::wal::WalRecord;

/// Default flush threshold: 4 MiB.
///
/// The Engine compares `Memtable::size_bytes()` against this constant (or a
/// user-supplied override) after each write to decide whether to trigger a
/// Level-0 flush.
pub const DEFAULT_MEMTABLE_THRESHOLD: usize = 4 * 1024 * 1024;

/// The value stored for a key in the Memtable.
///
/// `Tombstone` is stored on delete so that the Memtable can shadow any older
/// copy of the key that lives in a lower-level SSTable. The Engine returns
/// `None` to callers only when the key is completely absent from all levels;
/// a `Tombstone` here means "deleted, do not look further down".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemValue {
    /// A live value associated with a key.
    Value(
        /// The raw value bytes.
        Vec<u8>,
    ),
    /// A deletion marker. The key was explicitly deleted; no value exists.
    Tombstone,
}

/// In-memory sorted write buffer.
///
/// Keys are kept in ascending byte order via `BTreeMap` so that `iter()` and
/// `scan_prefix` produce sorted output suitable for writing directly into
/// SSTable data blocks.
pub struct Memtable {
    /// The backing store: key → value-or-tombstone, sorted by key.
    map: BTreeMap<Vec<u8>, MemValue>,
    /// Running tally of key + value bytes inserted so far.
    ///
    /// Monotonically non-decreasing: overwrites add the new value's bytes
    /// without subtracting the old, and tombstones do not reduce the count.
    /// This is intentional — the value is a conservative upper bound used
    /// only for the flush threshold, so an over-estimate just flushes sooner,
    /// never incorrectly.
    size_bytes: usize,
}

impl Memtable {
    /// Returns an empty Memtable with zero size.
    pub fn new() -> Memtable {
        Memtable {
            map: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    /// Inserts or overwrites a key with the given value bytes.
    ///
    /// Size accounting: if the key is new, `key.len() + value.len()` is added;
    /// if the key already exists only `value.len()` is added (the old value's
    /// bytes are not subtracted). See the `size_bytes` field doc for rationale.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if self.map.contains_key(&key) {
            self.size_bytes += value.len();
        } else {
            self.size_bytes += key.len() + value.len();
        }
        self.map.insert(key, MemValue::Value(value));
    }

    /// Marks a key as deleted by inserting a tombstone.
    ///
    /// If the key is new, `key.len()` is added to `size_bytes`; if the key
    /// already has an entry (value or prior tombstone), `size_bytes` is
    /// unchanged — the tombstone replaces the old entry at no additional cost.
    pub fn delete(&mut self, key: Vec<u8>) {
        if !self.map.contains_key(&key) {
            self.size_bytes += key.len();
        }
        self.map.insert(key, MemValue::Tombstone);
    }

    /// Returns the entry for `key`, or `None` if the key has never been written.
    ///
    /// `Some(&MemValue::Tombstone)` means the key was explicitly deleted and
    /// the Engine must not fall through to SSTables looking for an older copy.
    /// `None` means the key is absent from the Memtable entirely; the Engine
    /// may then consult SSTables.
    pub fn get(&self, key: &[u8]) -> Option<&MemValue> {
        self.map.get(key)
    }

    /// Returns an iterator over all entries whose key starts with `prefix`,
    /// in ascending key order.
    ///
    /// Tombstones are included in the output; the caller (the Engine) is
    /// responsible for deciding how to surface them to the user.
    pub fn scan_prefix<'a>(
        &'a self,
        prefix: &'a [u8],
    ) -> impl Iterator<Item = (&'a Vec<u8>, &'a MemValue)> + 'a {
        // BTreeMap::range gives sorted entries starting at the first key >= prefix.
        // take_while stops as soon as a key no longer carries the prefix.
        self.map
            .range(prefix.to_vec()..)
            .take_while(move |(k, _)| k.starts_with(prefix))
    }

    /// Returns the current size estimate in bytes.
    ///
    /// This is a conservative upper bound, not exact heap occupancy. BTreeMap
    /// node overhead is not counted; only key and value byte lengths are.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Returns `true` when `size_bytes` has reached or exceeded `threshold`.
    pub fn is_full(&self, threshold: usize) -> bool {
        self.size_bytes >= threshold
    }

    /// Returns an iterator over every entry in ascending key order.
    ///
    /// Used by the SSTable writer to drain the Memtable into a sorted data
    /// block. Tombstones are included so they persist to Level-0 SSTables and
    /// continue to shadow older copies until bottom-level compaction removes
    /// them.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &MemValue)> {
        self.map.iter()
    }

    /// Builds a new Memtable by replaying WAL records in order.
    ///
    /// Order is preserved because the WAL records operations in the sequence
    /// they were originally applied; replaying out of order could produce a
    /// different final state (e.g., a delete followed by a put would become
    /// a put-then-delete if reversed, leaving a tombstone where a value should
    /// be).
    pub fn restore_from_wal(records: Vec<WalRecord>) -> Memtable {
        let mut memtable = Memtable::new();
        for record in records {
            match record {
                WalRecord::Put { key, value } => memtable.put(key, value),
                WalRecord::Delete { key } => memtable.delete(key),
            }
        }
        memtable
    }
}

impl Default for Memtable {
    /// Returns an empty Memtable. Delegates to [`Memtable::new`].
    fn default() -> Self {
        Memtable::new()
    }
}
