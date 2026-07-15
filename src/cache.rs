//! # cache
//!
//! Hand-rolled bounded LRU block cache for SSTable data blocks.
//!
//! ## Role in the LSM pipeline
//! The Block Cache sits between `SSTableReader::get_with_cache` and the kernel
//! page cache. Hot data blocks (≤ 4 KiB each) are kept in a bounded in-memory
//! structure so that repeated `get` calls for the same block avoid the `pread`
//! syscall. The Engine constructs one cache instance shared across all open
//! SSTables. The Compactor calls `invalidate` when deleting an SSTable file
//! so no stale block remains in the cache.
//!
//! ## Internal structure
//! A doubly linked list (most-recently-used at head, LRU at tail) gives O(1)
//! promotion and O(1) eviction. A `HashMap<CacheKey, *mut Node>` gives O(1)
//! lookup by cache key. The list exclusively owns every `Node` allocation;
//! the map holds raw pointers, not `Box`es.
//!
//! ## Dependencies
//! - `std::collections::HashMap` — keyed O(1) lookup.
//! - `std::path::{Path, PathBuf}` — SSTable paths are part of the cache key.
//! - `std::ptr::null_mut` — null-pointer list-endpoint sentinel.
//!
//! ## Used by
//! - `sstable::SSTableReader::get_with_cache` — probes and populates the cache.
//! - `engine::Engine` — holds the single `BlockCache` instance.
//! - `compactor::Compactor` — calls `invalidate` after deleting a file.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::ptr::null_mut;

/// Identifies a single data block within the SSTable file universe.
///
/// The path is the absolute path of the SSTable file; the `u64` is the byte
/// offset of the data block within that file. Together they are globally
/// unique because SSTable paths are unique and block offsets are fixed at
/// write time.
pub type CacheKey = (PathBuf, u64);

/// One node in the doubly linked list backing the LRU cache.
///
/// The list is the sole owner of every `Node`. The `HashMap` in `BlockCache`
/// holds raw pointers into the list but never allocates or frees nodes.
/// Both pointer fields are `null_mut()` at allocation time and at the
/// respective list endpoints.
struct Node {
    /// The cache key this node was inserted under; read during eviction to
    /// remove the corresponding `HashMap` entry.
    key: CacheKey,
    /// Raw block bytes as read from disk (CRC header + entry payload).
    value: Vec<u8>,
    /// Pointer toward the head (more recently used); `null_mut()` at head.
    prev: *mut Node,
    /// Pointer toward the tail (less recently used); `null_mut()` at tail.
    next: *mut Node,
}

/// Bounded LRU cache for SSTable data blocks, hand-rolled with `unsafe` Rust.
///
/// Internally: a `HashMap<CacheKey, *mut Node>` backed by a doubly linked
/// list where head = most recently used and tail = least recently used
/// (eviction candidate). All three core operations (`get`, `insert`,
/// `invalidate`) run in O(1) amortised time.
///
/// # Why raw pointers rather than `Rc<RefCell<_>>`
/// `Rc<RefCell<_>>` requires runtime borrow checks and cycle-breaking `Weak`
/// pointers, adding overhead and obscuring the ownership model. A raw-pointer
/// list with explicit invariants is the textbook LRU layout and is clearer
/// to audit once the safety contract is written down.
///
/// # Safety invariants upheld by every method
/// 1. Every `*mut Node` stored in `self.map` was produced by `Box::into_raw`
///    and is exclusively owned by this cache until freed by `Box::from_raw`.
/// 2. Every `node.prev` and `node.next` is either `null_mut()` (list
///    endpoint) or a valid `*mut Node` owned by this same cache.
/// 3. `head.is_null() ⟺ tail.is_null() ⟺ map.is_empty()`.
/// 4. `size_bytes == Σ node.value.len()` across all live nodes.
///
/// `BlockCache` is `!Send + !Sync` by virtue of the raw pointers.
/// Used behind the single-threaded `Engine`.
pub struct BlockCache {
    /// Maps each key to the raw pointer of its node in the linked list.
    map: HashMap<CacheKey, *mut Node>,
    /// Head of the list (most recently used); `null_mut()` when empty.
    head: *mut Node,
    /// Tail of the list (least recently used); `null_mut()` when empty.
    tail: *mut Node,
    /// Sum of `value.len()` for every node currently in the cache.
    size_bytes: usize,
    /// Byte threshold; once `size_bytes` exceeds this, tail nodes are evicted.
    capacity_bytes: usize,
    /// Count of `get` calls that found a matching node.
    hits: u64,
    /// Count of `get` calls that found no matching node.
    misses: u64,
}

impl BlockCache {
    /// Creates an empty cache. Eviction of LRU blocks begins as soon as
    /// `size_bytes` would exceed `capacity_bytes` after an `insert`.
    pub fn new(capacity_bytes: usize) -> BlockCache {
        BlockCache {
            map: HashMap::new(),
            head: null_mut(),
            tail: null_mut(),
            size_bytes: 0,
            capacity_bytes,
            hits: 0,
            misses: 0,
        }
    }

    /// Returns a shared reference to the cached bytes for `key`, promoting
    /// the node to most recently used.
    ///
    /// Returns `None` when `key` is absent. `hit_count` is incremented on a
    /// hit; `miss_count` is incremented on a miss.
    pub fn get(&mut self, key: &CacheKey) -> Option<&Vec<u8>> {
        // Copy the raw pointer out of the map so the immutable borrow on
        // self.map ends before we call move_to_front, which requires &mut self.
        let ptr_opt = self.map.get(key).copied();
        if let Some(ptr) = ptr_opt {
            // SAFETY: ptr was produced by Box::into_raw and is owned by this
            // cache (invariant 1). move_to_front only adjusts list pointers
            // (invariant 2) — it never frees the node.
            unsafe {
                self.move_to_front(ptr);
            }
            self.hits += 1;
            // SAFETY: ptr is valid for the entire lifetime of &mut self.
            // The node cannot be dropped while this cache is alive.
            Some(unsafe { &(*ptr).value })
        } else {
            self.misses += 1;
            None
        }
    }

    /// Inserts `block` under `key`, evicting LRU entries as needed to stay
    /// within `capacity_bytes`.
    ///
    /// If `key` is already cached the value is replaced in-place and the
    /// node is promoted to the head (no allocation). If `block` is larger
    /// than `capacity_bytes`, it is inserted and then immediately evicted,
    /// leaving the cache empty — the caller will experience a miss on the
    /// next read, but the invariants are maintained.
    pub fn insert(&mut self, key: CacheKey, block: Vec<u8>) {
        // Copy the pointer out before any further borrows on self.
        let existing = self.map.get(&key).copied();
        if let Some(ptr) = existing {
            // Overwrite path: replace value bytes in-place and promote to head.
            // SAFETY: ptr is a valid Box<Node> owned by this cache (invariant 1).
            unsafe {
                let old_len = (*ptr).value.len();
                let new_len = block.len();
                // Invariant 4: adjust size_bytes by the delta.
                self.size_bytes = self.size_bytes - old_len + new_len;
                (*ptr).value = block;
                self.move_to_front(ptr);
            }
        } else {
            // New-key path: allocate a fresh node and link it at the head.
            let new_len = block.len();
            let raw = Box::into_raw(Box::new(Node {
                key: key.clone(),
                value: block,
                prev: null_mut(),
                next: null_mut(),
            }));
            // SAFETY: raw came from Box::into_raw with null prev/next,
            // satisfying push_front's precondition. We record it in the map
            // immediately after, satisfying invariant 1.
            unsafe {
                self.push_front(raw);
            }
            self.map.insert(key, raw);
            self.size_bytes += new_len;
        }

        // Evict from the tail until we are within capacity.
        while self.size_bytes > self.capacity_bytes && !self.tail.is_null() {
            let lru = self.tail;
            // SAFETY: lru is non-null and a valid Box<Node> (invariants 1–3).
            // We extract key and len before unlinking because unlink zeroes
            // the node's prev/next; the node itself is still valid until freed.
            let lru_key = unsafe { (*lru).key.clone() };
            let lru_len = unsafe { (*lru).value.len() };
            // SAFETY: lru is a valid non-null node in this list.
            unsafe {
                self.unlink(lru);
            }
            self.map.remove(&lru_key);
            self.size_bytes -= lru_len;
            // SAFETY: lru is now fully unlinked and removed from the map;
            // no other references to it exist anywhere in this cache.
            drop(unsafe { Box::from_raw(lru) });
        }
    }

    /// Removes all cached blocks whose path component equals `path`.
    ///
    /// Called by `Compactor` after an SSTable file is deleted so
    /// stale block data cannot be served from the cache.
    pub fn invalidate(&mut self, path: &Path) {
        // Collect matching keys first so we are not mutating the map while
        // iterating over it.
        let to_remove: Vec<CacheKey> = self
            .map
            .keys()
            .filter(|(p, _)| p.as_path() == path)
            .cloned()
            .collect();
        for key in to_remove {
            self.remove(&key);
        }
    }

    /// Number of `get` calls that returned `Some`.
    pub fn hit_count(&self) -> u64 {
        self.hits
    }

    /// Number of `get` calls that returned `None`.
    pub fn miss_count(&self) -> u64 {
        self.misses
    }

    /// Current sum of `value.len()` across all nodes in the cache.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Byte threshold at which eviction is triggered.
    pub fn capacity_bytes(&self) -> usize {
        self.capacity_bytes
    }

    /// Number of entries currently in the cache.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if the cache contains no entries.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Splices `node` out of the doubly linked list without freeing it.
    ///
    /// After this call `node.prev` and `node.next` are both `null_mut()` and
    /// no other node references `node`. The `HashMap` is NOT updated; callers
    /// that need to remove the map entry must do so separately.
    ///
    /// # Safety
    /// `node` must be a non-null pointer to a `Node` that is currently linked
    /// in this cache's list and owned by this cache (invariants 1 and 2).
    unsafe fn unlink(&mut self, node: *mut Node) {
        let prev = (*node).prev;
        let next = (*node).next;

        if prev.is_null() {
            // node is the head; its follower becomes the new head.
            self.head = next;
        } else {
            (*prev).next = next;
        }

        if next.is_null() {
            // node is the tail; its predecessor becomes the new tail.
            self.tail = prev;
        } else {
            (*next).prev = prev;
        }

        (*node).prev = null_mut();
        (*node).next = null_mut();
    }

    /// Links `node` at the front (head) of the list — the most recently used
    /// position.
    ///
    /// # Safety
    /// `node` must be a non-null pointer to a `Node` with `prev` and `next`
    /// both set to `null_mut()` (i.e., not currently part of any list).
    unsafe fn push_front(&mut self, node: *mut Node) {
        if self.head.is_null() {
            // List was empty; node becomes both head and tail.
            self.head = node;
            self.tail = node;
        } else {
            // Splice node in front of the current head.
            (*node).next = self.head;
            (*self.head).prev = node;
            self.head = node;
        }
    }

    /// Moves `node` to the head position. No-op if `node` is already at head.
    ///
    /// # Safety
    /// `node` must be a non-null pointer to a `Node` currently linked in this
    /// cache's list (invariants 1 and 2).
    unsafe fn move_to_front(&mut self, node: *mut Node) {
        if self.head == node {
            return;
        }
        self.unlink(node);
        self.push_front(node);
    }

    /// Removes the entry for `key`: unlinks its node, subtracts its byte
    /// contribution from `size_bytes`, and drops the heap allocation.
    ///
    /// No-op if `key` is not in the cache.
    fn remove(&mut self, key: &CacheKey) {
        if let Some(ptr) = self.map.remove(key) {
            // SAFETY: ptr is a valid Box<Node> owned by this cache (invariant 1).
            unsafe {
                self.size_bytes -= (*ptr).value.len();
                self.unlink(ptr);
                drop(Box::from_raw(ptr));
            }
        }
    }
}

impl Drop for BlockCache {
    /// Frees every node by walking the list from head to null.
    ///
    /// The `HashMap` stores raw pointers, not `Box`es, so the default `Drop`
    /// for `HashMap` would leak every node without this impl. Walking the list
    /// (rather than the map) ensures each node is freed exactly once in
    /// insertion order.
    fn drop(&mut self) {
        let mut current = self.head;
        while !current.is_null() {
            // SAFETY: current is a valid Box<Node> owned by this cache
            // (invariant 1). We capture `next` before freeing so the list
            // traversal is not invalidated by the drop.
            let next = unsafe { (*current).next };
            drop(unsafe { Box::from_raw(current) });
            current = next;
        }
    }
}
