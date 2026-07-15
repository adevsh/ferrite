# ferrite

A production-grade embedded key-value store built entirely from scratch in Rust, using an LSM-tree storage engine.

## What it is

Ferrite is a durable, persistent key-value store backed by a Log-Structured Merge-tree (LSM-tree). Writes land in a durable Write-Ahead Log and an in-memory sorted Memtable. When the active Memtable crosses a size threshold it is rotated into a frozen Memtable and flushed to an immutable Sorted String Table (SSTable) on disk in the background while writes continue in a fresh active Memtable. SSTables land in overlapping level 0 and are compacted into non-overlapping L1+ levels with a leveled compaction policy. Reads check the active Memtable first, then the frozen Memtable, then level 0, then deeper levels; a per-SSTable Bloom filter and an in-memory LRU block cache keep most reads fast.

## Why it exists

This is a learning project. Every byte on disk is encoded by hand — there are no external storage, serialisation, or compression crates. The only runtime dependencies are `thiserror` (error ergonomics), `crc32fast` (block checksums), and `clap` (CLI argument parsing). `tempfile` is the sole dev-dependency.

## Architecture

```
Write path
----------
  put(key, value)
       |
       v
  WAL (wal.log)         <- fsynced before Memtable insert
       |
       v
  Active Memtable       <- BTreeMap<key, Value|Tombstone>
       |  (threshold crossed)
       v
  Frozen Memtable       <- remains readable while flush runs
       v
  Background flush      <- writes new L0 SSTable without blocking later puts
       |
       v
  Compactor             <- leveled rewrite: L0 overlaps, L1+ stay non-overlapping
       |                   tombstones GC'd only when no deeper overlap remains
       v
  MANIFEST              <- atomic rename; tracks next sequence number

Read path
---------
  get(key)
       |
       +-- 1. Active Memtable (exact match, O(log n))
       |
       +-- 2. Frozen Memtable
       |
       +-- 3. levels[0] newest -> oldest
       |        +-- Bloom filter: skip if absent
       |        +-- binary-search index -> BlockCache (LRU) -> pread
       |
       +-- 4. levels[1], levels[2], ... (non-overlapping by key range)
```

## Build

```sh
cargo build --release
```

Or via the Makefile:

```sh
make build     # cargo build --release
make test      # cargo test -- --nocapture
make check     # cargo clippy -- -D warnings
make fmt       # cargo fmt
```

Requires Rust 1.75 or later (2021 edition).

## CLI

All commands accept a `--data-dir <path>` flag (default: `./data`).

| Command    | Usage                          | Output                                    |
|------------|--------------------------------|-------------------------------------------|
| `put`      | `ferrite put <key> <value>`    | `OK`                                      |
| `get`      | `ferrite get <key>`            | value, or `NOT FOUND`                     |
| `delete`   | `ferrite delete <key>`         | `DELETED`                                 |
| `scan`     | `ferrite scan <prefix>`        | `key | value` per matching key            |
| `compact`  | `ferrite compact`              | `Compaction complete. N files merged.`    |
| `stats`    | `ferrite stats`                | active/frozen memtable bytes, flush state, per-level SSTable counts, cache ratio |
| `bench`    | `ferrite bench <count>`        | write ops/sec, read ops/sec, total time   |

### Example session

```sh
ferrite --data-dir /tmp/demo put user:alice admin
ferrite --data-dir /tmp/demo put user:bob   reader
ferrite --data-dir /tmp/demo get user:alice    # admin
ferrite --data-dir /tmp/demo get user:carol    # NOT FOUND
ferrite --data-dir /tmp/demo scan user:        # user:alice | admin
                                               # user:bob   | reader
ferrite --data-dir /tmp/demo delete user:bob
ferrite --data-dir /tmp/demo scan user:        # user:alice | admin
ferrite --data-dir /tmp/demo stats
ferrite --data-dir /tmp/demo compact
```

## Benchmark

Measured on Apple M-series (macOS/APFS), 100 000 keys:

```
Writing 100000 keys...
Write phase: 100000 ops in 675.024s (148 ops/sec)
Reading 100000 keys in random order...
Read phase:  100000 ops in 0.056s (1769971 ops/sec)
Total time: 675.083s
```

Write throughput (148 ops/sec) reflects the per-write `fsync` latency on macOS/APFS — every `put` is immediately durable. Read throughput (~1.77M ops/sec) reflects the warm block cache; cached blocks are served without touching disk.

To improve write throughput, WAL group commit (see Future work) is the most direct next step.

## On-disk layout

### WAL record (`wal.log`)

```
[CRC32 (4 B)] [type (1 B)] [key_len (4 B)] [val_len (4 B)] [key] [value]
```

All integers little-endian. `type` is `0x01` (Put) or `0x02` (Delete). Recovery stops at the first CRC-failing or partial record.

### SSTable (`L{level}_{seq:08}.sst`)

```
[Data blocks ...] [Index block] [Bloom filter bytes] [Footer (24 B)]
```

Each data block: `[CRC32 (4 B)] [entry_count (4 B)] [entries ...]`. Each entry: `[type (1 B)] [key_len (4 B)] [val_len (4 B)] [key] [value]`. Footer: `[index_offset (8 B)] [bloom_offset (8 B)] [magic (8 B)]`.

### MANIFEST

Plaintext file holding the next sequence number. Written atomically via temp-file + rename after every flush and compaction.

## Project layout

```
src/
  codec.rs       -- LE binary encode/decode primitives (no I/O)
  error.rs       -- FerriteError enum (thiserror)
  bloom.rs       -- hand-rolled Bloom filter (FNV-1a + djb2 double-hashing)
  wal.rs         -- Write-Ahead Log (append + fsync + truncate + recover)
  memtable.rs    -- in-memory sorted buffer (BTreeMap, Value/Tombstone)
  sstable.rs     -- SSTableWriter (single-use) + SSTableReader (pread)
  cache.rs       -- LRU BlockCache (unsafe doubly-linked list)
  compaction.rs  -- leveled Compactor (overlap selection, non-overlapping L1+)
  engine.rs      -- LSM Engine coordinator (active/frozen Memtables, flush/compact)
  cli.rs         -- clap CLI definition (Cli, Command)
  lib.rs         -- library crate root (re-exports for integration tests)
  main.rs        -- binary entry point (command dispatch + bench helper)

tests/
  wal_test.rs          -- WAL recovery (5 tests)
  memtable_test.rs     -- Memtable operations (6 tests)
  bloom_test.rs        -- Bloom filter (5 tests)
  sstable_test.rs      -- SSTableWriter + Reader (9 tests)
  cache_test.rs        -- BlockCache LRU (8 tests)
  compaction_test.rs   -- leveled compaction (10 tests)
  engine_test.rs       -- Engine integration (10 tests)
```

63 tests pass; 10 additional unit tests live inline in `src/codec.rs` and `src/bloom.rs`.

## Future work

The foreground API is still **single-threaded**, although Memtable flush now runs on a background worker. Possible next steps, roughly ordered by effort:

1. **Concurrent reads** — replace `BlockCache::get(&mut self)` with clock-style eviction (`&self`), allowing `Engine::get` to take `&self` and enabling `Arc<RwLock<Engine>>` or a sharded read path.
2. **WAL group commit** — batch multiple puts under one `fsync` to amortise the per-write APFS latency (~10x write throughput improvement).
3. **Level sizing policy** — add per-level byte budgets and fanout targets on top of the current overlap-based leveled rewrites.
4. **Block-level compression** — LZ4 or Snappy on data blocks before CRC; halves typical disk footprint.
5. **Prefix bloom filters** — short-circuit `scan_prefix` the same way point gets are short-circuited today.
6. **Snapshots / MVCC** — sequence-number-tagged keys and snapshot handles for consistent reads under concurrent writes.
7. **Range deletes** — a single `[start, end)` tombstone for bulk deletes.
8. **Async I/O** — `io_uring` or Tokio for the read path (useful once the engine is multi-threaded).
9. **Bench enhancements** — p50/p99 latency histograms, cold-cache vs warm-cache phases, configurable key/value sizes.

## Licence

MIT — see [LICENSE](LICENSE).
