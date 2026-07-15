# Graph Report - ferrite  (2026-07-15)

## Corpus Check
- 22 files · ~25,794 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 338 nodes · 701 edges · 27 communities (16 shown, 11 thin omitted)
- Extraction: 97% EXTRACTED · 3% INFERRED · 0% AMBIGUOUS · INFERRED: 18 edges (avg confidence: 0.8)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `a6418861`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- Bloom Filter
- SSTable Format
- Block Cache
- Engine Core
- Write-Ahead Log
- Memtable
- Compaction Logic
- Graphify Docs
- CLI And Errors
- Compaction Tests
- Cache Tests
- Engine Tests
- Project Overview
- Export Docs
- AGENTS.md Integration
- AGENTS.md
- Wiki Export
- LRU Block Cache
- LSM-tree Storage Engine
- Memtable
- Size-tiered Compactor
- SSTable
- WAL Group Commit
- Write-Ahead Log
- join_panic_message

## God Nodes (most connected - your core abstractions)
1. `Engine` - 37 edges
2. `SSTableReader` - 34 edges
3. `BlockCache` - 27 edges
4. `Memtable` - 21 edges
5. `Wal` - 15 edges
6. `compact_level()` - 14 edges
7. `BloomFilter` - 12 edges
8. `make_reader_entries()` - 12 edges
9. `ferrite` - 11 edges
10. `HeapEntry` - 10 edges

## Surprising Connections (you probably didn't know these)
- `wait_for_background_flush()` --references--> `Engine`  [EXTRACTED]
  tests/engine_test.rs → src/engine.rs
- `assert_level_non_overlapping()` --references--> `SSTableReader`  [EXTRACTED]
  tests/compaction_test.rs → src/sstable.rs
- `find_value_in_level()` --references--> `SSTableReader`  [EXTRACTED]
  tests/compaction_test.rs → src/sstable.rs
- `find_value_in_reader()` --references--> `SSTableReader`  [EXTRACTED]
  tests/compaction_test.rs → src/sstable.rs
- `make_reader()` --references--> `SSTableReader`  [EXTRACTED]
  tests/compaction_test.rs → src/sstable.rs

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Graphify Extraction Flow** — _trae_skills_graphify_skill_structural_extraction, _trae_skills_graphify_skill_semantic_extraction, _trae_skills_graphify_references_extraction_spec_extraction_subagent_prompt, _trae_skills_graphify_references_transcribe_whisper_transcription, _trae_skills_graphify_references_update_incremental_update [INFERRED 0.85]
- **Graph Freshness Mechanisms** — _trae_skills_graphify_references_add_watch_watch_mode, _trae_skills_graphify_references_hooks_post_commit_hook, _trae_skills_graphify_references_hooks_agents_md_integration, _trae_skills_graphify_references_update_incremental_update, agents_workspace_graphify_rules [INFERRED 0.75]
- **Ferrite Storage Pipeline** — readme_write_ahead_log, readme_memtable, readme_sstable, readme_size_tiered_compactor, readme_manifest [EXTRACTED 1.00]

## Communities (27 total, 11 thin omitted)

### Community 0 - "Bloom Filter"
Cohesion: 0.11
Nodes (8): BloomFilter, djb2_64(), fnv1a_64(), probe_idx(), Result, Vec, test_probe_idx_in_range(), encode_u64()

### Community 1 - "SSTable Format"
Cohesion: 0.12
Nodes (24): BlockEntries, decode_u32(), decode_u64(), encode_bytes(), encode_u32(), Result, Vec, test_encode_bytes_empty_val() (+16 more)

### Community 2 - "Block Cache"
Cohesion: 0.13
Nodes (13): CacheKey, Drop, HashMap, BlockCache, Node, Option, Path, Vec (+5 more)

### Community 3 - "Engine Core"
Cohesion: 0.16
Nodes (14): Into, JoinHandle, BackgroundFlush, Engine, EngineConfig, EngineStats, parse_sstable_filename(), read_manifest() (+6 more)

### Community 4 - "Write-Ahead Log"
Cohesion: 0.28
Nodes (7): File, Path, PathBuf, Result, Vec, Wal, WalRecord

### Community 5 - "Memtable"
Cohesion: 0.12
Nodes (9): BTreeMap, Default, Memtable, MemValue, Item, Iterator, Option, Self (+1 more)

### Community 6 - "Compaction Logic"
Cohesion: 0.13
Nodes (25): AtomicU64, Eq, Ord, Ordering, PartialEq, PartialOrd, collect_paths(), compact_level() (+17 more)

### Community 7 - "Graphify Docs"
Cohesion: 0.20
Nodes (14): graphify add, Watch Mode, Extraction Subagent Prompt, Cross-repo Graph Merge, Post-commit Hook, Constrained Query Expansion, graphify query CLI, Whisper Transcription (+6 more)

### Community 8 - "CLI And Errors"
Cohesion: 0.20
Nodes (12): Error, Cli, Command, PathBuf, String, FerriteError, String, main() (+4 more)

### Community 9 - "Compaction Tests"
Cohesion: 0.24
Nodes (20): assert_level_non_overlapping(), find_value_in_level(), find_value_in_reader(), make_reader(), make_reader_entries(), Option, Path, Result (+12 more)

### Community 10 - "Cache Tests"
Cohesion: 0.29
Nodes (11): block(), key(), PathBuf, Vec, test_access_pattern_promotes_node(), test_basic_insert_and_get(), test_capacity_eviction_lru_order(), test_drop_releases_all_nodes() (+3 more)

### Community 11 - "Engine Tests"
Cohesion: 0.24
Nodes (16): Result, test_100k_sequential_puts_then_reads(), test_background_flush_completion_rewrites_wal_to_live_memtable_state(), test_background_flush_failure_propagates_to_next_engine_operation(), test_delete_tombstone_before_and_after_flush(), test_overwrite_after_flush_returns_newest(), test_put_get_across_flush_boundary(), test_put_get_basic() (+8 more)

### Community 13 - "Project Overview"
Cohesion: 0.12
Nodes (15): Architecture, Benchmark, Build, CLI, Example session, ferrite, Future work, Licence (+7 more)

### Community 26 - "join_panic_message"
Cohesion: 0.50
Nodes (4): Any, Send, join_panic_message(), String

## Knowledge Gaps
- **22 isolated node(s):** `graphify`, `What it is`, `Why it exists`, `Architecture`, `Build` (+17 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **11 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Engine` connect `Engine Core` to `Block Cache`, `Write-Ahead Log`, `Memtable`, `Compaction Logic`, `CLI And Errors`, `Engine Tests`?**
  _High betweenness centrality (0.228) - this node is a cross-community bridge._
- **Why does `SSTableReader` connect `Compaction Logic` to `Bloom Filter`, `SSTable Format`, `Engine Core`, `Compaction Tests`?**
  _High betweenness centrality (0.175) - this node is a cross-community bridge._
- **Why does `Memtable` connect `Memtable` to `Engine Core`?**
  _High betweenness centrality (0.114) - this node is a cross-community bridge._
- **What connects `graphify`, `What it is`, `Why it exists` to the rest of the system?**
  _22 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Bloom Filter` be split into smaller, more focused modules?**
  _Cohesion score 0.11333333333333333 - nodes in this community are weakly interconnected._
- **Should `SSTable Format` be split into smaller, more focused modules?**
  _Cohesion score 0.11806543385490754 - nodes in this community are weakly interconnected._
- **Should `Block Cache` be split into smaller, more focused modules?**
  _Cohesion score 0.12688172043010754 - nodes in this community are weakly interconnected._