# Graph Report - /Users/adev/playground/ferrite  (2026-07-15)

## Corpus Check
- Corpus is ~34,526 words - fits in a single context window. You may not need a graph.

## Summary
- 275 nodes · 522 edges · 16 communities (15 shown, 1 thin omitted)
- Extraction: 96% EXTRACTED · 4% INFERRED · 0% AMBIGUOUS · INFERRED: 19 edges (avg confidence: 0.81)
- Token cost: 0 input · 0 output

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

## God Nodes (most connected - your core abstractions)
1. `BlockCache` - 27 edges
2. `SSTableReader` - 22 edges
3. `Engine` - 21 edges
4. `Memtable` - 18 edges
5. `BloomFilter` - 12 edges
6. `Wal` - 12 edges
7. `HeapEntry` - 10 edges
8. `compact_level()` - 9 edges
9. `read_exact_at()` - 9 edges
10. `parse_block_entries()` - 9 edges

## Surprising Connections (you probably didn't know these)
- `make_reader()` --references--> `SSTableReader`  [EXTRACTED]
  tests/compaction_test.rs → src/sstable.rs
- `Post-commit Hook` --semantically_similar_to--> `Watch Mode`  [INFERRED] [semantically similar]
  .trae/skills/graphify/references/hooks.md → .trae/skills/graphify/references/add-watch.md
- `Workspace Graphify Rules` --references--> `Wiki Export`  [EXTRACTED]
  AGENTS.md → .trae/skills/graphify/references/exports.md
- `AGENTS.md Integration` --references--> `Workspace Graphify Rules`  [EXTRACTED]
  .trae/skills/graphify/references/hooks.md → AGENTS.md
- `Workspace Graphify Rules` --references--> `graphify query CLI`  [EXTRACTED]
  AGENTS.md → .trae/skills/graphify/references/query.md

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Graphify Extraction Flow** — _trae_skills_graphify_skill_structural_extraction, _trae_skills_graphify_skill_semantic_extraction, _trae_skills_graphify_references_extraction_spec_extraction_subagent_prompt, _trae_skills_graphify_references_transcribe_whisper_transcription, _trae_skills_graphify_references_update_incremental_update [INFERRED 0.85]
- **Graph Freshness Mechanisms** — _trae_skills_graphify_references_add_watch_watch_mode, _trae_skills_graphify_references_hooks_post_commit_hook, _trae_skills_graphify_references_hooks_agents_md_integration, _trae_skills_graphify_references_update_incremental_update, agents_workspace_graphify_rules [INFERRED 0.75]
- **Ferrite Storage Pipeline** — readme_write_ahead_log, readme_memtable, readme_sstable, readme_size_tiered_compactor, readme_manifest [EXTRACTED 1.00]

## Communities (16 total, 1 thin omitted)

### Community 0 - "Bloom Filter"
Cohesion: 0.08
Nodes (14): BloomFilter, djb2_64(), fnv1a_64(), probe_idx(), Result, Vec, test_probe_idx_in_range(), decode_u64() (+6 more)

### Community 1 - "SSTable Format"
Cohesion: 0.16
Nodes (20): AtomicU64, BlockEntries, decode_u32(), encode_u32(), parse_block_entries(), read_exact_at(), File, Item (+12 more)

### Community 2 - "Block Cache"
Cohesion: 0.13
Nodes (13): CacheKey, Drop, HashMap, BlockCache, Node, Option, Path, Vec (+5 more)

### Community 3 - "Engine Core"
Cohesion: 0.20
Nodes (12): Into, Engine, EngineConfig, EngineStats, parse_sstable_filename(), read_manifest(), Option, Path (+4 more)

### Community 4 - "Write-Ahead Log"
Cohesion: 0.15
Nodes (7): File, Path, PathBuf, Result, Vec, Wal, WalRecord

### Community 5 - "Memtable"
Cohesion: 0.19
Nodes (9): BTreeMap, Default, Memtable, MemValue, Item, Iterator, Option, Self (+1 more)

### Community 6 - "Compaction Logic"
Cohesion: 0.20
Nodes (14): Eq, Ord, Ordering, PartialEq, PartialOrd, compact_level(), Compactor, HeapEntry (+6 more)

### Community 7 - "Graphify Docs"
Cohesion: 0.16
Nodes (17): graphify add, Watch Mode, Wiki Export, Extraction Subagent Prompt, Cross-repo Graph Merge, AGENTS.md Integration, Post-commit Hook, Constrained Query Expansion (+9 more)

### Community 8 - "CLI And Errors"
Cohesion: 0.20
Nodes (12): Error, Cli, Command, PathBuf, String, FerriteError, String, main() (+4 more)

### Community 9 - "Compaction Tests"
Cohesion: 0.26
Nodes (13): make_reader(), Path, Result, test_cache_invalidated_for_old_sstables(), test_cascade_l0_to_l1_to_l2(), test_compact_drops_tombstone_at_bottom_level(), test_compact_keeps_tombstone_when_lower_levels_exist(), test_compact_l0_to_l1_dedupes_and_clears_l0() (+5 more)

### Community 10 - "Cache Tests"
Cohesion: 0.29
Nodes (11): block(), key(), PathBuf, Vec, test_access_pattern_promotes_node(), test_basic_insert_and_get(), test_capacity_eviction_lru_order(), test_drop_releases_all_nodes() (+3 more)

### Community 11 - "Engine Tests"
Cohesion: 0.32
Nodes (11): Result, test_100k_sequential_puts_then_reads(), test_delete_tombstone_before_and_after_flush(), test_overwrite_after_flush_returns_newest(), test_put_get_across_flush_boundary(), test_put_get_basic(), test_restart_engine_after_flush_reads_from_sstable(), test_restart_engine_recovers_memtable_from_wal() (+3 more)

### Community 13 - "Project Overview"
Cohesion: 0.22
Nodes (9): Ferrite, LRU Block Cache, LSM-tree Storage Engine, MANIFEST, Memtable, Size-tiered Compactor, SSTable, WAL Group Commit (+1 more)

## Knowledge Gaps
- **6 isolated node(s):** `graphify add`, `Wiki Export`, `MCP Server Export`, `Extraction Subagent Prompt`, `LRU Block Cache` (+1 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **1 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Engine` connect `Engine Core` to `SSTable Format`, `Block Cache`, `Write-Ahead Log`, `Memtable`, `CLI And Errors`, `Compaction Tests`, `Engine Tests`?**
  _High betweenness centrality (0.260) - this node is a cross-community bridge._
- **Why does `BlockCache` connect `Block Cache` to `SSTable Format`, `Engine Core`, `Compaction Logic`?**
  _High betweenness centrality (0.156) - this node is a cross-community bridge._
- **Why does `SSTableReader` connect `SSTable Format` to `Bloom Filter`, `Compaction Tests`, `Engine Core`, `Compaction Logic`?**
  _High betweenness centrality (0.146) - this node is a cross-community bridge._
- **What connects `graphify add`, `Wiki Export`, `MCP Server Export` to the rest of the system?**
  _6 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Bloom Filter` be split into smaller, more focused modules?**
  _Cohesion score 0.07777777777777778 - nodes in this community are weakly interconnected._
- **Should `Block Cache` be split into smaller, more focused modules?**
  _Cohesion score 0.12688172043010754 - nodes in this community are weakly interconnected._
- **Should `Write-Ahead Log` be split into smaller, more focused modules?**
  _Cohesion score 0.14761904761904762 - nodes in this community are weakly interconnected._