//! # main
//!
//! Binary entry point for Ferrite. Parses CLI arguments and dispatches to the
//! appropriate Engine operation.
//!
//! ## Role in the LSM pipeline
//! main sits above all other modules — it is the only executable entry point.
//! It constructs an Engine and delegates each subcommand to it. All seven
//! subcommands (`put`, `get`, `delete`, `scan`, `compact`, `stats`, `bench`)
//! are fully wired and produce clean, human-readable output.
//!
//! ## Dependencies
//! - `ferrite` (the library crate) — exposes `cli`, `engine`, and `error`; main
//!   imports from it rather than declaring sibling modules so integration tests
//!   can also access the same library.
//! - `std::time::Instant` — wall-clock timing for the `bench` command.
//!
//! ## Used by
//! - The OS — `main` is the process entry point invoked by the shell.

use std::time::Instant;

use clap::Parser;
use ferrite::cli::{Cli, Command};
use ferrite::engine::{Engine, EngineConfig, EngineStats};
use ferrite::error;

/// Parses CLI arguments, opens the Engine, and dispatches to the
/// appropriate storage operation.
///
/// # Errors
/// Returns `FerriteError::Io` or `FerriteError::Corruption` if any storage
/// operation fails.
fn main() -> error::Result<()> {
    let cli = Cli::parse();
    let config = EngineConfig::new(cli.data_dir);
    let mut engine = Engine::open(config)?;

    match cli.command {
        Command::Put { key, value } => {
            engine.put(key.as_bytes(), value.as_bytes())?;
            println!("OK");
        }
        Command::Get { key } => match engine.get(key.as_bytes())? {
            Some(v) => println!("{}", String::from_utf8_lossy(&v)),
            None => println!("NOT FOUND"),
        },
        Command::Delete { key } => {
            engine.delete(key.as_bytes())?;
            println!("DELETED");
        }
        Command::Scan { prefix } => {
            for (k, v) in engine.scan_prefix(prefix.as_bytes())? {
                println!(
                    "{} | {}",
                    String::from_utf8_lossy(&k),
                    String::from_utf8_lossy(&v),
                );
            }
        }
        Command::Compact => {
            let merged = engine.compact()?;
            println!("Compaction complete. {merged} files merged.");
        }
        Command::Stats => print_stats(&engine.stats()),
        Command::Bench { count } => run_bench(&mut engine, count)?,
    }

    Ok(())
}

/// Prints the engine stats snapshot to stdout in a human-readable format.
///
/// Lines cover: active versus frozen Memtable footprint, background flush
/// state, per-level SSTable file counts, and the block cache hit/miss ratio.
/// If no SSTables exist yet a single placeholder line is emitted so the
/// output is never empty.
fn print_stats(stats: &EngineStats) {
    let active_memtable_size_bytes = stats
        .memtable_size_bytes
        .saturating_sub(stats.frozen_memtable_size_bytes);

    println!("Active memtable: {} bytes", active_memtable_size_bytes);
    println!(
        "Frozen memtable: {} bytes",
        stats.frozen_memtable_size_bytes
    );
    println!("Total memtable:  {} bytes", stats.memtable_size_bytes);
    println!(
        "Flush pending:   {}",
        if stats.has_pending_flush { "yes" } else { "no" }
    );
    println!(
        "Flush in flight: {}",
        if stats.flush_in_flight { "yes" } else { "no" }
    );
    if stats.sstable_count_per_level.is_empty() {
        println!("Levels:         (no SSTables on disk)");
    } else {
        for (i, count) in stats.sstable_count_per_level.iter().enumerate() {
            let noun = if *count == 1 { "SSTable" } else { "SSTables" };
            let layout = if i == 0 {
                "overlapping flush output"
            } else {
                "non-overlapping"
            };
            println!("Level {i}:        {count} {noun} ({layout})");
        }
    }
    let total = stats.cache_hit_count + stats.cache_miss_count;
    let ratio = if total == 0 {
        0.0f64
    } else {
        stats.cache_hit_count as f64 / total as f64 * 100.0
    };
    println!(
        "Cache hits:     {} / {} ({:.2}%)",
        stats.cache_hit_count, total, ratio,
    );
}

/// xorshift64 PRNG — single-pass, no allocations, approximately 1 ns per call.
///
/// Used only by `run_bench` to drive the Fisher–Yates shuffle so the read
/// phase exercises arbitrary-order lookups rather than the sequential pattern
/// that would maximise OS read-ahead and bias throughput numbers.
fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// Runs a sequential write + random-order read benchmark and prints throughput.
///
/// Write phase: inserts `count` keys of the form `bench_key_{i}` →
/// `bench_val_{i}`. Read phase: reads all keys back in a Fisher–Yates-shuffled
/// order so the access pattern is not sequential. Reports write ops/sec, read
/// ops/sec, and total elapsed time.
///
/// # Errors
/// Returns `FerriteError::Io` or `FerriteError::Corruption` if any Engine
/// call fails during the benchmark.
fn run_bench(engine: &mut Engine, count: u64) -> error::Result<()> {
    let total_start = Instant::now();

    // Write phase.
    println!("Writing {count} keys...");
    let write_start = Instant::now();
    for i in 0..count {
        let key = format!("bench_key_{i}");
        let val = format!("bench_val_{i}");
        engine.put(key.as_bytes(), val.as_bytes())?;
    }
    let write_elapsed = write_start.elapsed();
    let write_ops = (count as f64 / write_elapsed.as_secs_f64()) as u64;
    println!(
        "Write phase: {count} ops in {:.3}s ({write_ops} ops/sec)",
        write_elapsed.as_secs_f64(),
    );

    // Build a shuffled read order via Fisher–Yates + xorshift64.
    // Fixed seed for reproducibility across invocations.
    let mut order: Vec<u64> = (0..count).collect();
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    for i in (1..count as usize).rev() {
        // Modulo bias is negligible at this scale (u64 range >> i+1).
        let j = (xorshift64(&mut state) % (i as u64 + 1)) as usize;
        order.swap(i, j);
    }

    // Read phase.
    println!("Reading {count} keys in random order...");
    let read_start = Instant::now();
    for i in &order {
        let key = format!("bench_key_{i}");
        let result = engine.get(key.as_bytes())?;
        // debug_assert so this check is compiled out in --release builds,
        // keeping reported ops/sec honest for the acceptance benchmark.
        debug_assert_eq!(
            result.as_deref(),
            Some(format!("bench_val_{i}").as_bytes()),
            "bench key {key} returned wrong value",
        );
    }
    let read_elapsed = read_start.elapsed();
    let read_ops = (count as f64 / read_elapsed.as_secs_f64()) as u64;
    println!(
        "Read phase:  {count} ops in {:.3}s ({read_ops} ops/sec)",
        read_elapsed.as_secs_f64(),
    );

    println!("Total time: {:.3}s", total_start.elapsed().as_secs_f64());
    Ok(())
}
