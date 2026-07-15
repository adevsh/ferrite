//! # cli
//!
//! Command-line interface definition for the Ferrite binary.
//!
//! ## Role in the LSM pipeline
//! cli is the user-facing entry point, sitting above the Engine. It translates
//! shell arguments into typed Rust values; the Engine performs the actual
//! storage operations. All seven subcommands are fully wired.
//!
//! ## Dependencies
//! - `clap` — provides the `Parser` and `Subcommand` derive macros that generate
//!   the argument parser and help text from struct and enum annotations.
//!
//! ## Used by
//! - `main` — calls `Cli::parse()` and matches on `Cli::command` to dispatch.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Top-level CLI arguments for the Ferrite embedded key-value store.
#[derive(Debug, Parser)]
#[command(
    name = "ferrite",
    about = "A production-grade LSM-tree key-value store"
)]
pub struct Cli {
    /// Directory used to store WAL, SSTable, and MANIFEST files.
    #[arg(long, default_value = "./data")]
    pub data_dir: PathBuf,

    /// Operation to perform against the store.
    #[command(subcommand)]
    pub command: Command,
}

/// All operations the Ferrite CLI supports.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Insert or overwrite a key–value pair.
    Put {
        /// The key to write.
        key: String,
        /// The value to associate with the key.
        value: String,
    },
    /// Retrieve the value for a key, printing it to stdout.
    Get {
        /// The key to look up.
        key: String,
    },
    /// Delete a key by writing a tombstone record.
    Delete {
        /// The key to delete.
        key: String,
    },
    /// Print all keys that begin with the given prefix.
    Scan {
        /// The prefix to match against.
        prefix: String,
    },
    /// Trigger a manual compaction of all levels.
    Compact,
    /// Print memtable size, SSTable file counts per level, and cache hit ratio.
    Stats,
    /// Run a sequential write + read benchmark and report throughput.
    Bench {
        /// Number of key–value pairs to write and then read back.
        count: u64,
    },
}
