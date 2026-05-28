//! # ferrite
//!
//! Library facade for the Ferrite embedded key-value store.
//!
//! ## Role in the LSM pipeline
//! This is the library crate root. It exposes every storage engine module so
//! the binary (`src/main.rs`) and integration tests (`tests/*.rs`) can import
//! them via `ferrite::<module>` rather than including source files directly.
//! The library carries no executable logic of its own.
//!
//! ## Dependencies
//! None beyond the declared child modules; each module manages its own imports.
//!
//! ## Used by
//! - `main` (binary) — imports `cli` and `error` to drive the CLI.
//! - `tests/wal_test.rs` and later test files — import individual modules to
//!   exercise the storage engine without going through the CLI.

pub mod bloom;
pub mod cache;
pub mod cli;
pub mod codec;
pub mod compaction;
pub mod engine;
pub mod error;
pub mod memtable;
pub mod sstable;
pub mod wal;
