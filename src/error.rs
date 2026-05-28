//! # error
//!
//! Defines the single error type and `Result` alias used throughout Ferrite.
//!
//! ## Role in the LSM pipeline
//! This module is not part of the pipeline itself — it underpins every other
//! module. All public functions in WAL, Memtable, SSTable, codec, Engine, and
//! Compactor return `error::Result<T>` so callers share a unified error surface.
//!
//! ## Dependencies
//! - `thiserror` — provides the `#[derive(Error)]` macro that generates `Display`
//!   and `From` implementations automatically.
//!
//! ## Used by
//! - Every module in this crate — all `Result<T>` types resolve to `FerriteError`.

use thiserror::Error;

/// All error conditions that Ferrite can encounter.
#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum FerriteError {
    /// An OS-level I/O failure (file read, write, fsync, etc.).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// On-disk data failed an integrity check (e.g. CRC32 mismatch in WAL or SSTable).
    #[error("corruption: {0}")]
    Corruption(String),

    /// A `get` operation found no entry for the requested key in any layer.
    #[error("key not found")]
    KeyNotFound,

    /// A byte buffer was shorter than expected or contained an unrecognised tag byte.
    #[error("invalid format: {0}")]
    InvalidFormat(String),
}

/// Crate-wide `Result` alias — avoids repeating the error type in every signature.
pub type Result<T> = std::result::Result<T, FerriteError>;
