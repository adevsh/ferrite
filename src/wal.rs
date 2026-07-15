//! # wal
//!
//! Write-Ahead Log: an append-only file that records every put and delete
//! before the Memtable is mutated, ensuring writes survive a crash.
//!
//! ## Role in the LSM pipeline
//! WAL is the first stage of the write path. The Engine calls `append_put` or
//! `append_delete` and waits for fsync before mutating the Memtable. On
//! startup, `recover` replays all valid records into a fresh Memtable. After a
//! successful Memtable→SSTable flush, the Engine calls `truncate` to reset the
//! log.
//!
//! ## Dependencies
//! - `codec` — `encode_u32`, `decode_u32`, and `encode_bytes` serialise the
//!   record header and key/value payload.
//! - `crc32fast` — computes the 4-byte checksum that guards each record.
//! - `error` — all public methods return `Result<T>`.
//!
//! ## Used by
//! - `engine` — calls `append_put`/`append_delete` on every write,
//!   `recover` on startup, and `truncate` after every Memtable flush.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::codec::{decode_u32, encode_bytes, encode_u32};
use crate::error::{FerriteError, Result};

/// Magic byte identifying a Put record in the WAL.
const TYPE_PUT: u8 = 0x01;

/// Magic byte identifying a Delete (tombstone) record in the WAL.
const TYPE_DELETE: u8 = 0x02;

/// Filename of the WAL file within the data directory.
const WAL_FILENAME: &str = "wal.log";

/// A single logical operation recovered from the WAL during startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    /// A key was inserted or overwritten with a value.
    Put {
        /// The key bytes.
        key: Vec<u8>,
        /// The value bytes associated with the key.
        value: Vec<u8>,
    },
    /// A key was deleted; no value bytes are stored for tombstone records.
    Delete {
        /// The key that was deleted.
        key: Vec<u8>,
    },
}

/// Append-only, fsync-on-every-write durability log.
///
/// Every put and delete is serialised, appended, and fsynced before the
/// corresponding Memtable mutation occurs, guaranteeing the write survives
/// a process crash.
pub struct Wal {
    /// Open file handle in append mode for writing new records.
    file: File,
    /// Absolute path to `wal.log`; retained so `truncate` can call `set_len`.
    path: PathBuf,
}

impl Wal {
    /// Opens (or creates) `<dir>/wal.log` in append mode.
    ///
    /// Creates the data directory if it does not yet exist so callers do not
    /// need to create it beforehand.
    ///
    /// # Errors
    /// Returns `FerriteError::Io` if the directory or file cannot be created
    /// or opened.
    pub fn open(dir: &Path) -> Result<Wal> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(WAL_FILENAME);
        let file = OpenOptions::new().append(true).create(true).open(&path)?;
        Ok(Wal { file, path })
    }

    /// Appends a Put record to the WAL and fsyncs to disk before returning.
    ///
    /// Must be called before inserting the key into the Memtable so that a
    /// crash between the two operations always recovers the write on restart.
    ///
    /// # Errors
    /// Returns `FerriteError::Io` if the write or fsync fails.
    pub fn append_put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.append_record(TYPE_PUT, key, value)
    }

    /// Appends a Delete (tombstone) record to the WAL and fsyncs before returning.
    ///
    /// No value bytes are stored; `val_len` is always 0 in delete records.
    ///
    /// # Errors
    /// Returns `FerriteError::Io` if the write or fsync fails.
    pub fn append_delete(&mut self, key: &[u8]) -> Result<()> {
        self.append_record(TYPE_DELETE, key, &[])
    }

    /// Truncates the WAL to zero bytes; called after a successful Memtable flush.
    ///
    /// Opens `self.path` in create/truncate mode, fsyncs the zero-byte state,
    /// then reopens in append mode and replaces `self.file`. Using a fresh
    /// truncate handle (rather than `set_len` on the append handle) ensures
    /// the kernel flushes metadata (including the new file length) before we
    /// return.
    ///
    /// # Errors
    /// Returns `FerriteError::Io` if any file operation or fsync fails.
    pub fn truncate(&mut self) -> Result<()> {
        self.rewrite(Vec::new())
    }

    /// Replaces the WAL contents with `records`, atomically.
    ///
    /// Writes a full replacement log to `wal.log.tmp`, fsyncs it, renames it
    /// over `wal.log`, then reopens the append handle. This lets the Engine
    /// drop flushed records while retaining any still-live Memtable state
    /// without exposing a partially rewritten WAL after a failed rewrite.
    ///
    /// # Errors
    /// Returns `FerriteError::Io` if any file operation, write, or fsync fails.
    pub fn rewrite(&mut self, records: Vec<WalRecord>) -> Result<()> {
        let tmp_path = self.path.with_extension("log.tmp");
        {
            let mut tmp = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;

            for record in records {
                match record {
                    WalRecord::Put { key, value } => {
                        tmp.write_all(&Self::encode_record(TYPE_PUT, &key, &value))?;
                    }
                    WalRecord::Delete { key } => {
                        tmp.write_all(&Self::encode_record(TYPE_DELETE, &key, &[]))?;
                    }
                }
            }

            tmp.sync_all()?;
        }

        std::fs::rename(&tmp_path, &self.path)?;
        self.file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)?;
        Ok(())
    }

    /// Reads `<dir>/wal.log` and returns every record that passes CRC validation.
    ///
    /// Reading stops at the first malformed or CRC-failing record. The assumption
    /// is that any inconsistency at the tail is a half-written record from a crash
    /// mid-fsync; all records before that point are trustworthy and are returned.
    ///
    /// A missing `wal.log` is treated as an empty log — a fresh engine has no
    /// records yet.
    ///
    /// # Errors
    /// Returns `FerriteError::Io` if the file cannot be read (other than
    /// `NotFound`, which is treated as empty).
    pub fn recover(dir: &Path) -> Result<Vec<WalRecord>> {
        let path = dir.join(WAL_FILENAME);

        let mut file = match OpenOptions::new().read(true).open(&path) {
            Ok(f) => f,
            // A missing WAL is not an error — the engine simply has no prior state.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(FerriteError::Io(e)),
        };

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut records = Vec::new();
        let mut pos = 0;

        while pos < buf.len() {
            // Each record needs at least 13 bytes of header before variable payload.
            // Fewer bytes means a partial trailing record — stop reading.
            let remaining = buf.len() - pos;
            if remaining < 13 {
                break;
            }

            // These unwraps are safe: we already verified remaining >= 13.
            let stored_crc = decode_u32(&buf[pos..pos + 4]).expect("4 bytes available");
            let record_type = buf[pos + 4];
            let key_len = decode_u32(&buf[pos + 5..pos + 9]).expect("4 bytes available") as usize;
            let val_len = decode_u32(&buf[pos + 9..pos + 13]).expect("4 bytes available") as usize;

            // Full record: 4 (CRC) + 1 (type) + 4 (key_len) + 4 (val_len) + key + value.
            let record_len = 13 + key_len + val_len;
            if remaining < record_len {
                // Partial trailing record — crash happened mid-write; stop here.
                break;
            }

            // CRC covers the body: type byte through end of value bytes.
            let body = &buf[pos + 4..pos + record_len];
            let computed_crc = crc32fast::hash(body);
            if stored_crc != computed_crc {
                // CRC mismatch — bytes after this point cannot be trusted; stop reading.
                break;
            }

            let key_start = pos + 13;
            let val_start = key_start + key_len;
            let key = buf[key_start..val_start].to_vec();

            let record = match record_type {
                TYPE_PUT => WalRecord::Put {
                    key,
                    value: buf[val_start..val_start + val_len].to_vec(),
                },
                TYPE_DELETE => WalRecord::Delete { key },
                // Unknown type byte — stop conservatively rather than skipping,
                // since we cannot know the record length to jump over it.
                _ => break,
            };

            records.push(record);
            pos += record_len;
        }

        Ok(records)
    }

    /// Builds, writes, and fsyncs a single WAL record of the given type.
    ///
    /// Record layout on disk: `[ CRC32 (4) | type (1) | key_len (4) | val_len (4) | key | value ]`.
    /// The CRC32 covers everything after itself (type byte through end of value).
    ///
    /// # Errors
    /// Returns `FerriteError::Io` if `write_all` or `sync_data` fails.
    fn append_record(&mut self, record_type: u8, key: &[u8], value: &[u8]) -> Result<()> {
        let record = Self::encode_record(record_type, key, value);
        self.file.write_all(&record)?;
        // sync_data flushes file contents without waiting for directory metadata;
        // safe here because the WAL only ever appends — we never rename or relink.
        self.file.sync_data()?;
        Ok(())
    }

    /// Encodes one WAL record into its on-disk byte layout.
    fn encode_record(record_type: u8, key: &[u8], value: &[u8]) -> Vec<u8> {
        // Body = type byte followed by the length-prefixed key/value payload.
        let mut body = Vec::with_capacity(1 + 8 + key.len() + value.len());
        body.push(record_type);
        body.extend_from_slice(&encode_bytes(key, value));

        let crc = crc32fast::hash(&body);

        let mut record = Vec::with_capacity(4 + body.len());
        record.extend_from_slice(&encode_u32(crc));
        record.extend_from_slice(&body);
        record
    }
}
