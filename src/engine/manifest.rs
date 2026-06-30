//! Database manifest — tracks segment files on remote storage.
//!
//! The manifest records which files make up a consistent database snapshot.
//! After every `compact()`, a new manifest is written with updated segment
//! checksums. On `open()`, the manifest tells us which files to fetch.
//!
//! Gated behind `#[cfg(feature = "s3")]`.

use serde::{Deserialize, Serialize};

/// Describes a consistent set of database segment files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub generation: u64,
    pub segments: Vec<Segment>,
    pub created_unix: u64,
}

/// A single file in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Segment {
    pub name: String,
    pub size: u64,
    pub crc32: u32,
}

impl Manifest {
    pub fn new(generation: u64, segments: Vec<Segment>) -> Self {
        Self {
            version: 1,
            generation,
            segments,
            created_unix: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}
