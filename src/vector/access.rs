//! Trait for zero-copy read access to vectors.
//!
//! Decouples HNSW and query execution from any specific storage backend.
//! Implemented by both in-memory `HashMap<u64, Vec<f32>>` (ephemeral DB)
//! and the disk-backed mmap vector store (persistent DB).

use std::collections::HashMap;

/// Zero-copy read access to vectors for a single field.
///
/// Every function in [`HnswGraph`](super::HnswGraph) that needs to read
/// vectors is generic over this trait, allowing the same graph code to
/// work with in-memory HashMaps, mmap-backed files, or any future backend.
pub trait VectorAccess {
    /// Return the vector for `id`, or `None` if absent.
    ///
    /// For mmap-backed stores, the returned slice points directly into the
    /// memory-mapped region (zero-copy).
    fn get(&self, id: u64) -> Option<&[f32]>;

    /// Number of vectors stored.
    fn len(&self) -> usize;

    /// Returns `true` if no vectors are stored.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl VectorAccess for HashMap<u64, Vec<f32>> {
    #[inline]
    fn get(&self, id: u64) -> Option<&[f32]> {
        HashMap::get(self, &id).map(|v| v.as_slice())
    }

    #[inline]
    fn len(&self) -> usize {
        HashMap::len(self)
    }
}
