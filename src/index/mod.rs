//! Multi-modal indexing
//!
//! Provides indexing structures for:
//! - Slug-based lookups (O(log N) with redb B+Tree)
//! - Spatial queries (O(log N) with rstar R-tree)
//! - Fulltext search (O(1) with Tantivy inverted index)
//! - Vector search (O(log N) with HNSW)
//! - Async index building (background thread for non-blocking ingestion)

#[cfg(feature = "spatial")]
pub mod spatial;
#[cfg(feature = "spatial")]
pub use spatial::SpatialIndex;

#[cfg(feature = "fulltext")]
pub mod fulltext;
#[cfg(feature = "fulltext")]
pub use fulltext::{FulltextConfig, FulltextIndex, FulltextResult, FulltextStats};

pub mod async_indexer;
pub use async_indexer::{AsyncIndexer, IndexJob, IndexerStats};

use crate::{NodeId, SlugHash};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::path::Path;

/// Define the slugs table
const SLUGS_TABLE: TableDefinition<SlugHash, NodeId> = TableDefinition::new("slugs");

/// Slug-based index using redb B+Tree for persistent storage
///
/// Provides O(log N) lookups with durability. Uses redb's
/// embedded B+Tree storage for efficient slug hash to node ID mapping.
pub struct SlugIndex {
    db: Database,
    path: std::path::PathBuf,
}

impl SlugIndex {
    /// Create a new or open existing slug index
    pub fn new(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let path_buf = path.to_path_buf();

        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Open or create the redb database
        let db = Database::create(path)?;

        Ok(Self { db, path: path_buf })
    }

    /// Insert a slug hash -> node ID mapping
    pub fn insert(&self, slug_hash: SlugHash, node_id: NodeId) {
        let write = self.db.begin_write().expect("Failed to begin write");
        {
            let mut table = write.open_table(SLUGS_TABLE).expect("Failed to open table");
            table
                .insert(&slug_hash, &node_id)
                .expect("Failed to insert");
        }
        write.commit().expect("Failed to commit");
    }

    /// Get node ID by slug hash
    pub fn get(&self, slug_hash: SlugHash) -> Option<NodeId> {
        let read = self.db.begin_read().expect("Failed to begin read");
        let table = read.open_table(SLUGS_TABLE).expect("Failed to open table");
        table.get(&slug_hash).ok().flatten().map(|v| v.value())
    }

    /// Check if slug hash exists
    pub fn contains(&self, slug_hash: SlugHash) -> bool {
        let read = self.db.begin_read().expect("Failed to begin read");
        let table = read.open_table(SLUGS_TABLE).expect("Failed to open table");
        match table.get(&slug_hash) {
            Ok(Some(_)) => true,
            _ => false,
        }
    }

    /// Remove a slug hash -> node ID mapping
    pub fn remove(&self, slug_hash: SlugHash) -> Option<NodeId> {
        let write = self.db.begin_write().expect("Failed to begin write");
        let result = {
            let mut table = write.open_table(SLUGS_TABLE).expect("Failed to open table");
            table.remove(&slug_hash).ok().flatten().map(|v| v.value())
        };
        write.commit().expect("Failed to commit");
        result
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        let read = self.db.begin_read().expect("Failed to begin read");
        match read.open_table(SLUGS_TABLE) {
            Ok(table) => table.len().expect("Failed to get len") as usize,
            Err(_) => 0, // Table doesn't exist yet
        }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        let read = self.db.begin_read().expect("Failed to begin read");
        match read.open_table(SLUGS_TABLE) {
            Ok(table) => table.is_empty().expect("Failed to check is_empty"),
            Err(_) => true, // Table doesn't exist yet, so it's empty
        }
    }

    /// Clear all entries
    pub fn clear(&self) {
        let write = self.db.begin_write().expect("Failed to begin write");
        {
            let table = write.open_table(SLUGS_TABLE).expect("Failed to open table");
            // Iterate and delete all entries (redb doesn't have clear())
            // Note: iter() returns Result<Range, StorageError>, need to handle it
            let range = table.iter().expect("Failed to get iter");
            let _keys: Vec<SlugHash> = range
                .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                .collect();
            // Need to reopen table for modification or use a different approach
            // Since we can't modify while iterating, collect keys first
        }
        // Reopen and delete
        {
            let mut table = write.open_table(SLUGS_TABLE).expect("Failed to open table");
            // Get keys again (table is still valid after range goes out of scope)
            let keys: Vec<SlugHash> = table
                .iter()
                .expect("Failed to get iter")
                .filter_map(|r| r.ok().map(|(k, _)| k.value()))
                .collect();
            for key in keys {
                table.remove(&key).ok();
            }
        }
        write.commit().expect("Failed to commit");
    }

    /// Iterate over all entries
    pub fn iter(&self) -> Vec<(SlugHash, NodeId)> {
        let read = self.db.begin_read().expect("Failed to begin read");
        let table = read.open_table(SLUGS_TABLE).expect("Failed to open table");
        let range = table.iter().expect("Failed to get iter");
        range
            .filter_map(|result| match result {
                Ok((k, v)) => Some((k.value(), v.value())),
                Err(_) => None,
            })
            .collect()
    }

    /// Get database path
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_slug_index_insert_get() {
        let temp_dir = TempDir::new().unwrap();
        let index = SlugIndex::new(&temp_dir.path().join("slugs.redb")).unwrap();

        index.insert(12345, 1);
        index.insert(67890, 2);

        assert_eq!(index.get(12345), Some(1));
        assert_eq!(index.get(67890), Some(2));
        assert_eq!(index.get(99999), None);
    }

    #[test]
    fn test_slug_index_contains() {
        let temp_dir = TempDir::new().unwrap();
        let index = SlugIndex::new(&temp_dir.path().join("slugs.redb")).unwrap();

        index.insert(12345, 1);

        assert!(index.contains(12345));
        assert!(!index.contains(99999));
    }

    #[test]
    fn test_slug_index_remove() {
        let temp_dir = TempDir::new().unwrap();
        let index = SlugIndex::new(&temp_dir.path().join("slugs.redb")).unwrap();

        index.insert(12345, 1);
        assert_eq!(index.get(12345), Some(1));

        index.remove(12345);
        assert_eq!(index.get(12345), None);
    }

    #[test]
    fn test_slug_index_len() {
        let temp_dir = TempDir::new().unwrap();
        let index = SlugIndex::new(&temp_dir.path().join("slugs.redb")).unwrap();

        assert!(index.is_empty());

        index.insert(1, 1);
        index.insert(2, 2);
        index.insert(3, 3);

        assert_eq!(index.len(), 3);
    }

    #[test]
    fn test_slug_index_clear() {
        let temp_dir = TempDir::new().unwrap();
        let index = SlugIndex::new(&temp_dir.path().join("slugs.redb")).unwrap();

        index.insert(1, 1);
        index.insert(2, 2);

        index.clear();

        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_slug_index_iter() {
        let temp_dir = TempDir::new().unwrap();
        let index = SlugIndex::new(&temp_dir.path().join("slugs.redb")).unwrap();

        index.insert(100, 1);
        index.insert(200, 2);
        index.insert(300, 3);

        let entries = index.iter();
        assert_eq!(entries.len(), 3);
    }
}
