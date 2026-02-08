//! MVCC Storage Layer - Immutable Version Control with Tombstones
//!
//! This module provides MVCC (Multi-Version Concurrency Control) storage using DashMap
//! for concurrent access. Implements immutable storage with tombstone-based deletions
//! to eliminate double-delete complexity and enable 2TB+ scaling.

use crate::types::HeadPointer;
use crate::{NodeHeader, NodeId, SlugHash};
use dashmap::DashMap;
use std::path::PathBuf;

/// Composite key for node store (node_id + rev)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NodeKey {
    node_id: NodeId,
    rev: u64,
}

impl NodeKey {
    fn new(node_id: NodeId, rev: u64) -> Self {
        Self { node_id, rev }
    }
}

/// MVCC storage layer using DashMap for concurrent access
///
/// Implements immutable storage with tombstone-based deletions:
/// - Never modifies nodes in-place
/// - Updates create new versions with incremented rev
/// - Deletes create tombstone records
/// - Head pointers track current revision
#[derive(Clone)]
pub struct SingleStorage {
    /// Head index: slug_hash -> HeadPointer (current revision)
    head_index: DashMap<SlugHash, HeadPointer>,

    /// Node store: (node_id, rev) -> NodeHeader (all versions)
    node_store: DashMap<NodeKey, NodeHeader>,

    /// Base directory for persistent storage (future use)
    _base_dir: PathBuf,
}

impl SingleStorage {
    /// Create a new MVCC storage instance
    pub fn new(base_dir: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        Ok(Self {
            head_index: DashMap::new(),
            node_store: DashMap::new(),
            _base_dir: base_dir.as_ref().to_path_buf(),
        })
    }

    /// Insert or update a node (creates new revision)
    ///
    /// # Arguments
    /// * `node` - New version of node to insert/update
    ///
    /// # Returns
    /// * `Option<NodeHeader>` - Previous version if exists
    pub fn upsert(&self, node: NodeHeader) -> Option<NodeHeader> {
        // Store new version (keep all versions for historical queries)
        let key = NodeKey::new(node.node_id, node.rev);
        self.node_store.insert(key, node.clone());

        // Update head pointer to new revision
        let head = HeadPointer::new(node.node_id, node.rev);
        self.head_index.insert(node.slug_hash, head);

        // Return previous version if it exists
        if node.rev > 0 {
            let prev_key = NodeKey::new(node.node_id, node.rev - 1);
            self.node_store.get(&prev_key).map(|n| n.value().clone())
        } else {
            None
        }
    }

    /// Get current version of node by slug hash
    ///
    /// # Arguments
    /// * `slug_hash` - Slug hash to retrieve
    ///
    /// # Returns
    /// * `Option<NodeHeader>` - Current node if found and not deleted
    pub fn get_by_slug(&self, slug_hash: SlugHash) -> Option<NodeHeader> {
        // Get head pointer
        let head = self.head_index.get(&slug_hash)?;

        // Get node at current revision
        let key = NodeKey::new(head.node_id, head.rev);
        let node = self.node_store.get(&key)?;

        // Check if deleted (tombstone)
        if node.value().deleted {
            None
        } else {
            Some(node.value().clone())
        }
    }

    /// Get node by NodeId and revision
    ///
    /// # Arguments
    /// * `node_id` - Node ID to retrieve
    /// * `rev` - Revision number (use None for current revision)
    ///
    /// # Returns
    /// * `Option<NodeHeader>` - Node if found
    pub fn get_by_id(&self, node_id: NodeId, rev: Option<u64>) -> Option<NodeHeader> {
        let rev = match rev {
            Some(r) => r,
            None => {
                // Find current revision from any head pointer pointing to this node_id
                // This is inefficient - in production you'd maintain a reverse index
                for head in self.head_index.iter() {
                    if head.value().node_id == node_id {
                        return self
                            .node_store
                            .get(&NodeKey::new(node_id, head.value().rev))
                            .filter(|n| !n.value().deleted)
                            .map(|n| n.value().clone());
                    }
                }
                return None;
            }
        };

        let key = NodeKey::new(node_id, rev);
        let node = self.node_store.get(&key)?;

        // Check if deleted (tombstone)
        if node.value().deleted {
            None
        } else {
            Some(node.value().clone())
        }
    }

    /// Delete a node by slug hash (creates tombstone)
    ///
    /// # Arguments
    /// * `slug_hash` - Slug hash to delete
    /// * `reason` - Optional reason for deletion (for audit trail)
    ///
    /// # Returns
    /// * `Option<NodeHeader>` - Deleted node if found
    pub fn delete_by_slug(
        &self,
        slug_hash: SlugHash,
        reason: Option<String>,
    ) -> Option<NodeHeader> {
        // Get current node
        let current = self.get_by_slug(slug_hash)?;

        // Create tombstone version
        let tombstone = current.as_tombstone(reason);

        // Store tombstone and update head pointer
        let head = HeadPointer::new(current.node_id, tombstone.rev);
        self.head_index.insert(slug_hash, head);
        let key = NodeKey::new(tombstone.node_id, tombstone.rev);
        self.node_store.insert(key, tombstone);

        Some(current)
    }

    /// Check if storage is empty
    pub fn is_empty(&self) -> bool {
        self.head_index.is_empty()
    }

    /// Get number of nodes (count of head pointers)
    pub fn len(&self) -> usize {
        self.head_index.len()
    }

    /// Get all current (non-deleted) nodes
    ///
    /// # Returns
    /// * `Vec<NodeHeader>` - All current nodes
    pub fn all(&self) -> Vec<NodeHeader> {
        self.head_index
            .iter()
            .filter_map(|entry| {
                let head = entry.value();
                let key = NodeKey::new(head.node_id, head.rev);
                self.node_store
                    .get(&key)
                    .filter(|n| !n.value().deleted)
                    .map(|n| n.value().clone())
            })
            .collect()
    }

    /// Iterate over all nodes
    ///
    /// # Returns
    /// * `impl Iterator` - Iterator over current nodes
    pub fn iter(&self) -> impl Iterator<Item = NodeHeader> {
        self.head_index.iter().filter_map(|entry| {
            let head = entry.value();
            let key = NodeKey::new(head.node_id, head.rev);
            self.node_store
                .get(&key)
                .filter(|n| !n.value().deleted)
                .map(|n| n.value().clone())
        })
    }

    /// Get all versions of a node (for historical queries)
    ///
    /// # Arguments
    /// * `node_id` - Node ID to retrieve versions for
    ///
    /// # Returns
    /// * `Vec<NodeHeader>` - All versions of node, sorted by revision number
    pub fn get_all_versions(&self, node_id: NodeId) -> Vec<NodeHeader> {
        let mut versions: Vec<NodeHeader> = self
            .node_store
            .iter()
            .filter(|entry| entry.key().node_id == node_id)
            .map(|entry| entry.value().clone())
            .collect();

        // Sort by revision number (ascending)
        versions.sort_by_key(|n| n.rev);
        versions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BlobPtr;
    use tempfile::TempDir;

    #[test]
    fn test_upsert() {
        let temp_dir = TempDir::new().unwrap();
        let storage = SingleStorage::new(temp_dir.path()).unwrap();

        let node = NodeHeader::new(123u128, 456, 789, BlobPtr::new(0, 100, 200), 1700000000000);

        storage.upsert(node.clone());
        assert_eq!(storage.len(), 1);

        let retrieved = storage.get_by_slug(456).unwrap();
        assert_eq!(retrieved.node_id, 123u128);
        assert_eq!(retrieved.rev, 0);
        assert!(!retrieved.deleted);
    }

    #[test]
    fn test_versioning() {
        let temp_dir = TempDir::new().unwrap();
        let storage = SingleStorage::new(temp_dir.path()).unwrap();

        let node1 = NodeHeader::new(123u128, 456, 789, BlobPtr::new(0, 100, 200), 1700000000000);

        storage.upsert(node1.clone());

        // Create new version
        let node2 = node1.new_version(Some(BlobPtr::new(0, 200, 300)));
        let prev = storage.upsert(node2).unwrap();

        assert_eq!(prev.rev, 0);

        let retrieved = storage.get_by_slug(456).unwrap();
        assert_eq!(retrieved.rev, 1);
        assert_eq!(retrieved.payload_ptr.offset, 200);
    }

    #[test]
    fn test_tombstone_delete() {
        let temp_dir = TempDir::new().unwrap();
        let storage = SingleStorage::new(temp_dir.path()).unwrap();

        let node = NodeHeader::new(123u128, 456, 789, BlobPtr::new(0, 100, 200), 1700000000000);

        storage.upsert(node.clone());
        assert_eq!(storage.len(), 1);

        // Delete node
        let deleted = storage
            .delete_by_slug(456, Some("test deletion".to_string()))
            .unwrap();
        assert_eq!(deleted.node_id, 123u128);

        // Check that node is not found (tombstone)
        let retrieved = storage.get_by_slug(456);
        assert!(retrieved.is_none());

        // But storage still has the head pointer
        assert_eq!(storage.len(), 1);
    }

    #[test]
    fn test_get_all_versions() {
        let temp_dir = TempDir::new().unwrap();
        let storage = SingleStorage::new(temp_dir.path()).unwrap();

        let node1 = NodeHeader::new(123u128, 456, 789, BlobPtr::new(0, 100, 200), 1700000000000);

        storage.upsert(node1.clone());

        let node2 = node1.new_version(Some(BlobPtr::new(0, 200, 300)));
        storage.upsert(node2.clone());

        let node3 = node2.new_version(None);
        storage.upsert(node3);

        let versions = storage.get_all_versions(123u128);
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].rev, 0);
        assert_eq!(versions[1].rev, 1);
        assert_eq!(versions[2].rev, 2);
    }

    #[test]
    fn test_concurrent_inserts() {
        use std::sync::Arc;
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(SingleStorage::new(temp_dir.path()).unwrap());

        let handles: Vec<_> = (0..100)
            .map(|i| {
                let storage = storage.clone();
                thread::spawn(move || {
                    let node = NodeHeader::new(
                        i as NodeId,
                        i as SlugHash,
                        (i + 1000) as u64,
                        BlobPtr::new(0, i as u64, 100),
                        1700000000000,
                    );
                    storage.upsert(node);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(storage.len(), 100);
    }
}
