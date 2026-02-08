//! Persistent Storage Layer - redb-backed MVCC with serde serialization
//!
//! This module provides persistent MVCC storage using redb (embedded B+Tree database)
//! with serde for serialization. All data survives restarts and supports
//! crash recovery via WAL.

use crate::types::HeadPointer;
use crate::{NodeHeader, NodeId, SlugHash};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::sync::Arc;

// Table definitions for redb
// Values are stored as Vec<u8> for flexible byte storage
const HEAD_TABLE: TableDefinition<SlugHash, Vec<u8>> = TableDefinition::new("head_index");
const NODE_TABLE: TableDefinition<[u8; 24], Vec<u8>> = TableDefinition::new("node_store"); // 24 bytes = NodeKey (16 bytes node_id + 8 bytes rev)

/// Composite key for node store (node_id + rev)
#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeKey {
    node_id: NodeId,
    rev: u64,
}

impl NodeKey {
    fn new(node_id: NodeId, rev: u64) -> Self {
        Self { node_id, rev }
    }

    fn to_bytes(&self) -> [u8; 24] {
        let mut bytes = [0u8; 24];
        bytes[0..16].copy_from_slice(&self.node_id.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.rev.to_be_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let node_id = u128::from_be_bytes(bytes[0..16].try_into().unwrap());
        let rev = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
        Self { node_id, rev }
    }
}

/// Persistent storage layer using redb
///
/// Implements MVCC with persistent storage:
/// - All data survives restarts (redb B+Tree)
/// - Serde serialization for simplicity
/// - Automatic WAL for crash recovery
/// - Same API as SingleStorage for easy migration
pub struct PersistentStorage {
    /// redb database instance
    db: Arc<Database>,
}

impl PersistentStorage {
    /// Create or open a persistent storage instance
    ///
    /// # Arguments
    /// * `base_dir` - Directory for redb database files
    ///
    /// # Returns
    /// * `std::io::Result<Self>` - Persistent storage instance
    pub fn new(base_dir: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let db_path = base_dir.as_ref().join("sekejap.redb");
        let db = Database::create(db_path).map_err(|e| std::io::Error::other(e.to_string()))?;

        // Create tables if they don't exist
        let write_txn = db
            .begin_write()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        {
            let table = write_txn
                .open_table(HEAD_TABLE)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let _ = table; // Just ensure table exists

            let table = write_txn
                .open_table(NODE_TABLE)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            let _ = table; // Just ensure table exists
        }
        write_txn
            .commit()
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Insert or update a node (creates new revision)
    ///
    /// # Arguments
    /// * `node` - New version of node to insert/update
    ///
    /// # Returns
    /// * `Option<NodeHeader>` - Previous version if exists
    pub fn upsert(&self, node: NodeHeader) -> Option<NodeHeader> {
        let write_txn = self.db.begin_write().ok()?;
        {
            // Store new version (keep all versions for historical queries)
            let mut node_table = write_txn.open_table(NODE_TABLE).ok()?;
            let key = NodeKey::new(node.node_id, node.rev);
            let key_bytes = key.to_bytes();

            // Serialize node header using serde
            let node_bytes = bincode::serialize(&node).ok()?;
            node_table.insert(&key_bytes, &node_bytes).ok()?;

            // Update head pointer to new revision
            let mut head_table = write_txn.open_table(HEAD_TABLE).ok()?;
            let head = HeadPointer::new(node.node_id, node.rev);
            let head_bytes = bincode::serialize(&head).ok()?;
            head_table.insert(node.slug_hash, &head_bytes).ok()?;
        }
        write_txn.commit().ok()?;

        // Return previous version if it exists
        if node.rev > 0 {
            self.get_by_id(node.node_id, Some(node.rev - 1))
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
        let read_txn = self.db.begin_read().ok()?;
        let head_table = read_txn.open_table(HEAD_TABLE).ok()?;

        // get() returns Result<Option<AccessGuard>>
        let guard = head_table.get(&slug_hash).ok()??;
        let head_bytes = guard.value();

        // Deserialize head pointer using bincode
        let head: HeadPointer = bincode::deserialize(&head_bytes).ok()?;

        // Get node at current revision
        let node_table = read_txn.open_table(NODE_TABLE).ok()?;
        let key = NodeKey::new(head.node_id, head.rev);
        let key_bytes = key.to_bytes();
        let guard = node_table.get(&key_bytes).ok()??;
        let node_bytes = guard.value();

        // Deserialize node header using bincode
        let node: NodeHeader = bincode::deserialize(&node_bytes).ok()?;

        // Check if deleted (tombstone)
        if node.deleted { None } else { Some(node) }
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
                let read_txn = self.db.begin_read().ok()?;
                let head_table = read_txn.open_table(HEAD_TABLE).ok()?;
                let mut iter = head_table.iter().ok()?;

                loop {
                    let entry_result = iter.next();
                    match entry_result {
                        Some(Ok(entry)) => {
                            let head_bytes = entry.1.value();
                            let head: HeadPointer = bincode::deserialize(&head_bytes).ok()?;
                            if head.node_id == node_id {
                                return self.get_by_id(node_id, Some(head.rev));
                            }
                        }
                        Some(Err(_)) | None => return None,
                    }
                }
            }
        };

        let read_txn = self.db.begin_read().ok()?;
        let node_table = read_txn.open_table(NODE_TABLE).ok()?;
        let key = NodeKey::new(node_id, rev);
        let key_bytes = key.to_bytes();
        let guard = node_table.get(&key_bytes).ok()??;
        let node_bytes = guard.value();

        // Deserialize node header using bincode
        let node: NodeHeader = bincode::deserialize(&node_bytes).ok()?;

        // Check if deleted (tombstone)
        if node.deleted { None } else { Some(node) }
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
        let write_txn = self.db.begin_write().ok()?;
        {
            let mut node_table = write_txn.open_table(NODE_TABLE).ok()?;
            let key = NodeKey::new(tombstone.node_id, tombstone.rev);
            let key_bytes = key.to_bytes();
            let node_bytes = bincode::serialize(&tombstone).ok()?;
            node_table.insert(&key_bytes, &node_bytes).ok()?;

            let mut head_table = write_txn.open_table(HEAD_TABLE).ok()?;
            let head = HeadPointer::new(current.node_id, tombstone.rev);
            let head_bytes = bincode::serialize(&head).ok()?;
            head_table.insert(slug_hash, &head_bytes).ok()?;
        }
        write_txn.commit().ok()?;

        Some(current)
    }

    /// Insert/update a batch of nodes in a SINGLE transaction (fastest for bulk promotion)
    ///
    /// # Arguments
    /// * `nodes` - Slice of nodes to insert/update
    ///
    /// # Performance
    /// Uses one redb transaction for all nodes - 10-100x faster than individual upsert()
    /// Ideal for Tier 1 → Tier 2 promotion
    ///
    /// # Returns
    /// * `Result<(), Box<dyn Error>>` - Success or error
    pub fn upsert_batch(&self, nodes: &[NodeHeader]) -> Result<(), Box<dyn std::error::Error>> {
        if nodes.is_empty() {
            return Ok(());
        }

        let write_txn = self.db.begin_write()?;
        {
            let mut node_table = write_txn.open_table(NODE_TABLE)?;
            let mut head_table = write_txn.open_table(HEAD_TABLE)?;

            for node in nodes {
                // Store new version (keep all versions for historical queries)
                let key = NodeKey::new(node.node_id, node.rev);
                let key_bytes = key.to_bytes();
                let node_bytes = bincode::serialize(node)?;
                node_table.insert(&key_bytes, &node_bytes)?;

                // Update head pointer to new revision
                let head = HeadPointer::new(node.node_id, node.rev);
                let head_bytes = bincode::serialize(&head)?;
                head_table.insert(node.slug_hash, &head_bytes)?;
            }
        }
        write_txn.commit()?;

        Ok(())
    }

    /// Check if storage is empty
    pub fn is_empty(&self) -> bool {
        let read_txn = self.db.begin_read();
        if read_txn.is_err() {
            return true;
        }
        let head_table_result = read_txn.unwrap().open_table(HEAD_TABLE);
        if head_table_result.is_err() {
            return true;
        }

        let head_table = head_table_result.unwrap();
        let mut iter = match head_table.iter() {
            Ok(iter) => iter,
            Err(_) => return true,
        };
        match iter.next() {
            Some(Ok(_)) => false,
            Some(Err(_)) | None => true,
        }
    }

    /// Get number of nodes (count of head pointers)
    pub fn len(&self) -> usize {
        let read_txn = self.db.begin_read();
        if read_txn.is_err() {
            return 0;
        }
        let head_table_result = read_txn.unwrap().open_table(HEAD_TABLE);
        if head_table_result.is_err() {
            return 0;
        }

        let mut count = 0;
        let head_table = head_table_result.unwrap();
        let mut iter = match head_table.iter() {
            Ok(iter) => iter,
            Err(_) => return 0,
        };
        loop {
            let result = iter.next();
            match result {
                Some(Ok(_)) => count += 1,
                Some(Err(_)) | None => break,
            }
        }
        count
    }

    /// Get all current (non-deleted) nodes
    ///
    /// # Returns
    /// * `Vec<NodeHeader>` - All current nodes
    pub fn all(&self) -> Vec<NodeHeader> {
        let mut nodes = Vec::new();
        let read_txn = self.db.begin_read();
        if read_txn.is_err() {
            return nodes;
        }
        let head_table_result = read_txn.unwrap().open_table(HEAD_TABLE);
        if head_table_result.is_err() {
            return nodes;
        }

        let head_table = head_table_result.unwrap();
        let mut iter = match head_table.iter() {
            Ok(iter) => iter,
            Err(_) => return nodes,
        };
        loop {
            let result = iter.next();
            match result {
                Some(Ok(entry)) => {
                    let head_bytes = entry.1.value();
                    if let Ok(head) = bincode::deserialize::<HeadPointer>(&head_bytes) {
                        // Get node at current revision
                        if let Some(node) = self.get_by_id(head.node_id, Some(head.rev)) {
                            nodes.push(node);
                        }
                    }
                }
                Some(Err(_)) | None => break,
            }
        }

        nodes
    }

    /// Get all versions of a node (for historical queries)
    ///
    /// # Arguments
    /// * `node_id` - Node ID to retrieve versions for
    ///
    /// # Returns
    /// * `Vec<NodeHeader>` - All versions of node, sorted by revision number
    pub fn get_all_versions(&self, node_id: NodeId) -> Vec<NodeHeader> {
        let mut versions = Vec::new();
        let read_txn = self.db.begin_read();
        if read_txn.is_err() {
            return versions;
        }
        let node_table_result = read_txn.unwrap().open_table(NODE_TABLE);
        if node_table_result.is_err() {
            return versions;
        }

        // Scan all nodes and filter by node_id
        let node_table = node_table_result.unwrap();
        let mut iter = match node_table.iter() {
            Ok(iter) => iter,
            Err(_) => return versions,
        };
        loop {
            let result = iter.next();
            match result {
                Some(Ok(entry)) => {
                    let key_bytes = entry.0.value();
                    let key = NodeKey::from_bytes(&key_bytes);

                    if key.node_id == node_id {
                        let node_bytes = entry.1.value();
                        if let Ok(node) = bincode::deserialize(&node_bytes) {
                            versions.push(node);
                        }
                    }
                }
                Some(Err(_)) | None => break,
            }
        }

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
    fn test_persistent_upsert() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::new(temp_dir.path()).unwrap();

        let node = NodeHeader::new(123u128, 456, 789, BlobPtr::new(0, 100, 200), 1700000000000);

        storage.upsert(node.clone());
        assert_eq!(storage.len(), 1);

        let retrieved = storage.get_by_slug(456).unwrap();
        assert_eq!(retrieved.node_id, 123u128);
        assert_eq!(retrieved.rev, 0);
        assert!(!retrieved.deleted);
    }

    #[test]
    fn test_persistent_versioning() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::new(temp_dir.path()).unwrap();

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
    fn test_persistent_tombstone_delete() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::new(temp_dir.path()).unwrap();

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

        // But storage still has head pointer
        assert_eq!(storage.len(), 1);
    }

    #[test]
    fn test_persistence_across_restarts() {
        let temp_dir = TempDir::new().unwrap();

        // Create storage and write data
        let node = NodeHeader::new(123u128, 456, 789, BlobPtr::new(0, 100, 200), 1700000000000);

        {
            let storage = PersistentStorage::new(temp_dir.path()).unwrap();
            storage.upsert(node.clone());
            assert_eq!(storage.len(), 1);
        }

        // Re-open storage and verify data persists
        let storage2 = PersistentStorage::new(temp_dir.path()).unwrap();
        assert_eq!(storage2.len(), 1);

        let retrieved = storage2.get_by_slug(456).unwrap();
        assert_eq!(retrieved.node_id, 123u128);
        assert_eq!(retrieved.rev, 0);
    }

    #[test]
    fn test_get_all_versions() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::new(temp_dir.path()).unwrap();

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
}
