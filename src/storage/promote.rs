//! Promotion from Ingestion Buffer (Tier 1) to Persistent Storage (Tier 2)
//!
//! This module handles the promotion of nodes from the high-velocity
//! ingestion buffer to the persistent serving layer.

use crate::storage::{IngestionBuffer, PersistentStorage};
use crate::{NodeHeader, NodeId};

/// Promote a single node from Ingestion Buffer to Persistent Storage
///
/// This moves the node from the volatile in-memory buffer to the
/// persistent redb-backed storage layer.
///
/// # Arguments
/// * `ingestion` - Mutable reference to Ingestion Buffer (Tier 1)
/// * `persistent` - Reference to Persistent Storage (Tier 2)
/// * `node_id` - Node ID to promote
///
/// # Returns
/// * `Option<NodeHeader>` - The promoted node if found, None otherwise
///
/// # Example
/// ```rust,no_run
/// use sekejap::storage::{promote_node, IngestionBuffer, PersistentStorage};
/// # use std::error::Error;
/// # fn main() -> Result<(), Box<dyn Error>> {
/// let mut ingestion = IngestionBuffer::new("/tmp/ingestion")?;
/// let persistent = PersistentStorage::new("/tmp/persistent")?;
/// # Ok(())
/// # }
/// ```
pub fn promote_node(
    ingestion: &mut IngestionBuffer,
    persistent: &PersistentStorage,
    node_id: NodeId,
) -> Option<NodeHeader> {
    // Get node from ingestion buffer
    let node = ingestion.get(node_id)?;

    // Write to persistent storage (Tier 2)
    persistent.upsert(node.clone());

    // Remove from ingestion buffer (successful promotion)
    ingestion.remove(node_id);

    Some(node)
}

/// Promote multiple nodes from Ingestion Buffer to Persistent Storage
///
/// Batch promotion for better performance. This function promotes
/// all nodes currently in the ingestion buffer.
///
/// # Arguments
/// * `ingestion` - Mutable reference to Ingestion Buffer (Tier 1)
/// * `persistent` - Reference to Persistent Storage (Tier 2)
///
/// # Returns
/// * `Vec<NodeHeader>` - All promoted nodes
///
/// # Example
/// ```rust,no_run
/// use sekejap::storage::{promote_all, IngestionBuffer, PersistentStorage};
/// # use std::error::Error;
/// # fn main() -> Result<(), Box<dyn Error>> {
/// let mut ingestion = IngestionBuffer::new("/tmp/ingestion")?;
/// let persistent = PersistentStorage::new("/tmp/persistent")?;
/// # Ok(())
/// # }
/// ```
pub fn promote_all(
    ingestion: &mut IngestionBuffer,
    persistent: &PersistentStorage,
) -> Vec<NodeHeader> {
    let mut promoted = Vec::new();

    // Collect all nodes first (need IDs, not references)
    let node_ids: Vec<NodeId> = ingestion.all().into_iter().map(|n| n.node_id).collect();

    // Promote each node
    for node_id in node_ids {
        if let Some(node) = promote_node(ingestion, persistent, node_id) {
            promoted.push(node);
        }
    }

    promoted
}

/// Promote nodes matching a predicate function
///
/// Selective promotion based on custom criteria.
///
/// # Arguments
/// * `ingestion` - Mutable reference to Ingestion Buffer (Tier 1)
/// * `persistent` - Reference to Persistent Storage (Tier 2)
/// * `predicate` - Function that returns true for nodes to promote
///
/// # Returns
/// * `Vec<NodeHeader>` - All promoted nodes
///
/// # Example
/// ```rust,no_run
/// use sekejap::storage::{promote_if, IngestionBuffer, PersistentStorage};
/// # use std::error::Error;
/// # fn main() -> Result<(), Box<dyn Error>> {
/// let mut ingestion = IngestionBuffer::new("/tmp/ingestion")?;
/// let persistent = PersistentStorage::new("/tmp/persistent")?;
/// # Ok(())
/// # }
/// ```
pub fn promote_if<F>(
    ingestion: &mut IngestionBuffer,
    persistent: &PersistentStorage,
    predicate: F,
) -> Vec<NodeHeader>
where
    F: Fn(&NodeHeader) -> bool,
{
    let mut promoted = Vec::new();

    // Collect matching node IDs
    let node_ids: Vec<NodeId> = ingestion
        .all()
        .into_iter()
        .filter(|n| predicate(n))
        .map(|n| n.node_id)
        .collect();

    // Promote each matching node
    for node_id in node_ids {
        if let Some(node) = promote_node(ingestion, persistent, node_id) {
            promoted.push(node);
        }
    }

    promoted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BlobPtr;
    use tempfile::TempDir;

    #[test]
    fn test_promote_single_node() {
        let temp_dir = TempDir::new().unwrap();

        let mut ingestion = IngestionBuffer::new(temp_dir.path()).unwrap();
        let persistent = PersistentStorage::new(temp_dir.path()).unwrap();

        let node = NodeHeader::new(123u128, 456, 789, BlobPtr::new(0, 100, 200), 1700000000000);

        // Write to ingestion buffer
        ingestion.upsert(node.clone());
        assert_eq!(ingestion.len(), 1);
        assert!(persistent.get_by_slug(456).is_none());

        // Promote to persistent storage
        let promoted = promote_node(&mut ingestion, &persistent, 123u128).unwrap();
        assert_eq!(promoted.node_id, 123u128);

        // Verify promotion
        assert_eq!(ingestion.len(), 0);
        let retrieved = persistent.get_by_slug(456).unwrap();
        assert_eq!(retrieved.node_id, 123u128);
    }

    #[test]
    fn test_promote_all_nodes() {
        let temp_dir = TempDir::new().unwrap();

        let mut ingestion = IngestionBuffer::new(temp_dir.path()).unwrap();
        let persistent = PersistentStorage::new(temp_dir.path()).unwrap();

        let node1 = NodeHeader::new(1u128, 100, 200, BlobPtr::new(0, 10, 20), 1700000000000);
        let node2 = NodeHeader::new(2u128, 101, 201, BlobPtr::new(0, 30, 40), 1700000000001);
        let node3 = NodeHeader::new(3u128, 102, 202, BlobPtr::new(0, 50, 60), 1700000000002);

        // Write all nodes to ingestion buffer
        ingestion.upsert(node1);
        ingestion.upsert(node2);
        ingestion.upsert(node3);
        assert_eq!(ingestion.len(), 3);

        // Promote all nodes
        let promoted = promote_all(&mut ingestion, &persistent);
        assert_eq!(promoted.len(), 3);

        // Verify promotion
        assert_eq!(ingestion.len(), 0);
        assert_eq!(persistent.len(), 3);
    }

    #[test]
    fn test_promote_if_predicate() {
        let temp_dir = TempDir::new().unwrap();

        let mut ingestion = IngestionBuffer::new(temp_dir.path()).unwrap();
        let persistent = PersistentStorage::new(temp_dir.path()).unwrap();

        let node1 = NodeHeader::new(1u128, 100, 200, BlobPtr::new(0, 10, 20), 1700000000000);
        let node2 = NodeHeader::new(2u128, 101, 201, BlobPtr::new(0, 30, 40), 1700000001000);
        let node3 = NodeHeader::new(3u128, 102, 202, BlobPtr::new(0, 50, 60), 1700000002000);

        // Write all nodes to ingestion buffer
        ingestion.upsert(node1);
        ingestion.upsert(node2);
        ingestion.upsert(node3);
        assert_eq!(ingestion.len(), 3);

        // Promote only nodes with timestamp > 1700000001000
        let promoted = promote_if(&mut ingestion, &persistent, |node| {
            node.epoch_created > 1700000001000
        });
        assert_eq!(promoted.len(), 1);

        // Verify: node1 and node2 remain in ingestion, node3 promoted
        assert_eq!(ingestion.len(), 2);
        assert_eq!(persistent.len(), 1);
        assert!(ingestion.get(1u128).is_some());
        assert!(ingestion.get(2u128).is_some());
        assert!(ingestion.get(3u128).is_none());
    }

    #[test]
    fn test_promote_nonexistent_node() {
        let temp_dir = TempDir::new().unwrap();

        let mut ingestion = IngestionBuffer::new(temp_dir.path()).unwrap();
        let persistent = PersistentStorage::new(temp_dir.path()).unwrap();

        // Try to promote non-existent node
        let result = promote_node(&mut ingestion, &persistent, 999u128);
        assert!(result.is_none());
    }
}
