//! Ingestion Buffer (Tier 1)
//! Hyperminimalist implementation using in-memory HashMap for now

use crate::{NodeId, SlugHash, NodeHeader};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

/// Tier 1: Ingestion Buffer for high-velocity writes
/// Currently using simple HashMap with Mutex for thread safety
pub struct IngestionBuffer {
    nodes: Mutex<HashMap<NodeId, NodeHeader>>,
    _base_dir: PathBuf,
}

impl IngestionBuffer {
    pub fn new(base_dir: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        Ok(Self {
            nodes: Mutex::new(HashMap::new()),
            _base_dir: base_dir.as_ref().to_path_buf(),
        })
    }

    /// Upsert a node into the ingestion buffer
    pub fn upsert(&self, node: NodeHeader) {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.insert(node.node_id, node);
    }

    /// Get a node by ID
    pub fn get(&self, node_id: NodeId) -> Option<NodeHeader> {
        let nodes = self.nodes.lock().unwrap();
        nodes.get(&node_id).cloned()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        let nodes = self.nodes.lock().unwrap();
        nodes.is_empty()
    }

    /// Get number of nodes in buffer
    pub fn len(&self) -> usize {
        let nodes = self.nodes.lock().unwrap();
        nodes.len()
    }

    /// Remove a node from buffer (for promotion)
    pub fn remove(&self, node_id: NodeId) -> Option<NodeHeader> {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.remove(&node_id)
    }

    /// Get all nodes
    pub fn all(&self) -> Vec<NodeHeader> {
        let nodes = self.nodes.lock().unwrap();
        nodes.values().cloned().collect()
    }

    /// Drain all nodes from the buffer (for promotion)
    pub fn drain_all(&self) -> Vec<NodeHeader> {
        let mut nodes = self.nodes.lock().unwrap();
        nodes.drain().map(|(_, node)| node).collect()
    }

    /// Get node by slug hash (for tier-agnostic resolution)
    pub fn get_by_slug(&self, slug_hash: SlugHash) -> Option<NodeHeader> {
        let nodes = self.nodes.lock().unwrap();
        nodes.values().find(|node| node.slug_hash == slug_hash).cloned()
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
        let buffer = IngestionBuffer::new(temp_dir.path().to_path_buf()).unwrap();

        let node = NodeHeader::new(
            123u128,
            456,
            789,
            BlobPtr::new(0, 100, 200),
            1700000000000,
        );

        buffer.upsert(node.clone());
        assert_eq!(buffer.len(), 1);

        let retrieved = buffer.get(123u128).unwrap();
        assert_eq!(retrieved.node_id, 123u128);
    }

    #[test]
    fn test_upsert_update() {
        let temp_dir = TempDir::new().unwrap();
        let buffer = IngestionBuffer::new(temp_dir.path().to_path_buf()).unwrap();

        let node1 = NodeHeader::new(
            123u128,
            456,
            789,
            BlobPtr::new(0, 100, 200),
            1700000000000,
        );

        let node2 = NodeHeader::new(
            123u128,
            999,
            888,
            BlobPtr::new(0, 300, 400),
            1700000000001,
        );

        buffer.upsert(node1);
        buffer.upsert(node2);

        assert_eq!(buffer.len(), 1);
        let retrieved = buffer.get(123u128).unwrap();
        assert_eq!(retrieved.slug_hash, 999);
    }
}