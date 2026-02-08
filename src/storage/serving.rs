//! Serving Layer (Tier 2)
//! Hyperminimalist implementation using in-memory HashMap for now

use crate::{NodeHeader, NodeId, SlugHash};
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::path::PathBuf;

/// Tier 2: Serving Layer for sub-millisecond reads
/// Currently using simple HashMap (hyperminimalist - will upgrade to redb later)
pub struct ServingLayer {
    // Using ArcSwap for zero-lock MVCC reads
    nodes: ArcSwap<HashMap<SlugHash, NodeHeader>>,
    _base_dir: PathBuf,
}

impl ServingLayer {
    pub fn new(base_dir: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        Ok(Self {
            nodes: ArcSwap::new(HashMap::new().into()),
            _base_dir: base_dir.as_ref().to_path_buf(),
        })
    }

    /// Insert or update a node (atomic)
    pub fn upsert(&self, node: NodeHeader) {
        let mut current = self.nodes.load().as_ref().clone();
        current.insert(node.slug_hash, node);
        self.nodes.store(current.into());
    }

    /// Get a node by slug hash (lock-free read)
    pub fn get_by_slug(&self, slug_hash: SlugHash) -> Option<NodeHeader> {
        self.nodes.load().get(&slug_hash).cloned()
    }

    /// Get a node by ID (linear search - will optimize with redb later)
    pub fn get_by_id(&self, node_id: NodeId) -> Option<NodeHeader> {
        self.nodes
            .load()
            .values()
            .find(|n| n.node_id == node_id)
            .cloned()
    }

    /// Check if serving layer is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.load().is_empty()
    }

    /// Get number of nodes in serving layer
    pub fn len(&self) -> usize {
        self.nodes.load().len()
    }

    /// Get all nodes (for iteration)
    pub fn all(&self) -> Vec<NodeHeader> {
        self.nodes.load().values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BlobPtr;
    use tempfile::TempDir;

    #[test]
    fn test_upsert_and_get() {
        let temp_dir = TempDir::new().unwrap();
        let serving = ServingLayer::new(temp_dir.path().to_path_buf()).unwrap();

        let node = NodeHeader::new(123u128, 456, 789, BlobPtr::new(0, 100, 200), 1700000000000);

        serving.upsert(node.clone());
        assert_eq!(serving.len(), 1);

        let retrieved = serving.get_by_slug(456).unwrap();
        assert_eq!(retrieved.node_id, 123u128);
    }

    #[test]
    fn test_get_by_id() {
        let temp_dir = TempDir::new().unwrap();
        let serving = ServingLayer::new(temp_dir.path().to_path_buf()).unwrap();

        let node = NodeHeader::new(123u128, 456, 789, BlobPtr::new(0, 100, 200), 1700000000000);

        serving.upsert(node);
        let retrieved = serving.get_by_id(123u128).unwrap();
        assert_eq!(retrieved.slug_hash, 456);
    }
}
