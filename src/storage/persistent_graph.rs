//! Persistent Graph Storage
//!
//! Provides durable edge storage backed by redb.
//! Maintains both forward and reverse edge indexes for efficient traversals.

use crate::types::{EntityId, WeightedEdge, EdgeType};
use crate::hashing::hash_slug;
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::path::Path;

/// Table definitions for edge storage - stores Vec<WeightedEdge> per source/target
const FORWARD_EDGES: TableDefinition<u64, Vec<u8>> = TableDefinition::new("forward_edges");
const REVERSE_EDGES: TableDefinition<u64, Vec<u8>> = TableDefinition::new("reverse_edges");

/// Persistent graph storage with redb backing
#[derive(Debug)]
pub struct PersistentGraph {
    /// The redb database
    db: Database,
}

impl PersistentGraph {
    /// Convert EntityId to u64 hash for storage
    fn entity_id_to_key(entity_id: &EntityId) -> u64 {
        hash_slug(entity_id.to_string().as_str())
    }

    /// Open or create a persistent graph database
    pub fn new(path: &Path) -> Self {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        let db = redb::Database::create(path).unwrap();

        {
            let write_txn = db.begin_write().unwrap();
            let _ = write_txn.open_table(FORWARD_EDGES);
            let _ = write_txn.open_table(REVERSE_EDGES);
            write_txn.commit().unwrap();
        }

        Self { db }
    }

    /// Add an edge to the graph
    pub fn add_edge(&self, edge: &WeightedEdge) -> bool {
        let _edge_data = bincode::serialize(edge).unwrap();
        let from_key = Self::entity_id_to_key(&edge._from);
        let to_key = Self::entity_id_to_key(&edge._to);

        // Use a single write transaction for both tables
        let write_txn = self.db.begin_write().unwrap();
        
        // Add to forward edges (append to existing edges for source)
        {
            let mut forward_table = write_txn.open_table(FORWARD_EDGES).unwrap();
            let mut edges: Vec<WeightedEdge> = match forward_table.get(&from_key) {
                Ok(Some(data)) => bincode::deserialize(data.value().as_slice()).unwrap_or_default(),
                _ => Vec::new(),
            };
            
            // Check if edge already exists
            let exists = edges.iter().any(|e| {
                e._from == edge._from && e._to == edge._to
            });
            
            if !exists {
                edges.push(edge.clone());
                let data = bincode::serialize(&edges).unwrap();
                forward_table.insert(&from_key, &data).unwrap();
            }
        }
        
        // Add to reverse edges (append to existing edges for target)
        {
            let mut reverse_table = write_txn.open_table(REVERSE_EDGES).unwrap();
            let mut edges: Vec<WeightedEdge> = match reverse_table.get(&to_key) {
                Ok(Some(data)) => bincode::deserialize(data.value().as_slice()).unwrap_or_default(),
                _ => Vec::new(),
            };
            
            edges.push(edge.clone());
            let data = bincode::serialize(&edges).unwrap();
            reverse_table.insert(&to_key, &data).unwrap();
        }
        
        write_txn.commit().unwrap();
        
        true
    }

    /// Get all outgoing edges from a node
    pub fn get_edges_from(&self, entity_id: &EntityId) -> Vec<WeightedEdge> {
        let txn = self.db.begin_read().unwrap();
        let forward_table = txn.open_table(FORWARD_EDGES).unwrap();

        let key = Self::entity_id_to_key(entity_id);
        match forward_table.get(&key) {
            Ok(Some(edge_data)) => bincode::deserialize(edge_data.value().as_slice()).unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    /// Get all incoming edges to a node
    pub fn get_edges_to(&self, entity_id: &EntityId) -> Vec<WeightedEdge> {
        let txn = self.db.begin_read().unwrap();
        let reverse_table = txn.open_table(REVERSE_EDGES).unwrap();

        let key = Self::entity_id_to_key(entity_id);
        match reverse_table.get(&key) {
            Ok(Some(edge_data)) => bincode::deserialize(edge_data.value().as_slice()).unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    /// Remove an edge from the graph
    pub fn remove_edge(&self, from: &EntityId, to: &EntityId) -> bool {
        let from_key = Self::entity_id_to_key(from);
        let to_key = Self::entity_id_to_key(to);
        let mut removed = false;

        // Process forward table
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut forward_table = write_txn.open_table(FORWARD_EDGES).unwrap();
            let bytes = match forward_table.get(&from_key) {
                Ok(Some(data)) => data.value().to_vec(),
                _ => Vec::new(),
            };

            if !bytes.is_empty() {
                let mut edges: Vec<WeightedEdge> = bincode::deserialize(&bytes).unwrap_or_default();
                let original_len = edges.len();
                edges.retain(|e| e._to != *to);

                if edges.len() < original_len {
                    removed = true;
                    if edges.is_empty() {
                        forward_table.remove(&from_key).unwrap();
                    } else {
                        let edge_data = bincode::serialize(&edges).unwrap();
                        forward_table.insert(&from_key, &edge_data).unwrap();
                    }
                }
            }
        }
        write_txn.commit().unwrap();

        // Process reverse table
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut reverse_table = write_txn.open_table(REVERSE_EDGES).unwrap();
            let bytes = match reverse_table.get(&to_key) {
                Ok(Some(data)) => data.value().to_vec(),
                _ => Vec::new(),
            };

            if !bytes.is_empty() {
                let mut edges: Vec<WeightedEdge> = bincode::deserialize(&bytes).unwrap_or_default();
                let original_len = edges.len();
                edges.retain(|e| e._from != *from);

                if edges.len() < original_len {
                    if edges.is_empty() {
                        reverse_table.remove(&to_key).unwrap();
                    } else {
                        let edge_data = bincode::serialize(&edges).unwrap();
                        reverse_table.insert(&to_key, &edge_data).unwrap();
                    }
                }
            }
        }
        write_txn.commit().unwrap();

        removed
    }

    /// Check if an edge exists
    pub fn has_edge(&self, from: &EntityId, to: &EntityId) -> bool {
        let txn = self.db.begin_read().unwrap();
        let forward_table = txn.open_table(FORWARD_EDGES).unwrap();

        let from_key = Self::entity_id_to_key(from);
        match forward_table.get(&from_key) {
            Ok(Some(edge_data)) => {
                let edges: Vec<WeightedEdge> = bincode::deserialize(edge_data.value().as_slice()).unwrap_or_default();
                edges.iter().any(|e| e._to == *to)
            }
            _ => false,
        }
    }

    /// Get the number of nodes in the graph
    pub fn node_count(&self) -> u64 {
        let txn = self.db.begin_read().unwrap();
        let forward_table = txn.open_table(FORWARD_EDGES).unwrap();
        forward_table.len().unwrap()
    }

    /// Get the total number of edges
    pub fn edge_count(&self) -> u64 {
        let txn = self.db.begin_read().unwrap();
        let forward_table = txn.open_table(FORWARD_EDGES).unwrap();

        let mut total = 0;
        for entry in forward_table.iter().unwrap() {
            let (_, value) = entry.unwrap();
            let edges: Vec<WeightedEdge> = bincode::deserialize(value.value().as_slice()).unwrap_or_default();
            total += edges.len() as u64;
        }
        total
    }

    /// Clear all edges
    pub fn clear(&self) {
        let txn = self.db.begin_write().unwrap();
        let _ = txn.delete_table(FORWARD_EDGES);
        let _ = txn.delete_table(REVERSE_EDGES);
        txn.commit().unwrap();
        
        // Recreate tables for future use
        let txn = self.db.begin_write().unwrap();
        let _ = txn.open_table(FORWARD_EDGES);
        let _ = txn.open_table(REVERSE_EDGES);
        txn.commit().unwrap();
    }
}

/// Create a new edge with automatic entity ID creation from slugs
pub fn create_edge_from_slugs(
    source_slug: &str,
    target_slug: &str,
    weight: f32,
    edge_type: EdgeType,
    evidence_ptr: u64,
    timestamp: u64,
) -> WeightedEdge {
    let _from = EntityId::new("nodes", source_slug.to_string());
    let _to = EntityId::new("nodes", target_slug.to_string());

    WeightedEdge::new(
        _from, _to, weight, edge_type, evidence_ptr, timestamp, None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_persistent_graph_basic() {
        let temp_dir = TempDir::new().unwrap();
        let graph = PersistentGraph::new(temp_dir.path().join("graph.redb").as_path());

        let edge = create_edge_from_slugs("node-a", "node-b", 0.9, "causal".to_string(), 0, 1000);
        assert!(graph.add_edge(&edge));

        let from_edges = graph.get_edges_from(&EntityId::new("nodes", "node-a"));
        assert_eq!(from_edges.len(), 1);
        assert_eq!(from_edges[0]._to.key(), "node-b");

        let to_edges = graph.get_edges_to(&EntityId::new("nodes", "node-b"));
        assert_eq!(to_edges.len(), 1);
        assert_eq!(to_edges[0]._from.key(), "node-a");

        assert_eq!(graph.node_count(), 1);
        assert_eq!(graph.edge_count(), 1);
        assert!(graph.has_edge(
            &EntityId::new("nodes", "node-a"),
            &EntityId::new("nodes", "node-b"),
        ));
    }

    #[test]
    fn test_persistent_graph_multiple_edges() {
        let temp_dir = TempDir::new().unwrap();
        let graph = PersistentGraph::new(temp_dir.path().join("graph.redb").as_path());

        let edge1 = create_edge_from_slugs("node-a", "node-b", 0.9, "causal".to_string(), 0, 1000);
        let edge2 = create_edge_from_slugs("node-a", "node-c", 0.8, "causal".to_string(), 0, 1000);

        graph.add_edge(&edge1);
        graph.add_edge(&edge2);

        let from_edges = graph.get_edges_from(&EntityId::new("nodes", "node-a"));
        assert_eq!(from_edges.len(), 2);
        assert_eq!(graph.edge_count(), 2);
    }

    #[test]
    fn test_persistent_graph_remove() {
        let temp_dir = TempDir::new().unwrap();
        let graph = PersistentGraph::new(temp_dir.path().join("graph.redb").as_path());

        let edge1 = create_edge_from_slugs("node-a", "node-b", 0.9, "causal".to_string(), 0, 1000);
        let edge2 = create_edge_from_slugs("node-a", "node-c", 0.8, "causal".to_string(), 0, 1000);

        graph.add_edge(&edge1);
        graph.add_edge(&edge2);
        assert_eq!(graph.get_edges_from(&EntityId::new("nodes", "node-a")).len(), 2);

        assert!(graph.remove_edge(
            &EntityId::new("nodes", "node-a"),
            &EntityId::new("nodes", "node-b"),
        ));

        let from_edges = graph.get_edges_from(&EntityId::new("nodes", "node-a"));
        assert_eq!(from_edges.len(), 1);
        assert_eq!(from_edges[0]._to.key(), "node-c");
    }

    #[test]
    fn test_persistent_graph_clear() {
        let temp_dir = TempDir::new().unwrap();
        let graph = PersistentGraph::new(temp_dir.path().join("graph.redb").as_path());

        let edge = create_edge_from_slugs("a", "b", 0.5, "causal".to_string(), 0, 1000);
        graph.add_edge(&edge);

        assert_eq!(graph.node_count(), 1);
        assert_eq!(graph.edge_count(), 1);

        graph.clear();

        assert_eq!(graph.node_count(), 0);
        assert_eq!(graph.edge_count(), 0);
    }
}
