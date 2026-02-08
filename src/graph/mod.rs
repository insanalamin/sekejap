//! Causal Graph (Tier 3)
//! Hyperminimalist implementation using in-memory adjacency lists

pub mod bitmap;
pub mod bloom;
pub mod concurrent;
pub mod csr;

use crate::{EdgeType, EntityId, WeightedEdge};
use std::collections::{HashMap, HashSet};
use std::mem;

pub use bitmap::Bitmap;
pub use bloom::{BloomConfig, BloomFilter};
pub use concurrent::ConcurrentGraph;
pub use csr::{CsrConfig, CsrEdge, CsrMatrix};

/// Result of graph traversal
#[derive(Debug, Clone)]
pub struct TraversalResult {
    pub path: Vec<EntityId>,
    pub edges: Vec<WeightedEdge>,
    pub total_weight: f32,
}

/// Edge data for batch insertion (avoids EntityId allocation overhead)
#[derive(Debug, Clone)]
struct BatchEdge {
    from_key: String, // EntityId key (avoid allocation)
    from_collection: String,
    to_key: String,
    to_collection: String,
    weight: f32,
    edge_type: String,
    timestamp: u64,
}

impl BatchEdge {
    fn to_weighted_edge(&self) -> WeightedEdge {
        WeightedEdge::new(
            EntityId::new(self.from_collection.clone(), self.from_key.clone()),
            EntityId::new(self.to_collection.clone(), self.to_key.clone()),
            self.weight,
            EdgeType::from(self.edge_type.clone()),
            0,
            self.timestamp,
            None,
        )
    }
}

/// Tier 3: Causal Graph for Root Cause Analysis
pub struct CausalGraph {
    edges: HashMap<EntityId, Vec<WeightedEdge>>, // outgoing edges
    incoming: HashMap<EntityId, Vec<EntityId>>,  // for reverse traversal
    // Batch insertion buffer
    edge_buffer: Vec<BatchEdge>,
    buffer_capacity: usize,
}

impl Default for CausalGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl CausalGraph {
    pub fn new() -> Self {
        Self {
            edges: HashMap::new(),
            incoming: HashMap::new(),
            edge_buffer: Vec::with_capacity(1_000_000),
            buffer_capacity: 1_000_000,
        }
    }

    /// Set buffer capacity for batch inserts
    pub fn set_buffer_capacity(&mut self, capacity: usize) {
        self.buffer_capacity = capacity;
        self.edge_buffer.shrink_to_fit();
    }

    /// Add an edge to the graph
    pub fn add_edge(&mut self, edge: WeightedEdge) {
        let from = edge._from.clone();
        let to = edge._to.clone();

        self.edges
            .entry(from.clone())
            .or_default()
            .push(edge.clone());

        self.incoming.entry(to.clone()).or_default().push(from);
    }

    /// Add edge in batch mode (buffer instead of immediate insert)
    /// Much faster for bulk loads - defers actual insertion
    pub fn add_edge_batch(
        &mut self,
        from_key: &str,
        from_collection: &str,
        to_key: &str,
        to_collection: &str,
        weight: f32,
        edge_type: &str,
        timestamp: u64,
    ) {
        // Flush buffer if full
        if self.edge_buffer.len() >= self.buffer_capacity {
            self.flush_edge_buffer();
        }

        self.edge_buffer.push(BatchEdge {
            from_key: from_key.to_string(),
            from_collection: from_collection.to_string(),
            to_key: to_key.to_string(),
            to_collection: to_collection.to_string(),
            weight,
            edge_type: edge_type.to_string(),
            timestamp,
        });
    }

    /// Add multiple edges in batch (bulk operation)
    pub fn add_edges_batch(
        &mut self,
        edges: &[(String, String, String, String, f32, String, u64)],
    ) {
        for edge in edges {
            self.add_edge_batch(&edge.0, &edge.1, &edge.2, &edge.3, edge.4, &edge.5, edge.6);
        }
    }

    /// Flush buffered edges to graph
    pub fn flush_edge_buffer(&mut self) {
        if self.edge_buffer.is_empty() {
            return;
        }

        // Sort by source for better cache locality
        self.edge_buffer.sort_by_key(|e| e.from_key.clone());

        // Build adjacency lists in one pass (allocate once per src)
        let mut current_src: Option<(String, String)> = None;
        let mut current_edges: Vec<WeightedEdge> = Vec::new();

        for batch_edge in &self.edge_buffer {
            let src = (
                batch_edge.from_key.clone(),
                batch_edge.from_collection.clone(),
            );

            if current_src.as_ref() != Some(&src) {
                // Flush previous source's edges
                if let Some((ref from_key, ref from_collection)) = current_src {
                    let entity = EntityId::new(from_collection.clone(), from_key.clone());
                    let edges = mem::take(&mut current_edges);
                    self.edges.insert(entity.clone(), edges);
                    for edge in &self.edges[&entity] {
                        self.incoming
                            .entry(edge._to.clone())
                            .or_default()
                            .push(entity.clone());
                    }
                }
                current_src = Some(src);
                current_edges = Vec::new();
            }

            current_edges.push(batch_edge.to_weighted_edge());
        }

        // Flush last source
        if let Some((ref from_key, ref from_collection)) = current_src {
            let entity = EntityId::new(from_collection.clone(), from_key.clone());
            let edges = mem::take(&mut current_edges);
            self.edges.insert(entity.clone(), edges);
            for edge in &self.edges[&entity] {
                self.incoming
                    .entry(edge._to.clone())
                    .or_default()
                    .push(entity.clone());
            }
        }

        self.edge_buffer.clear();
    }

    /// Get outgoing edges from a node
    pub fn outgoing(&self, entity_id: &EntityId) -> Vec<WeightedEdge> {
        self.edges.get(entity_id).cloned().unwrap_or_default()
    }

    /// Get incoming edges to a node
    pub fn incoming(&self, entity_id: &EntityId) -> Vec<WeightedEdge> {
        // Find edges where this node is the target
        self.edges
            .values()
            .flat_map(|edges: &Vec<WeightedEdge>| edges.iter())
            .filter(|e| e._to == *entity_id)
            .cloned()
            .collect()
    }

    /// Backward BFS traversal for RCA
    pub fn backward_bfs(
        &self,
        start_entity: &EntityId,
        max_hops: usize,
        weight_threshold: f32,
        edge_type_filter: Option<&str>,
        time_window: Option<(u64, u64)>,
    ) -> TraversalResult {
        let mut visited = HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        let mut path = Vec::new();
        let mut edges = Vec::new();
        let mut total_weight = 0.0;

        queue.push_back((start_entity.clone(), 0));
        visited.insert(start_entity.clone());

        while let Some((entity_id, depth)) = queue.pop_front() {
            if depth >= max_hops {
                continue;
            }

            for edge in self.incoming(&entity_id) {
                if !edge.meets_threshold(weight_threshold) {
                    continue;
                }

                // Filter by edge type
                if let Some(filter) = edge_type_filter {
                    if edge._type != filter {
                        continue;
                    }
                }

                if let Some((start, end)) = time_window
                    && (!edge.is_valid_at(start) || edge.is_valid_at(end))
                {
                    continue;
                }

                let source = edge._from.clone();
                if !visited.contains(&source) {
                    visited.insert(source.clone());
                    queue.push_back((source.clone(), depth + 1));
                    path.push(source);
                    edges.push(edge.clone());
                    total_weight += edge.weight;
                }
            }
        }

        TraversalResult {
            path,
            edges,
            total_weight,
        }
    }

    /// Forward BFS traversal for JOIN operations
    ///
    /// Given a starting node, find all nodes it points TO.
    /// This is ESSENTIAL for implementing graph-based JOINs.
    ///
    /// # Example (INNER JOIN: restaurants -> cuisines)
    /// ```rust,ignore
    /// // For each restaurant, traverse FORWARD to find cuisines
    /// for restaurant in restaurants {
    ///     let result = graph.forward_bfs(&restaurant, 1, 0.0, None, None);
    ///     for edge in &result.edges {
    ///         if edge._type == "related" {
    ///             // JOIN: restaurant -> cuisine
    ///             join_results.push((restaurant, edge._to.clone()));
    ///         }
    ///     }
    /// }
    /// ```
    pub fn forward_bfs(
        &self,
        start_entity: &EntityId,
        max_hops: usize,
        weight_threshold: f32,
        edge_type_filter: Option<&str>,
        time_window: Option<(u64, u64)>,
    ) -> TraversalResult {
        let mut visited = HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        let mut path = Vec::new();
        let mut edges = Vec::new();
        let mut total_weight = 0.0;

        queue.push_back((start_entity.clone(), 0));
        visited.insert(start_entity.clone());

        while let Some((entity_id, depth)) = queue.pop_front() {
            if depth >= max_hops {
                continue;
            }

            // Get OUTGOING edges (forward traversal - FOR JOINS!)
            for edge in self.outgoing(&entity_id) {
                if !edge.meets_threshold(weight_threshold) {
                    continue;
                }

                // Filter by edge type (e.g., only "related" for cuisine joins)
                if let Some(filter) = edge_type_filter {
                    if edge._type != filter {
                        continue;
                    }
                }

                // Check time window
                if let Some((start, end)) = time_window
                    && (!edge.is_valid_at(start) || edge.is_valid_at(end))
                {
                    continue;
                }

                let target = edge._to.clone();
                if !visited.contains(&target) {
                    visited.insert(target.clone());
                    queue.push_back((target.clone(), depth + 1));
                    path.push(target);
                    edges.push(edge.clone());
                    total_weight += edge.weight;
                }
            }
        }

        TraversalResult {
            path,
            edges,
            total_weight,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_edge() {
        let mut graph = CausalGraph::new();

        let edge = WeightedEdge::new(
            EntityId::new("nodes", "1"),
            EntityId::new("nodes", "2"),
            0.85,
            "causal".to_string(),
            100,
            1700000000000,
            None,
        );

        graph.add_edge(edge);
        assert_eq!(graph.outgoing(&EntityId::new("nodes", "1")).len(), 1);
    }

    #[test]
    fn test_backward_bfs() {
        let mut graph = CausalGraph::new();

        // Create chain: 3 -> 2 -> 1
        graph.add_edge(WeightedEdge::new(
            EntityId::new("nodes", "3"),
            EntityId::new("nodes", "2"),
            0.9,
            "causal".to_string(),
            100,
            1700000000000,
            None,
        ));

        graph.add_edge(WeightedEdge::new(
            EntityId::new("nodes", "2"),
            EntityId::new("nodes", "1"),
            0.8,
            "causal".to_string(),
            101,
            1700000000000,
            None,
        ));

        let result = graph.backward_bfs(&EntityId::new("nodes", "1"), 2, 0.5, None, None);
        assert!(result.path.iter().any(|e| e.key() == "2"));
        assert!(result.path.iter().any(|e| e.key() == "3"));
    }

    #[test]
    fn test_forward_bfs() {
        let mut graph = CausalGraph::new();

        // Create chain: 1 -> 2 -> 3
        graph.add_edge(WeightedEdge::new(
            EntityId::new("nodes", "1"),
            EntityId::new("nodes", "2"),
            0.9,
            "causal".to_string(),
            100,
            1700000000000,
            None,
        ));

        graph.add_edge(WeightedEdge::new(
            EntityId::new("nodes", "2"),
            EntityId::new("nodes", "3"),
            0.8,
            "causal".to_string(),
            101,
            1700000000000,
            None,
        ));

        // Forward BFS from 1 should find 2 and 3
        let result = graph.forward_bfs(&EntityId::new("nodes", "1"), 2, 0.5, None, None);
        assert!(result.path.iter().any(|e| e.key() == "2"));
        assert!(result.path.iter().any(|e| e.key() == "3"));
        assert_eq!(result.edges.len(), 2);
    }

    #[test]
    fn test_forward_bfs_with_edge_type_filter() {
        let mut graph = CausalGraph::new();

        // Create edges with different types
        graph.add_edge(WeightedEdge::new(
            EntityId::new("nodes", "restaurant-1"),
            EntityId::new("nodes", "cuisine-italian"),
            0.95,
            "related".to_string(),
            100,
            1700000000000,
            None,
        ));

        graph.add_edge(WeightedEdge::new(
            EntityId::new("nodes", "restaurant-1"),
            EntityId::new("nodes", "location-jakarta"),
            0.8,
            "located_at".to_string(),
            101,
            1700000000000,
            None,
        ));

        // Filter by "related" type - should only find cuisine
        let result = graph.forward_bfs(
            &EntityId::new("nodes", "restaurant-1"),
            1,
            0.0,
            Some("related"),
            None,
        );
        assert_eq!(result.path.len(), 1);
        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.path[0].key(), "cuisine-italian");
    }
}
