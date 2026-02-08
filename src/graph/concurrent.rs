//! Concurrent Causal Graph
//!
//! Thread-safe implementation of causal graph using DashMap
//! for concurrent access in Axum/async environments.

use crate::{EdgeType, EntityId, WeightedEdge};
use dashmap::DashMap;
use std::collections::{HashSet, VecDeque};

/// Thread-safe Causal Graph for concurrent access
///
/// Uses DashMap for edge storage allowing multiple threads
/// to read/write edges concurrently without locking the entire graph.
pub struct ConcurrentGraph {
    /// Adjacency list: EntityId -> Vec of outgoing edges
    edges: DashMap<EntityId, Vec<WeightedEdge>>,

    /// Reverse adjacency list: EntityId -> Vec of incoming edges
    reverse_edges: DashMap<EntityId, Vec<WeightedEdge>>,
}

impl ConcurrentGraph {
    /// Create a new concurrent graph
    pub fn new() -> Self {
        Self {
            edges: DashMap::new(),
            reverse_edges: DashMap::new(),
        }
    }

    /// Add an edge to the graph
    ///
    /// # Arguments
    /// * `edge` - Edge to add
    pub fn add_edge(&self, edge: WeightedEdge) {
        let from = edge._from.clone();
        let to = edge._to.clone();

        self.edges.entry(from).or_default().push(edge.clone());

        self.reverse_edges.entry(to).or_default().push(edge);
    }

    /// Get all outgoing edges from a node
    ///
    /// # Arguments
    /// * `entity_id` - Entity ID to get edges from
    ///
    /// # Returns
    /// * `Vec<WeightedEdge>` - Outgoing edges
    pub fn get_edges_from(&self, entity_id: &EntityId) -> Vec<WeightedEdge> {
        self.edges
            .get(entity_id)
            .map(|r| r.value().clone())
            .unwrap_or_default()
    }

    /// Get all incoming edges to a node
    ///
    /// # Arguments
    /// * `entity_id` - Entity ID to get edges to
    ///
    /// # Returns
    /// * `Vec<WeightedEdge>` - Incoming edges
    pub fn get_edges_to(&self, entity_id: &EntityId) -> Vec<WeightedEdge> {
        self.reverse_edges
            .get(entity_id)
            .map(|r| r.value().clone())
            .unwrap_or_default()
    }

    /// Remove a node and all its edges
    ///
    /// # Arguments
    /// * `entity_id` - Entity ID to remove
    pub fn remove_node(&self, entity_id: &EntityId) {
        // Remove all outgoing edges
        if let Some((_, edges)) = self.edges.remove(entity_id) {
            // Clean up reverse edges
            for edge in edges {
                if let Some(mut rev_edges) = self.reverse_edges.get_mut(&edge._to) {
                    rev_edges.retain(|e| e._from != *entity_id);
                }
            }
        }

        // Remove all incoming edges
        if let Some((_, rev_edges)) = self.reverse_edges.remove(entity_id) {
            // Clean up forward edges
            for edge in rev_edges {
                if let Some(mut edges) = self.edges.get_mut(&edge._from) {
                    edges.retain(|e| e._to != *entity_id);
                }
            }
        }
    }

    /// Remove a specific edge
    ///
    /// # Arguments
    /// * `from` - Source entity ID
    /// * `to` - Target entity ID
    /// * `edge_type` - Edge type (optional, None removes any type)
    ///
    /// # Returns
    /// * `bool` - True if edge was found and removed
    pub fn remove_edge(&self, from: &EntityId, to: &EntityId, edge_type: Option<EdgeType>) -> bool {
        let mut removed = false;

        // Remove from forward adjacency list
        if let Some(mut edges) = self.edges.get_mut(from) {
            let original_len = edges.len();
            if let Some(ref et) = edge_type {
                edges.retain(|e| !(e._from == *from && e._to == *to && e._type == *et));
            } else {
                edges.retain(|e| !(e._from == *from && e._to == *to));
            }
            removed = edges.len() < original_len;
        }

        // Remove from reverse adjacency list
        if let Some(mut rev_edges) = self.reverse_edges.get_mut(to) {
            if let Some(ref et) = edge_type {
                rev_edges.retain(|e| !(e._from == *from && e._to == *to && e._type == *et));
            } else {
                rev_edges.retain(|e| !(e._from == *from && e._to == *to));
            }
        }

        removed
    }

    /// Backward BFS traversal for Root Cause Analysis
    ///
    /// # Arguments
    /// * `start_entity_id` - Starting entity ID
    /// * `max_hops` - Maximum number of hops to traverse
    /// * `weight_threshold` - Minimum edge weight to consider (0.0 - 1.0)
    /// * `edge_type_filter` - Optional filter by edge type
    /// * `time_window` - Optional time window constraint (start_ms, end_ms)
    ///
    /// # Returns
    /// * `TraversalResult` - Path, edges, and total weight
    pub fn backward_bfs(
        &self,
        start_entity_id: &EntityId,
        max_hops: usize,
        weight_threshold: f32,
        edge_type_filter: Option<&str>,
        time_window: Option<(u64, u64)>,
    ) -> crate::TraversalResult {
        use crate::TraversalResult;

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut path = Vec::new();
        let mut edges = Vec::new();

        queue.push_back((start_entity_id.clone(), 0, 1.0f32));
        visited.insert(start_entity_id.clone());
        path.push(start_entity_id.clone());

        while let Some((entity_id, depth, cumulative_weight)) = queue.pop_front() {
            if depth >= max_hops {
                continue;
            }

            // Get incoming edges (backward traversal)
            if let Some(rev_edges) = self.reverse_edges.get(&entity_id) {
                for edge in rev_edges.value().iter() {
                    // Check weight threshold
                    if edge.weight < weight_threshold {
                        continue;
                    }

                    // Filter by edge type
                    if let Some(filter) = edge_type_filter {
                        if edge._type != filter {
                            continue;
                        }
                    }

                    // Check time window - edge is valid if its validity range overlaps with window
                    if let Some((window_start, window_end)) = time_window {
                        // Edge must be valid at some point within the window
                        let edge_valid_in_window = edge.valid_start < window_end
                            && edge.valid_end.is_none_or(|e| e > window_start);

                        if !edge_valid_in_window {
                            continue;
                        }
                    }

                    if !visited.contains(&edge._from) {
                        visited.insert(edge._from.clone());
                        queue.push_back((
                            edge._from.clone(),
                            depth + 1,
                            cumulative_weight * edge.weight,
                        ));
                        // Add to path
                        if !path.contains(&edge._from) {
                            path.push(edge._from.clone());
                        }
                        edges.push(edge.clone());
                    }
                }
            }
        }

        let path_len = path.len() as f32;
        TraversalResult {
            path,
            edges,
            total_weight: path_len,
        }
    }

    /// Forward BFS traversal for JOIN operations
    ///
    /// Given a starting node, find all nodes it points TO.
    /// This is ESSENTIAL for implementing graph-based JOINs.
    ///
    /// # Arguments
    /// * `start_entity_id` - Starting entity ID
    /// * `max_hops` - Maximum number of hops to traverse
    /// * `weight_threshold` - Minimum edge weight to consider (0.0 - 1.0)
    /// * `edge_type_filter` - Optional filter by edge type
    /// * `time_window` - Optional time window constraint (start_ms, end_ms)
    ///
    /// # Returns
    /// * `TraversalResult` - Path, edges, and total weight
    pub fn forward_bfs(
        &self,
        start_entity_id: &EntityId,
        max_hops: usize,
        weight_threshold: f32,
        edge_type_filter: Option<&str>,
        time_window: Option<(u64, u64)>,
    ) -> crate::TraversalResult {
        use crate::TraversalResult;

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut path = Vec::new();
        let mut edges = Vec::new();

        queue.push_back((start_entity_id.clone(), 0));
        visited.insert(start_entity_id.clone());
        path.push(start_entity_id.clone());

        while let Some((entity_id, depth)) = queue.pop_front() {
            if depth >= max_hops {
                continue;
            }

            // Get outgoing edges (forward traversal - FOR JOINS!)
            if let Some(out_edges) = self.edges.get(&entity_id) {
                for edge in out_edges.value().iter() {
                    // Check weight threshold
                    if edge.weight < weight_threshold {
                        continue;
                    }

                    // Filter by edge type
                    if let Some(filter) = edge_type_filter {
                        if edge._type != filter {
                            continue;
                        }
                    }

                    // Check time window
                    if let Some((window_start, window_end)) = time_window {
                        let edge_valid_in_window = edge.valid_start < window_end
                            && edge.valid_end.is_none_or(|e| e > window_start);

                        if !edge_valid_in_window {
                            continue;
                        }
                    }

                    if !visited.contains(&edge._to) {
                        visited.insert(edge._to.clone());
                        queue.push_back((edge._to.clone(), depth + 1));
                        path.push(edge._to.clone());
                        edges.push(edge.clone());
                    }
                }
            }
        }

        let total_weight = edges.iter().map(|e| e.weight).sum();
        TraversalResult {
            path,
            edges,
            total_weight,
        }
    }

    /// Check if graph is empty
    pub fn is_empty(&self) -> bool {
        self.edges.is_empty()
    }

    /// Get number of edges in graph
    pub fn len(&self) -> usize {
        self.edges.iter().map(|r| r.value().len()).sum()
    }

    /// Get edges by slug (convenience method)
    pub fn get_edges_from_slug(&self, slug: &str) -> Vec<WeightedEdge> {
        let entity_id = EntityId::new("nodes", slug.to_string());
        self.get_edges_from(&entity_id)
    }

    /// Get edges to slug (convenience method)
    pub fn get_edges_to_slug(&self, slug: &str) -> Vec<WeightedEdge> {
        let entity_id = EntityId::new("nodes", slug.to_string());
        self.get_edges_to(&entity_id)
    }

    /// Get all unique edges in the graph (iterator-like)
    /// Returns a vector of unique edges (deduplicated)
    pub fn iter(&self) -> Vec<WeightedEdge> {
        use std::collections::HashSet;
        let mut seen = HashSet::new();
        let mut edges = Vec::new();

        for entry in self.edges.iter() {
            for edge in entry.value() {
                // Use a simple hash of the edge for deduplication
                let edge_key = format!("{}-{}-{}", edge._from, edge._to, edge._type);
                if !seen.contains(&edge_key) {
                    seen.insert(edge_key);
                    edges.push(edge.clone());
                }
            }
        }

        edges
    }
}
