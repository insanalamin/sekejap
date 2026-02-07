//! # Atomic Building Blocks for LLM Composition
//!
//! This module provides composable, low-level operations designed for LLM-generated queries.
//! Each atom is a primitive operation that can be combined to build complex queries.
//!
//! # Design Philosophy
//!
//! - **Small**: Each function does one thing well
//! - **Composable**: Atoms chain together naturally
//! - **Transparent**: Clear input/output types
//! - **Zero-Copy**: Avoid allocations where possible
//!
//! # Basic Usage
//!
//! ```rust,no_run
//! use hsdl_sekejap::{SekejapDB, atoms::*};
//!
//! # fn example(db: &SekejapDB) {
//! // Atom 1: Get all nodes with hierarchy edges
//! let nodes = get_nodes_by_edge_type(db, "hierarchy".to_string());
//!
//! // Atom 2: Filter by metadata
//! let filtered = nodes.into_iter()
//!     .filter(|node| has_metadata_key(node, "type", "city"))
//!     .collect::<Vec<_>>();
//! # }
//! ```
//!
//! # Performance Characteristics
//!
//! | Atom | Complexity | Notes |
//! |-------|-----------|--------|
//! | `get_node` | O(1) | Slug-based lookup |
//! | `get_edges_from` | O(k) | k = outgoing edges |
//! | `traverse_bfs` | O(V + E) | V = vertices, E = edges |
//! | `is_point_in_polygon` | O(m) | m = polygon vertices |
//! | `cosine_similarity` | O(d) | d = vector dimension |

use crate::{EntityId, SekejapDB, EdgeType, NodeHeader};

/// Get a node by its slug
///
/// This is the most efficient way to access a single node.
///
/// # Arguments
///
/// * `db` - Database reference
/// * `slug` - Unique node identifier
///
/// # Returns
///
/// * `Option<NodeHeader>` - Node if found, None otherwise
///
/// # Performance
///
/// O(1) - Hash map lookup in serving layer
///
/// # Examples
///
/// ```rust,no_run
/// # use hsdl_sekejap::{SekejapDB, atoms::*};
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// if let Some(node) = get_node(&db, "jakarta-city") {
///     println!("Found node: {:?}", node);
/// }
/// ```
pub fn get_node(db: &SekejapDB, slug: &str) -> Option<NodeHeader> {
    let slug_hash = crate::hash_slug(slug);
    db.storage().get_by_slug(slug_hash)
}

/// Get all nodes that have an edge of specific type
///
/// Useful for finding all cities, restaurants, crimes, etc.
///
/// # Arguments
///
/// * `db` - Database reference
/// * `edge_type` - Type of edge to filter by
///
/// # Returns
///
/// * `Vec<NodeHeader>` - All nodes with matching edge type
///
/// # Performance
///
/// O(V + E) - Scans all edges in graph
///
/// # Examples
///
/// ```rust,no_run
/// # use hsdl_sekejap::{SekejapDB, atoms::*};
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// // Get all nodes with hierarchy edges (e.g., administrative relationships)
/// let nodes = get_nodes_by_edge_type(&db, "hierarchy".to_string());
/// ```
pub fn get_nodes_by_edge_type(db: &SekejapDB, edge_type: EdgeType) -> Vec<NodeHeader> {
    let graph = db.graph();
    
    // Collect all entity IDs from storage by checking their entity_id field
    let all_nodes: Vec<NodeHeader> = db.storage().all();
    
    // Find all nodes that have edges of this type
    all_nodes
        .into_iter()
        .filter_map(|node| {
            // Use the node's entity_id if available, otherwise create one from slug
            let entity_id = node.entity_id.clone()
                .unwrap_or_else(|| EntityId::new("nodes", ""));
            
            let edges = graph.get_edges_from(&entity_id);
            if edges.iter().any(|e| e._type == edge_type) {
                Some(node)
            } else {
                None
            }
        })
        .collect()
}

/// Get all nodes that point to a specific target
///
/// Useful for finding all cities in a province, or restaurants in a city.
///
/// # Arguments
///
/// * `db` - Database reference
/// * `target_slug` - Target node slug
/// * `edge_type` - Type of edge to filter by
///
/// # Returns
///
/// * `Vec<NodeHeader>` - All source nodes pointing to target
///
/// # Performance
///
/// O(V + E) - Scans all edges in graph
///
/// # Examples
///
/// ```rust,no_run
/// # use hsdl_sekejap::{SekejapDB, atoms::*};
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// // Get all nodes with hierarchy edges pointing to a specific target
/// let children = get_nodes_with_edge_to(&db, "west-java", "hierarchy".to_string());
/// ```
pub fn get_nodes_with_edge_to(db: &SekejapDB, target_slug: &str, edge_type: EdgeType) -> Vec<NodeHeader> {
    let target_entity_id = EntityId::new("nodes".to_string(), target_slug.to_string());
    
    let graph = db.graph();
    
    // Collect all nodes from storage
    let all_nodes: Vec<NodeHeader> = db.storage().all();
    
    // Find all nodes that point to target with specific edge type
    all_nodes
        .into_iter()
        .filter_map(|node| {
            let entity_id = node.entity_id.clone()
                .unwrap_or_else(|| EntityId::new("nodes", ""));
            
            let edges = graph.get_edges_from(&entity_id);
            if edges.iter().any(|e| e._to == target_entity_id && e._type == edge_type) {
                Some(node)
            } else {
                None
            }
        })
        .collect()
}

/// Get outgoing edges from a node
///
/// # Arguments
///
/// * `db` - Database reference
/// * `source_slug` - Source node slug
///
/// # Returns
///
/// * `Vec<crate::WeightedEdge>` - All outgoing edges
///
/// # Performance
///
/// O(k) - k = number of outgoing edges
///
/// # Examples
///
/// ```rust,no_run
/// # use hsdl_sekejap::{SekejapDB, atoms::*};
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// let edges = get_edges_from(&db, "bogor-city");
/// for edge in edges {
///     println!("Edge to: {:?}, weight: {}", edge._to, edge.weight);
/// }
/// ```
pub fn get_edges_from(db: &SekejapDB, source_slug: &str) -> Vec<crate::WeightedEdge> {
    let entity_id = EntityId::new("nodes".to_string(), source_slug.to_string());
    db.graph().get_edges_from(&entity_id)
}

/// Get incoming edges to a node
///
/// # Arguments
///
/// * `db` - Database reference
/// * `target_slug` - Target node slug
///
/// # Returns
///
/// * `Vec<crate::WeightedEdge>` - All incoming edges
///
/// # Performance
///
/// O(k) - k = number of incoming edges
///
/// # Examples
///
/// ```rust,no_run
/// # use hsdl_sekejap::{SekejapDB, atoms::*};
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// let edges = get_edges_to(&db, "nanggung-village");
/// for edge in edges {
///     println!("Edge from: {:?}, weight: {}", edge._from, edge.weight);
/// }
/// ```
pub fn get_edges_to(db: &SekejapDB, target_slug: &str) -> Vec<crate::WeightedEdge> {
    let entity_id = EntityId::new("nodes".to_string(), target_slug.to_string());
    db.graph().get_edges_to(&entity_id)
}

/// Check if node has metadata key with specific value
///
/// # Arguments
///
/// * `node` - Node header
/// * `key` - Metadata key to check
/// * `value` - Expected value
///
/// # Returns
///
/// * `bool` - True if metadata matches
///
/// # Performance
///
/// O(1) - HashMap lookup in metadata
///
/// # Examples
///
/// ```rust,no_run
/// # use hsdl_sekejap::{SekejapDB, atoms::*};
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// # let node = get_node(&db, "jakarta-city").unwrap();
/// if has_metadata_key(&node, "province", "West Java") {
///     println!("This is a West Java city");
/// }
/// ```
pub fn has_metadata_key(_node: &NodeHeader, _key: &str, _value: &str) -> bool {
    // This would need access to NodePayload
    // For now, this is a placeholder
    false
}

/// Get metadata value from node
///
/// # Arguments
///
/// * `db` - Database reference
/// * `node` - Node header
/// * `key` - Metadata key to retrieve
///
/// # Returns
///
/// * `Option<String>` - Value if key exists
///
/// # Performance
///
/// O(1) - HashMap lookup + Blob read
///
/// # Examples
///
/// ```rust,no_run
/// # use hsdl_sekejap::{SekejapDB, atoms::*};
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// # let node = get_node(&db, "jakarta-city").unwrap();
/// if let Some(population) = get_metadata(&db, &node, "population") {
///     println!("Population: {}", population);
/// }
/// ```
pub fn get_metadata(_db: &SekejapDB, _node: &NodeHeader, _key: &str) -> Option<String> {
    // TODO: Implement blob store lookup and JSON parsing
    None
}

/// Check if a point is inside a polygon (requires "spatial" feature)
///
/// # Arguments
///
/// * `lat` - Latitude of point
/// * `lon` - Longitude of point
/// * `node` - Node containing polygon data
///
/// # Returns
///
/// * `bool` - True if point is inside polygon
///
/// # Performance
///
/// O(m) - m = number of polygon vertices
///
/// # Algorithm
///
/// Uses ray casting algorithm (point-in-polygon)
///
/// # Examples
///
/// ```rust,no_run
/// # use hsdl_sekejap::{SekejapDB, atoms::*};
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// # let node = get_node(&db, "bogor-city-boundary").unwrap();
/// if is_point_in_polygon(-6.5950, 106.8170, &node) {
///     println!("This point is in Bogor city");
/// }
/// ```
#[cfg(feature = "spatial")]
pub fn is_point_in_polygon(lat: f64, lon: f64, node: &NodeHeader) -> bool {
    // TODO: Implement ray casting algorithm
    // This would:
    // 1. Get polygon coordinates from metadata
    // 2. Cast ray from point
    // 3. Count intersections
    // 4. Odd = inside, Even = outside
    false
}

/// Calculate distance between two points (Haversine formula)
///
/// # Arguments
///
/// * `lat1`, `lon1` - First point coordinates
/// * `lat2`, `lon2` - Second point coordinates
///
/// # Returns
///
/// * `f64` - Distance in kilometers
///
/// # Performance
///
/// O(1) - Constant time calculation
///
/// # Examples
///
/// ```rust,no_run
/// # use hsdl_sekejap::atoms::*;
/// let distance = haversine_distance(-6.2088, 106.8456, -6.5950, 106.8170);
/// println!("Distance: {:.2} km", distance);
/// ```
pub fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_KM: f64 = 6371.0;
    
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos()
        * (dlon / 2.0).sin().powi(2);
    
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    
    EARTH_RADIUS_KM * c
}

/// Find nodes within radius of a point (requires "spatial" feature)
///
/// # Arguments
///
/// * `db` - Database reference
/// * `lat`, `lon` - Center point coordinates
/// * `radius_km` - Search radius in kilometers
///
/// # Returns
///
/// * `Vec<NodeHeader>` - Nodes within radius
///
/// # Performance
///
/// O(n) - n = total nodes (linear scan with HashMap stub)
/// O(log n) - n = total nodes (with R-tree spatial index)
///
/// # Examples
///
/// ```rust,no_run
/// # use hsdl_sekejap::{SekejapDB, atoms::*};
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// // Find all cities within 50km of Jakarta
/// let nearby = find_within_radius(&db, -6.2088, 106.8456, 50.0);
/// println!("Found {} nearby cities", nearby.len());
/// ```
#[cfg(feature = "spatial")]
pub fn find_within_radius(
    db: &SekejapDB,
    lat: f64,
    lon: f64,
    radius_km: f64,
) -> Vec<NodeHeader> {
    // Get all nodes (would use spatial index in production)
    let all_nodes: Vec<NodeHeader> = db.storage().all();
    
    all_nodes
        .into_iter()
        .filter(|_| {
            // NodeHeader doesn't have coordinates field directly
            // For now, skip filtering - TODO: get coordinates from payload
            false
        })
        .collect()
}

/// Calculate cosine similarity between two vectors (requires "vector" feature)
///
/// # Arguments
///
/// * `vec1` - First vector
/// * `vec2` - Second vector
///
/// # Returns
///
/// * `f32` - Similarity score (0.0 to 1.0)
///
/// # Performance
///
/// O(d) - d = vector dimension
///
/// # Examples
///
/// ```rust,no_run
/// # use hsdl_sekejap::atoms::*;
/// let vec1 = vec![0.1, 0.2, 0.3];
/// let vec2 = vec![0.2, 0.3, 0.4];
/// let similarity = cosine_similarity(&vec1, &vec2);
/// println!("Similarity: {:.3}", similarity);
/// ```
#[cfg(feature = "vector")]
pub fn cosine_similarity(vec1: &[f32], vec2: &[f32]) -> f32 {
    if vec1.len() != vec2.len() {
        return 0.0;
    }
    
    let dot_product: f32 = vec1.iter()
        .zip(vec2.iter())
        .map(|(a, b)| a * b)
        .sum();
    
    let mag1: f32 = vec1.iter().map(|x| x * x).sum::<f32>().sqrt();
    let mag2: f32 = vec2.iter().map(|x| x * x).sum::<f32>().sqrt();
    
    if mag1 == 0.0 || mag2 == 0.0 {
        return 0.0;
    }
    
    dot_product / (mag1 * mag2)
}

/// Find most similar vectors (requires "vector" feature)
///
/// # Arguments
///
/// * `db` - Database reference
/// * `query_vector` - Query vector
/// * `top_k` - Number of results to return
/// * `threshold` - Minimum similarity score
///
/// # Returns
///
/// * `Vec<(NodeHeader, f32)>` - Top-k similar nodes with scores
///
/// # Performance
///
/// O(n * d) - n = total nodes, d = vector dimension
///
/// # Examples
///
/// ```rust,no_run
/// # use hsdl_sekejap::{SekejapDB, atoms::*};
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// let query = vec![0.1, 0.2, 0.3, 0.4];
/// let similar = find_similar_vectors(&db, &query, 5, 0.7);
/// for (node, score) in similar {
///     println!("Score: {:.3}, Node: {:?}", score, node);
/// }
/// ```
#[cfg(feature = "vector")]
pub fn find_similar_vectors(
    db: &SekejapDB,
    query_vector: &[f32],
    top_k: usize,
    threshold: f32,
) -> Vec<(NodeHeader, f32)> {
    // Get all nodes with vectors
    let all_nodes: Vec<NodeHeader> = db.storage().all();
    
    let mut similarities: Vec<(NodeHeader, f32)> = all_nodes
        .into_iter()
        .filter_map(|node| {
            // TODO: Get vectors from node payload
            let node_vectors: Option<Vec<f32>> = None;
            
            node_vectors.and_then(|v| {
                let score = cosine_similarity(query_vector, &v);
                if score >= threshold {
                    Some((node, score))
                } else {
                    None
                }
            })
        }).collect();
    
    // Sort by similarity (descending)
    similarities.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    
    // Return top-k
    similarities.into_iter().take(top_k).collect()
}

/// Traverse graph in BFS pattern
///
/// # Arguments
///
/// * `db` - Database reference
/// * `start_slug` - Starting node slug
/// * `max_depth` - Maximum traversal depth
///
/// # Returns
///
/// * `Vec<NodeHeader>` - All nodes in traversal path
///
/// # Performance
///
/// O(V + E) - V = vertices, E = edges
///
/// # Examples
///
/// ```rust,no_run
/// # use hsdl_sekejap::{SekejapDB, atoms::*};
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// // Find all villages in Bogor regency
/// let hierarchy = traverse_bfs(&db, "bogor-regency", 3);
/// println!("Found {} administrative units", hierarchy.len());
/// ```
pub fn traverse_bfs(
    db: &SekejapDB,
    start_slug: &str,
    max_depth: usize,
) -> Vec<NodeHeader> {
    let start_entity_id = EntityId::new("nodes".to_string(), start_slug.to_string());
    let start_slug_hash = crate::hash_slug(start_slug);
    let graph = db.graph();
    
    let mut visited = std::collections::HashSet::new();
    let mut queue = std::collections::VecDeque::new();
    let mut result = Vec::new();
    
    queue.push_back((start_entity_id.clone(), 0));
    visited.insert(start_entity_id.clone());
    
    // Get the start node and add to results
    if let Some(start_node) = db.storage().get_by_slug(start_slug_hash) {
        result.push(start_node);
    }
    
    while let Some((entity_id, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
        
        for edge in graph.get_edges_from(&entity_id) {
            let target_id = edge._to.clone();
            let target_slug = target_id.key();
            let target_slug_hash = crate::hash_slug(target_slug);
            
            if !visited.contains(&target_id) {
                visited.insert(target_id.clone());
                queue.push_back((target_id.clone(), depth + 1));
                
                // Find node by slug hash (more efficient than entity_id matching)
                if let Some(node) = db.storage().get_by_slug(target_slug_hash) {
                    result.push(node);
                }
            }
        }
    }
    
    result
}

/// Traverse graph backward (for root cause analysis)
///
/// # Arguments
///
/// * `db` - Database reference
/// * `start_slug` - Starting node slug
/// * `max_depth` - Maximum traversal depth
///
/// # Returns
///
/// * `Vec<NodeHeader>` - All ancestor nodes in traversal
///
/// # Performance
///
/// O(V + E) - V = vertices, E = edges
///
/// # Examples
///
/// ```rust,no_run
/// # use hsdl_sekejap::{SekejapDB, atoms::*};
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// // Find root causes of a crime
/// let root_causes = traverse_backward(&db, "theft-incident", 3);
/// for cause in root_causes {
///     println!("Potential cause: {:?}", cause);
/// }
/// ```
pub fn traverse_backward(
    db: &SekejapDB,
    start_slug: &str,
    max_depth: usize,
) -> Vec<NodeHeader> {
    let start_entity_id = EntityId::new("nodes".to_string(), start_slug.to_string());
    
    let graph = db.graph();
    let result = graph.backward_bfs(&start_entity_id, max_depth, 0.0, None);
    
    // Find nodes by entity_id
    let all_nodes: Vec<NodeHeader> = db.storage().all();
    
    result.path.into_iter()
        .filter_map(|entity_id| {
            all_nodes.iter()
                .find(|node| node.entity_id.as_ref() == Some(&entity_id))
                .cloned()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_haversine_distance() {
        // Jakarta to Bogor (actual distance ~43km based on coordinates)
        let dist = haversine_distance(-6.2088, 106.8456, -6.5950, 106.8170);
        // Allow 5km tolerance
        assert!((dist - 43.0).abs() < 5.0, "Distance should be ~43km, got: {:.2}km", dist);
        
        // Test zero distance
        let zero_dist = haversine_distance(-6.2088, 106.8456, -6.2088, 106.8456);
        assert!(zero_dist.abs() < 0.1, "Zero distance should be ~0");
    }

    #[cfg(feature = "vector")]
    #[test]
    fn test_cosine_similarity() {
        let vec1 = vec![1.0, 0.0, 0.0];
        let vec2 = vec![1.0, 0.0, 0.0];
        let sim = cosine_similarity(&vec1, &vec2);
        assert!((sim - 1.0).abs() < 0.001, "Identical vectors should have similarity 1.0");
        
        let vec3 = vec![0.0, 1.0, 0.0];
        let sim2 = cosine_similarity(&vec1, &vec3);
        assert!(sim2 < 0.5, "Perpendicular vectors should have low similarity");
    }
}
