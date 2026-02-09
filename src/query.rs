//! Query builder for multi-modal database queries
//!
//! This module provides a chainable query builder API with optional features:
//! - Vector similarity search (requires "vector" feature)
//! - Spatial radius/intersection queries (requires "spatial" feature)
//! - Fulltext search (requires "fulltext" feature)
//!
//! All features are optional and have zero performance impact when disabled.

use crate::{EdgeType, NodeHeader, NodeId, NodePayload, SekejapDB, SlugHash};
use std::collections::HashSet;

/// Query builder for multi-modal searches
///
/// # Example
/// ```rust,no_run
/// # use sekejap::SekejapDB;
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// // Simple query by slug
/// let results = db.query().by_slug("jakarta-crime-2024").execute()?;
///
/// # #[cfg(feature = "spatial")]
/// # {
/// // Multi-modal query (requires features)
/// let results = db.query()
///     .spatial(-6.2088, 106.8456, 5.0)?
///     .execute()?;
/// # }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Query<'db> {
    db: &'db SekejapDB,
    filters: QueryFilters,
}

/// Internal filter storage
#[derive(Default)]
#[allow(dead_code)]
struct QueryFilters {
    slug_hash: Option<SlugHash>,
    spatial_center: Option<(f64, f64)>,
    spatial_radius_km: Option<f64>,
    vector_query: Option<Vec<f32>>,
    vector_k: Option<usize>,
    fulltext_query: Option<String>,
    edge_target: Option<String>,
    edge_source: Option<String>,
    edge_type: Option<EdgeType>,
    limit: Option<usize>,
}

impl<'db> Query<'db> {
    /// Create a new query builder
    pub fn new(db: &'db SekejapDB) -> Self {
        Self {
            db,
            filters: QueryFilters::default(),
        }
    }

    /// Filter by exact slug
    pub fn by_slug(mut self, slug: &str) -> Self {
        use crate::{hash_slug, EntityId};
        let entity_id = EntityId::parse(slug)
            .unwrap_or_else(|_| EntityId::new("nodes".to_string(), slug.to_string()));
        self.filters.slug_hash = Some(hash_slug(&entity_id.to_string()));
        self
    }

    /// Filter by geographic radius (requires "spatial" feature)
    #[cfg(feature = "spatial")]
    pub fn spatial(
        mut self,
        lat: f64,
        lon: f64,
        radius_km: f64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        self.filters.spatial_center = Some((lat, lon));
        self.filters.spatial_radius_km = Some(radius_km);
        Ok(self)
    }

    /// Filter by vector similarity (requires "vector" feature)
    #[cfg(feature = "vector")]
    pub fn vector_search(mut self, query_vector: Vec<f32>, k: usize) -> Self {
        self.filters.vector_query = Some(query_vector);
        self.filters.vector_k = Some(k);
        self
    }

    /// Filter by fulltext search (requires "fulltext" feature)
    #[cfg(feature = "fulltext")]
    pub fn fulltext(mut self, query: &str) -> Result<Self, Box<dyn std::error::Error>> {
        self.filters.fulltext_query = Some(query.to_string());
        Ok(self)
    }

    /// Filter by edge relationship to target node
    pub fn has_edge_to(mut self, target_slug: &str, edge_type: EdgeType) -> Self {
        self.filters.edge_target = Some(target_slug.to_string());
        self.filters.edge_type = Some(edge_type);
        self
    }

    /// Filter by edge relationship from source node
    pub fn has_edge_from(mut self, source_slug: &str, edge_type: EdgeType) -> Self {
        self.filters.edge_source = Some(source_slug.to_string());
        self.filters.edge_type = Some(edge_type);
        self
    }

    /// Limit number of results
    pub fn limit(mut self, n: usize) -> Self {
        self.filters.limit = Some(n);
        self
    }

    /// Execute query and return results
    ///
    /// Implements "Index Intersection" strategy for high performance:
    /// 1. Collect candidates from "Driver" indices (Text, Geo, Vector, Graph)
    /// 2. Intersect candidates efficiently
    /// 3. Apply remaining filters on the reduced set
    pub fn execute(&self) -> Result<Vec<NodeHeader>, Box<dyn std::error::Error>> {
        let mut candidates: Option<HashSet<NodeId>> = None;

        // Helper to intersect sets
        let intersect =
            |current: Option<HashSet<NodeId>>, new: HashSet<NodeId>| -> Option<HashSet<NodeId>> {
                match current {
                    Some(c) => Some(c.intersection(&new).cloned().collect()),
                    None => Some(new),
                }
            };

        // 1. Driver: Fulltext (O(log N))
        #[cfg(feature = "fulltext")]
        if let Some(ref q) = self.filters.fulltext_query {
            let limit = self.filters.limit.unwrap_or(100); // Default limit for text search
            let hits = self.db.search_text(q, limit)?;
            println!("DEBUG: Fulltext('{}') returned {} hits", q, hits.len());
            let ids: HashSet<_> = hits.into_iter().collect();
            candidates = intersect(candidates, ids);
            if let Some(c) = &candidates {
                println!("DEBUG: Candidates after Fulltext: {}", c.len());
            }
        }

        // 2. Driver: Spatial (O(log N))
        #[cfg(feature = "spatial")]
        if let (Some((lat, lon)), Some(radius)) =
            (self.filters.spatial_center, self.filters.spatial_radius_km)
        {
            let hits = self.db.search_spatial(lat, lon, radius)?;
            println!("DEBUG: Spatial returned {} hits", hits.len());
            let ids: HashSet<_> = hits.into_iter().collect();
            candidates = intersect(candidates, ids);
            if let Some(c) = &candidates {
                println!("DEBUG: Candidates after Spatial: {}", c.len());
            }
        }

        // 3. Driver: Vector (O(log N) with HNSW)
        #[cfg(feature = "vector")]
        if let Some(ref q) = self.filters.vector_query {
            let k = self.filters.vector_k.unwrap_or(10);
            let hits = self.db.search_vector(q, k)?;
            println!("DEBUG: Vector search returned {} hits", hits.len());
            let ids: HashSet<_> = hits.into_iter().map(|(id, _)| id).collect();
            candidates = intersect(candidates, ids);
            if let Some(c) = &candidates {
                println!("DEBUG: Candidates after Vector: {}", c.len());
            }
        }

        // 4. Driver: Graph (Traversal)
        // If edge_target is set, we want nodes that point TO target (incoming to target, or source of edge)
        // This is "reverse" traversal if we start from target? No, "has_edge_to(target)" means find X where X -> target.
        // This is exactly what `graph.get_edges_to(target)` gives (incoming edges).
        if let Some(ref target) = self.filters.edge_target {
            let entity_id = crate::EntityId::parse(target)
                .unwrap_or_else(|_| crate::EntityId::new("nodes".to_string(), target.clone()));
            let edges = self.db.graph().get_edges_to(&entity_id);
            let ids: HashSet<_> = edges
                .iter()
                .filter(|e| {
                    self.filters
                        .edge_type
                        .as_ref()
                        .map_or(true, |t| &e._type == t)
                })
                .filter_map(|e| {
                    let hash = crate::hash_slug(&e._from.to_string());
                    self.db.storage().get_by_slug(hash).map(|n| n.node_id)
                })
                .collect();
            candidates = intersect(candidates, ids);
        }

        // If edge_source is set, we want nodes that are pointed FROM source.
        // Find X where source -> X. This is `graph.get_edges_from(source)`.
        if let Some(ref source) = self.filters.edge_source {
            let entity_id = crate::EntityId::parse(source)
                .unwrap_or_else(|_| crate::EntityId::new("nodes".to_string(), source.clone()));
            let edges = self.db.graph().get_edges_from(&entity_id);
            let ids: HashSet<_> = edges
                .iter()
                .filter(|e| {
                    self.filters
                        .edge_type
                        .as_ref()
                        .map_or(true, |t| &e._type == t)
                })
                .filter_map(|e| {
                    let hash = crate::hash_slug(&e._to.to_string());
                    self.db.storage().get_by_slug(hash).map(|n| n.node_id)
                })
                .collect();
            candidates = intersect(candidates, ids);
        }

        // 5. Candidate Resolution
        let mut final_results = Vec::new();

        if let Some(cands) = candidates {
            // We have a filtered set of IDs
            for node_id in cands {
                if let Some(node) = self.db.storage().get_by_id(node_id, None) {
                    if self.matches_filters(&node)? {
                        final_results.push(node);
                    }
                }
            }
        } else {
            // Fallback: Scan all nodes (O(N)) - only if no indices used
            // This is slow but necessary if only checking e.g. SlugHash (which is O(N) scan here,
            // though get_by_slug is O(1) - we should optimize that too!)

            // Optimization: If only slug_hash is set, do O(1) lookup
            if let Some(slug_hash) = self.filters.slug_hash {
                if let Some(node) = self.db.storage().get_by_slug(slug_hash) {
                    if self.matches_filters(&node)? {
                        final_results.push(node);
                    }
                }
            } else {
                // True full scan
                for node in self.db.storage().all() {
                    if self.matches_filters(&node)? {
                        final_results.push(node.clone());
                        if let Some(limit) = self.filters.limit
                            && final_results.len() >= limit
                        {
                            break;
                        }
                    }
                }
            }
        }

        // Apply limit if not already applied in scan
        if let Some(limit) = self.filters.limit {
            if final_results.len() > limit {
                final_results.truncate(limit);
            }
        }

        Ok(final_results)
    }

    /// Check if a node matches remaining filters (that weren't drivers)
    fn matches_filters(&self, node: &NodeHeader) -> Result<bool, Box<dyn std::error::Error>> {
        // Slug check (redundant if looked up by slug, but safe)
        if let Some(slug_hash) = self.filters.slug_hash
            && node.slug_hash != slug_hash
        {
            return Ok(false);
        }

        // TODO: Time range check (requires payload read)
        // if let Some(time_range) = self.filters.time_range ...

        Ok(true)
    }
}

impl SekejapDB {
    /// Create a new query builder
    pub fn query(&self) -> Query<'_> {
        Query::new(self)
    }

    /// Get payload for a node (helper method)
    #[allow(dead_code)]
    fn get_payload(
        &self,
        node: &NodeHeader,
    ) -> Result<Option<NodePayload>, Box<dyn std::error::Error>> {
        let payload_bytes = self.blob_store.read(node.payload_ptr)?;
        let payload: NodePayload = serde_json::from_slice(&payload_bytes)?;
        Ok(Some(payload))
    }
}

/// Calculate Haversine distance between two coordinates in kilometers
#[cfg(feature = "spatial")]
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_KM: f64 = 6371.0;

    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_KM * c
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WriteOptions;
    use tempfile::TempDir;

    #[test]
    fn test_query_by_slug() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        // Write directly to Tier 2 for query tests
        db.write_with_options(
            "test-node",
            r#"{"title": "Test"}"#,
            WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();

        let results = db.query().by_slug("test-node").execute().unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_query_limit() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        for i in 0..5 {
            db.write_with_options(
                &format!("node-{}", i),
                r#"{"title": "Test"}"#,
                WriteOptions {
                    publish_now: true,
                    ..Default::default()
                },
            )
            .unwrap();
        }

        let results = db.query().limit(3).execute().unwrap();
        assert_eq!(results.len(), 3);
    }

    #[cfg(feature = "spatial")]
    #[test]
    fn test_spatial_filter() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        db.write(
            "jakarta",
            r#"{"title": "Jakarta", "coordinates": {"lat": -6.2088, "lon": 106.8456}}"#,
        )
        .unwrap();
        db.write(
            "sydney",
            r#"{"title": "Sydney", "coordinates": {"lat": -33.8688, "lon": 151.2093}}"#,
        )
        .unwrap();
        
        db.flush().unwrap();

        // Search near Jakarta
        let results = db
            .query()
            .spatial(-6.2088, 106.8456, 10.0)
            .unwrap()
            .execute()
            .unwrap();

        assert_eq!(results.len(), 1);
    }
}
