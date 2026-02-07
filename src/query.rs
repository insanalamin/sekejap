//! Query builder for multi-modal database queries
//!
//! This module provides a chainable query builder API with optional features:
//! - Vector similarity search (requires "vector" feature)
//! - Spatial radius/intersection queries (requires "spatial" feature)
//! - Fulltext search (requires "fulltext" feature)
//!
//! All features are optional and have zero performance impact when disabled.

use crate::{NodeHeader, NodePayload, SlugHash, EdgeType, SekejapDB};

/// Query builder for multi-modal searches
///
/// # Example
/// ```rust,no_run
/// # use hsdl_sekejap::SekejapDB;
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
    ///
/// # Example
/// ```rust,no_run
/// # use hsdl_sekejap::SekejapDB;
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// let results = db.query()
///     .by_slug("jakarta-crime-2024")
///     .execute()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
    pub fn by_slug(mut self, slug: &str) -> Self {
        use crate::hash_slug;
        self.filters.slug_hash = Some(hash_slug(slug));
        self
    }

    /// Filter by geographic radius (requires "spatial" feature)
    ///
    /// # Arguments
    /// * `lat` - Center latitude
    /// * `lon` - Center longitude
    /// * `radius_km` - Search radius in kilometers
    ///
    /// # Example
    /// ```rust,no_run
    /// # use hsdl_sekejap::{SekejapDB};
    /// # use std::path::Path;
    /// # let db = SekejapDB::new(Path::new("./data")).unwrap();
    /// let results = db.query()
    ///     .spatial(-6.2088, 106.8456, 5.0)?
    ///     .execute()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[cfg(feature = "spatial")]
    pub fn spatial(mut self, lat: f64, lon: f64, radius_km: f64) -> Result<Self, Box<dyn std::error::Error>> {
        self.filters.spatial_center = Some((lat, lon));
        self.filters.spatial_radius_km = Some(radius_km);
        Ok(self)
    }

    /// Filter by vector similarity (requires "vector" feature)
    ///
    /// Uses brute-force search to find top-k similar vectors by cosine similarity.
    ///
    /// # Arguments
    /// * `query_vector` - Query vector for similarity search
    /// * `k` - Number of top results to return
    ///
    /// # Example
    /// ```rust,no_run
    /// # use hsdl_sekejap::{SekejapDB};
    /// # use std::path::Path;
    /// # let db = SekejapDB::new(Path::new("./data")).unwrap();
    /// let query_embedding = vec![0.1, 0.2, 0.3];
    /// let results = db.query()
    ///     .vector_search(query_embedding, 10)
    ///     .execute()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[cfg(feature = "vector")]
    pub fn vector_search(mut self, query_vector: Vec<f32>, k: usize) -> Self {
        self.filters.vector_query = Some(query_vector);
        self.filters.vector_k = Some(k);
        self
    }

    /// Filter by fulltext search (requires "fulltext" feature)
    ///
    /// # Arguments
    /// * `query` - Search query string
    ///
    /// # Example
    /// ```rust,no_run
    /// # use hsdl_sekejap::{SekejapDB};
    /// # use std::path::Path;
    /// # let db = SekejapDB::new(Path::new("./data")).unwrap();
    /// let results = db.query()
    ///     .fulltext("jakarta crime theft")?
    ///     .execute()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[cfg(feature = "fulltext")]
    pub fn fulltext(mut self, query: &str) -> Result<Self, Box<dyn std::error::Error>> {
        self.filters.fulltext_query = Some(query.to_string());
        Ok(self)
    }

    /// Filter by edge relationship to target node
    ///
    /// # Arguments
    /// * `target_slug` - Slug of the target node
    /// * `edge_type` - Type of edge relationship
    ///
    /// # Example
    /// ```rust,no_run
    /// # use hsdl_sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let db = SekejapDB::new(Path::new("./data")).unwrap();
    /// let results = db.query()
    ///     .has_edge_to("Uber Eats", "related".to_string())
    ///     .execute()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn has_edge_to(mut self, target_slug: &str, edge_type: EdgeType) -> Self {
        self.filters.edge_target = Some(target_slug.to_string());
        self.filters.edge_type = Some(edge_type);
        self
    }

    /// Filter by edge relationship from source node
    ///
    /// # Arguments
    /// * `source_slug` - Slug of the source node
    /// * `edge_type` - Type of edge relationship
    ///
    /// # Example
    /// ```rust,no_run
    /// # use hsdl_sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let db = SekejapDB::new(Path::new("./data")).unwrap();
    /// let results = db.query()
    ///     .has_edge_from("poverty", "causal".to_string())
    ///     .execute()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn has_edge_from(mut self, source_slug: &str, edge_type: EdgeType) -> Self {
        self.filters.edge_source = Some(source_slug.to_string());
        self.filters.edge_type = Some(edge_type);
        self
    }

    /// Limit number of results
    ///
    /// # Example
    /// ```rust,no_run
    /// # use hsdl_sekejap::{SekejapDB};
    /// # use std::path::Path;
    /// # let db = SekejapDB::new(Path::new("./data")).unwrap();
    /// let results = db.query()
    ///     .limit(10)
    ///     .execute()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn limit(mut self, n: usize) -> Self {
        self.filters.limit = Some(n);
        self
    }

    /// Execute's query and return results
    ///
    /// # Returns
    /// * `Vec<NodeHeader>` - List of matching nodes
    ///
    /// # Example
    /// ```rust,no_run
    /// # use hsdl_sekejap::{SekejapDB};
    /// # use std::path::Path;
    /// # let db = SekejapDB::new(Path::new("./data")).unwrap();
    /// let results = db.query()
    ///     .by_slug("jakarta-crime-2024")
    ///     .execute()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn execute(&self) -> Result<Vec<NodeHeader>, Box<dyn std::error::Error>> {
        // If vector search is enabled, use brute-force search
        #[cfg(feature = "vector")]
        if let Some(ref query_vector) = self.filters.vector_query {
            if let Some(k) = self.filters.vector_k {
                return self.execute_vector_search(query_vector, k);
            }
        }

        // Otherwise, do regular query execution
        let mut results = Vec::new();

        // Iterate through all nodes
        for node in self.db.storage().all() {
            if self.matches_filters(&node)? {
                results.push(node.clone());
                if let Some(limit) = self.filters.limit
                    && results.len() >= limit {
                        break;
                    }
            }
        }

        Ok(results)
    }

    /// Execute vector search using brute-force algorithm
    #[cfg(feature = "vector")]
    fn execute_vector_search(&self, query_vector: &[f32], k: usize) -> Result<Vec<NodeHeader>, Box<dyn std::error::Error>> {
        use crate::brute_force_search;

        // Perform brute-force vector search (reads vectors from BlobStore)
        let vector_results = brute_force_search(
            self.db.storage(),
            self.db.blob_store(),  // Use accessor method
            query_vector,
            k,
        )?;
        
        // Convert vector results to node headers
        let mut node_headers = Vec::new();
        for result in vector_results {
            // Find node by node_id
            for node in self.db.storage().all() {
                if node.node_id == result.node_id {
                    node_headers.push(node);
                    break;
                }
            }
        }
        
        Ok(node_headers)
    }

    /// Check if a node matches all filters
    fn matches_filters(&self, node: &NodeHeader) -> Result<bool, Box<dyn std::error::Error>> {
        // Slug filter
        if let Some(slug_hash) = self.filters.slug_hash
            && node.slug_hash != slug_hash {
                return Ok(false);
            }

        // Spatial filter
        #[cfg(feature = "spatial")]
        if let Some((center_lat, center_lon)) = self.filters.spatial_center {
            if let Some(radius_km) = self.filters.spatial_radius_km {
                if let Some(payload) = self.db.get_payload(node)? {
                    if let Some(coords) = payload.coordinates {
                        let distance = haversine_distance(
                            center_lat, center_lon,
                            coords.latitude, coords.longitude
                        );
                        if distance > radius_km {
                            return Ok(false);
                        }
                    } else {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            }
        }

        // Vector filter - skip nodes without vectors
        #[cfg(feature = "vector")]
        if self.filters.vector_query.is_some() {
            if node.vector_ptr.is_none() {
                return Ok(false);
            }
        }

        // Fulltext filter
        #[cfg(feature = "fulltext")]
        if let Some(ref query) = self.filters.fulltext_query {
            if let Some(payload) = self.db.get_payload(node)? {
                if let Some(content) = &payload.content {
                    let content_lower = content.to_lowercase();
                    let query_lower = query.to_lowercase();
                    let query_words: Vec<&str> = query_lower.split_whitespace().collect();
                    
                    for word in query_words {
                        if !content_lower.contains(word) {
                            return Ok(false);
                        }
                    }
                } else {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }
        }

        // Edge filter
        if self.filters.edge_target.is_some() || self.filters.edge_source.is_some() {
            // TODO: Implement edge filtering
            // This would require checking the graph for edges
        }

        Ok(true)
    }
}

impl SekejapDB {
    /// Create a new query builder
    ///
/// # Example
/// ```rust,no_run
/// # use hsdl_sekejap::SekejapDB;
/// # use std::path::Path;
/// # let db = SekejapDB::new(Path::new("./data")).unwrap();
/// let results = db.query()
    ///     .has_edge_from("poverty", "causal".to_string())
    ///     .execute()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
    pub fn query(&self) -> Query<'_> {
        Query::new(self)
    }

    /// Get payload for a node (helper method)
    #[allow(dead_code)]
    fn get_payload(&self, node: &NodeHeader) -> Result<Option<NodePayload>, Box<dyn std::error::Error>> {
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
        db.write_with_options("test-node", r#"{"title": "Test"}"#,
            WriteOptions { publish_now: true, ..Default::default() }).unwrap();
        
        let results = db.query().by_slug("test-node").execute().unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_query_limit() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();
        
        for i in 0..5 {
            db.write_with_options(&format!("node-{}", i), r#"{"title": "Test"}"#,
                WriteOptions { publish_now: true, ..Default::default() }).unwrap();
        }
        
        let results = db.query().limit(3).execute().unwrap();
        assert_eq!(results.len(), 3);
    }

    #[cfg(feature = "spatial")]
    #[test]
    fn test_spatial_filter() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();
        
        db.write("jakarta", r#"{"title": "Jakarta", "coordinates": {"lat": -6.2088, "lon": 106.8456}}"#).unwrap();
        db.write("sydney", r#"{"title": "Sydney", "coordinates": {"lat": -33.8688, "lon": 151.2093}}"#).unwrap();
        
        // Search near Jakarta
        let results = db.query()
            .spatial(-6.2088, 106.8456, 10.0)
            .unwrap()
            .execute()
            .unwrap();
        
        assert_eq!(results.len(), 1);
    }
}