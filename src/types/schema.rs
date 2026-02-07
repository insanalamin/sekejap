//! Collection Schema types for Sekejap-DB
//!
//! Provides schema definition for collections with indexing hints:
//! - CollectionSchema: Complete schema definition
//! - HotFields: Fields to index for fast queries
//! - VectorSchema, SpatialSchema: Index configuration
//!
//! # Example
//!
//! ```rust
//! use hsdl_sekejap::types::{CollectionSchema, HotFields, VectorSchema, SpatialSchema, GeoType};
//!
//! let schema = CollectionSchema {
//!     hot_fields: HotFields {
//!         vector: vec!["vectors.dense".to_string(), "vectors.colbert".to_string()],
//!         spatial: vec!["geo.area".to_string(), "geo.center".to_string()],
//!         fulltext: vec!["title".to_string(), "content".to_string()],
//!     },
//!     vectors: vec![
//!         ("dense".to_string(), VectorSchema {
//!             model: "bge-m3".to_string(),
///             dims: 1024,
///             index_hnsw: true,
///         }),
///     ],
///     spatial: vec![
///         ("area".to_string(), SpatialSchema {
///             geo_type: GeoType::Polygon,
///             index_rtree: true,
///         }),
///     ],
///     edge_types: vec!["mentions".to_string(), "caused_by".to_string()],
///     fulltext: vec!["title".to_string(), "content".to_string()],
/// };
/// ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Geo type for schema definition
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[derive(Default)]
pub enum GeoType {
    /// Point geometry
    #[default]
    Point,
    /// LineString geometry
    LineString,
    /// Polygon geometry
    Polygon,
    /// MultiPolygon geometry
    MultiPolygon,
}


impl fmt::Display for GeoType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeoType::Point => write!(f, "Point"),
            GeoType::LineString => write!(f, "LineString"),
            GeoType::Polygon => write!(f, "Polygon"),
            GeoType::MultiPolygon => write!(f, "MultiPolygon"),
        }
    }
}

/// Vector channel schema for index configuration
///
/// Defines how a vector channel should be indexed and stored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorSchema {
    /// Embedding model name
    pub model: String,
    
    /// Vector dimensions
    pub dims: usize,
    
    /// Build HNSW index for this channel (default: true)
    #[serde(default = "default_true")]
    pub index_hnsw: bool,
    
    /// Quantization type (none, pq, sq)
    #[serde(default)]
    pub quantization: Option<QuantizationType>,
    
    /// Index parameters
    #[serde(default)]
    pub hnsw_params: Option<HnswParams>,
}

fn default_true() -> bool {
    true
}

/// Quantization type for vector compression
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum QuantizationType {
    /// Product Quantization
    ProductQuantization,
    /// Scalar Quantization (FP32 -> INT8)
    ScalarQuantization,
}

impl fmt::Display for QuantizationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QuantizationType::ProductQuantization => write!(f, "PQ"),
            QuantizationType::ScalarQuantization => write!(f, "SQ"),
        }
    }
}

/// HNSW index parameters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HnswParams {
    /// Number of neighbors to consider during construction (default: 16)
    #[serde(default = "default_m")]
    pub m: usize,
    
    /// Number of neighbors to consider during search (default: 32)
    #[serde(default = "default_ef_construction")]
    pub ef_construction: usize,
    
    /// Threshold for search (default: 64)
    #[serde(default = "default_ef")]
    pub ef: usize,
    
    /// Distance metric (l2, cosine, dot)
    #[serde(default = "default_metric")]
    pub metric: String,
}

fn default_m() -> usize {
    16
}

fn default_ef_construction() -> usize {
    32
}

fn default_ef() -> usize {
    64
}

fn default_metric() -> String {
    "cosine".to_string()
}

impl Default for HnswParams {
    fn default() -> Self {
        Self {
            m: default_m(),
            ef_construction: default_ef_construction(),
            ef: default_ef(),
            metric: default_metric(),
        }
    }
}

/// Spatial field schema for index configuration
///
/// Defines how a geo field should be indexed and stored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpatialSchema {
    /// Geometry type expected
    #[serde(default)]
    pub geo_type: GeoType,
    
    /// Build R-tree index for this field (default: true)
    #[serde(default = "default_true")]
    pub index_rtree: bool,
    
    /// Maximum entries per node (default: 16)
    #[serde(default = "default_max_entries")]
    pub max_entries: usize,
    
    /// Fill percentage (default: 0.5)
    #[serde(default = "default_fill")]
    pub fill: f64,
}

fn default_max_entries() -> usize {
    16
}

fn default_fill() -> f64 {
    0.5
}

/// Hot fields to index for fast queries
///
/// These fields are automatically indexed for fast access.
/// Used by the query planner to optimize multi-modal queries.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HotFields {
    /// Vector fields to index (e.g., ["vectors.dense", "vectors.colbert"])
    #[serde(default)]
    pub vector: Vec<String>,
    
    /// Spatial fields to index (e.g., ["geo.area", "geo.center"])
    #[serde(default)]
    pub spatial: Vec<String>,
    
    /// Fulltext fields to index (e.g., ["title", "content"])
    #[serde(default)]
    pub fulltext: Vec<String>,
}

impl HotFields {
    /// Create empty hot fields
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Add a vector field
    pub fn add_vector_field(&mut self, field: impl Into<String>) {
        self.vector.push(field.into());
    }
    
    /// Add a spatial field
    pub fn add_spatial_field(&mut self, field: impl Into<String>) {
        self.spatial.push(field.into());
    }
    
    /// Add a fulltext field
    pub fn add_fulltext_field(&mut self, field: impl Into<String>) {
        self.fulltext.push(field.into());
    }
    
    /// Check if any vector fields are configured
    pub fn has_vector_fields(&self) -> bool {
        !self.vector.is_empty()
    }
    
    /// Check if any spatial fields are configured
    pub fn has_spatial_fields(&self) -> bool {
        !self.spatial.is_empty()
    }
    
    /// Check if any fulltext fields are configured
    pub fn has_fulltext_fields(&self) -> bool {
        !self.fulltext.is_empty()
    }
}

/// Complete collection schema definition
///
/// Defines how a collection should be indexed and stored.
/// When present, the schema is used for deterministic indexing.
/// When absent, flex mode is used (auto-detect).
///
/// # Example
///
/// ```rust
/// use hsdl_sekejap::types::{CollectionSchema, HotFields, VectorSchema, SpatialSchema, GeoType};
///
/// let schema = CollectionSchema {
///     hot_fields: HotFields {
///         vector: vec!["vectors.dense".to_string()],
///         spatial: vec!["geo.center".to_string()],
///         fulltext: vec!["title".to_string(), "content".to_string()],
///     },
///     vectors: vec![
///         ("dense".to_string(), VectorSchema {
///             model: "bge-m3".to_string(),
///             dims: 1024,
///             index_hnsw: true,
///             quantization: None,
///             hnsw_params: None,
///         }),
///     ],
///     spatial: vec![
///         ("center".to_string(), SpatialSchema {
///             geo_type: GeoType::Point,
///             index_rtree: true,
///             max_entries: 16,
///             fill: 0.5,
///         }),
///     ],
///     edge_types: vec!["mentions".to_string(), "related_to".to_string()],
///     fulltext: vec!["title".to_string(), "content".to_string()],
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionSchema {
    /// Hot fields for query optimization
    #[serde(default)]
    pub hot_fields: HotFields,
    
    /// Vector channel configurations
    #[serde(default)]
    pub vectors: Vec<(String, VectorSchema)>,
    
    /// Spatial field configurations
    #[serde(default)]
    pub spatial: Vec<(String, SpatialSchema)>,
    
    /// Valid edge types for this collection (for edges referencing this collection)
    #[serde(default)]
    pub edge_types: Vec<String>,
    
    /// Fulltext fields to index (when using fulltext feature)
    #[serde(default)]
    pub fulltext: Vec<String>,
}

impl CollectionSchema {
    /// Create empty schema (flex mode)
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Create schema with hot fields
    pub fn with_hot_fields(hot_fields: HotFields) -> Self {
        Self {
            hot_fields,
            ..Default::default()
        }
    }
    
    /// Add a vector channel
    pub fn add_vector(&mut self, name: String, model: String, dims: usize) -> &mut VectorSchema {
        let schema = VectorSchema {
            model,
            dims,
            index_hnsw: true,
            quantization: None,
            hnsw_params: None,
        };
        self.vectors.push((name, schema));
        self.vectors.last_mut().unwrap().1.index_hnsw = true;
        &mut self.vectors.last_mut().unwrap().1
    }
    
    /// Add a spatial field
    pub fn add_spatial(&mut self, name: String, geo_type: GeoType) -> &mut SpatialSchema {
        let schema = SpatialSchema {
            geo_type,
            index_rtree: true,
            max_entries: 16,
            fill: 0.5,
        };
        self.spatial.push((name, schema));
        &mut self.spatial.last_mut().unwrap().1
    }
    
    /// Get vector schema by channel name
    pub fn get_vector_schema(&self, channel: &str) -> Option<&VectorSchema> {
        self.vectors.iter().find(|(name, _)| name == channel).map(|(_, s)| s)
    }
    
    /// Get spatial schema by field name
    pub fn get_spatial_schema(&self, field: &str) -> Option<&SpatialSchema> {
        self.spatial.iter().find(|(name, _)| name == field).map(|(_, s)| s)
    }
    
    /// Check if a vector channel is configured for HNSW indexing
    pub fn has_hnsw_index(&self, channel: &str) -> bool {
        self.get_vector_schema(channel).is_some_and(|s| s.index_hnsw)
    }
    
    /// Check if a spatial field is configured for R-tree indexing
    pub fn has_rtree_index(&self, field: &str) -> bool {
        self.get_spatial_schema(field).is_some_and(|s| s.index_rtree)
    }
    
    /// Check if edge type is valid for this collection
    pub fn is_valid_edge_type(&self, edge_type: &str) -> bool {
        self.edge_types.is_empty() || self.edge_types.contains(&edge_type.to_string())
    }
    
    /// Get total configured vector dimensions
    pub fn total_vector_dims(&self) -> usize {
        self.vectors.iter().map(|(_, s)| s.dims).sum()
    }
    
    /// Check if this schema has any indexing configured
    pub fn has_indexing(&self) -> bool {
        !self.vectors.is_empty() || !self.spatial.is_empty() || !self.fulltext.is_empty()
    }
}

impl Default for CollectionSchema {
    fn default() -> Self {
        Self {
            hot_fields: HotFields::new(),
            vectors: Vec::new(),
            spatial: Vec::new(),
            edge_types: Vec::new(),
            fulltext: Vec::new(),
        }
    }
}

impl fmt::Display for CollectionSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CollectionSchema(")?;
        
        if self.has_indexing() {
            if !self.vectors.is_empty() {
                write!(f, "{} vectors, ", self.vectors.len())?;
            }
            if !self.spatial.is_empty() {
                write!(f, "{} spatial, ", self.spatial.len())?;
            }
            if !self.fulltext.is_empty() {
                write!(f, "{} fulltext, ", self.fulltext.len())?;
            }
            if !self.edge_types.is_empty() {
                write!(f, "{} edge_types, ", self.edge_types.len())?;
            }
        } else {
            write!(f, "flex mode")?;
        }
        
        write!(f, ")")
    }
}

/// Collection registry for managing multiple collection schemas
///
/// Provides O(1) access to collection schemas by collection ID.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CollectionRegistry(HashMap<String, CollectionSchema>);

impl CollectionRegistry {
    /// Create empty registry
    pub fn new() -> Self {
        Self(HashMap::new())
    }
    
    /// Register a collection schema
    pub fn register(&mut self, collection_id: impl Into<String>, schema: CollectionSchema) {
        self.0.insert(collection_id.into(), schema);
    }
    
    /// Get a collection schema
    pub fn get(&self, collection_id: &str) -> Option<&CollectionSchema> {
        self.0.get(collection_id)
    }
    
    /// Check if collection has a schema
    pub fn has_schema(&self, collection_id: &str) -> bool {
        self.0.contains_key(collection_id)
    }
    
    /// Remove a collection schema
    pub fn remove(&mut self, collection_id: &str) -> Option<CollectionSchema> {
        self.0.remove(collection_id)
    }
    
    /// Get number of registered collections
    pub fn len(&self) -> usize {
        self.0.len()
    }
    
    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    
    /// Iterate over collections
    pub fn iter(&self) -> impl Iterator<Item = (&str, &CollectionSchema)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_hot_fields() {
        let mut hot = HotFields::new();
        assert!(!hot.has_vector_fields());
        assert!(!hot.has_spatial_fields());
        assert!(!hot.has_fulltext_fields());
        
        hot.add_vector_field("vectors.dense");
        hot.add_spatial_field("geo.center");
        hot.add_fulltext_field("title");
        
        assert!(hot.has_vector_fields());
        assert!(hot.has_spatial_fields());
        assert!(hot.has_fulltext_fields());
        assert_eq!(hot.vector.len(), 1);
        assert_eq!(hot.spatial.len(), 1);
        assert_eq!(hot.fulltext.len(), 1);
    }
    
    #[test]
    fn test_vector_schema() {
        let schema = VectorSchema {
            model: "bge-m3".to_string(),
            dims: 1024,
            index_hnsw: true,
            quantization: None,
            hnsw_params: None,
        };
        
        assert_eq!(schema.model, "bge-m3");
        assert_eq!(schema.dims, 1024);
        assert!(schema.index_hnsw);
    }
    
    #[test]
    fn test_spatial_schema() {
        let schema = SpatialSchema {
            geo_type: GeoType::Polygon,
            index_rtree: true,
            max_entries: 32,
            fill: 0.7,
        };
        
        assert_eq!(schema.geo_type, GeoType::Polygon);
        assert!(schema.index_rtree);
        assert_eq!(schema.max_entries, 32);
    }
    
    #[test]
    fn test_collection_schema() {
        let mut schema = CollectionSchema::new();
        
        // Add vector channel
        {
            let vec_schema = schema.add_vector("dense".to_string(), "bge-m3".to_string(), 1024);
            vec_schema.index_hnsw = true;
        }
        
        // Add spatial field
        {
            let spatial_schema = schema.add_spatial("center".to_string(), GeoType::Point);
            spatial_schema.index_rtree = true;
        }
        
        schema.edge_types = vec!["mentions".to_string(), "caused_by".to_string()];
        schema.fulltext = vec!["title".to_string(), "content".to_string()];
        
        assert!(schema.has_indexing());
        assert!(schema.has_hnsw_index("dense"));
        assert!(schema.has_rtree_index("center"));
        assert!(!schema.has_hnsw_index("missing"));
        assert!(!schema.has_rtree_index("missing"));
        
        assert!(schema.is_valid_edge_type("mentions"));
        assert!(!schema.is_valid_edge_type("invalid"));
        
        // With empty edge_types, all should be valid
        let flex_schema = CollectionSchema::new();
        assert!(flex_schema.is_valid_edge_type("any"));
    }
    
    #[test]
    fn test_collection_registry() {
        let mut registry = CollectionRegistry::new();
        
        let schema = CollectionSchema::new();
        registry.register("news".to_string(), schema);
        
        assert!(registry.has_schema("news"));
        assert!(!registry.has_schema("missing"));
        
        let retrieved = registry.get("news").unwrap();
        assert!(!retrieved.has_indexing()); // Empty schema
        
        assert_eq!(registry.len(), 1);
    }
}
