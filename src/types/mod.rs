//! Types module for Sekejap-DB
//!
//! Contains all data types used across the database:
//! - Collection types (CollectionId, EntityId, Collection)
//! - Node types (NodeId, NodeHeader, NodePayload)
//! - Edge types (WeightedEdge, EdgeType)
//! - Blob storage types (BlobPtr, BlobStore)
//! - Geometry types (Point, Polygon, Polyline)
//! - Vector types (VectorChannel, VectorStore)
//! - Geo types (GeoFeature, GeoStore, GeoGeometry)
//! - Props and Decay types
//! - Schema types (CollectionSchema, etc.)

pub mod node;
pub mod edge;
pub mod blob;
pub mod payload;
pub mod geometry;
pub mod collection;
pub mod vector;
pub mod geo;
pub mod decay;
pub mod schema;

pub use node::{NodeId, SlugHash, SpatialHash, Epoch, Coordinates, SpatialResult, NodeHeader, NodePayload, Tombstone, HeadPointer};
pub use edge::{WeightedEdge, EdgeType, EdgePayload, Evidence};
pub use blob::{BlobPtr, BlobStore};
pub use payload::{Payload, SerializablePayload};
pub use geometry::{Point, Polygon, Polyline, Geometry, point_in_polygon, polyline_intersects_polygon, distance};
pub use collection::{CollectionId, EntityId, EdgeRef, Collection, parse_entity_id};
pub use vector::{VectorChannel, VectorStore};
pub use geo::{GeoGeometry, GeoFeature, GeoStore};
pub use decay::{Props, TemporalDecay, DecayFunction};
pub use schema::{CollectionSchema, HotFields, VectorSchema, SpatialSchema, GeoType, CollectionRegistry, HnswParams, QuantizationType};

/// Write options for controlling write behavior
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// If true, writes immediately to Tier 2 (bypasses staging)
    /// If false, writes to Tier 1 and waits for promotion (default)
    pub publish_now: bool,

    /// Optional vector embedding - stored as blob and linked via NodeHeader::vector_ptr
    /// When present, vector is stored canonically in BlobStore
    #[cfg(feature = "vector")]
    pub vector: Option<Vec<f32>>,

    /// Coordinates for spatial indexing (default: 0.0, 0.0)
    pub latitude: f64,
    pub longitude: f64,

    /// If true, creates a tombstone (deleted node)
    pub deleted: bool,

    /// Optional geometry (Point, Polygon, or Polyline) for spatial queries
    /// When present, geometry is stored and indexed for spatial operations
    #[cfg(feature = "spatial")]
    pub geometry: Option<Geometry>,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            publish_now: false,
            #[cfg(feature = "vector")]
            vector: None,
            latitude: 0.0,
            longitude: 0.0,
            deleted: false,
            #[cfg(feature = "spatial")]
            geometry: None,
        }
    }
}

/// Read options for controlling read behavior
#[derive(Debug, Clone)]
#[derive(Default)]
pub struct ReadOptions {
    /// If true, includes staged data from Tier 1 in read results
    /// If false, only reads validated data from Tier 2 (default)
    pub include_staged: bool,
}


/// Delete options for controlling delete behavior
#[derive(Debug, Clone)]
#[derive(Default)]
pub struct DeleteOptions {
    /// If true, excludes edges from deletion (keeps them for audit)
    /// If false, cascades delete to edges (default)
    pub exclude_edges: bool,
}

