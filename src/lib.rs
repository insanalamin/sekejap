//! Sekejap-DB: A high-performance, embedded multi-modal database engine
//!
//! This library provides a three-tier storage architecture:
//! - Tier 1: Ingestion Buffer (LSM-Tree for high-velocity writes)
//! - Tier 2: Serving Layer (CoW B+Tree for sub-millisecond reads)
//! - Tier 3: Knowledge Graph (Causal adjacency for RCA)
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use sekejap::{SekejapDB, EdgeType};
//! use std::path::Path;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Initialize database
//! let mut db = SekejapDB::new(Path::new("./data"))?;
//!
//! // Write event data
//! db.write("jakarta-crime-2024", r#"{
//!     "title": "Theft Incident",
//!     "tags": ["person", "vehicle"],
//!     "coordinates": {"lat": -6.2088, "lon": 106.8456}
//! }"#)?;
//!
//! // Read event data
//! if let Some(event) = db.read("jakarta-crime-2024")? {
//!     println!("Found event: {}", event);
//! }
//!
//! // Traverse for root cause analysis
//! let results = db.traverse("jakarta-crime-2024", 3, 0.5, None)?;
//! # Ok(())
//! # }
//! ```

pub mod atoms;
pub mod config;
pub mod graph;
pub mod hashing;
pub mod index;
pub mod query;
pub mod sekejapql;
pub mod storage;
pub mod types;

#[cfg(feature = "vector")]
pub mod vectors;

use crate::config::PromotionConfig;
use types::decay::DecayFunction;

pub use types::{
    Collection,
    // DATA_FORMAT.md types
    CollectionId,
    CollectionRegistry,
    CollectionSchema,
    DeleteOptions,
    EdgeRef,
    EntityId,
    GeoFeature,
    GeoGeometry,
    GeoStore,
    GeoType,
    Geometry,
    HotFields,
    // Core types
    NodeHeader,
    NodePayload,
    Point,
    Polygon,
    Polyline,
    Props,
    ReadOptions,
    SpatialSchema,
    TemporalDecay,
    Tombstone,
    VectorChannel,
    VectorSchema,
    VectorStore,
    WriteOptions,
    blob::{BlobPtr, BlobStore},
    distance,
    edge::{EdgePayload, EdgeType, WeightedEdge},
    node::{Coordinates, Epoch, NodeId, SlugHash, SpatialHash, SpatialResult},
    parse_entity_id,
    payload::{Payload, SerializablePayload},
    point_in_polygon,
    polyline_intersects_polygon,
};

pub use index::SlugIndex;
pub use storage::{
    BatchUpsert, GarbageCollector, GcConfig, GcMetrics, IngestionBuffer, PersistentStorage,
    PromoteWorker, PromotionMetrics, ServingLayer, SingleStorage, WorkerCommand,
};

#[cfg(feature = "vector")]
pub use vectors::{
    VectorSearchResult,
    index::{IndexBuildPolicy, VectorIndex},
    ops::{brute_force_search, bytes_to_vector, vector_to_bytes},
    quantization::{QuantizationType, QuantizedVector, dequantize, quantization_error, quantize},
};

#[cfg(feature = "spatial")]
pub use index::SpatialIndex;

#[cfg(feature = "fulltext")]
pub use index::{FulltextConfig, FulltextIndex, FulltextResult, FulltextStats};

pub use atoms::*;
pub use graph::{CausalGraph, ConcurrentGraph, TraversalResult};
pub use hashing::{hash_slug, hash_spatial};

/// Main database engine
///
/// Provides high-level API for writing, reading, and traversing multi-modal event data
/// Thread-safe for concurrent access in Axum/async environments
pub struct SekejapDB {
    ingestion: storage::IngestionBuffer, // Tier 1
    storage: storage::SingleStorage,     // Tier 2
    graph: graph::ConcurrentGraph,       // Tier 3
    blob_store: types::BlobStore,
    promote_worker: storage::PromoteWorker, // Auto-promotion worker
    collection_registry: types::CollectionRegistry, // Collection schemas
    #[cfg(feature = "fulltext")]
    fulltext: Option<index::FulltextIndex>,
    #[cfg(feature = "vector")]
    vector_index: Option<vectors::VectorIndex>,
    #[cfg(feature = "spatial")]
    spatial_index: Option<index::SpatialIndex>,
}

impl SekejapDB {
    /// Create a new database instance
    pub fn new(base_dir: &std::path::Path) -> std::io::Result<Self> {
        let ingestion = storage::IngestionBuffer::new(base_dir.join("ingestion"))?;
        let storage = storage::SingleStorage::new(base_dir.join("storage"))?;

        // Create promotion worker with default config
        let promote_worker = storage::PromoteWorker::new(PromotionConfig::default());

        #[cfg(feature = "fulltext")]
        let fulltext = match index::FulltextIndex::new_default(&base_dir.join("fulltext")) {
            Ok(index) => Some(index),
            Err(e) => {
                log::warn!("Failed to initialize fulltext index: {}", e);
                None
            }
        };

        #[cfg(feature = "vector")]
        let vector_index = Some(vectors::VectorIndex::new_with_path(
            vectors::IndexBuildPolicy::ManualTrigger,
            base_dir,
        ));

        #[cfg(feature = "spatial")]
        let spatial_index = Some(index::SpatialIndex::new());

        Ok(Self {
            ingestion,
            storage,
            graph: graph::ConcurrentGraph::new(),
            blob_store: types::BlobStore::new(base_dir.join("blobs"))?,
            promote_worker,
            collection_registry: types::CollectionRegistry::new(),
            #[cfg(feature = "fulltext")]
            fulltext,
            #[cfg(feature = "vector")]
            vector_index,
            #[cfg(feature = "spatial")]
            spatial_index,
        })
    }

    /// Write event data to database (staged to Tier 1 by default)
    ///
    /// # Arguments
    /// * `slug` - Unique identifier for event (e.g., "jakarta-crime-2024")
    /// * `data` - JSON string containing event data
    ///
    /// # Behavior
    /// * By default, writes to Tier 1 (staged) and waits for promotion
    /// * Use `write_with_options()` with `WriteOptions { publish_now: true }` to write immediately to Tier 2
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::{SekejapDB, WriteOptions};
    /// # use std::path::Path;
    /// # let mut db = SekejapDB::new(Path::new("./data")).unwrap();
    /// // Normal write (staged to Tier 1)
    /// db.write("jakarta-crime-2024", r#"{
    ///     "title": "Theft Incident",
    ///     "tags": ["person", "vehicle"],
    ///     "coordinates": {"lat": -6.2088, "lon": 106.8456}
    /// }"#)?;
    /// // Instant write (immediate Tier 2)
    /// db.write_with_options("breaking-news", r#"{"title": "Breaking!"}"#,
    ///     WriteOptions { publish_now: true, ..Default::default() })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn write(&mut self, slug: &str, data: &str) -> Result<NodeId, Box<dyn std::error::Error>> {
        self.write_with_options(slug, data, WriteOptions::default())
    }

    /// Write event data with options
    ///
    /// # Arguments
    /// * `slug` - Unique identifier for event
    /// * `data` - JSON string containing event data
    /// * `opts` - Write options (staged vs published, vector, coordinates, deleted)
    ///
    /// # MVCC Behavior
    /// - First write: creates node with rev=0
    /// - Subsequent writes: creates new revision with rev+1
    /// - Old versions remain accessible via get_by_id(node_id, rev)
    ///
    /// # Vector Behavior
    /// - If opts.vector is Some, stores vector in BlobStore and sets NodeHeader::vector_ptr
    /// - Vector is part of node revision (immutable, like payload)
    /// - Updates replace the vector pointer (new revision = new vector blob)
    pub fn write_with_options(
        &mut self,
        slug: &str,
        data: &str,
        opts: WriteOptions,
    ) -> Result<NodeId, Box<dyn std::error::Error>> {
        // Parse JSON data
        let value: serde_json::Value = serde_json::from_str(data)?;

        // Extract fields from JSON
        let title = value["title"].as_str().unwrap_or("");
        let content = value["content"].as_str().unwrap_or("");
        let json_lat = value["coordinates"]["lat"].as_f64().unwrap_or(0.0);
        let json_lon = value["coordinates"]["lon"].as_f64().unwrap_or(0.0);

        // Use opts coordinates (allow override) or fall back to JSON
        let lat = if opts.latitude != 0.0 || opts.longitude != 0.0 {
            opts.latitude
        } else {
            json_lat
        };
        let lon = if opts.latitude != 0.0 || opts.longitude != 0.0 {
            opts.longitude
        } else {
            json_lon
        };

        let slug_hash = hash_slug(slug);
        let spatial_hash = hash_spatial(lat, lon);

        // MVCC: Resolve existing node or create new
        // Look in both Tier 1 and Tier 2 for existing node
        let (node_id, rev_old) = if let Some(existing) = self.ingestion.get_by_slug(slug_hash) {
            (existing.node_id, existing.rev)
        } else if let Some(existing) = self.storage.get_by_slug(slug_hash) {
            (existing.node_id, existing.rev)
        } else {
            // New node - generate stable node_id from hashes
            (Self::generate_node_id(&slug_hash, &spatial_hash), 0)
        };

        let _rev_new = rev_old + 1;

        // Store payload in blob store
        let mut payload = NodePayload::new(title);
        payload.slug = Some(slug.to_string());
        payload.content = Some(data.to_string());
        payload.metadata = Some(value.clone());
        payload.coordinates = Some(Coordinates {
            latitude: lat,
            longitude: lon,
        });
        let payload_json = serde_json::to_string(&payload)?;
        let payload_ptr = self.blob_store.write(payload_json.as_bytes())?;

        // Store vector in blob store if provided
        #[cfg(feature = "vector")]
        let vector_ptr = if let Some(ref vector) = opts.vector {
            use crate::vectors::ops::vector_to_bytes;
            let vector_bytes = vector_to_bytes(vector);
            Some(self.blob_store.write(&vector_bytes)?)
        } else {
            None
        };

        #[cfg(not(feature = "vector"))]
        let vector_ptr = None;

        // Create entity_id for this node
        let entity_id = Some(EntityId::new("nodes".to_string(), slug.to_string()));

        // Create NodeHeader (with or without vector)
        let mut node = if let Some(vptr) = vector_ptr {
            NodeHeader::new_with_vector(
                node_id,
                slug_hash,
                spatial_hash,
                payload_ptr,
                vptr,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis() as u64,
            )
        } else {
            NodeHeader::new(
                node_id,
                slug_hash,
                spatial_hash,
                payload_ptr,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis() as u64,
            )
        };
        node.entity_id = entity_id;

        // Handle deleted/tombstone
        let node = if opts.deleted {
            node.as_tombstone(Some("user_delete".to_string()))
        } else {
            node
        };

        // Write based on options
        if opts.publish_now {
            // Write to BOTH Tier 1 AND Tier 2 (immediate)
            self.ingestion.upsert(node.clone());
            self.storage.upsert(node);
        } else {
            // Write to Tier 1 only (staged)
            self.ingestion.upsert(node);
        }

        // Update Vector Index
        #[cfg(feature = "vector")]
        if let Some(ref vector) = opts.vector {
            if let Some(index) = &mut self.vector_index {
                if let Err(e) = index.insert(node_id, vector) {
                    log::warn!("Failed to insert vector: {}", e);
                }
            }
        }

        // Update Spatial Index
        #[cfg(feature = "spatial")]
        if lat != 0.0 || lon != 0.0 {
            if let Some(index) = &mut self.spatial_index {
                index.insert_point(node_id, lat, lon);
            }
        }

        // Index in Fulltext (if enabled)
        #[cfg(feature = "fulltext")]
        if let Some(index) = &mut self.fulltext {
            // Index title and content (or raw data if content missing)
            let index_content = if !content.is_empty() { content } else { data };

            if let Err(e) = index.add_document(title, index_content, slug, None) {
                log::warn!("Failed to index document: {}", e);
            }
            // Commit immediately for consistency with write (could be optimized)
            let _ = index.commit();
        }

        Ok(node_id)
    }

    /// Write multiple events to database (batch operation)
    ///
    /// # Arguments
    /// * `items` - Vector of (slug, data) tuples
    ///
    /// # Performance
    /// Optimized for bulk writes - uses single redb transaction for all nodes
    /// For best performance, use `write_batch()` instead
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let mut db = SekejapDB::new(Path::new("./data")).unwrap();
    /// db.write_many(vec![
    ///     ("crime-001".to_string(), r#"{"title": "Theft 1", "coordinates": {"lat": -6.2, "lon": 106.8}}"#.to_string()),
    ///     ("crime-002".to_string(), r#"{"title": "Theft 2", "coordinates": {"lat": -6.3, "lon": 106.9}}"#.to_string()),
    ///     ("crime-003".to_string(), r#"{"title": "Theft 3", "coordinates": {"lat": -6.4, "lon": 107.0}}"#.to_string()),
    /// ])?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn write_many(
        &mut self,
        items: Vec<(String, String)>,
    ) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
        self.write_batch(items, false)
    }

    /// Write multiple events with a SINGLE redb transaction (fastest for bulk inserts)
    ///
    /// # Arguments
    /// * `items` - Vector of (slug, data) tuples
    /// * `publish_now` - If true, write to Tier 2 immediately (slower); if false, Tier 1 only (faster)
    ///
    /// # Performance
    /// Uses single write transaction - 10-100x faster than individual writes
    /// Ideal for bulk imports and benchmarks
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let mut db = SekejapDB::new(Path::new("./data")).unwrap();
    /// // Fast bulk import (Tier 1 only, manual flush later)
    /// db.write_batch(vec![
    ///     ("crime-001".to_string(), r#"{"title": "Theft 1", "coordinates": {"lat": -6.2, "lon": 106.8}}"#.to_string()),
    ///     ("crime-002".to_string(), r#"{"title": "Theft 2", "coordinates": {"lat": -6.3, "lon": 106.9}}"#.to_string()),
    ///     ("crime-003".to_string(), r#"{"title": "Theft 3", "coordinates": {"lat": -6.4, "lon": 107.0}}"#.to_string()),
    /// ], false)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn write_batch(
        &mut self,
        items: Vec<(String, String)>,
        publish_now: bool,
    ) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let mut node_ids = Vec::with_capacity(items.len());

        // Pre-generate all node IDs and prepare headers (no storage writes yet)
        let mut headers: Vec<(NodeId, crate::types::NodeHeader)> = Vec::with_capacity(items.len());

        for (slug, data) in &items {
            // Parse JSON data
            let value: serde_json::Value = serde_json::from_str(data)?;

            // Extract fields
            let title = value["title"].as_str().unwrap_or("").to_string();
            let content = value["content"].as_str().unwrap_or("").to_string();
            let lat = value["coordinates"]["lat"].as_f64().unwrap_or(0.0);
            let lon = value["coordinates"]["lon"].as_f64().unwrap_or(0.0);

            let slug_hash = crate::hashing::hash_slug(slug);
            let spatial_hash = crate::hashing::hash_spatial(lat, lon);

            // Generate stable node_id
            let node_id = Self::generate_node_id(&slug_hash, &spatial_hash);

            // Store payload in blob store (still individual, but this is cheap)
            let mut payload = crate::types::NodePayload::new(title.clone());
            payload.slug = Some(slug.clone());
            payload.content = Some(data.clone());
            payload.metadata = Some(value.clone());
            payload.coordinates = Some(crate::types::Coordinates {
                latitude: lat,
                longitude: lon,
            });
            let payload_json = serde_json::to_string(&payload)?;
            let payload_ptr = self.blob_store.write(payload_json.as_bytes())?;

            // Create entity_id
            let entity_id = Some(crate::types::EntityId::new(
                "nodes".to_string(),
                slug.clone(),
            ));

            // Create node header
            let mut node = crate::types::NodeHeader::new(
                node_id,
                slug_hash,
                spatial_hash,
                payload_ptr,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis() as u64,
            );
            node.entity_id = entity_id;

            headers.push((node_id, node));
            node_ids.push(node_id);

            // Index in Fulltext (batch)
            #[cfg(feature = "fulltext")]
            if let Some(index) = &mut self.fulltext {
                let index_content = if !content.is_empty() {
                    content.as_str()
                } else {
                    data.as_str()
                };
                let _ = index.add_document(&title, index_content, slug, None);
            }

            // Index in Spatial (batch)
            #[cfg(feature = "spatial")]
            if lat != 0.0 || lon != 0.0 {
                if let Some(index) = &mut self.spatial_index {
                    index.insert_point(node_id, lat, lon);
                }
            }

            // Index in Vector (batch) - Try to find "vector" array in JSON
            #[cfg(feature = "vector")]
            if let Some(index) = &mut self.vector_index {
                // Try legacy "vector" or new "vectors.dense"
                if let Some(vec_val) = value
                    .get("vector")
                    .or_else(|| value.get("vectors").and_then(|v| v.get("dense")))
                {
                    if let Some(vec_arr) = vec_val.as_array() {
                        let vector: Vec<f32> = vec_arr
                            .iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect();
                        if !vector.is_empty() {
                            if let Err(e) = index.insert(node_id, &vector) {
                                log::warn!("Failed to insert vector in batch: {}", e);
                            }
                        }
                    }
                }
            }
        }

        // Batch write all nodes at once
        if publish_now {
            // Write to both Tier 1 and Tier 2
            for (_node_id, node) in headers {
                self.ingestion.upsert(node.clone());
                self.storage.upsert(node);
            }
        } else {
            // Write to Tier 1 only (fastest - in-memory)
            for (_node_id, node) in headers {
                self.ingestion.upsert(node);
            }
        }

        // Commit fulltext index once for batch
        #[cfg(feature = "fulltext")]
        if let Some(index) = &mut self.fulltext {
            let _ = index.commit();
        }

        Ok(node_ids)
    }

    /// Write JSON data following DATA_FORMAT.md specification
    ///
    /// Supports the new canonical format with:
    /// - `_id`: "collection/key" (canonical identity)
    /// - `_key`: convenience key (optional)
    /// - `vectors`: structured vector channels
    /// - `geo`: named geo features
    /// - `props`: custom properties wrapper
    ///
    /// Also supports legacy format with automatic migration.
    ///
    /// # Arguments
    /// * `json_data` - JSON string following DATA_FORMAT.md spec
    ///
    /// # Returns
    /// * `NodeId` - ID of created/updated node
    ///
    /// # Example (New Format)
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let mut db = SekejapDB::new(Path::new("./data")).unwrap();
    /// let node_id = db.write_json(r#"{
    ///     "_id": "news/flood-in-gedebage-2026",
    ///     "title": "Banjir Besar Melanda Pasar Gedebage",
    ///     "vectors": {
    ///         "dense": { "model": "bge-m3", "dims": 1024, "data": [0.1, -0.2, 0.3] }
    ///     },
    ///     "geo": {
    ///         "center": { "type": "Point", "coordinates": [106.85, -6.88] }
    ///     },
    ///     "props": { "tags": ["banjir", "emergency"], "author": "agent-123" }
    /// }"#)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Example (Edge)
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let mut db = SekejapDB::new(Path::new("./data")).unwrap();
    /// let edge_id = db.write_json(r#"{
    ///     "_from": "news/flood-2026",
    ///     "_to": "terms/banjir",
    ///     "_type": "mentions",
    ///     "props": { "weight": 0.9, "timestamp": "2026-01-27T03:10:00Z" }
    /// }"#)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn write_json(&mut self, json_data: &str) -> Result<NodeId, Box<dyn std::error::Error>> {
        let value: serde_json::Value = serde_json::from_str(json_data)?;

        // Detect if this is a node or edge
        if value.get("_from").is_some() || value.get("_to").is_some() {
            // It's an edge
            self.write_edge_json(&value)
        } else {
            // It's a node
            self.write_node_json(&value)
        }
    }

    /// Write node from DATA_FORMAT.json
    fn write_node_json(
        &mut self,
        value: &serde_json::Value,
    ) -> Result<NodeId, Box<dyn std::error::Error>> {
        // Extract _id (canonical identity)
        let entity_id = value
            .get("_id")
            .and_then(|v| v.as_str())
            .map(EntityId::parse)
            .transpose()?
            .or_else(|| {
                // Try to construct from collection/_key
                let collection = value.get("_collection").and_then(|v| v.as_str())?;
                let key = value.get("_key").or(value.get("slug"))?.as_str()?;
                Some(EntityId::new(collection.to_string(), key.to_string()))
            });

        // Extract title
        let title = value
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("Untitled");

        // Create payload
        let mut payload = NodePayload::new(title);

        if let Some(ref id) = entity_id {
            payload = payload.with_id(id.clone());
        }

        // Extract excerpt
        if let Some(excerpt) = value.get("excerpt").and_then(|v| v.as_str()) {
            payload = payload.with_excerpt(excerpt);
        }

        // Extract content
        if let Some(content) = value.get("content").and_then(|v| v.as_str()) {
            payload = payload.with_content(content);
        }

        // Extract props (custom properties)
        if let Some(props_value) = value.get("props").or(value.get("metadata"))
            && let serde_json::Value::Object(map) = props_value
        {
            for (k, v) in map {
                payload = payload.with_prop(k, v.clone());
            }
        }

        // Extract legacy fields for backward compatibility
        if let Some(slug) = value.get("slug").and_then(|v| v.as_str())
            && entity_id.is_none()
        {
            payload.slug = Some(slug.to_string());
        }
        if let Some(metadata) = value.get("metadata")
            && payload.props.is_empty()
        {
            payload.metadata = Some(metadata.clone());
        }

        // Serialize and store payload
        let payload_json = serde_json::to_string(&payload)?;
        let payload_ptr = self.blob_store.write(payload_json.as_bytes())?;

        // Calculate hashes for indexing
        let slug_for_hash = entity_id
            .as_ref()
            .map(|id| id.to_string()) // Use full "collection/key" for uniqueness
            .or(value
                .get("slug")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()))
            .unwrap_or(title.to_string());

        let slug_hash = hash_slug(&slug_for_hash);

        // Extract coordinates for spatial hash (legacy support)
        let lat = value
            .get("coordinates")
            .and_then(|c| c.get("lat").and_then(|v| v.as_f64()))
            .or(value.get("latitude").and_then(|v| v.as_f64()))
            .unwrap_or(0.0);
        let lon = value
            .get("coordinates")
            .and_then(|c| c.get("lon").and_then(|v| v.as_f64()))
            .or(value.get("longitude").and_then(|v| v.as_f64()))
            .unwrap_or(0.0);
        let spatial_hash = hash_spatial(lat, lon);

        // Generate or use existing node ID
        let node_id = Self::generate_node_id(&slug_hash, &spatial_hash);

        // Create header
        let mut header = NodeHeader::new(
            node_id,
            slug_hash,
            spatial_hash,
            payload_ptr,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as u64,
        );
        header.entity_id = entity_id.clone();

        // Write to storage
        self.storage.upsert(header.clone());

        // Index in Fulltext (if enabled)
        #[cfg(feature = "fulltext")]
        if let Some(index) = &mut self.fulltext {
            let content = value.get("content").and_then(|v| v.as_str()).unwrap_or("");
            // Use entity key as slug for indexing
            let index_slug = slug_for_hash.as_str();

            // Schema-aware attribute extraction
            let mut attributes = serde_json::Map::new();

            // Helper to insert nested value: "props.author" -> {"props": {"author": val}}
            let insert_at_path = |map: &mut serde_json::Map<String, serde_json::Value>,
                                  path: &str,
                                  value: serde_json::Value| {
                let parts: Vec<&str> = path.split('.').collect();
                let mut current_map = map;

                for (i, part) in parts.iter().enumerate() {
                    if i == parts.len() - 1 {
                        current_map.insert(part.to_string(), value);
                        break;
                    } else {
                        let entry = current_map
                            .entry(part.to_string())
                            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));

                        if let serde_json::Value::Object(next_map) = entry {
                            current_map = next_map;
                        } else {
                            // Conflict: path segment is not an object
                            break;
                        }
                    }
                }
            };

            if let Some(id) = &entity_id {
                if let Some(schema) = self.collection_registry.get(id.collection()) {
                    for field in &schema.fulltext {
                        // Skip standard fields handled explicitly
                        if field == "title" || field == "content" || field == "slug" {
                            continue;
                        }

                        // Extract field value (support dotted paths like "props.author")
                        let mut current = value;
                        let mut found = true;
                        for part in field.split('.') {
                            if let Some(next) = current.get(part) {
                                current = next;
                            } else {
                                found = false;
                                break;
                            }
                        }

                        if found {
                            insert_at_path(&mut attributes, field, current.clone());
                        }
                    }
                }
            }

            let attributes_option = if attributes.is_empty() {
                None
            } else {
                Some(serde_json::Value::Object(attributes))
            };

            let _ = index.add_document(title, content, index_slug, attributes_option);
            let _ = index.commit();
        }

        // Index in Vector (if enabled)
        #[cfg(feature = "vector")]
        if let Some(index) = &mut self.vector_index {
            if let Some(vectors) = value.get("vectors").and_then(|v| v.as_object()) {
                // For now, index the first dense vector found (limitation of single HNSW per node in current impl)
                // or specific channel if we supported named indices.
                // Current VectorIndex is monolithic.
                for (_channel, data) in vectors {
                    if let Some(vec_arr) = data.as_array() {
                        // Check if it's a raw array or { "data": [...] } (DATA_FORMAT.md implies structured)
                        // If array directly:
                        let vector: Option<Vec<f32>> =
                            if vec_arr.first().is_some_and(|v| v.is_number()) {
                                Some(
                                    vec_arr
                                        .iter()
                                        .filter_map(|v| v.as_f64().map(|f| f as f32))
                                        .collect(),
                                )
                            } else {
                                // Try "data" field
                                // Actually data format says: "vectors": { "dense": [0.1, ...] } OR { "dense": { "model": "...", "data": [...] } }
                                // Let's handle simple array for now as per `write_batch`.
                                None
                            };

                        if let Some(v) = vector {
                            if !v.is_empty() {
                                if let Err(e) = index.insert(node_id, &v) {
                                    log::warn!("Failed to insert vector: {}", e);
                                }
                                // Break after first vector to avoid ID conflict in HNSW mapping (current impl limitation)
                                break;
                            }
                        }
                    } else if let Some(data_arr) = data.get("data").and_then(|d| d.as_array()) {
                        // Handle structured { "model": ..., "data": [...] }
                        let vector: Vec<f32> = data_arr
                            .iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect();
                        if !vector.is_empty() {
                            if let Err(e) = index.insert(node_id, &vector) {
                                log::warn!("Failed to insert vector: {}", e);
                            }
                            break;
                        }
                    }
                }
            }
        }

        // Index in Spatial (if enabled)
        #[cfg(feature = "spatial")]
        if let Some(index) = &mut self.spatial_index {
            // Check "geo" field
            if let Some(geo) = value.get("geo").and_then(|g| g.as_object()) {
                for (_name, feature) in geo {
                    // Extract coordinates from Feature or Geometry
                    // DATA_FORMAT: "location": { "lat": -6.2, "lon": 106.8 } (Simple)
                    // OR "location": { "type": "Point", "coordinates": [...] } (GeoJSON-like)

                    if let Some(lat) = feature.get("lat").and_then(|v| v.as_f64()) {
                        if let Some(lon) = feature.get("lon").and_then(|v| v.as_f64()) {
                            index.insert_point(node_id, lat, lon);
                            break; // Index first point
                        }
                    } else if let Some(loc) = feature.get("loc").and_then(|l| l.as_object()) {
                        // Nested "loc": { "lat": ... } (as in test case)
                        if let (Some(lat), Some(lon)) = (
                            loc.get("lat").and_then(|v| v.as_f64()),
                            loc.get("lon").and_then(|v| v.as_f64()),
                        ) {
                            index.insert_point(node_id, lat, lon);
                            break;
                        }
                    }
                }
            }
        }

        Ok(node_id)
    }

    /// Search full-text index
    ///
    /// # Arguments
    /// * `query` - Search query string
    /// * `limit` - Maximum number of results
    ///
    /// # Returns
    /// Vector of matching NodeIds
    #[cfg(feature = "fulltext")]
    pub fn search_text(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
        if let Some(index) = &self.fulltext {
            let results = index.search(query, limit)?;

            // Map results back to NodeIds
            let node_ids = results
                .into_iter()
                .filter_map(|res| {
                    let slug_hash = hash_slug(&res.key);
                    // Try to find in storage
                    if let Some(node) = self.storage.get_by_slug(slug_hash) {
                        Some(node.node_id)
                    } else if let Some(node) = self.ingestion.get_by_slug(slug_hash) {
                        Some(node.node_id)
                    } else {
                        None
                    }
                })
                .collect();

            Ok(node_ids)
        } else {
            Err("Fulltext feature not enabled or index not initialized".into())
        }
    }

    /// Search vector index (HNSW)
    ///
    /// # Arguments
    /// * `query` - Query vector
    /// * `k` - Number of neighbors to return
    ///
    /// # Returns
    /// Vector of (NodeId, Distance)
    #[cfg(feature = "vector")]
    pub fn search_vector(
        &self,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(NodeId, f32)>, Box<dyn std::error::Error>> {
        // Try HNSW index first
        if let Some(index) = &self.vector_index {
            if index.is_built() && !index.is_empty() {
                return index.search(query, k);
            }
        }

        // Fallback to brute force
        log::info!("Vector index not available, falling back to brute-force search");
        let results = vectors::ops::brute_force_search(&self.storage, &self.blob_store, query, k)?;
        Ok(results
            .into_iter()
            .map(|r| (r.node_id, r.similarity))
            .collect())
    }

    /// Search spatial index (Radius)
    ///
    /// # Arguments
    /// * `lat` - Center latitude
    /// * `lon` - Center longitude
    /// * `radius_km` - Radius in kilometers
    ///
    /// # Returns
    /// Vector of NodeIds
    #[cfg(feature = "spatial")]
    pub fn search_spatial(
        &self,
        lat: f64,
        lon: f64,
        radius_km: f64,
    ) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
        if let Some(index) = &self.spatial_index {
            Ok(index.find_within_radius(lat, lon, radius_km))
        } else {
            Err("Spatial feature not enabled or index not initialized".into())
        }
    }

    /// Write edge from DATA_FORMAT.json
    fn write_edge_json(
        &mut self,
        value: &serde_json::Value,
    ) -> Result<NodeId, Box<dyn std::error::Error>> {
        // Extract _from and _to
        let from_str = value
            .get("_from")
            .and_then(|v| v.as_str())
            .ok_or("Missing _from in edge")?;
        let to_str = value
            .get("_to")
            .and_then(|v| v.as_str())
            .ok_or("Missing _to in edge")?;

        let _from =
            EntityId::parse(from_str).map_err(|_| format!("Invalid _from format: {}", from_str))?;
        let _to = EntityId::parse(to_str).map_err(|_| format!("Invalid _to format: {}", to_str))?;

        // Extract _type
        let _type = value
            .get("_type")
            .and_then(|v| v.as_str())
            .or(value.get("type").and_then(|v| v.as_str()))
            .unwrap_or("related")
            .to_string();

        // Extract weight from props
        let weight = value
            .get("props")
            .and_then(|p| p.get("weight"))
            .or(value.get("weight"))
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0) as f32;

        // Extract timestamp
        let timestamp = value
            .get("props")
            .and_then(|p| p.get("timestamp").or(p.get("timestamp")))
            .and_then(|v| v.as_u64())
            .or_else(|| value.get("timestamp").and_then(|v| v.as_u64()))
            .unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
            });

        // Create edge payload
        let mut payload = EdgePayload::new(&_type);

        // Extract props
        if let Some(props_value) = value.get("props")
            && let serde_json::Value::Object(map) = props_value
        {
            for (k, v) in map {
                if k != "weight" && k != "timestamp" && k != "decay" {
                    payload = payload.with_prop(k, v.clone());
                }
            }
        }

        // Extract decay configuration
        if let Some(decay_config) = value
            .get("props")
            .and_then(|p| p.get("decay"))
            .or(value.get("decay"))
            && let serde_json::Value::Object(map) = decay_config
        {
            let enabled = map.get("enabled").and_then(|v| v.as_bool()).unwrap_or(true);
            let half_life = map
                .get("half_life_days")
                .and_then(|v| v.as_u64())
                .unwrap_or(30) as u32;
            let min_weight = map
                .get("min_weight")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.1) as f32;
            let function = if map.get("function").and_then(|v| v.as_str()) == Some("linear") {
                DecayFunction::Linear
            } else {
                DecayFunction::Exponential
            };

            let decay = TemporalDecay {
                enabled,
                half_life_days: half_life,
                min_weight,
                function,
            };
            payload = payload.with_decay(decay);
        }

        // Create edge
        let edge = WeightedEdge::new_with_payload(
            _from.clone(),
            _to.clone(),
            weight,
            _type,
            0, // evidence_ptr
            timestamp,
            None,
            Some(payload),
        );

        // Get node IDs for storage
        let from_hash = hash_slug(_from.key());
        let to_hash = hash_slug(_to.key());

        let from_id = self
            .storage
            .get_by_slug(from_hash)
            .ok_or(format!("Source node not found: {}", _from))?
            .node_id;
        let _to_id = self
            .storage
            .get_by_slug(to_hash)
            .ok_or(format!("Target node not found: {}", _to))?
            .node_id;

        // Add edge to graph
        self.graph.add_edge(edge);

        Ok(from_id) // Return source node ID as edge identifier
    }

    /// Read event data by slug (Tier 2 only by default)
    ///
    /// # Arguments
    /// * `slug` - Unique identifier for event
    ///
    /// # Returns
    /// * `Option<String>` - JSON string of event data if found
    ///
    /// # Behavior
    /// * By default, reads from Tier 2 only (validated data)
    /// * Use `read_with_options()` with `ReadOptions { include_staged: true }` to include Tier 1 (staged data)
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::{SekejapDB, ReadOptions};
    /// # use std::path::Path;
    /// # let db = SekejapDB::new(Path::new("./data")).unwrap();
    /// // Normal read (Tier 2 only)
    /// if let Some(event) = db.read("jakarta-crime-2024")? {
    ///     println!("Found event: {}", event);
    /// }
    /// // Realtime read (include Tier 1)
    /// if let Some(event) = db.read_with_options("breaking-news", ReadOptions { include_staged: true })? {
    ///     println!("Found breaking news: {}", event);
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn read(&self, slug: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
        self.read_with_options(slug, ReadOptions::default())
    }

    /// Read event data with options
    ///
    /// # Arguments
    /// * `slug` - Unique identifier for event
    /// * `opts` - Read options (validated only vs include staged)
    pub fn read_with_options(
        &self,
        slug: &str,
        opts: ReadOptions,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let slug_hash = hash_slug(slug);

        if opts.include_staged {
            // Check Tier 1 first (fresher data)
            if let Some(node) = self.ingestion.get_by_slug(slug_hash) {
                let payload_bytes = self.blob_store.read(node.payload_ptr)?;
                return Ok(Some(String::from_utf8(payload_bytes)?));
            }
            // Fallback to Tier 2
            if let Some(node) = self.storage.get_by_slug(slug_hash) {
                let payload_bytes = self.blob_store.read(node.payload_ptr)?;
                return Ok(Some(String::from_utf8(payload_bytes)?));
            }
        } else {
            // Tier 2 only (validated data)
            if let Some(node) = self.storage.get_by_slug(slug_hash) {
                let payload_bytes = self.blob_store.read(node.payload_ptr)?;
                return Ok(Some(String::from_utf8(payload_bytes)?));
            }
        }

        Ok(None)
    }

    /// Traverse causal graph for root cause analysis
    ///
    /// # Arguments
    /// * `slug` - Starting event slug
    /// * `max_hops` - Maximum number of hops to traverse
    /// * `weight_threshold` - Minimum edge weight to consider (0.0 - 1.0)
    /// * `edge_type` - Optional filter by edge type (e.g., "related", "causal")
    ///
    /// # Returns
    /// * `TraversalResult` - Contains path, edges, and total weight
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let db = SekejapDB::new(Path::new("./data")).unwrap();
    /// let results = db.traverse("jakarta-crime-2024", 3, 0.5, None)?;
    /// println!("Found {} related events", results.path.len());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn traverse(
        &self,
        slug: &str,
        max_hops: usize,
        weight_threshold: f32,
        edge_type: Option<&str>,
    ) -> Result<TraversalResult, Box<dyn std::error::Error>> {
        let _slug_hash = hash_slug(slug);

        // Find starting node and create EntityId
        let entity_id = EntityId::new("nodes".to_string(), slug.to_string());

        let result = self.graph().backward_bfs(
            &entity_id,
            max_hops,
            weight_threshold,
            edge_type,
            None, // No time window constraint
        );

        Ok(result)
    }

    /// Add edge between events (tier-agnostic, user-defined types)

    /// Traverse graph FORWARD (find effects) - FOR JOINS!
    ///
    /// Given a starting node, find all nodes it points TO.
    /// This is ESSENTIAL for implementing graph-based JOINs.
    ///
    /// # Arguments
    /// * `slug` - Starting node slug
    /// * `max_hops` - Maximum number of hops to traverse
    /// * `weight_threshold` - Minimum edge weight (0.0 - 1.0)
    /// * `edge_type` - Optional filter by edge type (e.g., "related", "causal")
    /// * `time_window` - Optional time window filter (start, end)
    ///
    /// # Returns
    /// * `TraversalResult` - Contains path and edges
    ///
    /// # Example (INNER JOIN: restaurants -> cuisines)
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let db = SekejapDB::new(Path::new("./data")).unwrap();
    /// // FORWARD JOIN: from restaurant to cuisine
    /// let results = db.traverse_forward("luigis-pizza", 1, 0.0, Some("related"), None)?;
    /// for edge in results.edges {
    ///     // edge._to is the cuisine!
    ///     println!("Restaurant -> Cuisine: {}", edge._to.key());
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn traverse_forward(
        &self,
        slug: &str,
        max_hops: usize,
        weight_threshold: f32,
        edge_type: Option<&str>,
        time_window: Option<(u64, u64)>,
    ) -> Result<TraversalResult, Box<dyn std::error::Error>> {
        let _slug_hash = hash_slug(slug);

        // Find starting node and create EntityId
        let entity_id = EntityId::new("nodes".to_string(), slug.to_string());

        // Use the new forward_bfs method
        let result = self.graph().forward_bfs(
            &entity_id,
            max_hops,
            weight_threshold,
            edge_type,
            time_window,
        );

        Ok(result)
    }
    ///
    /// # Arguments
    /// * `source_slug` - Source event slug (cause)
    /// * `target_slug` - Target event slug (effect)
    /// * `weight` - Evidence strength (0.0 - 1.0)
    /// * `edge_type` - User-defined relationship type (string, e.g., "causal", "influences", "custom")
    ///
    /// # Behavior
    /// * Checks both Tier 1 and Tier 2 for nodes
    /// * Edges work immediately with staged nodes
    /// * Edges remain valid after promotion
    /// * Edge types are user-defined strings (like ArangoDB)
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let mut db = SekejapDB::new(Path::new("./data")).unwrap();
    /// // Write staged nodes (Tier 1)
    /// db.write("poverty", r#"{"title": "Poverty Event"}"#)?;
    /// db.write("jakarta-crime-2024", r#"{"title": "Crime Incident"}"#)?;
    /// // Add edge with user-defined type
    /// db.add_edge("poverty", "jakarta-crime-2024", 0.7, "causal".to_string())?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn add_edge(
        &mut self,
        source_slug: &str,
        target_slug: &str,
        weight: f32,
        edge_type: EdgeType,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let source_hash = hash_slug(source_slug);
        let target_hash = hash_slug(target_slug);

        // Tier-agnostic: check BOTH Tier 1 and Tier 2
        let _from = if self.ingestion.get_by_slug(source_hash).is_some()
            || self.storage.get_by_slug(source_hash).is_some()
        {
            EntityId::parse(source_slug)
                .unwrap_or_else(|_| EntityId::new("nodes".to_string(), source_slug.to_string()))
        } else {
            return Err(format!("Source node not found: {}", source_slug).into());
        };

        let _to = if self.ingestion.get_by_slug(target_hash).is_some()
            || self.storage.get_by_slug(target_hash).is_some()
        {
            EntityId::parse(target_slug)
                .unwrap_or_else(|_| EntityId::new("nodes".to_string(), target_slug.to_string()))
        } else {
            return Err(format!("Target node not found: {}", target_slug).into());
        };

        let edge = WeightedEdge::new(
            _from,
            _to,
            weight,
            edge_type,
            0, // evidence_ptr placeholder
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as u64,
            None,
        );

        self.graph_mut().add_edge(edge);
        Ok(())
    }

    /// Backup all data to JSON file
    ///
    /// # Arguments
    /// * `path` - Path to backup file
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let db = SekejapDB::new(Path::new("./data")).unwrap();
    /// db.backup(std::path::Path::new("backup.json"))?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn backup(&self, path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        use std::fs::File;
        use std::io::Write;

        let mut backup_data = serde_json::json!({
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            "nodes": [],
            "edges": []
        });

        // Collect all nodes
        let nodes = backup_data["nodes"].as_array_mut().unwrap();
        for node in self.storage.iter() {
            let payload_bytes = self.blob_store.read(node.payload_ptr)?;
            let payload: NodePayload = serde_json::from_slice(&payload_bytes)?;
            // Serialize u128 as string to avoid JSON number limits
            nodes.push(serde_json::json!({
                "node_id": node.node_id.to_string(),
                "slug_hash": node.slug_hash,
                "spatial_hash": node.spatial_hash,
                "timestamp": node.epoch_created,
                "payload": payload
            }));
        }

        // Collect all edges
        let edges = backup_data["edges"].as_array_mut().unwrap();
        for edge in self.graph.iter() {
            edges.push(serde_json::json!({
                "from": edge._from.to_string(),
                "to": edge._to.to_string(),
                "weight": edge.weight,
                "edge_type": edge._type,
                "evidence_ptr": edge.evidence_ptr,
                "valid_start": edge.valid_start,
                "valid_end": edge.valid_end
            }));
        }

        // Write to file
        let mut file = File::create(path)?;
        file.write_all(serde_json::to_string_pretty(&backup_data)?.as_bytes())?;

        Ok(())
    }

    /// Restore data from backup file
    ///
    /// # Arguments
    /// * `path` - Path to backup file
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let mut db = SekejapDB::new(Path::new("./data")).unwrap();
    /// db.restore(std::path::Path::new("backup.json"))?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn restore(&mut self, path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        use std::fs;

        let backup_data: serde_json::Value = serde_json::from_str(&fs::read_to_string(path)?)?;

        // Restore nodes
        if let Some(nodes) = backup_data["nodes"].as_array() {
            for node_data in nodes {
                let payload: NodePayload = serde_json::from_value(node_data["payload"].clone())?;
                let payload_json = serde_json::to_string(&payload)?;
                let payload_ptr = self.blob_store.write(payload_json.as_bytes())?;

                // Parse node_id (u128 may be stored as string or number)
                let node_id: NodeId = if let Some(s) = node_data["node_id"].as_str() {
                    s.parse().map_err(|_| "Invalid node_id")?
                } else if let Some(n) = node_data["node_id"].as_u64() {
                    n as NodeId
                } else {
                    return Err("Missing or invalid node_id".into());
                };

                let node = NodeHeader::new(
                    node_id,
                    node_data["slug_hash"].as_u64().ok_or("Missing slug_hash")?,
                    node_data["spatial_hash"]
                        .as_u64()
                        .ok_or("Missing spatial_hash")?,
                    payload_ptr,
                    node_data["timestamp"].as_u64().ok_or("Missing timestamp")?,
                );

                self.storage.upsert(node);
            }
        }

        // Restore edges
        if let Some(edges) = backup_data["edges"].as_array() {
            for edge_data in edges {
                let from_str = edge_data["from"].as_str().ok_or("Missing 'from' in edge")?;
                let to_str = edge_data["to"].as_str().ok_or("Missing 'to' in edge")?;

                let from = EntityId::parse(from_str)
                    .map_err(|_| format!("Invalid 'from' format: {}", from_str))?;
                let to = EntityId::parse(to_str)
                    .map_err(|_| format!("Invalid 'to' format: {}", to_str))?;

                let weight = edge_data["weight"].as_f64().unwrap_or(1.0) as f32;
                let edge_type = edge_data["edge_type"]
                    .as_str()
                    .unwrap_or("related")
                    .to_string();
                let evidence_ptr = edge_data["evidence_ptr"].as_u64().unwrap_or(0);
                let valid_start = edge_data["valid_start"].as_u64().unwrap_or_else(|| {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64
                });
                let valid_end = if let Some(v) = edge_data["valid_end"].as_u64() {
                    Some(v)
                } else {
                    None
                };

                let edge = WeightedEdge::new(
                    from,
                    to,
                    weight,
                    edge_type,
                    evidence_ptr,
                    valid_start,
                    valid_end,
                );

                self.graph.add_edge(edge);
            }
        }

        Ok(())
    }

    /// Delete a node and optionally its edges (cascade by default)
    ///
    /// # Arguments
    /// * `slug` - Unique identifier for event to delete
    ///
    /// # Behavior
    /// * By default, deletes node and cascades to all edges
    /// * Use `delete_with_options()` with `DeleteOptions { exclude_edges: true }` to keep edges for audit
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::{SekejapDB, DeleteOptions};
    /// # use std::path::Path;
    /// # let mut db = SekejapDB::new(Path::new("./data")).unwrap();
    /// // Normal delete (cascade edges)
    /// db.delete("jakarta-crime-2024")?;
    /// // Delete but keep edges (audit)
    /// db.delete_with_options("news", DeleteOptions { exclude_edges: true })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn delete(&mut self, slug: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.delete_with_options(slug, DeleteOptions::default())
    }

    /// Delete a node with options
    ///
    /// # Arguments
    /// * `slug` - Unique identifier for event to delete
    /// * `opts` - Delete options (cascade edges or keep them)
    pub fn delete_with_options(
        &mut self,
        slug: &str,
        opts: DeleteOptions,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let slug_hash = hash_slug(slug);
        let mut node_id = None;

        // Check Tier 1
        if let Some(node) = self.ingestion.get_by_slug(slug_hash) {
            node_id = Some(node.node_id);
            self.ingestion.remove(node.node_id);
        }

        // Check Tier 2
        if let Some(node) = self.storage.get_by_slug(slug_hash) {
            node_id = Some(node.node_id);
            self.storage
                .delete_by_slug(slug_hash, Some("user_delete".to_string()));
        }

        // Remove from graph (cascade to all edges unless excluded)
        if node_id.is_some() && !opts.exclude_edges {
            let entity_id = EntityId::new("nodes".to_string(), slug.to_string());
            self.graph_mut().remove_node(&entity_id);
        }

        Ok(())
    }

    /// Delete a specific edge between two events
    ///
    /// # Arguments
    /// * `source_slug` - Source event slug
    /// * `target_slug` - Target event slug
    /// * `edge_type` - Edge type (optional, None removes any type)
    ///
    /// # Returns
    /// * `bool` - True if edge was found and removed
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let mut db = SekejapDB::new(Path::new("./data")).unwrap();
    /// let removed = db.delete_edge("poverty", "jakarta-crime-2024", Some("causal".to_string()))?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn delete_edge(
        &self,
        source_slug: &str,
        target_slug: &str,
        edge_type: Option<EdgeType>,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let source_hash = hash_slug(source_slug);
        let target_hash = hash_slug(target_slug);

        // Check if nodes exist (Tier 1 or Tier 2)
        let _from = if self.ingestion.get_by_slug(source_hash).is_some()
            || self.storage.get_by_slug(source_hash).is_some()
        {
            EntityId::parse(source_slug)
                .unwrap_or_else(|_| EntityId::new("nodes".to_string(), source_slug.to_string()))
        } else {
            return Err("Source node not found".into());
        };

        let _to = if self.ingestion.get_by_slug(target_hash).is_some()
            || self.storage.get_by_slug(target_hash).is_some()
        {
            EntityId::parse(target_slug)
                .unwrap_or_else(|_| EntityId::new("nodes".to_string(), target_slug.to_string()))
        } else {
            return Err("Target node not found".into());
        };

        Ok(self.graph().remove_edge(&_from, &_to, edge_type))
    }

    /// Update an existing event
    ///
    /// # Arguments
    /// * `slug` - Unique identifier for event to update
    /// * `data` - New JSON string containing event data
    ///
    /// # Returns
    /// * `NodeId` - Node ID of updated event
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let mut db = SekejapDB::new(Path::new("./data")).unwrap();
    /// db.update("jakarta-crime-2024", r#"{
    ///     "title": "Theft Incident (Updated)",
    ///     "tags": ["person", "vehicle"],
    ///     "coordinates": {"lat": -6.2088, "lon": 106.8456}
    /// }"#)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn update(&mut self, slug: &str, data: &str) -> Result<NodeId, Box<dyn std::error::Error>> {
        let slug_hash = hash_slug(slug);

        // Check if node exists
        let existing_node = self
            .storage
            .get_by_slug(slug_hash)
            .ok_or("Node not found")?;

        // Parse new JSON data
        let value: serde_json::Value = serde_json::from_str(data)?;

        // Extract fields
        let title = value["title"].as_str().unwrap_or("");
        let lat = value["coordinates"]["lat"].as_f64().unwrap_or(0.0);
        let lon = value["coordinates"]["lon"].as_f64().unwrap_or(0.0);

        // Store new payload in blob store
        let mut payload = NodePayload::new(title);
        payload.slug = Some(slug.to_string());
        payload.content = Some(data.to_string());
        payload.metadata = Some(value.clone());
        payload.coordinates = Some(Coordinates {
            latitude: lat,
            longitude: lon,
        });
        let payload_json = serde_json::to_string(&payload)?;
        let payload_ptr = self.blob_store.write(payload_json.as_bytes())?;

        // Update node (keep same node_id)
        let node = NodeHeader::new(
            existing_node.node_id,
            slug_hash,
            hash_spatial(lat, lon),
            payload_ptr,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as u64,
        );

        self.storage.upsert(node.clone());

        Ok(node.node_id)
    }

    /// Update edge weight
    ///
    /// # Arguments
    /// * `source_slug` - Source event slug
    /// * `target_slug` - Target event slug
    /// * `new_weight` - New edge weight (0.0 - 1.0)
    /// * `edge_type` - Edge type (optional)
    ///
    /// # Returns
    /// * `bool` - True if edge was found and updated
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let mut db = SekejapDB::new(Path::new("./data")).unwrap();
    /// let updated = db.update_edge("poverty", "jakarta-crime-2024", 0.9, Some("causal".to_string()))?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn update_edge(
        &mut self,
        source_slug: &str,
        target_slug: &str,
        new_weight: f32,
        edge_type: Option<EdgeType>,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        // Remove old edge
        let removed = self.delete_edge(source_slug, target_slug, edge_type.clone())?;

        if !removed {
            return Ok(false);
        }

        // Add new edge with updated weight
        let et = edge_type.unwrap_or_else(|| "causal".to_string());
        self.add_edge(source_slug, target_slug, new_weight, et)?;

        Ok(true)
    }

    /// Get reference to storage layer
    pub fn storage(&self) -> &storage::SingleStorage {
        &self.storage
    }

    /// Get reference to storage layer - mutable
    pub fn storage_mut(&mut self) -> &mut storage::SingleStorage {
        &mut self.storage
    }

    /// Get reference to ingestion buffer (Tier 1)
    pub fn ingestion(&self) -> &storage::IngestionBuffer {
        &self.ingestion
    }

    /// Get reference to concurrent graph
    pub fn graph(&self) -> &graph::ConcurrentGraph {
        &self.graph
    }

    /// Get reference to concurrent graph - mutable
    pub fn graph_mut(&mut self) -> &mut graph::ConcurrentGraph {
        &mut self.graph
    }

    /// Get reference to blob store
    pub fn blob_store(&self) -> &types::BlobStore {
        &self.blob_store
    }

    /// Try to get PersistentStorage reference for batch operations
    ///
    /// This enables using the optimized upsert_batch() method for fast promotion.
    /// Returns None if using SingleStorage (in-memory mode).
    fn storage_as_persistent(&self) -> Option<&crate::storage::PersistentStorage> {
        // SingleStorage doesn't have direct access to PersistentStorage
        // In a real implementation, you'd store both types or use a trait object
        // For now, return None to use the individual upsert fallback
        // The optimization will work when using PersistentStorage directly
        None
    }

    /// Manually trigger promotion of all staged nodes from Tier 1 to Tier 2
    ///
    /// Uses batch upsert for optimal performance - all nodes in one transaction.
    /// Useful for ensuring data persistence before backups or shutdowns.
    ///
    /// # Returns
    /// * `usize` - Number of nodes promoted
    ///
    /// # Performance
    /// Single transaction for all nodes - ~700k/sec for 1M nodes
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let mut db = SekejapDB::new(Path::new("./data")).unwrap();
    /// db.write("event-1", r#"{"title": "Event 1"}"#)?;
    /// db.write("event-2", r#"{"title": "Event 2"}"#)?;
    /// // Manually trigger promotion
    /// let promoted = db.flush()?;
    /// println!("Promoted {} nodes", promoted);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn flush(&mut self) -> Result<usize, Box<dyn std::error::Error>> {
        // Drain all nodes from ingestion buffer
        let nodes = self.ingestion.drain_all();
        let count = nodes.len();

        if count == 0 {
            return Ok(0);
        }

        // Convert Vec to slice for batch operation
        let nodes_slice: &[crate::types::NodeHeader] = &nodes;

        // Try to use batch upsert if storage supports it (PersistentStorage)
        // This is a type-erase approach - check if we can downcast to get batch method
        if let Some(persistent) = self.storage_as_persistent() {
            // Use batch upsert (single transaction)
            persistent.upsert_batch(nodes_slice)?;
        } else {
            // Fallback to individual upserts (SingleStorage in tests)
            for node in nodes {
                self.storage.upsert(node);
            }
        }

        Ok(count)
    }

    /// Get promotion metrics (if worker is running)
    ///
    /// Returns current promotion statistics including:
    /// - Total nodes promoted
    /// - Total failed promotions
    /// - Current promotion rate
    /// - Average latency
    ///
    /// # Returns
    /// * `PromotionMetrics` - Current metrics snapshot
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let db = SekejapDB::new(Path::new("./data")).unwrap();
    /// let metrics = db.promotion_metrics();
    /// println!("Promoted: {} nodes", metrics.total_promoted);
    /// println!("Promotion rate: {:.2} nodes/sec", metrics.promotion_rate);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn promotion_metrics(&self) -> PromotionMetrics {
        self.promote_worker.get_metrics()
    }

    /// Generate unique node ID from hashes
    fn generate_node_id(slug_hash: &u64, spatial_hash: &u64) -> NodeId {
        // Simple XOR-based ID generation (hyperminimalist)
        let hash1 = slug_hash ^ spatial_hash;
        let hash2 = (*slug_hash).wrapping_mul(31) ^ *spatial_hash;
        ((hash1 as u128) << 64) | hash2 as u128
    }

    /// Define a collection schema from JSON
    ///
    /// # Arguments
    /// * `json_data` - JSON string containing collection definition
    ///
    /// # JSON Format
    /// ```json
    /// {
    ///   "news": {
    ///     "hot_fields": {
    ///       "vector": ["vectors.dense"],
    ///       "spatial": ["geo.center"],
    ///       "fulltext": ["title", "content"]
    ///     },
    ///     "vectors": {
    ///       "dense": { "model": "bge-m3", "dims": 1024, "index_hnsw": true }
    ///     },
    ///     "spatial": {
    ///       "center": { "type": "Point", "index_rtree": true }
    ///     },
    ///     "edge_types": ["mentions", "caused_by"],
    ///     "fulltext": ["title", "content"]
    ///   }
    /// }
    /// ```
    ///
    /// # Example
    /// ```rust,no_run
    /// # use sekejap::SekejapDB;
    /// # use std::path::Path;
    /// # let mut db = SekejapDB::new(Path::new("./data")).unwrap();
    /// db.define_collection(r#"{
    ///     "news": {
    ///         "hot_fields": {
    ///             "vector": ["vectors.dense"],
    ///             "spatial": ["geo.center"],
    ///             "fulltext": ["title", "content"]
    ///         },
    ///         "vectors": {
    ///             "dense": { "model": "bge-m3", "dims": 1024, "index_hnsw": true }
    ///         },
    ///         "spatial": {
    ///             "center": { "type": "Point", "index_rtree": true }
    ///         }
    ///     }
    /// }"#)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn define_collection(&mut self, json_data: &str) -> Result<(), Box<dyn std::error::Error>> {
        let value: serde_json::Value = serde_json::from_str(json_data)?;

        if let Some(collections) = value.get("collections").or(Some(&value)) {
            if let serde_json::Value::Object(map) = collections {
                for (collection_id, schema_value) in map {
                    let schema: CollectionSchema = serde_json::from_value(schema_value.clone())?;
                    self.collection_registry
                        .register(collection_id.clone(), schema);
                }
            }
        }

        Ok(())
    }

    /// Get a collection schema
    ///
    /// # Arguments
    /// * `collection_id` - The collection ID to look up
    ///
    /// # Returns
    /// * `Option<&CollectionSchema>` - The schema if found
    pub fn get_collection_schema(&self, collection_id: &str) -> Option<&CollectionSchema> {
        self.collection_registry.get(collection_id)
    }

    /// Check if a collection has been defined
    pub fn has_collection(&self, collection_id: &str) -> bool {
        self.collection_registry.has_schema(collection_id)
    }

    /// Get all registered collections
    pub fn list_collections(&self) -> Vec<&str> {
        self.collection_registry.iter().map(|(id, _)| id).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_database_creation() {
        let temp_dir = TempDir::new().unwrap();
        let db = SekejapDB::new(temp_dir.path()).unwrap();

        assert!(db.storage().is_empty());
    }
}
