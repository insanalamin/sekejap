//! Main database engine for Sekejap-DB
//! 
//! Provides high-level API for multi-modal event data storage, retrieval, and traversal.
//! Unifies Graph, Vector, Spatial, and Text search into a single hardware-aligned engine.

use crate::config::PromotionConfig;
use crate::types::NodeHeader;
use log;
use std::path::Path;
use std::sync::Arc;

pub mod atoms;
pub mod config;
pub mod graph;
pub mod hashing;
pub mod index;
pub mod query;
pub mod sekejapql;
pub mod storage;
pub mod types;
pub mod vectors;

pub use types:: { 
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
    NodeHeader as NodeHeaderType,
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
    ingestion: Arc<storage::IngestionBuffer>, // Tier 1
    storage: Arc<storage::SingleStorage>,     // Tier 2
    graph: graph::ConcurrentGraph,            // Tier 3
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
        let ingestion = Arc::new(storage::IngestionBuffer::new(base_dir.join("ingestion"))?);
        let storage = Arc::new(storage::SingleStorage::new(base_dir.join("storage"))?);

        // Auto-start promotion worker
        let mut promote_worker = storage::PromoteWorker::new(PromotionConfig::default());
        let storage_for_worker: Arc<dyn BatchUpsert + Send + Sync> = storage.clone();
        let _ = promote_worker.start(Arc::clone(&ingestion), storage_for_worker);

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
            promote_worker, // Auto-promotion worker
            collection_registry: types::CollectionRegistry::new(), // Collection schemas
            #[cfg(feature = "fulltext")]
            fulltext,
            #[cfg(feature = "vector")]
            vector_index,
            #[cfg(feature = "spatial")]
            spatial_index,
        })
    }

    /// Helper to downcast storage to PersistentStorage if possible
    fn storage_as_persistent(&self) -> Option<&storage::SingleStorage> {
        Some(&self.storage)
    }

    /// Generate stable node ID from slug and spatial hashes
    fn generate_node_id(slug_hash: &SlugHash, spatial_hash: &SpatialHash) -> NodeId {
        (slug_hash ^ spatial_hash) as NodeId
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
    /// db.write_with_options("breaking-news", r#"{ "title": "Breaking!" }"#, 
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
    pub fn write_many(
        &mut self,
        items: Vec<(String, String)>, 
    ) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
        self.write_batch(items, false)
    }

    /// Write multiple events with a SINGLE redb transaction (fastest for bulk inserts)
    pub fn write_batch(
        &mut self,
        items: Vec<(String, String)>, 
        publish_now: bool,
    ) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let mut node_ids = Vec::with_capacity(items.len());
        let mut headers: Vec<(NodeId, crate::types::NodeHeader)> = Vec::with_capacity(items.len());

        for (slug, data) in &items {
            let value: serde_json::Value = serde_json::from_str(data)?;
            let title = value["title"].as_str().unwrap_or("").to_string();
            let content = value["content"].as_str().unwrap_or("").to_string();
            let lat = value["coordinates"]["lat"].as_f64().unwrap_or(0.0);
            let lon = value["coordinates"]["lon"].as_f64().unwrap_or(0.0);

            let slug_hash = crate::hashing::hash_slug(slug);
            let spatial_hash = crate::hashing::hash_spatial(lat, lon);
            let node_id = Self::generate_node_id(&slug_hash, &spatial_hash);

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

            let entity_id = Some(crate::types::EntityId::new(
                "nodes".to_string(),
                slug.clone(),
            ));

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

            #[cfg(feature = "fulltext")]
            if let Some(index) = &mut self.fulltext {
                let index_content = if !content.is_empty() {
                    content.as_str()
                } else {
                    data.as_str()
                };
                let _ = index.add_document(&title, index_content, slug, None);
            }

            #[cfg(feature = "spatial")]
            if lat != 0.0 || lon != 0.0 {
                if let Some(index) = &mut self.spatial_index {
                    index.insert_point(node_id, lat, lon);
                }
            }

            #[cfg(feature = "vector")]
            if let Some(index) = &mut self.vector_index {
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

        if publish_now {
            for (_node_id, node) in headers {
                self.ingestion.upsert(node.clone());
                self.storage.upsert(node);
            }
        } else {
            for (_node_id, node) in headers {
                self.ingestion.upsert(node);
            }
        }

        #[cfg(feature = "fulltext")]
        if let Some(index) = &mut self.fulltext {
            let _ = index.commit();
        }

        Ok(node_ids)
    }

    /// Write JSON data following DATA_FORMAT.md specification
            pub fn write_json(&mut self, json_data: &str) -> Result<NodeId, Box<dyn std::error::Error>> {
                println!("DEBUG: write_json called with data: {}", json_data);
                let value: serde_json::Value = serde_json::from_str(json_data)?;
                if value.get("_from").is_some() || value.get("_to").is_some() {
                    println!("DEBUG: Routing to write_edge_json");
                    self.write_edge_json(&value)
                } else {
                    println!("DEBUG: Routing to write_node_json");
                    self.write_node_json(&value)
                }
            }
        
    /// Write node from DATA_FORMAT.json
    fn write_node_json(
        &mut self,
        value: &serde_json::Value,
    ) -> Result<NodeId, Box<dyn std::error::Error>> {
        let entity_id = value
            .get("_id")
            .and_then(|v| v.as_str())
            .map(EntityId::parse)
            .transpose()? 
            .or_else(|| {
                let collection = value.get("_collection").and_then(|v| v.as_str())?;
                let key = value.get("_key").or(value.get("slug"))?.as_str()?;
                Some(EntityId::new(collection.to_string(), key.to_string()))
            });

        let title = value
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("Untitled");

        let mut payload = NodePayload::new(title);
        if let Some(ref id) = entity_id {
            payload = payload.with_id(id.clone());
        }

        if let Some(excerpt) = value.get("excerpt").and_then(|v| v.as_str()) {
            payload = payload.with_excerpt(excerpt);
        }
        if let Some(content) = value.get("content").and_then(|v| v.as_str()) {
            payload = payload.with_content(content);
        }
        if let Some(props_value) = value.get("props").or(value.get("metadata")) && let serde_json::Value::Object(map) = props_value {
            for (k, v) in map {
                payload = payload.with_prop(k, v.clone());
            }
        }

        let payload_json = serde_json::to_string(&payload)?;
        let payload_ptr = self.blob_store.write(payload_json.as_bytes())?;

        let slug_for_hash = entity_id
            .as_ref()
            .map(|id| id.to_string())
            .or(
                value
                    .get("slug")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            )
            .unwrap_or(title.to_string());

        let slug_hash = hash_slug(&slug_for_hash);
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
        let node_id = Self::generate_node_id(&slug_hash, &spatial_hash);

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

        self.storage.upsert(header.clone());

        #[cfg(feature = "fulltext")]
        if let Some(index) = &mut self.fulltext {
            let content = value.get("content").and_then(|v| v.as_str()).unwrap_or("");
            let index_slug = slug_for_hash.as_str();
            let mut attributes = serde_json::Map::new();

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
                            break;
                        }
                    }
                }
            };

            if let Some(id) = &entity_id {
                if let Some(schema) = self.collection_registry.get(id.collection()) {
                    println!("DEBUG: Indexing with schema for collection: {}", id.collection());
                    for field in &schema.fulltext {
                        if field == "title" || field == "content" || field == "slug" { continue; }
                        let mut curr = value;
                        let mut found = true;
                        for p in field.split('.') { if let Some(n) = curr.get(p) { curr = n; } else { found = false; break; } } // This line is problematic
                        if found { 
                            println!("DEBUG: Indexing attribute field: {} = {}", field, curr);
                            insert_at_path(&mut attributes, field, curr.clone()); 
                        }
                    }
                } else {
                    println!("DEBUG: No schema found for collection: {}", id.collection());
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

        #[cfg(feature = "vector")]
        if let Some(index) = &mut self.vector_index {
            if let Some(vectors) = value.get("vectors").and_then(|v| v.as_object()) {
                for (_channel, data) in vectors {
                    if let Some(vec_arr) = data.as_array() {
                        let vector: Option<Vec<f32>> = 
                            if vec_arr.first().is_some_and(|v| v.is_number()) { 
                                Some(
                                    vec_arr
                                        .iter()
                                        .filter_map(|v| v.as_f64().map(|f| f as f32))
                                        .collect(),
                                )
                            } else {
                                None
                            };
                        if let Some(v) = vector {
                            if !v.is_empty() {
                                if let Err(e) = index.insert(node_id, &v) {
                                    log::warn!("Failed to insert vector: {}", e);
                                }
                                break;
                            }
                        }
                    } else if let Some(data_arr) = data.get("data").and_then(|d| d.as_array()) {
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

        #[cfg(feature = "spatial")]
        if let Some(index) = &mut self.spatial_index {
            if let Some(geo) = value.get("geo").and_then(|g| g.as_object()) {
                for (_name, feature) in geo {
                    if let Some(lat) = feature.get("lat").and_then(|v| v.as_f64()) {
                        if let Some(lon) = feature.get("lon").and_then(|v| v.as_f64()) {
                            index.insert_point(node_id, lat, lon);
                            break;
                        }
                    } else if let Some(loc) = feature.get("loc").and_then(|l| l.as_object()) {
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
    #[cfg(feature = "fulltext")]
    pub fn search_text(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
        if let Some(index) = &self.fulltext {
            let results = index.search(query, limit)?;
            println!("DEBUG: Fulltext search returned {} raw results", results.len());
            let node_ids = results
                .into_iter()
                .filter_map(|res| {
                    println!("DEBUG: Mapping search result key: {}", res.key);
                    let slug_hash = hash_slug(&res.key);
                    if let Some(node) = self.storage.get_by_slug(slug_hash) {
                        Some(node.node_id)
                    } else if let Some(node) = self.ingestion.get_by_slug(slug_hash) {
                        Some(node.node_id)
                    } else {
                        println!("DEBUG: Search result key {} could not be found in storage (hash: {})", res.key, slug_hash);
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
    #[cfg(feature = "vector")]
    pub fn search_vector(
        &self,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(NodeId, f32)>, Box<dyn std::error::Error>> {
        if let Some(index) = &self.vector_index {
            if index.is_built() && !index.is_empty() {
                return index.search(query, k);
            }
        }
        log::info!("Vector index not available, falling back to brute-force search");
        let results = vectors::ops::brute_force_search(&self.storage, &self.blob_store, query, k)?;
        Ok(
            results
                .into_iter()
                .map(|r| (r.node_id, r.similarity))
                .collect(),
        )
    }

    /// Search spatial index (Radius)
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
        let from_str = value.get("_from").and_then(|v| v.as_str()).ok_or("Missing _from")?;
        let to_str = value.get("_to").and_then(|v| v.as_str()).ok_or("Missing _to")?;
        let _from = EntityId::parse(from_str)?;
        let _to = EntityId::parse(to_str)?;
        let _type = value.get("_type").and_then(|v| v.as_str()).unwrap_or("related").to_string();
        let weight = value.get("props").and_then(|p| p.get("weight")).or(value.get("weight")).and_then(|v| v.as_f64()).unwrap_or(1.0) as f32;
        let timestamp = value.get("props").and_then(|p| p.get("timestamp")).or(value.get("timestamp")).and_then(|v| v.as_u64()).unwrap_or_else(|| std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64);

        let mut payload = EdgePayload::new(&_type);
        if let Some(props_value) = value.get("props") && let serde_json::Value::Object(map) = props_value {
            for (k, v) in map {
                if k != "weight" && k != "timestamp" && k != "decay" {
                    payload = payload.with_prop(k, v.clone());
                }
            }
        }

        let edge = WeightedEdge::new_with_payload(_from.clone(), _to.clone(), weight, _type, 0, timestamp, None, Some(payload));
        let from_hash = hash_slug(_from.key());
        let from_id = self.storage.get_by_slug(from_hash).or_else(|| self.ingestion.get_by_slug(from_hash)).ok_or(format!("Source node not found: {}", _from))?.node_id;
        self.graph.add_edge(edge);
        Ok(from_id)
    }

    /// Read event data from database
    ///
    /// # Behavior
    /// * By default, reads from Tier 1 (staged) OR Tier 2 (validated) for realtime accuracy.
    pub fn read(&self, slug: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
        self.read_with_options(slug, ReadOptions { include_staged: true })
    }

    /// Read event data with options
    pub fn read_with_options(
        &self,
        slug: &str,
        opts: ReadOptions,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let slug_hash = hash_slug(slug);
        if opts.include_staged {
            if let Some(node) = self.ingestion.get_by_slug(slug_hash) {
                let payload_bytes = self.blob_store.read(node.payload_ptr)?;
                return Ok(Some(String::from_utf8(payload_bytes)?));
            }
        }
        if let Some(node) = self.storage.get_by_slug(slug_hash) {
            let payload_bytes = self.blob_store.read(node.payload_ptr)?;
            return Ok(Some(String::from_utf8(payload_bytes)?));
        }
        Ok(None)
    }

    /// Add edge between events
    pub fn add_edge(
        &mut self,
        source_slug: &str,
        target_slug: &str,
        weight: f32,
        edge_type: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let source_hash = hash_slug(source_slug);
        let target_hash = hash_slug(target_slug);

        let _from = if let Some(n) = self.ingestion.get_by_slug(source_hash) {
            n.entity_id.clone().unwrap_or_else(|| EntityId::new("nodes".to_string(), source_slug.to_string()))
        } else if let Some(n) = self.storage.get_by_slug(source_hash) {
            n.entity_id.clone().unwrap_or_else(|| EntityId::new("nodes".to_string(), source_slug.to_string()))
        } else {
            EntityId::new("nodes".to_string(), source_slug.to_string())
        };

        let _to = if let Some(n) = self.ingestion.get_by_slug(target_hash) {
            n.entity_id.clone().unwrap_or_else(|| EntityId::new("nodes".to_string(), target_slug.to_string()))
        } else if let Some(n) = self.storage.get_by_slug(target_hash) {
            n.entity_id.clone().unwrap_or_else(|| EntityId::new("nodes".to_string(), target_slug.to_string()))
        } else {
            EntityId::new("nodes".to_string(), target_slug.to_string())
        };

        let edge = WeightedEdge::new(_from, _to, weight, edge_type, 0, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64, None);
        self.graph.add_edge(edge);
        Ok(())
    }

    /// Traverse graph backward (RCA)
    pub fn traverse(
        &self,
        slug: &str,
        max_hops: usize,
        weight_threshold: f32,
        edge_type: Option<&str>,
    ) -> Result<TraversalResult, Box<dyn std::error::Error>> {
        let entity_id = EntityId::parse(slug).unwrap_or_else(|_| EntityId::new("nodes".to_string(), slug.to_string()));
        Ok(self.graph.backward_bfs(&entity_id, max_hops, weight_threshold, edge_type, None))
    }

    /// Traverse graph forward (Joins)
    pub fn traverse_forward(
        &self,
        slug: &str,
        max_hops: usize,
        weight_threshold: f32,
        edge_type: Option<&str>,
        time_window: Option<(u64, u64)>,
    ) -> Result<TraversalResult, Box<dyn std::error::Error>> {
        let entity_id = EntityId::parse(slug).unwrap_or_else(|_| EntityId::new("nodes".to_string(), slug.to_string()));
        Ok(self.graph.forward_bfs(&entity_id, max_hops, weight_threshold, edge_type, time_window))
    }

    /// Delete event and edges
    pub fn delete(&mut self, slug: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.delete_with_options(slug, DeleteOptions::default())
    }

    /// Delete event with options
    pub fn delete_with_options(&mut self, slug: &str, _opts: DeleteOptions) -> Result<(), Box<dyn std::error::Error>> {
        let slug_hash = hash_slug(slug);
        if let Some(node) = self.ingestion.get_by_slug(slug_hash) {
            self.ingestion.remove(node.node_id);
        }
        if let Some(node) = self.storage.get_by_slug(slug_hash) {
            let tombstone = node.clone().as_tombstone(Some("user_delete".to_string()));
            self.storage.upsert(tombstone);
        }
        Ok(())
    }

    /// Update existing event
    pub fn update(&mut self, slug: &str, data: &str) -> Result<NodeId, Box<dyn std::error::Error>> {
        self.write(slug, data)
    }

    /// Update edge weight
    pub fn update_edge(&mut self, source: &str, target: &str, weight: f32, edge_type: Option<String>) -> Result<bool, Box<dyn std::error::Error>> {
        let src_id = EntityId::parse(source).unwrap_or_else(|_| EntityId::new("nodes".to_string(), source.to_string()));
        let dst_id = EntityId::parse(target).unwrap_or_else(|_| EntityId::new("nodes".to_string(), target.to_string()));
        Ok(self.graph.update_edge_weight(&src_id, &dst_id, weight, edge_type.as_deref()))
    }

    /// Delete edge
    pub fn delete_edge(&self, source: &str, target: &str, edge_type: Option<String>) -> Result<bool, Box<dyn std::error::Error>> {
        let src_id = EntityId::parse(source).unwrap_or_else(|_| EntityId::new("nodes".to_string(), source.to_string()));
        let dst_id = EntityId::parse(target).unwrap_or_else(|_| EntityId::new("nodes".to_string(), target.to_string()));
        Ok(self.graph.remove_edge(&src_id, &dst_id, edge_type))
    }

    /// Manual flush
    pub fn flush(&mut self) -> Result<usize, Box<dyn std::error::Error>> {
        let nodes = self.ingestion.drain_all();
        let count = nodes.len();
        if count == 0 { return Ok(0); }
        for node in nodes {
            self.storage.upsert(node);
        }
        Ok(count)
    }

        /// Backup database

        pub fn backup(&self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {

            let mut nodes = Vec::new();

            for node in self.storage.iter() {

                let bytes = self.blob_store.read(node.payload_ptr)?;

                nodes.push(serde_json::from_slice::<serde_json::Value>(&bytes)?);

            }

    

            let edges: Vec<serde_json::Value> = self.graph.iter().into_iter().map(|e| {

                serde_json::json!({

                    "_from": e._from.to_string(),

                    "_to": e._to.to_string(),

                    "_type": e._type,

                    "props": { "weight": e.weight }

                })

            }).collect();

    

            let backup = serde_json::json!({

                "nodes": nodes,

                "edges": edges

            });

    

            std::fs::write(path, serde_json::to_string_pretty(&backup)?)?;

            Ok(())

        }

    

        /// Restore database

        pub fn restore(&mut self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {

            let content = std::fs::read_to_string(path)?;

            let backup: serde_json::Value = serde_json::from_str(&content)?;

    

            if let Some(nodes) = backup.get("nodes").and_then(|n| n.as_array()) {

                for node in nodes {

                    self.write_json(&serde_json::to_string(node)?)?;

                }

            }

    

            if let Some(edges) = backup.get("edges").and_then(|e| e.as_array()) {

                for edge in edges {

                    self.write_json(&serde_json::to_string(edge)?)?;

                }

            }

    

            Ok(())

        }

    

    /// Define collection
    pub fn define_collection(&mut self, json_data: &str) -> Result<(), Box<dyn std::error::Error>> {
        let value: serde_json::Value = serde_json::from_str(json_data)?;
        if let Some(obj) = value.as_object() {
            for (name, schema_val) in obj {
                let schema: types::CollectionSchema = serde_json::from_value(schema_val.clone())?;
                println!("DEBUG: Registering schema for collection: {}", name);
                self.collection_registry.register(name, schema);
            }
        } else {
            // Fallback for single schema without name (use name from schema if exists or default)
            let schema: types::CollectionSchema = serde_json::from_value(value)?;
            self.collection_registry.register("default", schema);
        }
        Ok(())
    }

    pub fn graph(&self) -> &graph::ConcurrentGraph { &self.graph }
    pub fn storage(&self) -> &storage::SingleStorage { &self.storage }
    pub fn ingestion(&self) -> &storage::IngestionBuffer { &self.ingestion }
    pub fn ingestion_mut(&mut self) -> &mut storage::IngestionBuffer { Arc::get_mut(&mut self.ingestion).unwrap() }
    pub fn blob_store(&self) -> &types::BlobStore { &self.blob_store }


    /// Get storage metrics
    pub fn promotion_metrics(&self) -> PromotionMetrics { self.promote_worker.get_metrics() }
}