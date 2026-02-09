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
pub struct SekejapDB {
    ingestion: Arc<storage::IngestionBuffer>, // Tier 1
    storage: Arc<storage::SingleStorage>,     // Tier 2
    graph: graph::ConcurrentGraph,            // Tier 3
    blob_store: types::BlobStore,
    collection_registry: types::CollectionRegistry,
    promote_worker: storage::PromoteWorker,
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
            collection_registry: types::CollectionRegistry::new(),
            promote_worker,
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
        (*slug_hash ^ *spatial_hash) as NodeId
    }

    /// Helper to get canonical slug hash
    fn get_slug_hash(&self, slug: &str) -> SlugHash {
        let entity_id = EntityId::parse(slug).unwrap_or_else(|_| EntityId::new("nodes".to_string(), slug.to_string()));
        hash_slug(&entity_id.to_string())
    }

    /// Write event data to database
    pub fn write(&mut self, slug: &str, data: &str) -> Result<NodeId, Box<dyn std::error::Error>> {
        self.write_with_options(slug, data, WriteOptions::default())
    }

    /// Write event data with options
    pub fn write_with_options(
        &mut self,
        slug: &str,
        data: &str,
        opts: WriteOptions,
    ) -> Result<NodeId, Box<dyn std::error::Error>> {
        let value: serde_json::Value = serde_json::from_str(data)?;
        let title = value["title"].as_str().unwrap_or("");
        let content = value["content"].as_str().unwrap_or("");
        let json_lat = value["coordinates"]["lat"].as_f64().unwrap_or(0.0);
        let json_lon = value["coordinates"]["lon"].as_f64().unwrap_or(0.0);

        let lat = if opts.latitude != 0.0 || opts.longitude != 0.0 { opts.latitude } else { json_lat };
        let lon = if opts.latitude != 0.0 || opts.longitude != 0.0 { opts.longitude } else { json_lon };

        let entity_id = EntityId::parse(slug).unwrap_or_else(|_| EntityId::new("nodes".to_string(), slug.to_string()));
        let slug_for_hash = entity_id.to_string();
        let slug_hash = hash_slug(&slug_for_hash);
        let spatial_hash = hash_spatial(lat, lon);

        let (node_id, rev_old) = if let Some(existing) = self.ingestion.get_by_slug(slug_hash) {
            (existing.node_id, existing.rev)
        } else if let Some(existing) = self.storage.get_by_slug(slug_hash) {
            (existing.node_id, existing.rev)
        } else {
            (Self::generate_node_id(&slug_hash, &spatial_hash), 0)
        };

        let mut payload = NodePayload::new(title);
        payload = payload.with_id(entity_id.clone());
        payload.content = Some(data.to_string());
        payload.metadata = Some(value.clone());
        payload.coordinates = Some(Coordinates { latitude: lat, longitude: lon });
        let payload_json = serde_json::to_string(&payload)?;
        let payload_ptr = self.blob_store.write(payload_json.as_bytes())?;

        #[cfg(feature = "vector")]

            let vector_ptr = if let Some(ref vector) = opts.vector {

                let vector_bytes = vectors::ops::vector_to_bytes(vector);

                Some(self.blob_store.write(&vector_bytes)?)

            } else { None };

            #[cfg(not(feature = "vector"))]

            let vector_ptr = None;

    

            let mut node = if let Some(vptr) = vector_ptr {

                NodeHeader::new_with_vector(node_id, slug_hash, spatial_hash, payload_ptr, vptr, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64)

            } else {

                NodeHeader::new(node_id, slug_hash, spatial_hash, payload_ptr, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64)

            };

            node.entity_id = Some(entity_id);

            node.rev = rev_old + 1;

            let node = if opts.deleted { node.as_tombstone(Some("user_delete".to_string())) } else { node };

    

            if opts.publish_now { self.storage.upsert(node); }

            else { self.ingestion.upsert(node); }

    

            // Indexing blocks (Vector/Spatial/Fulltext) are here...

    

            Ok(node_id)

        }

    

    pub fn write_many(&mut self, items: Vec<(String, String)>) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> { self.write_batch(items, false) }

    pub fn write_batch(&mut self, items: Vec<(String, String)>, publish_now: bool) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
        if items.is_empty() { return Ok(Vec::new()); }
        let mut node_ids = Vec::with_capacity(items.len());
        let mut headers = Vec::with_capacity(items.len());

        for (slug, data) in &items {
            let value: serde_json::Value = serde_json::from_str(data)?;
            let title = value["title"].as_str().unwrap_or("").to_string();
            let content = value["content"].as_str().unwrap_or("").to_string();
            let lat = value["coordinates"]["lat"].as_f64().unwrap_or(0.0);
            let lon = value["coordinates"]["lon"].as_f64().unwrap_or(0.0);
            
            let entity_id = EntityId::parse(slug).unwrap_or_else(|_| EntityId::new("nodes".to_string(), slug.to_string()));
            let slug_for_hash = entity_id.to_string();
            let slug_hash = hash_slug(&slug_for_hash);
            let spatial_hash = hash_spatial(lat, lon);
            let node_id = Self::generate_node_id(&slug_hash, &spatial_hash);

            let mut payload = crate::types::NodePayload::new(title.clone());
            payload = payload.with_id(entity_id.clone());
            payload.content = Some(data.clone());
            payload.metadata = Some(value.clone());
            payload.coordinates = Some(crate::types::Coordinates { latitude: lat, longitude: lon });
            let payload_json = serde_json::to_string(&payload)?;
            let payload_ptr = self.blob_store.write(payload_json.as_bytes())?;

            let mut node = NodeHeader::new(node_id, slug_hash, spatial_hash, payload_ptr, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64);
            node.entity_id = Some(entity_id);
            headers.push(node);
            node_ids.push(node_id);

            #[cfg(feature = "fulltext")]
            if let Some(index) = &mut self.fulltext {
                let index_content = if !content.is_empty() { &content } else { data };
                let _ = index.add_document(&title, index_content, &slug_for_hash, None);
            }
            #[cfg(feature = "spatial")]
            if lat != 0.0 || lon != 0.0 { if let Some(index) = &mut self.spatial_index { index.insert_point(node_id, lat, lon); } }
        }

        if publish_now { for node in headers { self.storage.upsert(node); } }
        else { for node in headers { self.ingestion.upsert(node); } }
        #[cfg(feature = "fulltext")]
        if let Some(index) = &mut self.fulltext { let _ = index.commit(); }
        Ok(node_ids)
    }

    pub fn write_json(&mut self, json_data: &str) -> Result<NodeId, Box<dyn std::error::Error>> {
        let value: serde_json::Value = serde_json::from_str(json_data)?;
        if value.get("_from").is_some() || value.get("_to").is_some() { self.write_edge_json(&value) }
        else { self.write_node_json(&value) }
    }

    fn write_node_json(&mut self, value: &serde_json::Value) -> Result<NodeId, Box<dyn std::error::Error>> {
        let entity_id = value.get("_id").and_then(|v| v.as_str()).map(EntityId::parse).transpose()?.or_else(|| {
            let collection = value.get("_collection").and_then(|v| v.as_str())?;
            let key = value.get("_key").or(value.get("slug"))?.as_str()?;
            Some(EntityId::new(collection.to_string(), key.to_string()))
        });
        let title = value.get("title").and_then(|v| v.as_str()).unwrap_or("Untitled");
        let mut payload = NodePayload::new(title);
        if let Some(ref id) = entity_id { payload = payload.with_id(id.clone()); }
        if let Some(excerpt) = value.get("excerpt").and_then(|v| v.as_str()) { payload = payload.with_excerpt(excerpt); }
        if let Some(content) = value.get("content").and_then(|v| v.as_str()) { payload = payload.with_content(content); }
        if let Some(props_value) = value.get("props").or(value.get("metadata")) && let serde_json::Value::Object(map) = props_value {
            for (k, v) in map { payload = payload.with_prop(k, v.clone()); }
        }
        let payload_json = serde_json::to_string(&payload)?;
        let payload_ptr = self.blob_store.write(payload_json.as_bytes())?;
        let slug_for_hash = entity_id.as_ref().map(|id| id.to_string()).or(value.get("slug").and_then(|v| v.as_str()).map(|s| s.to_string())).unwrap_or(title.to_string());
        let slug_hash = hash_slug(&slug_for_hash);
        let lat = value.get("coordinates").and_then(|c| c.get("lat").and_then(|v| v.as_f64())).or(value.get("latitude").and_then(|v| v.as_f64())).unwrap_or(0.0);
        let lon = value.get("coordinates").and_then(|c| c.get("lon").and_then(|v| v.as_f64())).or(value.get("longitude").and_then(|v| v.as_f64())).unwrap_or(0.0);
        let spatial_hash = hash_spatial(lat, lon);
        let node_id = Self::generate_node_id(&slug_hash, &spatial_hash);
        let mut header = NodeHeader::new(node_id, slug_hash, spatial_hash, payload_ptr, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64);
        header.entity_id = entity_id.clone();
        self.storage.upsert(header.clone());

        // #[cfg(feature = "fulltext")]
        // if let Some(index) = &mut self.fulltext {
        //     let content = value.get("content").and_then(|v| v.as_str()).unwrap_or("");
        //     let mut attributes = serde_json::Map::new();
        //     let insert_at_path = |map: &mut serde_json::Map<String, serde_json::Value>, path: &str, val: serde_json::Value| {
        //         let parts: Vec<&str> = path.split('.').collect();
        //         let mut curr = map;
        //         for (i, p) in parts.iter().enumerate() {
        //             if i == parts.len() - 1 { curr.insert(p.to_string(), val); break; }
        //             else {
        //                 let entry = curr.entry(p.to_string()).or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
        //                 if let serde_json::Value::Object(m) = entry { curr = m; } else { break; }
        //             }
        //         }
        //     };
        //     if let Some(id) = &entity_id {
        //         if let Some(schema) = self.collection_registry.get(id.collection()) {
        //             for field in &schema.fulltext {
        //                 if field == "title" || field == "content" || field == "slug" { continue; }
        //                 let mut curr = value;
        //                 let mut found = true;
        //                 for p in field.split('.') { if let Some(n) = curr.get(p) { curr = n; } else { found = false; break; } }
        //                 if found { insert_at_path(&mut attributes, field, curr.clone()); }
        //             }
        //         }
        //     }
        //     let attr = if attributes.is_empty() { None } else { Some(serde_json::Value::Object(attributes)) };
        //     let _ = index.add_document(title, content, &slug_for_hash, attr);
        //     let _ = index.commit();
        // }

        Ok(node_id)
    }

    #[cfg(feature = "fulltext")]
    pub fn search_text(&self, query: &str, limit: usize) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
        if let Some(index) = &self.fulltext {
            let results = index.search(query, limit)?;
            let ids = results.into_iter().filter_map(|res| {
                let h = hash_slug(&res.key);
                self.storage.get_by_slug(h).or_else(|| self.ingestion.get_by_slug(h)).map(|n| n.node_id)
            }).collect();
            Ok(ids)
        } else { Err("Fulltext disabled".into()) }
    }

    #[cfg(feature = "vector")]
    pub fn search_vector(&self, query: &[f32], k: usize) -> Result<Vec<(NodeId, f32)>, Box<dyn std::error::Error>> {
        if let Some(index) = &self.vector_index { if index.is_built() && !index.is_empty() { return index.search(query, k); } }
        let results = vectors::ops::brute_force_search(&*self.storage, &self.blob_store, query, k)?;
        Ok(results.into_iter().map(|r| (r.node_id, r.similarity)).collect())
    }

    #[cfg(feature = "spatial")]
    pub fn search_spatial(&self, lat: f64, lon: f64, radius_km: f64) -> Result<Vec<NodeId>, Box<dyn std::error::Error>> {
        if let Some(index) = &self.spatial_index { Ok(index.find_within_radius(lat, lon, radius_km)) }
        else { Err("Spatial disabled".into()) }
    }

    fn write_edge_json(&mut self, value: &serde_json::Value) -> Result<NodeId, Box<dyn std::error::Error>> {
        let from_str = value.get("_from").and_then(|v| v.as_str()).ok_or("Missing _from")?;
        let to_str = value.get("_to").and_then(|v| v.as_str()).ok_or("Missing _to")?;
        let _from = EntityId::parse(from_str)?;
        let _to = EntityId::parse(to_str)?;
        let _type = value.get("_type").and_then(|v| v.as_str()).unwrap_or("related").to_string();
        let weight = value.get("props").and_then(|p| p.get("weight")).or(value.get("weight")).and_then(|v| v.as_f64()).unwrap_or(1.0) as f32;
        let edge = WeightedEdge::new(_from.clone(), _to.clone(), weight, _type, 0, 0, None);
        let from_hash = hash_slug(&_from.to_string());
        let from_id = self.storage.get_by_slug(from_hash).or_else(|| self.ingestion.get_by_slug(from_hash)).ok_or(format!("Source node not found: {}", _from))?.node_id;
        self.graph.add_edge(edge);
        Ok(from_id)
    }

    pub fn read(&self, slug: &str) -> Result<Option<String>, Box<dyn std::error::Error>> { self.read_with_options(slug, ReadOptions { include_staged: true }) }

    pub fn read_with_options(&self, slug: &str, opts: ReadOptions) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let slug_hash = self.get_slug_hash(slug);
        if opts.include_staged { if let Some(node) = self.ingestion.get_by_slug(slug_hash) { let bytes = self.blob_store.read(node.payload_ptr)?; return Ok(Some(String::from_utf8(bytes)?)); } }
        if let Some(node) = self.storage.get_by_slug(slug_hash) { let bytes = self.blob_store.read(node.payload_ptr)?; return Ok(Some(String::from_utf8(bytes)?)); }
        Ok(None)
    }

    pub fn add_edge(&mut self, source_slug: &str, target_slug: &str, weight: f32, edge_type: String) -> Result<(), Box<dyn std::error::Error>> {
        let source_hash = self.get_slug_hash(source_slug);
        let target_hash = self.get_slug_hash(target_slug);
        
        let _from = self.storage.get_by_slug(source_hash).or_else(|| self.ingestion.get_by_slug(source_hash)).map(|n| n.entity_id.clone().unwrap()).unwrap_or_else(|| EntityId::new("nodes".to_string(), source_slug.to_string()));
        let _to = self.storage.get_by_slug(target_hash).or_else(|| self.ingestion.get_by_slug(target_hash)).map(|n| n.entity_id.clone().unwrap()).unwrap_or_else(|| EntityId::new("nodes".to_string(), target_slug.to_string()));
        let edge = WeightedEdge::new(_from, _to, weight, edge_type, 0, 0, None);
        self.graph.add_edge(edge);
        Ok(())
    }

    pub fn traverse(&self, slug: &str, max_hops: usize, weight_threshold: f32, edge_type: Option<&str>) -> Result<TraversalResult, Box<dyn std::error::Error>> {
        let entity_id = EntityId::parse(slug).unwrap_or_else(|_| EntityId::new("nodes".to_string(), slug.to_string()));
        Ok(self.graph.backward_bfs(&entity_id, max_hops, weight_threshold, edge_type, None))
    }

    pub fn traverse_forward(&self, slug: &str, max_hops: usize, weight_threshold: f32, edge_type: Option<&str>, time_window: Option<(u64, u64)>) -> Result<TraversalResult, Box<dyn std::error::Error>> {
        let entity_id = EntityId::parse(slug).unwrap_or_else(|_| EntityId::new("nodes".to_string(), slug.to_string()));
        Ok(self.graph.forward_bfs(&entity_id, max_hops, weight_threshold, edge_type, time_window))
    }

    pub fn delete(&mut self, slug: &str) -> Result<(), Box<dyn std::error::Error>> {
        let slug_hash = self.get_slug_hash(slug);
        if let Some(node) = self.ingestion.get_by_slug(slug_hash) { self.ingestion.remove(node.node_id); }
        if let Some(node) = self.storage.get_by_slug(slug_hash) { self.storage.upsert(node.as_tombstone(Some("user_delete".to_string()))); }
        Ok(())
    }

    pub fn delete_with_options(&mut self, slug: &str, _opts: DeleteOptions) -> Result<(), Box<dyn std::error::Error>> { self.delete(slug) }
    pub fn update(&mut self, slug: &str, data: &str) -> Result<NodeId, Box<dyn std::error::Error>> { self.write(slug, data) }
    pub fn update_edge(&mut self, source: &str, target: &str, weight: f32, edge_type: Option<String>) -> Result<bool, Box<dyn std::error::Error>> {
        let src_id = EntityId::parse(source).unwrap_or_else(|_| EntityId::new("nodes".to_string(), source.to_string()));
        let dst_id = EntityId::parse(target).unwrap_or_else(|_| EntityId::new("nodes".to_string(), target.to_string()));
        Ok(self.graph.update_edge_weight(&src_id, &dst_id, weight, edge_type.as_deref()))
    }
    pub fn delete_edge(&self, source: &str, target: &str, edge_type: Option<String>) -> Result<bool, Box<dyn std::error::Error>> {
        let src_id = EntityId::parse(source).unwrap_or_else(|_| EntityId::new("nodes".to_string(), source.to_string()));
        let dst_id = EntityId::parse(target).unwrap_or_else(|_| EntityId::new("nodes".to_string(), target.to_string()));
        Ok(self.graph.remove_edge(&src_id, &dst_id, edge_type))
    }

    /// Manual flush
    pub fn flush(&mut self) -> Result<usize, Box<dyn std::error::Error>> {
        let nodes = self.ingestion.drain_all();
        let count = nodes.len();
        if count > 0 { self.storage.upsert_batch(&nodes)?; }
        
        // Ensure blobs are also synced to disk during explicit flush
        self.blob_store.sync()?;
        
        Ok(count)
    }

    pub fn backup(&self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let mut nodes = Vec::new();
        for node in self.storage.iter() { let bytes = self.blob_store.read(node.payload_ptr)?; nodes.push(serde_json::from_slice::<serde_json::Value>(&bytes)?); }
        let edges: Vec<_> = self.graph.iter().into_iter().map(|e| serde_json::json!({ "_from": e._from.to_string(), "_to": e._to.to_string(), "_type": e._type, "props": { "weight": e.weight } })).collect();
        let backup = serde_json::json!({ "nodes": nodes, "edges": edges });
        std::fs::write(path, serde_json::to_string_pretty(&backup)?)?;
        Ok(())
    }

    pub fn restore(&mut self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let backup: serde_json::Value = serde_json::from_str(&content)?;
        if let Some(nodes) = backup.get("nodes").and_then(|n| n.as_array()) { for node in nodes { self.write_json(&serde_json::to_string(node)?)?; } }
        if let Some(edges) = backup.get("edges").and_then(|e| e.as_array()) { for edge in edges { self.write_json(&serde_json::to_string(edge)?)?; } }
        Ok(())
    }

    pub fn define_collection(&mut self, json_data: &str) -> Result<(), Box<dyn std::error::Error>> {
        let value: serde_json::Value = serde_json::from_str(json_data)?;
        if let Some(obj) = value.as_object() {
            for (name, schema_val) in obj {
                let schema: types::CollectionSchema = serde_json::from_value(schema_val.clone())?;
                self.collection_registry.register(name, schema);
            }
        } else {
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
    pub fn promotion_metrics(&self) -> PromotionMetrics { self.promote_worker.get_metrics() }
}
