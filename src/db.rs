use crate::arena::{BlobArena, DurableArena};
use crate::collection_bitmap::CollectionBitmapIndex;
#[cfg(feature = "fulltext")]
use crate::fulltext::FullTextAdapter;
use crate::hnsw::{ArenaVectorStore, CosineDistance, HyperHNSW};
use crate::index::{HashIndex, PropertyIndex, RangeIndex};
use crate::mmap_hash::MmapHashIndex;
use crate::sekejapql::QueryCompiler;
use crate::set::Set;
use crate::stores::{EdgeStore, NodeStore, SchemaStore};
use crate::types::{
    AggOp, CollectionSchema, EdgeSlot, Hit, NodeSlot, Outcome, SpatialNode, Step, VectorSlot,
};
use dashmap::DashMap;
use rstar::RTree;
use serde_json::Value;
use smallvec::SmallVec;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct SekejapDB {
    /// Capacity passed to new() — stored so init_hnsw() can size the vector arena.
    node_capacity: usize,
    pub nodes: DurableArena<NodeSlot>,
    pub edges: DurableArena<EdgeSlot>,
    /// Lazy — allocated only on init_hnsw(). Zero bytes on disk until vectors are enabled.
    pub vectors: parking_lot::RwLock<DurableArena<VectorSlot>>,
    pub blobs: BlobArena,
    /// Slug hash → arena index. Backed by mmap Robin Hood hash table (~0 MB RAM at 50M).
    pub slug_index: Arc<parking_lot::RwLock<MmapHashIndex>>,
    pub adj_fwd: DashMap<u32, SmallVec<[u32; 8]>>,
    pub adj_rev: DashMap<u32, SmallVec<[u32; 8]>>,
    pub collections: DashMap<u64, CollectionSchema>,
    pub collection_counts: DashMap<u64, AtomicUsize>,
    /// Per-collection RoaringBitmaps — O(1) collection queries
    pub collection_bitmaps: Arc<CollectionBitmapIndex>,
    pub cached_timestamp: AtomicU64,
    pub spatial: Arc<parking_lot::RwLock<RTree<SpatialNode>>>,
    pub hnsw: parking_lot::RwLock<Option<HyperHNSW<CosineDistance>>>,
    /// field_name → HashIndex (equality lookups, O(1))
    pub field_hash_indexes: DashMap<String, Arc<HashIndex>>,
    /// field_name → RangeIndex (range queries, O(log N))
    pub field_range_indexes: DashMap<String, Arc<RangeIndex>>,
    #[cfg(feature = "fulltext")]
    pub fulltext: parking_lot::RwLock<Option<Box<dyn FullTextAdapter>>>,
    /// base path (kept for bitmap flush)
    base_path: std::path::PathBuf,
}

impl SekejapDB {
    pub fn new(base_path: &Path, count: usize) -> std::io::Result<Self> {
        std::fs::create_dir_all(base_path)?;
        let slug_cap = (count as u64).max(1024);
        // Blob size: ~200 bytes/node estimate, minimum 128 MB
        let blob_mb = ((count as u64 * 200) / (1024 * 1024)).max(128) as usize;
        let db = Self {
            node_capacity: count,
            nodes: DurableArena::new(&base_path.join("nodes.mmap"), count)?,
            edges: DurableArena::new(&base_path.join("edges.mmap"), count * 3)?,
            // Vector arena starts empty — expanded only when init_hnsw() is called.
            // This avoids pre-allocating N×512B (25 GB at 50M) for non-vector workloads.
            vectors: parking_lot::RwLock::new(DurableArena::new(
                &base_path.join("vectors.mmap"),
                0,
            )?),
            blobs: BlobArena::new(&base_path.join("blobs.mmap"), blob_mb)?,
            slug_index: Arc::new(parking_lot::RwLock::new(MmapHashIndex::new(
                &base_path.join("slug_index.mhash"),
                slug_cap,
            )?)),
            adj_fwd: DashMap::with_capacity(count),
            adj_rev: DashMap::with_capacity(count),
            collections: DashMap::new(),
            collection_counts: DashMap::new(),
            collection_bitmaps: Arc::new(CollectionBitmapIndex::new(base_path)?),
            cached_timestamp: AtomicU64::new(Self::now_raw()),
            spatial: Arc::new(parking_lot::RwLock::new(RTree::new())),
            hnsw: parking_lot::RwLock::new(None),
            field_hash_indexes: DashMap::new(),
            field_range_indexes: DashMap::new(),
            #[cfg(feature = "fulltext")]
            fulltext: parking_lot::RwLock::new(None),
            base_path: base_path.to_path_buf(),
        };
        if db.nodes.write_head.load(Ordering::Acquire) > 0 {
            db.rebuild_indexes();
        }
        Ok(db)
    }

    /// Rebuild slug_index, adj_fwd, adj_rev, spatial, collection_bitmaps from mmap.
    /// HNSW is NOT rebuilt automatically — call init_hnsw() + nodes().build_hnsw() separately.
    fn rebuild_indexes(&self) {
        let node_count = self.nodes.write_head.load(Ordering::Acquire);
        let mut spatial_nodes = Vec::new();
        let mut bm_pairs: Vec<(u64, u32)> = Vec::new();

        {
            let mut slug_w = self.slug_index.write();
            for i in 0..node_count {
                let slot = self.nodes.read_at(i);
                if slot.flags == 0 {
                    continue;
                }
                slug_w.insert(slot.slug_hash, i as u32);
                self.collection_counts
                    .entry(slot.collection_hash)
                    .or_insert_with(|| AtomicUsize::new(0))
                    .fetch_add(1, Ordering::Relaxed);
                bm_pairs.push((slot.collection_hash, i as u32));
                if slot.lat != 0.0 || slot.lon != 0.0 {
                    let bytes = self.blobs.read(slot.blob_offset, slot.blob_len);
                    let sn = if let Ok(val) = serde_json::from_slice::<Value>(bytes) {
                        Self::extract_spatial_node(i as u32, &val)
                            .unwrap_or_else(|| SpatialNode::from_point(i as u32, slot.lat, slot.lon))
                    } else {
                        SpatialNode::from_point(i as u32, slot.lat, slot.lon)
                    };
                    spatial_nodes.push(sn);
                }
            }
        }

        self.collection_bitmaps
            .rebuild_from_iter(bm_pairs.into_iter());

        if !spatial_nodes.is_empty() {
            *self.spatial.write() = RTree::bulk_load(spatial_nodes);
        }

        let edge_count = self.edges.write_head.load(Ordering::Acquire);
        for i in 0..edge_count {
            let edge = self.edges.read_at(i);
            if edge.flags == 0 {
                continue;
            }
            self.adj_fwd
                .entry(edge.from_node)
                .or_default()
                .push(i as u32);
            self.adj_rev.entry(edge.to_node).or_default().push(i as u32);
        }

        // Rebuild field indexes from schema (if any were defined before restart)
        // This is a best-effort scan — only runs if schemas were pre-loaded.
        self.rebuild_field_indexes();
    }

    /// Scan mmap and populate any defined field_hash_indexes / field_range_indexes.
    pub(crate) fn rebuild_field_indexes(&self) {
        if self.field_hash_indexes.is_empty() && self.field_range_indexes.is_empty() {
            return;
        }
        let node_count = self.nodes.write_head.load(Ordering::Acquire);
        for i in 0..node_count {
            let slot = self.nodes.read_at(i);
            if slot.flags == 0 {
                continue;
            }
            let bytes = self.blobs.read(slot.blob_offset, slot.blob_len);
            let Ok(json) = serde_json::from_slice::<Value>(bytes) else {
                continue;
            };

            for entry in self.field_hash_indexes.iter() {
                if let Some(v) = json.get(entry.key().as_str()) {
                    entry.value().insert(i as u32, v);
                }
            }
            for entry in self.field_range_indexes.iter() {
                if let Some(v) = json.get(entry.key().as_str()).and_then(|v| v.as_f64()) {
                    entry.value().insert_f64(i as u32, v);
                }
            }
        }
    }

    /// Resolve arena index → original slug string (read from blob _id field).
    pub fn slug_from_idx(&self, idx: u32) -> Option<String> {
        let slot = self.nodes.read_at(idx as u64);
        if slot.flags == 0 {
            return None;
        }
        let bytes = self.blobs.read(slot.blob_offset, slot.blob_len);
        let json: Value = serde_json::from_slice(bytes).ok()?;
        json.get("_id")?.as_str().map(|s| s.to_string())
    }

    pub fn init_hnsw(&self, m: usize) {
        // Expand the vector arena to full node capacity (lazy allocation).
        // First call allocates node_capacity × 512 B on disk; subsequent calls are no-ops.
        self.vectors
            .write()
            .resize(self.node_capacity)
            .expect("failed to resize vector arena");
        let store = ArenaVectorStore::new(&*self.vectors.read(), 128);
        let hnsw = HyperHNSW::new(store, m);
        *self.hnsw.write() = Some(hnsw);
    }

    #[cfg(feature = "fulltext")]
    pub fn init_fulltext(&self, base_path: &Path) {
        let ft_path = base_path.join("fulltext_index");
        let fulltext = crate::fulltext::create_default_adapter(&ft_path).ok();
        *self.fulltext.write() = fulltext;
    }

    // --- Resource Accessors (Public API) ---
    pub fn nodes(&self) -> NodeStore<'_> {
        NodeStore::new(self)
    }
    pub fn edges(&self) -> EdgeStore<'_> {
        EdgeStore::new(self)
    }
    pub fn schema(&self) -> SchemaStore<'_> {
        SchemaStore::new(self)
    }

    pub fn parse_entity_id(slug: &str) -> (u64, u64) {
        let full_hash = seahash::hash(slug.as_bytes());
        let collection_hash = if let Some(slash_pos) = slug.find('/') {
            seahash::hash(&slug.as_bytes()[..slash_pos])
        } else {
            seahash::hash(b"nodes")
        };
        (collection_hash, full_hash)
    }

    fn now_raw() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    pub fn update_timestamp(&self) {
        self.cached_timestamp
            .store(Self::now_raw(), Ordering::Relaxed);
    }

    // --- Internal Methods (called by stores) ---

    /// Write with explicit slug (USES PROVIDED SLUG)
    pub(crate) fn write_internal(
        &self,
        slug: &str,
        data: &str,
    ) -> Result<u32, Box<dyn std::error::Error>> {
        let value: Value = serde_json::from_str(data)?;
        self.write_with_value(slug, data, &value)
    }

    /// Write with auto-detected slug from JSON
    pub(crate) fn write_json_internal(
        &self,
        json_data: &str,
    ) -> Result<u32, Box<dyn std::error::Error>> {
        let value: Value = serde_json::from_str(json_data)?;

        if value.get("_from").is_some() && value.get("_to").is_some() {
            let from = value["_from"].as_str().ok_or("Missing _from")?;
            let to = value["_to"].as_str().ok_or("Missing _to")?;
            let edge_type = value["_type"].as_str().unwrap_or("related");
            let weight = value
                .get("weight")
                .or_else(|| value.get("props").and_then(|p| p.get("weight")))
                .and_then(|v| v.as_f64())
                .unwrap_or(1.0) as f32;
            self.add_edge_internal(from, to, weight, edge_type)?;
            Ok(0)
        } else {
            let slug = value
                .get("_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or_else(|| {
                    let c = value.get("_collection")?.as_str()?;
                    let k = value.get("_key").or(value.get("slug"))?.as_str()?;
                    Some(format!("{}/{}", c, k))
                })
                .unwrap_or_else(|| "nodes/untitled".to_string());
            self.write_with_value(&slug, json_data, &value)
        }
    }

    /// Batch write
    pub(crate) fn write_batch_internal(
        &self,
        items: &[(&str, &str)],
    ) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
        let mut indices = Vec::with_capacity(items.len());
        for &(slug, data) in items {
            let idx = self.write_internal(slug, data)?;
            indices.push(idx);
        }
        Ok(indices)
    }

    /// Internal write with parsed value (single-item path — indexes inline)
    fn write_with_value(
        &self,
        slug: &str,
        _raw: &str,
        value: &Value,
    ) -> Result<u32, Box<dyn std::error::Error>> {
        let (collection_hash, slug_hash) = Self::parse_entity_id(slug);
        let (lat, lon) = Self::extract_coords(value);

        // Inject slug as _id if not present
        let mut final_value = value.clone();
        if final_value.get("_id").is_none() {
            if let Some(obj) = final_value.as_object_mut() {
                obj.insert("_id".to_string(), Value::String(slug.to_string()));
            }
        }
        let final_raw = serde_json::to_string(&final_value)?;

        let (b_off, b_len) = self.blobs.append(final_raw.as_bytes());
        let existing_idx = { self.slug_index.read().get(slug_hash) };
        let is_new = existing_idx.is_none();
        let n_idx = existing_idx
            .map(|idx| idx as u64)
            .unwrap_or_else(|| self.nodes.write_head.fetch_add(1, Ordering::Relaxed));

        if !is_new {
            let old_slot = self.nodes.read_at(n_idx);
            if old_slot.flags != 0 {
                if old_slot.lat != 0.0 || old_slot.lon != 0.0 {
                    let old_bytes = self.blobs.read(old_slot.blob_offset, old_slot.blob_len);
                    let old_sn = if let Ok(old_val) = serde_json::from_slice::<Value>(old_bytes) {
                        Self::extract_spatial_node(n_idx as u32, &old_val)
                            .unwrap_or_else(|| SpatialNode::from_point(n_idx as u32, old_slot.lat, old_slot.lon))
                    } else {
                        SpatialNode::from_point(n_idx as u32, old_slot.lat, old_slot.lon)
                    };
                    self.spatial.write().remove(&old_sn);
                }
                for entry in self.field_hash_indexes.iter() {
                    entry.value().remove(n_idx as u32);
                }
                for entry in self.field_range_indexes.iter() {
                    entry.value().remove(n_idx as u32);
                }
                if old_slot.collection_hash != collection_hash {
                    self.collection_bitmaps
                        .remove(old_slot.collection_hash, n_idx as u32);
                    if let Some(count) = self.collection_counts.get(&old_slot.collection_hash) {
                        count.fetch_sub(1, Ordering::Relaxed);
                    }
                    self.collection_bitmaps
                        .insert(collection_hash, n_idx as u32);
                    self.collection_counts
                        .entry(collection_hash)
                        .or_insert_with(|| AtomicUsize::new(0))
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        } else {
            self.collection_bitmaps
                .insert(collection_hash, n_idx as u32);
            self.collection_counts
                .entry(collection_hash)
                .or_insert_with(|| AtomicUsize::new(0))
                .fetch_add(1, Ordering::Relaxed);
        }

        let vec_present = Self::write_vector_if_present(&self.vectors, n_idx, value);

        let slot = NodeSlot {
            crc32: crc32fast::hash(final_raw.as_bytes()),
            slug_hash,
            collection_hash,
            flags: 1,
            lat,
            lon,
            blob_offset: b_off,
            blob_len: b_len,
            vec_slot: if vec_present { n_idx as u32 } else { u32::MAX },
            ..Default::default()
        };
        self.nodes.write_at(n_idx, &slot);

        if vec_present {
            if let Some(ref hnsw) = *self.hnsw.read() {
                hnsw.insert_index(n_idx as u32, 32)
                    .map_err(|_e: String| "HNSW insert failed".to_string())?;
            }
        }

        #[cfg(feature = "fulltext")]
        if let Some(ref ft) = *self.fulltext.read() {
            let title = value.get("title").and_then(|v| v.as_str()).unwrap_or("");
            let content = value
                .get("content")
                .and_then(|v| v.as_str())
                .or_else(|| value.get("body").and_then(|v| v.as_str()))
                .unwrap_or("");
            if !title.is_empty() || !content.is_empty() {
                let _ = ft.add_document(title, content, slug_hash);
            }
        }

        // Update slug index (exclusive write)
        self.slug_index.write().insert(slug_hash, n_idx as u32);

        if let Some(sn) = Self::extract_spatial_node(n_idx as u32, value) {
            self.spatial.write().insert(sn);
        }

        // Index hot fields
        for entry in self.field_hash_indexes.iter() {
            if let Some(v) = value.get(entry.key().as_str()) {
                entry.value().insert(n_idx as u32, v);
            }
        }
        for entry in self.field_range_indexes.iter() {
            if let Some(v) = value.get(entry.key().as_str()).and_then(|v| v.as_f64()) {
                entry.value().insert_f64(n_idx as u32, v);
            }
        }

        let committed = self.nodes.write_head.load(Ordering::Acquire).max(n_idx + 1);
        self.nodes.commit(committed);
        self.blobs.commit();
        Ok(n_idx as u32)
    }

    // =========================================================================
    // BATCH INGESTION — Deferred index building for 100x+ faster writes
    // =========================================================================

    /// Write node to arena + slug_index. NO spatial, NO HNSW, NO per-item commit.
    /// Returns (arena_index, has_vector, lat, lon).
    fn write_node_deferred(
        &self,
        slug: &str,
        raw: &str,
        value: &Value,
    ) -> Result<(u32, bool, f32, f32), Box<dyn std::error::Error>> {
        let (collection_hash, slug_hash) = Self::parse_entity_id(slug);
        let (lat, lon) = Self::extract_coords(value);
        let (b_off, b_len) = self.blobs.append(raw.as_bytes());
        let n_idx = self.nodes.write_head.fetch_add(1, Ordering::Relaxed);
        let vec_present = Self::write_vector_if_present(&self.vectors, n_idx, value);

        let slot = NodeSlot {
            crc32: crc32fast::hash(raw.as_bytes()),
            slug_hash,
            collection_hash,
            flags: 1,
            lat,
            lon,
            blob_offset: b_off,
            blob_len: b_len,
            vec_slot: if vec_present { n_idx as u32 } else { u32::MAX },
            ..Default::default()
        };
        self.nodes.write_at(n_idx, &slot);
        self.slug_index.write().insert(slug_hash, n_idx as u32);
        self.collection_bitmaps
            .insert(collection_hash, n_idx as u32);
        self.collection_counts
            .entry(collection_hash)
            .or_insert_with(|| AtomicUsize::new(0))
            .fetch_add(1, Ordering::Relaxed);

        // Index hot fields (same as single-item path)
        for entry in self.field_hash_indexes.iter() {
            if let Some(v) = value.get(entry.key().as_str()) {
                entry.value().insert(n_idx as u32, v);
            }
        }
        for entry in self.field_range_indexes.iter() {
            if let Some(v) = value.get(entry.key().as_str()).and_then(|v| v.as_f64()) {
                entry.value().insert_f64(n_idx as u32, v);
            }
        }

        // Fulltext indexing (same as single-item path)
        #[cfg(feature = "fulltext")]
        if let Some(ref ft) = *self.fulltext.read() {
            let title = value.get("title").and_then(|v| v.as_str()).unwrap_or("");
            let content = value
                .get("content")
                .and_then(|v| v.as_str())
                .or_else(|| value.get("body").and_then(|v| v.as_str()))
                .unwrap_or("");
            if !title.is_empty() || !content.is_empty() {
                let _ = ft.add_document(title, content, slug_hash);
            }
        }

        Ok((n_idx as u32, vec_present, lat, lon))
    }

    /// Add edge without per-item commit.
    fn add_edge_deferred(
        &self,
        source_slug: &str,
        target_slug: &str,
        weight: f32,
        edge_type: &str,
    ) -> Result<(), String> {
        let (_, src_hash) = Self::parse_entity_id(source_slug);
        let (_, dst_hash) = Self::parse_entity_id(target_slug);
        let type_hash = seahash::hash(edge_type.as_bytes());
        let src_idx = self
            .slug_index
            .read()
            .get(src_hash)
            .ok_or_else(|| format!("Source not found: {}", source_slug))?;
        let dst_idx = self
            .slug_index
            .read()
            .get(dst_hash)
            .ok_or_else(|| format!("Target not found: {}", target_slug))?;

        let e_idx = self.edges.write_head.fetch_add(1, Ordering::Relaxed);
        let edge = EdgeSlot {
            from_node: src_idx,
            to_node: dst_idx,
            weight,
            edge_type_hash: type_hash,
            timestamp: self.cached_timestamp.load(Ordering::Relaxed),
            flags: 1,
            ..Default::default()
        };
        self.edges.write_at(e_idx, &edge);
        self.adj_fwd.entry(src_idx).or_default().push(e_idx as u32);
        self.adj_rev.entry(dst_idx).or_default().push(e_idx as u32);
        Ok(())
    }

    /// Batch ingest nodes: deferred arena writes → bulk spatial → single commit.
    pub(crate) fn ingest_nodes_batch(
        &self,
        items: &[(&str, &str)],
    ) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
        let (indices, _) = self.ingest_nodes_raw(items)?;
        self.build_hnsw_batch()?;
        Ok(indices)
    }

    /// Phase 1+2+3: Raw data ingestion (arena + spatial). NO HNSW.
    pub(crate) fn ingest_nodes_raw(
        &self,
        items: &[(&str, &str)],
    ) -> Result<(Vec<u32>, Vec<u32>), Box<dyn std::error::Error>> {
        // Sequential batch insert (slug_index needs exclusive write lock per insert;
        // DashMap is gone — serialised writes are acceptable for batch path).
        // Cache parsed JSON Values to avoid re-parsing from blob for spatial extraction.
        let mut node_meta: Vec<(u32, bool, f32, f32, Value)> = Vec::with_capacity(items.len());
        for &(slug, raw) in items {
            let value: Value = serde_json::from_str(raw)?;
            let meta = self.write_node_deferred(slug, raw, &value)?;
            node_meta.push((meta.0, meta.1, meta.2, meta.3, value));
        }

        // Single commit for all arena writes
        let node_count = self.nodes.write_head.load(Ordering::Acquire);
        self.nodes.commit(node_count);
        self.blobs.commit();

        // Bulk-load spatial R-Tree — use cached Values (no re-parse from blob)
        let mut spatial_nodes: Vec<SpatialNode> = node_meta
            .iter()
            .filter(|(_, _, lat, lon, _)| *lat != 0.0 || *lon != 0.0)
            .map(|(idx, _, lat, lon, val)| {
                Self::extract_spatial_node(*idx, val)
                    .unwrap_or_else(|| SpatialNode::from_point(*idx, *lat, *lon))
            })
            .collect();
        if !spatial_nodes.is_empty() {
            {
                let existing = self.spatial.read();
                for node in existing.iter() {
                    spatial_nodes.push(*node);
                }
            }
            *self.spatial.write() = RTree::bulk_load(spatial_nodes);
        }

        let all_indices: Vec<u32> = node_meta.iter().map(|(idx, _, _, _, _)| *idx).collect();
        let vec_indices: Vec<u32> = node_meta
            .iter()
            .filter(|(_, has_vec, _, _, _)| *has_vec)
            .map(|(idx, _, _, _, _)| *idx)
            .collect();

        Ok((all_indices, vec_indices))
    }

    /// Phase 4: Build HNSW index from all vectors already in arena.
    ///
    /// Parallel construction via Rayon with adaptive ef_construction schedule:
    /// - First node inserted sequentially to bootstrap the entry point
    /// - Remaining nodes inserted in parallel (DashMap + Atomic<NeighborList> are concurrent-safe)
    /// - ef schedule: 8 (sparse) → 12 → 16 (dense), averaged ~13 vs old fixed 32
    /// - Thread-local SearchContext reuse eliminates per-insert allocation
    /// - Entry point updates use CAS loop for thread safety
    pub(crate) fn build_hnsw_batch(&self) -> Result<(), Box<dyn std::error::Error>> {
        use crate::hnsw::SearchContext;
        use rayon::prelude::*;

        let node_count = self.nodes.write_head.load(Ordering::Acquire);

        let vec_indices: Vec<u32> = (0..node_count as u32)
            .filter(|&idx| {
                let slot = self.nodes.read_at(idx as u64);
                slot.flags != 0 && slot.vec_slot != u32::MAX
            })
            .collect();

        if vec_indices.is_empty() {
            return Ok(());
        }

        let hnsw_guard = self.hnsw.read();
        if let Some(ref hnsw) = *hnsw_guard {
            let total = vec_indices.len();

            // Bootstrap: insert first node sequentially to establish entry point
            let mut bootstrap_ctx = SearchContext::new(total + 1);
            hnsw.insert_index_with_ctx(vec_indices[0], 8, &mut bootstrap_ctx)
                .map_err(|e: String| -> Box<dyn std::error::Error> { e.into() })?;

            if total == 1 {
                return Ok(());
            }

            // Parallel insertion: each thread gets its own SearchContext via thread_local!
            // All graph internals (DashMap, Atomic<NeighborList> + epoch GC) are concurrent-safe.
            let err: std::sync::Mutex<Option<Box<dyn std::error::Error + Send + Sync>>> =
                std::sync::Mutex::new(None);

            vec_indices[1..].par_iter().enumerate().for_each(|(_i, &idx)| {
                if err.lock().unwrap().is_some() {
                    return;
                }

                thread_local! {
                    static CTX: std::cell::RefCell<Option<SearchContext>> =
                        std::cell::RefCell::new(None);
                }

                CTX.with(|cell| {
                    let mut borrow = cell.borrow_mut();
                    let ctx = borrow.get_or_insert_with(|| SearchContext::new(total + 1));
                    ctx.ensure_capacity(total + 1);

                    // Fixed ef=32: same quality as single-item path.
                    // Speed gain comes from parallelism + deferred commits, not lower ef.
                    let ef = 32;

                    if let Err(e) = hnsw.insert_index_with_ctx(idx, ef, ctx) {
                        *err.lock().unwrap() = Some(e.into());
                    }
                });
            });

            if let Some(e) = err.into_inner().unwrap() {
                return Err(e);
            }
        }

        Ok(())
    }

    /// Batch ingest edges: deferred writes → parallel → single commit
    pub(crate) fn ingest_edges_batch(
        &self,
        edges: &[(&str, &str, &str, f32)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        use rayon::prelude::*;
        edges
            .par_iter()
            .try_for_each(|&(src, dst, etype, weight)| {
                self.add_edge_deferred(src, dst, weight, etype)
                    .map_err(|e| e.to_string())
            })
            .map_err(|e: String| -> Box<dyn std::error::Error> { e.into() })?;
        let edge_count = self.edges.write_head.load(Ordering::Acquire);
        self.edges.commit(edge_count);
        Ok(())
    }

    // =========================================================================
    // Shared helper extractors
    // =========================================================================

    fn extract_coords(value: &Value) -> (f32, f32) {
        if let Some(info) = crate::geometry::extract_geo_info(value) {
            return (info.centroid_lat, info.centroid_lon);
        }
        (0.0, 0.0)
    }

    /// Build a SpatialNode from a node value. Uses geometry bbox when available.
    fn extract_spatial_node(idx: u32, value: &Value) -> Option<SpatialNode> {
        crate::geometry::extract_geo_info(value).map(|info| {
            SpatialNode::from_bbox(
                idx,
                info.centroid_lat,
                info.centroid_lon,
                info.bbox_min_lat,
                info.bbox_min_lon,
                info.bbox_max_lat,
                info.bbox_max_lon,
            )
        })
    }

    fn write_vector_if_present(
        vectors: &parking_lot::RwLock<DurableArena<VectorSlot>>,
        n_idx: u64,
        value: &Value,
    ) -> bool {
        if let Some(vec_arr) = value
            .get("vectors")
            .and_then(|v| v.get("dense"))
            .and_then(|v| v.as_array())
        {
            let mut data = [0.0f32; 128];
            for (i, v) in vec_arr.iter().take(128).enumerate() {
                data[i] = v.as_f64().unwrap_or(0.0) as f32;
            }
            let v = vectors.read();
            // Guard: only write if the vector arena has been initialised (init_hnsw called)
            if n_idx < v.capacity() as u64 {
                v.write_at(n_idx, &VectorSlot { data });
            }
            true
        } else {
            false
        }
    }

    /// Read internal
    pub(crate) fn read_internal(&self, slug: &str) -> Option<String> {
        let (_, slug_hash) = Self::parse_entity_id(slug);
        let idx = self.slug_index.read().get(slug_hash)?;
        let slot = self.nodes.read_at(idx as u64);
        if slot.flags == 0 {
            return None;
        }
        let bytes = self.blobs.read(slot.blob_offset, slot.blob_len);
        Some(String::from_utf8_lossy(bytes).into_owned())
    }

    /// Add edge internal
    pub(crate) fn add_edge_internal(
        &self,
        source_slug: &str,
        target_slug: &str,
        weight: f32,
        edge_type: &str,
    ) -> Result<(), String> {
        let (_, src_hash) = Self::parse_entity_id(source_slug);
        let (_, dst_hash) = Self::parse_entity_id(target_slug);
        let type_hash = seahash::hash(edge_type.as_bytes());

        let src_idx = self
            .slug_index
            .read()
            .get(src_hash)
            .ok_or_else(|| format!("Source not found: {}", source_slug))?;
        let dst_idx = self
            .slug_index
            .read()
            .get(dst_hash)
            .ok_or_else(|| format!("Target not found: {}", target_slug))?;

        let e_idx = self.edges.write_head.fetch_add(1, Ordering::Relaxed);
        let edge = EdgeSlot {
            from_node: src_idx,
            to_node: dst_idx,
            weight,
            edge_type_hash: type_hash,
            timestamp: self.cached_timestamp.load(Ordering::Relaxed),
            flags: 1,
            ..Default::default()
        };

        self.edges.write_at(e_idx, &edge);
        self.adj_fwd.entry(src_idx).or_default().push(e_idx as u32);
        self.adj_rev.entry(dst_idx).or_default().push(e_idx as u32);
        self.edges.commit(e_idx + 1);

        Ok(())
    }

    /// Add edge with arbitrary JSON metadata stored inline (≤32 bytes) or in blob arena (>32 bytes).
    pub(crate) fn add_edge_meta_internal(
        &self,
        source_slug: &str,
        target_slug: &str,
        weight: f32,
        edge_type: &str,
        meta_json: &str,
    ) -> Result<(), String> {
        let (_, src_hash) = Self::parse_entity_id(source_slug);
        let (_, dst_hash) = Self::parse_entity_id(target_slug);
        let type_hash = seahash::hash(edge_type.as_bytes());

        let src_idx = self
            .slug_index
            .read()
            .get(src_hash)
            .ok_or_else(|| format!("Source not found: {}", source_slug))?;
        let dst_idx = self
            .slug_index
            .read()
            .get(dst_hash)
            .ok_or_else(|| format!("Target not found: {}", target_slug))?;

        let e_idx = self.edges.write_head.fetch_add(1, Ordering::Relaxed);

        let (meta_kind, meta_len, meta) = if meta_json.is_empty() {
            (0u8, 0u8, [0u8; 32])
        } else {
            let bytes = meta_json.as_bytes();
            if bytes.len() <= 32 {
                let mut buf = [0u8; 32];
                buf[..bytes.len()].copy_from_slice(bytes);
                (1u8, bytes.len() as u8, buf)
            } else {
                let (offset, blen) = self.blobs.append(bytes);
                self.blobs.commit();
                let mut buf = [0u8; 32];
                buf[..8].copy_from_slice(&offset.to_le_bytes());
                buf[8..12].copy_from_slice(&blen.to_le_bytes());
                (2u8, 0u8, buf)
            }
        };

        let edge = EdgeSlot {
            from_node: src_idx,
            to_node: dst_idx,
            weight,
            edge_type_hash: type_hash,
            timestamp: self.cached_timestamp.load(Ordering::Relaxed),
            flags: 1,
            meta_kind,
            meta_len,
            meta,
            ..Default::default()
        };

        self.edges.write_at(e_idx, &edge);
        self.adj_fwd.entry(src_idx).or_default().push(e_idx as u32);
        self.adj_rev.entry(dst_idx).or_default().push(e_idx as u32);
        self.edges.commit(e_idx + 1);

        Ok(())
    }

    /// Delete internal
    pub(crate) fn delete_internal(&self, slug: &str) -> Result<(), Box<dyn std::error::Error>> {
        let (_, slug_hash) = Self::parse_entity_id(slug);
        let idx_opt = {
            let slug_r = self.slug_index.read();
            slug_r.get(slug_hash)
        };
        if let Some(idx) = idx_opt {
            let mut slot = self.nodes.read_at(idx as u64);
            let collection_hash = slot.collection_hash;
            slot.flags = 0;
            self.nodes.write_at(idx as u64, &slot);
            if let Some(count) = self.collection_counts.get(&collection_hash) {
                count.fetch_sub(1, Ordering::Relaxed);
            }
            self.collection_bitmaps.remove(collection_hash, idx);
            // Remove from field indexes
            for entry in self.field_hash_indexes.iter() {
                entry.value().remove(idx);
            }
            for entry in self.field_range_indexes.iter() {
                entry.value().remove(idx);
            }
            self.slug_index.write().remove(slug_hash);
        }
        Ok(())
    }

    /// Delete edge internal
    pub(crate) fn delete_edge_internal(
        &self,
        source: &str,
        target: &str,
        etype: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (_, src_hash) = Self::parse_entity_id(source);
        let (_, dst_hash) = Self::parse_entity_id(target);
        let type_hash = seahash::hash(etype.as_bytes());
        let src_idx = self
            .slug_index
            .read()
            .get(src_hash)
            .ok_or("Source not found")?;
        let dst_idx = self
            .slug_index
            .read()
            .get(dst_hash)
            .ok_or("Target not found")?;

        if let Some(edges) = self.adj_fwd.get(&src_idx) {
            for &e_idx in edges.iter() {
                let mut edge = self.edges.read_at(e_idx as u64);
                if edge.to_node == dst_idx && edge.edge_type_hash == type_hash && edge.flags != 0 {
                    edge.flags = 0;
                    self.edges.write_at(e_idx as u64, &edge);
                    break;
                }
            }
        }
        Ok(())
    }

    /// Define collection internal
    pub(crate) fn define_collection_internal(
        &self,
        name: &str,
        json: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let v: Value = serde_json::from_str(json)?;
        let hash = seahash::hash(name.as_bytes());
        let hot = if v["hot_fields"].is_object() {
            &v["hot_fields"]
        } else {
            &v["hot"]
        };
        let col_schema = CollectionSchema {
            vector_fields: hot["vector"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect()
                })
                .unwrap_or_default(),
            spatial_fields: hot["spatial"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect()
                })
                .unwrap_or_default(),
            fulltext_fields: hot["fulltext"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect()
                })
                .unwrap_or_default(),
            hash_indexed_fields: hot["hash_index"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect()
                })
                .unwrap_or_default(),
            range_indexed_fields: hot["range_index"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect()
                })
                .unwrap_or_default(),
        };

        // Activate field indexes for newly defined hot fields
        for field in &col_schema.hash_indexed_fields {
            self.field_hash_indexes
                .entry(field.clone())
                .or_insert_with(|| Arc::new(HashIndex::new(field)));
        }
        for field in &col_schema.range_indexed_fields {
            self.field_range_indexes
                .entry(field.clone())
                .or_insert_with(|| Arc::new(RangeIndex::new(field)));
        }

        self.collections.insert(hash, col_schema);
        Ok(())
    }

    /// Count collection internal
    pub(crate) fn count_collection_internal(&self, collection: &str) -> usize {
        let col_hash = seahash::hash(collection.as_bytes());
        self.collection_counts
            .get(&col_hash)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    // --- Helper Methods ---

    pub(crate) fn resolve_hits(
        &self,
        bitmap: &roaring::RoaringBitmap,
        with_payload: bool,
    ) -> Vec<Hit> {
        let mut out = Vec::with_capacity(bitmap.len() as usize);
        let slug_r = self.slug_index.read();
        for idx in bitmap.iter() {
            let slot = self.nodes.read_at(idx as u64);
            if slot.flags == 0 {
                continue;
            }
            if slug_r.get(slot.slug_hash) != Some(idx) {
                continue;
            }
            out.push(Hit {
                idx,
                slug_hash: slot.slug_hash,
                collection_hash: slot.collection_hash,
                payload: if with_payload {
                    let bytes = self.blobs.read(slot.blob_offset, slot.blob_len);
                    Some(String::from_utf8_lossy(bytes).into_owned())
                } else {
                    None
                },
                lat: slot.lat,
                lon: slot.lon,
                score: None,
            });
        }
        out
    }

    pub(crate) fn resolve_single_hit(&self, idx: u32, with_payload: bool) -> Hit {
        let slot = self.nodes.read_at(idx as u64);
        Hit {
            idx,
            slug_hash: slot.slug_hash,
            collection_hash: slot.collection_hash,
            payload: if with_payload {
                let bytes = self.blobs.read(slot.blob_offset, slot.blob_len);
                Some(String::from_utf8_lossy(bytes).into_owned())
            } else {
                None
            },
            lat: slot.lat,
            lon: slot.lon,
            score: None,
        }
    }

    pub(crate) fn aggregate_field(
        &self,
        bitmap: &roaring::RoaringBitmap,
        field: &str,
        op: AggOp,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let mut sum = 0.0;
        let mut count = 0usize;
        for idx in bitmap.iter() {
            let slot = self.nodes.read_at(idx as u64);
            if slot.flags == 0 {
                continue;
            }
            let bytes = self.blobs.read(slot.blob_offset, slot.blob_len);
            if let Ok(json) = serde_json::from_slice::<Value>(bytes) {
                if let Some(num) = json.get(field).and_then(|v| v.as_f64()) {
                    sum += num;
                    count += 1;
                }
            }
        }
        match op {
            AggOp::Avg => Ok(if count > 0 { sum / count as f64 } else { 0.0 }),
            AggOp::Sum => Ok(sum),
        }
    }

    // --- System Methods ---

    pub fn flush(&self) -> std::io::Result<()> {
        self.nodes.flush_written()?;
        self.edges.flush_written()?;
        self.vectors.read().flush_written()?;
        self.blobs.flush_written()?;
        self.slug_index.read().flush()?;
        self.collection_bitmaps.flush()?;

        #[cfg(feature = "fulltext")]
        if let Some(ref ft) = *self.fulltext.read() {
            let _ = ft.commit();
        }

        Ok(())
    }

    pub fn backup(&self, path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        let count = self.nodes.write_head.load(Ordering::Relaxed);

        // Build idx → slug map so edges can reference slugs instead of raw indices
        let mut idx_to_slug: Vec<Option<String>> = vec![None; count as usize];
        let mut nodes = Vec::new();
        for i in 0..count {
            let slot = self.nodes.read_at(i);
            if slot.flags == 0 {
                continue;
            }
            let bytes = self.blobs.read(slot.blob_offset, slot.blob_len);
            let json: Value = serde_json::from_slice(bytes)?;
            if let Some(id) = json.get("_id").and_then(|v| v.as_str()) {
                idx_to_slug[i as usize] = Some(id.to_string());
            }
            nodes.push(json);
        }

        let mut edges = Vec::new();
        let edge_count = self.edges.write_head.load(Ordering::Relaxed);
        for i in 0..edge_count {
            let edge = self.edges.read_at(i);
            if edge.flags == 0 {
                continue;
            }

            // Decode edge metadata payload
            let meta_json: Option<String> = match edge.meta_kind {
                1 => {
                    let len = edge.meta_len as usize;
                    std::str::from_utf8(&edge.meta[..len])
                        .ok()
                        .map(|s| s.to_string())
                }
                2 => {
                    let offset = u64::from_le_bytes(edge.meta[..8].try_into().unwrap_or([0u8; 8]));
                    let len = u32::from_le_bytes(edge.meta[8..12].try_into().unwrap_or([0u8; 4]));
                    let bytes = self.blobs.read(offset, len);
                    std::str::from_utf8(bytes).ok().map(|s| s.to_string())
                }
                _ => None,
            };

            let from_slug = idx_to_slug
                .get(edge.from_node as usize)
                .and_then(|s| s.as_deref())
                .unwrap_or("");
            let to_slug = idx_to_slug
                .get(edge.to_node as usize)
                .and_then(|s| s.as_deref())
                .unwrap_or("");

            let mut edge_obj = serde_json::json!({
                "from": from_slug,
                "to": to_slug,
                "weight": edge.weight,
                "type_hash": edge.edge_type_hash,
            });
            if let Some(meta) = meta_json {
                edge_obj["meta"] = Value::String(meta);
            }
            edges.push(edge_obj);
        }

        let backup = serde_json::json!({ "nodes": nodes, "edges": edges });
        std::fs::write(path, serde_json::to_string_pretty(&backup)?)?;
        Ok(())
    }

    pub fn restore(&self, path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let backup: Value = serde_json::from_str(&content)?;

        // Restore nodes first
        if let Some(nodes) = backup["nodes"].as_array() {
            for node in nodes {
                let json = serde_json::to_string(node)?;
                let slug = node
                    .get("_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| {
                        let c = node.get("_collection")?.as_str()?;
                        let k = node.get("_key").or(node.get("slug"))?.as_str()?;
                        Some(format!("{}/{}", c, k))
                    })
                    .unwrap_or_else(|| "nodes/untitled".to_string());
                self.write_internal(&slug, &json)?;
            }
        }

        // Restore edges (after nodes so slugs resolve correctly)
        if let Some(edges) = backup["edges"].as_array() {
            for edge in edges {
                let from = edge["from"].as_str().unwrap_or("");
                let to = edge["to"].as_str().unwrap_or("");
                let weight = edge["weight"].as_f64().unwrap_or(1.0) as f32;
                let type_hash = edge["type_hash"].as_u64().unwrap_or(0);
                let meta_str = edge["meta"].as_str();

                let (_, from_hash) = Self::parse_entity_id(from);
                let (_, to_hash) = Self::parse_entity_id(to);
                let from_idx = match self.slug_index.read().get(from_hash) {
                    Some(idx) => idx,
                    None => continue,
                };
                let to_idx = match self.slug_index.read().get(to_hash) {
                    Some(idx) => idx,
                    None => continue,
                };

                let (meta_kind, meta_len, meta) = if let Some(s) = meta_str {
                    let bytes = s.as_bytes();
                    if bytes.len() <= 32 {
                        let mut buf = [0u8; 32];
                        buf[..bytes.len()].copy_from_slice(bytes);
                        (1u8, bytes.len() as u8, buf)
                    } else {
                        let (offset, blen) = self.blobs.append(bytes);
                        self.blobs.commit();
                        let mut buf = [0u8; 32];
                        buf[..8].copy_from_slice(&offset.to_le_bytes());
                        buf[8..12].copy_from_slice(&blen.to_le_bytes());
                        (2u8, 0u8, buf)
                    }
                } else {
                    (0u8, 0u8, [0u8; 32])
                };

                let e_idx = self.edges.write_head.fetch_add(1, Ordering::Relaxed);
                let edge_slot = EdgeSlot {
                    from_node: from_idx,
                    to_node: to_idx,
                    weight,
                    edge_type_hash: type_hash,
                    timestamp: self.cached_timestamp.load(Ordering::Relaxed),
                    flags: 1,
                    meta_kind,
                    meta_len,
                    meta,
                    ..Default::default()
                };
                self.edges.write_at(e_idx, &edge_slot);
                self.adj_fwd.entry(from_idx).or_default().push(e_idx as u32);
                self.adj_rev.entry(to_idx).or_default().push(e_idx as u32);
                self.edges.commit(e_idx + 1);
            }
        }

        Ok(())
    }

    // --- Unified Query/Mutate Interface ---

    /// Parse SekejapQL text or JSON pipeline — auto-detected by leading '{'.
    fn parse_input(input: &str) -> Result<Vec<Step>, Box<dyn std::error::Error>> {
        if input.trim_start().starts_with('{') {
            QueryCompiler::new().parse_pipeline_direct(input)
        } else {
            QueryCompiler::new().parse_text_pipeline(input)
        }
    }

    /// Execute a query. Accepts SekejapQL text or JSON pipeline (auto-detected).
    pub fn query(&self, input: &str) -> Result<Outcome<Vec<Hit>>, Box<dyn std::error::Error>> {
        Set::from_steps(self, Self::parse_input(input)?).collect()
    }

    /// Count results. Accepts SekejapQL text or JSON pipeline (auto-detected).
    pub fn count(&self, input: &str) -> Result<Outcome<usize>, Box<dyn std::error::Error>> {
        Set::from_steps(self, Self::parse_input(input)?).count()
    }

    /// Compile to steps without executing. Accepts SekejapQL text or JSON (auto-detected).
    pub fn explain(&self, input: &str) -> Result<Vec<Step>, Box<dyn std::error::Error>> {
        Self::parse_input(input)
    }

    pub fn mutate(&self, json: &str) -> Result<Value, Box<dyn std::error::Error>> {
        let doc: Value = serde_json::from_str(json)?;
        let op = doc["mutation"].as_str().ok_or("Missing 'mutation' field")?;
        match op {
            "put" => {
                let slug = doc["slug"].as_str().ok_or("Missing slug")?;
                let data = serde_json::to_string(&doc["data"])?;
                let idx = self.write_internal(slug, &data)?;
                Ok(serde_json::json!({"ok": true, "idx": idx}))
            }
            "put_json" => {
                let data = serde_json::to_string(&doc["data"])?;
                let idx = self.write_json_internal(&data)?;
                Ok(serde_json::json!({"ok": true, "idx": idx}))
            }
            "link" => {
                let src = doc["source"].as_str().ok_or("Missing source")?;
                let dst = doc["target"].as_str().ok_or("Missing target")?;
                let etype = doc["type"].as_str().unwrap_or("related");
                let weight = doc["weight"].as_f64().unwrap_or(1.0) as f32;
                let meta_json = if let Some(raw) = doc.get("meta_json").and_then(|v| v.as_str()) {
                    Some(raw.to_string())
                } else if let Some(meta) = doc.get("meta") {
                    Some(serde_json::to_string(meta)?)
                } else {
                    None
                };

                if let Some(meta_json) = meta_json {
                    self.add_edge_meta_internal(src, dst, weight, etype, &meta_json)?;
                    Ok(serde_json::json!({"ok": true, "meta": true}))
                } else {
                    self.add_edge_internal(src, dst, weight, etype)?;
                    Ok(serde_json::json!({"ok": true, "meta": false}))
                }
            }
            "link_meta" => {
                let src = doc["source"].as_str().ok_or("Missing source")?;
                let dst = doc["target"].as_str().ok_or("Missing target")?;
                let etype = doc["type"].as_str().unwrap_or("related");
                let weight = doc["weight"].as_f64().unwrap_or(1.0) as f32;
                let meta_json = if let Some(raw) = doc.get("meta_json").and_then(|v| v.as_str()) {
                    raw.to_string()
                } else if let Some(meta) = doc.get("meta") {
                    serde_json::to_string(meta)?
                } else {
                    return Err("Missing meta/meta_json for link_meta".into());
                };
                self.add_edge_meta_internal(src, dst, weight, etype, &meta_json)?;
                Ok(serde_json::json!({"ok": true, "meta": true}))
            }
            "remove" => {
                let slug = doc["slug"].as_str().ok_or("Missing slug")?;
                self.delete_internal(slug)?;
                Ok(serde_json::json!({"ok": true}))
            }
            "unlink" => {
                let src = doc["source"].as_str().ok_or("Missing source")?;
                let dst = doc["target"].as_str().ok_or("Missing target")?;
                let etype = doc["type"].as_str().unwrap_or("related");
                self.delete_edge_internal(src, dst, etype)?;
                Ok(serde_json::json!({"ok": true}))
            }
            _ => Err(format!("Unknown mutation: {}", op).into()),
        }
    }

    pub fn describe(&self) -> Value {
        #[cfg(feature = "fulltext")]
        let fulltext_enabled = self.fulltext.read().is_some();
        #[cfg(not(feature = "fulltext"))]
        let fulltext_enabled = false;

        let nodes_write_head = self.nodes.write_head.load(Ordering::Acquire);
        let edges_write_head = self.edges.write_head.load(Ordering::Acquire);
        let vector_enabled = self.hnsw.read().is_some();
        let vector_slots_used = self.vectors.read().write_head.load(Ordering::Acquire);
        let spatial_indexed_nodes = self.spatial.read().size();
        let graph_forward_buckets = self.adj_fwd.len();
        let graph_backward_buckets = self.adj_rev.len();

        let hash_fields: Vec<String> = self
            .field_hash_indexes
            .iter()
            .map(|e| e.key().clone())
            .collect();
        let range_fields: Vec<String> = self
            .field_range_indexes
            .iter()
            .map(|e| e.key().clone())
            .collect();

        let collections: Vec<Value> = self
            .collections
            .iter()
            .map(|entry| {
                let hash = *entry.key();
                let schema = entry.value();
                let count = self
                    .collection_counts
                    .get(&hash)
                    .map(|v| v.load(Ordering::Relaxed))
                    .unwrap_or(0);

                let hash_ready: Vec<Value> = schema
                    .hash_indexed_fields
                    .iter()
                    .map(|field| {
                        serde_json::json!({
                            "field": field,
                            "ready": self.field_hash_indexes.contains_key(field)
                        })
                    })
                    .collect();

                let range_ready: Vec<Value> = schema
                    .range_indexed_fields
                    .iter()
                    .map(|field| {
                        serde_json::json!({
                            "field": field,
                            "ready": self.field_range_indexes.contains_key(field)
                        })
                    })
                    .collect();

                serde_json::json!({
                    "hash": hash,
                    "count": count,
                    "schema": {
                        "vector_fields": schema.vector_fields,
                        "spatial_fields": schema.spatial_fields,
                        "fulltext_fields": schema.fulltext_fields,
                        "hash_indexed_fields": schema.hash_indexed_fields,
                        "range_indexed_fields": schema.range_indexed_fields
                    },
                    "indexes": {
                        "graph": {
                            "collection_bitmap_ready": true,
                            "adjacency_forward_ready": count == 0 || graph_forward_buckets > 0,
                            "adjacency_backward_ready": count == 0 || graph_backward_buckets > 0,
                            "nodes": count
                        },
                        "vector": {
                            "hnsw_ready": vector_enabled,
                            "fields": schema.vector_fields,
                            "vector_slots_used_global": vector_slots_used
                        },
                        "spatial": {
                            "rtree_ready": true,
                            "fields": schema.spatial_fields,
                            "indexed_nodes_global": spatial_indexed_nodes
                        },
                        "fulltext": {
                            "feature_enabled": cfg!(feature = "fulltext"),
                            "adapter_ready": fulltext_enabled,
                            "fields": schema.fulltext_fields
                        },
                        "payload": {
                            "hash_ready": hash_ready,
                            "range_ready": range_ready
                        }
                    }
                })
            })
            .collect();

        serde_json::json!({
            "graph": {
                "nodes_write_head": nodes_write_head,
                "edges_write_head": edges_write_head,
                "adjacency_forward_buckets": graph_forward_buckets,
                "adjacency_backward_buckets": graph_backward_buckets,
                "collection_bitmap_ready": true
            },
            "vector": {
                "enabled": vector_enabled,
                "vector_slots_used": vector_slots_used,
                "index_impl": "hnsw"
            },
            "spatial": {
                "enabled": true,
                "indexed_nodes": spatial_indexed_nodes,
                "index_impl": "rtree"
            },
            "fulltext": {
                "feature_enabled": cfg!(feature = "fulltext"),
                "enabled": fulltext_enabled
            },
            "indexes": {
                "hash_fields": hash_fields,
                "range_fields": range_fields
            },
            "collections": collections
        })
    }

    pub fn describe_collection(&self, name: &str) -> Value {
        #[cfg(feature = "fulltext")]
        let fulltext_enabled = self.fulltext.read().is_some();
        #[cfg(not(feature = "fulltext"))]
        let fulltext_enabled = false;

        let hash = seahash::hash(name.as_bytes());
        let count = self
            .collection_counts
            .get(&hash)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0);

        let schema_entry = self.collections.get(&hash);
        let exists = schema_entry.is_some();

        let (schema_json, hash_ready, range_ready, vector_fields, spatial_fields, fulltext_fields) =
            if let Some(schema) = schema_entry {
                let hash_ready: Vec<Value> = schema
                    .hash_indexed_fields
                    .iter()
                    .map(|field| {
                        serde_json::json!({
                            "field": field,
                            "ready": self.field_hash_indexes.contains_key(field)
                        })
                    })
                    .collect();
                let range_ready: Vec<Value> = schema
                    .range_indexed_fields
                    .iter()
                    .map(|field| {
                        serde_json::json!({
                            "field": field,
                            "ready": self.field_range_indexes.contains_key(field)
                        })
                    })
                    .collect();

                (
                    serde_json::json!({
                        "vector_fields": schema.vector_fields,
                        "spatial_fields": schema.spatial_fields,
                        "fulltext_fields": schema.fulltext_fields,
                        "hash_indexed_fields": schema.hash_indexed_fields,
                        "range_indexed_fields": schema.range_indexed_fields
                    }),
                    hash_ready,
                    range_ready,
                    schema.vector_fields.clone(),
                    schema.spatial_fields.clone(),
                    schema.fulltext_fields.clone(),
                )
            } else {
                (
                    serde_json::json!({}),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )
            };

        serde_json::json!({
            "name": name,
            "hash": hash,
            "count": count,
            "exists": exists,
            "schema": schema_json,
            "indexes": {
                "graph": {
                    "collection_bitmap_ready": true,
                    "adjacency_forward_ready": count == 0 || self.adj_fwd.len() > 0,
                    "adjacency_backward_ready": count == 0 || self.adj_rev.len() > 0,
                    "nodes": count
                },
                "vector": {
                    "hnsw_ready": self.hnsw.read().is_some(),
                    "fields": vector_fields,
                    "vector_slots_used_global": self.vectors.read().write_head.load(Ordering::Acquire)
                },
                "spatial": {
                    "rtree_ready": true,
                    "fields": spatial_fields,
                    "indexed_nodes_global": self.spatial.read().size()
                },
                "fulltext": {
                    "feature_enabled": cfg!(feature = "fulltext"),
                    "adapter_ready": fulltext_enabled,
                    "fields": fulltext_fields
                },
                "payload": {
                    "hash_ready": hash_ready,
                    "range_ready": range_ready
                }
            }
        })
    }
    #[deprecated(note = "Use query")]
    pub fn query_json(&self, json: &str) -> Result<Outcome<Vec<Hit>>, Box<dyn std::error::Error>> {
        self.query(json)
    }

    #[deprecated(note = "Use count")]
    pub fn query_json_count(
        &self,
        json: &str,
    ) -> Result<Outcome<usize>, Box<dyn std::error::Error>> {
        self.count(json)
    }

    #[deprecated(note = "Use explain")]
    pub fn explain_json(&self, json: &str) -> Result<Vec<Step>, Box<dyn std::error::Error>> {
        self.explain(json)
    }

    #[deprecated(note = "Use mutate")]
    pub fn mutate_json(&self, json: &str) -> Result<Value, Box<dyn std::error::Error>> {
        self.mutate(json)
    }
}
