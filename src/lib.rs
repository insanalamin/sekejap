//! sekejap — lite graph database engine
//!
//! HashMap-backed, minimal deps.
//! Same chainable query API as sekejap full, without spatial/vector/fulltext.
//!
//! # In-memory (ephemeral)
//! ```
//! use sekejap::CoreDB;
//!
//! let mut db = CoreDB::new();
//! db.put("alice", r#"{"name":"Alice","age":30,"_collection":"users"}"#).unwrap();
//! db.put("bob",   r#"{"name":"Bob",  "age":25,"_collection":"users"}"#).unwrap();
//! db.link("alice", "bob", "follows", 1.0); // strength = 1.0
//!
//! let hits = db.one("alice").forward("follows").collect();
//! assert_eq!(hits[0].slug, "bob");
//! ```
//!
//! # Persistent (WAL-backed)
//! ```no_run
//! use sekejap::CoreDB;
//!
//! let mut db = CoreDB::open("mydb").unwrap();
//! db.put("alice", r#"{"name":"Alice","_collection":"users"}"#).unwrap();
//! db.compact().unwrap();  // flush snapshot + truncate WAL
//! ```

pub mod bm25;
pub mod geo;
pub mod pipeline;
mod query;
pub mod scalar;
pub mod sql;
mod storage;
pub mod text_index;
pub mod vector;

pub use vector::{CosineDistance, Distance, DotProduct, L2Distance};

pub use pipeline::Pipeline;
pub use query::{Hit, MathExpr, MatchAggReturn, MatchAggStart, MatchAggStmt, Set, Step};
pub use sql::{CompiledMutation, EdgeDelete, EdgeInsert, SqlError, TableSchema};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Path, PathBuf};

use storage::wal::{WalEntry, WalReader, WalWriter};
use text_index::gin::GINIndex;
use text_index::gist::GiSTIndex;

// ── Field index key ───────────────────────────────────────────────────────────

/// Totally-ordered wrapper for f64 (NaN sorts last, uses `total_cmp`).
#[derive(Clone, Debug, PartialEq)]
struct OrdF64(f64);
impl Eq for OrdF64 {}
impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Ordered key for a field index: null < bool < number < string.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum FieldKey {
    Null,
    Bool(bool),
    Number(OrdF64),
    Str(String),
}

impl FieldKey {
    pub(crate) fn from_json(v: &Value) -> Option<Self> {
        match v {
            Value::Null => Some(FieldKey::Null),
            Value::Bool(b) => Some(FieldKey::Bool(*b)),
            Value::Number(n) => n.as_f64().map(|f| FieldKey::Number(OrdF64(f))),
            Value::String(s) => Some(FieldKey::Str(s.clone())),
            _ => None,
        }
    }
    pub(crate) fn from_f64(f: f64) -> Self {
        FieldKey::Number(OrdF64(f))
    }
}

// ── Internal types ────────────────────────────────────────────────────────────

/// Hash a string with SeaHash (fast, non-cryptographic, deterministic).
pub(crate) fn sk_hash(s: &str) -> u64 {
    seahash::hash(s.as_bytes())
}

pub struct NodeData {
    pub slug: String,
    pub payload: Value,
}

pub(crate) struct EdgeEntry {
    pub other: u64,     // neighbour hash (to for fwd, from for rev)
    pub edge_type: u64, // hash of the edge type label
    pub strength: f32,
    pub meta: Option<Value>,
}

// ── EdgeHit ───────────────────────────────────────────────────────────────────

/// A resolved edge returned from `db.edges_from()` / `db.edges_to()`.
#[derive(Debug, Clone)]
pub struct EdgeHit {
    pub from_slug: Option<String>,
    pub to_slug: Option<String>,
    /// Human-readable edge type label (e.g. `"taught_by"`), if recorded.
    pub edge_type: Option<String>,
    pub edge_type_hash: u64,
    pub strength: f32,
    pub meta: Option<Value>,
}

// ── CoreDB ────────────────────────────────────────────────────────────────────

/// The database. Not thread-safe by itself — wrap in `Mutex<CoreDB>` if needed.
///
/// Writes take `&mut self`. Reads and query starters take `&self`.
///
/// Use [`CoreDB::new`] for an in-memory DB, or [`CoreDB::open`] for a
/// WAL-backed persistent DB.
pub struct CoreDB {
    nodes: HashMap<u64, NodeData>,
    slug_map: HashMap<String, u64>,
    /// from_hash → outgoing edges
    adj_fwd: HashMap<u64, Vec<EdgeEntry>>,
    /// to_hash → incoming edges
    adj_rev: HashMap<u64, Vec<EdgeEntry>>,
    /// collection_hash → member slug hashes
    collections: HashMap<u64, Vec<u64>>,
    /// edge_type_hash → original name  (needed to rebuild snapshots)
    edge_type_names: HashMap<u64, String>,
    /// WAL writer — `Some` when opened from disk, `None` for in-memory.
    wal: Option<WalWriter>,
    /// Data directory path.
    data_dir: Option<PathBuf>,
    /// Grid-based spatial index for accelerating spatial queries.
    spatial_grid: Option<geo::SpatialGrid>,
    /// GiST trigram indexes for text fields (field_name -> index).
    /// Built automatically for all text fields — cheap enough to always have.
    text_indexes: HashMap<String, GiSTIndex>,
    /// GIN trigram indexes for text fields (field_name -> index).
    /// Built explicitly via build_gin_index() for exact matching (no verification).
    gin_indexes: HashMap<String, GINIndex>,
    /// BM25 full-text indexes for ranked search (field_name -> index).
    /// Built explicitly via build_bm25_index() for relevance-ranked results.
    bm25_indexes: HashMap<String, bm25::Bm25Index>,
    /// Table schemas (collection name -> schema).
    /// Persisted in WAL/snapshot.
    schemas: HashMap<String, sql::TableSchema>,
    /// Vector store: field_name → (slug_hash → vector)
    vectors: HashMap<String, HashMap<u64, Vec<f32>>>,
    /// HNSW approximate-NN indexes: field_name → graph.
    /// Built explicitly via [`CoreDB::build_hnsw_index`].
    /// Secondary index — never affects the main store on error.
    hnsw_indexes: HashMap<String, vector::HnswGraph>,
    /// Btree field indexes: (collection_hash, field_name) → ordered value → [node hashes].
    /// Built via `CREATE INDEX ON collection(field) USING btree`.
    /// Maintained incrementally on every put()/remove().
    field_indexes: HashMap<(u64, String), BTreeMap<FieldKey, Vec<u64>>>,
}

impl Default for CoreDB {
    fn default() -> Self {
        Self::new()
    }
}

impl CoreDB {
    // ── Constructors ──────────────────────────────────────────────────────────

    /// Create a new in-memory database (no persistence).
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            slug_map: HashMap::new(),
            adj_fwd: HashMap::new(),
            adj_rev: HashMap::new(),
            collections: HashMap::new(),
            edge_type_names: HashMap::new(),
            wal: None,
            data_dir: None,
            spatial_grid: None,
            text_indexes: HashMap::new(),
            gin_indexes: HashMap::new(),
            bm25_indexes: HashMap::new(),
            schemas: HashMap::new(),
            vectors: HashMap::new(),
            hnsw_indexes: HashMap::new(),
            field_indexes: HashMap::new(),
        }
    }

    /// Open (or create) a persistent database in `dir`.
    ///
    /// On startup:
    /// 1. Loads the latest snapshot (if any).
    /// 2. Replays WAL entries written after the snapshot.
    /// 3. Opens the WAL for subsequent writes.
    ///
    /// If the WAL contains a corrupted frame, recovery stops at that frame —
    /// all entries before it are intact. A warning is printed to stderr.
    ///
    /// # Errors
    /// Returns an error if the directory cannot be created, the snapshot
    /// cannot be parsed, or the WAL file cannot be opened.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;

        let mut db = Self::new();
        db.data_dir = Some(dir.to_path_buf());

        // 1. Load snapshot
        let snap_path = dir.join("snapshot.json");
        if snap_path.exists() {
            let data = std::fs::read(&snap_path)?;
            let snap: Snapshot = serde_json::from_slice(&data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            db.load_snapshot(snap);
        }

        // 2. Replay WAL
        let wal_path = dir.join("wal.log");
        if wal_path.exists() {
            let (entries, corrupted) = WalReader::open(&wal_path)?.read_all();
            for entry in entries {
                db.replay(entry);
            }
            if corrupted {
                eprintln!(
                    "sekejap: WAL at `{}` had a corrupted frame — \
                     replayed up to last good entry. Run compact() to clean up.",
                    wal_path.display()
                );
            }
        }

        // 3. Open WAL in append mode
        db.wal = Some(WalWriter::open(&wal_path)?);

        // 4. Build spatial index from loaded data
        db.rebuild_spatial_grid();

        Ok(db)
    }

    // ── Raw internals (no WAL write — used during replay and open) ────────────

    fn put_raw(&mut self, slug: &str, payload_json: &str) -> Result<u64, serde_json::Error> {
        let mut payload: Value = serde_json::from_str(payload_json)?;
        let hash = sk_hash(slug);
        let now = chrono::Utc::now().timestamp_millis();

        // Auto-timestamps: preserve existing _created_unix, always update _updated_unix
        {
            let obj = payload.as_object_mut().expect("payload must be object");
            let created_unix = if obj.contains_key("_created_unix") {
                // Use value from new payload if present
                obj.get("_created_unix").cloned()
            } else {
                // Check if old node has _created_unix to preserve
                self.nodes
                    .get(&hash)
                    .and_then(|n| n.payload.get("_created_unix"))
                    .cloned()
            };
            if let Some(v) = created_unix {
                obj.insert("_created_unix".into(), v);
            } else {
                obj.insert("_created_unix".into(), serde_json::json!(now));
            }
            obj.insert("_updated_unix".into(), serde_json::json!(now));
        }

        // Extract spatial meta before payload is moved
        let spatial_meta = geo::extract_spatial_meta(&payload);

        // Remove old collection + field-index entries for this hash (if updating)
        if let Some(old) = self.nodes.get(&hash) {
            if let Some(coll) = old.payload.get("_collection").and_then(|v| v.as_str()) {
                let coll_hash = sk_hash(coll);
                if let Some(members) = self.collections.get_mut(&coll_hash) {
                    members.retain(|&h| h != hash);
                }
                // Remove from all field indexes for this collection
                let old_payload = old.payload.clone(); // clone to release borrow
                for ((idx_coll, idx_field), btree) in &mut self.field_indexes {
                    if *idx_coll == coll_hash {
                        if let Some(key) = FieldKey::from_json(
                            old_payload.get(idx_field.as_str()).unwrap_or(&Value::Null)
                        ) {
                            if let Some(ids) = btree.get_mut(&key) {
                                ids.retain(|&id| id != hash);
                                if ids.is_empty() { btree.remove(&key); }
                            }
                        }
                    }
                }
            }
        }

        if let Some(coll) = payload.get("_collection").and_then(|v| v.as_str()) {
            let coll_hash = sk_hash(coll);
            let members = self.collections.entry(coll_hash).or_default();
            if !members.contains(&hash) {
                members.push(hash);
            }
            // Add to all field indexes for this collection
            for ((idx_coll, idx_field), btree) in &mut self.field_indexes {
                if *idx_coll == coll_hash {
                    if let Some(key) = FieldKey::from_json(
                        payload.get(idx_field.as_str()).unwrap_or(&Value::Null)
                    ) {
                        let ids = btree.entry(key).or_default();
                        if !ids.contains(&hash) { ids.push(hash); }
                    }
                }
            }
        }

        self.slug_map.insert(slug.to_string(), hash);
        self.nodes.insert(
            hash,
            NodeData {
                slug: slug.to_string(),
                payload,
            },
        );

        // Update spatial grid incrementally
        if let Some(grid) = &mut self.spatial_grid {
            grid.remove(hash);
            if let Some(meta) = spatial_meta {
                grid.insert(hash, meta);
            }
        }

        Ok(hash)
    }

    fn remove_raw(&mut self, slug: &str) {
        let hash = sk_hash(slug);
        if let Some(node) = self.nodes.remove(&hash) {
            self.slug_map.remove(slug);
            if let Some(coll) = node.payload.get("_collection").and_then(|v| v.as_str()) {
                let coll_hash = sk_hash(coll);
                if let Some(members) = self.collections.get_mut(&coll_hash) {
                    members.retain(|&h| h != hash);
                }
                // Remove from field indexes
                for ((idx_coll, idx_field), btree) in &mut self.field_indexes {
                    if *idx_coll == coll_hash {
                        if let Some(key) = FieldKey::from_json(
                            node.payload.get(idx_field.as_str()).unwrap_or(&Value::Null)
                        ) {
                            if let Some(ids) = btree.get_mut(&key) {
                                ids.retain(|&id| id != hash);
                                if ids.is_empty() { btree.remove(&key); }
                            }
                        }
                    }
                }
            }
            // Cascade-delete edges: collect neighbour hashes before mutating.
            // Forward edges (this node → others): remove the corresponding
            // back-pointers from each target's adj_rev.
            let fwd_targets: Vec<u64> = self.adj_fwd
                .get(&hash)
                .map(|es| es.iter().map(|e| e.other).collect())
                .unwrap_or_default();
            for target in fwd_targets {
                if let Some(rev) = self.adj_rev.get_mut(&target) {
                    rev.retain(|e| e.other != hash);
                }
            }
            // Reverse edges (others → this node): remove the corresponding
            // forward-pointers from each source's adj_fwd.
            let rev_sources: Vec<u64> = self.adj_rev
                .get(&hash)
                .map(|es| es.iter().map(|e| e.other).collect())
                .unwrap_or_default();
            for source in rev_sources {
                if let Some(fwd) = self.adj_fwd.get_mut(&source) {
                    fwd.retain(|e| e.other != hash);
                }
            }

            self.adj_fwd.remove(&hash);
            self.adj_rev.remove(&hash);

            if let Some(grid) = &mut self.spatial_grid {
                grid.remove(hash);
            }

            // Keep vector index consistent with main data: remove all field
            // entries for this node so orphan vectors never accumulate.
            for field_vecs in self.vectors.values_mut() {
                field_vecs.remove(&hash);
            }
        }
    }

    fn link_raw(&mut self, from: &str, to: &str, edge_type: &str, strength: f32) {
        let from_h = sk_hash(from);
        let to_h = sk_hash(to);
        let type_h = sk_hash(edge_type);
        self.edge_type_names.insert(type_h, edge_type.to_string());
        self.adj_fwd.entry(from_h).or_default().push(EdgeEntry {
            other: to_h,
            edge_type: type_h,
            strength,
            meta: None,
        });
        self.adj_rev.entry(to_h).or_default().push(EdgeEntry {
            other: from_h,
            edge_type: type_h,
            strength,
            meta: None,
        });
    }

    fn link_meta_raw(
        &mut self,
        from: &str,
        to: &str,
        edge_type: &str,
        strength: f32,
        meta_json: &str,
    ) -> Result<(), serde_json::Error> {
        let meta: Value = serde_json::from_str(meta_json)?;
        let from_h = sk_hash(from);
        let to_h = sk_hash(to);
        let type_h = sk_hash(edge_type);
        self.edge_type_names.insert(type_h, edge_type.to_string());
        self.adj_fwd.entry(from_h).or_default().push(EdgeEntry {
            other: to_h,
            edge_type: type_h,
            strength,
            meta: Some(meta.clone()),
        });
        self.adj_rev.entry(to_h).or_default().push(EdgeEntry {
            other: from_h,
            edge_type: type_h,
            strength,
            meta: Some(meta),
        });
        Ok(())
    }

    fn unlink_raw(&mut self, from: &str, to: &str, edge_type: &str) {
        let from_h = sk_hash(from);
        let to_h = sk_hash(to);
        let type_h = sk_hash(edge_type);
        if let Some(edges) = self.adj_fwd.get_mut(&from_h) {
            edges.retain(|e| !(e.other == to_h && e.edge_type == type_h));
        }
        if let Some(edges) = self.adj_rev.get_mut(&to_h) {
            edges.retain(|e| !(e.other == from_h && e.edge_type == type_h));
        }
    }

    // ── WAL helpers ───────────────────────────────────────────────────────────

    fn wal_write(&mut self, entry: WalEntry) {
        if let Some(wal) = &mut self.wal {
            wal.append(&entry)
                .expect("sekejap: WAL write failed — disk error");
        }
    }

    fn replay(&mut self, entry: WalEntry) {
        match entry {
            WalEntry::Put { slug, payload } => {
                let _ = self.put_raw(&slug, &payload);
            }
            WalEntry::Remove { slug } => self.remove_raw(&slug),
            WalEntry::Link {
                from,
                to,
                edge_type,
                strength,
            } => {
                self.link_raw(&from, &to, &edge_type, strength);
            }
            WalEntry::LinkMeta {
                from,
                to,
                edge_type,
                strength,
                meta,
            } => {
                let _ = self.link_meta_raw(&from, &to, &edge_type, strength, &meta);
            }
            WalEntry::Unlink {
                from,
                to,
                edge_type,
            } => {
                self.unlink_raw(&from, &to, &edge_type);
            }
            WalEntry::CreateTable {
                collection: _,
                schema_json,
            } => {
                if let Ok(schema) = serde_json::from_str::<sql::TableSchema>(&schema_json) {
                    self.schemas.insert(schema.collection.clone(), schema);
                }
            }
            WalEntry::PutVector { slug, field, data } => {
                let hash = sk_hash(&slug);
                self.vectors.entry(field).or_default().insert(hash, data);
            }
            WalEntry::CreateIndex { collection, method, fields } => {
                use sql::IndexMethod;
                let m = match method.as_str() {
                    "btree"   => IndexMethod::Btree,
                    "hash"    => IndexMethod::Hash,
                    "gin"     => IndexMethod::Gin,
                    "gist"    => IndexMethod::Gist,
                    "bm25"    => IndexMethod::Bm25,
                    "spatial" => IndexMethod::Spatial,
                    "hnsw"    => IndexMethod::Hnsw,
                    _ => return,
                };
                self.apply_index(&collection, &m, &fields);
            }
        }
    }

    // ── Writes ────────────────────────────────────────────────────────────────

    /// Insert or update a node. The `_collection` field in the payload
    /// registers the node in a named collection for `db.collection()` queries.
    ///
    /// Returns the slug hash on success.
    pub fn put(&mut self, slug: &str, payload_json: &str) -> Result<u64, serde_json::Error> {
        let hash = self.put_raw(slug, payload_json)?;
        self.wal_write(WalEntry::Put {
            slug: slug.to_string(),
            payload: payload_json.to_string(),
        });
        Ok(hash)
    }

    /// Bulk insert. Stops and returns the first error encountered.
    pub fn put_many<'a>(
        &mut self,
        items: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Vec<u64>, serde_json::Error> {
        items
            .into_iter()
            .map(|(slug, json)| self.put(slug, json))
            .collect()
    }

    /// Remove a node by slug. Also removes its collection membership and edges.
    pub fn remove(&mut self, slug: &str) {
        self.remove_raw(slug);
        self.wal_write(WalEntry::Remove {
            slug: slug.to_string(),
        });
    }

    /// Create a directed edge: `from` → `to` with a type label and strength.
    /// Nodes do not need to exist before linking.
    pub fn link(&mut self, from: &str, to: &str, edge_type: &str, strength: f32) {
        self.link_raw(from, to, edge_type, strength);
        self.wal_write(WalEntry::Link {
            from: from.to_string(),
            to: to.to_string(),
            edge_type: edge_type.to_string(),
            strength,
        });
    }

    /// Like `link` but attaches a JSON metadata object to the edge.
    pub fn link_meta(
        &mut self,
        from: &str,
        to: &str,
        edge_type: &str,
        strength: f32,
        meta_json: &str,
    ) -> Result<(), serde_json::Error> {
        self.link_meta_raw(from, to, edge_type, strength, meta_json)?;
        self.wal_write(WalEntry::LinkMeta {
            from: from.to_string(),
            to: to.to_string(),
            edge_type: edge_type.to_string(),
            strength,
            meta: meta_json.to_string(),
        });
        Ok(())
    }

    /// Remove all directed edges from → to with the given type.
    pub fn unlink(&mut self, from: &str, to: &str, edge_type: &str) {
        self.unlink_raw(from, to, edge_type);
        self.wal_write(WalEntry::Unlink {
            from: from.to_string(),
            to: to.to_string(),
            edge_type: edge_type.to_string(),
        });
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    /// Compact the database: write a full snapshot then truncate the WAL.
    ///
    /// After compaction the WAL is empty and `snapshot.json` contains the
    /// complete current state. All previous WAL entries are discarded.
    ///
    /// In-memory (`CoreDB::new()`) databases silently ignore this call.
    pub fn compact(&mut self) -> io::Result<()> {
        let dir = match self.data_dir.clone() {
            Some(d) => d,
            None => return Ok(()),
        };

        // 1. Write snapshot atomically (tmp → rename)
        let snap_json = serde_json::to_vec_pretty(&self.build_snapshot())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let snap_tmp = dir.join("snapshot.json.tmp");
        let snap_path = dir.join("snapshot.json");
        std::fs::write(&snap_tmp, &snap_json)?;
        std::fs::rename(&snap_tmp, &snap_path)?;

        // 2. Truncate WAL: close current writer → rename → open fresh → delete old
        self.wal = None;
        let wal_path = dir.join("wal.log");
        let wal_old = dir.join("wal.old");
        if wal_path.exists() {
            std::fs::rename(&wal_path, &wal_old)?;
        }
        self.wal = Some(WalWriter::open(&wal_path)?);
        if wal_old.exists() {
            std::fs::remove_file(&wal_old)?;
        }

        Ok(())
    }

    /// Force WAL data to reach disk (fsync).
    /// By default writes are flushed to the OS buffer but not fsynced.
    /// Call this after a critical batch of writes if you need guaranteed
    /// on-disk durability before the OS flushes on its own schedule.
    pub fn sync(&mut self) -> io::Result<()> {
        if let Some(wal) = &mut self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    // ── Snapshot helpers ──────────────────────────────────────────────────────

    fn build_snapshot(&self) -> Snapshot {
        let nodes: Vec<SnapNode> = self
            .nodes
            .values()
            .map(|n| SnapNode {
                slug: n.slug.clone(),
                payload: n.payload.clone(),
            })
            .collect();

        let mut edges: Vec<SnapEdge> = Vec::new();
        for (&from_h, edge_list) in &self.adj_fwd {
            let from_slug = match self.nodes.get(&from_h) {
                Some(n) => n.slug.clone(),
                None => continue, // dangling edge, skip
            };
            for e in edge_list {
                let to_slug = match self.nodes.get(&e.other) {
                    Some(n) => n.slug.clone(),
                    None => continue,
                };
                let edge_type = self
                    .edge_type_names
                    .get(&e.edge_type)
                    .cloned()
                    .unwrap_or_else(|| format!("{:016x}", e.edge_type));
                edges.push(SnapEdge {
                    from: from_slug.clone(),
                    to: to_slug,
                    edge_type,
                    strength: e.strength,
                    meta: e.meta.clone(),
                });
            }
        }

        // Collect vectors — only for hashes that still resolve to a live node.
        // This auto-prunes any orphan entries left by bugs or direct HashMap
        // manipulation; main data is always the authority.
        let mut snap_vectors: Vec<SnapVector> = Vec::new();
        for (field, field_vecs) in &self.vectors {
            for (&hash, data) in field_vecs {
                if let Some(node) = self.nodes.get(&hash) {
                    snap_vectors.push(SnapVector {
                        slug: node.slug.clone(),
                        field: field.clone(),
                        data: data.clone(),
                    });
                }
            }
        }

        let snap_hnsw: Vec<SnapHnsw> = self
            .hnsw_indexes
            .iter()
            .map(|(field, graph)| SnapHnsw { field: field.clone(), graph: graph.clone() })
            .collect();

        Snapshot {
            version: 1,
            nodes,
            edges,
            schemas: Some(self.schemas.values().cloned().collect()),
            vectors: if snap_vectors.is_empty() { None } else { Some(snap_vectors) },
            hnsw_indexes: if snap_hnsw.is_empty() { None } else { Some(snap_hnsw) },
        }
    }

    fn load_snapshot(&mut self, snap: Snapshot) {
        for n in snap.nodes {
            let _ = self.put_raw(&n.slug, &n.payload.to_string());
        }
        for e in snap.edges {
            if let Some(meta) = e.meta {
                let _ =
                    self.link_meta_raw(&e.from, &e.to, &e.edge_type, e.strength, &meta.to_string());
            } else {
                self.link_raw(&e.from, &e.to, &e.edge_type, e.strength);
            }
        }
        if let Some(schemas) = snap.schemas {
            for schema in schemas {
                self.schemas.insert(schema.collection.clone(), schema);
            }
        }
        // Restore vector index from snapshot — WAL replay will add anything
        // written after the snapshot was taken.
        if let Some(vecs) = snap.vectors {
            for sv in vecs {
                let hash = sk_hash(&sv.slug);
                self.vectors.entry(sv.field).or_default().insert(hash, sv.data);
            }
        }
        // Restore HNSW graphs — avoids expensive rebuild on every startup.
        if let Some(hnsw_list) = snap.hnsw_indexes {
            for sh in hnsw_list {
                self.hnsw_indexes.insert(sh.field, sh.graph);
            }
        }
        // Rebuild btree field indexes from persisted schema hints.
        // Nodes are already loaded at this point so build_field_index can scan them.
        let to_rebuild: Vec<(String, String)> = self
            .schemas
            .values()
            .flat_map(|s| s.indexes.range.iter().map(|f| (s.collection.clone(), f.clone())))
            .collect();
        for (coll, field) in to_rebuild {
            self.build_field_index(&coll, &field);
        }
    }

    // ── Reads ─────────────────────────────────────────────────────────────────

    /// Get raw JSON payload for a slug. Returns `None` if not found.
    pub fn get(&self, slug: &str) -> Option<String> {
        self.nodes
            .get(&sk_hash(slug))
            .map(|n| n.payload.to_string())
    }

    /// Check if a node exists.
    pub fn contains(&self, slug: &str) -> bool {
        self.nodes.contains_key(&sk_hash(slug))
    }

    /// Total number of nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the number of directed edges currently stored.
    pub fn edge_count(&self) -> usize {
        self.adj_fwd.values().map(|v| v.len()).sum()
    }

    /// Returns all distinct collection names present in the graph, sorted.
    ///
    /// Includes collections that have nodes but no explicit `CREATE TABLE` schema.
    pub fn collection_names(&self) -> Vec<String> {
        let mut names: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for node in self.nodes.values() {
            if let Some(c) = node.payload.get("_collection").and_then(|v| v.as_str()) {
                names.insert(c.to_string());
            }
        }
        names.into_iter().collect()
    }

    /// Returns a `CREATE TABLE` DDL string for a collection if a schema was declared.
    /// Returns `None` if no `CREATE TABLE` was issued for that collection.
    pub fn schema_ddl(&self, collection: &str) -> Option<String> {
        let schema = self.schemas.get(collection)?;
        let mut ddl = format!("CREATE TABLE {} (", schema.collection);
        let parts: Vec<String> = schema.fields.iter().map(|f| {
            let ty = match f.ty {
                sql::FieldType::Text        => "TEXT",
                sql::FieldType::Integer     => "INTEGER",
                sql::FieldType::Real        => "REAL",
                sql::FieldType::Timestamptz => "TIMESTAMPTZ",
                sql::FieldType::Geo         => "GEO",
                sql::FieldType::Vector      => "VECTOR",
                sql::FieldType::Json        => "JSON",
            };
            if f.is_primary_key {
                format!("{} {} PRIMARY KEY", f.name, ty)
            } else {
                format!("{} {}", f.name, ty)
            }
        }).collect();
        ddl.push_str(&parts.join(", "));
        ddl.push(')');
        Some(ddl)
    }

    /// Get all outgoing edges from a node, resolved to slugs where available.
    pub fn edges_from(&self, slug: &str) -> Vec<EdgeHit> {
        let hash = sk_hash(slug);
        self.adj_fwd
            .get(&hash)
            .map(|edges| {
                edges
                    .iter()
                    .map(|e| EdgeHit {
                        from_slug: Some(slug.to_string()),
                        to_slug: self.nodes.get(&e.other).map(|n| n.slug.clone()),
                        edge_type: self.edge_type_names.get(&e.edge_type).cloned(),
                        edge_type_hash: e.edge_type,
                        strength: e.strength,
                        meta: e.meta.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all incoming edges to a node, resolved to slugs where available.
    pub fn edges_to(&self, slug: &str) -> Vec<EdgeHit> {
        let hash = sk_hash(slug);
        self.adj_rev
            .get(&hash)
            .map(|edges| {
                edges
                    .iter()
                    .map(|e| EdgeHit {
                        from_slug: self.nodes.get(&e.other).map(|n| n.slug.clone()),
                        to_slug: Some(slug.to_string()),
                        edge_type: self.edge_type_names.get(&e.edge_type).cloned(),
                        edge_type_hash: e.edge_type,
                        strength: e.strength,
                        meta: e.meta.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// List all outgoing edges from every node in `from_collection`.
    pub fn edges_from_collection(&self, from_collection: &str) -> Vec<EdgeHit> {
        let col_h = sk_hash(from_collection);
        let mut result = Vec::new();
        for (&node_h, node) in &self.nodes {
            let in_col = node.payload.get("_collection")
                .and_then(|v| v.as_str())
                .map(|c| sk_hash(c) == col_h)
                .unwrap_or(false);
            if !in_col { continue; }
            if let Some(edges) = self.adj_fwd.get(&node_h) {
                for e in edges {
                    result.push(EdgeHit {
                        from_slug: Some(node.slug.clone()),
                        to_slug: self.nodes.get(&e.other).map(|n| n.slug.clone()),
                        edge_type: self.edge_type_names.get(&e.edge_type).cloned(),
                        edge_type_hash: e.edge_type,
                        strength: e.strength,
                        meta: e.meta.clone(),
                    });
                }
            }
        }
        result
    }

    /// List edges that go from nodes in `from_collection` to nodes in `to_collection`.
    pub fn edges_between(&self, from_collection: &str, to_collection: &str) -> Vec<EdgeHit> {
        let to_col_h = sk_hash(to_collection);
        self.edges_from_collection(from_collection)
            .into_iter()
            .filter(|e| {
                e.to_slug.as_deref()
                    .and_then(|s| self.slug_map.get(s))
                    .and_then(|h| self.nodes.get(h))
                    .and_then(|n| n.payload.get("_collection"))
                    .and_then(|v| v.as_str())
                    .map(|c| sk_hash(c) == to_col_h)
                    .unwrap_or(false)
            })
            .collect()
    }

    /// Distinct edge type labels on outgoing edges from a single node.
    ///
    /// ```
    /// # use sekejap::CoreDB;
    /// # let mut db = CoreDB::new();
    /// # db.put("cls/math", r#"{"_collection":"classrooms"}"#).unwrap();
    /// # db.put("lec/ali",  r#"{"_collection":"lecturers"}"#).unwrap();
    /// # db.link("cls/math", "lec/ali", "taught_by", 1.0);
    /// let types = db.edge_types_from("cls/math");
    /// assert_eq!(types, vec!["taught_by"]);
    /// ```
    pub fn edge_types_from(&self, slug: &str) -> Vec<String> {
        let hash = sk_hash(slug);
        let mut seen = std::collections::HashSet::new();
        let mut types = Vec::new();
        if let Some(edges) = self.adj_fwd.get(&hash) {
            for e in edges {
                if let Some(label) = self.edge_type_names.get(&e.edge_type) {
                    if seen.insert(e.edge_type) {
                        types.push(label.clone());
                    }
                }
            }
        }
        types.sort();
        types
    }

    /// Distinct edge type labels on outgoing edges from any node in a collection.
    ///
    /// ```
    /// # use sekejap::CoreDB;
    /// # let mut db = CoreDB::new();
    /// # db.put("cls/math", r#"{"_collection":"classrooms"}"#).unwrap();
    /// # db.put("lec/ali",  r#"{"_collection":"lecturers"}"#).unwrap();
    /// # db.link("cls/math", "lec/ali", "taught_by", 1.0);
    /// let types = db.edge_types_from_collection("classrooms");
    /// assert_eq!(types, vec!["taught_by"]);
    /// ```
    pub fn edge_types_from_collection(&self, collection: &str) -> Vec<String> {
        let col_h = sk_hash(collection);
        let mut seen = std::collections::HashSet::new();
        let mut types = Vec::new();
        for (&node_h, node) in &self.nodes {
            let in_col = node.payload.get("_collection")
                .and_then(|v| v.as_str())
                .map(|c| sk_hash(c) == col_h)
                .unwrap_or(false);
            if !in_col { continue; }
            if let Some(edges) = self.adj_fwd.get(&node_h) {
                for e in edges {
                    if let Some(label) = self.edge_type_names.get(&e.edge_type) {
                        if seen.insert(e.edge_type) {
                            types.push(label.clone());
                        }
                    }
                }
            }
        }
        types.sort();
        types
    }

    /// Full graph schema: distinct `(from_collection, edge_type, to_collection)` triples.
    ///
    /// Tells you what relationships actually exist between collections in the data.
    ///
    /// ```
    /// # use sekejap::CoreDB;
    /// # let mut db = CoreDB::new();
    /// # db.put("cls/math", r#"{"_collection":"classrooms"}"#).unwrap();
    /// # db.put("lec/ali",  r#"{"_collection":"lecturers"}"#).unwrap();
    /// # db.link("cls/math", "lec/ali", "taught_by", 1.0);
    /// let schema = db.edge_schema();
    /// assert_eq!(schema, vec![("classrooms".into(), "taught_by".into(), "lecturers".into())]);
    /// ```
    pub fn edge_schema(&self) -> Vec<(String, String, String)> {
        let mut seen = std::collections::HashSet::new();
        let mut triples = Vec::new();
        for (&from_h, node) in &self.nodes {
            let from_col = match node.payload.get("_collection").and_then(|v| v.as_str()) {
                Some(c) => c.to_string(),
                None => continue,
            };
            if let Some(edges) = self.adj_fwd.get(&from_h) {
                for e in edges {
                    let edge_label = match self.edge_type_names.get(&e.edge_type) {
                        Some(l) => l.clone(),
                        None => continue,
                    };
                    let to_col = match self.nodes.get(&e.other)
                        .and_then(|n| n.payload.get("_collection"))
                        .and_then(|v| v.as_str())
                    {
                        Some(c) => c.to_string(),
                        None => continue,
                    };
                    let key = (from_col.clone(), edge_label.clone(), to_col.clone());
                    if seen.insert(key.clone()) {
                        triples.push(key);
                    }
                }
            }
        }
        triples.sort();
        triples
    }

    // ── Query starters ────────────────────────────────────────────────────────

    /// Start a query from a single node.
    pub fn one(&self, slug: &str) -> Set<'_> {
        Set::new(self, Step::One(sk_hash(slug)))
    }

    /// Start a query from a set of nodes.
    pub fn many<'a>(&self, slugs: impl IntoIterator<Item = &'a str>) -> Set<'_> {
        Set::new(self, Step::Many(slugs.into_iter().map(sk_hash).collect()))
    }

    /// Start a query over all nodes.
    pub fn all(&self) -> Set<'_> {
        Set::new(self, Step::All)
    }

    /// Start a query over all nodes in a named collection.
    pub fn collection(&self, name: &str) -> Set<'_> {
        Set::new(self, Step::Collection(sk_hash(name)))
    }

    /// Execute a SQL SELECT query and return a lazy [`Set`].
    ///
    /// # Errors
    /// Returns [`SqlError`] if the SQL is syntactically invalid.
    ///
    /// # Example
    /// ```
    /// # use sekejap::CoreDB;
    /// let mut db = CoreDB::new();
    /// db.put("alice", r#"{"name":"Alice","_collection":"users"}"#).unwrap();
    /// let hits = db.query("SELECT * FROM users WHERE name = 'Alice'")
    ///     .unwrap().collect();
    /// assert_eq!(hits[0].slug, "alice");
    /// ```
    pub fn query(&self, sql: &str) -> Result<Set<'_>, SqlError> {
        match sql::parse_match_or_agg(sql)? {
            sql::MatchOrAgg::Agg(stmt) => {
                let hits = query::execute_match_agg(self, stmt);
                Ok(Set::from_hits(self, hits))
            }
            sql::MatchOrAgg::Steps(steps) => Ok(Set::from_steps(self, steps)),
        }
    }

    /// Execute a `MATCH + WITH` pipeline query.
    ///
    /// Implements the Cypher-style pipeline: one or more `MATCH` stages
    /// interleaved with `WITH` aggregation checkpoints, ending with `RETURN`.
    ///
    /// # Syntax
    /// ```text
    /// MATCH ('start')-[:edge1]->(a)-[:edge2]->(b)
    /// WITH  b.field AS alias, SUM(a.x * b.y) AS score
    /// MATCH (c:clos WHERE _key = alias)-[:edge3]->(d:dest)
    /// RETURN d._key AS out, SUM(score * c.w) AS total ORDER BY total DESC LIMIT 10
    /// ```
    ///
    /// # Errors
    /// Returns [`SqlError`] if the statement is syntactically invalid.
    pub fn pipeline_query(&self, sql: &str) -> Result<Vec<query::Hit>, SqlError> {
        let pipe = sql::parse_pipeline(sql)?;
        Ok(pipeline::execute_pipeline(self, pipe))
    }

    /// Execute a `SHOW EDGES` introspection statement.
    ///
    /// Syntax:
    /// ```text
    /// SHOW EDGES                         -- all (from, type, to) triples distinct
    /// SHOW EDGES FROM classrooms         -- distinct edge types from that collection
    /// SHOW EDGES FROM classrooms TO lecturers  -- types between two collections
    /// ```
    ///
    /// Returns `Vec<Hit>` where each hit's payload is a JSON object with the
    /// relevant fields (`from`, `type`, `to` for full schema; `type` for filtered).
    pub fn show(&self, sql: &str) -> Result<Vec<query::Hit>, SqlError> {
        let stmt = sql::parse_show(sql)?;

        let make_hit = |payload: serde_json::Value| query::Hit {
            slug: String::new(),
            slug_hash: 0,
            payload: Some(payload),
        };

        let hits = match (stmt.from_col, stmt.to_col) {
            (None, _) => {
                // Full graph schema
                self.edge_schema()
                    .into_iter()
                    .map(|(from, kind, to)| make_hit(serde_json::json!({
                        "from": from, "type": kind, "to": to
                    })))
                    .collect()
            }
            (Some(from_col), None) => {
                // Distinct types from one collection
                self.edge_types_from_collection(&from_col)
                    .into_iter()
                    .map(|kind| make_hit(serde_json::json!({ "from": from_col, "type": kind })))
                    .collect()
            }
            (Some(from_col), Some(to_col)) => {
                // Distinct types between two collections
                let to_col_h = sk_hash(&to_col);
                let mut seen = std::collections::HashSet::new();
                let mut hits = Vec::new();
                for e in self.edges_from_collection(&from_col) {
                    let in_to = e.to_slug.as_deref()
                        .and_then(|s| self.slug_map.get(s))
                        .and_then(|h| self.nodes.get(h))
                        .and_then(|n| n.payload.get("_collection"))
                        .and_then(|v| v.as_str())
                        .map(|c| sk_hash(c) == to_col_h)
                        .unwrap_or(false);
                    if in_to {
                        if let Some(kind) = e.edge_type {
                            if seen.insert(kind.clone()) {
                                hits.push(make_hit(serde_json::json!({
                                    "from": from_col, "type": kind, "to": to_col
                                })));
                            }
                        }
                    }
                }
                hits.sort_by(|a, b| {
                    let ka = a.payload.as_ref().and_then(|p| p["type"].as_str()).unwrap_or("");
                    let kb = b.payload.as_ref().and_then(|p| p["type"].as_str()).unwrap_or("");
                    ka.cmp(kb)
                });
                hits
            }
        };
        Ok(hits)
    }

    /// Execute a mutation SQL statement.
    ///
    /// Returns the number of rows affected.
    ///
    /// Supports: `INSERT INTO`, `INSERT (edge)`, `DELETE FROM`, `DELETE (edge)`,
    /// `UPDATE`.
    ///
    /// # Errors
    /// Returns [`SqlError`] if the SQL is invalid.
    pub fn execute(&mut self, sql: &str) -> Result<usize, SqlError> {
        match sql::parse_mutation(sql)? {
            sql::CompiledMutation::Insert { collection, slug, payload_json, vectors } => {
                // Type-check against schema if one is registered for this collection.
                if let Some(schema) = self.schemas.get(&collection) {
                    let payload: Value = serde_json::from_str(&payload_json)
                        .map_err(|e| SqlError::InvalidValue(e.to_string()))?;
                    if let Some(err) = validate_payload_against_schema(schema, &payload) {
                        return Err(err);
                    }
                }
                self.put(&slug, &payload_json)
                    .map_err(|e| SqlError::InvalidValue(e.to_string()))?;
                for (field, data) in vectors {
                    self.put_vector(&slug, &field, &data)
                        .map_err(|e| SqlError::InvalidValue(e.to_string()))?;
                }
                Ok(1)
            }
            sql::CompiledMutation::Delete(steps) => {
                let slugs: Vec<String> = Set::from_steps(self, steps)
                    .collect()
                    .into_iter()
                    .map(|h| h.slug)
                    .collect();
                let count = slugs.len();
                for slug in &slugs {
                    self.remove(slug);
                }
                Ok(count)
            }
            sql::CompiledMutation::InsertEdge(edges) => {
                let count = edges.len();
                for edge in edges {
                    match edge.props_json {
                        Some(json) => self
                            .link_meta(&edge.from, &edge.to, &edge.edge_type, edge.strength, &json)
                            .map_err(|e| SqlError::InvalidValue(e.to_string()))?,
                        None => self.link(&edge.from, &edge.to, &edge.edge_type, edge.strength),
                    }
                }
                Ok(count)
            }
            sql::CompiledMutation::DeleteEdge(edges) => {
                let count = edges.len();
                for edge in edges {
                    self.unlink(&edge.from, &edge.to, &edge.edge_type);
                }
                Ok(count)
            }
            sql::CompiledMutation::MatchInsert {
                match_steps,
                target,
                edge_type,
                strength,
                props,
            } => {
                let source_slugs: Vec<String> = Set::from_steps(self, match_steps.clone())
                    .collect()
                    .into_iter()
                    .map(|h| h.slug)
                    .collect();
                let count = source_slugs.len();
                for src_slug in source_slugs {
                    match &props {
                        Some(json) => {
                            self.link_meta(&src_slug, &target, &edge_type, strength, json)
                                .map_err(|e| SqlError::InvalidValue(e.to_string()))?;
                        }
                        None => {
                            self.link(&src_slug, &target, &edge_type, strength);
                        }
                    }
                }
                Ok(count)
            }
            sql::CompiledMutation::Update { steps, updates } => {
                let hits: Vec<(String, Value)> = Set::from_steps(self, steps)
                    .collect()
                    .into_iter()
                    .filter_map(|h| {
                        self.nodes
                            .get(&h.slug_hash)
                            .map(|n| (n.slug.clone(), n.payload.clone()))
                    })
                    .collect();
                let count = hits.len();
                for (slug, mut payload) in hits {
                    // Type-check updated fields against schema if registered.
                    if let Some(coll) = payload.get("_collection").and_then(|v| v.as_str()) {
                        if let Some(schema) = self.schemas.get(coll) {
                            if let Some(err) = validate_updates_against_schema(schema, &updates) {
                                return Err(err);
                            }
                        }
                    }
                    let mut vec_updates: Vec<(String, Vec<f32>)> = Vec::new();
                    for (field, value) in &updates {
                        // Array-of-numbers → vector field, not patched into JSON
                        if let Some(floats) = value_as_f32_vec(value) {
                            vec_updates.push((field.clone(), floats));
                        } else if let Value::Object(ref mut map) = payload {
                            map.insert(field.clone(), value.clone());
                        }
                    }
                    let json = serde_json::to_string(&payload)
                        .map_err(|e| SqlError::InvalidValue(e.to_string()))?;
                    self.put(&slug, &json)
                        .map_err(|e| SqlError::InvalidValue(e.to_string()))?;
                    for (field, data) in vec_updates {
                        self.put_vector(&slug, &field, &data)
                            .map_err(|e| SqlError::InvalidValue(e.to_string()))?;
                    }
                }
                Ok(count)
            }
            sql::CompiledMutation::CreateTable { collection, schema } => {
                self.schemas.insert(collection.clone(), schema.clone());
                let schema_json = serde_json::to_string(&schema)
                    .map_err(|e| SqlError::InvalidValue(e.to_string()))?;
                self.wal_write(WalEntry::CreateTable { collection, schema_json });
                Ok(1)
            }
            sql::CompiledMutation::CreateIndex { name: _, collection, method, fields } => {
                self.apply_index(&collection, &method, &fields);
                self.wal_write(WalEntry::CreateIndex {
                    collection,
                    method: method.to_string(),
                    fields,
                });
                Ok(1)
            }
        }
    }

    // ── Internal accessors for the query executor ─────────────────────────────

    pub(crate) fn node_data(&self, hash: u64) -> Option<&NodeData> {
        self.nodes.get(&hash)
    }

    pub(crate) fn all_hashes(&self) -> Vec<u64> {
        self.nodes.keys().copied().collect()
    }

    pub(crate) fn fwd_edges(&self, hash: u64) -> Option<&Vec<EdgeEntry>> {
        self.adj_fwd.get(&hash)
    }

    pub(crate) fn rev_edges(&self, hash: u64) -> Option<&Vec<EdgeEntry>> {
        self.adj_rev.get(&hash)
    }

    pub(crate) fn resolve_edge_type(&self, hash: u64) -> Option<String> {
        self.edge_type_names.get(&hash).cloned()
    }

    pub(crate) fn collection_members(&self, hash: u64) -> Option<&Vec<u64>> {
        self.collections.get(&hash)
    }

    pub(crate) fn spatial_grid(&self) -> Option<&geo::SpatialGrid> {
        self.spatial_grid.as_ref()
    }

    // ── Spatial index ─────────────────────────────────────────────────────────

    /// Build (or rebuild) the spatial grid index from all current nodes.
    ///
    /// Call this after bulk inserts on in-memory databases to enable
    /// grid-accelerated spatial queries. For persistent databases opened
    /// with [`CoreDB::open`], the grid is built automatically.
    pub fn build_spatial_index(&mut self) {
        self.rebuild_spatial_grid();
    }

    fn rebuild_spatial_grid(&mut self) {
        let items = self.nodes.iter().filter_map(|(&hash, node)| {
            geo::extract_spatial_meta(&node.payload).map(|m| (hash, m))
        });
        self.spatial_grid = Some(geo::SpatialGrid::build(items));
    }

    // ── Text index ─────────────────────────────────────────────────────────────

    /// Build (or rebuild) GiST trigram indexes for all text fields.
    ///
    /// Automatically detects all string fields across all nodes and builds
    /// a GiST bitmap signature index for each. This is cheap enough to always
    /// have enabled (~12MB/1M docs).
    ///
    /// Call this after bulk inserts to enable ILIKE acceleration.
    /// For persistent databases opened with [`CoreDB::open`], call this manually
    /// after bulk loading data.
    pub fn build_text_indexes(&mut self) {
        self.rebuild_text_indexes();
    }

    fn rebuild_text_indexes(&mut self) {
        let mut field_values: HashMap<String, Vec<(u64, String)>> = HashMap::new();

        for (&hash, node) in &self.nodes {
            extract_string_fields(&node.payload, "", &mut field_values, hash);
        }

        // Build into a fresh map first, then replace atomically.
        // This ensures that if GiSTIndex::build panics, self.text_indexes
        // retains its previous state and is never left half-cleared.
        let mut new_indexes = HashMap::new();
        for (field, values) in field_values {
            let values_ref: Vec<(u64, &str)> =
                values.iter().map(|(id, s)| (*id, s.as_str())).collect();
            let index = GiSTIndex::build(values_ref.into_iter(), &field);
            new_indexes.insert(field, index);
        }
        self.text_indexes = new_indexes;
    }

    /// Get ILIKE candidate doc IDs from text index for a field.
    ///
    /// Returns `Some(candidates)` if an index exists for this field,
    /// or `None` if no index is available.
    ///
    /// Candidates are unverified — use [`Self::ilike_verify`] to confirm matches.
    pub fn text_index_candidates(&self, field: &str, pattern: &str) -> Option<Vec<u64>> {
        self.text_indexes
            .get(field)
            .map(|idx| idx.ilike_candidates(pattern, None))
    }

    pub fn text_index_candidates_with_limit(
        &self,
        field: &str,
        pattern: &str,
        limit: Option<usize>,
    ) -> Option<Vec<u64>> {
        self.text_indexes
            .get(field)
            .map(|idx| idx.ilike_candidates(pattern, limit))
    }

    /// Get ILIKE candidate doc IDs from text index for a field.
    ///
    /// Returns candidates that MAY match (GiST is lossy — verification needed).
    /// Use [`Self::ilike_verify`] to confirm actual matches.
    pub fn ilike_candidates(&self, field: &str, pattern: &str) -> Vec<u64> {
        self.text_indexes
            .get(field)
            .map(|idx| idx.ilike_candidates(pattern, None))
            .unwrap_or_default()
    }

    /// Verify ILIKE candidates against actual stored text.
    pub fn ilike_verify(&self, field: &str, pattern: &str, candidates: &[u64]) -> Vec<u64> {
        use text_index::query::ilike_matches;
        let mut results = Vec::new();
        for &hash in candidates {
            if let Some(node_data) = self.node_data(hash) {
                if let Some(text) = node_data.payload.get(field).and_then(|v| v.as_str()) {
                    if ilike_matches(text, pattern) {
                        results.push(hash);
                    }
                }
            }
        }
        results
    }

    /// Execute an ILIKE query using the text index.
    ///
    /// This is a convenience method that:
    /// 1. Looks up candidates from the GiST index
    /// 2. Verifies each candidate against the actual ILIKE pattern
    /// 3. Returns the verified matching doc IDs
    ///
    /// If no text index exists for the field, returns an empty result.
    ///
    /// # Arguments
    /// * `field` - The field name to search (e.g., "name" or "description")
    /// * `pattern` - ILIKE pattern (e.g., "%Alpha%" or "%foo_bar%")
    /// * `limit` - Maximum results to return (None for all)
    ///
    /// # Example
    /// ```
    /// # use sekejap::CoreDB;
    /// let mut db = CoreDB::new();
    /// db.put("alice", r#"{"name":"Alice Smith","_collection":"users"}"#).unwrap();
    /// db.put("bob",   r#"{"name":"Bob Jones",  "_collection":"users"}"#).unwrap();
    /// db.build_text_indexes();
    ///
    /// let matches = db.ilike("name", "%Alice%", None);
    /// assert_eq!(matches.len(), 1);
    /// ```
    pub fn ilike(&self, field: &str, pattern: &str, limit: Option<usize>) -> Vec<u64> {
        // Prefer GIN (exact) over GiST (lossy) when available
        if let Some(results) = self.gin_indexes.get(field) {
            let mut r = results.ilike(pattern, None);
            if let Some(l) = limit {
                r.truncate(l);
            }
            return r;
        }
        // Fall back to GiST + verification
        let candidates = self.ilike_candidates(field, pattern);
        let verified = self.ilike_verify(field, pattern, &candidates);
        match limit {
            Some(l) => verified.into_iter().take(l).collect(),
            None => verified,
        }
    }

    /// Build a GIN trigram index for a specific field.
    ///
    /// GIN provides exact trigram matching (no verification step needed) but
    /// uses more memory than GiST (~100MB/1M docs vs ~12MB/1M docs).
    ///
    /// Use this when you need exact ILIKE matching without the false-positive
    /// verification step of GiST.
    ///
    /// # Arguments
    /// * `field` - The field name to index (e.g., "name")
    ///
    /// # Example
    /// ```
    /// # use sekejap::CoreDB;
    /// let mut db = CoreDB::new();
    /// db.put("a1", r#"{"name":"Alpha","_collection":"items"}"#).unwrap();
    /// db.put("a2", r#"{"name":"Beta","_collection":"items"}"#).unwrap();
    /// db.build_gin_index("name");
    ///
    /// // GIN is exact — no verification step needed
    /// let matches = db.gin_ilike("name", "%Alpha%", None);
    /// assert_eq!(matches.len(), 1);
    /// ```
    pub fn build_gin_index(&mut self, field: &str) {
        let values: Vec<(u64, &str)> = self
            .nodes
            .iter()
            .filter_map(|(&hash, node)| {
                node.payload
                    .get(field)
                    .and_then(|v| v.as_str())
                    .map(|s| (hash, s))
            })
            .collect();

        if !values.is_empty() {
            let index = GINIndex::build(values.into_iter(), field);
            self.gin_indexes.insert(field.to_string(), index);
        }
    }

    /// Execute ILIKE using GIN index (exact — no verification needed).
    ///
    /// Returns exact matching doc IDs directly from the GIN index.
    /// If no GIN index exists for the field, returns an empty result.
    ///
    /// # Arguments
    /// * `field` - The field name to search
    /// * `pattern` - ILIKE pattern (e.g., "%Alpha%")
    /// * `limit` - Maximum results (None for all)
    pub fn gin_ilike(&self, field: &str, pattern: &str, limit: Option<usize>) -> Vec<u64> {
        self.gin_indexes
            .get(field)
            .map(|idx| idx.ilike(pattern, limit))
            .unwrap_or_default()
    }

    // ── BM25 full-text search ───────────────────────────────────────────────

    /// Build a BM25 index for a specific text field.
    ///
    /// BM25 provides relevance-ranked results (like Google search) instead of
    /// exact substring matching. The index is compressed with varint encoding.
    ///
    /// # Arguments
    /// * `field` - The text field to index (e.g., "name", "description")
    ///
    /// # Storage
    /// Approximately 100-150 MB per 1M documents for a typical text field.
    ///
    /// # Example
    /// ```
    /// # use sekejap::CoreDB;
    /// let mut db = CoreDB::new();
    /// db.put("a1", r#"{"name":"Rust Tutorial for Beginners","_collection":"tutorials"}"#).unwrap();
    /// db.put("a2", r#"{"name":"Python Tutorial for Beginners","_collection":"tutorials"}"#).unwrap();
    /// db.build_bm25_index("name");
    ///
    /// let results = db.bm25_search("name", "rust tutorial", 10);
    /// assert!(results.len() >= 1);
    /// // The top result should be the doc that best matches all query terms.
    /// ```
    pub fn build_bm25_index(&mut self, field: &str) {
        let values: Vec<(u64, &str)> = self
            .nodes
            .iter()
            .filter_map(|(&hash, node)| {
                node.payload
                    .get(field)
                    .and_then(|v| v.as_str())
                    .map(|s| (hash, s))
            })
            .collect();

        if !values.is_empty() {
            let index = bm25::Bm25Index::build(field, values.into_iter());
            self.bm25_indexes.insert(field.to_string(), index);
        }
    }

    /// Search using BM25 index and return ranked results.
    ///
    /// Returns `(doc_id, score)` pairs sorted by relevance score (highest first).
    /// Requires the field to have been indexed with [`build_bm25_index`](Self::build_bm25_index).
    ///
    /// # Arguments
    /// * `field` - The indexed field to search
    /// * `query` - Search query (will be tokenized and matched)
    /// * `top_k` - Maximum number of results to return
    ///
    /// # Example
    /// ```
    /// # use sekejap::CoreDB;
    /// let mut db = CoreDB::new();
    /// db.put("a1", r#"{"name":"Rust Tutorial for Beginners","_collection":"tutorials"}"#).unwrap();
    /// db.put("a2", r#"{"name":"Python Tutorial for Beginners","_collection":"tutorials"}"#).unwrap();
    /// db.build_bm25_index("name");
    ///
    /// let results = db.bm25_search("name", "rust tutorial", 10);
    /// // results[0] is the most relevant doc for "rust tutorial"
    /// ```
    pub fn bm25_search(&self, field: &str, query: &str, top_k: usize) -> Vec<(u64, f64)> {
        self.bm25_indexes
            .get(field)
            .map(|idx| {
                idx.search(query, top_k)
                    .into_iter()
                    .map(|hit| (hit.doc_id, hit.score))
                    .collect()
            })
            .unwrap_or_default()
    }

    // ── Vector storage ─────────────────────────────────────────────────────────

    /// Store a vector for a node under a named field.
    ///
    /// The vector is indexed by `sk_hash(slug)` and persisted in the WAL
    /// when the database is opened from disk.
    ///
    /// Returns the slug hash on success.
    pub fn put_vector(&mut self, slug: &str, field: &str, data: &[f32]) -> Result<u64, serde_json::Error> {
        let hash = sk_hash(slug);
        self.vectors
            .entry(field.to_string())
            .or_default()
            .insert(hash, data.to_vec());
        self.wal_write(WalEntry::PutVector {
            slug: slug.to_string(),
            field: field.to_string(),
            data: data.to_vec(),
        });
        Ok(hash)
    }

    /// Retrieve the stored vector for a node under a named field.
    ///
    /// Returns `None` if the node has no vector for that field.
    pub fn get_vector(&self, slug: &str, field: &str) -> Option<&[f32]> {
        let hash = sk_hash(slug);
        self.vectors.get(field)?.get(&hash).map(|v| v.as_slice())
    }

    /// Access all vectors for a given field (used by the query executor).
    pub(crate) fn vector_field(&self, field: &str) -> Option<&HashMap<u64, Vec<f32>>> {
        self.vectors.get(field)
    }

    /// Access the HNSW index for a field (used by the query executor).
    pub(crate) fn hnsw_index(&self, field: &str) -> Option<&vector::HnswGraph> {
        self.hnsw_indexes.get(field)
    }

    // ── Btree field index ──────────────────────────────────────────────────────

    /// Build (or rebuild) a btree field index for a specific collection and field.
    ///
    /// Scans all collection members and builds an ordered BTreeMap from field
    /// value → `[node_hash, …]`. Called automatically by
    /// `CREATE INDEX ON coll(field) USING btree`.
    ///
    /// Incrementally maintained by every subsequent `put()` / `remove()`.
    pub fn build_field_index(&mut self, collection: &str, field: &str) {
        let coll_hash = sk_hash(collection);
        let members: Vec<u64> = self.collections.get(&coll_hash).cloned().unwrap_or_default();
        let mut btree: BTreeMap<FieldKey, Vec<u64>> = BTreeMap::new();
        for hash in members {
            if let Some(node) = self.nodes.get(&hash) {
                if let Some(fk) =
                    FieldKey::from_json(node.payload.get(field).unwrap_or(&Value::Null))
                {
                    btree.entry(fk).or_default().push(hash);
                }
            }
        }
        self.field_indexes.insert((coll_hash, field.to_string()), btree);
    }

    /// Try to seed the candidate list for a `Collection` step from a btree index.
    ///
    /// Looks ahead in `remaining` for the first filter step that has a btree
    /// index on this collection. Returns a pre-filtered `Vec<u64>` on a hit so
    /// the Collection step can skip the full member scan, or `None` to fall back.
    pub(crate) fn btree_seed(&self, coll_hash: u64, remaining: &[Step]) -> Option<Vec<u64>> {
        use std::ops::Bound;
        for step in remaining {
            match step {
                Step::WhereEq(field, value) => {
                    if let Some(idx) = self.field_indexes.get(&(coll_hash, field.clone())) {
                        if let Some(fk) = FieldKey::from_json(value) {
                            return Some(idx.get(&fk).cloned().unwrap_or_default());
                        }
                    }
                }
                Step::WhereGt(field, t) => {
                    if let Some(idx) = self.field_indexes.get(&(coll_hash, field.clone())) {
                        let fk = FieldKey::from_f64(*t);
                        return Some(
                            idx.range((Bound::Excluded(fk), Bound::Unbounded))
                                .flat_map(|(_, ids)| ids.iter().copied())
                                .collect(),
                        );
                    }
                }
                Step::WhereLt(field, t) => {
                    if let Some(idx) = self.field_indexes.get(&(coll_hash, field.clone())) {
                        let fk = FieldKey::from_f64(*t);
                        return Some(
                            idx.range(..fk)
                                .flat_map(|(_, ids)| ids.iter().copied())
                                .collect(),
                        );
                    }
                }
                Step::WhereGte(field, t) => {
                    if let Some(idx) = self.field_indexes.get(&(coll_hash, field.clone())) {
                        let fk = FieldKey::from_f64(*t);
                        return Some(
                            idx.range(fk..)
                                .flat_map(|(_, ids)| ids.iter().copied())
                                .collect(),
                        );
                    }
                }
                Step::WhereLte(field, t) => {
                    if let Some(idx) = self.field_indexes.get(&(coll_hash, field.clone())) {
                        let fk = FieldKey::from_f64(*t);
                        return Some(
                            idx.range(..=fk)
                                .flat_map(|(_, ids)| ids.iter().copied())
                                .collect(),
                        );
                    }
                }
                Step::WhereBetween(field, lo, hi) => {
                    if let Some(idx) = self.field_indexes.get(&(coll_hash, field.clone())) {
                        let fk_lo = FieldKey::from_f64(*lo);
                        let fk_hi = FieldKey::from_f64(*hi);
                        return Some(
                            idx.range(fk_lo..=fk_hi)
                                .flat_map(|(_, ids)| ids.iter().copied())
                                .collect(),
                        );
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Try to seed the candidate list for a `Collection` step using an ORDER BY index scan.
    ///
    /// Applies when there are **no filter steps** between `Collection` and `Sort`, the
    /// sort is single-column, and a btree index exists on that field. The candidates are
    /// returned pre-sorted; the subsequent `Sort` step then runs on a small set (k items
    /// for `LIMIT k`) rather than the full collection.
    pub(crate) fn btree_sorted_seed_from_steps(
        &self,
        coll_hash: u64,
        remaining: &[Step],
    ) -> Option<Vec<u64>> {
        // Find the first Sort step
        let sort_pos = remaining.iter().position(|s| matches!(s, Step::Sort(_)))?;

        // Only valid when there are no filter/traversal steps before Sort
        let pre_sort = &remaining[..sort_pos];
        if pre_sort.iter().any(|s| is_filter_or_traversal(s)) {
            return None;
        }

        let Step::Sort(cols) = &remaining[sort_pos] else {
            return None;
        };
        // Only single-column sort qualifies (multi-column can't use a single btree)
        if cols.len() != 1 {
            return None;
        }
        let (field, asc) = &cols[0];

        let idx = self.field_indexes.get(&(coll_hash, field.clone()))?;

        // Look ahead for a Take limit — enables O(k) extraction instead of O(N)
        let take_n = remaining[sort_pos + 1..]
            .iter()
            .find_map(|s| if let Step::Take(n) = s { Some(*n) } else { None });

        let result: Vec<u64> = if *asc {
            idx.values().flat_map(|ids| ids.iter().copied()).collect()
        } else {
            idx.values().rev().flat_map(|ids| ids.iter().copied()).collect()
        };

        Some(match take_n {
            Some(n) => result.into_iter().take(n).collect(),
            None => result,
        })
    }

    /// Build (or rebuild) an HNSW approximate-NN index for a vector field.
    ///
    /// The index is constructed entirely in a local value and only stored after
    /// successful completion — the main store (`self.vectors`, `self.nodes`)
    /// is never modified by this call.
    ///
    /// # Parameters
    /// - `field`: the vector field name (must have been populated via `put_vector`)
    /// - `m`: max connections per node (8–32; 16 is a good default)
    /// - `ef_construction`: beam width during build (100–400; 200 is a good default)
    ///
    /// Returns `Err` if `field` has no stored vectors.
    pub fn build_hnsw_index(
        &mut self,
        field: &str,
        m: usize,
        ef_construction: usize,
    ) -> Result<(), String> {
        let field_vecs = self
            .vectors
            .get(field)
            .ok_or_else(|| format!("no vectors stored for field '{field}'"))?;

        // Build entirely into a local — zero writes to self until this line.
        let graph =
            vector::HnswGraph::build::<CosineDistance>(field_vecs, m, ef_construction);

        // Atomic replace: old index (if any) is dropped here.
        self.hnsw_indexes.insert(field.to_string(), graph);
        Ok(())
    }

    // ── CREATE INDEX executor ──────────────────────────────────────────────────

    /// Build the in-memory index for a `CREATE INDEX` statement and update
    /// the collection schema's index hints.
    fn apply_index(&mut self, collection: &str, method: &sql::IndexMethod, fields: &[String]) {
        use sql::IndexMethod;

        // Update schema index hints so introspection always reflects reality.
        let schema = self.schemas
            .entry(collection.to_string())
            .or_insert_with(|| sql::TableSchema {
                collection: collection.to_string(),
                fields: vec![],
                indexes: sql::IndexHint::default(),
            });
        for field in fields {
            let list = match method {
                IndexMethod::Bm25    => &mut schema.indexes.bm25,
                IndexMethod::Hnsw    => &mut schema.indexes.vector,
                IndexMethod::Spatial => &mut schema.indexes.spatial,
                IndexMethod::Gin | IndexMethod::Gist => &mut schema.indexes.fulltext,
                IndexMethod::Btree   => &mut schema.indexes.range,
                IndexMethod::Hash    => &mut schema.indexes.hash,
            };
            if !list.contains(field) {
                list.push(field.clone());
            }
        }

        // Build the actual in-memory index structure.
        match method {
            IndexMethod::Bm25 => {
                for field in fields {
                    self.build_bm25_index(field);
                }
            }
            IndexMethod::Gin => {
                for field in fields {
                    self.build_gin_index(field);
                }
            }
            IndexMethod::Gist => {
                self.rebuild_text_indexes();
            }
            IndexMethod::Spatial => {
                self.rebuild_spatial_grid();
            }
            IndexMethod::Hnsw => {
                for field in fields {
                    // Silently skip if no vectors exist yet — the user can call
                    // build_hnsw_index() explicitly after loading vectors.
                    let _ = self.build_hnsw_index(field, 16, 200);
                }
            }
            IndexMethod::Btree => {
                for field in fields {
                    self.build_field_index(collection, field);
                }
            }
            // Hash — hint stored; no in-memory structure needed
            IndexMethod::Hash => {}
        }
    }
}

// ── Schema validation ─────────────────────────────────────────────────────────

/// Returns true when `v` is compatible with the declared field type.
/// NULL is always accepted; the check is intentionally lenient (e.g. any
/// JSON number passes for both Integer and Real).
fn field_type_matches(ty: &sql::FieldType, v: &Value) -> bool {
    if v.is_null() {
        return true;
    }
    match ty {
        sql::FieldType::Text        => v.is_string(),
        sql::FieldType::Integer     => v.is_number(),
        sql::FieldType::Real        => v.is_number(),
        sql::FieldType::Timestamptz => v.is_string() || v.is_number(),
        sql::FieldType::Geo         => v.is_object() || v.is_array(),
        sql::FieldType::Vector      => v.is_array(),
        sql::FieldType::Json        => true,
    }
}

/// Validate all fields in `payload` that have a matching declaration in `schema`.
/// Unknown/missing fields are silently ignored (lenient / open-world).
/// Returns `Some(SqlError)` on the first type mismatch; `None` when valid.
fn validate_payload_against_schema(schema: &sql::TableSchema, payload: &Value) -> Option<SqlError> {
    let obj = payload.as_object()?;
    for field_def in &schema.fields {
        if let Some(v) = obj.get(&field_def.name) {
            if !field_type_matches(&field_def.ty, v) {
                return Some(SqlError::InvalidValue(format!(
                    "field '{}': expected {:?}, got {}",
                    field_def.name,
                    field_def.ty,
                    v,
                )));
            }
        }
    }
    None
}

/// Validate the (field, value) pairs being written by an UPDATE statement.
/// Only fields declared in the schema are checked; unknown fields are ignored.
fn validate_updates_against_schema(
    schema: &sql::TableSchema,
    updates: &[(String, Value)],
) -> Option<SqlError> {
    for (field, value) in updates {
        if let Some(field_def) = schema.fields.iter().find(|f| &f.name == field) {
            if !field_type_matches(&field_def.ty, value) {
                return Some(SqlError::InvalidValue(format!(
                    "field '{}': expected {:?}, got {}",
                    field_def.name,
                    field_def.ty,
                    value,
                )));
            }
        }
    }
    None
}

/// Returns true for any step that narrows, reorders, or re-sources the candidate list.
/// Used to detect whether a btree ORDER BY index scan is safe for `Collection → Sort`.
fn is_filter_or_traversal(s: &Step) -> bool {
    matches!(
        s,
        Step::WhereEq(..)
            | Step::WhereNeq(..)
            | Step::WhereGt(..)
            | Step::WhereLt(..)
            | Step::WhereGte(..)
            | Step::WhereLte(..)
            | Step::WhereBetween(..)
            | Step::WhereIn(..)
            | Step::Like(..)
            | Step::WhereNot(..)
            | Step::WhereOr(..)
            | Step::WhereIsNull(..)
            | Step::Forward(..)
            | Step::Backward(..)
            | Step::Hops(..)
            | Step::HopsTyped { .. }
            | Step::MinStrength(..)
            | Step::Leaves
            | Step::Roots
            | Step::StDWithin(..)
            | Step::StContainsPoint(..)
            | Step::StWithin(..)
            | Step::StContains(..)
            | Step::StIntersects(..)
            | Step::StDistance(..)
            | Step::StLength(..)
            | Step::StArea(..)
            | Step::VectorNear { .. }
            | Step::Bm25Filter(..)
            | Step::Intersect(..)
            | Step::Union(..)
            | Step::Subtract(..)
    )
}

// ── Transaction ───────────────────────────────────────────────────────────────

/// A buffered write transaction. Writes are visible **only after [`commit`](Transaction::commit)**.
///
/// Obtained from [`CoreDB::begin`]. Drop to roll back silently.
///
/// # Example
/// ```
/// # use sekejap::CoreDB;
/// let mut db = CoreDB::new();
/// let mut txn = db.begin();
/// txn.put("users/alice", r#"{"_collection":"users","name":"Alice"}"#).unwrap();
/// txn.put("users/bob",   r#"{"_collection":"users","name":"Bob"}"#).unwrap();
/// txn.commit().unwrap();
/// assert_eq!(db.collection("users").count(), 2);
/// ```
pub struct Transaction<'db> {
    db: &'db mut CoreDB,
    ops: Vec<TxnOp>,
}

enum TxnOp {
    Put(String, String),
    Remove(String),
    Link(String, String, String, f32),
    LinkMeta(String, String, String, f32, String),
    Unlink(String, String, String),
    PutVector(String, String, Vec<f32>),
}

impl CoreDB {
    /// Begin a new transaction. Writes are buffered until [`Transaction::commit`].
    ///
    /// Dropping the returned `Transaction` without calling `commit` is a silent rollback.
    pub fn begin(&mut self) -> Transaction<'_> {
        Transaction { db: self, ops: Vec::new() }
    }
}

impl<'db> Transaction<'db> {
    /// Queue a node insert/update. Validates JSON immediately; returns error on bad JSON.
    pub fn put(&mut self, slug: &str, payload_json: &str) -> Result<(), serde_json::Error> {
        serde_json::from_str::<Value>(payload_json)?;
        self.ops.push(TxnOp::Put(slug.to_string(), payload_json.to_string()));
        Ok(())
    }

    /// Queue a node removal.
    pub fn remove(&mut self, slug: &str) {
        self.ops.push(TxnOp::Remove(slug.to_string()));
    }

    /// Queue an edge creation.
    pub fn link(&mut self, from: &str, to: &str, edge_type: &str, strength: f32) {
        self.ops.push(TxnOp::Link(
            from.to_string(), to.to_string(), edge_type.to_string(), strength,
        ));
    }

    /// Queue an edge creation with JSON metadata. Validates JSON immediately.
    pub fn link_meta(
        &mut self,
        from: &str,
        to: &str,
        edge_type: &str,
        strength: f32,
        meta_json: &str,
    ) -> Result<(), serde_json::Error> {
        serde_json::from_str::<Value>(meta_json)?;
        self.ops.push(TxnOp::LinkMeta(
            from.to_string(), to.to_string(), edge_type.to_string(), strength, meta_json.to_string(),
        ));
        Ok(())
    }

    /// Queue an edge removal.
    pub fn unlink(&mut self, from: &str, to: &str, edge_type: &str) {
        self.ops.push(TxnOp::Unlink(
            from.to_string(), to.to_string(), edge_type.to_string(),
        ));
    }

    /// Queue a vector store.
    pub fn put_vector(&mut self, slug: &str, field: &str, data: Vec<f32>) {
        self.ops.push(TxnOp::PutVector(slug.to_string(), field.to_string(), data));
    }

    /// Commit all queued writes atomically: apply to in-memory store then flush to WAL.
    ///
    /// Returns the number of operations committed.
    ///
    /// # Errors
    /// Only fails if a queued `Put` payload is invalid JSON (shouldn't happen if
    /// the `put()` helper was used, since it validates eagerly).
    pub fn commit(self) -> Result<usize, serde_json::Error> {
        let count = self.ops.len();
        // Apply all ops to in-memory store in order
        for op in &self.ops {
            match op {
                TxnOp::Put(slug, json) => { self.db.put_raw(slug, json)?; }
                TxnOp::Remove(slug) => { self.db.remove_raw(slug); }
                TxnOp::Link(from, to, et, strength) => {
                    self.db.link_raw(from, to, et, *strength);
                }
                TxnOp::LinkMeta(from, to, et, strength, meta) => {
                    self.db.link_meta_raw(from, to, et, *strength, meta)?;
                }
                TxnOp::Unlink(from, to, et) => { self.db.unlink_raw(from, to, et); }
                TxnOp::PutVector(slug, field, data) => {
                    let hash = sk_hash(slug);
                    self.db.vectors.entry(field.clone()).or_default().insert(hash, data.clone());
                }
            }
        }
        // Write all ops to WAL in one sequential batch
        for op in self.ops {
            match op {
                TxnOp::Put(slug, payload) => {
                    self.db.wal_write(WalEntry::Put { slug, payload });
                }
                TxnOp::Remove(slug) => {
                    self.db.wal_write(WalEntry::Remove { slug });
                }
                TxnOp::Link(from, to, edge_type, strength) => {
                    self.db.wal_write(WalEntry::Link { from, to, edge_type, strength });
                }
                TxnOp::LinkMeta(from, to, edge_type, strength, meta) => {
                    self.db.wal_write(WalEntry::LinkMeta { from, to, edge_type, strength, meta });
                }
                TxnOp::Unlink(from, to, edge_type) => {
                    self.db.wal_write(WalEntry::Unlink { from, to, edge_type });
                }
                TxnOp::PutVector(slug, field, data) => {
                    self.db.wal_write(WalEntry::PutVector { slug, field, data });
                }
            }
        }
        Ok(count)
    }

    /// Discard all queued writes. Equivalent to dropping the `Transaction`.
    pub fn rollback(self) {
        // Nothing to do — ops were never applied.
    }
}

/// Extract all string fields from a JSON value recursively.
fn extract_string_fields(
    value: &Value,
    prefix: &str,
    out: &mut HashMap<String, Vec<(u64, String)>>,
    doc_id: u64,
) {
    match value {
        Value::String(s) => {
            let key = if prefix.is_empty() {
                "<root>".to_string()
            } else {
                prefix.to_string()
            };
            out.entry(key).or_default().push((doc_id, s.clone()));
        }
        Value::Object(map) => {
            for (k, v) in map {
                let new_prefix = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{}.{}", prefix, k)
                };
                extract_string_fields(v, &new_prefix, out, doc_id);
            }
        }
        Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                let new_prefix = format!("{}[{}]", prefix, i);
                extract_string_fields(v, &new_prefix, out, doc_id);
            }
        }
        _ => {}
    }
}

// ── Spatial helpers ───────────────────────────────────────────────────────────

impl CoreDB {
    /// Extract centroid (lat, lon) from a node's geometry field.
    ///
    /// Returns `None` if the node doesn't exist or has no valid geometry.
    ///
    /// # Arguments
    /// * `slug` - The node slug (e.g., "places/pt1")
    ///
    /// # Example
    /// ```
    /// # use sekejap::CoreDB;
    /// let mut db = CoreDB::new();
    /// db.put("zones/z1", r#"{
    ///     "name": "Zone 1",
    ///     "geometry": {"type": "Polygon", "coordinates": [[[144.95,-37.80],[144.98,-37.80],[144.98,-37.83],[144.95,-37.83],[144.95,-37.80]]]}
    /// }"#).unwrap();
    /// if let Some((lat, lon)) = db.centroid("zones/z1") {
    ///     println!("Centroid: ({lat}, {lon})");
    /// }
    /// ```
    pub fn centroid(&self, slug: &str) -> Option<(f64, f64)> {
        let hash = *self.slug_map.get(slug)?;
        let node = self.nodes.get(&hash)?;
        geo::extract_centroid(&node.payload)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// If a `serde_json::Value::Array` contains only numbers, return them as `Vec<f32>`.
/// Used by the SQL executor to detect vector literals in INSERT/UPDATE values.
fn value_as_f32_vec(v: &Value) -> Option<Vec<f32>> {
    let arr = v.as_array()?;
    if arr.is_empty() {
        return None;
    }
    arr.iter()
        .map(|x| x.as_f64().map(|f| f as f32))
        .collect()
}

// ── Snapshot format ───────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    nodes: Vec<SnapNode>,
    edges: Vec<SnapEdge>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schemas: Option<Vec<sql::TableSchema>>,
    /// Vector data is stored alongside node data so compact() + reload loses nothing.
    /// `None` on old snapshots is safe — WAL replay fills the gap.
    #[serde(skip_serializing_if = "Option::is_none")]
    vectors: Option<Vec<SnapVector>>,
    /// HNSW graphs — persisted so they don't need rebuilding on startup.
    #[serde(skip_serializing_if = "Option::is_none")]
    hnsw_indexes: Option<Vec<SnapHnsw>>,
}

#[derive(Serialize, Deserialize)]
struct SnapHnsw {
    field: String,
    graph: vector::HnswGraph,
}

#[derive(Serialize, Deserialize)]
struct SnapNode {
    slug: String,
    payload: Value,
}

#[derive(Serialize, Deserialize)]
struct SnapEdge {
    from: String,
    to: String,
    edge_type: String,
    strength: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    meta: Option<Value>,
}

#[derive(Serialize, Deserialize)]
struct SnapVector {
    slug: String,
    field: String,
    data: Vec<f32>,
}

#[cfg(test)]
mod hybrid_query_tests {
    use super::*;

    #[test]
    fn test_hybrid_graph_spatial_range_query() {
        let mut db = CoreDB::new();

        // 100 shops with geometry
        for i in 0..100 {
            db.put(
                &format!("places/shop{}", i),
                &serde_json::json!({
                    "_collection": "places",
                    "_key": format!("shop{}", i),
                    "name": format!("Shop {i}"),
                    "category": if i % 3 == 0 { "electronics" } else { "food" },
                    "price": 10.0 + (i as f64 * 1.5),
                    "geometry": {
                        "type": "Point",
                        "coordinates": [144.9 + (i as f64 * 0.001), -37.8 + (i as f64 * 0.001)]
                    }
                })
                .to_string(),
            )
            .unwrap();
        }

        // Graph edges: shop0 -> shop1 -> shop2 ...
        for i in 0..99 {
            db.link(
                &format!("places/shop{}", i),
                &format!("places/shop{}", i + 1),
                "nearby",
                1.0,
            );
        }

        // Build indexes
        db.build_spatial_index();

        // Graph traversal + spatial + range filter + sort
        let results = db
            .one("places/shop0")
            .forward("nearby")
            .st_dwithin(-37.81, 144.95, 1.0)
            .where_gt("price", 50.0)
            .sort("price", true)
            .take(10)
            .collect();

        assert!(results.len() <= 10);
        println!("Hybrid query returned {} results", results.len());
    }

    #[test]
    fn test_hybrid_with_scalar_functions() {
        let mut db = CoreDB::new();

        for i in 0..10 {
            db.put(
                &format!("users/user{}", i),
                &serde_json::json!({
                    "_collection": "users",
                    "_key": format!("user{}", i),
                    "name": format!("  User {i}  "),
                    "email": format!("user{}@example.com", i),
                    "created_at": format!("2024-01-{:02}T12:00:00Z", i + 1)
                })
                .to_string(),
            )
            .unwrap();
        }

        let results = db
            .query(
                "SELECT LENGTH(name), LOWER(email), YEAR(created_at), MONTH(created_at)
             FROM users 
             ORDER BY LENGTH(name) DESC",
            )
            .unwrap()
            .collect();

        assert!(!results.is_empty());
        println!("Scalar function query returned {} results", results.len());
    }

    #[test]
    fn test_auto_timestamps() {
        let mut db = CoreDB::new();

        // Insert without timestamps
        db.put(
            "users/alice",
            r#"{"name": "Alice", "_collection": "users"}"#,
        )
        .unwrap();

        // Verify timestamps were auto-added
        let hash = *db.slug_map.get("users/alice").unwrap();
        let node = db.node_data(hash).unwrap();
        let payload = &node.payload;

        // _created_unix and _updated_unix should exist
        assert!(
            payload.get("_created_unix").is_some(),
            "should have _created_unix"
        );
        assert!(
            payload.get("_updated_unix").is_some(),
            "should have _updated_unix"
        );

        // Values should be integers (unix timestamp millis)
        let created = payload.get("_created_unix").unwrap().as_i64().unwrap();
        let updated = payload.get("_updated_unix").unwrap().as_i64().unwrap();
        assert!(created > 0, "_created_unix should be positive");
        assert!(updated > 0, "_updated_unix should be positive");
        assert_eq!(created, updated, "created == updated on insert");

        // Now update
        std::thread::sleep(std::time::Duration::from_millis(10));
        db.put(
            "users/alice",
            r#"{"name": "Alice Updated", "_collection": "users"}"#,
        )
        .unwrap();

        let hash = *db.slug_map.get("users/alice").unwrap();
        let node = db.node_data(hash).unwrap();
        let payload = &node.payload;

        // _created_unix should be preserved, _updated_unix should change
        let created2 = payload.get("_created_unix").unwrap().as_i64().unwrap();
        let updated2 = payload.get("_updated_unix").unwrap().as_i64().unwrap();
        assert_eq!(
            created, created2,
            "_created_unix should be preserved on update"
        );
        assert!(updated2 > updated, "_updated_unix should change on update");

        println!("Auto-timestamps: created={}, updated={}", created, updated);
    }

    #[test]
    fn test_match_insert() {
        let mut db = CoreDB::new();

        // Create people
        for i in 0..10 {
            db.put(
                &format!("people/p{}", i),
                &serde_json::json!({
                    "_collection": "people",
                    "_key": format!("p{}", i),
                    "name": format!("Person {}", i),
                    "grade": 50 + i * 5  // 50, 55, 60, 65, 70, 75, 80, 85, 90, 95
                })
                .to_string(),
            )
            .unwrap();
        }

        // Create classroom
        db.put(
            "classroom/A",
            r#"{"_collection": "classroom", "_key": "A", "name": "Classroom A"}"#,
        )
        .unwrap();

        // MATCH INSERT: link people with grade < 80 to classroom/A
        let count = db
            .execute("MATCH (p:people) WHERE p.grade < 80 INSERT (p)-[:member_of]->(classroom/A)")
            .unwrap();

        assert_eq!(count, 6, "Should link 6 people (grade 50-75)");

        println!("MATCH INSERT created {} edges", count);
    }

    #[test]
    fn test_put_get_vector() {
        let mut db = CoreDB::new();
        db.put(
            "articles/a1",
            r#"{"_collection":"articles","_key":"a1","title":"Rust"}"#,
        )
        .unwrap();
        db.put_vector("articles/a1", "embedding", &[0.1, 0.2, 0.3, 0.4])
            .unwrap();

        let v = db.get_vector("articles/a1", "embedding").unwrap();
        assert_eq!(v, &[0.1f32, 0.2, 0.3, 0.4]);

        // Non-existent field returns None
        assert!(db.get_vector("articles/a1", "other_field").is_none());
        // Non-existent slug returns None
        assert!(db.get_vector("articles/missing", "embedding").is_none());
    }

    #[test]
    fn test_vector_near_api() {
        let mut db = CoreDB::new();
        db.put(
            "articles/a1",
            r#"{"_collection":"articles","_key":"a1","title":"Rust"}"#,
        )
        .unwrap();
        db.put(
            "articles/a2",
            r#"{"_collection":"articles","_key":"a2","title":"Python"}"#,
        )
        .unwrap();
        db.put(
            "articles/a3",
            r#"{"_collection":"articles","_key":"a3","title":"Go"}"#,
        )
        .unwrap();

        db.put_vector("articles/a1", "embedding", &[1.0, 0.0, 0.0, 0.0])
            .unwrap();
        db.put_vector("articles/a2", "embedding", &[0.0, 1.0, 0.0, 0.0])
            .unwrap();
        db.put_vector("articles/a3", "embedding", &[0.0, 0.0, 1.0, 0.0])
            .unwrap();

        // Query closest to a1's embedding
        let results = db
            .collection("articles")
            .vector_near("embedding", vec![1.0, 0.0, 0.0, 0.0], 1)
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].slug, "articles/a1");

        // Top-k=2 should return a1 and one of the orthogonal ones
        let results = db
            .collection("articles")
            .vector_near("embedding", vec![0.1, 0.2, 0.3, 0.4], 5)
            .collect();
        assert_eq!(results.len(), 3, "should return all 3 articles");
    }

    #[test]
    fn test_vector_near_as_starter() {
        let mut db = CoreDB::new();
        db.put("docs/d1", r#"{"_collection":"docs"}"#).unwrap();
        db.put("docs/d2", r#"{"_collection":"docs"}"#).unwrap();

        db.put_vector("docs/d1", "emb", &[1.0, 0.0]).unwrap();
        db.put_vector("docs/d2", "emb", &[0.0, 1.0]).unwrap();

        // VectorNear used as a starter (no prior step besides collection)
        let results = db
            .collection("docs")
            .vector_near("emb", vec![1.0, 0.0], 5)
            .collect();
        assert_eq!(results.len(), 2);
        // d1 should be first (distance = 0)
        assert_eq!(results[0].slug, "docs/d1");
    }

    // ── DDL + vector full flow ─────────────────────────────────────────────────

    #[test]
    fn test_create_table_with_vector_then_insert_and_search() {
        let mut db = CoreDB::new();

        // DDL — table definition
        db.execute(
            "CREATE TABLE articles (
                _key       TEXT PRIMARY KEY,
                title      TEXT,
                embedding  VECTOR
            )",
        )
        .unwrap();

        // CREATE INDEX registers the vector hint (PostgreSQL style)
        db.execute("CREATE INDEX ON articles USING hnsw (embedding)").unwrap();

        let schema = db.schemas.get("articles").expect("schema must exist");
        assert!(
            schema.indexes.vector.contains(&"embedding".to_string()),
            "embedding must be in indexes.vector after CREATE INDEX"
        );

        // INSERT with vector
        db.execute(
            "INSERT INTO articles (_key, title, embedding) \
             VALUES ('a1', 'Rust', [1.0, 0.0, 0.0, 0.0])",
        )
        .unwrap();

        // Query
        let results = db
            .query("SELECT * FROM articles WHERE VECTOR_NEAR(embedding, [1.0, 0.0, 0.0, 0.0], 5)")
            .unwrap()
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].slug, "articles/a1");
    }

    // ── SQL vector INSERT / UPDATE ─────────────────────────────────────────────

    #[test]
    fn test_sql_insert_with_vector() {
        let mut db = CoreDB::new();
        db.execute(
            "INSERT INTO articles (_key, title, embedding) \
             VALUES ('a1', 'Rust', [1.0, 0.0, 0.0, 0.0])",
        )
        .unwrap();

        // Node must exist
        assert!(db.contains("articles/a1"));

        // Vector must be queryable
        let v = db.get_vector("articles/a1", "embedding").expect("vector must be stored");
        assert_eq!(v, &[1.0_f32, 0.0, 0.0, 0.0]);

        // Must show up in VECTOR_NEAR
        let results = db
            .query("SELECT * FROM articles WHERE VECTOR_NEAR(embedding, [1.0, 0.0, 0.0, 0.0], 5)")
            .unwrap()
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].slug, "articles/a1");
    }

    #[test]
    fn test_sql_update_vector() {
        let mut db = CoreDB::new();
        db.execute(
            "INSERT INTO articles (_key, title, embedding) \
             VALUES ('a1', 'Rust', [1.0, 0.0, 0.0, 0.0])",
        )
        .unwrap();

        // Update the vector
        db.execute(
            "UPDATE articles SET embedding = [0.0, 1.0, 0.0, 0.0] WHERE _key = 'a1'",
        )
        .unwrap();

        let v = db.get_vector("articles/a1", "embedding").expect("vector must survive update");
        assert_eq!(v, &[0.0_f32, 1.0, 0.0, 0.0]);

        // Search with the new vector — must return a1
        let results = db
            .collection("articles")
            .vector_near("embedding", vec![0.0, 1.0, 0.0, 0.0], 5)
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].slug, "articles/a1");
    }

    // ── Vector guardrail tests ─────────────────────────────────────────────────

    /// compact() must survive with vectors intact. Before this fix, compact()
    /// silently dropped all vector data (snapshot had no vectors field).
    #[test]
    fn test_vectors_survive_compact_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        // Write node + vector, then compact
        {
            let mut db = CoreDB::open(path).unwrap();
            db.put("docs/d1", r#"{"_collection":"docs"}"#).unwrap();
            db.put_vector("docs/d1", "emb", &[1.0_f32, 0.0, 0.0]).unwrap();
            db.compact().unwrap();
        }

        // Cold open — must see the vector
        {
            let db = CoreDB::open(path).unwrap();
            let v = db.get_vector("docs/d1", "emb").expect("vector must survive compact");
            assert_eq!(v, &[1.0_f32, 0.0, 0.0]);
        }
    }

    /// WAL entries after the last compact() must also survive a cold open.
    #[test]
    fn test_vectors_survive_cold_restart_via_wal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        // Write and drop — no compact, so data lives only in WAL
        {
            let mut db = CoreDB::open(path).unwrap();
            db.put("docs/d1", r#"{"_collection":"docs"}"#).unwrap();
            db.put_vector("docs/d1", "emb", &[0.5_f32, 0.5]).unwrap();
        }

        // Cold reload — WAL replay must restore the vector
        {
            let db = CoreDB::open(path).unwrap();
            let v = db.get_vector("docs/d1", "emb").expect("vector must survive WAL replay");
            assert_eq!(v, &[0.5_f32, 0.5]);
        }
    }

    /// Deleting a node must remove its vector from the index immediately.
    /// A subsequent search must not see the deleted node.
    #[test]
    fn test_remove_node_removes_vector() {
        let mut db = CoreDB::new();
        db.put("docs/d1", r#"{"_collection":"docs"}"#).unwrap();
        db.put("docs/d2", r#"{"_collection":"docs"}"#).unwrap();
        db.put_vector("docs/d1", "emb", &[1.0_f32, 0.0]).unwrap();
        db.put_vector("docs/d2", "emb", &[0.0_f32, 1.0]).unwrap();

        db.remove("docs/d1");

        // Direct get must return None
        assert!(db.get_vector("docs/d1", "emb").is_none(), "vector must be gone after remove");

        // Search must not return d1
        let results = db
            .collection("docs")
            .vector_near("emb", vec![1.0_f32, 0.0], 10)
            .collect();
        assert!(results.iter().all(|h| h.slug != "docs/d1"), "d1 must not appear in search after remove");
    }

    /// compact() must only persist vectors whose node still exists.
    /// Orphan entries (removed node, stale vector) must be pruned silently.
    #[test]
    fn test_compact_prunes_orphan_vectors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        {
            let mut db = CoreDB::open(path).unwrap();
            db.put("docs/d1", r#"{"_collection":"docs"}"#).unwrap();
            db.put_vector("docs/d1", "emb", &[1.0_f32]).unwrap();
            db.remove("docs/d1"); // node deleted, vector cleaned up
            db.compact().unwrap();
        }

        // Reload — neither node nor vector should exist
        {
            let db = CoreDB::open(path).unwrap();
            assert!(!db.contains("docs/d1"));
            assert!(db.get_vector("docs/d1", "emb").is_none());
        }
    }

    #[test]
    fn test_vector_near_sql() {
        let mut db = CoreDB::new();
        db.put(
            "articles/a1",
            r#"{"_collection":"articles","_key":"a1","title":"Rust"}"#,
        )
        .unwrap();
        db.put_vector("articles/a1", "embedding", &[0.1, 0.2, 0.3, 0.4])
            .unwrap();

        let results = db
            .query("SELECT * FROM articles WHERE VECTOR_NEAR(embedding, [0.1, 0.2, 0.3, 0.4], 5)")
            .unwrap()
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].slug, "articles/a1");
    }
}
