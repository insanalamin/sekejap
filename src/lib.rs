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
mod query;
pub mod scalar;
pub mod sql;
mod storage;
pub mod text_index;
pub mod vector;

pub use vector::{CosineDistance, Distance, DotProduct, L2Distance};

pub use query::{CmpOp, DestWhere, Hit, MathExpr, MatchAggReturn, MatchAggStart, MatchAggStmt, Set, Step, WhereValue, WithExpr, WithOutExpr, WithRow, WithStage};
pub use sql::{CompiledMutation, EdgeDelete, EdgeInsert, SqlError, TableSchema};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Path, PathBuf};

use storage::wal::{WalEntry, WalReader, WalWriter};
use text_index::gin::GINIndex;
use text_index::gist::GiSTIndex;

// ── Storage format version constants ─────────────────────────────────────────

/// Bump when the snapshot schema changes in a backwards-incompatible way.
/// Old binaries that encounter a higher version return an error on open().
const SNAPSHOT_FORMAT_VERSION: u32 = 1;

/// Bump each constant when the corresponding index algorithm changes in a way
/// that makes indexes built by the previous version produce wrong results.
const GIN_INDEX_VERSION:     u32 = 2; // slot-map fix 2026-04-13
const BM25_INDEX_VERSION:    u32 = 1;
const BTREE_INDEX_VERSION:   u32 = 1;
const HNSW_INDEX_VERSION:    u32 = 1;

// ── Field index key ───────────────────────────────────────────────────────────

/// Totally-ordered wrapper for f64 (NaN sorts last, uses `total_cmp`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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

/// Payload storage backend — either an in-memory `Vec<u8>` (ephemeral DB) or
/// a memory-mapped append file `payloads.bin` (persistent DB).
///
/// For persistent databases the file is truncated to zero on every `open()`,
/// then refilled by snapshot + WAL replay. This keeps all geometry / large-JSON
/// bytes on disk and out of RAM. Only `NodeData` metadata (≈ 100 B per node)
/// stays in the `HashMap`.
pub(crate) struct PayloadStore {
    inner: PayloadInner,
}

// ── Read-only mmap for payloads.bin ──────────────────────────────────────────
// Eliminates per-read pread syscalls — reads become pointer dereferences
// with OS-managed page faults and readahead.  Zero new dependencies:
// mmap/munmap/madvise are system C library functions always linked by Rust.
#[cfg(unix)]
struct MmapView {
    ptr: *const u8,
    len: usize,
}

#[cfg(unix)]
unsafe impl Send for MmapView {}
#[cfg(unix)]
unsafe impl Sync for MmapView {}

#[cfg(unix)]
impl MmapView {
    fn try_new(file: &std::fs::File, len: usize) -> Option<Self> {
        if len == 0 { return None; }
        use std::os::unix::io::AsRawFd;
        extern "C" {
            fn mmap(
                addr: *mut std::ffi::c_void, length: usize,
                prot: i32, flags: i32, fd: i32, offset: i64,
            ) -> *mut std::ffi::c_void;
            fn madvise(addr: *mut std::ffi::c_void, length: usize, advice: i32) -> i32;
        }
        const PROT_READ: i32 = 1;
        const MAP_PRIVATE: i32 = 2;
        let ptr = unsafe {
            mmap(std::ptr::null_mut(), len, PROT_READ, MAP_PRIVATE, file.as_raw_fd(), 0)
        };
        if ptr == !0usize as *mut std::ffi::c_void { // MAP_FAILED
            return None;
        }
        // MADV_NORMAL (0) — let OS use default readahead policy.
        // Sorted-offset access patterns in batch reads benefit from readahead.
        unsafe { madvise(ptr, len, 0); }
        Some(Self { ptr: ptr as *const u8, len })
    }

    #[inline]
    fn slice(&self, offset: usize, read_len: usize) -> Option<&[u8]> {
        let end = offset.checked_add(read_len)?;
        if end > self.len { return None; }
        unsafe { Some(std::slice::from_raw_parts(self.ptr.add(offset), read_len)) }
    }
}

#[cfg(unix)]
impl Drop for MmapView {
    fn drop(&mut self) {
        extern "C" {
            fn munmap(addr: *mut std::ffi::c_void, length: usize) -> i32;
        }
        unsafe { munmap(self.ptr as *mut std::ffi::c_void, self.len); }
    }
}

enum PayloadInner {
    Memory { data: Vec<u8> },
    Disk {
        file: std::fs::File,
        total_len: u64,
        #[cfg(unix)]
        mmap: Option<MmapView>,
    },
}

impl PayloadStore {
    fn new() -> Self {
        Self { inner: PayloadInner::Memory { data: Vec::new() } }
    }

    /// Open (or create) a disk-backed store, truncating to zero.
    fn open_file(path: &std::path::Path) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(Self { inner: PayloadInner::Disk {
            file,
            total_len: 0,
            #[cfg(unix)]
            mmap: None,
        } })
    }

    /// Open an existing disk-backed store without truncating.
    fn open_existing(path: &std::path::Path, total_len: u64) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        #[cfg(unix)]
        let mmap = MmapView::try_new(&file, total_len as usize);
        Ok(Self { inner: PayloadInner::Disk {
            file,
            total_len,
            #[cfg(unix)]
            mmap,
        } })
    }

    fn is_disk(&self) -> bool {
        matches!(self.inner, PayloadInner::Disk { .. })
    }

    /// Append raw bytes; returns `(offset, len)`.
    /// Panics on disk write failure (disk-full etc.) — callers do not recover.
    fn append(&mut self, bytes: &[u8]) -> (u64, u32) {
        match &mut self.inner {
            PayloadInner::Memory { data } => {
                let offset = data.len() as u64;
                data.extend_from_slice(bytes);
                (offset, bytes.len() as u32)
            }
            PayloadInner::Disk { file, total_len, .. } => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::FileExt;
                    file.write_all_at(bytes, *total_len)
                        .expect("sekejap: payload disk write failed");
                }
                #[cfg(not(unix))]
                {
                    use std::io::{Seek, SeekFrom, Write};
                    file.seek(SeekFrom::Start(*total_len))
                        .expect("sekejap: payload disk seek failed");
                    file.write_all(bytes)
                        .expect("sekejap: payload disk write failed");
                }
                let offset = *total_len;
                *total_len += bytes.len() as u64;
                (offset, bytes.len() as u32)
            }
        }
    }

    /// Parse JSON at the given position. Returns `None` if invalid.
    fn get(&self, offset: u64, len: u32) -> Option<Value> {
        self.get_raw(offset, len)
            .and_then(|b| serde_json::from_slice(&b).ok())
    }

    /// Return raw JSON bytes at the given position (owned copy).
    pub(crate) fn get_raw(&self, offset: u64, len: u32) -> Option<Vec<u8>> {
        self.get_raw_at(offset, len as usize)
    }

    /// Read `read_len` bytes starting at an arbitrary absolute byte offset.
    /// Uses mmap when available (zero syscalls), falls back to pread.
    pub(crate) fn get_raw_at(&self, abs_offset: u64, read_len: usize) -> Option<Vec<u8>> {
        if read_len == 0 {
            return Some(vec![]);
        }
        match &self.inner {
            PayloadInner::Memory { data } => {
                let start = abs_offset as usize;
                let end = start.checked_add(read_len)?;
                data.get(start..end).map(|b| b.to_vec())
            }
            #[cfg(unix)]
            PayloadInner::Disk { file, mmap, .. } => {
                // Fast path: read from mmap (no syscall — just memcpy from page cache).
                if let Some(ref m) = mmap {
                    if let Some(slice) = m.slice(abs_offset as usize, read_len) {
                        return Some(slice.to_vec());
                    }
                }
                // Fallback: pread for data written after the mmap was created.
                use std::os::unix::fs::FileExt;
                let mut buf = vec![0u8; read_len];
                file.read_exact_at(&mut buf, abs_offset).ok()?;
                Some(buf)
            }
            #[cfg(not(unix))]
            PayloadInner::Disk { file, .. } => {
                let _ = (file, abs_offset, read_len);
                None
            }
        }
    }

    /// Borrow a slice of the payload store without copying (zero-alloc).
    /// Returns `None` if offset/len is out of range or no mmap is available.
    #[cfg(unix)]
    fn get_slice(&self, abs_offset: u64, read_len: usize) -> Option<&[u8]> {
        if read_len == 0 { return Some(&[]); }
        match &self.inner {
            PayloadInner::Memory { data } => {
                let start = abs_offset as usize;
                let end = start.checked_add(read_len)?;
                data.get(start..end)
            }
            PayloadInner::Disk { mmap, .. } => {
                mmap.as_ref()?.slice(abs_offset as usize, read_len)
            }
        }
    }

    /// Reset the slab (in-memory only — used after in-memory compaction).
    fn reset(&mut self, new_data: Vec<u8>) {
        if let PayloadInner::Memory { data } = &mut self.inner {
            *data = new_data;
        }
    }
}

pub struct NodeData {
    pub slug: String,
    /// Cached `_collection` field value (empty string if no collection).
    /// Avoids parsing JSON for collection-only lookups.
    pub collection: String,
    /// Cached spatial bounding-box, computed once in `put_raw()`.
    /// `rebuild_spatial_grid()` reads from here to avoid disk reads.
    pub spatial_meta: Option<geo::SpatialMeta>,
    /// Byte offset of this node's raw JSON payload in `CoreDB.payload_store`.
    pub payload_offset: u64,
    /// Byte length of this node's raw JSON payload.
    pub payload_len: u32,
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

// ── BfsPath (internal only) ───────────────────────────────────────────────────

/// Internal result of `bfs_shortest_path`. Not part of the public API.
/// Use `db.query("SELECT … FROM MATCH SHORTEST …")` instead.
#[derive(Debug, Clone)]
pub(crate) struct BfsPath {
    pub(crate) nodes: Vec<query::Hit>,
    pub(crate) edges: Vec<EdgeHit>,
    pub(crate) length: usize,
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
    /// collection_hash → collection name (for O(1) SHOW TABLES without node scan)
    collection_names_map: HashMap<u64, String>,
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
    /// Build params for each HNSW index: field → (m, ef_construction).
    /// Populated by build_hnsw_index(); used to auto-rebuild on version mismatch.
    hnsw_params: HashMap<String, (usize, usize)>,
    /// Append-only byte slab for raw JSON payloads.
    /// All `NodeData` entries index into this store via `(payload_offset, payload_len)`.
    payload_store: PayloadStore,
    /// Set to `true` during WAL replay in `open()`.
    /// Guards expensive per-entry rebuilds (e.g. HNSW entry-point check in remove_raw)
    /// that must not fire O(N) times during replay — open() handles those once at the end.
    replaying: bool,
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
            collection_names_map: HashMap::new(),
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
            hnsw_params: HashMap::new(),
            payload_store: PayloadStore::new(),
            replaying: false,
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

        // 1. Load snapshot (peek before touching payloads.bin).
        //    Disk-backed snapshots store only metadata — payloads stay in payloads.bin.
        //    We must NOT truncate payloads.bin in that case.
        let snap_path = dir.join("snapshot.json");
        // Measure size before parsing — used later to detect legacy bloated snapshots.
        let snap_file_size = if snap_path.exists() {
            std::fs::metadata(&snap_path).map(|m| m.len()).unwrap_or(0)
        } else {
            0
        };
        let snap: Option<Snapshot> = if snap_path.exists() {
            // Stream-parse rather than loading the whole file into RAM.
            // This handles legacy snapshots that embedded gin_indexes (multi-GB).
            // serde_json::from_reader reads incrementally; IgnoredAny skips gin_indexes
            // without allocating, so a 2.3GB legacy snapshot costs <1 MB to parse.
            let file = std::fs::File::open(&snap_path)?;
            match serde_json::from_reader::<_, Snapshot>(std::io::BufReader::new(file)) {
                Ok(s) if s.version > SNAPSHOT_FORMAT_VERSION => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "snapshot version {} requires a newer sekejap (max supported: {})",
                            s.version, SNAPSHOT_FORMAT_VERSION
                        ),
                    ));
                }
                Ok(s) => Some(s),
                Err(_) => None, // corrupt snapshot — fall back to full WAL replay
            }
        } else {
            None
        };

        // Open payload store: preserve existing payloads.bin for disk-backed snapshots,
        // truncate to zero otherwise (WAL replay or legacy snapshot will refill it).
        let pay_path = dir.join("payloads.bin");
        let preserve = snap.as_ref().map_or(false, |s| s.is_disk_backed);
        if preserve && pay_path.exists() {
            let existing_len = std::fs::metadata(&pay_path)?.len();
            db.payload_store = PayloadStore::open_existing(&pay_path, existing_len)?;
        } else {
            db.payload_store = PayloadStore::open_file(&pay_path)?;
        }

        if let Some(snap) = snap {
            db.load_snapshot(snap);
        }

        // One-time migration: if the snapshot was large (legacy had embedded gin_indexes),
        // rewrite it immediately as a clean compact snapshot so subsequent opens are fast.
        // A normal disk-backed snapshot with 89k nodes is ~50-80 MB (pretty-printed).
        // The legacy bloated variant (gin_indexes embedded as JSON) was 1-10 GB.
        // Use 500 MB as the threshold — safely above any real snapshot, far below bloated ones.
        if snap_file_size > 500 * 1024 * 1024 {
            if let Ok(snap_json) = serde_json::to_vec(&db.build_snapshot()) {
                let snap_tmp = snap_path.with_extension("json.tmp");
                if std::fs::write(&snap_tmp, &snap_json).is_ok() {
                    let _ = std::fs::rename(&snap_tmp, &snap_path);
                }
            }
        }

        // 2. Replay WAL — stream one entry at a time to avoid loading all
        //    payloads into RAM simultaneously (critical for large datasets).
        //    Track two separate flags:
        //    - wal_had_payload: Put/Remove/PutVector — affects GIN text indexes
        //    - wal_had_graph:   Link/LinkMeta/Unlink — only affects graph topology
        //    GIN rebuild is expensive (reads every payload from disk); we must not
        //    trigger it for edge-only WAL entries.
        let wal_path = dir.join("wal.log");
        let mut wal_had_payload = false;
        let mut wal_had_graph   = false;
        if wal_path.exists() {
            db.replaying = true;
            let corrupted = WalReader::open(&wal_path)?.replay_all(|entry| {
                match &entry {
                    WalEntry::Put { .. }
                    | WalEntry::Remove { .. }
                    | WalEntry::PutVector { .. } => wal_had_payload = true,
                    WalEntry::Link { .. }
                    | WalEntry::LinkMeta { .. }
                    | WalEntry::Unlink { .. } => wal_had_graph = true,
                    _ => {}
                }
                db.replay(entry);
            });
            db.replaying = false;
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

        // 5. Rebuild GIN and HNSW when WAL added new data, or load GIN from the
        //    binary sidecar gin.bin (compact, fast — no JSON parsing overhead).
        //    GIN: only rebuild when payload-mutating entries (Put/Remove) were in WAL.
        //    HNSW: rebuild when any data changed (payloads or vectors).
        let gin_bin_path = dir.join("gin.bin");
        if wal_had_payload {
            // Payload changed — rebuild GIN + HNSW from current data and refresh gin.bin.
            db.rebuild_declared_gin_indexes();
            db.rebuild_declared_hnsw_indexes();
            let _ = db.save_gin_binary(&gin_bin_path);
        } else {
            // No payload changes — try loading GIN from gin.bin. If missing or
            // stale, rebuild once (covers first open after CREATE INDEX, etc.).
            if !db.load_gin_binary(&gin_bin_path) {
                db.rebuild_declared_gin_indexes();
                let _ = db.save_gin_binary(&gin_bin_path);
            }
            // HNSW: rebuild only when vectors changed (PutVector is part of wal_had_payload,
            // so here vectors are unchanged — no rebuild needed).
        }
        let _ = wal_had_graph; // used only to determine topology was replayed (no index rebuild needed)

        Ok(db)
    }

    // ── Raw internals (no WAL write — used during replay and open) ────────────

    fn put_raw(&mut self, slug: &str, payload_json: &str) -> Result<u64, serde_json::Error> {
        let mut payload: Value = serde_json::from_str(payload_json)?;
        let hash = sk_hash(slug);
        let now = chrono::Utc::now().timestamp_millis();

        // Collect old node metadata (separate let to release borrow before mutations)
        let old_info: Option<(String, u64, u32)> = self.nodes
            .get(&hash)
            .map(|n| (n.collection.clone(), n.payload_offset, n.payload_len));

        // Auto-timestamps: preserve existing _created_unix, always update _updated_unix
        {
            let obj = payload.as_object_mut().expect("payload must be object");
            let created_unix = if obj.contains_key("_created_unix") {
                obj.get("_created_unix").cloned()
            } else {
                // Preserve from the old stored payload (if updating)
                old_info.as_ref()
                    .and_then(|(_, off, len)| self.payload_store.get(*off, *len))
                    .and_then(|old_p| old_p.get("_created_unix").cloned())
            };
            if let Some(v) = created_unix {
                obj.insert("_created_unix".into(), v);
            } else {
                obj.insert("_created_unix".into(), serde_json::json!(now));
            }
            obj.insert("_updated_unix".into(), serde_json::json!(now));
        }

        // Extract spatial meta now (while we have the parsed Value in hand).
        // Stored in NodeData so rebuild_spatial_grid() can reuse it without
        // re-parsing geometry from disk.
        let spatial_meta = geo::extract_spatial_meta(&payload);

        // Remove old collection + field-index entries for this hash (if updating)
        if let Some((ref old_coll, old_off, old_len)) = old_info {
            if !old_coll.is_empty() {
                let coll_hash = sk_hash(old_coll);
                if let Some(members) = self.collections.get_mut(&coll_hash) {
                    members.retain(|&h| h != hash);
                }
                // Remove from all field indexes for this collection.
                // Only parse old payload when field indexes exist (avoids work for plain nodes).
                let has_fi = self.field_indexes.keys().any(|(c, _)| *c == coll_hash);
                if has_fi {
                    let old_payload = self.payload_store.get(old_off, old_len)
                        .unwrap_or(Value::Null);
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
        }

        if let Some(coll) = payload.get("_collection").and_then(|v| v.as_str()) {
            let coll_hash = sk_hash(coll);
            let members = self.collections.entry(coll_hash).or_default();
            if !members.contains(&hash) {
                members.push(hash);
            }
            self.collection_names_map.entry(coll_hash).or_insert_with(|| coll.to_string());
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

        // Check BM25 fields before storing (while we still have the local payload Value)
        let bm25_fields: Vec<String> = if self.bm25_indexes.is_empty() {
            Vec::new()
        } else {
            self.bm25_indexes
                .keys()
                .filter(|f| {
                    payload.get(f.as_str()).and_then(|v| v.as_str()).is_some()
                })
                .cloned()
                .collect()
        };

        // Serialize updated payload and store bytes in the slab.
        let serialized = serde_json::to_string(&payload)?;
        let (offset, len) = self.payload_store.append(serialized.as_bytes());

        let collection_str = payload.get("_collection")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        self.slug_map.insert(slug.to_string(), hash);
        self.nodes.insert(hash, NodeData {
            slug: slug.to_string(),
            collection: collection_str,
            spatial_meta: spatial_meta.clone(),
            payload_offset: offset,
            payload_len: len,
        });

        // Rebuild BM25 indexes for any field present in the new payload.
        // Full rebuild per field is O(N) but necessary since BM25 postings are
        // compressed; no true incremental-add path exists.
        for field in bm25_fields {
            self.build_bm25_index(&field);
        }

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
            if !node.collection.is_empty() {
                let coll_hash = sk_hash(&node.collection);
                if let Some(members) = self.collections.get_mut(&coll_hash) {
                    members.retain(|&h| h != hash);
                    if members.is_empty() {
                        self.collection_names_map.remove(&coll_hash);
                    }
                }
                // Remove from field indexes (read old payload from slab for key lookup)
                let has_fi = self.field_indexes.keys().any(|(c, _)| *c == coll_hash);
                if has_fi {
                    let old_payload = self.payload_store
                        .get(node.payload_offset, node.payload_len)
                        .unwrap_or(Value::Null);
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

            // If this node was the HNSW entry point, the graph can no longer
            // navigate (search_layer returns [] when entry vector is missing).
            // Rebuild affected HNSW indexes immediately — but NOT during WAL replay:
            // open() calls rebuild_declared_hnsw_indexes() once at the end, which
            // handles all removes in the WAL in a single O(N log N) pass.
            if !self.replaying {
                use crate::vector::{HnswGraph, CosineDistance};
                let hnsw_rebuild: Vec<String> = self.hnsw_indexes
                    .iter()
                    .filter(|(_, g)| g.entry_point_id() == Some(hash))
                    .map(|(f, _)| f.clone())
                    .collect();
                for field in hnsw_rebuild {
                    match self.vectors.get(&field) {
                        Some(field_vecs) => {
                            let (m, ef) = self.hnsw_params.get(&field).copied().unwrap_or((16, 200));
                            self.hnsw_indexes.insert(field, HnswGraph::build::<CosineDistance>(field_vecs, m, ef));
                        }
                        None => { self.hnsw_indexes.remove(&field); }
                    }
                }
            }

            // Incrementally update BM25 indexes: adjusts the running
            // `num_docs` and `sum_doc_len` counters and drops the
            // `doc_id_to_idx` entry so the deleted node is invisible
            // to subsequent searches immediately — without a full
            // rebuild.  The corresponding `doc_lengths` slot becomes a
            // harmless 4-byte orphan until the next rebuild.
            for bm25_idx in self.bm25_indexes.values_mut() {
                bm25_idx.delete(hash);
            }
        }
    }

    /// Drop an entire collection: removes all its nodes (cascading all edges),
    /// clears the declared schema, and removes the collection-level btree index
    /// entries. Returns the number of nodes deleted.
    fn drop_table_raw(&mut self, collection: &str) -> usize {
        let col_hash = sk_hash(collection);

        // Build a set of node hashes belonging to this collection
        let member_hashes: std::collections::HashSet<u64> = self.collections
            .get(&col_hash)
            .into_iter()
            .flat_map(|v| v.iter().copied())
            .collect();

        // Collect slugs (cannot hold borrow while mutating)
        let slugs: Vec<String> = self.slug_map
            .iter()
            .filter(|(_, h)| member_hashes.contains(h))
            .map(|(s, _)| s.clone())
            .collect();

        let count = slugs.len();

        for slug in slugs {
            self.remove_raw(&slug); // cascades edges, cleans per-node indexes
        }

        // Remove the now-empty collection btree index entries
        self.field_indexes.retain(|(c, _), _| *c != col_hash);

        // Remove declared schema (if any)
        self.schemas.remove(collection);

        count
    }

    /// Apply an ALTER TABLE operation in-memory (no WAL write).
    /// Used by both execute() (which writes WAL after) and replay().
    fn alter_table_raw(&mut self, collection: &str, op: sql::AlterTableOp) -> Result<usize, sql::SqlError> {
        use sql::AlterTableOp;
        match op {
            // ── ADD COLUMN ────────────────────────────────────────────────────
            AlterTableOp::AddColumn { def } => {
                let schema = self.schemas.get_mut(collection).ok_or_else(|| {
                    sql::SqlError::InvalidValue(format!("table '{collection}' does not exist"))
                })?;
                if schema.fields.iter().any(|f| f.name == def.name) {
                    return Err(sql::SqlError::InvalidValue(format!(
                        "column '{}' already exists in '{collection}'",
                        def.name
                    )));
                }
                schema.fields.push(def);
                Ok(0) // schema-only; no rows touched
            }

            // ── DROP COLUMN ───────────────────────────────────────────────────
            AlterTableOp::DropColumn { name, if_exists } => {
                let (had_fulltext, had_bm25, had_hnsw) = {
                    let schema = self.schemas.get_mut(collection).ok_or_else(|| {
                        sql::SqlError::InvalidValue(format!("table '{collection}' does not exist"))
                    })?;
                    let idx = schema.fields.iter().position(|f| f.name == name);
                    match idx {
                        None if if_exists => return Ok(0),
                        None => return Err(sql::SqlError::InvalidValue(format!(
                            "column '{name}' does not exist in '{collection}'"
                        ))),
                        Some(i) => { schema.fields.remove(i); }
                    }
                    // Remove field from every index hint list so WAL replay
                    // doesn't try to rebuild an index for a dropped column.
                    let ix = &mut schema.indexes;
                    ix.range.retain(|f| f != &name);
                    ix.hash.retain(|f| f != &name);
                    let had_fulltext = ix.fulltext.iter().any(|f| f == &name);
                    ix.fulltext.retain(|f| f != &name);
                    let had_bm25 = ix.bm25.iter().any(|f| f == &name);
                    ix.bm25.retain(|f| f != &name);
                    ix.spatial.retain(|f| f != &name);
                    let had_hnsw = ix.vector.iter().any(|f| f == &name);
                    ix.vector.retain(|f| f != &name);
                    (had_fulltext, had_bm25, had_hnsw)
                }; // release schema borrow — returns tuple of global-index flags

                // Drop the btree index data for this field (no longer valid).
                let col_hash = sk_hash(collection);
                self.field_indexes.remove(&(col_hash, name.clone()));

                // Remove field from all nodes in the collection.
                // This must happen BEFORE rebuilding global indexes so the rebuild
                // naturally sees the field absent from this collection's nodes.
                let node_meta: Vec<(u64, u64, u32)> = self.collections
                    .get(&col_hash).into_iter().flatten()
                    .filter_map(|&h| self.nodes.get(&h).map(|n| (h, n.payload_offset, n.payload_len)))
                    .collect();
                let mut count = 0usize;
                let mut node_updates: Vec<(u64, u64, u32)> = Vec::new();
                for (h, off, len) in node_meta {
                    if let Some(mut p) = self.payload_store.get(off, len) {
                        if p.as_object_mut().map(|o| o.remove(&name).is_some()).unwrap_or(false) {
                            let new_json = serde_json::to_string(&p)
                                .unwrap_or_else(|_| "{}".to_string());
                            let (new_off, new_len) = self.payload_store.append(new_json.as_bytes());
                            node_updates.push((h, new_off, new_len));
                            count += 1;
                        }
                    }
                }
                for (h, new_off, new_len) in node_updates {
                    if let Some(node) = self.nodes.get_mut(&h) {
                        node.payload_offset = new_off;
                        node.payload_len = new_len;
                    }
                }

                // Rebuild global indexes from remaining data (nodes for the dropped
                // collection no longer carry the field, so the rebuild is naturally clean).
                // Only rebuild if the in-memory structure actually exists.
                if had_fulltext && self.gin_indexes.contains_key(&name)  { self.rebuild_gin_for_remaining(&name); }
                if had_bm25    && self.bm25_indexes.contains_key(&name)  { self.rebuild_bm25_for_remaining(&name); }
                if had_hnsw    && self.hnsw_indexes.contains_key(&name)  { self.rebuild_hnsw_for_remaining(&name); }

                Ok(count)
            }

            // ── RENAME COLUMN ─────────────────────────────────────────────────
            AlterTableOp::RenameColumn { old_name, new_name } => {
                {
                    let schema = self.schemas.get_mut(collection).ok_or_else(|| {
                        sql::SqlError::InvalidValue(format!("table '{collection}' does not exist"))
                    })?;
                    let idx = schema.fields.iter().position(|f| f.name == old_name)
                        .ok_or_else(|| sql::SqlError::InvalidValue(format!(
                            "column '{old_name}' does not exist in '{collection}'"
                        )))?;
                    if schema.fields.iter().any(|f| f.name == new_name) {
                        return Err(sql::SqlError::InvalidValue(format!(
                            "column '{new_name}' already exists in '{collection}'"
                        )));
                    }
                    schema.fields[idx].name = new_name.clone();
                } // release schema borrow

                // Rename the field key in every node of the collection
                let col_hash = sk_hash(collection);
                let node_meta: Vec<(u64, u64, u32)> = self.collections
                    .get(&col_hash).into_iter().flatten()
                    .filter_map(|&h| self.nodes.get(&h).map(|n| (h, n.payload_offset, n.payload_len)))
                    .collect();
                let mut count = 0usize;
                let mut node_updates: Vec<(u64, u64, u32)> = Vec::new();
                for (h, off, len) in node_meta {
                    if let Some(mut p) = self.payload_store.get(off, len) {
                        if let Some(obj) = p.as_object_mut() {
                            if let Some(val) = obj.remove(&old_name) {
                                obj.insert(new_name.clone(), val);
                                let new_json = serde_json::to_string(&p)
                                    .unwrap_or_else(|_| "{}".to_string());
                                let (new_off, new_len) = self.payload_store.append(new_json.as_bytes());
                                node_updates.push((h, new_off, new_len));
                                count += 1;
                            }
                        }
                    }
                }
                for (h, new_off, new_len) in node_updates {
                    if let Some(node) = self.nodes.get_mut(&h) {
                        node.payload_offset = new_off;
                        node.payload_len = new_len;
                    }
                }

                // Move the btree index data from old field name to new field name
                if let Some(btree) = self.field_indexes.remove(&(col_hash, old_name.clone())) {
                    self.field_indexes.insert((col_hash, new_name.clone()), btree);
                }

                // Update field name inside every index hint list so WAL replay
                // rebuilds the index under the new name.
                if let Some(schema) = self.schemas.get_mut(collection) {
                    for list in [
                        &mut schema.indexes.range,
                        &mut schema.indexes.hash,
                        &mut schema.indexes.fulltext,
                        &mut schema.indexes.bm25,
                        &mut schema.indexes.spatial,
                        &mut schema.indexes.vector,
                    ] {
                        for entry in list.iter_mut() {
                            if *entry == old_name {
                                *entry = new_name.clone();
                            }
                        }
                    }
                }

                Ok(count)
            }

            // ── RENAME TABLE ──────────────────────────────────────────────────
            // Note: existing slugs (e.g. "old_col/key") remain unchanged.
            // Only the logical _collection metadata and index buckets are moved.
            AlterTableOp::RenameTable { new_name } => {
                if self.schemas.contains_key(&new_name) {
                    return Err(sql::SqlError::InvalidValue(format!(
                        "table '{new_name}' already exists"
                    )));
                }
                let mut schema = self.schemas.remove(collection).ok_or_else(|| {
                    sql::SqlError::InvalidValue(format!("table '{collection}' does not exist"))
                })?;
                schema.collection = new_name.clone();
                self.schemas.insert(new_name.clone(), schema);

                // Move collection bucket to new hash
                let old_hash = sk_hash(collection);
                let new_hash = sk_hash(&new_name);
                let node_hashes: Vec<u64> =
                    self.collections.remove(&old_hash).unwrap_or_default();
                let count = node_hashes.len();
                self.collections.insert(new_hash, node_hashes.clone());
                // Update the O(1) name map
                self.collection_names_map.remove(&old_hash);
                self.collection_names_map.insert(new_hash, new_name.clone());

                // Update _collection field in every node payload + cached collection field
                let node_meta: Vec<(u64, u64, u32)> = node_hashes.iter()
                    .filter_map(|&h| self.nodes.get(&h).map(|n| (h, n.payload_offset, n.payload_len)))
                    .collect();
                let mut node_updates: Vec<(u64, u64, u32)> = Vec::new();
                for (h, off, len) in node_meta {
                    if let Some(mut p) = self.payload_store.get(off, len) {
                        if let Some(obj) = p.as_object_mut() {
                            obj.insert("_collection".to_string(), serde_json::json!(new_name));
                        }
                        let new_json = serde_json::to_string(&p)
                            .unwrap_or_else(|_| "{}".to_string());
                        let (new_off, new_len) = self.payload_store.append(new_json.as_bytes());
                        node_updates.push((h, new_off, new_len));
                    }
                }
                for (h, new_off, new_len) in node_updates {
                    if let Some(node) = self.nodes.get_mut(&h) {
                        node.collection = new_name.clone();
                        node.payload_offset = new_off;
                        node.payload_len = new_len;
                    }
                }

                // Move field_indexes from old collection hash to new
                let old_keys: Vec<(u64, String)> = self.field_indexes.keys()
                    .filter(|(c, _)| *c == old_hash)
                    .cloned()
                    .collect();
                for (_, field) in old_keys {
                    if let Some(btree) = self.field_indexes.remove(&(old_hash, field.clone())) {
                        self.field_indexes.insert((new_hash, field), btree);
                    }
                }

                Ok(count)
            }

            // ── ALTER COLUMN TYPE ─────────────────────────────────────────────
            // Schema annotation updated; existing data is not coerced.
            // If a btree index exists for this field it is rebuilt from scratch
            // so FieldKey variants match the new type (mirrors PostgreSQL REINDEX).
            AlterTableOp::AlterColumnType { name, ty } => {
                let has_btree = {
                    let schema = self.schemas.get_mut(collection).ok_or_else(|| {
                        sql::SqlError::InvalidValue(format!("table '{collection}' does not exist"))
                    })?;
                    let field = schema.fields.iter_mut().find(|f| f.name == name)
                        .ok_or_else(|| sql::SqlError::InvalidValue(format!(
                            "column '{name}' does not exist in '{collection}'"
                        )))?;
                    field.ty = ty;
                    schema.indexes.range.contains(&name)
                };

                if has_btree {
                    // Drop stale btree entries, then rebuild from current node data.
                    let col_hash = sk_hash(collection);
                    self.field_indexes.remove(&(col_hash, name.clone()));
                    self.build_field_index(collection, &name);
                }

                Ok(0)
            }
        }
    }

    /// Remove one index from a collection, then rebuild any global structure
    /// (GIN / BM25 / HNSW) from only the collections that still hold that index.
    ///
    /// Returns `true` when the index existed and was removed, `false` otherwise.
    fn drop_index_raw(&mut self, collection: &str, method: &sql::IndexMethod, field: &str) -> bool {
        use sql::IndexMethod;

        // Remove the hint from this collection's schema.
        let removed = if let Some(schema) = self.schemas.get_mut(collection) {
            let list = match method {
                IndexMethod::Btree                  => &mut schema.indexes.range,
                IndexMethod::Hash                   => &mut schema.indexes.hash,
                IndexMethod::Gin | IndexMethod::Gist => &mut schema.indexes.fulltext,
                IndexMethod::Bm25                   => &mut schema.indexes.bm25,
                IndexMethod::Spatial                => &mut schema.indexes.spatial,
                IndexMethod::Hnsw                   => &mut schema.indexes.vector,
            };
            let before = list.len();
            list.retain(|f| f != field);
            list.len() < before
        } else {
            false
        };

        if !removed {
            return false;
        }

        let col_hash = sk_hash(collection);

        match method {
            // ── Per-collection indexes: drop directly ─────────────────────────
            IndexMethod::Btree => {
                self.field_indexes.remove(&(col_hash, field.to_string()));
            }
            IndexMethod::Hash => {
                // Hint-only — nothing to drop.
            }

            // ── Global indexes: rebuild from remaining indexed collections ─────
            // After removing this collection's hint, re-scan only the collections
            // whose schema still lists this field in the relevant hint.
            // Only rebuild if the in-memory index actually exists — if CREATE INDEX
            // was called before any data was inserted, there is no structure to
            // clean up, and creating one here would produce stale truncated IDs.
            IndexMethod::Gin | IndexMethod::Gist => {
                if self.gin_indexes.contains_key(field) {
                    self.rebuild_gin_for_remaining(field);
                }
            }
            IndexMethod::Bm25 => {
                if self.bm25_indexes.contains_key(field) {
                    self.rebuild_bm25_for_remaining(field);
                }
            }
            IndexMethod::Hnsw => {
                if self.hnsw_indexes.contains_key(field) {
                    self.rebuild_hnsw_for_remaining(field);
                }
            }
            IndexMethod::Spatial => {
                // Spatial grid covers all GEO nodes regardless of collection;
                // removing the hint is sufficient — no rebuild needed.
            }
        }

        true
    }

    /// Rebuild `gin_indexes[field]` from only the collections whose schema
    /// still declares a `fulltext` index on this field.
    fn rebuild_gin_for_remaining(&mut self, field: &str) {
        let col_hashes: Vec<u64> = self.schemas.values()
            .filter(|s| s.indexes.fulltext.contains(&field.to_string()))
            .map(|s| sk_hash(&s.collection))
            .collect();

        if col_hashes.is_empty() {
            self.gin_indexes.remove(field);
            return;
        }

        let values: Vec<(u64, String)> = col_hashes.iter()
            .flat_map(|ch| self.collections.get(ch).into_iter().flatten().copied())
            .filter_map(|hash| {
                let node = self.nodes.get(&hash)?;
                let payload = self.payload_store.get(node.payload_offset, node.payload_len)?;
                payload.get(field)?.as_str().map(|s| (hash, s.to_string()))
            })
            .collect();

        if values.is_empty() {
            self.gin_indexes.remove(field);
        } else {
            let refs: Vec<(u64, &str)> = values.iter().map(|(h, s)| (*h, s.as_str())).collect();
            let index = text_index::gin::GINIndex::build(refs.into_iter(), field);
            self.gin_indexes.insert(field.to_string(), index);
        }
    }

    /// Rebuild `bm25_indexes[field]` from only the collections whose schema
    /// still declares a `bm25` index on this field.
    fn rebuild_bm25_for_remaining(&mut self, field: &str) {
        let col_hashes: Vec<u64> = self.schemas.values()
            .filter(|s| s.indexes.bm25.contains(&field.to_string()))
            .map(|s| sk_hash(&s.collection))
            .collect();

        if col_hashes.is_empty() {
            self.bm25_indexes.remove(field);
            return;
        }

        let values: Vec<(u64, String)> = col_hashes.iter()
            .flat_map(|ch| self.collections.get(ch).into_iter().flatten().copied())
            .filter_map(|hash| {
                let node = self.nodes.get(&hash)?;
                let payload = self.payload_store.get(node.payload_offset, node.payload_len)?;
                payload.get(field)?.as_str().map(|s| (hash, s.to_string()))
            })
            .collect();

        if values.is_empty() {
            self.bm25_indexes.remove(field);
        } else {
            let refs: Vec<(u64, &str)> = values.iter().map(|(h, s)| (*h, s.as_str())).collect();
            let index = bm25::Bm25Index::build(field, refs.into_iter());
            self.bm25_indexes.insert(field.to_string(), index);
        }
    }

    /// Rebuild `hnsw_indexes[field]` from only the collections whose schema
    /// still declares a `vector` index on this field.
    fn rebuild_hnsw_for_remaining(&mut self, field: &str) {
        let col_hashes: Vec<u64> = self.schemas.values()
            .filter(|s| s.indexes.vector.contains(&field.to_string()))
            .map(|s| sk_hash(&s.collection))
            .collect();

        if col_hashes.is_empty() {
            self.hnsw_indexes.remove(field);
            return;
        }

        let member_hashes: std::collections::HashSet<u64> = col_hashes.iter()
            .flat_map(|ch| self.collections.get(ch).into_iter().flatten().copied())
            .collect();

        if let Some(field_vecs) = self.vectors.get(field) {
            let filtered: HashMap<u64, Vec<f32>> = field_vecs.iter()
                .filter(|(h, _)| member_hashes.contains(*h))
                .map(|(h, v)| (*h, v.clone()))
                .collect();

            if filtered.is_empty() {
                self.hnsw_indexes.remove(field);
            } else {
                use vector::CosineDistance;
                let graph = vector::HnswGraph::build::<CosineDistance>(&filtered, 16, 200);
                self.hnsw_indexes.insert(field.to_string(), graph);
            }
        } else {
            self.hnsw_indexes.remove(field);
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
                // WAL replay is fault-tolerant — ignore build failures.
                let _ = self.apply_index(&collection, &m, &fields);
            }
            WalEntry::DropTable { collection } => {
                self.drop_table_raw(&collection);
            }
            WalEntry::DropIndex { collection, method, field } => {
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
                self.drop_index_raw(&collection, &m, &field);
            }
            WalEntry::AlterTable { collection, op_json } => {
                if let Ok(op) = serde_json::from_str::<sql::AlterTableOp>(&op_json) {
                    let _ = self.alter_table_raw(&collection, op);
                }
            }
            WalEntry::Unknown => { /* forward-compat: skip entries from newer binaries */ }
        }
    }

    // ── Writes ────────────────────────────────────────────────────────────────

    /// Insert or update a node. The `_collection` field in the payload
    /// registers the node in a named collection for `db.collection()` queries.
    ///
    /// Returns the slug hash on success.
    pub fn put(&mut self, slug: &str, payload_json: &str) -> Result<u64, serde_json::Error> {
        // Check before put_raw so we know whether this is a new node or an update.
        let node_hash = sk_hash(slug);
        let is_update = self.nodes.contains_key(&node_hash);

        let hash = self.put_raw(slug, payload_json)?;

        // Auto-maintain GIN indexes for any field declared fulltext in this collection.
        if let Ok(payload) = serde_json::from_str::<Value>(payload_json) {
            if let Some(coll) = payload.get("_collection").and_then(|v| v.as_str()) {
                let coll_hash = sk_hash(coll);
                // Collect declared GIN fields and their text values (releases borrow).
                let gin_updates: Vec<(String, Option<String>)> = self.schemas.values()
                    .filter(|s| sk_hash(&s.collection) == coll_hash)
                    .flat_map(|s| s.indexes.fulltext.iter().map(|f| {
                        let text = payload.get(f.as_str())
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        (f.clone(), text)
                    }))
                    .collect();
                for (gin_field, text_opt) in gin_updates {
                    if is_update {
                        // Update: full rebuild to remove old trigrams for this doc.
                        self.build_gin_index(&gin_field);
                    } else if let Some(text) = text_opt {
                        // New node: incremental O(trigrams) insert.
                        if let Some(gin_idx) = self.gin_indexes.get_mut(gin_field.as_str()) {
                            gin_idx.insert_doc(hash, &text);
                        } else {
                            // GIN not yet built (e.g. first doc after CREATE INDEX on empty).
                            self.build_gin_index(&gin_field);
                        }
                    }
                }
            }
        }

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

        // 1. Compact payload store: rebuild from live nodes only.
        // Must happen BEFORE build_snapshot() so the snapshot records the
        // new (post-compaction) offsets, not the pre-compaction ones.
        // Memory DB: rebuild Vec<u8> in-place.
        // Disk DB: streaming rewrite to payloads.bin.tmp then atomic rename.
        // Neither approach loads all payloads into RAM simultaneously.
        let node_keys: Vec<u64> = self.nodes.keys().copied().collect();
        if self.payload_store.is_disk() {
            // Disk-backed: stream each live node's bytes through a temp file.
            let pay_tmp  = dir.join("payloads.bin.tmp");
            let pay_path = dir.join("payloads.bin");
            let mut node_new_offsets: Vec<(u64, u64, u32)> = Vec::new(); // (hash, off, len)
            let mut write_cursor = 0u64;
            {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::FileExt;
                    let tmp_file = std::fs::OpenOptions::new()
                        .read(true).write(true).create(true).truncate(true)
                        .open(&pay_tmp)?;
                    for &h in &node_keys {
                        if let Some(node) = self.nodes.get(&h) {
                            if let Some(bytes) = self.payload_store.get_raw(
                                node.payload_offset, node.payload_len)
                            {
                                tmp_file.write_all_at(&bytes, write_cursor)?;
                                node_new_offsets.push((h, write_cursor, bytes.len() as u32));
                                write_cursor += bytes.len() as u64;
                            }
                        }
                    }
                }
                #[cfg(not(unix))]
                let _ = write_cursor; // non-unix fallback — no-op
            }
            // Apply the new offsets now that tmp_file is closed.
            for &(h, new_off, new_len) in &node_new_offsets {
                if let Some(node) = self.nodes.get_mut(&h) {
                    node.payload_offset = new_off;
                    node.payload_len    = new_len;
                }
            }
            // Atomically replace file, then reopen.
            std::fs::rename(&pay_tmp, &pay_path)?;
            self.payload_store = PayloadStore::open_existing(&pay_path, write_cursor)?;
        } else {
            // Memory DB: rebuild Vec<u8> without touching disk.
            let mut new_slab: Vec<u8> = Vec::new();
            for h in node_keys {
                if let Some(node) = self.nodes.get(&h) {
                    let old_off = node.payload_offset;
                    let old_len = node.payload_len;
                    if let Some(bytes) = self.payload_store.get_raw(old_off, old_len) {
                        let new_off = new_slab.len() as u64;
                        new_slab.extend_from_slice(&bytes);
                        if let Some(n) = self.nodes.get_mut(&h) {
                            n.payload_offset = new_off;
                            n.payload_len    = old_len;
                        }
                    }
                }
            }
            self.payload_store.reset(new_slab);
        }

        // 2. Write snapshot atomically (tmp → rename) — AFTER payload compaction
        //    so disk-backed SnapNode offsets match the new payloads.bin layout.
        let snap_json = serde_json::to_vec(&self.build_snapshot())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let snap_tmp = dir.join("snapshot.json.tmp");
        let snap_path = dir.join("snapshot.json");
        std::fs::write(&snap_tmp, &snap_json)?;
        std::fs::rename(&snap_tmp, &snap_path)?;

        // 3. Truncate WAL: close current writer → rename → open fresh → delete old
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

        // Regenerate gin.bin so the next open loads GIN instantly.
        if let Some(ref gin_bin_path) = self.data_dir.as_ref().map(|d| d.join("gin.bin")) {
            let _ = self.save_gin_binary(gin_bin_path);
        }

        Ok(())
    }

    /// Save all current GIN indexes to a compact binary sidecar `gin.bin`.
    ///
    /// The file format uses RoaringBitmap's native binary serialization, which
    /// is ~10-50× smaller and faster to load than JSON integer arrays.
    /// Called automatically after GIN is rebuilt so future opens skip the rebuild.
    fn save_gin_binary(&self, path: &Path) -> io::Result<()> {
        use std::io::Write;
        let tmp = path.with_extension("bin.tmp");
        let mut f = std::io::BufWriter::new(
            std::fs::OpenOptions::new().write(true).create(true).truncate(true).open(&tmp)?
        );
        // Magic header
        f.write_all(b"SKGIN001")?;
        // Number of GIN indexes
        f.write_all(&(self.gin_indexes.len() as u32).to_le_bytes())?;
        for gin in self.gin_indexes.values() {
            gin.write_binary(&mut f, GIN_INDEX_VERSION)?;
        }
        f.flush()?;
        drop(f);
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Load GIN indexes from the binary sidecar `gin.bin`.
    ///
    /// Returns `true` if the file was successfully loaded (all indexes had
    /// matching versions), `false` if missing, corrupt, or version-mismatched
    /// (caller should then call `rebuild_declared_gin_indexes` + `save_gin_binary`).
    fn load_gin_binary(&mut self, path: &Path) -> bool {
        use std::io::Read;
        let data = match std::fs::read(path) {
            Ok(d) => d,
            Err(_) => return false,
        };
        if data.len() < 12 || &data[..8] != b"SKGIN001" {
            return false;
        }
        let mut cursor = std::io::Cursor::new(&data[8..]);
        let mut count_buf = [0u8; 4];
        if cursor.read_exact(&mut count_buf).is_err() { return false; }
        let count = u32::from_le_bytes(count_buf) as usize;
        let mut loaded = HashMap::new();
        for _ in 0..count {
            match GINIndex::read_binary(&mut cursor, GIN_INDEX_VERSION) {
                Ok((field, idx)) => { loaded.insert(field, idx); }
                Err(_) => return false,
            }
        }
        // Only accept if all declared fields are present
        let declared_ok = self.schemas.values()
            .flat_map(|s| s.indexes.fulltext.iter())
            .all(|f| loaded.contains_key(f));
        if !declared_ok {
            return false;
        }
        for (field, idx) in loaded {
            self.record_index_version("gin", &field, GIN_INDEX_VERSION);
            self.gin_indexes.insert(field, idx);
        }
        true
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
        let is_disk = self.payload_store.is_disk();
        let nodes: Vec<SnapNode> = if is_disk {
            // Disk-backed: payloads live in payloads.bin — only store metadata.
            self.nodes.values().map(|n| SnapNode {
                slug:           n.slug.clone(),
                payload:        None,
                payload_offset: Some(n.payload_offset),
                payload_len:    Some(n.payload_len),
                collection:     Some(n.collection.clone()),
                spatial_meta:   n.spatial_meta.clone(),
            }).collect()
        } else {
            self.nodes
                .values()
                .filter_map(|n| {
                    self.payload_store
                        .get(n.payload_offset, n.payload_len)
                        .map(|payload| SnapNode {
                            slug: n.slug.clone(),
                            payload: Some(payload),
                            payload_offset: None,
                            payload_len:    None,
                            collection:     None,
                            spatial_meta:   None,
                        })
                })
                .collect()
        };

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
            .map(|(field, graph)| {
                let (m, ef) = self.hnsw_params.get(field).copied().unwrap_or((16, 200));
                SnapHnsw {
                    field: field.clone(),
                    version: HNSW_INDEX_VERSION,
                    m,
                    ef_construction: ef,
                    graph: graph.clone(),
                }
            })
            .collect();

        // Persist btree indexes for disk-backed snapshots (avoids re-scan on reload).
        let snap_btree: Option<Vec<SnapBtree>> = if is_disk && !self.field_indexes.is_empty() {
            Some(self.field_indexes.iter().map(|((coll_hash, field), btree)| {
                SnapBtree {
                    collection_hash: *coll_hash,
                    field: field.clone(),
                    entries: btree.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                }
            }).collect())
        } else {
            None
        };

        Snapshot {
            version: SNAPSHOT_FORMAT_VERSION,
            is_disk_backed: is_disk,
            nodes,
            edges,
            schemas: Some(self.schemas.values().cloned().collect()),
            vectors: if snap_vectors.is_empty() { None } else { Some(snap_vectors) },
            hnsw_indexes: if snap_hnsw.is_empty() { None } else { Some(snap_hnsw) },
            btree_indexes: snap_btree,
            gin_indexes: Ignored,
        }
    }

    fn load_snapshot(&mut self, snap: Snapshot) {
        for n in snap.nodes {
            if snap.is_disk_backed {
                // Disk-backed: restore NodeData from metadata; payload bytes are
                // already in payloads.bin at the stored offset.
                if let (Some(offset), Some(len)) = (n.payload_offset, n.payload_len) {
                    let hash = sk_hash(&n.slug);
                    let coll = n.collection.clone().unwrap_or_default();
                    let coll_hash = if coll.is_empty() { 0 } else { sk_hash(&coll) };
                    self.nodes.insert(hash, NodeData {
                        slug:           n.slug.clone(),
                        collection:     coll.clone(),
                        spatial_meta:   n.spatial_meta,
                        payload_offset: offset,
                        payload_len:    len,
                    });
                    if !coll.is_empty() {
                        self.collections.entry(coll_hash).or_default().push(hash);
                        self.collection_names_map.entry(coll_hash)
                            .or_insert_with(|| coll.clone());
                    }
                }
            } else if let Some(payload) = n.payload {
                let _ = self.put_raw(&n.slug, &payload.to_string());
            }
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
        // Restore HNSW graphs — rebuild if the stored version doesn't match.
        if let Some(hnsw_list) = snap.hnsw_indexes {
            for sh in hnsw_list {
                if sh.version == HNSW_INDEX_VERSION {
                    self.hnsw_params.insert(sh.field.clone(), (sh.m, sh.ef_construction));
                    self.hnsw_indexes.insert(sh.field, sh.graph);
                } else {
                    // Version mismatch — rebuild from stored vectors.
                    let _ = self.build_hnsw_index(&sh.field, sh.m, sh.ef_construction);
                }
            }
        }
        // Restore persisted btree indexes (disk-backed snapshots only).
        // This avoids re-scanning payloads.bin to rebuild them.
        let has_snap_btree = snap.btree_indexes.is_some();
        if let Some(btrees) = snap.btree_indexes {
            for sb in btrees {
                let btmap: std::collections::BTreeMap<FieldKey, Vec<u64>> =
                    sb.entries.into_iter().collect();
                self.field_indexes.insert((sb.collection_hash, sb.field), btmap);
            }
        }

        // Rebuild btree field indexes — only when stored version mismatches,
        // or when no btree snapshot was present (legacy snapshot or new index).
        let btree_rebuild: Vec<(String, String)> = self
            .schemas
            .values()
            .flat_map(|s| s.indexes.range.iter().map(|f| {
                let v = s.indexes.build_versions.get(&format!("btree:{f}")).copied().unwrap_or(0);
                (s.collection.clone(), f.clone(), v)
            }))
            .filter(|(c, f, v)| {
                if has_snap_btree {
                    // Already restored from snapshot — only rebuild on version mismatch
                    *v != BTREE_INDEX_VERSION
                } else {
                    // No btree snapshot — rebuild everything
                    let _ = (c, f);
                    true
                }
            })
            .map(|(c, f, _)| (c, f))
            .collect();
        for (coll, field) in btree_rebuild {
            self.build_field_index(&coll, &field);
        }

        // Rebuild BM25 indexes — only when stored version mismatches.
        let bm25_rebuild: Vec<String> = {
            let mut seen = std::collections::HashSet::new();
            self.schemas.values()
                .flat_map(|s| s.indexes.bm25.iter().filter(|f| {
                    s.indexes.build_versions.get(&format!("bm25:{f}")).copied().unwrap_or(0)
                        != BM25_INDEX_VERSION
                }).cloned())
                .filter(|f| seen.insert(f.clone()))
                .collect()
        };
        for field in bm25_rebuild { self.build_bm25_index(&field); }

        // Rebuild GIN indexes — only when stored version mismatches.
        let gin_rebuild: Vec<String> = {
            let mut seen = std::collections::HashSet::new();
            self.schemas.values()
                .flat_map(|s| s.indexes.fulltext.iter().filter(|f| {
                    s.indexes.build_versions.get(&format!("gin:{f}")).copied().unwrap_or(0)
                        != GIN_INDEX_VERSION
                }).cloned())
                .filter(|f| seen.insert(f.clone()))
                .collect()
        };
        for field in gin_rebuild { self.build_gin_index(&field); }
    }

    // ── Reads ─────────────────────────────────────────────────────────────────

    /// Get raw JSON payload for a slug. Returns `None` if not found.
    pub fn get(&self, slug: &str) -> Option<String> {
        let node = self.nodes.get(&sk_hash(slug))?;
        self.payload_store
            .get_raw(node.payload_offset, node.payload_len)
            .map(|b| String::from_utf8_lossy(&b).into_owned())
    }

    /// Parse and return the JSON payload for a node hash. Returns `None` if
    /// the node does not exist or the payload cannot be parsed.
    pub(crate) fn get_payload(&self, hash: u64) -> Option<Value> {
        let node = self.nodes.get(&hash)?;
        self.payload_store.get(node.payload_offset, node.payload_len)
    }

    /// Return the raw JSON bytes for a node's payload, along with (offset, len).
    /// Used by the fast field-extraction path in collect() to avoid full JSON parsing.
    pub(crate) fn get_payload_raw(&self, hash: u64) -> Option<(Vec<u8>, u64, u32)> {
        let node = self.nodes.get(&hash)?;
        let bytes = self.payload_store.get_raw(node.payload_offset, node.payload_len)?;
        Some((bytes, node.payload_offset, node.payload_len))
    }

    /// For large payloads, read just a head slice and a tail slice to extract fields
    /// without loading the full payload (e.g. avoids reading a 12 MB geometry blob).
    pub(crate) fn get_payload_head_tail(
        &self,
        hash: u64,
        head_bytes: usize,
        tail_bytes: usize,
    ) -> Option<(Vec<u8>, Vec<u8>)> {
        let node = self.nodes.get(&hash)?;
        let len = node.payload_len as usize;
        let off = node.payload_offset;
        let head_size = head_bytes.min(len);
        let tail_size = tail_bytes.min(len);
        // If the ranges overlap (small payload), just read the full thing once.
        if head_size + tail_size >= len {
            let full = self.payload_store.get_raw(off, len as u32)?;
            return Some((full.clone(), full));
        }
        let head = self.payload_store.get_raw_at(off, head_size)?;
        let tail_off = off + (len - tail_size) as u64;
        let tail = self.payload_store.get_raw_at(tail_off, tail_size)?;
        Some((head, tail))
    }

    /// Read only the first `head_bytes` of each payload for multiple nodes.
    ///
    /// Used for field extraction when only small metadata fields are needed
    /// (e.g. `level`, `name`, `pcode`), avoiding reading multi-MB GeoJSON blobs.
    /// Sorts by payload offset for sequential I/O.
    /// Zero-copy tail slice for a single node (mmap path only).
    #[cfg(unix)]
    pub(crate) fn payload_tail_slice(&self, hash: u64, tail_bytes: usize) -> Option<&[u8]> {
        let node = self.nodes.get(&hash)?;
        let len = node.payload_len as usize;
        if len <= tail_bytes {
            self.payload_store.get_slice(node.payload_offset, len)
        } else {
            let tail_off = node.payload_offset + (len - tail_bytes) as u64;
            self.payload_store.get_slice(tail_off, tail_bytes)
        }
    }

    /// Read the last `tail_bytes` of each node's payload in offset order.
    ///
    /// For JSON payloads where scalar metadata fields (level, name, pcode) appear
    /// AFTER large embedded objects like GeoJSON geometry, reading the tail is
    /// much more effective than reading the head.
    pub(crate) fn read_payload_tails_batched(
        &self,
        hashes: &[u64],
        tail_bytes: usize,
    ) -> HashMap<u64, Vec<u8>> {
        let mut sorted: Vec<(u64, u64, u32)> = hashes
            .iter()
            .filter_map(|&h| {
                self.nodes.get(&h).map(|nd| (h, nd.payload_offset, nd.payload_len))
            })
            .collect();
        sorted.sort_unstable_by_key(|&(_, off, _)| off);

        let mut result = HashMap::with_capacity(hashes.len());

        for &(hash, off, len) in &sorted {
            let len_usize = len as usize;
            if len_usize <= tail_bytes {
                // Small payload — read the whole thing.
                if let Some(raw) = self.payload_store.get_raw(off, len) {
                    result.insert(hash, raw);
                }
            } else {
                // Large payload — read only the tail.
                let tail_off = off + (len_usize - tail_bytes) as u64;
                if let Some(raw) = self.payload_store.get_raw_at(tail_off, tail_bytes) {
                    result.insert(hash, raw);
                }
            }
        }

        result
    }

    /// Read raw JSON bytes for multiple nodes with minimal I/O syscalls.
    ///
    /// Sorts hashes by `payload_offset`, groups nodes whose payloads are
    /// close together (gap ≤ `MAX_GAP`) into one batch, and issues a single
    /// `pread` per batch instead of one syscall per node.
    ///
    /// For sequentially-inserted data (the common case), all payloads in a
    /// collection are contiguous in `payloads.bin`, so the entire collection
    /// can be read in **one** syscall rather than O(N).
    ///
    /// Returns a `HashMap<u64, Vec<u8>>` of raw JSON bytes keyed by node hash.
    pub(crate) fn read_raw_payloads_batched(&self, hashes: &[u64]) -> HashMap<u64, Vec<u8>> {
        /// Bridge gaps between payload regions up to this many bytes.
        const MAX_GAP: u64 = 16 * 1024;
        /// Cap each batch read at 32 MB to keep peak RAM bounded.
        const MAX_BATCH: usize = 32 * 1024 * 1024;

        // Sort candidates by payload_offset for sequential I/O.
        let mut sorted: Vec<(u64, u64, u32)> = hashes
            .iter()
            .filter_map(|&h| {
                self.nodes
                    .get(&h)
                    .map(|nd| (h, nd.payload_offset, nd.payload_len))
            })
            .collect();
        sorted.sort_unstable_by_key(|&(_, off, _)| off);

        let mut result = HashMap::with_capacity(hashes.len());
        let mut i = 0;

        while i < sorted.len() {
            let batch_off = sorted[i].1;
            let mut j = i + 1;
            let mut batch_end = sorted[i].1 + sorted[i].2 as u64;

            // Extend batch while gap and size constraints hold.
            while j < sorted.len() {
                let (_, next_off, next_len) = sorted[j];
                if next_off.saturating_sub(batch_end) > MAX_GAP {
                    break;
                }
                let cand_end = next_off + next_len as u64;
                if (cand_end.saturating_sub(batch_off)) as usize > MAX_BATCH {
                    break;
                }
                batch_end = batch_end.max(cand_end);
                j += 1;
            }

            // One read for the entire contiguous region.
            let batch_len = (batch_end - batch_off) as usize;
            if let Some(buf) = self.payload_store.get_raw_at(batch_off, batch_len) {
                for &(hash, off, len) in &sorted[i..j] {
                    let start = (off - batch_off) as usize;
                    let end = start + len as usize;
                    if end <= buf.len() {
                        result.insert(hash, buf[start..end].to_vec());
                    }
                }
            } else {
                // Fallback: read each node individually on I/O error.
                for &(hash, off, len) in &sorted[i..j] {
                    if let Some(raw) = self.payload_store.get_raw(off, len) {
                        result.insert(hash, raw);
                    }
                }
            }
            i = j;
        }

        result
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
            if !node.collection.is_empty() {
                names.insert(node.collection.clone());
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
            if node.collection.is_empty() || sk_hash(&node.collection) != col_h { continue; }
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
                    .map(|n| !n.collection.is_empty() && sk_hash(&n.collection) == to_col_h)
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
            if node.collection.is_empty() || sk_hash(&node.collection) != col_h { continue; }
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
            if node.collection.is_empty() { continue; }
            let from_col = node.collection.clone();
            if let Some(edges) = self.adj_fwd.get(&from_h) {
                for e in edges {
                    let edge_label = match self.edge_type_names.get(&e.edge_type) {
                        Some(l) => l.clone(),
                        None => continue,
                    };
                    let to_col = match self.nodes.get(&e.other) {
                        Some(n) if !n.collection.is_empty() => n.collection.clone(),
                        _ => continue,
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

    /// Execute a SQL query and return a lazy [`Set`].
    ///
    /// Accepts all SekejapQL query forms:
    ///
    /// ```text
    /// -- Standard SELECT
    /// SELECT * FROM collection [WHERE ...] [ORDER BY ...] [LIMIT n]
    ///
    /// -- Graph aggregate
    /// SELECT b._key AS name, SUM(r.weight) AS total
    /// FROM MATCH (a:col)-[r:edge]->(b:col) [WHERE a._key = 'val']
    /// [GROUP BY b._key] [ORDER BY total DESC] [LIMIT n]
    ///
    /// -- Graph aggregate with WITH chaining (multi-stage)
    /// SELECT c.name AS city, COUNT(*) AS friends
    /// FROM MATCH (a:users)-[:knows*1..3]->(b:users)
    /// WHERE a._key = 'alice'
    /// WITH b
    /// MATCH (b)-[:lives_in]->(c:cities)
    /// WHERE c.population > 100000
    /// GROUP BY c.name ORDER BY friends DESC LIMIT 10
    ///
    /// -- MATCH...RETURN (Cypher-style, routed through query())
    /// MATCH (a:col)-[:edge]->(b:col) RETURN a._key AS name, b.score AS val
    /// MATCH (a:col)-[:e]->(b) WITH b MATCH (b)-[:e2]->(c) RETURN c._key AS dest
    ///
    /// -- Shortest path (0 rows = unreachable, 1 row = found)
    /// SELECT a.field AS from_f, b.field AS to_f, r.length AS hops
    /// FROM MATCH SHORTEST (a)-[r*]->(b)
    /// WHERE a._key = 'start/slug' AND b._key = 'end/slug'
    /// [AND ANY(n IN nodes(r) WHERE n.field op val)]
    ///
    /// -- Multi-FROM cross-join
    /// SELECT a.field AS af, b.field AS bf
    /// FROM MATCH (a:col)-[:edge]->(b), collection_name AS alias
    ///
    /// -- Supported return expressions
    /// var.field | COUNT(*) | SUM(math) | AVG(math) | MIN(math) | MAX(math)
    /// PATH_AVG(var.field) | PATH_SUM | PATH_MIN | PATH_MAX | PATH_PRODUCT
    /// PATH_FIRST(var.field) | PATH_LAST(var.field)
    /// CASE WHEN var.field op literal THEN literal [WHEN ...] [ELSE literal] END
    /// AGE_DAYS(var.field) | AGE_HOURS(var.field) | NOW()
    /// JSON_ARRAY_LENGTH(var.field)
    /// ```
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
            sql::MatchOrAgg::Shortest(stmt) => {
                let hits = query::execute_shortest_select(self, stmt);
                Ok(Set::from_hits(self, hits))
            }
            sql::MatchOrAgg::MultiFrom(stmt) => {
                let hits = query::execute_multi_from(self, stmt);
                Ok(Set::from_hits(self, hits))
            }
            sql::MatchOrAgg::Steps(steps) => {
                Ok(Set::from_steps(self, steps))
            }
        }
    }

    /// Parameterized SELECT / MATCH query.
    ///
    /// Values are bound to `$1`, `$2`, … placeholders in the SQL string.
    /// Parameters are resolved at parse time — the execution layer is unchanged.
    ///
    /// # Example
    /// ```
    /// # use sekejap::CoreDB;
    /// # use serde_json::json;
    /// let mut db = CoreDB::new();
    /// db.put("users/alice", r#"{"name":"Alice","age":30,"_collection":"users"}"#).unwrap();
    /// let hits = db.query_params(
    ///     "SELECT * FROM users WHERE name = $1",
    ///     &[json!("Alice")],
    /// ).unwrap().collect();
    /// assert_eq!(hits[0].slug, "users/alice");
    /// ```
    pub fn query_params(&self, sql: &str, params: &[Value]) -> Result<Set<'_>, SqlError> {
        match sql::parse_match_or_agg_params(sql, params.to_vec())? {
            sql::MatchOrAgg::Agg(stmt) => {
                let hits = query::execute_match_agg(self, stmt);
                Ok(Set::from_hits(self, hits))
            }
            sql::MatchOrAgg::Shortest(stmt) => {
                let hits = query::execute_shortest_select(self, stmt);
                Ok(Set::from_hits(self, hits))
            }
            sql::MatchOrAgg::MultiFrom(stmt) => {
                let hits = query::execute_multi_from(self, stmt);
                Ok(Set::from_hits(self, hits))
            }
            sql::MatchOrAgg::Steps(steps) => {
                Ok(Set::from_steps(self, steps))
            }
        }
    }

    // ── Graph path queries ────────────────────────────────────────────────────

    /// BFS from `start` to `end`, tracking the parent pointer and edge used at
    /// each hop so the path can be reconstructed.
    ///
    /// Returns `None` when no path exists.
    /// Returns a zero-hop `BfsPath` when `start == end`.
    pub(crate) fn bfs_shortest_path(&self, start: u64, end: u64) -> Option<BfsPath> {
        use std::collections::{HashMap, VecDeque};

        // Sentinel: parent for the start node points to itself with a zero
        // edge_type hash so we can detect "we are at the root" during
        // reconstruction without a separate visited set.
        // (from_hash, edge_type_hash, strength, meta)
        let mut parent: HashMap<u64, (u64, u64, f32, Option<Value>)> = HashMap::new();

        // Same-node degenerate case
        if start == end {
            if let Some(node) = self.nodes.get(&start) {
                let hit = query::Hit {
                    slug: node.slug.clone(),
                    slug_hash: start,
                    payload: self.payload_store.get(node.payload_offset, node.payload_len),
                };
                return Some(BfsPath { nodes: vec![hit], edges: vec![], length: 0 });
            } else {
                return None; // start node doesn't exist
            }
        }

        // The start node must exist
        if !self.nodes.contains_key(&start) {
            return None;
        }

        parent.insert(start, (start, 0, 0.0, None)); // sentinel
        let mut queue: VecDeque<u64> = VecDeque::new();
        queue.push_back(start);

        while let Some(current) = queue.pop_front() {
            if let Some(edges) = self.adj_fwd.get(&current) {
                for e in edges {
                    if parent.contains_key(&e.other) {
                        continue; // already visited
                    }
                    parent.insert(e.other, (current, e.edge_type, e.strength, e.meta.clone()));
                    if e.other == end {
                        // Reconstruct path: walk parent map from end → start, then reverse.
                        let mut node_hashes: Vec<u64> = Vec::new();
                        let mut cur = end;
                        loop {
                            node_hashes.push(cur);
                            let (prev, _, _, _) = parent[&cur];
                            if prev == cur {
                                break; // reached the sentinel (start node)
                            }
                            cur = prev;
                        }
                        node_hashes.reverse();

                        // Build Hit list from the ordered hashes
                        let nodes: Vec<query::Hit> = node_hashes
                            .iter()
                            .filter_map(|&h| {
                                self.nodes.get(&h).map(|n| query::Hit {
                                    slug: n.slug.clone(),
                                    slug_hash: h,
                                    payload: self.payload_store.get(n.payload_offset, n.payload_len),
                                })
                            })
                            .collect();

                        // Build EdgeHit list: edges[i] connects nodes[i] → nodes[i+1]
                        let edges: Vec<EdgeHit> = node_hashes
                            .windows(2)
                            .map(|w| {
                                let (_, edge_type_hash, strength, meta) = parent[&w[1]].clone();
                                EdgeHit {
                                    from_slug: self.nodes.get(&w[0]).map(|n| n.slug.clone()),
                                    to_slug: self.nodes.get(&w[1]).map(|n| n.slug.clone()),
                                    edge_type: self.edge_type_names.get(&edge_type_hash).cloned(),
                                    edge_type_hash,
                                    strength,
                                    meta,
                                }
                            })
                            .collect();

                        let length = edges.len();
                        return Some(BfsPath { nodes, edges, length });
                    }
                    queue.push_back(e.other);
                }
            }
        }

        None // no path found
    }

    /// Execute a `SHOW` introspection statement.
    ///
    /// Syntax:
    /// ```text
    /// SHOW TABLES
    ///     → [{name, count}, ...]  — all collections with row counts (includes declared-empty tables)
    ///
    /// SHOW EDGES
    ///     → [{from, type, to, count}, ...]  — full graph schema with edge counts
    ///
    /// SHOW EDGES FROM collection
    ///     → [{from, type, count}, ...]  — edge types leaving that collection + counts
    ///
    /// SHOW EDGES FROM col1 TO col2
    ///     → [{from, type, to, count}, ...]  — edge types between two collections + counts
    ///
    /// SHOW CREATE TABLE collection
    ///     → [{ddl: "CREATE TABLE ..."}]  — DDL that recreates the declared schema
    ///
    /// SHOW collection
    ///     → [{field, type, primary_key?, source}, ...]
    ///       Uses declared schema if CREATE TABLE was issued; otherwise infers
    ///       types from actual node data. source = "declared" | "inferred".
    /// ```
    pub fn show(&self, sql: &str) -> Result<Vec<query::Hit>, SqlError> {
        let stmt = sql::parse_show(sql)?;

        let make_hit = |payload: serde_json::Value| query::Hit {
            slug: String::new(),
            slug_hash: 0,
            payload: Some(payload),
        };

        match stmt {
            // ── SHOW TABLES ───────────────────────────────────────────────────
            sql::ShowStmt::Tables => {
                // Use collection_names_map (O(1) per collection) — no node scan needed.
                // Insert actual counts first, then seed declared-but-empty schemas with 0.
                let mut counts: std::collections::BTreeMap<String, usize> =
                    std::collections::BTreeMap::new();
                for (hash, name) in &self.collection_names_map {
                    let count = self.collections.get(hash).map(|v| v.len()).unwrap_or(0);
                    counts.insert(name.clone(), count);
                }
                for name in self.schemas.keys() {
                    counts.entry(name.clone()).or_insert(0);
                }
                Ok(counts.into_iter()
                    .map(|(name, count)| make_hit(serde_json::json!({ "name": name, "count": count })))
                    .collect())
            }

            // ── SHOW EDGES ────────────────────────────────────────────────────
            sql::ShowStmt::Edges(e) => {
                match (e.from_col, e.to_col) {
                    (None, _) => {
                        // Full schema — count all edges per (from, type, to) triple
                        let mut counts: std::collections::HashMap<(String, String, String), usize> =
                            std::collections::HashMap::new();
                        for (&from_h, node) in &self.nodes {
                            let from_col = if node.collection.is_empty() {
                                continue;
                            } else {
                                node.collection.clone()
                            };
                            if let Some(edges) = self.adj_fwd.get(&from_h) {
                                for edge in edges {
                                    let label = match self.edge_type_names.get(&edge.edge_type) {
                                        Some(l) => l.clone(),
                                        None => continue,
                                    };
                                    let to_col = match self.nodes.get(&edge.other)
                                        .map(|n| &n.collection)
                                    {
                                        Some(c) if !c.is_empty() => c.clone(),
                                        _ => continue,
                                    };
                                    *counts.entry((from_col.clone(), label, to_col)).or_insert(0) += 1;
                                }
                            }
                        }
                        let mut hits: Vec<_> = counts.into_iter()
                            .map(|((from, kind, to), count)| make_hit(serde_json::json!({
                                "from": from, "type": kind, "to": to, "count": count
                            })))
                            .collect();
                        hits.sort_by(|a, b| {
                            let ka = a.payload.as_ref().and_then(|p| p["from"].as_str()).unwrap_or("");
                            let kb = b.payload.as_ref().and_then(|p| p["from"].as_str()).unwrap_or("");
                            ka.cmp(kb)
                        });
                        Ok(hits)
                    }
                    (Some(from_col), None) => {
                        // Types leaving one collection + counts
                        let col_h = sk_hash(&from_col);
                        let mut counts: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        for (&node_h, node) in &self.nodes {
                            if !node.collection.is_empty() && sk_hash(&node.collection) == col_h {
                                if let Some(edges) = self.adj_fwd.get(&node_h) {
                                    for edge in edges {
                                        if let Some(label) = self.edge_type_names.get(&edge.edge_type) {
                                            *counts.entry(label.clone()).or_insert(0) += 1;
                                        }
                                    }
                                }
                            }
                        }
                        let mut hits: Vec<_> = counts.into_iter()
                            .map(|(kind, count)| make_hit(serde_json::json!({
                                "from": from_col, "type": kind, "count": count
                            })))
                            .collect();
                        hits.sort_by(|a, b| {
                            let ka = a.payload.as_ref().and_then(|p| p["type"].as_str()).unwrap_or("");
                            let kb = b.payload.as_ref().and_then(|p| p["type"].as_str()).unwrap_or("");
                            ka.cmp(kb)
                        });
                        Ok(hits)
                    }
                    (Some(from_col), Some(to_col)) => {
                        // Types between two collections + counts
                        let from_h = sk_hash(&from_col);
                        let to_col_h = sk_hash(&to_col);
                        let mut counts: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        for (&node_h, node) in &self.nodes {
                            if !node.collection.is_empty() && sk_hash(&node.collection) == from_h {
                                if let Some(edges) = self.adj_fwd.get(&node_h) {
                                    for edge in edges {
                                        let in_to = self.nodes.get(&edge.other)
                                            .map(|n| !n.collection.is_empty() && sk_hash(&n.collection) == to_col_h)
                                            .unwrap_or(false);
                                        if in_to {
                                            if let Some(label) = self.edge_type_names.get(&edge.edge_type) {
                                                *counts.entry(label.clone()).or_insert(0) += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        let mut hits: Vec<_> = counts.into_iter()
                            .map(|(kind, count)| make_hit(serde_json::json!({
                                "from": from_col, "type": kind, "to": to_col, "count": count
                            })))
                            .collect();
                        hits.sort_by(|a, b| {
                            let ka = a.payload.as_ref().and_then(|p| p["type"].as_str()).unwrap_or("");
                            let kb = b.payload.as_ref().and_then(|p| p["type"].as_str()).unwrap_or("");
                            ka.cmp(kb)
                        });
                        Ok(hits)
                    }
                }
            }

            // ── SHOW CREATE TABLE <collection> ───────────────────────────────
            sql::ShowStmt::CreateTable(collection) => {
                let ddl = self.schema_ddl(&collection).unwrap_or_else(|| {
                    format!("-- no CREATE TABLE declared for '{collection}'")
                });
                Ok(vec![make_hit(serde_json::json!({ "ddl": ddl }))])
            }

            // ── SHOW <collection> ─────────────────────────────────────────────
            sql::ShowStmt::Collection(collection) => {
                // Declared schema takes priority
                if let Some(schema) = self.schemas.get(&collection) {
                    let hits = schema.fields.iter().map(|f| {
                        let ty = match f.ty {
                            sql::FieldType::Text        => "TEXT",
                            sql::FieldType::Integer     => "INTEGER",
                            sql::FieldType::Real        => "REAL",
                            sql::FieldType::Timestamptz => "TIMESTAMPTZ",
                            sql::FieldType::Geo         => "GEO",
                            sql::FieldType::Vector      => "VECTOR",
                            sql::FieldType::Json        => "JSON",
                        };
                        make_hit(serde_json::json!({
                            "field": f.name,
                            "type": ty,
                            "primary_key": f.is_primary_key,
                            "source": "declared",
                        }))
                    }).collect();
                    return Ok(hits);
                }

                // Inferred from data — scan nodes in collection
                let col_h = sk_hash(&collection);
                const SKIP: &[&str] = &["_collection", "_id", "_created_unix", "_updated_unix"];
                let mut field_types: std::collections::BTreeMap<String, &'static str> =
                    std::collections::BTreeMap::new();

                for node in self.nodes.values() {
                    if !node.collection.is_empty() && sk_hash(&node.collection) == col_h {
                        if let Some(payload) = self.payload_store.get(node.payload_offset, node.payload_len) {
                            if let serde_json::Value::Object(map) = payload {
                                for (k, v) in &map {
                                    if SKIP.contains(&k.as_str()) { continue; }
                                    let inferred = match v {
                                        serde_json::Value::String(_) => "TEXT",
                                        serde_json::Value::Number(n)
                                            if n.is_i64() || n.is_u64() => "INTEGER",
                                        serde_json::Value::Number(_) => "REAL",
                                        serde_json::Value::Bool(_) => "BOOLEAN",
                                        serde_json::Value::Array(a)
                                            if a.iter().all(|x| x.is_number()) => "VECTOR",
                                        serde_json::Value::Array(_)
                                        | serde_json::Value::Object(_) => "JSON",
                                        serde_json::Value::Null => continue,
                                    };
                                    field_types.entry(k.clone()).or_insert(inferred);
                                }
                            }
                        }
                    }
                }

                Ok(field_types.into_iter()
                    .map(|(field, ty)| make_hit(serde_json::json!({
                        "field": field,
                        "type": ty,
                        "source": "inferred",
                    })))
                    .collect())
            }
        }
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
        let mutation = sql::parse_mutation(sql)?;
        self.execute_mutation(mutation)
    }

    /// Parameterized mutation (INSERT / UPDATE / DELETE).
    ///
    /// Values are bound to `$1`, `$2`, … placeholders in the SQL string.
    ///
    /// # Example
    /// ```
    /// # use sekejap::CoreDB;
    /// # use serde_json::json;
    /// let mut db = CoreDB::new();
    /// db.execute("CREATE TABLE users (_key TEXT PRIMARY KEY, name TEXT, age INTEGER)").unwrap();
    /// let n = db.execute_params(
    ///     "INSERT INTO users (_key, name, age) VALUES ($1, $2, $3)",
    ///     &[json!("u1"), json!("Bob"), json!(30)],
    /// ).unwrap();
    /// assert_eq!(n, 1);
    /// ```
    pub fn execute_params(&mut self, sql: &str, params: &[Value]) -> Result<usize, SqlError> {
        // Re-parse with params, then delegate to the same execution arms.
        // We cannot just call self.execute() because it would re-parse without params.
        match sql::parse_mutation_params(sql, params.to_vec())? {
            m => {
                // Build a temporary SQL string? No — reuse the mutation directly.
                // We need to inline the same execute() body. Instead, factor via a helper.
                self.execute_mutation(m)
            }
        }
    }

    /// Internal: execute an already-parsed mutation.
    fn execute_mutation(&mut self, mutation: sql::CompiledMutation) -> Result<usize, SqlError> {
        match mutation {
            sql::CompiledMutation::Insert { collection, mut slug, payload_json, vectors } => {
                let payload_json = if let Some(schema) = self.schemas.get(&collection).cloned() {
                    let mut payload: Value = serde_json::from_str(&payload_json)
                        .map_err(|e| SqlError::InvalidValue(e.to_string()))?;
                    if let Value::Object(ref mut map) = payload {
                        for field in &schema.fields {
                            if map.contains_key(&field.name) {
                                continue;
                            }
                            if field.default_uuid4 {
                                map.insert(
                                    field.name.clone(),
                                    Value::String(crate::scalar::uuid_v4()),
                                );
                            } else if let Some((ns, nm)) = &field.default_uuid5 {
                                map.insert(
                                    field.name.clone(),
                                    Value::String(crate::scalar::uuid_v5(ns, nm)),
                                );
                            }
                        }
                        if slug.is_empty() {
                            match map.get("_key").and_then(|v| v.as_str()) {
                                Some(key_val) => {
                                    slug = format!("{}/{}", collection, key_val);
                                    map.insert("_id".into(), Value::String(slug.clone()));
                                }
                                None => {
                                    return Err(SqlError::MissingField { field: "_key" });
                                }
                            }
                        }
                    }
                    if let Some(err) = validate_payload_against_schema(&schema, &payload) {
                        return Err(err);
                    }
                    serde_json::to_string(&payload)
                        .map_err(|e| SqlError::InvalidValue(e.to_string()))?
                } else if slug.is_empty() {
                    return Err(SqlError::MissingField { field: "_key" });
                } else {
                    payload_json
                };
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
                        let n = self.nodes.get(&h.slug_hash)?;
                        let payload = self.payload_store.get(n.payload_offset, n.payload_len)?;
                        Some((n.slug.clone(), payload))
                    })
                    .collect();
                let count = hits.len();
                for (slug, mut payload) in hits {
                    if let Some(coll) = payload.get("_collection").and_then(|v| v.as_str()) {
                        if let Some(schema) = self.schemas.get(coll) {
                            if let Some(err) = validate_updates_against_schema(schema, &updates) {
                                return Err(err);
                            }
                        }
                    }
                    let mut vec_updates: Vec<(String, Vec<f32>)> = Vec::new();
                    for (field, value) in &updates {
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
                self.apply_index(&collection, &method, &fields)?;
                self.wal_write(WalEntry::CreateIndex {
                    collection,
                    method: method.to_string(),
                    fields,
                });
                Ok(1)
            }
            sql::CompiledMutation::Reindex { collection, method, fields } => {
                self.apply_index(&collection, &method, &fields)?;
                Ok(1)
            }
            sql::CompiledMutation::DropTable { collection, if_exists } => {
                let has_schema = self.schemas.contains_key(&collection);
                let has_nodes  = self.collections
                    .get(&sk_hash(&collection))
                    .map(|v| !v.is_empty())
                    .unwrap_or(false);

                if !has_schema && !has_nodes {
                    if if_exists {
                        return Ok(0);
                    } else {
                        return Err(sql::SqlError::InvalidValue(
                            format!("table '{collection}' does not exist")
                        ));
                    }
                }

                let count = self.drop_table_raw(&collection);
                self.wal_write(WalEntry::DropTable { collection });
                Ok(count)
            }
            sql::CompiledMutation::DropIndex { collection, method, field, if_exists } => {
                let removed = self.drop_index_raw(&collection, &method, &field);
                if !removed && !if_exists {
                    return Err(sql::SqlError::InvalidValue(format!(
                        "index on '{field}' does not exist for table '{collection}'"
                    )));
                }
                if removed {
                    self.wal_write(WalEntry::DropIndex {
                        collection,
                        method: method.to_string(),
                        field,
                    });
                }
                Ok(if removed { 1 } else { 0 })
            }
            sql::CompiledMutation::AlterTable { collection, op } => {
                let op_json = serde_json::to_string(&op)
                    .map_err(|e| sql::SqlError::InvalidValue(e.to_string()))?;
                let count = self.alter_table_raw(&collection, op)?;
                self.wal_write(WalEntry::AlterTable { collection, op_json });
                Ok(count)
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

    /// Return the btree index for `(collection_hash, field)` if one exists.
    /// Used by the query executor for index-only scans (GROUP BY, DISTINCT, etc.).
    pub(crate) fn field_index(
        &self,
        coll_hash: u64,
        field: &str,
    ) -> Option<&BTreeMap<FieldKey, Vec<u64>>> {
        self.field_indexes.get(&(coll_hash, field.to_string()))
    }

    /// Convert a `FieldKey` to a `serde_json::Value` for result projection.
    pub(crate) fn field_key_to_value(key: &FieldKey) -> Value {
        match key {
            FieldKey::Null        => Value::Null,
            FieldKey::Bool(b)     => Value::Bool(*b),
            FieldKey::Number(OrdF64(f)) => {
                serde_json::Number::from_f64(*f)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            }
            FieldKey::Str(s)      => Value::String(s.clone()),
        }
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
        let items: Vec<(u64, geo::SpatialMeta)> = self.nodes.iter()
            .filter_map(|(&hash, node)| node.spatial_meta.clone().map(|m| (hash, m)))
            .collect();
        self.spatial_grid = Some(geo::SpatialGrid::build(items.into_iter()));
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
            if let Some(payload) = self.payload_store.get(node.payload_offset, node.payload_len) {
                extract_string_fields(&payload, "", &mut field_values, hash);
            }
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
            if let Some(payload) = self.get_payload(hash) {
                if let Some(text) = payload.get(field).and_then(|v| v.as_str()) {
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
        let owned: Vec<(u64, String)> = self
            .nodes
            .iter()
            .filter_map(|(&hash, node)| {
                let payload = self.payload_store.get(node.payload_offset, node.payload_len)?;
                payload.get(field)?.as_str().map(|s| (hash, s.to_string()))
            })
            .collect();
        if !owned.is_empty() {
            let refs: Vec<(u64, &str)> = owned.iter().map(|(h, s)| (*h, s.as_str())).collect();
            let index = GINIndex::build(refs.into_iter(), field);
            self.gin_indexes.insert(field.to_string(), index);
        }
        self.record_index_version("gin", field, GIN_INDEX_VERSION);
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
        // Belt-and-suspenders: filter out hashes whose nodes were deleted after
        // the GIN index was last built.  Mirrors the same guard in bm25_search().
        self.gin_indexes
            .get(field)
            .map(|idx| idx.ilike(pattern, limit))
            .unwrap_or_default()
            .into_iter()
            .filter(|h| self.nodes.contains_key(h))
            .collect()
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
        let owned: Vec<(u64, String)> = self
            .nodes
            .iter()
            .filter_map(|(&hash, node)| {
                let payload = self.payload_store.get(node.payload_offset, node.payload_len)?;
                payload.get(field)?.as_str().map(|s| (hash, s.to_string()))
            })
            .collect();
        if !owned.is_empty() {
            let refs: Vec<(u64, &str)> = owned.iter().map(|(h, s)| (*h, s.as_str())).collect();
            let index = bm25::Bm25Index::build(field, refs.into_iter());
            self.bm25_indexes.insert(field.to_string(), index);
        }
        self.record_index_version("bm25", field, BM25_INDEX_VERSION);
    }

    /// Search the BM25 index for `field` and return the top-`top_k`
    /// results ranked by relevance score (highest first).
    ///
    /// Requires [`build_bm25_index`](Self::build_bm25_index) to have
    /// been called for `field`.  Returns an empty `Vec` if the index
    /// does not exist or the query produces no matches.
    ///
    /// # Deletion safety
    ///
    /// Two complementary guards ensure deleted documents never appear:
    ///
    /// 1. **Inside the index** — [`Bm25Index::delete`] is called by
    ///    [`remove`](Self::remove) and removes the document's entry
    ///    from `doc_id_to_idx`, so it can never score in `search`.
    /// 2. **Here** — results are filtered through `self.nodes` as a
    ///    belt-and-suspenders check covering any narrow window between
    ///    a node deletion and the BM25 index update.
    ///
    /// # Returns
    ///
    /// `Vec<(doc_id, score)>` — `doc_id` is `sk_hash(slug)`.
    ///
    /// # Example
    ///
    /// ```
    /// # use sekejap::CoreDB;
    /// let mut db = CoreDB::new();
    /// db.put("a1", r#"{"name":"Rust Tutorial for Beginners","_collection":"tutorials"}"#).unwrap();
    /// db.put("a2", r#"{"name":"Python Tutorial for Beginners","_collection":"tutorials"}"#).unwrap();
    /// db.build_bm25_index("name");
    ///
    /// let results = db.bm25_search("name", "rust tutorial", 10);
    /// // results[0] is the most relevant doc — deleted docs never appear
    /// ```
    pub fn bm25_search(&self, field: &str, query: &str, top_k: usize) -> Vec<(u64, f64)> {
        self.bm25_indexes
            .get(field)
            .map(|idx| {
                idx.search(query, top_k)
                    .into_iter()
                    // Belt-and-suspenders: exclude any doc not present in
                    // the live node map, covering the narrow window between
                    // node deletion and BM25 index update.
                    .filter(|hit| self.nodes.contains_key(&hit.doc_id))
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
        // Auto-rebuild HNSW if this field has a declared index.
        // Uses stored params when available, falls back to sensible defaults.
        let hnsw_declared = self.schemas.values()
            .any(|s| s.indexes.vector.contains(&field.to_string()));
        if hnsw_declared {
            let (m, ef) = self.hnsw_params.get(field).copied().unwrap_or((16, 200));
            let _ = self.build_hnsw_index(field, m, ef);
        }
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
                let payload = self.payload_store.get(node.payload_offset, node.payload_len)
                    .unwrap_or(Value::Null);
                if let Some(fk) = FieldKey::from_json(payload.get(field).unwrap_or(&Value::Null)) {
                    btree.entry(fk).or_default().push(hash);
                }
            }
        }
        self.field_indexes.insert((coll_hash, field.to_string()), btree);
        self.record_index_version("btree", field, BTREE_INDEX_VERSION);
    }

    /// Record the build version for an index in every schema that declares it.
    ///
    /// Key format: `"method:field"` (e.g. `"gin:name"`, `"btree:price"`).
    fn record_index_version(&mut self, method: &str, field: &str, version: u32) {
        for schema in self.schemas.values_mut() {
            let declares = match method {
                "gin"   => schema.indexes.fulltext.contains(&field.to_string()),
                "bm25"  => schema.indexes.bm25.contains(&field.to_string()),
                "btree" => schema.indexes.range.contains(&field.to_string()),
                _       => false,
            };
            if declares {
                schema.indexes.build_versions
                    .insert(format!("{}:{}", method, field), version);
            }
        }
    }

    /// Try to seed the candidate list for a `Collection` step from a btree index.
    ///
    /// Looks ahead in `remaining` for the first filter step that has a btree
    /// index on this collection. Returns `(candidates, skip_idx)` on a hit,
    /// where `skip_j` is the index in `remaining` of the step that was consumed
    /// (so the caller can skip it in the main pipeline loop). The optional third
    /// element is a second consumed step index (e.g. the upper-bound companion
    /// for a two-sided range like `WhereGt + WhereLte`). Returns `None` to fall
    /// back to a full collection scan.
    pub(crate) fn btree_seed(
        &self,
        coll_hash: u64,
        remaining: &[Step],
    ) -> Option<(Vec<u64>, usize, Option<usize>)> {
        use std::ops::Bound;
        for (j, step) in remaining.iter().enumerate() {
            match step {
                Step::WhereEq(field, value) => {
                    if let Some(idx) = self.field_indexes.get(&(coll_hash, field.clone())) {
                        if let Some(fk) = FieldKey::from_json(value) {
                            return Some((idx.get(&fk).cloned().unwrap_or_default(), j, None));
                        }
                    }
                }
                Step::WhereNeq(field, value) => {
                    if let Some(idx) = self.field_indexes.get(&(coll_hash, field.clone())) {
                        if let Some(fk) = FieldKey::from_json(value) {
                            // Set-difference: all collection members minus those matching value.
                            let excluded: std::collections::HashSet<u64> = idx
                                .get(&fk)
                                .map(|ids| ids.iter().copied().collect())
                                .unwrap_or_default();
                            let all = self.collections
                                .get(&coll_hash)
                                .cloned()
                                .unwrap_or_default();
                            return Some((
                                all.into_iter().filter(|h| !excluded.contains(h)).collect(),
                                j,
                                None,
                            ));
                        }
                    }
                }
                Step::WhereGt(field, lo) => {
                    if let Some(idx) = self.field_indexes.get(&(coll_hash, field.clone())) {
                        let fk_lo = FieldKey::from_f64(*lo);
                        // Look ahead: combine with WhereLte/WhereLt on same field into
                        // a single btree range scan, consuming both steps.
                        let upper = remaining[j + 1..].iter().enumerate().find_map(|(k, s)| {
                            match s {
                                Step::WhereLte(f2, hi) if f2 == field =>
                                    Some((j + 1 + k, Bound::Included(FieldKey::from_f64(*hi)))),
                                Step::WhereLt(f2, hi) if f2 == field =>
                                    Some((j + 1 + k, Bound::Excluded(FieldKey::from_f64(*hi)))),
                                _ => None,
                            }
                        });
                        return if let Some((pair_j, upper_bound)) = upper {
                            Some((
                                idx.range((Bound::Excluded(fk_lo), upper_bound))
                                    .flat_map(|(_, ids)| ids.iter().copied())
                                    .collect(),
                                j,
                                Some(pair_j),
                            ))
                        } else {
                            Some((
                                idx.range((Bound::Excluded(fk_lo), Bound::Unbounded))
                                    .flat_map(|(_, ids)| ids.iter().copied())
                                    .collect(),
                                j,
                                None,
                            ))
                        };
                    }
                }
                Step::WhereLt(field, hi) => {
                    if let Some(idx) = self.field_indexes.get(&(coll_hash, field.clone())) {
                        let fk_hi = FieldKey::from_f64(*hi);
                        // Look ahead for lower bound on same field.
                        let lower = remaining[j + 1..].iter().enumerate().find_map(|(k, s)| {
                            match s {
                                Step::WhereGte(f2, lo) if f2 == field =>
                                    Some((j + 1 + k, Bound::Included(FieldKey::from_f64(*lo)))),
                                Step::WhereGt(f2, lo) if f2 == field =>
                                    Some((j + 1 + k, Bound::Excluded(FieldKey::from_f64(*lo)))),
                                _ => None,
                            }
                        });
                        return if let Some((pair_j, lower_bound)) = lower {
                            Some((
                                idx.range((lower_bound, Bound::Excluded(fk_hi)))
                                    .flat_map(|(_, ids)| ids.iter().copied())
                                    .collect(),
                                j,
                                Some(pair_j),
                            ))
                        } else {
                            Some((
                                idx.range(..fk_hi)
                                    .flat_map(|(_, ids)| ids.iter().copied())
                                    .collect(),
                                j,
                                None,
                            ))
                        };
                    }
                }
                Step::WhereGte(field, lo) => {
                    if let Some(idx) = self.field_indexes.get(&(coll_hash, field.clone())) {
                        let fk_lo = FieldKey::from_f64(*lo);
                        let upper = remaining[j + 1..].iter().enumerate().find_map(|(k, s)| {
                            match s {
                                Step::WhereLte(f2, hi) if f2 == field =>
                                    Some((j + 1 + k, Bound::Included(FieldKey::from_f64(*hi)))),
                                Step::WhereLt(f2, hi) if f2 == field =>
                                    Some((j + 1 + k, Bound::Excluded(FieldKey::from_f64(*hi)))),
                                _ => None,
                            }
                        });
                        return if let Some((pair_j, upper_bound)) = upper {
                            Some((
                                idx.range((Bound::Included(fk_lo), upper_bound))
                                    .flat_map(|(_, ids)| ids.iter().copied())
                                    .collect(),
                                j,
                                Some(pair_j),
                            ))
                        } else {
                            Some((
                                idx.range(fk_lo..)
                                    .flat_map(|(_, ids)| ids.iter().copied())
                                    .collect(),
                                j,
                                None,
                            ))
                        };
                    }
                }
                Step::WhereLte(field, hi) => {
                    if let Some(idx) = self.field_indexes.get(&(coll_hash, field.clone())) {
                        let fk_hi = FieldKey::from_f64(*hi);
                        let lower = remaining[j + 1..].iter().enumerate().find_map(|(k, s)| {
                            match s {
                                Step::WhereGte(f2, lo) if f2 == field =>
                                    Some((j + 1 + k, Bound::Included(FieldKey::from_f64(*lo)))),
                                Step::WhereGt(f2, lo) if f2 == field =>
                                    Some((j + 1 + k, Bound::Excluded(FieldKey::from_f64(*lo)))),
                                _ => None,
                            }
                        });
                        return if let Some((pair_j, lower_bound)) = lower {
                            Some((
                                idx.range((lower_bound, Bound::Included(fk_hi)))
                                    .flat_map(|(_, ids)| ids.iter().copied())
                                    .collect(),
                                j,
                                Some(pair_j),
                            ))
                        } else {
                            Some((
                                idx.range(..=fk_hi)
                                    .flat_map(|(_, ids)| ids.iter().copied())
                                    .collect(),
                                j,
                                None,
                            ))
                        };
                    }
                }
                Step::WhereBetween(field, lo, hi) => {
                    if let Some(idx) = self.field_indexes.get(&(coll_hash, field.clone())) {
                        let fk_lo = FieldKey::from_f64(*lo);
                        let fk_hi = FieldKey::from_f64(*hi);
                        return Some((
                            idx.range(fk_lo..=fk_hi)
                                .flat_map(|(_, ids)| ids.iter().copied())
                                .collect(),
                            j,
                            None,
                        ));
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
    /// sort is single-column, and a btree index exists on that field.  Returns the
    /// pre-sorted candidates **and** the index of the `Sort` step in `remaining`
    /// (so the caller can add it to `skip_set` — data is already sorted, no
    /// payload-reading re-sort needed).
    pub(crate) fn btree_sorted_seed_from_steps(
        &self,
        coll_hash: u64,
        remaining: &[Step],
    ) -> Option<(Vec<u64>, usize)> {
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

        let candidates = match take_n {
            Some(n) => result.into_iter().take(n).collect(),
            None => result,
        };
        Some((candidates, sort_pos))
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
        self.hnsw_params.insert(field.to_string(), (m, ef_construction));
        Ok(())
    }

    // ── CREATE INDEX executor ──────────────────────────────────────────────────

    /// Build the in-memory index for a `CREATE INDEX` statement and update
    /// the collection schema's index hints.
    fn apply_index(
        &mut self,
        collection: &str,
        method: &sql::IndexMethod,
        fields: &[String],
    ) -> Result<(), sql::SqlError> {
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
                    // Silently skip when no vectors exist yet — the index
                    // will be (re)built automatically when vectors are inserted
                    // via put_vector(), or explicitly via REINDEX.
                    let _ = self.build_hnsw_index(field, 16, 200);
                }
            }
            IndexMethod::Btree => {
                for field in fields {
                    self.build_field_index(collection, field);
                }
            }
            IndexMethod::Hash => {
                for field in fields {
                    self.build_field_index(collection, field);
                }
            }
        }

        Ok(())
    }

    /// Rebuild all declared GIN indexes from all currently loaded nodes.
    ///
    /// Called after WAL replay in `open()` to ensure GIN is fresh regardless
    /// of the order in which WAL entries were written (e.g. CreateIndex before Put).
    fn rebuild_declared_gin_indexes(&mut self) {
        let fields: Vec<String> = {
            let mut seen = std::collections::HashSet::new();
            self.schemas.values()
                .flat_map(|s| s.indexes.fulltext.iter().cloned())
                .filter(|f| seen.insert(f.clone()))
                .collect()
        };
        for field in fields {
            self.build_gin_index(&field);
        }
    }

    /// Rebuild all declared HNSW indexes from all currently loaded vectors.
    ///
    /// Called after WAL replay in `open()` so that vectors written after the
    /// original `CREATE INDEX` are incorporated.
    fn rebuild_declared_hnsw_indexes(&mut self) {
        let params: Vec<(String, usize, usize)> = {
            let mut seen = std::collections::HashSet::new();
            self.schemas.values()
                .flat_map(|s| s.indexes.vector.iter().cloned())
                .filter(|f| seen.insert(f.clone()))
                .map(|f| {
                    let (m, ef) = self.hnsw_params.get(&f).copied().unwrap_or((16, 200));
                    (f, m, ef)
                })
                .collect()
        };
        for (field, m, ef) in params {
            let _ = self.build_hnsw_index(&field, m, ef);
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
        let payload = self.payload_store.get(node.payload_offset, node.payload_len)?;
        geo::extract_centroid(&payload)
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

/// Serde visitor that tokenizes and discards any JSON value without allocating.
/// Used to skip legacy fields (e.g. `gin_indexes`) that were written by older
/// binaries but are no longer needed.
#[derive(Default)]
struct Ignored;
impl<'de> serde::Deserialize<'de> for Ignored {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        serde::de::IgnoredAny::deserialize(d)?;
        Ok(Ignored)
    }
}

#[derive(Serialize, Deserialize)]
struct Snapshot {
    version: u32,
    /// true = disk-backed snapshot: payloads are in payloads.bin, SnapNode has
    /// offset/len/collection/spatial_meta but no payload field.
    #[serde(default)]
    is_disk_backed: bool,
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
    /// Btree field indexes — stored in disk-backed snapshots so they don't need
    /// to be rebuilt by scanning payloads.bin on every open.
    #[serde(skip_serializing_if = "Option::is_none")]
    btree_indexes: Option<Vec<SnapBtree>>,
    /// Legacy field written by older builds — never serialised, silently consumed
    /// during deserialisation to avoid allocating a multi-GB serde_json Value.
    #[serde(default, skip_serializing)]
    gin_indexes: Ignored,
}

#[derive(Serialize, Deserialize)]
struct SnapHnsw {
    field: String,
    #[serde(default)]
    version: u32,
    #[serde(default = "default_hnsw_m")]
    m: usize,
    #[serde(default = "default_hnsw_ef")]
    ef_construction: usize,
    graph: vector::HnswGraph,
}
fn default_hnsw_m()  -> usize { 16 }
fn default_hnsw_ef() -> usize { 200 }

#[derive(Serialize, Deserialize)]
struct SnapNode {
    slug: String,
    /// Full payload — used by in-memory (non-disk-backed) snapshots.
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<Value>,
    /// Disk-backed snapshot fields — offset/len into payloads.bin plus cached metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_len: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    collection: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    spatial_meta: Option<geo::SpatialMeta>,
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

/// Persisted btree field index for fast disk-backed snapshot reload.
#[derive(Serialize, Deserialize)]
struct SnapBtree {
    collection_hash: u64,
    field: String,
    /// Sorted (key, Vec<node_hash>) pairs — reconstructs the BTreeMap directly.
    entries: Vec<(FieldKey, Vec<u64>)>,
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
        let payload = db.get_payload(hash).unwrap();

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
        let payload = db.get_payload(hash).unwrap();

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

        // CREATE INDEX on an empty table must always succeed — schema hint is recorded
        // and the index will be built automatically when vectors are inserted.
        db.execute("CREATE INDEX ON articles USING hnsw (embedding)").unwrap();

        let schema = db.schemas.get("articles").expect("schema must exist");
        assert!(
            schema.indexes.vector.contains(&"embedding".to_string()),
            "embedding must be in indexes.vector after CREATE INDEX"
        );

        // INSERT with vector — HNSW is rebuilt automatically.
        db.execute(
            "INSERT INTO articles (_key, title, embedding) \
             VALUES ('a1', 'Rust', [1.0, 0.0, 0.0, 0.0])",
        )
        .unwrap();

        // Query works without an explicit REINDEX.
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

    /// `ST_AsGeoJSON(field)` in SELECT must return the geometry value as a
    /// JSON text string (Value::String), matching PostGIS semantics.
    /// Without AS alias the output key is the inner field name.
    #[test]
    fn test_st_asgeojson_select() {
        let mut db = CoreDB::new();
        db.put(
            "places/mel",
            r#"{"_collection":"places","name":"Melbourne",
                "geometry":{"type":"Point","coordinates":[144.9631,-37.8136]}}"#,
        )
        .unwrap();

        // Without alias — output key should be "geometry"
        let hits: Vec<_> = db
            .query("SELECT ST_AsGeoJSON(geometry) FROM places")
            .unwrap()
            .collect();
        assert_eq!(hits.len(), 1);
        let payload = hits[0].payload.as_ref().unwrap();
        let geom_str = payload["geometry"].as_str()
            .expect("ST_AsGeoJSON must return a string value");
        let geom: serde_json::Value = serde_json::from_str(geom_str).unwrap();
        assert_eq!(geom["type"], "Point");
        assert_eq!(geom["coordinates"][0].as_f64().unwrap(), 144.9631);

        // With alias — output key should be "geom"
        let hits: Vec<_> = db
            .query("SELECT ST_AsGeoJSON(geometry) AS geom FROM places")
            .unwrap()
            .collect();
        let payload = hits[0].payload.as_ref().unwrap();
        assert!(payload["geom"].is_string(), "aliased column must be present as string");
    }

    /// `ST_GeomFromGeoJSON('...')` in INSERT VALUES must store the geometry as
    /// a proper JSON object (not a raw string) in the node payload.
    #[test]
    fn test_st_geomfromgeojson_insert() {
        let mut db = CoreDB::new();
        db.execute(
            r#"INSERT INTO places (_key, name, geometry)
               VALUES ('fitzroy', 'Fitzroy',
                       ST_GeomFromGeoJSON('{"type":"Point","coordinates":[144.9775,-37.7963]}'))"#,
        )
        .unwrap();

        let hits: Vec<_> = db
            .query("SELECT * FROM places WHERE _key = 'fitzroy'")
            .unwrap()
            .collect();
        assert_eq!(hits.len(), 1);
        let payload = hits[0].payload.as_ref().unwrap();
        // geometry must be a JSON object, not a raw string
        assert!(
            payload["geometry"].is_object(),
            "geometry must be stored as a JSON object, not a string"
        );
        assert_eq!(payload["geometry"]["type"], "Point");
        assert_eq!(
            payload["geometry"]["coordinates"][0].as_f64().unwrap(),
            144.9775
        );
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
