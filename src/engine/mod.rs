//! Engine — concurrent, multi-model wrapper for [`CoreDB`].
//!
//! The engine layer sits on top of the raw [`CoreDB`] and adds:
//!
//! - **RwLock-based concurrency** — multiple readers proceed in parallel;
//!   writes take an exclusive lock but hold it only for the duration of the
//!   mutation (microseconds with incremental HNSW).
//! - **Write buffering** — accumulate SQL statements and apply them in one
//!   short lock acquisition via [`Engine::flush()`].
//! - **WAL compaction policy** — auto-compact when the write-ahead log
//!   exceeds a byte or entry threshold.
//! - **Index rebuild scheduling** — track dirty fields and rebuild HNSW /
//!   GIN / BM25 indexes on a configurable cadence.
//! - **S3 sync** (feature `s3`) — upload compacted segments to S3; download
//!   on open. Local stays fast; S3 is cold storage + distribution.
//! - **Read-only replicas** — open from S3 segments, block writes, hot-swap
//!   on [`refresh()`](Engine::refresh) when the writer publishes new data.
//!
//! The engine exposes the **full** Sekejap SQL surface — graph traversals,
//! spatial queries, vector search, full-text search, and standard CRUD — all
//! through [`Engine::query()`] and [`Engine::execute()`].
//!
//! Gated behind `#[cfg(feature = "engine")]`. Zero cost when disabled.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use sekejap::engine::{Engine, RebuildStrategy, WalPolicy};
//!
//! // Open (or create) a persistent database
//! let engine = Engine::builder("/tmp/mydb")
//!     .buffer_size(100)
//!     .rebuild_strategy(RebuildStrategy::Lazy)
//!     .wal_policy(WalPolicy::default())
//!     .build()
//!     .unwrap();
//!
//! // Reads — shared lock, concurrent
//! let hits = engine.query("SELECT name FROM users").unwrap();
//!
//! // Writes — exclusive lock (or buffered)
//! engine.execute("INSERT INTO users (_key, name) VALUES ('alice', 'Alice')").unwrap();
//!
//! // Graph
//! engine.execute("LINK alice -> bob AS follows WEIGHT 1.0").unwrap();
//!
//! // Vector
//! engine.execute("INSERT INTO docs (_key, emb) VALUES ('d1', [1.0, 0.0, 0.0])").unwrap();
//! let similar = engine.query(
//!     "SELECT _key FROM docs WHERE VECTOR_NEAR(emb, [1.0, 0.1, 0.0], 10)"
//! ).unwrap();
//!
//! // Flush buffered writes + maybe compact WAL
//! engine.flush().unwrap();
//! ```

pub mod buffer;
pub mod guard;
pub mod policy;
pub mod scheduler;

#[cfg(feature = "s3")]
pub mod cache;
#[cfg(feature = "s3")]
pub mod manifest;
#[cfg(feature = "s3")]
pub mod remote;

pub use policy::WalPolicy;
pub use scheduler::RebuildStrategy;

use buffer::WriteBuffer;
use guard::ReadWriteGuard;
use scheduler::IndexScheduler;

use crate::query::Hit;
use crate::CoreDB;
use serde_json::Value;

/// Concurrent database engine wrapping [`CoreDB`].
///
/// Provides thread-safe read/write access to the full Sekejap multi-model
/// database: graph, spatial, vector, full-text, and relational — all via SQL.
///
/// Use [`Engine::builder()`] for persistent databases or [`Engine::memory()`]
/// for ephemeral in-memory instances.
pub struct Engine {
    guard: ReadWriteGuard,
    buffer: Option<WriteBuffer>,
    #[allow(dead_code)]
    scheduler: IndexScheduler,
    wal_policy: WalPolicy,
    path: Option<String>,
    read_only: bool,
    #[cfg(feature = "s3")]
    remote: Option<remote::RemoteSync>,
    #[cfg(feature = "s3")]
    generation: std::sync::atomic::AtomicU64,
}

impl Engine {
    /// Start building an Engine from a database directory path.
    ///
    /// The directory will be created if it does not exist. Pass the same path
    /// to reopen an existing database.
    pub fn builder(path: &str) -> EngineBuilder {
        EngineBuilder {
            path: path.to_string(),
            buffer_size: None,
            rebuild_strategy: RebuildStrategy::default(),
            wal_policy: WalPolicy::default(),
            read_only: false,
            #[cfg(feature = "s3")]
            remote_url: None,
            #[cfg(feature = "s3")]
            remote_creds: None,
            #[cfg(feature = "s3")]
            remote_only: false,
            #[cfg(feature = "s3")]
            cache_budget: None,
            #[cfg(feature = "s3")]
            cache_dir: None,
        }
    }

    /// Create an in-memory Engine (no persistence).
    pub fn memory() -> Self {
        Self {
            guard: ReadWriteGuard::new(CoreDB::new()),
            buffer: None,
            scheduler: IndexScheduler::new(RebuildStrategy::Immediate),
            wal_policy: WalPolicy::Manual,
            path: None,
            read_only: false,
            #[cfg(feature = "s3")]
            remote: None,
            #[cfg(feature = "s3")]
            generation: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Whether this engine is in read-only mode.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    // ── Reads ────────────────────────────────────────────────────────────────

    /// Execute a read-only SQL query. Takes a shared read lock.
    ///
    /// Multiple `query()` calls from different threads proceed concurrently.
    /// Supports the full Sekejap SQL surface: `SELECT`, `MATCH`, `VECTOR_NEAR`,
    /// `GEO_NEAR`, `BM25_SEARCH`, etc.
    pub fn query(&self, sql: &str) -> Result<Vec<Hit>, String> {
        let db = self.guard.read();
        db.query(sql)
            .map(|set| set.collect())
            .map_err(|e| e.to_string())
    }

    /// Execute a read-only SQL query with parameter bindings.
    ///
    /// Parameters are referenced as `$1`, `$2`, ... in the SQL string.
    /// Takes a shared read lock (concurrent with other readers).
    pub fn query_params(&self, sql: &str, params: &[Value]) -> Result<Vec<Hit>, String> {
        let db = self.guard.read();
        db.query_params(sql, params)
            .map(|set| set.collect())
            .map_err(|e| e.to_string())
    }

    // ── Writes ───────────────────────────────────────────────────────────────

    /// Execute a write SQL statement.
    ///
    /// If a write buffer is configured, the statement is buffered and only
    /// applied when the threshold is reached or [`flush()`](Self::flush) is called.
    /// Without a buffer, the write is applied immediately.
    ///
    /// Returns the number of affected rows.
    /// Returns an error if the engine is in read-only mode.
    pub fn execute(&self, sql: &str) -> Result<usize, String> {
        if self.read_only {
            return Err("database is read-only".to_string());
        }
        if let Some(ref buf) = self.buffer {
            let should_flush = buf.push(sql.to_string());
            if should_flush {
                return self.flush();
            }
            return Ok(0);
        }
        // No buffer — apply immediately
        let mut db = self.guard.write();
        db.execute(sql).map_err(|e| e.to_string())
    }

    /// Execute a write SQL statement with parameter bindings.
    ///
    /// Parameters are referenced as `$1`, `$2`, ... in the SQL string.
    /// Always applied immediately (bypasses the write buffer).
    /// Takes an exclusive write lock.
    ///
    /// Returns an error if the engine is in read-only mode.
    pub fn execute_params(&self, sql: &str, params: &[Value]) -> Result<usize, String> {
        if self.read_only {
            return Err("database is read-only".to_string());
        }
        let mut db = self.guard.write();
        db.execute_params(sql, params).map_err(|e| e.to_string())
    }

    // ── Flush & Maintenance ──────────────────────────────────────────────────

    /// Drain the write buffer, apply all pending statements, and optionally
    /// compact the WAL if the policy threshold is exceeded.
    ///
    /// Returns the total number of rows affected across all flushed statements.
    /// Returns `Ok(0)` if no buffer is configured or the buffer is empty.
    pub fn flush(&self) -> Result<usize, String> {
        let statements = match self.buffer {
            Some(ref buf) => buf.drain(),
            None => return Ok(0),
        };

        if statements.is_empty() {
            return Ok(0);
        }

        let mut db = self.guard.write();
        let mut total = 0;
        for sql in &statements {
            total += db.execute(sql).map_err(|e| e.to_string())?;
        }

        // Check WAL compaction policy
        if let Some(ref path) = db.data_dir {
            let wal_path = path.join("wal.log");
            let wal_bytes = std::fs::metadata(&wal_path)
                .map(|m| m.len())
                .unwrap_or(0);
            if self.wal_policy.should_compact(wal_bytes, statements.len()) {
                let _ = db.compact();
            }
        }

        Ok(total)
    }

    /// Force WAL compaction regardless of policy.
    ///
    /// Rewrites the snapshot + payloads.bin and truncates the WAL log to zero.
    /// Takes an exclusive write lock for the duration of the compaction.
    ///
    /// When an S3 remote is configured (feature `s3`), uploads the compacted
    /// segments after successful local compaction.
    ///
    /// Returns an error if the engine is in read-only mode.
    pub fn compact(&self) -> Result<(), String> {
        if self.read_only {
            return Err("database is read-only".to_string());
        }
        {
            let mut db = self.guard.write();
            db.compact().map_err(|e| e.to_string())?;
        }

        #[cfg(feature = "s3")]
        if let (Some(ref remote), Some(ref path)) = (&self.remote, &self.path) {
            remote
                .sync_to_remote(std::path::Path::new(path))
                .map_err(|e| format!("S3 sync after compact: {e}"))?;
        }

        Ok(())
    }

    /// Manually sync local segments to S3.
    ///
    /// Uploads all segment files and writes a new manifest.
    /// Only available when the `s3` feature is enabled and a remote is configured.
    #[cfg(feature = "s3")]
    pub fn sync(&self) -> Result<(), String> {
        let remote = self.remote.as_ref().ok_or("no remote configured")?;
        let path = self.path.as_ref().ok_or("no database path")?;
        remote.sync_to_remote(std::path::Path::new(path))
    }

    /// Check S3 for a newer manifest and hot-swap the local database.
    ///
    /// Returns `true` if the database was refreshed (newer generation found),
    /// `false` if already up-to-date.
    ///
    /// Only available in read-only mode with an S3 remote configured.
    /// During the swap, queries briefly block (microseconds) while the inner
    /// `CoreDB` is replaced — in-flight reads continue on the old data.
    #[cfg(feature = "s3")]
    pub fn refresh(&self) -> Result<bool, String> {
        if !self.read_only {
            return Err("refresh() is only available in read-only mode".to_string());
        }
        let remote = self.remote.as_ref().ok_or("no remote configured")?;
        let path = self.path.as_ref().ok_or("no database path")?;

        let latest = remote.latest_generation()?;
        let current = self
            .generation
            .load(std::sync::atomic::Ordering::Relaxed);
        if latest <= current {
            return Ok(false);
        }

        // Download newer segments (incremental — skips matching files).
        remote.sync_from_remote(std::path::Path::new(path))?;

        // Reopen CoreDB from updated directory.
        let new_db =
            CoreDB::open_read_only(path).map_err(|e| format!("reopening db: {e}"))?;
        let _old = self.guard.replace(new_db);
        self.generation
            .store(latest, std::sync::atomic::Ordering::Relaxed);

        Ok(true)
    }

    /// The current manifest generation this engine was opened from.
    ///
    /// Increments after each successful [`compact()`](Self::compact) upload
    /// or [`refresh()`](Self::refresh) download.
    #[cfg(feature = "s3")]
    pub fn generation(&self) -> u64 {
        self.generation
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Consume the engine and return the inner `CoreDB`.
    pub fn into_inner(self) -> CoreDB {
        self.guard.into_inner()
    }
}

/// Builder for configuring an [`Engine`] instance.
///
/// Obtained via [`Engine::builder()`]. All settings have sensible defaults
/// — you only need to call [`build()`](Self::build) to get a working engine.
///
/// # Example
///
/// ```rust,no_run
/// use sekejap::engine::{Engine, RebuildStrategy, WalPolicy};
///
/// let engine = Engine::builder("/tmp/mydb")
///     .buffer_size(100)                       // buffer 100 writes before flush
///     .rebuild_strategy(RebuildStrategy::Lazy) // no auto-rebuild
///     .wal_policy(WalPolicy::Auto {           // compact at 32 MB
///         max_bytes: 32 * 1024 * 1024,
///         max_entries: 10_000,
///     })
///     .build()
///     .unwrap();
/// ```
pub struct EngineBuilder {
    path: String,
    buffer_size: Option<usize>,
    rebuild_strategy: RebuildStrategy,
    wal_policy: WalPolicy,
    read_only: bool,
    #[cfg(feature = "s3")]
    remote_url: Option<String>,
    #[cfg(feature = "s3")]
    remote_creds: Option<remote::S3Credentials>,
    #[cfg(feature = "s3")]
    remote_only: bool,
    #[cfg(feature = "s3")]
    cache_budget: Option<cache::CacheBudget>,
    #[cfg(feature = "s3")]
    cache_dir: Option<String>,
}

impl EngineBuilder {
    /// Set the write buffer threshold (number of statements before auto-flush).
    /// Pass `0` or omit to disable buffering.
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = if size > 0 { Some(size) } else { None };
        self
    }

    /// Set the index rebuild strategy.
    pub fn rebuild_strategy(mut self, strategy: RebuildStrategy) -> Self {
        self.rebuild_strategy = strategy;
        self
    }

    /// Set the WAL compaction policy.
    pub fn wal_policy(mut self, policy: WalPolicy) -> Self {
        self.wal_policy = policy;
        self
    }

    /// Open the database in read-only mode.
    ///
    /// Read-only engines skip the exclusive file lock and WAL writer.
    /// Write operations ([`execute`](Engine::execute), [`compact`](Engine::compact))
    /// return an error. Use [`refresh()`](Engine::refresh) (with S3) to pull
    /// newer data published by a writer.
    pub fn read_only(mut self, ro: bool) -> Self {
        self.read_only = ro;
        self
    }

    /// Configure S3 remote storage for segment sync.
    ///
    /// The URL should be `s3://bucket-name/optional/prefix`.
    /// Credentials must be set via [`.credentials()`](Self::credentials).
    ///
    /// On `build()`, any remote segments missing locally will be downloaded.
    /// On `compact()`, compacted segments will be uploaded.
    #[cfg(feature = "s3")]
    pub fn remote(mut self, url: &str) -> Self {
        self.remote_url = Some(url.to_string());
        self
    }

    /// Set S3 credentials. Required when using `.remote()`.
    #[cfg(feature = "s3")]
    pub fn credentials(mut self, creds: remote::S3Credentials) -> Self {
        self.remote_creds = Some(creds);
        self
    }

    /// Enable remote-only mode: payloads stay on S3, fetched on demand.
    ///
    /// Only the snapshot (node index) is downloaded. Payload reads go through
    /// a bounded LRU block cache backed by S3 `GET_RANGE`. This allows
    /// querying datasets much larger than local disk.
    ///
    /// Implies `read_only(true)`. Requires `.remote(url)`.
    #[cfg(feature = "s3")]
    pub fn remote_only(mut self, enabled: bool) -> Self {
        self.remote_only = enabled;
        if enabled {
            self.read_only = true;
        }
        self
    }

    /// Set the local block cache budget for remote-only mode.
    /// Default: 10 GB.
    #[cfg(feature = "s3")]
    pub fn cache_budget(mut self, budget: cache::CacheBudget) -> Self {
        self.cache_budget = Some(budget);
        self
    }

    /// Set a local disk cache directory for remote-only mode.
    ///
    /// Blocks evicted from the in-memory tier are written here (64 KB files).
    /// Survives process restarts — blocks are re-discovered on next open.
    #[cfg(feature = "s3")]
    pub fn cache_dir(mut self, dir: &str) -> Self {
        self.cache_dir = Some(dir.to_string());
        self
    }

    /// Build the Engine, opening (or creating) the database at the configured path.
    pub fn build(self) -> Result<Engine, String> {
        #[cfg(feature = "s3")]
        let remote_sync = match self.remote_url {
            Some(ref url) => {
                let creds = self.remote_creds.as_ref()
                    .ok_or("S3 credentials required — call .credentials() on the builder")?;
                Some(remote::RemoteSync::from_url(url, creds)?)
            }
            None => None,
        };

        #[cfg(feature = "s3")]
        let mut initial_gen = 0u64;

        #[cfg(feature = "s3")]
        let use_remote_only = self.remote_only;
        #[cfg(not(feature = "s3"))]
        let use_remote_only = false;

        let db;

        #[cfg(feature = "s3")]
        if use_remote_only {
            let r = remote_sync
                .as_ref()
                .ok_or("remote_only requires .remote(url)")?;
            initial_gen = r.latest_generation().unwrap_or(0);
            let budget = self
                .cache_budget
                .unwrap_or_else(cache::CacheBudget::default);
            db = CoreDB::open_s3(
                r,
                budget,
                self.cache_dir.as_deref().map(std::path::Path::new),
            )?;
        } else {
            // Full sync: download all segments, then open locally.
            if let Some(ref r) = remote_sync {
                r.sync_from_remote(std::path::Path::new(&self.path))
                    .map_err(|e| format!("S3 initial sync: {e}"))?;
                initial_gen = r.latest_generation().unwrap_or(0);
            }

            db = if self.read_only {
                CoreDB::open_read_only(&self.path).map_err(|e| e.to_string())?
            } else {
                CoreDB::open(&self.path).map_err(|e| e.to_string())?
            };
        }

        #[cfg(not(feature = "s3"))]
        {
            db = if self.read_only {
                CoreDB::open_read_only(&self.path).map_err(|e| e.to_string())?
            } else {
                CoreDB::open(&self.path).map_err(|e| e.to_string())?
            };
        }

        Ok(Engine {
            guard: ReadWriteGuard::new(db),
            buffer: if self.read_only {
                None
            } else {
                self.buffer_size.map(WriteBuffer::new)
            },
            scheduler: IndexScheduler::new(self.rebuild_strategy),
            wal_policy: self.wal_policy,
            path: Some(self.path),
            read_only: self.read_only,
            #[cfg(feature = "s3")]
            remote: remote_sync,
            #[cfg(feature = "s3")]
            generation: std::sync::atomic::AtomicU64::new(initial_gen),
        })
    }
}

#[cfg(all(test, feature = "s3"))]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_read_only_blocks_writes() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Engine::builder(dir.path().to_str().unwrap())
            .read_only(true)
            .build()
            .unwrap();

        assert!(engine.is_read_only());

        let err = engine
            .execute("INSERT INTO t (_key, x) VALUES ('a', 1)")
            .unwrap_err();
        assert!(err.contains("read-only"));

        let err = engine
            .execute_params("INSERT INTO t (_key) VALUES ($1)", &[])
            .unwrap_err();
        assert!(err.contains("read-only"));

        let err = engine.compact().unwrap_err();
        assert!(err.contains("read-only"));
    }

    #[test]
    fn test_read_only_allows_queries() {
        let dir = tempfile::tempdir().unwrap();

        // Write some data first.
        {
            let w = Engine::builder(dir.path().to_str().unwrap())
                .build()
                .unwrap();
            w.execute("CREATE TABLE items (_key TEXT PRIMARY KEY, name TEXT)")
                .unwrap();
            w.execute("INSERT INTO items (_key, name) VALUES ('a', 'Alice')")
                .unwrap();
            w.compact().unwrap();
        }

        // Open read-only and verify query works.
        let r = Engine::builder(dir.path().to_str().unwrap())
            .read_only(true)
            .build()
            .unwrap();
        let hits = r.query("SELECT name FROM items").unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn test_refresh_detects_new_generation() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let writer_remote =
            remote::RemoteSync::from_store(store.clone(), "refreshtest").unwrap();
        let reader_remote =
            remote::RemoteSync::from_store(store.clone(), "refreshtest").unwrap();

        // Writer: create db, insert data, compact, upload.
        let w_dir = tempfile::tempdir().unwrap();
        {
            let db = CoreDB::open(w_dir.path()).unwrap();
            drop(db);
        }
        {
            let mut db = CoreDB::open(w_dir.path()).unwrap();
            db.execute("CREATE TABLE items (_key TEXT PRIMARY KEY, val INTEGER)")
                .unwrap();
            db.execute("INSERT INTO items (_key, val) VALUES ('x', 10)")
                .unwrap();
            db.compact().unwrap();
        }
        writer_remote.sync_to_remote(w_dir.path()).unwrap();

        // Reader: pull from S3, open read-only.
        let r_dir = tempfile::tempdir().unwrap();
        reader_remote.sync_from_remote(r_dir.path()).unwrap();
        let r_path = r_dir.path().to_str().unwrap().to_string();

        // Build engine manually with injected remote.
        let db = CoreDB::open_read_only(&r_path).unwrap();
        let engine = Engine {
            guard: ReadWriteGuard::new(db),
            buffer: None,
            scheduler: IndexScheduler::new(RebuildStrategy::Immediate),
            wal_policy: WalPolicy::Manual,
            path: Some(r_path),
            read_only: true,
            remote: Some(reader_remote),
            generation: std::sync::atomic::AtomicU64::new(1),
        };

        // Query should see data.
        let hits = engine.query("SELECT val FROM items").unwrap();
        assert_eq!(hits.len(), 1);

        // No new generation — refresh returns false.
        assert!(!engine.refresh().unwrap());

        // Writer adds more data and uploads gen 2.
        {
            let mut db = CoreDB::open(w_dir.path()).unwrap();
            db.execute("INSERT INTO items (_key, val) VALUES ('y', 20)")
                .unwrap();
            db.compact().unwrap();
        }
        writer_remote.sync_to_remote(w_dir.path()).unwrap();

        // Reader refresh — should detect gen 2 and hot-swap.
        assert!(engine.refresh().unwrap());
        assert_eq!(engine.generation(), 2);

        let hits = engine.query("SELECT val FROM items").unwrap();
        assert_eq!(hits.len(), 2);
    }
}
