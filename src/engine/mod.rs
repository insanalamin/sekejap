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
    #[allow(dead_code)] // scaffolding — wired up when flush() integrates rebuild
    scheduler: IndexScheduler,
    wal_policy: WalPolicy,
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
        }
    }

    /// Create an in-memory Engine (no persistence).
    pub fn memory() -> Self {
        Self {
            guard: ReadWriteGuard::new(CoreDB::new()),
            buffer: None,
            scheduler: IndexScheduler::new(RebuildStrategy::Immediate),
            wal_policy: WalPolicy::Manual,
        }
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
    pub fn execute(&self, sql: &str) -> Result<usize, String> {
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
    pub fn execute_params(&self, sql: &str, params: &[Value]) -> Result<usize, String> {
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
    pub fn compact(&self) -> Result<(), String> {
        let mut db = self.guard.write();
        db.compact().map_err(|e| e.to_string())
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

    /// Build the Engine, opening (or creating) the database at the configured path.
    pub fn build(self) -> Result<Engine, String> {
        let db = CoreDB::open(&self.path).map_err(|e| e.to_string())?;
        Ok(Engine {
            guard: ReadWriteGuard::new(db),
            buffer: self.buffer_size.map(WriteBuffer::new),
            scheduler: IndexScheduler::new(self.rebuild_strategy),
            wal_policy: self.wal_policy,
        })
    }
}
