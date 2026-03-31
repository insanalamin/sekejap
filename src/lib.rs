// Sekejap DB v0.3.0 - Graph-First Multi-Model Database
// Unified query/mutate pipeline interface

pub mod arena;
pub mod collection_bitmap;
pub mod db;
pub mod geometry;
pub mod hnsw;
pub mod index;
pub mod mmap_hash;
pub mod sekejapql;
pub mod sql;
pub mod set;
pub mod stores;
pub mod txn;
pub mod types;
pub mod wal;

// FullText adapter module (always available, requires feature flag for implementation)
#[cfg(any(
    feature = "fulltext",
    feature = "fulltext-tantivy",
    feature = "fulltext-seekstorm"
))]
pub mod fulltext;

// Re-export main types
pub use db::SekejapDB;
pub use sekejapql::{QueryCompiler, SecurityLimits};
pub use sql::{lower_statement as lower_sql_statement, parse_sql, SqlCompiler, SqlError, SqlStatement};
pub use set::Set;
pub use stores::{EdgeStore, NodeStore, SchemaStore};
pub use types::{Hit, Outcome, Plan, Step, TimeOfDayQuery, TimeQuery, Trace};


