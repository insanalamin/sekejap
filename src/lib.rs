// Sekejap DB v0.3.0 - Graph-First Multi-Model Database
// Unified query/mutate pipeline interface

pub mod arena;
pub mod collection_bitmap;
pub mod db;
pub mod hnsw;
pub mod index;
pub mod mmap_hash;
pub mod sekejapql;
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
pub use set::Set;
pub use stores::{EdgeStore, NodeStore, SchemaStore};
pub use types::{Hit, Outcome, Plan, Step, Trace};
