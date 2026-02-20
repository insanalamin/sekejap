// Sekejap DB v0.3.0 - Graph-First Multi-Model Database
// JSON Pipeline Query Language (SekejapQL)

pub mod arena;
pub mod types;
pub mod db;
pub mod stores;
pub mod set;
pub mod sekejapql;
pub mod hnsw;
pub mod wal;
pub mod txn;
pub mod index;
pub mod mmap_hash;
pub mod collection_bitmap;

// FullText adapter module (always available, requires feature flag for implementation)
#[cfg(any(feature = "fulltext", feature = "fulltext-tantivy", feature = "fulltext-seekstorm"))]
pub mod fulltext;

// Re-export main types
pub use db::SekejapDB;
pub use types::{Step, Hit, Outcome, Trace, Plan};
pub use stores::{NodeStore, EdgeStore, SchemaStore};
pub use set::Set;
pub use sekejapql::{SekejapQL, SecurityLimits};