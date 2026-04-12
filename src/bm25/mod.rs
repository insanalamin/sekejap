//! Lightweight BM25 full-text search index.
//!
//! Design goals:
//! - **Opt-in only**: Not automatic, you choose which fields to index
//! - **Disk-first**: mmap for persistence, RAM only for queried terms
//! - **Compressed**: Varint encoding for postings, ~6x smaller than naive
//!
//! Storage layout per field for 1M docs:
//! - Postings: ~80 MB (compressed)
//! - Term dict: ~30 MB
//! - Doc lengths: ~4 MB
//! - Total: ~114 MB (vs 1.7GB naive)
//!
//! # Usage
//!
//! ```ignore
//! // Build index on specific fields
//! db.build_bm25_index("name");
//!
//! // Search with BM25 scoring
//! let results = db.bm25_search("name", "rust tutorial", 10);
//! // Returns top 10 docs ranked by BM25 score
//! ```

mod dict;
mod index;
mod postings;
mod tokenizer;

pub use index::Bm25Index;
pub use tokenizer::tokenize;
