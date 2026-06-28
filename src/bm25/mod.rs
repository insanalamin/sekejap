//! Lightweight BM25 full-text search index.
//!
//! # Design goals
//!
//! - **Opt-in only** — not automatic; you choose which fields to index.
//! - **Pi-friendly** — fixed allocations, no background threads, no
//!   allocator pressure.
//! - **Compressed** — varint + delta encoding on postings (~6× smaller
//!   than a naive implementation).
//! - **Incremental deletion** — [`Bm25Index::delete`] removes a
//!   document in O(1) without rewriting the postings blob.  A full
//!   rebuild is only needed when the orphan ratio exceeds
//!   [`DEFAULT_REBUILD_THRESHOLD`].
//!
//! # Approximate storage per indexed field (1 M docs)
//!
//! | Structure | Size |
//! |---|---|
//! | Postings blob | 90–450 MB |
//! | Term dictionary | ~2–30 MB |
//! | Doc-length array | 4 MB |
//! | Doc-ID lookup map | ~19 MB |
//!
//! # Usage
//!
//! ```ignore
//! // Build index on a field
//! db.build_bm25_index("body");
//!
//! // Search — deleted documents are automatically excluded
//! let results = db.bm25_search("body", "graph traversal", 10);
//!
//! // After many deletes, check if a rebuild is due
//! if db.bm25_needs_rebuild("body") {
//!     db.build_bm25_index("body");
//! }
//! ```

mod dict;
mod index;
mod postings;
pub mod tokenizer;

pub use index::{Bm25Index, DEFAULT_REBUILD_THRESHOLD};
pub use tokenizer::tokenize;
