//! Full-text search adapters.
//!
//! **Tantivy** (default): Production-ready, portable, ~100K docs/sec
//! **SeekStorm** (opt-in): 2-3x faster but needs `RUSTFLAGS="-C target-cpu=native"`
//!
//! # Examples
//! ```toml
//! # Production (works everywhere)
//! sekejap = { features = ["fulltext"] }
//!
//! # High-performance (modern CPUs only)
//! sekejap = { features = ["fulltext-seekstorm"] }
//! ```
//!
//! Build SeekStorm: `RUSTFLAGS="-C target-cpu=native" cargo build --features fulltext-seekstorm`

use std::path::Path;

#[cfg(feature = "fulltext-tantivy")]
mod tantivy;
#[cfg(feature = "fulltext-tantivy")]
pub use tantivy::TantivyAdapter;

#[cfg(feature = "fulltext-seekstorm")]
mod seekstorm;
#[cfg(feature = "fulltext-seekstorm")]
pub use seekstorm::SeekStormAdapter;

/// Search hit result
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub id: u64,
    pub score: f32,
}

/// Trait for full-text search adapters
pub trait FullTextAdapter: Send + Sync {
    /// Add a document with title, content, and unique id (slug_hash)
    fn add_document(&self, title: &str, content: &str, id: u64) -> Result<(), Box<dyn std::error::Error>>;
    
    /// Search for documents matching query, returning top-k hits
    fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, Box<dyn std::error::Error>>;
    
    /// Commit pending writes to disk
    fn commit(&self) -> Result<(), Box<dyn std::error::Error>>;
}

/// Create fulltext adapter. Priority: Tantivy > SeekStorm > Error.
pub fn create_default_adapter(path: &Path) -> Result<Box<dyn FullTextAdapter>, Box<dyn std::error::Error>> {
    #[cfg(feature = "fulltext-tantivy")]
    { return Ok(Box::new(TantivyAdapter::new(path)?)); }
    
    #[cfg(all(feature = "fulltext-seekstorm", not(feature = "fulltext-tantivy")))]
    { return Ok(Box::new(SeekStormAdapter::new(path)?)); }
    
    #[cfg(not(any(feature = "fulltext-seekstorm", feature = "fulltext-tantivy")))]
    {
        Err("No fulltext feature enabled. Use 'fulltext-tantivy' (recommended) or 'fulltext-seekstorm' (requires RUSTFLAGS).".into())
    }
}