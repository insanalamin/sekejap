//! Property Index Module
//!
//! Ultra-fast secondary index for property filtering.
//! Target: 10M+ lookups/sec, 1M+ range queries/sec
//!
//! Design:
//! - HashIndex: O(1) equality lookups (DashMap)
//! - RangeIndex: B-tree-like sorted vectors (cache-friendly)
//! - Lock-free concurrent reads
//! - Batch indexing mode

mod hash_index;
mod range_index;

pub use hash_index::HashIndex;
pub use range_index::RangeIndex;

use std::sync::Arc;
use dashmap::DashMap;
use serde_json::Value;

/// Property index trait
pub trait PropertyIndex: Send + Sync {
    /// Insert a value for a node
    fn insert(&self, node_idx: u32, value: &Value);
    
    /// Remove a node from index
    fn remove(&self, node_idx: u32);
    
    /// Lookup exact match
    fn lookup_eq(&self, value: &Value) -> Vec<u32>;
    
    /// Lookup range (for numeric fields)
    fn lookup_range(&self, min: &Value, max: &Value) -> Vec<u32>;
    
    /// Get index name
    fn name(&self) -> &str;
    
    /// Get indexed count
    fn count(&self) -> usize;
}

/// Index type selection
#[derive(Debug, Clone, Copy)]
pub enum IndexType {
    /// Hash index for equality (O(1))
    Hash,
    /// Range index for comparisons (O(log n))
    Range,
}

/// Create property index based on type
pub fn create_index(name: &str, index_type: IndexType) -> Box<dyn PropertyIndex> {
    match index_type {
        IndexType::Hash => Box::new(HashIndex::new(name)),
        IndexType::Range => Box::new(RangeIndex::new(name)),
    }
}