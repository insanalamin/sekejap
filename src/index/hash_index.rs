//! Hash Index for O(1) Equality Lookups
//!
//! Ultra-fast hash index using DashMap (lock-free, sharded).
//! Target: 10M+ lookups/sec

use super::PropertyIndex;
use dashmap::DashMap;
use serde_json::Value;
use std::sync::Arc;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// Fast hash index for equality lookups
pub struct HashIndex {
    name: String,
    /// value_hash -> set of node indices (lock-free)
    index: DashMap<u64, Vec<u32>>,
    /// Reverse index for removals: node_idx -> value_hash
    reverse: DashMap<u32, u64>,
}

impl HashIndex {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            index: DashMap::new(),
            reverse: DashMap::new(),
        }
    }
    
    /// Hash a JSON value to u64
    fn hash_value(value: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        match value {
            Value::String(s) => s.hash(&mut hasher),
            Value::Number(n) => n.to_string().hash(&mut hasher),
            Value::Bool(b) => b.hash(&mut hasher),
            Value::Null => 0u8.hash(&mut hasher),
            _ => value.to_string().hash(&mut hasher),
        }
        hasher.finish()
    }
}

impl PropertyIndex for HashIndex {
    fn insert(&self, node_idx: u32, value: &Value) {
        // Remove old entry if exists
        self.remove(node_idx);
        
        let hash = Self::hash_value(value);
        
        // Forward: value -> nodes
        self.index.entry(hash).or_default().push(node_idx);
        
        // Reverse: node -> value (for fast removal)
        self.reverse.insert(node_idx, hash);
    }
    
    fn remove(&self, node_idx: u32) {
        if let Some((_, old_hash)) = self.reverse.remove(&node_idx) {
            if let Some(mut nodes) = self.index.get_mut(&old_hash) {
                // Remove node from vector (swap with last for O(1))
                if let Some(pos) = nodes.iter().position(|&x| x == node_idx) {
                    nodes.swap_remove(pos);
                }
                // Remove entry if empty
                if nodes.is_empty() {
                    drop(nodes);
                    self.index.remove(&old_hash);
                }
            }
        }
    }
    
    fn lookup_eq(&self, value: &Value) -> Vec<u32> {
        let hash = Self::hash_value(value);
        // Use reference-based iteration - much faster than clone
        self.index.get(&hash).map(|v| v.to_vec()).unwrap_or_default()
    }
    
    fn lookup_range(&self, _min: &Value, _max: &Value) -> Vec<u32> {
        // Hash index doesn't support range queries
        Vec::new()
    }
    
    fn name(&self) -> &str {
        &self.name
    }
    
    fn count(&self) -> usize {
        self.reverse.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_hash_index_basic() {
        let idx = HashIndex::new("status");
        
        idx.insert(1, &Value::String("active".to_string()));
        idx.insert(2, &Value::String("active".to_string()));
        idx.insert(3, &Value::String("inactive".to_string()));
        
        let active = idx.lookup_eq(&Value::String("active".to_string()));
        assert_eq!(active.len(), 2);
        assert!(active.contains(&1));
        assert!(active.contains(&2));
        
        let inactive = idx.lookup_eq(&Value::String("inactive".to_string()));
        assert_eq!(inactive.len(), 1);
        assert!(inactive.contains(&3));
    }
    
    #[test]
    fn test_hash_index_remove() {
        let idx = HashIndex::new("status");
        
        idx.insert(1, &Value::String("active".to_string()));
        assert_eq!(idx.lookup_eq(&Value::String("active".to_string())).len(), 1);
        
        idx.remove(1);
        assert_eq!(idx.lookup_eq(&Value::String("active".to_string())).len(), 0);
    }
    
    #[test]
    fn test_hash_index_update() {
        let idx = HashIndex::new("status");
        
        idx.insert(1, &Value::String("active".to_string()));
        idx.insert(1, &Value::String("inactive".to_string()));
        
        assert_eq!(idx.lookup_eq(&Value::String("active".to_string())).len(), 0);
        assert_eq!(idx.lookup_eq(&Value::String("inactive".to_string())).len(), 1);
    }
    
    #[test]
    fn test_hash_index_concurrent() {
        use std::sync::Arc;
        use std::thread;
        
        let idx = Arc::new(HashIndex::new("status"));
        
        let handles: Vec<_> = (0..10)
            .map(|t| {
                let idx = Arc::clone(&idx);
                thread::spawn(move || {
                    for i in 0..1000 {
                        let node_idx = t * 1000 + i;
                        let status = if i % 2 == 0 { "active" } else { "inactive" };
                        idx.insert(node_idx as u32, &Value::String(status.to_string()));
                    }
                })
            })
            .collect();
        
        for h in handles {
            h.join().unwrap();
        }
        
        // Should have 10,000 entries
        assert_eq!(idx.count(), 10000);
    }
}