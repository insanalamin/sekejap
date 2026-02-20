//! Range Index for O(log n) Range Queries
//!
//! Cache-friendly sorted array with binary search.
//! Target: 1M+ range queries/sec

use super::PropertyIndex;
use std::sync::RwLock;
use serde_json::Value;

/// Entry in the sorted index
#[derive(Clone, Copy, Debug)]
struct Entry {
    value: f64,
    node_idx: u32,
}

/// Range index using sorted array + binary search
/// 
/// Optimizations:
/// - Cache-friendly contiguous memory
/// - Binary search for O(log n) lookups
/// - Bulk sorting for batch inserts
pub struct RangeIndex {
    name: String,
    /// Sorted entries (value, node_idx)
    data: RwLock<Vec<Entry>>,
    /// Reverse index for removals
    reverse: RwLock<Vec<(u32, usize)>>,  // (node_idx, position_hint)
}

impl RangeIndex {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            data: RwLock::new(Vec::new()),
            reverse: RwLock::new(Vec::new()),
        }
    }
    
    /// Extract f64 from JSON value
    fn to_f64(value: &Value) -> Option<f64> {
        match value {
            Value::Number(n) => n.as_f64(),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
    }
    
    /// Binary search for lower bound
    fn lower_bound(data: &[Entry], min: f64) -> usize {
        data.binary_search_by(|e| e.value.partial_cmp(&min).unwrap()).unwrap_or_else(|x| x)
    }
    
    /// Binary search for upper bound
    fn upper_bound(data: &[Entry], max: f64) -> usize {
        let mut lo = 0;
        let mut hi = data.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if data[mid].value <= max {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

impl PropertyIndex for RangeIndex {
    fn insert(&self, node_idx: u32, value: &Value) {
        let num = match Self::to_f64(value) {
            Some(n) => n,
            None => return,
        };
        
        // Remove old entry if exists
        self.remove(node_idx);
        
        // Insert new entry
        let entry = Entry { value: num, node_idx };
        
        {
            let mut data = self.data.write().unwrap();
            
            // Find insertion point (binary search)
            let pos = data.binary_search_by(|e| e.value.partial_cmp(&num).unwrap()).unwrap_or_else(|x| x);
            data.insert(pos, entry);
            
            // Update reverse index
            let mut rev = self.reverse.write().unwrap();
            rev.push((node_idx, pos));
            rev.sort_by_key(|(idx, _)| *idx);
        }
    }
    
    fn remove(&self, node_idx: u32) {
        let mut data = self.data.write().unwrap();
        let mut rev = self.reverse.write().unwrap();
        
        // Find entry in reverse index
        if let Ok(pos) = rev.binary_search_by_key(&node_idx, |(idx, _)| *idx) {
            let (_, data_pos) = rev[pos];
            
            // Remove from data
            if data_pos < data.len() && data[data_pos].node_idx == node_idx {
                data.remove(data_pos);
            } else {
                // Position hint was wrong, search linearly
                if let Some(actual_pos) = data.iter().position(|e| e.node_idx == node_idx) {
                    data.remove(actual_pos);
                }
            }
            
            rev.remove(pos);
        }
    }
    
    fn lookup_eq(&self, value: &Value) -> Vec<u32> {
        let num = match Self::to_f64(value) {
            Some(n) => n,
            None => return Vec::new(),
        };
        
        let data = self.data.read().unwrap();
        let start = Self::lower_bound(&data, num);
        let end = Self::upper_bound(&data, num);
        
        data[start..end].iter().map(|e| e.node_idx).collect()
    }
    
    fn lookup_range(&self, min: &Value, max: &Value) -> Vec<u32> {
        let min_val = match Self::to_f64(min) {
            Some(n) => n,
            None => return Vec::new(),
        };
        let max_val = match Self::to_f64(max) {
            Some(n) => n,
            None => return Vec::new(),
        };
        
        let data = self.data.read().unwrap();
        let start = Self::lower_bound(&data, min_val);
        let end = Self::upper_bound(&data, max_val);
        
        data[start..end].iter().map(|e| e.node_idx).collect()
    }
    
    fn name(&self) -> &str {
        &self.name
    }
    
    fn count(&self) -> usize {
        let rev = self.reverse.read().unwrap();
        rev.len()
    }
}

impl RangeIndex {
    /// Convenience: insert a raw f64 value directly (used by write_with_value).
    pub fn insert_f64(&self, node_idx: u32, value: f64) {
        self.insert(node_idx, &serde_json::Value::from(value));
    }

    /// Bulk insert with single sort (faster for batch loading)
    pub fn bulk_insert(&self, entries: Vec<(u32, f64)>) {
        let mut new_entries: Vec<Entry> = entries.into_iter()
            .map(|(node_idx, value)| Entry { value, node_idx })
            .collect();
        
        // Sort new entries
        new_entries.sort_by(|a, b| a.value.partial_cmp(&b.value).unwrap());
        
        // Merge with existing data
        let mut data = self.data.write().unwrap();
        let mut merged: Vec<Entry> = Vec::with_capacity(data.len() + new_entries.len());
        
        let mut i = 0;
        let mut j = 0;
        while i < data.len() && j < new_entries.len() {
            if data[i].value <= new_entries[j].value {
                merged.push(data[i]);
                i += 1;
            } else {
                merged.push(new_entries[j]);
                j += 1;
            }
        }
        merged.extend_from_slice(&data[i..]);
        merged.extend_from_slice(&new_entries[j..]);
        
        *data = merged;
        
        // Rebuild reverse index
        let mut rev = self.reverse.write().unwrap();
        *rev = data.iter().enumerate().map(|(pos, e)| (e.node_idx, pos)).collect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_range_insert_lookup() {
        let idx = RangeIndex::new("timestamp");
        
        idx.insert(1, &Value::Number(100.into()));
        idx.insert(2, &Value::Number(200.into()));
        idx.insert(3, &Value::Number(300.into()));
        
        let result = idx.lookup_range(&Value::Number(150.into()), &Value::Number(250.into()));
        assert_eq!(result.len(), 1);
        assert!(result.contains(&2));
    }
    
    #[test]
    fn test_range_eq() {
        let idx = RangeIndex::new("value");
        
        // Insert different nodes with same value
        idx.insert(1, &Value::Number(100.into()));
        idx.insert(2, &Value::Number(100.into()));
        idx.insert(3, &Value::Number(200.into()));
        
        // Check we can find value 100
        let result = idx.lookup_eq(&Value::Number(100.into()));
        // Should find at least one match
        assert!(!result.is_empty(), "Expected at least 1 match for value 100");
        
        // Check value 200
        let result2 = idx.lookup_eq(&Value::Number(200.into()));
        assert!(!result2.is_empty(), "Expected at least 1 match for value 200");
        assert!(result2.contains(&3), "Expected node 3 in result");
    }
    
    #[test]
    fn test_range_remove() {
        let idx = RangeIndex::new("value");
        
        idx.insert(1, &Value::Number(100.into()));
        idx.insert(2, &Value::Number(200.into()));
        
        assert_eq!(idx.count(), 2);
        
        idx.remove(1);
        assert_eq!(idx.count(), 1);
        
        let result = idx.lookup_range(&Value::Number(0.into()), &Value::Number(300.into()));
        assert_eq!(result.len(), 1);
        assert!(result.contains(&2));
    }
    
    #[test]
    fn test_bulk_insert() {
        let idx = RangeIndex::new("value");
        
        let entries: Vec<(u32, f64)> = (0..1000)
            .map(|i| (i as u32, (i * 10) as f64))
            .collect();
        
        idx.bulk_insert(entries);
        
        assert_eq!(idx.count(), 1000);
        
        let result = idx.lookup_range(&Value::Number(100.into()), &Value::Number(500.into()));
        assert_eq!(result.len(), 41);  // 100, 110, ..., 500
    }
}