//! Bloom Filter for Fast Edge Existence Checks
//!
//! This module provides a probabilistic data structure for O(k) edge existence
//! queries with no false negatives and configurable false positive rate.
//!
//! # Benefits
//!
//! - **Speed**: O(k) lookup where k = number of hash functions (typically 3-7)
//! - **No false negatives**: If we say edge doesn't exist, it really doesn't
//! - **Memory efficient**: 1 bit per element per 10% false positive rate
//! - **Cache-friendly**: Compact bit array fits in CPU cache
//!
//! # Usage
//!
//! ```rust
//! use hsdl_sekejap::graph::bloom::BloomFilter;
//!
//! let mut filter = BloomFilter::new(10000, 0.01); // 10K elements, 1% FPR
//! filter.add_edge(1, 2);
//! assert!(filter.has_edge(1, 2));  // Always true for added edges
//! assert!(!filter.has_edge(1, 3)); // Probably false (may be false positive)
//! ```

use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::fmt;

/// Configuration for Bloom filter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomConfig {
    /// Expected number of elements
    pub expected_elements: usize,
    /// False positive rate (0.0 to 1.0)
    pub false_positive_rate: f64,
    /// Number of hash functions
    pub num_hashes: usize,
    /// Size of bit array in bits
    pub bit_array_size: usize,
}

impl Default for BloomConfig {
    fn default() -> Self {
        Self::with_elements_and_fpr(10000, 0.01)
    }
}

impl BloomConfig {
    /// Create config for expected elements with target false positive rate
    ///
    /// Uses optimal number of hash functions: k = (m/n) * ln(2)
    /// where m = bit array size, n = expected elements
    pub fn with_elements_and_fpr(expected_elements: usize, fpr: f64) -> Self {
        // Calculate optimal bit array size: m = -n * ln(p) / (ln(2))^2
        let ln2_sq = (std::f64::consts::LN_2).powi(2);
        let bit_array_size = ((expected_elements as f64 * (-fpr.ln())) / ln2_sq).ceil() as usize;

        // Calculate optimal number of hash functions: k = (m/n) * ln(2)
        let num_hashes = ((bit_array_size as f64 / expected_elements as f64) * std::f64::consts::LN_2)
            .ceil() as usize;

        // Ensure at least 1 hash function
        let num_hashes = num_hashes.max(1);

        Self {
            expected_elements,
            false_positive_rate: fpr,
            num_hashes,
            bit_array_size,
        }
    }

    /// Get memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        self.bit_array_size.div_ceil(8) // Round up to bytes
    }
}

/// Bloom Filter for Edge Existence Checks
///
/// Probabilistic data structure for fast membership queries.
/// - `add_edge()`: O(k) - always adds the edge
/// - `has_edge()`: O(k) - never false negatives, configurable false positives
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilter {
    /// Configuration
    config: BloomConfig,
    /// Bit array (packed bits)
    bit_array: Vec<u8>,
    /// Number of elements added
    count: usize,
}

impl BloomFilter {
    /// Create a new Bloom filter with default config
    pub fn new() -> Self {
        Self::with_config(BloomConfig::default())
    }

    /// Create with expected elements and false positive rate
    pub fn with_elements_and_fpr(expected_elements: usize, fpr: f64) -> Self {
        Self::with_config(BloomConfig::with_elements_and_fpr(expected_elements, fpr))
    }

    /// Create with custom configuration
    pub fn with_config(config: BloomConfig) -> Self {
        let bit_array_size_bytes = config.bit_array_size.div_ceil(8);
        Self {
            config,
            bit_array: vec![0u8; bit_array_size_bytes],
            count: 0,
        }
    }

    /// Get hash values for an edge (source, target)
    ///
    /// Uses double hashing technique for k hash functions
    /// h_i(x) = h1(x) + i * h2(x) mod m
    fn get_hashes(&self, source: u64, target: u64) -> Vec<usize> {
        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        (source, target).hash(&mut hasher1);
        (source, target).hash(&mut hasher2);
        let h1 = hasher1.finish() as usize;
        let h2 = hasher2.finish() as usize;

        let m = self.config.bit_array_size;
        (0..self.config.num_hashes)
            .map(|i| (h1.wrapping_add(i.wrapping_mul(h2))) % m)
            .collect()
    }

    /// Set a bit in the bit array
    #[allow(dead_code)]
    pub(crate) fn set_bit(&mut self, index: usize) {
        let byte_index = index / 8;
        let bit_index = index % 8;
        self.bit_array[byte_index] |= 1 << bit_index;
    }

    /// Check if a bit is set
    fn get_bit(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let bit_index = index % 8;
        (self.bit_array[byte_index] >> bit_index) & 1 != 0
    }

    /// Add an edge to the filter
    ///
    /// Returns true if this is a new edge (bit was not already set)
    pub fn add_edge(&mut self, source: u64, target: u64) -> bool {
        let hashes = self.get_hashes(source, target);
        let mut was_new = false;

        for &h in &hashes {
            let byte_index = h / 8;
            let bit_index = h % 8;
            let old_bit = self.bit_array[byte_index] & (1 << bit_index);
            if old_bit == 0 {
                was_new = true;
            }
            self.bit_array[byte_index] |= 1 << bit_index;
        }

        if was_new {
            self.count += 1;
        }
        was_new
    }

    /// Check if an edge might exist
    ///
    /// Returns:
    /// - `true`: Edge definitely exists OR false positive
    /// - `false`: Edge definitely does not exist
    pub fn has_edge(&self, source: u64, target: u64) -> bool {
        let hashes = self.get_hashes(source, target);
        hashes.iter().all(|&h| self.get_bit(h))
    }

    /// Check if edge definitely does NOT exist
    pub fn may_have_edge(&self, source: u64, target: u64) -> bool {
        self.has_edge(source, target)
    }

    /// Get current false positive rate estimate
    pub fn current_fpr(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        // FPR = (1 - e^(-kn/m))^k
        let n = self.count as f64;
        let m = self.config.bit_array_size as f64;
        let k = self.config.num_hashes as f64;
        let exponent = -(k * n) / m;
        (1.0 - exponent.exp()).powf(k)
    }

    /// Get number of elements in filter
    pub fn len(&self) -> usize {
        self.count
    }

    /// Check if filter is empty
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        self.bit_array.len()
            + std::mem::size_of::<BloomConfig>()
            + std::mem::size_of::<usize>()
    }

    /// Clear the filter
    pub fn clear(&mut self) {
        self.bit_array.fill(0);
        self.count = 0;
    }

    /// Merge two Bloom filters (for parallel query routing)
    ///
    /// Returns a new filter that is the union of both
    pub fn merge(&self, other: &BloomFilter) -> Option<Self> {
        if self.config.bit_array_size != other.config.bit_array_size {
            return None;
        }

        let mut result = self.clone();
        for (i, &byte) in self.bit_array.iter().enumerate() {
            result.bit_array[i] = byte | other.bit_array[i];
        }
        result.count = self.count.max(other.count);
        Some(result)
    }

    /// Serialize to bytes for persistence
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap_or_default()
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        bincode::deserialize(data).ok()
    }
}

impl Default for BloomFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for BloomFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "BloomFilter: {} elements, {:.2}% FPR, {} bits",
            self.count,
            self.current_fpr() * 100.0,
            self.config.bit_array_size
        )?;
        writeln!(f, "Memory: {} bytes", self.memory_usage())?;
        writeln!(
            f,
            "Hash functions: {}, Capacity: {} elements",
            self.config.num_hashes, self.config.expected_elements
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_basic_operations() {
        let mut filter = BloomFilter::with_elements_and_fpr(1000, 0.01);

        // Add some edges
        filter.add_edge(1, 2);
        filter.add_edge(1, 3);
        filter.add_edge(2, 3);

        // Check added edges exist
        assert!(filter.has_edge(1, 2));
        assert!(filter.has_edge(1, 3));
        assert!(filter.has_edge(2, 3));

        // Check non-added edges (may be false positive, but should rarely be true)
        let false_positives: usize = (0..100)
            .map(|i| filter.has_edge(1000 + i as u64, 2000 + i as u64) as usize)
            .sum();

        // With 1% FPR, expect < 5 false positives out of 100
        assert!(false_positives < 5);
    }

    #[test]
    fn test_bloom_no_false_negatives() {
        let mut filter = BloomFilter::with_elements_and_fpr(1000, 0.01);

        // Add many edges
        for i in 0..500 {
            for j in 0..10 {
                filter.add_edge(i as u64, (i + j) as u64);
            }
        }

        // Verify all added edges are found
        for i in 0..500 {
            for j in 0..10 {
                assert!(filter.has_edge(i as u64, (i + j) as u64));
            }
        }
    }

    #[test]
    fn test_bloom_merge() {
        let mut filter1 = BloomFilter::with_elements_and_fpr(1000, 0.01);
        let mut filter2 = BloomFilter::with_elements_and_fpr(1000, 0.01);

        filter1.add_edge(1, 2);
        filter1.add_edge(1, 3);

        filter2.add_edge(2, 3);
        filter2.add_edge(3, 4);

        let merged = filter1.merge(&filter2).unwrap();

        // All edges should be in merged filter
        assert!(merged.has_edge(1, 2));
        assert!(merged.has_edge(1, 3));
        assert!(merged.has_edge(2, 3));
        assert!(merged.has_edge(3, 4));
    }

    #[test]
    fn test_bloom_serialization() {
        let mut filter = BloomFilter::with_elements_and_fpr(1000, 0.01);
        filter.add_edge(1, 2);
        filter.add_edge(3, 4);

        let bytes = filter.to_bytes();
        let restored = BloomFilter::from_bytes(&bytes).unwrap();

        assert!(restored.has_edge(1, 2));
        assert!(restored.has_edge(3, 4));
    }

    #[test]
    fn test_bloom_display() {
        let mut filter = BloomFilter::with_elements_and_fpr(1000, 0.01);
        filter.add_edge(1, 2);

        let display = format!("{}", filter);
        assert!(display.contains("BloomFilter"));
        assert!(display.contains("elements"));
        assert!(display.contains("FPR"));
    }

    #[test]
    fn test_bloom_config() {
        let config = BloomConfig::with_elements_and_fpr(10000, 0.001);
        assert_eq!(config.expected_elements, 10000);
        assert_eq!(config.false_positive_rate, 0.001);
        assert!(config.num_hashes > 0);
        assert!(config.bit_array_size > 0);

        // Memory should be ~12KB for 10K elements at 0.1% FPR
        let memory_kb = config.memory_usage() / 1024;
        assert!(memory_kb > 10 && memory_kb < 20);
    }
}
