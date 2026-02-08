//! Bitmap-based Traversal Acceleration
//!
//! This module provides Roaring Bitmap-style operations for efficient
//! BFS/DFS frontier management during graph traversal.
//!
//! # Benefits
//!
//! - **O(1) set operations**: Union, intersection, difference in single pass
//! - **Cache-friendly**: Packed 64-bit words for vectorized operations
//! - **Memory efficient**: Variable-size, auto-growing bit array
//! - **Fast BFS**: Efficient frontier management with bitwise operations
//!
//! # Usage
//!
//! ```rust
//! use sekejap::graph::bitmap::Bitmap;
//!
//! let mut frontier = Bitmap::new(1000);
//! frontier.set(1);
//! frontier.set(2);
//! frontier.set(3);
//!
//! let mut visited = Bitmap::new(1000);
//! visited.union_assign(&frontier);
//!
//! let next_frontier = frontier.difference(&visited);
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;

/// Bitmap configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitmapConfig {
    /// Initial capacity in bits
    pub initial_capacity: usize,
    /// Grow factor when resizing
    pub grow_factor: f64,
}

impl Default for BitmapConfig {
    fn default() -> Self {
        Self {
            initial_capacity: 1024,
            grow_factor: 1.5,
        }
    }
}

/// Roaring-style Bitmap for Efficient Set Operations
///
/// Provides fast set operations (union, intersection, difference) for
/// graph traversal frontier management.
#[derive(Debug, Serialize, Deserialize)]
pub struct Bitmap {
    /// 64-bit words (each represents 64 bits)
    words: Vec<u64>,
    /// Total number of bits
    capacity: usize,
    /// Number of set bits
    cardinality: usize,
    /// Configuration
    config: BitmapConfig,
}

impl Bitmap {
    /// Create a new bitmap with capacity
    pub fn new(capacity: usize) -> Self {
        Self::with_config(capacity, BitmapConfig::default())
    }

    /// Create with custom configuration
    pub fn with_config(capacity: usize, config: BitmapConfig) -> Self {
        let words = vec![0u64; capacity.div_ceil(64)];
        Self {
            words,
            capacity,
            cardinality: 0,
            config,
        }
    }

    /// Create from existing words (takes ownership)
    pub fn from_words(words: Vec<u64>) -> Self {
        let capacity = words.len() * 64;
        let cardinality = words.iter().map(|w| w.count_ones() as usize).sum();
        Self {
            words,
            capacity,
            cardinality,
            config: BitmapConfig::default(),
        }
    }

    /// Get word index for a bit
    fn word_index(bit: usize) -> usize {
        bit / 64
    }

    /// Get bit mask for a bit within a word
    fn bit_mask(bit: usize) -> u64 {
        1u64 << (bit % 64)
    }

    /// Ensure capacity for a given bit
    fn ensure_capacity(&mut self, bit: usize) {
        if bit >= self.capacity {
            let new_capacity = ((bit as f64 * self.config.grow_factor) / 64.0).ceil() as usize * 64;
            self.words.resize(new_capacity, 0u64);
            self.capacity = new_capacity;
        }
    }

    /// Set a bit
    pub fn set(&mut self, bit: usize) -> bool {
        self.ensure_capacity(bit);
        let word_idx = Self::word_index(bit);
        let mask = Self::bit_mask(bit);
        let was_set = self.words[word_idx] & mask != 0;
        if !was_set {
            self.words[word_idx] |= mask;
            self.cardinality += 1;
        }
        !was_set
    }

    /// Clear a bit
    pub fn clear(&mut self, bit: usize) -> bool {
        if bit >= self.capacity {
            return false;
        }
        let word_idx = Self::word_index(bit);
        let mask = Self::bit_mask(bit);
        let was_set = self.words[word_idx] & mask != 0;
        if was_set {
            self.words[word_idx] &= !mask;
            self.cardinality -= 1;
        }
        was_set
    }

    /// Check if a bit is set
    pub fn contains(&self, bit: usize) -> bool {
        if bit >= self.capacity {
            return false;
        }
        let word_idx = Self::word_index(bit);
        let mask = Self::bit_mask(bit);
        self.words[word_idx] & mask != 0
    }

    /// Get cardinality (number of set bits)
    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.cardinality == 0
    }

    /// Get capacity in bits
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get next set bit at or after position
    pub fn next_set_bit(&self, from: usize) -> Option<usize> {
        let mut word_idx = Self::word_index(from);
        let bit_offset = from % 64;

        if word_idx >= self.words.len() {
            return None;
        }

        // Check current word
        let word = self.words[word_idx] >> bit_offset;
        if word != 0 {
            let bit = word.trailing_zeros() as usize;
            return Some(word_idx * 64 + bit_offset + bit);
        }

        // Check subsequent words
        word_idx += 1;
        while word_idx < self.words.len() {
            if self.words[word_idx] != 0 {
                let bit = self.words[word_idx].trailing_zeros() as usize;
                return Some(word_idx * 64 + bit);
            }
            word_idx += 1;
        }

        None
    }

    /// Get first set bit
    pub fn first(&self) -> Option<usize> {
        self.next_set_bit(0)
    }

    /// Get last set bit
    pub fn last(&self) -> Option<usize> {
        let mut word_idx = self.words.len();
        while word_idx > 0 {
            word_idx -= 1;
            if self.words[word_idx] != 0 {
                let bit = 63 - self.words[word_idx].leading_zeros() as usize;
                return Some(word_idx * 64 + bit);
            }
        }
        None
    }

    /// Union: self |= other
    pub fn union_assign(&mut self, other: &Bitmap) {
        self.ensure_capacity(other.capacity);
        for (i, word) in self.words.iter_mut().enumerate() {
            if i < other.words.len() {
                *word |= other.words[i];
            }
        }
        self.recalculate_cardinality();
    }

    /// Intersection: self &= other
    pub fn intersect_assign(&mut self, other: &Bitmap) {
        let min_len = std::cmp::min(self.words.len(), other.words.len());
        for i in 0..min_len {
            self.words[i] &= other.words[i];
        }
        self.words.truncate(min_len);
        self.recalculate_cardinality();
    }

    /// Difference: self -= other (elements in other removed from self)
    pub fn difference_assign(&mut self, other: &Bitmap) {
        for i in 0..std::cmp::min(self.words.len(), other.words.len()) {
            self.words[i] &= !other.words[i];
        }
        self.recalculate_cardinality();
    }

    /// Symmetric difference: self ^= other
    pub fn symmetric_difference_assign(&mut self, other: &Bitmap) {
        self.ensure_capacity(other.capacity);
        for (i, word) in self.words.iter_mut().enumerate() {
            if i < other.words.len() {
                *word ^= other.words[i];
            }
        }
        self.recalculate_cardinality();
    }

    /// Create a new bitmap as union
    pub fn union(&self, other: &Bitmap) -> Bitmap {
        let mut result = self.clone();
        result.union_assign(other);
        result
    }

    /// Create a new bitmap as intersection
    pub fn intersect(&self, other: &Bitmap) -> Bitmap {
        let min_len = std::cmp::min(self.words.len(), other.words.len());
        let mut words = self.words[..min_len].to_vec();
        for (i, word) in words.iter_mut().enumerate() {
            *word &= other.words[i];
        }
        Bitmap::from_words(words)
    }

    /// Create a new bitmap as difference
    pub fn difference(&self, other: &Bitmap) -> Bitmap {
        let mut result = self.clone();
        result.difference_assign(other);
        result
    }

    /// Check if two bitmaps have any common elements
    pub fn intersects(&self, other: &Bitmap) -> bool {
        let min_len = std::cmp::min(self.words.len(), other.words.len());
        for i in 0..min_len {
            if self.words[i] & other.words[i] != 0 {
                return true;
            }
        }
        false
    }

    /// Check if self is a subset of other
    pub fn is_subset_of(&self, other: &Bitmap) -> bool {
        for (i, &word) in self.words.iter().enumerate() {
            if i >= other.words.len() {
                if word != 0 {
                    return false;
                }
            } else if word & !other.words[i] != 0 {
                return false;
            }
        }
        true
    }

    /// Check if self is a superset of other
    pub fn is_superset_of(&self, other: &Bitmap) -> bool {
        other.is_subset_of(self)
    }

    /// Clear all bits
    pub fn clear_all(&mut self) {
        self.words.fill(0);
        self.cardinality = 0;
    }

    /// Get memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        self.words.len() * std::mem::size_of::<u64>()
    }

    /// Serialize to Vec<u64>
    pub fn to_u64_slice(&self) -> &[u64] {
        &self.words
    }

    /// Create from Vec<u64>
    pub fn from_u64_slice(words: &[u64]) -> Self {
        Self::from_words(words.to_vec())
    }

    /// Recalculate cardinality (used after operations)
    fn recalculate_cardinality(&mut self) {
        self.cardinality = self.words.iter().map(|w| w.count_ones() as usize).sum();
    }

    /// Iterate over set bits
    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.words.iter().enumerate().flat_map(|(word_idx, &word)| {
            if word == 0 {
                return Vec::new();
            }
            let mut bits = Vec::new();
            let mut w = word;
            while w != 0 {
                let bit = w.trailing_zeros() as usize;
                bits.push(word_idx * 64 + bit);
                w &= w - 1; // Clear lowest set bit
            }
            bits
        })
    }

    /// Convert to Vec<usize>
    pub fn to_vec(&self) -> Vec<usize> {
        self.iter().collect()
    }
}

impl Default for Bitmap {
    fn default() -> Self {
        Self::new(1024)
    }
}

impl Clone for Bitmap {
    fn clone(&self) -> Self {
        Self {
            words: self.words.clone(),
            capacity: self.capacity,
            cardinality: self.cardinality,
            config: self.config.clone(),
        }
    }
}

impl fmt::Display for Bitmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "Bitmap: {} bits set, capacity: {} bits, memory: {} KB",
            self.cardinality,
            self.capacity,
            self.memory_usage() / 1024
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_basic_operations() {
        let mut bitmap = Bitmap::new(256);

        // Set some bits
        assert!(bitmap.set(0));
        assert!(bitmap.set(1));
        assert!(bitmap.set(64));
        assert!(bitmap.set(100));
        assert!(!bitmap.set(0)); // Already set

        // Check bits
        assert!(bitmap.contains(0));
        assert!(bitmap.contains(1));
        assert!(bitmap.contains(64));
        assert!(bitmap.contains(100));
        assert!(!bitmap.contains(2));
        assert!(!bitmap.contains(200));

        // Cardinality
        assert_eq!(bitmap.cardinality(), 4);
    }

    #[test]
    fn test_bitmap_clear() {
        let mut bitmap = Bitmap::new(256);
        bitmap.set(5);
        bitmap.set(10);

        assert!(bitmap.contains(5));
        assert!(bitmap.clear(5));
        assert!(!bitmap.contains(5));
        assert!(!bitmap.clear(5)); // Already clear

        assert_eq!(bitmap.cardinality(), 1);
    }

    #[test]
    fn test_bitmap_union() {
        let mut bitmap1 = Bitmap::new(256);
        let mut bitmap2 = Bitmap::new(256);

        bitmap1.set(1);
        bitmap1.set(2);
        bitmap2.set(2);
        bitmap2.set(3);

        let union = bitmap1.union(&bitmap2);

        assert!(union.contains(1));
        assert!(union.contains(2));
        assert!(union.contains(3));
        assert_eq!(union.cardinality(), 3);
    }

    #[test]
    fn test_bitmap_intersection() {
        let mut bitmap1 = Bitmap::new(256);
        let mut bitmap2 = Bitmap::new(256);

        bitmap1.set(1);
        bitmap1.set(2);
        bitmap2.set(2);
        bitmap2.set(3);

        let intersect = bitmap1.intersect(&bitmap2);

        assert!(!intersect.contains(1));
        assert!(intersect.contains(2));
        assert!(!intersect.contains(3));
        assert_eq!(intersect.cardinality(), 1);
    }

    #[test]
    fn test_bitmap_difference() {
        let mut bitmap1 = Bitmap::new(256);
        let mut bitmap2 = Bitmap::new(256);

        bitmap1.set(1);
        bitmap1.set(2);
        bitmap1.set(3);
        bitmap2.set(2);

        let diff = bitmap1.difference(&bitmap2);

        assert!(diff.contains(1));
        assert!(!diff.contains(2));
        assert!(diff.contains(3));
        assert_eq!(diff.cardinality(), 2);
    }

    #[test]
    fn test_bitmap_iteration() {
        let mut bitmap = Bitmap::new(256);
        bitmap.set(0);
        bitmap.set(64);
        bitmap.set(100);

        let items: Vec<_> = bitmap.iter().collect();
        assert_eq!(items, vec![0, 64, 100]);
    }

    #[test]
    fn test_bitmap_next_set_bit() {
        let mut bitmap = Bitmap::new(256);
        bitmap.set(10);
        bitmap.set(50);
        bitmap.set(100);

        assert_eq!(bitmap.next_set_bit(0), Some(10));
        assert_eq!(bitmap.next_set_bit(10), Some(10));
        assert_eq!(bitmap.next_set_bit(11), Some(50));
        assert_eq!(bitmap.next_set_bit(51), Some(100));
        assert_eq!(bitmap.next_set_bit(101), None);
    }

    #[test]
    fn test_bitmap_intersects() {
        let mut bitmap1 = Bitmap::new(256);
        let mut bitmap2 = Bitmap::new(256);

        bitmap1.set(1);
        bitmap1.set(2);
        bitmap2.set(2);
        bitmap2.set(3);

        assert!(bitmap1.intersects(&bitmap2));
    }

    #[test]
    fn test_bitmap_subsets() {
        let mut bitmap1 = Bitmap::new(256);
        let mut bitmap2 = Bitmap::new(256);

        bitmap1.set(1);
        bitmap1.set(2);
        bitmap2.set(1);
        bitmap2.set(2);
        bitmap2.set(3);

        assert!(bitmap1.is_subset_of(&bitmap2));
        assert!(bitmap2.is_superset_of(&bitmap1));
    }

    #[test]
    fn test_bitmap_grow() {
        let mut bitmap = Bitmap::new(64); // Start with 1 word
        bitmap.set(0);
        bitmap.set(100); // This should trigger growth
        bitmap.set(200);

        assert!(bitmap.contains(0));
        assert!(bitmap.contains(100));
        assert!(bitmap.contains(200));
        assert_eq!(bitmap.cardinality(), 3);
    }

    #[test]
    fn test_bitmap_display() {
        let mut bitmap = Bitmap::new(256);
        bitmap.set(1);
        bitmap.set(2);

        let display = format!("{}", bitmap);
        assert!(display.contains("Bitmap"));
        assert!(display.contains("bits set"));
    }

    #[test]
    fn test_bitmap_serialization() {
        let mut bitmap = Bitmap::new(256);
        bitmap.set(1);
        bitmap.set(100);
        bitmap.set(200);

        let words = bitmap.to_u64_slice().to_vec();
        let restored = Bitmap::from_u64_slice(&words);

        assert!(restored.contains(1));
        assert!(restored.contains(100));
        assert!(restored.contains(200));
        assert_eq!(restored.cardinality(), 3);
    }
}
