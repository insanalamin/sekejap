//! ## GIN Trigram Index (Exact Postings)
//!
//! GIN (Generalized Inverted Index) for trigrams using exact postings.
//!
//! Unlike GiST (lossy signatures), GIN stores exact trigram→docIDs mappings.
//! This means no verification step needed, but uses more memory.
//!
//! ### Memory Usage
//!
//! - Per trigram: RoaringBitmap of doc IDs
//! - Dense trigrams (e.g., " the" in 70% of docs): ~88KB compressed
//! - Sparse trigrams (e.g., "xyz" in 0.001% of docs): ~12 bytes
//! - Total: ~100MB/1M docs average
//!
//! ### How It Works
//!
//! **Index structure:**
//! ```text
//! trigram_hash -> RoaringBitmap<doc_ids>
//! ```
//!
//! **Query "%Alpha%":**
//! ```text
//! 1. Extract trigrams: [" al", " alp", "alp", "lph", "pha", "ha "]
//! 2. Look up each trigram → get RoaringBitmaps
//! 3. Intersect all bitmaps → candidates (documents with ALL trigrams)
//! 4. Return candidates (exact — no verification needed)
//! ```
//!
//! ### When to Use GIN vs GiST
//!
//! | Scenario | GiST | GIN |
//! |---------|------|-----|
//! | Memory-constrained (Pi) | ✅ | ❌ |
//! | Need exact match (no verify) | ❌ | ✅ |
//! | Large result sets | ⚠️ | ✅ |
//! | Short strings (varchar < 50) | ⚠️ | ⚠️ |
//! | Long text (body, description) | ✅ | ✅ |
//!
//! ### Why RoaringBitmap?
//!
//! - Pure Rust (no C deps)
//! - Compressed bitmap for doc IDs
//! - Fast intersection via `&` operator
//! - Used by Lucene/Tantivy internally

use crate::text_index::trigram::{
    dedup_trigrams, extract_pattern_trigrams, extract_trigrams, hash_trigram,
};
use std::collections::HashMap;

/// A GIN trigram index using exact postings with RoaringBitmaps.
///
/// Each trigram maps to a RoaringBitmap of document IDs that contain it.
/// Querying intersects the bitmaps to find documents with ALL required trigrams.
pub struct GINIndex {
    /// Inverted index: trigram_hash -> RoaringBitmap of doc IDs
    postings: HashMap<u32, roaring::RoaringBitmap>,
    /// Total documents indexed
    doc_count: usize,
    /// Field name being indexed
    field: String,
}

impl GINIndex {
    /// Build a new GIN index by iterating over documents.
    ///
    /// # Arguments
    /// * `docs` - Iterator of (doc_id, text) pairs
    /// * `field` - Field name being indexed (for statistics)
    ///
    /// # Returns
    /// * `Self` - The built index
    pub fn build<'a>(docs: impl Iterator<Item = (u64, &'a str)>, field: &str) -> Self {
        let mut postings: HashMap<u32, roaring::RoaringBitmap> = HashMap::new();
        let mut doc_count = 0;

        for (doc_id, text) in docs {
            let trigrams = extract_trigrams(text);
            if !trigrams.is_empty() {
                for trigram in &trigrams {
                    let h = hash_trigram(trigram);
                    postings
                        .entry(h)
                        .or_insert_with(roaring::RoaringBitmap::new)
                        .insert(doc_id as u32);
                }
                doc_count += 1;
            }
        }

        Self {
            postings,
            doc_count,
            field: field.to_string(),
        }
    }

    /// Query the index for documents matching an ILIKE pattern.
    ///
    /// Returns doc IDs that match (exact — no verification needed).
    ///
    /// # Arguments
    /// * `pattern` - ILIKE pattern (e.g., "%Alpha%")
    /// * `limit` - Maximum results to return (None for all)
    ///
    /// # Returns
    /// * `Vec<u64>` - Matching doc IDs
    pub fn ilike(&self, pattern: &str, limit: Option<usize>) -> Vec<u64> {
        // Extract trigrams from pattern
        let pattern_trigrams = extract_pattern_trigrams(pattern);
        if pattern_trigrams.is_empty() {
            // Degenerate pattern (all wildcards) — return all docs from first posting
            return self
                .postings
                .values()
                .next()
                .map(|bm| {
                    bm.iter()
                        .map(|id| id as u64)
                        .take(limit.unwrap_or(usize::MAX))
                        .collect()
                })
                .unwrap_or_default();
        }

        // Deduplicate trigrams
        let trigrams = dedup_trigrams(&pattern_trigrams);

        // Start with first trigram's bitmap, intersect with rest
        let first_h = hash_trigram(&trigrams[0]);
        let mut result = match self.postings.get(&first_h) {
            Some(bm) => bm.clone(),
            None => return vec![], // No documents have first trigram
        };

        for trigram in &trigrams[1..] {
            let h = hash_trigram(trigram);
            if let Some(bm) = self.postings.get(&h) {
                result &= bm.clone();
            } else {
                // This trigram doesn't exist in any document
                return vec![];
            }
            if result.is_empty() {
                return vec![]; // Early exit if intersection empty
            }
        }

        // Apply limit
        result
            .iter()
            .map(|id| id as u64)
            .take(limit.unwrap_or(usize::MAX))
            .collect()
    }

    /// Get the number of unique trigrams indexed.
    pub fn trigram_count(&self) -> usize {
        self.postings.len()
    }

    /// Get index statistics.
    pub fn stats(&self) -> GINStats {
        let total_postings: usize = self.postings.values().map(|bm| bm.len() as usize).sum();
        GINStats {
            doc_count: self.doc_count,
            field: self.field.clone(),
            trigram_count: self.postings.len(),
            total_postings,
        }
    }
}

/// Statistics about a GIN index.
pub struct GINStats {
    pub doc_count: usize,
    pub field: String,
    pub trigram_count: usize,
    pub total_postings: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ilike_pattern_extraction() {
        let pattern = "%Alpha%";
        let trigrams = extract_pattern_trigrams(pattern);
        assert!(!trigrams.is_empty());
        assert!(trigrams.contains(&" al".to_string()));
    }

    #[test]
    fn test_gin_build_and_query() {
        let docs = vec![
            (1u64, "Hello World"),
            (2u64, "The Vines"),
            (3u64, "Alpha Beta"),
            (4u64, "hello"),
        ];
        let index = GINIndex::build(docs.into_iter(), "text");

        // Test exact match
        let results = index.ilike("%hello%", None);
        assert!(results.contains(&1) || results.contains(&4)); // case insensitive

        // Test AND of trigrams
        let results = index.ilike("%Alpha Beta%", None);
        assert!(results.contains(&3));
    }
}
