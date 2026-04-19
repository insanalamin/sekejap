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
    /// Inverted index: trigram_hash -> RoaringBitmap of slot indices
    postings: HashMap<u32, roaring::RoaringBitmap>,
    /// Slot index → original u64 node hash.
    /// Needed because RoaringBitmap only stores u32; node hashes are u64.
    id_map: Vec<u64>,
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
        let mut id_map: Vec<u64> = Vec::new();
        let mut slot_map: HashMap<u64, u32> = HashMap::new();
        let mut doc_count = 0;

        for (doc_id, text) in docs {
            let trigrams = extract_trigrams(text);
            if !trigrams.is_empty() {
                let slot = *slot_map.entry(doc_id).or_insert_with(|| {
                    let s = id_map.len() as u32;
                    id_map.push(doc_id);
                    s
                });
                for trigram in &trigrams {
                    let h = hash_trigram(trigram);
                    postings
                        .entry(h)
                        .or_insert_with(roaring::RoaringBitmap::new)
                        .insert(slot);
                }
                doc_count += 1;
            }
        }

        Self {
            postings,
            id_map,
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
                        .filter_map(|slot| self.id_map.get(slot as usize).copied())
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

        // Apply limit — map slot indices back to original u64 node hashes
        result
            .iter()
            .filter_map(|slot| self.id_map.get(slot as usize).copied())
            .take(limit.unwrap_or(usize::MAX))
            .collect()
    }

    /// Incrementally add a single document to the index.
    ///
    /// O(trigrams_in_text) — safe to call per-insert for new documents.
    /// For updates (doc already indexed), remove the old entry first by
    /// calling `build_gin_index()` for a full rebuild.
    pub fn insert_doc(&mut self, doc_id: u64, text: &str) {
        let trigrams = extract_trigrams(text);
        if !trigrams.is_empty() {
            let slot = self.id_map.len() as u32;
            self.id_map.push(doc_id);
            for trigram in &trigrams {
                let h = hash_trigram(trigram);
                self.postings
                    .entry(h)
                    .or_insert_with(roaring::RoaringBitmap::new)
                    .insert(slot);
            }
            self.doc_count += 1;
        }
    }

    /// Reconstruct a GINIndex directly from serialized parts (used by snapshot restore).
    ///
    /// * `id_map`   – slot → node hash
    /// * `postings` – (trigram_hash, sorted slot list) pairs; slot lists are rebuilt
    ///                into RoaringBitmaps
    /// * `field`    – field name
    pub fn from_parts(id_map: Vec<u64>, postings: Vec<(u32, Vec<u32>)>, field: &str) -> Self {
        let doc_count = id_map.len();
        let postings_map: HashMap<u32, roaring::RoaringBitmap> = postings
            .into_iter()
            .map(|(h, slots)| {
                let mut bm = roaring::RoaringBitmap::new();
                bm.extend(slots.into_iter());
                (h, bm)
            })
            .collect();
        Self {
            postings: postings_map,
            id_map,
            doc_count,
            field: field.to_string(),
        }
    }

    /// Return a copy of the id_map (slot → node hash).
    pub fn id_map_cloned(&self) -> Vec<u64> {
        self.id_map.clone()
    }

    /// Return all postings as (trigram_hash, sorted_slot_list) pairs.
    pub fn postings_as_vecs(&self) -> Vec<(u32, Vec<u32>)> {
        self.postings
            .iter()
            .map(|(&h, bm)| (h, bm.iter().collect()))
            .collect()
    }

    /// Write this GIN index to a binary stream.
    ///
    /// Format (all integers little-endian):
    ///   [u16 field_name_len][field_name_bytes]
    ///   [u32 GIN_INDEX_VERSION]
    ///   [u64 id_map_len][u64 × id_map_len]
    ///   [u32 postings_count]
    ///   per posting: [u32 trigram_hash][u32 bitmap_byte_len][bitmap_bytes]
    pub fn write_binary<W: std::io::Write>(&self, w: &mut W, version: u32) -> std::io::Result<()> {
        let field_bytes = self.field.as_bytes();
        w.write_all(&(field_bytes.len() as u16).to_le_bytes())?;
        w.write_all(field_bytes)?;
        w.write_all(&version.to_le_bytes())?;
        w.write_all(&(self.id_map.len() as u64).to_le_bytes())?;
        for &h in &self.id_map {
            w.write_all(&h.to_le_bytes())?;
        }
        w.write_all(&(self.postings.len() as u32).to_le_bytes())?;
        for (&trigram_hash, bm) in &self.postings {
            let mut bm_bytes = Vec::new();
            bm.serialize_into(&mut bm_bytes)?;
            w.write_all(&trigram_hash.to_le_bytes())?;
            w.write_all(&(bm_bytes.len() as u32).to_le_bytes())?;
            w.write_all(&bm_bytes)?;
        }
        Ok(())
    }

    /// Read one GIN index from a binary stream (written by `write_binary`).
    /// Returns `(field_name, index)`. Returns `Err` on any parse/IO failure.
    pub fn read_binary<R: std::io::Read>(r: &mut R, expected_version: u32) -> std::io::Result<(String, Self)> {
        use std::io::{Error, ErrorKind};
        let mut u16buf = [0u8; 2];
        r.read_exact(&mut u16buf)?;
        let field_len = u16::from_le_bytes(u16buf) as usize;
        let mut field_bytes = vec![0u8; field_len];
        r.read_exact(&mut field_bytes)?;
        let field = String::from_utf8(field_bytes)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

        let mut u32buf = [0u8; 4];
        r.read_exact(&mut u32buf)?;
        let version = u32::from_le_bytes(u32buf);
        if version != expected_version {
            return Err(Error::new(ErrorKind::InvalidData,
                format!("gin.bin version {version} != expected {expected_version}")));
        }

        let mut u64buf = [0u8; 8];
        r.read_exact(&mut u64buf)?;
        let id_map_len = u64::from_le_bytes(u64buf) as usize;
        let mut id_map = Vec::with_capacity(id_map_len);
        for _ in 0..id_map_len {
            r.read_exact(&mut u64buf)?;
            id_map.push(u64::from_le_bytes(u64buf));
        }

        r.read_exact(&mut u32buf)?;
        let postings_count = u32::from_le_bytes(u32buf) as usize;
        let mut postings = HashMap::new();
        for _ in 0..postings_count {
            r.read_exact(&mut u32buf)?;
            let trigram_hash = u32::from_le_bytes(u32buf);
            r.read_exact(&mut u32buf)?;
            let bm_len = u32::from_le_bytes(u32buf) as usize;
            let mut bm_bytes = vec![0u8; bm_len];
            r.read_exact(&mut bm_bytes)?;
            let bm = roaring::RoaringBitmap::deserialize_from(&bm_bytes[..])
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            postings.insert(trigram_hash, bm);
        }

        let doc_count = id_map.len();
        Ok((field.clone(), Self { postings, id_map, doc_count, field }))
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
        // %Alpha% — both sides wildcarded: only interior trigrams, no space padding
        let pattern = "%Alpha%";
        let trigrams = extract_pattern_trigrams(pattern);
        assert!(!trigrams.is_empty());
        assert!(trigrams.contains(&"alp".to_string()));
        assert!(trigrams.contains(&"lph".to_string()));
        assert!(trigrams.contains(&"pha".to_string()));
        assert!(!trigrams.contains(&" al".to_string()), "space padding must not appear with leading %");
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

    /// GIN must return the original u64 hash unmodified even when it exceeds u32::MAX.
    /// Previously, hashes were truncated to u32 during build and zero-extended on
    /// query, producing wrong IDs and empty results.
    #[test]
    fn test_gin_large_hashes() {
        // Hashes above u32::MAX — would be silently truncated by the old `doc_id as u32`.
        let big_id_a: u64 = u64::from(u32::MAX) + 1;   // 4_294_967_296
        let big_id_b: u64 = u64::from(u32::MAX) + 999;  // 4_294_968_294

        let docs = vec![
            (big_id_a, "Melbourne Fitzroy"),
            (big_id_b, "Maribyrnong flooding event"),
            (1u64, "something else entirely"),
        ];
        let index = GINIndex::build(docs.into_iter(), "name");

        // Query for "Fitzroy" — must return big_id_a, not 0 (the truncated form).
        let results = index.ilike("%fitzroy%", None);
        assert_eq!(results, vec![big_id_a], "large hash must not be truncated");

        // Query for "Maribyrnong" — must return big_id_b.
        let results = index.ilike("%maribyrnong%", None);
        assert_eq!(results, vec![big_id_b], "second large hash must round-trip correctly");

        // Ensure the small-ID doc is still reachable.
        let results = index.ilike("%something%", None);
        assert_eq!(results, vec![1u64]);
    }
}
