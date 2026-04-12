//! ## GiST Trigram Index (Bitmap Signature)
//!
//! GiST (Generalized Search Tree) index for trigrams using bitmap signatures.
//!
//! Each document is represented by a fixed-size bitmap signature where each
//! bit represents whether a trigram hash is present in the document.
//!
//! ### Memory Efficiency
//!
//! - 96 bits (12 bytes) per document signature
//! - Total: ~12MB/1M docs — Pi-friendly!
//!
//! ### How It Works
//!
//! **Signature generation:**
//! ```text
//! Document "Alpha" has trigrams: [" al", " alp", "alp", "lph", "pha", "ha "]
//! Hash each trigram: h1, h2, h3, h4, h5, h6
//! Set bits at positions (h1 % SIG_BITS), (h2 % SIG_BITS), etc. in signature
//! ```
//!
//! **Query:**
//! ```text
//! Query "%Alpha%" needs trigrams: [" al", " alp", ...]
//! Check which documents have ALL required bits set
//! Candidates = documents where (signature AND query_signature) == query_signature
//! ```
//!
//! ### Why Verification Step?
//!
//! GiST signatures are lossy (hash collisions, fixed size). A document might
//! pass the signature check but NOT actually contain the pattern (false positive).
//! The verification step confirms with actual ILIKE check.
//!
//! This is exactly how PostgreSQL's pg_trgm GiST works.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use sekejap::text_index::gist::GiSTIndex;
//!
//! let docs = vec![(1u64, "Hello World"), (2u64, "The Vines")];
//! let index = GiSTIndex::build(docs.into_iter(), "text");
//! let candidates = index.ilike_candidates("%Vines%", None);
//! ```

use crate::text_index::trigram::{
    dedup_trigrams, extract_pattern_trigrams, extract_trigrams, hash_trigram,
};
use std::collections::HashMap;

/// GiST Signature size in bits.
///
/// 96 bits = 12 bytes per document = ~12MB/1M docs total.
///
/// Larger signatures = fewer false positives but more memory.
/// 96 bits chosen as balance between Pi memory constraints and accuracy.
pub const GIST_SIG_BITS: usize = 96;

/// A GiST trigram index using bitmap signatures.
///
/// Each document gets a fixed-size signature bitmap. Querying checks
/// which documents have the required trigram bits set.
pub struct GiSTIndex {
    /// Document signatures: doc_id -> signature bitmap (as u128 array)
    /// 96 bits / 64 bits per u64 = 2 u64s
    signatures: HashMap<u64, [u64; 2]>,
    /// Raw text for each doc (cached for O(1) verification without JSON parsing)
    texts: HashMap<u64, String>,
    /// Total documents indexed
    doc_count: usize,
    /// Field name being indexed
    field: String,
}

impl GiSTIndex {
    /// Build a new GiST index by iterating over documents.
    ///
    /// # Arguments
    /// * `docs` - Iterator of (doc_id, text) pairs
    /// * `field` - Field name being indexed (for statistics)
    ///
    /// # Returns
    /// * `Self` - The built index
    pub fn build<'a>(docs: impl Iterator<Item = (u64, &'a str)>, field: &str) -> Self {
        let mut signatures = HashMap::new();
        let mut texts = HashMap::new();
        let mut doc_count = 0;

        for (doc_id, text) in docs {
            let trigrams = extract_trigrams(text);
            if !trigrams.is_empty() {
                let sig = Self::build_signature(&trigrams);
                signatures.insert(doc_id, sig);
                texts.insert(doc_id, text.to_lowercase());
                doc_count += 1;
            }
        }

        Self {
            signatures,
            texts,
            doc_count,
            field: field.to_string(),
        }
    }

    /// Build a signature bitmap for a document's trigrams.
    fn build_signature(trigrams: &[String]) -> [u64; 2] {
        let mut sig = [0u64, 0u64];
        for trigram in trigrams {
            let h = hash_trigram(trigram);
            let bit_pos = h as usize % GIST_SIG_BITS;
            if bit_pos < 64 {
                sig[0] |= 1u64 << bit_pos;
            } else {
                sig[1] |= 1u64 << (bit_pos - 64);
            }
        }
        sig
    }

    /// Build a query signature from pattern trigrams.
    fn build_query_signature(trigrams: &[String]) -> [u64; 2] {
        let mut sig = [0u64, 0u64];
        for trigram in trigrams {
            let h = hash_trigram(trigram);
            let bit_pos = h as usize % GIST_SIG_BITS;
            if bit_pos < 64 {
                sig[0] |= 1u64 << bit_pos;
            } else {
                sig[1] |= 1u64 << (bit_pos - 64);
            }
        }
        sig
    }

    /// Check if a signature matches (all query bits are set).
    fn signature_matches(doc_sig: &[u64; 2], query_sig: &[u64; 2]) -> bool {
        (doc_sig[0] & query_sig[0]) == query_sig[0] && (doc_sig[1] & query_sig[1]) == query_sig[1]
    }

    /// Query the index for documents matching an ILIKE pattern.
    ///
    /// Returns candidate doc IDs that may match (verification needed).
    ///
    /// # Arguments
    /// * `pattern` - ILIKE pattern (e.g., "%Alpha%")
    /// * `limit` - Maximum candidates to return (None for all)
    ///
    /// # Returns
    /// * `Vec<u64>` - Candidate doc IDs
    pub fn ilike_candidates(&self, pattern: &str, limit: Option<usize>) -> Vec<u64> {
        // Extract trigrams from pattern
        let pattern_trigrams = extract_pattern_trigrams(pattern);
        if pattern_trigrams.is_empty() {
            // Degenerate pattern (all wildcards) — return all docs
            return self
                .signatures
                .keys()
                .copied()
                .take(limit.unwrap_or(usize::MAX))
                .collect();
        }

        // Deduplicate trigrams
        let trigrams = dedup_trigrams(&pattern_trigrams);
        let query_sig = Self::build_query_signature(&trigrams);

        // Find candidates whose signatures contain all query bits
        // Limit here is a hint — we return more candidates than limit because
        // GiST is lossy and verification will filter out false positives.
        let mut candidates = Vec::new();
        let effective_limit = limit.unwrap_or(usize::MAX).min(10_000); // Cap at 10K
        for (doc_id, doc_sig) in &self.signatures {
            if Self::signature_matches(doc_sig, &query_sig) {
                candidates.push(*doc_id);
                if candidates.len() >= effective_limit {
                    break;
                }
            }
        }

        candidates
    }

    /// Verify candidates against an ILIKE pattern using cached text.
    ///
    /// Returns only the candidates that actually match the pattern.
    /// Uses cached lowercase text and memchr for fast verification.
    ///
    /// # Arguments
    /// * `candidates` - Doc IDs to verify
    /// * `pattern` - ILIKE pattern (e.g., "%Alpha%")
    /// * `limit` - Max results (None for all)
    pub fn verify(&self, candidates: &[u64], pattern: &str, limit: Option<usize>) -> Vec<u64> {
        use memchr::memmem;
        let needle = pattern.trim_matches('%').to_lowercase();
        if needle.is_empty() {
            return candidates
                .iter()
                .copied()
                .take(limit.unwrap_or(usize::MAX))
                .collect();
        }
        let finder = memmem::Finder::new(needle.as_bytes());
        let mut results = Vec::new();
        for &doc_id in candidates {
            if let Some(text) = self.texts.get(&doc_id) {
                if finder.find(text.as_bytes()).is_some() {
                    results.push(doc_id);
                    if let Some(l) = limit {
                        if results.len() >= l {
                            break;
                        }
                    }
                }
            }
        }
        results
    }

    /// Get index statistics.
    pub fn stats(&self) -> GiSTStats {
        GiSTStats {
            doc_count: self.doc_count,
            field: self.field.clone(),
            indexed_count: self.signatures.len(),
            sig_bits: GIST_SIG_BITS,
            est_size_bytes: self.signatures.len() * std::mem::size_of::<[u64; 2]>(),
        }
    }
}

/// Statistics about a GiST index.
pub struct GiSTStats {
    pub doc_count: usize,
    pub field: String,
    pub indexed_count: usize,
    pub sig_bits: usize,
    pub est_size_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signature_bits_set() {
        let trigrams = vec![
            " al".to_string(),
            " alp".to_string(),
            "alp".to_string(),
            "lph".to_string(),
            "pha".to_string(),
            "ha ".to_string(),
        ];
        let sig = GiSTIndex::build_signature(&trigrams);

        // Signature should have some bits set
        assert!(sig[0] != 0 || sig[1] != 0);
    }

    #[test]
    fn test_signature_matching() {
        let doc_sig = [0b1010u64, 0b0101u64];
        let query_sig_all = [0b0010u64, 0b0100u64]; // Subset of doc
        let query_sig_partial = [0b1100u64, 0u64]; // Not subset

        assert!(GiSTIndex::signature_matches(&doc_sig, &query_sig_all));
        assert!(!GiSTIndex::signature_matches(&doc_sig, &query_sig_partial));
    }

    #[test]
    fn test_gist_build_and_query() {
        let docs = vec![
            (1u64, "Hello World"),
            (2u64, "The Vines"),
            (3u64, "Alpha Beta"),
        ];
        let index = GiSTIndex::build(docs.into_iter(), "text");

        // Should get some candidates (may include false positives)
        let candidates = index.ilike_candidates("%Alpha%", None);
        assert!(!candidates.is_empty());
    }
}
