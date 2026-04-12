//! BM25 full-text search index.
//!
//! Lightweight, disk-first, compressed.
//!
//! # Storage Format
//!
//! Each indexed field gets a separate index directory:
//! ```text
//! data_dir/bm25_{field}/
//!   ├── meta.json        # Collection stats
//!   ├── dict.bin        # Term dictionary
//!   ├── postings.bin    # Compressed postings
//!   └── doclen.bin     # Document lengths
//! ```
//!
//! # BM25 Parameters
//!
//! Standard parameters:
//! - k1 = 1.2 (term frequency saturation)
//! - b = 0.75 (length normalization)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::dict::TermDict;
use super::postings::{decode_postings_from_bytes, encode_postings_to_file, Posting};
use super::tokenizer::tokenize;

/// BM25 parameters (standard values).
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

/// Collection metadata for BM25 scoring.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bm25Meta {
    pub num_docs: u64,
    pub avg_doc_len: f64,
    pub field: String,
}

/// A scored document result from BM25 search.
#[derive(Clone, Debug)]
pub struct Bm25Hit {
    pub doc_id: u64,
    pub score: f64,
}

/// Lightweight BM25 index for a single field.
pub struct Bm25Index {
    meta: Bm25Meta,
    dict: TermDict,
    postings_bytes: Vec<u8>,
    doc_lengths: Vec<u32>,
    doc_id_to_idx: HashMap<u64, usize>,
}

impl Bm25Index {
    /// Build a BM25 index from documents.
    /// `docs` is an iterator of (doc_id, text).
    pub fn build<'a>(field: &str, docs: impl Iterator<Item = (u64, &'a str)>) -> Self {
        let mut term_doc_freqs: HashMap<String, HashMap<u64, u32>> = HashMap::new();
        let mut doc_lengths: Vec<u32> = Vec::new();
        let mut doc_ids: Vec<u64> = Vec::new();
        let mut doc_id_to_idx: HashMap<u64, usize> = HashMap::new();

        // First pass: tokenize and build inverted index
        let mut num_tokens_total: u64 = 0;
        for (doc_id, text) in docs {
            let idx = doc_ids.len();
            doc_ids.push(doc_id);
            doc_id_to_idx.insert(doc_id, idx);

            let terms = tokenize(text);
            let doc_len = terms.len() as u32;
            doc_lengths.push(doc_len);
            num_tokens_total += doc_len as u64;

            for term in terms {
                let entry = term_doc_freqs.entry(term).or_default();
                *entry.entry(doc_id).or_default() += 1;
            }
        }

        let num_docs = doc_ids.len() as u64;
        let avg_doc_len = if num_docs > 0 {
            num_tokens_total as f64 / num_docs as f64
        } else {
            1.0
        };

        // Build postings lists
        let mut postings_map: HashMap<String, Vec<Posting>> = HashMap::new();
        for (term, doc_freqs) in term_doc_freqs {
            let mut postings: Vec<Posting> = doc_freqs
                .into_iter()
                .map(|(doc_id, freq)| Posting { doc_id, freq })
                .collect();
            postings.sort_by_key(|p| p.doc_id);
            postings_map.insert(term, postings);
        }

        // Build dictionary and serialize postings
        let mut dict = TermDict::new();
        let mut all_postings: Vec<u8> = Vec::new();
        let mut offset: u64 = 0;

        let mut terms: Vec<_> = postings_map.keys().cloned().collect();
        terms.sort();

        for term in terms {
            let postings = postings_map.get(&term).unwrap();
            let postings_bytes = encode_postings_to_file(postings);
            let len = postings_bytes.len() as u32;

            // Pad to 8-byte alignment
            while offset % 8 != 0 {
                all_postings.push(0);
                offset += 1;
            }

            let data_offset = offset;
            dict.insert(term.clone(), data_offset, len);

            all_postings.extend_from_slice(&postings_bytes);
            offset += postings_bytes.len() as u64;
        }

        dict.build_index();

        let meta = Bm25Meta {
            num_docs,
            avg_doc_len,
            field: field.to_string(),
        };

        Self {
            meta,
            dict,
            postings_bytes: all_postings,
            doc_lengths,
            doc_id_to_idx,
        }
    }

    /// Search the index and return top-K documents by BM25 score.
    pub fn search(&self, query: &str, top_k: usize) -> Vec<Bm25Hit> {
        let query_terms = tokenize(query);
        if query_terms.is_empty() {
            return Vec::new();
        }

        // Calculate IDF for each term
        let idf: HashMap<&str, f64> = query_terms
            .iter()
            .filter_map(|t| {
                if let Some(entry) = self.dict.get(t) {
                    let postings = self.get_postings(entry);
                    let df = postings.len() as f64;
                    let idf = ((self.meta.num_docs as f64 - df + 0.5) / (df + 0.5)).ln();
                    Some((t.as_str(), idf.max(0.0)))
                } else {
                    None
                }
            })
            .collect();

        if idf.is_empty() {
            return Vec::new();
        }

        // Calculate BM25 score for each document
        let mut scores: HashMap<u64, f64> = HashMap::new();

        for (term, term_idf) in &idf {
            let entry = self.dict.get(term).unwrap();
            let postings = self.get_postings(entry);

            for posting in postings {
                let doc_idx = match self.doc_id_to_index(posting.doc_id) {
                    Some(idx) => idx,
                    None => continue,
                };

                let doc_len = self.doc_lengths[doc_idx] as f64;
                let tf = posting.freq as f64;

                // BM25 formula
                let numerator = tf * (BM25_K1 + 1.0);
                let denominator =
                    tf + BM25_K1 * (1.0 - BM25_B + BM25_B * doc_len / self.meta.avg_doc_len);
                let score = term_idf * numerator / denominator;

                *scores.entry(posting.doc_id).or_insert(0.0) += score;
            }
        }

        // Sort by score descending
        let mut hits: Vec<Bm25Hit> = scores
            .into_iter()
            .map(|(doc_id, score)| Bm25Hit { doc_id, score })
            .collect();
        hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());

        hits.truncate(top_k);
        hits
    }

    fn get_postings(&self, entry: &super::dict::TermEntry) -> Vec<Posting> {
        let start = entry.postings_offset as usize;
        let end = start + entry.postings_len as usize;
        if start >= self.postings_bytes.len() || end > self.postings_bytes.len() {
            return Vec::new();
        }
        decode_postings_from_bytes(&self.postings_bytes[start..end])
    }

    fn doc_id_to_index(&self, doc_id: u64) -> Option<usize> {
        self.doc_id_to_idx.get(&doc_id).copied()
    }

    pub fn num_docs(&self) -> u64 {
        self.meta.num_docs
    }

    pub fn num_terms(&self) -> usize {
        self.dict.num_terms()
    }
}

/// BM25 result with document slug.
pub struct Bm25SearchResult {
    pub slug: String,
    pub score: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bm25_build_and_search() {
        let docs = vec![
            (1, "Rust is a systems programming language"),
            (2, "Python is great for beginners"),
            (3, "Rust vs Go performance comparison"),
            (4, "Learning Rust programming"),
            (5, "Go is a modern systems language"),
        ];

        let index = Bm25Index::build("content", docs.into_iter());

        // Check basic info
        assert_eq!(index.num_docs(), 5);
        assert!(index.num_terms() > 0);

        // Search for "rust" - should return docs containing "rust"
        let hits = index.search("rust", 10);
        assert!(!hits.is_empty(), "Expected some hits for 'rust' query");
        assert!(
            hits.iter().any(|h| h.doc_id == 1),
            "Expected doc 1 for 'rust' query"
        );
        assert!(
            hits.iter().any(|h| h.doc_id == 3),
            "Expected doc 3 for 'rust' query"
        );
        assert!(
            hits.iter().any(|h| h.doc_id == 4),
            "Expected doc 4 for 'rust' query, got: {:?}",
            hits
        );

        // Search for "python" - should match doc 2
        let hits = index.search("python", 10);
        assert!(hits.iter().any(|h| h.doc_id == 2));

        // Search for "nonexistent" - should return empty
        let hits = index.search("xyzabc", 10);
        assert!(hits.is_empty());
    }
}
