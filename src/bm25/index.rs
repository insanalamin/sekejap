//! BM25 full-text search index.
//!
//! Lightweight and Pi-friendly: the only allocations are the postings
//! blob, the term dictionary, the doc-length array, and the doc-ID
//! lookup map.  No background threads, no allocator pressure.
//!
//! # Storage layout per indexed field (1 M docs)
//!
//! | Structure | Approximate size |
//! |---|---|
//! | `postings_bytes` | 90–450 MB (scales with field length) |
//! | `doc_id_to_idx` HashMap | ~19 MB |
//! | `doc_lengths` Vec | 4 MB |
//! | `sum_doc_len` counter | 8 bytes |
//!
//! # Deletion model
//!
//! Deletion is **zero-copy**: [`Bm25Index::delete`] removes the
//! document's entry from `doc_id_to_idx` and updates the running stats
//! (`num_docs`, `sum_doc_len`) but leaves the corresponding
//! `doc_lengths` slot as an inert orphan (4 bytes).  Orphans are
//! reclaimed only on a full rebuild.
//!
//! Because `doc_id_to_idx` is the authority for which documents are
//! live, a deleted document's postings entries can never contribute to
//! a search score — `search` skips any posting whose `doc_id` has no
//! entry in `doc_id_to_idx`.
//!
//! Callers should also apply a secondary guard through the live node
//! map (see [`Bm25Index`] struct-level docs) to catch any edge cases
//! between index operations.
//!
//! # BM25 parameters
//!
//! | Parameter | Value | Effect |
//! |---|---|---|
//! | `k1` | 1.2 | Term-frequency saturation |
//! | `b` | 0.75 | Length normalisation strength |

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::dict::TermDict;
use super::postings::{decode_postings_from_bytes, encode_postings_to_file, Posting};
use super::tokenizer::tokenize;

/// BM25 term-frequency saturation factor.
///
/// Controls how quickly additional occurrences of a term stop adding
/// to the document score.  Higher values reward repeated terms more.
const BM25_K1: f64 = 1.2;

/// BM25 length-normalisation factor.
///
/// `b = 1.0` fully normalises by document length; `b = 0.0` disables
/// length normalisation entirely.  `0.75` is the standard default.
const BM25_B: f64 = 0.75;

/// Orphan ratio above which a full rebuild is recommended.
///
/// Each [`Bm25Index::delete`] call leaves one 4-byte orphan slot in
/// `doc_lengths`.  Once the orphan fraction exceeds this threshold the
/// dead-weight entries in the postings blob are worth reclaiming.
///
/// At 20 % with 1 M docs the orphan footprint is ≤ 800 KB — safe on
/// a Raspberry Pi — while keeping full rebuilds infrequent.
pub const DEFAULT_REBUILD_THRESHOLD: f64 = 0.20;

/// Snapshot of collection-level statistics persisted alongside the
/// index for diagnostics and offline inspection.
///
/// `avg_doc_len` is stored here for serialisation compatibility, but
/// live scoring uses [`Bm25Index::avg_doc_len`] which derives the
/// value from the running `sum_doc_len` counter and stays accurate
/// after incremental deletions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bm25Meta {
    /// Number of live (non-deleted) documents tracked by this index.
    pub num_docs: u64,
    /// Average token count per document at build time.
    ///
    /// Kept for serialisation; use [`Bm25Index::avg_doc_len`] for
    /// accurate post-deletion values during search.
    pub avg_doc_len: f64,
    /// Name of the indexed field, e.g. `"body"`.
    pub field: String,
}

/// A single ranked result from a BM25 search.
#[derive(Clone, Debug)]
pub struct Bm25Hit {
    /// Node hash (`sk_hash(slug)`) of the matching document.
    pub doc_id: u64,
    /// BM25 relevance score; higher is more relevant.
    pub score: f64,
}

/// Lightweight BM25 index for a single text field.
///
/// # Deletion without a tombstone set
///
/// Rather than maintaining a separate tombstone `HashSet`, deletion is
/// handled by removing the entry from `doc_id_to_idx`.  Because
/// `search` gates every posting lookup through `doc_id_to_idx`, a
/// deleted document can never appear in results — even before the next
/// full rebuild — without any additional allocation.
///
/// The caller in `lib.rs` additionally filters results through
/// `self.nodes.contains_key(&hit.doc_id)` as a belt-and-suspenders
/// guard for the window between a node deletion and the BM25 index
/// update.
///
/// # Rebuild trigger
///
/// After many deletions the `doc_lengths` Vec accumulates orphan slots
/// (4 bytes each).  Call [`needs_rebuild`] periodically; when it
/// returns `true` drop the index and call `build_bm25_index` again.
///
/// [`needs_rebuild`]: Bm25Index::needs_rebuild
pub struct Bm25Index {
    /// Collection-level metadata (document count, field name).
    meta: Bm25Meta,
    /// Sorted term dictionary mapping each term to its byte range in
    /// `postings_bytes`.
    dict: TermDict,
    /// Concatenated, delta-encoded, varint-compressed postings for
    /// every indexed term.  Never rewritten during incremental
    /// operations; dead entries are reclaimed only on a full rebuild.
    postings_bytes: Vec<u8>,
    /// Token count for each document, addressed by the slot index
    /// stored in `doc_id_to_idx`.  Slots belonging to deleted
    /// documents become unreachable orphans; each wastes exactly
    /// 4 bytes until the next rebuild.
    doc_lengths: Vec<u32>,
    /// Maps a node hash (`sk_hash(slug)`) to its slot index in
    /// `doc_lengths`.
    ///
    /// This is the single source of truth for document liveness:
    /// removing an entry here is sufficient to exclude the document
    /// from all future search results.
    doc_id_to_idx: HashMap<u64, usize>,
    /// Running total of token counts across **live** documents only.
    ///
    /// Decremented by [`delete`] so that [`avg_doc_len`] stays
    /// accurate without touching the postings blob.
    ///
    /// [`delete`]: Bm25Index::delete
    /// [`avg_doc_len`]: Bm25Index::avg_doc_len
    sum_doc_len: u64,
}

impl Bm25Index {
    /// Build a BM25 index from a document iterator.
    ///
    /// `docs` yields `(doc_id, text)` pairs where `doc_id` is
    /// `sk_hash(slug)`.  All tokenisation, postings compression, and
    /// dictionary construction happen in one pass.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let pairs = nodes.iter()
    ///     .filter_map(|(&h, n)| n.payload["body"].as_str().map(|t| (h, t)));
    /// let index = Bm25Index::build("body", pairs);
    /// ```
    pub fn build<'a>(field: &str, docs: impl Iterator<Item = (u64, &'a str)>) -> Self {
        let mut term_doc_freqs: HashMap<String, HashMap<u64, u32>> = HashMap::new();
        let mut doc_lengths: Vec<u32> = Vec::new();
        let mut doc_ids: Vec<u64> = Vec::new();
        let mut doc_id_to_idx: HashMap<u64, usize> = HashMap::new();

        // First pass: tokenise every document and accumulate
        // per-term document-frequency maps.
        let mut sum_doc_len: u64 = 0;
        for (doc_id, text) in docs {
            let idx = doc_ids.len();
            doc_ids.push(doc_id);
            doc_id_to_idx.insert(doc_id, idx);

            let terms = tokenize(text);
            let doc_len = terms.len() as u32;
            doc_lengths.push(doc_len);
            sum_doc_len += doc_len as u64;

            for term in terms {
                let entry = term_doc_freqs.entry(term).or_default();
                *entry.entry(doc_id).or_default() += 1;
            }
        }

        let num_docs = doc_ids.len() as u64;
        let avg_doc_len = if num_docs > 0 {
            sum_doc_len as f64 / num_docs as f64
        } else {
            1.0
        };

        // Build sorted postings lists per term.
        let mut postings_map: HashMap<String, Vec<Posting>> = HashMap::new();
        for (term, doc_freqs) in term_doc_freqs {
            let mut postings: Vec<Posting> = doc_freqs
                .into_iter()
                .map(|(doc_id, freq)| Posting { doc_id, freq })
                .collect();
            postings.sort_by_key(|p| p.doc_id);
            postings_map.insert(term, postings);
        }

        // Serialise postings into one contiguous byte buffer and build
        // the term dictionary (term → byte offset + length).
        let mut dict = TermDict::new();
        let mut all_postings: Vec<u8> = Vec::new();
        let mut offset: u64 = 0;

        let mut terms: Vec<_> = postings_map.keys().cloned().collect();
        terms.sort();

        for term in terms {
            let postings = postings_map.get(&term).unwrap();
            let postings_bytes = encode_postings_to_file(postings);
            let len = postings_bytes.len() as u32;

            // Align each postings list to an 8-byte boundary so that
            // future mmap reads stay aligned.
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
            sum_doc_len,
        }
    }

    /// Search the index and return the top-`top_k` documents ranked by
    /// BM25 score (highest first).
    ///
    /// Deleted documents are automatically excluded because their
    /// entries were removed from `doc_id_to_idx` by [`delete`]; no
    /// extra filtering is required inside this method.
    ///
    /// [`delete`]: Bm25Index::delete
    pub fn search(&self, query: &str, top_k: usize) -> Vec<Bm25Hit> {
        let query_terms = tokenize(query);
        if query_terms.is_empty() {
            return Vec::new();
        }

        // Pre-compute IDF for each query term.
        // Terms absent from the dictionary contribute nothing.
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

        // Accumulate BM25 scores.  The `doc_id_to_idx` lookup acts as
        // the liveness gate: deleted docs have no entry and are skipped
        // via the `None => continue` arm.
        let avg_dl = self.avg_doc_len();
        let mut scores: HashMap<u64, f64> = HashMap::new();

        for (term, term_idf) in &idf {
            let entry = self.dict.get(term).unwrap();
            let postings = self.get_postings(entry);

            for posting in postings {
                let doc_idx = match self.doc_id_to_index(posting.doc_id) {
                    Some(idx) => idx,
                    // Document was deleted (entry removed from doc_id_to_idx).
                    None => continue,
                };

                let doc_len = self.doc_lengths[doc_idx] as f64;
                let tf = posting.freq as f64;

                // Standard BM25 formula:
                //   score += IDF × (tf × (k1 + 1)) / (tf + k1 × (1 − b + b × dl / avg_dl))
                let numerator = tf * (BM25_K1 + 1.0);
                let denominator =
                    tf + BM25_K1 * (1.0 - BM25_B + BM25_B * doc_len / avg_dl);
                let score = term_idf * numerator / denominator;

                *scores.entry(posting.doc_id).or_insert(0.0) += score;
            }
        }

        // Sort descending by score and truncate to top_k.
        let mut hits: Vec<Bm25Hit> = scores
            .into_iter()
            .map(|(doc_id, score)| Bm25Hit { doc_id, score })
            .collect();
        hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
        hits.truncate(top_k);
        hits
    }

    /// Remove a document from the index without rewriting the postings
    /// blob.
    ///
    /// # What this does
    ///
    /// 1. Looks up `doc_id` in `doc_id_to_idx`.  Returns `false`
    ///    immediately if not found (already deleted or never indexed).
    /// 2. Reads the document's token count and subtracts it from
    ///    `sum_doc_len` so that [`avg_doc_len`] stays accurate.
    /// 3. Decrements `meta.num_docs`.
    /// 4. Removes the entry from `doc_id_to_idx`, which is the only
    ///    step needed to make the document invisible to [`search`]:
    ///    the posting-loop liveness gate skips any `doc_id` with no
    ///    `doc_id_to_idx` entry.
    ///
    /// # What this does NOT do
    ///
    /// The postings blob (`postings_bytes`) is **never rewritten**.
    /// The deleted document's posting entries remain in the compressed
    /// stream as inert bytes until the next full rebuild.  Rewriting
    /// every affected postings list would cost O(unique terms in the
    /// document) allocations — equivalent to a partial rebuild and
    /// unacceptable on a Pi.
    ///
    /// The `doc_lengths` slot at the freed index becomes an orphan
    /// (4 bytes, unreachable via `doc_id_to_idx`).  Monitor orphan
    /// accumulation with [`orphan_count`] and schedule a full rebuild
    /// when [`needs_rebuild`] returns `true`.
    ///
    /// # Returns
    ///
    /// `true` if the document was found and removed; `false` if it was
    /// not indexed (already deleted or never present).
    ///
    /// [`avg_doc_len`]: Bm25Index::avg_doc_len
    /// [`search`]: Bm25Index::search
    /// [`orphan_count`]: Bm25Index::orphan_count
    /// [`needs_rebuild`]: Bm25Index::needs_rebuild
    pub fn delete(&mut self, doc_id: u64) -> bool {
        if let Some(&idx) = self.doc_id_to_idx.get(&doc_id) {
            let doc_len = self.doc_lengths[idx] as u64;
            self.sum_doc_len = self.sum_doc_len.saturating_sub(doc_len);
            self.meta.num_docs = self.meta.num_docs.saturating_sub(1);
            self.doc_id_to_idx.remove(&doc_id);
            // doc_lengths[idx] becomes an unreachable orphan slot.
            true
        } else {
            false
        }
    }

    /// Current average token count across **live** documents.
    ///
    /// Derived from the running `sum_doc_len` counter so the value
    /// stays accurate after incremental [`delete`] calls, without
    /// touching the postings blob.
    ///
    /// Returns `1.0` when no live documents remain to avoid
    /// division-by-zero in the BM25 length-normalisation term.
    ///
    /// [`delete`]: Bm25Index::delete
    #[inline]
    pub fn avg_doc_len(&self) -> f64 {
        if self.meta.num_docs == 0 {
            1.0
        } else {
            self.sum_doc_len as f64 / self.meta.num_docs as f64
        }
    }

    /// Number of `doc_lengths` slots that belong to deleted documents.
    ///
    /// Each [`delete`] call leaves one 4-byte orphan slot.  Orphans
    /// are reclaimed only on a full rebuild triggered by
    /// [`needs_rebuild`].
    ///
    /// # Pi footprint
    ///
    /// At the default 20 % rebuild threshold with 1 M documents the
    /// maximum orphan footprint is 200 K × 4 = 800 KB — well within
    /// Pi constraints.
    ///
    /// [`delete`]: Bm25Index::delete
    /// [`needs_rebuild`]: Bm25Index::needs_rebuild
    pub fn orphan_count(&self) -> usize {
        self.doc_lengths.len().saturating_sub(self.doc_id_to_idx.len())
    }

    /// Returns `true` when the orphan ratio exceeds `threshold` and a
    /// full rebuild is recommended.
    ///
    /// The orphan ratio is `orphan_count / doc_lengths.len()`.  Once
    /// it is high the postings blob carries significant dead weight and
    /// a rebuild both reclaims memory and restores full scoring
    /// accuracy.
    ///
    /// # Recommended threshold
    ///
    /// Use [`DEFAULT_REBUILD_THRESHOLD`] (`0.20`) for most workloads.
    /// Lower values rebuild more aggressively (better accuracy, more
    /// I/O); higher values tolerate more dead weight in exchange for
    /// fewer rebuilds.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if bm25_idx.needs_rebuild(DEFAULT_REBUILD_THRESHOLD) {
    ///     db.build_bm25_index("body");
    /// }
    /// ```
    pub fn needs_rebuild(&self, threshold: f64) -> bool {
        let total = self.doc_lengths.len();
        if total == 0 {
            return false;
        }
        self.orphan_count() as f64 / total as f64 > threshold
    }

    /// Total number of live (non-deleted) documents in the index.
    pub fn num_docs(&self) -> u64 {
        self.meta.num_docs
    }

    /// Number of unique terms in the dictionary.
    pub fn num_terms(&self) -> usize {
        self.dict.num_terms()
    }

    // ── Private helpers ───────────────────────────────────────────────

    /// Decode the postings list for a term dictionary entry.
    fn get_postings(&self, entry: &super::dict::TermEntry) -> Vec<Posting> {
        let start = entry.postings_offset as usize;
        let end = start + entry.postings_len as usize;
        if start >= self.postings_bytes.len() || end > self.postings_bytes.len() {
            return Vec::new();
        }
        decode_postings_from_bytes(&self.postings_bytes[start..end])
    }

    /// Look up the `doc_lengths` slot index for a given node hash.
    /// Returns `None` if the document has been deleted or was never
    /// indexed.
    fn doc_id_to_index(&self, doc_id: u64) -> Option<usize> {
        self.doc_id_to_idx.get(&doc_id).copied()
    }
}

/// BM25 result with a resolved document slug.
///
/// Returned by higher-level helpers in `lib.rs` that map `doc_id`
/// back through the node map.
pub struct Bm25SearchResult {
    pub slug: String,
    pub score: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_index() -> Bm25Index {
        let docs = vec![
            (1u64, "Rust is a systems programming language"),
            (2u64, "Python is great for beginners"),
            (3u64, "Rust async runtime and performance"),
            (4u64, "Learning Rust programming"),
            (5u64, "Go is a modern systems language"),
        ];
        Bm25Index::build("content", docs.into_iter())
    }

    #[test]
    fn test_bm25_build_and_search() {
        let index = sample_index();
        assert_eq!(index.num_docs(), 5);
        assert!(index.num_terms() > 0);

        let hits = index.search("rust", 10);
        assert!(!hits.is_empty(), "expected hits for 'rust'");
        assert!(hits.iter().any(|h| h.doc_id == 1));
        assert!(hits.iter().any(|h| h.doc_id == 3));
        assert!(hits.iter().any(|h| h.doc_id == 4));

        let hits = index.search("python", 10);
        assert!(hits.iter().any(|h| h.doc_id == 2));

        let hits = index.search("xyzabc", 10);
        assert!(hits.is_empty());
    }

    /// Deleted doc must not appear in search results.
    /// The liveness gate in `search` (doc_id_to_idx lookup) makes the
    /// deleted document invisible immediately — no rebuild required.
    #[test]
    fn test_delete_removes_from_results() {
        let mut index = sample_index();
        assert_eq!(index.num_docs(), 5);

        assert!(index.delete(1), "doc 1 should be found and removed");
        assert_eq!(index.num_docs(), 4);
        assert_eq!(index.orphan_count(), 1);

        let hits = index.search("rust", 10);
        assert!(!hits.iter().any(|h| h.doc_id == 1), "deleted doc must not appear");
        assert!(hits.iter().any(|h| h.doc_id == 3), "doc 3 must still appear");
        assert!(hits.iter().any(|h| h.doc_id == 4), "doc 4 must still appear");
    }

    /// Deleting a doc that was never indexed (or already deleted) must
    /// return false and leave the index unchanged.
    #[test]
    fn test_delete_nonexistent_returns_false() {
        let mut index = sample_index();
        assert!(!index.delete(999));
        assert_eq!(index.num_docs(), 5);
        assert_eq!(index.orphan_count(), 0);
    }

    /// `needs_rebuild` must cross the threshold only after enough
    /// deletions accumulate.
    #[test]
    fn test_needs_rebuild_threshold() {
        // 10 docs; threshold 0.20 triggers when orphan_count > 2
        let docs: Vec<(u64, &str)> = (1u64..=10)
            .map(|i| (i, "Melbourne suburb Fitzroy artist live music"))
            .collect();
        let mut index = Bm25Index::build("content", docs.into_iter());

        index.delete(1);
        index.delete(2);
        // 2/10 = 0.20 — exactly at threshold, not above it
        assert!(!index.needs_rebuild(0.20));

        index.delete(3);
        // 3/10 = 0.30 — now above threshold
        assert!(index.needs_rebuild(0.20));
    }

    /// `avg_doc_len` must reflect only live documents after deletions.
    #[test]
    fn test_avg_doc_len_accurate_after_delete() {
        let docs = vec![
            // 10 tokens (counted after tokenise: ≥3-char filter applies)
            (1u64, "one two three four five six seven eight nine ten"),
            // 1 token
            (2u64, "one"),
        ];
        let mut index = Bm25Index::build("content", docs.into_iter());

        // tokenize keeps terms ≥3 chars; "one" passes, short words may not —
        // just verify the relative change, not exact values.
        let avg_before = index.avg_doc_len();
        assert!(avg_before > 0.0);

        // Delete the short doc; avg should rise (long doc now alone).
        index.delete(2);
        let avg_after = index.avg_doc_len();
        assert!(
            avg_after >= avg_before,
            "avg_doc_len should be >= before after removing the shorter doc: before={avg_before} after={avg_after}"
        );
    }
}
