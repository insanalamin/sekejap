//! # Text Index — Trigram-based ILIKE Acceleration
//!
//! ## Problem
//!
//! `ILIKE '%foo%'` on 10K+ documents requires O(n) payload scan.
//! Current sekejap ILIKE: ~1.5ms for 10K docs (full scan).
//! Target with trigram index: ~50µs (30x faster).
//!
//! ## Solution: Trigram Inverted Index
//!
//! Fulltext engines (Tantivy/SeekStorm) use inverted indexes.
//! We distill the core principle for ILIKE: substring match via trigrams.
//!
//! Inspired by PostgreSQL's `pg_trgm` extension which shows trigram
//! indexes work for ILIKE on short strings (varchar < 255).
//!
//! ## Architecture
//!
//! ### 1. Trigram Extraction (like pg_trgm)
//!
//! ```text
//! "Alpha" → extract trigrams with space padding: " al", " alp", "alp", "lph", "pha", "ha "
//! "The Vines" → " th", "the", "he ", "e v", " vi", "vin", "ine", "nes", "es "
//! ```
//!
//! - Space padding before and after string (pg_trgm convention)
//! - Lowercase for case-insensitive ILIKE
//! - Sliding window of 3 characters
//!
//! ### 2. Index Types
//!
//! **GiST (default, Pi-friendly):**
//! - Bitmap signature per document (~12 bytes/doc)
//! - ~12MB/1M docs total — tiny footprint
//! - Fast lookup, may need verification step (like PostgreSQL GiST)
//!
//! **GIN (explicit, precise):**
//! - Exact trigram → doc ID postings
//! - ~100MB/1M docs
//! - No verification needed (exact match)
//!
//! ### 3. Inverted Index Structure
//!
//! ```text
//! HashMap<trigram_hash(u32), RoaringBitmap<doc_ids>>
//! ```
//!
//! - Trigram hash → posting list (doc IDs containing that trigram)
//! - RoaringBitmap for memory-efficient sets & fast intersection
//!
//! ### 4. Query Flow for ILIKE '%Alpha%'
//!
//! ```text
//! 1. Parse pattern → extract trigrams: [" al", " alp", "alp", "lph", "pha", "ha "]
//! 2. Look up each trigram → get RoaringBitmaps
//! 3. Intersect bitmaps → candidate doc IDs (AND semantics)
//! 4. Verify each candidate with full ILIKE check
//! 5. Apply LIMIT → early termination
//! ```
//!
//! ### 5. Safety Properties (core principle)
//!
//! - Index is DERIVED from main HashMap storage
//! - Index can be deleted and rebuilt anytime
//! - Main engine works even if index is corrupted
//! - No WAL dependence — index rebuilt from HashMap on load
//! - GiST: verification step ensures correctness despite lossy signature
//!
//! ## File Layout
//!
//! ```text
//! text_index/
//!   mod.rs      ← you are here (architecture)
//!   trigram.rs  ← extract_trigrams() with space padding like pg_trgm
//!   gist.rs    ← GiST bitmap signature index (~12MB/1M)
//!   gin.rs     ← GIN exact postings index (~100MB/1M)
//!   query.rs   ← execute_ilike(), verify_pattern()
//! ```
//!
//! ## Comparison to Fulltext Engines
//!
//! | Feature | Tantivy | pg_trgm | Our Trigram Engine |
//! |---------|---------|---------|-------------------|
//! | Tokenization | NLTK/stemmer | Sliding window (all substrings) | Sliding window (all substrings) |
//! | Query type | Full-text search | ILIKE %substr% | ILIKE %substr% only |
//! | Index size | 20MB+ | ~12MB/1M (GiST) | ~12MB/1M (GiST) or ~100MB/1M (GIN) |
//! | Recovery | Reopen mmap | Reindex | Rebuild from HashMap |
//! | External deps | tantivy crate | PostgreSQL built-in | roaring crate only |
//! | Memory-mapped | ✅ | ✅ (OS managed) | ✅ (our plan) |
//!
//! ## Implementation Phases
//!
//! ### Phase 1: Core trigram extraction
//! - `extract_trigrams(text: &str) -> Vec<String>` with space padding
//! - `hash_trigram(t: &str) -> u32` using FNV-1a
//! - Verify correctness before building index
//!
//! ### Phase 2: GiST bitmap signature index
//! - `GiSTIndex` struct with bitmap signatures per doc
//! - Build by scanning all HashMap nodes
//! - Query by signature lookup + verification
//! - Memory target: ~12MB/1M docs
//!
//! ### Phase 3: GIN exact postings index
//! - `GINIndex` struct with RoaringBitmap postings
//! - Fast intersection via `&` operator
//! - Memory: ~100MB/1M docs, but exact (no verify needed)
//!
//! ### Phase 4: Memory-mapped storage (optional)
//! - Persist index to disk with `Mmap`
//! - Load on startup, validate against HashMap
//! - Rebuild if checksum mismatch
//!
//! ## Performance Target
//!
//! | Scenario | Current | Target |
//! |----------|---------|--------|
//! | ILIKE on 10K docs | ~1.5ms (full scan) | ~50µs |
//! | ILIKE on 1M docs | ~150s (full scan) | ~5-20ms |
//! | Memory for 1M docs | 0MB index | ~12MB (GiST) |
//!
//! ## Usage Example
//!
//! ```rust,ignore
//! use sekejap::text_index::{GiSTIndex, GINIndex};
//!
//! // Build GiST index (default, Pi-friendly)
//! let docs = vec![(1u64, "Hello World"), (2u64, "The Vines")];
//! let gist = GiSTIndex::build(docs.into_iter(), "text");
//! let candidates = gist.ilike_candidates("%Vines%", None);
//!
//! // Build GIN index (exact, more memory)
//! let docs = vec![(1u64, "Hello World"), (2u64, "The Vines")];
//! let gin = GINIndex::build(docs.into_iter(), "text");
//! let matches = gin.ilike("%Vines%", Some(10));
//! ```
//!
//! ## References
//!
//! - [pg_trgm PostgreSQL docs](https://www.postgresql.org/docs/current/pgtrgm.html)
//! - Tantivy architecture (inverted index, posting blocks)
//! - RoaringBitmap crate (pure Rust, no C deps)

pub mod gin;
pub mod gist;
pub mod query;
pub mod trigram;
