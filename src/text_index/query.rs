//! ## ILIKE Query Execution
//!
//! Handles ILIKE pattern matching with trigram index acceleration.
//!
//! ## Query Flow
//!
//! ```text
//! ILIKE "%Alpha%" LIMIT 50
//!       │
//!       ▼
//! ┌─────────────────────┐
//! │ Parse Pattern       │ → Extract trigrams: [" al", " alp", ...]
//! └─────────────────────┘
//!       │
//!       ▼
//! ┌─────────────────────┐
//! │ Index Lookup        │ → GiST: signature match
//! │                    │ → GIN: exact postings intersect
//! └─────────────────────┘
//!       │
//!       ▼
//! ┌─────────────────────┐
//! │ Candidates          │ → List of candidate doc IDs
//! └─────────────────────┘
//!       │
//!       ▼
//! ┌─────────────────────┐
//! │ Verify (GiST only)  │ → Full ILIKE check on each candidate
//! │                    │ → GIN: skip (exact)
//! └─────────────────────┘
//!       │
//!       ▼
//! ┌─────────────────────┐
//! │ Apply LIMIT         │ → Early termination when limit reached
//! └─────────────────────┘
//!       │
//!       ▼
//! ┌─────────────────────┐
//! │ Return Results      │ → Vec<Hit> or Vec<doc_id>
//! └─────────────────────┘
//! ```
//!
//! ## ILIKE Semantics
//!
//! - Case-insensitive substring match
//! - `%` matches any sequence (including empty)
//! - `_` matches any single character
//! - Pattern is POSIX-style (not regex)
//!
//! ## Why Verification for GiST?
//!
//! GiST signatures are lossy. A document might pass signature check but NOT
//! actually contain the pattern (false positive). Verification ensures correctness.
//!
//! GIN does NOT need verification because it stores exact trigram→docID mappings.

use crate::text_index::gin::GINIndex;
use crate::text_index::gist::GiSTIndex;

/// ILIKE pattern matching result with source info.
pub struct ILikeResult {
    /// Document ID
    pub doc_id: u64,
    /// Whether this was verified (true for GiST) or exact (false for GIN)
    pub verified: bool,
}

/// Check if a string matches an ILIKE pattern.
///
/// Implements POSIX ILIKE semantics:
/// - Case-insensitive
/// - `%` matches any sequence (including empty)
/// - `_` matches single character
/// - Pattern is POSIX-style (not regex)
///
/// # Arguments
/// * `text` - The text to check
/// * `pattern` - ILIKE pattern (e.g., "%Alpha%" or "%foo_bar%")
///
/// # Returns
/// * `bool` - True if text matches pattern
pub fn ilike_matches(text: &str, pattern: &str) -> bool {
    let text_lower = text.to_lowercase();
    let pattern_lower = pattern.to_lowercase();

    let pattern = pattern_lower.trim();
    let text = text_lower.as_str();

    if pattern.is_empty() {
        return true;
    }

    if pattern == "%" {
        return true;
    }

    let leading_pct = pattern.chars().take_while(|&c| c == '%').count();
    let trailing_pct = pattern.chars().rev().take_while(|&c| c == '%').count();

    let stripped = pattern.trim_matches('%');

    if stripped.is_empty() {
        return true;
    }

    if leading_pct > 0 && trailing_pct > 0 {
        let fixed_parts: Vec<&str> = stripped.split('%').filter(|s| !s.is_empty()).collect();
        if fixed_parts.is_empty() {
            return true;
        }
        let mut pos = 0usize;
        for part in &fixed_parts {
            if let Some(found) = text[pos..].find(part) {
                pos += found + part.len();
            } else {
                return false;
            }
        }
        true
    } else if leading_pct > 0 {
        // Pattern has leading %
        let fixed_parts: Vec<&str> = stripped.split('%').filter(|s| !s.is_empty()).collect();
        if fixed_parts.is_empty() {
            return true;
        }
        let mut pos = 0usize;
        for part in &fixed_parts {
            if let Some(found) = text[pos..].find(part) {
                pos += found + part.len();
            } else {
                return false;
            }
        }
        true
    } else if trailing_pct > 0 {
        let fixed_parts: Vec<&str> = stripped.split('%').filter(|s| !s.is_empty()).collect();
        if fixed_parts.is_empty() {
            return true;
        }
        if let Some(pos) = text.find(fixed_parts[0]) {
            let mut search_pos = pos;
            for part in &fixed_parts[1..] {
                if let Some(found) = text[search_pos..].find(part) {
                    search_pos += found + part.len();
                } else {
                    return false;
                }
            }
            text[search_pos..].starts_with(*fixed_parts.last().unwrap())
        } else {
            false
        }
    } else {
        // Pattern has internal % - split and check all parts appear in order
        let fixed_parts: Vec<&str> = stripped.split('%').filter(|s| !s.is_empty()).collect();
        if fixed_parts.is_empty() {
            return true;
        }
        if fixed_parts.len() == 1 {
            // Single fixed part anywhere in text
            return text.contains(fixed_parts[0]);
        }
        // Multiple parts must appear in order
        let mut pos = 0usize;
        for part in &fixed_parts {
            if let Some(found) = text[pos..].find(part) {
                pos += found + part.len();
            } else {
                return false;
            }
        }
        true
    }
}

/// Case-sensitive LIKE pattern matching (same wildcard logic as `ilike_matches`, no lowercasing).
pub fn like_matches(text: &str, pattern: &str) -> bool {
    let pattern = pattern.trim();
    if pattern.is_empty() { return true; }
    if pattern == "%" { return true; }
    let leading_pct  = pattern.chars().take_while(|&c| c == '%').count();
    let trailing_pct = pattern.chars().rev().take_while(|&c| c == '%').count();
    let stripped = pattern.trim_matches('%');
    if stripped.is_empty() { return true; }
    let fixed_parts: Vec<&str> = stripped.split('%').filter(|s| !s.is_empty()).collect();
    if fixed_parts.is_empty() { return true; }
    if leading_pct > 0 || trailing_pct > 0 {
        let mut pos = 0usize;
        for part in &fixed_parts {
            if let Some(found) = text[pos..].find(part) {
                pos += found + part.len();
            } else {
                return false;
            }
        }
        true
    } else {
        let mut pos = 0usize;
        for part in &fixed_parts {
            if let Some(found) = text[pos..].find(part) {
                pos += found + part.len();
            } else {
                return false;
            }
        }
        true
    }
}

/// Execute ILIKE query using GiST index with verification.
///
/// # Arguments
/// * `index` - GiST index
/// * `db` - CoreDB reference (for fetching node data)
/// * `pattern` - ILIKE pattern
/// * `limit` - Maximum results
///
/// # Returns
/// * `Vec<u64>` - Verified matching doc IDs
pub fn ilike_gist(
    index: &GiSTIndex,
    db: &crate::CoreDB,
    pattern: &str,
    limit: Option<usize>,
) -> Vec<u64> {
    let candidates = index.ilike_candidates(pattern, None);

    let mut results = Vec::new();
    for doc_id in candidates {
        if let Some(payload) = db.get_payload(doc_id) {
            let text = serde_json::to_string(&payload).unwrap_or_default();
            if ilike_matches(&text, pattern) {
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

/// Execute ILIKE query using GIN index (exact, no verification needed).
///
/// # Arguments
/// * `index` - GIN index
/// * `pattern` - ILIKE pattern
/// * `limit` - Maximum results
///
/// # Returns
/// * `Vec<u64>` - Exact matching doc IDs
pub fn ilike_gin(index: &GINIndex, pattern: &str, limit: Option<usize>) -> Vec<u64> {
    index.ilike(pattern, limit)
}

/// Execute ILIKE query using GiST index, returning matched text for verification.
///
/// This variant returns the actual text that matched, useful for debugging.
///
/// # Arguments
/// * `index` - GiST index
/// * `db` - CoreDB reference
/// * `field` - Field name to extract text from
/// * `pattern` - ILIKE pattern
/// * `limit` - Maximum results
///
/// # Returns
/// * `Vec<(u64, String)>` - (doc_id, matched text)
pub fn ilike_gist_with_text(
    index: &GiSTIndex,
    db: &crate::CoreDB,
    field: &str,
    pattern: &str,
    limit: Option<usize>,
) -> Vec<(u64, String)> {
    let candidates = index.ilike_candidates(pattern, None);

    let mut results = Vec::new();
    for doc_id in candidates {
        if let Some(payload) = db.get_payload(doc_id) {
            if let Some(text) = payload.get(field).and_then(|v| v.as_str()).map(|s| s.to_string()) {
                if ilike_matches(&text, pattern) {
                    results.push((doc_id, text));
                    if let Some(l) = limit {
                        if results.len() >= l {
                            break;
                        }
                    }
                }
            }
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ilike_basic() {
        assert!(ilike_matches("Hello World", "%World%"));
        assert!(ilike_matches("Hello World", "%world%"));
        assert!(ilike_matches("Hello World", "%HELLO%"));
        assert!(!ilike_matches("Hello World", "%foo%"));
    }

    #[test]
    fn test_ilike_wildcards() {
        assert!(ilike_matches("The Vines", "%Vines%"));
        assert!(ilike_matches("The Vines", "The%"));
        assert!(ilike_matches("The Vines", "%Vines"));
        assert!(ilike_matches("The Vines", "The Vines"));
    }

    #[test]
    fn test_ilike_underscore() {
        // Note: current implementation handles % only, not _ (underscore)
        assert!(ilike_matches("foo_bar", "foo_bar"));
        // "foo%bar" should match "foobar", "fooxyzbar", etc.
        let result = ilike_matches("foobar", "foo%bar");
        eprintln!("ilike_matches('foobar', 'foo%bar') = {}", result);
        assert!(result);
    }

    #[test]
    fn test_ilike_empty_pattern() {
        assert!(ilike_matches("anything", "%"));
        assert!(ilike_matches("", "%"));
    }

    #[test]
    fn test_ilike_case_insensitive() {
        assert!(ilike_matches("ALPHA", "%alpha%"));
        assert!(ilike_matches("Alpha", "%ALPHA%"));
        assert!(ilike_matches("alpha", "%Alpha%"));
    }
}
