//! ## Trigram Extraction
//!
//! A trigram is a 3-character substring extracted via sliding window.
//! Follows PostgreSQL's pg_trgm convention: space padding before and after.
//!
//! ### Why Space Padding?
//!
//! pg_trgm adds spaces before and after the string to improve edge trigram matching:
//! - "cat" → " al", " alp", "alp", "lph", "pha", "ha "
//! - This helps ILIKE '%cat' match strings starting with "cat"
//!
//! ### Examples
//!
//! ```rust
//! use sekejap::text_index::trigram::extract_trigrams;
//!
//! // Basic extraction with space padding (like pg_trgm)
//! let trigrams = sekejap::text_index::trigram::extract_trigrams("Alpha");
//! assert!(trigrams.contains(&" al".to_string())); // leading space padded
//! assert!(trigrams.contains(&" al".to_string())); // leading space padded
//! assert!(trigrams.contains(&"alp".to_string()));   // no padding overlap
//! assert!(trigrams.contains(&"lph".to_string()));
//!
//! // Short strings (< 3 chars) return empty
//! let trigrams = extract_trigrams("AB");
//! assert!(trigrams.is_empty());
//! ```
//!
//! ### Hashing
//!
//! FNV-1a hash for trigrams — fast and good distribution for 3-byte strings.

use std::collections::HashSet;

/// Extract trigrams from a string with space padding (pg_trgm convention).
///
/// Adds ' ' (space) before and after the string, then extracts all
/// 3-character substrings via sliding window. Lowercase for case-insensitive matching.
///
/// # Arguments
/// * `text` - The input string to extract trigrams from
///
/// # Returns
/// * `Vec<String>` - All trigrams found (lowercased, with leading/trailing spaces)
///
/// # Example (run with `cargo test --doc` to see actual output)
///
/// ```rust,ignore
/// let trigrams = sekejap::text_index::trigram::extract_trigrams("Alpha");
/// // Returns: [" al", "alp", "lph", "pha", "ha "]
/// ```
pub fn extract_trigrams(text: &str) -> Vec<String> {
    let lower = text.to_lowercase();
    let chars: Vec<char> = lower.chars().collect();
    let len = chars.len();

    if len < 3 {
        return vec![];
    }

    // Create padded string: " text" with leading space
    let mut result = Vec::with_capacity(len + 2);
    result.push(' ');

    for c in &chars {
        result.push(*c);
    }
    result.push(' ');

    // Extract trigrams via sliding window
    let mut trigrams = Vec::with_capacity(len);
    for window in result.windows(3) {
        trigrams.push(window.iter().collect::<String>());
    }

    trigrams
}

/// Extract trigrams from a pattern string (for ILIKE query).
///
/// Unlike document extraction, patterns may contain '%' wildcards.
/// We extract only the fixed (non-wildcard) parts as trigrams.
///
/// # Arguments
/// * `pattern` - ILIKE pattern like "%Alpha%" or "foo%bar%"
///
/// # Returns
/// * `Vec<String>` - Trigrams that MUST appear in matching documents
///
/// # Example
/// ```
/// use sekejap::text_index::trigram::extract_pattern_trigrams;
/// let trigrams = extract_pattern_trigrams("%Alpha%");
/// // Returns trigrams for "Alpha": [" al", "alp", "lph", "pha", "ha "]
/// ```
pub fn extract_pattern_trigrams(pattern: &str) -> Vec<String> {
    // Split pattern on wildcards and collect fixed literal segments.
    // Each segment separated by % can match at any position in the document,
    // so we must NOT add space-padding — only interior trigrams are valid.
    // Space-padding is only appropriate when a segment is anchored to the
    // start or end of the value (no leading/trailing %).
    let has_leading_pct  = pattern.starts_with('%');
    let has_trailing_pct = pattern.ends_with('%');

    // Strip leading/trailing wildcards and split remaining on %
    let inner = pattern.trim_matches(|c| c == '%' || c == '_');
    let segments: Vec<&str> = inner.split('%').filter(|s| s.len() >= 3).collect();

    if segments.is_empty() {
        return vec![];
    }

    let mut all_trigrams: Vec<String> = Vec::new();

    for (i, seg) in segments.iter().enumerate() {
        let lower = seg.to_lowercase();
        let chars: Vec<char> = lower.chars().collect();
        if chars.len() < 3 { continue; }

        // Decide whether to space-pad this segment's edges:
        // - pad start only if this is the first segment AND no leading %
        // - pad end only if this is the last segment AND no trailing %
        let pad_start = i == 0 && !has_leading_pct;
        let pad_end   = i == segments.len() - 1 && !has_trailing_pct;

        let mut padded: Vec<char> = Vec::with_capacity(chars.len() + 2);
        if pad_start { padded.push(' '); }
        padded.extend_from_slice(&chars);
        if pad_end   { padded.push(' '); }

        for window in padded.windows(3) {
            all_trigrams.push(window.iter().collect());
        }
    }

    all_trigrams
}

/// Hash a trigram string to a u32 value using FNV-1a.
///
/// FNV-1a is chosen because:
/// - Fast computation
/// - Good distribution for short strings
/// - No cryptographic requirements
///
/// # Arguments
/// * `trigram` - A 3-character string (may include spaces)
///
/// # Returns
/// * `u32` - Hash value
pub fn hash_trigram(trigram: &str) -> u32 {
    let bytes = trigram.as_bytes();
    let mut hash: u32 = 2166136261; // FNV offset basis
    for &b in bytes {
        hash ^= b as u32;
        hash = hash.wrapping_mul(16777619); // FNV prime
    }
    hash
}

/// Deduplicate trigrams while preserving order.
///
/// For ILIKE queries, we want to use as many trigrams as possible
/// but avoid redundant AND operations on the same trigram.
///
/// # Arguments
/// * `trigrams` - List of trigrams (possibly with duplicates)
///
/// # Returns
/// * `Vec<String>` - Trigrams in order, without duplicates
pub fn dedup_trigrams(trigrams: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();
    for t in trigrams {
        if seen.insert(t) {
            result.push(t.clone());
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_trigrams_basic() {
        let trigrams = extract_trigrams("Alpha");
        eprintln!("trigrams for 'Alpha': {:?}", trigrams);
        // Implementation: space padding + sliding window of 3
        // " Alpha" → [" al", "alp", "lph", "pha", "ha "]
        assert!(
            trigrams.contains(&" al".to_string()),
            "should have ' al': {:?}",
            trigrams
        );
        assert!(
            trigrams.contains(&"alp".to_string()),
            "should have 'alp': {:?}",
            trigrams
        );
        assert!(
            trigrams.contains(&"lph".to_string()),
            "should have 'lph': {:?}",
            trigrams
        );
        assert!(
            trigrams.contains(&"pha".to_string()),
            "should have 'pha': {:?}",
            trigrams
        );
        assert!(
            trigrams.contains(&"ha ".to_string()),
            "should have 'ha ': {:?}",
            trigrams
        );
    }

    #[test]
    fn test_extract_trigrams_case_insensitive() {
        let lower = extract_trigrams("alpha");
        let upper = extract_trigrams("ALPHA");
        assert_eq!(lower, upper);
    }

    #[test]
    fn test_extract_trigrams_short() {
        assert!(extract_trigrams("AB").is_empty());
        assert!(extract_trigrams("A").is_empty());
        assert!(extract_trigrams("").is_empty());
    }

    #[test]
    fn test_extract_trigrams_with_spaces() {
        let trigrams = extract_trigrams("The Vines");
        assert!(trigrams.contains(&" th".to_string()));
        assert!(trigrams.contains(&"the".to_string()));
        assert!(trigrams.contains(&"he ".to_string()));
        assert!(trigrams.contains(&"e v".to_string()));
    }

    #[test]
    fn test_extract_pattern_trigrams() {
        // %Alpha% — both sides wildcarded, so only interior trigrams (no space padding)
        let trigrams = extract_pattern_trigrams("%Alpha%");
        assert!(trigrams.contains(&"alp".to_string()));
        assert!(trigrams.contains(&"lph".to_string()));
        assert!(trigrams.contains(&"pha".to_string()));
        // Space-padded boundary trigrams must NOT be present
        assert!(!trigrams.contains(&" al".to_string()), "leading space should not appear with leading %");
        assert!(!trigrams.contains(&"ha ".to_string()), "trailing space should not appear with trailing %");

        // Alpha% — no leading wildcard, so leading space IS added
        let trigrams2 = extract_pattern_trigrams("Alpha%");
        assert!(trigrams2.contains(&" al".to_string()), "no leading % → leading space expected");
        assert!(!trigrams2.contains(&"ha ".to_string()), "trailing % → trailing space NOT expected");

        // %Alpha — no trailing wildcard, trailing space IS added
        let trigrams3 = extract_pattern_trigrams("%Alpha");
        assert!(!trigrams3.contains(&" al".to_string()), "leading % → leading space NOT expected");
        assert!(trigrams3.contains(&"ha ".to_string()), "no trailing % → trailing space expected");
    }

    #[test]
    fn test_extract_pattern_trigrams_wildcards_removed() {
        let trigrams = extract_pattern_trigrams("%foo_bar%");
        let has_underscore = trigrams.iter().any(|t| t == "_");
        assert!(!has_underscore);
    }

    #[test]
    fn test_hash_trigram() {
        let h1 = hash_trigram(" alp");
        let h2 = hash_trigram(" alp");
        let h3 = hash_trigram("bet");
        assert_eq!(h1, h2); // Same input = same hash
        assert_ne!(h1, h3); // Different input = different hash
    }

    #[test]
    fn test_dedup_trigrams() {
        let input = vec![
            " al".to_string(),
            " alp".to_string(),
            "alp".to_string(),
            " al".to_string(),
        ];
        let deduped = dedup_trigrams(&input);
        assert_eq!(deduped.len(), 3);
        assert_eq!(deduped[0], " al");
        assert_eq!(deduped[1], " alp");
        assert_eq!(deduped[2], "alp");
    }
}
