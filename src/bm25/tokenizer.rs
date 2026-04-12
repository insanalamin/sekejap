//! Simple tokenizer for BM25.
//!
//! Splits text into lowercase terms, filtering out short words (< 3 chars).
//! No stemming, no stop words (keep it simple).

use std::collections::HashSet;

/// Tokenize text into terms.
/// Returns lowercase terms with length >= 3.
pub fn tokenize(text: &str) -> Vec<String> {
    let mut terms: Vec<String> = Vec::new();
    let mut current = String::new();

    for c in text.to_lowercase().chars() {
        if c.is_alphanumeric() {
            current.push(c);
        } else if !current.is_empty() {
            if current.len() >= 3 {
                terms.push(current.clone());
            }
            current.clear();
        }
    }

    if !current.is_empty() && current.len() >= 3 {
        terms.push(current);
    }

    terms
}

/// Tokenize and deduplicate, preserving frequency count.
pub fn tokenize_with_freq(text: &str) -> HashSet<(String, u32)> {
    let tokens = tokenize(text);
    let mut freq: HashSet<(String, u32)> = HashSet::new();
    let mut counts: std::collections::HashMap<String, u32> = std::collections::HashMap::new();

    for term in tokens {
        *counts.entry(term).or_default() += 1;
    }

    freq.extend(counts.into_iter().map(|(t, c)| (t, c)));
    freq
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_basic() {
        let terms = tokenize("Hello World Rust Tutorial");
        assert_eq!(terms, &["hello", "world", "rust", "tutorial"]);
    }

    #[test]
    fn test_tokenize_punctuation() {
        let terms = tokenize("Hello, world! Rust is great?");
        assert_eq!(terms, &["hello", "world", "rust", "great"]);
    }

    #[test]
    fn test_tokenize_short_words() {
        let terms = tokenize("I am a Rust programmer");
        assert_eq!(terms, &["rust", "programmer"]);
    }

    #[test]
    fn test_tokenize_min_length() {
        let terms = tokenize("The Rust is great");
        assert_eq!(terms, &["the", "rust", "great"]);
    }
}
