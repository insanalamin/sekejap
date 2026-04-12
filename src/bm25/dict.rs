//! Term dictionary for BM25.
//!
//! Maps each term to its postings list location in the postings file.
//!
//! Layout:
//! [header: u32 num_terms]
//! [sorted term entries...]
//!
//! Each term entry:
//! [term_len (u8)] [term_bytes...] [postings_offset (u64)] [postings_len (u32)]

use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct TermEntry {
    pub term: String,
    pub postings_offset: u64,
    pub postings_len: u32,
}

pub struct TermDict {
    entries: HashMap<String, TermEntry>,
    sorted_terms: Vec<String>,
}

impl TermDict {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            sorted_terms: Vec::new(),
        }
    }

    pub fn insert(&mut self, term: String, postings_offset: u64, postings_len: u32) {
        let entry = TermEntry {
            term: term.clone(),
            postings_offset,
            postings_len,
        };
        self.entries.insert(term, entry);
    }

    pub fn get(&self, term: &str) -> Option<&TermEntry> {
        self.entries.get(term)
    }

    pub fn build_index(&mut self) {
        self.sorted_terms = self.entries.keys().cloned().collect();
        self.sorted_terms.sort();
    }

    pub fn terms(&self) -> &[String] {
        &self.sorted_terms
    }

    pub fn num_terms(&self) -> usize {
        self.entries.len()
    }

    pub fn get_offset(&self, term: &str) -> Option<u64> {
        self.entries.get(term).map(|e| e.postings_offset)
    }
}

impl Default for TermDict {
    fn default() -> Self {
        Self::new()
    }
}

/// Serialize term dict to bytes for mmap.
pub fn serialize_dict(dict: &TermDict) -> Vec<u8> {
    let mut bytes = Vec::new();

    // Header: num_terms (u32)
    let num_terms = dict.num_terms() as u32;
    bytes.extend_from_slice(&num_terms.to_le_bytes());

    // Sort terms for binary search
    let mut terms: Vec<_> = dict.entries.values().collect();
    terms.sort_by_key(|e| e.term.clone());

    // Calculate index offset (comes after all entries)
    let mut data_offset = 4 + terms.len() * 12; // header + (term_len + postings_offset + postings_len) * num_terms

    // Write entries (terms stored separately at the end)
    for entry in &terms {
        let term_bytes = entry.term.as_bytes();
        let term_len = term_bytes.len() as u8;
        bytes.push(term_len);
        bytes.extend_from_slice(term_bytes);
        bytes.extend_from_slice(&entry.postings_offset.to_le_bytes());
        bytes.extend_from_slice(&entry.postings_len.to_le_bytes());
    }

    // Write term strings
    for entry in &terms {
        bytes.extend_from_slice(entry.term.as_bytes());
    }

    bytes
}

/// Deserialize term dict from bytes.
pub fn deserialize_dict(bytes: &[u8]) -> TermDict {
    let mut dict = TermDict::new();

    if bytes.len() < 4 {
        return dict;
    }

    let num_terms = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let mut offset = 4;

    // Read entry headers
    let mut entries: Vec<(String, u64, u32)> = Vec::with_capacity(num_terms);
    for _ in 0..num_terms {
        if offset >= bytes.len() {
            break;
        }
        let term_len = bytes[offset] as usize;
        offset += 1;

        if offset + term_len + 12 > bytes.len() {
            break;
        }

        let term = String::from_utf8_lossy(&bytes[offset..offset + term_len]).to_string();
        offset += term_len;

        let postings_offset = u64::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        let postings_len = u32::from_le_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        entries.push((term, postings_offset, postings_len));
    }

    for (term, postings_offset, postings_len) in entries {
        dict.insert(term, postings_offset, postings_len);
    }

    dict.build_index();
    dict
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dict_serialize_roundtrip() {
        let mut dict = TermDict::new();
        dict.insert("rust".to_string(), 0, 100);
        dict.insert("tutorial".to_string(), 100, 200);
        dict.insert("programming".to_string(), 300, 150);
        dict.build_index();

        let bytes = serialize_dict(&dict);
        let dict2 = deserialize_dict(&bytes);

        assert_eq!(dict2.get("rust").unwrap().postings_len, 100);
        assert_eq!(dict2.get("tutorial").unwrap().postings_offset, 100);
        assert_eq!(dict2.num_terms(), 3);
    }
}
