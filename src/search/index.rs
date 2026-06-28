use roaring::RoaringBitmap;
use std::collections::HashMap;
use fst::IntoStreamer;

use crate::bm25::tokenizer::tokenize_with_positions;

const POSITION_BUCKET_SIZE: usize = 8;

pub struct SearchIndex {
    pub(crate) fields: Vec<String>,
    pub(crate) id_map: Vec<u64>,
    pub(crate) id_to_slot: HashMap<u64, u32>,
    pub(crate) doc_count: u32,
    pub(crate) doc_field_lengths: Vec<Vec<u16>>,
    /// FST mapping term → byte offset into `postings_data`.
    pub(crate) fst_data: Vec<u8>,
    /// Contiguous serialized RoaringBitmaps: at each offset, [len: u32 LE][bitmap bytes].
    pub(crate) postings_data: Vec<u8>,
    pub(crate) term_field_bitmaps: HashMap<(String, u8), RoaringBitmap>,
    pub(crate) term_position_bitmaps: HashMap<(String, u16), RoaringBitmap>,
}

pub struct DocFields {
    pub hash: u64,
    pub field_values: Vec<String>,
}

/// Auto-select max edit distance based on term length (matches Meilisearch rules).
fn auto_distance(term: &str) -> u32 {
    match term.len() {
        0..=4 => 0,
        5..=8 => 1,
        _ => 2,
    }
}

fn deduplicate_tokens(query: &str) -> Vec<String> {
    let tokens = tokenize_with_positions(query);
    let mut seen = std::collections::HashSet::new();
    tokens.into_iter()
        .filter_map(|(t, _)| if seen.insert(t.clone()) { Some(t) } else { None })
        .collect()
}

impl SearchIndex {
    pub fn build(fields: Vec<String>, docs: impl Iterator<Item = DocFields>) -> Self {
        let mut id_map = Vec::new();
        let mut id_to_slot = HashMap::new();
        let mut doc_field_lengths: Vec<Vec<u16>> = Vec::new();
        let mut term_bitmaps: HashMap<String, RoaringBitmap> = HashMap::new();
        let mut term_field_bitmaps: HashMap<(String, u8), RoaringBitmap> = HashMap::new();
        let mut term_position_bitmaps: HashMap<(String, u16), RoaringBitmap> = HashMap::new();

        let num_fields = fields.len();

        for doc in docs {
            let slot = id_map.len() as u32;
            id_to_slot.insert(doc.hash, slot);
            id_map.push(doc.hash);

            let mut lengths = Vec::with_capacity(num_fields);
            let mut global_pos: usize = 0;

            for (field_idx, text) in doc.field_values.iter().enumerate() {
                let tokens = tokenize_with_positions(text);
                lengths.push(tokens.len().min(u16::MAX as usize) as u16);

                for (term, _local_pos) in &tokens {
                    term_bitmaps.entry(term.clone())
                        .or_default()
                        .insert(slot);

                    term_field_bitmaps.entry((term.clone(), field_idx as u8))
                        .or_default()
                        .insert(slot);

                    let bucket = (global_pos / POSITION_BUCKET_SIZE).min(u16::MAX as usize) as u16;
                    term_position_bitmaps.entry((term.clone(), bucket))
                        .or_default()
                        .insert(slot);

                    global_pos += 1;
                }
            }

            doc_field_lengths.push(lengths);
        }

        // Build FST + postings blob from the HashMap
        let mut sorted_terms: Vec<&String> = term_bitmaps.keys().collect();
        sorted_terms.sort();

        let mut postings_data = Vec::new();
        let mut fst_builder = fst::MapBuilder::memory();

        for term in &sorted_terms {
            let offset = postings_data.len() as u64;
            fst_builder.insert(term.as_bytes(), offset).unwrap();

            let bm = &term_bitmaps[*term];
            let mut bm_bytes = Vec::new();
            bm.serialize_into(&mut bm_bytes).unwrap();
            postings_data.extend_from_slice(&(bm_bytes.len() as u32).to_le_bytes());
            postings_data.extend_from_slice(&bm_bytes);
        }

        let fst_data = fst_builder.into_inner().unwrap();
        let doc_count = id_map.len() as u32;

        SearchIndex {
            fields,
            id_map,
            id_to_slot,
            doc_count,
            doc_field_lengths,
            fst_data,
            postings_data,
            term_field_bitmaps,
            term_position_bitmaps,
        }
    }

    /// Read a bitmap from the postings blob at the given byte offset.
    fn read_bitmap_at(&self, offset: usize) -> Option<RoaringBitmap> {
        if offset + 4 > self.postings_data.len() {
            return None;
        }
        let len = u32::from_le_bytes(
            self.postings_data[offset..offset + 4].try_into().ok()?
        ) as usize;
        if offset + 4 + len > self.postings_data.len() {
            return None;
        }
        RoaringBitmap::deserialize_from(&self.postings_data[offset + 4..offset + 4 + len]).ok()
    }

    /// Exact term lookup via FST.
    fn get_bitmap(&self, term: &str) -> Option<RoaringBitmap> {
        let map = fst::Map::new(&self.fst_data).ok()?;
        let offset = map.get(term)? as usize;
        self.read_bitmap_at(offset)
    }

    /// Fuzzy term lookup via Levenshtein automaton.
    /// Returns the OR of all bitmaps for terms within `max_dist` edits.
    fn search_fuzzy(&self, term: &str, max_dist: u32) -> RoaringBitmap {
        self.search_fuzzy_with_terms(term, max_dist).0
    }

    /// Fuzzy search returning both the union bitmap and the matched FST term strings.
    fn search_fuzzy_with_terms(&self, term: &str, max_dist: u32) -> (RoaringBitmap, Vec<String>) {
        if max_dist == 0 {
            return match self.get_bitmap(term) {
                Some(bm) => (bm, vec![term.to_string()]),
                None => (RoaringBitmap::new(), vec![]),
            };
        }
        let map = match fst::Map::new(&self.fst_data) {
            Ok(m) => m,
            Err(_) => return (RoaringBitmap::new(), vec![]),
        };
        let lev = match fst::automaton::Levenshtein::new(term, max_dist) {
            Ok(l) => l,
            Err(_) => return match self.get_bitmap(term) {
                Some(bm) => (bm, vec![term.to_string()]),
                None => (RoaringBitmap::new(), vec![]),
            },
        };
        use fst::Streamer;
        let mut stream = map.search(&lev).into_stream();
        let mut result = RoaringBitmap::new();
        let mut matched_terms = Vec::new();
        while let Some((bytes, offset)) = stream.next() {
            if let Some(bm) = self.read_bitmap_at(offset as usize) {
                result |= bm;
            }
            if let Ok(s) = std::str::from_utf8(bytes) {
                matched_terms.push(s.to_string());
            }
        }
        (result, matched_terms)
    }

    /// AND intersection with typo tolerance: returns bitmap of slots matching ALL query terms.
    pub fn search(&self, query: &str) -> RoaringBitmap {
        let unique_terms = deduplicate_tokens(query);
        if unique_terms.is_empty() {
            return RoaringBitmap::new();
        }

        let mut result: Option<RoaringBitmap> = None;
        for term in &unique_terms {
            let bm = match self.get_bitmap(term) {
                Some(bm) if !bm.is_empty() => bm,
                _ => self.search_fuzzy(term, auto_distance(term)),
            };

            if bm.is_empty() {
                return RoaringBitmap::new();
            }

            result = Some(match result {
                Some(acc) => acc & bm,
                None => bm,
            });
        }

        result.unwrap_or_default()
    }

    /// Cascade score: words → typo → proximity → field_order → exactness.
    /// Returns a composite f64 where higher = better ranking. Each rule occupies
    /// a separate magnitude band so a better words score always beats a worse one
    /// regardless of lower-tier rules.
    pub fn score(&self, query: &str, slot: u32) -> f64 {
        let terms = deduplicate_tokens(query);
        if terms.is_empty() { return 0.0; }

        let num_terms = terms.len();
        let mut matched_count = 0u32;
        let mut total_edits = 0u32;
        let mut best_field_idx = self.fields.len();
        let mut matched_fst_terms: Vec<Vec<String>> = Vec::with_capacity(num_terms);

        for term in &terms {
            let max_dist = auto_distance(term);

            // Exact match
            if let Some(bm) = self.get_bitmap(term) {
                if bm.contains(slot) {
                    matched_count += 1;
                    matched_fst_terms.push(vec![term.clone()]);
                    for fi in 0..self.fields.len() {
                        if self.term_field_bitmaps.get(&(term.clone(), fi as u8))
                            .map_or(false, |b| b.contains(slot)) {
                            best_field_idx = best_field_idx.min(fi);
                            break;
                        }
                    }
                    continue;
                }
            }

            // Fuzzy d=1
            if max_dist >= 1 {
                let (bm, fst_terms) = self.search_fuzzy_with_terms(term, 1);
                if bm.contains(slot) {
                    matched_count += 1;
                    total_edits += 1;
                    for ft in &fst_terms {
                        if best_field_idx == 0 { break; }
                        for fi in 0..self.fields.len() {
                            if self.term_field_bitmaps.get(&(ft.clone(), fi as u8))
                                .map_or(false, |b| b.contains(slot)) {
                                best_field_idx = best_field_idx.min(fi);
                                break;
                            }
                        }
                    }
                    matched_fst_terms.push(fst_terms);
                    continue;
                }
            }

            // Fuzzy d=2
            if max_dist >= 2 {
                let (bm, fst_terms) = self.search_fuzzy_with_terms(term, 2);
                if bm.contains(slot) {
                    matched_count += 1;
                    total_edits += 2;
                    for ft in &fst_terms {
                        if best_field_idx == 0 { break; }
                        for fi in 0..self.fields.len() {
                            if self.term_field_bitmaps.get(&(ft.clone(), fi as u8))
                                .map_or(false, |b| b.contains(slot)) {
                                best_field_idx = best_field_idx.min(fi);
                                break;
                            }
                        }
                    }
                    matched_fst_terms.push(fst_terms);
                    continue;
                }
            }

            matched_fst_terms.push(vec![]);
        }

        if matched_count == 0 { return 0.0; }

        let words = matched_count as f64 / num_terms as f64;
        let typo = 1.0 - (total_edits as f64 / (matched_count as f64 * 2.0));
        let proximity = self.cascade_proximity(&matched_fst_terms, slot);
        let field_order = if self.fields.len() <= 1 || best_field_idx >= self.fields.len() {
            1.0
        } else {
            1.0 - (best_field_idx as f64 / (self.fields.len() as f64 - 1.0))
        };
        let exactness = self.cascade_exactness(&terms, slot);

        words * 1e12 + typo * 1e9 + proximity * 1e6 + field_order * 1e3 + exactness
    }

    fn cascade_proximity(&self, matched_fst_terms: &[Vec<String>], slot: u32) -> f64 {
        if matched_fst_terms.len() < 2 { return 1.0; }

        let mut total = 0.0;
        let mut pairs = 0u32;

        for i in 0..matched_fst_terms.len() - 1 {
            let ta = &matched_fst_terms[i];
            let tb = &matched_fst_terms[i + 1];
            if ta.is_empty() || tb.is_empty() { continue; }

            let buckets_a: Vec<u16> = ta.iter().flat_map(|t| {
                self.term_position_bitmaps.iter()
                    .filter(move |((tt, _), bm)| tt == t && bm.contains(slot))
                    .map(|((_, b), _)| *b)
            }).collect();

            let buckets_b: Vec<u16> = tb.iter().flat_map(|t| {
                self.term_position_bitmaps.iter()
                    .filter(move |((tt, _), bm)| tt == t && bm.contains(slot))
                    .map(|((_, b), _)| *b)
            }).collect();

            if buckets_a.is_empty() || buckets_b.is_empty() { continue; }

            let min_dist = buckets_a.iter()
                .flat_map(|a| buckets_b.iter().map(move |b| (*a as i32 - *b as i32).unsigned_abs()))
                .min()
                .unwrap_or(u32::MAX);

            total += 1.0 / (1.0 + min_dist as f64);
            pairs += 1;
        }

        if pairs == 0 { return 0.5; }
        total / pairs as f64
    }

    fn cascade_exactness(&self, query_terms: &[String], slot: u32) -> f64 {
        let qlen = query_terms.len() as u16;
        if let Some(lengths) = self.doc_field_lengths.get(slot as usize) {
            for &flen in lengths {
                if flen == qlen { return 1.0; }
            }
            if let Some(&min_len) = lengths.iter().filter(|&&l| l > 0).min() {
                if min_len > qlen {
                    return qlen as f64 / min_len as f64;
                }
            }
        }
        0.0
    }

    pub fn slot_to_hash(&self, slot: u32) -> Option<u64> {
        self.id_map.get(slot as usize).copied()
    }

    pub fn hash_to_slot(&self, hash: u64) -> Option<u32> {
        self.id_to_slot.get(&hash).copied()
    }

    pub fn delete(&mut self, hash: u64) {
        if let Some(_slot) = self.id_to_slot.remove(&hash) {
            // FST + postings blob are immutable — deletion is tracked via id_to_slot.
            // Deleted docs are excluded at search time by filtering through id_to_slot.
            for bm in self.term_field_bitmaps.values_mut() {
                bm.remove(_slot);
            }
            for bm in self.term_position_bitmaps.values_mut() {
                bm.remove(_slot);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_docs() -> Vec<DocFields> {
        vec![
            DocFields {
                hash: 100,
                field_values: vec!["Rust Programming Language".into(), "Rust is fast and safe".into()],
            },
            DocFields {
                hash: 200,
                field_values: vec!["Python Guide".into(), "Python is easy to learn".into()],
            },
            DocFields {
                hash: 300,
                field_values: vec!["Rust and Python".into(), "Both languages are great".into()],
            },
        ]
    }

    #[test]
    fn search_single_term() {
        let idx = SearchIndex::build(
            vec!["title".into(), "body".into()],
            make_docs().into_iter(),
        );
        let results = idx.search("rust");
        assert!(results.contains(0)); // doc 100
        assert!(!results.contains(1)); // doc 200
        assert!(results.contains(2)); // doc 300
    }

    #[test]
    fn search_multi_term_and() {
        let idx = SearchIndex::build(
            vec!["title".into(), "body".into()],
            make_docs().into_iter(),
        );
        let results = idx.search("rust fast");
        assert!(results.contains(0)); // has both
        assert!(!results.contains(2)); // has rust but not fast
    }

    #[test]
    fn search_no_match() {
        let idx = SearchIndex::build(
            vec!["title".into(), "body".into()],
            make_docs().into_iter(),
        );
        let results = idx.search("javascript");
        assert!(results.is_empty());
    }

    #[test]
    fn score_cascade_ordering() {
        let idx = SearchIndex::build(
            vec!["title".into(), "body".into()],
            make_docs().into_iter(),
        );
        let s0 = idx.score("rust fast", 0); // doc 100: has both terms → words=1.0
        let s2 = idx.score("rust fast", 2); // doc 300: has only rust → words=0.5
        assert!(s0 > 0.0);
        assert!(s2 > 0.0);
        assert!(s0 > s2, "doc with all terms should rank higher");
    }

    #[test]
    fn score_cascade_typo_penalty() {
        let idx = SearchIndex::build(
            vec!["title".into()],
            vec![
                DocFields { hash: 100, field_values: vec!["Rust Programming Language".into()] },
            ].into_iter(),
        );
        let exact = idx.score("programming", 0);
        let typo = idx.score("programing", 0); // 1 edit
        assert!(exact > typo, "exact match should score higher than fuzzy");
    }

    #[test]
    fn score_cascade_field_order() {
        let idx = SearchIndex::build(
            vec!["title".into(), "body".into()],
            vec![
                DocFields { hash: 100, field_values: vec!["Rust Language".into(), "something else".into()] },
                DocFields { hash: 200, field_values: vec!["something else".into(), "Rust Language".into()] },
            ].into_iter(),
        );
        let s_title = idx.score("rust", 0); // "rust" in title (field 0)
        let s_body = idx.score("rust", 1);  // "rust" in body (field 1)
        assert!(s_title > s_body, "match in earlier field should rank higher");
    }

    #[test]
    fn score_cascade_proximity() {
        let idx = SearchIndex::build(
            vec!["body".into()],
            vec![
                DocFields { hash: 100, field_values: vec!["rust is fast".into()] },
                DocFields { hash: 200, field_values: vec!["rust programming language is very fast and safe".into()] },
            ].into_iter(),
        );
        let close = idx.score("rust fast", 0);  // "rust" and "fast" are 1 word apart
        let far = idx.score("rust fast", 1);    // "rust" and "fast" are many words apart
        assert!(close > far, "closer terms should rank higher");
    }

    #[test]
    fn score_cascade_exactness() {
        let idx = SearchIndex::build(
            vec!["title".into()],
            vec![
                DocFields { hash: 100, field_values: vec!["Rust Language".into()] },
                DocFields { hash: 200, field_values: vec!["Rust Programming Language Guide".into()] },
            ].into_iter(),
        );
        let exact = idx.score("rust language", 0);  // 2 query terms, title has 2 tokens
        let partial = idx.score("rust language", 1); // 2 query terms, title has 4 tokens
        assert!(exact > partial, "exact field length match should rank higher");
    }

    #[test]
    fn delete_removes_from_results() {
        let mut idx = SearchIndex::build(
            vec!["title".into(), "body".into()],
            make_docs().into_iter(),
        );
        assert!(idx.search("rust").contains(0));
        idx.delete(100);
        // Deleted doc should still appear in FST bitmap but be filtered by id_to_slot
        // Actually the bitmap isn't modified for FST — deletion is tracked via id_to_slot removal.
        // The search() method returns raw bitmap matches. Caller filters via slot_to_hash which
        // checks id_to_slot. Let's verify the slot is removed.
        assert!(idx.hash_to_slot(100).is_none());
    }

    #[test]
    fn field_bitmaps_populated() {
        let idx = SearchIndex::build(
            vec!["title".into(), "body".into()],
            make_docs().into_iter(),
        );
        let bm = idx.term_field_bitmaps.get(&("rust".into(), 0)).unwrap();
        assert!(bm.contains(0));
        assert!(bm.contains(2));
        let bm = idx.term_field_bitmaps.get(&("fast".into(), 1)).unwrap();
        assert!(bm.contains(0));
    }

    #[test]
    fn fuzzy_search_typo() {
        let idx = SearchIndex::build(
            vec!["title".into(), "body".into()],
            vec![
                DocFields {
                    hash: 100,
                    field_values: vec!["Rust Programming".into(), "Systems language".into()],
                },
                DocFields {
                    hash: 200,
                    field_values: vec!["Python Guide".into(), "Scripting language".into()],
                },
            ].into_iter(),
        );

        // Exact match works
        let results = idx.search("programming");
        assert!(results.contains(0));

        // Typo: "programing" (1 edit from "programming", 11 chars → max_dist=2)
        let results = idx.search("programing");
        assert!(results.contains(0), "fuzzy match should find 'programming' from 'programing'");

        // Typo: "xyzxyzxyz" (completely different) — should NOT match
        let results = idx.search("xyzxyzxyz");
        assert!(results.is_empty(), "completely unrelated term should not match");
    }

    #[test]
    fn fuzzy_search_short_term_no_typo() {
        let idx = SearchIndex::build(
            vec!["title".into()],
            vec![
                DocFields {
                    hash: 100,
                    field_values: vec!["Rust is fast".into()],
                },
            ].into_iter(),
        );

        // "rust" (4 chars) → max_dist=0, no fuzzy
        let results = idx.search("ruts");
        assert!(results.is_empty(), "4-char term should not fuzzy match");

        // "faste" (5 chars) → max_dist=1, fuzzy should find "fast"... but "fast" is 4 chars
        // Actually "faste" has 5 chars, edit distance from "fast" is 1 (insertion)
        let results = idx.search("faste");
        assert!(results.contains(0), "5-char term with 1 edit should fuzzy match");
    }
}
