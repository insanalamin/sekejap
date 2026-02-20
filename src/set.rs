use crate::db::SekejapDB;
use crate::types::{Step, Hit, EdgeHit, Outcome, Trace, StepReport, Plan, AggOp};
use crate::index::PropertyIndex;
use smallvec::SmallVec;
use roaring::RoaringBitmap;
use std::time::Instant;
use serde_json::Value;

#[cfg(feature = "fulltext")]
use crate::fulltext::SearchHit;

pub struct Set<'db> {
    db: &'db SekejapDB,
    steps: SmallVec<[Step; 8]>,
    // Post-bitmap processing (applied at terminal time)
    sort_by: Option<(String, bool)>,   // (field, ascending)
    skip_n: usize,
    select_fields: Option<Vec<String>>,
}

/// Sort key for mixed numeric/string fields.
enum SortKey {
    Num(f64),
    Str(String),
    Null,
}

impl SortKey {
    fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (SortKey::Num(a), SortKey::Num(b)) => a.total_cmp(b),
            (SortKey::Str(a), SortKey::Str(b)) => a.cmp(b),
            (SortKey::Null, SortKey::Null) => std::cmp::Ordering::Equal,
            (SortKey::Null, _) => std::cmp::Ordering::Greater, // nulls last
            (_, SortKey::Null) => std::cmp::Ordering::Less,
            (SortKey::Num(_), SortKey::Str(_)) => std::cmp::Ordering::Less,
            (SortKey::Str(_), SortKey::Num(_)) => std::cmp::Ordering::Greater,
        }
    }
}

fn extract_sort_key(hit: &Hit, field: &str) -> SortKey {
    let payload = match &hit.payload {
        Some(p) => p,
        None => return SortKey::Null,
    };
    let val: Value = match serde_json::from_str(payload) {
        Ok(v) => v,
        Err(_) => return SortKey::Null,
    };
    match val.get(field) {
        Some(Value::Number(n)) => SortKey::Num(n.as_f64().unwrap_or(0.0)),
        Some(Value::String(s)) => SortKey::Str(s.clone()),
        _ => SortKey::Null,
    }
}

fn project_fields(payload: &str, fields: &[String]) -> String {
    let Ok(val) = serde_json::from_str::<Value>(payload) else {
        return payload.to_string();
    };
    let Some(obj) = val.as_object() else {
        return payload.to_string();
    };
    let projected: serde_json::Map<String, Value> = fields.iter()
        .filter_map(|f| obj.get(f).map(|v| (f.clone(), v.clone())))
        .collect();
    serde_json::to_string(&projected).unwrap_or_else(|_| payload.to_string())
}

impl<'db> Set<'db> {
    pub(crate) fn new(db: &'db SekejapDB, starter: Step) -> Self {
        let mut steps = SmallVec::new();
        steps.push(starter);
        Self { db, steps, sort_by: None, skip_n: 0, select_fields: None }
    }

    pub fn forward(mut self, edge_type: &str) -> Self {
        self.steps.push(Step::Forward(seahash::hash(edge_type.as_bytes())));
        self
    }
    pub fn backward(mut self, edge_type: &str) -> Self {
        self.steps.push(Step::Backward(seahash::hash(edge_type.as_bytes())));
        self
    }
    /// Parallel forward traversal using Rayon (for deep/hops > 3)
    pub fn forward_parallel(mut self, edge_type: &str) -> Self {
        self.steps.push(Step::ForwardParallel(seahash::hash(edge_type.as_bytes())));
        self
    }
    /// Parallel backward traversal using Rayon (for deep/hops > 3)
    pub fn backward_parallel(mut self, edge_type: &str) -> Self {
        self.steps.push(Step::BackwardParallel(seahash::hash(edge_type.as_bytes())));
        self
    }
    pub fn hops(mut self, n: u32) -> Self {
        self.steps.push(Step::Hops(n));
        self
    }
    pub fn leaves(mut self) -> Self { self.steps.push(Step::Leaves); self }
    pub fn roots(mut self) -> Self { self.steps.push(Step::Roots); self }

    pub fn near(mut self, lat: f32, lon: f32, radius_km: f32) -> Self {
        self.steps.push(Step::Near(lat, lon, radius_km));
        self
    }
    pub fn similar(mut self, query: &[f32], k: usize) -> Self {
        self.steps.push(Step::Similar(query.to_vec(), k));
        self
    }
    #[cfg(feature = "fulltext")]
    pub fn matching(mut self, text: &str) -> Self {
        self.steps.push(Step::Matching(text.to_string()));
        self
    }

    pub fn where_eq(mut self, field: &str, value: serde_json::Value) -> Self {
        self.steps.push(Step::WhereEq(field.to_string(), value));
        self
    }
    pub fn where_between(mut self, field: &str, lo: f64, hi: f64) -> Self {
        self.steps.push(Step::WhereBetween(field.to_string(), lo, hi));
        self
    }
    pub fn where_gt(mut self, field: &str, threshold: f64) -> Self {
        self.steps.push(Step::WhereGt(field.to_string(), threshold));
        self
    }
    pub fn where_in(mut self, field: &str, values: Vec<serde_json::Value>) -> Self {
        self.steps.push(Step::WhereIn(field.to_string(), values));
        self
    }
    pub fn where_lt(mut self, field: &str, threshold: f64) -> Self {
        self.steps.push(Step::WhereLt(field.to_string(), threshold));
        self
    }
    pub fn where_lte(mut self, field: &str, threshold: f64) -> Self {
        self.steps.push(Step::WhereLte(field.to_string(), threshold));
        self
    }
    pub fn where_gte(mut self, field: &str, threshold: f64) -> Self {
        self.steps.push(Step::WhereGte(field.to_string(), threshold));
        self
    }

    /// Sort results by a field. Applied after bitmap execution at terminal time.
    pub fn sort(mut self, field: &str, ascending: bool) -> Self {
        self.sort_by = Some((field.to_string(), ascending));
        self
    }
    /// Skip the first N results. Combine with take() for pagination.
    pub fn skip(mut self, n: usize) -> Self {
        self.skip_n = n;
        self
    }
    /// Return only specified fields from each node's JSON payload.
    pub fn select(mut self, fields: &[&str]) -> Self {
        self.select_fields = Some(fields.iter().map(|s| s.to_string()).collect());
        self
    }

    pub fn intersect(mut self, other: Set<'db>) -> Self {
        self.steps.push(Step::Intersect(other.steps.into_vec()));
        self
    }
    pub fn union(mut self, other: Set<'db>) -> Self {
        self.steps.push(Step::Union(other.steps.into_vec()));
        self
    }
    pub fn subtract(mut self, other: Set<'db>) -> Self {
        self.steps.push(Step::Subtract(other.steps.into_vec()));
        self
    }

    pub fn take(mut self, n: usize) -> Self {
        self.steps.push(Step::Take(n));
        self
    }

    pub fn collect(self) -> Result<Outcome<Vec<Hit>>, Box<dyn std::error::Error>> {
        let sort_by = self.sort_by.clone();
        let skip_n = self.skip_n;
        let select_fields = self.select_fields.clone();

        let (bitmap, trace) = self.execute_pipeline()?;
        let mut hits = self.db.resolve_hits(&bitmap, true);

        if let Some((ref field, ascending)) = sort_by {
            let field = field.clone();
            hits.sort_unstable_by(|a, b| {
                let cmp = extract_sort_key(a, &field).cmp_key(&extract_sort_key(b, &field));
                if ascending { cmp } else { cmp.reverse() }
            });
        }

        if skip_n > 0 {
            hits.drain(..skip_n.min(hits.len()));
        }

        if let Some(ref fields) = select_fields {
            for hit in &mut hits {
                if let Some(ref payload) = hit.payload.clone() {
                    hit.payload = Some(project_fields(payload, fields));
                }
            }
        }

        Ok(Outcome { data: hits, trace })
    }

    /// Collect edges outgoing from the candidate set, including metadata.
    pub fn edge_collect(self) -> Result<Outcome<Vec<EdgeHit>>, Box<dyn std::error::Error>> {
        let (bitmap, trace) = self.execute_pipeline()?;
        let mut hits = Vec::new();

        for from_idx in bitmap.iter() {
            let from_slot = self.db.nodes.read_at(from_idx as u64);
            if from_slot.flags == 0 { continue; }

            if let Some(edge_indices) = self.db.adj_fwd.get(&from_idx) {
                for &e_idx in edge_indices.iter() {
                    let edge = self.db.edges.read_at(e_idx as u64);
                    if edge.flags == 0 { continue; }

                    let to_slot = self.db.nodes.read_at(edge.to_node as u64);

                    let meta = match edge.meta_kind {
                        1 if edge.meta_len > 0 => {
                            std::str::from_utf8(&edge.meta[..edge.meta_len as usize])
                                .ok()
                                .map(|s| s.to_string())
                        }
                        2 => {
                            let offset = u64::from_le_bytes(edge.meta[..8].try_into().unwrap_or_default());
                            let len = u32::from_le_bytes(edge.meta[8..12].try_into().unwrap_or_default());
                            if len > 0 {
                                let bytes = self.db.blobs.read(offset, len);
                                std::str::from_utf8(bytes).ok().map(|s| s.to_string())
                            } else { None }
                        }
                        _ => None,
                    };

                    hits.push(EdgeHit {
                        from_idx,
                        to_idx: edge.to_node,
                        from_slug_hash: from_slot.slug_hash,
                        to_slug_hash: to_slot.slug_hash,
                        edge_type_hash: edge.edge_type_hash,
                        weight: edge.weight,
                        timestamp: edge.timestamp,
                        meta,
                    });
                }
            }
        }

        Ok(Outcome { data: hits, trace })
    }

    pub fn count(self) -> Result<Outcome<usize>, Box<dyn std::error::Error>> {
        let (bitmap, trace) = self.execute_pipeline()?;
        Ok(Outcome { data: bitmap.len() as usize, trace })
    }

    pub fn first(self) -> Result<Outcome<Option<Hit>>, Box<dyn std::error::Error>> {
        let (bitmap, trace) = self.execute_pipeline()?;
        let hit = bitmap.iter().next().map(|idx| {
            self.db.resolve_single_hit(idx, true)
        });
        Ok(Outcome { data: hit, trace })
    }

    pub fn exists(self) -> Result<Outcome<bool>, Box<dyn std::error::Error>> {
        let (bitmap, trace) = self.execute_pipeline()?;
        Ok(Outcome { data: !bitmap.is_empty(), trace })
    }

    pub fn avg(self, field: &str) -> Result<Outcome<f64>, Box<dyn std::error::Error>> {
        let (bitmap, trace) = self.execute_pipeline()?;
        let avg = self.db.aggregate_field(&bitmap, field, AggOp::Avg)?;
        Ok(Outcome { data: avg, trace })
    }

    pub fn sum(self, field: &str) -> Result<Outcome<f64>, Box<dyn std::error::Error>> {
        let (bitmap, trace) = self.execute_pipeline()?;
        let sum = self.db.aggregate_field(&bitmap, field, AggOp::Sum)?;
        Ok(Outcome { data: sum, trace })
    }

    pub fn explain(&self) -> Plan {
        Plan { steps: self.steps.to_vec() }
    }

    fn execute_pipeline(&self) -> Result<(RoaringBitmap, Trace), Box<dyn std::error::Error>> {
        let mut trace = Trace { steps: Vec::new(), total_us: 0 };
        let total_start = Instant::now();
        let mut candidates: Option<RoaringBitmap> = None;
        let mut pending_hops: Option<u32> = None;

        for step in &self.steps {
            let step_start = Instant::now();
            let input_size = candidates.as_ref().map_or(0, |b| b.len() as usize);
            let mut index_used = "scan";

            match step {
                Step::One(hash) => {
                    let mut bm = RoaringBitmap::new();
                    if let Some(idx) = self.db.slug_index.read().get(*hash) {
                        bm.insert(idx);
                    }
                    candidates = Some(bm);
                    index_used = "slug_index";
                }
                Step::Many(hashes) => {
                    let mut bm = RoaringBitmap::new();
                    {
                        let slug_r = self.db.slug_index.read();
                        for &hash in hashes {
                            if let Some(idx) = slug_r.get(hash) {
                                bm.insert(idx);
                            }
                        }
                    }
                    candidates = Some(bm);
                    index_used = "slug_index";
                }
                Step::Collection(hash) => {
                    // O(1) bitmap lookup — replaces O(N) mmap scan
                    let bm = self.db.collection_bitmaps.get_snapshot(*hash);
                    if let Some(ref mut curr) = candidates {
                        *curr &= bm;
                    } else {
                        candidates = Some(bm);
                    }
                    index_used = "collection_bitmap";
                }
                Step::All => {
                    let mut bm = RoaringBitmap::new();
                    let count = self.db.nodes.write_head.load(std::sync::atomic::Ordering::Acquire);
                    for i in 0..count {
                        let slot = self.db.nodes.read_at(i);
                        if slot.flags != 0 {
                            bm.insert(i as u32);
                        }
                    }
                    candidates = Some(bm);
                }
                Step::Forward(type_hash) => {
                    let hops = pending_hops.take().unwrap_or(1);
                    candidates = Some(self.bfs_forward(candidates.as_ref(), *type_hash, hops as usize));
                    index_used = "adj_fwd";
                }
                Step::Backward(type_hash) => {
                    let hops = pending_hops.take().unwrap_or(1);
                    candidates = Some(self.bfs_backward(candidates.as_ref(), *type_hash, hops as usize));
                    index_used = "adj_rev";
                }
                Step::ForwardParallel(type_hash) => {
                    let hops = pending_hops.take().unwrap_or(1);
                    candidates = Some(self.bfs_forward_parallel(candidates.as_ref(), *type_hash, hops as usize));
                    index_used = "adj_fwd_parallel";
                }
                Step::BackwardParallel(type_hash) => {
                    let hops = pending_hops.take().unwrap_or(1);
                    candidates = Some(self.bfs_backward_parallel(candidates.as_ref(), *type_hash, hops as usize));
                    index_used = "adj_rev_parallel";
                }
                Step::Hops(n) => {
                    pending_hops = Some(*n);
                    continue;
                }
                Step::Leaves => {
                    if let Some(ref mut bm) = candidates {
                        let mut to_remove = RoaringBitmap::new();
                        for idx in bm.iter() {
                            if let Some(edges) = self.db.adj_fwd.get(&idx) {
                                if !edges.is_empty() {
                                    to_remove.insert(idx);
                                }
                            }
                        }
                        let mut result = RoaringBitmap::new();
                        for idx in bm.iter() {
                            if !to_remove.contains(idx) {
                                result.insert(idx);
                            }
                        }
                        *bm = result;
                    }
                    index_used = "adj_fwd";
                }
                Step::Roots => {
                    if let Some(ref mut bm) = candidates {
                        let mut to_remove = RoaringBitmap::new();
                        for idx in bm.iter() {
                            if let Some(edges) = self.db.adj_rev.get(&idx) {
                                if !edges.is_empty() {
                                    to_remove.insert(idx);
                                }
                            }
                        }
                        let mut result = RoaringBitmap::new();
                        for idx in bm.iter() {
                            if !to_remove.contains(idx) {
                                result.insert(idx);
                            }
                        }
                        *bm = result;
                    }
                    index_used = "adj_rev";
                }
                Step::Near(lat, lon, radius) => {
                    let r_sq = radius * radius;
                    match candidates {
                        Some(ref mut curr) if curr.len() < 500 => {
                            let mut filtered = RoaringBitmap::new();
                            for idx in curr.iter() {
                                let slot = self.db.nodes.read_at(idx as u64);
                                let dx = slot.lat - lat;
                                let dy = slot.lon - lon;
                                if dx*dx + dy*dy <= r_sq {
                                    filtered.insert(idx);
                                }
                            }
                            *curr = filtered;
                            index_used = "filter";
                        }
                        _ => {
                            let results: RoaringBitmap = self.db.spatial.read()
                                .locate_within_distance([*lat, *lon], r_sq)
                                .map(|n| n.id)
                                .collect();
                            if let Some(ref mut curr) = candidates {
                                *curr &= results;
                            } else {
                                candidates = Some(results);
                            }
                            index_used = "rtree";
                        }
                    }
                }
                Step::Similar(query, k) => {
                    let mut bm = RoaringBitmap::new();
                    if let Some(ref hnsw) = *self.db.hnsw.read() {
                        let results = hnsw.search(query, *k, 32);
                        for res in results {
                            bm.insert(res.id);
                        }
                    }
                    if let Some(ref mut curr) = candidates {
                        *curr &= bm;
                    } else {
                        candidates = Some(bm);
                    }
                    index_used = "hnsw";
                }
                #[cfg(feature = "fulltext")]
                Step::Matching(text) => {
                    if let Some(ref ft) = *self.db.fulltext.read() {
                        let slug_hashes = ft.search(text, 1000).unwrap_or_default();
                        let mut bm = RoaringBitmap::new();
                        {
                            let slug_r = self.db.slug_index.read();
                            for hash in slug_hashes {
                                if let Some(idx) = slug_r.get(hash.id) {
                                    bm.insert(idx);
                                }
                            }
                        }
                        if let Some(ref mut curr) = candidates {
                            *curr &= bm;
                        } else {
                            candidates = Some(bm);
                        }
                        index_used = "tantivy";
                    }
                }
                Step::WhereEq(field, value) => {
                    if let Some(hash_idx) = self.db.field_hash_indexes.get(field) {
                        // O(1) HashIndex lookup
                        let hits: RoaringBitmap = hash_idx.lookup_eq(value).into_iter().collect();
                        if let Some(ref mut curr) = candidates {
                            *curr &= hits;
                        } else {
                            candidates = Some(hits);
                        }
                        index_used = "hash_index";
                    } else if let Some(ref mut bm) = candidates {
                        // Fall back to payload scan
                        let mut filtered = RoaringBitmap::new();
                        for idx in bm.iter() {
                            let slot = self.db.nodes.read_at(idx as u64);
                            if slot.flags == 0 { continue; }
                            let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                                if json.get(field) == Some(value) {
                                    filtered.insert(idx);
                                }
                            }
                        }
                        *bm = filtered;
                        index_used = "payload";
                    }
                }
                Step::WhereBetween(field, lo, hi) => {
                    if let Some(range_idx) = self.db.field_range_indexes.get(field) {
                        let hits: RoaringBitmap = range_idx.lookup_range(
                            &serde_json::Value::from(*lo),
                            &serde_json::Value::from(*hi),
                        ).into_iter().collect();
                        if let Some(ref mut curr) = candidates {
                            *curr &= hits;
                        } else {
                            candidates = Some(hits);
                        }
                        index_used = "range_index";
                    } else if let Some(ref mut bm) = candidates {
                        let mut filtered = RoaringBitmap::new();
                        for idx in bm.iter() {
                            let slot = self.db.nodes.read_at(idx as u64);
                            if slot.flags == 0 { continue; }
                            let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                                if let Some(num) = json.get(field).and_then(|v| v.as_f64()) {
                                    if num >= *lo && num <= *hi {
                                        filtered.insert(idx);
                                    }
                                }
                            }
                        }
                        *bm = filtered;
                        index_used = "payload";
                    }
                }
                Step::WhereGt(field, threshold) => {
                    if let Some(range_idx) = self.db.field_range_indexes.get(field) {
                        let hits: RoaringBitmap = range_idx.lookup_range(
                            &serde_json::Value::from(*threshold + f64::EPSILON),
                            &serde_json::Value::from(f64::MAX),
                        ).into_iter().collect();
                        if let Some(ref mut curr) = candidates {
                            *curr &= hits;
                        } else {
                            candidates = Some(hits);
                        }
                        index_used = "range_index";
                    } else if let Some(ref mut bm) = candidates {
                        let mut filtered = RoaringBitmap::new();
                        for idx in bm.iter() {
                            let slot = self.db.nodes.read_at(idx as u64);
                            if slot.flags == 0 { continue; }
                            let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                                if let Some(num) = json.get(field).and_then(|v| v.as_f64()) {
                                    if num > *threshold {
                                        filtered.insert(idx);
                                    }
                                }
                            }
                        }
                        *bm = filtered;
                        index_used = "payload";
                    }
                }
                Step::WhereIn(field, values) => {
                    if let Some(ref mut bm) = candidates {
                        let values_set: std::collections::HashSet<_> = values.iter().collect();
                        let mut filtered = RoaringBitmap::new();
                        for idx in bm.iter() {
                            let slot = self.db.nodes.read_at(idx as u64);
                            if slot.flags == 0 { continue; }
                            let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                                if let Some(v) = json.get(field) {
                                    if values_set.contains(&v) {
                                        filtered.insert(idx);
                                    }
                                }
                            }
                        }
                        *bm = filtered;
                    }
                    index_used = "payload";
                }
                Step::Intersect(other_steps) => {
                    let other_set = Set::from_steps(self.db, other_steps.clone());
                    let (other_bm, _) = other_set.execute_pipeline()?;
                    if let Some(ref mut curr) = candidates {
                        *curr &= other_bm;
                    } else {
                        candidates = Some(other_bm);
                    }
                    index_used = "intersect";
                }
                Step::Union(other_steps) => {
                    let other_set = Set::from_steps(self.db, other_steps.clone());
                    let (other_bm, _) = other_set.execute_pipeline()?;
                    if let Some(ref mut curr) = candidates {
                        *curr |= other_bm;
                    } else {
                        candidates = Some(other_bm);
                    }
                    index_used = "union";
                }
                Step::Subtract(other_steps) => {
                    let other_set = Set::from_steps(self.db, other_steps.clone());
                    let (other_bm, _) = other_set.execute_pipeline()?;
                    if let Some(ref mut curr) = candidates {
                        let mut result = RoaringBitmap::new();
                        for idx in curr.iter() {
                            if !other_bm.contains(idx) {
                                result.insert(idx);
                            }
                        }
                        *curr = result;
                    }
                    index_used = "subtract";
                }
                Step::WhereLt(field, threshold) => {
                    if let Some(range_idx) = self.db.field_range_indexes.get(field) {
                        let hits: RoaringBitmap = range_idx.lookup_range(
                            &serde_json::Value::from(f64::MIN),
                            &serde_json::Value::from(*threshold - f64::EPSILON),
                        ).into_iter().collect();
                        if let Some(ref mut curr) = candidates {
                            *curr &= hits;
                        } else {
                            candidates = Some(hits);
                        }
                        index_used = "range_index";
                    } else if let Some(ref mut bm) = candidates {
                        let mut filtered = RoaringBitmap::new();
                        for idx in bm.iter() {
                            let slot = self.db.nodes.read_at(idx as u64);
                            if slot.flags == 0 { continue; }
                            let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                                if let Some(num) = json.get(field).and_then(|v| v.as_f64()) {
                                    if num < *threshold { filtered.insert(idx); }
                                }
                            }
                        }
                        *bm = filtered;
                        index_used = "payload";
                    }
                }
                Step::WhereLte(field, threshold) => {
                    if let Some(range_idx) = self.db.field_range_indexes.get(field) {
                        let hits: RoaringBitmap = range_idx.lookup_range(
                            &serde_json::Value::from(f64::MIN),
                            &serde_json::Value::from(*threshold),
                        ).into_iter().collect();
                        if let Some(ref mut curr) = candidates {
                            *curr &= hits;
                        } else {
                            candidates = Some(hits);
                        }
                        index_used = "range_index";
                    } else if let Some(ref mut bm) = candidates {
                        let mut filtered = RoaringBitmap::new();
                        for idx in bm.iter() {
                            let slot = self.db.nodes.read_at(idx as u64);
                            if slot.flags == 0 { continue; }
                            let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                                if let Some(num) = json.get(field).and_then(|v| v.as_f64()) {
                                    if num <= *threshold { filtered.insert(idx); }
                                }
                            }
                        }
                        *bm = filtered;
                        index_used = "payload";
                    }
                }
                Step::WhereGte(field, threshold) => {
                    if let Some(range_idx) = self.db.field_range_indexes.get(field) {
                        let hits: RoaringBitmap = range_idx.lookup_range(
                            &serde_json::Value::from(*threshold),
                            &serde_json::Value::from(f64::MAX),
                        ).into_iter().collect();
                        if let Some(ref mut curr) = candidates {
                            *curr &= hits;
                        } else {
                            candidates = Some(hits);
                        }
                        index_used = "range_index";
                    } else if let Some(ref mut bm) = candidates {
                        let mut filtered = RoaringBitmap::new();
                        for idx in bm.iter() {
                            let slot = self.db.nodes.read_at(idx as u64);
                            if slot.flags == 0 { continue; }
                            let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                                if let Some(num) = json.get(field).and_then(|v| v.as_f64()) {
                                    if num >= *threshold { filtered.insert(idx); }
                                }
                            }
                        }
                        *bm = filtered;
                        index_used = "payload";
                    }
                }
                Step::Take(n) => {
                    if let Some(ref mut bm) = candidates {
                        let limited: RoaringBitmap = bm.iter().take(*n).collect();
                        *bm = limited;
                    }
                    index_used = "limit";
                }
                // Sort/Skip/Select are extracted into Set state by from_steps().
                // If somehow reached here, skip silently.
                Step::Sort(_, _) | Step::Skip(_) | Step::Select(_) => {
                    index_used = "noop";
                }
            }

            let elapsed_us = step_start.elapsed().as_micros() as u64;
            let output_size = candidates.as_ref().map_or(0, |b| b.len() as usize);
            trace.steps.push(StepReport {
                atom: format!("{:?}", step),
                input_size,
                output_size,
                index_used: index_used.to_string(),
                time_us: elapsed_us,
            });
        }

        trace.total_us = total_start.elapsed().as_micros() as u64;
        Ok((candidates.unwrap_or_default(), trace))
    }

    fn bfs_forward(&self, candidates: Option<&RoaringBitmap>, type_hash: u64, max_hops: usize) -> RoaringBitmap {
        let mut visited = RoaringBitmap::new();
        let mut frontier: RoaringBitmap = candidates.cloned().unwrap_or_default();
        
        for _ in 0..max_hops {
            if frontier.is_empty() { break; }
            let mut next_frontier = RoaringBitmap::new();
            for idx in frontier.iter() {
                visited.insert(idx);
                if let Some(edge_indices) = self.db.adj_fwd.get(&idx) {
                    for &e_idx in edge_indices.iter() {
                        let edge = self.db.edges.read_at(e_idx as u64);
                        if edge.edge_type_hash == type_hash && edge.flags != 0 && !visited.contains(edge.to_node) {
                            next_frontier.insert(edge.to_node);
                        }
                    }
                }
            }
            frontier = next_frontier;
        }
        visited |= frontier;
        visited
    }

    fn bfs_backward(&self, candidates: Option<&RoaringBitmap>, type_hash: u64, max_hops: usize) -> RoaringBitmap {
        let mut visited = RoaringBitmap::new();
        let mut frontier: RoaringBitmap = candidates.cloned().unwrap_or_default();
        
        for _ in 0..max_hops {
            if frontier.is_empty() { break; }
            let mut next_frontier = RoaringBitmap::new();
            for idx in frontier.iter() {
                visited.insert(idx);
                if let Some(edge_indices) = self.db.adj_rev.get(&idx) {
                    for &e_idx in edge_indices.iter() {
                        let edge = self.db.edges.read_at(e_idx as u64);
                        if edge.edge_type_hash == type_hash && edge.flags != 0 && !visited.contains(edge.from_node) {
                            next_frontier.insert(edge.from_node);
                        }
                    }
                }
            }
            frontier = next_frontier;
        }
        visited |= frontier;
        visited
    }

    /// Parallel forward BFS using Rayon - faster for deep traversals (hops > 3)
    #[cfg(feature = "parallel")]
    fn bfs_forward_parallel(&self, candidates: Option<&RoaringBitmap>, type_hash: u64, max_hops: usize) -> RoaringBitmap {
        use rayon::prelude::*;
        use std::sync::Mutex;
        
        let mut visited = RoaringBitmap::new();
        let mut frontier: RoaringBitmap = candidates.cloned().unwrap_or_default();
        
        for _ in 0..max_hops {
            if frontier.is_empty() { break; }
            
            // Collect frontier into vec for parallel processing
            let frontier_vec: Vec<u32> = frontier.iter().collect();
            
            // Parallel expansion of frontier
            let next_frontier: Mutex<RoaringBitmap> = Mutex::new(RoaringBitmap::new());
            let visited_local: Mutex<RoaringBitmap> = Mutex::new(RoaringBitmap::new());
            
            frontier_vec.into_par_iter().for_each(|idx| {
                visited_local.lock().unwrap().insert(idx);
                if let Some(edge_indices) = self.db.adj_fwd.get(&idx) {
                    for &e_idx in edge_indices.iter() {
                        let edge = self.db.edges.read_at(e_idx as u64);
                        if edge.edge_type_hash == type_hash && edge.flags != 0 {
                            let mut nf = next_frontier.lock().unwrap();
                            nf.insert(edge.to_node);
                        }
                    }
                }
            });
            
            visited |= visited_local.into_inner().unwrap();
            frontier = next_frontier.into_inner().unwrap();
            frontier -= &visited;  // Remove already-visited nodes
        }
        visited |= frontier;
        visited
    }

    #[cfg(not(feature = "parallel"))]
    fn bfs_forward_parallel(&self, candidates: Option<&RoaringBitmap>, type_hash: u64, max_hops: usize) -> RoaringBitmap {
        // Fallback to sequential when parallel feature is disabled
        self.bfs_forward(candidates, type_hash, max_hops)
    }

    /// Parallel backward BFS using Rayon - faster for deep traversals (hops > 3)
    #[cfg(feature = "parallel")]
    fn bfs_backward_parallel(&self, candidates: Option<&RoaringBitmap>, type_hash: u64, max_hops: usize) -> RoaringBitmap {
        use rayon::prelude::*;
        use std::sync::Mutex;
        
        let mut visited = RoaringBitmap::new();
        let mut frontier: RoaringBitmap = candidates.cloned().unwrap_or_default();
        
        for _ in 0..max_hops {
            if frontier.is_empty() { break; }
            
            // Collect frontier into vec for parallel processing
            let frontier_vec: Vec<u32> = frontier.iter().collect();
            
            // Parallel expansion of frontier
            let next_frontier: Mutex<RoaringBitmap> = Mutex::new(RoaringBitmap::new());
            let visited_local: Mutex<RoaringBitmap> = Mutex::new(RoaringBitmap::new());
            
            frontier_vec.into_par_iter().for_each(|idx| {
                visited_local.lock().unwrap().insert(idx);
                if let Some(edge_indices) = self.db.adj_rev.get(&idx) {
                    for &e_idx in edge_indices.iter() {
                        let edge = self.db.edges.read_at(e_idx as u64);
                        if edge.edge_type_hash == type_hash && edge.flags != 0 {
                            let mut nf = next_frontier.lock().unwrap();
                            nf.insert(edge.from_node);
                        }
                    }
                }
            });
            
            visited |= visited_local.into_inner().unwrap();
            frontier = next_frontier.into_inner().unwrap();
            frontier -= &visited;  // Remove already-visited nodes
        }
        visited |= frontier;
        visited
    }

    #[cfg(not(feature = "parallel"))]
    fn bfs_backward_parallel(&self, candidates: Option<&RoaringBitmap>, type_hash: u64, max_hops: usize) -> RoaringBitmap {
        // Fallback to sequential when parallel feature is disabled
        self.bfs_backward(candidates, type_hash, max_hops)
    }

    /// Convert pipeline to JSON format (round-trip serialization).
    pub fn to_json(&self) -> serde_json::Value {
        let mut steps: Vec<_> = self.steps.iter().map(|s| s.to_json()).collect();
        // Append post-bitmap ops so the JSON round-trips correctly.
        if let Some((ref field, asc)) = self.sort_by {
            steps.push(serde_json::json!({ "op": "sort", "field": field, "asc": asc }));
        }
        if self.skip_n > 0 {
            steps.push(serde_json::json!({ "op": "skip", "n": self.skip_n }));
        }
        if let Some(ref fields) = self.select_fields {
            steps.push(serde_json::json!({ "op": "select", "fields": fields }));
        }
        serde_json::json!({ "pipeline": steps })
    }

    /// Build a Set from a parsed step list, extracting Sort/Skip/Select into Set state.
    pub(crate) fn from_steps(db: &'db SekejapDB, steps: Vec<Step>) -> Self {
        let mut set = Self {
            db,
            steps: SmallVec::new(),
            sort_by: None,
            skip_n: 0,
            select_fields: None,
        };
        for step in steps {
            match step {
                Step::Sort(field, asc) => set.sort_by = Some((field, asc)),
                Step::Skip(n) => set.skip_n = n,
                Step::Select(fields) => set.select_fields = Some(fields),
                other => set.steps.push(other),
            }
        }
        set
    }
}
