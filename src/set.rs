use crate::db::SekejapDB;
use crate::index::TimeIndex;
use crate::index::PropertyIndex;
use crate::types::{AggOp, EdgeHit, Hit, Outcome, Plan, Step, StepReport, TimeQuery, Trace};
use roaring::RoaringBitmap;
use rstar::AABB;
use serde_json::Value;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;

#[cfg(feature = "fulltext")]
use crate::fulltext::{SearchHit, SearchOptions};

const SPATIAL_DIRECT_SCAN_THRESHOLD: u64 = 10_000;

pub struct Set<'db> {
    db: &'db SekejapDB,
    steps: SmallVec<[Step; 8]>,
    // Post-bitmap processing (applied at terminal time)
    sort_by: Option<(String, bool)>, // (field, ascending)
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
    let projected: serde_json::Map<String, Value> = fields
        .iter()
        .filter_map(|f| obj.get(f).map(|v| (f.clone(), v.clone())))
        .collect();
    serde_json::to_string(&projected).unwrap_or_else(|_| payload.to_string())
}

fn project_single_string_field_fast(payload: &[u8], field: &str) -> Option<String> {
    let field_start = find_bytes(payload, format!("\"{field}\":").as_bytes())?;
    let mut value_start = field_start + field.len() + 3;
    while value_start < payload.len() && payload[value_start].is_ascii_whitespace() {
        value_start += 1;
    }
    if value_start >= payload.len() || payload[value_start] != b'"' {
        return None;
    }
    let raw_start = value_start;
    let mut i = value_start + 1;
    let mut escaped = false;
    while i < payload.len() {
        let b = payload[i];
        if escaped {
            escaped = false;
            i += 1;
            continue;
        }
        match b {
            b'\\' => escaped = true,
            b'"' => {
                let raw_value = std::str::from_utf8(&payload[raw_start..=i]).ok()?;
                return Some(format!("{{\"{field}\":{raw_value}}}"));
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn extract_single_string_field_fast<'a>(payload: &'a [u8], field: &str) -> Option<&'a str> {
    let field_start = find_bytes(payload, format!("\"{field}\":").as_bytes())?;
    let mut value_start = field_start + field.len() + 3;
    while value_start < payload.len() && payload[value_start].is_ascii_whitespace() {
        value_start += 1;
    }
    if value_start >= payload.len() || payload[value_start] != b'"' {
        return None;
    }
    let raw_start = value_start + 1;
    let mut i = raw_start;
    let mut escaped = false;
    while i < payload.len() {
        let b = payload[i];
        if escaped {
            return None;
        }
        match b {
            b'\\' => escaped = true,
            b'"' => return std::str::from_utf8(&payload[raw_start..i]).ok(),
            _ => {}
        }
        i += 1;
    }
    None
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}

fn inject_score(payload: Option<String>, score: f32) -> Option<String> {
    let mut obj = payload
        .as_deref()
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    if !obj.is_object() {
        obj = serde_json::json!({ "_value": obj });
    }
    if let Some(map) = obj.as_object_mut() {
        map.insert("_score".to_string(), Value::from(score));
    }
    serde_json::to_string(&obj).ok()
}

fn point_in_polygon(lat: f32, lon: f32, polygon: &[[f32; 2]]) -> bool {
    if polygon.len() < 3 {
        return false;
    }
    let mut inside = false;
    let mut j = polygon.len() - 1;
    for i in 0..polygon.len() {
        let yi = polygon[i][0];
        let xi = polygon[i][1];
        let yj = polygon[j][0];
        let xj = polygon[j][1];
        let intersects = ((yi > lat) != (yj > lat))
            && (lon < (xj - xi) * (lat - yi) / ((yj - yi).max(f32::EPSILON)) + xi);
        if intersects {
            inside = !inside;
        }
        j = i;
    }
    inside
}

fn polygon_bbox(polygon: &[[f32; 2]]) -> Option<(f32, f32, f32, f32)> {
    if polygon.is_empty() {
        return None;
    }
    let (min_lat, max_lat, min_lon, max_lon) = polygon.iter().fold(
        (f32::MAX, f32::MIN, f32::MAX, f32::MIN),
        |(mn_la, mx_la, mn_lo, mx_lo), p| {
            (
                mn_la.min(p[0]),
                mx_la.max(p[0]),
                mn_lo.min(p[1]),
                mx_lo.max(p[1]),
            )
        },
    );
    Some((min_lat, max_lat, min_lon, max_lon))
}

fn polygon_is_axis_aligned_rectangle(polygon: &[[f32; 2]]) -> bool {
    if polygon.len() != 5 || polygon.first() != polygon.last() {
        return false;
    }
    let Some((min_lat, max_lat, min_lon, max_lon)) = polygon_bbox(polygon) else {
        return false;
    };
    let expected = [
        [min_lat, min_lon],
        [min_lat, max_lon],
        [max_lat, max_lon],
        [max_lat, min_lon],
    ];
    let corners = &polygon[..4];
    corners.iter().all(|p| expected.contains(p))
}

fn bytes_contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|window| window == needle)
}

fn payload_looks_like_point_geometry(bytes: &[u8]) -> bool {
    bytes_contains(bytes, br#""type":"Point""#)
        || bytes_contains(bytes, br#""type": "Point""#)
        || bytes_contains(bytes, br#""geo":{"loc""#)
        || bytes_contains(bytes, br#""coords":{"lat""#)
        || bytes_contains(bytes, br#""coordinates":{"lat""#)
}

fn spatial_scan_all_live_if_small<F>(
    db: &SekejapDB,
    limit: Option<u32>,
    mut predicate: F,
) -> Option<RoaringBitmap>
where
    F: FnMut(f32, f32) -> bool,
{
    let node_count = db.nodes.write_head.load(Ordering::Acquire);
    if node_count > SPATIAL_DIRECT_SCAN_THRESHOLD {
        return None;
    }
    let mut filtered = RoaringBitmap::new();
    for idx in 0..node_count {
        let slot = db.nodes.read_at(idx);
        if slot.flags == 0 {
            continue;
        }
        if predicate(slot.lat, slot.lon) {
            filtered.insert(idx as u32);
            if let Some(limit) = limit {
                if filtered.len() >= limit as u64 {
                    break;
                }
            }
        }
    }
    Some(filtered)
}

impl<'db> Set<'db> {
    pub(crate) fn new(db: &'db SekejapDB, starter: Step) -> Self {
        let mut steps = SmallVec::new();
        steps.push(starter);
        Self {
            db,
            steps,
            sort_by: None,
            skip_n: 0,
            select_fields: None,
        }
    }

    pub fn forward(mut self, edge_type: &str) -> Self {
        self.steps
            .push(Step::Forward(seahash::hash(edge_type.as_bytes())));
        self
    }
    pub fn backward(mut self, edge_type: &str) -> Self {
        self.steps
            .push(Step::Backward(seahash::hash(edge_type.as_bytes())));
        self
    }
    /// Parallel forward traversal using Rayon (for deep/hops > 3)
    pub fn forward_parallel(mut self, edge_type: &str) -> Self {
        self.steps
            .push(Step::ForwardParallel(seahash::hash(edge_type.as_bytes())));
        self
    }
    /// Parallel backward traversal using Rayon (for deep/hops > 3)
    pub fn backward_parallel(mut self, edge_type: &str) -> Self {
        self.steps
            .push(Step::BackwardParallel(seahash::hash(edge_type.as_bytes())));
        self
    }
    pub fn hops(mut self, n: u32) -> Self {
        self.steps.push(Step::Hops(n));
        self
    }
    pub fn leaves(mut self) -> Self {
        self.steps.push(Step::Leaves);
        self
    }
    pub fn roots(mut self) -> Self {
        self.steps.push(Step::Roots);
        self
    }

    pub fn near(mut self, lat: f32, lon: f32, radius_km: f32) -> Self {
        self.steps.push(Step::Near(lat, lon, radius_km));
        self
    }
    pub fn time_intersects(mut self, field: &str, query: TimeQuery) -> Self {
        self.steps.push(Step::TimeIntersects(field.to_string(), query));
        self
    }
    pub fn time_within(mut self, field: &str, query: TimeQuery) -> Self {
        self.steps.push(Step::TimeWithin(field.to_string(), query));
        self
    }
    pub fn time_near(mut self, field: &str, query: TimeQuery) -> Self {
        self.steps.push(Step::TimeNear(field.to_string(), query));
        self
    }
    pub fn within_bbox(
        mut self,
        min_lat: f32,
        min_lon: f32,
        max_lat: f32,
        max_lon: f32,
    ) -> Self {
        self.steps
            .push(Step::SpatialWithinBbox(min_lat, min_lon, max_lat, max_lon));
        self
    }
    /// Filter nodes whose geometry is completely within the query polygon.
    pub fn st_within(mut self, polygon: Vec<[f32; 2]>) -> Self {
        self.steps.push(Step::StWithin(polygon));
        self
    }
    /// Filter nodes whose geometry contains the query polygon.
    pub fn st_contains(mut self, polygon: Vec<[f32; 2]>) -> Self {
        self.steps.push(Step::StContains(polygon));
        self
    }
    /// Filter nodes whose geometry intersects the query polygon.
    pub fn st_intersects(mut self, polygon: Vec<[f32; 2]>) -> Self {
        self.steps.push(Step::StIntersects(polygon));
        self
    }
    /// Filter nodes whose centroid is within distance_km of a point (PostGIS st_dwithin).
    pub fn st_dwithin(mut self, lat: f32, lon: f32, distance_km: f32) -> Self {
        self.steps.push(Step::StDWithin(lat, lon, distance_km));
        self
    }
    pub fn similar(mut self, query: &[f32], k: usize) -> Self {
        self.steps.push(Step::Similar(query.to_vec(), k));
        self
    }
    #[cfg(feature = "fulltext")]
    pub fn matching(mut self, text: &str) -> Self {
        self.steps.push(Step::Matching {
            text: text.to_string(),
            limit: 1000,
            title_weight: 1.0,
            content_weight: 1.0,
        });
        self
    }
    #[cfg(feature = "fulltext")]
    pub fn matching_weighted(
        mut self,
        text: &str,
        limit: usize,
        title_weight: f32,
        content_weight: f32,
    ) -> Self {
        self.steps.push(Step::Matching {
            text: text.to_string(),
            limit,
            title_weight,
            content_weight,
        });
        self
    }

    pub fn where_eq(mut self, field: &str, value: serde_json::Value) -> Self {
        self.steps.push(Step::WhereEq(field.to_string(), value));
        self
    }
    pub fn where_between(mut self, field: &str, lo: f64, hi: f64) -> Self {
        self.steps
            .push(Step::WhereBetween(field.to_string(), lo, hi));
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
        self.steps
            .push(Step::WhereLte(field.to_string(), threshold));
        self
    }
    pub fn where_gte(mut self, field: &str, threshold: f64) -> Self {
        self.steps
            .push(Step::WhereGte(field.to_string(), threshold));
        self
    }
    pub fn like(mut self, field: &str, pattern: &str) -> Self {
        self.steps
            .push(Step::Like(field.to_string(), pattern.to_string(), false));
        self
    }
    pub fn ilike(mut self, field: &str, pattern: &str) -> Self {
        self.steps
            .push(Step::Like(field.to_string(), pattern.to_string(), true));
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

        let (bitmap, trace, score_map) = self.execute_pipeline()?;

        // Defer payload loading: only load eagerly if sort_by requires a JSON field.
        // Score-based sort and skip/limit only need indices + lat/lon, not payload.
        let needs_payload_for_sort = sort_by.as_ref().map_or(false, |(field, _)| {
            field != "_score" && field != "lat" && field != "lon"
        });
        let mut hits = self.db.resolve_hits(&bitmap, needs_payload_for_sort);

        if sort_by.is_none() && !score_map.is_empty() {
            hits.sort_unstable_by(|a, b| {
                let sa = score_map.get(&a.idx).copied().unwrap_or(0.0);
                let sb = score_map.get(&b.idx).copied().unwrap_or(0.0);
                sb.total_cmp(&sa)
            });
        } else if let Some((ref field, ascending)) = sort_by {
            let field = field.clone();
            hits.sort_unstable_by(|a, b| {
                let cmp = extract_sort_key(a, &field).cmp_key(&extract_sort_key(b, &field));
                if ascending {
                    cmp
                } else {
                    cmp.reverse()
                }
            });
        }

        if skip_n > 0 {
            hits.drain(..skip_n.min(hits.len()));
        }

        // Load payloads now for remaining hits (after sort + skip)
        if !needs_payload_for_sort {
            let fast_single_field = select_fields.as_ref().and_then(|fields| {
                if fields.len() == 1 {
                    match fields[0].as_str() {
                        "id" | "_id" => Some(fields[0].clone()),
                        _ => None,
                    }
                } else {
                    None
                }
            });
            for hit in &mut hits {
                let slot = self.db.nodes.read_at(hit.idx as u64);
                if slot.flags != 0 {
                    let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                    hit.payload = if let Some(ref field) = fast_single_field {
                        project_single_string_field_fast(bytes, field)
                            .or_else(|| Some(String::from_utf8_lossy(bytes).into_owned()))
                    } else {
                        Some(String::from_utf8_lossy(bytes).into_owned())
                    };
                }
            }
        }

        if let Some(ref fields) = select_fields {
            let already_fast_projected =
                fields.len() == 1 && matches!(fields[0].as_str(), "id" | "_id") && !needs_payload_for_sort;
            for hit in &mut hits {
                if already_fast_projected {
                    continue;
                }
                if let Some(ref payload) = hit.payload.clone() {
                    hit.payload = Some(project_fields(payload, fields));
                }
            }
        }
        for hit in &mut hits {
            if let Some(score) = score_map.get(&hit.idx).copied() {
                hit.score = Some(score);
                hit.payload = inject_score(hit.payload.clone(), score);
            }
        }

        Ok(Outcome { data: hits, trace })
    }

    /// Collect edges outgoing from the candidate set, including metadata.
    pub fn edge_collect(self) -> Result<Outcome<Vec<EdgeHit>>, Box<dyn std::error::Error>> {
        let (bitmap, trace, _) = self.execute_pipeline()?;
        let mut hits = Vec::new();

        for from_idx in bitmap.iter() {
            let from_slot = self.db.nodes.read_at(from_idx as u64);
            if from_slot.flags == 0 {
                continue;
            }

            if let Some(edge_indices) = self.db.adj_fwd.get(&from_idx) {
                for &e_idx in edge_indices.iter() {
                    let edge = self.db.edges.read_at(e_idx as u64);
                    if edge.flags == 0 {
                        continue;
                    }

                    let to_slot = self.db.nodes.read_at(edge.to_node as u64);

                    let meta = match edge.meta_kind {
                        1 if edge.meta_len > 0 => {
                            std::str::from_utf8(&edge.meta[..edge.meta_len as usize])
                                .ok()
                                .map(|s| s.to_string())
                        }
                        2 => {
                            let offset =
                                u64::from_le_bytes(edge.meta[..8].try_into().unwrap_or_default());
                            let len =
                                u32::from_le_bytes(edge.meta[8..12].try_into().unwrap_or_default());
                            if len > 0 {
                                let bytes = self.db.blobs.read(offset, len);
                                std::str::from_utf8(bytes).ok().map(|s| s.to_string())
                            } else {
                                None
                            }
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
        // Fast path: single Collection step → skip bitmap clone, use len_for() directly.
        if let [Step::Collection(hash)] = self.steps.as_slice() {
            let count = self.db.collection_bitmaps.len_for(*hash);
            let trace = Trace {
                steps: vec![StepReport {
                    atom: "collection".to_string(),
                    input_size: 0,
                    output_size: count,
                    index_used: "collection_bitmap".to_string(),
                    time_us: 0,
                }],
                total_us: 0,
            };
            return Ok(Outcome { data: count, trace });
        }
        let (bitmap, trace, _) = self.execute_pipeline()?;
        Ok(Outcome {
            data: bitmap.len() as usize,
            trace,
        })
    }

    pub fn first(self) -> Result<Outcome<Option<Hit>>, Box<dyn std::error::Error>> {
        let (bitmap, trace, score_map) = self.execute_pipeline()?;
        let hit = bitmap.iter().next().map(|idx| {
            let mut hit = self.db.resolve_single_hit(idx, true);
            if let Some(score) = score_map.get(&idx).copied() {
                hit.score = Some(score);
                hit.payload = inject_score(hit.payload.clone(), score);
            }
            hit
        });
        Ok(Outcome { data: hit, trace })
    }

    pub fn exists(self) -> Result<Outcome<bool>, Box<dyn std::error::Error>> {
        let (bitmap, trace, _) = self.execute_pipeline()?;
        Ok(Outcome {
            data: !bitmap.is_empty(),
            trace,
        })
    }

    pub fn avg(self, field: &str) -> Result<Outcome<f64>, Box<dyn std::error::Error>> {
        let (bitmap, trace, _) = self.execute_pipeline()?;
        let avg = self.db.aggregate_field(&bitmap, field, AggOp::Avg)?;
        Ok(Outcome { data: avg, trace })
    }

    pub fn sum(self, field: &str) -> Result<Outcome<f64>, Box<dyn std::error::Error>> {
        let (bitmap, trace, _) = self.execute_pipeline()?;
        let sum = self.db.aggregate_field(&bitmap, field, AggOp::Sum)?;
        Ok(Outcome { data: sum, trace })
    }

    pub fn explain(&self) -> Plan {
        Plan {
            steps: self.steps.to_vec(),
        }
    }

    fn execute_pipeline(
        &self,
    ) -> Result<(RoaringBitmap, Trace, HashMap<u32, f32>), Box<dyn std::error::Error>> {
        let mut trace = Trace {
            steps: Vec::new(),
            total_us: 0,
        };
        let total_start = Instant::now();
        let mut candidates: Option<RoaringBitmap> = None;
        let mut score_map: HashMap<u32, f32> = HashMap::new();
        let mut pending_hops: Option<u32> = None;

        for (step_idx, step) in self.steps.iter().enumerate() {
            let step_start = Instant::now();
            let input_size = candidates.as_ref().map_or(0, |b| b.len() as usize);
            let mut index_used = "scan";
            let next_take_limit = match self.steps.get(step_idx + 1) {
                Some(Step::Take(n)) => Some(*n as u32),
                _ => None,
            };

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
                    let count = self
                        .db
                        .nodes
                        .write_head
                        .load(std::sync::atomic::Ordering::Acquire);
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
                    candidates = Some(self.bfs_forward(
                        candidates.as_ref(),
                        *type_hash,
                        hops as usize,
                        next_take_limit,
                    ));
                    index_used = "adj_fwd";
                }
                Step::Backward(type_hash) => {
                    let hops = pending_hops.take().unwrap_or(1);
                    candidates = Some(self.bfs_backward(
                        candidates.as_ref(),
                        *type_hash,
                        hops as usize,
                        next_take_limit,
                    ));
                    index_used = "adj_rev";
                }
                Step::ForwardParallel(type_hash) => {
                    let hops = pending_hops.take().unwrap_or(1);
                    candidates = Some(self.bfs_forward_parallel(
                        candidates.as_ref(),
                        *type_hash,
                        hops as usize,
                        next_take_limit,
                    ));
                    index_used = "adj_fwd_parallel";
                }
                Step::BackwardParallel(type_hash) => {
                    let hops = pending_hops.take().unwrap_or(1);
                    candidates = Some(self.bfs_backward_parallel(
                        candidates.as_ref(),
                        *type_hash,
                        hops as usize,
                        next_take_limit,
                    ));
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
                        Some(ref mut curr) if curr.len() < SPATIAL_DIRECT_SCAN_THRESHOLD => {
                            let mut filtered = RoaringBitmap::new();
                            for idx in curr.iter() {
                                let slot = self.db.nodes.read_at(idx as u64);
                                let dx = slot.lat - lat;
                                let dy = slot.lon - lon;
                                if dx * dx + dy * dy <= r_sq {
                                    filtered.insert(idx);
                                    if let Some(limit) = next_take_limit {
                                        if filtered.len() >= limit as u64 {
                                            break;
                                        }
                                    }
                                }
                            }
                            *curr = filtered;
                            index_used = "filter";
                        }
                        None => {
                            if let Some(results) =
                                spatial_scan_all_live_if_small(self.db, next_take_limit, |slot_lat, slot_lon| {
                                    let dx = slot_lat - lat;
                                    let dy = slot_lon - lon;
                                    dx * dx + dy * dy <= r_sq
                                })
                            {
                                candidates = Some(results);
                                index_used = "filter_all";
                            } else {
                                let results: RoaringBitmap = self
                                    .db
                                    .spatial
                                    .read()
                                    .locate_within_distance([*lat, *lon], r_sq)
                                    .take(next_take_limit.unwrap_or(u32::MAX) as usize)
                                    .map(|n| n.id)
                                    .collect();
                                candidates = Some(results);
                                index_used = "rtree";
                            }
                        }
                        _ => {
                            let results: RoaringBitmap = self
                                .db
                                .spatial
                                .read()
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
                Step::TimeIntersects(field, query) => {
                    if let Some(time_idx) = self.db.field_time_indexes.get(field) {
                        let hits: RoaringBitmap =
                            time_idx.lookup_intersects(query).into_iter().collect();
                        if let Some(ref mut curr) = candidates {
                            *curr &= hits;
                        } else {
                            candidates = Some(hits);
                        }
                        index_used = "temporal_index";
                    } else if let Some(ref mut bm) = candidates {
                        let mut filtered = RoaringBitmap::new();
                        for idx in bm.iter() {
                            let slot = self.db.nodes.read_at(idx as u64);
                            if slot.flags == 0 {
                                continue;
                            }
                            let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                                if TimeIndex::payload_intersects(&json, field, query) {
                                    filtered.insert(idx);
                                }
                            }
                        }
                        *bm = filtered;
                        index_used = "temporal_payload";
                    }
                }
                Step::TimeWithin(field, query) => {
                    if let Some(time_idx) = self.db.field_time_indexes.get(field) {
                        let hits: RoaringBitmap =
                            time_idx.lookup_within(query).into_iter().collect();
                        if let Some(ref mut curr) = candidates {
                            *curr &= hits;
                        } else {
                            candidates = Some(hits);
                        }
                        index_used = "temporal_index";
                    } else if let Some(ref mut bm) = candidates {
                        let mut filtered = RoaringBitmap::new();
                        for idx in bm.iter() {
                            let slot = self.db.nodes.read_at(idx as u64);
                            if slot.flags == 0 {
                                continue;
                            }
                            let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                                if TimeIndex::payload_within(&json, field, query) {
                                    filtered.insert(idx);
                                }
                            }
                        }
                        *bm = filtered;
                        index_used = "temporal_payload";
                    }
                }
                Step::TimeNear(field, query) => {
                    if let Some(time_idx) = self.db.field_time_indexes.get(field) {
                        let hits: RoaringBitmap =
                            time_idx.lookup_near(query).into_iter().collect();
                        if let Some(ref mut curr) = candidates {
                            *curr &= hits;
                        } else {
                            candidates = Some(hits);
                        }
                        index_used = "temporal_index";
                    } else if let Some(ref mut bm) = candidates {
                        let mut filtered = RoaringBitmap::new();
                        for idx in bm.iter() {
                            let slot = self.db.nodes.read_at(idx as u64);
                            if slot.flags == 0 {
                                continue;
                            }
                            let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                                if TimeIndex::payload_near(&json, field, query) {
                                    filtered.insert(idx);
                                }
                            }
                        }
                        *bm = filtered;
                        index_used = "temporal_payload";
                    }
                }
                Step::SpatialWithinBbox(min_lat, min_lon, max_lat, max_lon)
                | Step::SpatialIntersectsBbox(min_lat, min_lon, max_lat, max_lon) => {
                    let env = AABB::from_corners([*min_lat, *min_lon], [*max_lat, *max_lon]);
                    match candidates {
                        Some(ref mut curr) if curr.len() < SPATIAL_DIRECT_SCAN_THRESHOLD => {
                            let mut filtered = RoaringBitmap::new();
                            for idx in curr.iter() {
                                let slot = self.db.nodes.read_at(idx as u64);
                                if slot.lat >= *min_lat
                                    && slot.lat <= *max_lat
                                    && slot.lon >= *min_lon
                                    && slot.lon <= *max_lon
                                {
                                    filtered.insert(idx);
                                    if let Some(limit) = next_take_limit {
                                        if filtered.len() >= limit as u64 {
                                            break;
                                        }
                                    }
                                }
                            }
                            *curr = filtered;
                            index_used = "filter";
                        }
                        None => {
                            if let Some(results) =
                                spatial_scan_all_live_if_small(self.db, next_take_limit, |slot_lat, slot_lon| {
                                    slot_lat >= *min_lat
                                        && slot_lat <= *max_lat
                                        && slot_lon >= *min_lon
                                        && slot_lon <= *max_lon
                                })
                            {
                                candidates = Some(results);
                                index_used = "filter_all";
                            } else {
                                let results: RoaringBitmap = self
                                    .db
                                    .spatial
                                    .read()
                                    .locate_in_envelope_intersecting(&env)
                                    .take(next_take_limit.unwrap_or(u32::MAX) as usize)
                                    .map(|n| n.id)
                                    .collect();
                                candidates = Some(results);
                                index_used = "rtree";
                            }
                        }
                        _ => {
                            let results: RoaringBitmap = self
                                .db
                                .spatial
                                .read()
                                .locate_in_envelope_intersecting(&env)
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
                Step::SpatialWithinPolygon(polygon) => {
                    if polygon.len() < 3 {
                        candidates = Some(RoaringBitmap::new());
                        index_used = "spatial_polygon";
                        continue;
                    }

                    let (min_lat, max_lat, min_lon, max_lon) = polygon.iter().fold(
                        (f32::MAX, f32::MIN, f32::MAX, f32::MIN),
                        |(mn_la, mx_la, mn_lo, mx_lo), p| {
                            (
                                mn_la.min(p[0]),
                                mx_la.max(p[0]),
                                mn_lo.min(p[1]),
                                mx_lo.max(p[1]),
                            )
                        },
                    );
                    let env = AABB::from_corners([min_lat, min_lon], [max_lat, max_lon]);
                    let mut filtered = RoaringBitmap::new();
                    match candidates {
                        Some(ref curr) => {
                            for idx in curr.iter() {
                                let slot = self.db.nodes.read_at(idx as u64);
                                if point_in_polygon(slot.lat, slot.lon, polygon) {
                                    filtered.insert(idx);
                                }
                            }
                        }
                        None => {
                            for n in self.db.spatial.read().locate_in_envelope_intersecting(&env) {
                                let slot = self.db.nodes.read_at(n.id as u64);
                                if point_in_polygon(slot.lat, slot.lon, polygon) {
                                    filtered.insert(n.id);
                                }
                            }
                        }
                    }
                    candidates = Some(filtered);
                    index_used = "spatial_polygon";
                }

                // ── DE-9IM geometry predicates ──────────────────────────────
                Step::StWithin(polygon) | Step::StContains(polygon) | Step::StIntersects(polygon) => {
                    let predicate: fn(&serde_json::Value, &[[f32; 2]]) -> bool = match step {
                        Step::StWithin(_) => crate::geometry::geom_within_polygon,
                        Step::StContains(_) => crate::geometry::geom_contains_polygon,
                        _ => crate::geometry::geom_intersects_polygon,
                    };
                    if polygon.len() < 3 {
                        candidates = Some(RoaringBitmap::new());
                        index_used = "geometry";
                        continue;
                    }
                    // R-Tree pre-filter on envelope
                    let (min_lat, max_lat, min_lon, max_lon) = polygon_bbox(polygon)
                        .expect("validated non-empty polygon");
                    let point_rectangle_fast_path = matches!(step, Step::StWithin(_) | Step::StIntersects(_))
                        && polygon_is_axis_aligned_rectangle(polygon);
                    let env = AABB::from_corners([min_lat, min_lon], [max_lat, max_lon]);
                    let mut filtered = RoaringBitmap::new();
                    let test_node = |idx: u32, db: &SekejapDB, polygon: &[[f32; 2]]| -> bool {
                        let slot = db.nodes.read_at(idx as u64);
                        if slot.flags == 0 { return false; }
                        if point_rectangle_fast_path {
                            return slot.lat >= min_lat
                                && slot.lat <= max_lat
                                && slot.lon >= min_lon
                                && slot.lon <= max_lon;
                        }
                        let bytes = db.blobs.read(slot.blob_offset, slot.blob_len);
                        if matches!(step, Step::StWithin(_) | Step::StIntersects(_))
                            && payload_looks_like_point_geometry(bytes)
                        {
                            return point_in_polygon(slot.lat, slot.lon, polygon);
                        }
                        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                            predicate(&json, polygon)
                        } else {
                            false
                        }
                    };
                    match candidates {
                        Some(ref curr) => {
                            for idx in curr.iter() {
                                if test_node(idx, self.db, polygon) {
                                    filtered.insert(idx);
                                    if let Some(limit) = next_take_limit {
                                        if filtered.len() >= limit as u64 {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            if let Some(results) = spatial_scan_all_live_if_small(
                                self.db,
                                next_take_limit,
                                |slot_lat, slot_lon| {
                                    slot_lat >= min_lat
                                        && slot_lat <= max_lat
                                        && slot_lon >= min_lon
                                        && slot_lon <= max_lon
                                        && if point_rectangle_fast_path {
                                            true
                                        } else {
                                            point_in_polygon(slot_lat, slot_lon, polygon)
                                        }
                                },
                            ) {
                                filtered = results;
                                index_used = if point_rectangle_fast_path {
                                    "filter_all_bbox"
                                } else {
                                    "filter_all_polygon"
                                };
                            } else {
                                for n in self
                                    .db
                                    .spatial
                                    .read()
                                    .locate_in_envelope_intersecting(&env)
                                {
                                    if test_node(n.id, self.db, polygon) {
                                        filtered.insert(n.id);
                                        if let Some(limit) = next_take_limit {
                                            if filtered.len() >= limit as u64 {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    candidates = Some(filtered);
                    index_used = "geometry";
                }

                Step::StDWithin(lat, lon, distance_km) => {
                    // Uses centroid distance (slot.lat/lon) — same as Near but named for PostGIS familiarity
                    let r_sq = distance_km * distance_km;
                    match candidates {
                        Some(ref mut curr) if curr.len() < SPATIAL_DIRECT_SCAN_THRESHOLD => {
                            let mut filtered = RoaringBitmap::new();
                            for idx in curr.iter() {
                                let slot = self.db.nodes.read_at(idx as u64);
                                let dx = slot.lat - lat;
                                let dy = slot.lon - lon;
                                if dx * dx + dy * dy <= r_sq {
                                    filtered.insert(idx);
                                    if let Some(limit) = next_take_limit {
                                        if filtered.len() >= limit as u64 {
                                            break;
                                        }
                                    }
                                }
                            }
                            *curr = filtered;
                            index_used = "filter";
                        }
                        None => {
                            if let Some(results) =
                                spatial_scan_all_live_if_small(self.db, next_take_limit, |slot_lat, slot_lon| {
                                    let dx = slot_lat - lat;
                                    let dy = slot_lon - lon;
                                    dx * dx + dy * dy <= r_sq
                                })
                            {
                                candidates = Some(results);
                                index_used = "filter_all";
                            } else {
                                let results: RoaringBitmap = self
                                    .db
                                    .spatial
                                    .read()
                                    .locate_within_distance([*lat, *lon], r_sq)
                                    .take(next_take_limit.unwrap_or(u32::MAX) as usize)
                                    .map(|n| n.id)
                                    .collect();
                                candidates = Some(results);
                                index_used = "rtree";
                            }
                        }
                        _ => {
                            let results: RoaringBitmap = self
                                .db
                                .spatial
                                .read()
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
                Step::Matching {
                    text,
                    limit,
                    title_weight,
                    content_weight,
                } => {
                    if let Some(ref ft) = *self.db.fulltext.read() {
                        let opts = SearchOptions {
                            title_weight: *title_weight,
                            content_weight: *content_weight,
                        };
                        let slug_hashes = ft.search(text, *limit, Some(&opts)).unwrap_or_default();
                        let mut bm = RoaringBitmap::new();
                        {
                            let slug_r = self.db.slug_index.read();
                            for SearchHit { id, score } in slug_hashes {
                                if let Some(idx) = slug_r.get(id) {
                                    bm.insert(idx);
                                    score_map.insert(idx, score);
                                }
                            }
                        }
                        if let Some(ref mut curr) = candidates {
                            *curr &= bm.clone();
                            score_map.retain(|idx, _| curr.contains(*idx));
                        } else {
                            candidates = Some(bm);
                        }
                        index_used = "fulltext";
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
                            if slot.flags == 0 {
                                continue;
                            }
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
                        let hits: RoaringBitmap = range_idx
                            .lookup_range(
                                &serde_json::Value::from(*lo),
                                &serde_json::Value::from(*hi),
                            )
                            .into_iter()
                            .collect();
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
                            if slot.flags == 0 {
                                continue;
                            }
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
                        let hits: RoaringBitmap = range_idx
                            .lookup_range(
                                &serde_json::Value::from(*threshold + f64::EPSILON),
                                &serde_json::Value::from(f64::MAX),
                            )
                            .into_iter()
                            .collect();
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
                            if slot.flags == 0 {
                                continue;
                            }
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
                            if slot.flags == 0 {
                                continue;
                            }
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
                Step::Like(field, pattern, case_insensitive) => {
                    if let Some(ref mut bm) = candidates {
                        let mut filtered = RoaringBitmap::new();
                        for idx in bm.iter() {
                            let slot = self.db.nodes.read_at(idx as u64);
                            if slot.flags == 0 {
                                continue;
                            }
                            let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                            if let Some(value) = extract_single_string_field_fast(bytes, field) {
                                if like_matches_fast(value, pattern, *case_insensitive) {
                                    filtered.insert(idx);
                                }
                                continue;
                            }
                            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                                if let Some(value) = payload_field_text(&json, field) {
                                    if like_matches_fast(value, pattern, *case_insensitive) {
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
                    let (other_bm, _, _) = other_set.execute_pipeline()?;
                    if let Some(ref mut curr) = candidates {
                        *curr &= other_bm;
                    } else {
                        candidates = Some(other_bm);
                    }
                    index_used = "intersect";
                }
                Step::Union(other_steps) => {
                    let other_set = Set::from_steps(self.db, other_steps.clone());
                    let (other_bm, _, _) = other_set.execute_pipeline()?;
                    if let Some(ref mut curr) = candidates {
                        *curr |= other_bm;
                    } else {
                        candidates = Some(other_bm);
                    }
                    index_used = "union";
                }
                Step::Subtract(other_steps) => {
                    let other_set = Set::from_steps(self.db, other_steps.clone());
                    let (other_bm, _, _) = other_set.execute_pipeline()?;
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
                        let hits: RoaringBitmap = range_idx
                            .lookup_range(
                                &serde_json::Value::from(f64::MIN),
                                &serde_json::Value::from(*threshold - f64::EPSILON),
                            )
                            .into_iter()
                            .collect();
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
                            if slot.flags == 0 {
                                continue;
                            }
                            let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                                if let Some(num) = json.get(field).and_then(|v| v.as_f64()) {
                                    if num < *threshold {
                                        filtered.insert(idx);
                                    }
                                }
                            }
                        }
                        *bm = filtered;
                        index_used = "payload";
                    }
                }
                Step::WhereLte(field, threshold) => {
                    if let Some(range_idx) = self.db.field_range_indexes.get(field) {
                        let hits: RoaringBitmap = range_idx
                            .lookup_range(
                                &serde_json::Value::from(f64::MIN),
                                &serde_json::Value::from(*threshold),
                            )
                            .into_iter()
                            .collect();
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
                            if slot.flags == 0 {
                                continue;
                            }
                            let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                                if let Some(num) = json.get(field).and_then(|v| v.as_f64()) {
                                    if num <= *threshold {
                                        filtered.insert(idx);
                                    }
                                }
                            }
                        }
                        *bm = filtered;
                        index_used = "payload";
                    }
                }
                Step::WhereGte(field, threshold) => {
                    if let Some(range_idx) = self.db.field_range_indexes.get(field) {
                        let hits: RoaringBitmap = range_idx
                            .lookup_range(
                                &serde_json::Value::from(*threshold),
                                &serde_json::Value::from(f64::MAX),
                            )
                            .into_iter()
                            .collect();
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
                            if slot.flags == 0 {
                                continue;
                            }
                            let bytes = self.db.blobs.read(slot.blob_offset, slot.blob_len);
                            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(bytes) {
                                if let Some(num) = json.get(field).and_then(|v| v.as_f64()) {
                                    if num >= *threshold {
                                        filtered.insert(idx);
                                    }
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

        let mut canonical = RoaringBitmap::new();
        let slug_r = self.db.slug_index.read();
        for idx in candidates.unwrap_or_default().iter() {
            let slot = self.db.nodes.read_at(idx as u64);
            if slot.flags == 0 {
                continue;
            }
            if slug_r.get(slot.slug_hash) == Some(idx) {
                canonical.insert(idx);
            }
        }

        trace.total_us = total_start.elapsed().as_micros() as u64;
        Ok((canonical, trace, score_map))
    }

    fn bfs_forward(
        &self,
        candidates: Option<&RoaringBitmap>,
        type_hash: u64,
        max_hops: usize,
        limit: Option<u32>,
    ) -> RoaringBitmap {
        let mut visited = RoaringBitmap::new();
        let mut frontier: RoaringBitmap = candidates.cloned().unwrap_or_default();

        for _ in 0..max_hops {
            if frontier.is_empty() {
                break;
            }
            let mut next_frontier = RoaringBitmap::new();
            for idx in frontier.iter() {
                visited.insert(idx);
                if let Some(edge_indices) = self.db.adj_fwd.get(&idx) {
                    for &e_idx in edge_indices.iter() {
                        let edge = self.db.edges.read_at(e_idx as u64);
                        if edge.edge_type_hash == type_hash
                            && edge.flags != 0
                            && !visited.contains(edge.to_node)
                        {
                            next_frontier.insert(edge.to_node);
                            if let Some(limit) = limit {
                                if visited.len() + next_frontier.len() >= limit as u64 {
                                    visited |= next_frontier;
                                    return visited;
                                }
                            }
                        }
                    }
                }
            }
            frontier = next_frontier;
        }
        visited |= frontier;
        visited
    }

    fn bfs_backward(
        &self,
        candidates: Option<&RoaringBitmap>,
        type_hash: u64,
        max_hops: usize,
        limit: Option<u32>,
    ) -> RoaringBitmap {
        let mut visited = RoaringBitmap::new();
        let mut frontier: RoaringBitmap = candidates.cloned().unwrap_or_default();

        for _ in 0..max_hops {
            if frontier.is_empty() {
                break;
            }
            let mut next_frontier = RoaringBitmap::new();
            for idx in frontier.iter() {
                visited.insert(idx);
                if let Some(edge_indices) = self.db.adj_rev.get(&idx) {
                    for &e_idx in edge_indices.iter() {
                        let edge = self.db.edges.read_at(e_idx as u64);
                        if edge.edge_type_hash == type_hash
                            && edge.flags != 0
                            && !visited.contains(edge.from_node)
                        {
                            next_frontier.insert(edge.from_node);
                            if let Some(limit) = limit {
                                if visited.len() + next_frontier.len() >= limit as u64 {
                                    visited |= next_frontier;
                                    return visited;
                                }
                            }
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
    fn bfs_forward_parallel(
        &self,
        candidates: Option<&RoaringBitmap>,
        type_hash: u64,
        max_hops: usize,
        limit: Option<u32>,
    ) -> RoaringBitmap {
        if limit.is_some() {
            return self.bfs_forward(candidates, type_hash, max_hops, limit);
        }
        use rayon::prelude::*;
        use std::sync::Mutex;

        let mut visited = RoaringBitmap::new();
        let mut frontier: RoaringBitmap = candidates.cloned().unwrap_or_default();

        for _ in 0..max_hops {
            if frontier.is_empty() {
                break;
            }

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
            frontier -= &visited; // Remove already-visited nodes
        }
        visited |= frontier;
        visited
    }

    #[cfg(not(feature = "parallel"))]
    fn bfs_forward_parallel(
        &self,
        candidates: Option<&RoaringBitmap>,
        type_hash: u64,
        max_hops: usize,
        limit: Option<u32>,
    ) -> RoaringBitmap {
        // Fallback to sequential when parallel feature is disabled
        self.bfs_forward(candidates, type_hash, max_hops, limit)
    }

    /// Parallel backward BFS using Rayon - faster for deep traversals (hops > 3)
    #[cfg(feature = "parallel")]
    fn bfs_backward_parallel(
        &self,
        candidates: Option<&RoaringBitmap>,
        type_hash: u64,
        max_hops: usize,
        limit: Option<u32>,
    ) -> RoaringBitmap {
        if limit.is_some() {
            return self.bfs_backward(candidates, type_hash, max_hops, limit);
        }
        use rayon::prelude::*;
        use std::sync::Mutex;

        let mut visited = RoaringBitmap::new();
        let mut frontier: RoaringBitmap = candidates.cloned().unwrap_or_default();

        for _ in 0..max_hops {
            if frontier.is_empty() {
                break;
            }

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
            frontier -= &visited; // Remove already-visited nodes
        }
        visited |= frontier;
        visited
    }

    #[cfg(not(feature = "parallel"))]
    fn bfs_backward_parallel(
        &self,
        candidates: Option<&RoaringBitmap>,
        type_hash: u64,
        max_hops: usize,
        limit: Option<u32>,
    ) -> RoaringBitmap {
        // Fallback to sequential when parallel feature is disabled
        self.bfs_backward(candidates, type_hash, max_hops, limit)
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
    /// Also optimizes the pipeline:
    /// - removes redundant `All` before seedable search steps
    /// - reorders unanchored retrieval filters so sharper seeds run before vague time
    pub fn from_steps(db: &'db SekejapDB, steps: Vec<Step>) -> Self {
        let mut set = Self {
            db,
            steps: SmallVec::new(),
            sort_by: None,
            skip_n: 0,
            select_fields: None,
        };
        let mut normalized: Vec<Step> = Vec::new();
        let mut iter = steps.into_iter().peekable();
        while let Some(step) = iter.next() {
            match step {
                Step::Sort(field, asc) => set.sort_by = Some((field, asc)),
                Step::Skip(n) => set.skip_n = n,
                Step::Select(fields) => set.select_fields = Some(fields),
                Step::All => {
                    // Drop All when the next step can seed candidates directly.
                    let next_is_direct_seed = iter.peek().map_or(false, is_direct_seed_step);
                    if !next_is_direct_seed {
                        normalized.push(Step::All);
                    }
                }
                other => normalized.push(other),
            }
        }
        reorder_unanchored_seed_steps(&mut normalized);
        set.steps.extend(normalized);
        set
    }
}

fn is_graph_step(step: &Step) -> bool {
    matches!(
        step,
        Step::Forward(..)
            | Step::Backward(..)
            | Step::ForwardParallel(..)
            | Step::BackwardParallel(..)
            | Step::Hops(..)
            | Step::Leaves
            | Step::Roots
    )
}

fn is_anchored_start(step: &Step) -> bool {
    matches!(step, Step::One(..) | Step::Many(..))
}

fn seed_priority(step: &Step) -> usize {
    match step {
        Step::WhereEq(..) => 0,
        Step::WhereBetween(..) | Step::WhereGt(..) | Step::WhereLt(..) | Step::WhereGte(..) | Step::WhereLte(..) | Step::WhereIn(..) => 1,
        Step::Near(..)
        | Step::StDWithin(..)
        | Step::SpatialWithinBbox(..)
        | Step::SpatialIntersectsBbox(..)
        | Step::SpatialWithinPolygon(..)
        | Step::StWithin(..)
        | Step::StContains(..)
        | Step::StIntersects(..) => 2,
        #[cfg(feature = "fulltext")]
        Step::Matching { .. } => 3,
        Step::Similar(..) => 4,
        Step::TimeIntersects(..) | Step::TimeWithin(..) | Step::TimeNear(..) => 5,
        Step::Like(..) => 6,
        _ => usize::MAX,
    }
}

fn can_reorder_seed(step: &Step) -> bool {
    seed_priority(step) != usize::MAX
}

fn reorder_unanchored_seed_steps(steps: &mut Vec<Step>) {
    if steps.is_empty() || is_anchored_start(&steps[0]) {
        return;
    }

    let start = if matches!(steps.first(), Some(Step::Collection(..) | Step::All)) {
        1
    } else {
        0
    };

    let mut end = start;
    while end < steps.len() && !is_graph_step(&steps[end]) && can_reorder_seed(&steps[end]) {
        end += 1;
    }
    if end - start <= 1 {
        return;
    }

    let mut indexed: Vec<(usize, Step)> = steps[start..end]
        .iter()
        .cloned()
        .enumerate()
        .collect();
    indexed.sort_by_key(|(idx, step)| (seed_priority(step), *idx));
    for (offset, (_, step)) in indexed.into_iter().enumerate() {
        steps[start + offset] = step;
    }
}

fn is_direct_seed_step(step: &Step) -> bool {
    match step {
        Step::Near(..)
        | Step::TimeIntersects(..)
        | Step::TimeWithin(..)
        | Step::TimeNear(..)
        | Step::SpatialWithinBbox(..)
        | Step::SpatialIntersectsBbox(..)
        | Step::SpatialWithinPolygon(..)
        | Step::StWithin(..)
        | Step::StContains(..)
        | Step::StIntersects(..)
        | Step::StDWithin(..)
        | Step::WhereEq(..)
        | Step::WhereBetween(..)
        | Step::WhereGt(..)
        | Step::WhereLt(..)
        | Step::WhereGte(..)
        | Step::WhereLte(..)
        | Step::WhereIn(..)
        | Step::Similar(..) => true,
        #[cfg(feature = "fulltext")]
        Step::Matching { .. } => true,
        _ => false,
    }
}

fn payload_field_text<'a>(json: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    let key = field.rsplit('.').next().unwrap_or(field);
    json.get(key).and_then(|value| value.as_str())
}

fn like_matches(value: &str, pattern: &str, case_insensitive: bool) -> bool {
    let value_chars: Vec<char> = if case_insensitive {
        value.to_lowercase().chars().collect()
    } else {
        value.chars().collect()
    };
    let pattern_chars: Vec<char> = if case_insensitive {
        pattern.to_lowercase().chars().collect()
    } else {
        pattern.chars().collect()
    };

    let mut v = 0usize;
    let mut p = 0usize;
    let mut star: Option<usize> = None;
    let mut match_idx = 0usize;

    while v < value_chars.len() {
        if p < pattern_chars.len() && (pattern_chars[p] == '_' || pattern_chars[p] == value_chars[v]) {
            p += 1;
            v += 1;
        } else if p < pattern_chars.len() && pattern_chars[p] == '%' {
            star = Some(p);
            p += 1;
            match_idx = v;
        } else if let Some(star_pos) = star {
            p = star_pos + 1;
            match_idx += 1;
            v = match_idx;
        } else {
            return false;
        }
    }

    while p < pattern_chars.len() && pattern_chars[p] == '%' {
        p += 1;
    }

    p == pattern_chars.len()
}

enum LikePatternKind<'a> {
    Exact(&'a str),
    Prefix(&'a str),
    Suffix(&'a str),
    Contains(&'a str),
    Generic,
}

fn classify_like_pattern(pattern: &str) -> LikePatternKind<'_> {
    if pattern.contains('_') {
        return LikePatternKind::Generic;
    }
    let percent_count = pattern.as_bytes().iter().filter(|&&b| b == b'%').count();
    match percent_count {
        0 => LikePatternKind::Exact(pattern),
        1 if pattern.ends_with('%') => LikePatternKind::Prefix(&pattern[..pattern.len() - 1]),
        1 if pattern.starts_with('%') => LikePatternKind::Suffix(&pattern[1..]),
        2 if pattern.starts_with('%') && pattern.ends_with('%') => {
            let inner = &pattern[1..pattern.len() - 1];
            if inner.contains('%') {
                LikePatternKind::Generic
            } else {
                LikePatternKind::Contains(inner)
            }
        }
        _ => LikePatternKind::Generic,
    }
}

fn like_matches_fast(value: &str, pattern: &str, case_insensitive: bool) -> bool {
    match classify_like_pattern(pattern) {
        LikePatternKind::Generic => like_matches(value, pattern, case_insensitive),
        LikePatternKind::Exact(needle) => simple_text_match(value, needle, case_insensitive, "exact"),
        LikePatternKind::Prefix(needle) => {
            simple_text_match(value, needle, case_insensitive, "prefix")
        }
        LikePatternKind::Suffix(needle) => {
            simple_text_match(value, needle, case_insensitive, "suffix")
        }
        LikePatternKind::Contains(needle) => {
            simple_text_match(value, needle, case_insensitive, "contains")
        }
    }
}

fn simple_text_match(value: &str, needle: &str, case_insensitive: bool, mode: &str) -> bool {
    if case_insensitive {
        let value_l = value.to_lowercase();
        let needle_l = needle.to_lowercase();
        match mode {
            "exact" => value_l == needle_l,
            "prefix" => value_l.starts_with(&needle_l),
            "suffix" => value_l.ends_with(&needle_l),
            "contains" => value_l.contains(&needle_l),
            _ => false,
        }
    } else {
        match mode {
            "exact" => value == needle,
            "prefix" => value.starts_with(needle),
            "suffix" => value.ends_with(needle),
            "contains" => value.contains(needle),
            _ => false,
        }
    }
}
