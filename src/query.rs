//! Chainable query builder and executor.

use crate::{sk_hash, CoreDB, FieldKey};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

// ── Result types ──────────────────────────────────────────────────────────────

/// A resolved node returned from `.collect()`.
#[derive(Debug, Clone)]
pub struct Hit {
    pub slug: String,
    pub slug_hash: u64,
    /// Full payload, or projected subset if `.select()` was used.
    pub payload: Option<Value>,
}

// ── VecMetric ─────────────────────────────────────────────────────────────────

/// Which vector distance metric to use.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum VecMetric {
    /// Cosine distance (`<=>`, `VECTOR_COSINE`). Lower = more similar.
    Cosine,
    /// Squared Euclidean distance (`<->`, `VECTOR_L2`). Lower = closer.
    L2,
    /// Inner product (`<#>`, `VECTOR_DOT`). Higher = more similar (negated for sort).
    Dot,
    /// Manhattan / taxicab distance (`<+>`, `VECTOR_L1`). Lower = closer.
    L1,
}

// ── ScoreExpr ─────────────────────────────────────────────────────────────────

/// An arithmetic score expression used in `ORDER BY` scoring.
///
/// Evaluates to an `f64` per node. Designed for weighted multi-signal ranking:
///
/// ```sql
/// ORDER BY BM25(title, 'rust') * 0.7 + BM25(body, 'rust') * 0.3 DESC
/// ORDER BY VECTOR_COSINE(embedding, [0.1, 0.2]) * 0.5 + popularity * 0.5 DESC
/// ```
#[derive(Clone, Debug)]
pub enum ScoreExpr {
    /// Numeric literal, e.g. `0.7`.
    Lit(f64),
    /// Payload field coerced to `f64` (absent or non-numeric → 0.0).
    Field(String),
    /// BM25 relevance score: `BM25(field, 'query')`.
    Bm25 { field: String, query: String },
    /// Cosine similarity (1 − cosine distance): `VECTOR_COSINE(field, [vec])`.
    VectorCosine { field: String, query: Vec<f32> },
    /// Squared Euclidean distance: `VECTOR_L2(field, [vec])`.
    VectorL2 { field: String, query: Vec<f32> },
    /// Inner product: `VECTOR_DOT(field, [vec])`. Higher = more similar.
    VectorDot { field: String, query: Vec<f32> },
    /// Manhattan distance: `VECTOR_L1(field, [vec])`. Lower = closer.
    VectorL1 { field: String, query: Vec<f32> },
    /// Great-circle distance in km: `ST_DISTANCE_KM(field, POINT(lon lat))`.
    ///
    /// Returns the Haversine distance from the node's geometry to the given point.
    /// Absent or non-GeoJSON fields → `f64::MAX` (very far away).
    StDistance { field: String, lat: f64, lon: f64 },
    /// `a + b`.
    Add(Box<ScoreExpr>, Box<ScoreExpr>),
    /// `a - b`.
    Sub(Box<ScoreExpr>, Box<ScoreExpr>),
    /// `a * b`.
    Mul(Box<ScoreExpr>, Box<ScoreExpr>),
    /// `a / b` — division by zero yields `0.0`.
    Div(Box<ScoreExpr>, Box<ScoreExpr>),
    /// `-a`.
    Neg(Box<ScoreExpr>),
}

// ── Step ──────────────────────────────────────────────────────────────────────

/// A single pipeline step.
///
/// Steps are accumulated in `Set` and executed lazily on `.collect()` / `.count()`.
#[derive(Clone, Debug)]
pub enum Step {
    // ── Starters (always the first step) ──────────────────────────────────────
    /// Resolve a single node by slug hash.
    One(u64),
    /// Resolve a list of nodes by slug hashes.
    Many(Vec<u64>),
    /// All nodes in a named collection (hash of the collection name).
    Collection(u64),
    /// Every node in the database.
    All,

    // ── Graph traversal ───────────────────────────────────────────────────────
    /// Follow outgoing edges of the given type.
    Forward(u64),
    /// Follow incoming edges of the given type.
    Backward(u64),
    /// BFS up to N hops forward over any edge type.
    Hops(u32),
    /// Typed BFS: follow only edges matching `type_hash`, collect at depths `min..=max`.
    HopsTyped {
        type_hash: u64,
        min_depth: u32,
        max_depth: u32,
    },
    /// Filter: only traverse edges whose strength >= threshold (applied after Forward/Backward).
    MinStrength(f32),
    /// Keep only nodes with no outgoing edges.
    Leaves,
    /// Keep only nodes with no incoming edges.
    Roots,

    // ── Payload filters ───────────────────────────────────────────────────────
    WhereEq(String, Value),
    WhereNeq(String, Value),
    WhereGt(String, f64),
    WhereLt(String, f64),
    WhereGte(String, f64),
    WhereLte(String, f64),
    WhereBetween(String, f64, f64),
    WhereIn(String, Vec<Value>),
    /// Substring match. Third param: `true` = case-insensitive (ILIKE).
    Like(String, String, bool),

    // ── Spatial filters ───────────────────────────────────────────────────
    /// Centroid within `distance_km` of `(lat, lon)`. Uses Haversine.
    StDWithin(f64, f64, f64),
    /// Node geometry contains query point (reverse geocoding).
    StContainsPoint(f64, f64),
    /// Node geometry completely within query polygon. Ring: `[[lat, lon], ...]`.
    StWithin(Vec<[f64; 2]>),
    /// Node geometry contains query polygon.
    StContains(Vec<[f64; 2]>),
    /// Node geometry intersects query polygon.
    StIntersects(Vec<[f64; 2]>),
    /// Geometry distance to point < max_km.
    StDistance(String, f64, f64, f64),
    /// Geometry length (LineString) > min_km.
    StLength(String, f64),
    /// Geometry area (Polygon) > min_km2.
    StArea(String, f64),

    // ── Vector similarity ──────────────────────────────────────────────────
    /// Brute-force top-k cosine similarity search over a named vector field.
    VectorNear {
        field: String,
        query: Vec<f32>,
        k: usize,
    },

    // ── BM25 full-text filter ──────────────────────────────────────────────
    /// BM25 score > min_score on field.
    Bm25Filter(String, String, f64),
    /// Sort by BM25 score (field, query, ascending).
    Bm25Sort(String, String, bool),
    /// Add BM25 score column to result (field, query).
    Bm25Score(String, String),

    // ── Null / logical ────────────────────────────────────────────────────────
    /// `field IS NULL` (negated=false) or `IS NOT NULL` (negated=true).
    WhereIsNull(String, bool),
    /// Negate an inner filter step: `NOT <step>`.
    WhereNot(Box<Step>),
    /// OR of AND-groups: each inner Vec is one AND branch.
    WhereOr(Vec<Vec<Step>>),

    // ── Set algebra ───────────────────────────────────────────────────────────
    Intersect(Vec<Step>),
    Union(Vec<Step>),
    Subtract(Vec<Step>),

    // ── Grouping / dedup ──────────────────────────────────────────────────────────
    /// Partition candidates by these field keys; collect() produces one Hit per group.
    GroupBy(Vec<String>),
    /// HAVING conditions applied after grouping (evaluated in collect()).
    Having(Vec<Step>),
    /// Deduplicate results by projected payload (applied in collect()).
    Distinct,

    // ── Ordering / pagination / projection ────────────────────────────────────
    /// Multi-column sort. Columns applied left-to-right; ties broken by next column.
    Sort(Vec<(String, bool)>), // (field, ascending) — evaluated in order
    /// Sort by vector distance (ascending — nearest first; Dot negated so higher = first).
    SortByVector { field: String, query: Vec<f32>, metric: VecMetric },
    /// Sort by an arithmetic score expression (highest score first by default).
    ///
    /// `ascending = false` (default for scores): highest score → first result.
    /// `ascending = true`: lowest score → first result.
    SortByExpr { expr: ScoreExpr, ascending: bool },
    Skip(usize),
    Take(usize),
    /// Project only these fields in the returned payload.
    Select(Vec<String>),
}

// ── Set ───────────────────────────────────────────────────────────────────────

/// Chainable, lazy query builder. Execute with `.collect()`, `.count()`, etc.
pub struct Set<'db> {
    db: &'db CoreDB,
    pub(crate) steps: Vec<Step>,
    /// Pre-computed hits (for aggregate MATCH — bypasses the step executor).
    pub(crate) precomputed: Option<Vec<Hit>>,
}

impl<'db> Set<'db> {
    pub(crate) fn new(db: &'db CoreDB, starter: Step) -> Self {
        Self {
            db,
            steps: vec![starter],
            precomputed: None,
        }
    }

    /// Build a Set from a pre-constructed step list (useful for serialisation / Python bindings).
    pub fn from_steps(db: &'db CoreDB, steps: Vec<Step>) -> Self {
        Self { db, steps, precomputed: None }
    }

    /// Build a Set wrapping pre-computed hits (used for aggregate MATCH results).
    pub(crate) fn from_hits(db: &'db CoreDB, hits: Vec<Hit>) -> Self {
        Self { db, steps: Vec::new(), precomputed: Some(hits) }
    }

    // ── Graph traversal ───────────────────────────────────────────────────────

    pub fn forward(mut self, edge_type: &str) -> Self {
        self.steps.push(Step::Forward(sk_hash(edge_type)));
        self
    }

    pub fn backward(mut self, edge_type: &str) -> Self {
        self.steps.push(Step::Backward(sk_hash(edge_type)));
        self
    }

    /// Filter traversal results to only nodes reached via edges with strength >= threshold.
    /// Place this after `.forward()` or `.backward()`.
    pub fn min_strength(mut self, threshold: f32) -> Self {
        self.steps.push(Step::MinStrength(threshold));
        self
    }

    /// BFS expansion: follow forward edges up to `n` hops (any type).
    pub fn hops(mut self, n: u32) -> Self {
        self.steps.push(Step::Hops(n));
        self
    }

    /// Typed BFS: follow only `edge_type` edges up to `max_depth` hops.
    ///
    /// Equivalent to the MATCH `(a)-[:edge_type*1..max_depth]->(b)` clause.
    /// Unlike [`hops`], the source node is **not** included in the result.
    pub fn hops_typed(mut self, edge_type: &str, max_depth: u32) -> Self {
        self.steps.push(Step::HopsTyped {
            type_hash: sk_hash(edge_type),
            min_depth: 1,
            max_depth,
        });
        self
    }

    /// Keep only nodes with no outgoing edges.
    pub fn leaves(mut self) -> Self {
        self.steps.push(Step::Leaves);
        self
    }

    /// Keep only nodes with no incoming edges.
    pub fn roots(mut self) -> Self {
        self.steps.push(Step::Roots);
        self
    }

    // ── Payload filters ───────────────────────────────────────────────────────

    pub fn where_eq(mut self, field: &str, value: impl Into<Value>) -> Self {
        self.steps
            .push(Step::WhereEq(field.to_string(), value.into()));
        self
    }

    pub fn where_neq(mut self, field: &str, value: impl Into<Value>) -> Self {
        self.steps
            .push(Step::WhereNeq(field.to_string(), value.into()));
        self
    }

    pub fn where_gt(mut self, field: &str, threshold: f64) -> Self {
        self.steps.push(Step::WhereGt(field.to_string(), threshold));
        self
    }

    pub fn where_lt(mut self, field: &str, threshold: f64) -> Self {
        self.steps.push(Step::WhereLt(field.to_string(), threshold));
        self
    }

    pub fn where_gte(mut self, field: &str, threshold: f64) -> Self {
        self.steps
            .push(Step::WhereGte(field.to_string(), threshold));
        self
    }

    pub fn where_lte(mut self, field: &str, threshold: f64) -> Self {
        self.steps
            .push(Step::WhereLte(field.to_string(), threshold));
        self
    }

    pub fn where_between(mut self, field: &str, lo: f64, hi: f64) -> Self {
        self.steps
            .push(Step::WhereBetween(field.to_string(), lo, hi));
        self
    }

    pub fn where_in(mut self, field: &str, values: Vec<Value>) -> Self {
        self.steps.push(Step::WhereIn(field.to_string(), values));
        self
    }

    /// Case-sensitive substring filter.
    pub fn like(mut self, field: &str, pattern: &str) -> Self {
        self.steps
            .push(Step::Like(field.to_string(), pattern.to_string(), false));
        self
    }

    /// Case-insensitive substring filter (ILIKE).
    pub fn ilike(mut self, field: &str, pattern: &str) -> Self {
        self.steps
            .push(Step::Like(field.to_string(), pattern.to_string(), true));
        self
    }

    // ── Spatial filters ───────────────────────────────────────────────────

    /// Keep nodes whose centroid is within `distance_km` of `(lat, lon)`.
    pub fn st_dwithin(mut self, lat: f64, lon: f64, distance_km: f64) -> Self {
        self.steps.push(Step::StDWithin(lat, lon, distance_km));
        self
    }

    /// Alias for [`st_dwithin`](Self::st_dwithin).
    pub fn near(self, lat: f64, lon: f64, radius_km: f64) -> Self {
        self.st_dwithin(lat, lon, radius_km)
    }

    /// Keep nodes whose geometry contains the query point.
    pub fn st_contains_point(mut self, lat: f64, lon: f64) -> Self {
        self.steps.push(Step::StContainsPoint(lat, lon));
        self
    }

    /// Keep nodes whose geometry is completely within the query polygon.
    pub fn st_within(mut self, ring: Vec<[f64; 2]>) -> Self {
        self.steps.push(Step::StWithin(ring));
        self
    }

    /// Keep nodes whose geometry contains the query polygon.
    pub fn st_contains(mut self, ring: Vec<[f64; 2]>) -> Self {
        self.steps.push(Step::StContains(ring));
        self
    }

    /// Keep nodes whose geometry intersects the query polygon.
    pub fn st_intersects(mut self, ring: Vec<[f64; 2]>) -> Self {
        self.steps.push(Step::StIntersects(ring));
        self
    }

    // ── Vector similarity ──────────────────────────────────────────────────

    /// Return the top-k nodes by cosine similarity to `query` in the named vector field.
    ///
    /// Acts as a STARTER when no prior steps have produced candidates, otherwise
    /// re-ranks the existing candidate set. Results are sorted ascending by cosine
    /// distance (lower = more similar).
    pub fn vector_near(mut self, field: &str, query: Vec<f32>, k: usize) -> Self {
        self.steps
            .push(Step::VectorNear { field: field.to_string(), query, k });
        self
    }

    // ── BM25 full-text filter ──────────────────────────────────────────────

    /// Keep nodes where BM25 score on `field` for `query` exceeds `min_score`.
    pub fn bm25_filter(mut self, field: &str, query: &str, min_score: f64) -> Self {
        self.steps.push(Step::Bm25Filter(
            field.to_string(),
            query.to_string(),
            min_score,
        ));
        self
    }

    // ── Set algebra ───────────────────────────────────────────────────────────

    pub fn intersect(mut self, other: Set<'_>) -> Self {
        self.steps.push(Step::Intersect(other.steps));
        self
    }

    pub fn union(mut self, other: Set<'_>) -> Self {
        self.steps.push(Step::Union(other.steps));
        self
    }

    pub fn subtract(mut self, other: Set<'_>) -> Self {
        self.steps.push(Step::Subtract(other.steps));
        self
    }

    // ── Shaping ───────────────────────────────────────────────────────────────

    /// Sort by a single field.
    pub fn sort(self, field: &str, ascending: bool) -> Self {
        self.sort_multi(vec![(field.to_string(), ascending)])
    }

    /// Sort by multiple columns, evaluated left-to-right (ties broken by the next column).
    pub fn sort_multi(mut self, columns: Vec<(String, bool)>) -> Self {
        self.steps.push(Step::Sort(columns));
        self
    }

    pub fn skip(mut self, n: usize) -> Self {
        self.steps.push(Step::Skip(n));
        self
    }

    pub fn take(mut self, n: usize) -> Self {
        self.steps.push(Step::Take(n));
        self
    }

    /// Project only these payload fields in the result.
    pub fn select(mut self, fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.steps
            .push(Step::Select(fields.into_iter().map(Into::into).collect()));
        self
    }

    // ── Execute ───────────────────────────────────────────────────────────────

    /// Resolve nodes and the edge that connected them to the previous step.
    ///
    /// Use after `.forward(kind)` or `.backward(kind)` to get `(destination_hit, edge_hit)` pairs.
    ///
    /// ```
    /// # use sekejap::CoreDB;
    /// let mut db = CoreDB::new();
    /// db.put("a", "{}").unwrap();
    /// db.put("b", "{}").unwrap();
    /// db.link("a", "b", "rel", 0.9);
    /// let pairs = db.one("a").forward("rel").edge_collect();
    /// assert_eq!(pairs[0].0.slug, "b");
    /// assert!((pairs[0].1.strength - 0.9).abs() < 1e-6);
    /// ```
    pub fn edge_collect(self) -> Vec<(Hit, crate::EdgeHit)> {
        // Find the last Forward or Backward step to determine edge type and direction.
        let last_traversal = self
            .steps
            .iter()
            .enumerate()
            .rev()
            .find_map(|(i, s)| match s {
                Step::Forward(h) => Some((i, *h, true)),
                Step::Backward(h) => Some((i, *h, false)),
                _ => None,
            });
        let (trav_idx, type_h, is_forward) = match last_traversal {
            Some(x) => x,
            None => return vec![],
        };

        // Run steps up to the traversal to get source nodes.
        let sources: std::collections::HashSet<u64> = execute(self.db, &self.steps[..trav_idx])
            .into_iter()
            .collect();

        // Run all steps to get final destination nodes.
        let dests = execute(self.db, &self.steps);

        let db = self.db;
        dests
            .into_iter()
            .filter_map(|dest_h| {
                let dest_node = db.node_data(dest_h)?;
                // Find an edge from a source node to this dest (or vice versa for backward).
                let edge = if is_forward {
                    // Look in rev_edges of dest for a source
                    db.rev_edges(dest_h)?
                        .iter()
                        .find(|e| e.edge_type == type_h && sources.contains(&e.other))
                        .map(|e| crate::EdgeHit {
                            from_slug: db.node_data(e.other).map(|n| n.slug.clone()),
                            to_slug: Some(dest_node.slug.clone()),
                            edge_type: db.resolve_edge_type(e.edge_type),
                            edge_type_hash: e.edge_type,
                            strength: e.strength,
                            meta: e.meta.clone(),
                        })
                } else {
                    // Backward: look in fwd_edges of dest for a source
                    db.fwd_edges(dest_h)?
                        .iter()
                        .find(|e| e.edge_type == type_h && sources.contains(&e.other))
                        .map(|e| crate::EdgeHit {
                            from_slug: Some(dest_node.slug.clone()),
                            to_slug: db.node_data(e.other).map(|n| n.slug.clone()),
                            edge_type: db.resolve_edge_type(e.edge_type),
                            edge_type_hash: e.edge_type,
                            strength: e.strength,
                            meta: e.meta.clone(),
                        })
                }?;
                let hit = Hit {
                    slug: dest_node.slug.clone(),
                    slug_hash: dest_h,
                    payload: db.get_payload(dest_h),
                };
                Some((hit, edge))
            })
            .collect()
    }
}

/// Return the output JSON key name for a field expression.
///
/// Rules (in priority order):
/// - `__AS__alias\x01inner`  → `alias`
/// - `__AGG__FUNC__field`    → lowercase function name (`count`, `sum`, …)
/// - `__JP_TEXT__a__b`       → last path segment (`b`)
/// - `__JP_OBJ__a__b`        → last path segment (`b`)
/// - anything else           → the expression itself
fn field_output_key(expr: &str) -> String {
    if let Some(rest) = expr.strip_prefix("__AS__") {
        // __AS__alias\x01inner  → alias
        if let Some(alias) = rest.split('\x01').next() {
            return alias.to_string();
        }
    }
    if let Some(rest) = expr.strip_prefix("__AGG__") {
        // __AGG__FUNC__field  → lowercase(FUNC)
        let func = rest.split("__").next().unwrap_or("agg");
        return func.to_lowercase();
    }
    if expr.starts_with("__JP_TEXT__") || expr.starts_with("__JP_OBJ__") {
        return expr.rsplit("__").next().unwrap_or(expr).to_string();
    }
    // ST_AsGeoJSON(field) — default output key is the inner field name.
    // e.g. __ST_AsGeoJSON__geometry → "geometry"
    if let Some(field) = expr.strip_prefix("__ST_AsGeoJSON__") {
        return field.to_string();
    }
    expr.to_string()
}

/// Extract the raw (unaliased) field name from an encoded SELECT expression.
///
/// Strips `__AS__alias\x01` if present and returns the inner expression.
/// For plain fields this is a no-op.  Used by the GROUP BY executor to look up
/// a non-aggregate field's value in the group's uniform field map.
fn field_inner_name(expr: &str) -> &str {
    if let Some(rest) = expr.strip_prefix("__AS__") {
        if let Some(idx) = rest.find('\x01') {
            return &rest[idx + 1..];
        }
    }
    expr
}

/// Check whether a stored JSON value matches any entry in an IN-list.
///
/// Falls back to f64 comparison when the stored value is a JSON integer but the
/// query value was parsed as a float (or vice-versa), which happens when data is
/// inserted via `db.put(r#"{"v":1}"#)` (integer) but queried via SQL `IN (1, 2)`
/// (tokeniser always yields f64).
fn value_in(stored: &serde_json::Value, candidates: &[serde_json::Value]) -> bool {
    if candidates.contains(stored) {
        return true;
    }
    if let Some(n) = stored.as_f64() {
        return candidates.iter().any(|c| c.as_f64() == Some(n));
    }
    false
}

/// Resolve a field name (or encoded JSON path) against a node payload.
///
/// Handles three cases:
/// - `__JP_TEXT__seg1__seg2__…` — navigate path, coerce final value to `Value::String`
/// - `__JP_OBJ__seg1__seg2__…`  — navigate path, return value as-is
/// - anything else              — plain `payload[field]` lookup (cloned)
pub(crate) fn json_path_get(field: &str, payload: &serde_json::Value) -> Option<serde_json::Value> {
    if let Some(path) = field.strip_prefix("__JP_TEXT__") {
        let keys: Vec<&str> = path.split("__").collect();
        let last_idx = keys.len().saturating_sub(1);
        let mut cur = payload;
        for (i, key) in keys.iter().enumerate() {
            cur = cur.get(*key)?;
            if i == last_idx {
                // Coerce final value to a JSON string (mirrors PostgreSQL ->>).
                return Some(match cur {
                    serde_json::Value::String(s) => serde_json::Value::String(s.clone()),
                    other => serde_json::Value::String(other.to_string()),
                });
            }
        }
        None
    } else if let Some(path) = field.strip_prefix("__JP_OBJ__") {
        let keys: Vec<&str> = path.split("__").collect();
        let mut cur = payload;
        for key in &keys {
            cur = cur.get(*key)?;
        }
        Some(cur.clone())
    } else {
        payload.get(field).cloned()
    }
}

fn eval_field_expr(expr: &str, payload: &serde_json::Value) -> Option<serde_json::Value> {
    // AS alias: __AS__alias\x01inner  — strip alias, evaluate inner expression.
    if let Some(rest) = expr.strip_prefix("__AS__") {
        if let Some(idx) = rest.find('\x01') {
            let inner = &rest[idx + 1..];
            return eval_field_expr(inner, payload);
        }
    }
    // Aggregate functions are not per-node — skip here; collect() handles them.
    if expr.starts_with("__AGG__") {
        return None;
    }
    // JSON path operators (-> / ->>) — handled first.
    if expr.starts_with("__JP_TEXT__") || expr.starts_with("__JP_OBJ__") {
        return json_path_get(expr, payload);
    }
    if expr.starts_with("__ST_Centroid__") {
        let _geom_field = expr.strip_prefix("__ST_Centroid__")?;
        if let Some(centroid) = crate::geo::extract_centroid(payload) {
            let point = serde_json::json!({
                "type": "Point",
                "coordinates": [centroid.1, centroid.0]
            });
            return Some(point);
        }
        return None;
    }
    // ST_AsGeoJSON(field) — serialise the named geometry field to a GeoJSON
    // text string, matching PostGIS ST_AsGeoJSON() semantics.
    //
    // Since sekejap stores geometry as a native JSON object in the payload,
    // this is simply a re-serialisation to `Value::String`.  The caller gets
    // a string like `"{\"type\":\"Point\",\"coordinates\":[144.96,-37.81]}"`.
    //
    // If the field is absent or the geometry cannot be serialised, `None` is
    // returned and the column is omitted from the result row.
    if let Some(field) = expr.strip_prefix("__ST_AsGeoJSON__") {
        let geom = payload.get(field)?;
        let s = serde_json::to_string(geom).ok()?;
        return Some(Value::String(s));
    }
    if expr.starts_with("__FUNC__") {
        let rest = expr.strip_prefix("__FUNC__")?;
        let parts: Vec<&str> = rest.split("__").collect();
        if parts.len() < 2 {
            return None;
        }
        let func_name = parts[0];
        let payload_map = payload.as_object()?;
        let args: Vec<serde_json::Value> = parts[1..]
            .iter()
            .map(|s| {
                if let Some(v) = payload_map.get(*s) {
                    v.clone()
                } else {
                    serde_json::Value::String(s.to_string())
                }
            })
            .collect();
        return Some(crate::scalar::eval_scalar_func(
            func_name,
            &args,
            payload_map,
        ));
    }
    payload.get(expr).cloned()
}

// ── Aggregation helpers ───────────────────────────────────────────────────────

/// If `expr` is (or wraps via `__AS__`) an aggregate expression (`__AGG__…`),
/// return the raw `__AGG__…` string; otherwise return `None`.
fn agg_inner(expr: &str) -> Option<&str> {
    if let Some(rest) = expr.strip_prefix("__AS__") {
        if let Some(idx) = rest.find('\x01') {
            let inner = &rest[idx + 1..];
            if inner.starts_with("__AGG__") {
                return Some(inner);
            }
        }
        return None;
    }
    if expr.starts_with("__AGG__") {
        return Some(expr);
    }
    None
}

/// Running accumulator for a single aggregate column.
struct AggAccum {
    func: String,
    arg: String,
    all_count: usize,
    sum: f64,
    count_notnull: usize,
    min: Option<f64>,
    max: Option<f64>,
}

impl AggAccum {
    fn new(func: &str, arg: &str) -> Self {
        Self {
            func: func.to_uppercase(),
            arg: arg.to_string(),
            all_count: 0,
            sum: 0.0,
            count_notnull: 0,
            min: None,
            max: None,
        }
    }

    fn push(&mut self, payload: &Value) {
        self.all_count += 1;
        if self.arg == "*" {
            return;
        }
        if let Some(f) = payload.get(&self.arg).and_then(|v| v.as_f64()) {
            self.count_notnull += 1;
            self.sum += f;
            self.min = Some(self.min.map_or(f, |m: f64| m.min(f)));
            self.max = Some(self.max.map_or(f, |m: f64| m.max(f)));
        }
    }

    fn finalize(&self) -> Value {
        match self.func.as_str() {
            "COUNT" => {
                let n = if self.arg == "*" { self.all_count } else { self.count_notnull };
                Value::Number(serde_json::Number::from(n as i64))
            }
            "SUM" => serde_json::json!(self.sum),
            "AVG" => {
                if self.count_notnull > 0 {
                    serde_json::json!(self.sum / self.count_notnull as f64)
                } else {
                    Value::Null
                }
            }
            "MIN" => self.min.map(|v| serde_json::json!(v)).unwrap_or(Value::Null),
            "MAX" => self.max.map(|v| serde_json::json!(v)).unwrap_or(Value::Null),
            _ => Value::Null,
        }
    }
}

impl<'db> Set<'db> {
    /// Attempt an index-only GROUP BY scan.
    ///
    /// Returns `Some(hits)` when the entire query can be answered from the btree
    /// without reading any payload bytes.  Returns `None` to fall through to the
    /// normal payload-scan path.
    ///
    /// Preconditions checked here (all must hold):
    /// - Exactly one GROUP BY field.
    /// - Every SELECT expression is either that field or `COUNT(*)`.
    /// - No HAVING, no SKIP, no additional filter steps after Collection.
    /// - The step chain is `[Collection(hash)]` (possibly preceded by nothing).
    /// - A btree index exists for `(collection_hash, field)`.
    fn try_index_only_group_by(
        &self,
        group_fields: &[String],
        select_fields: &Option<Vec<String>>,
    ) -> Option<Vec<Hit>> {
        // Only single-field GROUP BY for now.
        if group_fields.len() != 1 {
            return None;
        }
        let gf = &group_fields[0];

        // Verify SELECT: every column is either the group field or COUNT(*).
        // If no SELECT clause, fall through (SELECT * needs payloads).
        let fields = select_fields.as_deref()?;
        if fields.is_empty() {
            return None;
        }
        let mut has_count = false;
        for f in fields {
            let out = field_inner_name(f);
            if let Some(agg) = agg_inner(f) {
                let rest = agg.strip_prefix("__AGG__").unwrap_or(agg);
                let func = rest.splitn(2, "__").next().unwrap_or("").to_uppercase();
                if func == "COUNT" {
                    has_count = true;
                } else {
                    return None; // SUM/AVG/MIN/MAX need payloads
                }
            } else if out != gf.as_str() {
                return None; // Non-agg field that isn't the GROUP BY key
            }
        }
        if !has_count {
            return None; // No aggregation — pure GROUP BY without count makes no sense here
        }

        // No HAVING, no SKIP — these require payload evaluation.
        let has_having = self.steps.iter().any(|s| matches!(s, Step::Having(_)));
        let has_skip   = self.steps.iter().any(|s| matches!(s, Step::Skip(_)));
        if has_having || has_skip {
            return None;
        }

        // Step chain must be exactly: [Collection(hash)] with no payload-filter steps.
        // Allow Sort/Take/Distinct/Select/GroupBy after Collection — those are handled here.
        // Reject any filter step (WhereEq, WhereBetween, Like, etc.) because those
        // would change which hashes are included and the btree has all of them.
        let collection_hash = {
            let mut ch: Option<u64> = None;
            for s in &self.steps {
                match s {
                    Step::Collection(h) => {
                        if ch.is_some() { return None; } // two collections — give up
                        ch = Some(*h);
                    }
                    // Projection/shaping steps are fine — we apply them ourselves.
                    Step::Select(_) | Step::GroupBy(_) | Step::Sort(_)
                    | Step::Take(_) | Step::Distinct => {}
                    // Any filter or traversal means the hash set is not the full collection.
                    _ => return None,
                }
            }
            ch?
        };

        // Look up the btree for this (collection, field) pair.
        let btree = self.db.field_index(collection_hash, gf)?;

        // Build result rows from btree entries — zero disk reads.
        let sort_step: Option<&Vec<(String, bool)>> = self.steps.iter().find_map(|s| {
            if let Step::Sort(cols) = s { Some(cols) } else { None }
        });
        let take_n = self.steps.iter().find_map(|s| if let Step::Take(n) = s { Some(*n) } else { None });

        // Collect (group_value, count) pairs from the btree.
        let mut rows: Vec<(Value, u64)> = btree
            .iter()
            .map(|(key, hashes)| (CoreDB::field_key_to_value(key), hashes.len() as u64))
            .collect();

        // Apply ORDER BY if present (sort by count alias or group field).
        if let Some(sort_cols) = sort_step {
            // Find the COUNT alias in the select fields.
            let count_alias = fields.iter().find_map(|f| {
                if agg_inner(f).is_some() { Some(field_output_key(f)) } else { None }
            });
            rows.sort_by(|(av, ac), (bv, bc)| {
                let mut ord = std::cmp::Ordering::Equal;
                for (col, asc) in sort_cols {
                    ord = if Some(col.as_str()) == count_alias.as_deref() {
                        ac.cmp(bc)
                    } else {
                        // Sort by group field value.
                        let sa = serde_json::to_string(av).unwrap_or_default();
                        let sb = serde_json::to_string(bv).unwrap_or_default();
                        sa.cmp(&sb)
                    };
                    if !asc { ord = ord.reverse(); }
                    if ord != std::cmp::Ordering::Equal { break; }
                }
                ord
            });
        }

        // Apply LIMIT.
        if let Some(n) = take_n {
            rows.truncate(n);
        }

        // Build Hit payloads.
        let count_alias = fields.iter().find_map(|f| {
            if agg_inner(f).is_some() { Some(field_output_key(f)) } else { None }
        });
        let group_alias = fields.iter().find_map(|f| {
            if agg_inner(f).is_none() { Some((field_inner_name(f).to_string(), field_output_key(f))) } else { None }
        });

        let hits = rows.into_iter().map(|(val, cnt)| {
            let mut map = serde_json::Map::new();
            if let Some(ref alias) = count_alias {
                map.insert(alias.clone(), serde_json::json!(cnt));
            }
            if let Some((ref _inner, ref alias)) = group_alias {
                map.insert(alias.clone(), val);
            }
            Hit { slug: String::new(), slug_hash: 0, payload: Some(Value::Object(map)) }
        }).collect();

        Some(hits)
    }

    pub fn collect(self) -> Vec<Hit> {
        // Short-circuit for pre-computed aggregate results.
        if let Some(hits) = self.precomputed {
            return hits;
        }

        let select_fields: Option<Vec<String>> = self.steps.iter().find_map(|s| {
            if let Step::Select(f) = s {
                Some(f.clone())
            } else {
                None
            }
        });

        let bm25_scores: Option<(String, String, HashMap<u64, f64>)> =
            self.steps.iter().find_map(|s| {
                if let Step::Bm25Score(field, query) = s {
                    let all_scores = self.db.bm25_indexes.get(field).map(|idx| {
                        idx.search(query, 10000)
                            .into_iter()
                            .map(|h| (h.doc_id, h.score))
                            .collect::<HashMap<_, _>>()
                    });
                    all_scores.map(|scores| (field.clone(), query.clone(), scores))
                } else {
                    None
                }
            });

        // ── GROUP BY mode ─────────────────────────────────────────────────────
        // Single-pass: fetch each node's payload exactly once, extract the group
        // key, and accumulate aggregates inline — no intermediate Vec<u64> per group.
        let group_by_fields: Option<Vec<String>> = self.steps.iter().find_map(|s| {
            if let Step::GroupBy(f) = s { Some(f.clone()) } else { None }
        });

        if let Some(ref group_fields) = group_by_fields {
            // ── Index-only scan fast path ──────────────────────────────────────
            // Conditions: single GROUP BY field + all SELECT exprs are that field
            // or COUNT(*) + a btree index exists + no HAVING/SKIP + source is a
            // single named collection (not All / Many / complex filter chain).
            //
            // When true, the btree already holds `value → Vec<hashes>` — we just
            // iterate its entries and take `len()` as the count. Zero disk reads.
            if let Some(hits) = self.try_index_only_group_by(group_fields, &select_fields) {
                return hits;
            }

            let hashes = execute(self.db, &self.steps);
            let fields = select_fields.as_deref().unwrap_or(&[]);
            let having_steps: Vec<&[Step]> = self.steps.iter().filter_map(|s| {
                if let Step::Having(inner) = s { Some(inner.as_slice()) } else { None }
            }).collect();
            let sort_step: Option<&Vec<(String, bool)>> = self.steps.iter().find_map(|s| {
                if let Step::Sort(cols) = s { Some(cols) } else { None }
            });
            let skip_n = self.steps.iter().find_map(|s| if let Step::Skip(n) = s { Some(*n) } else { None });
            let take_n = self.steps.iter().find_map(|s| if let Step::Take(n) = s { Some(*n) } else { None });
            let distinct = self.steps.iter().any(|s| matches!(s, Step::Distinct));

            /// Per-group accumulation state.
            struct GroupState {
                /// Running aggregate accumulators, keyed by output alias.
                accums: HashMap<String, AggAccum>,
                /// Uniform values of the GROUP BY fields (same for every member).
                group_vals: HashMap<String, Value>,
                /// Full payload of the first node seen — used only for `SELECT *`.
                first_payload: Option<Value>,
            }

            let mut group_order: Vec<String> = Vec::new();
            let mut groups: HashMap<String, GroupState> = HashMap::new();

            for &h in &hashes {
                if let Some(payload) = self.db.get_payload(h) {
                    // Build composite group key — one JSON-encoded segment per GROUP BY field.
                    let key = group_fields.iter()
                        .map(|f| serde_json::to_string(
                            payload.get(f).unwrap_or(&Value::Null)
                        ).unwrap_or_default())
                        .collect::<Vec<_>>()
                        .join("\x00");

                    if !groups.contains_key(&key) {
                        group_order.push(key.clone());
                        let mut gv = HashMap::new();
                        for gf in group_fields {
                            if let Some(v) = payload.get(gf) {
                                gv.insert(gf.clone(), v.clone());
                            }
                        }
                        groups.insert(key.clone(), GroupState {
                            accums: HashMap::new(),
                            group_vals: gv,
                            first_payload: Some(payload.clone()),
                        });
                    }
                    let state = groups.get_mut(&key).unwrap();

                    // Accumulate aggregate expressions for this node.
                    for f in fields {
                        if let Some(agg_expr) = agg_inner(f) {
                            let out_key = field_output_key(f);
                            let rest = agg_expr.strip_prefix("__AGG__").unwrap_or(agg_expr);
                            let mut parts = rest.splitn(2, "__");
                            let func = parts.next().unwrap_or("COUNT").to_uppercase();
                            let arg = parts.next().unwrap_or("*");
                            let acc = state.accums
                                .entry(out_key)
                                .or_insert_with(|| AggAccum::new(&func, arg));
                            acc.push(&payload);
                        }
                    }
                }
            }

            let mut results: Vec<Hit> = group_order.into_iter().filter_map(|key| {
                let state = groups.remove(&key)?;

                // Build synthetic payload for HAVING: raw __AGG__ keys + GROUP BY fields.
                let mut synthetic = serde_json::Map::new();
                for f in fields {
                    if let Some(agg_expr) = agg_inner(f) {
                        let out_key = field_output_key(f);
                        if let Some(acc) = state.accums.get(&out_key) {
                            synthetic.insert(agg_expr.to_string(), acc.finalize());
                        }
                    }
                }
                for (gf, v) in &state.group_vals {
                    synthetic.insert(gf.clone(), v.clone());
                }
                let synthetic_val = Value::Object(synthetic);

                // Apply HAVING conditions.
                for having in &having_steps {
                    if !having.iter().all(|s| eval_step_on_payload(s, &synthetic_val)) {
                        return None;
                    }
                }

                // Build output payload.
                let map = if !fields.is_empty() {
                    let mut m = serde_json::Map::new();
                    for f in fields {
                        let out_key = field_output_key(f);
                        if let Some(acc) = state.accums.get(&out_key) {
                            // Aggregate field — finalize accumulator.
                            m.insert(out_key, acc.finalize());
                        } else {
                            // Non-aggregate field (enforced to be a GROUP BY key by the parser).
                            // Look up by raw inner field name; emit under the output alias.
                            let inner = field_inner_name(f);
                            if let Some(v) = state.group_vals.get(inner) {
                                m.insert(out_key, v.clone());
                            } else if let Some(ref fp) = state.first_payload {
                                // Function sentinels (ST_AsGeoJSON, JSON path, etc.)
                                if let Some(v) = eval_field_expr(f, fp) {
                                    m.insert(out_key, v);
                                }
                            }
                        }
                    }
                    Value::Object(m)
                } else {
                    // SELECT * — return first node's full payload.
                    state.first_payload
                        .unwrap_or_else(|| Value::Object(serde_json::Map::new()))
                };

                Some(Hit { slug: String::new(), slug_hash: 0, payload: Some(map) })
            }).collect();

            // Sort grouped results.
            if let Some(columns) = sort_step {
                results.sort_by(|a, b| {
                    for (sort_field, asc) in columns {
                        let va = a.payload.as_ref().and_then(|p| p.get(sort_field.as_str()));
                        let vb = b.payload.as_ref().and_then(|p| p.get(sort_field.as_str()));
                        let ord = cmp_json(va, vb);
                        if ord != std::cmp::Ordering::Equal {
                            return if *asc { ord } else { ord.reverse() };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
            if let Some(n) = skip_n {
                if n >= results.len() { results.clear(); } else { results.drain(..n); }
            }
            if let Some(n) = take_n { results.truncate(n); }
            if distinct {
                let mut seen: HashSet<String> = HashSet::new();
                results.retain(|hit| {
                    let key = serde_json::to_string(hit.payload.as_ref().unwrap_or(&Value::Null)).unwrap_or_default();
                    seen.insert(key)
                });
            }
            return results;
        }

        // ── Aggregation mode ──────────────────────────────────────────────────
        // If any SELECT field is an aggregate function, process all candidates
        // and return a single summary Hit.
        let has_agg = select_fields
            .as_ref()
            .map_or(false, |fields| fields.iter().any(|f| agg_inner(f).is_some()));

        if has_agg {
            let hashes = execute(self.db, &self.steps);
            let fields = match &select_fields {
                Some(f) => f.as_slice(),
                None => &[],
            };
            // Accumulate state for every aggregate field.
            // key = field_output_key(expr), value = AggState
            let mut states: HashMap<String, AggAccum> = HashMap::new();
            // For non-aggregate fields, keep the first non-null value seen.
            let mut non_agg: HashMap<String, Value> = HashMap::new();

            // Fast path: if every selected field is COUNT(*) and there are no
            // non-aggregate fields, we can skip reading every payload entirely.
            let all_count_star = !fields.is_empty() && fields.iter().all(|f| {
                agg_inner(f).map_or(false, |expr| {
                    let rest = expr.strip_prefix("__AGG__").unwrap_or(expr);
                    let mut parts = rest.splitn(2, "__");
                    let func = parts.next().unwrap_or("").to_uppercase();
                    let arg  = parts.next().unwrap_or("*");
                    func == "COUNT" && arg == "*"
                })
            });

            if all_count_star {
                // No payload reads needed — just count the candidates.
                let n = hashes.len() as i64;
                let mut map = serde_json::Map::new();
                for f in fields {
                    map.insert(field_output_key(f), Value::Number(serde_json::Number::from(n)));
                }
                return vec![Hit { slug: String::new(), slug_hash: 0, payload: Some(Value::Object(map)) }];
            }

            for &hash in &hashes {
                if let Some(payload) = self.db.get_payload(hash) {
                    for f in fields {
                        if let Some(agg_expr) = agg_inner(f) {
                            let key = field_output_key(f);
                            let rest = agg_expr.strip_prefix("__AGG__").unwrap_or(agg_expr);
                            let mut parts = rest.splitn(2, "__");
                            let func = parts.next().unwrap_or("COUNT").to_uppercase();
                            let arg = parts.next().unwrap_or("*");
                            let state = states.entry(key).or_insert_with(|| AggAccum::new(&func, arg));
                            state.push(&payload);
                        } else if !non_agg.contains_key(&field_output_key(f)) {
                            if let Some(v) = eval_field_expr(f, &payload) {
                                non_agg.insert(field_output_key(f), v);
                            }
                        }
                    }
                }
            }
            // Build the result object in SELECT order.
            let mut map = serde_json::Map::new();
            for f in fields {
                let key = field_output_key(f);
                if let Some(state) = states.get(&key) {
                    map.insert(key, state.finalize());
                } else if let Some(v) = non_agg.get(&key) {
                    map.insert(key, v.clone());
                }
            }
            return vec![Hit {
                slug: String::new(),
                slug_hash: 0,
                payload: Some(Value::Object(map)),
            }];
        }

        // Determine whether the fast raw-byte field extractor can be used.
        // Requirements: SELECT has fields, no BM25 scoring, and ALL fields are
        // plain top-level names (no functions, JSON path, or `*`).
        let can_use_fast_path = bm25_scores.is_none()
            && select_fields.as_ref().map_or(false, |fs| {
                !fs.is_empty() && fs.iter().all(|f| is_simple_field(f))
            });

        // Payload size threshold above which we use the fast raw-byte extraction
        // path instead of full serde_json deserialization.  Nodes smaller than
        // this are cheaply deserialized by serde_json, so the extra complexity
        // of the raw scanner is not worthwhile.  64 KB covers most small records.
        const FAST_PATH_THRESHOLD: u32 = 64 * 1024;

        // ── Batch collect path ─────────────────────────────────────────────────
        // When SELECT uses only simple fields (no functions, JSON paths, BM25)
        // and there are ≥ 2 results, batch-read all small payloads in one or a
        // few sequential pread() calls (sorted by offset) rather than N
        // individual syscalls.  Large payloads fall back to the head+tail path.
        // This eliminates the N-syscall cost for typical point-attribute queries.
        if can_use_fast_path && bm25_scores.is_none() {
            let fields = select_fields.as_ref().unwrap(); // safe: can_use_fast_path requires Some
            let hashes = execute(self.db, &self.steps);
            // Collect which hashes need a full payload and which can be batched.
            let raw_map: HashMap<u64, Vec<u8>> = {
                let small: Vec<u64> = hashes.iter().copied().filter(|&h| {
                    self.db.node_data(h)
                        .map_or(true, |nd| nd.payload_len <= FAST_PATH_THRESHOLD)
                }).collect();
                if small.len() >= 2 {
                    self.db.read_raw_payloads_batched(&small)
                } else {
                    HashMap::new()
                }
            };
            let mut hits: Vec<Hit> = hashes.into_iter().filter_map(|hash| {
                let node = self.db.node_data(hash)?;
                let out = if node.payload_len > FAST_PATH_THRESHOLD {
                    // Large payload: head + tail read (avoids loading e.g. 12 MB GeoJSON).
                    let (head, tail) = self.db.get_payload_head_tail(
                        hash, 512, 16 * 1024,
                    )?;
                    let mut map = extract_fields_by_search(&tail, fields);
                    let missing: Vec<String> = fields.iter()
                        .filter(|f| !map.contains_key(f.as_str()))
                        .cloned()
                        .collect();
                    if !missing.is_empty() {
                        for (k, v) in extract_fields_by_search(&head, &missing) {
                            map.entry(k).or_insert(v);
                        }
                    }
                    let mut out = serde_json::Map::new();
                    for f in fields {
                        if let Some(v) = map.remove(f.as_str()) {
                            out.insert(field_output_key(f), v);
                        }
                    }
                    out
                } else if let Some(bytes) = raw_map.get(&hash) {
                    // Small payload from batch buffer — byte-search, no full JSON parse.
                    let mut found = extract_fields_by_search(bytes, fields);
                    let mut out = serde_json::Map::new();
                    for f in fields {
                        if let Some(v) = found.remove(f.as_str()) {
                            out.insert(field_output_key(f), v);
                        }
                    }
                    out
                } else {
                    // Fallback: individual pread + full parse.
                    let raw_payload = self.db.get_payload(hash).unwrap_or(Value::Null);
                    let mut out = serde_json::Map::new();
                    for f in fields {
                        if let Some(v) = eval_field_expr(f, &raw_payload) {
                            out.insert(field_output_key(f), v);
                        }
                    }
                    out
                };
                Some(Hit {
                    slug: node.slug.clone(),
                    slug_hash: hash,
                    payload: Some(Value::Object(out)),
                })
            }).collect();
            let distinct = self.steps.iter().any(|s| matches!(s, Step::Distinct));
            if distinct {
                let mut seen: HashSet<String> = HashSet::new();
                hits.retain(|hit| {
                    let key = serde_json::to_string(
                        hit.payload.as_ref().unwrap_or(&Value::Null),
                    ).unwrap_or_default();
                    seen.insert(key)
                });
            }
            return hits;
        }

        let mut hits: Vec<Hit> = execute(self.db, &self.steps)
            .into_iter()
            .filter_map(|hash| {
                let node = self.db.node_data(hash)?;
                let payload = match (&select_fields, &bm25_scores) {
                    (None, None) => {
                        self.db.get_payload(hash)
                    }
                    (Some(fields), None) if can_use_fast_path => {
                        // Fast path: direct byte-pattern search on head + tail slices.
                        // For large payloads (e.g. 12 MB GeoJSON polygons) this avoids
                        // reading the full blob; metadata fields are found in ≤ 16 KB.
                        if node.payload_len > FAST_PATH_THRESHOLD {
                            let (head, tail) = self.db.get_payload_head_tail(
                                hash,
                                512,        // first 512 bytes — _key, _collection, etc.
                                16 * 1024,  // last 16 KB — metadata: name, level, etc.
                            )?;
                            // Search tail first (metadata fields live at the end).
                            let mut map = extract_fields_by_search(&tail, fields);
                            // Any fields not found in tail → search head.
                            let missing: Vec<String> = fields.iter()
                                .filter(|f| !map.contains_key(f.as_str()))
                                .cloned()
                                .collect();
                            if !missing.is_empty() {
                                let head_map = extract_fields_by_search(&head, &missing);
                                for (k, v) in head_map {
                                    map.entry(k).or_insert(v);
                                }
                            }
                            // Build output map with correct output key names.
                            let mut out = serde_json::Map::new();
                            for f in fields {
                                let key = field_output_key(f);
                                if let Some(v) = map.remove(f.as_str()) {
                                    out.insert(key, v);
                                }
                            }
                            Some(Value::Object(out))
                        } else {
                            // Small payload — full parse is cheap.
                            let raw_payload = self.db.get_payload(hash).unwrap_or(Value::Null);
                            let mut out = serde_json::Map::new();
                            for f in fields {
                                if let Some(v) = eval_field_expr(f, &raw_payload) {
                                    out.insert(field_output_key(f), v);
                                }
                            }
                            Some(Value::Object(out))
                        }
                    }
                    (Some(fields), None) => {
                        let raw_payload = self.db.get_payload(hash).unwrap_or(Value::Null);
                        let mut map = serde_json::Map::new();
                        for f in fields {
                            if let Some(v) = eval_field_expr(f, &raw_payload) {
                                map.insert(field_output_key(f), v);
                            }
                        }
                        Some(Value::Object(map))
                    }
                    (None, Some((bm25_field, _bm25_query, scores))) => {
                        let raw_payload = self.db.get_payload(hash).unwrap_or(Value::Null);
                        let mut map = raw_payload.as_object().cloned().unwrap_or_default();
                        let score_key = format!("_bm25_{}_score", bm25_field);
                        if let Some(&s) = scores.get(&hash) {
                            map.insert(score_key, serde_json::json!(s));
                        } else {
                            map.insert(score_key, serde_json::json!(0.0));
                        }
                        Some(Value::Object(map))
                    }
                    (Some(fields), Some((bm25_field, _bm25_query, scores))) => {
                        let raw_payload = self.db.get_payload(hash).unwrap_or(Value::Null);
                        let mut map = serde_json::Map::new();
                        for f in fields {
                            if let Some(v) = eval_field_expr(f, &raw_payload) {
                                map.insert(field_output_key(f), v);
                            }
                        }
                        let score_key = format!("_bm25_{}_score", bm25_field);
                        if let Some(&s) = scores.get(&hash) {
                            map.insert(score_key, serde_json::json!(s));
                        } else {
                            map.insert(score_key, serde_json::json!(0.0));
                        }
                        Some(Value::Object(map))
                    }
                };
                Some(Hit {
                    slug: node.slug.clone(),
                    slug_hash: hash,
                    payload,
                })
            })
            .collect::<Vec<_>>();

        // ── DISTINCT deduplication ────────────────────────────────────────────
        let distinct = self.steps.iter().any(|s| matches!(s, Step::Distinct));
        if distinct {
            let mut seen: HashSet<String> = HashSet::new();
            hits.retain(|hit| {
                let key = serde_json::to_string(hit.payload.as_ref().unwrap_or(&Value::Null))
                    .unwrap_or_default();
                seen.insert(key)
            });
        }
        hits
    }

    /// Return the number of matching nodes without resolving payloads.
    pub fn count(self) -> usize {
        if let Some(hits) = self.precomputed {
            return hits.len();
        }
        execute(self.db, &self.steps).len()
    }

    /// Return the first matching node, or `None`.
    pub fn first(self) -> Option<Hit> {
        // Re-use collect; a future optimisation could short-circuit.
        self.collect().into_iter().next()
    }

    /// Return `true` if at least one node matches.
    pub fn exists(self) -> bool {
        if let Some(hits) = self.precomputed {
            return !hits.is_empty();
        }
        !execute(self.db, &self.steps).is_empty()
    }
}

// ── Condition evaluator ───────────────────────────────────────────────────────

/// Evaluate a single filter step for one candidate hash.
///
/// Used by `WhereNot` and `WhereOr` to check predicates recursively.
/// Numeric-aware equality: `42` (JSON int) == `42.0` (SQL float).
fn values_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => n1.as_f64() == n2.as_f64(),
        _ => a == b,
    }
}

/// Evaluate a filter step directly against a `Value` payload (no DB lookup).
/// Used for HAVING conditions evaluated against synthetic per-group payloads.
/// Resolve a field value from a payload for WHERE/HAVING evaluation.
///
/// - `__FUNC__*` sentinels are evaluated via [`eval_field_expr`] (date functions, etc.)
/// - Everything else (plain fields, `__AGG__*`, JSON-path `__JP_*`, etc.) uses
///   [`json_path_get`], which preserves the literal key lookup needed for HAVING
///   on synthetic aggregate payloads.
#[inline]
fn resolve_field(field: &str, payload: &Value) -> Option<Value> {
    if field.starts_with("__FUNC__") {
        eval_field_expr(field, payload)
    } else {
        json_path_get(field, payload)
    }
}

// ── Fast raw-byte JSON field extractor ────────────────────────────────────────

/// Advance `i` past the closing `"`, handling `\` escape sequences.
/// Expects `i` to point to the first byte INSIDE the string (after the opening `"`).
fn scan_string_end(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() {
        match bytes[i] {
            b'"'  => return i + 1,
            b'\\' => i += 2,
            _     => i += 1,
        }
    }
    i
}

/// Find the last occurrence of `needle` in `haystack`.
fn rfind_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    let limit = haystack.len() - needle.len();
    for i in (0..=limit).rev() {
        if &haystack[i..i + needle.len()] == needle {
            return Some(i);
        }
    }
    None
}

/// Extract a simple JSON value (string, number, boolean, null) from `bytes`
/// starting at `start`.  Returns `(Value, end_position)` on success.
/// Returns `None` for complex values (objects/arrays).
fn extract_simple_value(bytes: &[u8], start: usize) -> Option<(Value, usize)> {
    match bytes.get(start)? {
        b'"' => {
            // String: find closing '"' respecting escapes
            let mut end = start + 1;
            while end < bytes.len() {
                match bytes[end] {
                    b'"'  => { end += 1; break; }
                    b'\\' => end += 2,
                    _     => end += 1,
                }
            }
            // Let serde_json decode escape sequences correctly
            let v = serde_json::from_slice::<Value>(&bytes[start..end]).ok()?;
            Some((v, end))
        }
        b't' if bytes.get(start..start + 4) == Some(b"true") => {
            Some((Value::Bool(true), start + 4))
        }
        b'f' if bytes.get(start..start + 5) == Some(b"false") => {
            Some((Value::Bool(false), start + 5))
        }
        b'n' if bytes.get(start..start + 4) == Some(b"null") => {
            Some((Value::Null, start + 4))
        }
        b'-' | b'0'..=b'9' => {
            let mut end = start;
            while end < bytes.len()
                && !matches!(bytes[end], b',' | b'}' | b']' | b' ' | b'\n' | b'\r' | b'\t')
            {
                end += 1;
            }
            let v = serde_json::from_slice::<Value>(&bytes[start..end]).ok()?;
            Some((v, end))
        }
        _ => None, // complex value (object/array)
    }
}

/// Search `bytes` for each requested field name using a direct byte pattern search
/// (`"<field>":`) and extract its simple value.
///
/// Uses `rfind` so the LAST occurrence of the pattern is returned first — this
/// means fields that appear near the END of the blob (e.g. metadata after geometry)
/// are found quickly without scanning from the beginning.
///
/// This function does NOT require `bytes` to start at `{`.  It works on any
/// slice of a JSON payload (head, tail, or full blob).
fn extract_fields_by_search(
    bytes: &[u8],
    fields: &[String],
) -> serde_json::Map<String, Value> {
    let mut result = serde_json::Map::new();
    for field in fields {
        let needle = format!("\"{}\":", field);
        // Try rfind first (finds occurrence near the end — fast for tail metadata)
        if let Some(pos) = rfind_bytes(bytes, needle.as_bytes()) {
            let val_start_raw = pos + needle.len();
            // Skip optional whitespace
            let val_start = bytes[val_start_raw..]
                .iter()
                .position(|&b| !matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
                .map(|off| val_start_raw + off)
                .unwrap_or(val_start_raw);
            if let Some((v, _)) = extract_simple_value(bytes, val_start) {
                result.insert(field.clone(), v);
            }
        }
    }
    result
}

/// Check whether a SELECT field expression is a plain top-level field name
/// (no `__` prefix, no `->` operators, no `*`).  Only plain fields can use
/// the raw-byte fast extraction path.
fn is_simple_field(expr: &str) -> bool {
    !expr.contains("__")
        && !expr.contains("->")
        && !expr.contains('*')
        && !expr.contains('(')
        && !expr.contains(')')
}

fn eval_step_on_payload(step: &Step, payload: &Value) -> bool {
    match step {
        Step::WhereEq(field, value) => resolve_field(field, payload)
            .map(|v| values_eq(&v, value))
            .unwrap_or(false),
        Step::WhereNeq(field, value) => resolve_field(field, payload)
            .map(|v| !values_eq(&v, value))
            .unwrap_or(true),
        Step::WhereGt(field, t) => resolve_field(field, payload)
            .and_then(|v| v.as_f64())
            .map(|f| f > *t)
            .unwrap_or(false),
        Step::WhereLt(field, t) => resolve_field(field, payload)
            .and_then(|v| v.as_f64())
            .map(|f| f < *t)
            .unwrap_or(false),
        Step::WhereGte(field, t) => resolve_field(field, payload)
            .and_then(|v| v.as_f64())
            .map(|f| f >= *t)
            .unwrap_or(false),
        Step::WhereLte(field, t) => resolve_field(field, payload)
            .and_then(|v| v.as_f64())
            .map(|f| f <= *t)
            .unwrap_or(false),
        Step::WhereIsNull(field, negated) => {
            let is_null = resolve_field(field, payload)
                .map(|v| v.is_null())
                .unwrap_or(true);
            if *negated { !is_null } else { is_null }
        }
        Step::WhereNot(inner) => !eval_step_on_payload(inner, payload),
        Step::WhereOr(branches) => branches
            .iter()
            .any(|branch| branch.iter().all(|s| eval_step_on_payload(s, payload))),
        _ => true,
    }
}

/// Complex steps (spatial, vector, BM25, index-backed Like) fall back to
/// `true` when nested inside NOT/OR — callers should avoid those patterns
/// for best results.
fn eval_cond(db: &CoreDB, h: u64, step: &Step) -> bool {
    match step {
        Step::WhereEq(field, value) => db
            .get_payload(h)
            .and_then(|p| resolve_field(field, &p))
            .map(|v| values_eq(&v, value))
            .unwrap_or(false),
        Step::WhereNeq(field, value) => db
            .get_payload(h)
            .and_then(|p| resolve_field(field, &p))
            .map(|v| !values_eq(&v, value))
            .unwrap_or(true),
        Step::WhereGt(field, t) => db
            .get_payload(h)
            .and_then(|p| resolve_field(field, &p))
            .and_then(|v| v.as_f64())
            .map(|f| f > *t)
            .unwrap_or(false),
        Step::WhereLt(field, t) => db
            .get_payload(h)
            .and_then(|p| resolve_field(field, &p))
            .and_then(|v| v.as_f64())
            .map(|f| f < *t)
            .unwrap_or(false),
        Step::WhereGte(field, t) => db
            .get_payload(h)
            .and_then(|p| resolve_field(field, &p))
            .and_then(|v| v.as_f64())
            .map(|f| f >= *t)
            .unwrap_or(false),
        Step::WhereLte(field, t) => db
            .get_payload(h)
            .and_then(|p| resolve_field(field, &p))
            .and_then(|v| v.as_f64())
            .map(|f| f <= *t)
            .unwrap_or(false),
        Step::WhereBetween(field, lo, hi) => db
            .get_payload(h)
            .and_then(|p| resolve_field(field, &p))
            .and_then(|v| v.as_f64())
            .map(|f| f >= *lo && f <= *hi)
            .unwrap_or(false),
        Step::WhereIn(field, values) => db
            .get_payload(h)
            .and_then(|p| resolve_field(field, &p))
            .map(|v| value_in(&v, values))
            .unwrap_or(false),
        Step::WhereIsNull(field, negated) => {
            let v = db.get_payload(h).and_then(|p| resolve_field(field, &p));
            let is_null = v.is_none() || matches!(v, Some(Value::Null));
            if *negated { !is_null } else { is_null }
        }
        Step::Like(field, pattern, case_insensitive) => {
            use crate::text_index::query::ilike_matches;
            let v = db.get_payload(h).and_then(|p| resolve_field(field, &p));
            v.as_ref()
                .and_then(|v| v.as_str())
                .map(|s| {
                    if *case_insensitive {
                        ilike_matches(s, pattern)
                    } else {
                        s.contains(pattern.as_str())
                    }
                })
                .unwrap_or(false)
        }
        Step::WhereNot(inner) => !eval_cond(db, h, inner),
        Step::WhereOr(branches) => branches
            .iter()
            .any(|branch| branch.iter().all(|s| eval_cond(db, h, s))),
        // Non-filter steps always pass in nested context
        _ => true,
    }
}

// ── ScoreExpr helpers ─────────────────────────────────────────────────────────

/// Collect all unique (field, query) pairs from BM25 leaves in the expression.
fn gather_bm25_keys(expr: &ScoreExpr, out: &mut HashSet<(String, String)>) {
    match expr {
        ScoreExpr::Bm25 { field, query } => { out.insert((field.clone(), query.clone())); }
        ScoreExpr::Add(a, b) | ScoreExpr::Sub(a, b)
        | ScoreExpr::Mul(a, b) | ScoreExpr::Div(a, b) => {
            gather_bm25_keys(a, out);
            gather_bm25_keys(b, out);
        }
        ScoreExpr::Neg(a) => gather_bm25_keys(a, out),
        _ => {}
    }
}

/// Collect the first query vector seen per (metric, field) pair in the expression.
fn gather_vector_keys(expr: &ScoreExpr, out: &mut HashMap<(VecMetric, String), Vec<f32>>) {
    match expr {
        ScoreExpr::VectorCosine { field, query } => {
            out.entry((VecMetric::Cosine, field.clone())).or_insert_with(|| query.clone());
        }
        ScoreExpr::VectorL2 { field, query } => {
            out.entry((VecMetric::L2, field.clone())).or_insert_with(|| query.clone());
        }
        ScoreExpr::VectorDot { field, query } => {
            out.entry((VecMetric::Dot, field.clone())).or_insert_with(|| query.clone());
        }
        ScoreExpr::VectorL1 { field, query } => {
            out.entry((VecMetric::L1, field.clone())).or_insert_with(|| query.clone());
        }
        ScoreExpr::Add(a, b) | ScoreExpr::Sub(a, b)
        | ScoreExpr::Mul(a, b) | ScoreExpr::Div(a, b) => {
            gather_vector_keys(a, out);
            gather_vector_keys(b, out);
        }
        ScoreExpr::Neg(a) => gather_vector_keys(a, out),
        _ => {}
    }
}

/// Evaluate a `ScoreExpr` for one node, given pre-computed score maps.
fn eval_score(
    expr: &ScoreExpr,
    hash: u64,
    payload: &Value,
    db: &CoreDB,
    bm25_maps: &HashMap<(String, String), HashMap<u64, f64>>,
    vec_maps: &HashMap<(VecMetric, String), HashMap<u64, f32>>,
) -> f64 {
    // Shorthand for recursive calls.
    macro_rules! rec {
        ($e:expr) => {
            eval_score($e, hash, payload, db, bm25_maps, vec_maps)
        };
    }
    match expr {
        ScoreExpr::Lit(n) => *n,
        ScoreExpr::Field(name) => payload.get(name).and_then(|v| v.as_f64()).unwrap_or(0.0),
        ScoreExpr::Bm25 { field, query } => {
            bm25_maps
                .get(&(field.clone(), query.clone()))
                .and_then(|m| m.get(&hash))
                .copied()
                .unwrap_or(0.0)
        }
        ScoreExpr::VectorCosine { field, .. } => {
            vec_maps.get(&(VecMetric::Cosine, field.clone()))
                .and_then(|m| m.get(&hash)).map(|&s| s as f64).unwrap_or(0.0)
        }
        ScoreExpr::VectorL2 { field, .. } => {
            vec_maps.get(&(VecMetric::L2, field.clone()))
                .and_then(|m| m.get(&hash)).map(|&s| s as f64).unwrap_or(0.0)
        }
        ScoreExpr::VectorDot { field, .. } => {
            vec_maps.get(&(VecMetric::Dot, field.clone()))
                .and_then(|m| m.get(&hash)).map(|&s| s as f64).unwrap_or(0.0)
        }
        ScoreExpr::VectorL1 { field, .. } => {
            vec_maps.get(&(VecMetric::L1, field.clone()))
                .and_then(|m| m.get(&hash)).map(|&s| s as f64).unwrap_or(0.0)
        }
        ScoreExpr::StDistance { field, lat, lon } => {
            let point = serde_json::json!({ "type": "Point", "coordinates": [lon, lat] });
            payload
                .get(field)
                .and_then(|geom| crate::geo::distance_km(geom, &point))
                .unwrap_or(f64::MAX)
        }
        ScoreExpr::Add(a, b) => rec!(a) + rec!(b),
        ScoreExpr::Sub(a, b) => rec!(a) - rec!(b),
        ScoreExpr::Mul(a, b) => rec!(a) * rec!(b),
        ScoreExpr::Div(a, b) => {
            let denom = rec!(b);
            if denom == 0.0 { 0.0 } else { rec!(a) / denom }
        }
        ScoreExpr::Neg(a) => -rec!(a),
    }
}

// ── Executor ──────────────────────────────────────────────────────────────────

/// Execute the step pipeline and return candidate slug hashes in order.
fn execute(db: &CoreDB, steps: &[Step]) -> Vec<u64> {
    let mut candidates: Vec<u64> = Vec::new();
    // Steps consumed by btree_seed (already applied as the seed filter)
    let mut skip_set: HashSet<usize> = HashSet::new();
    // Track the active collection hash so post-seed filters can use btree indexes.
    let mut current_coll_hash: Option<u64> = None;

    for (i, step) in steps.iter().enumerate() {
        if skip_set.contains(&i) {
            continue;
        }
        let remaining = &steps[i + 1..];
        match step {
            // ── Starters ────────────────────────────────────────────────────
            Step::One(hash) => {
                candidates = if db.node_data(*hash).is_some() {
                    vec![*hash]
                } else {
                    vec![]
                };
            }
            Step::Many(hashes) => {
                candidates = hashes
                    .iter()
                    .copied()
                    .filter(|&h| db.node_data(h).is_some())
                    .collect();
            }
            Step::Collection(hash) => {
                current_coll_hash = Some(*hash);
                // Priority 1: btree equality/range filter seed (most selective)
                if let Some((seeded, skip_j, opt_skip_j2)) = db.btree_seed(*hash, remaining) {
                    candidates = seeded;
                    // skip the step(s) consumed by the btree index
                    skip_set.insert(i + 1 + skip_j);
                    if let Some(j2) = opt_skip_j2 {
                        skip_set.insert(i + 1 + j2);
                    }
                // Priority 2: btree ORDER BY index scan (pre-sorted candidates)
                // Also skip the Sort step — data is already in index order.
                } else if let Some((sorted, sort_j)) = db.btree_sorted_seed_from_steps(*hash, remaining) {
                    candidates = sorted;
                    skip_set.insert(i + 1 + sort_j);
                } else {
                    candidates = db.collection_members(*hash).cloned().unwrap_or_default();
                }
            }
            Step::All => {
                candidates = db.all_hashes();
            }

            // ── Graph traversal ──────────────────────────────────────────────
            Step::Forward(type_hash) => {
                let mut next: HashSet<u64> = HashSet::new();
                for &node in &candidates {
                    if let Some(edges) = db.fwd_edges(node) {
                        for e in edges {
                            if e.edge_type == *type_hash {
                                next.insert(e.other);
                            }
                        }
                    }
                }
                // Only keep nodes that exist in the DB
                candidates = next
                    .into_iter()
                    .filter(|&h| db.node_data(h).is_some())
                    .collect();
            }
            Step::Backward(type_hash) => {
                let mut next: HashSet<u64> = HashSet::new();
                for &node in &candidates {
                    if let Some(edges) = db.rev_edges(node) {
                        for e in edges {
                            if e.edge_type == *type_hash {
                                next.insert(e.other);
                            }
                        }
                    }
                }
                candidates = next
                    .into_iter()
                    .filter(|&h| db.node_data(h).is_some())
                    .collect();
            }
            Step::Hops(n) => {
                // BFS: expand forward over any edge type, up to n levels.
                let mut visited: HashSet<u64> = candidates.iter().copied().collect();
                let mut frontier: Vec<u64> = candidates.clone();
                for _ in 0..*n {
                    let mut next: Vec<u64> = Vec::new();
                    for &node in &frontier {
                        if let Some(edges) = db.fwd_edges(node) {
                            for e in edges {
                                if visited.insert(e.other) {
                                    next.push(e.other);
                                }
                            }
                        }
                    }
                    if next.is_empty() {
                        break;
                    }
                    frontier = next;
                }
                candidates = visited
                    .into_iter()
                    .filter(|&h| db.node_data(h).is_some())
                    .collect();
            }
            Step::HopsTyped {
                type_hash,
                min_depth,
                max_depth,
            } => {
                // Typed BFS: follow only edges matching type_hash.
                // Collect nodes reached at depths min_depth..=max_depth.
                let mut visited: HashSet<u64> = HashSet::new();
                let mut frontier: Vec<u64> = candidates.clone();
                let mut result: Vec<u64> = Vec::new();
                for depth in 1..=*max_depth {
                    let mut next: Vec<u64> = Vec::new();
                    for &node in &frontier {
                        if let Some(edges) = db.fwd_edges(node) {
                            for e in edges {
                                if e.edge_type == *type_hash && visited.insert(e.other) {
                                    next.push(e.other);
                                }
                            }
                        }
                    }
                    if next.is_empty() {
                        break;
                    }
                    if depth >= *min_depth {
                        result.extend(&next);
                    }
                    frontier = next;
                }
                candidates = result
                    .into_iter()
                    .filter(|&h| db.node_data(h).is_some())
                    .collect();
            }
            Step::MinStrength(threshold) => {
                // Find the most recent Forward/Backward step to know which edge type to check.
                // Walk backwards through the step list up to (but not including) this step.
                let this_pos = steps
                    .iter()
                    .position(|s| {
                        if let Step::MinStrength(t) = s {
                            *t == *threshold
                        } else {
                            false
                        }
                    })
                    .unwrap_or(0);
                let edge_type_hash = steps[..this_pos].iter().rev().find_map(|s| match s {
                    Step::Forward(h) | Step::Backward(h) => Some(*h),
                    _ => None,
                });
                if let Some(type_h) = edge_type_hash {
                    let thr = *threshold;
                    candidates.retain(|&dest| {
                        // dest is reachable — check that at least one incoming edge of the
                        // correct type has strength >= threshold.
                        db.rev_edges(dest)
                            .map(|edges| {
                                edges
                                    .iter()
                                    .any(|e| e.edge_type == type_h && e.strength >= thr)
                            })
                            .unwrap_or(false)
                    });
                }
                // If no prior Forward/Backward found, MinStrength is a no-op.
            }
            Step::Leaves => {
                candidates.retain(|&h| {
                    db.fwd_edges(h)
                        .map(|edges| edges.is_empty())
                        .unwrap_or(true)
                });
            }
            Step::Roots => {
                candidates.retain(|&h| {
                    db.rev_edges(h)
                        .map(|edges| edges.is_empty())
                        .unwrap_or(true)
                });
            }

            // ── Payload filters ──────────────────────────────────────────────
            //
            // WhereEq: try btree intersection first (zero payload reads).
            // Falls back to batch pread + byte-search, then individual pread.
            Step::WhereEq(field, value) => {
                // Btree intersection: O(|btree_set|) HashSet build + O(|candidates|) retain.
                // Zero payload reads — the index already maps value → [hash, …].
                if let (Some(coll), Some(fk)) = (
                    current_coll_hash,
                    FieldKey::from_json(value),
                ) {
                    if let Some(idx) = db.field_index(coll, field) {
                        let btree_set: HashSet<u64> = idx
                            .get(&fk)
                            .into_iter()
                            .flat_map(|ids| ids.iter().copied())
                            .collect();
                        candidates.retain(|h| btree_set.contains(h));
                        // Skip payload reads entirely — done.
                    } else {
                        // No index: batch read + byte-search.
                        const FILTER_BATCH_MIN: usize = 64;
                        if is_simple_field(field) && candidates.len() >= FILTER_BATCH_MIN {
                            let raw_map = db.read_raw_payloads_batched(&candidates);
                            let fq = vec![field.clone()];
                            candidates.retain(|&h| {
                                raw_map.get(&h)
                                    .and_then(|bytes| {
                                        extract_fields_by_search(bytes, &fq).remove(field.as_str())
                                    })
                                    .map(|v| values_eq(&v, value))
                                    .unwrap_or(false)
                            });
                        } else {
                            candidates.retain(|&h| {
                                db.get_payload(h)
                                    .and_then(|p| resolve_field(field, &p))
                                    .map(|v| values_eq(&v, value))
                                    .unwrap_or(false)
                            });
                        }
                    }
                } else {
                    // No collection context or non-indexable value: batch read + byte-search.
                    const FILTER_BATCH_MIN: usize = 64;
                    if is_simple_field(field) && candidates.len() >= FILTER_BATCH_MIN {
                        let raw_map = db.read_raw_payloads_batched(&candidates);
                        let fq = vec![field.clone()];
                        candidates.retain(|&h| {
                            raw_map.get(&h)
                                .and_then(|bytes| {
                                    extract_fields_by_search(bytes, &fq).remove(field.as_str())
                                })
                                .map(|v| values_eq(&v, value))
                                .unwrap_or(false)
                        });
                    } else {
                        candidates.retain(|&h| {
                            db.get_payload(h)
                                .and_then(|p| resolve_field(field, &p))
                                .map(|v| values_eq(&v, value))
                                .unwrap_or(false)
                        });
                    }
                }
            }
            Step::WhereNeq(field, value) => {
                const FILTER_BATCH_MIN: usize = 64;
                if is_simple_field(field) && candidates.len() >= FILTER_BATCH_MIN {
                    let raw_map = db.read_raw_payloads_batched(&candidates);
                    let fq = vec![field.clone()];
                    candidates.retain(|&h| {
                        // field absent → keep (same semantics as individual path)
                        raw_map.get(&h)
                            .map(|bytes| {
                                extract_fields_by_search(bytes, &fq)
                                    .remove(field.as_str())
                                    .map(|v| !values_eq(&v, value))
                                    .unwrap_or(true)
                            })
                            .unwrap_or(true)
                    });
                } else {
                    candidates.retain(|&h| {
                        db.get_payload(h)
                            .and_then(|p| resolve_field(field, &p))
                            .map(|v| !values_eq(&v, value))
                            .unwrap_or(true) // field absent → keep
                    });
                }
            }
            Step::WhereGt(field, threshold) => {
                const FILTER_BATCH_MIN: usize = 64;
                if is_simple_field(field) && candidates.len() >= FILTER_BATCH_MIN {
                    let raw_map = db.read_raw_payloads_batched(&candidates);
                    let fq = vec![field.clone()];
                    candidates.retain(|&h| {
                        raw_map.get(&h)
                            .and_then(|bytes| {
                                extract_fields_by_search(bytes, &fq).remove(field.as_str())
                            })
                            .and_then(|v| v.as_f64())
                            .map(|f| f > *threshold)
                            .unwrap_or(false)
                    });
                } else {
                    candidates.retain(|&h| {
                        db.get_payload(h)
                            .and_then(|p| resolve_field(field, &p))
                            .and_then(|v| v.as_f64())
                            .map(|f| f > *threshold)
                            .unwrap_or(false)
                    });
                }
            }
            Step::WhereLt(field, threshold) => {
                const FILTER_BATCH_MIN: usize = 64;
                if is_simple_field(field) && candidates.len() >= FILTER_BATCH_MIN {
                    let raw_map = db.read_raw_payloads_batched(&candidates);
                    let fq = vec![field.clone()];
                    candidates.retain(|&h| {
                        raw_map.get(&h)
                            .and_then(|bytes| {
                                extract_fields_by_search(bytes, &fq).remove(field.as_str())
                            })
                            .and_then(|v| v.as_f64())
                            .map(|f| f < *threshold)
                            .unwrap_or(false)
                    });
                } else {
                    candidates.retain(|&h| {
                        db.get_payload(h)
                            .and_then(|p| resolve_field(field, &p))
                            .and_then(|v| v.as_f64())
                            .map(|f| f < *threshold)
                            .unwrap_or(false)
                    });
                }
            }
            Step::WhereGte(field, threshold) => {
                const FILTER_BATCH_MIN: usize = 64;
                if is_simple_field(field) && candidates.len() >= FILTER_BATCH_MIN {
                    let raw_map = db.read_raw_payloads_batched(&candidates);
                    let fq = vec![field.clone()];
                    candidates.retain(|&h| {
                        raw_map.get(&h)
                            .and_then(|bytes| {
                                extract_fields_by_search(bytes, &fq).remove(field.as_str())
                            })
                            .and_then(|v| v.as_f64())
                            .map(|f| f >= *threshold)
                            .unwrap_or(false)
                    });
                } else {
                    candidates.retain(|&h| {
                        db.get_payload(h)
                            .and_then(|p| resolve_field(field, &p))
                            .and_then(|v| v.as_f64())
                            .map(|f| f >= *threshold)
                            .unwrap_or(false)
                    });
                }
            }
            Step::WhereLte(field, threshold) => {
                const FILTER_BATCH_MIN: usize = 64;
                if is_simple_field(field) && candidates.len() >= FILTER_BATCH_MIN {
                    let raw_map = db.read_raw_payloads_batched(&candidates);
                    let fq = vec![field.clone()];
                    candidates.retain(|&h| {
                        raw_map.get(&h)
                            .and_then(|bytes| {
                                extract_fields_by_search(bytes, &fq).remove(field.as_str())
                            })
                            .and_then(|v| v.as_f64())
                            .map(|f| f <= *threshold)
                            .unwrap_or(false)
                    });
                } else {
                    candidates.retain(|&h| {
                        db.get_payload(h)
                            .and_then(|p| resolve_field(field, &p))
                            .and_then(|v| v.as_f64())
                            .map(|f| f <= *threshold)
                            .unwrap_or(false)
                    });
                }
            }
            Step::WhereBetween(field, lo, hi) => {
                const FILTER_BATCH_MIN: usize = 64;
                if is_simple_field(field) && candidates.len() >= FILTER_BATCH_MIN {
                    let raw_map = db.read_raw_payloads_batched(&candidates);
                    let fq = vec![field.clone()];
                    candidates.retain(|&h| {
                        raw_map.get(&h)
                            .and_then(|bytes| {
                                extract_fields_by_search(bytes, &fq).remove(field.as_str())
                            })
                            .and_then(|v| v.as_f64())
                            .map(|f| f >= *lo && f <= *hi)
                            .unwrap_or(false)
                    });
                } else {
                    candidates.retain(|&h| {
                        db.get_payload(h)
                            .and_then(|p| resolve_field(field, &p))
                            .and_then(|v| v.as_f64())
                            .map(|f| f >= *lo && f <= *hi)
                            .unwrap_or(false)
                    });
                }
            }
            Step::WhereIn(field, values) => {
                candidates.retain(|&h| {
                    db.get_payload(h)
                        .and_then(|p| resolve_field(field, &p))
                        .map(|v| value_in(&v, values))
                        .unwrap_or(false)
                });
            }
            Step::WhereIsNull(field, negated) => {
                let negated = *negated;
                candidates.retain(|&h| eval_cond(db, h, step) == true);
                let _ = (field, negated); // used in eval_cond
            }
            Step::WhereNot(_) | Step::WhereOr(_) => {
                candidates.retain(|&h| eval_cond(db, h, step));
            }
            Step::Like(field, pattern, case_insensitive) => {
                use crate::text_index::query::{ilike_matches, like_matches};
                // Look ahead for a Take limit to enable early termination
                let take_limit = find_take_limit(remaining);
                // Prefer GIN (exact trigram candidates) over GiST (lossy) when available.
                // GIN may produce false positives after the trigram fix (no space-padding for
                // interior patterns), so we verify each candidate with ilike_matches/like_matches.
                // If GIN index exists for this field and returns 0 candidates, the index
                // is complete (every document trigram was indexed), so 0 means no match.
                // Skip the expensive brute-force scan entirely.
                let gin_has_index = db.gin_indexes.contains_key(field.as_str());
                let gin_results = db.gin_ilike(field, pattern, take_limit);
                if gin_has_index && gin_results.is_empty() {
                    candidates.clear();
                } else if !gin_results.is_empty() {
                    let verify = |h: u64| -> bool {
                        db.get_payload(h)
                            .and_then(|p| json_path_get(field, &p))
                            .and_then(|v| v.as_str().map(|s| s.to_string()))
                            .map(|s| if *case_insensitive { ilike_matches(&s, pattern) }
                                     else { like_matches(&s, pattern) })
                            .unwrap_or(false)
                    };
                    if candidates.is_empty() {
                        // STARTER: verify GIN candidates
                        candidates = gin_results.into_iter().filter(|&h| verify(h)).collect();
                    } else {
                        // FILTER: intersect + verify
                        let gin_set: std::collections::HashSet<u64> =
                            gin_results.into_iter().collect();
                        candidates.retain(|h| gin_set.contains(h) && verify(*h));
                    }
                } else if let Some(candidates_from_index) =
                    db.text_index_candidates_with_limit(field, pattern, take_limit)
                {
                    // GiST is lossy — use verify() with cached text + memchr for fast verification
                    if candidates.is_empty() {
                        // STARTER: verify false positives using GiST's cached text
                        if let Some(gist) = db.text_indexes.get(field) {
                            candidates = gist.verify(&candidates_from_index, pattern, take_limit);
                        } else {
                            // Fall back to old method if no GiST
                            let verified: Vec<u64> = candidates_from_index
                                .into_iter()
                                .filter(|&h| {
                                    let v = db.get_payload(h)
                                        .and_then(|p| json_path_get(field, &p));
                                    v.as_ref()
                                        .and_then(|v| v.as_str())
                                        .map(|s| {
                                            if *case_insensitive {
                                                ilike_matches(s, pattern)
                                            } else {
                                                s.contains(pattern.as_str())
                                            }
                                        })
                                        .unwrap_or(false)
                                })
                                .take(take_limit.unwrap_or(usize::MAX))
                                .collect();
                            candidates = verified;
                        }
                    } else {
                        // FILTER: intersect with index candidates
                        let index_set: std::collections::HashSet<u64> =
                            candidates_from_index.into_iter().collect();
                        candidates.retain(|h| index_set.contains(h));
                        // Verify remaining candidates using GiST cached text
                        if let Some(gist) = db.text_indexes.get(field) {
                            candidates = gist.verify(&candidates, pattern, take_limit);
                        } else {
                            candidates.retain(|&h| {
                                let v = db.get_payload(h)
                                    .and_then(|p| json_path_get(field, &p));
                                v.as_ref()
                                    .and_then(|v| v.as_str())
                                    .map(|s| {
                                        if *case_insensitive {
                                            ilike_matches(s, pattern)
                                        } else {
                                            s.contains(pattern.as_str())
                                        }
                                    })
                                    .unwrap_or(false)
                            });
                        }
                    }
                } else {
                    // No index — brute-force payload scan with proper wildcard matching.
                    // ilike_matches handles % wildcards case-insensitively;
                    // like_matches does the same but case-sensitively.
                    candidates.retain(|&h| {
                        let v = db.get_payload(h)
                            .and_then(|p| json_path_get(field, &p));
                        v.as_ref()
                            .and_then(|v| v.as_str())
                            .map(|s| {
                                if *case_insensitive {
                                    ilike_matches(s, pattern)
                                } else {
                                    like_matches(s, pattern)
                                }
                            })
                            .unwrap_or(false)
                    });
                }
            }

            // ── Spatial filters ──────────────────────────────────────────────
            //
            // Each spatial step can act as either a STARTER or a FILTER:
            //
            // • STARTER (candidates is empty): the SQL compiler emitted this step
            //   before any Collection step so the grid produces the initial candidate
            //   list directly.  Cost: O(grid_result) — typically tens of nodes.
            //
            // • FILTER (candidates non-empty): classic intersect-with-grid path.
            //   Cost: O(grid_result) for HashSet build + O(candidates) for retain.
            //
            // The no-grid fallback for a STARTER expands to all_hashes() and brute-
            // forces, which is slow but correct.  Production use should always call
            // `db.build_spatial_index()` before running spatial queries.
            Step::StDWithin(lat, lon, distance_km) => {
                if let Some(grid) = db.spatial_grid() {
                    if candidates.is_empty() {
                        // STARTER: grid → exact Haversine (no large collection scan)
                        candidates = grid
                            .candidates_within_distance(*lat, *lon, *distance_km)
                            .into_iter()
                            .filter(|&h| {
                                grid.get_meta(h)
                                    .map(|m| {
                                        crate::geo::haversine_km(
                                            m.centroid_lat,
                                            m.centroid_lon,
                                            *lat,
                                            *lon,
                                        ) <= *distance_km
                                    })
                                    .unwrap_or(false)
                            })
                            .collect();
                    } else {
                        // FILTER: intersect current candidates with grid result
                        let grid_set: HashSet<u64> = grid
                            .candidates_within_distance(*lat, *lon, *distance_km)
                            .into_iter()
                            .collect();
                        candidates.retain(|h| grid_set.contains(h));
                        candidates.retain(|&h| {
                            grid.get_meta(h)
                                .map(|m| {
                                    crate::geo::haversine_km(
                                        m.centroid_lat,
                                        m.centroid_lon,
                                        *lat,
                                        *lon,
                                    ) <= *distance_km
                                })
                                .unwrap_or(false)
                        });
                    }
                } else {
                    if candidates.is_empty() {
                        candidates = db.all_hashes();
                    }
                    candidates.retain(|&h| {
                        db.get_payload(h)
                            .and_then(|p| crate::geo::extract_centroid(&p))
                            .map(|(clat, clon)| {
                                crate::geo::haversine_km(clat, clon, *lat, *lon) <= *distance_km
                            })
                            .unwrap_or(false)
                    });
                }
            }
            Step::StContainsPoint(lat, lon) => {
                if let Some(grid) = db.spatial_grid() {
                    if candidates.is_empty() {
                        // STARTER: grid → exact polygon check
                        candidates = grid
                            .candidates_containing_point(*lat, *lon)
                            .into_iter()
                            .filter(|&h| {
                                db.get_payload(h)
                                    .map(|p| crate::geo::geom_contains_point(&p, *lat, *lon))
                                    .unwrap_or(false)
                            })
                            .collect();
                    } else {
                        // FILTER
                        let grid_set: HashSet<u64> = grid
                            .candidates_containing_point(*lat, *lon)
                            .into_iter()
                            .collect();
                        candidates.retain(|h| grid_set.contains(h));
                        candidates.retain(|&h| {
                            db.get_payload(h)
                                .map(|p| crate::geo::geom_contains_point(&p, *lat, *lon))
                                .unwrap_or(false)
                        });
                    }
                } else {
                    if candidates.is_empty() {
                        candidates = db.all_hashes();
                    }
                    candidates.retain(|&h| {
                        db.get_payload(h)
                            .map(|p| crate::geo::geom_contains_point(&p, *lat, *lon))
                            .unwrap_or(false)
                    });
                }
            }
            Step::StWithin(ring) => {
                // Compute query polygon bbox once
                let (mut qmin_lat, mut qmin_lon) = (f64::MAX, f64::MAX);
                let (mut qmax_lat, mut qmax_lon) = (f64::MIN, f64::MIN);
                for pt in ring.iter() {
                    qmin_lat = qmin_lat.min(pt[0]);
                    qmax_lat = qmax_lat.max(pt[0]);
                    qmin_lon = qmin_lon.min(pt[1]);
                    qmax_lon = qmax_lon.max(pt[1]);
                }
                if let Some(grid) = db.spatial_grid() {
                    if candidates.is_empty() {
                        // STARTER
                        candidates = grid
                            .candidates_in_bbox(qmin_lat, qmin_lon, qmax_lat, qmax_lon)
                            .into_iter()
                            .filter(|&h| {
                                if let Some(m) = grid.get_meta(h) {
                                    if !(m.bbox_min_lat >= qmin_lat
                                        && m.bbox_max_lat <= qmax_lat
                                        && m.bbox_min_lon >= qmin_lon
                                        && m.bbox_max_lon <= qmax_lon)
                                    {
                                        return false;
                                    }
                                }
                                db.get_payload(h)
                                    .map(|p| crate::geo::geom_within_polygon(&p, ring))
                                    .unwrap_or(false)
                            })
                            .collect();
                    } else {
                        // FILTER
                        let grid_set: HashSet<u64> = grid
                            .candidates_in_bbox(qmin_lat, qmin_lon, qmax_lat, qmax_lon)
                            .into_iter()
                            .collect();
                        candidates.retain(|h| grid_set.contains(h));
                        candidates.retain(|&h| {
                            if let Some(m) = grid.get_meta(h) {
                                if !(m.bbox_min_lat >= qmin_lat
                                    && m.bbox_max_lat <= qmax_lat
                                    && m.bbox_min_lon >= qmin_lon
                                    && m.bbox_max_lon <= qmax_lon)
                                {
                                    return false;
                                }
                            }
                            db.get_payload(h)
                                .map(|p| crate::geo::geom_within_polygon(&p, ring))
                                .unwrap_or(false)
                        });
                    }
                } else {
                    if candidates.is_empty() {
                        candidates = db.all_hashes();
                    }
                    candidates.retain(|&h| {
                        db.get_payload(h)
                            .map(|p| crate::geo::geom_within_polygon(&p, ring))
                            .unwrap_or(false)
                    });
                }
            }
            Step::StContains(ring) => {
                let (mut qmin_lat, mut qmin_lon) = (f64::MAX, f64::MAX);
                let (mut qmax_lat, mut qmax_lon) = (f64::MIN, f64::MIN);
                for pt in ring.iter() {
                    qmin_lat = qmin_lat.min(pt[0]);
                    qmax_lat = qmax_lat.max(pt[0]);
                    qmin_lon = qmin_lon.min(pt[1]);
                    qmax_lon = qmax_lon.max(pt[1]);
                }
                if let Some(grid) = db.spatial_grid() {
                    if candidates.is_empty() {
                        // STARTER
                        candidates = grid
                            .candidates_in_bbox(qmin_lat, qmin_lon, qmax_lat, qmax_lon)
                            .into_iter()
                            .filter(|&h| {
                                if let Some(m) = grid.get_meta(h) {
                                    if !(m.bbox_min_lat <= qmin_lat
                                        && m.bbox_max_lat >= qmax_lat
                                        && m.bbox_min_lon <= qmin_lon
                                        && m.bbox_max_lon >= qmax_lon)
                                    {
                                        return false;
                                    }
                                }
                                db.get_payload(h)
                                    .map(|p| crate::geo::geom_contains_polygon(&p, ring))
                                    .unwrap_or(false)
                            })
                            .collect();
                    } else {
                        // FILTER
                        let grid_set: HashSet<u64> = grid
                            .candidates_in_bbox(qmin_lat, qmin_lon, qmax_lat, qmax_lon)
                            .into_iter()
                            .collect();
                        candidates.retain(|h| grid_set.contains(h));
                        candidates.retain(|&h| {
                            if let Some(m) = grid.get_meta(h) {
                                if !(m.bbox_min_lat <= qmin_lat
                                    && m.bbox_max_lat >= qmax_lat
                                    && m.bbox_min_lon <= qmin_lon
                                    && m.bbox_max_lon >= qmax_lon)
                                {
                                    return false;
                                }
                            }
                            db.get_payload(h)
                                .map(|p| crate::geo::geom_contains_polygon(&p, ring))
                                .unwrap_or(false)
                        });
                    }
                } else {
                    if candidates.is_empty() {
                        candidates = db.all_hashes();
                    }
                    candidates.retain(|&h| {
                        db.get_payload(h)
                            .map(|p| crate::geo::geom_contains_polygon(&p, ring))
                            .unwrap_or(false)
                    });
                }
            }
            Step::StIntersects(ring) => {
                let (mut qmin_lat, mut qmin_lon) = (f64::MAX, f64::MAX);
                let (mut qmax_lat, mut qmax_lon) = (f64::MIN, f64::MIN);
                for pt in ring.iter() {
                    qmin_lat = qmin_lat.min(pt[0]);
                    qmax_lat = qmax_lat.max(pt[0]);
                    qmin_lon = qmin_lon.min(pt[1]);
                    qmax_lon = qmax_lon.max(pt[1]);
                }
                if let Some(grid) = db.spatial_grid() {
                    if candidates.is_empty() {
                        candidates = grid
                            .candidates_in_bbox(qmin_lat, qmin_lon, qmax_lat, qmax_lon)
                            .into_iter()
                            .filter(|&h| {
                                db.get_payload(h)
                                    .map(|p| crate::geo::geom_intersects_polygon(&p, ring))
                                    .unwrap_or(false)
                            })
                            .collect();
                    } else {
                        let grid_set: HashSet<u64> = grid
                            .candidates_in_bbox(qmin_lat, qmin_lon, qmax_lat, qmax_lon)
                            .into_iter()
                            .collect();
                        candidates.retain(|h| grid_set.contains(h));
                        candidates.retain(|&h| {
                            db.get_payload(h)
                                .map(|p| crate::geo::geom_intersects_polygon(&p, ring))
                                .unwrap_or(false)
                        });
                    }
                } else {
                    if candidates.is_empty() {
                        candidates = db.all_hashes();
                    }
                    candidates.retain(|&h| {
                        db.get_payload(h)
                            .map(|p| crate::geo::geom_intersects_polygon(&p, ring))
                            .unwrap_or(false)
                    });
                }
            }
            Step::StDistance(field, lat, lon, max_km) => {
                // ST_Distance(field, POINT(lon lat), max_km)
                if candidates.is_empty() {
                    candidates = db.all_hashes();
                }
                candidates.retain(|&h| {
                    db.get_payload(h)
                        .and_then(|p| p.get(&*field).cloned())
                        .and_then(|geom| {
                            crate::geo::distance_km(
                                &geom,
                                &serde_json::json!({
                                    "type": "Point",
                                    "coordinates": [lon, lat]
                                }),
                            )
                        })
                        .map(|d| d < *max_km)
                        .unwrap_or(false)
                });
            }
            Step::StLength(field, min_km) => {
                // ST_Length(field) > min_km
                if candidates.is_empty() {
                    candidates = db.all_hashes();
                }
                candidates.retain(|&h| {
                    db.get_payload(h)
                        .and_then(|p| p.get(&*field).cloned())
                        .and_then(|geom| crate::geo::length_km(&geom))
                        .map(|l| l > *min_km)
                        .unwrap_or(false)
                });
            }
            Step::StArea(field, min_km2) => {
                // ST_Area(field) > min_km2
                if candidates.is_empty() {
                    candidates = db.all_hashes();
                }
                candidates.retain(|&h| {
                    db.get_payload(h)
                        .and_then(|p| p.get(&*field).cloned())
                        .and_then(|geom| crate::geo::area_km2(&geom))
                        .map(|a| a > *min_km2)
                        .unwrap_or(false)
                });
            }
            Step::VectorNear { field, query, k } => {
                use crate::vector::{CosineDistance, Distance};
                if let Some(field_vecs) = db.vector_field(field) {
                    // ── HNSW fast path ────────────────────────────────────────
                    if candidates.is_empty() {
                        if let Some(hnsw) = db.hnsw_index(field) {
                            // HNSW STARTER: approximate search over all vectors.
                            let ef = (*k * 3).max(50);
                            candidates =
                                hnsw.search::<CosineDistance>(query, field_vecs, *k, ef);
                            // Skip to next step — HNSW result is already top-k.
                            continue;
                        }
                    }
                    // ── Flat-scan fallback ────────────────────────────────────
                    let mut scored: Vec<(u64, f32)> = if candidates.is_empty() {
                        // STARTER: scan all vectors in this field
                        field_vecs
                            .iter()
                            .map(|(&h, v)| (h, CosineDistance::eval(query, v)))
                            .collect()
                    } else {
                        // FILTER: re-rank only the existing candidates
                        let set: HashSet<u64> = candidates.iter().copied().collect();
                        field_vecs
                            .iter()
                            .filter(|(h, _)| set.contains(h))
                            .map(|(&h, v)| (h, CosineDistance::eval(query, v)))
                            .collect()
                    };
                    scored.sort_unstable_by(|a, b| {
                        a.1.partial_cmp(&b.1)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                    scored.truncate(*k);
                    candidates = scored.into_iter().map(|(h, _)| h).collect();
                } else {
                    candidates = vec![];
                }
            }

            Step::Bm25Filter(field, query, min_score) => {
                // BM25(field, 'query') > min_score
                if candidates.is_empty() {
                    candidates = db.all_hashes();
                }
                let min_score = *min_score;
                candidates.retain(|&h| {
                    if let Some(hits) = db.bm25_indexes.get(field) {
                        let results = hits.search(query, 100);
                        results.iter().any(|r| r.doc_id == h && r.score > min_score)
                    } else {
                        false
                    }
                });
            }
            Step::Bm25Sort(field, query, ascending) => {
                if let Some(index) = db.bm25_indexes.get(field) {
                    let hits = index.search(query, candidates.len().max(100));
                    let asc = *ascending;
                    let score_map: HashMap<u64, f64> =
                        hits.iter().map(|h| (h.doc_id, h.score)).collect();
                    candidates.sort_by(|&a, &b| {
                        let sa = score_map.get(&a).copied().unwrap_or(0.0);
                        let sb = score_map.get(&b).copied().unwrap_or(0.0);
                        let ord = sa.partial_cmp(&sb).unwrap();
                        if asc {
                            ord
                        } else {
                            ord.reverse()
                        }
                    });
                }
            }
            Step::Bm25Score(_, _) => {
                // BM25 score annotation happens in collect(), not execute()
            }

            // ── Set algebra ──────────────────────────────────────────────────
            Step::Intersect(sub_steps) => {
                let other: HashSet<u64> = execute(db, sub_steps).into_iter().collect();
                candidates.retain(|h| other.contains(h));
            }
            Step::Union(sub_steps) => {
                let other = execute(db, sub_steps);
                let existing: HashSet<u64> = candidates.iter().copied().collect();
                for h in other {
                    if !existing.contains(&h) {
                        candidates.push(h);
                    }
                }
            }
            Step::Subtract(sub_steps) => {
                let other: HashSet<u64> = execute(db, sub_steps).into_iter().collect();
                candidates.retain(|h| !other.contains(h));
            }

            // ── Shaping ──────────────────────────────────────────────────────
            Step::Sort(columns) => {
                // ── Btree-assisted sort (zero payload reads) ────────────────
                // When a btree index exists for the sort field, iterate it in
                // order and filter by the current candidate set.  Zero payload
                // reads; cost = HashSet<candidates> build + btree scan.
                // Applies only to single-column sorts.
                if columns.len() == 1 {
                    let (sort_field, asc) = &columns[0];
                    if let Some(coll) = current_coll_hash {
                        if let Some(idx) = db.field_index(coll, sort_field) {
                            let candidate_set: HashSet<u64> =
                                candidates.iter().copied().collect();
                            let limit = find_take_limit(remaining).unwrap_or(usize::MAX);
                            let mut sorted_result: Vec<u64> =
                                Vec::with_capacity(limit.min(candidate_set.len()));
                            if *asc {
                                'asc: for ids in idx.values() {
                                    for &h in ids {
                                        if candidate_set.contains(&h) {
                                            sorted_result.push(h);
                                            if sorted_result.len() >= limit {
                                                break 'asc;
                                            }
                                        }
                                    }
                                }
                            } else {
                                'desc: for ids in idx.values().rev() {
                                    for &h in ids {
                                        if candidate_set.contains(&h) {
                                            sorted_result.push(h);
                                            if sorted_result.len() >= limit {
                                                break 'desc;
                                            }
                                        }
                                    }
                                }
                            }
                            candidates = sorted_result;
                            continue; // Sort done — skip payload-read fallback below.
                        }
                    }
                }
                // ── Pre-compute sort keys (fallback when no btree index) ────
                // Pre-compute sort keys once per candidate to avoid calling
                // get_payload O(n log n) times in the sort comparator.
                // For large payloads (e.g. GeoJSON polygons), this reduces
                // redundant disk reads from O(n log n) to O(n).
                //
                // Batch-read path: when there are many candidates and all sort
                // fields are simple top-level names, sort by payload_offset and
                // read in sequential groups — often just ONE pread instead of
                // O(N) individual preads.
                let sort_fields: Vec<String> = columns.iter().map(|(f, _)| f.clone()).collect();
                let use_fast = sort_fields.iter().all(|f| is_simple_field(f));
                // Threshold: only batch-read when the candidate set is large enough
                // that the sort/HashMap overhead is worth paying.
                const BATCH_THRESHOLD: usize = 64;
                let raw_map: Option<HashMap<u64, Vec<u8>>> =
                    if use_fast && candidates.len() >= BATCH_THRESHOLD {
                        Some(db.read_raw_payloads_batched(&candidates))
                    } else {
                        None
                    };
                // Pair each candidate with its pre-computed sort keys, sort, then unzip.
                let mut keyed: Vec<(u64, Vec<Option<Value>>)> = candidates
                    .iter()
                    .map(|&h| {
                        let vals: Vec<Option<Value>> = if let Some(ref rm) = raw_map {
                            // Batch path: slice bytes from the pre-fetched map.
                            if let Some(bytes) = rm.get(&h) {
                                let mut map = extract_fields_by_search(bytes, &sort_fields);
                                sort_fields.iter().map(|f| map.remove(f.as_str())).collect()
                            } else {
                                sort_fields.iter().map(|_| None).collect()
                            }
                        } else if use_fast {
                            if let Some((head, tail)) = db.get_payload_head_tail(h, 512, 16 * 1024) {
                                let mut map = extract_fields_by_search(&tail, &sort_fields);
                                let missing: Vec<String> = sort_fields.iter()
                                    .filter(|f| !map.contains_key(f.as_str()))
                                    .cloned()
                                    .collect();
                                if !missing.is_empty() {
                                    for (k, v) in extract_fields_by_search(&head, &missing) {
                                        map.entry(k).or_insert(v);
                                    }
                                }
                                sort_fields.iter().map(|f| map.remove(f.as_str())).collect()
                            } else {
                                sort_fields.iter().map(|_| None).collect()
                            }
                        } else if let Some(payload) = db.get_payload(h) {
                            sort_fields.iter().map(|f| json_path_get(f, &payload)).collect()
                        } else {
                            sort_fields.iter().map(|_| None).collect()
                        };
                        (h, vals)
                    })
                    .collect();
                keyed.sort_by(|(_, ka), (_, kb)| {
                    for (i, (_, asc)) in columns.iter().enumerate() {
                        let va = ka.get(i).and_then(|v| v.as_ref());
                        let vb = kb.get(i).and_then(|v| v.as_ref());
                        let ord = cmp_json(va, vb);
                        if ord != std::cmp::Ordering::Equal {
                            return if *asc { ord } else { ord.reverse() };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
                candidates = keyed.into_iter().map(|(h, _)| h).collect();
            }
            Step::SortByVector { field, query, metric } => {
                use crate::vector::{CosineDistance, L2Distance, DotProduct, L1Distance, Distance};
                if let Some(field_vecs) = db.vector_field(field) {
                    let mut scored: Vec<(u64, f32)> = candidates.iter().map(|&h| {
                        let dist = field_vecs.get(&h).map(|v| match metric {
                            VecMetric::Cosine => CosineDistance::eval(query, v),
                            VecMetric::L2     => L2Distance::eval(query, v),
                            // Negate dot product so higher similarity → lower sort key → first.
                            VecMetric::Dot    => -DotProduct::eval(query, v),
                            VecMetric::L1     => L1Distance::eval(query, v),
                        }).unwrap_or(f32::MAX);
                        (h, dist)
                    }).collect();
                    scored.sort_unstable_by(|a, b| {
                        a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                    });
                    candidates = scored.into_iter().map(|(h, _)| h).collect();
                }
            }
            Step::SortByExpr { expr, ascending } => {
                use crate::vector::{CosineDistance, L2Distance, DotProduct, L1Distance, Distance};

                // Pre-compute BM25 score maps (one search per unique field+query pair).
                let mut bm25_keys: HashSet<(String, String)> = HashSet::new();
                gather_bm25_keys(expr, &mut bm25_keys);
                let bm25_maps: HashMap<(String, String), HashMap<u64, f64>> = bm25_keys
                    .into_iter()
                    .filter_map(|(field, query)| {
                        let index = db.bm25_indexes.get(&field)?;
                        let k = candidates.len().max(100);
                        let results = index.search(&query, k);
                        let m: HashMap<u64, f64> =
                            results.iter().map(|h| (h.doc_id, h.score)).collect();
                        Some(((field, query), m))
                    })
                    .collect();

                // Pre-compute vector score maps keyed by (metric, field).
                let mut vec_keys: HashMap<(VecMetric, String), Vec<f32>> = HashMap::new();
                gather_vector_keys(expr, &mut vec_keys);
                let vec_maps: HashMap<(VecMetric, String), HashMap<u64, f32>> = vec_keys
                    .into_iter()
                    .filter_map(|((metric, field), query_vec)| {
                        let field_vecs = db.vector_field(&field)?;
                        let m: HashMap<u64, f32> = candidates
                            .iter()
                            .map(|&h| {
                                let score = field_vecs.get(&h).map(|v| match &metric {
                                    VecMetric::Cosine => 1.0 - CosineDistance::eval(&query_vec, v),
                                    VecMetric::L2     => L2Distance::eval(&query_vec, v),
                                    VecMetric::Dot    => DotProduct::eval(&query_vec, v),
                                    VecMetric::L1     => L1Distance::eval(&query_vec, v),
                                }).unwrap_or(0.0);
                                (h, score)
                            })
                            .collect();
                        Some(((metric, field), m))
                    })
                    .collect();

                let asc = *ascending;
                let mut scored: Vec<(u64, f64)> = candidates
                    .iter()
                    .map(|&h| {
                        let payload_owned = db.get_payload(h).unwrap_or(Value::Null);
                        let s = eval_score(
                            expr, h, &payload_owned, db,
                            &bm25_maps, &vec_maps,
                        );
                        (h, s)
                    })
                    .collect();
                scored.sort_by(|a, b| {
                    let ord = a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal);
                    if asc { ord } else { ord.reverse() }
                });
                candidates = scored.into_iter().map(|(h, _)| h).collect();
            }
            Step::Skip(n) => {
                let n = *n;
                if n >= candidates.len() {
                    candidates.clear();
                } else {
                    candidates.drain(..n);
                }
            }
            Step::Take(n) => {
                candidates.truncate(*n);
            }
            // Select / GroupBy / Having / Distinct are projection / shaping steps
            // handled in Set::collect(), not here.
            Step::Select(_) | Step::GroupBy(_) | Step::Having(_) | Step::Distinct => {}
        }
    }

    candidates
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Look ahead in remaining steps to find a Take limit.
/// Skips Skip and Select which don't affect the limit.
fn find_take_limit(remaining_steps: &[Step]) -> Option<usize> {
    for step in remaining_steps {
        match step {
            Step::Take(n) => return Some(*n),
            Step::Skip(_) | Step::Select(_) | Step::Distinct | Step::GroupBy(_) | Step::Having(_) => continue,
            _ => break,
        }
    }
    None
}

// ── Traversal aggregation ─────────────────────────────────────────────────────

/// A single complete path row from a multi-hop traversal.
/// Maps bind_name → node payload at that hop.
pub type PathRow = HashMap<String, Value>;

/// Math expression evaluated per [`PathRow`] for aggregation.
#[derive(Clone, Debug)]
pub enum MathExpr {
    /// Access field from a bound variable: `var.field`
    VarField { var: String, field: String },
    /// Literal numeric constant
    Literal(f64),
    /// Multiplication
    Mul(Box<MathExpr>, Box<MathExpr>),
    /// Addition
    Add(Box<MathExpr>, Box<MathExpr>),
    /// Subtraction
    Sub(Box<MathExpr>, Box<MathExpr>),
    /// Division (zero-safe: returns 0.0 if divisor is 0)
    Div(Box<MathExpr>, Box<MathExpr>),
}

impl MathExpr {
    pub fn references_var(&self, var: &str) -> bool {
        match self {
            MathExpr::VarField { var: v, .. } => v == var,
            MathExpr::Literal(_) => false,
            MathExpr::Mul(a, b) | MathExpr::Add(a, b)
            | MathExpr::Sub(a, b) | MathExpr::Div(a, b) => {
                a.references_var(var) || b.references_var(var)
            }
        }
    }

    pub fn eval(&self, row: &PathRow) -> f64 {
        match self {
            MathExpr::VarField { var, field } => row
                .get(var.as_str())
                .and_then(|v| v.get(field.as_str()))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
            MathExpr::Literal(n) => *n,
            MathExpr::Mul(a, b) => a.eval(row) * b.eval(row),
            MathExpr::Add(a, b) => a.eval(row) + b.eval(row),
            MathExpr::Sub(a, b) => a.eval(row) - b.eval(row),
            MathExpr::Div(a, b) => {
                let d = b.eval(row);
                if d == 0.0 { 0.0 } else { a.eval(row) / d }
            }
        }
    }
}

/// Comparison operator for CASE WHEN conditions.
#[derive(Clone, Debug)]
pub enum CmpOp {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
}

/// A single CASE WHEN condition: `var.field op literal`.
#[derive(Clone, Debug)]
pub struct CaseCond {
    pub var: String,
    pub field: String,
    pub op: CmpOp,
    pub val: Value,
}

impl CaseCond {
    pub fn references_var(&self, var: &str) -> bool {
        self.var == var
    }
}

/// A simple field comparison condition used in path predicates.
#[derive(Clone, Debug)]
pub struct SimpleCond {
    pub field: String,
    pub op:    CmpOp,
    pub val:   Value,
}

/// A path predicate applied to the nodes in a `MATCH SHORTEST` result.
#[derive(Clone, Debug)]
pub enum PathPredicate {
    /// At least one node satisfies the condition.
    Any    { var: String, cond: SimpleCond },
    /// Every node satisfies the condition.
    All    { var: String, cond: SimpleCond },
    /// No node satisfies the condition.
    None_  { var: String, cond: SimpleCond },
    /// Exactly one node satisfies the condition.
    Single { var: String, cond: SimpleCond },
}

/// A compiled `SELECT … FROM MATCH SHORTEST` statement.
#[derive(Clone, Debug)]
pub struct ShortestSelectStmt {
    /// Full slug of the start node (e.g. `"characters/coby"`).
    pub from_slug:  String,
    /// Full slug of the end node.
    pub to_slug:    String,
    /// Variable name bound to the start node in the SELECT list.
    pub start_bind: String,
    /// Variable name bound to the end node in the SELECT list.
    pub end_bind:   String,
    /// Optional variable name bound to the path object (`nodes`, `edges`, `length`, intrinsics).
    pub path_bind:  Option<String>,
    /// SELECT expressions: `(expression, output_alias)`.
    pub returns:    Vec<(MatchAggReturn, String)>,
    /// Path predicates applied after BFS (ANY/ALL/NONE/SINGLE).
    pub predicates: Vec<PathPredicate>,
    /// Optional ORDER BY.
    pub order_by:   Option<(String, bool)>,
    /// Optional LIMIT.
    pub limit:      Option<usize>,
}

/// A source in a multi-FROM query.
#[derive(Clone, Debug)]
pub enum FromSource {
    /// `FROM MATCH (a:col)-[r:edge]->(b:col) [WHERE …]`
    Match(MatchAggStmt),
    /// `FROM MATCH SHORTEST (a)-[r*]->(b) WHERE a._key = 'x' AND b._key = 'y'`
    Shortest(ShortestSelectStmt),
    /// `FROM collection_name [AS alias]`
    Collection { alias: String, name_hash: u64 },
}

/// A compiled `SELECT … FROM source1, source2, …` multi-FROM statement.
#[derive(Clone, Debug)]
pub struct MultiFromStmt {
    pub sources:  Vec<FromSource>,
    pub returns:  Vec<(MatchAggReturn, String)>,
    pub order_by: Option<(String, bool)>,
    pub limit:    Option<usize>,
}

/// Return expression in a MATCH aggregate RETURN clause.
#[derive(Clone, Debug)]
pub enum MatchAggReturn {
    /// `var.field` — scalar field from a bound variable (takes first row in group)
    Field { var: String, field: String },
    /// `SUM(math_expr)`
    Sum(MathExpr),
    /// `COUNT(*)`
    Count,
    /// `AVG(math_expr)`
    Avg(MathExpr),
    /// `MIN(math_expr)`
    Min(MathExpr),
    /// `MAX(math_expr)`
    Max(MathExpr),
    /// `PATH_AVG(var.field)` — average of JSON array elements in field (first row)
    PathAvg { var: String, field: String },
    /// `PATH_SUM(var.field)` — sum of JSON array elements in field (first row)
    PathSum { var: String, field: String },
    /// `PATH_MIN(var.field)` — min of JSON array elements in field (first row)
    PathMin { var: String, field: String },
    /// `PATH_MAX(var.field)` — max of JSON array elements in field (first row)
    PathMax { var: String, field: String },
    /// `PATH_PRODUCT(var.field)` — product of JSON array elements in field (first row)
    PathProduct { var: String, field: String },
    /// `PATH_FIRST(var.field)` — first element of JSON array in field (first row)
    PathFirst { var: String, field: String },
    /// `PATH_LAST(var.field)` — last element of JSON array in field (first row)
    PathLast { var: String, field: String },
    /// `CASE WHEN var.field op literal THEN literal ... [ELSE literal] END`
    Case { branches: Vec<(CaseCond, Value)>, else_val: Value },
    /// `AGE_DAYS(var.field)` — whole days elapsed since field's Unix epoch
    AgeDays { var: String, field: String },
    /// `AGE_HOURS(var.field)` — whole hours elapsed since field's Unix epoch
    AgeHours { var: String, field: String },
    /// `NOW()` — current Unix timestamp in seconds as `i64`
    Now,
    /// `JSON_ARRAY_LENGTH(var.field)` — length of a JSON array field
    JsonArrayLen { var: String, field: String },
}

impl MatchAggReturn {
    /// Returns true if this expression reads any field from the given variable binding.
    /// Used to decide whether to materialise the start node's payload.
    pub fn references_var(&self, var: &str) -> bool {
        match self {
            MatchAggReturn::Field       { var: v, .. }
            | MatchAggReturn::PathAvg   { var: v, .. }
            | MatchAggReturn::PathSum   { var: v, .. }
            | MatchAggReturn::PathMin   { var: v, .. }
            | MatchAggReturn::PathMax   { var: v, .. }
            | MatchAggReturn::PathProduct { var: v, .. }
            | MatchAggReturn::PathFirst { var: v, .. }
            | MatchAggReturn::PathLast  { var: v, .. }
            | MatchAggReturn::AgeDays   { var: v, .. }
            | MatchAggReturn::AgeHours  { var: v, .. }
            | MatchAggReturn::JsonArrayLen { var: v, .. } => v == var,
            // MathExpr-based aggregates may reference the var inside their expression.
            MatchAggReturn::Sum(e) | MatchAggReturn::Avg(e)
            | MatchAggReturn::Min(e) | MatchAggReturn::Max(e) => e.references_var(var),
            MatchAggReturn::Case { branches, .. } => {
                branches.iter().any(|(cond, _)| cond.references_var(var))
            }
            MatchAggReturn::Count | MatchAggReturn::Now => false,
        }
    }

    /// Evaluate this expression over a group of [`PathRow`]s.
    pub fn eval_group(&self, rows: &[PathRow]) -> Value {
        match self {
            MatchAggReturn::Field { var, field } => rows
                .first()
                .and_then(|r| r.get(var.as_str()))
                .and_then(|v| v.get(field.as_str()))
                .cloned()
                .unwrap_or(Value::Null),
            MatchAggReturn::Sum(expr) => {
                let sum: f64 = rows.iter().map(|r| expr.eval(r)).sum();
                serde_json::json!(sum)
            }
            MatchAggReturn::Count => serde_json::json!(rows.len() as i64),
            MatchAggReturn::Avg(expr) => {
                if rows.is_empty() { return Value::Null; }
                let sum: f64 = rows.iter().map(|r| expr.eval(r)).sum();
                serde_json::json!(sum / rows.len() as f64)
            }
            MatchAggReturn::Min(expr) => {
                let min = rows.iter().map(|r| expr.eval(r)).fold(f64::INFINITY, f64::min);
                if min.is_infinite() { Value::Null } else { serde_json::json!(min) }
            }
            MatchAggReturn::Max(expr) => {
                let max = rows.iter().map(|r| expr.eval(r)).fold(f64::NEG_INFINITY, f64::max);
                if max.is_infinite() { Value::Null } else { serde_json::json!(max) }
            }
            MatchAggReturn::PathAvg { var, field } => {
                let nums = path_field_nums(rows, var, field);
                if nums.is_empty() { Value::Null } else { serde_json::json!(nums.iter().sum::<f64>() / nums.len() as f64) }
            }
            MatchAggReturn::PathSum { var, field } => {
                let nums = path_field_nums(rows, var, field);
                if nums.is_empty() { Value::Null } else { serde_json::json!(nums.iter().sum::<f64>()) }
            }
            MatchAggReturn::PathMin { var, field } => {
                let nums = path_field_nums(rows, var, field);
                let min = nums.iter().cloned().fold(f64::INFINITY, f64::min);
                if min.is_infinite() { Value::Null } else { serde_json::json!(min) }
            }
            MatchAggReturn::PathMax { var, field } => {
                let nums = path_field_nums(rows, var, field);
                let max = nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
                if max.is_infinite() { Value::Null } else { serde_json::json!(max) }
            }
            MatchAggReturn::PathProduct { var, field } => {
                let nums = path_field_nums(rows, var, field);
                if nums.is_empty() { Value::Null } else { serde_json::json!(nums.iter().fold(1.0_f64, |acc, x| acc * x)) }
            }
            MatchAggReturn::PathFirst { var, field } => {
                rows.first()
                    .and_then(|r| r.get(var.as_str()))
                    .and_then(|v| v.get(field.as_str()))
                    .and_then(|v| v.as_array())
                    .and_then(|a| a.first())
                    .cloned()
                    .unwrap_or(Value::Null)
            }
            MatchAggReturn::PathLast { var, field } => {
                rows.first()
                    .and_then(|r| r.get(var.as_str()))
                    .and_then(|v| v.get(field.as_str()))
                    .and_then(|v| v.as_array())
                    .and_then(|a| a.last())
                    .cloned()
                    .unwrap_or(Value::Null)
            }
            MatchAggReturn::Case { branches, else_val } => {
                let row = match rows.first() {
                    Some(r) => r,
                    None => return else_val.clone(),
                };
                for (cond, then_val) in branches {
                    let actual = row.get(cond.var.as_str())
                        .and_then(|v| v.get(cond.field.as_str()))
                        .cloned()
                        .unwrap_or(Value::Null);
                    if eval_cmp(&actual, &cond.op, &cond.val) {
                        return then_val.clone();
                    }
                }
                else_val.clone()
            }
            MatchAggReturn::AgeDays { var, field } => {
                let v = rows.first()
                    .and_then(|r| r.get(var.as_str()))
                    .and_then(|v| v.get(field.as_str()))
                    .cloned()
                    .unwrap_or(Value::Null);
                match field_as_epoch(&v) {
                    None => Value::Null,
                    Some(epoch) => {
                        let now = now_secs();
                        serde_json::json!((now - epoch) / 86400)
                    }
                }
            }
            MatchAggReturn::AgeHours { var, field } => {
                let v = rows.first()
                    .and_then(|r| r.get(var.as_str()))
                    .and_then(|v| v.get(field.as_str()))
                    .cloned()
                    .unwrap_or(Value::Null);
                match field_as_epoch(&v) {
                    None => Value::Null,
                    Some(epoch) => {
                        let now = now_secs();
                        serde_json::json!((now - epoch) / 3600)
                    }
                }
            }
            MatchAggReturn::Now => serde_json::json!(now_secs()),
            MatchAggReturn::JsonArrayLen { var, field } => {
                rows.first()
                    .and_then(|r| r.get(var.as_str()))
                    .and_then(|v| v.get(field.as_str()))
                    .and_then(|v| v.as_array())
                    .map(|a| serde_json::json!(a.len() as i64))
                    .unwrap_or(Value::Null)
            }
        }
    }
}

// ── Private helpers ────────────────────────────────────────────────────────────

/// Extract all f64 elements from `row[var][field]` (first row, must be JSON array).
fn path_field_nums(rows: &[PathRow], var: &str, field: &str) -> Vec<f64> {
    rows.first()
        .and_then(|r| r.get(var))
        .and_then(|v| v.get(field))
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_f64()).collect())
        .unwrap_or_default()
}

/// Evaluate a comparison between a JSON value and an RHS literal.
fn eval_cmp(actual: &Value, op: &CmpOp, rhs: &Value) -> bool {
    match op {
        // Use numeric comparison for Eq/Neq to handle int vs float mismatch
        // (e.g. _depth stored as i64 but SQL literal parsed as f64).
        CmpOp::Eq  => actual == rhs || cmp_ordered(actual, rhs) == Some(std::cmp::Ordering::Equal),
        CmpOp::Neq => actual != rhs && cmp_ordered(actual, rhs) != Some(std::cmp::Ordering::Equal),
        CmpOp::Lt  => cmp_ordered(actual, rhs) == Some(std::cmp::Ordering::Less),
        CmpOp::Gt  => cmp_ordered(actual, rhs) == Some(std::cmp::Ordering::Greater),
        CmpOp::Lte => matches!(cmp_ordered(actual, rhs), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)),
        CmpOp::Gte => matches!(cmp_ordered(actual, rhs), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)),
    }
}

fn cmp_ordered(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return x.partial_cmp(&y);
    }
    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
        return Some(x.cmp(y));
    }
    None
}

/// Convert a JSON value (Unix int or "YYYY-MM-DD" string) to a Unix epoch (seconds).
fn field_as_epoch(v: &Value) -> Option<i64> {
    if let Some(n) = v.as_i64() { return Some(n); }
    if let Some(n) = v.as_f64() { return Some(n as i64); }
    if let Some(s) = v.as_str() {
        if s.len() == 10 {
            let parts: Vec<&str> = s.splitn(3, '-').collect();
            if parts.len() == 3 {
                if let (Ok(y), Ok(m), Ok(d)) = (
                    parts[0].parse::<i64>(),
                    parts[1].parse::<i64>(),
                    parts[2].parse::<i64>(),
                ) {
                    return Some(ymd_to_epoch(y, m, d));
                }
            }
        }
    }
    None
}

/// Proleptic Gregorian calendar date → Unix epoch seconds (midnight UTC).
fn ymd_to_epoch(y: i64, m: i64, d: i64) -> i64 {
    // Julian Day Number (JDN) algorithm by Fliegel & Van Flandern (1968).
    let jdn = (1461 * (y + 4800 + (m - 14) / 12)) / 4
        + (367 * (m - 2 - 12 * ((m - 14) / 12))) / 12
        - (3 * ((y + 4900 + (m - 14) / 12) / 100)) / 4
        + d - 32075;
    (jdn - 2_440_588) * 86400 // 2440588 = JDN of 1970-01-01
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Starting point for a MATCH aggregate statement.
#[derive(Clone, Debug)]
pub enum MatchAggStart {
    /// A specific node identified by its slug hash.
    Slug(u64),
    /// All nodes in the named collection (collection name hash).
    Collection(u64),
    /// Every node in the database.
    All,
}

/// One hop in a MATCH aggregate path.
#[derive(Clone, Debug)]
pub struct HopSpec {
    /// Hash of the required edge type (`0` = any type).
    pub edge_type_hash: u64,
    /// Name to bind the destination node in [`PathRow`].
    pub node_bind: String,
    /// Optional edge binding — if set, that name is bound in [`PathRow`] to a JSON
    /// object exposing path intrinsics: `_depth`, `_path_keys`, `_path_strength`,
    /// `_avg_strength`, `_min_strength`, `_max_strength`.
    pub edge_bind: Option<String>,
    /// Minimum traversal depth (inclusive). 1 for a plain single-hop `-[:e]->`.
    pub min_depth: u32,
    /// Maximum traversal depth (inclusive). 1 for a plain single-hop `-[:e]->`.
    /// When `max_depth > 1`, `collect_paths` uses batch BFS instead of per-node DFS.
    pub max_depth: u32,
}

/// A fully parsed and compiled MATCH aggregate statement.
#[derive(Clone, Debug)]
pub struct MatchAggStmt {
    /// Where traversal begins.
    pub start: MatchAggStart,
    /// Variable name bound to the start node (e.g. `"a"` in `MATCH (a:posts)-...`).
    /// When `Some`, `collect_paths` binds the start node payload under this key in
    /// every returned [`PathRow`], making `a.title`, `a.id`, etc. available in
    /// SELECT / GROUP BY / RETURN expressions.
    pub start_var: Option<String>,
    /// Hop chain.
    pub hops: Vec<HopSpec>,
    /// `RETURN` clause: `(expression, output_alias)`.
    pub returns: Vec<(MatchAggReturn, String)>,
    /// `GROUP BY`: list of `(var, field)` pairs — supports multi-field grouping,
    /// e.g. `GROUP BY a.city, b.role`.
    pub group_by: Option<Vec<(String, String)>>,
    /// `ORDER BY`: `(alias, ascending)`.
    pub order_by: Option<(String, bool)>,
    /// `LIMIT n`.
    pub limit: Option<usize>,
    /// Post-traversal equality filters on hop-bound variables.
    /// `WHERE b.level = 1` → `[("b", "level", Value::Number(1))]`.
    /// Applied after topology traversal, before grouping.
    pub dest_where: Vec<(String, String, serde_json::Value)>,
}

/// Topology-only path skeleton returned by [`collect_raw_paths`].
/// Contains node hashes and slugs — no payload bytes are loaded.
pub(crate) struct RawPath {
    pub start_hash:        u64,
    pub dest_per_hop:      Vec<u64>,
    pub slugs_per_hop:     Vec<String>,
    pub strengths_per_hop: Vec<f32>,
}

/// Phase 1 of [`collect_paths`]: traverse the graph and return topology skeletons only.
/// No payload bytes are read — only node hash + slug lookups from the in-memory index.
///
/// Used by the GROUP BY fast path in [`execute_match_agg`] to avoid materialising
/// one full payload clone per result row (which OOMs on large collections).
pub(crate) fn collect_raw_paths(
    db: &CoreDB,
    starts: &[u64],
    hops: &[HopSpec],
    limit: Option<usize>,
) -> Vec<RawPath> {
    if hops.is_empty() || starts.is_empty() {
        return vec![];
    }

    struct Partial {
        start_hash:       u64,
        current_hash:     u64,
        dest_so_far:      Vec<u64>,
        slugs_so_far:     Vec<String>,
        strengths_so_far: Vec<f32>,
    }

    let mut in_flight: Vec<Partial> = starts
        .iter()
        .filter_map(|&h| {
            let node = db.node_data(h)?;
            Some(Partial {
                start_hash:       h,
                current_hash:     h,
                dest_so_far:      Vec::new(),
                slugs_so_far:     vec![node.slug.clone()],
                strengths_so_far: Vec::new(),
            })
        })
        .collect();

    for hop in hops {
        let mut next_in_flight: Vec<Partial> = Vec::new();

        if hop.max_depth == 1 {
            'single: for partial in in_flight {
                if let Some(edges) = db.fwd_edges(partial.current_hash) {
                    for e in edges {
                        if hop.edge_type_hash != 0 && e.edge_type != hop.edge_type_hash {
                            continue;
                        }
                        if let Some(node) = db.node_data(e.other) {
                            let mut d = partial.dest_so_far.clone();
                            let mut s = partial.slugs_so_far.clone();
                            let mut st = partial.strengths_so_far.clone();
                            d.push(e.other);
                            s.push(node.slug.clone());
                            st.push(e.strength);
                            next_in_flight.push(Partial {
                                start_hash:       partial.start_hash,
                                current_hash:     e.other,
                                dest_so_far:      d,
                                slugs_so_far:     s,
                                strengths_so_far: st,
                            });
                            if limit.map_or(false, |l| next_in_flight.len() >= l) {
                                break 'single;
                            }
                        }
                    }
                }
            }
        } else {
            // Multi-depth flat pair propagation.
            // Propagate ALL partials simultaneously as (partial_idx, current_hash) pairs.
            // No per-start visited set; no String cloning until result materialisation.
            let mut pairs: Vec<(usize, u64)> = in_flight
                .iter()
                .enumerate()
                .map(|(idx, p)| (idx, p.current_hash))
                .collect();

            let mut result_pairs: Vec<(usize, u64)> = Vec::new();

            for depth in 1..=hop.max_depth {
                let mut next_pairs: Vec<(usize, u64)> = Vec::new();
                for &(pidx, current_h) in &pairs {
                    if let Some(edges) = db.fwd_edges(current_h) {
                        for e in edges {
                            if hop.edge_type_hash != 0 && e.edge_type != hop.edge_type_hash {
                                continue;
                            }
                            if db.node_data(e.other).is_some() {
                                next_pairs.push((pidx, e.other));
                            }
                        }
                    }
                }
                if depth >= hop.min_depth {
                    result_pairs.extend_from_slice(&next_pairs);
                    if limit.map_or(false, |l| result_pairs.len() >= l) {
                        result_pairs.truncate(limit.unwrap());
                        break;
                    }
                }
                pairs = next_pairs;
                if pairs.is_empty() {
                    break;
                }
            }

            for (pidx, dest_h) in result_pairs {
                let prior = &in_flight[pidx];
                let mut d = prior.dest_so_far.clone();
                d.push(dest_h);
                let dest_slug = db.node_data(dest_h)
                    .map(|nd| nd.slug.clone())
                    .unwrap_or_default();
                let mut s = prior.slugs_so_far.clone();
                s.push(dest_slug);
                let mut st = prior.strengths_so_far.clone();
                st.push(1.0);
                next_in_flight.push(Partial {
                    start_hash:       prior.start_hash,
                    current_hash:     dest_h,
                    dest_so_far:      d,
                    slugs_so_far:     s,
                    strengths_so_far: st,
                });
            }
        }

        in_flight = next_in_flight;
    }

    in_flight
        .into_iter()
        .filter(|p| p.dest_so_far.len() == hops.len())
        .map(|p| RawPath {
            start_hash:        p.start_hash,
            dest_per_hop:      p.dest_so_far,
            slugs_per_hop:     p.slugs_so_far,
            strengths_per_hop: p.strengths_so_far,
        })
        .collect()
}

/// Pure-topology BFS for GROUP BY COUNT(*) — returns `final_dest_hash → path_count`.
///
/// Merges the traversal frontier at every level so memory is O(unique_nodes_at_level)
/// rather than O(total_paths).  For 81,912 villages through `child_of*3` this
/// produces a 34-entry map (one entry per province) instead of 81,912 RawPath
/// structs — zero Vec/String allocation per individual path.
///
/// For multi-hop chains the output of each hop becomes the weighted frontier for
/// the next hop, so counts propagate correctly through any number of hops.
///
/// Used by [`execute_match_agg`] when all GROUP-BY keys and non-COUNT return
/// expressions reference only the final hop variable (and all dest_where conditions
/// do the same), making intermediate path data irrelevant.
fn collect_final_dest_counts(
    db: &CoreDB,
    starts: &[u64],
    hops: &[HopSpec],
) -> HashMap<u64, usize> {
    if hops.is_empty() || starts.is_empty() {
        return HashMap::new();
    }

    // Initial frontier: each valid start node contributes weight = 1.
    let mut frontier: HashMap<u64, usize> = starts
        .iter()
        .copied()
        .filter(|&h| db.node_data(h).is_some())
        .map(|h| (h, 1usize))
        .collect();

    let last_hop_idx = hops.len() - 1;

    for (hop_idx, hop) in hops.iter().enumerate() {
        // BFS through this hop's depth range, merging node weights at each level.
        let mut depth_frontier: HashMap<u64, usize> = frontier.clone();
        let mut hop_results: HashMap<u64, usize> = HashMap::new();

        for depth in 1u32..=hop.max_depth {
            let mut next: HashMap<u64, usize> = HashMap::new();
            for (&current_h, &count) in &depth_frontier {
                if let Some(edges) = db.fwd_edges(current_h) {
                    for e in edges {
                        if hop.edge_type_hash != 0 && e.edge_type != hop.edge_type_hash {
                            continue;
                        }
                        if db.node_data(e.other).is_some() {
                            *next.entry(e.other).or_insert(0) += count;
                        }
                    }
                }
            }
            // Accumulate results at every depth in [min_depth, max_depth].
            if depth >= hop.min_depth {
                for (&h, &c) in &next {
                    *hop_results.entry(h).or_insert(0) += c;
                }
            }
            depth_frontier = next;
            if depth_frontier.is_empty() {
                break;
            }
        }

        if hop_idx == last_hop_idx {
            return hop_results;
        }
        // Next hop's frontier = all nodes reachable via any valid depth of this hop.
        frontier = hop_results;
    }

    HashMap::new() // unreachable: loop always returns on last hop
}

/// Collect all complete paths from `starts` through the hop chain.
///
/// Build [`PathRow`]s from pre-collected raw paths.
///
/// Reads each unique payload exactly once (sorted by offset for sequential I/O),
/// then assembles PathRows from the shared cache.  Only reads start-node payloads
/// when `start_var` is `Some`.
///
/// Separated from [`collect_raw_paths`] so callers can filter / truncate raw paths
/// before paying the cost of payload reads.
pub(crate) fn build_path_rows_from_raw(
    db: &CoreDB,
    raw_paths: &[RawPath],
    hops: &[HopSpec],
    start_var: Option<&str>,
) -> Vec<PathRow> {
    if raw_paths.is_empty() { return vec![]; }

    // Collect unique hashes, sort by payload offset for sequential I/O.
    let mut needed: Vec<u64> = {
        let mut set: HashSet<u64> = HashSet::new();
        if start_var.is_some() {
            for rp in raw_paths { set.insert(rp.start_hash); }
        }
        for rp in raw_paths {
            for &h in &rp.dest_per_hop { set.insert(h); }
        }
        let mut v: Vec<u64> = set.into_iter().collect();
        v.sort_unstable_by_key(|&h| db.node_data(h).map(|nd| nd.payload_offset).unwrap_or(0));
        v
    };

    let payload_cache: HashMap<u64, Value> = needed
        .into_iter()
        .filter_map(|h| db.get_payload(h).map(|p| (h, p)))
        .collect();

    let null = Value::Null;
    raw_paths
        .iter()
        .map(|rp| {
            let mut row: PathRow = HashMap::new();

            if let Some(sv) = start_var {
                let p = payload_cache.get(&rp.start_hash).unwrap_or(&null);
                row.insert(sv.to_string(), p.clone());
            }

            for (hop_idx, hop) in hops.iter().enumerate() {
                let dest_h = rp.dest_per_hop[hop_idx];
                let dest_payload = payload_cache.get(&dest_h).unwrap_or(&null).clone();

                if let Some(ref edge_bind) = hop.edge_bind {
                    let path_slugs: Vec<&str> = rp.slugs_per_hop
                        .iter()
                        .take(hop_idx + 2)
                        .map(|s| s.as_str())
                        .collect();
                    let path_strengths: &[f32] = &rp.strengths_per_hop[..=hop_idx];
                    let n = path_strengths.len() as f64;
                    let sum: f64 = path_strengths.iter().map(|&s| s as f64).sum();
                    let avg = if n > 0.0 { sum / n } else { 0.0 };
                    let min_s = path_strengths.iter().cloned().fold(f32::INFINITY, f32::min);
                    let max_s = path_strengths.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
                    row.insert(edge_bind.clone(), serde_json::json!({
                        "_depth":         hop_idx + 1,
                        "_path_keys":     path_slugs,
                        "_path_strength": path_strengths,
                        "_avg_strength":  avg,
                        "_min_strength":  if min_s.is_infinite() { 0.0_f32 } else { min_s },
                        "_max_strength":  if max_s.is_infinite() { 0.0_f32 } else { max_s },
                    }));
                }

                row.insert(hop.node_bind.clone(), dest_payload);
            }

            row
        })
        .collect()
}

/// Filter raw paths by equality conditions on hop-bound variables.
///
/// Only loads payloads for *unique* destination hashes (sorted by offset for
/// sequential I/O), and uses head+tail extraction for large payloads (> 64 KB)
/// to avoid parsing multi-MB GeoJSON blobs.
///
/// Conditions referencing variables not in `var_to_hop` (e.g. start variable)
/// are skipped here — callers must filter those separately on PathRows.
fn filter_raw_by_dest_where(
    db: &CoreDB,
    raw: Vec<RawPath>,
    conditions: &[(String, String, serde_json::Value)],
    var_to_hop: &HashMap<&str, usize>,
) -> Vec<RawPath> {
    const FAST_PATH_THRESHOLD: u32 = 64 * 1024;

    // Fields required by conditions on hop variables.
    let where_fields: Vec<String> = conditions.iter()
        .filter(|(v, _, _)| var_to_hop.contains_key(v.as_str()))
        .map(|(_, f, _)| f.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    if where_fields.is_empty() { return raw; } // all conditions are on start var

    // Unique dest hashes referenced by hop conditions, sorted by payload offset.
    let mut unique_dests: Vec<u64> = {
        let mut seen: HashSet<u64> = HashSet::new();
        for rp in &raw {
            for (cond_var, _, _) in conditions {
                if let Some(&hi) = var_to_hop.get(cond_var.as_str()) {
                    if let Some(&h) = rp.dest_per_hop.get(hi) {
                        seen.insert(h);
                    }
                }
            }
        }
        let mut v: Vec<u64> = seen.into_iter().collect();
        v.sort_unstable_by_key(|&h| db.node_data(h).map(|nd| nd.payload_offset).unwrap_or(0));
        v
    };

    // Load each unique dest payload once — head+tail for large payloads.
    let payload_cache: HashMap<u64, Value> = unique_dests
        .into_iter()
        .filter_map(|h| {
            let p = if let Some(node) = db.node_data(h) {
                if node.payload_len > FAST_PATH_THRESHOLD {
                    if let Some((head, tail)) = db.get_payload_head_tail(h, 512, 16384) {
                        let combined = [head.as_slice(), tail.as_slice()].concat();
                        let extracted = extract_fields_by_search(&combined, &where_fields);
                        let all_found = where_fields.iter().all(|f| extracted.contains_key(f.as_str()));
                        if all_found && !extracted.is_empty() {
                            return Some((h, Value::Object(extracted)));
                        }
                    }
                }
                db.get_payload(h)?
            } else {
                db.get_payload(h)?
            };
            Some((h, p))
        })
        .collect();

    // Retain only raw paths whose hop-variable payloads satisfy all conditions.
    raw.into_iter().filter(|rp| {
        conditions.iter().all(|(cond_var, cond_field, cond_val)| {
            match var_to_hop.get(cond_var.as_str()) {
                None => true, // start var or unknown — not our responsibility here
                Some(&hi) => {
                    let h = rp.dest_per_hop.get(hi).copied().unwrap_or(0);
                    payload_cache.get(&h)
                        .and_then(|p| p.get(cond_field.as_str()))
                        .map_or(false, |v| values_eq(v, cond_val))
                }
            }
        })
    }).collect()
}

/// Collect all complete paths from `starts` through the hop chain as [`PathRow`]s.
///
/// Thin wrapper: topology via [`collect_raw_paths`] then payload materialisation
/// via [`build_path_rows_from_raw`].  Pass `limit` to short-circuit traversal when
/// you know no ordering or post-traversal filtering is needed.
pub fn collect_paths(
    db: &CoreDB,
    starts: &[u64],
    hops: &[HopSpec],
    start_var: Option<&str>,
    limit: Option<usize>,
) -> Vec<PathRow> {
    if hops.is_empty() || starts.is_empty() { return vec![]; }
    let raw = collect_raw_paths(db, starts, hops, limit);
    build_path_rows_from_raw(db, &raw, hops, start_var)
}

/// Execute a [`MatchAggStmt`] and return synthetic result [`Hit`]s.
///
/// Each Hit has an empty slug and a payload equal to one result row.
pub fn execute_match_agg(db: &CoreDB, stmt: MatchAggStmt) -> Vec<Hit> {
    // 1. Resolve starting hashes
    let starts: Vec<u64> = match stmt.start {
        MatchAggStart::Slug(h) => {
            if db.node_data(h).is_some() { vec![h] } else { vec![] }
        }
        MatchAggStart::Collection(h) => {
            db.collection_members(h).cloned().unwrap_or_default()
        }
        MatchAggStart::All => db.all_hashes(),
    };

    // 2. Collect all path rows.
    //
    // Only pass start_var when the start variable is actually referenced in a
    // RETURN expression or GROUP BY clause — otherwise the materialiser would
    // eagerly read every start-node payload even though the query doesn't need it.
    // For "SELECT b.x, COUNT(*) FROM MATCH (a:col)-[:e]->(b:col) GROUP BY b.x"
    // the variable `a` is declared but never used → skip ~N payload reads.
    let effective_start_var: Option<&str> = stmt.start_var.as_deref().filter(|sv| {
        let in_returns = stmt.returns.iter().any(|(expr, _)| expr.references_var(sv));
        let in_group   = stmt.group_by.as_ref().map_or(false, |gk| {
            gk.iter().any(|(var, _)| var.as_str() == *sv)
        });
        in_returns || in_group
    });
    // Hop-variable → hop index (used by both fast and slow paths for dest_where).
    let var_to_hop: HashMap<&str, usize> = stmt.hops.iter()
        .enumerate()
        .map(|(i, h)| (h.node_bind.as_str(), i))
        .collect();

    // ── GROUP BY fast path ─────────────────────────────────────────────────────
    // For queries with COUNT(*) / Field / scalar-first-row accesses only,
    // bypass full PathRow materialisation.  Uses three-phase approach:
    //
    //   Phase 1 — hash-keyed grouping (ZERO payload reads)
    //   Phase 2 — dest_where filter using unique dest payloads only
    //   Phase 3 — field-value grouping from payload_cache
    //
    // Example: 81,912 villages -[:child_of*3]-> 34 province destinations.
    //   Old: load 34 × 2 MB province GeoJSON → full serde_json parse → 146 s cold.
    //   New: group by hash (free) → load 34 × 16 KB head+tail → extract 3 fields → 5 ms.
    //
    // Large-payload fast path: for payloads > FAST_PATH_THRESHOLD and simple returns
    // (Field/Count/Now), uses get_payload_head_tail + extract_fields_by_search instead
    // of loading and parsing the full JSON blob.
    if let Some(ref group_keys) = stmt.group_by {
        let needs_row_scan = stmt.returns.iter().any(|(e, _)| matches!(e,
            MatchAggReturn::Sum(_) | MatchAggReturn::Avg(_) |
            MatchAggReturn::Min(_) | MatchAggReturn::Max(_)));
        let start_in_group = stmt.start_var.as_ref()
            .map_or(false, |sv| group_keys.iter().any(|(gvar, _)| gvar == sv));

        if !needs_row_scan && !start_in_group {
            // ── Shared: field list + payload loader (used by both fast paths) ──
            // Compute before path collection so the ultra-fast path can reuse them.
            let all_returns_simple = stmt.returns.iter().all(|(ret, _)|
                matches!(ret, MatchAggReturn::Count | MatchAggReturn::Field { .. }
                             | MatchAggReturn::Now));
            let needed_fields: Vec<String> = {
                let mut v: Vec<String> = Vec::new();
                let mut add = |f: &String| {
                    if !v.iter().any(|x| x == f) { v.push(f.clone()); }
                };
                for (_, f, _) in &stmt.dest_where { add(f); }
                for (gvar, gfield) in group_keys.iter() {
                    if var_to_hop.contains_key(gvar.as_str()) { add(gfield); }
                }
                for (ret, _) in &stmt.returns {
                    if let MatchAggReturn::Field { var, field } = ret {
                        if var_to_hop.contains_key(var.as_str()) { add(field); }
                    }
                }
                v
            };
            // Payload size threshold above which we use head+tail extraction to avoid
            // parsing multi-megabyte GeoJSON blobs for 3 scalar metadata fields.
            const FAST_PATH_THRESHOLD: u32 = 64 * 1024;
            let where_fields: Vec<&str> = stmt.dest_where.iter()
                .map(|(_, f, _)| f.as_str())
                .collect();
            let load_for_fields = |h: u64| -> Option<Value> {
                if all_returns_simple && !needed_fields.is_empty() {
                    if let Some(node) = db.node_data(h) {
                        if node.payload_len > FAST_PATH_THRESHOLD {
                            if let Some((head, tail)) = db.get_payload_head_tail(h, 512, 16384) {
                                let combined: Vec<u8> =
                                    [head.as_slice(), tail.as_slice()].concat();
                                let extracted =
                                    extract_fields_by_search(&combined, &needed_fields);
                                let where_ok = where_fields.iter()
                                    .all(|f| extracted.contains_key(*f));
                                if where_ok && !extracted.is_empty() {
                                    return Some(Value::Object(extracted));
                                }
                            }
                        }
                    }
                }
                db.get_payload(h)
            };

            // ── Ultra-fast path: GROUP BY on final hop variable only ───────────
            //
            // When every GROUP-BY key and every non-COUNT return expression
            // references *only* the last hop variable (e.g. `b` in `->[:e*3]->(b)`),
            // AND all dest_where conditions reference the same variable, we skip
            // per-path materialisation entirely and use collect_final_dest_counts:
            //
            //   - propagates a merged frontier HashMap<hash, weight> through hops
            //   - O(unique_nodes_at_each_level) space instead of O(total_paths)
            //   - for 81,912 villages → 3 hops → 34 provinces: 34 HashMap entries
            //     instead of 81,912 RawPath structs + 7,626 hash_groups entries
            let last_hop_idx = stmt.hops.len().saturating_sub(1);
            let last_bind = stmt.hops.last().map(|h| h.node_bind.as_str()).unwrap_or("");
            let only_final_var = !stmt.hops.is_empty()
                && group_keys.iter().all(|(gvar, _)| gvar.as_str() == last_bind)
                && stmt.returns.iter().all(|(ret, _)| match ret {
                    MatchAggReturn::Count | MatchAggReturn::Now => true,
                    MatchAggReturn::Field { var, .. } => var.as_str() == last_bind,
                    _ => false,
                })
                && stmt.hops.iter().all(|h| h.edge_bind.is_none())
                && stmt.dest_where.iter().all(|(cond_var, _, _)| {
                    // Only allow conditions on the start var (checked elsewhere) or
                    // the final hop var.  Conditions on intermediate hops require the
                    // full per-path data that collect_raw_paths provides.
                    var_to_hop.get(cond_var.as_str())
                        .map_or(true, |&hi| hi == last_hop_idx)
                });

            if only_final_var {
                let mut dest_counts =
                    collect_final_dest_counts(db, &starts, &stmt.hops);
                if dest_counts.is_empty() { return vec![]; }

                let null = Value::Null;
                let mut payload_cache: HashMap<u64, Value> = HashMap::new();

                // dest_where: load unique dest payloads (offset-sorted), filter.
                if !stmt.dest_where.is_empty() {
                    let mut unique_dests: Vec<u64> = dest_counts.keys().copied().collect();
                    unique_dests.sort_unstable_by_key(|&h| {
                        db.node_data(h).map(|nd| nd.payload_offset).unwrap_or(0)
                    });
                    for h in unique_dests {
                        if let Some(p) = load_for_fields(h) {
                            payload_cache.insert(h, p);
                        }
                    }
                    dest_counts.retain(|dest_h, _| {
                        stmt.dest_where.iter().all(|(cond_var, cond_field, cond_val)| {
                            match var_to_hop.get(cond_var.as_str()) {
                                None => true, // start var — skip (no start_in_group)
                                Some(_) => payload_cache.get(dest_h)
                                    .and_then(|p| p.get(cond_field.as_str()))
                                    .map_or(false, |v| values_eq(v, cond_val)),
                            }
                        })
                    });
                    if dest_counts.is_empty() { return vec![]; }
                }

                // Load payloads for dests not yet in cache (offset-sorted I/O).
                {
                    let mut remaining: Vec<u64> = dest_counts.keys()
                        .copied()
                        .filter(|h| !payload_cache.contains_key(h))
                        .collect();
                    remaining.sort_unstable_by_key(|&h| {
                        db.node_data(h).map(|nd| nd.payload_offset).unwrap_or(0)
                    });
                    for h in remaining {
                        if let Some(p) = load_for_fields(h) {
                            payload_cache.insert(h, p);
                        }
                    }
                }

                // Group by GROUP-BY field values, merging counts for same-value dests.
                let mut group_order: Vec<String> = Vec::new();
                let mut groups: HashMap<String, (usize, u64)> = HashMap::new();
                for (&dest_h, &count) in &dest_counts {
                    let key: String = group_keys.iter()
                        .map(|(_, gfield)| {
                            serde_json::to_string(
                                payload_cache.get(&dest_h)
                                    .and_then(|p| p.get(gfield.as_str()))
                                    .unwrap_or(&null)
                            ).unwrap_or_default()
                        })
                        .collect::<Vec<_>>()
                        .join("\x00");
                    match groups.entry(key.clone()) {
                        std::collections::hash_map::Entry::Vacant(e) => {
                            group_order.push(key);
                            e.insert((count, dest_h));
                        }
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            e.get_mut().0 += count;
                        }
                    }
                }

                // Build result rows from groups.
                let mut result_rows: Vec<Value> = group_order.iter().map(|key| {
                    let (count, rep_h) = groups[key];
                    let mut repr: PathRow = HashMap::new();
                    repr.insert(last_bind.to_string(),
                        payload_cache.get(&rep_h).cloned().unwrap_or_else(|| null.clone()));
                    let mut map = serde_json::Map::new();
                    for (ret_expr, alias) in &stmt.returns {
                        let val = if matches!(ret_expr, MatchAggReturn::Count) {
                            serde_json::json!(count as i64)
                        } else {
                            ret_expr.eval_group(std::slice::from_ref(&repr))
                        };
                        map.insert(alias.clone(), val);
                    }
                    Value::Object(map)
                }).collect();

                if let Some((ref order_field, ascending)) = stmt.order_by {
                    result_rows.sort_by(|a, b| {
                        let va = a.get(order_field.as_str()).and_then(|v| v.as_f64());
                        let vb = b.get(order_field.as_str()).and_then(|v| v.as_f64());
                        let ord = match (va, vb) {
                            (Some(na), Some(nb)) => na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal),
                            (None, Some(_)) => std::cmp::Ordering::Less,
                            (Some(_), None) => std::cmp::Ordering::Greater,
                            (None, None) => {
                                let sa = a.get(order_field.as_str()).map(|v| v.to_string()).unwrap_or_default();
                                let sb = b.get(order_field.as_str()).map(|v| v.to_string()).unwrap_or_default();
                                sa.cmp(&sb)
                            }
                        };
                        if ascending { ord } else { ord.reverse() }
                    });
                }
                if let Some(n) = stmt.limit { result_rows.truncate(n); }
                return result_rows
                    .into_iter()
                    .map(|v| Hit { slug: String::new(), slug_hash: 0, payload: Some(v) })
                    .collect();
            }

            // ── Standard fast path (GROUP BY with intermediate vars or edge binds) ─
            let raw = collect_raw_paths(db, &starts, &stmt.hops, None);
            if raw.is_empty() { return vec![]; }

            // ── Phase 1: hash-keyed grouping (zero payload reads) ──────────────
            // Group raw paths by their dest_per_hop tuple (just hashes).
            // Key  = Vec<u64> of destination hashes at each hop.
            // Value = (path_count, representative_rp_index).
            //
            // For 81,912 village→province paths ending at 34 unique provinces this
            // produces 34 groups of ~2,400 paths each — no payload read needed.
            let mut hash_groups: HashMap<Vec<u64>, (usize, usize)> = HashMap::new();
            for (rp_idx, rp) in raw.iter().enumerate() {
                hash_groups.entry(rp.dest_per_hop.clone())
                    .and_modify(|(cnt, _)| *cnt += 1)
                    .or_insert((1, rp_idx));
            }
            // ── Phase 2: dest_where filtering ─────────────────────────────────
            // Load payloads only for UNIQUE dest tuples (not per raw path).
            // Reads are sorted by payload_offset for sequential I/O (better OS prefetch).
            let null = Value::Null;
            let mut payload_cache: HashMap<u64, Value> = HashMap::new();

            if !stmt.dest_where.is_empty() {
                // Collect all unique dest hashes across remaining groups, sorted by offset.
                let mut unique_dests: Vec<u64> = hash_groups.keys()
                    .flat_map(|dp| dp.iter().copied())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();
                unique_dests.sort_unstable_by_key(|&h| {
                    db.node_data(h).map(|nd| nd.payload_offset).unwrap_or(0)
                });
                for h in unique_dests {
                    if let Some(p) = load_for_fields(h) {
                        payload_cache.insert(h, p);
                    }
                }

                // Apply dest_where: remove groups whose destination fails the condition.
                hash_groups.retain(|dest_per_hop, _| {
                    stmt.dest_where.iter().all(|(cond_var, cond_field, cond_val)| {
                        match var_to_hop.get(cond_var.as_str()) {
                            None => true, // start var or unknown — skip
                            Some(&hi) => {
                                let h = dest_per_hop.get(hi).copied().unwrap_or(0);
                                payload_cache.get(&h)
                                    .and_then(|p| p.get(cond_field.as_str()))
                                    .map_or(false, |v| values_eq(v, cond_val))
                            }
                        }
                    })
                });
                if hash_groups.is_empty() { return vec![]; }
            }

            // ── Phase 3: load payloads for remaining unique dests ──────────────
            // Any dests not already in payload_cache (those not loaded during dest_where)
            // are loaded now, sorted by offset for sequential I/O.
            {
                let mut remaining: Vec<u64> = hash_groups.keys()
                    .flat_map(|dp| dp.iter().copied())
                    .filter(|h| !payload_cache.contains_key(h))
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();
                remaining.sort_unstable_by_key(|&h| {
                    db.node_data(h).map(|nd| nd.payload_offset).unwrap_or(0)
                });
                for h in remaining {
                    if let Some(p) = load_for_fields(h) {
                        payload_cache.insert(h, p);
                    }
                }
            }

            // ── Phase 4: group by field values (merge same-field-value groups) ──
            // Two different dest hashes with identical GROUP BY field values are merged.
            // e.g. two districts named "North Melbourne" → same group, summed counts.
            let mut group_order: Vec<String> = Vec::new();
            let mut groups: HashMap<String, (usize, usize)> = HashMap::new();

            for (dest_per_hop, &(count, rep_rp_idx)) in &hash_groups {
                let key: String = group_keys.iter()
                    .map(|(gvar, gfield)| {
                        let hi = var_to_hop.get(gvar.as_str()).copied().unwrap_or(0);
                        let h = dest_per_hop.get(hi).copied().unwrap_or(0);
                        serde_json::to_string(
                            payload_cache.get(&h)
                                .and_then(|p| p.get(gfield.as_str()))
                                .unwrap_or(&null)
                        ).unwrap_or_default()
                    })
                    .collect::<Vec<_>>()
                    .join("\x00");
                match groups.entry(key.clone()) {
                    std::collections::hash_map::Entry::Vacant(e) => {
                        group_order.push(key);
                        e.insert((count, rep_rp_idx));
                    }
                    std::collections::hash_map::Entry::Occupied(mut e) => {
                        e.get_mut().0 += count; // merge counts for same field values
                    }
                }
            }

            // ── Phase 5: build result rows → ORDER BY → LIMIT ─────────────────
            let mut result_rows: Vec<Value> = group_order.iter().map(|key| {
                let (count, rep_idx) = groups[key];
                let rp = &raw[rep_idx];
                let mut repr: PathRow = HashMap::new();
                for (hop_idx, hop) in stmt.hops.iter().enumerate() {
                    if hop_idx < rp.dest_per_hop.len() {
                        let h = rp.dest_per_hop[hop_idx];
                        repr.insert(hop.node_bind.clone(),
                            payload_cache.get(&h).cloned().unwrap_or_else(|| null.clone()));
                    }
                }
                let mut map = serde_json::Map::new();
                for (ret_expr, alias) in &stmt.returns {
                    let val = if matches!(ret_expr, MatchAggReturn::Count) {
                        serde_json::json!(count as i64)
                    } else {
                        ret_expr.eval_group(std::slice::from_ref(&repr))
                    };
                    map.insert(alias.clone(), val);
                }
                Value::Object(map)
            }).collect();

            // ORDER BY
            if let Some((ref order_field, ascending)) = stmt.order_by {
                result_rows.sort_by(|a, b| {
                    let va = a.get(order_field.as_str()).and_then(|v| v.as_f64());
                    let vb = b.get(order_field.as_str()).and_then(|v| v.as_f64());
                    let ord = match (va, vb) {
                        (Some(na), Some(nb)) => na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal),
                        (None, Some(_)) => std::cmp::Ordering::Less,
                        (Some(_), None) => std::cmp::Ordering::Greater,
                        (None, None) => {
                            let sa = a.get(order_field.as_str()).map(|v| v.to_string()).unwrap_or_default();
                            let sb = b.get(order_field.as_str()).map(|v| v.to_string()).unwrap_or_default();
                            sa.cmp(&sb)
                        }
                    };
                    if ascending { ord } else { ord.reverse() }
                });
            }
            if let Some(n) = stmt.limit { result_rows.truncate(n); }
            return result_rows
                .into_iter()
                .map(|v| Hit { slug: String::new(), slug_hash: 0, payload: Some(v) })
                .collect();
        }
    }

    // Slow path: SUM/AVG/MIN/MAX or start_var in GROUP BY.
    //
    // Topology-first, same principle as the GROUP BY fast path:
    //   Phase 1 — topology only (collect_raw_paths, no payload reads)
    //   Phase 2 — filter hop-variable conditions via filter_raw_by_dest_where
    //             (reads only unique dest payloads, head+tail for large blobs)
    //   Phase 3 — truncate to LIMIT before payload reads (when safe)
    //   Phase 4 — read payloads only for survivors → build PathRows
    //   Phase 5 — filter start-variable conditions (requires PathRow)

    // Push LIMIT into the traversal only when there is nothing that requires
    // seeing all results (no dest_where filter, no ordering, no aggregation).
    let traversal_limit = if stmt.dest_where.is_empty()
        && stmt.order_by.is_none()
        && stmt.group_by.is_none()
    {
        stmt.limit
    } else {
        None
    };

    // Phase 1: topology.
    let mut raw = collect_raw_paths(db, &starts, &stmt.hops, traversal_limit);
    if raw.is_empty() { return vec![]; }

    // Phase 2: filter by hop-variable conditions (topology-first, efficient).
    // Conditions on the start variable are skipped here and applied in Phase 5.
    if !stmt.dest_where.is_empty() {
        raw = filter_raw_by_dest_where(db, raw, &stmt.dest_where, &var_to_hop);
        if raw.is_empty() { return vec![]; }
    }

    // Phase 3: truncate to LIMIT before paying for payload reads.
    // Safe when: no ORDER BY (order would change which rows survive) and no GROUP BY.
    // start_where conditions are post-filter, so only skip truncation if they exist.
    let has_start_conditions = stmt.dest_where.iter()
        .any(|(v, _, _)| !var_to_hop.contains_key(v.as_str()));
    if stmt.group_by.is_none() && stmt.order_by.is_none() && !has_start_conditions {
        if let Some(n) = stmt.limit { raw.truncate(n); }
    }

    // Phase 4: read payloads only for the surviving raw paths.
    let all_paths = build_path_rows_from_raw(db, &raw, &stmt.hops, effective_start_var);
    if all_paths.is_empty() { return vec![]; }

    // Phase 5: filter start-variable conditions (requires PathRow payload).
    let paths: Vec<PathRow> = if !has_start_conditions {
        all_paths
    } else {
        all_paths.into_iter().filter(|row| {
            stmt.dest_where.iter()
                .filter(|(v, _, _)| !var_to_hop.contains_key(v.as_str()))
                .all(|(cond_var, cond_field, cond_val)| {
                    row.get(cond_var.as_str())
                        .and_then(|v| v.get(cond_field.as_str()))
                        .map_or(false, |v| values_eq(v, cond_val))
                })
        }).collect()
    };
    if paths.is_empty() { return vec![]; }

    // 3. GROUP BY or flat pass-through
    let mut result_rows: Vec<Value> = if let Some(ref group_keys) = stmt.group_by {
        // Build one group key per path row by concatenating all (var, field) values.
        let mut group_order: Vec<String> = Vec::new();
        let mut groups: HashMap<String, Vec<PathRow>> = HashMap::new();

        for row in paths {
            let key = group_keys.iter()
                .map(|(gvar, gfield)| {
                    serde_json::to_string(
                        row.get(gvar.as_str())
                            .and_then(|v| v.get(gfield.as_str()))
                            .unwrap_or(&Value::Null),
                    )
                    .unwrap_or_default()
                })
                .collect::<Vec<_>>()
                .join("\x00");
            if !groups.contains_key(&key) {
                group_order.push(key.clone());
            }
            groups.entry(key).or_default().push(row);
        }

        group_order
            .into_iter()
            .map(|key| {
                let group_rows = &groups[&key];
                let mut map = serde_json::Map::new();
                for (ret_expr, alias) in &stmt.returns {
                    map.insert(alias.clone(), ret_expr.eval_group(group_rows));
                }
                Value::Object(map)
            })
            .collect()
    } else {
        // No GROUP BY — one result row per complete path
        paths
            .into_iter()
            .map(|row| {
                let mut map = serde_json::Map::new();
                for (ret_expr, alias) in &stmt.returns {
                    map.insert(alias.clone(), ret_expr.eval_group(std::slice::from_ref(&row)));
                }
                Value::Object(map)
            })
            .collect()
    };

    // 4. ORDER BY
    if let Some((ref order_field, ascending)) = stmt.order_by {
        result_rows.sort_by(|a, b| {
            let va = a.get(order_field.as_str()).and_then(|v| v.as_f64());
            let vb = b.get(order_field.as_str()).and_then(|v| v.as_f64());
            let ord = match (va, vb) {
                (Some(na), Some(nb)) => na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal),
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, None) => {
                    let sa = a.get(order_field.as_str()).map(|v| v.to_string()).unwrap_or_default();
                    let sb = b.get(order_field.as_str()).map(|v| v.to_string()).unwrap_or_default();
                    sa.cmp(&sb)
                }
            };
            if ascending { ord } else { ord.reverse() }
        });
    }

    // 5. LIMIT
    if let Some(n) = stmt.limit {
        result_rows.truncate(n);
    }

    // 6. Wrap in Hits (synthetic — no real node slug)
    result_rows
        .into_iter()
        .map(|v| Hit { slug: String::new(), slug_hash: 0, payload: Some(v) })
        .collect()
}

// ── execute_shortest_select ───────────────────────────────────────────────────

/// Build the single `PathRow` for a `SELECT … FROM MATCH SHORTEST` result.
/// Returns `None` when no path exists (BFS returned nothing).
fn build_shortest_path_row(
    db: &CoreDB,
    stmt: &ShortestSelectStmt,
) -> Option<PathRow> {
    use crate::sk_hash;

    let start = sk_hash(&stmt.from_slug);
    let end   = sk_hash(&stmt.to_slug);

    // Diagnose missing endpoints before running BFS.
    // Common cause: no collection type in pattern e.g. (a)-[r*]->(b) instead of (a:col)-[r*]->(b:col)
    // so the slug is built as "key" instead of "collection/key".
    if db.node_data(start).is_none() {
        eprintln!(
            "MATCH SHORTEST: start node '{}' not found — did you specify the collection? e.g. ({}:collection)",
            stmt.from_slug, stmt.start_bind
        );
        return None;
    }
    if db.node_data(end).is_none() {
        eprintln!(
            "MATCH SHORTEST: end node '{}' not found — did you specify the collection? e.g. ({}:collection)",
            stmt.to_slug, stmt.end_bind
        );
        return None;
    }

    let pr    = db.bfs_shortest_path(start, end)?;

    let mut row: PathRow = HashMap::new();

    // Bind start node
    if let Some(payload) = db.get_payload(start) {
        row.insert(stmt.start_bind.clone(), payload);
    }

    // Bind end node
    if let Some(payload) = db.get_payload(end) {
        row.insert(stmt.end_bind.clone(), payload);
    }

    // Bind path object when path_bind is set
    if let Some(ref pb) = stmt.path_bind {
        let node_slugs: Vec<Value> = pr.nodes.iter()
            .map(|n| Value::String(n.slug.clone()))
            .collect();
        let strengths: Vec<Value> = pr.edges.iter()
            .map(|e| serde_json::json!(e.strength))
            .collect();
        let edges_arr: Vec<Value> = pr.edges.iter()
            .map(|e| serde_json::json!({
                "from":     e.from_slug,
                "to":       e.to_slug,
                "type":     e.edge_type,
                "strength": e.strength,
            }))
            .collect();
        let path_obj = serde_json::json!({
            "nodes":          &node_slugs,
            "edges":          &edges_arr,
            "length":         pr.length,
            "_path_keys":     &node_slugs,
            "_path_strength": &strengths,
        });
        row.insert(pb.clone(), path_obj);
    }

    Some(row)
}

/// Evaluate a `PathPredicate` against the path stored in `row`.
///
/// `nodes(path_bind)` provides the list of node slugs; we load each from DB
/// to evaluate the field condition.
fn eval_path_predicate(db: &CoreDB, pred: &PathPredicate, row: &PathRow) -> bool {
    let (var, cond) = match pred {
        PathPredicate::Any    { var, cond } => (var, cond),
        PathPredicate::All    { var, cond } => (var, cond),
        PathPredicate::None_  { var, cond } => (var, cond),
        PathPredicate::Single { var, cond } => (var, cond),
    };

    // Collect node slugs from the path object stored in `row` under `var`
    let slugs: Vec<String> = row
        .get(var.as_str())
        .and_then(|v| v.get("_path_keys"))
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|s| s.as_str().map(|s| s.to_string())).collect())
        .unwrap_or_default();

    let matches: Vec<bool> = slugs.iter().map(|slug| {
        let hash = crate::sk_hash(slug);
        if let Some(payload) = db.get_payload(hash) {
            let actual = payload.get(&cond.field).cloned().unwrap_or(Value::Null);
            eval_cmp(&actual, &cond.op, &cond.val)
        } else {
            false
        }
    }).collect();

    match pred {
        PathPredicate::Any    { .. } => matches.iter().any(|&b| b),
        PathPredicate::All    { .. } => !matches.is_empty() && matches.iter().all(|&b| b),
        PathPredicate::None_  { .. } => matches.iter().all(|&b| !b),
        PathPredicate::Single { .. } => matches.iter().filter(|&&b| b).count() == 1,
    }
}

/// Shared ORDER BY + LIMIT + Hit-wrapping finalizer used by shortest and multi-from executors.
fn finalize_rows(
    mut result_rows: Vec<Value>,
    order_by: Option<&(String, bool)>,
    limit: Option<usize>,
) -> Vec<Hit> {
    if let Some((ref field, ascending)) = order_by {
        result_rows.sort_by(|a, b| {
            let va = a.get(field.as_str()).and_then(|v| v.as_f64());
            let vb = b.get(field.as_str()).and_then(|v| v.as_f64());
            let ord = match (va, vb) {
                (Some(na), Some(nb)) => na.partial_cmp(&nb).unwrap_or(std::cmp::Ordering::Equal),
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                (None, None) => {
                    let sa = a.get(field.as_str()).map(|v| v.to_string()).unwrap_or_default();
                    let sb = b.get(field.as_str()).map(|v| v.to_string()).unwrap_or_default();
                    sa.cmp(&sb)
                }
            };
            if *ascending { ord } else { ord.reverse() }
        });
    }
    if let Some(n) = limit { result_rows.truncate(n); }
    result_rows
        .into_iter()
        .map(|v| Hit { slug: String::new(), slug_hash: 0, payload: Some(v) })
        .collect()
}

/// Execute a `SELECT … FROM MATCH SHORTEST` statement.
///
/// Returns 0 rows when no path exists, 1 row when found (after predicate filtering).
pub fn execute_shortest_select(db: &CoreDB, stmt: ShortestSelectStmt) -> Vec<Hit> {
    let row = match build_shortest_path_row(db, &stmt) {
        Some(r) => r,
        None    => return vec![],
    };

    // Apply path predicates
    for pred in &stmt.predicates {
        if !eval_path_predicate(db, pred, &row) {
            return vec![];
        }
    }

    // eval_group for each return expr over the single row
    let rows_slice: &[PathRow] = std::slice::from_ref(&row);
    let mut map = serde_json::Map::new();
    for (ret_expr, alias) in &stmt.returns {
        map.insert(alias.clone(), ret_expr.eval_group(rows_slice));
    }

    finalize_rows(vec![Value::Object(map)], stmt.order_by.as_ref(), stmt.limit)
}

// ── execute_multi_from ────────────────────────────────────────────────────────

/// Compute the Cartesian product of multiple sets of `PathRow`s.
fn cartesian_product(sources: Vec<Vec<PathRow>>) -> Vec<PathRow> {
    sources.into_iter().fold(vec![HashMap::new()], |acc, source| {
        let mut result = Vec::with_capacity(acc.len() * source.len().max(1));
        for existing_row in &acc {
            for src_row in &source {
                let mut merged = existing_row.clone();
                merged.extend(src_row.iter().map(|(k, v)| (k.clone(), v.clone())));
                result.push(merged);
            }
        }
        result
    })
}

/// Execute a `SELECT … FROM source1, source2, …` multi-FROM statement.
///
/// Each source is executed independently; rows are cross-joined (Cartesian product).
pub fn execute_multi_from(db: &CoreDB, stmt: MultiFromStmt) -> Vec<Hit> {
    let source_rows: Vec<Vec<PathRow>> = stmt.sources.into_iter().map(|src| match src {
        FromSource::Match(agg) => {
            let starts: Vec<u64> = match agg.start {
                MatchAggStart::Slug(h)       => if db.node_data(h).is_some() { vec![h] } else { vec![] },
                MatchAggStart::Collection(h) => db.collection_members(h).cloned().unwrap_or_default(),
                MatchAggStart::All           => db.all_hashes(),
            };
            collect_paths(db, &starts, &agg.hops, agg.start_var.as_deref(), None)
        }
        FromSource::Shortest(s) => {
            match build_shortest_path_row(db, &s) {
                Some(row) => vec![row],
                None      => vec![],
            }
        }
        FromSource::Collection { alias, name_hash } => {
            db.collection_members(name_hash)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|h| {
                    // check node exists and get payload
                    let payload = db.get_payload(h)?;
                    let mut row: PathRow = HashMap::new();
                    row.insert(alias.clone(), payload);
                    Some(row)
                })
                .collect()
        }
    }).collect();

    let all_rows = cartesian_product(source_rows);
    if all_rows.is_empty() {
        return vec![];
    }

    let result_rows: Vec<Value> = all_rows.into_iter().map(|row| {
        let mut map = serde_json::Map::new();
        for (ret_expr, alias) in &stmt.returns {
            map.insert(alias.clone(), ret_expr.eval_group(std::slice::from_ref(&row)));
        }
        Value::Object(map)
    }).collect();

    finalize_rows(result_rows, stmt.order_by.as_ref(), stmt.limit)
}

fn cmp_json(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(a), Some(b)) => match (a, b) {
            (Value::Number(na), Value::Number(nb)) => na
                .as_f64()
                .unwrap_or(0.0)
                .partial_cmp(&nb.as_f64().unwrap_or(0.0))
                .unwrap_or(Ordering::Equal),
            (Value::String(sa), Value::String(sb)) => sa.cmp(sb),
            (Value::Bool(ba), Value::Bool(bb)) => ba.cmp(bb),
            _ => Ordering::Equal,
        },
    }
}
