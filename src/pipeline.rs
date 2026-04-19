//! MATCH + WITH pipeline executor.
//!
//! Implements the Cypher-style multi-stage pipeline:
//! ```text
//! MATCH ('start/slug')-[:edge1]->(a)-[:edge2]->(b)
//! WITH  b.field AS alias, SUM(a.x * b.y) AS score
//! MATCH (c:clos WHERE _key = alias)-[:edge3]->(d:dest)
//! RETURN d._key AS out, SUM(score * c.w) AS total ORDER BY total DESC LIMIT 10
//! ```
//!
//! Each row in the pipeline is a [`PipeRow`] (`HashMap<String, Value>`) that
//! carries all named bindings from `MATCH` clauses and computed values from
//! `WITH` checkpoints.

use crate::{sk_hash, CoreDB};
use crate::query::Hit;
use serde_json::Value;
use std::collections::HashMap;

// ── PipeRow ───────────────────────────────────────────────────────────────────

/// One row in the pipeline: maps bind names and WITH aliases to JSON values.
pub type PipeRow = HashMap<String, Value>;

// ── PipeExpr ──────────────────────────────────────────────────────────────────

/// Math expression evaluated per [`PipeRow`] for aggregation or projection.
#[derive(Clone, Debug)]
pub enum PipeExpr {
    /// `var.field` — access a field inside a bound node payload.
    RowField { var: String, field: String },
    /// `key` — access a top-level row value (e.g. a WITH alias such as `clo_score`).
    RowKey(String),
    /// Numeric literal.
    Literal(f64),
    Mul(Box<PipeExpr>, Box<PipeExpr>),
    Add(Box<PipeExpr>, Box<PipeExpr>),
    Sub(Box<PipeExpr>, Box<PipeExpr>),
    Div(Box<PipeExpr>, Box<PipeExpr>),
}

impl PipeExpr {
    /// Evaluate to f64 (for aggregate computations).
    pub fn eval(&self, row: &PipeRow) -> f64 {
        match self {
            PipeExpr::RowField { var, field } => row
                .get(var.as_str())
                .and_then(|v| v.get(field.as_str()))
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
            PipeExpr::RowKey(key) => row
                .get(key.as_str())
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0),
            PipeExpr::Literal(n) => *n,
            PipeExpr::Mul(a, b) => a.eval(row) * b.eval(row),
            PipeExpr::Add(a, b) => a.eval(row) + b.eval(row),
            PipeExpr::Sub(a, b) => a.eval(row) - b.eval(row),
            PipeExpr::Div(a, b) => {
                let d = b.eval(row);
                if d == 0.0 { 0.0 } else { a.eval(row) / d }
            }
        }
    }

    /// Evaluate to a JSON Value (preserves string type for scalar projections).
    pub fn eval_as_value(&self, row: &PipeRow) -> Value {
        match self {
            PipeExpr::RowField { var, field } => row
                .get(var.as_str())
                .and_then(|v| v.get(field.as_str()))
                .cloned()
                .unwrap_or(Value::Null),
            PipeExpr::RowKey(key) => row.get(key.as_str()).cloned().unwrap_or(Value::Null),
            _ => serde_json::json!(self.eval(row)),
        }
    }
}

// ── PipeOutExpr ───────────────────────────────────────────────────────────────

/// Output expression in a `WITH` or `RETURN` clause.
#[derive(Clone, Debug)]
pub enum PipeOutExpr {
    /// Scalar projection — takes the value from the first row in each group.
    Scalar(PipeExpr),
    Sum(PipeExpr),
    Avg(PipeExpr),
    Min(PipeExpr),
    Max(PipeExpr),
    Count,
}

impl PipeOutExpr {
    pub fn is_agg(&self) -> bool {
        !matches!(self, PipeOutExpr::Scalar(_))
    }
}

// ── CmpOp ─────────────────────────────────────────────────────────────────────

/// Comparison operator used in pipeline WHERE conditions.
#[derive(Clone, Debug)]
pub enum CmpOp { Eq, Ne, Lt, Lte, Gt, Gte }

impl CmpOp {
    /// Apply this operator to two JSON values.  Numbers compare numerically;
    /// strings compare lexicographically; booleans and nulls support Eq/Ne only.
    pub fn apply(&self, a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Number(n1), Value::Number(n2)) => {
                let (x, y) = (n1.as_f64().unwrap_or(0.0), n2.as_f64().unwrap_or(0.0));
                match self {
                    CmpOp::Eq  => x == y,
                    CmpOp::Ne  => x != y,
                    CmpOp::Lt  => x <  y,
                    CmpOp::Lte => x <= y,
                    CmpOp::Gt  => x >  y,
                    CmpOp::Gte => x >= y,
                }
            }
            (Value::String(s1), Value::String(s2)) => match self {
                CmpOp::Eq  => s1 == s2,
                CmpOp::Ne  => s1 != s2,
                CmpOp::Lt  => s1 <  s2,
                CmpOp::Lte => s1 <= s2,
                CmpOp::Gt  => s1 >  s2,
                CmpOp::Gte => s1 >= s2,
            },
            (Value::Bool(b1), Value::Bool(b2)) => match self {
                CmpOp::Eq => b1 == b2,
                CmpOp::Ne => b1 != b2,
                _         => false,
            },
            (Value::Null, Value::Null) => matches!(self, CmpOp::Eq),
            (Value::Null, _) | (_, Value::Null) => matches!(self, CmpOp::Ne),
            _ => false,
        }
    }
}

// ── PipeWhere ─────────────────────────────────────────────────────────────────

/// WHERE condition inside a MATCH node.  Used to filter start nodes per-row.
#[derive(Clone, Debug)]
pub enum PipeWhere {
    /// `field <op> row_key` — compares node's field against a pipeline row value.
    FieldCmpRowRef { field: String, op: CmpOp, row_key: String },
    /// `field <op> literal` — compares node's field against a literal value.
    FieldCmpLiteral { field: String, op: CmpOp, value: Value },
}

impl PipeWhere {
    pub fn matches(&self, payload: &Value, row: &PipeRow) -> bool {
        match self {
            PipeWhere::FieldCmpRowRef { field, op, row_key } => {
                let node_val = payload.get(field.as_str());
                let row_val  = row.get(row_key.as_str());
                match (node_val, row_val) {
                    (Some(a), Some(b)) => op.apply(a, b),
                    _ => false,
                }
            }
            PipeWhere::FieldCmpLiteral { field, op, value } => payload
                .get(field.as_str())
                .map(|v| op.apply(v, value))
                .unwrap_or(false),
        }
    }
}

// ── PipeMatchStart ────────────────────────────────────────────────────────────

/// Starting point for one `MATCH` stage.
#[derive(Clone, Debug)]
pub enum PipeMatchStart {
    /// `('slug/key')` — a specific node.
    Slug(u64),
    /// `(var:collection [WHERE field = value_or_ref])` — collection scan.
    Collection {
        /// Bind name for the start node (e.g. `c` in `(c:clos WHERE ...)`).
        bind: Option<String>,
        col_hash: u64,
        filters: Vec<PipeWhere>,
    },
}

// ── PipeHop ───────────────────────────────────────────────────────────────────

/// One forward hop: `-[:edge_type]->(bind_name[:collection])`.
#[derive(Clone, Debug)]
pub struct PipeHop {
    pub edge_type_hash: u64,
    pub bind: String,
    /// If `Some`, only endpoint nodes in this collection are accepted.
    pub col_filter: Option<u64>,
}

// ── PipeMatchStage ────────────────────────────────────────────────────────────

/// A `MATCH` stage: expands the current row set by graph traversal.
#[derive(Clone, Debug)]
pub struct PipeMatchStage {
    pub start: PipeMatchStart,
    pub hops: Vec<PipeHop>,
}

// ── PipeProjectStage ──────────────────────────────────────────────────────────

/// A `WITH` or `RETURN` stage: aggregates/projects the current row set.
#[derive(Clone, Debug)]
pub struct PipeProjectStage {
    pub outputs: Vec<(PipeOutExpr, String)>,
    pub order_by: Option<(String, bool)>,
    pub limit: Option<usize>,
}

// ── Pipeline ──────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub enum PipelineStage {
    Match(PipeMatchStage),
    Project(PipeProjectStage),
}

/// A fully parsed and compiled `MATCH + WITH` pipeline.
#[derive(Clone, Debug)]
pub struct Pipeline {
    pub stages: Vec<PipelineStage>,
}

// ── Execution ─────────────────────────────────────────────────────────────────

/// Execute a [`Pipeline`] and return result [`Hit`]s.
///
/// Starts with a single empty row `[{}]`, then alternates between
/// graph expansion (`MATCH`) and aggregation/projection (`WITH`/`RETURN`).
pub fn execute_pipeline(db: &CoreDB, pipeline: Pipeline) -> Vec<Hit> {
    let mut rows: Vec<PipeRow> = vec![PipeRow::new()];

    for stage in pipeline.stages {
        if rows.is_empty() {
            break;
        }
        match stage {
            PipelineStage::Match(m) => rows = expand_match(db, rows, &m),
            PipelineStage::Project(p) => rows = project_rows(rows, &p),
        }
    }

    rows.into_iter()
        .map(|row| {
            let map: serde_json::Map<String, Value> = row.into_iter().collect();
            Hit { slug: String::new(), slug_hash: 0, payload: Some(Value::Object(map)) }
        })
        .collect()
}

// ── expand_match ──────────────────────────────────────────────────────────────

fn resolve_starts(db: &CoreDB, start: &PipeMatchStart, row: &PipeRow) -> Vec<u64> {
    match start {
        PipeMatchStart::Slug(h) => {
            if db.node_data(*h).is_some() { vec![*h] } else { vec![] }
        }
        PipeMatchStart::Collection { col_hash, filters, .. } => {
            let members = db.collection_members(*col_hash).cloned().unwrap_or_default();
            if filters.is_empty() {
                members
            } else {
                members
                    .into_iter()
                    .filter(|&h| {
                        db.get_payload(h)
                            .map(|p| filters.iter().all(|f| f.matches(&p, row)))
                            .unwrap_or(false)
                    })
                    .collect()
            }
        }
    }
}

fn expand_match(db: &CoreDB, rows: Vec<PipeRow>, stage: &PipeMatchStage) -> Vec<PipeRow> {
    let mut result = Vec::new();

    for row in rows {
        let starts = resolve_starts(db, &stage.start, &row);

        for &start_h in &starts {
            // Bind the start node if a bind name is specified.
            let start_bindings: Vec<(String, Value)> =
                match &stage.start {
                    PipeMatchStart::Collection { bind: Some(b), .. } => {
                        match db.get_payload(start_h) {
                            Some(payload) => vec![(b.clone(), payload)],
                            None => continue,
                        }
                    }
                    _ => vec![],
                };

            if stage.hops.is_empty() {
                let mut new_row = row.clone();
                for (k, v) in start_bindings {
                    new_row.insert(k, v);
                }
                result.push(new_row);
                continue;
            }

            // DFS stack: (current_hash, next_hop_index, accumulated_bindings)
            let mut stack: Vec<(u64, usize, Vec<(String, Value)>)> =
                vec![(start_h, 0, start_bindings)];

            while let Some((current_h, hop_idx, bindings)) = stack.pop() {
                let hop = &stage.hops[hop_idx];
                let Some(edges) = db.fwd_edges(current_h) else { continue };

                for e in edges {
                    if e.edge_type != hop.edge_type_hash {
                        continue;
                    }
                    // Optional collection filter on endpoint node.
                    if let Some(col_h) = hop.col_filter {
                        if !node_in_collection(db, e.other, col_h) {
                            continue;
                        }
                    }

                    let node_payload = match db.get_payload(e.other) {
                        Some(p) => p,
                        None => continue,
                    };
                    let mut new_bindings = bindings.clone();
                    new_bindings.push((hop.bind.clone(), node_payload));

                    let next_hop = hop_idx + 1;
                    if next_hop >= stage.hops.len() {
                        // Complete path — merge all bindings into the row.
                        let mut new_row = row.clone();
                        for (k, v) in &new_bindings {
                            new_row.insert(k.clone(), v.clone());
                        }
                        result.push(new_row);
                    } else {
                        stack.push((e.other, next_hop, new_bindings));
                    }
                }
            }
        }
    }

    result
}

fn node_in_collection(db: &CoreDB, hash: u64, col_hash: u64) -> bool {
    db.node_data(hash)
        .map(|n| !n.collection.is_empty() && sk_hash(&n.collection) == col_hash)
        .unwrap_or(false)
}

// ── project_rows ──────────────────────────────────────────────────────────────

fn project_rows(rows: Vec<PipeRow>, stage: &PipeProjectStage) -> Vec<PipeRow> {
    if rows.is_empty() {
        return vec![];
    }

    let has_agg = stage.outputs.iter().any(|(e, _)| e.is_agg());

    let mut result: Vec<PipeRow> = if has_agg {
        // Group by all scalar (non-aggregate) expressions, then aggregate per group.
        let mut group_order: Vec<String> = Vec::new();
        let mut groups: HashMap<String, Vec<PipeRow>> = HashMap::new();

        for row in rows {
            let key = stage
                .outputs
                .iter()
                .filter(|(e, _)| !e.is_agg())
                .map(|(e, _)| {
                    let val = if let PipeOutExpr::Scalar(inner) = e {
                        inner.eval_as_value(&row)
                    } else {
                        Value::Null
                    };
                    serde_json::to_string(&val).unwrap_or_default()
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
                let group = &groups[&key];
                let mut new_row = PipeRow::new();
                for (expr, alias) in &stage.outputs {
                    new_row.insert(alias.clone(), eval_out_over_group(expr, group));
                }
                new_row
            })
            .collect()
    } else {
        // Pure projection — no aggregation.
        rows.into_iter()
            .map(|row| {
                let mut new_row = PipeRow::new();
                for (expr, alias) in &stage.outputs {
                    let val = if let PipeOutExpr::Scalar(inner) = expr {
                        inner.eval_as_value(&row)
                    } else {
                        Value::Null
                    };
                    new_row.insert(alias.clone(), val);
                }
                new_row
            })
            .collect()
    };

    // ORDER BY
    if let Some((ref field, asc)) = stage.order_by {
        result.sort_by(|a, b| {
            let va = a.get(field.as_str());
            let vb = b.get(field.as_str());
            let ord = cmp_values(va, vb);
            if asc { ord } else { ord.reverse() }
        });
    }

    // LIMIT
    if let Some(n) = stage.limit {
        result.truncate(n);
    }

    result
}

fn eval_out_over_group(expr: &PipeOutExpr, group: &[PipeRow]) -> Value {
    match expr {
        PipeOutExpr::Scalar(e) => {
            group.first().map(|r| e.eval_as_value(r)).unwrap_or(Value::Null)
        }
        PipeOutExpr::Sum(e) => serde_json::json!(group.iter().map(|r| e.eval(r)).sum::<f64>()),
        PipeOutExpr::Avg(e) => {
            if group.is_empty() {
                return Value::Null;
            }
            let s: f64 = group.iter().map(|r| e.eval(r)).sum();
            serde_json::json!(s / group.len() as f64)
        }
        PipeOutExpr::Min(e) => {
            let m = group.iter().map(|r| e.eval(r)).fold(f64::INFINITY, f64::min);
            if m.is_infinite() { Value::Null } else { serde_json::json!(m) }
        }
        PipeOutExpr::Max(e) => {
            let m = group.iter().map(|r| e.eval(r)).fold(f64::NEG_INFINITY, f64::max);
            if m.is_infinite() { Value::Null } else { serde_json::json!(m) }
        }
        PipeOutExpr::Count => serde_json::json!(group.len() as i64),
    }
}

fn cmp_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
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
