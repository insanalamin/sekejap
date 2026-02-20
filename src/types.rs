use rstar::RTreeObject;
use serde_json::Value;

#[repr(C)]
#[derive(Copy, Clone, Debug, Default)]
pub struct NodeSlot {
    pub crc32: u32,
    pub slug_hash: u64,       // Key hash
    pub collection_hash: u64,  // Collection hash
    pub flags: u64,           // 1 = Active, 0 = Deleted
    pub lat: f32,
    pub lon: f32,
    pub blob_offset: u64,
    pub blob_len: u32,
    pub vec_slot: u32,
    pub edge_head: u32,
    pub edge_count: u32,
    pub _pad: [u64; 7],       // Pad to 128 bytes total
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct EdgeSlot {
    pub from_node: u32,
    pub to_node: u32,
    pub weight: f32,
    pub edge_type_hash: u64,
    pub timestamp: u64,
    pub flags: u8,
    /// 0 = no metadata, 1 = inline JSON (≤32 bytes), 2 = blob arena reference
    pub meta_kind: u8,
    /// byte count of inline data when meta_kind=1
    pub meta_len: u8,
    pub _reserved: u8,
    /// inline bytes (meta_kind=1) or (offset:u64 ++ len:u32) (meta_kind=2)
    pub meta: [u8; 32],
}

impl Default for EdgeSlot {
    fn default() -> Self {
        Self {
            from_node: 0,
            to_node: 0,
            weight: 0.0,
            edge_type_hash: 0,
            timestamp: 0,
            flags: 0,
            meta_kind: 0,
            meta_len: 0,
            _reserved: 0,
            meta: [0u8; 32],
        }
    }
}

/// A resolved edge result from edge_collect()
#[derive(Clone, Debug)]
pub struct EdgeHit {
    pub from_idx: u32,
    pub to_idx: u32,
    pub from_slug_hash: u64,
    pub to_slug_hash: u64,
    pub edge_type_hash: u64,
    pub weight: f32,
    pub timestamp: u64,
    pub meta: Option<String>,
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct VectorSlot {
    pub data: [f32; 128],
}

impl Default for VectorSlot {
    fn default() -> Self { Self { data: [0.0f32; 128] } }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct SpatialNode {
    pub id: u32,
    pub coords: [f32; 2],
}

impl RTreeObject for SpatialNode {
    type Envelope = rstar::AABB<[f32; 2]>;
    fn envelope(&self) -> Self::Envelope { rstar::AABB::from_point(self.coords) }
}

impl rstar::PointDistance for SpatialNode {
    fn distance_2(&self, point: &[f32; 2]) -> f32 {
        let dx = self.coords[0] - point[0];
        let dy = self.coords[1] - point[1];
        dx * dx + dy * dy
    }
}

pub struct TraversalResult {
    pub nodes: Vec<u32>,
    pub edges: Vec<(u32, u32, f32)>,
    pub path: Vec<u64>,
}

pub struct CollectionSchema {
    pub vector_fields: Vec<String>,
    pub spatial_fields: Vec<String>,
    pub fulltext_fields: Vec<String>,
    /// Fields indexed for O(1) equality queries via HashIndex
    pub hash_indexed_fields: Vec<String>,
    /// Fields indexed for O(log N) range queries via RangeIndex
    pub range_indexed_fields: Vec<String>,
}

// ============ NEW TYPES FOR SCENARIO 12 ============

/// A single step in the query pipeline. Stack-allocated in SmallVec.
#[derive(Clone, Debug)]
pub enum Step {
    // Starters (produce initial candidate set)
    One(u64),                         // slug_hash
    Many(Vec<u64>),                   // multiple slug_hashes
    Collection(u64),                   // collection_hash
    All,

    // Graph transforms
    Forward(u64),                     // edge_type_hash (single-threaded)
    Backward(u64),                    // edge_type_hash (single-threaded)
    ForwardParallel(u64),             // edge_type_hash (multi-threaded with Rayon)
    BackwardParallel(u64),            // edge_type_hash (multi-threaded with Rayon)
    Hops(u32),                        // max depth
    Leaves,                           // filter to nodes with no outgoing edges
    Roots,                            // filter to nodes with no incoming edges

    // Search transforms
    Near(f32, f32, f32),              // lat, lon, radius_km (radius is squared internally)
    Similar(Vec<f32>, usize),          // query_vec, k

    #[cfg(feature = "fulltext")]
    Matching(String),                 // fulltext query

    // Payload filters
    WhereEq(String, Value),            // field, value
    WhereBetween(String, f64, f64),   // field, lo, hi
    WhereGt(String, f64),             // field, threshold
    WhereLt(String, f64),             // field, threshold
    WhereGte(String, f64),            // field, threshold (>=)
    WhereLte(String, f64),            // field, threshold (<=)
    WhereIn(String, Vec<Value>),      // field, values

    // Set algebra
    Intersect(Vec<Step>),            // another pipeline to intersect with
    Union(Vec<Step>),                 // another pipeline to union with
    Subtract(Vec<Step>),              // another pipeline to subtract

    // Ordering / pagination / projection
    Sort(String, bool),               // field, ascending
    Skip(usize),                      // skip N results
    Select(Vec<String>),              // return only these fields from payload

    // Limit
    Take(usize),
}

/// Execution trace for observability
#[derive(Clone, Debug)]
pub struct StepReport {
    pub atom: String,
    pub input_size: usize,
    pub output_size: usize,
    pub index_used: String,
    pub time_us: u64,
}

/// Trace containing step-by-step execution report
#[derive(Clone, Debug)]
pub struct Trace {
    pub steps: Vec<StepReport>,
    pub total_us: u64,
}

/// Every query returns Outcome<T> — data + trace
#[derive(Clone, Debug)]
pub struct Outcome<T> {
    pub data: T,
    pub trace: Trace,
}

/// A resolved node result with payload
#[derive(Clone, Debug)]
pub struct Hit {
    pub idx: u32,
    pub slug_hash: u64,
    pub collection_hash: u64,
    pub payload: Option<String>,
    pub lat: f32,
    pub lon: f32,
}

/// Plan type returned by explain()
#[derive(Clone, Debug)]
pub struct Plan {
    pub steps: Vec<Step>,
}

/// Aggregation operation for avg/sum
pub(crate) enum AggOp {
    Avg,
    Sum,
}

// JSON serialization for Step
impl Step {
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Step::One(hash) => serde_json::json!({ "op": "one", "slug_hash": hash }),
            Step::Many(hashes) => serde_json::json!({ "op": "many", "slug_hashes": hashes }),
            Step::Collection(hash) => serde_json::json!({ "op": "collection", "collection_hash": hash }),
            Step::All => serde_json::json!({ "op": "all" }),
            Step::Forward(hash) => serde_json::json!({ "op": "forward", "type_hash": hash }),
            Step::Backward(hash) => serde_json::json!({ "op": "backward", "type_hash": hash }),
            Step::ForwardParallel(hash) => serde_json::json!({ "op": "forward_parallel", "type_hash": hash }),
            Step::BackwardParallel(hash) => serde_json::json!({ "op": "backward_parallel", "type_hash": hash }),
            Step::Hops(n) => serde_json::json!({ "op": "hops", "n": n }),
            Step::Leaves => serde_json::json!({ "op": "leaves" }),
            Step::Roots => serde_json::json!({ "op": "roots" }),
            Step::Near(lat, lon, radius) => serde_json::json!({ "op": "near", "lat": lat, "lon": lon, "radius": radius }),
            Step::Similar(query, k) => serde_json::json!({ "op": "similar", "query": query, "k": k }),
            #[cfg(feature = "fulltext")]
            Step::Matching(text) => serde_json::json!({ "op": "matching", "text": text }),
            Step::WhereEq(field, value) => serde_json::json!({ "op": "where_eq", "field": field, "value": value }),
            Step::WhereBetween(field, lo, hi) => serde_json::json!({ "op": "where_between", "field": field, "lo": lo, "hi": hi }),
            Step::WhereGt(field, threshold) => serde_json::json!({ "op": "where_gt", "field": field, "threshold": threshold }),
            Step::WhereLt(field, threshold) => serde_json::json!({ "op": "where_lt", "field": field, "threshold": threshold }),
            Step::WhereGte(field, threshold) => serde_json::json!({ "op": "where_gte", "field": field, "threshold": threshold }),
            Step::WhereLte(field, threshold) => serde_json::json!({ "op": "where_lte", "field": field, "threshold": threshold }),
            Step::WhereIn(field, values) => serde_json::json!({ "op": "where_in", "field": field, "values": values }),
            Step::Intersect(steps) => serde_json::json!({ "op": "intersect", "steps": steps.iter().map(|s| s.to_json()).collect::<Vec<_>>() }),
            Step::Union(steps) => serde_json::json!({ "op": "union", "steps": steps.iter().map(|s| s.to_json()).collect::<Vec<_>>() }),
            Step::Subtract(steps) => serde_json::json!({ "op": "subtract", "steps": steps.iter().map(|s| s.to_json()).collect::<Vec<_>>() }),
            Step::Sort(field, asc) => serde_json::json!({ "op": "sort", "field": field, "asc": asc }),
            Step::Skip(n) => serde_json::json!({ "op": "skip", "n": n }),
            Step::Select(fields) => serde_json::json!({ "op": "select", "fields": fields }),
            Step::Take(n) => serde_json::json!({ "op": "take", "n": n }),
        }
    }
}

// JSON serialization for Trace
impl Trace {
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "steps": self.steps.iter().map(|s| serde_json::json!({
                "atom": s.atom,
                "input_size": s.input_size,
                "output_size": s.output_size,
                "index_used": s.index_used,
                "time_us": s.time_us
            })).collect::<Vec<_>>(),
            "total_us": self.total_us
        })
    }
}
