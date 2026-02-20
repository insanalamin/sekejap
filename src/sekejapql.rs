// SekejapQL Parser - JSON Pipeline Query Language
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;
use crate::types::Step;

// ============================================================================
// OPTIMIZED: Typed serde deserialization (~2-3x faster)
// ============================================================================

#[derive(Serialize, Deserialize, Debug)]
pub struct Query {
    pub pipeline: Vec<StepDef>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "op")]
pub enum StepDef {
    #[serde(rename = "one")]
    One { slug: Option<String>, #[serde(rename = "slug_hash")] slug_hash: Option<u64> },
    #[serde(rename = "many")]
    Many { #[serde(rename = "slugs")] slugs: Option<Vec<String>> },
    #[serde(rename = "all")]
    All,
    #[serde(rename = "collection")]
    Collection { #[serde(rename = "name")] name: Option<String> },
    #[serde(rename = "forward")]
    Forward { #[serde(rename = "type")] r#type: Option<String> },
    #[serde(rename = "backward")]
    Backward { #[serde(rename = "type")] r#type: Option<String> },
    #[serde(rename = "forward_parallel")]
    ForwardParallel { #[serde(rename = "type")] r#type: Option<String> },
    #[serde(rename = "backward_parallel")]
    BackwardParallel { #[serde(rename = "type")] r#type: Option<String> },
    #[serde(rename = "hops")]
    Hops { #[serde(rename = "n")] n: Option<u32> },
    #[serde(rename = "leaves")]
    Leaves,
    #[serde(rename = "roots")]
    Roots,
    #[serde(rename = "near")]
    Near { #[serde(rename = "lat")] lat: Option<f32>, #[serde(rename = "lon")] lon: Option<f32>, #[serde(rename = "radius")] radius: Option<f32> },
    #[serde(rename = "similar")]
    Similar { #[serde(rename = "query")] query: Option<Vec<f32>>, k: Option<usize> },
    #[serde(rename = "where_eq")]
    WhereEq { #[serde(rename = "field")] field: Option<String>, value: Option<Value> },
    #[serde(rename = "where_gt")]
    WhereGt { #[serde(rename = "field")] field: Option<String>, #[serde(rename = "value")] value: Option<f64> },
    #[serde(rename = "where_between")]
    WhereBetween { #[serde(rename = "field")] field: Option<String>, #[serde(rename = "lo")] lo: Option<f64>, #[serde(rename = "hi")] hi: Option<f64> },
    #[serde(rename = "where_in")]
    WhereIn { #[serde(rename = "field")] field: Option<String>, #[serde(rename = "values")] values: Option<Vec<Value>> },
    #[serde(rename = "intersect")]
    Intersect { #[serde(rename = "other")] other: Option<Vec<StepDef>> },
    #[serde(rename = "union")]
    Union { #[serde(rename = "other")] other: Option<Vec<StepDef>> },
    #[serde(rename = "subtract")]
    Subtract { #[serde(rename = "other")] other: Option<Vec<StepDef>> },
    #[serde(rename = "take")]
    Take { #[serde(rename = "n")] n: Option<usize> },
    #[serde(rename = "where_lt")]
    WhereLt { field: Option<String>, value: Option<f64> },
    #[serde(rename = "where_lte")]
    WhereLte { field: Option<String>, value: Option<f64> },
    #[serde(rename = "where_gte")]
    WhereGte { field: Option<String>, value: Option<f64> },
    #[serde(rename = "sort")]
    Sort { field: Option<String>, asc: Option<bool> },
    #[serde(rename = "skip")]
    Skip { n: Option<usize> },
    #[serde(rename = "select")]
    Select { fields: Option<Vec<String>> },
}

impl StepDef {
    pub fn to_step(&self) -> Result<Step, Box<dyn Error>> {
        match self {
            StepDef::One { slug, slug_hash } => {
                let h = if let Some(s) = slug { sekejapql_hash(s) } else if let Some(h) = slug_hash { *h } else { return Err("one: missing slug".into()) };
                Ok(Step::One(h))
            }
            StepDef::Many { slugs } => {
                let h = slugs.as_ref().ok_or("many: missing slugs")?.iter().map(|s| sekejapql_hash(s)).collect();
                Ok(Step::Many(h))
            }
            StepDef::All => Ok(Step::All),
            StepDef::Collection { name } => Ok(Step::Collection(sekejapql_hash(name.as_ref().ok_or("collection: missing name")?))),
            StepDef::Forward { r#type } => Ok(Step::Forward(sekejapql_hash(r#type.as_ref().unwrap_or(&"related".to_string())))),
            StepDef::Backward { r#type } => Ok(Step::Backward(sekejapql_hash(r#type.as_ref().unwrap_or(&"related".to_string())))),
            StepDef::ForwardParallel { r#type } => Ok(Step::ForwardParallel(sekejapql_hash(r#type.as_ref().unwrap_or(&"related".to_string())))),
            StepDef::BackwardParallel { r#type } => Ok(Step::BackwardParallel(sekejapql_hash(r#type.as_ref().unwrap_or(&"related".to_string())))),
            StepDef::Hops { n } => Ok(Step::Hops(n.unwrap_or(1))),
            StepDef::Leaves => Ok(Step::Leaves),
            StepDef::Roots => Ok(Step::Roots),
            StepDef::Near { lat, lon, radius } => Ok(Step::Near(lat.unwrap_or(0.0), lon.unwrap_or(0.0), radius.unwrap_or(10.0))),
            StepDef::Similar { query, k } => Ok(Step::Similar(query.clone().unwrap_or_default(), k.unwrap_or(10))),
            StepDef::WhereEq { field, value } => Ok(Step::WhereEq(field.as_ref().ok_or("where_eq: missing field")?.clone(), value.clone().ok_or("where_eq: missing value")?)),
            StepDef::WhereGt { field, value } => Ok(Step::WhereGt(field.as_ref().ok_or("where_gt: missing field")?.clone(), value.ok_or("where_gt: missing value")?)),
            StepDef::WhereBetween { field, lo, hi } => Ok(Step::WhereBetween(field.as_ref().ok_or("where_between: missing field")?.clone(), lo.ok_or("where_between: missing lo")?, hi.ok_or("where_between: missing hi")?)),
            StepDef::WhereIn { field, values } => Ok(Step::WhereIn(field.as_ref().ok_or("where_in: missing field")?.clone(), values.clone().ok_or("where_in: missing values")?)),
            StepDef::Intersect { other } => Ok(Step::Intersect(other.as_ref().ok_or("intersect: missing other")?.iter().map(|s| s.to_step()).collect::<Result<_, _>>()?)),
            StepDef::Union { other } => Ok(Step::Union(other.as_ref().ok_or("union: missing other")?.iter().map(|s| s.to_step()).collect::<Result<_, _>>()?)),
            StepDef::Subtract { other } => Ok(Step::Subtract(other.as_ref().ok_or("subtract: missing other")?.iter().map(|s| s.to_step()).collect::<Result<_, _>>()?)),
            StepDef::Take { n } => Ok(Step::Take(n.unwrap_or(100))),
            StepDef::WhereLt { field, value } => Ok(Step::WhereLt(field.as_ref().ok_or("where_lt: missing field")?.clone(), value.ok_or("where_lt: missing value")?)),
            StepDef::WhereLte { field, value } => Ok(Step::WhereLte(field.as_ref().ok_or("where_lte: missing field")?.clone(), value.ok_or("where_lte: missing value")?)),
            StepDef::WhereGte { field, value } => Ok(Step::WhereGte(field.as_ref().ok_or("where_gte: missing field")?.clone(), value.ok_or("where_gte: missing value")?)),
            StepDef::Sort { field, asc } => Ok(Step::Sort(field.as_ref().ok_or("sort: missing field")?.clone(), asc.unwrap_or(true))),
            StepDef::Skip { n } => Ok(Step::Skip(n.unwrap_or(0))),
            StepDef::Select { fields } => Ok(Step::Select(fields.clone().ok_or("select: missing fields")?)),
        }
    }
}

// ============================================================================
// API (uses Value intermediate for compatibility)
// ============================================================================

#[derive(Clone)]
pub struct SecurityLimits {
    pub max_pipeline_length: usize,
    pub max_nested_pipelines: usize,
    pub max_slug_length: usize,
    pub max_text_length: usize,
}

impl Default for SecurityLimits {
    fn default() -> Self {
        Self { max_pipeline_length: 50, max_nested_pipelines: 3, max_slug_length: 1024, max_text_length: 1_000_000 }
    }
}

pub struct SekejapQL { limits: SecurityLimits }

impl SekejapQL {
    pub fn new() -> Self { Self { limits: SecurityLimits::default() } }
    pub fn with_limits(limits: SecurityLimits) -> Self { Self { limits } }

    /// Parse via Value intermediate
    pub fn parse_pipeline(&self, doc: &Value) -> Result<Vec<Step>, Box<dyn Error>> {
        let pipeline = doc.get("pipeline").and_then(|v| v.as_array()).ok_or("Query missing pipeline array")?;
        if pipeline.len() > self.limits.max_pipeline_length { return Err(format!("Pipeline exceeds {}", self.limits.max_pipeline_length).into()); }
        let mut steps = Vec::new();
        for step_obj in pipeline.iter() { steps.push(self.parse_step(step_obj, 0)?); }
        Ok(steps)
    }

    /// NEW: Direct deserialization (~2-3x faster, skips Value intermediate)
    pub fn parse_pipeline_direct(&self, json: &str) -> Result<Vec<Step>, Box<dyn Error>> {
        let query: Query = serde_json::from_str(json)?;
        if query.pipeline.len() > self.limits.max_pipeline_length { return Err(format!("Pipeline exceeds {}", self.limits.max_pipeline_length).into()); }
        query.pipeline.iter().map(|s| s.to_step()).collect()
    }

    fn parse_step(&self, obj: &Value, depth: usize) -> Result<Step, Box<dyn Error>> {
        if depth > self.limits.max_nested_pipelines { return Err("Nested pipeline exceeds max depth".into()); }
        let op = obj.get("op").and_then(|v| v.as_str()).ok_or("Missing op")?;
        match op {
            "one" => {
                let h = if let Some(s) = obj.get("slug").and_then(|v| v.as_str()) { sekejapql_hash(s) }
                        else if let Some(h) = obj.get("slug_hash").and_then(|v| v.as_u64()) { h }
                        else { return Err("one: missing slug".into()) };
                Ok(Step::One(h))
            }
            "many" => {
                let hashes: Vec<u64> = obj.get("slugs").and_then(|v| v.as_array()).ok_or("many: missing slugs")?.iter().filter_map(|v| v.as_str()).map(sekejapql_hash).collect();
                Ok(Step::Many(hashes))
            }
            "all" => Ok(Step::All),
            "collection" => {
                let h = obj.get("name").and_then(|v| v.as_str()).ok_or("collection: missing name")?;
                Ok(Step::Collection(sekejapql_hash(h)))
            }
            "forward" => {
                let e = obj.get("type").or(obj.get("edge_type")).and_then(|v| v.as_str()).unwrap_or("related");
                Ok(Step::Forward(sekejapql_hash(e)))
            }
            "backward" => {
                let e = obj.get("type").or(obj.get("edge_type")).and_then(|v| v.as_str()).unwrap_or("related");
                Ok(Step::Backward(sekejapql_hash(e)))
            }
            "forward_parallel" => {
                let e = obj.get("type").or(obj.get("edge_type")).and_then(|v| v.as_str()).unwrap_or("related");
                Ok(Step::ForwardParallel(sekejapql_hash(e)))
            }
            "backward_parallel" => {
                let e = obj.get("type").or(obj.get("edge_type")).and_then(|v| v.as_str()).unwrap_or("related");
                Ok(Step::BackwardParallel(sekejapql_hash(e)))
            }
            "hops" => Ok(Step::Hops(obj.get("n").and_then(|v| v.as_u64()).unwrap_or(1) as u32)),
            "leaves" => Ok(Step::Leaves),
            "roots" => Ok(Step::Roots),
            "near" => Ok(Step::Near(
                obj.get("lat").and_then(|v| v.as_f64()).unwrap_or(0.0) as f32,
                obj.get("lon").and_then(|v| v.as_f64()).unwrap_or(0.0) as f32,
                obj.get("radius").or(obj.get("radius_km")).and_then(|v| v.as_f64()).unwrap_or(10.0) as f32,
            )),
            "similar" => Ok(Step::Similar(
                obj.get("query").and_then(|v| v.as_array()).map(|a| a.iter().filter_map(|v| v.as_f64().map(|f| f as f32)).collect()).unwrap_or_default(),
                obj.get("k").and_then(|v| v.as_u64()).map(|n| n as usize).unwrap_or(10),
            )),
            "where_eq" => Ok(Step::WhereEq(
                obj.get("field").or(obj.get("key")).and_then(|v| v.as_str()).ok_or("where_eq: missing field")?.to_string(),
                obj.get("value").cloned().ok_or("where_eq: missing value")?,
            )),
            "where_gt" => Ok(Step::WhereGt(
                obj.get("field").or(obj.get("key")).and_then(|v| v.as_str()).ok_or("where_gt: missing field")?.to_string(),
                obj.get("value").or(obj.get("threshold")).and_then(|v| v.as_f64()).ok_or("where_gt: missing value")?,
            )),
            "where_between" => Ok(Step::WhereBetween(
                obj.get("field").or(obj.get("key")).and_then(|v| v.as_str()).ok_or("where_between: missing field")?.to_string(),
                obj.get("lo").or(obj.get("min")).or(obj.get("from")).and_then(|v| v.as_f64()).ok_or("where_between: missing lo")?,
                obj.get("hi").or(obj.get("max")).or(obj.get("to")).and_then(|v| v.as_f64()).ok_or("where_between: missing hi")?,
            )),
            "where_in" => Ok(Step::WhereIn(
                obj.get("field").or(obj.get("key")).and_then(|v| v.as_str()).ok_or("where_in: missing field")?.to_string(),
                obj.get("values").or(obj.get("in")).and_then(|v| v.as_array()).ok_or("where_in: missing values")?.to_vec(),
            )),
            "intersect" => {
                let other = obj.get("other").or(obj.get("set")).and_then(|v| v.as_array()).ok_or("intersect: missing other")?;
                let other_steps = self.parse_pipeline(&serde_json::json!({"pipeline": other}))?;
                Ok(Step::Intersect(other_steps))
            }
            "union" => {
                let other = obj.get("other").or(obj.get("set")).and_then(|v| v.as_array()).ok_or("union: missing other")?;
                let other_steps = self.parse_pipeline(&serde_json::json!({"pipeline": other}))?;
                Ok(Step::Union(other_steps))
            }
            "subtract" | "difference" => {
                let other = obj.get("other").or(obj.get("set")).and_then(|v| v.as_array()).ok_or("subtract: missing other")?;
                let other_steps = self.parse_pipeline(&serde_json::json!({"pipeline": other}))?;
                Ok(Step::Subtract(other_steps))
            }
            "take" | "limit" => Ok(Step::Take(obj.get("n").or(obj.get("limit")).and_then(|v| v.as_u64()).map(|n| n as usize).unwrap_or(100))),
            "where_lt" => Ok(Step::WhereLt(
                obj.get("field").or(obj.get("key")).and_then(|v| v.as_str()).ok_or("where_lt: missing field")?.to_string(),
                obj.get("value").or(obj.get("threshold")).and_then(|v| v.as_f64()).ok_or("where_lt: missing value")?,
            )),
            "where_lte" => Ok(Step::WhereLte(
                obj.get("field").or(obj.get("key")).and_then(|v| v.as_str()).ok_or("where_lte: missing field")?.to_string(),
                obj.get("value").or(obj.get("threshold")).and_then(|v| v.as_f64()).ok_or("where_lte: missing value")?,
            )),
            "where_gte" => Ok(Step::WhereGte(
                obj.get("field").or(obj.get("key")).and_then(|v| v.as_str()).ok_or("where_gte: missing field")?.to_string(),
                obj.get("value").or(obj.get("threshold")).and_then(|v| v.as_f64()).ok_or("where_gte: missing value")?,
            )),
            "sort" => Ok(Step::Sort(
                obj.get("field").and_then(|v| v.as_str()).ok_or("sort: missing field")?.to_string(),
                obj.get("asc").and_then(|v| v.as_bool()).unwrap_or(true),
            )),
            "skip" => Ok(Step::Skip(obj.get("n").and_then(|v| v.as_u64()).map(|n| n as usize).unwrap_or(0))),
            "select" => Ok(Step::Select(
                obj.get("fields").and_then(|v| v.as_array()).ok_or("select: missing fields")?.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect(),
            )),
            _ => Err(format!("Unknown op: {}", op).into()),
        }
    }
}

fn sekejapql_hash(s: &str) -> u64 { seahash::hash(s.as_bytes()) }

impl crate::types::Outcome<Vec<crate::types::Hit>> {
    pub fn to_json_response(&self) -> Value {
        serde_json::json!({
            "data": self.data.iter().map(|h| serde_json::json!({"idx": h.idx, "slug_hash": h.slug_hash, "collection_hash": h.collection_hash, "payload": h.payload, "lat": h.lat, "lon": h.lon})).collect::<Vec<_>>(),
            "trace": self.trace.to_json()
        })
    }
}
