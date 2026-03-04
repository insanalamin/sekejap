// Query Compiler - JSON pipeline query language
use crate::types::Step;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;

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
    One {
        slug: Option<String>,
        #[serde(rename = "slug_hash")]
        slug_hash: Option<u64>,
    },
    #[serde(rename = "many")]
    Many {
        #[serde(rename = "slugs")]
        slugs: Option<Vec<String>>,
    },
    #[serde(rename = "all")]
    All,
    #[serde(rename = "collection")]
    Collection {
        #[serde(rename = "name")]
        name: Option<String>,
    },
    #[serde(rename = "forward")]
    Forward {
        #[serde(rename = "type")]
        r#type: Option<String>,
    },
    #[serde(rename = "backward")]
    Backward {
        #[serde(rename = "type")]
        r#type: Option<String>,
    },
    #[serde(rename = "forward_parallel")]
    ForwardParallel {
        #[serde(rename = "type")]
        r#type: Option<String>,
    },
    #[serde(rename = "backward_parallel")]
    BackwardParallel {
        #[serde(rename = "type")]
        r#type: Option<String>,
    },
    #[serde(rename = "hops")]
    Hops {
        #[serde(rename = "n")]
        n: Option<u32>,
    },
    #[serde(rename = "leaves")]
    Leaves,
    #[serde(rename = "roots")]
    Roots,
    #[serde(rename = "near")]
    Near {
        #[serde(rename = "lat")]
        lat: Option<f32>,
        #[serde(rename = "lon")]
        lon: Option<f32>,
        #[serde(rename = "radius")]
        radius: Option<f32>,
    },
    #[serde(rename = "spatial_within_bbox")]
    SpatialWithinBbox {
        min_lat: Option<f32>,
        min_lon: Option<f32>,
        max_lat: Option<f32>,
        max_lon: Option<f32>,
    },
    #[serde(rename = "spatial_intersects_bbox")]
    SpatialIntersectsBbox {
        min_lat: Option<f32>,
        min_lon: Option<f32>,
        max_lat: Option<f32>,
        max_lon: Option<f32>,
    },
    #[serde(rename = "spatial_within_polygon")]
    SpatialWithinPolygon { polygon: Option<Vec<[f32; 2]>> },
    #[serde(rename = "st_within")]
    StWithin { polygon: Option<Vec<[f32; 2]>> },
    #[serde(rename = "st_contains")]
    StContains { polygon: Option<Vec<[f32; 2]>> },
    #[serde(rename = "st_intersects")]
    StIntersects { polygon: Option<Vec<[f32; 2]>> },
    #[serde(rename = "st_dwithin")]
    StDWithin {
        lat: Option<f32>,
        lon: Option<f32>,
        distance_km: Option<f32>,
    },
    #[serde(rename = "similar")]
    Similar {
        #[serde(rename = "query")]
        query: Option<Vec<f32>>,
        k: Option<usize>,
    },
    #[serde(rename = "matching")]
    Matching {
        text: Option<String>,
        limit: Option<usize>,
        title_weight: Option<f32>,
        content_weight: Option<f32>,
    },
    #[serde(rename = "where_eq")]
    WhereEq {
        #[serde(rename = "field")]
        field: Option<String>,
        value: Option<Value>,
    },
    #[serde(rename = "where_gt")]
    WhereGt {
        #[serde(rename = "field")]
        field: Option<String>,
        #[serde(rename = "value")]
        value: Option<f64>,
    },
    #[serde(rename = "where_between")]
    WhereBetween {
        #[serde(rename = "field")]
        field: Option<String>,
        #[serde(rename = "lo")]
        lo: Option<f64>,
        #[serde(rename = "hi")]
        hi: Option<f64>,
    },
    #[serde(rename = "where_in")]
    WhereIn {
        #[serde(rename = "field")]
        field: Option<String>,
        #[serde(rename = "values")]
        values: Option<Vec<Value>>,
    },
    #[serde(rename = "intersect")]
    Intersect {
        #[serde(rename = "other")]
        other: Option<Vec<StepDef>>,
    },
    #[serde(rename = "union")]
    Union {
        #[serde(rename = "other")]
        other: Option<Vec<StepDef>>,
    },
    #[serde(rename = "subtract")]
    Subtract {
        #[serde(rename = "other")]
        other: Option<Vec<StepDef>>,
    },
    #[serde(rename = "take")]
    Take {
        #[serde(rename = "n")]
        n: Option<usize>,
    },
    #[serde(rename = "where_lt")]
    WhereLt {
        field: Option<String>,
        value: Option<f64>,
    },
    #[serde(rename = "where_lte")]
    WhereLte {
        field: Option<String>,
        value: Option<f64>,
    },
    #[serde(rename = "where_gte")]
    WhereGte {
        field: Option<String>,
        value: Option<f64>,
    },
    #[serde(rename = "sort")]
    Sort {
        field: Option<String>,
        asc: Option<bool>,
    },
    #[serde(rename = "skip")]
    Skip { n: Option<usize> },
    #[serde(rename = "select")]
    Select { fields: Option<Vec<String>> },
}

impl StepDef {
    pub fn to_step(&self) -> Result<Step, Box<dyn Error>> {
        match self {
            StepDef::One { slug, slug_hash } => {
                let h = if let Some(s) = slug {
                    sekejapql_hash(s)
                } else if let Some(h) = slug_hash {
                    *h
                } else {
                    return Err("one: missing slug".into());
                };
                Ok(Step::One(h))
            }
            StepDef::Many { slugs } => {
                let h = slugs
                    .as_ref()
                    .ok_or("many: missing slugs")?
                    .iter()
                    .map(|s| sekejapql_hash(s))
                    .collect();
                Ok(Step::Many(h))
            }
            StepDef::All => Ok(Step::All),
            StepDef::Collection { name } => Ok(Step::Collection(sekejapql_hash(
                name.as_ref().ok_or("collection: missing name")?,
            ))),
            StepDef::Forward { r#type } => Ok(Step::Forward(sekejapql_hash(
                r#type.as_ref().unwrap_or(&"related".to_string()),
            ))),
            StepDef::Backward { r#type } => Ok(Step::Backward(sekejapql_hash(
                r#type.as_ref().unwrap_or(&"related".to_string()),
            ))),
            StepDef::ForwardParallel { r#type } => Ok(Step::ForwardParallel(sekejapql_hash(
                r#type.as_ref().unwrap_or(&"related".to_string()),
            ))),
            StepDef::BackwardParallel { r#type } => Ok(Step::BackwardParallel(sekejapql_hash(
                r#type.as_ref().unwrap_or(&"related".to_string()),
            ))),
            StepDef::Hops { n } => Ok(Step::Hops(n.unwrap_or(1))),
            StepDef::Leaves => Ok(Step::Leaves),
            StepDef::Roots => Ok(Step::Roots),
            StepDef::Near { lat, lon, radius } => Ok(Step::Near(
                lat.unwrap_or(0.0),
                lon.unwrap_or(0.0),
                radius.unwrap_or(10.0),
            )),
            StepDef::SpatialWithinBbox {
                min_lat,
                min_lon,
                max_lat,
                max_lon,
            } => Ok(Step::SpatialWithinBbox(
                min_lat.unwrap_or(-90.0),
                min_lon.unwrap_or(-180.0),
                max_lat.unwrap_or(90.0),
                max_lon.unwrap_or(180.0),
            )),
            StepDef::SpatialIntersectsBbox {
                min_lat,
                min_lon,
                max_lat,
                max_lon,
            } => Ok(Step::SpatialIntersectsBbox(
                min_lat.unwrap_or(-90.0),
                min_lon.unwrap_or(-180.0),
                max_lat.unwrap_or(90.0),
                max_lon.unwrap_or(180.0),
            )),
            StepDef::SpatialWithinPolygon { polygon } => Ok(Step::SpatialWithinPolygon(
                polygon
                    .clone()
                    .ok_or("spatial_within_polygon: missing polygon")?,
            )),
            StepDef::StWithin { polygon } => Ok(Step::StWithin(
                polygon.clone().ok_or("st_within: missing polygon")?,
            )),
            StepDef::StContains { polygon } => Ok(Step::StContains(
                polygon.clone().ok_or("st_contains: missing polygon")?,
            )),
            StepDef::StIntersects { polygon } => Ok(Step::StIntersects(
                polygon.clone().ok_or("st_intersects: missing polygon")?,
            )),
            StepDef::StDWithin { lat, lon, distance_km } => Ok(Step::StDWithin(
                lat.unwrap_or(0.0),
                lon.unwrap_or(0.0),
                distance_km.unwrap_or(1.0),
            )),
            StepDef::Similar { query, k } => Ok(Step::Similar(
                query.clone().unwrap_or_default(),
                k.unwrap_or(10),
            )),
            #[cfg(feature = "fulltext")]
            StepDef::Matching {
                text,
                limit,
                title_weight,
                content_weight,
            } => Ok(Step::Matching {
                text: text.clone().ok_or("matching: missing text")?,
                limit: limit.unwrap_or(1000),
                title_weight: title_weight.unwrap_or(1.0),
                content_weight: content_weight.unwrap_or(1.0),
            }),
            #[cfg(not(feature = "fulltext"))]
            StepDef::Matching { .. } => Err("matching requires fulltext feature".into()),
            StepDef::WhereEq { field, value } => Ok(Step::WhereEq(
                field.as_ref().ok_or("where_eq: missing field")?.clone(),
                value.clone().ok_or("where_eq: missing value")?,
            )),
            StepDef::WhereGt { field, value } => Ok(Step::WhereGt(
                field.as_ref().ok_or("where_gt: missing field")?.clone(),
                value.ok_or("where_gt: missing value")?,
            )),
            StepDef::WhereBetween { field, lo, hi } => Ok(Step::WhereBetween(
                field
                    .as_ref()
                    .ok_or("where_between: missing field")?
                    .clone(),
                lo.ok_or("where_between: missing lo")?,
                hi.ok_or("where_between: missing hi")?,
            )),
            StepDef::WhereIn { field, values } => Ok(Step::WhereIn(
                field.as_ref().ok_or("where_in: missing field")?.clone(),
                values.clone().ok_or("where_in: missing values")?,
            )),
            StepDef::Intersect { other } => Ok(Step::Intersect(
                other
                    .as_ref()
                    .ok_or("intersect: missing other")?
                    .iter()
                    .map(|s| s.to_step())
                    .collect::<Result<_, _>>()?,
            )),
            StepDef::Union { other } => Ok(Step::Union(
                other
                    .as_ref()
                    .ok_or("union: missing other")?
                    .iter()
                    .map(|s| s.to_step())
                    .collect::<Result<_, _>>()?,
            )),
            StepDef::Subtract { other } => Ok(Step::Subtract(
                other
                    .as_ref()
                    .ok_or("subtract: missing other")?
                    .iter()
                    .map(|s| s.to_step())
                    .collect::<Result<_, _>>()?,
            )),
            StepDef::Take { n } => Ok(Step::Take(n.unwrap_or(100))),
            StepDef::WhereLt { field, value } => Ok(Step::WhereLt(
                field.as_ref().ok_or("where_lt: missing field")?.clone(),
                value.ok_or("where_lt: missing value")?,
            )),
            StepDef::WhereLte { field, value } => Ok(Step::WhereLte(
                field.as_ref().ok_or("where_lte: missing field")?.clone(),
                value.ok_or("where_lte: missing value")?,
            )),
            StepDef::WhereGte { field, value } => Ok(Step::WhereGte(
                field.as_ref().ok_or("where_gte: missing field")?.clone(),
                value.ok_or("where_gte: missing value")?,
            )),
            StepDef::Sort { field, asc } => Ok(Step::Sort(
                field.as_ref().ok_or("sort: missing field")?.clone(),
                asc.unwrap_or(true),
            )),
            StepDef::Skip { n } => Ok(Step::Skip(n.unwrap_or(0))),
            StepDef::Select { fields } => Ok(Step::Select(
                fields.clone().ok_or("select: missing fields")?,
            )),
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
        Self {
            max_pipeline_length: 50,
            max_nested_pipelines: 3,
            max_slug_length: 1024,
            max_text_length: 1_000_000,
        }
    }
}

pub struct QueryCompiler {
    limits: SecurityLimits,
}

impl QueryCompiler {
    pub fn new() -> Self {
        Self {
            limits: SecurityLimits::default(),
        }
    }
    pub fn with_limits(limits: SecurityLimits) -> Self {
        Self { limits }
    }

    /// Parse via Value intermediate
    pub fn parse_pipeline(&self, doc: &Value) -> Result<Vec<Step>, Box<dyn Error>> {
        let pipeline = doc
            .get("pipeline")
            .and_then(|v| v.as_array())
            .ok_or("Query missing pipeline array")?;
        if pipeline.len() > self.limits.max_pipeline_length {
            return Err(format!("Pipeline exceeds {}", self.limits.max_pipeline_length).into());
        }
        let mut steps = Vec::new();
        for step_obj in pipeline.iter() {
            steps.push(self.parse_step(step_obj, 0)?);
        }
        Ok(steps)
    }

    /// NEW: Direct deserialization (~2-3x faster, skips Value intermediate)
    pub fn parse_pipeline_direct(&self, json: &str) -> Result<Vec<Step>, Box<dyn Error>> {
        let query: Query = serde_json::from_str(json)?;
        if query.pipeline.len() > self.limits.max_pipeline_length {
            return Err(format!("Pipeline exceeds {}", self.limits.max_pipeline_length).into());
        }
        query.pipeline.iter().map(|s| s.to_step()).collect()
    }

    fn parse_step(&self, obj: &Value, depth: usize) -> Result<Step, Box<dyn Error>> {
        if depth > self.limits.max_nested_pipelines {
            return Err("Nested pipeline exceeds max depth".into());
        }
        let op = obj.get("op").and_then(|v| v.as_str()).ok_or("Missing op")?;
        match op {
            "one" => {
                let h = if let Some(s) = obj.get("slug").and_then(|v| v.as_str()) {
                    sekejapql_hash(s)
                } else if let Some(h) = obj.get("slug_hash").and_then(|v| v.as_u64()) {
                    h
                } else {
                    return Err("one: missing slug".into());
                };
                Ok(Step::One(h))
            }
            "many" => {
                let hashes: Vec<u64> = obj
                    .get("slugs")
                    .and_then(|v| v.as_array())
                    .ok_or("many: missing slugs")?
                    .iter()
                    .filter_map(|v| v.as_str())
                    .map(sekejapql_hash)
                    .collect();
                Ok(Step::Many(hashes))
            }
            "all" => Ok(Step::All),
            "collection" => {
                let h = obj
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or("collection: missing name")?;
                Ok(Step::Collection(sekejapql_hash(h)))
            }
            "forward" => {
                let e = obj
                    .get("type")
                    .or(obj.get("edge_type"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("related");
                Ok(Step::Forward(sekejapql_hash(e)))
            }
            "backward" => {
                let e = obj
                    .get("type")
                    .or(obj.get("edge_type"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("related");
                Ok(Step::Backward(sekejapql_hash(e)))
            }
            "forward_parallel" => {
                let e = obj
                    .get("type")
                    .or(obj.get("edge_type"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("related");
                Ok(Step::ForwardParallel(sekejapql_hash(e)))
            }
            "backward_parallel" => {
                let e = obj
                    .get("type")
                    .or(obj.get("edge_type"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("related");
                Ok(Step::BackwardParallel(sekejapql_hash(e)))
            }
            "hops" => Ok(Step::Hops(
                obj.get("n").and_then(|v| v.as_u64()).unwrap_or(1) as u32,
            )),
            "leaves" => Ok(Step::Leaves),
            "roots" => Ok(Step::Roots),
            "near" => Ok(Step::Near(
                obj.get("lat").and_then(|v| v.as_f64()).unwrap_or(0.0) as f32,
                obj.get("lon").and_then(|v| v.as_f64()).unwrap_or(0.0) as f32,
                obj.get("radius")
                    .or(obj.get("radius_km"))
                    .and_then(|v| v.as_f64())
                    .unwrap_or(10.0) as f32,
            )),
            "spatial_within_bbox" => Ok(Step::SpatialWithinBbox(
                obj.get("min_lat").and_then(|v| v.as_f64()).unwrap_or(-90.0) as f32,
                obj.get("min_lon")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(-180.0) as f32,
                obj.get("max_lat").and_then(|v| v.as_f64()).unwrap_or(90.0) as f32,
                obj.get("max_lon").and_then(|v| v.as_f64()).unwrap_or(180.0) as f32,
            )),
            "spatial_intersects_bbox" => Ok(Step::SpatialIntersectsBbox(
                obj.get("min_lat").and_then(|v| v.as_f64()).unwrap_or(-90.0) as f32,
                obj.get("min_lon")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(-180.0) as f32,
                obj.get("max_lat").and_then(|v| v.as_f64()).unwrap_or(90.0) as f32,
                obj.get("max_lon").and_then(|v| v.as_f64()).unwrap_or(180.0) as f32,
            )),
            "spatial_within_polygon" => {
                let polygon = obj
                    .get("polygon")
                    .and_then(|v| v.as_array())
                    .ok_or("spatial_within_polygon: missing polygon")?
                    .iter()
                    .map(|pt| {
                        let arr = pt
                            .as_array()
                            .ok_or("polygon point must be array [lat, lon]")?;
                        let lat = arr
                            .first()
                            .and_then(|v| v.as_f64())
                            .ok_or("missing point lat")? as f32;
                        let lon = arr
                            .get(1)
                            .and_then(|v| v.as_f64())
                            .ok_or("missing point lon")? as f32;
                        Ok::<[f32; 2], Box<dyn Error>>([lat, lon])
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Step::SpatialWithinPolygon(polygon))
            }
            "similar" => Ok(Step::Similar(
                obj.get("query")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect()
                    })
                    .unwrap_or_default(),
                obj.get("k")
                    .and_then(|v| v.as_u64())
                    .map(|n| n as usize)
                    .unwrap_or(10),
            )),
            "matching" => {
                let text = obj
                    .get("text")
                    .and_then(|v| v.as_str())
                    .ok_or("matching: missing text")?
                    .to_string();
                #[cfg(feature = "fulltext")]
                {
                    Ok(Step::Matching {
                        text,
                        limit: obj
                            .get("limit")
                            .and_then(|v| v.as_u64())
                            .map(|n| n as usize)
                            .unwrap_or(1000),
                        title_weight: obj
                            .get("title_weight")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(1.0) as f32,
                        content_weight: obj
                            .get("content_weight")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(1.0) as f32,
                    })
                }
                #[cfg(not(feature = "fulltext"))]
                {
                    let _ = text;
                    Err("matching requires fulltext feature".into())
                }
            }
            "where_eq" => Ok(Step::WhereEq(
                obj.get("field")
                    .or(obj.get("key"))
                    .and_then(|v| v.as_str())
                    .ok_or("where_eq: missing field")?
                    .to_string(),
                obj.get("value").cloned().ok_or("where_eq: missing value")?,
            )),
            "where_gt" => Ok(Step::WhereGt(
                obj.get("field")
                    .or(obj.get("key"))
                    .and_then(|v| v.as_str())
                    .ok_or("where_gt: missing field")?
                    .to_string(),
                obj.get("value")
                    .or(obj.get("threshold"))
                    .and_then(|v| v.as_f64())
                    .ok_or("where_gt: missing value")?,
            )),
            "where_between" => Ok(Step::WhereBetween(
                obj.get("field")
                    .or(obj.get("key"))
                    .and_then(|v| v.as_str())
                    .ok_or("where_between: missing field")?
                    .to_string(),
                obj.get("lo")
                    .or(obj.get("min"))
                    .or(obj.get("from"))
                    .and_then(|v| v.as_f64())
                    .ok_or("where_between: missing lo")?,
                obj.get("hi")
                    .or(obj.get("max"))
                    .or(obj.get("to"))
                    .and_then(|v| v.as_f64())
                    .ok_or("where_between: missing hi")?,
            )),
            "where_in" => Ok(Step::WhereIn(
                obj.get("field")
                    .or(obj.get("key"))
                    .and_then(|v| v.as_str())
                    .ok_or("where_in: missing field")?
                    .to_string(),
                obj.get("values")
                    .or(obj.get("in"))
                    .and_then(|v| v.as_array())
                    .ok_or("where_in: missing values")?
                    .to_vec(),
            )),
            "intersect" => {
                let other = obj
                    .get("other")
                    .or(obj.get("set"))
                    .and_then(|v| v.as_array())
                    .ok_or("intersect: missing other")?;
                let other_steps = self.parse_pipeline(&serde_json::json!({"pipeline": other}))?;
                Ok(Step::Intersect(other_steps))
            }
            "union" => {
                let other = obj
                    .get("other")
                    .or(obj.get("set"))
                    .and_then(|v| v.as_array())
                    .ok_or("union: missing other")?;
                let other_steps = self.parse_pipeline(&serde_json::json!({"pipeline": other}))?;
                Ok(Step::Union(other_steps))
            }
            "subtract" | "difference" => {
                let other = obj
                    .get("other")
                    .or(obj.get("set"))
                    .and_then(|v| v.as_array())
                    .ok_or("subtract: missing other")?;
                let other_steps = self.parse_pipeline(&serde_json::json!({"pipeline": other}))?;
                Ok(Step::Subtract(other_steps))
            }
            "take" | "limit" => Ok(Step::Take(
                obj.get("n")
                    .or(obj.get("limit"))
                    .and_then(|v| v.as_u64())
                    .map(|n| n as usize)
                    .unwrap_or(100),
            )),
            "where_lt" => Ok(Step::WhereLt(
                obj.get("field")
                    .or(obj.get("key"))
                    .and_then(|v| v.as_str())
                    .ok_or("where_lt: missing field")?
                    .to_string(),
                obj.get("value")
                    .or(obj.get("threshold"))
                    .and_then(|v| v.as_f64())
                    .ok_or("where_lt: missing value")?,
            )),
            "where_lte" => Ok(Step::WhereLte(
                obj.get("field")
                    .or(obj.get("key"))
                    .and_then(|v| v.as_str())
                    .ok_or("where_lte: missing field")?
                    .to_string(),
                obj.get("value")
                    .or(obj.get("threshold"))
                    .and_then(|v| v.as_f64())
                    .ok_or("where_lte: missing value")?,
            )),
            "where_gte" => Ok(Step::WhereGte(
                obj.get("field")
                    .or(obj.get("key"))
                    .and_then(|v| v.as_str())
                    .ok_or("where_gte: missing field")?
                    .to_string(),
                obj.get("value")
                    .or(obj.get("threshold"))
                    .and_then(|v| v.as_f64())
                    .ok_or("where_gte: missing value")?,
            )),
            "sort" => Ok(Step::Sort(
                obj.get("field")
                    .and_then(|v| v.as_str())
                    .ok_or("sort: missing field")?
                    .to_string(),
                obj.get("asc").and_then(|v| v.as_bool()).unwrap_or(true),
            )),
            "skip" => Ok(Step::Skip(
                obj.get("n")
                    .and_then(|v| v.as_u64())
                    .map(|n| n as usize)
                    .unwrap_or(0),
            )),
            "select" => Ok(Step::Select(
                obj.get("fields")
                    .and_then(|v| v.as_array())
                    .ok_or("select: missing fields")?
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect(),
            )),
            "st_within" => {
                let polygon = parse_polygon_arg(obj, "st_within")?;
                Ok(Step::StWithin(polygon))
            }
            "st_contains" => {
                let polygon = parse_polygon_arg(obj, "st_contains")?;
                Ok(Step::StContains(polygon))
            }
            "st_intersects" => {
                let polygon = parse_polygon_arg(obj, "st_intersects")?;
                Ok(Step::StIntersects(polygon))
            }
            "st_dwithin" => Ok(Step::StDWithin(
                obj.get("lat").and_then(|v| v.as_f64()).unwrap_or(0.0) as f32,
                obj.get("lon").and_then(|v| v.as_f64()).unwrap_or(0.0) as f32,
                obj.get("distance_km")
                    .or(obj.get("distance"))
                    .and_then(|v| v.as_f64())
                    .unwrap_or(1.0) as f32,
            )),
            _ => Err(format!("Unknown op: {}", op).into()),
        }
    }
}

fn sekejapql_hash(s: &str) -> u64 {
    seahash::hash(s.as_bytes())
}

fn parse_polygon_arg(obj: &Value, op: &str) -> Result<Vec<[f32; 2]>, Box<dyn Error>> {
    obj.get("polygon")
        .and_then(|v| v.as_array())
        .ok_or_else(|| format!("{}: missing polygon", op))?
        .iter()
        .map(|pt| {
            let arr = pt
                .as_array()
                .ok_or_else(|| format!("{}: polygon point must be array [lat, lon]", op))?;
            let lat = arr
                .first()
                .and_then(|v| v.as_f64())
                .ok_or_else(|| format!("{}: missing point lat", op))? as f32;
            let lon = arr
                .get(1)
                .and_then(|v| v.as_f64())
                .ok_or_else(|| format!("{}: missing point lon", op))? as f32;
            Ok::<[f32; 2], Box<dyn Error>>([lat, lon])
        })
        .collect()
}

// ============================================================================
// SekejapQL text format parser
// ============================================================================
// One op per line, or pipe-separated.  Quoted strings, bare numbers, booleans.
// Example:
//   collection "crimes"
//   where_eq "type" "robbery"
//   near 3.1291 101.6710 1.0
//   sort "severity" desc
//   take 20

impl QueryCompiler {
    /// Parse a SekejapQL text query (one-op-per-line or pipe-separated).
    pub fn parse_text_pipeline(&self, text: &str) -> Result<Vec<Step>, Box<dyn Error>> {
        // Normalise: replace pipes with newlines, strip comment lines
        let normalised: String = text
            .lines()
            .map(|line| {
                // Remove inline comments (# ...)
                let line = if let Some(pos) = line.find('#') {
                    &line[..pos]
                } else {
                    line
                };
                line.trim().to_string()
            })
            .filter(|line| !line.is_empty())
            .flat_map(|line| {
                // Split by pipe, treating each segment as a separate line
                line.split('|')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .join("\n");

        let mut steps = Vec::new();
        let mut pending_polygon: Option<(String, Vec<[f32; 2]>)> = None;

        for line in normalised.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Handle polygon continuation lines: (lat,lon)
            if line.starts_with('(') {
                if let Some((_, ref mut ring)) = pending_polygon {
                    for pair in parse_coord_pairs(line) {
                        ring.push(pair);
                    }
                    continue;
                }
            }

            // Flush any pending polygon on a non-coord line
            if let Some((op, ring)) = pending_polygon.take() {
                steps.push(build_polygon_step(&op, ring)?);
            }

            let tokens = tokenise(line);
            if tokens.is_empty() {
                continue;
            }

            let op = tokens[0].to_lowercase();
            let args = &tokens[1..];

            let step = match op.as_str() {
                "one" => Step::One(sekejapql_hash(require_str(args, 0, "one")?)),
                "many" => {
                    let hashes: Vec<u64> = args
                        .iter()
                        .map(|s| sekejapql_hash(s.trim_matches('"')))
                        .collect();
                    if hashes.is_empty() {
                        return Err("many: at least one slug required".into());
                    }
                    Step::Many(hashes)
                }
                "all" => Step::All,
                "collection" => Step::Collection(sekejapql_hash(require_str(args, 0, "collection")?)),
                "forward" => Step::Forward(sekejapql_hash(require_str(args, 0, "forward")?)),
                "backward" => Step::Backward(sekejapql_hash(require_str(args, 0, "backward")?)),
                "forward_parallel" => {
                    Step::ForwardParallel(sekejapql_hash(require_str(args, 0, "forward_parallel")?))
                }
                "backward_parallel" => Step::BackwardParallel(sekejapql_hash(require_str(
                    args,
                    0,
                    "backward_parallel",
                )?)),
                "hops" => Step::Hops(require_u32(args, 0, "hops")?),
                "leaves" => Step::Leaves,
                "roots" => Step::Roots,
                "near" => Step::Near(
                    require_f32(args, 0, "near lat")?,
                    require_f32(args, 1, "near lon")?,
                    require_f32(args, 2, "near radius_km")?,
                ),
                "spatial_within_bbox" => Step::SpatialWithinBbox(
                    require_f32(args, 0, "spatial_within_bbox min_lat")?,
                    require_f32(args, 1, "spatial_within_bbox min_lon")?,
                    require_f32(args, 2, "spatial_within_bbox max_lat")?,
                    require_f32(args, 3, "spatial_within_bbox max_lon")?,
                ),
                "spatial_intersects_bbox" => Step::SpatialIntersectsBbox(
                    require_f32(args, 0, "spatial_intersects_bbox min_lat")?,
                    require_f32(args, 1, "spatial_intersects_bbox min_lon")?,
                    require_f32(args, 2, "spatial_intersects_bbox max_lat")?,
                    require_f32(args, 3, "spatial_intersects_bbox max_lon")?,
                ),
                "spatial_within_polygon" => {
                    // May have inline coords on the same line OR span multiple lines
                    let ring = parse_coord_pairs(
                        &args.join(" "),
                    );
                    if ring.is_empty() {
                        // Multi-line polygon — accumulate
                        pending_polygon = Some((op, Vec::new()));
                        continue;
                    }
                    Step::SpatialWithinPolygon(ring)
                }
                "st_within" => {
                    let ring = parse_coord_pairs(&args.join(" "));
                    if ring.is_empty() {
                        pending_polygon = Some((op, Vec::new()));
                        continue;
                    }
                    Step::StWithin(ring)
                }
                "st_contains" => {
                    let ring = parse_coord_pairs(&args.join(" "));
                    if ring.is_empty() {
                        pending_polygon = Some((op, Vec::new()));
                        continue;
                    }
                    Step::StContains(ring)
                }
                "st_intersects" => {
                    let ring = parse_coord_pairs(&args.join(" "));
                    if ring.is_empty() {
                        pending_polygon = Some((op, Vec::new()));
                        continue;
                    }
                    Step::StIntersects(ring)
                }
                "st_dwithin" => Step::StDWithin(
                    require_f32(args, 0, "st_dwithin lat")?,
                    require_f32(args, 1, "st_dwithin lon")?,
                    require_f32(args, 2, "st_dwithin distance_km")?,
                ),
                "similar" => {
                    let slug = require_str(args, 0, "similar slug")?;
                    let k = args
                        .get(1)
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(10);
                    // Store slug hash as special marker — resolved at execute time
                    Step::Similar(vec![sekejapql_hash(slug) as f32], k)
                }
                "matching" => {
                    let text = require_str(args, 0, "matching")?;
                    let mut limit = 1000usize;
                    let mut title_weight = 1.0f32;
                    let mut content_weight = 1.0f32;
                    for arg in args.iter().skip(1) {
                        if let Some(rest) = arg.strip_prefix("limit:") {
                            limit = rest.parse().unwrap_or(1000);
                        } else if let Some(rest) = arg.strip_prefix("title_weight:") {
                            title_weight = rest.parse().unwrap_or(1.0);
                        } else if let Some(rest) = arg.strip_prefix("content_weight:") {
                            content_weight = rest.parse().unwrap_or(1.0);
                        }
                    }
                    #[cfg(feature = "fulltext")]
                    {
                        Step::Matching {
                            text: text.to_string(),
                            limit,
                            title_weight,
                            content_weight,
                        }
                    }
                    #[cfg(not(feature = "fulltext"))]
                    {
                        let _ = (text, limit, title_weight, content_weight);
                        return Err("matching requires fulltext feature".into());
                    }
                }
                "where_eq" => {
                    let field = require_str(args, 0, "where_eq field")?.to_string();
                    let val = parse_value_arg(args, 1, "where_eq value")?;
                    Step::WhereEq(field, val)
                }
                "where_gt" => Step::WhereGt(
                    require_str(args, 0, "where_gt field")?.to_string(),
                    require_f64(args, 1, "where_gt value")?,
                ),
                "where_lt" => Step::WhereLt(
                    require_str(args, 0, "where_lt field")?.to_string(),
                    require_f64(args, 1, "where_lt value")?,
                ),
                "where_gte" => Step::WhereGte(
                    require_str(args, 0, "where_gte field")?.to_string(),
                    require_f64(args, 1, "where_gte value")?,
                ),
                "where_lte" => Step::WhereLte(
                    require_str(args, 0, "where_lte field")?.to_string(),
                    require_f64(args, 1, "where_lte value")?,
                ),
                "where_between" => Step::WhereBetween(
                    require_str(args, 0, "where_between field")?.to_string(),
                    require_f64(args, 1, "where_between lo")?,
                    require_f64(args, 2, "where_between hi")?,
                ),
                "where_in" => {
                    let field = require_str(args, 0, "where_in field")?.to_string();
                    let values: Vec<Value> = args
                        .iter()
                        .skip(1)
                        .map(|s| parse_value_token(s))
                        .collect();
                    Step::WhereIn(field, values)
                }
                "sort" => {
                    let field = require_str(args, 0, "sort field")?.to_string();
                    let asc = args
                        .get(1)
                        .map(|s| s.to_lowercase() != "desc")
                        .unwrap_or(true);
                    Step::Sort(field, asc)
                }
                "skip" => Step::Skip(require_u32(args, 0, "skip")? as usize),
                "take" | "limit" => Step::Take(require_u32(args, 0, "take")? as usize),
                "select" => {
                    let fields: Vec<String> =
                        args.iter().map(|s| s.trim_matches('"').to_string()).collect();
                    if fields.is_empty() {
                        return Err("select: at least one field required".into());
                    }
                    Step::Select(fields)
                }
                unknown => return Err(format!("Unknown SekejapQL op: '{}'", unknown).into()),
            };
            steps.push(step);
        }

        // Flush trailing polygon
        if let Some((op, ring)) = pending_polygon.take() {
            steps.push(build_polygon_step(&op, ring)?);
        }

        if steps.is_empty() {
            return Err("Empty pipeline".into());
        }
        Ok(steps)
    }
}

// ── Tokeniser ─────────────────────────────────────────────────────────────────
// Splits a line into tokens, respecting double-quoted strings.
fn tokenise(line: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for ch in line.chars() {
        match ch {
            '"' => {
                in_quotes = !in_quotes;
                // Don't include the quote char — callers get clean strings
            }
            ' ' | '\t' if !in_quotes => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

// ── Coordinate pair parser ────────────────────────────────────────────────────
// Parses `(lat,lon)` pairs from a string like `(3.128,101.665) (3.135,101.678)`
fn parse_coord_pairs(s: &str) -> Vec<[f32; 2]> {
    let mut pairs = Vec::new();
    let mut rest = s.trim();
    while let Some(start) = rest.find('(') {
        rest = &rest[start + 1..];
        if let Some(end) = rest.find(')') {
            let inner = &rest[..end];
            let parts: Vec<&str> = inner.split(',').collect();
            if parts.len() >= 2 {
                if let (Ok(lat), Ok(lon)) =
                    (parts[0].trim().parse::<f32>(), parts[1].trim().parse::<f32>())
                {
                    pairs.push([lat, lon]);
                }
            }
            rest = &rest[end + 1..];
        } else {
            break;
        }
    }
    pairs
}

fn build_polygon_step(op: &str, ring: Vec<[f32; 2]>) -> Result<Step, Box<dyn Error>> {
    match op {
        "spatial_within_polygon" => Ok(Step::SpatialWithinPolygon(ring)),
        "st_within" => Ok(Step::StWithin(ring)),
        "st_contains" => Ok(Step::StContains(ring)),
        "st_intersects" => Ok(Step::StIntersects(ring)),
        _ => Err(format!("Unknown polygon op: {}", op).into()),
    }
}

// ── Argument helpers ──────────────────────────────────────────────────────────

fn require_str<'a>(args: &'a [String], idx: usize, ctx: &str) -> Result<&'a str, Box<dyn Error>> {
    args.get(idx)
        .map(|s| s.trim_matches('"'))
        .ok_or_else(|| format!("{}: missing argument", ctx).into())
}

fn require_f32(args: &[String], idx: usize, ctx: &str) -> Result<f32, Box<dyn Error>> {
    args.get(idx)
        .and_then(|s| s.parse::<f32>().ok())
        .ok_or_else(|| format!("{}: expected number", ctx).into())
}

fn require_f64(args: &[String], idx: usize, ctx: &str) -> Result<f64, Box<dyn Error>> {
    args.get(idx)
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| format!("{}: expected number", ctx).into())
}

fn require_u32(args: &[String], idx: usize, ctx: &str) -> Result<u32, Box<dyn Error>> {
    args.get(idx)
        .and_then(|s| s.parse::<u32>().ok())
        .ok_or_else(|| format!("{}: expected integer", ctx).into())
}

fn parse_value_arg(args: &[String], idx: usize, ctx: &str) -> Result<Value, Box<dyn Error>> {
    let s = args
        .get(idx)
        .ok_or_else(|| format!("{}: missing argument", ctx))?;
    Ok(parse_value_token(s))
}

fn parse_value_token(s: &str) -> Value {
    if s == "true" {
        return Value::Bool(true);
    }
    if s == "false" {
        return Value::Bool(false);
    }
    if s == "null" {
        return Value::Null;
    }
    if let Ok(n) = s.parse::<i64>() {
        return Value::Number(n.into());
    }
    if let Ok(f) = s.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Value::Number(n);
        }
    }
    Value::String(s.trim_matches('"').to_string())
}

#[deprecated(note = "Use QueryCompiler")]
pub type SekejapQL = QueryCompiler;

impl crate::types::Outcome<Vec<crate::types::Hit>> {
    pub fn to_json_response(&self) -> Value {
        serde_json::json!({
            "data": self.data.iter().map(|h| serde_json::json!({
                "idx": h.idx,
                "slug_hash": h.slug_hash,
                "collection_hash": h.collection_hash,
                "payload": h.payload,
                "lat": h.lat,
                "lon": h.lon,
                "score": h.score
            })).collect::<Vec<_>>(),
            "trace": self.trace.to_json()
        })
    }
}
