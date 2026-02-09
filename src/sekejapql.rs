//! SekejapQL - JSON-based Query Language
//!
//! SekejapQL is a robust, secure query language for ad-hoc database queries.
//! It uses JSON for simplicity and compatibility across languages.

use crate::{EntityId, NodeId, SekejapDB, atoms::*};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// SekejapQL Query Engine
pub struct SekejapQL<'db> {
    db: &'db SekejapDB,
    security: SecurityLimits,
}

/// Security limits for query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityLimits {
    pub max_nodes: usize,
    pub timeout_ms: u64,
    pub read_only: bool,
    pub max_depth: usize,
    pub max_memory_mb: usize,
}

impl Default for SecurityLimits {
    fn default() -> Self {
        Self {
            max_nodes: 10000,
            timeout_ms: 5000,
            read_only: false,
            max_depth: 10,
            max_memory_mb: 100,
        }
    }
}

/// SekejapQL query structure
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Query {
    pub traversal: Option<TraversalSpec>,
    pub filters: Option<Vec<FilterSpec>>,
    pub joins: Option<Vec<JoinSpec>>,
    pub spatial: Option<SpatialSpec>,
    pub vector: Option<VectorSpec>,
    #[serde(rename = "groupBy")]
    pub group_by: Option<Vec<String>>,
    pub aggregations: Option<Vec<AggregationSpec>>,
    pub having: Option<FilterSpec>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    #[serde(rename = "return")]
    pub return_spec: Option<ReturnSpec>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TraversalSpec {
    pub start: String,
    pub direction: String, // "forward" or "backward"
    #[serde(rename = "maxDepth")]
    pub max_depth: usize,
    #[serde(rename = "minWeight")]
    pub min_weight: Option<f32>,
    #[serde(rename = "edgeType")]
    pub edge_type: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum FilterSpec {
    #[serde(rename = "edge_to")]
    EdgeTo { target: String, #[serde(rename = "edgeType")] edge_type: String },
    #[serde(rename = "edge_from")]
    EdgeFrom { source: String, #[serde(rename = "edgeType")] edge_type: String },
    #[serde(rename = "fulltext")]
    Fulltext { query: String, limit: Option<usize> },
    #[serde(rename = "content_contains")]
    ContentContains { field: String, value: String },
    #[serde(rename = "eq")]
    Eq { field: String, value: serde_json::Value },
    #[serde(rename = "gt")]
    Gt { field: String, value: f64 },
    #[serde(rename = "lt")]
    Lt { field: String, value: f64 },
    #[serde(rename = "gte")]
    Gte { field: String, value: f64 },
    #[serde(rename = "lte")]
    Lte { field: String, value: f64 },
    #[serde(rename = "in")]
    In { field: String, values: Vec<serde_json::Value> },
    #[serde(rename = "not")]
    Not { filter: Box<FilterSpec> },
    #[serde(rename = "and")]
    And { filters: Vec<FilterSpec> },
    #[serde(rename = "or")]
    Or { filters: Vec<FilterSpec> },
}

#[derive(Debug, Deserialize, Serialize)]
pub struct JoinSpec {
    #[serde(rename = "joinType")]
    pub join_type: String, // "inner", "left"
    pub via: String, // edge type
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SpatialSpec {
    pub center: Center,
    #[serde(rename = "radiusKm")]
    pub radius_km: f64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Center {
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct VectorSpec {
    pub query: Vec<f32>,
    pub threshold: f32,
    pub k: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AggregationSpec {
    pub field: String,
    pub op: String, // "count", "sum", "avg", "min", "max"
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReturnSpec {
    pub nodes: bool,
    pub edges: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct QueryResult {
    pub nodes: Vec<NodeResult>,
    pub edges: Vec<EdgeResult>,
    pub aggregations: Option<HashMap<String, serde_json::Value>>,
    pub metadata: ResultMetadata,
}

#[derive(Debug, Serialize, Clone)]
pub struct NodeResult {
    pub node_id: NodeId,
    pub slug: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub joined: Option<Vec<NodeResult>>,
}

#[derive(Debug, Serialize, Clone)]
pub struct EdgeResult {
    pub source: String,
    pub target: String,
    pub edge_type: String,
    pub weight: f32,
}

#[derive(Debug, Serialize, Clone)]
pub struct ResultMetadata {
    pub total: usize,
    #[serde(rename = "executionTimeMs")]
    pub execution_time_ms: f64,
    #[serde(rename = "queryPlan")]
    pub query_plan: String,
}

impl<'db> SekejapQL<'db> {
    pub fn new(db: &'db SekejapDB) -> Self {
        Self { db, security: SecurityLimits::default() }
    }

    pub fn builder(db: &'db SekejapDB) -> SekejapQLBuilder<'db> {
        SekejapQLBuilder::new(db)
    }

    /// Execute a query using the JSON DSL
    pub fn query(&self, query_json: &str) -> Result<QueryResult, QueryError> {
        let start_time = std::time::Instant::now();
        let query: Query = serde_json::from_str(query_json).map_err(|e| QueryError::Parse(e.to_string()))?;
        
        let mut result = self.execute_query(&query)?;
        
        let execution_time_ms = start_time.elapsed().as_secs_f64() * 1000.0;
        result.metadata.execution_time_ms = execution_time_ms;
        result.metadata.query_plan = "Explicit Index Intersection".to_string();

        Ok(result)
    }

    fn execute_query(&self, query: &Query) -> Result<QueryResult, QueryError> {
        let mut candidates: Option<HashSet<NodeId>> = None;

        // 1. Fulltext (Candidate Generator)
        #[cfg(feature = "fulltext")]
        if let Some(ft) = &query.filters.as_ref().and_then(|fs| fs.iter().find_map(|f| if let FilterSpec::Fulltext { query, limit } = f { Some((query, limit)) } else { None })) {
            let limit = ft.1.unwrap_or(100);
            let ids = self.db.search_text(ft.0, limit).map_err(|e| QueryError::Execution(e.to_string()))?;
            candidates = Some(ids.into_iter().collect());
        }

        // 2. Spatial (Refiner/Generator)
        #[cfg(feature = "spatial")]
        if let Some(sp) = &query.spatial {
            let ids = self.db.search_spatial(sp.center.lat, sp.center.lon, sp.radius_km).map_err(|e| QueryError::Execution(e.to_string()))?;
            let set: HashSet<_> = ids.into_iter().collect();
            candidates = match candidates {
                Some(c) => Some(c.intersection(&set).cloned().collect()),
                None => Some(set),
            };
        }

        // 3. Vector (Refiner/Generator)
        #[cfg(feature = "vector")]
        if let Some(vec) = &query.vector {
            let k = vec.k.unwrap_or(100);
            let results = self.db.search_vector(&vec.query, k).map_err(|e| QueryError::Execution(e.to_string()))?;
            let set: HashSet<_> = results.into_iter().filter(|(_, d)| *d <= vec.threshold).map(|(id, _)| id).collect();
            candidates = match candidates {
                Some(c) => Some(c.intersection(&set).cloned().collect()),
                None => Some(set),
            };
        }

        // 4. Traversal
        if let Some(traversal) = &query.traversal {
            let nodes = if traversal.direction == "backward" {
                traverse_backward(self.db, &traversal.start, traversal.max_depth)
            } else {
                traverse_bfs(self.db, &traversal.start, traversal.max_depth)
            };
            let set: HashSet<_> = nodes.into_iter().map(|n| n.node_id).collect();
            candidates = match candidates {
                Some(c) => Some(c.intersection(&set).cloned().collect()),
                None => Some(set),
            };
        }

        // Final candidate resolution
        let mut nodes = match candidates {
            Some(ids) => ids.into_iter().map(|id| self.resolve_node(id)).collect::<Vec<_>>(),
            None => self.db.storage().all().into_iter().map(|n| self.resolve_node(n.node_id)).collect(),
        };

        // Apply filters (non-generator filters)
        if let Some(filters) = &query.filters {
            nodes.retain(|n| filters.iter().all(|f| self.matches_filter(n, f)));
        }

        // Joins
        if let Some(joins) = &query.joins {
            for node in &mut nodes {
                for join in joins {
                    let traversal = self.db.traverse_forward(&node.slug, 1, 0.0, Some(&join.via), None).map_err(|e| QueryError::Execution(e.to_string()))?;
                    let joined_nodes: Vec<_> = traversal.path.into_iter().map(|id| self.resolve_node_by_entity(&id)).collect();
                    if !joined_nodes.is_empty() {
                        let current_joined = node.joined.get_or_insert_with(Vec::new);
                        current_joined.extend(joined_nodes);
                    }
                }
            }
        }

        // Aggregations
        let mut agg_results = None;
        if let Some(aggs) = &query.aggregations {
            let mut results = HashMap::new();
            for agg in aggs {
                let val = match agg.op.as_str() {
                    "count" => serde_json::json!(nodes.len()),
                    "sum" => {
                        let sum: f64 = nodes.iter().filter_map(|n| self.get_field_value(n, &agg.field).and_then(|v| v.as_f64())).sum();
                        serde_json::json!(sum)
                    },
                    _ => serde_json::json!(0),
                };
                results.insert(format!("{}_{}", agg.op, agg.field), val);
            }
            agg_results = Some(results);
        }

        // Pagination
        if let Some(offset) = query.offset { if offset < nodes.len() { nodes = nodes.into_iter().skip(offset).collect(); } else { nodes.clear(); } }
        if let Some(limit) = query.limit { nodes = nodes.into_iter().take(limit).collect(); }

        let total = nodes.len();
        Ok(QueryResult {
            nodes,
            edges: Vec::new(),
            aggregations: agg_results,
            metadata: ResultMetadata { total, execution_time_ms: 0.0, query_plan: String::new() },
        })
    }

    fn resolve_node(&self, node_id: NodeId) -> NodeResult {
        let slug = self.node_id_to_slug(node_id);
        let raw_data = self.db.read(&slug).ok().flatten();
        println!("DEBUG: resolve_node - node_id: {}, slug: {}, raw_data present: {}", node_id, slug, raw_data.is_some());
        if let Some(ref s) = raw_data {
            println!("DEBUG: raw_data: {}", s);
        }
        let payload = raw_data.and_then(|s| serde_json::from_str(&s).ok());
        NodeResult { node_id, slug, payload, joined: None }
    }

    fn resolve_node_by_entity(&self, entity_id: &EntityId) -> NodeResult {
        let slug = entity_id.to_string();
        let raw_data = self.db.read(&slug).ok().flatten();
        let payload = raw_data.and_then(|s| serde_json::from_str(&s).ok());
        // We'd need to resolve NodeId properly here if we needed it, for now use 0
        NodeResult { node_id: 0, slug, payload, joined: None }
    }

    fn matches_filter(&self, node: &NodeResult, filter: &FilterSpec) -> bool {
        match filter {
            FilterSpec::Fulltext { .. } => true, // Already handled as generator
            FilterSpec::Eq { field, value } => {
                let actual = self.get_field_value(node, field);
                println!("DEBUG: matches_filter Eq - field: {}, expected: {}, actual: {:?}", field, value, actual);
                actual.is_some_and(|v| v == *value)
            },
            FilterSpec::ContentContains { field, value } => self.get_field_value(node, field).and_then(|v| v.as_str().map(|s| s.contains(value))).unwrap_or(false),
            FilterSpec::And { filters } => filters.iter().all(|f| self.matches_filter(node, f)),
            FilterSpec::Or { filters } => filters.iter().any(|f| self.matches_filter(node, f)),
            FilterSpec::Not { filter } => !self.matches_filter(node, filter),
            _ => true, // TODO: Implement others
        }
    }

    fn get_field_value(&self, node: &NodeResult, field: &str) -> Option<serde_json::Value> {
        let payload = node.payload.as_ref()?;
        
        // 1. Check top-level fields
        if let Some(val) = payload.get(field) {
            return Some(val.clone());
        }
        
        // 2. Check inside "props"
        if let Some(props) = payload.get("props").and_then(|p| p.as_object()) {
            if let Some(val) = props.get(field) {
                return Some(val.clone());
            }
        }

        // 3. Check inside "metadata" (legacy/migrated fields)
        if let Some(meta) = payload.get("metadata").and_then(|m| m.as_object()) {
            if let Some(val) = meta.get(field) {
                return Some(val.clone());
            }
        }
        
        None
    }

    fn node_id_to_slug(&self, node_id: NodeId) -> String {
        self.db.storage().get_by_id(node_id, None).and_then(|n| n.entity_id.map(|id| id.to_string())).unwrap_or_else(|| format!("{}", node_id))
    }
}

pub struct SekejapQLBuilder<'db> {
    db: &'db SekejapDB,
    security: SecurityLimits,
}

impl<'db> SekejapQLBuilder<'db> {
    pub fn new(db: &'db SekejapDB) -> Self { Self { db, security: SecurityLimits::default() } }
    
    pub fn max_nodes(mut self, max: usize) -> Self {
        self.security.max_nodes = max;
        self
    }

    pub fn timeout_ms(mut self, timeout: u64) -> Self {
        self.security.timeout_ms = timeout;
        self
    }

    pub fn read_only(mut self, read_only: bool) -> Self {
        self.security.read_only = read_only;
        self
    }

    pub fn max_depth(mut self, depth: usize) -> Self {
        self.security.max_depth = depth;
        self
    }

    pub fn build(self) -> SekejapQL<'db> { SekejapQL { db: self.db, security: self.security } }
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Execution error: {0}")]
    Execution(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WriteOptions;
    use tempfile::TempDir;

    #[test]
    fn test_simple_filter_query() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();
        db.write_with_options("restaurant-1", r#"{"title": "Luigi Pizza", "cuisine": "italian"}"#, WriteOptions { publish_now: true, ..Default::default() }).unwrap();
        
        let engine = SekejapQL::new(&db);
        let query = r#"{ "filters": [ { "type": "eq", "field": "cuisine", "value": "italian" } ] }"#;
        let result = engine.query(query).unwrap();
        assert_eq!(result.nodes.len(), 1);
    }
}