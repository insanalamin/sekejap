//! Python bindings for Sekejap-DB v0.2.0 using PyO3
//!
//! A graph-first, embedded multi-model database engine.

use ::sekejap::SekejapDB;
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use std::path::Path;
use std::sync::{Arc, RwLock};

// ============================================================
// Python result types
// ============================================================

/// A resolved node result
#[pyclass(name = "Hit")]
#[derive(Debug, Clone)]
struct PyHit {
    idx: u32,
    slug_hash: u64,
    collection_hash: u64,
    payload: Option<String>,
    lat: f32,
    lon: f32,
}

#[pymethods]
impl PyHit {
    #[getter]
    fn idx(&self) -> u32 { self.idx }
    #[getter]
    fn slug_hash(&self) -> u64 { self.slug_hash }
    #[getter]
    fn collection_hash(&self) -> u64 { self.collection_hash }
    #[getter]
    fn payload(&self) -> Option<String> { self.payload.clone() }
    #[getter]
    fn lat(&self) -> f32 { self.lat }
    #[getter]
    fn lon(&self) -> f32 { self.lon }

    fn __repr__(&self) -> String {
        let preview = self.payload.as_deref().map(|s| &s[..s.len().min(50)]).unwrap_or("None");
        format!("Hit(idx={}, payload={:?})", self.idx, preview)
    }
}

// ============================================================
// Python wrapper for SekejapDB v0.2.0
// ============================================================

#[pyclass(name = "SekejapDB")]
struct PySekejapDB {
    db: Option<Arc<RwLock<SekejapDB>>>,
    path: String,
}

#[pymethods]
impl PySekejapDB {
    #[new]
    #[pyo3(signature = (path, capacity=1_000_000))]
    fn new(path: &str, capacity: usize) -> PyResult<Self> {
        let db_path = Path::new(path);
        std::fs::create_dir_all(db_path)
            .map_err(|e| PyIOError::new_err(format!("Failed to create db dir: {}", e)))?;
        match SekejapDB::new(db_path, capacity) {
            Ok(db) => Ok(Self {
                db: Some(Arc::new(RwLock::new(db))),
                path: path.to_string(),
            }),
            Err(e) => Err(PyIOError::new_err(format!("Failed to open database: {}", e))),
        }
    }

    // ---- Node operations ----

    fn put(&self, slug: &str, json: &str) -> PyResult<u32> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        db.nodes().put(slug, json).map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn get(&self, slug: &str) -> Option<String> {
        let db = self.db.as_ref()?;
        let db = db.read().ok()?;
        db.nodes().get(slug)
    }

    fn remove(&self, slug: &str) -> PyResult<()> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        db.nodes().remove(slug).map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn ingest_nodes(&self, items: Vec<(String, String)>) -> PyResult<Vec<u32>> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        let refs: Vec<(&str, &str)> = items.iter().map(|(s, j)| (s.as_str(), j.as_str())).collect();
        db.nodes().ingest(&refs).map_err(|e| PyIOError::new_err(e.to_string()))
    }

    // ---- Edge operations ----

    fn link(&self, source: &str, target: &str, edge_type: &str, weight: f32) -> PyResult<()> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        db.edges().link(source, target, edge_type, weight).map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn unlink(&self, source: &str, target: &str, edge_type: &str) -> PyResult<()> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        db.edges().unlink(source, target, edge_type).map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn link_meta(&self, source: &str, target: &str, edge_type: &str, weight: f32, meta: &str) -> PyResult<()> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        db.edges().link_meta(source, target, edge_type, weight, meta).map_err(|e| PyIOError::new_err(e.to_string()))
    }

    // ---- Schema operations ----

    fn define_collection(&self, name: &str, json: &str) -> PyResult<()> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        db.schema().define(name, json).map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn count_collection(&self, collection: &str) -> usize {
        let db = match self.db.as_ref() { Some(d) => d, None => return 0 };
        let db = match db.read() { Ok(d) => d, Err(_) => return 0 };
        db.schema().count(collection)
    }

    // ---- Query: SekejapQL (JSON pipeline) ----

    /// Execute a SekejapQL JSON pipeline, returns JSON string of Hit array
    fn query_json(&self, json: &str) -> PyResult<String> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        
        let proper_json = convert_shorthand_query(json);
        
        let result = db.query_json(&proper_json).map_err(|e| PyIOError::new_err(e.to_string()))?;
        let hits: Vec<serde_json::Value> = result.data.into_iter().map(|h| {
            serde_json::json!({
                "idx": h.idx,
                "slug_hash": h.slug_hash,
                "collection_hash": h.collection_hash,
                "payload": h.payload,
                "lat": h.lat,
                "lon": h.lon
            })
        }).collect();
        serde_json::to_string(&hits).map_err(|e| PyIOError::new_err(e.to_string()))
    }

    /// Execute a SekejapQL JSON pipeline, returns count only
    fn query_json_count(&self, json: &str) -> PyResult<usize> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;

        let proper_json = convert_shorthand_query(json);

        let result = db.query_json_count(&proper_json).map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(result.data)
    }

    /// Execute a JSON mutation (put, link, remove, etc.), returns JSON response
    fn mutate_json(&self, json: &str) -> PyResult<String> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        let result = db.mutate_json(json).map_err(|e| PyIOError::new_err(e.to_string()))?;
        serde_json::to_string(&result).map_err(|e| PyIOError::new_err(e.to_string()))
    }

    // ---- Query: Rust Set pipeline (typed) ----

    fn one(&self, slug: &str) -> PyResult<Option<PyHit>> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        let outcome = db.nodes().one(slug).first()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(outcome.data.map(|h| PyHit {
            idx: h.idx, slug_hash: h.slug_hash, collection_hash: h.collection_hash,
            payload: h.payload, lat: h.lat, lon: h.lon,
        }))
    }

    fn collection(&self, name: &str) -> PyResult<Vec<PyHit>> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        let outcome = db.nodes().collection(name).collect()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(outcome.data.into_iter().map(|h| PyHit {
            idx: h.idx, slug_hash: h.slug_hash, collection_hash: h.collection_hash,
            payload: h.payload, lat: h.lat, lon: h.lon,
        }).collect())
    }

    #[pyo3(signature = (slug, edge_type, max_hops=1))]
    fn forward(&self, slug: &str, edge_type: &str, max_hops: u32) -> PyResult<Vec<PyHit>> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        let outcome = db.nodes().one(slug)
            .forward(edge_type)
            .hops(max_hops)
            .collect()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(outcome.data.into_iter().map(|h| PyHit {
            idx: h.idx, slug_hash: h.slug_hash, collection_hash: h.collection_hash,
            payload: h.payload, lat: h.lat, lon: h.lon,
        }).collect())
    }

    #[pyo3(signature = (slug, edge_type, max_hops=1))]
    fn backward(&self, slug: &str, edge_type: &str, max_hops: u32) -> PyResult<Vec<PyHit>> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        let outcome = db.nodes().one(slug)
            .backward(edge_type)
            .hops(max_hops)
            .collect()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(outcome.data.into_iter().map(|h| PyHit {
            idx: h.idx, slug_hash: h.slug_hash, collection_hash: h.collection_hash,
            payload: h.payload, lat: h.lat, lon: h.lon,
        }).collect())
    }

    fn near(&self, lat: f32, lon: f32, radius_km: f32) -> PyResult<Vec<PyHit>> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        let outcome = db.nodes().all()
            .near(lat, lon, radius_km)
            .collect()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(outcome.data.into_iter().map(|h| PyHit {
            idx: h.idx, slug_hash: h.slug_hash, collection_hash: h.collection_hash,
            payload: h.payload, lat: h.lat, lon: h.lon,
        }).collect())
    }

    #[pyo3(signature = (m=16))]
    fn init_hnsw(&self, m: usize) -> PyResult<()> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        db.init_hnsw(m);
        Ok(())
    }

    fn build_hnsw(&self) -> PyResult<()> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        db.nodes().build_hnsw().map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn similar(&self, query: Vec<f32>, k: usize) -> PyResult<Vec<PyHit>> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        let outcome = db.nodes().all()
            .similar(&query, k)
            .collect()
            .map_err(|e| PyIOError::new_err(e.to_string()))?;
        Ok(outcome.data.into_iter().map(|h| PyHit {
            idx: h.idx, slug_hash: h.slug_hash, collection_hash: h.collection_hash,
            payload: h.payload, lat: h.lat, lon: h.lon,
        }).collect())
    }

    // ---- Persistence ----

    fn backup(&self, path: &str) -> PyResult<()> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        db.backup(Path::new(path)).map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn restore(&self, path: &str) -> PyResult<()> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        db.restore(Path::new(path)).map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn flush(&self) -> PyResult<()> {
        let db = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db.read().map_err(|e| PyIOError::new_err(e.to_string()))?;
        db.flush().map_err(|e| PyIOError::new_err(e.to_string()))
    }

    // ---- Lifecycle ----

    fn close(&mut self) { self.db.take(); }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> { slf }

    fn __exit__(&mut self, _exc_type: PyObject, _exc_value: PyObject, _traceback: PyObject) {
        self.close();
    }

    fn __repr__(&self) -> String {
        format!("SekejapDB(path={})", self.path)
    }
}

// ============================================================
// Helper functions
// ============================================================

/// Convert shorthand query format to proper SekejapQL format
fn convert_shorthand_query(json: &str) -> String {
    let val: serde_json::Value = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(_) => return json.to_string(),
    };
    
    // If already has "pipeline", return as-is
    if let Some(obj) = val.as_object() {
        if obj.contains_key("pipeline") {
            return json.to_string();
        }
    }
    
    // If it's an array, convert each step
    if let Some(arr) = val.as_array() {
        let steps: Vec<serde_json::Value> = arr.iter().map(convert_step).collect();
        return serde_json::to_string(&serde_json::json!({"pipeline": steps})).unwrap_or_else(|_| json.to_string());
    }
    
    json.to_string()
}

/// Convert a single step from shorthand to proper format
fn convert_step(step: &serde_json::Value) -> serde_json::Value {
    if let Some(obj) = step.as_object() {
        // Already has "op" field - keep as-is
        if obj.contains_key("op") {
            return step.clone();
        }
        
        let mut converted = serde_json::Map::new();
        
        for (key, value) in obj.iter() {
            match key.as_str() {
                "collection" => {
                    converted.insert("op".to_string(), serde_json::json!("collection"));
                    converted.insert("name".to_string(), value.clone());
                }
                "one" => {
                    converted.insert("op".to_string(), serde_json::json!("one"));
                    converted.insert("slug".to_string(), value.clone());
                }
                "many" => {
                    converted.insert("op".to_string(), serde_json::json!("many"));
                    converted.insert("slugs".to_string(), value.clone());
                }
                "forward" => {
                    converted.insert("op".to_string(), serde_json::json!("forward"));
                    converted.insert("type".to_string(), value.clone());
                }
                "backward" => {
                    converted.insert("op".to_string(), serde_json::json!("backward"));
                    converted.insert("type".to_string(), value.clone());
                }
                "hops" => {
                    converted.insert("op".to_string(), serde_json::json!("hops"));
                    converted.insert("n".to_string(), value.clone());
                }
                "all" | "leaves" | "roots" => {
                    converted.insert("op".to_string(), serde_json::json!(key));
                }
                _ => {
                    converted.insert(key.clone(), value.clone());
                }
            }
        }
        
        serde_json::Value::Object(converted)
    } else {
        step.clone()
    }
}

// ============================================================
// Utility functions (exposed to Python)
// ============================================================

#[pyfunction]
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_KM: f64 = 6371.0;
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();
    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    EARTH_RADIUS_KM * c
}

#[pyfunction]
fn cosine_similarity(v1: Vec<f32>, v2: Vec<f32>) -> f32 {
    if v1.len() != v2.len() || v1.is_empty() { return 0.0; }
    let dot: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
    let n1: f32 = v1.iter().map(|x| x * x).sum::<f32>().sqrt();
    let n2: f32 = v2.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n1 == 0.0 || n2 == 0.0 { return 0.0; }
    dot / (n1 * n2)
}

// ============================================================
// Module
// ============================================================

#[pymodule]
fn sekejap(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySekejapDB>()?;
    m.add_class::<PyHit>()?;
    m.add_function(wrap_pyfunction!(haversine_distance, m)?)?;
    m.add_function(wrap_pyfunction!(cosine_similarity, m)?)?;
    Ok(())
}