//! Python bindings for Sekejap-DB using PyO3
//! API: db.nodes() / db.edges() / db.schema() — mirrors Rust exactly.

use ::sekejap::types::Step;
use ::sekejap::SekejapDB;
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use std::path::Path;
use std::sync::{Arc, RwLock};

// ── Hit result ────────────────────────────────────────────────────────────────

#[pyclass(name = "Hit")]
#[derive(Debug, Clone)]
struct PyHit {
    idx: u32,
    slug_hash: u64,
    collection_hash: u64,
    payload: Option<String>,
    lat: f32,
    lon: f32,
    score: Option<f32>,
}

#[pymethods]
impl PyHit {
    #[getter] fn idx(&self) -> u32 { self.idx }
    #[getter] fn slug_hash(&self) -> u64 { self.slug_hash }
    #[getter] fn collection_hash(&self) -> u64 { self.collection_hash }
    #[getter] fn payload(&self) -> Option<String> { self.payload.clone() }
    #[getter] fn lat(&self) -> f32 { self.lat }
    #[getter] fn lon(&self) -> f32 { self.lon }
    #[getter] fn score(&self) -> Option<f32> { self.score }
    fn __repr__(&self) -> String {
        let preview = self.payload.as_deref().map(|s| &s[..s.len().min(50)]).unwrap_or("None");
        format!("Hit(idx={}, score={:?}, payload={:?})", self.idx, self.score, preview)
    }
}

fn to_pyhit(h: ::sekejap::types::Hit) -> PyHit {
    PyHit { idx: h.idx, slug_hash: h.slug_hash, collection_hash: h.collection_hash,
            payload: h.payload, lat: h.lat, lon: h.lon, score: h.score }
}

fn db_err(e: impl std::fmt::Display) -> PyErr { PyIOError::new_err(e.to_string()) }

// ── PySet — chainable query builder ──────────────────────────────────────────

#[pyclass(name = "Set")]
struct PySet {
    db: Arc<RwLock<SekejapDB>>,
    steps: Vec<Step>,
}

impl PySet {
    fn new(db: Arc<RwLock<SekejapDB>>, step: Step) -> Self {
        Self { db, steps: vec![step] }
    }
}

macro_rules! push_step {
    ($slf:expr, $py:expr, $step:expr) => {{
        $slf.borrow_mut($py).steps.push($step);
        $slf
    }};
}

#[pymethods]
impl PySet {
    // ── Graph ────────────────────────────────────────────────────────────────
    fn forward(slf: Py<Self>, py: Python<'_>, edge_type: &str) -> Py<Self> {
        push_step!(slf, py, Step::Forward(sk_hash(edge_type)))
    }
    fn backward(slf: Py<Self>, py: Python<'_>, edge_type: &str) -> Py<Self> {
        push_step!(slf, py, Step::Backward(sk_hash(edge_type)))
    }
    fn hops(slf: Py<Self>, py: Python<'_>, n: u32) -> Py<Self> {
        push_step!(slf, py, Step::Hops(n))
    }
    fn roots(slf: Py<Self>, py: Python<'_>) -> Py<Self> {
        push_step!(slf, py, Step::Roots)
    }
    fn leaves(slf: Py<Self>, py: Python<'_>) -> Py<Self> {
        push_step!(slf, py, Step::Leaves)
    }

    // ── Spatial ───────────────────────────────────────────────────────────────
    fn near(slf: Py<Self>, py: Python<'_>, lat: f32, lon: f32, km: f32) -> Py<Self> {
        push_step!(slf, py, Step::Near(lat, lon, km))
    }
    fn within_bbox(slf: Py<Self>, py: Python<'_>, min_lat: f32, min_lon: f32, max_lat: f32, max_lon: f32) -> Py<Self> {
        push_step!(slf, py, Step::SpatialWithinBbox(min_lat, min_lon, max_lat, max_lon))
    }
    fn st_within(slf: Py<Self>, py: Python<'_>, ring: Vec<[f32; 2]>) -> Py<Self> {
        push_step!(slf, py, Step::StWithin(ring))
    }
    fn st_contains(slf: Py<Self>, py: Python<'_>, ring: Vec<[f32; 2]>) -> Py<Self> {
        push_step!(slf, py, Step::StContains(ring))
    }
    fn st_intersects(slf: Py<Self>, py: Python<'_>, ring: Vec<[f32; 2]>) -> Py<Self> {
        push_step!(slf, py, Step::StIntersects(ring))
    }
    fn st_dwithin(slf: Py<Self>, py: Python<'_>, lat: f32, lon: f32, km: f32) -> Py<Self> {
        push_step!(slf, py, Step::StDWithin(lat, lon, km))
    }

    // ── Vector ────────────────────────────────────────────────────────────────
    fn similar(slf: Py<Self>, py: Python<'_>, vec: Vec<f32>, k: usize) -> Py<Self> {
        push_step!(slf, py, Step::Similar(vec, k))
    }

    // ── Full-text ─────────────────────────────────────────────────────────────
    #[cfg(feature = "fulltext")]
    fn matching(slf: Py<Self>, py: Python<'_>, text: &str) -> Py<Self> {
        push_step!(slf, py, Step::Matching { text: text.to_string(), limit: 1000, title_weight: 1.0, content_weight: 1.0 })
    }

    // ── Filters ───────────────────────────────────────────────────────────────
    fn where_eq(slf: Py<Self>, py: Python<'_>, field: &str, value: &Bound<'_, PyAny>) -> PyResult<Py<Self>> {
        let v = pyany_to_value(value)?;
        Ok(push_step!(slf, py, Step::WhereEq(field.to_string(), v)))
    }
    fn where_gt(slf: Py<Self>, py: Python<'_>, field: &str, value: f64) -> Py<Self> {
        push_step!(slf, py, Step::WhereGt(field.to_string(), value))
    }
    fn where_lt(slf: Py<Self>, py: Python<'_>, field: &str, value: f64) -> Py<Self> {
        push_step!(slf, py, Step::WhereLt(field.to_string(), value))
    }
    fn where_gte(slf: Py<Self>, py: Python<'_>, field: &str, value: f64) -> Py<Self> {
        push_step!(slf, py, Step::WhereGte(field.to_string(), value))
    }
    fn where_lte(slf: Py<Self>, py: Python<'_>, field: &str, value: f64) -> Py<Self> {
        push_step!(slf, py, Step::WhereLte(field.to_string(), value))
    }
    fn where_between(slf: Py<Self>, py: Python<'_>, field: &str, lo: f64, hi: f64) -> Py<Self> {
        push_step!(slf, py, Step::WhereBetween(field.to_string(), lo, hi))
    }
    fn where_in(slf: Py<Self>, py: Python<'_>, field: &str, values: Vec<PyObject>) -> PyResult<Py<Self>> {
        let vs: PyResult<Vec<serde_json::Value>> = values.iter().map(|v| pyany_to_value(v.bind(py))).collect();
        Ok(push_step!(slf, py, Step::WhereIn(field.to_string(), vs?)))
    }

    // ── Set algebra ───────────────────────────────────────────────────────────
    fn intersect(slf: Py<Self>, py: Python<'_>, other: &PySet) -> Py<Self> {
        let other_steps = other.steps.clone();
        push_step!(slf, py, Step::Intersect(other_steps))
    }
    fn union(slf: Py<Self>, py: Python<'_>, other: &PySet) -> Py<Self> {
        let other_steps = other.steps.clone();
        push_step!(slf, py, Step::Union(other_steps))
    }
    fn subtract(slf: Py<Self>, py: Python<'_>, other: &PySet) -> Py<Self> {
        let other_steps = other.steps.clone();
        push_step!(slf, py, Step::Subtract(other_steps))
    }

    // ── Shaping ───────────────────────────────────────────────────────────────
    #[pyo3(signature = (field, asc=true))]
    fn sort(slf: Py<Self>, py: Python<'_>, field: &str, asc: bool) -> Py<Self> {
        push_step!(slf, py, Step::Sort(field.to_string(), asc))
    }
    fn skip(slf: Py<Self>, py: Python<'_>, n: usize) -> Py<Self> {
        push_step!(slf, py, Step::Skip(n))
    }
    fn take(slf: Py<Self>, py: Python<'_>, n: usize) -> Py<Self> {
        push_step!(slf, py, Step::Take(n))
    }
    fn select(slf: Py<Self>, py: Python<'_>, fields: Vec<String>) -> Py<Self> {
        push_step!(slf, py, Step::Select(fields))
    }

    // ── Execute ───────────────────────────────────────────────────────────────
    fn collect(&self) -> PyResult<Vec<PyHit>> {
        let db = self.db.read().map_err(db_err)?;
        let set = ::sekejap::Set::from_steps(&*db, self.steps.clone());
        let outcome = set.collect().map_err(db_err)?;
        Ok(outcome.data.into_iter().map(to_pyhit).collect())
    }
    fn count(&self) -> PyResult<usize> {
        let db = self.db.read().map_err(db_err)?;
        let set = ::sekejap::Set::from_steps(&*db, self.steps.clone());
        Ok(set.count().map_err(db_err)?.data)
    }
}

// ── PyNodeStore ───────────────────────────────────────────────────────────────

#[pyclass(name = "NodeStore")]
struct PyNodeStore {
    db: Arc<RwLock<SekejapDB>>,
}

#[pymethods]
impl PyNodeStore {
    fn put(&self, slug: &str, json: &str) -> PyResult<u32> {
        self.db.read().map_err(db_err)?.nodes().put(slug, json).map_err(db_err)
    }
    fn put_json(&self, json: &str) -> PyResult<u32> {
        self.db.read().map_err(db_err)?.nodes().put_json(json).map_err(db_err)
    }
    fn get(&self, slug: &str) -> Option<String> {
        self.db.read().ok()?.nodes().get(slug)
    }
    fn remove(&self, slug: &str) -> PyResult<()> {
        self.db.read().map_err(db_err)?.nodes().remove(slug).map_err(db_err)
    }
    fn ingest(&self, items: Vec<(String, String)>) -> PyResult<Vec<u32>> {
        let db = self.db.read().map_err(db_err)?;
        let refs: Vec<(&str, &str)> = items.iter().map(|(s, j)| (s.as_str(), j.as_str())).collect();
        db.nodes().ingest(&refs).map_err(db_err)
    }
    fn build_hnsw(&self) -> PyResult<()> {
        self.db.read().map_err(db_err)?.nodes().build_hnsw().map_err(db_err)
    }
    // Query starters — return PySet
    fn one(&self, slug: &str) -> PySet {
        let hash = sk_hash(slug);
        PySet::new(self.db.clone(), Step::One(hash))
    }
    fn many(&self, slugs: Vec<String>) -> PySet {
        let hashes = slugs.iter().map(|s| sk_hash(s)).collect();
        PySet::new(self.db.clone(), Step::Many(hashes))
    }
    fn collection(&self, name: &str) -> PySet {
        PySet::new(self.db.clone(), Step::Collection(sk_hash(name)))
    }
    fn all(&self) -> PySet {
        PySet::new(self.db.clone(), Step::All)
    }
}

// ── PyEdgeStore ───────────────────────────────────────────────────────────────

#[pyclass(name = "EdgeStore")]
struct PyEdgeStore {
    db: Arc<RwLock<SekejapDB>>,
}

#[pymethods]
impl PyEdgeStore {
    fn link(&self, src: &str, dst: &str, edge_type: &str, weight: f32) -> PyResult<()> {
        self.db.read().map_err(db_err)?.edges().link(src, dst, edge_type, weight).map_err(db_err)
    }
    fn link_meta(&self, src: &str, dst: &str, edge_type: &str, weight: f32, meta_json: &str) -> PyResult<()> {
        self.db.read().map_err(db_err)?.edges().link_meta(src, dst, edge_type, weight, meta_json).map_err(db_err)
    }
    fn unlink(&self, src: &str, dst: &str, edge_type: &str) -> PyResult<()> {
        self.db.read().map_err(db_err)?.edges().unlink(src, dst, edge_type).map_err(db_err)
    }
    fn ingest(&self, edges: Vec<(String, String, String, f32)>) -> PyResult<()> {
        let db = self.db.read().map_err(db_err)?;
        let refs: Vec<(&str, &str, &str, f32)> = edges.iter().map(|(s, t, e, w)| (s.as_str(), t.as_str(), e.as_str(), *w)).collect();
        db.edges().ingest(&refs).map_err(db_err)
    }
}

// ── PySchemaStore ─────────────────────────────────────────────────────────────

#[pyclass(name = "SchemaStore")]
struct PySchemaStore {
    db: Arc<RwLock<SekejapDB>>,
}

#[pymethods]
impl PySchemaStore {
    fn define(&self, name: &str, json: &str) -> PyResult<()> {
        self.db.read().map_err(db_err)?.schema().define(name, json).map_err(db_err)
    }
    fn count(&self, name: &str) -> usize {
        self.db.read().map_or(0, |db| db.schema().count(name))
    }
}

// ── PySekejapDB ───────────────────────────────────────────────────────────────

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
        std::fs::create_dir_all(path).map_err(db_err)?;
        let db = SekejapDB::new(Path::new(path), capacity).map_err(db_err)?;
        Ok(Self { db: Some(Arc::new(RwLock::new(db))), path: path.to_string() })
    }

    // ── Store accessors ───────────────────────────────────────────────────────
    fn nodes(&self) -> PyResult<PyNodeStore> {
        Ok(PyNodeStore { db: self.arc()? })
    }
    fn edges(&self) -> PyResult<PyEdgeStore> {
        Ok(PyEdgeStore { db: self.arc()? })
    }
    fn schema(&self) -> PyResult<PySchemaStore> {
        Ok(PySchemaStore { db: self.arc()? })
    }

    // ── Index lifecycle ───────────────────────────────────────────────────────
    #[pyo3(signature = (m=16))]
    fn init_hnsw(&self, m: usize) -> PyResult<()> {
        self.arc()?.read().map_err(db_err)?.init_hnsw(m);
        Ok(())
    }
    fn init_fulltext(&self) -> PyResult<()> {
        let arc = self.arc()?;
        let db = arc.read().map_err(db_err)?;
        db.init_fulltext(Path::new(&self.path));
        Ok(())
    }

    // ── Unified query interface ───────────────────────────────────────────────
    fn query(&self, input: &str) -> PyResult<String> {
        let arc = self.arc()?;
        let db = arc.read().map_err(db_err)?;
        let result = db.query(input).map_err(db_err)?;
        let hits: Vec<serde_json::Value> = result.data.into_iter().map(|h| serde_json::json!({
            "idx": h.idx, "slug_hash": h.slug_hash, "collection_hash": h.collection_hash,
            "payload": h.payload, "lat": h.lat, "lon": h.lon, "score": h.score
        })).collect();
        serde_json::to_string(&hits).map_err(db_err)
    }
    fn count(&self, input: &str) -> PyResult<usize> {
        let arc = self.arc()?;
        let db = arc.read().map_err(db_err)?;
        Ok(db.count(input).map_err(db_err)?.data)
    }
    fn explain(&self, input: &str) -> PyResult<String> {
        let arc = self.arc()?;
        let db = arc.read().map_err(db_err)?;
        let steps = db.explain(input).map_err(db_err)?;
        serde_json::to_string(&steps.iter().map(|s| format!("{:?}", s)).collect::<Vec<_>>()).map_err(db_err)
    }
    fn mutate(&self, json: &str) -> PyResult<String> {
        let arc = self.arc()?;
        let db = arc.read().map_err(db_err)?;
        let result = db.mutate(json).map_err(db_err)?;
        serde_json::to_string(&result).map_err(db_err)
    }

    // ── Introspection ─────────────────────────────────────────────────────────
    fn describe(&self) -> PyResult<String> {
        let arc = self.arc()?;
        let db = arc.read().map_err(db_err)?;
        serde_json::to_string(&db.describe()).map_err(db_err)
    }
    fn describe_collection(&self, name: &str) -> PyResult<String> {
        let arc = self.arc()?;
        let db = arc.read().map_err(db_err)?;
        serde_json::to_string(&db.describe_collection(name)).map_err(db_err)
    }

    // ── Persistence ───────────────────────────────────────────────────────────
    fn flush(&self) -> PyResult<()> {
        self.arc()?.read().map_err(db_err)?.flush().map_err(db_err)
    }
    fn backup(&self, path: &str) -> PyResult<()> {
        self.arc()?.read().map_err(db_err)?.backup(Path::new(path)).map_err(db_err)
    }
    fn restore(&self, path: &str) -> PyResult<()> {
        self.arc()?.read().map_err(db_err)?.restore(Path::new(path)).map_err(db_err)
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────
    fn close(&mut self) { self.db.take(); }
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> { slf }
    fn __exit__(&mut self, _et: PyObject, _ev: PyObject, _tb: PyObject) { self.close(); }
    fn __repr__(&self) -> String { format!("SekejapDB(path={})", self.path) }
}

impl PySekejapDB {
    fn arc(&self) -> PyResult<Arc<RwLock<SekejapDB>>> {
        self.db.clone().ok_or_else(|| PyIOError::new_err("Database not open"))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn sk_hash(s: &str) -> u64 {
    seahash::hash(s.as_bytes())
}

fn pyany_to_value(v: &Bound<'_, PyAny>) -> PyResult<serde_json::Value> {
    if let Ok(b) = v.extract::<bool>() { return Ok(serde_json::Value::Bool(b)); }
    if let Ok(i) = v.extract::<i64>()  { return Ok(serde_json::json!(i)); }
    if let Ok(f) = v.extract::<f64>()  { return Ok(serde_json::json!(f)); }
    if let Ok(s) = v.extract::<String>() { return Ok(serde_json::Value::String(s)); }
    Ok(serde_json::Value::Null)
}

// ── Utility functions ─────────────────────────────────────────────────────────

#[pyfunction]
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6371.0;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    R * 2.0 * a.sqrt().atan2((1.0 - a).sqrt())
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

// ── Module ────────────────────────────────────────────────────────────────────

#[pymodule]
fn sekejap(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySekejapDB>()?;
    m.add_class::<PyNodeStore>()?;
    m.add_class::<PyEdgeStore>()?;
    m.add_class::<PySchemaStore>()?;
    m.add_class::<PySet>()?;
    m.add_class::<PyHit>()?;
    m.add_function(wrap_pyfunction!(haversine_distance, m)?)?;
    m.add_function(wrap_pyfunction!(cosine_similarity, m)?)?;
    Ok(())
}
