//! Python bindings for Sekejap-DB using PyO3
//!
//! A graph-first, embedded multi-model database engine.

use ::sekejap::types::WriteOptions;
use ::sekejap::SekejapDB;
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use std::path::Path;
use std::sync::{Arc, RwLock};

/// Represents an edge in the graph
#[pyclass(name = "Edge")]
#[derive(Debug, Clone)]
struct PyEdge {
    source: String,
    target: String,
    weight: f32,
    edge_type: String,
}

#[pymethods]
impl PyEdge {
    #[getter]
    fn source(&self) -> &str {
        &self.source
    }

    #[getter]
    fn target(&self) -> &str {
        &self.target
    }

    #[getter]
    fn weight(&self) -> f32 {
        self.weight
    }

    #[getter]
    fn edge_type(&self) -> &str {
        &self.edge_type
    }

    fn __repr__(&self) -> String {
        format!(
            "Edge({} -> {}, weight={:.2}, type={})",
            self.source, self.target, self.weight, self.edge_type
        )
    }
}

/// Represents a traversal result
#[pyclass(name = "TraversalResult")]
#[derive(Debug, Clone)]
struct PyTraversalResult {
    path: Vec<String>,
    edges: Vec<PyEdge>,
    total_weight: f32,
}

#[pymethods]
impl PyTraversalResult {
    #[getter]
    fn path(&self) -> Vec<String> {
        self.path.clone()
    }

    #[getter]
    fn edges(&self) -> Vec<PyEdge> {
        self.edges.clone()
    }

    #[getter]
    fn total_weight(&self) -> f32 {
        self.total_weight
    }

    fn __repr__(&self) -> String {
        format!(
            "TraversalResult(path_len={}, edges={})",
            self.path.len(),
            self.edges.len()
        )
    }
}

/// Query Builder for Python
#[pyclass(name = "QueryBuilder")]
struct PyQueryBuilder {
    db: Option<Arc<RwLock<SekejapDB>>>,
    spatial_filter: Option<(f64, f64, f64)>, // lat, lon, radius
    vector_filter: Option<(Vec<f32>, usize)>, // vector, k
    text_filter: Option<(String, usize)>, // query, limit
    edge_filter: Option<(String, String)>, // source_slug, edge_type
    limit: Option<usize>,
}

#[pymethods]
impl PyQueryBuilder {
    fn spatial(mut slf: PyRefMut<'_, Self>, lat: f64, lon: f64, radius: f64) -> PyRefMut<'_, Self> {
        slf.spatial_filter = Some((lat, lon, radius));
        slf
    }

    fn vector_search(mut slf: PyRefMut<'_, Self>, query: Vec<f32>, k: usize) -> PyRefMut<'_, Self> {
        slf.vector_filter = Some((query, k));
        slf
    }

    fn fulltext(mut slf: PyRefMut<'_, Self>, query: String) -> PyRefMut<'_, Self> {
        slf.text_filter = Some((query, 100));
        slf
    }

    fn has_edge_from(mut slf: PyRefMut<'_, Self>, source: String, edge_type: String) -> PyRefMut<'_, Self> {
        slf.edge_filter = Some((source, edge_type));
        slf
    }

    fn limit(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.limit = Some(n);
        slf
    }

    fn execute(&self) -> PyResult<Vec<String>> {
        let db_lock = self.db.as_ref().ok_or_else(|| PyIOError::new_err("Database not open"))?;
        let db = db_lock.read().map_err(|e| PyIOError::new_err(format!("Lock error: {}", e)))?;

        let mut candidates: Option<std::collections::HashSet<String>> = None;

        let intersect = |current: Option<std::collections::HashSet<String>>, new_set: std::collections::HashSet<String>| {
            match current {
                Some(c) => Some(c.intersection(&new_set).cloned().collect()),
                None => Some(new_set),
            }
        };

        #[cfg(feature = "spatial")]
        if let Some((lat, lon, radius)) = self.spatial_filter {
            match db.search_spatial(lat, lon, radius) {
                Ok(ids) => {
                    let slugs: std::collections::HashSet<String> = ids.into_iter().map(|id| {
                        db.storage().get_by_id(id, None)
                            .and_then(|n| n.entity_id)
                            .map(|eid| eid.to_string())
                            .unwrap_or_else(|| id.to_string())
                    }).collect();
                    candidates = intersect(candidates, slugs);
                },
                Err(e) => return Err(PyIOError::new_err(format!("Spatial search error: {}", e))),
            }
        }

        #[cfg(feature = "vector")]
        if let Some((ref query, k)) = self.vector_filter {
            match db.search_vector(query, k) {
                Ok(results) => {
                    let slugs: std::collections::HashSet<String> = results.into_iter().map(|(id, _)| {
                        db.storage().get_by_id(id, None)
                            .and_then(|n| n.entity_id)
                            .map(|eid| eid.to_string())
                            .unwrap_or_else(|| id.to_string())
                    }).collect();
                    candidates = intersect(candidates, slugs);
                },
                Err(e) => return Err(PyIOError::new_err(format!("Vector search error: {}", e))),
            }
        }

        #[cfg(feature = "fulltext")]
        if let Some((ref query, limit)) = self.text_filter {
            match db.search_text(query, limit) {
                Ok(ids) => {
                    let slugs: std::collections::HashSet<String> = ids.into_iter().map(|id| {
                        db.storage().get_by_id(id, None)
                            .and_then(|n| n.entity_id)
                            .map(|eid| eid.to_string())
                            .unwrap_or_else(|| id.to_string())
                    }).collect();
                    candidates = intersect(candidates, slugs);
                },
                Err(e) => return Err(PyIOError::new_err(format!("Fulltext search error: {}", e))),
            }
        }

        if let Some((ref source, ref edge_type)) = self.edge_filter {
            let entity_id = ::sekejap::EntityId::parse(source)
                .unwrap_or_else(|_| ::sekejap::EntityId::new("nodes".to_string(), source.clone()));
            
            let edges = db.graph().get_edges_from(&entity_id);
            let slugs: std::collections::HashSet<String> = edges.into_iter()
                .filter(|e| &e._type == edge_type)
                .map(|e| e._to.to_string())
                .collect();
            candidates = intersect(candidates, slugs);
        }

        let mut final_results = match candidates {
            Some(c) => c.into_iter().collect::<Vec<_>>(),
            None => Vec::new(),
        };

        if let Some(limit) = self.limit {
            if final_results.len() > limit {
                final_results.truncate(limit);
            }
        }

        Ok(final_results)
    }
}

impl PyQueryBuilder {
    fn new(db: Option<Arc<RwLock<SekejapDB>>>) -> Self {
        Self {
            db,
            spatial_filter: None,
            vector_filter: None,
            text_filter: None,
            edge_filter: None,
            limit: None,
        }
    }
}

/// Python wrapper for SekejapDB
#[pyclass(name = "SekejapDB")]
struct PySekejapDB {
    db: Option<Arc<RwLock<SekejapDB>>>,
    path: String,
}

#[pymethods]
impl PySekejapDB {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let path = Path::new(path);
        match SekejapDB::new(path) {
            Ok(db) => Ok(Self {
                db: Some(Arc::new(RwLock::new(db))),
                path: path.to_string_lossy().to_string(),
            }),
            Err(e) => Err(PyIOError::new_err(format!("Failed to open database: {}", e))),
        }
    }

    fn write(&self, slug: &str, data: &str) -> PyResult<String> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                match db.write(slug, data) {
                    Ok(id) => {
                        if let Some(node) = db.storage().get_by_id(id, None) {
                            if let Some(eid) = &node.entity_id {
                                return Ok(eid.to_string());
                            }
                        }
                        return Ok(format!("{}", id));
                    }
                    Err(e) => return Err(PyIOError::new_err(format!("Write failed: {}", e))),
                }
            }
        }
        Err(PyIOError::new_err("Database not open"))
    }

    fn write_with_options(
        &self,
        slug: &str,
        data: &str,
        opts: &PySekejapWriteOptions,
    ) -> PyResult<String> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                match db.write_with_options(
                    slug,
                    data,
                    WriteOptions {
                        publish_now: opts.publish_now,
                        ..Default::default()
                    },
                ) {
                    Ok(id) => {
                        if let Some(node) = db.storage().get_by_id(id, None) {
                            if let Some(eid) = &node.entity_id {
                                return Ok(eid.to_string());
                            }
                        }
                        return Ok(format!("{}", id));
                    }
                    Err(e) => return Err(PyIOError::new_err(format!("Write failed: {}", e))),
                }
            }
        }
        Err(PyIOError::new_err("Database not open"))
    }

    fn read(&self, slug: &str) -> Option<String> {
        let db = self.db.as_ref()?;
        if let Ok(db) = db.read() {
            return db.read(slug).ok().flatten();
        }
        None
    }

    fn delete(&self, slug: &str) -> PyResult<()> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                let _ = db.delete(slug);
            }
        }
        Ok(())
    }

    fn update(&self, slug: &str, data: &str) -> PyResult<String> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                match db.update(slug, data) {
                    Ok(id) => {
                        if let Some(node) = db.storage().get_by_id(id, None) {
                            if let Some(eid) = &node.entity_id {
                                return Ok(eid.to_string());
                            }
                        }
                        return Ok(format!("{}", id));
                    }
                    Err(e) => return Err(PyIOError::new_err(format!("Update failed: {}", e))),
                }
            }
        }
        Err(PyIOError::new_err("Database not open"))
    }

    #[pyo3(signature = (source, target, weight, edge_type=None))]
    fn update_edge(&self, source: &str, target: &str, weight: f32, edge_type: Option<&str>) -> PyResult<bool> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                let et = edge_type.map(|s| s.to_string());
                match db.update_edge(source, target, weight, et) {
                    Ok(updated) => return Ok(updated),
                    Err(e) => return Err(PyIOError::new_err(format!("Update edge failed: {}", e))),
                }
            }
        }
        Err(PyIOError::new_err("Database not open"))
    }

    #[pyo3(signature = (source, target, edge_type=None))]
    fn delete_edge(&self, source: &str, target: &str, edge_type: Option<&str>) -> PyResult<bool> {
        if let Some(db) = &self.db {
            if let Ok(db) = db.read() {
                let et = edge_type.map(|s| s.to_string());
                match db.delete_edge(source, target, et) {
                    Ok(deleted) => return Ok(deleted),
                    Err(e) => return Err(PyIOError::new_err(format!("Delete edge failed: {}", e))),
                }
            }
        }
        Err(PyIOError::new_err("Database not open"))
    }

    fn delete_with_options(&self, slug: &str, opts: &PyDeleteOptions) -> PyResult<()> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                let options = ::sekejap::DeleteOptions {
                    exclude_edges: opts.exclude_edges,
                };
                match db.delete_with_options(slug, options) {
                    Ok(_) => return Ok(()),
                    Err(e) => return Err(PyIOError::new_err(format!("Delete failed: {}", e))),
                }
            }
        }
        Err(PyIOError::new_err("Database not open"))
    }

    fn flush(&self) -> PyResult<usize> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                match db.flush() {
                    Ok(count) => return Ok(count),
                    Err(e) => return Err(PyIOError::new_err(format!("Flush failed: {}", e))),
                }
            }
        }
        Err(PyIOError::new_err("Database not open"))
    }

    fn restore(&self, path: &str) -> PyResult<()> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                match db.restore(Path::new(path)) {
                    Ok(_) => return Ok(()),
                    Err(e) => return Err(PyIOError::new_err(format!("Restore failed: {}", e))),
                }
            }
        }
        Err(PyIOError::new_err("Database not open"))
    }

    fn define_collection(&self, json_data: &str) -> PyResult<()> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                match db.define_collection(json_data) {
                    Ok(_) => return Ok(()),
                    Err(e) => return Err(PyIOError::new_err(format!("Define collection failed: {}", e))),
                }
            }
        }
        Err(PyIOError::new_err("Database not open"))
    }

    fn query(&self) -> PyResult<PyQueryBuilder> {
        Ok(PyQueryBuilder::new(self.db.clone()))
    }

    fn write_many(&self, items: Vec<(String, String)>) -> PyResult<Vec<String>> {
        let mut results = Vec::new();
        for (slug, data) in items {
            if let Some(db) = &self.db {
                if let Ok(mut db) = db.write() {
                    match db.write(&slug, &data) {
                        Ok(id) => {
                            if let Some(node) = db.storage().get_by_id(id, None) {
                                if let Some(eid) = &node.entity_id {
                                    results.push(eid.to_string());
                                    continue;
                                }
                            }
                            results.push(format!("{}", id));
                        }
                        Err(_) => results.push("error".to_string()),
                    }
                }
            }
        }
        Ok(results)
    }

    fn add_edge(&self, source_slug: &str, target_slug: &str, weight: f32, edge_type: &str) -> PyResult<()> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                let _ = db.add_edge(source_slug, target_slug, weight, edge_type.to_string());
            }
        }
        Ok(())
    }

    fn get_edges_from(&self, slug: &str) -> PyResult<Vec<PyEdge>> {
        if let Some(db) = &self.db {
            if let Ok(db) = db.read() {
                let entity_id = ::sekejap::EntityId::parse(slug)
                    .unwrap_or_else(|_| ::sekejap::EntityId::new("nodes".to_string(), slug.to_string()));
                let edges = db.graph().get_edges_from(&entity_id);

                return Ok(edges
                    .into_iter()
                    .map(|e| PyEdge {
                        source: e._from.to_string(),
                        target: e._to.to_string(),
                        weight: e.weight,
                        edge_type: e._type.clone(),
                    })
                    .collect());
            }
        }
        Ok(Vec::new())
    }

    fn write_json(&self, json_data: &str) -> PyResult<String> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                match db.write_json(json_data) {
                    Ok(id) => {
                        if let Some(node) = db.storage().get_by_id(id, None) {
                            if let Some(eid) = &node.entity_id {
                                return Ok(eid.to_string());
                            }
                        }
                        return Ok(format!("{}", id));
                    },
                    Err(e) => return Err(PyIOError::new_err(format!("Write JSON failed: {}", e))),
                }
            }
        }
        Err(PyIOError::new_err("Database not open"))
    }

    fn search_vector(&self, query: Vec<f32>, k: usize) -> PyResult<Vec<(String, f32)>> {
        #[cfg(feature = "vector")]
        if let Some(db) = &self.db {
            if let Ok(db) = db.read() {
                match db.search_vector(&query, k) {
                    Ok(results) => {
                        return Ok(results
                            .into_iter()
                            .map(|(id, dist)| {
                                let slug = db
                                    .storage()
                                    .get_by_id(id, None)
                                    .and_then(|n| n.entity_id)
                                    .map(|eid| eid.to_string())
                                    .unwrap_or_else(|| id.to_string());
                                (slug, dist)
                            })
                            .collect());
                    }
                    Err(e) => return Err(PyIOError::new_err(format!("Vector search failed: {}", e))),
                }
            }
        }
        Ok(Vec::new())
    }

    fn search_spatial(&self, lat: f64, lon: f64, radius_km: f64) -> PyResult<Vec<String>> {
        #[cfg(feature = "spatial")]
        if let Some(db) = &self.db {
            if let Ok(db) = db.read() {
                match db.search_spatial(lat, lon, radius_km) {
                    Ok(ids) => {
                        return Ok(ids
                            .into_iter()
                            .map(|id| {
                                db.storage()
                                    .get_by_id(id, None)
                                    .and_then(|n| n.entity_id)
                                    .map(|eid| eid.to_string())
                                    .unwrap_or_else(|| id.to_string())
                            })
                            .collect());
                    }
                    Err(e) => return Err(PyIOError::new_err(format!("Spatial search failed: {}", e))),
                }
            }
        }
        Ok(Vec::new())
    }

    fn search_text(&self, query: &str, limit: usize) -> PyResult<Vec<String>> {
        #[cfg(feature = "fulltext")]
        if let Some(db) = &self.db {
            if let Ok(db) = db.read() {
                match db.search_text(query, limit) {
                    Ok(ids) => {
                        return Ok(ids
                            .into_iter()
                            .map(|id| {
                                db.storage()
                                    .get_by_id(id, None)
                                    .and_then(|n| n.entity_id)
                                    .map(|eid| eid.to_string())
                                    .unwrap_or_else(|| id.to_string())
                            })
                            .collect());
                    }
                    Err(e) => return Err(PyIOError::new_err(format!("Fulltext search failed: {}", e))),
                }
            }
        }
        Ok(Vec::new())
    }

    #[pyo3(signature = (slug, max_hops, weight_threshold, edge_type=None))]
    fn traverse(
        &self,
        slug: &str,
        max_hops: usize,
        weight_threshold: f32,
        edge_type: Option<&str>,
    ) -> Option<PyTraversalResult> {
        let db = self.db.as_ref()?;
        if let Ok(db) = db.read() {
            match db.traverse(slug, max_hops, weight_threshold, edge_type) {
                Ok(result) => {
                    let edges: Vec<PyEdge> = result
                        .edges
                        .iter()
                        .map(|e| PyEdge {
                            source: e._from.to_string(),
                            target: e._to.to_string(),
                            weight: e.weight,
                            edge_type: e._type.clone(),
                        })
                        .collect();

                    let path: Vec<String> =
                        result.path.iter().map(|id| id.to_string()).collect();

                    return Some(PyTraversalResult {
                        path,
                        edges,
                        total_weight: result.total_weight,
                    });
                }
                Err(_) => return None,
            }
        }
        None
    }

    #[pyo3(signature = (slug, max_hops, weight_threshold, edge_type=None))]
    fn traverse_forward(
        &self,
        slug: &str,
        max_hops: usize,
        weight_threshold: f32,
        edge_type: Option<&str>,
    ) -> Option<PyTraversalResult> {
        let db = self.db.as_ref()?;
        if let Ok(db) = db.read() {
            match db.traverse_forward(slug, max_hops, weight_threshold, edge_type, None) {
                Ok(result) => {
                    let edges: Vec<PyEdge> = result
                        .edges
                        .iter()
                        .map(|e| PyEdge {
                            source: e._from.to_string(),
                            target: e._to.to_string(),
                            weight: e.weight,
                            edge_type: e._type.clone(),
                        })
                        .collect();

                    let path: Vec<String> =
                        result.path.iter().map(|id| id.to_string()).collect();

                    return Some(PyTraversalResult {
                        path,
                        edges,
                        total_weight: result.total_weight,
                    });
                }
                Err(_) => return None,
            }
        }
        None
    }

    fn backup(&self, path: &str) -> PyResult<()> {
        if let Some(db) = &self.db {
            if let Ok(db) = db.read() {
                let _ = db.backup(Path::new(path));
            }
        }
        Ok(())
    }

    fn close(&mut self) {
        self.db.take();
    }

    fn __enter__(&self) -> Self {
        Self {
            db: self.db.clone(),
            path: self.path.clone(),
        }
    }

    fn __exit__(&mut self, _exc_type: PyObject, _exc_value: PyObject, _traceback: PyObject) {
        self.close();
    }
}

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
    if v1.len() != v2.len() || v1.is_empty() {
        return 0.0;
    }
    let dot_product: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
    let norm1: f32 = v1.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm2: f32 = v2.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm1 == 0.0 || norm2 == 0.0 {
        return 0.0;
    }
    dot_product / (norm1 * norm2)
}

#[pyclass(name = "WriteOptions")]
#[derive(Debug, Clone)]
struct PySekejapWriteOptions {
    publish_now: bool,
}

#[pymethods]
impl PySekejapWriteOptions {
    #[new]
    fn new(publish_now: bool) -> Self {
        Self { publish_now }
    }
}

#[pyclass(name = "DeleteOptions")]
#[derive(Debug, Clone)]
struct PyDeleteOptions {
    exclude_edges: bool,
}

#[pymethods]
impl PyDeleteOptions {
    #[new]
    fn new(exclude_edges: bool) -> Self {
        Self { exclude_edges }
    }
}

#[pymodule]
fn sekejap(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySekejapDB>()?;
    m.add_class::<PyEdge>()?;
    m.add_class::<PyTraversalResult>()?;
    m.add_class::<PySekejapWriteOptions>()?;
    m.add_class::<PyDeleteOptions>()?;
    m.add_class::<PyQueryBuilder>()?;
    m.add_function(wrap_pyfunction!(haversine_distance, m)?)?;
    m.add_function(wrap_pyfunction!(cosine_similarity, m)?)?;
    Ok(())
}