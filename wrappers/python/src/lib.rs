//! Python bindings for Sekejap-DB using PyO3
//!
//! A graph-first, embedded multi-model database engine.
//!
//! # Installation
//!
//! ```bash
//! cd wrappers/python
//! pip install maturin
//! maturin develop
//! ```
//!
//! # Usage
//!
//! ```python
//! import sekejap
//!
//! # Create database
//! db = sekejap.SekejapDB("./data")
//!
//! # Write data (JSON string)
//! db.write("jakarta-crime", '{"title": "Jakarta Crime Report", "year": 2024}')
//!
//! # Read data
//! event = db.read("jakarta-crime")
//!
//! # Add edge (for graph joins)
//! db.add_edge("cause-node", "effect-node", 0.8, "causal")
//!
//! # Traverse graph (backward - find causes)
//! results = db.traverse("effect-node", 3, 0.5)
//!
//! # Traverse forward (find effects)
//! effects = db.traverse_forward("cause-node", 3, 0.5)
//!
//! db.close()
//! ```

use ::sekejap::types::WriteOptions;
use ::sekejap::SekejapDB;
use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;
use std::path::Path;
use std::sync::{Arc, RwLock};

/// Represents an edge in the graph (for join operations)
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

/// Python wrapper for SekejapDB
#[pyclass(name = "SekejapDB")]
struct PySekejapDB {
    /// Inner SekejapDB instance (thread-safe)
    db: Option<Arc<RwLock<SekejapDB>>>,
    /// Database path
    path: String,
}

#[pymethods]
impl PySekejapDB {
    /// Create a new SekejapDB instance
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

    /// Write data to the database
    fn write(&self, slug: &str, data: &str) -> PyResult<String> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                match db.write(slug, data) {
                    Ok(id) => return Ok(format!("{}", id)),
                    Err(e) => {
                        return Err(PyIOError::new_err(format!("Write failed: {}", e)))
                    }
                }
            }
        }
        Err(PyIOError::new_err("Database not open"))
    }

    /// Write data with options
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
                    Ok(id) => return Ok(format!("{}", id)),
                    Err(e) => {
                        return Err(PyIOError::new_err(format!("Write failed: {}", e)))
                    }
                }
            }
        }
        Err(PyIOError::new_err("Database not open"))
    }

    /// Read data from the database
    fn read(&self, slug: &str) -> Option<String> {
        let db = self.db.as_ref()?;
        if let Ok(db) = db.read() {
            return db.read(slug).ok().flatten();
        }
        None
    }

    /// Delete data from the database
    fn delete(&self, slug: &str) -> PyResult<()> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                let _ = db.delete(slug);
            }
        }
        Ok(())
    }

    /// Write multiple events to database
    fn write_many(&self, items: Vec<(String, String)>) -> PyResult<Vec<String>> {
        let mut results = Vec::new();
        for (slug, data) in items {
            if let Some(db) = &self.db {
                if let Ok(mut db) = db.write() {
                    match db.write(&slug, &data) {
                        Ok(id) => results.push(id.to_string()),
                        Err(_) => results.push("error".to_string()),
                    }
                }
            }
        }
        Ok(results)
    }

    /// Add edge between events (FOR JOINS!)
    ///
    /// This is the KEY method for implementing graph joins.
    /// Creates a relationship between two nodes that can be traversed.
    ///
    /// Args:
    ///     source_slug: Source event slug (cause)
    ///     target_slug: Target event slug (effect)  
    ///     weight: Evidence strength (0.0 - 1.0)
    ///     edge_type: Relationship type (e.g., "causal", "related", "influences")
    ///
    /// Example (for JOIN operations):
    ///     ```python
    ///     # Create nodes
    ///     db.write("restaurant-1", '{"title": "Luigi Pizza"}')
    ///     db.write("cuisine-italian", '{"title": "Italian"}')
    ///     
    ///     # Add edge FOR JOIN (restaurant -> cuisine)
    ///     db.add_edge("restaurant-1", "cuisine-italian", 0.95, "related")
    ///     ```
    fn add_edge(
        &self,
        source_slug: &str,
        target_slug: &str,
        weight: f32,
        edge_type: &str,
    ) -> PyResult<()> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                let _ = db.add_edge(source_slug, target_slug, weight, edge_type.to_string());
            }
        }
        Ok(())
    }

    /// Get all outgoing edges from a node (FOR JOINS!)
    ///
    /// Returns all edges starting FROM a specific node.
    fn get_edges_from(&self, slug: &str) -> PyResult<Vec<PyEdge>> {
        if let Some(db) = &self.db {
            if let Ok(db) = db.read() {
                let entity_id = ::sekejap::EntityId::new("nodes".to_string(), slug.to_string());
                let edges = db.graph().get_edges_from(&entity_id);

                return Ok(edges
                    .into_iter()
                    .map(|e| PyEdge {
                        source: e._from.key().to_string(),
                        target: e._to.key().to_string(),
                        weight: e.weight,
                        edge_type: e._type.clone(),
                    })
                    .collect());
            }
        }
        Ok(Vec::new())
    }

    /// Write JSON data directly (canonical format)
    fn write_json(&self, json_data: &str) -> PyResult<String> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                match db.write_json(json_data) {
                    Ok(id) => return Ok(format!("{}", id)),
                    Err(e) => {
                        return Err(PyIOError::new_err(format!(
                            "Write JSON failed: {}",
                            e
                        )))
                    }
                }
            }
        }
        Err(PyIOError::new_err("Database not open"))
    }

    /// Search vector index
    fn search_vector(&self, query: Vec<f32>, k: usize) -> PyResult<Vec<(String, f32)>> {
        #[cfg(feature = "vector")]
        if let Some(db) = &self.db {
            if let Ok(db) = db.read() {
                match db.search_vector(&query, k) {
                    Ok(results) => {
                        return Ok(results
                            .into_iter()
                            .map(|(id, dist)| {
                                // Try to map NodeId back to slug if possible, or stringify ID
                                let slug = db
                                    .storage()
                                    .get_by_id(id, None)
                                    .and_then(|n| n.entity_id)
                                    .map(|eid| eid.key().to_string())
                                    .unwrap_or_else(|| id.to_string());
                                (slug, dist)
                            })
                            .collect());
                    }
                    Err(e) => {
                        return Err(PyIOError::new_err(format!(
                            "Vector search failed: {}",
                            e
                        )))
                    }
                }
            }
        }
        #[cfg(not(feature = "vector"))]
        return Err(PyIOError::new_err("Vector feature not enabled"));

        Ok(Vec::new())
    }

    /// Search spatial index
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
                                    .map(|eid| eid.key().to_string())
                                    .unwrap_or_else(|| id.to_string())
                            })
                            .collect());
                    }
                    Err(e) => {
                        return Err(PyIOError::new_err(format!(
                            "Spatial search failed: {}",
                            e
                        )))
                    }
                }
            }
        }
        #[cfg(not(feature = "spatial"))]
        return Err(PyErr::new::<PyIOError, _>("Spatial feature not enabled"));

        Ok(Vec::new())
    }

    /// Search fulltext index
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
                                    .map(|eid| eid.key().to_string())
                                    .unwrap_or_else(|| id.to_string())
                            })
                            .collect());
                    }
                    Err(e) => {
                        return Err(PyErr::new::<PyIOError, _>(format!(
                            "Fulltext search failed: {}",
                            e
                        )))
                    }
                }
            }
        }
        #[cfg(not(feature = "fulltext"))]
        return Err(PyErr::new::<PyIOError, _>("Fulltext feature not enabled"));

        Ok(Vec::new())
    }

    /// Traverse graph BACKWARD (find causes) - for RCA
    ///
    /// Args:
    ///     slug: Starting node (effect)
    ///     max_hops: Maximum traversal depth
    ///     weight_threshold: Minimum edge weight (0.0 - 1.0)
    ///
    /// Returns:
    ///     TraversalResult with path and edges
    ///
    /// Example:
    ///     ```python
    ///     # Find causes of a crime
    ///     results = db.traverse("crime-2024", 3, 0.5)
    ///     for edge in results.edges:
    ///         print(f"Cause: {edge.source} -> {edge.target}")
    ///     ```
    fn traverse(
        &self,
        slug: &str,
        max_hops: usize,
        weight_threshold: f32,
    ) -> Option<PyTraversalResult> {
        let db = self.db.as_ref()?;
        if let Ok(db) = db.read() {
            match db.traverse(slug, max_hops, weight_threshold, None) {
                Ok(result) => {
                    let edges: Vec<PyEdge> = result
                        .edges
                        .iter()
                        .map(|e| PyEdge {
                            source: e._from.key().to_string(),
                            target: e._to.key().to_string(),
                            weight: e.weight,
                            edge_type: e._type.clone(),
                        })
                        .collect();

                    let path: Vec<String> =
                        result.path.iter().map(|id| id.key().to_string()).collect();

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

    /// Traverse graph FORWARD (find effects) - FOR JOINS!
    ///
    /// This is the CRITICAL method for implementing graph-based JOINs.
    /// Given a node, find all nodes it points TO (effects/causes).
    ///
    /// Args:
    ///     slug: Starting node (cause)
    ///     max_hops: Maximum traversal depth
    ///     weight_threshold: Minimum edge weight (0.0 - 1.0)
    ///     edge_type: Optional edge type filter (e.g., "related")
    ///
    /// Returns:
    ///     TraversalResult with path and edges
    ///
    /// Example (FORWARD JOIN implementation):
    ///     ```python
    ///     # INNER JOIN: restaurants -> cuisines
    ///     # Start from restaurants, traverse FORWARD to find cuisines
    ///     for restaurant in restaurants:
    ///         result = db.traverse_forward(restaurant, 1, 0.0, "related")
    ///         for edge in result.edges:
    ///             cuisine = db.read(edge.target)
    ///             join_results.append((restaurant, cuisine))
    ///     ```
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
                            source: e._from.key().to_string(),
                            target: e._to.key().to_string(),
                            weight: e.weight,
                            edge_type: e._type.clone(),
                        })
                        .collect();

                    let path: Vec<String> =
                        result.path.iter().map(|id| id.key().to_string()).collect();

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

    /// Backup all data to JSON file
    fn backup(&self, path: &str) -> PyResult<()> {
        if let Some(db) = &self.db {
            if let Ok(db) = db.read() {
                let _ = db.backup(Path::new(path));
            }
        }
        Ok(())
    }

    /// Close the database
    fn close(&mut self) {
        self.db.take();
    }

    /// Context manager support
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

/// Calculate distance between two points using Haversine formula
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

/// Calculate cosine similarity between two vectors
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

/// Write options for controlling write behavior
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

/// Python module definition
#[pymodule]
fn sekejap(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySekejapDB>()?;
    m.add_class::<PyEdge>()?;
    m.add_class::<PyTraversalResult>()?;
    m.add_class::<PySekejapWriteOptions>()?;
    m.add_function(wrap_pyfunction!(haversine_distance, m)?)?;
    m.add_function(wrap_pyfunction!(cosine_similarity, m)?)?;
    Ok(())
}
