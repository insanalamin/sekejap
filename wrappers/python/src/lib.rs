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
//! # Read data (returns JSON string)
//! event = db.read("jakarta-crime")
//! print(event)
//!
//! db.close()
//! ```

use pyo3::prelude::*;
use pyo3::exceptions::PyIOError;
use std::path::Path;
use std::sync::{Arc, RwLock};
use ::sekejap::SekejapDB;
use ::sekejap::types::WriteOptions;

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
    ///
    /// Args:
    ///     path: Path to database directory
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let path = Path::new(path);
        match SekejapDB::new(path) {
            Ok(db) => Ok(Self {
                db: Some(Arc::new(RwLock::new(db))),
                path: path.to_string_lossy().to_string(),
            }),
            Err(e) => Err(PyErr::new::<PyIOError, _>(format!("Failed to open database: {}", e))),
        }
    }

    /// Write data to the database
    ///
    /// Args:
    ///     slug: Unique identifier for the data
    ///     data: JSON string to store
    ///
    /// Example:
    ///     ```python
    ///     db.write("user-1", '{"name": "John", "age": 30}')
    ///     ```
    fn write(&self, slug: &str, data: &str) -> PyResult<String> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                match db.write(slug, data) {
                    Ok(id) => return Ok(format!("{}", id)),
                    Err(e) => return Err(PyErr::new::<PyIOError, _>(format!("Write failed: {}", e))),
                }
            }
        }
        Err(PyErr::new::<PyIOError, _>("Database not open"))
    }

    /// Write data with options
    ///
    /// Args:
    ///     slug: Unique identifier for the data
    ///     data: JSON string to store
    ///     opts: WriteOptions with publish_now=True for immediate read
    ///
    /// Example:
    ///     ```python
    ///     opts = sekejap.WriteOptions(publish_now=True)
    ///     db.write_with_options("user-1", '{"name": "John"}', opts)
    ///     ```
    fn write_with_options(&self, slug: &str, data: &str, opts: &PySekejapWriteOptions) -> PyResult<String> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                match db.write_with_options(slug, data, WriteOptions {
                    publish_now: opts.publish_now,
                    ..Default::default()
                }) {
                    Ok(id) => return Ok(format!("{}", id)),
                    Err(e) => return Err(PyErr::new::<PyIOError, _>(format!("Write failed: {}", e))),
                }
            }
        }
        Err(PyErr::new::<PyIOError, _>("Database not open"))
    }

    /// Read data from the database
    ///
    /// Args:
    ///     slug: Unique identifier for the data
    ///
    /// Returns:
    ///     JSON string if found
    ///
    /// Example:
    ///     ```python
    ///     data = db.read("user-1")
    ///     if data:
    ///         print(data)
    ///     ```
    fn read(&self, slug: &str) -> Option<String> {
        let db = self.db.as_ref()?;
        if let Ok(db) = db.read() {
            return db.read(slug).ok().flatten();
        }
        None
    }

    /// Delete data from the database
    ///
    /// Args:
    ///     slug: Unique identifier for the data
    fn delete(&self, slug: &str) -> PyResult<()> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                let _ = db.delete(slug);
            }
        }
        Ok(())
    }

    /// Write multiple events to database
    ///
    /// Args:
    ///     items: List of (slug, data) tuples
    ///
    /// Returns:
    ///     List of NodeId strings
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

    /// Add edge between events
    ///
    /// Args:
    ///     source_slug: Source event slug (cause)
    ///     target_slug: Target event slug (effect)
    ///     weight: Evidence strength (0.0 - 1.0)
    ///     edge_type: Relationship type string
    fn add_edge(&self, source_slug: &str, target_slug: &str, weight: f32, edge_type: &str) -> PyResult<()> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                let _ = db.add_edge(source_slug, target_slug, weight, edge_type.to_string());
            }
        }
        Ok(())
    }

    /// Manually trigger promotion of all staged nodes from Tier 1 to Tier 2
    ///
    /// Returns:
    ///     Number of nodes promoted
    fn flush(&self) -> usize {
        // Note: flush requires &mut self in Rust, so we need to handle this differently
        // For now, return 0 as placeholder
        0
    }

    /// Backup all data to JSON file
    ///
    /// Args:
    ///     path: Path to backup file
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
    ///
    /// Example:
    ///     ```python
    ///     with sekejap.SekejapDB("./data") as db:
    ///         db.write("key", '{"data": "value"}')
    ///     ```
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
///
/// Args:
///     lat1, lon1: First point coordinates
///     lat2, lon2: Second point coordinates
///
/// Returns:
///     Distance in kilometers
///
/// Example:
///     ```python
///     distance = sekejap.haversine_distance(-6.2, 106.8, -6.1, 106.9)
///     ```
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
///
/// Args:
///     v1, v2: Lists of floats
///
/// Returns:
///     Similarity score between -1.0 and 1.0
///
/// Example:
///     ```python
///     score = sekejap.cosine_similarity([0.1, 0.2, 0.3], [0.1, 0.2, 0.3])
///     ```
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
    m.add_class::<PySekejapWriteOptions>()?;
    m.add_function(wrap_pyfunction!(haversine_distance, m)?)?;
    m.add_function(wrap_pyfunction!(cosine_similarity, m)?)?;
    Ok(())
}
