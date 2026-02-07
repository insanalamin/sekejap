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

/// Python wrapper for SekejapDB
#[pyclass]
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
    fn write(&self, slug: &str, data: &str) -> PyResult<()> {
        if let Some(db) = &self.db {
            if let Ok(mut db) = db.write() {
                let _ = db.write(slug, data);
            }
        }
        Ok(())
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

/// Python module definition
#[pymodule]
fn sekejap(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySekejapDB>()?;
    m.add_function(wrap_pyfunction!(haversine_distance, m)?)?;
    m.add_function(wrap_pyfunction!(cosine_similarity, m)?)?;
    Ok(())
}
