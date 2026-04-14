//! Python bindings for sekejap via PyO3.

use pyo3::exceptions::PyIOError;
use pyo3::prelude::*;

use ::sekejap::CoreDB;
use ::sekejap::EdgeHit;
use ::sekejap::Hit;

// ── PyHit ─────────────────────────────────────────────────────────────────────

/// A single result row from a query.
///
/// Attributes:
///     slug (str): The node's key path, e.g. ``"students/ali"``.
///     payload (str | None): Raw JSON string of the node payload, or ``None``.
///         Parse with ``json.loads(hit.payload)``.
#[pyclass(name = "Hit")]
#[derive(Clone)]
pub struct PyHit {
    #[pyo3(get)]
    pub slug: String,
    /// Raw JSON string — call json.loads() on the Python side.
    #[pyo3(get)]
    pub payload: Option<String>,
}

#[pymethods]
impl PyHit {
    fn __repr__(&self) -> String {
        let preview = self.payload.as_deref()
            .map(|s| &s[..s.len().min(80)])
            .unwrap_or("None");
        format!("Hit(slug={:?}, payload={})", self.slug, preview)
    }
}

fn to_pyhit(h: Hit) -> PyHit {
    PyHit {
        slug: h.slug,
        payload: h.payload.as_ref().map(|v| v.to_string()),
    }
}

// ── PyEdgeHit ─────────────────────────────────────────────────────────────────

/// A resolved edge from a graph query or edge inspection call.
///
/// Attributes:
///     from_slug (str | None): Slug of the source node.
///     to_slug (str | None): Slug of the target node.
///     edge_type (str | None): Human-readable edge type label, e.g. ``"route_to"``.
///     strength (float): Edge weight.
///     meta_json (str | None): Raw JSON string of edge metadata, or ``None``.
///         Parse with ``json.loads(hit.meta_json)``.
#[pyclass(name = "EdgeHit")]
#[derive(Clone)]
pub struct PyEdgeHit {
    #[pyo3(get)]
    pub from_slug: Option<String>,
    #[pyo3(get)]
    pub to_slug: Option<String>,
    #[pyo3(get)]
    pub edge_type: Option<String>,
    #[pyo3(get)]
    pub strength: f32,
    #[pyo3(get)]
    pub meta_json: Option<String>,
}

#[pymethods]
impl PyEdgeHit {
    fn __repr__(&self) -> String {
        format!(
            "EdgeHit(from={:?}, to={:?}, type={:?}, strength={})",
            self.from_slug, self.to_slug, self.edge_type, self.strength
        )
    }
}

fn to_pyedgehit(e: EdgeHit) -> PyEdgeHit {
    PyEdgeHit {
        from_slug: e.from_slug,
        to_slug: e.to_slug,
        edge_type: e.edge_type,
        strength: e.strength,
        meta_json: e.meta.as_ref().map(|v| v.to_string()),
    }
}

fn db_err(e: impl std::fmt::Display) -> PyErr {
    PyIOError::new_err(e.to_string())
}

// ── PyDB ──────────────────────────────────────────────────────────────────────

/// An embedded graph + document database.
///
/// Example::
///
///     from sekejap import DB
///
///     db = DB()               # in-memory
///     db = DB("./data")       # persistent (WAL-backed)
///
///     db.put("students/ali", '{"_collection":"students","name":"Ali"}')
///     db.link("cls/math", "lec/ali", "taught_by", 1.0)
///
///     hits = db.query("SELECT * FROM students")
///     for h in hits:
///         print(h.slug, h.payload)   # payload is a JSON string
#[pyclass(name = "DB", subclass)]
pub struct PyDB {
    inner: Option<CoreDB>,
}

#[pymethods]
impl PyDB {
    /// Open or create a database.
    ///
    /// Args:
    ///     path (str, optional): Directory for persistent storage.
    ///         Omit or pass ``None`` for an in-memory database.
    #[new]
    #[pyo3(signature = (path=None))]
    fn new(path: Option<&str>) -> PyResult<Self> {
        let inner = match path {
            Some(p) => CoreDB::open(p).map_err(db_err)?,
            None    => CoreDB::new(),
        };
        Ok(Self { inner: Some(inner) })
    }

    // ── Nodes ─────────────────────────────────────────────────────────────────

    /// Store a node. ``json`` must contain ``_collection`` and ``_key``.
    fn put(&mut self, key: &str, json: &str) -> PyResult<()> {
        self.db_mut()?.put(key, json).map(|_| ()).map_err(db_err)
    }

    /// Retrieve a node's raw JSON string, or ``None``.
    fn get(&self, key: &str) -> PyResult<Option<String>> {
        Ok(self.db()?.get(key))
    }

    /// Delete a node (and its edges).
    fn remove(&mut self, key: &str) {
        if let Some(db) = self.inner.as_mut() { db.remove(key); }
    }

    /// Return ``True`` if the node exists.
    fn contains(&self, key: &str) -> PyResult<bool> {
        Ok(self.db()?.contains(key))
    }

    // ── Edges ─────────────────────────────────────────────────────────────────

    /// Create a directed edge: ``from -[edge_type]-> to``.
    fn link(&mut self, from: &str, to: &str, edge_type: &str, strength: f32) {
        if let Some(db) = self.inner.as_mut() {
            db.link(from, to, edge_type, strength);
        }
    }

    /// Create a directed edge with JSON metadata.
    fn link_meta(&mut self, from: &str, to: &str, edge_type: &str, strength: f32, meta_json: &str) -> PyResult<()> {
        self.db_mut()?.link_meta(from, to, edge_type, strength, meta_json).map_err(db_err)
    }

    /// Remove a directed edge.
    fn unlink(&mut self, from: &str, to: &str, edge_type: &str) {
        if let Some(db) = self.inner.as_mut() { db.unlink(from, to, edge_type); }
    }

    // ── SQL ───────────────────────────────────────────────────────────────────

    /// Execute a SQL query. Returns a list of :class:`Hit`.
    ///
    /// Each ``hit.payload`` is a raw JSON string — use ``json.loads(hit.payload)``.
    ///
    /// Supported forms::
    ///
    ///     # Standard SELECT
    ///     db.query("SELECT * FROM characters WHERE bounty >= 1000000000")
    ///
    ///     # Graph aggregate
    ///     db.query("""
    ///         SELECT b._key AS name, COUNT(a) AS rivals
    ///         FROM MATCH (a:characters)-[r:rival]->(b:characters)
    ///         GROUP BY b._key ORDER BY rivals DESC LIMIT 10
    ///     """)
    ///
    ///     # PATH_* aggregates — operate on path intrinsic arrays
    ///     db.query("""
    ///         SELECT c._key AS dest, PATH_PRODUCT(r2._path_strength) AS reliability
    ///         FROM MATCH (a:islands)-[r:route_to]->(b:islands)-[r2:route_to]->(c:islands)
    ///         WHERE a._key = 'marineford'
    ///     """)
    ///
    ///     # CASE WHEN
    ///     db.query("""
    ///         SELECT b._key AS name,
    ///                CASE WHEN r._depth = 1 THEN 'direct' ELSE 'indirect' END AS tier
    ///         FROM MATCH (a:characters)-[r:rival]->(b:characters)
    ///     """)
    ///
    ///     # MATCH SHORTEST — returns a row with path fields
    ///     db.query("""
    ///         SELECT a.name AS from_name, b.name AS to_name, r.length AS hops
    ///         FROM MATCH SHORTEST (a)-[r*]->(b)
    ///         WHERE a._key = 'characters/coby' AND b._key = 'characters/sabo'
    ///     """)
    ///
    ///     # Multi-FROM cross-join
    ///     db.query("""
    ///         SELECT a._key AS island, b._key AS character
    ///         FROM islands AS a, MATCH ('crews/straw_hats')-[:includes]->(b)
    ///     """)
    fn query(&self, sql: &str) -> PyResult<Vec<PyHit>> {
        let hits: Vec<Hit> = self.db()?.query(sql).map_err(db_err)?.collect();
        Ok(hits.into_iter().map(to_pyhit).collect())
    }

    /// Execute a mutating statement (INSERT / UPDATE / DELETE / CREATE / DROP).
    ///
    /// Returns the number of rows affected.
    fn execute(&mut self, sql: &str) -> PyResult<usize> {
        self.db_mut()?.execute(sql).map_err(db_err)
    }

    /// Execute a MATCH + WITH pipeline query.
    fn pipeline_query(&self, sql: &str) -> PyResult<Vec<PyHit>> {
        Ok(self.db()?.pipeline_query(sql).map_err(db_err)?
            .into_iter().map(to_pyhit).collect())
    }

    /// Execute a ``SHOW`` introspection statement.
    ///
    /// Supported forms::
    ///
    ///     db.show("SHOW TABLES")                      # [{name, count}, ...]
    ///     db.show("SHOW EDGES")                       # [{from, type, to, count}, ...]
    ///     db.show("SHOW EDGES FROM collection")       # [{from, type, count}, ...]
    ///     db.show("SHOW EDGES FROM col1 TO col2")     # [{from, type, to, count}, ...]
    ///     db.show("SHOW collection")                  # [{field, type, source, ...}, ...]
    ///
    /// Each hit's ``payload`` is a JSON string — use ``json.loads(hit.payload)``.
    fn show(&self, sql: &str) -> PyResult<Vec<PyHit>> {
        Ok(self.db()?.show(sql).map_err(db_err)?
            .into_iter().map(to_pyhit).collect())
    }

    // ── Introspection ─────────────────────────────────────────────────────────

    /// List all collection names in the database.
    fn collection_names(&self) -> PyResult<Vec<String>> {
        Ok(self.db()?.collection_names())
    }

    /// Return distinct ``(from_collection, edge_type, to_collection)`` triples.
    fn edge_schema(&self) -> PyResult<Vec<(String, String, String)>> {
        Ok(self.db()?.edge_schema())
    }

    /// Return distinct edge type names leaving a collection.
    fn edge_types_from_collection(&self, collection: &str) -> PyResult<Vec<String>> {
        Ok(self.db()?.edge_types_from_collection(collection))
    }

    /// DDL string for a collection schema, or ``None``.
    fn schema_ddl(&self, collection: &str) -> PyResult<Option<String>> {
        Ok(self.db()?.schema_ddl(collection))
    }

    /// Total number of nodes.
    fn node_count(&self) -> PyResult<usize> { Ok(self.db()?.node_count()) }

    /// Total number of edges.
    fn edge_count(&self) -> PyResult<usize> { Ok(self.db()?.edge_count()) }

    // ── Persistence ───────────────────────────────────────────────────────────

    /// Flush WAL snapshot and truncate the log.
    fn compact(&mut self) -> PyResult<()> {
        self.db_mut()?.compact().map_err(db_err)
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    fn close(&mut self) { self.inner.take(); }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> { slf }
    fn __exit__(&mut self, _et: PyObject, _ev: PyObject, _tb: PyObject) -> bool {
        self.close();
        false
    }

    fn __repr__(&self) -> String {
        if self.inner.is_some() { "DB(open)".into() } else { "DB(closed)".into() }
    }
}

impl PyDB {
    fn db(&self) -> PyResult<&CoreDB> {
        self.inner.as_ref().ok_or_else(|| PyIOError::new_err("DB is closed"))
    }
    fn db_mut(&mut self) -> PyResult<&mut CoreDB> {
        self.inner.as_mut().ok_or_else(|| PyIOError::new_err("DB is closed"))
    }
}

// ── Module ────────────────────────────────────────────────────────────────────

#[pymodule]
fn sekejap(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDB>()?;
    m.add_class::<PyHit>()?;
    m.add_class::<PyEdgeHit>()?;
    Ok(())
}
