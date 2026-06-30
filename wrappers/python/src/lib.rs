//! Python bindings for sekejap via PyO3.

use pyo3::exceptions::{PyIOError, PyTypeError};
use pyo3::prelude::*;
use serde_json::Value;

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

/// Convert a Python list of values to `Vec<serde_json::Value>`.
fn py_list_to_values(py: Python<'_>, objs: Vec<PyObject>) -> PyResult<Vec<Value>> {
    objs.into_iter().map(|o| {
        // bool must come before i64 (Python bool is a subclass of int)
        if let Ok(b) = o.extract::<bool>(py)   { return Ok(Value::Bool(b)); }
        if let Ok(i) = o.extract::<i64>(py)    { return Ok(serde_json::json!(i)); }
        if let Ok(f) = o.extract::<f64>(py)    { return Ok(serde_json::json!(f)); }
        if let Ok(s) = o.extract::<String>(py) { return Ok(Value::String(s)); }
        if o.is_none(py)                        { return Ok(Value::Null); }
        // Vec<f32> for vector params
        if let Ok(v) = o.extract::<Vec<f32>>(py) {
            return Ok(Value::Array(v.into_iter().map(|f| serde_json::json!(f as f64)).collect()));
        }
        Err(PyTypeError::new_err(
            "parameter must be str, int, float, bool, None, or list[float]"
        ))
    }).collect()
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

    /// Open a read-only database backed by S3.
    ///
    /// Payloads are fetched on demand via S3 ``GET_RANGE`` and cached in an
    /// LRU cache bounded by ``cache_budget_bytes``.
    ///
    /// If ``cache_dir`` is provided, evicted blocks are spilled to disk
    /// (bounded by ``cache_budget_bytes``). Otherwise blocks are cached in
    /// RAM only.
    ///
    /// Args:
    ///     url: S3 URL, e.g. ``"s3://bucket/prefix"``.
    ///     access_key_id: AWS access key.
    ///     secret_access_key: AWS secret key.
    ///     region: AWS region, e.g. ``"ap-southeast-1"``.
    ///     cache_budget_bytes: Cache size in bytes (RAM or disk depending on ``cache_dir``).
    ///     cache_dir (str, optional): Directory for disk-backed block cache.
    ///     endpoint (str, optional): Custom S3 endpoint (for MinIO, R2, etc.).
    ///     allow_http (bool): Allow plain HTTP connections. Default ``False``.
    ///
    /// Example::
    ///
    ///     # RAM cache (256 MB)
    ///     db = DB.open_s3("s3://bucket/data", "AKID", "secret", "ap-southeast-1",
    ///                     cache_budget_bytes=256 * 1024 * 1024)
    ///
    ///     # Disk cache (10 GB)
    ///     db = DB.open_s3("s3://bucket/data", "AKID", "secret", "ap-southeast-1",
    ///                     cache_budget_bytes=10 * 1024**3, cache_dir="/tmp/cache")
    #[cfg(feature = "s3")]
    #[staticmethod]
    #[pyo3(signature = (url, access_key_id, secret_access_key, region, cache_budget_bytes, cache_dir=None, endpoint=None, allow_http=false))]
    fn open_s3(
        url: &str,
        access_key_id: &str,
        secret_access_key: &str,
        region: &str,
        cache_budget_bytes: u64,
        cache_dir: Option<&str>,
        endpoint: Option<&str>,
        allow_http: bool,
    ) -> PyResult<Self> {
        use ::sekejap::engine::remote::{S3Credentials, RemoteSync};
        use ::sekejap::engine::cache::CacheBudget;

        let mut creds = S3Credentials::new(access_key_id, secret_access_key, region);
        if let Some(ep) = endpoint {
            creds = creds.endpoint(ep);
        }
        if allow_http {
            creds = creds.allow_http(true);
        }

        let remote = RemoteSync::from_url(url, &creds).map_err(db_err)?;
        let budget = CacheBudget::new(cache_budget_bytes);

        let inner = CoreDB::open_s3(
            &remote,
            budget,
            cache_dir.map(std::path::Path::new),
        ).map_err(db_err)?;

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
    ///     # Multi-stage graph query with WITH chaining
    ///     db.query("""
    ///         SELECT c.name AS city, COUNT(*) AS friends
    ///         FROM MATCH (a:users)-[:knows*1..3]->(b:users)
    ///         WHERE a._key = 'alice'
    ///         WITH b
    ///         MATCH (b)-[:lives_in]->(c:cities)
    ///         WHERE c.population > 100000
    ///         GROUP BY c.name ORDER BY friends DESC LIMIT 10
    ///     """)
    ///
    ///     # MATCH...RETURN (Cypher-style syntax, also via query())
    ///     db.query("""
    ///         MATCH (a:characters)-[:rival]->(b:characters)
    ///         RETURN a._key AS name, b.bounty AS rival_bounty
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
    ///
    /// Optionally pass ``params`` for ``$1``, ``$2``, … bindings.
    ///
    /// Example::
    ///
    ///     db.query("SELECT * FROM users WHERE name = $1 AND age > $2", ["Alice", 25])
    #[pyo3(signature = (sql, params=None))]
    fn query(&self, py: Python<'_>, sql: &str, params: Option<Vec<PyObject>>) -> PyResult<Vec<PyHit>> {
        let hits: Vec<Hit> = if let Some(p) = params {
            let vals = py_list_to_values(py, p)?;
            self.db()?.query_params(sql, &vals).map_err(db_err)?.collect()
        } else {
            self.db()?.query(sql).map_err(db_err)?.collect()
        };
        Ok(hits.into_iter().map(to_pyhit).collect())
    }

    /// Execute a mutating statement (INSERT / UPDATE / DELETE / CREATE / DROP).
    ///
    /// Returns the number of rows affected. Optionally pass ``params`` for ``$1``, ``$2``, … bindings.
    ///
    /// Example::
    ///
    ///     db.execute("INSERT INTO users (_key, name, age) VALUES ($1, $2, $3)", ["u1", "Bob", 30])
    #[pyo3(signature = (sql, params=None))]
    fn execute(&mut self, py: Python<'_>, sql: &str, params: Option<Vec<PyObject>>) -> PyResult<usize> {
        if let Some(p) = params {
            let vals = py_list_to_values(py, p)?;
            self.db_mut()?.execute_params(sql, &vals).map_err(db_err)
        } else {
            self.db_mut()?.execute(sql).map_err(db_err)
        }
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
