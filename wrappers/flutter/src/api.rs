//! Public API surface bridged to Dart by flutter_rust_bridge.

use flutter_rust_bridge::frb;
use sekejap::CoreDB;
use serde_json::Value;
use std::sync::Mutex;

/// Required frb init — called once at app startup from Dart.
#[frb(init)]
pub fn init_app() {
    flutter_rust_bridge::setup_default_user_utils();
}

// ── Opaque handle ──────────────────────────────────────────────────────────────

/// An open sekejap database instance.
///
/// Created via [`db_open`] or [`db_new`]. Freed automatically when the Dart
/// object is garbage-collected.
#[frb(opaque)]
pub struct SekejapDb(Mutex<CoreDB>);

// ── Lifecycle ──────────────────────────────────────────────────────────────────

/// Open a persistent database at `path` (a directory on the filesystem).
pub fn db_open(path: String) -> Result<SekejapDb, String> {
    CoreDB::open(&path)
        .map(|db| SekejapDb(Mutex::new(db)))
        .map_err(|e| e.to_string())
}

/// Create an in-memory (non-persistent) database.
pub fn db_new() -> SekejapDb {
    SekejapDb(Mutex::new(CoreDB::new()))
}

// ── Mutations ──────────────────────────────────────────────────────────────────

/// Run a DDL/DML statement (CREATE, INSERT, UPDATE, DELETE).
/// Returns the number of rows affected.
pub fn db_execute(db: &SekejapDb, sql: String) -> Result<usize, String> {
    db.0.lock().unwrap()
        .execute(&sql)
        .map_err(|e| format!("{e:?}"))
}

/// Store a node. `json` must be a valid JSON object containing `_collection`.
/// Returns the internal storage id of the written node.
pub fn db_put(db: &SekejapDb, slug: String, json: String) -> Result<u64, String> {
    db.0.lock().unwrap()
        .put(&slug, &json)
        .map_err(|e| e.to_string())
}

/// Remove a node (and its associated edges).
pub fn db_remove(db: &SekejapDb, slug: String) {
    db.0.lock().unwrap().remove(&slug);
}

/// Create a directed edge: `from -[edge_type]-> to`.
pub fn db_link(db: &SekejapDb, from: String, to: String, edge_type: String, strength: f32) {
    db.0.lock().unwrap().link(&from, &to, &edge_type, strength);
}

/// Remove a directed edge between two nodes.
pub fn db_unlink(db: &SekejapDb, from: String, to: String, edge_type: String) {
    db.0.lock().unwrap().unlink(&from, &to, &edge_type);
}

// ── Queries ────────────────────────────────────────────────────────────────────

/// Run a SELECT or MATCH query.
/// Returns a JSON array: `[{"slug":"...","payload":{...}}, ...]`
pub fn db_query(db: &SekejapDb, sql: String) -> Result<String, String> {
    let hits = db.0.lock().unwrap()
        .query(&sql)
        .map_err(|e| format!("{e:?}"))?
        .collect();
    let rows: Vec<_> = hits.into_iter().map(|h| {
        serde_json::json!({ "slug": h.slug, "payload": h.payload })
    }).collect();
    Ok(serde_json::to_string(&rows).unwrap())
}

/// Run a SELECT or MATCH query with parameter bindings ($1, $2, …).
/// `params_json` is a JSON array of values, e.g. `'["Alice", 25]'`.
/// Returns a JSON array: `[{"slug":"...","payload":{...}}, ...]`
pub fn db_query_params(db: &SekejapDb, sql: String, params_json: String) -> Result<String, String> {
    let params: Vec<Value> = serde_json::from_str(&params_json)
        .map_err(|e| format!("invalid params JSON: {e}"))?;
    let hits = db.0.lock().unwrap()
        .query_params(&sql, &params)
        .map_err(|e| format!("{e:?}"))?
        .collect();
    let rows: Vec<_> = hits.into_iter().map(|h| {
        serde_json::json!({ "slug": h.slug, "payload": h.payload })
    }).collect();
    Ok(serde_json::to_string(&rows).unwrap())
}

/// Run a DDL/DML statement with parameter bindings ($1, $2, …).
/// `params_json` is a JSON array of values.
/// Returns the number of rows affected.
pub fn db_execute_params(db: &SekejapDb, sql: String, params_json: String) -> Result<usize, String> {
    let params: Vec<Value> = serde_json::from_str(&params_json)
        .map_err(|e| format!("invalid params JSON: {e}"))?;
    db.0.lock().unwrap()
        .execute_params(&sql, &params)
        .map_err(|e| format!("{e:?}"))
}

/// Get a single node by slug. Returns its JSON payload string, or null.
pub fn db_get(db: &SekejapDb, slug: String) -> Option<String> {
    db.0.lock().unwrap().get(&slug)
}

/// Check whether a node with the given slug exists.
pub fn db_contains(db: &SekejapDb, slug: String) -> bool {
    db.0.lock().unwrap().contains(&slug)
}

/// Run a SHOW statement. Returns a JSON array.
pub fn db_show(db: &SekejapDb, sql: String) -> Result<String, String> {
    let hits = db.0.lock().unwrap()
        .show(&sql)
        .map_err(|e| format!("{e:?}"))?;
    let rows: Vec<_> = hits.into_iter().map(|h| {
        serde_json::json!({ "slug": h.slug, "payload": h.payload })
    }).collect();
    Ok(serde_json::to_string(&rows).unwrap())
}

// ── Maintenance ────────────────────────────────────────────────────────────────

/// Flush WAL and write a full snapshot.
pub fn db_compact(db: &SekejapDb) -> Result<(), String> {
    db.0.lock().unwrap().compact().map_err(|e| e.to_string())
}

/// Flush WAL to disk without full compaction.
pub fn db_sync(db: &SekejapDb) -> Result<(), String> {
    db.0.lock().unwrap().sync().map_err(|e| e.to_string())
}
