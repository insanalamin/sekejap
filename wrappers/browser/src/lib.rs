//! WebAssembly (Wasm) bindings for Sekejap-DB
//! Canonical high-level interface:
//! - `query(json)`
//! - `query_count(json)`
//! - `mutate(json)`
//!
//! Uses the same JSON query/mutate contract as Rust core.
//!
//! Provides a high-performance graph-first engine for the browser.

use ::sekejap::SekejapDB;
use std::path::Path;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct WasmSekejapDB {
    db: SekejapDB,
}

#[wasm_bindgen]
impl WasmSekejapDB {
    /// Create a new database instance (Browser version)
    ///
    /// Note: In the browser, this will eventually bridge to IndexedDB.
    /// For the initial prototype, it uses an in-memory/temp path.
    #[wasm_bindgen(constructor)]
    pub fn new(path: &str) -> Result<WasmSekejapDB, JsValue> {
        // Initialize panic hook for better debugging in console
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();

        // Default capacity of 1M nodes
        let db = SekejapDB::new(Path::new(path), 1_000_000)
            .map_err(|e| JsValue::from_str(&format!("Failed to open DB: {}", e)))?;

        Ok(WasmSekejapDB { db })
    }

    /// Execute a JSON query pipeline.
    pub fn query(&self, query_json: &str) -> Result<String, JsValue> {
        let result = self
            .db
            .query(query_json)
            .map_err(|e| JsValue::from_str(&format!("Query execution failed: {}", e)))?;

        // Convert Vec<Hit> to JSON manually
        let hits: Vec<serde_json::Value> = result
            .data
            .into_iter()
            .map(|h| {
                serde_json::json!({
                    "idx": h.idx,
                    "slug_hash": h.slug_hash,
                    "collection_hash": h.collection_hash,
                    "payload": h.payload,
                    "lat": h.lat,
                    "lon": h.lon,
                    "score": h.score
                })
            })
            .collect();

        serde_json::to_string(&hits)
            .map_err(|e| JsValue::from_str(&format!("JSON serialization failed: {}", e)))
    }

    /// Execute a JSON query pipeline and return count only.
    pub fn query_count(&self, query_json: &str) -> Result<usize, JsValue> {
        let result = self
            .db
            .query_count(query_json)
            .map_err(|e| JsValue::from_str(&format!("Query execution failed: {}", e)))?;
        Ok(result.data)
    }

    /// Execute a JSON mutation pipeline.
    pub fn mutate(&self, mutation_json: &str) -> Result<String, JsValue> {
        let result = self
            .db
            .mutate(mutation_json)
            .map_err(|e| JsValue::from_str(&format!("Mutation execution failed: {}", e)))?;
        serde_json::to_string(&result)
            .map_err(|e| JsValue::from_str(&format!("JSON serialization failed: {}", e)))
    }

    /// Backward-compatible alias.
    pub fn query_json(&self, query_json: &str) -> Result<String, JsValue> {
        self.query(query_json)
    }

    /// Backward-compatible alias.
    pub fn query_json_count(&self, query_json: &str) -> Result<usize, JsValue> {
        self.query_count(query_json)
    }

    /// Explicit flush
    pub fn flush(&self) -> Result<(), JsValue> {
        self.db
            .flush()
            .map_err(|e| JsValue::from_str(&format!("Flush failed: {}", e)))
    }

    /// Write node with explicit slug
    pub fn put(&self, slug: &str, json: &str) -> Result<u32, JsValue> {
        self.db
            .nodes()
            .put(slug, json)
            .map_err(|e| JsValue::from_str(&format!("Write failed: {}", e)))
    }

    /// Read node by slug
    pub fn get(&self, slug: &str) -> Option<String> {
        self.db.nodes().get(slug)
    }

    /// Remove a node
    pub fn remove(&self, slug: &str) -> Result<(), JsValue> {
        self.db
            .nodes()
            .remove(slug)
            .map_err(|e| JsValue::from_str(&format!("Remove failed: {}", e)))
    }

    /// Create an edge between two nodes
    pub fn link(
        &self,
        source: &str,
        target: &str,
        edge_type: &str,
        weight: f32,
    ) -> Result<(), JsValue> {
        self.db
            .edges()
            .link(source, target, edge_type, weight)
            .map_err(|e| JsValue::from_str(&format!("Link failed: {}", e)))
    }

    /// Remove an edge
    pub fn unlink(&self, source: &str, target: &str, edge_type: &str) -> Result<(), JsValue> {
        self.db
            .edges()
            .unlink(source, target, edge_type)
            .map_err(|e| JsValue::from_str(&format!("Unlink failed: {}", e)))
    }

    /// Get count of collection
    pub fn count_collection(&self, collection: &str) -> usize {
        self.db.schema().count(collection)
    }
}
