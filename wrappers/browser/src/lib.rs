//! WebAssembly (Wasm) bindings for Sekejap-DB
//!
//! Provides a high-performance graph-first engine for the browser.

use wasm_bindgen::prelude::*;
use ::sekejap::SekejapDB;
use std::path::Path;

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

    /// Execute a SekejapQL JSON query
    /// 
    /// This is the primary interface for JavaScript/TypeScript developers.
    /// It accepts a JSON string and returns a JSON string.
    pub fn query_json(&self, query_json: &str) -> Result<String, JsValue> {
        let result = self.db.query_json(query_json)
            .map_err(|e| JsValue::from_str(&format!("Query execution failed: {}", e)))?;
        
        // Convert Vec<Hit> to JSON manually
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
        
        serde_json::to_string(&hits)
            .map_err(|e| JsValue::from_str(&format!("JSON serialization failed: {}", e)))
    }

    /// Execute a SekejapQL query and return count only
    pub fn query_json_count(&self, query_json: &str) -> Result<usize, JsValue> {
        let result = self.db.query_json_count(query_json)
            .map_err(|e| JsValue::from_str(&format!("Query execution failed: {}", e)))?;
        Ok(result.data)
    }

    /// Explicit flush
    pub fn flush(&self) -> Result<(), JsValue> {
        self.db.flush()
            .map_err(|e| JsValue::from_str(&format!("Flush failed: {}", e)))
    }

    /// Write node with explicit slug
    pub fn put(&self, slug: &str, json: &str) -> Result<u32, JsValue> {
        self.db.nodes().put(slug, json)
            .map_err(|e| JsValue::from_str(&format!("Write failed: {}", e)))
    }

    /// Read node by slug
    pub fn get(&self, slug: &str) -> Option<String> {
        self.db.nodes().get(slug)
    }

    /// Remove a node
    pub fn remove(&self, slug: &str) -> Result<(), JsValue> {
        self.db.nodes().remove(slug)
            .map_err(|e| JsValue::from_str(&format!("Remove failed: {}", e)))
    }

    /// Create an edge between two nodes
    pub fn link(&self, source: &str, target: &str, edge_type: &str, weight: f32) -> Result<(), JsValue> {
        self.db.edges().link(source, target, edge_type, weight)
            .map_err(|e| JsValue::from_str(&format!("Link failed: {}", e)))
    }

    /// Remove an edge
    pub fn unlink(&self, source: &str, target: &str, edge_type: &str) -> Result<(), JsValue> {
        self.db.edges().unlink(source, target, edge_type)
            .map_err(|e| JsValue::from_str(&format!("Unlink failed: {}", e)))
    }

    /// Get count of collection
    pub fn count_collection(&self, collection: &str) -> usize {
        self.db.schema().count(collection)
    }
}