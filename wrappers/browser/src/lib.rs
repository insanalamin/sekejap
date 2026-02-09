//! WebAssembly (Wasm) bindings for Sekejap-DB
//!
//! Provides a high-performance graph-first engine for the browser.

use wasm_bindgen::prelude::*;
use ::sekejap::SekejapDB;
use ::sekejap::sekejapql::SekejapQL;
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

        let db = SekejapDB::new(Path::new(path))
            .map_err(|e| JsValue::from_str(&format!("Failed to open DB: {}", e)))?;

        Ok(WasmSekejapDB { db })
    }

    /// Execute a SekejapQL query (JSON object)
    /// 
    /// This is the primary interface for JavaScript/TypeScript developers.
    /// It accepts a JS Object and returns a JS Object.
    pub fn query(&self, query_obj: JsValue) -> Result<JsValue, JsValue> {
        // 1. Convert JS Object to Rust serde_json string
        let query_json: String = serde_json::to_string(&serde_wasm_bindgen::from_value::<serde_json::Value>(query_obj)?)
            .map_err(|e| JsValue::from_str(&format!("Invalid query format: {}", e)))?;

        // 2. Execute via the logic core
        let engine = SekejapQL::new(&self.db);
        let result = engine.query(&query_json)
            .map_err(|e| JsValue::from_str(&format!("Query execution failed: {}", e)))?;

        // 3. Convert result back to JS Object
        Ok(serde_wasm_bindgen::to_value(&result)?)
    }

    /// Explicit flush
    pub fn flush(&mut self) -> Result<usize, JsValue> {
        self.db.flush()
            .map_err(|e| JsValue::from_str(&format!("Flush failed: {}", e)))
    }

    /// Write raw data (convenience bridge)
    pub fn write(&mut self, slug: &str, data: &str) -> Result<String, JsValue> {
        let node_id = self.db.write(slug, data)
            .map_err(|e| JsValue::from_str(&format!("Write failed: {}", e)))?;
        Ok(format!("{}", node_id))
    }

    /// Read raw data
    pub fn read(&self, slug: &str) -> JsValue {
        match self.db.read(slug) {
            Ok(Some(data)) => JsValue::from_str(&data),
            _ => JsValue::NULL,
        }
    }
}
