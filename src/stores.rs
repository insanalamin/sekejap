use crate::db::SekejapDB;
use crate::set::Set;
use crate::types::Step;

// ============ NODE STORE ============

/// ZST wrapper for node operations. Compiles to nothing.
pub struct NodeStore<'db>(&'db SekejapDB);

impl<'db> NodeStore<'db> {
    pub(crate) fn new(db: &'db SekejapDB) -> Self {
        Self(db)
    }

    /// Write a node with explicit slug
    pub fn put(&self, slug: &str, json: &str) -> Result<u32, Box<dyn std::error::Error>> {
        self.0.write_internal(slug, json)
    }

    /// Auto-dispatch: parse JSON to determine slug from _id field
    pub fn put_json(&self, json: &str) -> Result<u32, Box<dyn std::error::Error>> {
        self.0.write_json_internal(json)
    }

    /// Batch write (legacy): sequential per-item write with inline indexing
    pub fn put_many(&self, items: &[(&str, &str)]) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
        self.0.write_batch_internal(items)
    }

    /// Fast batch ingest: deferred arena writes → bulk spatial → batch HNSW → single commit
    pub fn ingest(&self, items: &[(&str, &str)]) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
        self.0.ingest_nodes_batch(items)
    }

    /// Raw batch ingest: arena + spatial only (NO HNSW). Call build_hnsw() separately.
    pub fn ingest_raw(&self, items: &[(&str, &str)]) -> Result<(Vec<u32>, Vec<u32>), Box<dyn std::error::Error>> {
        self.0.ingest_nodes_raw(items)
    }

    /// Build HNSW index from all vectors in arena. Call after ingest_raw().
    pub fn build_hnsw(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.0.build_hnsw_batch()
    }

    /// Tombstone a node (set flags = 0)
    pub fn remove(&self, slug: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.0.delete_internal(slug)
    }

    /// Read raw JSON for a slug
    pub fn get(&self, slug: &str) -> Option<String> {
        self.0.read_internal(slug)
    }

    // --- Query entry points (return Set<'db>) ---

    /// Start a query from a single node
    pub fn one(&self, slug: &str) -> Set<'db> {
        let (_, hash) = SekejapDB::parse_entity_id(slug);
        Set::new(self.0, Step::One(hash))
    }

    /// Start a query from multiple nodes
    pub fn many(&self, slugs: &[&str]) -> Set<'db> {
        let hashes: Vec<u64> = slugs.iter()
            .map(|s| SekejapDB::parse_entity_id(s).1)
            .collect();
        Set::new(self.0, Step::Many(hashes))
    }

    /// Start a query from all nodes in a collection
    pub fn collection(&self, name: &str) -> Set<'db> {
        let hash = seahash::hash(name.as_bytes());
        Set::new(self.0, Step::Collection(hash))
    }

    /// Start a query from all nodes
    pub fn all(&self) -> Set<'db> {
        Set::new(self.0, Step::All)
    }
}

// ============ EDGE STORE ============

/// ZST wrapper for edge operations. Compiles to nothing.
pub struct EdgeStore<'db>(&'db SekejapDB);

impl<'db> EdgeStore<'db> {
    pub(crate) fn new(db: &'db SekejapDB) -> Self {
        Self(db)
    }

    /// Create an edge between two nodes
    pub fn link(&self, source: &str, target: &str, edge_type: &str, weight: f32) -> Result<(), Box<dyn std::error::Error>> {
        self.0.add_edge_internal(source, target, weight, edge_type)?;
        Ok(())
    }

    /// Batch create edges (legacy: per-item commit)
    pub fn link_many(&self, edges: &[(&str, &str, &str, f32)]) -> Result<(), Box<dyn std::error::Error>> {
        for &(src, dst, etype, weight) in edges {
            self.0.add_edge_internal(src, dst, weight, etype)?;
        }
        Ok(())
    }

    /// Fast batch ingest edges: deferred writes → single commit
    pub fn ingest(&self, edges: &[(&str, &str, &str, f32)]) -> Result<(), Box<dyn std::error::Error>> {
        self.0.ingest_edges_batch(edges)
    }

    /// Create an edge with arbitrary JSON metadata.
    /// Metadata ≤32 bytes is stored inline (zero extra I/O).
    /// Metadata >32 bytes is stored in the blob arena.
    pub fn link_meta(
        &self,
        source: &str,
        target: &str,
        edge_type: &str,
        weight: f32,
        meta_json: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.0.add_edge_meta_internal(source, target, weight, edge_type, meta_json)?;
        Ok(())
    }

    /// Remove an edge (tombstone)
    pub fn unlink(&self, source: &str, target: &str, edge_type: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.0.delete_edge_internal(source, target, edge_type)?;
        Ok(())
    }
}

// ============ SCHEMA STORE ============

/// ZST wrapper for schema/collection operations.
pub struct SchemaStore<'db>(&'db SekejapDB);

impl<'db> SchemaStore<'db> {
    pub(crate) fn new(db: &'db SekejapDB) -> Self {
        Self(db)
    }

    /// Define a collection schema (hot fields, vector config, etc.)
    pub fn define(&self, name: &str, json: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.0.define_collection_internal(name, json)?;
        Ok(())
    }

    /// Get collection count (O(1) via atomic counter)
    pub fn count(&self, collection: &str) -> usize {
        self.0.count_collection_internal(collection)
    }
}
