use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashMap;

use super::blob::BlobPtr;
use super::payload::Payload;
use super::collection::EntityId;
use super::vector::{VectorChannel, VectorStore};
use super::geo::{GeoFeature, GeoStore};
use super::decay::Props;

/// Unique identifier for nodes (128-bit)
pub type NodeId = u128;

/// Hash of slug for fast indexing
pub type SlugHash = u64;

/// Geohash for spatial indexing
pub type SpatialHash = u64;

/// MVCC epoch anchor
pub type Epoch = u64;

/// Tombstone metadata for deleted nodes
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tombstone {
    pub deleted_at: Epoch,      // When node was deleted
    pub reason: Option<String>,  // Optional reason for deletion (e.g., "user_deleted", "policy_cleanup")
}

impl Tombstone {
    pub fn new(reason: Option<String>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        Self {
            deleted_at: now,
            reason,
        }
    }
}

/// Head pointer for MVCC - maps slug to current revision
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeadPointer {
    pub node_id: NodeId,
    pub rev: u64,
}

impl HeadPointer {
    pub fn new(node_id: NodeId, rev: u64) -> Self {
        Self { node_id, rev }
    }
}

/// The hot index entry stored in B+Tree (Tier 2)
/// Minimal footprint for fast lookups
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeHeader {
    pub node_id: NodeId,
    pub rev: u64,              // Revision number for MVCC
    pub deleted: bool,          // Tombstone flag for deletions
    pub tombstone: Option<Tombstone>,  // Enhanced tombstone metadata
    pub slug_hash: SlugHash,
    pub spatial_hash: SpatialHash,
    pub payload_ptr: BlobPtr,
    pub vector_ptr: Option<BlobPtr>,  // Vector embedding (versioned with node)
    pub epoch_created: Epoch,
    pub entity_id: Option<EntityId>,  // Canonical _id for DATA_FORMAT.md
}

impl NodeHeader {
    pub fn new(
        node_id: NodeId,
        slug_hash: SlugHash,
        spatial_hash: SpatialHash,
        payload_ptr: BlobPtr,
        epoch_created: Epoch,
    ) -> Self {
        Self {
            node_id,
            rev: 0,
            deleted: false,
            tombstone: None,
            slug_hash,
            spatial_hash,
            payload_ptr,
            vector_ptr: None,
            epoch_created,
            entity_id: None,
        }
    }

    pub fn new_with_vector(
        node_id: NodeId,
        slug_hash: SlugHash,
        spatial_hash: SpatialHash,
        payload_ptr: BlobPtr,
        vector_ptr: BlobPtr,
        epoch_created: Epoch,
    ) -> Self {
        Self {
            node_id,
            rev: 0,
            deleted: false,
            tombstone: None,
            slug_hash,
            spatial_hash,
            payload_ptr,
            vector_ptr: Some(vector_ptr),
            epoch_created,
            entity_id: None,
        }
    }

    /// Create a new version of this node
    pub fn new_version(&self, payload_ptr: Option<BlobPtr>) -> Self {
        Self {
            node_id: self.node_id,
            rev: self.rev + 1,
            deleted: false,
            tombstone: None,
            slug_hash: self.slug_hash,
            spatial_hash: self.spatial_hash,
            payload_ptr: payload_ptr.unwrap_or_else(|| self.payload_ptr),
            vector_ptr: self.vector_ptr,
            epoch_created: self.epoch_created,
            entity_id: self.entity_id.clone(),
        }
    }

    /// Create a new version with updated vector
    pub fn new_version_with_vector(&self, payload_ptr: Option<BlobPtr>, vector_ptr: BlobPtr) -> Self {
        Self {
            node_id: self.node_id,
            rev: self.rev + 1,
            deleted: false,
            tombstone: None,
            slug_hash: self.slug_hash,
            spatial_hash: self.spatial_hash,
            payload_ptr: payload_ptr.unwrap_or_else(|| self.payload_ptr),
            vector_ptr: Some(vector_ptr),
            epoch_created: self.epoch_created,
            entity_id: self.entity_id.clone(),
        }
    }

    /// Create a tombstone version of this node
    pub fn as_tombstone(&self, reason: Option<String>) -> Self {
        Self {
            node_id: self.node_id,
            rev: self.rev + 1,
            deleted: true,
            tombstone: Some(Tombstone::new(reason)),
            slug_hash: self.slug_hash,
            spatial_hash: self.spatial_hash,
            payload_ptr: self.payload_ptr,
            vector_ptr: self.vector_ptr,
            epoch_created: self.epoch_created,
            entity_id: self.entity_id.clone(),
        }
    }
}

impl fmt::Display for NodeHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "NodeHeader(id={}, slug_hash={}, spatial_hash={}, epoch={})",
            self.node_id, self.slug_hash, self.spatial_hash, self.epoch_created
        )
    }
}

/// The payload data stored in BlobStore - DATA_FORMAT.md compliant
///
/// Contains actual node data with multi-modal support following
/// the new specification with _id, vectors, geo, and props fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePayload {
    /// Canonical identity "collection/key" (DATA_FORMAT.md)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub _id: Option<EntityId>,
    
    /// Convenience key (optional, derived from _id)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub _key: Option<String>,
    
    /// Title field (standard field)
    #[serde(default)]
    pub title: String,
    
    /// Excerpt/summary (standard field)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub excerpt: Option<String>,
    
    /// Full content (standard field)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    
    /// Named vector channels (DATA_FORMAT.md - Structured vectors)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vectors: Option<VectorStore>,
    
    /// Named geo features (DATA_FORMAT.md - Structured geo)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub geo: Option<GeoStore>,
    
    /// All custom properties (DATA_FORMAT.md - props wrapper)
    #[serde(default)]
    pub props: Props,
    
    /// Internal timestamp (when node was created)
    #[serde(default)]
    pub _timestamp: u64,
    
    /// Legacy fields (for backward compatibility during migration)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coordinates: Option<Coordinates>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_vectors: Option<HashMap<String, Vec<f32>>>,
}

impl Payload for NodePayload {
    fn get_type(&self) -> &str {
        "node"
    }
    
    fn get_title(&self) -> &str {
        &self.title
    }
    
    fn get_timestamp(&self) -> u64 {
        self._timestamp
    }
    
    fn get_metadata(&self, key: &str) -> Option<&serde_json::Value> {
        self.props.get(key)
    }
    
    fn get_all_metadata(&self) -> serde_json::Value {
        serde_json::Value::Object(self.props.inner().clone())
    }
}

impl NodePayload {
    /// Create a new node payload (DATA_FORMAT.md compliant)
    pub fn new(title: impl Into<String>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        Self {
            _id: None,
            _key: None,
            title: title.into(),
            excerpt: None,
            content: None,
            vectors: None,
            geo: None,
            props: Props::new(),
            _timestamp: now,
            slug: None,
            metadata: None,
            coordinates: None,
            legacy_vectors: None,
        }
    }
    
    /// Set the canonical _id
    pub fn with_id(mut self, entity_id: EntityId) -> Self {
        self._id = Some(entity_id.clone());
        self._key = Some(entity_id.key().to_string());
        self
    }
    
    /// Set excerpt
    pub fn with_excerpt(mut self, excerpt: impl Into<String>) -> Self {
        self.excerpt = Some(excerpt.into());
        self
    }
    
    /// Set content
    pub fn with_content(mut self, content: impl Into<String>) -> Self {
        self.content = Some(content.into());
        self
    }
    
    /// Set vectors
    pub fn with_vectors(mut self, vectors: VectorStore) -> Self {
        self.vectors = Some(vectors);
        self
    }
    
    /// Set geo
    pub fn with_geo(mut self, geo: GeoStore) -> Self {
        self.geo = Some(geo);
        self
    }
    
    /// Add a property
    pub fn with_prop(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.props.set(key, value);
        self
    }
    
    /// Migrate from legacy format (backward compatibility)
    pub fn migrate_from_legacy(&mut self) {
        if self._id.is_none() && self.slug.is_some() {
            // Try to parse slug as entity_id
            if let Ok(entity_id) = EntityId::parse(self.slug.as_ref().unwrap()) {
                self._id = Some(entity_id.clone());
                self._key = Some(entity_id.key().to_string());
            }
        }
        
        if self.props.is_empty() && self.metadata.is_some() {
            // Migrate metadata to props
            let metadata = self.metadata.take().unwrap();
            if let serde_json::Value::Object(map) = metadata {
                for (k, v) in map {
                    self.props.set(k, v);
                }
            }
        }
        
        // Migrate legacy vectors
        if self.vectors.is_none() && self.legacy_vectors.is_some() {
            let legacy = self.legacy_vectors.take().unwrap();
            let mut store = VectorStore::new();
            for (name, data) in legacy {
                store.insert(name, VectorChannel::dense("legacy", data.len(), data));
            }
            self.vectors = Some(store);
        }
    }
    
    /// Get entity ID or derive from slug
    pub fn entity_id(&self) -> Option<&EntityId> {
        self._id.as_ref()
    }
    
    /// Get effective ID string for indexing
    pub fn effective_id(&self) -> Option<String> {
        self._id.as_ref().map(|id| id.to_string())
    }
    
    /// Get tags from props
    pub fn tags(&self) -> Vec<&str> {
        self.props.tags()
    }
    
    /// Get first geo feature for spatial indexing
    pub fn first_geo(&self) -> Option<&GeoFeature> {
        self.geo.as_ref().and_then(|g| g.iter().next().map(|(_, f)| f))
    }
    
    /// Get first vector channel for vector operations
    pub fn first_vector(&self) -> Option<&VectorChannel> {
        self.vectors.as_ref().and_then(|v| v.iter().next().map(|(_, c)| c))
    }
}

/// GPS coordinates for spatial indexing (legacy, for backward compatibility)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Coordinates {
    pub latitude: f64,
    pub longitude: f64,
}

/// Spatial query result with distance
#[derive(Debug, Clone)]
pub struct SpatialResult {
    pub node_id: NodeId,
    pub distance_km: f64,
}

impl Coordinates {
    pub fn new(latitude: f64, longitude: f64) -> Self {
        Self {
            latitude,
            longitude,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_header_creation() {
        let header = NodeHeader::new(
            123456789012345678901234567890123456789u128,
            999,
            888,
            BlobPtr::new(0, 100, 200),
            1700000000000,
        );
        
        assert_eq!(header.node_id, 123456789012345678901234567890123456789u128);
        assert_eq!(header.slug_hash, 999);
        assert_eq!(header.spatial_hash, 888);
    }

    #[test]
    fn test_coordinates() {
        let coords = Coordinates::new(-6.2088, 106.8456);
        assert_eq!(coords.latitude, -6.2088);
        assert_eq!(coords.longitude, 106.8456);
    }
    
    #[test]
    fn test_node_payload_new_format() {
        let payload = NodePayload::new("Test Title")
            .with_id(EntityId::new("news", "test-2026"))
            .with_excerpt("Test excerpt")
            .with_content("Full content here")
            .with_prop("tags", serde_json::json!(["news", "test"]))
            .with_prop("author", serde_json::json!("agent-001"));
        
        assert_eq!(payload.title, "Test Title");
        assert_eq!(payload.excerpt, Some("Test excerpt".to_string()));
        assert_eq!(payload.content, Some("Full content here".to_string()));
        assert_eq!(payload.props.get_str("author").unwrap(), "agent-001");
        
        let entity_id = payload._id.unwrap();
        assert_eq!(entity_id.as_str(), "news/test-2026");
        assert_eq!(entity_id.collection(), "news");
        assert_eq!(entity_id.key(), "test-2026");
    }
    
    #[test]
    fn test_node_payload_migration() {
        let mut payload = NodePayload::new("Legacy");
        payload.slug = Some("news/legacy-2026".to_string());
        payload.metadata = Some(serde_json::json!({
            "tags": ["legacy"],
            "author": "old-agent"
        }));
        
        payload.migrate_from_legacy();
        
        let entity_id = payload._id.unwrap();
        assert_eq!(entity_id.as_str(), "news/legacy-2026");
        assert_eq!(payload.props.get_str("author").unwrap(), "old-agent");
        assert_eq!(payload.props.tags(), vec!["legacy"]);
    }
}
