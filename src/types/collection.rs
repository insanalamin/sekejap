//! Collection and Identity types for Sekejap-DB
//!
//! Provides canonical identity system following ArangoDB-style:
//! - `_id`: "collection/key" (canonical, for all references)
//! - `_key`: key (convenience, optional)
//! - `collection`: derived from `_id`
//!
//! # Example
//!
//! ```rust
//! use hsdl_sekejap::types::{EntityId, CollectionId, parse_entity_id, Collection, CollectionSchema};
//!
//! let entity_id = EntityId::new("news", "flood-in-gedebage-2026");
//! assert_eq!(entity_id.as_str(), "news/flood-in-gedebage-2026");
//! assert_eq!(entity_id.collection(), "news");
//! assert_eq!(entity_id.key(), "flood-in-gedebage-2026");
//!
//! // Create collection with schema
//! let mut collection = Collection::new("news".to_string());
//! collection.set_schema(CollectionSchema::new());
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

/// Collection identifier (e.g., "news", "places", "terms")
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CollectionId(pub String);

impl CollectionId {
    /// Create a new collection ID
    pub fn new<S: Into<String>>(collection: S) -> Self {
        Self(collection.into())
    }

    /// Get the collection name as string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CollectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for CollectionId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for CollectionId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

/// Entity identifier in canonical "collection/key" format
///
/// This is the primary identifier used throughout the database.
/// All edge references (`_from`, `_to`) use this format.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EntityId {
    collection: CollectionId,
    key: String,
    #[serde(skip)]
    cached_string: std::sync::OnceLock<String>,
}

impl EntityId {
    /// Create a new entity ID from collection and key
    pub fn new<C: Into<CollectionId>, K: Into<String>>(collection: C, key: K) -> Self {
        Self {
            collection: collection.into(),
            key: key.into(),
            cached_string: std::sync::OnceLock::new(),
        }
    }

    /// Parse from string (e.g., "news/flood-2026")
    pub fn parse(s: &str) -> Result<Self, ParseEntityIdError> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 2 {
            return Err(ParseEntityIdError::InvalidFormat(s.to_string()));
        }
        Ok(Self {
            collection: CollectionId::new(parts[0].to_string()),
            key: parts[1].to_string(),
            cached_string: std::sync::OnceLock::new(),
        })
    }

    /// Get the full entity ID as string ("collection/key")
    /// FIXED: Now correctly returns full format
    pub fn as_str(&self) -> &str {
        // Lazy initialization of cached string
        self.cached_string.get_or_init(|| format!("{}/{}", self.collection, self.key))
    }

    /// Get the collection component
    pub fn collection(&self) -> &str {
        self.collection.as_str()
    }

    /// Get the key component
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the collection as CollectionId
    pub fn collection_id(&self) -> CollectionId {
        self.collection.clone()
    }

    /// Create a reference to this entity (for edge _from/_to)
    pub fn to_reference(&self) -> String {
        format!("{}/{}", self.collection, self.key)
    }
}

impl fmt::Display for EntityId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.collection, self.key)
    }
}

impl PartialEq for EntityId {
    fn eq(&self, other: &Self) -> bool {
        self.collection == other.collection && self.key == other.key
    }
}

impl Eq for EntityId {}

impl Hash for EntityId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.collection.hash(state);
        self.key.hash(state);
    }
}

impl FromStr for EntityId {
    type Err = ParseEntityIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl From<EntityId> for String {
    fn from(entity_id: EntityId) -> Self {
        format!("{}/{}", entity_id.collection, entity_id.key)
    }
}

impl TryFrom<String> for EntityId {
    type Error = ParseEntityIdError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::parse(&s)
    }
}

/// Error parsing entity ID
#[derive(Debug, thiserror::Error)]
pub enum ParseEntityIdError {
    #[error("invalid entity ID format: '{0}' (expected 'collection/key')")]
    InvalidFormat(String),
}

/// Parse helper that returns None for invalid formats
pub fn parse_entity_id(s: &str) -> Option<EntityId> {
    EntityId::parse(s).ok()
}

/// Edge reference (from/to) - uses EntityId format
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EdgeRef {
    entity_id: EntityId,
}

impl EdgeRef {
    /// Create from entity ID string (e.g., "news/flood-2026")
    pub fn new<S: Into<String>>(entity_id: S) -> Result<Self, ParseEntityIdError> {
        Ok(Self {
            entity_id: EntityId::parse(&entity_id.into())?,
        })
    }

    /// Get the referenced entity ID
    pub fn entity_id(&self) -> &EntityId {
        &self.entity_id
    }

    /// Get as string
    pub fn as_str(&self) -> &str {
        // Note: This is simplified - in production you'd cache this
        self.entity_id.as_str()
    }

    /// Get collection
    pub fn collection(&self) -> &str {
        self.entity_id.collection()
    }

    /// Get key
    pub fn key(&self) -> &str {
        self.entity_id.key()
    }
}

impl fmt::Display for EdgeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.entity_id)
    }
}

impl FromStr for EdgeRef {
    type Err = ParseEntityIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<EdgeRef> for String {
    fn from(edge_ref: EdgeRef) -> Self {
        edge_ref.entity_id.to_string()
    }
}

impl TryFrom<String> for EdgeRef {
    type Error = ParseEntityIdError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

/// Collection with optional schema definition
///
/// Collections are logical groupings of entities. Each collection can have
/// an optional schema that defines how its entities should be indexed.
///
/// # Example
///
/// ```rust
/// use hsdl_sekejap::types::{Collection, CollectionId, CollectionSchema, HotFields};
///
/// let mut collection = Collection::new(CollectionId::new("news"));
/// collection.set_schema(CollectionSchema::new());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collection {
    /// Collection identifier
    pub id: CollectionId,
    
    /// Optional schema definition (None = flex mode)
    #[serde(default)]
    pub schema: Option<super::schema::CollectionSchema>,
    
    /// Collection metadata
    #[serde(default)]
    pub metadata: super::Props,
    
    /// Created timestamp
    #[serde(default)]
    pub created_at: u64,
    
    /// Last modified timestamp
    #[serde(default)]
    pub updated_at: u64,
}

impl Collection {
    /// Create a new collection
    pub fn new(id: CollectionId) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        Self {
            id,
            schema: None,
            metadata: super::Props::new(),
            created_at: now,
            updated_at: now,
        }
    }
    
    /// Set the collection schema
    pub fn set_schema(&mut self, schema: super::schema::CollectionSchema) {
        self.schema = Some(schema);
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
    }
    
    /// Clear the schema (revert to flex mode)
    pub fn clear_schema(&mut self) {
        self.schema = None;
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
    }
    
    /// Get collection ID
    pub fn id(&self) -> &CollectionId {
        &self.id
    }
    
    /// Get collection name
    pub fn name(&self) -> &str {
        self.id.as_str()
    }
    
    /// Check if schema is defined
    pub fn has_schema(&self) -> bool {
        self.schema.is_some()
    }
    
    /// Get reference to schema
    pub fn schema(&self) -> Option<&super::schema::CollectionSchema> {
        self.schema.as_ref()
    }
    
    /// Get mutable reference to schema
    pub fn schema_mut(&mut self) -> Option<&mut super::schema::CollectionSchema> {
        self.schema.as_mut()
    }
    
    /// Check if this collection uses flex mode (no schema)
    pub fn is_flex_mode(&self) -> bool {
        self.schema.is_none()
    }
    
    /// Get metadata
    pub fn metadata(&self) -> &super::Props {
        &self.metadata
    }
    
    /// Get mutable metadata
    pub fn metadata_mut(&mut self) -> &mut super::Props {
        &mut self.metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::schema::CollectionSchema;

    #[test]
    fn test_entity_id_creation() {
        let id = EntityId::new("news", "flood-2026");
        assert_eq!(id.as_str(), "news/flood-2026");
        assert_eq!(id.collection(), "news");
        assert_eq!(id.key(), "flood-2026");
    }

    #[test]
    fn test_entity_id_parse() {
        let id = EntityId::parse("places/gedebage-market").unwrap();
        assert_eq!(id.collection(), "places");
        assert_eq!(id.key(), "gedebage-market");
    }

    #[test]
    fn test_entity_id_parse_error() {
        assert!(EntityId::parse("invalid").is_err());
        assert!(EntityId::parse("a/b/c").is_err());
    }

    #[test]
    fn test_edge_ref() {
        let ref_ = EdgeRef::new("news/flood-2026").unwrap();
        assert_eq!(ref_.collection(), "news");
        assert_eq!(ref_.key(), "flood-2026");
    }

    #[test]
    fn test_parse_entity_id_helper() {
        let id = parse_entity_id("news/flood-2026");
        assert!(id.is_some());
        assert_eq!(id.unwrap().collection(), "news");

        let id = parse_entity_id("invalid");
        assert!(id.is_none());
    }
    
    #[test]
    fn test_collection_creation() {
        let collection = Collection::new(CollectionId::new("news"));
        assert_eq!(collection.name(), "news");
        assert!(collection.is_flex_mode());
        assert!(!collection.has_schema());
    }
    
    #[test]
    fn test_collection_with_schema() {
        let mut collection = Collection::new(CollectionId::new("news"));
        let schema = CollectionSchema::new();
        collection.set_schema(schema);
        
        assert!(collection.has_schema());
        assert!(!collection.is_flex_mode());
    }
}
