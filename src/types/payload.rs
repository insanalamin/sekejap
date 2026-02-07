//! Foundational trait for unified Node and Edge entities
//!
//! This module defines the `Payload` trait that both NodePayload and EdgePayload implement,
//! following ArangoDB's unified document model where both nodes and edges are first-class
//! entities with attributes.

use serde_json::Value;

/// Foundational trait for all entity payloads (Node and Edge)
///
/// This trait defines the common interface shared by NodePayload and EdgePayload,
/// following ArangoDB's unified document model. Both nodes and edges are first-class
/// entities with attributes, enabling polymorphic code that works with both.
///
/// # Design Philosophy
///
/// - **Unified Interface**: Nodes and edges share common operations (get/set metadata, timestamp)
/// - **Type-Specific Extensions**: Each payload type has specialized fields (coordinates for nodes, weight for edges)
/// - **Zero Performance Cost**: Trait methods are simple accessors, optimized away by monomorphization
/// - **ArangoDB Compatibility**: Mirrors ArangoDB's document model where edges are first-class entities
///
/// # Example
///
/// ```rust,ignore
/// use hsdl_sekejap::{NodePayload, EdgePayload, Payload};
///
/// fn print_entity_info<P: Payload>(entity: &P) {
///     println!("Type: {}", entity.get_type());
///     println!("Title: {}", entity.get_title());
///     println!("Created: {}", entity.get_timestamp());
/// }
///
/// // Works with both nodes and edges
/// let node = NodePayload::new("test".to_string(), "Test Node".to_string());
/// let edge = EdgePayload::new("causal".to_string(), "Causal Edge".to_string());
///
/// print_entity_info(&node);
/// print_entity_info(&edge);
/// ```
pub trait Payload {
    /// Get entity type identifier (e.g., "node", "causal", "hierarchy")
    fn get_type(&self) -> &str;
    
    /// Get human-readable title
    fn get_title(&self) -> &str;
    
    /// Get creation timestamp (Unix milliseconds)
    fn get_timestamp(&self) -> u64;
    
    /// Get metadata value by key
    fn get_metadata(&self, key: &str) -> Option<&Value>;
    
    /// Check if metadata contains a key
    fn has_metadata_key(&self, key: &str) -> bool {
        self.get_metadata(key).is_some()
    }
    
    /// Get all metadata as JSON value (owned)
    fn get_all_metadata(&self) -> Value;
}

/// Helper trait for payloads that can be converted to/from JSON
///
/// All payloads implement this for serialization support.
pub trait SerializablePayload: Payload + serde::Serialize + for<'de> serde::Deserialize<'de> {
    /// Serialize to JSON string
    fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
    
    /// Deserialize from JSON string
    fn from_json(json: &str) -> Result<Self, serde_json::Error>
    where
        Self: Sized;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodePayload, EdgePayload};
    
    #[test]
    fn test_payload_trait_on_node() {
        let node = NodePayload::new("Test Node".to_string());
        
        assert_eq!(node.get_type(), "node");
        assert_eq!(node.get_title(), "Test Node");
        assert!(node.get_timestamp() > 0);
        assert!(!node.has_metadata_key("test"));
    }
    
    #[test]
    fn test_payload_trait_on_edge() {
        let edge = EdgePayload::new("causal".to_string());
        
        assert_eq!(edge.get_type(), "causal");
        assert_eq!(edge.get_title(), "");
        assert!(edge.get_timestamp() > 0);
    }
    
    #[test]
    fn test_metadata_access() {
        let mut node = NodePayload::new("Test".to_string());
        node.props.set("test_key", serde_json::json!("test_value"));
        
        assert!(node.has_metadata_key("test_key"));
        assert_eq!(
            node.get_metadata("test_key"),
            Some(&serde_json::json!("test_value"))
        );
    }
}