use serde::{Deserialize, Serialize};
use std::fmt;

use super::payload::Payload;
use super::collection::EntityId;
use super::decay::{Props, TemporalDecay};
use super::node::NodeId;

/// User-defined edge type (string-based, like ArangoDB)
/// 
/// Unlike fixed enums, edge types are user-defined strings
/// allowing complete flexibility for domain-specific relationships.
///
/// # Examples
/// ```rust,ignore
/// // Domain-specific edge types
/// let causal_edge = WeightedEdge::new(
///     1u128, 2u128, 0.8,
///     "causal".to_string(),  // User-defined type
///     100,  // evidence_ptr
///     1700000000000,  // valid_start
///     None,  // valid_end
/// );
///
/// // With payload (edge attributes)
/// let edge_with_metadata = WeightedEdge::new_with_payload(
///     1u128, 2u128, 0.9,
///     "influences".to_string(),
///     100,  // evidence_ptr
///     1700000000000,  // valid_start
///     None,  // valid_end
///     Some(EdgePayload {
///         edge_type: "influences".to_string(),
///         title: "Influence relationship".to_string(),
///         metadata: serde_json::json!({
///             "confidence": 0.95,
///             "method": "regression_analysis"
///         }),
///         timestamp: 1700000000000,
///     }),
/// );
/// ```
pub type EdgeType = String;

/// Edge payload with user-defined attributes (DATA_FORMAT.md compliant)
///
/// Edges are first-class entities with metadata, just like nodes.
/// Uses _from, _to, _type, and props per DATA_FORMAT.md spec.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EdgePayload {
    /// Edge type identifier (DATA_FORMAT.md: _type)
    #[serde(default)]
    pub _type: String,
    
    /// Human-readable title
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    
    /// All edge properties (DATA_FORMAT.md: props wrapper)
    #[serde(default)]
    pub props: Props,
    
    /// Creation timestamp
    #[serde(default)]
    pub timestamp: u64, // Unix milliseconds
    
    /// Optional temporal decay configuration
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decay: Option<TemporalDecay>,
}

impl Payload for EdgePayload {
    fn get_type(&self) -> &str {
        &self._type
    }
    
    fn get_title(&self) -> &str {
        self.title.as_deref().unwrap_or("")
    }
    
    fn get_timestamp(&self) -> u64 {
        self.timestamp
    }
    
    fn get_metadata(&self, key: &str) -> Option<&serde_json::Value> {
        self.props.get(key)
    }
    
    fn get_all_metadata(&self) -> serde_json::Value {
        serde_json::Value::Object(self.props.inner().clone())
    }
}

impl EdgePayload {
    /// Create a new edge payload (DATA_FORMAT.md compliant)
    pub fn new(edge_type: impl Into<String>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        Self {
            _type: edge_type.into(),
            title: None,
            props: Props::new(),
            timestamp: now,
            decay: None,
        }
    }
    
    /// Create with title
    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }
    
    /// Add a property
    pub fn with_prop(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.props.set(key, value);
        self
    }
    
    /// Set temporal decay
    pub fn with_decay(mut self, decay: TemporalDecay) -> Self {
        self.decay = Some(decay);
        self
    }
    
    /// Get weight from props
    pub fn weight(&self) -> f32 {
        self.props.get_f64("weight").unwrap_or(1.0) as f32
    }
    
    /// Set weight in props
    pub fn set_weight(&mut self, weight: f32) {
        self.props.set_f64("weight", weight as f64);
    }
    
    /// Calculate effective weight with decay
    pub fn effective_weight(&self, initial_weight: f32, days_elapsed: u64) -> f32 {
        self.decay.as_ref()
            .map(|d| d.calculate_effective_weight(initial_weight, days_elapsed))
            .unwrap_or(initial_weight)
    }
}

/// Weighted edge for knowledge graph (DATA_FORMAT.md compliant)
///
/// Stores edge information with optional payload for user-defined attributes.
/// Uses EntityId-based _from and _to references.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WeightedEdge {
    /// Canonical source entity ID (DATA_FORMAT.md: _from)
    pub _from: EntityId,
    
    /// Canonical target entity ID (DATA_FORMAT.md: _to)
    pub _to: EntityId,
    
    /// Evidence strength 0.0 - 1.0 (stored in props)
    pub weight: f32,
    
    /// Edge type (user-defined string)
    pub _type: EdgeType,
    
    /// Pointer to Evidence Blob
    pub evidence_ptr: u64,
    
    /// Validity window start
    pub valid_start: u64,
    
    /// Validity window end (None = no end)
    pub valid_end: Option<u64>,
    
    /// User-defined edge attributes (DATA_FORMAT.md: props wrapper)
    pub payload: Option<EdgePayload>,
}

impl WeightedEdge {
    /// Create new edge without payload (DATA_FORMAT.md compliant)
    pub fn new(
        _from: EntityId,
        _to: EntityId,
        weight: f32,
        _type: EdgeType,
        evidence_ptr: u64,
        valid_start: u64,
        valid_end: Option<u64>,
    ) -> Self {
        Self {
            _from,
            _to,
            weight,
            _type,
            evidence_ptr,
            valid_start,
            valid_end,
            payload: None,
        }
    }
    
    /// Create new edge with payload (DATA_FORMAT.md compliant)
    pub fn new_with_payload(
        _from: EntityId,
        _to: EntityId,
        weight: f32,
        _type: EdgeType,
        evidence_ptr: u64,
        valid_start: u64,
        valid_end: Option<u64>,
        payload: Option<EdgePayload>,
    ) -> Self {
        Self {
            _from,
            _to,
            weight,
            _type,
            evidence_ptr,
            valid_start,
            valid_end,
            payload,
        }
    }

    /// Check if edge is valid at given timestamp
    pub fn is_valid_at(&self, timestamp: u64) -> bool {
        if timestamp < self.valid_start {
            return false;
        }
        if let Some(end) = self.valid_end
            && timestamp >= end {
                return false;
            }
        true
    }

    /// Check if edge meets weight threshold
    pub fn meets_threshold(&self, threshold: f32) -> bool {
        self.weight >= threshold
    }
    
    /// Check if edge meets threshold after decay
    pub fn meets_threshold_with_decay(&self, threshold: f32, now: u64) -> bool {
        let effective = self.effective_weight(now);
        effective >= threshold
    }
    
    /// Get effective weight with decay
    pub fn effective_weight(&self, now: u64) -> f32 {
        let days_elapsed = (now - self.valid_start) / (24 * 60 * 60 * 1000);
        self.payload.as_ref()
            .map(|p| p.effective_weight(self.weight, days_elapsed))
            .unwrap_or(self.weight)
    }
    
    /// Get metadata value by key
    pub fn get_metadata(&self, key: &str) -> Option<&serde_json::Value> {
        self.payload.as_ref().and_then(|p| p.props.get(key))
    }
    
    /// Get source collection
    pub fn source_collection(&self) -> &str {
        self._from.collection()
    }
    
    /// Get target collection
    pub fn target_collection(&self) -> &str {
        self._to.collection()
    }
    
    /// Get source key
    pub fn source_key(&self) -> &str {
        self._from.key()
    }
    
    /// Get target key
    pub fn target_key(&self) -> &str {
        self._to.key()
    }
    
    /// Get source ID (backward-compatible getter for DATA_FORMAT.md migration)
    pub fn source_id(&self) -> NodeId {
        self._from.key().parse().unwrap_or(0)
    }
    
    /// Get target ID (backward-compatible getter for DATA_FORMAT.md migration)
    pub fn target_id(&self) -> NodeId {
        self._to.key().parse().unwrap_or(0)
    }
    
    /// Get edge type (backward-compatible getter for DATA_FORMAT.md migration)
    pub fn edge_type(&self) -> &str {
        &self._type
    }
}

impl fmt::Display for WeightedEdge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Edge({} -> {}, weight={:.2}, type={})",
            self._from, self._to, self.weight, self._type
        )
    }
}

/// Evidence blob containing source references
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evidence {
    pub source_ids: Vec<u64>, // References to research papers, news articles
    pub confidence_scores: Vec<f32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::decay::DecayFunction;

    #[test]
    fn test_edge_creation() {
        let edge = WeightedEdge::new(
            EntityId::new("news", "event-001"),
            EntityId::new("terms", "banjir"),
            0.85,
            "mentions".to_string(),
            100,
            1700000000000,
            None,
        );
        
        assert_eq!(edge.source_collection(), "news");
        assert_eq!(edge.target_collection(), "terms");
        assert_eq!(edge.source_key(), "event-001");
        assert_eq!(edge.target_key(), "banjir");
        assert_eq!(edge.weight, 0.85);
        assert_eq!(edge._type, "mentions");
    }

    #[test]
    fn test_edge_with_payload() {
        let payload = EdgePayload::new("caused_by")
            .with_title("Causal relationship")
            .with_prop("confidence", serde_json::json!(0.95))
            .with_prop("method", serde_json::json!("regression_analysis"));
        
        let edge = WeightedEdge::new_with_payload(
            EntityId::new("crime", "theft-001"),
            EntityId::new("causes", "poverty"),
            0.7,
            "caused_by".to_string(),
            100,
            1700000000000,
            None,
            Some(payload),
        );
        
        assert_eq!(edge._type, "caused_by");
        assert!(edge.payload.is_some());
        assert_eq!(
            edge.get_metadata("confidence").unwrap(),
            &serde_json::json!(0.95)
        );
    }

    #[test]
    fn test_edge_validity() {
        let edge = WeightedEdge::new(
            EntityId::new("a", "b"),
            EntityId::new("c", "d"),
            0.85,
            "hierarchy".to_string(),
            100,
            1700000000000,
            Some(1700000100000),
        );
        
        // Before valid range
        assert!(!edge.is_valid_at(1699999999999));
        
        // Inside valid range
        assert!(edge.is_valid_at(1700000005000));
        
        // After valid range
        assert!(!edge.is_valid_at(1700000100001));
    }

    #[test]
    fn test_weight_threshold() {
        let edge = WeightedEdge::new(
            EntityId::new("a", "b"),
            EntityId::new("c", "d"),
            0.7,
            "custom-type".to_string(),
            100,
            1700000000000,
            None,
        );
        
        assert!(edge.meets_threshold(0.5));
        assert!(edge.meets_threshold(0.7));
        assert!(!edge.meets_threshold(0.8));
    }
    
    #[test]
    fn test_edge_with_decay() {
        let decay = TemporalDecay::enabled(30, 0.1, DecayFunction::Exponential);
        let payload = EdgePayload::new("caused_by")
            .with_decay(decay);
        
        let edge = WeightedEdge::new_with_payload(
            EntityId::new("crime", "theft"),
            EntityId::new("cause", "poverty"),
            0.8,
            "caused_by".to_string(),
            100,
            1700000000000,
            None,
            Some(payload),
        );
        
        // At day 0, weight unchanged
        let now = 1700000000000u64;
        assert!((edge.effective_weight(now) - 0.8).abs() < 0.0001);
        
        // After decay calculation (simulated)
        let effective = edge.effective_weight(now + 30 * 24 * 60 * 60 * 1000);
        assert!(effective < 0.8); // Should be decayed
        assert!(effective >= 0.1); // Should floor at min_weight
    }
}
