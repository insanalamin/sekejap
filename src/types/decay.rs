//! Props and Temporal Decay types for Sekejap-DB
//!
//! Provides wrapper types for flexible properties and optional temporal decay:
//! - Props: JSON wrapper for arbitrary properties
//! - TemporalDecay: Optional weight decay configuration
//!
//! # Example
//!
//! ```rust
//! use hsdl_sekejap::types::{Props, TemporalDecay, DecayFunction};
//!
//! // Properties wrapper
//! let props = Props::from(json!({
//!     "tags": ["banjir", "emergency"],
//!     "sentiment": -0.73,
//!     "published_at": "2026-01-27T03:10:00Z"
//! }));
//!
//! // Temporal decay configuration
//! let decay = TemporalDecay::enabled(30, 0.1, DecayFunction::Exponential);
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

/// Wrapper for arbitrary JSON properties
///
/// All custom node/edge properties should be stored here,
/// not at the top level. This ensures clean separation
/// between system fields and user data.
///
/// # Example
///
/// ```rust
/// use hsdl_sekejap::types::Props;
///
/// let props = Props::new();
/// assert!(props.is_empty());
///
/// let props = Props::from(json!({
///     "tags": ["news", "jakarta"],
///     "author": "agent-123",
///     "confidence": 0.95
/// }));
///
/// assert_eq!(props.get("author"), Some(&json!("agent-123")));
/// assert_eq!(props.tags().len(), 2);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(from = "Value", into = "Value")]
pub struct Props(serde_json::Map<String, Value>);

impl Props {
    /// Create empty properties
    pub fn new() -> Self {
        Self(serde_json::Map::new())
    }
    
    /// Create with capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self(serde_json::Map::with_capacity(capacity))
    }
    
    /// Get a property value
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }
    
    /// Get a property value as string
    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.as_str())
    }
    
    /// Get a property value as number
    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.0.get(key).and_then(|v| v.as_f64())
    }
    
    /// Get a property value as bool
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.0.get(key).and_then(|v| v.as_bool())
    }
    
    /// Get a property value as array
    pub fn get_array(&self, key: &str) -> Option<&Vec<Value>> {
        self.0.get(key).and_then(|v| v.as_array())
    }
    
    /// Get tags array (convenience method)
    pub fn tags(&self) -> Vec<&str> {
        self.get_array("tags").map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default()
    }
    
    /// Set a property value
    pub fn set(&mut self, key: impl Into<String>, value: Value) -> Option<Value> {
        self.0.insert(key.into(), value)
    }
    
    /// Set string property
    pub fn set_str(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.0.insert(key.into(), value.into());
    }
    
    /// Set number property
    pub fn set_f64(&mut self, key: impl Into<String>, value: f64) {
        self.0.insert(key.into(), value.into());
    }
    
    /// Set bool property
    pub fn set_bool(&mut self, key: impl Into<String>, value: bool) {
        self.0.insert(key.into(), value.into());
    }
    
    /// Remove a property
    pub fn remove(&mut self, key: &str) -> Option<Value> {
        self.0.remove(key)
    }
    
    /// Check if property exists
    pub fn contains(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }
    
    /// Get number of properties
    pub fn len(&self) -> usize {
        self.0.len()
    }
    
    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    
    /// Get all keys
    pub fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|s| s.as_str()).collect()
    }
    
    /// Get reference to inner map
    pub fn inner(&self) -> &serde_json::Map<String, Value> {
        &self.0
    }
    
    /// Get mutable reference to inner map
    pub fn inner_mut(&mut self) -> &mut serde_json::Map<String, Value> {
        &mut self.0
    }
    
    /// Convert to JSON value
    pub fn to_value(&self) -> Value {
        Value::Object(self.0.clone())
    }
}

impl From<Value> for Props {
    fn from(value: Value) -> Self {
        match value {
            Value::Object(map) => Self(map),
            _ => Self(serde_json::Map::new()),
        }
    }
}

impl From<Props> for Value {
    fn from(props: Props) -> Self {
        Value::Object(props.0)
    }
}

impl From<serde_json::Map<String, Value>> for Props {
    fn from(map: serde_json::Map<String, Value>) -> Self {
        Self(map)
    }
}

impl From<&str> for Props {
    fn from(s: &str) -> Self {
        serde_json::from_str(s).unwrap_or_else(|_| Self::new())
    }
}

impl fmt::Display for Props {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_string(&self.0).unwrap_or_default())
    }
}

/// Decay function type
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[derive(Default)]
pub enum DecayFunction {
    /// Exponential decay: weight * 0.5^(days / half_life)
    #[default]
    Exponential,
    /// Linear decay: weight - (days / (half_life * 10)), floored at min_weight
    Linear,
}


impl fmt::Display for DecayFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecayFunction::Exponential => write!(f, "exponential"),
            DecayFunction::Linear => write!(f, "linear"),
        }
    }
}

/// Optional temporal decay configuration for edges
///
/// When enabled, edge weights decay over time based on the configured
/// half-life. This is useful for time-sensitive causal relationships
/// where recent evidence is more relevant.
///
/// **Default: DISABLED** - decay must be explicitly enabled per edge.
///
/// # Example
///
/// ```rust
/// use hsdl_sekejap::types::{TemporalDecay, DecayFunction};
///
/// // Enabled with defaults
/// let decay = TemporalDecay::enabled(30, 0.1, DecayFunction::Exponential);
///
/// // Custom configuration
/// let decay = TemporalDecay {
///     enabled: true,
///     half_life_days: 60,
///     min_weight: 0.2,
///     function: DecayFunction::Linear,
/// };
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TemporalDecay {
    /// Enable weight decay (default: false)
    #[serde(default)]
    pub enabled: bool,
    
    /// Days for weight to halve (default: 30)
    #[serde(default = "default_half_life")]
    pub half_life_days: u32,
    
    /// Minimum weight floor (default: 0.1)
    #[serde(default = "default_min_weight")]
    pub min_weight: f32,
    
    /// Decay function (default: Exponential)
    #[serde(default)]
    pub function: DecayFunction,
}

fn default_half_life() -> u32 {
    30
}

fn default_min_weight() -> f32 {
    0.1
}

impl Default for TemporalDecay {
    fn default() -> Self {
        Self {
            enabled: false,
            half_life_days: 30,
            min_weight: 0.1,
            function: DecayFunction::Exponential,
        }
    }
}

impl TemporalDecay {
    /// Create enabled decay with parameters
    pub fn enabled(half_life_days: u32, min_weight: f32, function: DecayFunction) -> Self {
        Self {
            enabled: true,
            half_life_days,
            min_weight,
            function,
        }
    }
    
    /// Create disabled decay (default)
    pub fn disabled() -> Self {
        Self::default()
    }
    
    /// Check if decay is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
    
    /// Calculate effective weight after time elapsed
    ///
    /// # Arguments
    /// * `initial_weight` - Original weight (before decay)
    /// * `days_elapsed` - Days since edge creation
    ///
    /// # Returns
    /// Decayed weight, floored at min_weight
    pub fn calculate_effective_weight(&self, initial_weight: f32, days_elapsed: u64) -> f32 {
        if !self.enabled || days_elapsed == 0 {
            return initial_weight;
        }
        
        let effective = match self.function {
            DecayFunction::Exponential => {
                initial_weight * (0.5f32).powf(days_elapsed as f32 / self.half_life_days as f32)
            }
            DecayFunction::Linear => {
                let decay_rate = initial_weight / (self.half_life_days as f32 * 10.0);
                let decayed = initial_weight - (decay_rate * days_elapsed as f32);
                decayed.max(self.min_weight)
            }
        };
        
        effective.max(self.min_weight)
    }
    
    /// Check if weight meets threshold after decay
    pub fn meets_threshold_after_decay(&self, initial_weight: f32, days_elapsed: u64, threshold: f32) -> bool {
        self.calculate_effective_weight(initial_weight, days_elapsed) >= threshold
    }
}

impl fmt::Display for TemporalDecay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.enabled {
            write!(f, "TemporalDecay(disabled)")
        } else {
            write!(
                f,
                "TemporalDecay({:?}, half_life={}d, min={})",
                self.function, self.half_life_days, self.min_weight
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    #[test]
    fn test_props_creation() {
        let props = Props::new();
        assert!(props.is_empty());
        assert_eq!(props.len(), 0);
    }
    
    #[test]
    fn test_props_from_json() {
        let props = Props::from(json!({
            "tags": ["a", "b"],
            "count": 5
        }));
        
        // get_str returns string, not array
        assert_eq!(props.get_f64("count").unwrap(), 5.0);
        assert_eq!(props.tags().len(), 2);
        // Check array directly
        let tags = props.get_array("tags").unwrap();
        assert_eq!(tags.len(), 2);
    }
    
    #[test]
    fn test_props_set_get() {
        let mut props = Props::new();
        
        props.set_str("name", "test");
        props.set_f64("score", 95.5);
        props.set_bool("active", true);
        
        assert_eq!(props.get_str("name").unwrap(), "test");
        assert_eq!(props.get_f64("score").unwrap(), 95.5);
        assert_eq!(props.get_bool("active").unwrap(), true);
    }
    
    #[test]
    fn test_temporal_decay_disabled() {
        let decay = TemporalDecay::disabled();
        assert!(!decay.is_enabled());
        assert_eq!(decay.calculate_effective_weight(0.8, 100), 0.8);
    }
    
    #[test]
    fn test_temporal_decay_exponential() {
        let decay = TemporalDecay::enabled(30, 0.1, DecayFunction::Exponential);
        assert!(decay.is_enabled());
        
        // At day 0, weight unchanged
        assert!((decay.calculate_effective_weight(0.8, 0) - 0.8).abs() < 0.0001);
        
        // At day 30, weight should be half
        let weight_30 = decay.calculate_effective_weight(0.8, 30);
        assert!((weight_30 - 0.4).abs() < 0.001);
        
        // At day 60, weight should be quarter
        let weight_60 = decay.calculate_effective_weight(0.8, 60);
        assert!((weight_60 - 0.2).abs() < 0.001);
        
        // Should floor at min_weight
        let weight_very_old = decay.calculate_effective_weight(0.8, 10000);
        assert!((weight_very_old - 0.1).abs() < 0.0001);
    }
    
    #[test]
    fn test_temporal_decay_linear() {
        let decay = TemporalDecay::enabled(10, 0.1, DecayFunction::Linear);
        
        // Linear decay: 0.8 - (0.8 / 100) * 30 = 0.8 - 0.24 = 0.56
        let weight_30 = decay.calculate_effective_weight(0.8, 30);
        assert!((weight_30 - 0.56).abs() < 0.01);
        
        // Should floor at min_weight
        let weight_very_old = decay.calculate_effective_weight(0.8, 10000);
        assert!((weight_very_old - 0.1).abs() < 0.0001);
    }
    
    #[test]
    fn test_decay_threshold_check() {
        let decay = TemporalDecay::enabled(30, 0.1, DecayFunction::Exponential);
        
        // Initial weight 0.8 meets 0.5 threshold
        assert!(decay.meets_threshold_after_decay(0.8, 0, 0.5));
        
        // After 60 days, weight ~0.2, doesn't meet 0.5 threshold
        assert!(!decay.meets_threshold_after_decay(0.8, 60, 0.5));
        
        // After 60 days, weight ~0.2, DOES meet 0.1 threshold
        assert!(decay.meets_threshold_after_decay(0.8, 60, 0.1));
    }
}
