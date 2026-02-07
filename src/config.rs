//! Centralized Configuration System for Sekejap-DB
//!
//! Provides type-safe configuration with multiple sources:
//! 1. Environment variables (highest priority - for secrets/deployment)
//! 2. Config file (TOML - default settings)
//! 3. Code defaults (fallback values)

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::env;

/// Promotion configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PromotionConfig {
    /// Buffer size threshold in MB (trigger promotion when exceeded)
    pub buffer_threshold_mb: usize,
    
    /// Time interval in seconds between automatic promotions
    pub promote_interval_sec: u64,
    
    /// Batch size for promotion (number of nodes per batch)
    pub batch_size: usize,
    
    /// Idle timeout in seconds (promote if buffer inactive)
    pub idle_timeout_sec: Option<u64>,
    
    /// Maximum retry attempts for failed promotions
    pub max_retries: usize,
    
    /// Backoff multiplier for retries (e.g., 2.0 = exponential backoff)
    pub retry_backoff_multiplier: f64,
}

impl Default for PromotionConfig {
    fn default() -> Self {
        Self {
            buffer_threshold_mb: 100,       // 100MB threshold
            promote_interval_sec: 60,        // Every 60 seconds
            batch_size: 1000,               // 1000 nodes per batch
            idle_timeout_sec: Some(300),      // 5 minutes idle timeout
            max_retries: 3,                  // Max 3 retries
            retry_backoff_multiplier: 2.0,   // Exponential backoff
        }
    }
}

/// Deletion & Garbage Collection configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeletionConfig {
    /// Retention period in days for deleted nodes
    pub retention_days: u64,
    
    /// GC interval in seconds (run GC periodically)
    pub gc_interval_sec: u64,
    
    /// Minimum number of versions to keep per node
    pub min_versions_to_keep: usize,
    
    /// Maximum number of versions to keep per node (0 = unlimited)
    pub max_versions_to_keep: usize,
    
    /// Storage pressure threshold % (trigger GC when disk usage exceeds)
    pub storage_pressure_threshold_pct: usize,
    
    /// Enable GC for tombstone nodes
    pub enable_tombstone_gc: bool,
    
    /// Enable version compaction (keep only N versions)
    pub enable_version_compaction: bool,
}

impl Default for DeletionConfig {
    fn default() -> Self {
        Self {
            retention_days: 30,               // Keep deleted data for 30 days
            gc_interval_sec: 3600,           // Run GC every hour
            min_versions_to_keep: 1,          // Always keep at least 1 version
            max_versions_to_keep: 10,         // Keep max 10 versions
            storage_pressure_threshold_pct: 80,  // Trigger GC at 80% disk usage
            enable_tombstone_gc: true,        // Enable tombstone GC
            enable_version_compaction: true,   // Enable version compaction
        }
    }
}

/// Performance configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PerformanceConfig {
    /// Maximum number of concurrent operations
    pub max_concurrent_ops: usize,
    
    /// Cache size in MB for query results
    pub cache_size_mb: usize,
    
    /// Query timeout in milliseconds
    pub query_timeout_ms: u64,
    
    /// Write timeout in milliseconds
    pub write_timeout_ms: u64,
    
    /// Enable query result caching
    pub enable_query_cache: bool,
    
    /// TTL for cached queries in seconds
    pub query_cache_ttl_sec: u64,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            max_concurrent_ops: 100,           // Max 100 concurrent operations
            cache_size_mb: 256,               // 256MB query cache
            query_timeout_ms: 10000,           // 10 second query timeout
            write_timeout_ms: 5000,            // 5 second write timeout
            enable_query_cache: true,           // Enable caching
            query_cache_ttl_sec: 300,          // Cache for 5 minutes
        }
    }
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageConfig {
    /// Data directory path for database files
    pub data_dir_path: PathBuf,
    
    /// Enable Write-Ahead Log (WAL) for crash recovery
    pub wal_enabled: bool,
    
    /// Compaction interval in seconds
    pub compaction_interval_sec: u64,
    
    /// Maximum WAL size in MB before forced flush
    pub max_wal_size_mb: usize,
    
    /// Enable compression for stored data
    pub enable_compression: bool,
    
    /// Compression level (1-9, higher = better compression but slower)
    pub compression_level: u8,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir_path: PathBuf::from("./data"),  // Default to ./data directory
            wal_enabled: true,                        // Enable WAL
            compaction_interval_sec: 1800,           // Compact every 30 minutes
            max_wal_size_mb: 100,                   // 100MB WAL threshold
            enable_compression: true,                 // Enable compression
            compression_level: 6,                     // Balanced compression (1-9)
        }
    }
}

/// Main configuration structure for Sekejap-DB
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[derive(Default)]
pub struct SekejapConfig {
    /// Promotion configuration
    pub promote: PromotionConfig,
    
    /// Deletion & GC configuration
    pub deletion: DeletionConfig,
    
    /// Performance configuration
    pub performance: PerformanceConfig,
    
    /// Storage configuration
    pub storage: StorageConfig,
}


impl SekejapConfig {
    /// Load configuration from environment variables with optional TOML file
    /// 
    /// Priority: Environment variables > TOML file > Code defaults
    pub fn load(config_file: Option<PathBuf>) -> Result<Self, ConfigError> {
        // Start with defaults
        let mut config = Self::default();
        
        // Load from TOML file if provided
        if let Some(path) = config_file
            && path.exists() {
                config = Self::load_from_toml(&path)?;
            }
        
        // Override with environment variables
        config.apply_env_overrides();
        
        // Validate configuration
        config.validate()?;
        
        Ok(config)
    }
    
    /// Load configuration from TOML file
    fn load_from_toml(path: &PathBuf) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::IoError(e.to_string()))?;
        
        let config: Self = toml::from_str(&content)
            .map_err(|e| ConfigError::ParseError(e.to_string()))?;
        
        Ok(config)
    }
    
    /// Override configuration values from environment variables
    fn apply_env_overrides(&mut self) {
        // Promotion config
        if let Ok(val) = env::var("SEKEJAP_PROMOTE_BUFFER_THRESHOLD_MB") {
            self.promote.buffer_threshold_mb = val.parse().unwrap_or(self.promote.buffer_threshold_mb);
        }
        if let Ok(val) = env::var("SEKEJAP_PROMOTE_INTERVAL_SEC") {
            self.promote.promote_interval_sec = val.parse().unwrap_or(self.promote.promote_interval_sec);
        }
        if let Ok(val) = env::var("SEKEJAP_PROMOTE_BATCH_SIZE") {
            self.promote.batch_size = val.parse().unwrap_or(self.promote.batch_size);
        }
        if let Ok(val) = env::var("SEKEJAP_PROMOTE_IDLE_TIMEOUT_SEC") {
            self.promote.idle_timeout_sec = Some(val.parse().unwrap_or(300));
        }
        if let Ok(val) = env::var("SEKEJAP_PROMOTE_MAX_RETRIES") {
            self.promote.max_retries = val.parse().unwrap_or(self.promote.max_retries);
        }
        
        // Deletion config
        if let Ok(val) = env::var("SEKEJAP_DELETION_RETENTION_DAYS") {
            self.deletion.retention_days = val.parse().unwrap_or(self.deletion.retention_days);
        }
        if let Ok(val) = env::var("SEKEJAP_DELETION_GC_INTERVAL_SEC") {
            self.deletion.gc_interval_sec = val.parse().unwrap_or(self.deletion.gc_interval_sec);
        }
        if let Ok(val) = env::var("SEKEJAP_DELETION_MIN_VERSIONS") {
            self.deletion.min_versions_to_keep = val.parse().unwrap_or(self.deletion.min_versions_to_keep);
        }
        if let Ok(val) = env::var("SEKEJAP_DELETION_MAX_VERSIONS") {
            self.deletion.max_versions_to_keep = val.parse().unwrap_or(self.deletion.max_versions_to_keep);
        }
        
        // Performance config
        if let Ok(val) = env::var("SEKEJAP_PERF_MAX_CONCURRENT_OPS") {
            self.performance.max_concurrent_ops = val.parse().unwrap_or(self.performance.max_concurrent_ops);
        }
        if let Ok(val) = env::var("SEKEJAP_PERF_CACHE_SIZE_MB") {
            self.performance.cache_size_mb = val.parse().unwrap_or(self.performance.cache_size_mb);
        }
        if let Ok(val) = env::var("SEKEJAP_PERF_QUERY_TIMEOUT_MS") {
            self.performance.query_timeout_ms = val.parse().unwrap_or(self.performance.query_timeout_ms);
        }
        if let Ok(val) = env::var("SEKEJAP_PERF_ENABLE_QUERY_CACHE") {
            self.performance.enable_query_cache = val.parse().unwrap_or(self.performance.enable_query_cache);
        }
        
        // Storage config
        if let Ok(val) = env::var("SEKEJAP_STORAGE_DATA_DIR") {
            self.storage.data_dir_path = PathBuf::from(val);
        }
        if let Ok(val) = env::var("SEKEJAP_STORAGE_WAL_ENABLED") {
            self.storage.wal_enabled = val.parse().unwrap_or(self.storage.wal_enabled);
        }
        if let Ok(val) = env::var("SEKEJAP_STORAGE_COMPRESSION_LEVEL") {
            self.storage.compression_level = val.parse().unwrap_or(self.storage.compression_level);
        }
    }
    
    /// Validate configuration values
    fn validate(&self) -> Result<(), ConfigError> {
        // Validate promotion config
        if self.promote.buffer_threshold_mb == 0 {
            return Err(ConfigError::ValidationError("buffer_threshold_mb must be > 0".to_string()));
        }
        if self.promote.promote_interval_sec == 0 {
            return Err(ConfigError::ValidationError("promote_interval_sec must be > 0".to_string()));
        }
        if self.promote.batch_size == 0 {
            return Err(ConfigError::ValidationError("batch_size must be > 0".to_string()));
        }
        if self.promote.max_retries > 10 {
            return Err(ConfigError::ValidationError("max_retries should be <= 10".to_string()));
        }
        
        // Validate deletion config
        if self.deletion.retention_days == 0 {
            return Err(ConfigError::ValidationError("retention_days must be > 0".to_string()));
        }
        if self.deletion.min_versions_to_keep == 0 {
            return Err(ConfigError::ValidationError("min_versions_to_keep must be >= 1".to_string()));
        }
        if self.deletion.max_versions_to_keep > 0 && 
           self.deletion.max_versions_to_keep < self.deletion.min_versions_to_keep {
            return Err(ConfigError::ValidationError(
                "max_versions_to_keep must be >= min_versions_to_keep".to_string()
            ));
        }
        if self.deletion.storage_pressure_threshold_pct > 100 {
            return Err(ConfigError::ValidationError("storage_pressure_threshold_pct must be <= 100".to_string()));
        }
        
        // Validate performance config
        if self.performance.max_concurrent_ops == 0 {
            return Err(ConfigError::ValidationError("max_concurrent_ops must be > 0".to_string()));
        }
        if self.performance.query_timeout_ms == 0 {
            return Err(ConfigError::ValidationError("query_timeout_ms must be > 0".to_string()));
        }
        
        // Validate storage config
        if self.storage.compression_level == 0 || self.storage.compression_level > 9 {
            return Err(ConfigError::ValidationError("compression_level must be 1-9".to_string()));
        }
        
        Ok(())
    }
}

/// Configuration error types
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    IoError(String),
    
    #[error("Parse error: {0}")]
    ParseError(String),
    
    #[error("Validation error: {0}")]
    ValidationError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_default_config() {
        let config = SekejapConfig::default();
        
        assert_eq!(config.promote.buffer_threshold_mb, 100);
        assert_eq!(config.deletion.retention_days, 30);
        assert_eq!(config.performance.max_concurrent_ops, 100);
        assert_eq!(config.storage.data_dir_path, PathBuf::from("./data"));
    }
    
    #[test]
    fn test_load_from_toml() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        
        let toml_content = r#"
[promote]
buffer_threshold_mb = 200
promote_interval_sec = 120
batch_size = 2000
idle_timeout_sec = 600
max_retries = 5
retry_backoff_multiplier = 3.0

[deletion]
retention_days = 60
gc_interval_sec = 7200
min_versions_to_keep = 2
max_versions_to_keep = 20
storage_pressure_threshold_pct = 90
enable_tombstone_gc = true
enable_version_compaction = true

[performance]
max_concurrent_ops = 200
cache_size_mb = 512
query_timeout_ms = 20000
write_timeout_ms = 10000
enable_query_cache = true
query_cache_ttl_sec = 600

[storage]
data_dir_path = "/tmp/sekejap"
wal_enabled = false
compaction_interval_sec = 3600
max_wal_size_mb = 200
enable_compression = true
compression_level = 7
"#;
        std::fs::write(&config_path, toml_content).unwrap();
        
        let config = SekejapConfig::load(Some(config_path)).unwrap();
        
        assert_eq!(config.promote.buffer_threshold_mb, 200);
        assert_eq!(config.promote.promote_interval_sec, 120);
        assert_eq!(config.promote.max_retries, 5);
        assert_eq!(config.promote.retry_backoff_multiplier, 3.0);
        assert_eq!(config.deletion.retention_days, 60);
        assert_eq!(config.deletion.max_versions_to_keep, 20);
        assert_eq!(config.performance.max_concurrent_ops, 200);
        assert_eq!(config.performance.query_timeout_ms, 20000);
        assert_eq!(config.storage.data_dir_path, PathBuf::from("/tmp/sekejap"));
        assert_eq!(config.storage.compression_level, 7);
    }
    
    #[test]
    fn test_env_overrides() {
        unsafe {
            env::set_var("SEKEJAP_PROMOTE_BUFFER_THRESHOLD_MB", "500");
            env::set_var("SEKEJAP_DELETION_RETENTION_DAYS", "90");
        }
        
        let config = SekejapConfig::load(None).unwrap();
        
        assert_eq!(config.promote.buffer_threshold_mb, 500);
        assert_eq!(config.deletion.retention_days, 90);
        
        unsafe {
            env::remove_var("SEKEJAP_PROMOTE_BUFFER_THRESHOLD_MB");
            env::remove_var("SEKEJAP_DELETION_RETENTION_DAYS");
        }
    }
    
    #[test]
    fn test_validation_error_zero_threshold() {
        let config = SekejapConfig {
            promote: PromotionConfig {
                buffer_threshold_mb: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        
        assert!(config.validate().is_err());
    }
    
    #[test]
    fn test_validation_error_compression_level() {
        let config = SekejapConfig {
            storage: StorageConfig {
                compression_level: 10, // Invalid: must be 1-9
                ..Default::default()
            },
            ..Default::default()
        };
        
        assert!(config.validate().is_err());
    }
    
    #[test]
    fn test_validation_success() {
        let config = SekejapConfig::default();
        assert!(config.validate().is_ok());
    }
}