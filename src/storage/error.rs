//! Storage Error Types
//!
//! Defines all possible errors that can occur in the storage layer.
//! Provides detailed error information for debugging and recovery.

use std::path::PathBuf;
use thiserror::Error;

/// Main storage error type
#[derive(Debug, Error)]
pub enum StorageError {
    /// IO error (file not found, permission denied, disk full, etc.)
    #[error("IO error at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },

    /// Database corruption detected
    #[error("Database corruption detected: {message}")]
    Corruption {
        message: String,
        path: Option<PathBuf>,
    },

    /// Transaction conflict (concurrent modification)
    #[error("Transaction conflict: {message}")]
    Conflict {
        message: String,
        expected_rev: Option<u64>,
        actual_rev: Option<u64>,
    },

    /// Node not found
    #[error("Node not found: slug_hash={slug_hash:?}")]
    NotFound {
        slug_hash: Option<u128>,
        node_id: Option<u128>,
    },

    /// Edge not found
    #[error("Edge not found: from={from:?}, to={to:?}")]
    EdgeNotFound {
        from: Option<u128>,
        to: Option<u128>,
    },

    /// Schema mismatch (e.g., wrong vector dimension)
    #[error("Schema mismatch: {message}")]
    SchemaMismatch {
        message: String,
        expected: Option<String>,
        actual: Option<String>,
    },

    /// Serialization/Deserialization error
    #[error("Serialization error: {source}")]
    Serialization {
        source: bincode::Error,
        context: String,
    },

    /// Compression/Decompression error
    #[error("Compression error: {message}")]
    Compression { message: String },

    /// Database is locked by another process
    #[error("Database is locked")]
    Locked { path: PathBuf },

    /// Database is in read-only mode but write was attempted
    #[error("Database is read-only")]
    ReadOnly { path: PathBuf },

    /// Invalid argument passed to storage method
    #[error("Invalid argument: {message}")]
    InvalidArgument {
        message: String,
        field: Option<String>,
    },

    /// Timeout waiting for a resource
    #[error("Timeout: {message}")]
    Timeout { message: String, duration_ms: u64 },

    /// Custom error with message
    #[error("{message}")]
    Custom { message: String },
}

impl StorageError {
    /// Create an IO error with path context
    pub fn io(path: PathBuf, source: std::io::Error) -> Self {
        StorageError::Io { path, source }
    }

    /// Create a conflict error for MVCC retry
    pub fn conflict<E: Into<Option<u64>>, A: Into<Option<u64>>>(
        message: String,
        expected_rev: E,
        actual_rev: A,
    ) -> Self {
        StorageError::Conflict {
            message,
            expected_rev: expected_rev.into(),
            actual_rev: actual_rev.into(),
        }
    }

    /// Create a not found error
    pub fn not_found_slug(slug_hash: u128) -> Self {
        StorageError::NotFound {
            slug_hash: Some(slug_hash),
            node_id: None,
        }
    }

    /// Create a not found error for node_id
    pub fn not_found_node(node_id: u128) -> Self {
        StorageError::NotFound {
            slug_hash: None,
            node_id: Some(node_id),
        }
    }

    /// Create a schema mismatch error
    pub fn schema_mismatch(expected: &str, actual: &str) -> Self {
        StorageError::SchemaMismatch {
            message: format!("Expected {}, got {}", expected, actual),
            expected: Some(expected.to_string()),
            actual: Some(actual.to_string()),
        }
    }

    /// Check if this error indicates a conflict (MVCC retry needed)
    pub fn is_conflict(&self) -> bool {
        matches!(self, StorageError::Conflict { .. })
    }

    /// Check if this error indicates the resource was not found
    pub fn is_not_found(&self) -> bool {
        matches!(self, StorageError::NotFound { .. })
    }

    /// Check if this is a retry-able error
    pub fn is_retryable(&self) -> bool {
        match self {
            StorageError::Conflict { .. } => true,
            StorageError::Timeout { .. } => true,
            StorageError::Io { source, .. } => {
                source.kind() == std::io::ErrorKind::WouldBlock
                    || source.kind() == std::io::ErrorKind::Interrupted
            }
            _ => false,
        }
    }
}

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;

/// Wrapper for recoverable storage operations
#[derive(Debug)]
pub struct Recoverable<T> {
    pub value: T,
    pub warnings: Vec<StorageError>,
}

impl<T> Recoverable<T> {
    /// Create a new recoverable result
    pub fn new(value: T) -> Self {
        Self {
            value,
            warnings: Vec::new(),
        }
    }

    /// Add a warning
    pub fn with_warning(mut self, warning: StorageError) -> Self {
        self.warnings.push(warning);
        self
    }

    /// Check if there are any warnings
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}

/// Convert std::io::Error to StorageError
impl From<std::io::Error> for StorageError {
    fn from(source: std::io::Error) -> Self {
        StorageError::Io {
            path: PathBuf::new(),
            source,
        }
    }
}

/// Convert bincode::Error to StorageError
impl From<bincode::Error> for StorageError {
    fn from(source: bincode::Error) -> Self {
        StorageError::Serialization {
            source,
            context: String::new(),
        }
    }
}

/// Convert redb::Error to StorageError
impl From<redb::Error> for StorageError {
    fn from(source: redb::Error) -> Self {
        // Extract path and inner error from redb Io variant
        let path_str = format!("{:?}", source);
        StorageError::Custom { message: path_str }
    }
}
