//! Transaction Manager Module (MVCC)
//!
//! Multi-Version Concurrency Control for snapshot isolation.
//! Similar to SQLite's BEGIN/COMMIT with snapshot isolation.
//!
//! # Example
//! ```ignore
//! use sekejap::txn::{TxnConfig, TxnMode};
//!
//! // MVCC enabled (default)
//! let config = TxnConfig::default();  // mvcc_enabled: true
//!
//! // MVCC disabled (faster for single-threaded)
//! let config = TxnConfig::new(TxnMode::Disabled);
//! ```

mod traits;
mod transaction;
mod snapshot;
mod version;
mod manager;

pub use traits::{TransactionManager, IsolationLevel};
pub use transaction::{Transaction, TxnId, TxnState};
pub use snapshot::Snapshot;
pub use version::{VersionTracker, FastVersionTracker, QuickSnapshot};
pub use manager::{FastMvccManager, MvccManager, NoOpManager, create_txn_manager};

/// Transaction mode - runtime selectable like WAL
#[derive(Debug, Clone, Copy, Default)]
pub enum TxnMode {
    /// MVCC disabled - fastest, no isolation
    /// Use for: single-threaded, batch processing
    Disabled,
    
    /// MVCC enabled - snapshot isolation
    /// Use for: multi-threaded, production (default)
    #[default]
    Enabled,
}

impl TxnMode {
    /// Parse from string (case-insensitive)
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "off" | "disabled" | "none" => TxnMode::Disabled,
            "on" | "enabled" | "mvcc" | "snapshot" => TxnMode::Enabled,
            _ => TxnMode::default(),
        }
    }
}

/// Transaction configuration
#[derive(Debug, Clone)]
pub struct TxnConfig {
    /// MVCC mode
    pub mode: TxnMode,
    /// Isolation level (when MVCC enabled)
    pub isolation: IsolationLevel,
    /// Transaction timeout in milliseconds (0 = no timeout)
    pub timeout_ms: u64,
    /// Max retries on conflict
    pub max_retries: usize,
}

impl Default for TxnConfig {
    fn default() -> Self {
        Self {
            mode: TxnMode::default(),
            isolation: IsolationLevel::Snapshot,
            timeout_ms: 0,
            max_retries: 3,
        }
    }
}

impl TxnConfig {
    /// Create config with specific mode
    pub fn new(mode: TxnMode) -> Self {
        Self {
            mode,
            ..Default::default()
        }
    }
    
    /// Set MVCC mode
    pub fn mode(mut self, mode: TxnMode) -> Self {
        self.mode = mode;
        self
    }
    
    /// Set isolation level
    pub fn isolation(mut self, level: IsolationLevel) -> Self {
        self.isolation = level;
        self
    }
    
    /// Check if MVCC is enabled
    pub fn is_enabled(&self) -> bool {
        matches!(self.mode, TxnMode::Enabled)
    }
}