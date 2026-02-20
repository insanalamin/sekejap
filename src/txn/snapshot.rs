//! Snapshot Implementation

use std::sync::Arc;
use super::TxnId;

/// A snapshot of the database at a point in time
/// 
/// Used for:
/// - Read-only transactions
/// - Visibility checks during reads
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Version number of this snapshot
    pub version: u64,
    /// Set of active transaction IDs when snapshot was taken
    /// (transactions that were in-progress, their writes are not visible)
    pub active_txns: Vec<TxnId>,
    /// Timestamp when snapshot was created
    pub timestamp: u64,
}

impl Snapshot {
    /// Create a new snapshot
    pub fn new(version: u64, active_txns: Vec<TxnId>) -> Self {
        Self {
            version,
            active_txns,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        }
    }
    
    /// Check if a transaction's writes are visible in this snapshot
    /// 
    /// A write is visible if:
    /// 1. The writing transaction committed before this snapshot was taken
    /// 2. The writing transaction is not in the active_txns set
    pub fn is_visible(&self, writer_txn: TxnId, commit_version: u64) -> bool {
        // If commit version is after snapshot version, not visible
        if commit_version > self.version {
            return false;
        }
        
        // If writer was active when snapshot was taken, not visible
        if self.active_txns.contains(&writer_txn) {
            return false;
        }
        
        true
    }
    
    /// Create an empty snapshot (for MVCC disabled mode)
    pub fn empty() -> Self {
        Self {
            version: 0,
            active_txns: Vec::new(),
            timestamp: 0,
        }
    }
}

impl Default for Snapshot {
    fn default() -> Self {
        Self::empty()
    }
}