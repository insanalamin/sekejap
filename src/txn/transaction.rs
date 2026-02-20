//! Transaction Implementation

use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::{HashSet, HashMap};

/// Unique transaction identifier
pub type TxnId = u64;

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TxnState {
    /// Transaction is active
    Active,
    /// Transaction is being committed
    Committing,
    /// Transaction was committed successfully
    Committed,
    /// Transaction was rolled back
    RolledBack,
}

/// A transaction
pub struct Transaction {
    /// Unique transaction ID
    pub id: TxnId,
    /// Transaction state
    pub state: TxnState,
    /// Snapshot version at transaction start
    pub start_version: u64,
    /// Commit version (set on commit)
    pub commit_version: Option<u64>,
    /// Read set - nodes/edges that were read
    pub read_set: HashSet<u64>,
    /// Write set - nodes/edges that were modified
    pub write_set: HashSet<u64>,
    /// Is read-only transaction
    pub read_only: bool,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(id: TxnId, start_version: u64) -> Self {
        Self {
            id,
            state: TxnState::Active,
            start_version,
            commit_version: None,
            read_set: HashSet::new(),
            write_set: HashSet::new(),
            read_only: false,
        }
    }
    
    /// Create a read-only transaction
    pub fn new_readonly(id: TxnId, start_version: u64) -> Self {
        Self {
            id,
            state: TxnState::Active,
            start_version,
            commit_version: None,
            read_set: HashSet::new(),
            write_set: HashSet::new(),
            read_only: true,
        }
    }
    
    /// Record a read
    pub fn record_read(&mut self, key: u64) {
        self.read_set.insert(key);
    }
    
    /// Record a write
    pub fn record_write(&mut self, key: u64) {
        self.write_set.insert(key);
    }
    
    /// Check if there's a conflict with another transaction
    pub fn conflicts_with(&self, other: &Transaction) -> bool {
        // Conflict if:
        // 1. This transaction read something that other transaction wrote
        // 2. Other transaction committed after this one started
        if other.commit_version.unwrap_or(u64::MAX) <= self.start_version {
            return false;  // Other committed before this started
        }
        
        // Check read-write conflict
        for key in &self.read_set {
            if other.write_set.contains(key) {
                return true;
            }
        }
        
        false
    }
    
    /// Check if transaction is still active
    pub fn is_active(&self) -> bool {
        self.state == TxnState::Active
    }
}

/// Global transaction ID generator
pub struct TxnIdGenerator {
    next_id: AtomicU64,
}

impl TxnIdGenerator {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
        }
    }
    
    pub fn next(&self) -> TxnId {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }
}

impl Default for TxnIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}