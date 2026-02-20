//! Transaction Manager Traits

use super::{Transaction, Snapshot, TxnId};
use std::io::Result;

/// Isolation level for transactions
#[derive(Debug, Clone, Copy, Default)]
pub enum IsolationLevel {
    /// Read committed - see only committed data
    ReadCommitted,
    /// Snapshot - see snapshot at transaction start (default)
    #[default]
    Snapshot,
}

/// Transaction Manager trait
/// 
/// Implementations:
/// - `NoOpManager` - No MVCC (disabled mode)
/// - `MvccManager` - Full MVCC with snapshot isolation
pub trait TransactionManager: Send + Sync {
    /// Begin a new read-write transaction
    fn begin(&self) -> Result<Transaction>;
    
    /// Begin a read-only snapshot transaction
    fn begin_readonly(&self) -> Result<Snapshot>;
    
    /// Commit a transaction
    /// Returns error on conflict
    fn commit(&self, txn: Transaction) -> Result<()>;
    
    /// Rollback a transaction
    fn rollback(&self, txn: Transaction);
    
    /// Get current snapshot for reads (non-transactional)
    fn snapshot(&self) -> Snapshot;
    
    /// Get active transaction count
    fn active_count(&self) -> usize;
    
    /// Get current version number
    fn current_version(&self) -> u64;
    
    /// Check if MVCC is enabled
    fn is_enabled(&self) -> bool;
}