//! WAL Trait Definitions

use std::io::Result;

/// Log Sequence Number
pub type Lsn = u64;

/// A single WAL entry
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub lsn: Lsn,
    pub timestamp: u64,
    pub op: WalOp,
}

/// Operations that can be logged
#[derive(Debug, Clone)]
pub enum WalOp {
    /// Insert/update a node
    PutNode { 
        slug_hash: u64, 
        collection_hash: u64,
        data: Vec<u8> 
    },
    /// Delete a node
    DeleteNode { slug_hash: u64 },
    /// Insert an edge
    PutEdge { 
        from_node: u32, 
        to_node: u32, 
        edge_type_hash: u64,
        weight: f32,
    },
    /// Delete an edge
    DeleteEdge { 
        from_node: u32, 
        to_node: u32, 
        edge_type_hash: u64 
    },
    /// Checkpoint marker
    Checkpoint { lsn: Lsn },
}

/// Write-Ahead Log trait
/// 
/// Implementations:
/// - `NoOpWAL`: Does nothing (fastest, no durability)
/// - `DiskWAL`: Appends to disk with group commit (crash-safe)
pub trait WriteAheadLog: Send + Sync {
    /// Append a single entry, returns LSN
    fn append(&self, entry: &WalEntry) -> Result<Lsn>;
    
    /// Batch append with single fsync (group commit)
    fn append_batch(&self, entries: &[WalEntry]) -> Result<Lsn>;
    
    /// Force fsync to disk
    fn sync(&self) -> Result<()>;
    
    /// Replay entries from given LSN (for crash recovery)
    fn replay_from(&self, lsn: Lsn) -> Result<Vec<WalEntry>>;
    
    /// Truncate entries before LSN (after checkpoint)
    fn truncate_before(&self, lsn: Lsn) -> Result<()>;
    
    /// Current WAL size in bytes
    fn size_bytes(&self) -> u64;
    
    /// Current LSN
    fn current_lsn(&self) -> Lsn;
    
    /// Whether WAL is enabled
    fn is_enabled(&self) -> bool;
}