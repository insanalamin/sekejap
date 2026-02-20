//! No-Op WAL Implementation
//!
//! Does nothing - fastest option, no durability.
//! Use for benchmarking or when crash recovery is not needed.

use super::{WriteAheadLog, WalEntry, Lsn};
use std::io::Result;
use std::sync::atomic::{AtomicU64, Ordering};

/// A WAL that does nothing (null object pattern)
/// 
/// All operations succeed immediately with no disk I/O.
/// Use for:
/// - Benchmarking baseline (max throughput)
/// - Ephemeral data that doesn't need durability
/// - Testing
pub struct NoOpWAL {
    lsn: AtomicU64,
}

impl NoOpWAL {
    pub fn new() -> Self {
        Self {
            lsn: AtomicU64::new(0),
        }
    }
}

impl Default for NoOpWAL {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteAheadLog for NoOpWAL {
    fn append(&self, _entry: &WalEntry) -> Result<Lsn> {
        // Just increment LSN, do nothing
        Ok(self.lsn.fetch_add(1, Ordering::Relaxed))
    }
    
    fn append_batch(&self, entries: &[WalEntry]) -> Result<Lsn> {
        // Just increment LSN by batch size
        let count = entries.len() as u64;
        Ok(self.lsn.fetch_add(count, Ordering::Relaxed))
    }
    
    fn sync(&self) -> Result<()> {
        // No-op
        Ok(())
    }
    
    fn replay_from(&self, _lsn: Lsn) -> Result<Vec<WalEntry>> {
        // Nothing to replay
        Ok(vec![])
    }
    
    fn truncate_before(&self, _lsn: Lsn) -> Result<()> {
        // No-op
        Ok(())
    }
    
    fn size_bytes(&self) -> u64 {
        0
    }
    
    fn current_lsn(&self) -> Lsn {
        self.lsn.load(Ordering::Relaxed)
    }
    
    fn is_enabled(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{WriteAheadLog, WalEntry, WalOp};
    
    #[test]
    fn test_noop_wal() {
        let wal = NoOpWAL::new();
        let entry = WalEntry {
            lsn: 0,
            timestamp: 0,
            op: WalOp::PutNode {
                slug_hash: 123,
                collection_hash: 456,
                data: vec![],
            },
        };
        
        let lsn1 = wal.append(&entry).unwrap();
        let lsn2 = wal.append(&entry).unwrap();
        
        assert_eq!(lsn1, 0);
        assert_eq!(lsn2, 1);
        assert_eq!(wal.current_lsn(), 2);
        assert!(!wal.is_enabled());
    }
}