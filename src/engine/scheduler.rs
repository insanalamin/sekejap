use std::collections::HashSet;

/// Controls when secondary indexes (HNSW, GIN, BM25) are rebuilt after writes.
///
/// HNSW indexes are maintained incrementally on each insert (O(log n)),
/// so this primarily governs full-rebuild scenarios like batch imports
/// or GIN / BM25 indexes that require periodic reconstruction.
#[derive(Debug, Clone)]
pub enum RebuildStrategy {
    /// Rebuild dirty indexes after every write.
    ///
    /// Guarantees reads always see up-to-date indexes at the cost of
    /// slower writes.
    Immediate,

    /// Rebuild dirty indexes every `n` writes.
    ///
    /// Balances read freshness and write throughput. Good for workloads
    /// with bursty writes followed by queries.
    Every(usize),

    /// Never auto-rebuild. The caller manages index freshness manually
    /// by calling rebuild methods on the underlying [`CoreDB`](crate::CoreDB).
    ///
    /// This is the default — suitable when HNSW incremental insert is
    /// sufficient and GIN / BM25 are not used.
    Lazy,
}

impl Default for RebuildStrategy {
    fn default() -> Self {
        RebuildStrategy::Lazy
    }
}

/// Tracks which index fields have been modified since the last rebuild
/// and decides when to trigger a rebuild based on the configured
/// [`RebuildStrategy`].
#[derive(Debug)]
pub struct IndexScheduler {
    strategy: RebuildStrategy,
    /// Vector fields needing HNSW rebuild.
    dirty_hnsw: HashSet<String>,
    /// Text fields needing GIN rebuild.
    dirty_gin: HashSet<String>,
    /// Text fields needing BM25 rebuild.
    dirty_bm25: HashSet<String>,
    /// Writes since last rebuild check.
    write_count: usize,
}

impl IndexScheduler {
    /// Create a scheduler with the given rebuild strategy.
    pub fn new(strategy: RebuildStrategy) -> Self {
        Self {
            strategy,
            dirty_hnsw: HashSet::new(),
            dirty_gin: HashSet::new(),
            dirty_bm25: HashSet::new(),
            write_count: 0,
        }
    }

    /// Mark a vector field as dirty (needs HNSW rebuild on next trigger).
    pub fn mark_hnsw_dirty(&mut self, field: &str) {
        self.dirty_hnsw.insert(field.to_string());
    }

    /// Mark a text field as dirty (needs GIN rebuild on next trigger).
    pub fn mark_gin_dirty(&mut self, field: &str) {
        self.dirty_gin.insert(field.to_string());
    }

    /// Mark a text field as dirty (needs BM25 rebuild on next trigger).
    pub fn mark_bm25_dirty(&mut self, field: &str) {
        self.dirty_bm25.insert(field.to_string());
    }

    /// Record a write operation and check if a rebuild should be triggered.
    ///
    /// Returns `true` when the strategy says it is time to rebuild
    /// (immediately, or every N writes). The caller should then call
    /// the `take_dirty_*` methods to get the affected fields.
    pub fn record_write(&mut self) -> bool {
        self.write_count += 1;
        match &self.strategy {
            RebuildStrategy::Immediate => true,
            RebuildStrategy::Every(n) => self.write_count % n == 0,
            RebuildStrategy::Lazy => false,
        }
    }

    /// Drain and return all dirty HNSW fields, clearing the internal set.
    pub fn take_dirty_hnsw(&mut self) -> HashSet<String> {
        std::mem::take(&mut self.dirty_hnsw)
    }

    /// Drain and return all dirty GIN fields, clearing the internal set.
    pub fn take_dirty_gin(&mut self) -> HashSet<String> {
        std::mem::take(&mut self.dirty_gin)
    }

    /// Drain and return all dirty BM25 fields, clearing the internal set.
    pub fn take_dirty_bm25(&mut self) -> HashSet<String> {
        std::mem::take(&mut self.dirty_bm25)
    }

    /// Reset the write counter (typically called after a rebuild).
    pub fn reset_write_count(&mut self) {
        self.write_count = 0;
    }
}
