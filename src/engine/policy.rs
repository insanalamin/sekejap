/// WAL compaction policy — decides when the engine should auto-compact.
///
/// Compaction rewrites snapshot + payloads.bin and truncates the WAL log
/// to zero bytes. The policy is checked after every [`Engine::flush()`](super::Engine::flush).
///
/// # Example
///
/// ```rust
/// use sekejap::engine::WalPolicy;
///
/// // Auto-compact when WAL exceeds 32 MB or 20,000 entries
/// let policy = WalPolicy::Auto {
///     max_bytes: 32 * 1024 * 1024,
///     max_entries: 20_000,
/// };
///
/// // Or let the caller decide when to compact
/// let manual = WalPolicy::Manual;
/// ```
#[derive(Debug, Clone)]
pub enum WalPolicy {
    /// Never auto-compact. The caller is responsible for calling
    /// [`Engine::compact()`](super::Engine::compact) when desired.
    Manual,

    /// Compact when the WAL exceeds `max_bytes` **or** `max_entries`,
    /// whichever threshold is hit first.
    Auto {
        /// Maximum WAL file size in bytes before triggering compaction.
        max_bytes: u64,
        /// Maximum number of WAL entries before triggering compaction.
        max_entries: usize,
    },
}

impl Default for WalPolicy {
    /// Default: compact at 64 MB or 50,000 entries, whichever comes first.
    fn default() -> Self {
        WalPolicy::Auto {
            max_bytes: 64 * 1024 * 1024,
            max_entries: 50_000,
        }
    }
}

impl WalPolicy {
    /// Check whether the current WAL state exceeds the policy thresholds.
    ///
    /// Returns `true` if compaction should be triggered.
    /// Always returns `false` for [`WalPolicy::Manual`].
    pub fn should_compact(&self, wal_bytes: u64, wal_entries: usize) -> bool {
        match self {
            WalPolicy::Manual => false,
            WalPolicy::Auto { max_bytes, max_entries } => {
                wal_bytes >= *max_bytes || wal_entries >= *max_entries
            }
        }
    }
}
