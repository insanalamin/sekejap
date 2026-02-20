//! Write-Ahead Log (WAL) Module
//!
//! Pluggable durability layer with runtime selection.
//! Similar to SQLite's journal modes (DELETE, WAL, MEMORY, OFF).
//!
//! # Example
//! ```ignore
//! use sekejap::wal::{WalConfig, WalMode};
//!
//! // Fast mode (no durability)
//! let wal = WalConfig::new(WalMode::Disabled).build();
//!
//! // Crash-safe mode (with fsync)
//! let wal = WalConfig::new(WalMode::Sync).path("./data").build();
//! ```

mod traits;
mod noop;
mod disk;

pub use traits::{WriteAheadLog, WalEntry, WalOp, Lsn};
pub use noop::NoOpWAL;
pub use disk::DiskWAL;

use std::path::Path;

/// WAL mode - selectable at runtime
/// 
/// Similar to SQLite's journal modes:
/// - `Disabled` = SQLite OFF (fastest, no durability)
/// - `Async` = SQLite MEMORY (fast, partial durability)
/// - `Sync` = SQLite WAL (crash-safe, slower)
#[derive(Debug, Clone, Copy, Default)]
pub enum WalMode {
    /// No WAL - fastest, no durability (like SQLite OFF)
    Disabled,
    
    /// In-memory buffer with periodic flush (like SQLite MEMORY)
    Async,
    
    /// Synchronous WAL with fsync (like SQLite WAL) - DEFAULT
    #[default]
    Sync,
}

impl WalMode {
    /// Parse from string (case-insensitive)
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "off" | "disabled" | "none" | "noop" => WalMode::Disabled,
            "async" | "memory" | "buffered" => WalMode::Async,
            "sync" | "wal" | "durable" | "disk" => WalMode::Sync,
            _ => WalMode::default(),
        }
    }
}

/// WAL configuration
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// WAL mode
    pub mode: WalMode,
    /// Group commit interval in milliseconds (for Async mode)
    pub group_commit_ms: u64,
    /// Max batch size before force flush
    pub max_batch: usize,
    /// WAL file path (if disk-based)
    pub path: Option<std::path::PathBuf>,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            mode: WalMode::default(),
            group_commit_ms: 10,
            max_batch: 10_000,
            path: None,
        }
    }
}

impl WalConfig {
    /// Create config with specific mode
    pub fn new(mode: WalMode) -> Self {
        Self {
            mode,
            ..Default::default()
        }
    }
    
    /// Set WAL mode
    pub fn mode(mut self, mode: WalMode) -> Self {
        self.mode = mode;
        self
    }
    
    /// Set WAL path
    pub fn path(mut self, path: impl AsRef<Path>) -> Self {
        self.path = Some(path.as_ref().to_path_buf());
        self
    }
    
    /// Create WAL instance based on config
    pub fn build(&self) -> Box<dyn WriteAheadLog> {
        self.build_with_path(self.path.as_deref())
    }
    
    /// Create WAL instance with explicit path
    pub fn build_with_path(&self, path: Option<&Path>) -> Box<dyn WriteAheadLog> {
        match self.mode {
            WalMode::Disabled => Box::new(NoOpWAL::new()),
            WalMode::Async => {
                // For now, use NoOpWAL (in-memory would need implementation)
                // TODO: Implement async buffered WAL
                Box::new(NoOpWAL::new())
            }
            WalMode::Sync => {
                match path {
                    Some(p) => Box::new(DiskWAL::new(p).expect("Failed to create WAL")),
                    None => Box::new(NoOpWAL::new()), // Fallback if no path
                }
            }
        }
    }
}

/// Create WAL with default config
pub fn create_wal(path: &Path) -> Box<dyn WriteAheadLog> {
    WalConfig::default().path(path).build()
}

/// Create WAL with specific mode
pub fn create_wal_with_mode(path: &Path, mode: WalMode) -> Box<dyn WriteAheadLog> {
    WalConfig::new(mode).path(path).build()
}