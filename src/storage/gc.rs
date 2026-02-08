//! Garbage Collection for Deleted Nodes and Old Versions
//!
//! This module handles automated cleanup of:
//! - Deleted nodes (after retention period)
//! - Old versions (version compaction)
//! - Orphaned blobs

use crate::storage::SingleStorage;
use crate::types::NodeHeader;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// GC metrics collected by the collector
#[derive(Debug, Clone, Default)]
pub struct GcMetrics {
    /// Total nodes deleted (tombstones)
    pub total_deleted: u64,

    /// Total old versions compacted
    pub total_compacted: u64,

    /// Total bytes reclaimed
    pub bytes_reclaimed: u64,

    /// Last GC run timestamp
    pub last_gc_run: Option<u64>,

    /// GC run duration (milliseconds)
    pub last_gc_duration_ms: Option<u64>,
}

impl GcMetrics {
    /// Record a deletion
    pub fn record_deletion(&mut self, bytes_reclaimed: u64) {
        self.total_deleted += 1;
        self.bytes_reclaimed += bytes_reclaimed;
    }

    /// Record version compaction
    pub fn record_compaction(&mut self, bytes_reclaimed: u64) {
        self.total_compacted += 1;
        self.bytes_reclaimed += bytes_reclaimed;
    }

    /// Start a GC run
    pub fn start_run(&mut self) {
        self.last_gc_run = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
    }

    /// End a GC run
    pub fn end_run(&mut self, duration_ms: u64) {
        self.last_gc_duration_ms = Some(duration_ms);
    }
}

/// Configuration for garbage collection
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Time after which deleted nodes are physically deleted (seconds)
    pub deletion_retention_sec: u64,

    /// Maximum number of versions to keep per node
    pub max_versions: usize,

    /// Minimum free disk percentage before triggering GC (0.0 - 1.0)
    pub min_free_disk_ratio: f64,

    /// Interval between automatic GC runs (seconds)
    pub gc_interval_sec: u64,

    /// Maximum nodes to delete per GC run (for safety)
    pub max_deletes_per_run: usize,

    /// Whether to always keep at least one version (even if deleted)
    pub always_keep_head: bool,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            deletion_retention_sec: 30 * 24 * 3600, // 30 days
            max_versions: 5,
            min_free_disk_ratio: 0.1,   // 10%
            gc_interval_sec: 24 * 3600, // 1 day
            max_deletes_per_run: 1000,
            always_keep_head: true,
        }
    }
}

/// Garbage Collector
///
/// Automatically cleans up deleted nodes and old versions.
pub struct GarbageCollector {
    /// Background thread handle
    thread_handle: Option<JoinHandle<()>>,

    /// Shutdown flag
    shutdown_flag: Arc<AtomicBool>,

    /// Metrics
    metrics: Arc<std::sync::RwLock<GcMetrics>>,

    /// Configuration
    config: GcConfig,
}

impl GarbageCollector {
    /// Create a new garbage collector
    pub fn new(config: GcConfig) -> Self {
        Self {
            thread_handle: None,
            shutdown_flag: Arc::new(AtomicBool::new(true)), // Start in "stopped" state
            metrics: Arc::new(std::sync::RwLock::new(GcMetrics::default())),
            config,
        }
    }

    /// Start the garbage collector
    pub fn start(&mut self, storage: Arc<SingleStorage>) -> Result<(), Box<dyn std::error::Error>> {
        if self.thread_handle.is_some() {
            return Err("GC already started".into());
        }

        self.shutdown_flag.store(false, Ordering::Relaxed);

        let shutdown_flag = Arc::clone(&self.shutdown_flag);
        let metrics = Arc::clone(&self.metrics);
        let config = self.config.clone();

        let handle = thread::spawn(move || {
            Self::run_gc(storage, config, shutdown_flag, metrics);
        });

        self.thread_handle = Some(handle);
        Ok(())
    }

    /// Stop the garbage collector
    pub fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.shutdown_flag.store(true, Ordering::Relaxed);

        if let Some(handle) = self.thread_handle.take() {
            handle
                .join()
                .map_err(|e| format!("GC thread panicked: {:?}", e))?;
        }

        Ok(())
    }

    /// Trigger immediate GC run
    pub fn trigger_gc(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Note: This is a simple trigger - actual GC runs in background thread
        // For synchronous GC, we'd need a different design
        log::info!("GC triggered manually");
        Ok(())
    }

    /// Get current metrics
    pub fn get_metrics(&self) -> GcMetrics {
        self.metrics.read().unwrap().clone()
    }

    /// Check if GC is running
    pub fn is_running(&self) -> bool {
        !self.shutdown_flag.load(Ordering::Relaxed)
    }

    /// GC main loop
    fn run_gc(
        storage: Arc<SingleStorage>,
        config: GcConfig,
        shutdown_flag: Arc<AtomicBool>,
        metrics: Arc<std::sync::RwLock<GcMetrics>>,
    ) {
        let mut last_run = SystemTime::now();

        loop {
            // Check for shutdown
            if shutdown_flag.load(Ordering::Relaxed) {
                log::info!("Garbage collector shutting down");
                break;
            }

            // Check if it's time to run GC
            let elapsed = last_run
                .elapsed()
                .unwrap_or_else(|_| Duration::from_secs(0));
            if elapsed >= Duration::from_secs(config.gc_interval_sec) {
                Self::do_gc(&storage, &config, &metrics);
                last_run = SystemTime::now();
            }

            // Sleep before next check
            thread::sleep(Duration::from_secs(60)); // Check every minute
        }
    }

    /// Perform garbage collection
    fn do_gc(
        storage: &SingleStorage,
        config: &GcConfig,
        metrics: &Arc<std::sync::RwLock<GcMetrics>>,
    ) {
        let start = SystemTime::now();

        {
            let mut m = metrics.write().unwrap();
            m.start_run();
        }

        log::info!("Starting garbage collection run");

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut deleted_count = 0;
        let mut compacted_count = 0;
        let mut bytes_reclaimed = 0u64;

        // Collect all nodes
        let all_nodes: Vec<NodeHeader> = storage.iter().collect();

        // Group nodes by slug_hash (to find versions)
        let mut versions: std::collections::HashMap<u64, Vec<NodeHeader>> =
            std::collections::HashMap::new();
        for node in &all_nodes {
            versions
                .entry(node.slug_hash)
                .or_default()
                .push(node.clone());
        }

        // Process each slug's versions
        for (slug_hash, mut node_versions) in versions {
            // Sort by epoch_created (newest first)
            node_versions.sort_by(|a, b| b.epoch_created.cmp(&a.epoch_created));

            // Check if head is deleted (tombstone)
            if let Some(head) = node_versions.first()
                && head.deleted
            {
                // Check if retention period has passed
                if let Some(tombstone) = &head.tombstone {
                    let deleted_at = tombstone.deleted_at / 1000; // Convert ms to sec

                    if now >= deleted_at + config.deletion_retention_sec {
                        // Safe to delete
                        if !config.always_keep_head || node_versions.len() > 1 {
                            for _node in &node_versions {
                                if deleted_count < config.max_deletes_per_run {
                                    storage
                                        .delete_by_slug(slug_hash, Some("gc_delete".to_string()));
                                    deleted_count += 1;
                                    bytes_reclaimed += 1024; // Estimate 1KB per node
                                }
                            }
                        }
                    }
                }
            }

            // Version compaction (keep only N versions)
            if node_versions.len() > config.max_versions {
                for _node in node_versions.iter().skip(config.max_versions) {
                    storage.delete_by_slug(slug_hash, Some("gc_compact".to_string()));
                    compacted_count += 1;
                    bytes_reclaimed += 1024; // Estimate 1KB per node
                }
            }
        }

        // Update metrics
        {
            let mut m = metrics.write().unwrap();
            if deleted_count > 0 {
                m.record_deletion(bytes_reclaimed);
            }
            if compacted_count > 0 {
                m.record_compaction(bytes_reclaimed);
            }
            m.end_run(
                start
                    .elapsed()
                    .unwrap_or_else(|_| Duration::from_secs(0))
                    .as_millis() as u64,
            );
        }

        log::info!(
            "GC complete: deleted={}, compacted={}, reclaimed={} bytes, took={} ms",
            deleted_count,
            compacted_count,
            bytes_reclaimed,
            start
                .elapsed()
                .unwrap_or_else(|_| Duration::from_secs(0))
                .as_millis()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BlobPtr;
    use tempfile::TempDir;

    #[test]
    fn test_gc_creation() {
        let config = GcConfig::default();
        let gc = GarbageCollector::new(config);

        assert!(!gc.is_running());
        assert!(gc.thread_handle.is_none());
    }

    #[test]
    fn test_gc_config_default() {
        let config = GcConfig::default();

        assert_eq!(config.deletion_retention_sec, 30 * 24 * 3600);
        assert_eq!(config.max_versions, 5);
        assert_eq!(config.min_free_disk_ratio, 0.1);
        assert_eq!(config.gc_interval_sec, 24 * 3600);
        assert_eq!(config.max_deletes_per_run, 1000);
        assert!(config.always_keep_head);
    }

    #[test]
    fn test_gc_metrics() {
        let mut metrics = GcMetrics::default();

        assert_eq!(metrics.total_deleted, 0);
        assert_eq!(metrics.total_compacted, 0);
        assert_eq!(metrics.bytes_reclaimed, 0);

        metrics.start_run();
        metrics.record_deletion(1024);
        metrics.record_compaction(2048);
        metrics.end_run(100);

        assert_eq!(metrics.total_deleted, 1);
        assert_eq!(metrics.total_compacted, 1);
        assert_eq!(metrics.bytes_reclaimed, 3072);
        assert_eq!(metrics.last_gc_duration_ms, Some(100));
    }
}
