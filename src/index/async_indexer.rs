//! Async Index Builder - Decouple secondary indexes from flush path
//!
//! This module provides background index building that runs independently
//! of the main write/flush path. This keeps ingestion blazing fast while
//! indexes are eventually consistent.
//!
//! Architecture:
//! - Index jobs are enqueued during flush/promotion
//! - Background worker processes jobs sequentially
//! - Search queries work on "built" index while new vectors are pending
//!
//! # Usage
//!
//! ```rust
//! use std::sync::{Arc, Mutex};
//! use hsdl_sekejap::index::AsyncIndexer;
//! use hsdl_sekejap::storage::SingleStorage;
//! use hsdl_sekejap::types::BlobStore;
//!
//! // Create storage and blob store
//! let storage = Arc::new(SingleStorage::new("./data").unwrap());
//! let blob_store = Arc::new(BlobStore::new("./data/blobs").unwrap());
//!
//! // Create and start async indexer
//! let mut indexer = AsyncIndexer::new(storage, blob_store);
//! indexer.start();
//!
//! // Enqueue nodes for indexing (non-blocking)
//! indexer.enqueue_add_vector(node_id);
//!
//! // Check indexing progress
//! let stats = indexer.stats();
//! println!("Indexed: {} vectors", stats.vectors_indexed);
//!
//! // Stop when done
//! indexer.stop();
//! ```

use crate::types::node::NodeId;
use crate::storage::SingleStorage;
use crate::types::BlobStore;
use crossbeam::channel::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Index job types
#[derive(Debug, Clone)]
pub enum IndexJob {
    /// Add a single node to vector index
    AddVector { node_id: NodeId, priority: u32 },
    /// Add multiple nodes to vector index (batch)
    AddVectors { node_ids: Vec<NodeId>, priority: u32 },
    /// Rebuild entire index from storage
    RebuildAll,
    /// Clear and restart indexing
    ClearAndRestart,
}

/// Async indexer statistics
#[derive(Debug, Default, Clone)]
pub struct IndexerStats {
    pub jobs_enqueued: usize,
    pub jobs_processed: usize,
    pub vectors_indexed: usize,
    pub pending_count: usize,
    pub last_processed_at: Option<u64>,
}

/// Async Index Builder
///
/// Decouples index building from the write/flush path for better throughput.
/// Vectors are indexed in background while writes continue at full speed.
pub struct AsyncIndexer {
    /// Storage reference for reading vectors
    storage: Arc<SingleStorage>,
    /// Blob store for reading vector data
    blob_store: Arc<BlobStore>,
    /// Channel for sending jobs to worker
    job_sender: Sender<IndexJob>,
    /// Receiver for worker (stored to keep it alive)
    _job_receiver: Receiver<IndexJob>,
    /// Statistics (shared with worker)
    stats: Arc<Mutex<IndexerStats>>,
    /// Worker thread handle
    worker_handle: Option<thread::JoinHandle<()>>,
    /// Whether worker is running
    running: Arc<Mutex<bool>>,
}

impl AsyncIndexer {
    /// Create a new async indexer
    pub fn new(storage: Arc<SingleStorage>, blob_store: Arc<BlobStore>) -> Self {
        let (job_sender, job_receiver) = channel::unbounded();
        let stats = Arc::new(Mutex::new(IndexerStats::default()));
        let running = Arc::new(Mutex::new(false));

        Self {
            storage,
            blob_store,
            job_sender,
            _job_receiver: job_receiver,
            stats,
            worker_handle: None,
            running,
        }
    }

    /// Start the background worker thread
    pub fn start(&mut self) {
        let job_receiver = self._job_receiver.clone();
        let storage = Arc::clone(&self.storage);
        let blob_store = Arc::clone(&self.blob_store);
        let stats = Arc::clone(&self.stats);
        let running = Arc::clone(&self.running);

        *self.running.lock().unwrap() = true;

        self.worker_handle = Some(thread::spawn(move || {
            log::info!("Async indexer worker started");

            while *running.lock().unwrap() {
                // Process all pending jobs
                loop {
                    // Use recv_timeout for periodic checks
                    let job = job_receiver.recv_timeout(Duration::from_millis(100));
                    match job {
                        Ok(j) => {
                            let start_time = std::time::Instant::now();

                            match j {
                                IndexJob::AddVector { node_id, priority: _ } => {
                                    Self::process_add_vector(&storage, &blob_store, node_id, &stats);
                                }
                                IndexJob::AddVectors { node_ids, priority: _ } => {
                                    for node_id in node_ids {
                                        Self::process_add_vector(&storage, &blob_store, node_id, &stats);
                                    }
                                }
                                IndexJob::RebuildAll => {
                                    Self::process_rebuild_all(&storage, &blob_store, &stats);
                                }
                                IndexJob::ClearAndRestart => {
                                    Self::process_clear_and_restart(&stats);
                                }
                            }

                            let elapsed = start_time.elapsed();
                            if elapsed.as_millis() > 100 {
                                log::warn!("Index job took {}ms", elapsed.as_millis());
                            }
                        }
                        Err(_) => {
                            // Timeout - no jobs available
                            break;
                        }
                    }
                }
            }

            log::info!("Async indexer worker stopped");
        }));
    }

    /// Stop the background worker
    pub fn stop(&mut self) {
        *self.running.lock().unwrap() = false;
        if let Some(handle) = self.worker_handle.take() {
            handle.join().unwrap();
        }
    }

    /// Enqueue a node for vector indexing
    pub fn enqueue_add_vector(&self, node_id: NodeId) {
        let mut stats = self.stats.lock().unwrap();
        stats.jobs_enqueued += 1;
        stats.pending_count += 1;
        drop(stats);

        let _ = self.job_sender.send(IndexJob::AddVector {
            node_id,
            priority: 0,
        });
    }

    /// Enqueue multiple nodes for vector indexing
    pub fn enqueue_add_vectors(&self, node_ids: Vec<NodeId>) {
        let mut stats = self.stats.lock().unwrap();
        stats.jobs_enqueued += node_ids.len();
        stats.pending_count += node_ids.len();
        drop(stats);

        let _ = self.job_sender.send(IndexJob::AddVectors {
            node_ids,
            priority: 0,
        });
    }

    /// Enqueue all nodes from storage for indexing
    pub fn enqueue_rebuild_all(&self) {
        let mut stats = self.stats.lock().unwrap();
        stats.jobs_enqueued += 1;
        stats.pending_count += 1;
        drop(stats);

        let _ = self.job_sender.send(IndexJob::RebuildAll);
    }

    /// Get current statistics
    pub fn stats(&self) -> IndexerStats {
        self.stats.lock().unwrap().clone()
    }

    /// Process single vector add (called by worker)
    fn process_add_vector(
        storage: &SingleStorage,
        blob_store: &Arc<BlobStore>,
        node_id: NodeId,
        stats: &Arc<Mutex<IndexerStats>>,
    ) {
        // Get node header
        let header = match storage.get_by_id(node_id, None) {
            Some(h) => h,
            None => {
                log::warn!("Node {} not found for indexing", node_id);
                return;
            }
        };

        // Get vector from blob store
        if let Some(vector_ptr) = &header.vector_ptr {
            match blob_store.read(*vector_ptr) {
                Ok(_vector_bytes) => {
                    // TODO: Insert into actual HNSW index here
                    // For now, just count it
                    let mut s = stats.lock().unwrap();
                    s.vectors_indexed += 1;
                    s.pending_count = s.pending_count.saturating_sub(1);
                    s.jobs_processed += 1;
                    s.last_processed_at = Some(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64,
                    );

                    log::trace!("Indexed vector for node {}", node_id);
                }
                Err(e) => {
                    log::warn!("Failed to read vector for node {}: {}", node_id, e);
                }
            }
        }
    }

    /// Rebuild all vectors from storage
    fn process_rebuild_all(
        storage: &SingleStorage,
        blob_store: &Arc<BlobStore>,
        stats: &Arc<Mutex<IndexerStats>>,
    ) {
        log::info!("Starting full index rebuild...");

        let mut indexed = 0;

        for header in storage.iter() {
            if header.vector_ptr.is_some() {
                Self::process_add_vector(storage, blob_store, header.node_id, stats);
                indexed += 1;

                if indexed % 1000 == 0 {
                    log::info!("Rebuild progress: {} vectors indexed", indexed);
                }
            }
        }

        let mut s = stats.lock().unwrap();
        s.pending_count = 0;
        s.jobs_processed += 1;

        log::info!("Full index rebuild complete: {} vectors indexed", indexed);
    }

    /// Clear and restart indexing
    fn process_clear_and_restart(stats: &Arc<Mutex<IndexerStats>>) {
        let mut s = stats.lock().unwrap();
        s.vectors_indexed = 0;
        s.pending_count = 0;
        s.jobs_processed += 1;

        log::info!("Index cleared and restarted");
    }

    /// Check if index is stale (needs rebuild)
    pub fn is_stale(&self, max_pending: usize) -> bool {
        let stats = self.stats.lock().unwrap();
        stats.pending_count > max_pending
    }
}

/// Result from index job processing
#[derive(Debug)]
pub struct IndexerResult {
    pub node_id: NodeId,
    pub success: bool,
    pub duration_ms: u64,
}

impl Drop for AsyncIndexer {
    fn drop(&mut self) {
        self.stop();
    }
}
