//! Auto-Promotion Worker
//!
//! Background worker that automatically promotes nodes from Tier 1 (Ingestion Buffer)
//! to Tier 2 (Persistent Storage) based on configurable triggers.

use crate::config::PromotionConfig;
use crate::storage::{IngestionBuffer, PersistentStorage};
use std::sync::mpsc::{self, Sender, Receiver};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Promotion metrics collected by the worker
#[derive(Debug, Clone, Default)]
pub struct PromotionMetrics {
    /// Total number of nodes promoted
    pub total_promoted: u64,
    
    /// Total number of failed promotions
    pub total_failed: u64,
    
    /// Current promotion rate (nodes/second)
    pub promotion_rate: f64,
    
    /// Average promotion latency in milliseconds
    pub avg_latency_ms: f64,
    
    /// Current buffer utilization (bytes)
    pub buffer_size_bytes: usize,
    
    /// Last promotion timestamp
    pub last_promotion_at: Option<u64>,
}

impl PromotionMetrics {
    /// Record a successful promotion
    pub fn record_promotion(&mut self, count: usize, latency_ms: u64) {
        // Calculate weighted average latency (latency_ms is per-node average)
        if self.total_promoted == 0 {
            self.avg_latency_ms = latency_ms as f64;
        } else {
            // Weighted average: (old_avg * old_count + new_avg * new_count) / total_count
            self.avg_latency_ms = (self.avg_latency_ms * self.total_promoted as f64 + latency_ms as f64 * count as f64) / (self.total_promoted + count as u64) as f64;
        }
        
        self.total_promoted += count as u64;
        
        // Update promotion rate (simple moving average)
        if self.last_promotion_at.is_some() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let elapsed = now - self.last_promotion_at.unwrap();
            if elapsed > 0 {
                self.promotion_rate = count as f64 / elapsed as f64;
            }
        }
        
        self.last_promotion_at = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
    }
    
    /// Record a failed promotion
    pub fn record_failure(&mut self) {
        self.total_failed += 1;
    }
}

/// Commands that can be sent to the promotion worker
pub enum WorkerCommand {
    /// Trigger immediate promotion
    PromoteNow,
    
    /// Shutdown the worker
    Shutdown,
}

/// Auto-promotion worker
/// 
/// Runs in background thread and promotes nodes from Ingestion Buffer to
/// Persistent Storage based on configurable triggers.
pub struct PromoteWorker {
    /// Background thread handle
    thread_handle: Option<JoinHandle<()>>,
    
    /// Command sender to control the worker
    command_sender: Option<Sender<WorkerCommand>>,
    
    /// Shutdown flag (atomic for lock-free check)
    shutdown_flag: Arc<AtomicBool>,
    
    /// Metrics collected by the worker
    metrics: Arc<std::sync::RwLock<PromotionMetrics>>,
    
    /// Configuration
    config: PromotionConfig,
}

impl PromoteWorker {
    /// Create a new promotion worker
    /// 
    /// The worker starts in a paused state. Call `start()` to begin promotion.
    pub fn new(config: PromotionConfig) -> Self {
        Self {
            thread_handle: None,
            command_sender: None,
            shutdown_flag: Arc::new(AtomicBool::new(true)), // Start in "stopped" state
            metrics: Arc::new(std::sync::RwLock::new(PromotionMetrics::default())),
            config,
        }
    }
    
    /// Start the promotion worker
    /// 
    /// This spawns a background thread that periodically promotes nodes
    /// based on the configuration.
    pub fn start(
        &mut self,
        ingestion: Arc<IngestionBuffer>,
        persistent: Arc<PersistentStorage>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.thread_handle.is_some() {
            return Err("Worker already started".into());
        }
        
        // Reset shutdown flag
        self.shutdown_flag.store(false, Ordering::Relaxed);
        
        let (command_sender, command_receiver) = mpsc::channel();
        self.command_sender = Some(command_sender);
        
        let shutdown_flag = Arc::clone(&self.shutdown_flag);
        let metrics = Arc::clone(&self.metrics);
        let config = self.config.clone();
        
        // Spawn background thread
        let handle = thread::spawn(move || {
            Self::run_worker(ingestion, persistent, config, command_receiver, shutdown_flag, metrics);
        });
        
        self.thread_handle = Some(handle);
        Ok(())
    }
    
    /// Stop the promotion worker
    /// 
    /// Waits for the background thread to finish.
    pub fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Set shutdown flag
        self.shutdown_flag.store(true, Ordering::Relaxed);
        
        if let Some(sender) = &self.command_sender {
            let _ = sender.send(WorkerCommand::Shutdown); // Ignore errors if channel closed
        }
        
        if let Some(handle) = self.thread_handle.take() {
            handle.join().map_err(|e| format!("Worker thread panicked: {:?}", e))?;
        }
        
        Ok(())
    }
    
    /// Trigger immediate promotion
    /// 
    /// This bypasses the timer-based promotion and forces an immediate flush.
    pub fn trigger_promotion(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(sender) = &self.command_sender {
            sender.send(WorkerCommand::PromoteNow)?;
        }
        Ok(())
    }
    
    /// Get current metrics
    pub fn get_metrics(&self) -> PromotionMetrics {
        self.metrics.read().unwrap().clone()
    }
    
    /// Check if worker is running
    pub fn is_running(&self) -> bool {
        !self.shutdown_flag.load(Ordering::Relaxed)
    }
    
    /// Worker thread main loop
    fn run_worker(
        ingestion: Arc<IngestionBuffer>,
        persistent: Arc<PersistentStorage>,
        config: PromotionConfig,
        command_receiver: Receiver<WorkerCommand>,
        shutdown_flag: Arc<AtomicBool>,
        metrics: Arc<std::sync::RwLock<PromotionMetrics>>,
    ) {
        let mut last_promotion_time = Instant::now();
        let mut idle_start = None;
        
        loop {
            // Check for shutdown
            if shutdown_flag.load(Ordering::Relaxed) {
                log::info!("Promotion worker shutting down");
                break;
            }
            
            // Check for commands
            match command_receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(WorkerCommand::Shutdown) => {
                    log::info!("Promotion worker received shutdown command");
                    break;
                }
                Ok(WorkerCommand::PromoteNow) => {
                    log::info!("Promotion worker received manual promotion trigger");
                    Self::do_promotion(&ingestion, &persistent, &config, &metrics);
                    last_promotion_time = Instant::now();
                    idle_start = None;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    log::warn!("Promotion worker command channel disconnected");
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    // Continue to check triggers
                }
            }
            
            // Check buffer size trigger
            let buffer_size = ingestion.len();
            let buffer_bytes = buffer_size * 1024; // Estimate: 1KB per node (rough)
            
            // Update buffer size in metrics
            {
                let mut m = metrics.write().unwrap();
                m.buffer_size_bytes = buffer_bytes;
            }
            
            // Check if buffer exceeds threshold
            if buffer_bytes > config.buffer_threshold_mb * 1024 * 1024 {
                log::info!(
                    "Buffer size trigger: {} bytes > {} MB",
                    buffer_bytes,
                    config.buffer_threshold_mb
                );
                Self::do_promotion(&ingestion, &persistent, &config, &metrics);
                last_promotion_time = Instant::now();
                idle_start = None;
                continue;
            }
            
            // Check time-based trigger
            let elapsed_since_last = last_promotion_time.elapsed();
            if elapsed_since_last > Duration::from_secs(config.promote_interval_sec) {
                log::info!(
                    "Time-based trigger: {:?} elapsed",
                    elapsed_since_last
                );
                Self::do_promotion(&ingestion, &persistent, &config, &metrics);
                last_promotion_time = Instant::now();
                idle_start = None;
                continue;
            }
            
            // Check idle timeout
            let is_idle = ingestion.is_empty();
            if is_idle {
                if idle_start.is_none() {
                    idle_start = Some(Instant::now());
                } else if let Some(start) = idle_start
                    && let Some(timeout) = config.idle_timeout_sec {
                        let idle_duration = start.elapsed();
                        if idle_duration > Duration::from_secs(timeout) {
                            log::info!("Idle timeout trigger: {:?} idle", idle_duration);
                            // No need to promote if buffer is empty
                            idle_start = None;
                        }
                    }
            } else {
                idle_start = None;
            }
        }
    }
    
    /// Perform promotion with retry logic
    fn do_promotion(
        ingestion: &Arc<IngestionBuffer>,
        persistent: &Arc<PersistentStorage>,
        config: &PromotionConfig,
        metrics: &Arc<std::sync::RwLock<PromotionMetrics>>,
    ) {
        let start = Instant::now();
        
        // Retry logic with exponential backoff
        let mut attempt = 0;
        let mut backoff_ms = 100;
        
        loop {
            match Self::promote_batch(ingestion, persistent, config.batch_size) {
                Ok(count) => {
                    let latency = start.elapsed().as_millis() as u64;
                    
                    // Record success
                    {
                        let mut m = metrics.write().unwrap();
                        m.record_promotion(count, latency);
                    }
                    
                    log::info!(
                        "Promotion successful: {} nodes promoted in {} ms",
                        count,
                        latency
                    );
                    
                    break;
                }
                Err(e) => {
                    attempt += 1;
                    
                    // Record failure
                    {
                        let mut m = metrics.write().unwrap();
                        m.record_failure();
                    }
                    
                    log::warn!(
                        "Promotion attempt {} failed: {}",
                        attempt,
                        e
                    );
                    
                    if attempt >= config.max_retries {
                        log::error!(
                            "Promotion failed after {} attempts, giving up",
                            config.max_retries
                        );
                        break;
                    }
                    
                    // Exponential backoff
                    thread::sleep(Duration::from_millis(backoff_ms));
                    backoff_ms = (backoff_ms as f64 * config.retry_backoff_multiplier) as u64;
                }
            }
        }
    }
    
    /// Promote a batch of nodes
    fn promote_batch(
        ingestion: &Arc<IngestionBuffer>,
        persistent: &Arc<PersistentStorage>,
        _batch_size: usize,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        // Drain all nodes from ingestion buffer
        let nodes = ingestion.drain_all();
        
        if nodes.is_empty() {
            return Ok(0);
        }
        
        // Write all nodes to persistent storage
        for node in &nodes {
            persistent.upsert(node.clone());
        }
        
        log::info!("Promoted {} nodes to persistent storage", nodes.len());
        Ok(nodes.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PromotionConfig;
    
    #[test]
    fn test_promote_worker_creation() {
        let config = PromotionConfig::default();
        let worker = PromoteWorker::new(config);
        
        assert!(!worker.is_running());
        assert!(worker.thread_handle.is_none());
    }
    
    #[test]
    fn test_promotion_metrics() {
        let mut metrics = PromotionMetrics::default();
        
        assert_eq!(metrics.total_promoted, 0);
        assert_eq!(metrics.total_failed, 0);
        
        metrics.record_promotion(100, 50);
        assert_eq!(metrics.total_promoted, 100);
        assert_eq!(metrics.avg_latency_ms, 50.0);
        
        // Record another promotion
        metrics.record_promotion(50, 30);
        assert_eq!(metrics.total_promoted, 150);
        // Average: (100*50 + 50*30) / 150 = (5000 + 1500) / 150 = 6500 / 150 = 43.33
        assert!((metrics.avg_latency_ms - 43.33).abs() < 0.1);
        
        metrics.record_failure();
        assert_eq!(metrics.total_failed, 1);
    }
    
    #[test]
    fn test_command_channel() {
        let (sender, receiver) = mpsc::channel();
        
        sender.send(WorkerCommand::PromoteNow).unwrap();
        sender.send(WorkerCommand::Shutdown).unwrap();
        
        assert!(matches!(receiver.recv().unwrap(), WorkerCommand::PromoteNow));
        assert!(matches!(receiver.recv().unwrap(), WorkerCommand::Shutdown));
    }
}