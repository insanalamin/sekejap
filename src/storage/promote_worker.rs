//! Auto-Promotion Worker
//!
//! Background worker that automatically promotes nodes from Tier 1 (Ingestion Buffer)
//! to Tier 2 (Persistent Storage) based on configurable triggers.

use crate::config::PromotionConfig;
use crate::storage::{BatchUpsert, IngestionBuffer};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
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
        if self.total_promoted == 0 {
            self.avg_latency_ms = latency_ms as f64;
        } else {
            self.avg_latency_ms = (self.avg_latency_ms * self.total_promoted as f64
                + latency_ms as f64 * count as f64)
                / (self.total_promoted + count as u64) as f64;
        }

        self.total_promoted += count as u64;

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
                .as_secs(),
        );
    }

    pub fn record_failure(&mut self) {
        self.total_failed += 1;
    }
}

pub enum WorkerCommand {
    PromoteNow,
    Shutdown,
}

pub struct PromoteWorker {
    thread_handle: Option<JoinHandle<()>>,
    command_sender: Option<Sender<WorkerCommand>>,
    shutdown_flag: Arc<AtomicBool>,
    metrics: Arc<std::sync::RwLock<PromotionMetrics>>,
    config: PromotionConfig,
}

impl PromoteWorker {
    pub fn new(config: PromotionConfig) -> Self {
        Self {
            thread_handle: None,
            command_sender: None,
            shutdown_flag: Arc::new(AtomicBool::new(true)),
            metrics: Arc::new(std::sync::RwLock::new(PromotionMetrics::default())),
            config,
        }
    }

    pub fn start(
        &mut self,
        ingestion: Arc<IngestionBuffer>,
        storage: Arc<dyn BatchUpsert + Send + Sync>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.thread_handle.is_some() {
            return Err("Worker already started".into());
        }

        self.shutdown_flag.store(false, Ordering::Relaxed);
        let (command_sender, command_receiver) = mpsc::channel();
        self.command_sender = Some(command_sender);

        let shutdown_flag = Arc::clone(&self.shutdown_flag);
        let metrics = Arc::clone(&self.metrics);
        let config = self.config.clone();

        let handle = thread::spawn(move || {
            Self::run_worker(
                ingestion,
                storage,
                config,
                command_receiver,
                shutdown_flag,
                metrics,
            );
        });

        self.thread_handle = Some(handle);
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.shutdown_flag.store(true, Ordering::Relaxed);
        if let Some(sender) = &self.command_sender {
            let _ = sender.send(WorkerCommand::Shutdown);
        }
        if let Some(handle) = self.thread_handle.take() {
            handle
                .join()
                .map_err(|e| format!("Worker thread panicked: {:?}", e))?;
        }
        Ok(())
    }

    pub fn trigger_promotion(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(sender) = &self.command_sender {
            sender.send(WorkerCommand::PromoteNow)?;
        }
        Ok(())
    }

    pub fn get_metrics(&self) -> PromotionMetrics {
        self.metrics.read().unwrap().clone()
    }

    pub fn is_running(&self) -> bool {
        !self.shutdown_flag.load(Ordering::Relaxed)
    }

    fn run_worker(
        ingestion: Arc<IngestionBuffer>,
        storage: Arc<dyn BatchUpsert + Send + Sync>,
        config: PromotionConfig,
        command_receiver: Receiver<WorkerCommand>,
        shutdown_flag: Arc<AtomicBool>,
        metrics: Arc<std::sync::RwLock<PromotionMetrics>>,
    ) {
        let mut last_promotion_time = Instant::now();
        let mut idle_start = None;

        loop {
            if shutdown_flag.load(Ordering::Relaxed) { break; }

            match command_receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(WorkerCommand::Shutdown) => break,
                Ok(WorkerCommand::PromoteNow) => {
                    Self::do_promotion(&ingestion, &*storage, &config, &metrics);
                    last_promotion_time = Instant::now();
                    idle_start = None;
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }

            let buffer_size = ingestion.len();
            let buffer_bytes = buffer_size * 1024;
            {
                let mut m = metrics.write().unwrap();
                m.buffer_size_bytes = buffer_bytes;
            }

            if buffer_bytes > config.buffer_threshold_mb * 1024 * 1024 {
                Self::do_promotion(&ingestion, &*storage, &config, &metrics);
                last_promotion_time = Instant::now();
                idle_start = None;
                continue;
            }

            if last_promotion_time.elapsed() > Duration::from_secs(config.promote_interval_sec) {
                Self::do_promotion(&ingestion, &*storage, &config, &metrics);
                last_promotion_time = Instant::now();
                idle_start = None;
                continue;
            }

            if ingestion.is_empty() {
                if idle_start.is_none() {
                    idle_start = Some(Instant::now());
                }
            } else {
                idle_start = None;
            }
        }
    }

    fn do_promotion(
        ingestion: &Arc<IngestionBuffer>,
        storage: &dyn BatchUpsert,
        config: &PromotionConfig,
        metrics: &Arc<std::sync::RwLock<PromotionMetrics>>,
    ) {
        let start = Instant::now();
        let mut attempt = 0;
        let mut backoff_ms = 100;

        loop {
            match Self::promote_batch(ingestion, storage) {
                Ok(count) => {
                    let latency = start.elapsed().as_millis() as u64;
                    let mut m = metrics.write().unwrap();
                    m.record_promotion(count, latency);
                    break;
                }
                Err(e) => {
                    attempt += 1;
                    {
                        let mut m = metrics.write().unwrap();
                        m.record_failure();
                    }
                    if attempt >= config.max_retries { break; }
                    thread::sleep(Duration::from_millis(backoff_ms));
                    backoff_ms = (backoff_ms as f64 * config.retry_backoff_multiplier) as u64;
                }
            }
        }
    }

    fn promote_batch(
        ingestion: &Arc<IngestionBuffer>,
        storage: &dyn BatchUpsert,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let nodes = ingestion.drain_all();
        if nodes.is_empty() { return Ok(0); }
        let count = nodes.len();
        storage.upsert_batch(&nodes)?;
        Ok(count)
    }
}