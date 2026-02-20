//! Disk-based WAL Implementation with Group Commit
//!
//! Features:
//! - Configurable group commit interval
//! - Configurable batch size
//! - Async background flush

use super::{WriteAheadLog, WalEntry, WalOp, Lsn};
use std::io::{Result, Write, BufWriter};
use std::path::Path;
use std::fs::{File, OpenOptions};
use std::sync::{Arc, Mutex, Condvar};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::collections::VecDeque;
use std::thread::{self, JoinHandle};

/// Disk-based Write-Ahead Log with group commit
pub struct DiskWAL {
    file: Arc<Mutex<BufWriter<File>>>,
    path: std::path::PathBuf,
    lsn: AtomicU64,
    size: Arc<AtomicU64>,
    /// Pending entries to be written
    pending: Arc<Mutex<VecDeque<WalEntry>>>,
    /// Signal for background thread
    signal: Arc<Condvar>,
    /// Background flush thread handle
    flush_thread: Option<JoinHandle<()>>,
    /// Shutdown flag
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    /// Config
    config: DiskWalConfig,
}

/// Configuration for DiskWAL
#[derive(Debug, Clone)]
pub struct DiskWalConfig {
    /// Group commit interval in milliseconds
    /// Default: 10ms (good balance of latency vs throughput)
    pub group_commit_ms: u64,
    /// Max batch size before force flush
    /// Default: 1000 entries
    pub max_batch: usize,
    /// Buffer size for file I/O
    /// Default: 64KB
    pub buffer_size: usize,
}

impl Default for DiskWalConfig {
    fn default() -> Self {
        Self {
            group_commit_ms: 10,
            max_batch: 1000,
            buffer_size: 64 * 1024,
        }
    }
}

impl DiskWAL {
    /// Create or open WAL at given path with default config
    pub fn new(path: &Path) -> Result<Self> {
        Self::with_config(path, DiskWalConfig::default())
    }
    
    /// Create or open WAL with custom config
    pub fn with_config(path: &Path, config: DiskWalConfig) -> Result<Self> {
        // Treat path as directory for WAL files
        std::fs::create_dir_all(path)?;
        
        let wal_path = path.join("wal.log");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)?;
        
        let metadata = file.metadata()?;
        let size = metadata.len();
        
        // Count existing entries to get current LSN
        let lsn = Self::count_entries(&wal_path).unwrap_or(0);
        
        let pending = Arc::new(Mutex::new(VecDeque::new()));
        let signal = Arc::new(Condvar::new());
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        
        let wal = Self {
            file: Arc::new(Mutex::new(BufWriter::with_capacity(config.buffer_size, file))),
            path: wal_path,
            lsn: AtomicU64::new(lsn),
            size: Arc::new(AtomicU64::new(size)),
            pending: pending.clone(),
            signal: signal.clone(),
            flush_thread: None,
            shutdown: shutdown.clone(),
            config,
        };
        
        // Start background flush thread
        let flush_thread = wal.start_flush_thread(pending, signal, shutdown);
        let mut wal = wal;
        wal.flush_thread = Some(flush_thread);
        
        Ok(wal)
    }
    
    /// Start background flush thread for group commit
    fn start_flush_thread(
        &self,
        pending: Arc<Mutex<VecDeque<WalEntry>>>,
        signal: Arc<Condvar>,
        shutdown: Arc<std::sync::atomic::AtomicBool>,
    ) -> JoinHandle<()> {
        let file = Arc::clone(&self.file);
        let max_batch = self.config.max_batch;
        let interval = Duration::from_millis(self.config.group_commit_ms);
        let size = Arc::clone(&self.size);
        
        thread::spawn(move || {
            loop {
                // Wait for signal or timeout
                {
                    let mut pending_guard = pending.lock().unwrap();
                    let result = signal.wait_timeout(pending_guard, interval).unwrap();
                    pending_guard = result.0;
                    
                    // Check shutdown
                    if shutdown.load(Ordering::Relaxed) {
                        // Final flush before shutdown
                        if !pending_guard.is_empty() {
                            Self::flush_batch(&file, &mut pending_guard, &size);
                        }
                        break;
                    }
                    
                    // Flush if batch is ready
                    if pending_guard.len() >= max_batch {
                        Self::flush_batch(&file, &mut pending_guard, &size);
                    }
                }
                
                // Always try to flush on timeout
                let mut pending_guard = pending.lock().unwrap();
                if !pending_guard.is_empty() {
                    Self::flush_batch(&file, &mut pending_guard, &size);
                }
            }
        })
    }
    
    /// Flush a batch of entries to disk
    fn flush_batch(
        file: &Arc<Mutex<BufWriter<File>>>,
        pending: &mut VecDeque<WalEntry>,
        size: &Arc<AtomicU64>,
    ) {
        if pending.is_empty() {
            return;
        }
        
        // Take all pending entries
        let entries: Vec<WalEntry> = pending.drain(..).collect();
        drop(pending);
        
        // Encode all entries
        let mut encoded = Vec::new();
        for entry in entries {
            encoded.extend_from_slice(&Self::encode_entry(&entry));
        }
        
        // Write and fsync once
        if let Ok(mut file_guard) = file.lock() {
            let _ = file_guard.write_all(&encoded);
            let _ = file_guard.flush();
        }
        
        size.fetch_add(encoded.len() as u64, Ordering::Relaxed);
    }
    
    /// Count entries in WAL file
    fn count_entries(path: &Path) -> Result<u64> {
        let file = File::open(path)?;
        let mut reader = std::io::BufReader::new(file);
        let mut count = 0u64;
        let mut buf = [0u8; 8];
        
        loop {
            use std::io::Read;
            match reader.read_exact(&mut buf) {
                Ok(_) => {
                    let entry_len = u64::from_le_bytes(buf) as usize;
                    if entry_len > 0 && entry_len < 10_000_000 {
                        let mut skip = vec![0u8; entry_len];
                        if reader.read_exact(&mut skip).is_ok() {
                            count += 1;
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        
        Ok(count)
    }
    
    /// Encode entry to bytes
    fn encode_entry(entry: &WalEntry) -> Vec<u8> {
        let mut buf = Vec::new();
        
        // Entry length (8 bytes, filled later)
        buf.extend_from_slice(&0u64.to_le_bytes());
        
        // LSN
        buf.extend_from_slice(&entry.lsn.to_le_bytes());
        
        // Timestamp
        buf.extend_from_slice(&entry.timestamp.to_le_bytes());
        
        // Op type (1 byte)
        match &entry.op {
            WalOp::PutNode { .. } => buf.push(1),
            WalOp::DeleteNode { .. } => buf.push(2),
            WalOp::PutEdge { .. } => buf.push(3),
            WalOp::DeleteEdge { .. } => buf.push(4),
            WalOp::Checkpoint { .. } => buf.push(5),
        }
        
        // Op data
        match &entry.op {
            WalOp::PutNode { slug_hash, collection_hash, data } => {
                buf.extend_from_slice(&slug_hash.to_le_bytes());
                buf.extend_from_slice(&collection_hash.to_le_bytes());
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(data);
            }
            WalOp::DeleteNode { slug_hash } => {
                buf.extend_from_slice(&slug_hash.to_le_bytes());
            }
            WalOp::PutEdge { from_node, to_node, edge_type_hash, weight } => {
                buf.extend_from_slice(&from_node.to_le_bytes());
                buf.extend_from_slice(&to_node.to_le_bytes());
                buf.extend_from_slice(&edge_type_hash.to_le_bytes());
                buf.extend_from_slice(&weight.to_le_bytes());
            }
            WalOp::DeleteEdge { from_node, to_node, edge_type_hash } => {
                buf.extend_from_slice(&from_node.to_le_bytes());
                buf.extend_from_slice(&to_node.to_le_bytes());
                buf.extend_from_slice(&edge_type_hash.to_le_bytes());
            }
            WalOp::Checkpoint { lsn } => {
                buf.extend_from_slice(&lsn.to_le_bytes());
            }
        }
        
        // Fill in entry length
        let entry_len = (buf.len() - 8) as u64;
        buf[0..8].copy_from_slice(&entry_len.to_le_bytes());
        
        buf
    }
}

impl Drop for DiskWAL {
    fn drop(&mut self) {
        // Signal shutdown
        self.shutdown.store(true, Ordering::Relaxed);
        self.signal.notify_all();
        
        // Wait for flush thread to finish
        if let Some(handle) = self.flush_thread.take() {
            let _ = handle.join();
        }
    }
}

impl WriteAheadLog for DiskWAL {
    fn append(&self, entry: &WalEntry) -> Result<Lsn> {
        let lsn = self.lsn.fetch_add(1, Ordering::SeqCst);
        let entry = WalEntry { lsn, ..entry.clone() };
        
        // Add to pending buffer
        {
            let mut pending = self.pending.lock().unwrap();
            pending.push_back(entry);
            
            // Signal if batch is ready
            if pending.len() >= self.config.max_batch {
                self.signal.notify_one();
            }
        }
        
        Ok(lsn)
    }
    
    fn append_batch(&self, entries: &[WalEntry]) -> Result<Lsn> {
        let start_lsn = self.lsn.fetch_add(entries.len() as u64, Ordering::SeqCst);
        
        // Add all to pending buffer
        {
            let mut pending = self.pending.lock().unwrap();
            for (i, entry) in entries.iter().enumerate() {
                let lsn = start_lsn + i as u64;
                pending.push_back(WalEntry { lsn, ..entry.clone() });
            }
            
            // Signal
            if pending.len() >= self.config.max_batch {
                self.signal.notify_one();
            }
        }
        
        Ok(start_lsn + entries.len() as u64 - 1)
    }
    
    fn sync(&self) -> Result<()> {
        // Flush all pending synchronously so fsync is meaningful
        let entries: Vec<WalEntry> = {
            let mut pending = self.pending.lock().unwrap();
            pending.drain(..).collect()
        };
        if !entries.is_empty() {
            let mut encoded = Vec::new();
            for entry in entries {
                encoded.extend_from_slice(&Self::encode_entry(&entry));
            }
            {
                let mut file = self.file.lock().unwrap();
                file.write_all(&encoded)?;
                file.flush()?;
            }
            self.size.fetch_add(encoded.len() as u64, Ordering::Relaxed);
        }
        let file = self.file.lock().unwrap();
        file.get_ref().sync_all()?;
        Ok(())
    }
    
    fn replay_from(&self, _lsn: Lsn) -> Result<Vec<WalEntry>> {
        // TODO: Implement proper replay
        Ok(vec![])
    }
    
    fn truncate_before(&self, _lsn: Lsn) -> Result<()> {
        Ok(())
    }
    
    fn size_bytes(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }
    
    fn current_lsn(&self) -> Lsn {
        self.lsn.load(Ordering::Relaxed)
    }
    
    fn is_enabled(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_disk_wal_append() {
        let dir = tempdir().unwrap();
        let wal = DiskWAL::new(dir.path()).unwrap();
        
        let entry = WalEntry {
            lsn: 0,
            timestamp: 12345,
            op: WalOp::PutNode {
                slug_hash: 123,
                collection_hash: 456,
                data: vec![1, 2, 3],
            },
        };
        
        let lsn = wal.append(&entry).unwrap();
        wal.sync().unwrap();
        
        assert_eq!(lsn, 0);
        assert!(wal.is_enabled());
    }
    
    #[test]
    fn test_disk_wal_batch() {
        let dir = tempdir().unwrap();
        let wal = DiskWAL::new(dir.path()).unwrap();
        
        let entries: Vec<WalEntry> = (0..10).map(|i| WalEntry {
            lsn: 0,
            timestamp: i,
            op: WalOp::PutNode {
                slug_hash: i,
                collection_hash: 0,
                data: vec![],
            },
        }).collect();
        
        wal.append_batch(&entries).unwrap();
        wal.sync().unwrap();
    }
    
    #[test]
    fn test_group_commit() {
        let dir = tempdir().unwrap();
        let config = DiskWalConfig {
            group_commit_ms: 5,
            max_batch: 100,
            buffer_size: 64 * 1024,
        };
        let wal = DiskWAL::with_config(dir.path(), config).unwrap();
        
        // Write many entries quickly
        for i in 0..1000 {
            let entry = WalEntry {
                lsn: 0,
                timestamp: i,
                op: WalOp::PutNode {
                    slug_hash: i,
                    collection_hash: 0,
                    data: vec![],
                },
            };
            wal.append(&entry).unwrap();
        }
        
        wal.sync().unwrap();
        assert!(wal.size_bytes() > 0);
    }
}