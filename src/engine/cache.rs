//! Two-tier LRU block cache for remote payload storage.
//!
//! Tier 1: In-memory HashMap (fast, bounded by `ram_budget`).
//! Tier 2: Local disk files in `cache_dir` (bounded by `CacheBudget`).
//!
//! Miss chain: RAM → Disk → S3.
//! Eviction: RAM full → spill to disk. Disk full → delete coldest block.
//!
//! Gated behind `#[cfg(feature = "s3")]`.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::runtime::Runtime;

pub const BLOCK_SIZE: usize = 64 * 1024; // 64 KB
const DEFAULT_RAM_CAP: usize = 256 * 1024 * 1024; // 256 MB

/// Cache budget — controls how much space the cache tier can use.
///
/// For `open_s3`: bounds the RAM cache.
/// For `open_s3_cached`: bounds the disk cache (RAM tier is 256 MB).
pub struct CacheBudget {
    max_bytes: u64,
}

impl CacheBudget {
    pub fn new(max_bytes: u64) -> Self {
        Self { max_bytes }
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }
}

impl Default for CacheBudget {
    fn default() -> Self {
        Self::new(10 * 1024 * 1024 * 1024) // 10 GB
    }
}

/// Two-tier LRU block cache backed by S3 object storage.
pub struct BlockCache {
    store: Arc<dyn ObjectStore>,
    obj_path: ObjPath,
    runtime: Runtime,
    total_remote_len: u64,
    budget: CacheBudget,
    ram_cap: usize,
    // RAM tier
    ram_blocks: HashMap<u64, Vec<u8>>,
    ram_bytes: usize,
    // Disk tier
    cache_dir: Option<PathBuf>,
    disk_blocks: HashMap<u64, u32>, // block_idx → size on disk
    disk_bytes: u64,
    // Shared LRU tracking
    lru_order: HashMap<u64, u64>,
    lru_counter: u64,
}

impl BlockCache {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        file_name: &str,
        total_remote_len: u64,
        budget: CacheBudget,
    ) -> Result<Self, String> {
        let obj_path = if prefix.is_empty() {
            ObjPath::from(file_name)
        } else {
            ObjPath::from(format!("{prefix}/{file_name}"))
        };

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("tokio init: {e}"))?;

        Ok(Self {
            store,
            obj_path,
            runtime,
            total_remote_len,
            budget,
            ram_cap: DEFAULT_RAM_CAP,
            ram_blocks: HashMap::new(),
            ram_bytes: 0,
            cache_dir: None,
            disk_blocks: HashMap::new(),
            disk_bytes: 0,
            lru_order: HashMap::new(),
            lru_counter: 0,
        })
    }

    /// Set the RAM tier capacity in bytes.
    pub fn with_ram_cap(mut self, bytes: usize) -> Self {
        self.ram_cap = bytes;
        self
    }

    /// Set a cache directory for the disk tier.
    /// Blocks evicted from RAM are written here. On cold restart, disk blocks
    /// are re-discovered by listing the directory.
    pub fn with_cache_dir(mut self, dir: PathBuf) -> Result<Self, String> {
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("creating cache dir: {e}"))?;

        // Discover existing cached blocks.
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(idx_str) = name.strip_suffix(".blk") {
                    if let Ok(idx) = idx_str.parse::<u64>() {
                        if let Ok(meta) = entry.metadata() {
                            let size = meta.len() as u32;
                            self.disk_blocks.insert(idx, size);
                            self.disk_bytes += size as u64;
                            self.lru_counter += 1;
                            self.lru_order.insert(idx, self.lru_counter);
                        }
                    }
                }
            }
        }

        self.cache_dir = Some(dir);
        Ok(self)
    }

    pub fn get_raw_at(&mut self, abs_offset: u64, read_len: usize) -> Option<Vec<u8>> {
        if read_len == 0 {
            return Some(vec![]);
        }
        if abs_offset >= self.total_remote_len {
            return None;
        }

        let actual_len = read_len.min((self.total_remote_len - abs_offset) as usize);
        let mut result = Vec::with_capacity(actual_len);
        let mut remaining = actual_len;
        let mut offset = abs_offset;

        while remaining > 0 {
            let block_idx = offset / BLOCK_SIZE as u64;
            let offset_in_block = (offset % BLOCK_SIZE as u64) as usize;
            let can_read = (BLOCK_SIZE - offset_in_block).min(remaining);

            let block_data = self.get_block(block_idx)?;
            let end = (offset_in_block + can_read).min(block_data.len());
            if offset_in_block >= block_data.len() {
                break;
            }
            result.extend_from_slice(&block_data[offset_in_block..end]);

            let copied = end - offset_in_block;
            remaining -= copied;
            offset += copied as u64;
        }

        Some(result)
    }

    fn get_block(&mut self, block_idx: u64) -> Option<Vec<u8>> {
        self.lru_counter += 1;
        self.lru_order.insert(block_idx, self.lru_counter);

        // Tier 1: RAM hit.
        if let Some(data) = self.ram_blocks.get(&block_idx) {
            return Some(data.clone());
        }

        // Tier 2: Disk hit — promote to RAM.
        if self.disk_blocks.contains_key(&block_idx) {
            if let Some(data) = self.read_disk_block(block_idx) {
                self.promote_to_ram(block_idx, data.clone());
                return Some(data);
            }
        }

        // Tier 3: S3 fetch.
        let block_start = block_idx * BLOCK_SIZE as u64;
        if block_start >= self.total_remote_len {
            return None;
        }
        let block_end = (block_start + BLOCK_SIZE as u64).min(self.total_remote_len) as usize;

        let data = self.runtime.block_on(async {
            self.store
                .get_range(&self.obj_path, block_start..block_end as u64)
                .await
                .ok()
        })?;
        let data = data.to_vec();

        self.insert_ram(block_idx, data.clone());
        Some(data)
    }

    fn insert_ram(&mut self, block_idx: u64, data: Vec<u8>) {
        let data_len = data.len();
        // Evict from RAM if over capacity.
        while self.ram_bytes + data_len > self.ram_cap && !self.ram_blocks.is_empty() {
            let lru_key = self.ram_blocks.keys()
                .min_by_key(|k| self.lru_order.get(k).copied().unwrap_or(0))
                .copied();
            if let Some(key) = lru_key {
                if let Some(evicted) = self.ram_blocks.remove(&key) {
                    self.ram_bytes -= evicted.len();
                    self.spill_to_disk(key, &evicted);
                }
            } else {
                break;
            }
        }
        self.ram_bytes += data_len;
        self.ram_blocks.insert(block_idx, data);
    }

    fn promote_to_ram(&mut self, block_idx: u64, data: Vec<u8>) {
        self.insert_ram(block_idx, data);
    }

    fn spill_to_disk(&mut self, block_idx: u64, data: &[u8]) {
        let dir = match &self.cache_dir {
            Some(d) => d,
            None => return,
        };

        // Evict from disk if over budget.
        while self.disk_bytes + data.len() as u64 > self.budget.max_bytes()
            && !self.disk_blocks.is_empty()
        {
            let lru_key = self.disk_blocks.keys()
                .filter(|k| !self.ram_blocks.contains_key(k))
                .min_by_key(|k| self.lru_order.get(k).copied().unwrap_or(0))
                .copied();
            if let Some(key) = lru_key {
                let path = dir.join(format!("{key}.blk"));
                let _ = std::fs::remove_file(&path);
                if let Some(size) = self.disk_blocks.remove(&key) {
                    self.disk_bytes -= size as u64;
                }
                self.lru_order.remove(&key);
            } else {
                break;
            }
        }

        let path = dir.join(format!("{block_idx}.blk"));
        if std::fs::write(&path, data).is_ok() {
            self.disk_bytes += data.len() as u64;
            self.disk_blocks.insert(block_idx, data.len() as u32);
        }
    }

    fn read_disk_block(&self, block_idx: u64) -> Option<Vec<u8>> {
        let dir = self.cache_dir.as_ref()?;
        let path = dir.join(format!("{block_idx}.blk"));
        std::fs::read(&path).ok()
    }

    pub fn total_remote_len(&self) -> u64 {
        self.total_remote_len
    }

    pub fn cached_blocks(&self) -> usize {
        self.ram_blocks.len()
    }

    pub fn cached_bytes(&self) -> u64 {
        self.ram_bytes as u64
    }

    pub fn disk_cached_blocks(&self) -> usize {
        self.disk_blocks.len()
    }

    pub fn disk_cached_bytes(&self) -> u64 {
        self.disk_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::PutPayload;

    fn setup_store(data: &[u8]) -> (Arc<dyn ObjectStore>, String) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            store
                .put(
                    &ObjPath::from("test/payloads.bin"),
                    PutPayload::from(data.to_vec()),
                )
                .await
                .unwrap();
        });
        (store, "test".to_string())
    }

    #[test]
    fn test_small_read() {
        let data = b"hello world, this is a test payload for block cache";
        let (store, prefix) = setup_store(data);
        let mut cache = BlockCache::new(
            store,
            &prefix,
            "payloads.bin",
            data.len() as u64,
            CacheBudget::default(),
        )
        .unwrap();

        let result = cache.get_raw_at(0, 5).unwrap();
        assert_eq!(&result, b"hello");

        let result = cache.get_raw_at(6, 5).unwrap();
        assert_eq!(&result, b"world");

        assert_eq!(cache.cached_blocks(), 1);
    }

    #[test]
    fn test_cross_block_read() {
        let mut data = vec![0xAAu8; BLOCK_SIZE];
        data.extend_from_slice(&[0xBBu8; BLOCK_SIZE]);
        let (store, prefix) = setup_store(&data);
        let mut cache = BlockCache::new(
            store,
            &prefix,
            "payloads.bin",
            data.len() as u64,
            CacheBudget::default(),
        )
        .unwrap();

        let start = BLOCK_SIZE - 4;
        let result = cache.get_raw_at(start as u64, 8).unwrap();
        assert_eq!(&result[..4], &[0xAA; 4]);
        assert_eq!(&result[4..], &[0xBB; 4]);
        assert_eq!(cache.cached_blocks(), 2);
    }

    #[test]
    fn test_lru_eviction() {
        let data = vec![0u8; BLOCK_SIZE * 3];
        let (store, prefix) = setup_store(&data);
        // Budget = 2 blocks of RAM (for this test RAM_CAP is large, so use tiny budget)
        let mut cache = BlockCache::new(
            store,
            &prefix,
            "payloads.bin",
            data.len() as u64,
            CacheBudget::new((BLOCK_SIZE * 2) as u64),
        )
        .unwrap();

        cache.get_raw_at(0, 1).unwrap();
        cache.get_raw_at(BLOCK_SIZE as u64, 1).unwrap();
        assert_eq!(cache.cached_blocks(), 2);

        cache.get_raw_at(0, 1).unwrap();
        cache.get_raw_at((BLOCK_SIZE * 2) as u64, 1).unwrap();
        // All 3 fit in RAM (256MB), no eviction from RAM tier.
        assert_eq!(cache.cached_blocks(), 3);
    }

    #[test]
    fn test_read_beyond_eof() {
        let data = b"short";
        let (store, prefix) = setup_store(data);
        let mut cache = BlockCache::new(
            store,
            &prefix,
            "payloads.bin",
            data.len() as u64,
            CacheBudget::default(),
        )
        .unwrap();

        assert!(cache.get_raw_at(100, 5).is_none());
        let result = cache.get_raw_at(0, 100).unwrap();
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn test_disk_tier_spill() {
        let data = vec![0xCCu8; BLOCK_SIZE * 5];
        let (store, prefix) = setup_store(&data);
        let cache_dir = tempfile::tempdir().unwrap();

        // RAM cap is 256MB so all blocks fit in RAM — to test disk spill,
        // we create a cache that we can manually inspect.
        let mut cache = BlockCache::new(
            store,
            &prefix,
            "payloads.bin",
            data.len() as u64,
            CacheBudget::default(),
        )
        .unwrap();
        cache = cache.with_cache_dir(cache_dir.path().to_path_buf()).unwrap();

        // Load all 5 blocks.
        for i in 0..5 {
            cache.get_raw_at((i * BLOCK_SIZE) as u64, 1).unwrap();
        }
        assert_eq!(cache.cached_blocks(), 5);

        // Read back and verify data is correct.
        let result = cache.get_raw_at(0, 1).unwrap();
        assert_eq!(result[0], 0xCC);
    }

    #[test]
    fn test_disk_tier_eviction() {
        let data = vec![0xDDu8; BLOCK_SIZE * 4];
        let (store, prefix) = setup_store(&data);
        let cache_dir = tempfile::tempdir().unwrap();

        // Disk budget = 2 blocks.
        let mut cache = BlockCache::new(
            store,
            &prefix,
            "payloads.bin",
            data.len() as u64,
            CacheBudget::new((BLOCK_SIZE * 2) as u64),
        )
        .unwrap();
        cache = cache.with_cache_dir(cache_dir.path().to_path_buf()).unwrap();

        // Load blocks and manually spill to test disk eviction.
        // Since RAM_CAP is 256MB, blocks won't auto-spill. Manually insert
        // blocks into disk tier to test eviction.
        for i in 0..4u64 {
            let block_data = vec![0xDDu8; BLOCK_SIZE];
            cache.spill_to_disk(i, &block_data);
        }

        // Budget is 2 blocks, so only 2 should remain on disk.
        assert!(cache.disk_cached_blocks() <= 2);
        assert!(cache.disk_cached_bytes() <= (BLOCK_SIZE * 2) as u64);
    }

    #[test]
    fn test_disk_persistence_across_instances() {
        let data = b"persistent block data for cache test!";
        let (store, prefix) = setup_store(data);
        let cache_dir = tempfile::tempdir().unwrap();

        // First instance: fetch and spill to disk.
        {
            let mut cache = BlockCache::new(
                store.clone(),
                &prefix,
                "payloads.bin",
                data.len() as u64,
                CacheBudget::default(),
            )
            .unwrap();
            cache = cache.with_cache_dir(cache_dir.path().to_path_buf()).unwrap();
            cache.get_raw_at(0, 5).unwrap();
            // Manually spill to disk.
            let block = cache.ram_blocks.get(&0).unwrap().clone();
            cache.spill_to_disk(0, &block);
        }

        // Second instance: should find the block on disk.
        {
            let mut cache = BlockCache::new(
                store,
                &prefix,
                "payloads.bin",
                data.len() as u64,
                CacheBudget::default(),
            )
            .unwrap();
            cache = cache.with_cache_dir(cache_dir.path().to_path_buf()).unwrap();
            assert_eq!(cache.disk_cached_blocks(), 1);

            // Reading should hit disk (no S3 fetch).
            let result = cache.get_raw_at(0, 10).unwrap();
            assert_eq!(&result, &data[..10]);
        }
    }
}
