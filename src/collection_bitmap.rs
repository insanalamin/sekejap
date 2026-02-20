//! CollectionBitmapIndex — per-collection RoaringBitmaps persisted to disk.
//!
//! Replaces the O(N) mmap linear scan in Step::Collection.
//!
//! # Disk layout
//! ```text
//! {base}/collections/{collection_hash:016x}.rbm   ← serialised RoaringBitmap
//! ```
//!
//! # RAM usage
//! Lazy-loaded per active collection. ≈2–6 MB per collection for 50M nodes.
//! 10 collections ≈ 60 MB max.

use dashmap::DashMap;
use roaring::RoaringBitmap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct CollectionBitmapIndex {
    /// in-memory cache: collection_hash → bitmap (guarded for concurrent write)
    bitmaps: DashMap<u64, Arc<parking_lot::RwLock<RoaringBitmap>>>,
    /// tracks which collections have unsaved changes
    dirty: DashMap<u64, bool>,
    /// base directory for .rbm files
    base_dir: PathBuf,
}

impl CollectionBitmapIndex {
    /// Create or open a collection bitmap store rooted at `base_dir/collections/`.
    pub fn new(base_dir: &Path) -> io::Result<Self> {
        let dir = base_dir.join("collections");
        fs::create_dir_all(&dir)?;
        Ok(Self {
            bitmaps: DashMap::new(),
            dirty: DashMap::new(),
            base_dir: dir,
        })
    }

    fn rbm_path(&self, hash: u64) -> PathBuf {
        self.base_dir.join(format!("{:016x}.rbm", hash))
    }

    /// Insert a node index into a collection bitmap (write path).
    /// Marks the collection dirty so it will be flushed.
    pub fn insert(&self, collection_hash: u64, node_idx: u32) {
        let bm = self.bitmaps
            .entry(collection_hash)
            .or_insert_with(|| Arc::new(parking_lot::RwLock::new(RoaringBitmap::new())));
        bm.write().insert(node_idx);
        self.dirty.insert(collection_hash, true);
    }

    /// Remove a node index from a collection bitmap (delete path).
    pub fn remove(&self, collection_hash: u64, node_idx: u32) {
        if let Some(bm) = self.bitmaps.get(&collection_hash) {
            bm.write().remove(node_idx);
            self.dirty.insert(collection_hash, true);
        } else {
            // Try loading from disk first, then remove
            if let Ok(bm) = self.load_from_disk(collection_hash) {
                let mut locked = bm.write();
                locked.remove(node_idx);
                drop(locked);
                self.bitmaps.insert(collection_hash, bm);
                self.dirty.insert(collection_hash, true);
            }
        }
    }

    /// Get (or lazy-load) the RoaringBitmap for a collection.
    /// Returns a cloned bitmap snapshot for iteration.
    pub fn get_snapshot(&self, collection_hash: u64) -> RoaringBitmap {
        // Check in-memory cache first
        if let Some(bm) = self.bitmaps.get(&collection_hash) {
            return bm.read().clone();
        }

        // Try loading from disk
        match self.load_from_disk(collection_hash) {
            Ok(bm) => {
                let snapshot = bm.read().clone();
                self.bitmaps.insert(collection_hash, bm);
                snapshot
            }
            Err(_) => RoaringBitmap::new(),
        }
    }

    fn load_from_disk(&self, hash: u64) -> io::Result<Arc<parking_lot::RwLock<RoaringBitmap>>> {
        let path = self.rbm_path(hash);
        let file = fs::File::open(&path)?;
        let bm = RoaringBitmap::deserialize_from(file)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Arc::new(parking_lot::RwLock::new(bm)))
    }

    /// Flush all dirty bitmaps to disk.
    pub fn flush(&self) -> io::Result<()> {
        for entry in self.dirty.iter() {
            let hash = *entry.key();
            if let Some(bm) = self.bitmaps.get(&hash) {
                let path = self.rbm_path(hash);
                let file = fs::File::create(&path)?;
                bm.read().serialize_into(file)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            }
        }
        self.dirty.clear();
        Ok(())
    }

    /// Rebuild all bitmaps from an iterator of `(collection_hash, node_idx)` pairs.
    /// Called during `rebuild_indexes` when re-opening an existing DB.
    pub fn rebuild_from_iter(&self, iter: impl Iterator<Item = (u64, u32)>) {
        // Clear any stale in-memory state
        self.bitmaps.clear();
        self.dirty.clear();

        for (collection_hash, node_idx) in iter {
            let bm = self.bitmaps
                .entry(collection_hash)
                .or_insert_with(|| Arc::new(parking_lot::RwLock::new(RoaringBitmap::new())));
            bm.write().insert(node_idx);
        }
        // Mark all as dirty so they get flushed on next flush()
        for entry in self.bitmaps.iter() {
            self.dirty.insert(*entry.key(), true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_insert_get() {
        let dir = tempdir().unwrap();
        let idx = CollectionBitmapIndex::new(dir.path()).unwrap();

        let col_hash = seahash::hash(b"citizens");
        idx.insert(col_hash, 0);
        idx.insert(col_hash, 5);
        idx.insert(col_hash, 100);

        let snap = idx.get_snapshot(col_hash);
        assert_eq!(snap.len(), 3);
        assert!(snap.contains(5));
        assert!(!snap.contains(1));
    }

    #[test]
    fn test_flush_and_reload() {
        let dir = tempdir().unwrap();
        let col_hash = seahash::hash(b"services");

        {
            let idx = CollectionBitmapIndex::new(dir.path()).unwrap();
            idx.insert(col_hash, 1);
            idx.insert(col_hash, 2);
            idx.insert(col_hash, 3);
            idx.flush().unwrap();
        }

        {
            let idx = CollectionBitmapIndex::new(dir.path()).unwrap();
            let snap = idx.get_snapshot(col_hash);
            assert_eq!(snap.len(), 3);
            assert!(snap.contains(1));
            assert!(snap.contains(3));
        }
    }

    #[test]
    fn test_remove() {
        let dir = tempdir().unwrap();
        let idx = CollectionBitmapIndex::new(dir.path()).unwrap();
        let col_hash = seahash::hash(b"nodes");

        idx.insert(col_hash, 10);
        idx.insert(col_hash, 20);
        idx.remove(col_hash, 10);

        let snap = idx.get_snapshot(col_hash);
        assert_eq!(snap.len(), 1);
        assert!(!snap.contains(10));
        assert!(snap.contains(20));
    }

    #[test]
    fn test_rebuild_from_iter() {
        let dir = tempdir().unwrap();
        let idx = CollectionBitmapIndex::new(dir.path()).unwrap();
        let col_hash = seahash::hash(b"items");

        let pairs: Vec<(u64, u32)> = (0..100).map(|i| (col_hash, i)).collect();
        idx.rebuild_from_iter(pairs.into_iter());

        let snap = idx.get_snapshot(col_hash);
        assert_eq!(snap.len(), 100);
    }
}
