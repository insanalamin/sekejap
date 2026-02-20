//! MmapHashIndex — Robin Hood open-addressing hash table backed by mmap.
//!
//! Replaces the in-RAM DashMap<u64, u32> slug_index.
//!
//! # Layout
//! ```text
//! [0..64)   MhashHeader { magic:u64, capacity:u64, count:u64, _pad:[u64;5] }
//! [64..)    HashSlot array — capacity × 16 bytes
//! ```
//! Each slot: `key:u64 | value:u32 | probe_dist:u32`
//! - `key == 0`         → empty
//! - `key == u64::MAX`  → tombstone (deleted)
//!
//! # Thread Safety
//! Wrap in `parking_lot::RwLock<MmapHashIndex>`:
//! - `get(&self)`     → call under `read()`
//! - `insert/remove`  → call under `write()`

use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const MHASH_MAGIC: u64 = 0x5345_4B4D_4841_5348; // "SEKMHASH"
const HEADER_SIZE: usize = 64;
const SLOT_SIZE: usize = 16; // u64 key + u32 value + u32 probe_dist

// Header offsets (all little-endian u64)
const OFF_MAGIC:    usize = 0;
const OFF_CAPACITY: usize = 8;
const OFF_COUNT:    usize = 16;

/// In-memory representation of a hash slot (for arithmetic)
#[derive(Clone, Copy, Default, Debug)]
struct HashSlot {
    key: u64,
    value: u32,
    probe_dist: u32,
}

pub struct MmapHashIndex {
    mmap: MmapMut,
    _file: File,
    /// Must be a power of 2
    capacity: u64,
    /// Live entry count (mirrors the header bytes for fast access)
    count: AtomicU64,
}

impl MmapHashIndex {
    /// Open (or create) a mmap hash index at `path` with room for `capacity` entries.
    ///
    /// Actual allocated capacity is rounded up to the next power of 2 and inflated by
    /// 1/0.65 ≈ 1.54× to keep load factor ≤ 65%.
    pub fn new(path: &Path, capacity: u64) -> io::Result<Self> {
        // Inflate for 65% max load factor, then round to power-of-two
        let inflated = ((capacity as f64 / 0.65).ceil() as u64).max(16);
        let cap = inflated.next_power_of_two();
        let file_size = HEADER_SIZE as u64 + cap * SLOT_SIZE as u64;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        if file.metadata()?.len() < file_size {
            file.set_len(file_size)?;
        }

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        let existing_magic = u64::from_le_bytes(mmap[OFF_MAGIC..OFF_MAGIC + 8].try_into().unwrap());
        let (actual_cap, count) = if existing_magic == MHASH_MAGIC {
            let c = u64::from_le_bytes(mmap[OFF_CAPACITY..OFF_CAPACITY + 8].try_into().unwrap());
            let n = u64::from_le_bytes(mmap[OFF_COUNT..OFF_COUNT + 8].try_into().unwrap());
            (c, n)
        } else {
            // Initialise header
            mmap[OFF_MAGIC..OFF_MAGIC + 8].copy_from_slice(&MHASH_MAGIC.to_le_bytes());
            mmap[OFF_CAPACITY..OFF_CAPACITY + 8].copy_from_slice(&cap.to_le_bytes());
            mmap[OFF_COUNT..OFF_COUNT + 8].copy_from_slice(&0u64.to_le_bytes());
            (cap, 0)
        };

        Ok(Self {
            mmap,
            _file: file,
            capacity: actual_cap,
            count: AtomicU64::new(count),
        })
    }

    // ── slot I/O ──────────────────────────────────────────────────────────────

    #[inline]
    fn slot_off(slot_idx: u64) -> usize {
        HEADER_SIZE + slot_idx as usize * SLOT_SIZE
    }

    #[inline]
    fn read_slot(&self, slot_idx: u64) -> HashSlot {
        let off = Self::slot_off(slot_idx);
        let b = &self.mmap[off..off + SLOT_SIZE];
        HashSlot {
            key:        u64::from_le_bytes(b[0..8].try_into().unwrap()),
            value:      u32::from_le_bytes(b[8..12].try_into().unwrap()),
            probe_dist: u32::from_le_bytes(b[12..16].try_into().unwrap()),
        }
    }

    #[inline]
    fn write_slot(&mut self, slot_idx: u64, s: &HashSlot) {
        let off = Self::slot_off(slot_idx);
        let b = &mut self.mmap[off..off + SLOT_SIZE];
        b[0..8].copy_from_slice(&s.key.to_le_bytes());
        b[8..12].copy_from_slice(&s.value.to_le_bytes());
        b[12..16].copy_from_slice(&s.probe_dist.to_le_bytes());
    }

    #[inline]
    fn persist_count(&mut self, count: u64) {
        self.mmap[OFF_COUNT..OFF_COUNT + 8].copy_from_slice(&count.to_le_bytes());
    }

    // ── public API ───────────────────────────────────────────────────────────

    /// O(1) lookup. Call under `RwLock::read()`.
    pub fn get(&self, key: u64) -> Option<u32> {
        if key == 0 || key == u64::MAX { return None; }
        let mask = self.capacity - 1;
        let mut pos = key & mask;
        let mut probe_dist = 0u32;

        loop {
            let slot = self.read_slot(pos);
            if slot.key == 0 { return None; }
            if slot.key == key { return Some(slot.value); }
            // Robin Hood invariant: a slot with lower probe_dist than ours can't have been
            // displaced past us, so the key isn't here.
            if slot.probe_dist < probe_dist { return None; }
            probe_dist += 1;
            pos = (pos + 1) & mask;
        }
    }

    /// O(1) amortised insert (Robin Hood). Call under `RwLock::write()`.
    pub fn insert(&mut self, key: u64, value: u32) {
        if key == 0 || key == u64::MAX { return; }
        let mask = self.capacity - 1;
        let mut pos = key & mask;
        let mut incoming = HashSlot { key, value, probe_dist: 0 };
        let mut count_bumped = false;

        loop {
            let slot = self.read_slot(pos);

            // Empty or tombstone — place here
            if slot.key == 0 || slot.key == u64::MAX {
                self.write_slot(pos, &incoming);
                if !count_bumped {
                    let n = self.count.fetch_add(1, Ordering::Relaxed) + 1;
                    self.persist_count(n);
                }
                return;
            }

            // Same key — update value in place
            if slot.key == incoming.key {
                // Only update if this is still the original key being inserted
                if !count_bumped {
                    let mut updated = slot;
                    updated.value = incoming.value;
                    self.write_slot(pos, &updated);
                    return;
                }
                // During Robin Hood displacement, a key collision with displaced entry
                // means duplicate — shouldn't happen in a correct table; just overwrite.
                let mut updated = slot;
                updated.value = incoming.value;
                self.write_slot(pos, &updated);
                return;
            }

            // Robin Hood: steal slot from entry with lower probe_dist ("the rich")
            if incoming.probe_dist > slot.probe_dist {
                self.write_slot(pos, &incoming);
                if !count_bumped {
                    let n = self.count.fetch_add(1, Ordering::Relaxed) + 1;
                    self.persist_count(n);
                    count_bumped = true;
                }
                incoming = slot;
            }

            // Safety guard: probe_dist >= capacity means the table is full
            if incoming.probe_dist >= self.capacity as u32 {
                panic!(
                    "MmapHashIndex: table full (capacity={}, count={}). \
                     Increase capacity at creation time.",
                    self.capacity,
                    self.count.load(Ordering::Relaxed)
                );
            }

            incoming.probe_dist += 1;
            pos = (pos + 1) & mask;
        }
    }

    /// O(1) remove (marks tombstone). Call under `RwLock::write()`.
    pub fn remove(&mut self, key: u64) {
        if key == 0 || key == u64::MAX { return; }
        let mask = self.capacity - 1;
        let mut pos = key & mask;
        let mut probe_dist = 0u32;

        loop {
            let slot = self.read_slot(pos);
            if slot.key == 0 { return; }
            if slot.key == key {
                let tomb = HashSlot { key: u64::MAX, value: 0, probe_dist: 0 };
                self.write_slot(pos, &tomb);
                let n = self.count.fetch_sub(1, Ordering::Relaxed).saturating_sub(1);
                // Note: fetch_sub returns old value, so saturating_sub(1) gives new value
                self.persist_count(n);
                return;
            }
            if slot.probe_dist < probe_dist { return; }
            probe_dist += 1;
            pos = (pos + 1) & mask;
        }
    }

    /// Number of live entries.
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Flush mmap pages to OS (called from SekejapDB::flush).
    pub fn flush(&self) -> io::Result<()> {
        self.mmap.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_basic_insert_get() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.mhash");
        let mut idx = MmapHashIndex::new(&path, 100).unwrap();

        idx.insert(42, 7);
        idx.insert(100, 99);

        assert_eq!(idx.get(42), Some(7));
        assert_eq!(idx.get(100), Some(99));
        assert_eq!(idx.get(999), None);
        assert_eq!(idx.count(), 2);
    }

    #[test]
    fn test_update() {
        let dir = tempdir().unwrap();
        let mut idx = MmapHashIndex::new(&dir.path().join("t.mhash"), 100).unwrap();

        idx.insert(1, 10);
        idx.insert(1, 20); // update
        assert_eq!(idx.get(1), Some(20));
        assert_eq!(idx.count(), 1);
    }

    #[test]
    fn test_remove() {
        let dir = tempdir().unwrap();
        let mut idx = MmapHashIndex::new(&dir.path().join("t.mhash"), 100).unwrap();

        idx.insert(1, 10);
        idx.insert(2, 20);
        idx.remove(1);

        assert_eq!(idx.get(1), None);
        assert_eq!(idx.get(2), Some(20));
        assert_eq!(idx.count(), 1);
    }

    #[test]
    fn test_reinsert_after_remove() {
        let dir = tempdir().unwrap();
        let mut idx = MmapHashIndex::new(&dir.path().join("t.mhash"), 100).unwrap();

        idx.insert(1, 10);
        idx.remove(1);
        idx.insert(1, 30);

        assert_eq!(idx.get(1), Some(30));
        assert_eq!(idx.count(), 1);
    }

    #[test]
    fn test_collision_handling() {
        let dir = tempdir().unwrap();
        // Use enough capacity for 32 items (64 slots → 50% load)
        let mut idx = MmapHashIndex::new(&dir.path().join("t.mhash"), 32).unwrap();

        for i in 1u64..=32 {
            idx.insert(i * 100, i as u32);
        }
        for i in 1u64..=32 {
            assert_eq!(idx.get(i * 100), Some(i as u32), "missing key {}", i * 100);
        }
    }

    #[test]
    fn test_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("persist.mhash");

        {
            let mut idx = MmapHashIndex::new(&path, 1000).unwrap();
            for i in 1u64..=500 {
                idx.insert(seahash::hash(&i.to_le_bytes()), i as u32);
            }
        } // drop → file stays on disk

        {
            let idx = MmapHashIndex::new(&path, 1000).unwrap();
            assert_eq!(idx.count(), 500);
            for i in 1u64..=500 {
                assert!(idx.get(seahash::hash(&i.to_le_bytes())).is_some());
            }
        }
    }
}
