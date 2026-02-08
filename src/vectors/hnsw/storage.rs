//! Aligned contiguous vector storage
//!
//! Stores vectors in a single buffer to maximize cache locality.
//! Supports memory-mapping for zero-copy access.

use crate::NodeId;
use ahash::AHashMap;
use memmap2::{Mmap, MmapMut};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

/// Dense vector storage with alignment and optional persistence
pub struct VectorStore {
    /// Dimension of vectors
    dim: usize,
    /// Memory map for persistent storage
    mmap: Option<MmapMut>,
    /// In-memory fallback (if no file)
    buffer: Vec<f32>,
    /// Mapping: NodeId -> Internal Index
    id_map: AHashMap<NodeId, u32>,
    /// Count of stored vectors
    count: usize,
    /// Path to data file
    path: Option<PathBuf>,
}

impl VectorStore {
    /// Create a new in-memory vector store
    pub fn new(dim: usize) -> Self {
        Self {
            dim,
            mmap: None,
            buffer: Vec::new(),
            id_map: AHashMap::new(),
            count: 0,
            path: None,
        }
    }

    /// Create with memory mapping
    pub fn new_with_path(dim: usize, path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        // Initial size (e.g. 1MB)
        file.set_len(1024 * 1024)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Self {
            dim,
            mmap: Some(mmap),
            buffer: Vec::new(),
            id_map: AHashMap::new(),
            count: 0,
            path: Some(path.to_path_buf()),
        })
    }

    /// Insert a vector
    pub fn insert(&mut self, node_id: NodeId, vector: &[f32]) -> Result<u32, String> {
        if vector.len() != self.dim {
            return Err(format!(
                "Dim mismatch: expected {}, got {}",
                self.dim,
                vector.len()
            ));
        }

        let index = self.count as u32;

        if self.mmap.is_some() {
            // Check if we need to expand mmap
            let required_bytes = (self.count + 1) * self.dim * 4;
            let current_len = self.mmap.as_ref().unwrap().len();

            if required_bytes > current_len {
                self.expand_mmap(required_bytes)?;
            }

            if let Some(ref mut mmap) = self.mmap {
                unsafe {
                    let ptr = mmap.as_mut_ptr().add(self.count * self.dim * 4) as *mut f32;
                    std::ptr::copy_nonoverlapping(vector.as_ptr(), ptr, self.dim);
                }
            }
        } else {
            self.buffer.extend_from_slice(vector);
        }

        self.id_map.insert(node_id, index);
        self.count += 1;

        Ok(index)
    }

    /// Expand the memory mapped file
    fn expand_mmap(&mut self, required_bytes: usize) -> Result<(), String> {
        let path = self
            .path
            .as_ref()
            .ok_or("Cannot expand in-memory store (no path)")?;

        // Calculate new size (double current or at least required)
        let current_len = self.mmap.as_ref().unwrap().len();
        let new_len = std::cmp::max(current_len * 2, required_bytes + 1024 * 1024); // Double or +1MB

        // 1. Flush current changes
        self.mmap
            .as_ref()
            .unwrap()
            .flush()
            .map_err(|e| e.to_string())?;

        // 2. Drop current mmap to release file lock/handle
        self.mmap = None;

        // 3. Resize file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| format!("Failed to open file for resize: {}", e))?;

        file.set_len(new_len as u64)
            .map_err(|e| format!("Failed to resize file: {}", e))?;

        // 4. Re-map
        let mmap =
            unsafe { MmapMut::map_mut(&file).map_err(|e| format!("Failed to remap: {}", e))? };
        self.mmap = Some(mmap);

        Ok(())
    }

    /// Get vector by internal index
    #[inline(always)]
    pub fn get(&self, index: u32) -> &[f32] {
        let idx = index as usize * self.dim;
        if let Some(ref mmap) = self.mmap {
            unsafe {
                let ptr = mmap.as_ptr().add(idx * 4) as *const f32;
                std::slice::from_raw_parts(ptr, self.dim)
            }
        } else {
            &self.buffer[idx..idx + self.dim]
        }
    }

    /// Get internal index for a NodeId
    pub fn get_index(&self, node_id: NodeId) -> Option<u32> {
        self.id_map.get(&node_id).copied()
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Get NodeId at internal index
    pub fn id_at(&self, index: u32) -> NodeId {
        for (&node_id, &idx) in &self.id_map {
            if idx == index {
                return node_id;
            }
        }
        0
    }
}
