//! Edge storage backend.
//!
//! `EdgeStore` manages graph adjacency lists (forward + reverse) and edge
//! metadata.  Two modes:
//!
//! - **Fat** — edge metadata (`Option<Value>`) lives in RAM alongside the
//!   topology.  Same as the original sekejap representation.  Used for
//!   in-memory databases and when maximum edge-meta read speed is needed.
//!
//! - **Compact** — only the topology (other, type, strength) lives in RAM;
//!   edge metadata is stored in an append-only `edge_meta.bin` file read via
//!   mmap.  Cuts RAM ~2.7× per edge (64 → 24 bytes) and moves bulky JSON
//!   metadata to disk.
//!
//! The public API is identical for both modes — callers iterate `&[Edge]`
//! slices and call `edge_meta()` when (rarely) needed.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

use serde_json::Value;

/// Compact edge stored in adjacency lists.  24 bytes on 64-bit.
///
/// Used by both Fat and Compact modes — the only difference is where
/// metadata lives (RAM vs disk), pointed to by `meta_id`.
#[derive(Clone)]
pub(crate) struct Edge {
    pub other: u64,
    pub edge_type: u64,
    pub strength: f32,
    /// Index into the meta store.  `u32::MAX` = no metadata.
    meta_id: u32,
}

const NO_META: u32 = u32::MAX;

impl Edge {
    #[inline]
    pub fn has_meta(&self) -> bool {
        self.meta_id != NO_META
    }
}

/// Runtime edge-storage mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeMode {
    /// Full edge metadata in RAM (original behaviour).
    Fat,
    /// Compact topology in RAM, metadata on disk via mmap.
    Compact,
}

pub(crate) struct EdgeStore {
    /// Forward adjacency: from_hash → outgoing edges.
    fwd: HashMap<u64, Vec<Edge>>,
    /// Reverse adjacency: to_hash → incoming edges.
    rev: HashMap<u64, Vec<Edge>>,
    /// edge_type_hash → human-readable name.
    type_names: HashMap<u64, String>,
    /// Metadata backend.
    meta: MetaStore,
}

enum MetaStore {
    /// Metadata in RAM — `meta_id` indexes into `metas`.
    Ram {
        metas: Vec<Value>,
    },
    /// Metadata on disk — `meta_id` indexes into `offsets`, which point into
    /// `edge_meta.bin` via mmap.
    #[cfg(unix)]
    Disk {
        /// (byte_offset, byte_len) per meta entry.
        offsets: Vec<(u32, u16)>,
        file: std::fs::File,
        path: PathBuf,
        total_len: u64,
        mmap: Option<super::mmap::MmapView>,
    },
}

impl EdgeStore {
    // ── Constructors ─────────────────────────────────────────────────────

    /// Create an empty Fat (in-RAM) edge store.
    pub fn new_fat() -> Self {
        Self {
            fwd: HashMap::new(),
            rev: HashMap::new(),
            type_names: HashMap::new(),
            meta: MetaStore::Ram { metas: Vec::new() },
        }
    }

    /// Create an empty Compact (disk-backed meta) edge store.
    #[cfg(unix)]
    pub fn new_compact(dir: &Path) -> io::Result<Self> {
        let path = dir.join("edge_meta.bin");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Ok(Self {
            fwd: HashMap::new(),
            rev: HashMap::new(),
            type_names: HashMap::new(),
            meta: MetaStore::Disk {
                offsets: Vec::new(),
                file,
                path,
                total_len: 0,
                mmap: None,
            },
        })
    }

    /// Open an existing Compact edge store (re-reads edge_meta.bin).
    #[cfg(unix)]
    pub fn open_compact(dir: &Path) -> io::Result<Self> {
        let path = dir.join("edge_meta.bin");
        if path.exists() {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)?;
            let file_len = file.metadata()?.len();
            let mmap = super::mmap::MmapView::try_new(&file, file_len as usize);
            Ok(Self {
                fwd: HashMap::new(),
                rev: HashMap::new(),
                type_names: HashMap::new(),
                meta: MetaStore::Disk {
                    offsets: Vec::new(),
                    file,
                    path,
                    total_len: file_len,
                    mmap,
                },
            })
        } else {
            Self::new_compact(dir)
        }
    }

    // ── Mode query ───────────────────────────────────────────────────────

    pub fn mode(&self) -> EdgeMode {
        match &self.meta {
            MetaStore::Ram { .. } => EdgeMode::Fat,
            #[cfg(unix)]
            MetaStore::Disk { .. } => EdgeMode::Compact,
        }
    }

    // ── Edge insertion ───────────────────────────────────────────────────

    /// Insert an edge without metadata.
    pub fn link(
        &mut self,
        from_hash: u64,
        to_hash: u64,
        edge_type: u64,
        edge_type_name: &str,
        strength: f32,
    ) {
        self.type_names
            .insert(edge_type, edge_type_name.to_string());
        let edge_fwd = Edge {
            other: to_hash,
            edge_type,
            strength,
            meta_id: NO_META,
        };
        let edge_rev = Edge {
            other: from_hash,
            edge_type,
            strength,
            meta_id: NO_META,
        };
        self.fwd.entry(from_hash).or_default().push(edge_fwd);
        self.rev.entry(to_hash).or_default().push(edge_rev);
    }

    /// Insert an edge with metadata.
    pub fn link_meta(
        &mut self,
        from_hash: u64,
        to_hash: u64,
        edge_type: u64,
        edge_type_name: &str,
        strength: f32,
        meta: Value,
    ) {
        self.type_names
            .insert(edge_type, edge_type_name.to_string());
        let mid = self.store_meta(meta);
        let edge_fwd = Edge {
            other: to_hash,
            edge_type,
            strength,
            meta_id: mid,
        };
        let edge_rev = Edge {
            other: from_hash,
            edge_type,
            strength,
            meta_id: mid,
        };
        self.fwd.entry(from_hash).or_default().push(edge_fwd);
        self.rev.entry(to_hash).or_default().push(edge_rev);
    }

    /// Store metadata and return its id.
    fn store_meta(&mut self, meta: Value) -> u32 {
        match &mut self.meta {
            MetaStore::Ram { metas } => {
                let id = metas.len() as u32;
                metas.push(meta);
                id
            }
            #[cfg(unix)]
            MetaStore::Disk {
                offsets,
                file,
                total_len,
                ..
            } => {
                let json_bytes = serde_json::to_vec(&meta).unwrap_or_default();
                let offset = *total_len as u32;
                let len = json_bytes.len() as u16;
                use std::os::unix::fs::FileExt;
                file.write_all_at(&json_bytes, *total_len)
                    .expect("sekejap: edge meta disk write failed");
                *total_len += json_bytes.len() as u64;
                let id = offsets.len() as u32;
                offsets.push((offset, len));
                id
            }
        }
    }

    // ── Edge removal ─────────────────────────────────────────────────────

    /// Remove all edges of `edge_type` from `from_hash` to `to_hash`.
    pub fn unlink(
        &mut self,
        from_hash: u64,
        to_hash: u64,
        edge_type: u64,
    ) {
        if let Some(edges) = self.fwd.get_mut(&from_hash) {
            edges.retain(|e| !(e.other == to_hash && e.edge_type == edge_type));
        }
        if let Some(edges) = self.rev.get_mut(&to_hash) {
            edges.retain(|e| !(e.other == from_hash && e.edge_type == edge_type));
        }
        // Dead meta entries are reclaimed by compact().
    }

    /// Remove all edges involving `hash` (both directions).
    /// Returns the set of affected neighbour hashes for cascade cleanup.
    pub fn remove_node(&mut self, hash: u64) -> Vec<(u64, bool)> {
        let mut affected = Vec::new();

        // Remove forward edges: clean up reverse entries on targets.
        if let Some(fwd_edges) = self.fwd.remove(&hash) {
            for e in &fwd_edges {
                affected.push((e.other, true)); // true = was forward
                if let Some(rev) = self.rev.get_mut(&e.other) {
                    rev.retain(|r| r.other != hash);
                }
            }
        }
        // Remove reverse edges: clean up forward entries on sources.
        if let Some(rev_edges) = self.rev.remove(&hash) {
            for e in &rev_edges {
                affected.push((e.other, false)); // false = was reverse
                if let Some(fwd) = self.fwd.get_mut(&e.other) {
                    fwd.retain(|f| f.other != hash);
                }
            }
        }
        affected
    }

    // ── Edge reads ───────────────────────────────────────────────────────

    /// Outgoing edges from `hash`.
    #[inline]
    pub fn fwd_edges(&self, hash: u64) -> Option<&[Edge]> {
        self.fwd.get(&hash).map(|v| v.as_slice())
    }

    /// Incoming edges to `hash`.
    #[inline]
    pub fn rev_edges(&self, hash: u64) -> Option<&[Edge]> {
        self.rev.get(&hash).map(|v| v.as_slice())
    }

    /// Resolve metadata for an edge.  Returns `None` if the edge has no meta
    /// or if the meta could not be read.
    pub fn edge_meta(&self, edge: &Edge) -> Option<Value> {
        if edge.meta_id == NO_META {
            return None;
        }
        match &self.meta {
            MetaStore::Ram { metas } => {
                metas.get(edge.meta_id as usize).cloned()
            }
            #[cfg(unix)]
            MetaStore::Disk {
                offsets, mmap, ..
            } => {
                let &(offset, len) = offsets.get(edge.meta_id as usize)?;
                if len == 0 {
                    return None;
                }
                if let Some(ref m) = mmap {
                    let bytes = m.slice(offset as usize, len as usize)?;
                    serde_json::from_slice(bytes).ok()
                } else {
                    None
                }
            }
        }
    }

    /// Resolve edge type hash to human-readable name.
    #[inline]
    pub fn type_name(&self, type_hash: u64) -> Option<&str> {
        self.type_names.get(&type_hash).map(|s| s.as_str())
    }

    /// Insert or overwrite an edge type name.
    #[inline]
    pub fn set_type_name(&mut self, type_hash: u64, name: String) {
        self.type_names.insert(type_hash, name);
    }

    // ── Iteration & stats ────────────────────────────────────────────────

    /// Total number of edges (forward direction only — each edge counted once).
    pub fn edge_count(&self) -> usize {
        self.fwd.values().map(|v| v.len()).sum()
    }

    /// Iterate all forward adjacency entries: (from_hash, &[Edge]).
    pub fn iter_fwd(&self) -> impl Iterator<Item = (&u64, &[Edge])> {
        self.fwd.iter().map(|(k, v)| (k, v.as_slice()))
    }

    /// Access the type_names map (for snapshot serialization).
    pub fn type_names(&self) -> &HashMap<u64, String> {
        &self.type_names
    }

    // ── Compaction ───────────────────────────────────────────────────────

    /// Remap the metadata mmap to cover newly appended data.
    #[cfg(unix)]
    pub fn remap_meta(&mut self) {
        if let MetaStore::Disk {
            file,
            total_len,
            mmap,
            ..
        } = &mut self.meta
        {
            let len = *total_len as usize;
            if len == 0 {
                *mmap = None;
                return;
            }
            if let Some(ref m) = mmap {
                if m.len() >= len {
                    return;
                }
            }
            *mmap = super::mmap::MmapView::try_new(file, len);
        }
    }

    /// Compact the metadata file: rewrite with only referenced entries.
    #[cfg(unix)]
    pub fn compact_meta(&mut self) -> io::Result<()> {
        match &mut self.meta {
            MetaStore::Ram { .. } => Ok(()),
            MetaStore::Disk {
                offsets,
                file,
                path,
                total_len,
                mmap,
            } => {
                if offsets.is_empty() {
                    file.set_len(0)?;
                    *total_len = 0;
                    *mmap = None;
                    return Ok(());
                }

                // Collect live meta_ids from both fwd and rev edges.
                let mut live_ids: std::collections::HashSet<u32> =
                    std::collections::HashSet::new();
                for edges in self.fwd.values() {
                    for e in edges {
                        if e.meta_id != NO_META {
                            live_ids.insert(e.meta_id);
                        }
                    }
                }

                // Rewrite: only keep entries referenced by live edges.
                let tmp_path = path.with_extension("bin.tmp");
                let tmp_file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&tmp_path)?;

                let mut new_offsets: Vec<(u32, u16)> = Vec::with_capacity(offsets.len());
                let mut id_remap: HashMap<u32, u32> = HashMap::new();
                let mut write_pos: u64 = 0;

                use std::os::unix::fs::FileExt;
                for (old_id, &(offset, len)) in offsets.iter().enumerate() {
                    if !live_ids.contains(&(old_id as u32)) {
                        continue;
                    }
                    let new_id = new_offsets.len() as u32;
                    id_remap.insert(old_id as u32, new_id);

                    // Copy bytes from old file to new.
                    if let Some(ref m) = mmap {
                        if let Some(bytes) = m.slice(offset as usize, len as usize) {
                            tmp_file.write_all_at(bytes, write_pos)?;
                        }
                    }
                    new_offsets.push((write_pos as u32, len));
                    write_pos += len as u64;
                }

                // Atomic rename.
                std::fs::rename(&tmp_path, &*path)?;

                // Re-open and re-mmap.
                let new_file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&*path)?;
                let new_mmap =
                    super::mmap::MmapView::try_new(&new_file, write_pos as usize);

                // Update meta_ids in all edges.
                for edges in self.fwd.values_mut() {
                    for e in edges {
                        if let Some(&new_id) = id_remap.get(&e.meta_id) {
                            e.meta_id = new_id;
                        }
                    }
                }
                for edges in self.rev.values_mut() {
                    for e in edges {
                        if let Some(&new_id) = id_remap.get(&e.meta_id) {
                            e.meta_id = new_id;
                        }
                    }
                }

                *file = new_file;
                *total_len = write_pos;
                *offsets = new_offsets;
                *mmap = new_mmap;
                Ok(())
            }
        }
    }
}
