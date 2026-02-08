//! HNSW Multi-Layer Graph
//!
//! Handles the navigation topology of the index.
//! Uses Atomic pointers and Epoch-Based Reclamation for concurrency.

use crossbeam_epoch::{Atomic, Guard, Owned};
use rand::Rng;
use std::sync::atomic::Ordering;

/// List of neighbors for a specific node at a specific layer
pub struct NeighborList {
    pub data: Vec<u32>,
}

/// A node in the HNSW graph
pub struct Node {
    /// Neighbor lists for each layer (index 0 is highest layer)
    /// We use Atomic to allow lock-free reads during traversal
    pub layers: Vec<Atomic<NeighborList>>,
}

/// The Hierarchical Navigable Small World Graph
pub struct HNSWGraph {
    /// Max neighbors per node in upper layers
    m: usize,
    /// Max neighbors per node in layer 0 (usually 2 * m)
    m_max0: usize,
    /// Probability factor for layer assignment
    level_mult: f64,
    /// All nodes in the graph
    pub nodes: Vec<Node>,
    /// Entry point (node index and its max layer)
    pub entry_point: Atomic<(u32, usize)>,
}

impl HNSWGraph {
    pub fn new(m: usize) -> Self {
        Self {
            m,
            m_max0: 2 * m,
            level_mult: 1.0 / (m as f64).ln(),
            nodes: Vec::with_capacity(1000),
            entry_point: Atomic::null(),
        }
    }

    /// Randomly assign a maximum layer for a new node
    pub fn pick_level(&self) -> usize {
        let mut rng = rand::rng();
        let r: f64 = rng.random();
        let level = (-r.ln() * self.level_mult) as usize;
        level
    }

    /// Add a new node to the graph (pre-allocation)
    pub fn add_node(&mut self, max_level: usize) -> u32 {
        let index = self.nodes.len() as u32;
        let mut layers = Vec::with_capacity(max_level + 1);
        for _ in 0..=max_level {
            layers.push(Atomic::new(NeighborList { data: Vec::new() }));
        }

        self.nodes.push(Node { layers });
        index
    }

    /// Get neighbors of a node at a specific layer
    pub fn get_neighbors<'a>(
        &self,
        node_idx: u32,
        layer: usize,
        guard: &'a Guard,
    ) -> Option<&'a [u32]> {
        let node = &self.nodes[node_idx as usize];
        if layer >= node.layers.len() {
            return None;
        }

        let shared = node.layers[layer].load(Ordering::Acquire, guard);
        unsafe { shared.as_ref().map(|l| l.data.as_slice()) }
    }

    /// Atomic update of neighbor list (CAS)
    pub fn update_neighbors(
        &self,
        node_idx: u32,
        layer: usize,
        new_neighbors: Vec<u32>,
        guard: &Guard,
    ) {
        let node = &self.nodes[node_idx as usize];
        let new_list = Owned::new(NeighborList {
            data: new_neighbors,
        });

        // We can use swap if we don't care about the previous value
        // or compare_and_set if we want to be safe.
        // For HNSW construction, usually one writer per node-layer, so swap is fine.
        let old = node.layers[layer].swap(new_list, Ordering::AcqRel, guard);

        // Defer deletion of old list
        if !old.is_null() {
            unsafe {
                guard.defer_destroy(old);
            }
        }
    }
}
