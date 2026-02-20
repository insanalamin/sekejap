//! HNSW Multi-Layer Graph
//!
//! Handles the navigation topology of the index.
//! Uses Atomic pointers and Epoch-Based Reclamation for concurrency.

use crossbeam_epoch::{Atomic, Guard, Owned};
use rand::Rng;
use std::sync::atomic::Ordering;

pub struct NeighborList {
    pub data: Vec<u32>,
}

pub struct Node {
    pub layers: Vec<Atomic<NeighborList>>,
}

use dashmap::DashMap;



pub struct HNSWGraph {

    m: usize,

    m_max0: usize,

    level_mult: f64,

    pub nodes: DashMap<u32, Node>,

    pub entry_point: Atomic<(u32, usize)>,

}



impl HNSWGraph {

    pub fn new(m: usize) -> Self {

        Self {

            m,

            m_max0: 2 * m,

            level_mult: 1.0 / (m as f64).ln(),

            nodes: DashMap::new(),

            entry_point: Atomic::null(),

        }

    }



    pub fn pick_level(&self) -> usize {

        let mut rng = rand::thread_rng();

        let r: f64 = rng.gen();

        let level = (-r.ln() * self.level_mult) as usize;

        level

    }



    pub fn m_max(&self, level: usize) -> usize {

        if level == 0 { self.m_max0 } else { self.m }

    }



    pub fn add_node(&self, max_level: usize) -> u32 {

        let index = self.nodes.len() as u32;

        let mut layers = Vec::with_capacity(max_level + 1);

        for _ in 0..=max_level {

            layers.push(Atomic::new(NeighborList { data: Vec::new() }));

        }



        self.nodes.insert(index, Node { layers });

        index

    }

    /// Insert a node at an explicit index (for parallel batch builds).
    /// Caller guarantees `idx` is unique and pre-assigned (e.g. via fetch_add).
    pub fn add_node_at(&self, idx: u32, max_level: usize) {

        let mut layers = Vec::with_capacity(max_level + 1);

        for _ in 0..=max_level {

            layers.push(Atomic::new(NeighborList { data: Vec::new() }));

        }

        self.nodes.insert(idx, Node { layers });

    }



    pub fn get_neighbors<'a>(

        &self,

        node_idx: u32,

        layer: usize,

        guard: &'a Guard,

    ) -> Option<&'a [u32]> {

        let node = self.nodes.get(&node_idx)?;

        if layer >= node.layers.len() {

            return None;

        }



        let shared = node.layers[layer].load(Ordering::Acquire, guard);

        unsafe { shared.as_ref().map(|l| l.data.as_slice()) }

    }



    pub fn update_neighbors(

        &self,

        node_idx: u32,

        layer: usize,

        new_neighbors: Vec<u32>,

        guard: &Guard,

    ) {

        let node = self.nodes.get(&node_idx).unwrap();

        let new_list = Owned::new(NeighborList {

            data: new_neighbors,

        });



        let old = node.layers[layer].swap(new_list, Ordering::AcqRel, guard);



        if !old.is_null() {

            unsafe {

                guard.defer_destroy(old);

            }

        }

    }

}
