//! HNSW Search and Insertion Algorithms
//!
//! Implements the core greedy traversal and the Select-Neighbors-Heuristic.

use crate::vectors::hnsw::distance::Distance;
use crate::vectors::hnsw::graph::{HNSWGraph, NeighborList};
use crate::vectors::hnsw::storage::VectorStore;
use crossbeam_epoch::{Guard, pin};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Neighbor candidate for priority queues
#[derive(Debug, Clone)]
pub struct Candidate {
    pub id: u32,
    pub dist: f32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.dist == other.dist
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: smaller distance has higher priority
        other
            .dist
            .partial_cmp(&self.dist)
            .unwrap_or(Ordering::Equal)
    }
}

/// Result of a search
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub id: u32,
    pub dist: f32,
}

pub struct SearchContext {
    /// Epoch-based visited set
    visited: Vec<u32>,
    current_epoch: u32,
}

impl SearchContext {
    pub fn new(capacity: usize) -> Self {
        Self {
            visited: vec![0; capacity],
            current_epoch: 0,
        }
    }

    pub fn reset(&mut self) {
        if self.current_epoch == u32::MAX {
            self.visited.fill(0);
            self.current_epoch = 1;
        } else {
            self.current_epoch += 1;
        }
    }

    pub fn is_visited(&self, id: u32) -> bool {
        if (id as usize) >= self.visited.len() {
            return false;
        }
        self.visited[id as usize] == self.current_epoch
    }

    pub fn mark_visited(&mut self, id: u32) {
        if (id as usize) >= self.visited.len() {
            // Resize if needed
            self.visited.resize((id as usize) + 1000, 0);
        }
        self.visited[id as usize] = self.current_epoch;
    }
}

pub fn search_layer<D: Distance>(
    query: &[f32],
    entry_point: u32,
    ef: usize,
    layer: usize,
    graph: &HNSWGraph,
    storage: &VectorStore,
    ctx: &mut SearchContext,
) -> Vec<Candidate> {
    ctx.reset();

    let mut candidates = BinaryHeap::new();
    let mut top_results = BinaryHeap::new(); // Max-heap for keeping top-ef

    let d = D::eval(query, storage.get(entry_point));
    let first = Candidate {
        id: entry_point,
        dist: d,
    };

    candidates.push(first.clone());
    top_results.push(ReverseCandidate(first));
    ctx.mark_visited(entry_point);

    let guard = pin();

    while let Some(current) = candidates.pop() {
        // Optimization: if current is further than worst in top_results, we can't improve
        if let Some(worst) = top_results.peek() {
            if current.dist > worst.0.dist && top_results.len() >= ef {
                break;
            }
        }

        if let Some(neighbors) = graph.get_neighbors(current.id, layer, &guard) {
            for &nb_id in neighbors {
                if !ctx.is_visited(nb_id) {
                    ctx.mark_visited(nb_id);

                    let dist = D::eval(query, storage.get(nb_id));

                    if top_results.len() < ef || dist < top_results.peek().unwrap().0.dist {
                        let cand = Candidate { id: nb_id, dist };
                        candidates.push(cand.clone());
                        top_results.push(ReverseCandidate(cand));
                        if top_results.len() > ef {
                            top_results.pop();
                        }
                    }
                }
            }
        }
    }

    top_results.into_iter().map(|rc| rc.0).collect()
}

/// Select neighbors using the diversity heuristic
pub fn select_neighbors_heuristic<D: Distance>(
    storage: &VectorStore,
    candidates: &mut Vec<Candidate>,
    m: usize,
) -> Vec<u32> {
    if candidates.len() <= m {
        return candidates.iter().map(|c| c.id).collect();
    }

    candidates.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap());

    let mut result = Vec::with_capacity(m);
    for cand in candidates.iter() {
        if result.len() >= m {
            break;
        }

        let mut is_good = true;
        for &res_id in &result {
            let d_nb = D::eval(storage.get(cand.id), storage.get(res_id));
            if d_nb < cand.dist {
                is_good = false;
                break;
            }
        }

        if is_good {
            result.push(cand.id);
        }
    }

    result
}

/// Helper for Max-heap
#[derive(PartialEq, Eq)]
struct ReverseCandidate(Candidate);

impl PartialOrd for ReverseCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReverseCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Max-heap: larger distance has higher priority
        self.0
            .dist
            .partial_cmp(&other.0.dist)
            .unwrap_or(Ordering::Equal)
    }
}
