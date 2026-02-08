pub mod algo;
pub mod distance;
pub mod graph;
pub mod storage;

pub use algo::{Candidate, SearchContext, SearchResult};
pub use distance::{CosineDistance, Distance, DotProduct, L2Distance};
pub use graph::HNSWGraph;
pub use storage::VectorStore;

use crossbeam_epoch::pin;
use std::path::Path;
use std::sync::atomic::Ordering;

/// High-level Custom HNSW Engine ("Hyper-Sekejap")
pub struct HyperHNSW<D: Distance> {
    pub storage: VectorStore,
    pub graph: HNSWGraph,
    _metric: std::marker::PhantomData<D>,
}

impl<D: Distance> HyperHNSW<D> {
    pub fn new(dim: usize, m: usize) -> Self {
        Self {
            storage: VectorStore::new(dim),
            graph: HNSWGraph::new(m),
            _metric: std::marker::PhantomData,
        }
    }

    /// Insert a vector into the index
    pub fn insert(&mut self, node_id: crate::NodeId, vector: &[f32]) -> Result<(), String> {
        let new_idx = self.storage.insert(node_id, vector)?;
        let max_level = self.graph.pick_level();
        let actual_idx = self.graph.add_node(max_level);

        assert_eq!(
            new_idx, actual_idx,
            "Index mismatch between storage and graph"
        );

        let guard = pin();
        let entry_point = self.graph.entry_point.load(Ordering::Acquire, &guard);

        if entry_point.is_null() {
            // First node
            let ep = (actual_idx, max_level);
            self.graph
                .entry_point
                .store(crossbeam_epoch::Owned::new(ep), Ordering::Release);
            return Ok(());
        }

        let (mut curr_ep, curr_level) = unsafe { *entry_point.as_ref().unwrap() };
        let mut ctx = SearchContext::new(actual_idx as usize + 1);

        // 1. Traverse down to max_level
        for level in (max_level + 1..=curr_level).rev() {
            let candidates = algo::search_layer::<D>(
                vector,
                curr_ep,
                1,
                level,
                &self.graph,
                &self.storage,
                &mut ctx,
            );
            if let Some(best) = candidates.first() {
                curr_ep = best.id;
            }
        }

        // 2. Insert into layers
        for level in (0..=std::cmp::min(max_level, curr_level)).rev() {
            let ef_construction = 32; // TODO: make configurable
            let mut candidates = algo::search_layer::<D>(
                vector,
                curr_ep,
                ef_construction,
                level,
                &self.graph,
                &self.storage,
                &mut ctx,
            );

            // Select neighbors using heuristic
            let neighbors = algo::select_neighbors_heuristic::<D>(
                &self.storage,
                &mut candidates,
                self.graph.nodes[0].layers.len(),
            );

            // Update graph links
            self.graph
                .update_neighbors(actual_idx, level, neighbors.clone(), &guard);

            // Back-link: add actual_idx to neighbors' lists
            for &nb_id in &neighbors {
                let mut nb_neighbors = self
                    .graph
                    .get_neighbors(nb_id, level, &guard)
                    .map(|s| s.to_vec())
                    .unwrap_or_default();

                nb_neighbors.push(actual_idx);
                // Prune if exceeds M
                if nb_neighbors.len() > self.graph.nodes[0].layers.len() {
                    let mut cands: Vec<Candidate> = nb_neighbors
                        .iter()
                        .map(|&id| Candidate {
                            id,
                            dist: D::eval(self.storage.get(id), self.storage.get(nb_id)),
                        })
                        .collect();
                    let pruned = algo::select_neighbors_heuristic::<D>(
                        &self.storage,
                        &mut cands,
                        self.graph.nodes[0].layers.len(),
                    );
                    self.graph.update_neighbors(nb_id, level, pruned, &guard);
                } else {
                    self.graph
                        .update_neighbors(nb_id, level, nb_neighbors, &guard);
                }
            }

            if let Some(best) = candidates.first() {
                curr_ep = best.id;
            }
        }

        // Update entry point if new node is higher
        if max_level > curr_level {
            self.graph.entry_point.store(
                crossbeam_epoch::Owned::new((actual_idx, max_level)),
                Ordering::Release,
            );
        }

        Ok(())
    }

    /// Search the index
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<SearchResult> {
        let guard = pin();
        let entry_point = self.graph.entry_point.load(Ordering::Acquire, &guard);
        if entry_point.is_null() {
            return Vec::new();
        }

        let (mut curr_ep, curr_level) = unsafe { *entry_point.as_ref().unwrap() };
        let mut ctx = SearchContext::new(self.storage.len());

        for level in (1..=curr_level).rev() {
            let candidates = algo::search_layer::<D>(
                query,
                curr_ep,
                1,
                level,
                &self.graph,
                &self.storage,
                &mut ctx,
            );
            if let Some(best) = candidates.first() {
                curr_ep = best.id;
            }
        }

        let candidates =
            algo::search_layer::<D>(query, curr_ep, ef, 0, &self.graph, &self.storage, &mut ctx);

        let mut results: Vec<SearchResult> = candidates
            .into_iter()
            .map(|c| SearchResult {
                id: c.id,
                dist: c.dist,
            })
            .collect();

        results.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap());
        results.truncate(k);
        results
    }
}
