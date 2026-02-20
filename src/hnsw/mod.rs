pub mod algo;
pub mod distance;
pub mod graph;
pub mod storage;

pub use algo::{Candidate, SearchContext, SearchResult};
pub use distance::{CosineDistance, Distance, DotProduct, L2Distance};
pub use graph::HNSWGraph;
pub use storage::ArenaVectorStore;

use crossbeam_epoch::pin;
use std::sync::atomic::Ordering;

/// High-level Custom HNSW Engine adapted for Arena storage
pub struct HyperHNSW<D: Distance> {
    pub storage: ArenaVectorStore,
    pub graph: HNSWGraph,
    _metric: std::marker::PhantomData<D>,
}

impl<D: Distance> HyperHNSW<D> {
    pub fn new(storage: ArenaVectorStore, m: usize) -> Self {
        Self {
            storage,
            graph: HNSWGraph::new(m),
            _metric: std::marker::PhantomData,
        }
    }

    /// Insert a single node into the HNSW index (incremental, real-time).
    /// `ef_construction`: search beam width — use 32 for quality, 8 for bulk speed.
    /// Takes `&self` — all internals are already concurrent-safe (DashMap + Atomic).
    pub fn insert_index(&self, actual_idx: u32, ef_construction: usize) -> Result<(), String> {
        let vector = self.storage.get(actual_idx);
        let max_level = self.graph.pick_level();
        self.graph.add_node_at(actual_idx, max_level);

        let guard = pin();
        let entry_point = self.graph.entry_point.load(Ordering::Acquire, &guard);

        if entry_point.is_null() {
            let ep = (actual_idx, max_level);
            self.graph
                .entry_point
                .store(crossbeam_epoch::Owned::new(ep), Ordering::Release);
            return Ok(());
        }

        let (mut curr_ep, curr_level) = unsafe { *entry_point.as_ref().unwrap() };
        let mut ctx = SearchContext::new(actual_idx as usize + 1);

        // Navigate down to max_level via greedy descent (ef=1)
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

        // Connect neighbors at each level from max_level down to 0
        for level in (0..=std::cmp::min(max_level, curr_level)).rev() {
            curr_ep = self.connect_at_level(actual_idx, vector, curr_ep, level, ef_construction);
        }

        if max_level > curr_level {
            self.graph.entry_point.store(
                crossbeam_epoch::Owned::new((actual_idx, max_level)),
                Ordering::Release,
            );
        }

        Ok(())
    }

    /// Connect a pre-initialized node's neighbors at a single level.
    /// For parallel batch builds: node was already inserted via `add_node_at`,
    /// entry_point was already set. Each call is independent and parallel-safe.
    pub fn connect_neighbors(&self, actual_idx: u32, level: usize, ef: usize) -> Result<(), String> {
        let guard = pin();
        let entry_point = self.graph.entry_point.load(Ordering::Acquire, &guard);
        if entry_point.is_null() {
            return Ok(());
        }
        let (curr_ep, _) = unsafe { *entry_point.as_ref().unwrap() };
        let vector = self.storage.get(actual_idx);
        self.connect_at_level(actual_idx, vector, curr_ep, level, ef);
        Ok(())
    }

    /// Core neighbor-search-and-wire for one level.
    /// Searches from `curr_ep`, selects m_max best candidates, updates both directions.
    /// Returns the best candidate found (useful for cascading down levels).
    fn connect_at_level(
        &self,
        actual_idx: u32,
        vector: &[f32],
        curr_ep: u32,
        level: usize,
        ef: usize,
    ) -> u32 {
        let mut ctx = SearchContext::new(actual_idx as usize + 1);
        let mut candidates = algo::search_layer::<D>(
            vector,
            curr_ep,
            ef,
            level,
            &self.graph,
            &self.storage,
            &mut ctx,
        );

        let m_max = self.graph.m_max(level);
        let neighbors = algo::select_neighbors_heuristic::<D>(
            &self.storage,
            &mut candidates,
            m_max,
        );

        let guard = pin();
        self.graph
            .update_neighbors(actual_idx, level, neighbors.clone(), &guard);

        for &nb_id in &neighbors {
            let mut nb_neighbors = self
                .graph
                .get_neighbors(nb_id, level, &guard)
                .map(|s| s.to_vec())
                .unwrap_or_default();

            nb_neighbors.push(actual_idx);
            if nb_neighbors.len() > m_max {
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
                    m_max,
                );
                self.graph.update_neighbors(nb_id, level, pruned, &guard);
            } else {
                self.graph
                    .update_neighbors(nb_id, level, nb_neighbors, &guard);
            }
        }

        candidates.first().map(|c| c.id).unwrap_or(curr_ep)
    }

    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<SearchResult> {
        let guard = pin();
        let entry_point = self.graph.entry_point.load(Ordering::Acquire, &guard);
        if entry_point.is_null() {
            return Vec::new();
        }

        let (mut curr_ep, curr_level) = unsafe { *entry_point.as_ref().unwrap() };
        let mut ctx = SearchContext::new(self.storage.len() + 1);

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
