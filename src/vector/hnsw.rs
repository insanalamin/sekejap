//! In-memory HNSW (Hierarchical Navigable Small World) graph.
//!
//! Adapted from HyperHNSW (sekejap-full) for single-threaded use with
//! HashMap-backed vector storage.  No extra dependencies.
//!
//! # Algorithm
//! Standard HNSW as described in Malkov & Yashunin (2018):
//! - Random multi-layer graph — exponentially fewer nodes at higher layers.
//! - Greedy layer descent to find the entry point for the base layer.
//! - Beam search at layer 0 to find final k-NN results.
//! - Bidirectional edge wiring + diversity-heuristic pruning.
//!
//! # Atomicity
//! `HnswGraph::build` constructs the entire graph into a local value and
//! returns it only on completion.  The caller stores it with a single
//! assignment, so the main store is never partially modified on error.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::vector::Distance;

// ── Candidate types ───────────────────────────────────────────────────────────

/// Min-heap element: smallest distance = highest priority.
#[derive(Clone, PartialEq)]
struct MinCand {
    id: u64,
    dist: f32,
}
impl Eq for MinCand {}
impl PartialOrd for MinCand {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MinCand {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse so BinaryHeap (which is max by default) acts as min-heap.
        other
            .dist
            .partial_cmp(&self.dist)
            .unwrap_or(Ordering::Equal)
            .then_with(|| self.id.cmp(&other.id))
    }
}

/// Max-heap element: largest distance = highest priority (evict the farthest).
#[derive(Clone, PartialEq)]
struct MaxCand {
    id: u64,
    dist: f32,
}
impl Eq for MaxCand {}
impl PartialOrd for MaxCand {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MaxCand {
    fn cmp(&self, other: &Self) -> Ordering {
        self.dist
            .partial_cmp(&other.dist)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.id.cmp(&self.id))
    }
}

// ── PRNG (no external dep) ────────────────────────────────────────────────────

/// Xorshift64 — maps a (node_id, counter) pair to a float in (0, 1).
/// Deterministic for the same seed.  Good enough for level selection.
#[inline]
fn random_unit(seed: u64) -> f64 {
    let mut x = seed ^ 0x9e3779b97f4a7c15;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    x = x.wrapping_mul(2685821657736338717);
    // Map 53 random bits to (0, 1).
    let f = (x >> 11) as f64 / (1u64 << 53) as f64;
    f.max(1e-15) // guard against exact 0 → -ln(0) = ∞
}

// ── HnswGraph ─────────────────────────────────────────────────────────────────

/// In-memory HNSW graph for approximate nearest-neighbour search.
///
/// Node IDs are `u64` slug hashes.  Vectors are NOT stored inside the graph;
/// they are borrowed from the caller's `HashMap<u64, Vec<f32>>` at both build
/// and search time.
#[derive(Serialize, Deserialize, Clone)]
pub struct HnswGraph {
    m: usize,
    m_max0: usize,
    level_mult: f64,
    /// node_id → layers[0..=max_level], each layer a list of neighbour IDs.
    nodes: HashMap<u64, Vec<Vec<u64>>>,
    /// (node_id, max_level) entry point.
    entry_point: Option<(u64, usize)>,
}

impl HnswGraph {
    fn new(m: usize) -> Self {
        let m = m.max(2);
        Self {
            m,
            m_max0: 2 * m,
            level_mult: 1.0 / (m as f64).ln(),
            nodes: HashMap::new(),
            entry_point: None,
        }
    }

    fn m_max(&self, level: usize) -> usize {
        if level == 0 { self.m_max0 } else { self.m }
    }

    fn pick_level(&self, node_id: u64) -> usize {
        let seed = node_id.wrapping_add((self.nodes.len() as u64).wrapping_mul(6364136223846793005));
        let r = random_unit(seed);
        (-r.ln() * self.level_mult) as usize
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// Build a new HNSW graph from all entries in `field_vecs`.
    ///
    /// The caller's data is never modified — the graph is fully constructed
    /// in a local value and returned only when complete.
    ///
    /// # Parameters
    /// - `m`: max connections per node (recommended 8–32; 16 is a good default)
    /// - `ef_construction`: beam width during build (recommended 100–400; 200 is good)
    pub fn build<D: Distance>(
        field_vecs: &HashMap<u64, Vec<f32>>,
        m: usize,
        ef_construction: usize,
    ) -> Self {
        let mut graph = Self::new(m);
        for (&node_id, _) in field_vecs {
            graph.insert_node::<D>(node_id, field_vecs, ef_construction);
        }
        graph
    }

    /// Search for the `k` approximate nearest neighbours to `query`.
    ///
    /// - `ef`: exploration factor (must be ≥ k; try `ef = k * 3` for good recall)
    /// - Returns node IDs sorted ascending by distance (closest first).
    ///
    /// Falls back gracefully: if the graph is empty the result is empty.
    pub fn search<D: Distance>(
        &self,
        query: &[f32],
        vectors: &HashMap<u64, Vec<f32>>,
        k: usize,
        ef: usize,
    ) -> Vec<u64> {
        let (mut ep_id, ep_level) = match self.entry_point {
            Some(ep) => ep,
            None => return vec![],
        };

        // Greedy descent through upper layers (ef=1 → move to nearest at each hop).
        for level in (1..=ep_level).rev() {
            let cands = search_layer::<D>(&self.nodes, query, ep_id, 1, level, vectors);
            if let Some(best) = cands.into_iter().min_by(|a, b| {
                a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal)
            }) {
                ep_id = best.id;
            }
        }

        // Beam search at layer 0.
        let ef_actual = ef.max(k);
        let mut results = search_layer::<D>(&self.nodes, query, ep_id, ef_actual, 0, vectors);
        results.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal));
        results.truncate(k);
        results.into_iter().map(|c| c.id).collect()
    }

    /// Number of nodes in the graph.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    // ── Construction internals ────────────────────────────────────────────────

    fn insert_node<D: Distance>(
        &mut self,
        node_id: u64,
        vectors: &HashMap<u64, Vec<f32>>,
        ef_construction: usize,
    ) {
        let query = match vectors.get(&node_id) {
            Some(v) => v,
            None => return,
        };

        let max_level = self.pick_level(node_id);

        // First node: set as entry point with empty layers, done.
        if self.entry_point.is_none() {
            let layers = (0..=max_level).map(|_| Vec::new()).collect();
            self.nodes.insert(node_id, layers);
            self.entry_point = Some((node_id, max_level));
            return;
        }

        let (ep_id, ep_level) = self.entry_point.unwrap();

        // Pre-insert node with empty layers so search can see it (it has no
        // neighbours yet, so it won't be traversed back to).
        {
            let layers = (0..=max_level).map(|_| Vec::new()).collect();
            self.nodes.insert(node_id, layers);
        }

        // ── Phase 1: greedy descent from ep_level to max_level+1 (ef=1) ──────
        let mut curr_ep = ep_id;
        for level in (max_level + 1..=ep_level).rev() {
            let cands =
                search_layer::<D>(&self.nodes, query, curr_ep, 1, level, vectors);
            if let Some(best) = cands.into_iter().min_by(|a, b| {
                a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal)
            }) {
                curr_ep = best.id;
            }
        }

        // ── Phase 2: connect at each shared level ─────────────────────────────
        for level in (0..=max_level.min(ep_level)).rev() {
            let cands = search_layer::<D>(
                &self.nodes,
                query,
                curr_ep,
                ef_construction,
                level,
                vectors,
            );

            // Best candidate becomes entry for the next (lower) level.
            if let Some(best) = cands.iter().min_by(|a, b| {
                a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal)
            }) {
                curr_ep = best.id;
            }

            let m_max = self.m_max(level);
            let neighbors = select_neighbors_heuristic::<D>(&cands, m_max, vectors);

            // Write the new node's neighbour list at this level.
            if let Some(node_layers) = self.nodes.get_mut(&node_id) {
                if level < node_layers.len() {
                    node_layers[level] = neighbors.clone();
                }
            }

            // Bidirectional wiring.
            for &nb_id in &neighbors {
                // We need a clone of the current neighbour list to avoid
                // simultaneous mutable+immutable borrow of self.nodes.
                let needs_pruning = {
                    if let Some(nb_layers) = self.nodes.get_mut(&nb_id) {
                        if level < nb_layers.len() {
                            nb_layers[level].push(node_id);
                            nb_layers[level].len() > m_max
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };

                if needs_pruning {
                    // Clone the vec, compute pruned list, write back.
                    let current: Vec<u64> = self
                        .nodes
                        .get(&nb_id)
                        .and_then(|ls| ls.get(level))
                        .cloned()
                        .unwrap_or_default();
                    let nb_vec = vectors.get(&nb_id).cloned().unwrap_or_default();
                    let pruned = prune_neighbors::<D>(&nb_vec, &current, m_max, vectors);
                    if let Some(nb_layers) = self.nodes.get_mut(&nb_id) {
                        if level < nb_layers.len() {
                            nb_layers[level] = pruned;
                        }
                    }
                }
            }
        }

        // ── Phase 3: promote entry point if new node reached a higher level ───
        if max_level > ep_level {
            self.entry_point = Some((node_id, max_level));
        }
    }
}

// ── Module-level search helpers ───────────────────────────────────────────────

/// Beam search restricted to one layer of the graph.
///
/// Returns all explored candidates sorted ascending by distance.
fn search_layer<D: Distance>(
    nodes: &HashMap<u64, Vec<Vec<u64>>>,
    query: &[f32],
    entry_point: u64,
    ef: usize,
    layer: usize,
    vectors: &HashMap<u64, Vec<f32>>,
) -> Vec<MinCand> {
    let d0 = match vectors.get(&entry_point) {
        Some(v) => D::eval(query, v),
        None => return vec![],
    };

    let mut visited: HashSet<u64> = HashSet::new();
    visited.insert(entry_point);

    // Min-heap: process closest candidate first.
    let mut to_visit: BinaryHeap<MinCand> = BinaryHeap::new();
    to_visit.push(MinCand { id: entry_point, dist: d0 });

    // Max-heap: keep best ef results (evict farthest when over capacity).
    let mut results: BinaryHeap<MaxCand> = BinaryHeap::new();
    results.push(MaxCand { id: entry_point, dist: d0 });

    while let Some(MinCand { id, dist: c_dist }) = to_visit.pop() {
        let worst = results.peek().map(|r| r.dist).unwrap_or(f32::INFINITY);
        if c_dist > worst && results.len() >= ef {
            break;
        }

        let neighbours = nodes
            .get(&id)
            .and_then(|ls| ls.get(layer))
            .map(|ns| ns.as_slice())
            .unwrap_or(&[]);

        for &nb in neighbours {
            if visited.contains(&nb) {
                continue;
            }
            visited.insert(nb);

            let d = match vectors.get(&nb) {
                Some(v) => D::eval(query, v),
                None => continue,
            };

            let worst = results.peek().map(|r| r.dist).unwrap_or(f32::INFINITY);
            if d < worst || results.len() < ef {
                to_visit.push(MinCand { id: nb, dist: d });
                results.push(MaxCand { id: nb, dist: d });
                if results.len() > ef {
                    results.pop(); // evict farthest
                }
            }
        }
    }

    // Convert to Vec sorted ascending by distance.
    let mut out: Vec<MinCand> = results
        .into_iter()
        .map(|mc| MinCand { id: mc.id, dist: mc.dist })
        .collect();
    out.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal));
    out
}

/// Select up to `m` diverse neighbours using the paper's simple heuristic.
///
/// Accepts a candidate whose closest already-selected neighbour is farther from
/// it than the query is.  Fills remaining slots from discarded candidates.
fn select_neighbors_heuristic<D: Distance>(
    candidates: &[MinCand],
    m: usize,
    vectors: &HashMap<u64, Vec<f32>>,
) -> Vec<u64> {
    if candidates.len() <= m {
        return candidates.iter().map(|c| c.id).collect();
    }

    // Candidates are already sorted ascending by dist to query.
    let mut result: Vec<u64> = Vec::with_capacity(m);
    let mut discarded: Vec<&MinCand> = Vec::new();

    'outer: for candidate in candidates {
        if result.len() >= m {
            break;
        }
        let cv = match vectors.get(&candidate.id) {
            Some(v) => v,
            None => continue,
        };
        // Accept only if no already-chosen neighbour is closer to this candidate
        // than the query itself.
        for &sel_id in &result {
            if let Some(sv) = vectors.get(&sel_id) {
                if D::eval(cv, sv) < candidate.dist {
                    discarded.push(candidate);
                    continue 'outer;
                }
            }
        }
        result.push(candidate.id);
    }

    // Fill remaining slots from discarded (preserve count = min(candidates, m)).
    for c in discarded {
        if result.len() >= m {
            break;
        }
        result.push(c.id);
    }

    result
}

/// Re-select neighbours for an existing node after its list grew too large.
fn prune_neighbors<D: Distance>(
    query: &[f32],
    current: &[u64],
    m: usize,
    vectors: &HashMap<u64, Vec<f32>>,
) -> Vec<u64> {
    let mut candidates: Vec<MinCand> = current
        .iter()
        .filter_map(|&id| {
            vectors
                .get(&id)
                .map(|v| MinCand { id, dist: D::eval(query, v) })
        })
        .collect();
    candidates.sort_by(|a, b| a.dist.partial_cmp(&b.dist).unwrap_or(Ordering::Equal));
    select_neighbors_heuristic::<D>(&candidates, m, vectors)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::CosineDistance;

    fn make_vecs(n: usize, dim: usize) -> HashMap<u64, Vec<f32>> {
        let mut map = HashMap::new();
        for i in 0..n {
            // Simple deterministic vectors: mostly zeros with one hot-ish component.
            let mut v = vec![0.0f32; dim];
            v[i % dim] = 1.0;
            v[(i + 1) % dim] = 0.3;
            map.insert(i as u64, v);
        }
        map
    }

    #[test]
    fn build_and_search_basic() {
        let vecs = make_vecs(20, 8);
        let graph = HnswGraph::build::<CosineDistance>(&vecs, 4, 40);
        assert_eq!(graph.len(), 20);

        // Query identical to node 0's vector → the top result must have
        // cosine distance ≈ 0 (several nodes may share the same vector due
        // to the `i % dim` construction).
        let query = vecs[&0].clone();
        let results = graph.search::<CosineDistance>(&query, &vecs, 3, 10);
        assert!(!results.is_empty());
        let top_dist = CosineDistance::eval(&query, &vecs[&results[0]]);
        assert!(
            top_dist < 1e-5,
            "top result should be at distance ~0, got {top_dist}"
        );
    }

    #[test]
    fn search_returns_at_most_k() {
        let vecs = make_vecs(50, 16);
        let graph = HnswGraph::build::<CosineDistance>(&vecs, 8, 100);
        let query = vecs[&0].clone();
        let results = graph.search::<CosineDistance>(&query, &vecs, 5, 20);
        assert!(results.len() <= 5);
    }

    #[test]
    fn empty_graph_search_is_empty() {
        let graph = HnswGraph::new(8);
        let vecs: HashMap<u64, Vec<f32>> = HashMap::new();
        let query = vec![1.0f32, 0.0, 0.0];
        let results = graph.search::<CosineDistance>(&query, &vecs, 5, 10);
        assert!(results.is_empty());
    }

    #[test]
    fn single_node_graph() {
        let mut vecs = HashMap::new();
        vecs.insert(42u64, vec![1.0f32, 0.0, 0.0]);
        let graph = HnswGraph::build::<CosineDistance>(&vecs, 4, 20);
        let results = graph.search::<CosineDistance>(&[1.0, 0.0, 0.0], &vecs, 1, 5);
        assert_eq!(results, vec![42u64]);
    }

    #[test]
    fn recall_at_10_reasonable() {
        // Build 200 random-ish vectors in 32 dims.
        let n = 200usize;
        let dim = 32usize;
        let mut vecs: HashMap<u64, Vec<f32>> = HashMap::new();
        for i in 0..n {
            let v: Vec<f32> = (0..dim)
                .map(|j| {
                    // Deterministic pseudo-random value via xorshift
                    let seed = (i as u64).wrapping_mul(6364136223846793005)
                        ^ (j as u64).wrapping_mul(1442695040888963407);
                    let x = random_unit(seed) as f32;
                    x * 2.0 - 1.0
                })
                .collect();
            vecs.insert(i as u64, v);
        }

        let graph = HnswGraph::build::<CosineDistance>(&vecs, 16, 200);
        let query = vecs[&0].clone();
        let k = 10;

        // Brute-force ground truth.
        let mut brute: Vec<(u64, f32)> = vecs
            .iter()
            .map(|(&id, v)| (id, CosineDistance::eval(&query, v)))
            .collect();
        brute.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        let ground_truth: HashSet<u64> = brute.iter().take(k).map(|(id, _)| *id).collect();

        let hnsw_results: HashSet<u64> =
            graph.search::<CosineDistance>(&query, &vecs, k, k * 3).into_iter().collect();

        let hits = ground_truth.intersection(&hnsw_results).count();
        // Expect at least 70% recall (typically >90% with m=16, ef=30).
        assert!(
            hits >= 7,
            "recall@10 too low: {hits}/10 correct (HNSW: {:?}, truth: {:?})",
            hnsw_results,
            ground_truth
        );
    }
}
