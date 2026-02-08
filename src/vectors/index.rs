//! High-performance Vector Index using Custom HNSW ("Hyper-Sekejap")
//!
//! Replaces hnsw_rs with a zero-panic, cache-optimized implementation.

use crate::types::node::NodeId;
use crate::vectors::hnsw::*;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Distance metric type for HNSW
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistanceMetric {
    /// Cosine distance - best for embeddings
    Cosine,
    /// Euclidean distance (L2)
    L2,
    /// Dot product (for pre-normalized vectors)
    Dot,
}

impl Default for DistanceMetric {
    fn default() -> Self {
        DistanceMetric::Cosine
    }
}

/// Index build policy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexBuildPolicy {
    /// Build index when database opens
    OnStartup,
    /// Build in background thread (non-blocking)
    Background,
    /// Manual trigger via `build_index()` call
    ManualTrigger,
    /// Build after tier promotion (incremental)
    OnPromotion,
}

impl Default for IndexBuildPolicy {
    fn default() -> Self {
        IndexBuildPolicy::ManualTrigger
    }
}

/// The actual engine variations
enum HNSWEngine {
    L2(HyperHNSW<L2Distance>),
    Dot(HyperHNSW<DotProduct>),
    Cosine(HyperHNSW<CosineDistance>),
}

/// HNSW Vector Index with custom performance engine
pub struct VectorIndex {
    engine: Option<HNSWEngine>,
    metric: DistanceMetric,
    policy: IndexBuildPolicy,
}

impl VectorIndex {
    pub fn new(_dim: usize, metric: DistanceMetric, policy: IndexBuildPolicy) -> Self {
        // We ignore _dim for now and initialize lazily on first insert
        Self {
            engine: None,
            metric,
            policy,
        }
    }

    pub fn new_with_path(policy: IndexBuildPolicy, _path: &Path) -> Self {
        // Default to Cosine, dimension will be detected
        Self::new(0, DistanceMetric::Cosine, policy)
    }

    pub fn insert(&mut self, node_id: NodeId, vector: &[f32]) -> Result<(), String> {
        if self.engine.is_none() {
            let dim = vector.len();
            let m = 16;
            self.engine = Some(match self.metric {
                DistanceMetric::L2 => HNSWEngine::L2(HyperHNSW::new(dim, m)),
                DistanceMetric::Dot => HNSWEngine::Dot(HyperHNSW::new(dim, m)),
                DistanceMetric::Cosine => HNSWEngine::Cosine(HyperHNSW::new(dim, m)),
            });
        }

        match self.engine.as_mut().unwrap() {
            HNSWEngine::L2(e) => e.insert(node_id, vector),
            HNSWEngine::Dot(e) => e.insert(node_id, vector),
            HNSWEngine::Cosine(e) => e.insert(node_id, vector),
        }
    }

    pub fn search(
        &self,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(NodeId, f32)>, Box<dyn std::error::Error>> {
        if self.engine.is_none() {
            return Ok(Vec::new());
        }

        let ef = 64; // Default search window
        let results = match self.engine.as_ref().unwrap() {
            HNSWEngine::L2(e) => e.search(query, k, ef),
            HNSWEngine::Dot(e) => e.search(query, k, ef),
            HNSWEngine::Cosine(e) => e.search(query, k, ef),
        };

        Ok(results
            .into_iter()
            .map(|r| {
                // Map internal u32 index back to NodeId
                let node_id = match self.engine.as_ref().unwrap() {
                    HNSWEngine::L2(e) => e.storage.id_at(r.id),
                    HNSWEngine::Dot(e) => e.storage.id_at(r.id),
                    HNSWEngine::Cosine(e) => e.storage.id_at(r.id),
                };
                (node_id, r.dist)
            })
            .collect())
    }

    pub fn is_built(&self) -> bool {
        match self.engine.as_ref() {
            Some(HNSWEngine::L2(e)) => e.storage.len() > 0,
            Some(HNSWEngine::Dot(e)) => e.storage.len() > 0,
            Some(HNSWEngine::Cosine(e)) => e.storage.len() > 0,
            None => false,
        }
    }

    pub fn is_empty(&self) -> bool {
        !self.is_built()
    }
}
