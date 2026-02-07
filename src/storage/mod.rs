pub mod ingestion;
pub mod serving;
pub mod single;
pub mod persistent;
pub mod promote;
pub mod promote_worker;
pub mod gc;
pub mod error;
pub mod persistent_graph;

pub use single::SingleStorage;
pub use persistent::PersistentStorage;
pub use persistent_graph::PersistentGraph;
pub use error::{StorageError, StorageResult};

pub use ingestion::IngestionBuffer;
pub use serving::ServingLayer;
pub use promote::{promote_node, promote_all, promote_if};
pub use promote_worker::{PromoteWorker, PromotionMetrics, WorkerCommand};
pub use gc::{GarbageCollector, GcConfig, GcMetrics};

/// Trait for batch upsert operations (enables polymorphic batch promotion)
pub trait BatchUpsert {
    /// Insert/update a batch of nodes in a single transaction
    fn upsert_batch(&self, nodes: &[crate::types::NodeHeader]) -> Result<(), Box<dyn std::error::Error>>;
}

impl BatchUpsert for PersistentStorage {
    fn upsert_batch(&self, nodes: &[crate::types::NodeHeader]) -> Result<(), Box<dyn std::error::Error>> {
        PersistentStorage::upsert_batch(self, nodes)
    }
}
