pub mod error;
pub mod gc;
pub mod ingestion;
pub mod persistent;
pub mod persistent_graph;
pub mod promote;
pub mod promote_worker;
pub mod serving;
pub mod single;

pub use error::{StorageError, StorageResult};
pub use persistent::PersistentStorage;
pub use persistent_graph::PersistentGraph;
pub use single::SingleStorage;

pub use gc::{GarbageCollector, GcConfig, GcMetrics};
pub use ingestion::IngestionBuffer;
pub use promote::{promote_all, promote_if, promote_node};
pub use promote_worker::{PromoteWorker, PromotionMetrics, WorkerCommand};
pub use serving::ServingLayer;

/// Trait for batch upsert operations (enables polymorphic batch promotion)
pub trait BatchUpsert {
    /// Insert/update a batch of nodes in a single transaction
    fn upsert_batch(
        &self,
        nodes: &[crate::types::NodeHeader],
    ) -> Result<(), Box<dyn std::error::Error>>;
}

impl BatchUpsert for PersistentStorage {
    fn upsert_batch(
        &self,
        nodes: &[crate::types::NodeHeader],
    ) -> Result<(), Box<dyn std::error::Error>> {
        PersistentStorage::upsert_batch(self, nodes)
    }
}

impl BatchUpsert for SingleStorage {
    fn upsert_batch(
        &self,
        nodes: &[crate::types::NodeHeader],
    ) -> Result<(), Box<dyn std::error::Error>> {
        SingleStorage::upsert_batch(self, nodes)
    }
}
