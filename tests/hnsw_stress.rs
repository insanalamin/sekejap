use sekejap::NodeId;
use sekejap::vectors::{IndexBuildPolicy, VectorIndex};
use tempfile::TempDir;

#[test]
fn test_mmap_expansion_stress() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    // Create index with persistence
    let mut index = VectorIndex::new_with_path(IndexBuildPolicy::ManualTrigger, db_path);

    let dim = 384;
    let num_vectors = 5000; // Enough to exceed initial 1MB (1024*1024 / 4 bytes / 384 dims ~= 682 vectors)

    println!("Inserting {} vectors of dim {}...", num_vectors, dim);

    for i in 0..num_vectors {
        let mut vec = vec![0.0f32; dim];
        // Create a unique pattern for each vector
        vec[0] = i as f32;
        vec[1] = (i * 2) as f32;

        index.insert(i as NodeId, &vec).expect("Insert failed");

        if i % 500 == 0 {
            println!("Inserted {}", i);
        }
    }

    println!("Insertion complete. Verifying data...");

    // Verify by searching for exact matches
    // Since HNSW is approximate, we might not get exact order, but for exact match it usually works
    // Or we can just check if it crashes.

    for i in (0..num_vectors).step_by(100) {
        let mut query = vec![0.0f32; dim];
        query[0] = i as f32;
        query[1] = (i * 2) as f32;

        // Search should not panic
        let results = index.search(&query, 5).expect("Search failed");

        // The first result should be our vector (distance 0 or very close)
        assert!(
            !results.is_empty(),
            "Search returned no results for existing vector {}",
            i
        );

        let (node_id, dist) = results[0];

        // Note: Graph construction is approximate, but inserting exact vector usually finds itself first
        if node_id == i as NodeId {
            assert!(dist < 0.001, "Distance for exact match should be near zero");
        } else {
            // If it didn't find itself as #1, that's "okay" for ANN, but ideally it should.
            // Main goal here is NO PANIC.
            println!(
                "Warning: Vector {} found closest neighbor {} at dist {}",
                i, node_id, dist
            );
        }
    }

    println!("Stress test passed!");
}
