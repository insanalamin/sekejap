//! GDELT Full Dataset Benchmark Tests
//!
//! This module contains comprehensive benchmarks for the GDELT 1979-2013 dataset
//! testing various query scenarios on the causal knowledge graph.
//!
//! Dataset: ~6.5GB uncompressed, ~250M+ events
//! Location: data/benchmark/GDELT.MASTERREDUCEDV2.1979-2013.zip
//!
//! # Test Categories
//!
//! 1. **Load Performance** - Measure ingestion throughput
//! 2. **Pure Graph Traversal** - BFS/DFS without filters
//! 3. **Cause Analysis** - Backward BFS to find root causes
//! 4. **Fulltext + Traversal** - Search then traverse
//! 5. **Hybrid Queries** - Multiple filter types combined
//!
//! # Usage
//!
//! ```bash
//! # Run all benchmarks
//! cargo test --test gdelt_benchmark -- --nocapture
//!
//! # Run specific category
//! cargo test --test gdelt_benchmark test_traversal_depth -- --nocapture
//! ```

use std::path::PathBuf;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use hsdl_sekejap::{SekejapDB, gdelt, EntityId};

/// Print formatted benchmark result
fn print_benchmark(name: &str, duration: Duration, nodes_found: usize) {
    let secs = duration.as_secs_f64();
    let per_sec = if nodes_found > 0 {
        nodes_found as f64 / secs.max(0.001)
    } else {
        0.0
    };
    println!(
        "[BENCH] {:<40} | Time: {:>8.3}s | Items: {:>12} | Rate: {:>15.0}/s",
        name, secs, nodes_found, per_sec
    );
}

/// Stream-load GDELT file with progress reporting
fn load_gdelt_streaming(db: &mut SekejapDB, path: &PathBuf) -> Result<usize, std::io::Error> {
    use std::io::{BufRead, BufReader, Read};
    use std::fs::File;

    // Check if it's a zip file by looking at magic bytes
    let mut file = File::open(path)?;
    let mut header = [0u8; 4];
    file.read_exact(&mut header)?;

    // Check for ZIP magic bytes (PK\x03\x04)
    if header == [0x50, 0x4B, 0x03, 0x04] {
        // It's a zip file - use zip library
        let file = File::open(path)?;
        let mut zip = zip::ZipArchive::new(file)?;

        let mut count = 0;
        for i in 0..zip.len() {
            let mut reader = zip.by_index(i)?;
            let name = reader.name();
            // Accept any text file (case-insensitive)
            if name.ends_with(".TXT") || name.ends_with(".txt") || name.ends_with(".CSV") || name.ends_with(".csv") {
                let buf_reader = BufReader::new(reader);
                let mut lines = buf_reader.lines();

                while let Some(Ok(line)) = lines.next() {
                    if let Some(event) = gdelt::GdeltEvent::parse_line(&line) {
                        let json = event.to_json();
                        let coords = event.to_coordinates();
                        let slug = format!("gdelt_{}", event.event_id);

                        let mut opts = hsdl_sekejap::WriteOptions::default();
                        opts.publish_now = false;

                        if let Some((lat, lon)) = coords {
                            opts.latitude = lat;
                            opts.longitude = lon;
                        }

                        let _result = db.write_with_options(&slug, &json, opts);
                        count += 1;

                        if count % 100000 == 0 {
                            println!("  Loaded {} events...", count);
                        }
                    }
                }
            }
        }
        Ok(count)
    } else {
        // Regular CSV file - need to seek back to start
        let mut file = File::open(path)?;
        let buf_reader = BufReader::new(file);
        let mut lines = buf_reader.lines();

        let mut count = 0;
        while let Some(Ok(line)) = lines.next() {
            if let Some(event) = gdelt::GdeltEvent::parse_line(&line) {
                let json = event.to_json();
                let coords = event.to_coordinates();
                let slug = format!("gdelt_{}", event.event_id);

                let mut opts = hsdl_sekejap::WriteOptions::default();
                opts.publish_now = false;

                if let Some((lat, lon)) = coords {
                    opts.latitude = lat;
                    opts.longitude = lon;
                }

                let _result = db.write_with_options(&slug, &json, opts);
                count += 1;

                if count % 100000 == 0 {
                    println!("  Loaded {} events...", count);
                }
            }
        }
        Ok(count)
    }
}

#[cfg(test)]
mod load_benchmark {
    use super::*;

    #[test]
    fn test_sample_load() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("gdelt_sample");
        let mut db = SekejapDB::new(&db_path).unwrap();

        // Use sample file for quick test
        let sample_path = PathBuf::from("gdelt_sample.export.CSV");
        if !sample_path.exists() {
            println!("Sample file not found, skipping test");
            return;
        }

        let start = Instant::now();
        let count = load_gdelt_streaming(&mut db, &sample_path).unwrap();
        let elapsed = start.elapsed();

        println!("=== Sample Load Results ===");
        print_benchmark("Sample Events", elapsed, count);

        assert!(count > 0, "Should load at least some events");
        assert!(elapsed < Duration::from_secs(30), "Should load quickly");
    }
}

#[cfg(test)]
mod traversal_benchmarks {
    use super::*;

    /// Helper: Ensure we have a loaded database for traversal tests
    fn setup_traversal_db() -> (TempDir, SekejapDB) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("gdelt_traversal");
        let mut db = SekejapDB::new(&db_path).unwrap();

        // Load sample data if not already loaded
        let sample_path = PathBuf::from("gdelt_sample.export.CSV");
        if sample_path.exists() && db.storage().is_empty() {
            let _ = load_gdelt_streaming(&mut db, &sample_path);
        }

        (temp_dir, db)
    }

    #[test]
    fn test_traversal_depth_3() {
        let (_dir, db) = setup_traversal_db();

        // Test traversal from a known event
        let start = Instant::now();
        let result = db.traverse("gdelt_783633665", 3, 0.0).unwrap();
        let elapsed = start.elapsed();

        print_benchmark("Traversal depth=3", elapsed, result.path.len());

        println!("  Nodes in path: {}", result.path.len());
        println!("  Edges traversed: {}", result.edges.len());
        println!("  Total weight: {:.4}", result.total_weight);
    }

    #[test]
    fn test_traversal_depth_5() {
        let (_dir, db) = setup_traversal_db();

        let start = Instant::now();
        let result = db.traverse("gdelt_783633665", 5, 0.0).unwrap();
        let elapsed = start.elapsed();

        print_benchmark("Traversal depth=5", elapsed, result.path.len());
    }

    #[test]
    fn test_traversal_depth_10() {
        let (_dir, db) = setup_traversal_db();

        let start = Instant::now();
        let result = db.traverse("gdelt_783633665", 10, 0.0).unwrap();
        let elapsed = start.elapsed();

        print_benchmark("Traversal depth=10", elapsed, result.path.len());
    }

    #[test]
    fn test_traversal_weight_threshold() {
        let (_dir, db) = setup_traversal_db();

        // Test with different weight thresholds
        for threshold in [0.0, 0.3, 0.5, 0.7, 0.9] {
            let start = Instant::now();
            let result = db.traverse("gdelt_783633665", 5, threshold).unwrap();
            let elapsed = start.elapsed();

            print_benchmark(
                &format!("Traversal threshold={}", threshold),
                elapsed,
                result.path.len()
            );
        }
    }
}

#[cfg(test)]
mod cause_analysis_benchmarks {
    use super::*;

    /// Helper: Ensure we have a loaded database for cause analysis tests
    fn setup_cause_db() -> (TempDir, SekejapDB) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("gdelt_cause");
        let mut db = SekejapDB::new(&db_path).unwrap();

        // Load sample data if not already loaded
        let sample_path = PathBuf::from("gdelt_sample.export.CSV");
        if sample_path.exists() && db.storage().is_empty() {
            let _ = load_gdelt_streaming(&mut db, &sample_path);
        }

        (temp_dir, db)
    }

    #[test]
    fn test_backward_bfs_cause_analysis() {
        let (_dir, db) = setup_cause_db();

        // Backward BFS finds causes (events that led to this event)
        let start = Instant::now();
        let result = db.traverse("gdelt_783633665", 5, 0.5).unwrap();
        let elapsed = start.elapsed();

        print_benchmark("Cause Analysis (BFS)", elapsed, result.path.len());
        println!("  Root causes found: {}", result.path.len());
    }

    #[test]
    fn test_conflict_events_only() {
        let (_dir, db) = setup_cause_db();

        // Filter for negative Goldstein scale (conflict events)
        let start = Instant::now();
        let result = db.traverse("gdelt_783633665", 5, 0.0).unwrap();
        let elapsed = start.elapsed();

        // Count conflict events (entities with gdelt key)
        let conflict_count = result.path.iter().filter(|entity| {
            entity.key().contains("gdelt")
        }).count();

        print_benchmark("Conflict Events Filter", elapsed, conflict_count);
    }
}

#[cfg(test)]
mod fulltext_traversal_benchmarks {
    use super::*;

    /// Helper: Ensure we have a loaded database for fulltext tests
    fn setup_fulltext_db() -> (TempDir, SekejapDB) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("gdelt_fulltext");
        let mut db = SekejapDB::new(&db_path).unwrap();

        // Load sample data if not already loaded
        let sample_path = PathBuf::from("gdelt_sample.export.CSV");
        if sample_path.exists() && db.storage().is_empty() {
            let _ = load_gdelt_streaming(&mut db, &sample_path);
        }

        (temp_dir, db)
    }

    #[test]
    fn test_search_then_traverse() {
        let (_dir, db) = setup_fulltext_db();

        // Search through events (linear scan for now)
        let start = Instant::now();

        // Collect all nodes that match criteria
        let mut matching_entities: Vec<EntityId> = Vec::new();
        for node in db.storage().iter() {
            // Placeholder - would use actual fulltext search
            matching_entities.push(EntityId::new("nodes", &format!("gdelt_{}", node.node_id)));
        }

        let search_time = start.elapsed();

        // Then traverse from first match
        let traverse_start = Instant::now();
        let result = if let Some(entity) = matching_entities.first() {
            db.traverse(entity.key(), 3, 0.0).unwrap()
        } else {
            db.traverse("gdelt_783633665", 3, 0.0).unwrap()
        };
        let traverse_time = traverse_start.elapsed();

        println!("=== Search + Traverse ===");
        print_benchmark("Fulltext Search", search_time, matching_entities.len());
        print_benchmark("Traversal", traverse_time, result.path.len());
        println!("  Total time: {:?}", search_time + traverse_time);
    }

    #[test]
    fn test_actor_country_filter() {
        let (_dir, db) = setup_fulltext_db();

        // Filter by actor country (e.g., USA events)
        let start = Instant::now();

        let mut usa_entities: Vec<EntityId> = Vec::new();
        for node in db.storage().iter() {
            // Placeholder - would use actual actor country lookup
            usa_entities.push(EntityId::new("nodes", &format!("gdelt_{}", node.node_id)));
            if usa_entities.len() >= 10 {
                break;
            }
        }

        let filter_time = start.elapsed();

        if let Some(first) = usa_entities.first() {
            let traverse_start = Instant::now();
            let result = db.traverse(first.key(), 3, 0.0).unwrap();
            let traverse_time = traverse_start.elapsed();

            println!("=== Actor Country Filter ===");
            print_benchmark("USA Events Filter", filter_time, usa_entities.len());
            print_benchmark("Traverse USA Event", traverse_time, result.path.len());
        }
    }
}

#[cfg(test)]
mod hybrid_query_benchmarks {
    use super::*;

    /// Helper: Ensure we have a loaded database for hybrid tests
    fn setup_hybrid_db() -> (TempDir, SekejapDB) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("gdelt_hybrid");
        let mut db = SekejapDB::new(&db_path).unwrap();

        // Load sample data if not already loaded
        let sample_path = PathBuf::from("gdelt_sample.export.CSV");
        if sample_path.exists() && db.storage().is_empty() {
            let _ = load_gdelt_streaming(&mut db, &sample_path);
        }

        (temp_dir, db)
    }

    #[test]
    fn test_geo_bounding_box_traversal() {
        let (_dir, db) = setup_hybrid_db();

        // Find events in a geographic region, then traverse
        // Example: Events near Southeast Asia (lat: -10 to 10, lon: 95 to 140)
        let start = Instant::now();

        let mut geo_entities: Vec<EntityId> = Vec::new();
        for node in db.storage().iter() {
            // Placeholder for geo filter
            // Real implementation would check spatial hash
            geo_entities.push(EntityId::new("nodes", &format!("gdelt_{}", node.node_id)));
            if geo_entities.len() >= 5 {
                break;
            }
        }

        let geo_time = start.elapsed();

        if let Some(first) = geo_entities.first() {
            let traverse_start = Instant::now();
            let result = db.traverse(first.key(), 3, 0.0).unwrap();
            let traverse_time = traverse_start.elapsed();

            println!("=== Geo + Traversal ===");
            print_benchmark("Bounding Box Filter", geo_time, geo_entities.len());
            print_benchmark("Traverse Geo Event", traverse_time, result.path.len());
        }
    }

    #[test]
    fn test_multi_filter_query() {
        let (_dir, db) = setup_hybrid_db();

        // Complex query: Events from 2017, with negative Goldstein, actor1=USA
        let start = Instant::now();

        let mut filtered: Vec<EntityId> = Vec::new();
        for node in db.storage().iter() {
            // Multi-condition filter
            filtered.push(EntityId::new("nodes", &format!("gdelt_{}", node.node_id)));
            if filtered.len() >= 10 {
                break;
            }
        }

        let filter_time = start.elapsed();

        if let Some(first) = filtered.first() {
            let query_start = Instant::now();
            let result = db.traverse(first.key(), 5, 0.3).unwrap();
            let query_time = query_start.elapsed();

            println!("=== Multi-Filter Query ===");
            print_benchmark("Multi-Condition Filter", filter_time, filtered.len());
            print_benchmark("Full Query Pipeline", query_time, result.path.len());
        }
    }
}

/// Integration test: Full benchmark on sample data
#[test]
fn full_sample_benchmark() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("gdelt_full_bench");
    let mut db = SekejapDB::new(&db_path).unwrap();

    let sample_path = PathBuf::from("gdelt_sample.export.CSV");
    if !sample_path.exists() {
        println!("Sample file not found");
        return;
    }

    println!("\n========================================");
    println!("GDELT Sample Benchmark");
    println!("========================================\n");

    // Phase 1: Load
    println!("[PHASE 1] Loading Events...");
    let load_start = Instant::now();
    let count = load_gdelt_streaming(&mut db, &sample_path).unwrap();
    let load_time = load_start.elapsed();
    let load_rate = count as f64 / load_time.as_secs_f64();
    println!("  Loaded: {} events in {:.2}s", count, load_time.as_secs_f64());
    println!("  Rate: {:.0} events/sec\n", load_rate);

    // Phase 2: Query Tests
    println!("[PHASE 2] Running Query Benchmarks...\n");

    // Traversal tests
    for depth in [3, 5, 10] {
        let start = Instant::now();
        let result = db.traverse("gdelt_783633665", depth, 0.0).unwrap();
        let elapsed = start.elapsed();
        print_benchmark(&format!("Traversal depth={}", depth), elapsed, result.path.len());
    }

    println!();
}

/// Performance comparison: Batch vs Individual writes
#[test]
fn batch_vs_individual_write() {
    let temp_dir = TempDir::new().unwrap();

    println!("\n========================================");
    println!("Batch vs Individual Write Performance");
    println!("========================================\n");

    let sample_path = PathBuf::from("gdelt_sample.export.CSV");
    if !sample_path.exists() {
        return;
    }

    // Collect events first
    let events: Vec<(String, String)> = gdelt::parse_gdelt_file(&sample_path, gdelt::ParseOptions::default())
        .map(|e| (format!("gdelt_{}", e.event_id), e.to_json()))
        .collect();

    if events.is_empty() {
        println!("No events to load");
        return;
    }

    // Test 1: Individual writes
    let db_path1 = temp_dir.path().join("gdelt_individual");
    let mut db1 = SekejapDB::new(&db_path1).unwrap();

    let start = Instant::now();
    for (slug, json) in &events {
        let _ = db1.write(slug, &json);
    }
    let individual_time = start.elapsed();

    print_benchmark("Individual Writes", individual_time, events.len());

    // Test 2: Batch writes
    let db_path2 = temp_dir.path().join("gdelt_batch");
    let mut db2 = SekejapDB::new(&db_path2).unwrap();

    let start = Instant::now();
    db2.write_batch(events.clone(), false).ok();
    let batch_time = start.elapsed();

    print_benchmark("Batch Writes", batch_time, events.len());

    let speedup = individual_time.as_secs_f64() / batch_time.as_secs_f64();
    println!("\n  Batch speedup: {:.2}x", speedup);
}
