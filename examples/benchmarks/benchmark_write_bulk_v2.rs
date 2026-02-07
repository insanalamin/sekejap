//! Benchmark: Bulk Write Performance Tests v2
//!
//! This benchmark tests various bulk write scenarios including:
//! - Small, medium, and large batch sizes
//! - Different write modes (Tier 1 only, Tier 1+2)
//! - Flush performance with batch upsert
//!
//! Run with:
//! ```bash
//! cargo test --release --features vector --test benchmark_write_bulk_v2 -- --nocapture
//! ```

use sekejap::{SekejapDB, WriteOptions};
use rand::Rng;
use std::path::Path;
use std::time::Instant;
use std::fs;

/// Generate a random word/string of specified length
fn generate_random_string(length: usize) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::rng();
    let mut s = String::with_capacity(length);
    for _ in 0..length {
        let idx = rng.random_range(0..CHARSET.len());
        s.push(CHARSET[idx] as char);
    }
    s
}

/// Generate a random JSON payload
fn generate_random_payload() -> String {
    let mut rng = rand::rng();
    let id: u32 = rng.random();
    let category = generate_random_string(8);
    let action = generate_random_string(6);
    let value: f64 = rng.random_range(0.0..100.0);
    let active: bool = rng.random();

    format!(
        r#"{{"id": {}, "category": "{}", "action": "{}", "value": {:.2}, "active": {}}}"#,
        id, category, action, value, active
    )
}

/// Benchmark result structure
struct BenchmarkResult {
    name: String,
    total_writes: usize,
    duration_seconds: f64,
    rate_per_sec: f64,
}

impl BenchmarkResult {
    fn new(name: String, total_writes: usize, duration_seconds: f64) -> Self {
        Self {
            name,
            total_writes,
            duration_seconds,
            rate_per_sec: total_writes as f64 / duration_seconds,
        }
    }

    fn print(&self) {
        let rate_str = format!("{:.0}", self.rate_per_sec);
        let time_str = format!("{:.2}", self.duration_seconds);
        println!("  {:<30} | {:>10} writes/sec | {:>8}s", 
            self.name, rate_str, time_str);
    }
}

/// Helper to format rate with thousands separator
fn fmt_rate(rate: f64) -> String {
    format!("{:.0}", rate)
}

/// Test Scenario 1: Small Batch Size (100)
#[test]
fn benchmark_small_batch() {
    let test_path = "/tmp/hsdl_benchmark_small";
    let _ = fs::remove_dir_all(test_path);
    fs::create_dir_all(test_path).unwrap();

    let mut db = SekejapDB::new(Path::new(test_path)).expect("Failed to create database");
    
    let num_writes = 100_000;
    let batch_size = 100;

    let start = Instant::now();
    let mut total = 0;
    while total < num_writes {
        let end = (total + batch_size).min(num_writes);
        let mut items = Vec::with_capacity(end - total);
        for i in total..end {
            let slug = format!("small-{:06}", i);
            let payload = generate_random_payload();
            items.push((slug, payload));
        }
        db.write_batch(items, false).expect("Batch write failed");
        total = end;
    }
    
    let result = BenchmarkResult::new("Small Batch (100)".to_string(), num_writes, start.elapsed().as_secs_f64());
    result.print();

    // Flush to Tier 2
    let flush_start = Instant::now();
    let flushed = db.flush().expect("Flush failed");
    let flush_rate = flushed as f64 / flush_start.elapsed().as_secs_f64();
    println!("  {:<30} | {:>10} writes/sec", "Flush (batch upsert)", fmt_rate(flush_rate));

    drop(db);
    let _ = fs::remove_dir_all(test_path);
}

/// Test Scenario 2: Medium Batch Size (500)
#[test]
fn benchmark_medium_batch() {
    let test_path = "/tmp/hsdl_benchmark_medium";
    let _ = fs::remove_dir_all(test_path);
    fs::create_dir_all(test_path).unwrap();

    let mut db = SekejapDB::new(Path::new(test_path)).expect("Failed to create database");
    
    let num_writes = 200_000;
    let batch_size = 500;

    let start = Instant::now();
    let mut total = 0;
    while total < num_writes {
        let end = (total + batch_size).min(num_writes);
        let mut items = Vec::with_capacity(end - total);
        for i in total..end {
            let slug = format!("medium-{:06}", i);
            let payload = generate_random_payload();
            items.push((slug, payload));
        }
        db.write_batch(items, false).expect("Batch write failed");
        total = end;
    }
    
    let result = BenchmarkResult::new("Medium Batch (500)".to_string(), num_writes, start.elapsed().as_secs_f64());
    result.print();

    // Flush to Tier 2
    let flush_start = Instant::now();
    let flushed = db.flush().expect("Flush failed");
    let flush_rate = flushed as f64 / flush_start.elapsed().as_secs_f64();
    println!("  {:<30} | {:>10} writes/sec", "Flush (batch upsert)", fmt_rate(flush_rate));

    drop(db);
    let _ = fs::remove_dir_all(test_path);
}

/// Test Scenario 3: Large Batch Size (2000)
#[test]
fn benchmark_large_batch() {
    let test_path = "/tmp/hsdl_benchmark_large";
    let _ = fs::remove_dir_all(test_path);
    fs::create_dir_all(test_path).unwrap();

    let mut db = SekejapDB::new(Path::new(test_path)).expect("Failed to create database");
    
    let num_writes = 500_000;
    let batch_size = 2000;

    let start = Instant::now();
    let mut total = 0;
    while total < num_writes {
        let end = (total + batch_size).min(num_writes);
        let mut items = Vec::with_capacity(end - total);
        for i in total..end {
            let slug = format!("large-{:06}", i);
            let payload = generate_random_payload();
            items.push((slug, payload));
        }
        db.write_batch(items, false).expect("Batch write failed");
        total = end;
    }
    
    let result = BenchmarkResult::new("Large Batch (2000)".to_string(), num_writes, start.elapsed().as_secs_f64());
    result.print();

    // Flush to Tier 2
    let flush_start = Instant::now();
    let flushed = db.flush().expect("Flush failed");
    let flush_rate = flushed as f64 / flush_start.elapsed().as_secs_f64();
    println!("  {:<30} | {:>10} writes/sec", "Flush (batch upsert)", fmt_rate(flush_rate));

    drop(db);
    let _ = fs::remove_dir_all(test_path);
}

/// Test Scenario 4: Immediate Publish (write_batch with publish_now=true)
#[test]
fn benchmark_immediate_publish() {
    let test_path = "/tmp/hsdl_benchmark_immediate";
    let _ = fs::remove_dir_all(test_path);
    fs::create_dir_all(test_path).unwrap();

    let mut db = SekejapDB::new(Path::new(test_path)).expect("Failed to create database");
    
    let num_writes = 100_000;
    let batch_size = 1000;

    let start = Instant::now();
    let mut total = 0;
    while total < num_writes {
        let end = (total + batch_size).min(num_writes);
        let mut items = Vec::with_capacity(end - total);
        for i in total..end {
            let slug = format!("immediate-{:06}", i);
            let payload = generate_random_payload();
            items.push((slug, payload));
        }
        // Write to both Tier 1 and Tier 2 immediately
        db.write_batch(items, true).expect("Batch write failed");
        total = end;
    }
    
    let result = BenchmarkResult::new("Immediate Publish (1+2)".to_string(), num_writes, start.elapsed().as_secs_f64());
    result.print();

    drop(db);
    let _ = fs::remove_dir_all(test_path);
}

/// Test Scenario 5: Individual Writes with PublishNow
#[test]
fn benchmark_individual_publish_now() {
    let test_path = "/tmp/hsdl_benchmark_individual";
    let _ = fs::remove_dir_all(test_path);
    fs::create_dir_all(test_path).unwrap();

    let mut db = SekejapDB::new(Path::new(test_path)).expect("Failed to create database");
    
    let num_writes = 10_000;

    let start = Instant::now();
    for i in 0..num_writes {
        let slug = format!("ind-{:06}", i);
        let payload = generate_random_payload();
        db.write_with_options(&slug, &payload, WriteOptions {
            publish_now: true,
            ..Default::default()
        }).expect("Write failed");
    }
    
    let result = BenchmarkResult::new("Individual (publish_now)".to_string(), num_writes, start.elapsed().as_secs_f64());
    result.print();

    drop(db);
    let _ = fs::remove_dir_all(test_path);
}

/// Test Scenario 6: Staged then Flush (Optimal Pattern)
#[test]
fn benchmark_staged_then_flush() {
    let test_path = "/tmp/hsdl_benchmark_staged";
    let _ = fs::remove_dir_all(test_path);
    fs::create_dir_all(test_path).unwrap();

    let mut db = SekejapDB::new(Path::new(test_path)).expect("Failed to create database");
    
    let num_writes = 1_000_000;
    let batch_size = 1000;

    // Phase 1: Fast ingest to Tier 1
    let ingest_start = Instant::now();
    let mut total = 0;
    while total < num_writes {
        let end = (total + batch_size).min(num_writes);
        let mut items = Vec::with_capacity(end - total);
        for i in total..end {
            let slug = format!("staged-{:06}", i);
            let payload = generate_random_payload();
            items.push((slug, payload));
        }
        db.write_batch(items, false).expect("Batch write failed");
        total = end;
    }
    
    let ingest_rate = num_writes as f64 / ingest_start.elapsed().as_secs_f64();
    println!("  {:<30} | {:>10} writes/sec", "Phase 1: Ingest (Tier 1)", fmt_rate(ingest_rate));

    // Phase 2: Batch flush to Tier 2
    let flush_start = Instant::now();
    let flushed = db.flush().expect("Flush failed");
    let flush_rate = flushed as f64 / flush_start.elapsed().as_secs_f64();
    println!("  {:<30} | {:>10} writes/sec", "Phase 2: Flush (Tier 2)", fmt_rate(flush_rate));

    // Total time
    let total_time = ingest_start.elapsed().as_secs_f64();
    let total_rate = num_writes as f64 / total_time;
    let time_str = format!("{:.2}", total_time);
    println!("  {:<30} | {:>10} writes/sec | {:>8}s", "TOTAL (ingest + flush)", fmt_rate(total_rate), time_str);

    drop(db);
    let _ = fs::remove_dir_all(test_path);
}

/// Test Scenario 7: Incremental Flush (simulate auto-promotion)
#[test]
fn benchmark_incremental_flush() {
    let test_path = "/tmp/hsdl_benchmark_incremental";
    let _ = fs::remove_dir_all(test_path);
    fs::create_dir_all(test_path).unwrap();

    let mut db = SekejapDB::new(Path::new(test_path)).expect("Failed to create database");
    
    let num_writes = 500_000;
    let write_batch_size = 1000;
    let flush_interval = 50_000; // Flush every 50k writes

    let overall_start = Instant::now();
    let mut total_written = 0;
    let mut total_flushed = 0;

    while total_written < num_writes {
        // Write a batch
        let end = (total_written + write_batch_size).min(num_writes);
        let mut items = Vec::with_capacity(end - total_written);
        for i in total_written..end {
            let slug = format!("inc-{:06}", i);
            let payload = generate_random_payload();
            items.push((slug, payload));
        }
        db.write_batch(items, false).expect("Batch write failed");
        total_written = end;

        // Incremental flush
        if total_written % flush_interval == 0 || total_written == num_writes {
            let flush_start = Instant::now();
            let flushed = db.flush().expect("Flush failed");
            total_flushed += flushed;
            let flush_rate = flushed as f64 / flush_start.elapsed().as_secs_f64();
            if total_written % 100_000 == 0 {
                println!("  Wrote {} | Flushed {} ({} writes/sec)", total_written, flushed, fmt_rate(flush_rate));
            }
        }
    }
    
    let result = BenchmarkResult::new("Incremental Flush".to_string(), total_flushed, overall_start.elapsed().as_secs_f64());
    result.print();

    drop(db);
    let _ = fs::remove_dir_all(test_path);
}

/// Run all benchmarks and print summary
#[test]
fn benchmark_all_scenarios() {
    println!("\n========================================");
    println!("HSDL Bulk Write Performance Benchmarks");
    println!("========================================\n");

    // Run all tests that don't require specific output
    println!("Running scenarios...\n");

    // Small batch
    benchmark_small_batch();
    println!();

    // Medium batch
    benchmark_medium_batch();
    println!();

    // Large batch
    benchmark_large_batch();
    println!();

    // Immediate publish
    benchmark_immediate_publish();
    println!();

    // Individual with publish_now
    benchmark_individual_publish_now();
    println!();

    // Staged then flush (optimal)
    benchmark_staged_then_flush();
    println!();

    // Incremental flush
    benchmark_incremental_flush();
    println!();

    println!("========================================");
    println!("✅ All benchmarks complete!");
    println!("========================================\n");
}
