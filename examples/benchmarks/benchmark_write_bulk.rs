//! Benchmark: 1 Million Bulk Write Test
//!
//! This test writes 1 million entries using write_batch() API to compare
//! performance with individual writes.
//!
//! Run with:
//! ```bash
//! cargo test --release --features vector --test benchmark_write_bulk -- --nocapture
//! ```

use sekejap::SekejapDB;
use rand::Rng;
use std::path::Path;
use std::time::Instant;

const NUM_WRITES: usize = 1_000_000;
const BATCH_SIZE: usize = 1000;

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

/// Write using Tier 1 only (fastest for bulk ingest)
fn write_to_tier1(db: &mut SekejapDB, items: Vec<(String, String)>) {
    db.write_batch(items, false).expect("Batch write failed");
}

/// Write to both Tier 1 and Tier 2 (slower, but data is immediately queryable)
fn write_to_tier2(db: &mut SekejapDB, items: Vec<(String, String)>) {
    db.write_batch(items, true).expect("Batch write failed");
}

#[test]
fn benchmark_1_million_bulk_writes() {
    // Use a temporary directory for testing
    let test_path = "/tmp/hsdl_benchmark_bulk_1m";
    let _ = std::fs::remove_dir_all(test_path);
    std::fs::create_dir_all(test_path).unwrap();

    let start = Instant::now();

    // Create database
    let mut db = SekejapDB::new(Path::new(test_path)).expect("Failed to create database");

    println!("Starting 1,000,000 BULK writes (batch size: {})...", BATCH_SIZE);
    println!("Writing to Tier 1 only (fast ingest mode)...\n");

    let mut rng = rand::rng();
    let mut total_written = 0;

    // Build batches and write to Tier 1 only
    while total_written < NUM_WRITES {
        let batch_end = (total_written + BATCH_SIZE).min(NUM_WRITES);
        let batch_size = batch_end - total_written;

        // Build this batch
        let mut items = Vec::with_capacity(batch_size);
        for i in total_written..batch_end {
            let slug = format!("bulk-{:08}", i);
            let payload = generate_random_payload();
            items.push((slug, payload));
        }

        // Write batch to Tier 1 (fast!)
        write_to_tier1(&mut db, items);

        total_written = batch_end;

        // Progress update every 100k writes
        if total_written % 100_000 == 0 || total_written == NUM_WRITES {
            let elapsed = start.elapsed();
            let rate = total_written as f64 / elapsed.as_secs_f64();
            let percent = total_written as f64 / NUM_WRITES as f64 * 100.0;
            println!(
                "Progress: {:>7} / {} writes ({:.1}% | {:.0} writes/sec | {:.2?})",
                total_written, NUM_WRITES, percent, rate, elapsed
            );
        }
    }

    let total_duration = start.elapsed();

    // Calculate statistics
    let total_seconds = total_duration.as_secs_f64();
    let minutes = total_seconds / 60.0;
    let rate_per_sec = NUM_WRITES as f64 / total_seconds;
    let rate_per_min = NUM_WRITES as f64 / minutes;

    println!("\n========================================");
    println!("BENCHMARK RESULTS: 1 Million Bulk Writes");
    println!("========================================");
    println!("Mode:              {:>12}", "Tier 1 only");
    println!("Batch size:        {:>12}", BATCH_SIZE);
    println!("Total writes:      {:>12}", NUM_WRITES);
    println!("Total time:        {:>12.2} seconds", total_seconds);
    println!("                   {:>12.4} minutes", minutes);
    println!("Write rate:        {:>12.0} writes/second", rate_per_sec);
    println!("                   {:>12.0} writes/minute", rate_per_min);
    println!("========================================\n");

    // Flush to Tier 2 (simulate what happens during promotion)
    println!("Flushing to Tier 2 (promotion)...");
    let flush_start = Instant::now();
    let flushed = db.flush().expect("Flush failed");
    let flush_duration = flush_start.elapsed();
    let flush_rate = flushed as f64 / flush_duration.as_secs_f64();
    println!("Flushed {} nodes in {:.2?} ({:.0} writes/sec)", flushed, flush_duration, flush_rate);

    // Verify by reading a few random entries (now from Tier 2)
    println!("\nVerifying random entries...");
    let mut verified = 0;
    for _ in 0..10 {
        let idx = rng.random_range(0..NUM_WRITES);
        let slug = format!("bulk-{:08}", idx);
        if db.read(&slug).is_ok() {
            verified += 1;
        }
    }
    println!("Verified: {} / 10 random entries", verified);

    // Cleanup
    drop(db);
    let _ = std::fs::remove_dir_all(test_path);

    println!("\n✅ Bulk benchmark complete! Time: {:.4} minutes", minutes);
}

/// Quick test with smaller number - tests both Tier 1 and Tier 2 modes
#[test]
fn benchmark_quick_bulk_writes() {
    let test_path = "/tmp/hsdl_benchmark_bulk_quick";
    let _ = std::fs::remove_dir_all(test_path);
    std::fs::create_dir_all(test_path).unwrap();

    let num_writes = 10_000;
    let batch_size = 100;

    let mut db = SekejapDB::new(Path::new(test_path)).expect("Failed to create database");

    // Test 1: Tier 1 only (fast ingest)
    let start = Instant::now();
    let mut total_written = 0;
    while total_written < num_writes {
        let batch_end = (total_written + batch_size).min(num_writes);
        let mut items = Vec::with_capacity(batch_end - total_written);
        for i in total_written..batch_end {
            let slug = format!("tier1-{:06}", i);
            let payload = format!(r#"{{"n": {}, "data": "{}"}}"#, i, generate_random_string(16));
            items.push((slug, payload));
        }
        db.write_batch(items, false).expect("Batch write failed");
        total_written = batch_end;
    }
    let duration = start.elapsed();
    let rate = num_writes as f64 / duration.as_secs_f64();

    println!(
        "\n[Quick Bulk Test] {} writes (batch={}, Tier 1) in {:.2?}: {:.0} writes/sec",
        num_writes, batch_size, duration, rate
    );

    // Flush to Tier 2
    let _ = db.flush().expect("Flush failed");

    // Test 2: Both tiers (immediate queryable)
    let start = Instant::now();
    total_written = 0;
    while total_written < num_writes {
        let batch_end = (total_written + batch_size).min(num_writes);
        let mut items = Vec::with_capacity(batch_end - total_written);
        for i in total_written..batch_end {
            let slug = format!("tier2-{:06}", i);
            let payload = format!(r#"{{"n": {}, "data": "{}"}}"#, i, generate_random_string(16));
            items.push((slug, payload));
        }
        db.write_batch(items, true).expect("Batch write failed");
        total_written = batch_end;
    }
    let duration = start.elapsed();
    let rate = num_writes as f64 / duration.as_secs_f64();

    println!(
        "[Quick Bulk Test] {} writes (batch={}, Tier 1+2) in {:.2?}: {:.0} writes/sec",
        num_writes, batch_size, duration, rate
    );

    drop(db);
    let _ = std::fs::remove_dir_all(test_path);
}

/// Compare write modes: individual vs batch, Tier 1 vs Tier 2
#[test]
fn benchmark_comparison_write_modes() {
    let test_path = "/tmp/hsdl_benchmark_comparison";
    let _ = std::fs::remove_dir_all(test_path);
    std::fs::create_dir_all(test_path).unwrap();

    let num_writes = 50_000;
    let batch_size = 500;

    // Test 1: Individual writes with publish_now=true
    {
        let mut db = SekejapDB::new(Path::new(test_path)).expect("Failed to create database");
        let start = Instant::now();
        for i in 0..num_writes {
            let slug = format!("ind-{:06}", i);
            let payload = format!(r#"{{"n": {}}}"#, i);
            db.write_with_options(&slug, &payload, sekejap::WriteOptions {
                publish_now: true,
                ..Default::default()
            }).expect("Write failed");
        }
        let duration = start.elapsed();
        let rate = num_writes as f64 / duration.as_secs_f64();
        println!(
            "[Comparison] Individual writes (Tier 1+2): {:.0} writes/sec ({:.2?})",
            rate, duration
        );
        drop(db);
        let _ = std::fs::remove_dir_all(test_path);
        std::fs::create_dir_all(test_path).unwrap();
    }

    // Test 2: Batch writes to Tier 1 only
    {
        let mut db = SekejapDB::new(Path::new(test_path)).expect("Failed to create database");
        let start = Instant::now();
        let mut total = 0;
        while total < num_writes {
            let end = (total + batch_size).min(num_writes);
            let mut items = Vec::with_capacity(end - total);
            for i in total..end {
                items.push((format!("batch-{:06}", i), format!(r#"{{"n": {}}}"#, i)));
            }
            db.write_batch(items, false).expect("Batch write failed");
            total = end;
        }
        let duration = start.elapsed();
        let rate = num_writes as f64 / duration.as_secs_f64();
        println!(
            "[Comparison] Batch writes (Tier 1 only):    {:.0} writes/sec ({:.2?})",
            rate, duration
        );
        drop(db);
        let _ = std::fs::remove_dir_all(test_path);
        std::fs::create_dir_all(test_path).unwrap();
    }

    // Test 3: Batch writes to both tiers
    {
        let mut db = SekejapDB::new(Path::new(test_path)).expect("Failed to create database");
        let start = Instant::now();
        let mut total = 0;
        while total < num_writes {
            let end = (total + batch_size).min(num_writes);
            let mut items = Vec::with_capacity(end - total);
            for i in total..end {
                items.push((format!("batch-{:06}", i), format!(r#"{{"n": {}}}"#, i)));
            }
            db.write_batch(items, true).expect("Batch write failed");
            total = end;
        }
        let duration = start.elapsed();
        let rate = num_writes as f64 / duration.as_secs_f64();
        println!(
            "[Comparison] Batch writes (Tier 1+2):       {:.0} writes/sec ({:.2?})",
            rate, duration
        );
        drop(db);
    }

    let _ = std::fs::remove_dir_all(test_path);
}
