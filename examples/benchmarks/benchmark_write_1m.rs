//! Benchmark: 1 Million Write Test
//!
//! This test writes 1 million random entries to measure write performance.
//!
//! Run with:
//! ```bash
//! cargo test --release --features vector --test benchmark_write_1m -- --nocapture
//! ```

use hsdl_sekejap::{SekejapDB, WriteOptions};
use rand::Rng;
use std::path::Path;
use std::time::Instant;

const NUM_WRITES: usize = 1_000_000;

/// Generate a random word/string of specified length
fn generate_random_string(length: usize) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = rand::thread_rng();
    let mut s = String::with_capacity(length);
    for _ in 0..length {
        let idx = rng.gen_range(0..CHARSET.len());
        s.push(CHARSET[idx] as char);
    }
    s
}

/// Generate a random JSON payload
fn generate_random_payload() -> String {
    let mut rng = rand::thread_rng();
    let id: u32 = rng.r#gen();
    let category = generate_random_string(8);
    let action = generate_random_string(6);
    let value: f64 = rng.gen_range(0.0..100.0);
    let active: bool = rng.r#gen();

    format!(
        r#"{{"id": {}, "category": "{}", "action": "{}", "value": {:.2}, "active": {}}}"#,
        id, category, action, value, active
    )
}

/// Create WriteOptions with publish_now=true
#[inline]
fn write_options_publish_now() -> WriteOptions {
    WriteOptions {
        publish_now: true,
        #[cfg(feature = "vector")]
        vector: None,
        latitude: 0.0,
        longitude: 0.0,
        deleted: false,
        #[cfg(feature = "spatial")]
        geometry: None,
    }
}

#[test]
fn benchmark_1_million_writes() {
    // Use a temporary directory for testing
    let test_path = "/tmp/hsdl_benchmark_1m";
    let _ = std::fs::remove_dir_all(test_path); // Clean up previous run
    std::fs::create_dir_all(test_path).unwrap();

    let start = Instant::now();

    // Create database
    let mut db = SekejapDB::new(Path::new(test_path)).expect("Failed to create database");

    // Write 1 million entries with publish_now: true for immediate persistence
    println!("Starting 1,000,000 writes...");
    let mut rng = rand::thread_rng();

    for i in 0..NUM_WRITES {
        // Generate random slug and payload
        let slug = format!("event-{:08}", i);
        let payload = generate_random_payload();

        // Write with immediate publish (Tier 2)
        db.write_with_options(&slug, &payload, write_options_publish_now())
            .expect("Write failed");

        // Progress update every 100k writes
        if (i + 1) % 100_000 == 0 {
            let elapsed = start.elapsed();
            let rate = (i + 1) as f64 / elapsed.as_secs_f64();
            println!(
                "Progress: {:>7} / {} writes ({:.1}% | {:.0} writes/sec | {:.2?})",
                i + 1,
                NUM_WRITES,
                (i + 1) as f64 / NUM_WRITES as f64 * 100.0,
                rate,
                elapsed
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
    println!("BENCHMARK RESULTS: 1 Million Writes");
    println!("========================================");
    println!("Total writes:      {:>12}", NUM_WRITES);
    println!("Total time:        {:>12.2} seconds", total_seconds);
    println!("                   {:>12.4} minutes", minutes);
    println!("Write rate:        {:>12.0} writes/second", rate_per_sec);
    println!("                   {:>12.0} writes/minute", rate_per_min);
    println!("========================================\n");

    // Verify by reading a few random entries
    println!("Verifying random entries...");
    let mut verified = 0;
    for _ in 0..10 {
        let idx = rng.gen_range(0..NUM_WRITES);
        let slug = format!("event-{:08}", idx);
        if db.read(&slug).is_ok() {
            verified += 1;
        }
    }
    println!("Verified: {} / 10 random entries", verified);

    // Cleanup
    drop(db);
    let _ = std::fs::remove_dir_all(test_path);

    // Print final summary
    println!("\n✅ Benchmark complete! Time: {:.4} minutes", minutes);
}

/// Quick test with smaller number (for CI)
#[test]
fn benchmark_quick_writes() {
    let test_path = "/tmp/hsdl_benchmark_quick";
    let _ = std::fs::remove_dir_all(test_path);
    std::fs::create_dir_all(test_path).unwrap();

    let num_writes = 10_000; // Quick test with 10k
    let start = Instant::now();

    let mut db = SekejapDB::new(Path::new(test_path)).expect("Failed to create database");

    for i in 0..num_writes {
        let slug = format!("quick-{:06}", i);
        let payload = format!(r#"{{"n": {}, "data": "{}"}}"#, i, generate_random_string(16));
        db.write_with_options(&slug, &payload, write_options_publish_now())
            .expect("Write failed");
    }

    let duration = start.elapsed();
    let rate = num_writes as f64 / duration.as_secs_f64();

    println!("\n[Quick Test] {} writes in {:.2?}: {:.0} writes/sec", num_writes, duration, rate);

    drop(db);
    let _ = std::fs::remove_dir_all(test_path);
}
