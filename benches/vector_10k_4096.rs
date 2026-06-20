//! Throughput benchmark: 10,000 vectors × 4096 dimensions.
//!
//! Inserts vectors in batches of 100 via SQL INSERT, with a declared HNSW index.
//! Prints per-batch timing to verify constant throughput (no degradation as
//! the table grows).
//!
//! Run:  cargo bench --bench vector_10k_4096
//!
//! Expected: each batch ≈ same time (O(log n) incremental HNSW insert).
//! Before fix: O(n log n) full rebuild per insert → quadratic blowup.

use sekejap::CoreDB;
use std::time::Instant;

const DIM: usize = 4096;
const TOTAL: usize = 10_000;
const BATCH: usize = 100;

fn make_vec(seed: usize) -> Vec<f32> {
    (0..DIM)
        .map(|i| {
            let x = ((seed.wrapping_mul(6364136223846793005).wrapping_add(i.wrapping_mul(1442695040888963407))) >> 33) as f32;
            (x / u32::MAX as f32) * 2.0 - 1.0
        })
        .collect()
}

fn main() {
    let mut db = CoreDB::new();

    // Create table with HNSW index on embedding field
    db.execute("CREATE TABLE vectors (_key TEXT, label TEXT, embedding VECTOR)").unwrap();
    db.execute("CREATE INDEX ON vectors USING hnsw (embedding)").unwrap();

    println!("Inserting {} vectors of {} dims in batches of {}", TOTAL, DIM, BATCH);
    println!("{:>6} {:>10} {:>10} {:>10}", "batch", "rows", "batch_ms", "avg_ms/row");
    println!("{}", "-".repeat(42));

    let total_start = Instant::now();
    let mut batch_times = Vec::with_capacity(TOTAL / BATCH);

    for batch_idx in 0..(TOTAL / BATCH) {
        let batch_start = Instant::now();
        let start = batch_idx * BATCH;
        let end = start + BATCH;

        for i in start..end {
            let vec = make_vec(i);
            let coords: String = vec.iter()
                .map(|f| format!("{:.6}", f))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "INSERT INTO vectors (_key, label, embedding) VALUES ('v{}', 'label_{}', [{}])",
                i, i, coords
            );
            db.execute(&sql).unwrap();
        }

        let batch_ms = batch_start.elapsed().as_secs_f64() * 1000.0;
        let avg_per_row = batch_ms / BATCH as f64;
        batch_times.push(batch_ms);

        println!(
            "{:>6} {:>10} {:>10.1} {:>10.3}",
            batch_idx + 1,
            end,
            batch_ms,
            avg_per_row
        );
    }

    let total_s = total_start.elapsed().as_secs_f64();

    // Summary statistics
    println!("\n{}", "=".repeat(42));
    println!("Total: {} vectors in {:.2}s", TOTAL, total_s);
    println!("Average: {:.1} ms/batch, {:.3} ms/row",
        batch_times.iter().sum::<f64>() / batch_times.len() as f64,
        total_s * 1000.0 / TOTAL as f64);

    // Check for degradation: compare first 10 batches vs last 10 batches
    let first_10: f64 = batch_times[..10].iter().sum::<f64>() / 10.0;
    let last_10: f64 = batch_times[batch_times.len()-10..].iter().sum::<f64>() / 10.0;
    let ratio = last_10 / first_10;
    println!("\nDegradation check:");
    println!("  First 10 batches avg: {:.1} ms", first_10);
    println!("  Last  10 batches avg: {:.1} ms", last_10);
    println!("  Ratio (last/first):   {:.2}x", ratio);

    if ratio < 3.0 {
        println!("  PASS — throughput is stable (< 3x degradation)");
    } else {
        println!("  FAIL — throughput degraded {:.1}x", ratio);
        std::process::exit(1);
    }

    // Verify HNSW search works
    let query_vec = make_vec(42);
    let coords: String = query_vec.iter()
        .map(|f| format!("{:.6}", f))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT _key FROM vectors WHERE VECTOR_NEAR(embedding, [{}], 10)",
        coords
    );
    let hits = db.query(&sql).unwrap().collect();
    println!("\nVector search verification: {} hits (expected 10)", hits.len());
    assert_eq!(hits.len(), 10, "VECTOR_NEAR should return 10 results");
}
