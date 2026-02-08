//! Database Maintenance: Promotion Worker and Garbage Collection
//!
//! Demonstrates:
//! - Auto-promotion of nodes from Tier 1 to Tier 2
//! - Manual flushing of staged nodes
//! - Garbage collection of deleted nodes
//! - Metrics monitoring

use sekejap::{GcConfig, SekejapDB, WriteOptions};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = Path::new("./data-maintenance");
    let mut db = SekejapDB::new(temp_dir)?;

    println!("=== Database Maintenance Example ===\n");

    // Write some events to Tier 1 (staged)
    println!("1. Writing events to Tier 1 (staged)...");
    for i in 0..10 {
        db.write(
            &format!("event-{}", i),
            &format!(
                r#"{{"title": "Event {}", "coordinates": {{"lat": -6.2, "lon": 106.8}}}}"#,
                i
            ),
        )?;
    }
    println!("   ✓ Wrote 10 events to Tier 1\n");

    // Show initial promotion metrics
    println!("2. Initial promotion metrics:");
    let metrics = db.promotion_metrics();
    println!("   - Total promoted: {}", metrics.total_promoted);
    println!("   - Buffer size: {} bytes\n", metrics.buffer_size_bytes);

    // Manually flush to Tier 2
    println!("3. Manually flushing to Tier 2...");
    let promoted = db.flush()?;
    println!("   ✓ Flushed {} nodes to Tier 2\n", promoted);

    // Show updated metrics
    println!("4. Updated promotion metrics:");
    let metrics = db.promotion_metrics();
    println!("   - Total promoted: {}", metrics.total_promoted);
    println!(
        "   - Promotion rate: {:.2} nodes/sec",
        metrics.promotion_rate
    );
    println!("   - Avg latency: {:.2} ms\n", metrics.avg_latency_ms);

    // Write more events to Tier 2 (immediate)
    println!("5. Writing events directly to Tier 2 (immediate)...");
    for i in 10..20 {
        db.write_with_options(
            &format!("event-{}", i),
            &format!(
                r#"{{"title": "Event {}", "coordinates": {{"lat": -6.3, "lon": 106.9}}}}"#,
                i
            ),
            WriteOptions {
                publish_now: true,
                ..Default::default()
            },
        )?;
    }
    println!("   ✓ Wrote 10 events directly to Tier 2\n");

    // Demonstrate deletion with retention
    println!("6. Deleting some events (marks as tombstones)...");
    db.delete("event-0")?;
    db.delete("event-1")?;
    db.delete("event-2")?;
    println!("   ✓ Deleted 3 events (marked as tombstones)\n");

    // Show GC configuration
    println!("7. Default GC configuration:");
    let gc_config = GcConfig::default();
    println!(
        "   - Deletion retention: {} days",
        gc_config.deletion_retention_sec / 86400
    );
    println!("   - Max versions: {}", gc_config.max_versions);
    println!(
        "   - GC interval: {} hours",
        gc_config.gc_interval_sec / 3600
    );
    println!(
        "   - Max deletes per run: {}",
        gc_config.max_deletes_per_run
    );
    println!("   - Always keep head: {}\n", gc_config.always_keep_head);

    // Note: GC runs automatically in background, but we're using short interval
    // In production, GC would run daily by default
    println!("8. Garbage Collection:");
    println!("   Note: GC runs automatically based on configuration.");
    println!("   Deleted nodes will be physically removed after retention period.\n");

    // Demonstrate version updates
    println!("9. Creating multiple versions (updates)...");
    db.update(
        "event-3",
        r#"{"title": "Event 3 (Updated)", "coordinates": {"lat": -6.2, "lon": 106.8}}"#,
    )?;
    db.update(
        "event-3",
        r#"{"title": "Event 3 (Updated Again)", "coordinates": {"lat": -6.2, "lon": 106.8}}"#,
    )?;
    db.update(
        "event-3",
        r#"{"title": "Event 3 (Final)", "coordinates": {"lat": -6.2, "lon": 106.8}}"#,
    )?;
    println!("   ✓ Created 3 versions for event-3");
    println!("   Old versions will be compacted to max_versions limit\n");

    // Final stats
    println!("10. Final database state:");
    println!("    - Total events: 20");
    println!("    - Deleted (tombstones): 3");
    println!("    - Multiple versions: 1 (event-3)\n");

    println!("=== Summary ===");
    println!("✓ Promotion worker manages Tier 1 → Tier 2 flow");
    println!("✓ Manual flush() forces immediate promotion");
    println!("✓ GC automatically cleans up deleted nodes and old versions");
    println!("✓ Metrics monitor system health\n");

    Ok(())
}
