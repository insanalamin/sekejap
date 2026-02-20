/// M7: Scale — 100K node ingest, collection O(1), field index correctness, low RAM proxy
///
/// These tests verify performance characteristics at moderate scale.
/// (Full 50M test would be too slow for CI; 100K demonstrates the same patterns.)

use sekejap::SekejapDB;
use serde_json::json;
use std::time::Instant;
use tempfile::tempdir;

/// Returns current process RSS in MB (macOS/Linux via `ps`).
fn rss_mb() -> u64 {
    let pid = std::process::id();
    let stdout = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .map(|o| o.stdout)
        .unwrap_or_default();
    let kb: u64 = String::from_utf8_lossy(&stdout)
        .trim()
        .parse()
        .unwrap_or(0);
    kb / 1024
}

const SCALE: usize = 1_000_000;

/// Helper: create a DB with the given node count capacity.
fn make_db(count: usize) -> (SekejapDB, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let db = SekejapDB::new(dir.path(), count).unwrap();
    (db, dir)
}

/// Ingest N nodes split across two collections and return the time taken.
fn ingest_nodes(db: &SekejapDB, n: usize) -> std::time::Duration {
    let items: Vec<(String, String)> = (0..n)
        .map(|i| {
            let collection = if i % 2 == 0 { "citizens" } else { "services" };
            let slug = format!("{}/{}", collection, i);
            let json = format!(
                r#"{{"_id":"{}","idx":{},"status":"{}","age":{}}}"#,
                slug,
                i,
                if i % 3 == 0 { "active" } else { "inactive" },
                20 + (i % 60) as u32,
            );
            (slug, json)
        })
        .collect();

    let refs: Vec<(&str, &str)> = items.iter()
        .map(|(s, j)| (s.as_str(), j.as_str()))
        .collect();

    let t = Instant::now();
    db.nodes().ingest(&refs).unwrap();
    t.elapsed()
}

// ── Ingest benchmark ──────────────────────────────────────────────────────────

#[test]
fn test_ingest_100k_nodes() {
    let rss_before = rss_mb();
    let (db, _dir) = make_db(SCALE + 1024);
    let elapsed = ingest_nodes(&db, SCALE);
    let rss_after = rss_mb();

    println!(
        "Ingest {} nodes: {:?}  |  RAM before={} MB  after={} MB  delta=+{} MB",
        SCALE, elapsed, rss_before, rss_after,
        rss_after.saturating_sub(rss_before)
    );

    // Verify count
    let total = db.nodes().all().count().unwrap();
    assert_eq!(total.data, SCALE, "expected {} nodes after ingest", SCALE);
}

// ── Collection bitmap O(1) ────────────────────────────────────────────────────

#[test]
fn test_collection_bitmap_is_fast() {
    let (db, _dir) = make_db(SCALE + 1024);
    ingest_nodes(&db, SCALE);

    let expected_citizens = SCALE / 2;

    // First call may load from memory (already in DashMap after ingest)
    let t = Instant::now();
    let outcome = db.nodes().collection("citizens").count().unwrap();
    let elapsed = t.elapsed();

    println!("collection('citizens') count={} in {:?}", outcome.data, elapsed);
    assert_eq!(outcome.data, expected_citizens, "wrong citizen count");

    // Should be sub-millisecond (bitmap op, not scan)
    assert!(
        elapsed.as_millis() < 50,
        "collection() took {:?} — expected < 50ms (bitmap should be ~instant)",
        elapsed
    );

    // Verify trace says collection_bitmap
    let used_bitmap = outcome.trace.steps.iter()
        .any(|s| s.index_used == "collection_bitmap");
    assert!(used_bitmap, "expected collection_bitmap index in trace, got: {:?}",
        outcome.trace.steps.iter().map(|s| &s.index_used).collect::<Vec<_>>());
}

// ── Slug lookup (MmapHashIndex) ───────────────────────────────────────────────

#[test]
fn test_slug_lookup_100k() {
    let (db, _dir) = make_db(SCALE + 1024);
    ingest_nodes(&db, SCALE);

    // Spot checks at known-valid indices (even → citizens, odd → services)
    let last_even = ((SCALE - 2) / 2) * 2;  // largest even index < SCALE
    let last_odd  = ((SCALE - 1) / 2) * 2 + 1; // largest odd index < SCALE
    let slugs = [
        format!("citizens/{}", 0),
        format!("services/{}", 1),
        format!("citizens/{}", last_even),
        format!("services/{}", last_odd),
    ];
    for slug in &slugs {
        let result = db.nodes().get(slug);
        assert!(result.is_some(), "slug {} not found after {}K ingest", slug, SCALE / 1000);
    }
}

// ── Field index at scale ──────────────────────────────────────────────────────

#[test]
fn test_hash_index_at_scale() {
    let n = 10_000usize;
    let (db, _dir) = make_db(n + 1024);

    // Define schema BEFORE ingest to activate index
    db.schema().define("items", r#"{
        "hot_fields": {
            "hash_index": ["status"],
            "range_index": [],
            "vector": [],
            "spatial": [],
            "fulltext": []
        }
    }"#).unwrap();

    let items: Vec<(String, String)> = (0..n)
        .map(|i| {
            let slug = format!("items/{}", i);
            let status = if i % 4 == 0 { "active" } else { "inactive" };
            let json = format!(r#"{{"_id":"{}","status":"{}"}}"#, slug, status);
            (slug, json)
        })
        .collect();
    let refs: Vec<(&str, &str)> = items.iter().map(|(s, j)| (s.as_str(), j.as_str())).collect();
    db.nodes().ingest(&refs).unwrap();

    let t = Instant::now();
    let outcome = db.nodes().all()
        .where_eq("status", json!("active"))
        .count().unwrap();
    let elapsed = t.elapsed();

    println!("where_eq('status','active') at {}K: count={} in {:?}", n / 1000, outcome.data, elapsed);
    assert_eq!(outcome.data, n / 4, "expected {} active items", n / 4);
}

#[test]
fn test_range_index_at_scale() {
    let n = 10_000usize;
    let (db, _dir) = make_db(n + 1024);

    db.schema().define("products", r#"{
        "hot_fields": {
            "hash_index": [],
            "range_index": ["price"],
            "vector": [],
            "spatial": [],
            "fulltext": []
        }
    }"#).unwrap();

    let items: Vec<(String, String)> = (0..n)
        .map(|i| {
            let slug = format!("products/{}", i);
            // prices 1..=10000
            let json = format!(r#"{{"_id":"{}","price":{}}}"#, slug, i + 1);
            (slug, json)
        })
        .collect();
    let refs: Vec<(&str, &str)> = items.iter().map(|(s, j)| (s.as_str(), j.as_str())).collect();
    db.nodes().ingest(&refs).unwrap();

    let t = Instant::now();
    let outcome = db.nodes().all()
        .where_between("price", 1000.0, 5000.0)
        .count().unwrap();
    let elapsed = t.elapsed();

    println!("where_between('price',1000,5000) at {}K: count={} in {:?}", n / 1000, outcome.data, elapsed);
    // prices 1000..=5000 → 4001 items
    assert_eq!(outcome.data, 4001, "expected 4001 products in [1000,5000]");
}

// ── Persistence at scale ──────────────────────────────────────────────────────

#[test]
fn test_persist_and_reopen_at_scale() {
    let dir = tempdir().unwrap();
    let n = 5_000usize;

    {
        let db = SekejapDB::new(dir.path(), n + 64).unwrap();
        let items: Vec<(String, String)> = (0..n)
            .map(|i| {
                let slug = format!("persist/{}", i);
                let json = format!(r#"{{"_id":"{}","seq":{}}}"#, slug, i);
                (slug, json)
            })
            .collect();
        let refs: Vec<(&str, &str)> = items.iter().map(|(s, j)| (s.as_str(), j.as_str())).collect();
        db.nodes().ingest(&refs).unwrap();
        db.flush().unwrap();
    }

    // Reopen and verify
    {
        let db = SekejapDB::new(dir.path(), n + 64).unwrap();

        let count = db.nodes().all().count().unwrap();
        assert_eq!(count.data, n, "expected {} nodes after reopen", n);

        // Random slug lookup works
        let result = db.nodes().get("persist/4999");
        assert!(result.is_some(), "persist/4999 should survive reopen");

        // Collection bitmap works after reopen
        let col_count = db.nodes().collection("persist").count().unwrap();
        assert_eq!(col_count.data, n);
    }
}
