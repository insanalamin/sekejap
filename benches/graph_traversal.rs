//! Graph Traversal Benchmark
//!
//! Mirrors the sekejap `graph_traversal` benchmark so results are comparable.
//!
//! Dataset: 10 000 nodes in a ring graph where each node has OUT_DEGREE forward
//! edges.  Query: BFS from a fixed source node for up to HOPS hops, capped at
//! LIMIT results.
//!
//! Competitors
//! ───────────
//! • core_sql    – sekejap via MATCH SQL query
//! • core_atom   – sekejap via builder API (db.one().forward().hops())
//! • sekejap_sql – sekejap (original) via MATCH SQL query
//! • sqlite      – SQLite via recursive CTE + indexed edge table

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, black_box};
use rusqlite::params;
use serde_json::json;

const TOTAL_NODES: usize = 10_000;
const OUT_DEGREE: usize  = 3;
const HOPS: usize        = 5;
const LIMIT: usize       = 2_000;

// ── Dataset ──────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct Node {
    id:   String,
    slug: String,
    /// Slugs of forward neighbours.
    neighbours: Vec<String>,
}

fn build_dataset() -> Vec<Node> {
    (0..TOTAL_NODES)
        .map(|i| {
            let id   = format!("mem_{:05}", i);
            let slug = format!("memories/{}", id);
            let neighbours = (1..=OUT_DEGREE)
                .map(|d| format!("memories/mem_{:05}", (i + d) % TOTAL_NODES))
                .collect();
            Node { id, slug, neighbours }
        })
        .collect()
}

// ── Populate helpers ─────────────────────────────────────────────────────────

fn populate_core(dataset: &[Node]) -> sekejap::CoreDB {
    let mut db = sekejap::CoreDB::new();
    // nodes
    let pairs: Vec<(String, String)> = dataset
        .iter()
        .map(|n| {
            (
                n.slug.clone(),
                json!({
                    "_collection": "memories",
                    "_key": n.id,
                    "id": n.id,
                })
                .to_string(),
            )
        })
        .collect();
    let refs: Vec<(&str, &str)> =
        pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    db.put_many(refs).unwrap();
    // edges
    for node in dataset {
        for nbr in &node.neighbours {
            db.link(&node.slug, nbr, "related_to", 1.0);
        }
    }
    db
}

fn populate_sekejap(dataset: &[Node]) -> (sekejap::CoreDB, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = sekejap::CoreDB::open(dir.path()).unwrap();
    for n in dataset {
        db.put(
            &n.slug,
            &json!({
                "_collection": "memories",
                "_key": n.id,
                "id": n.id,
            })
            .to_string(),
        )
        .unwrap();
    }
    for n in dataset {
        for nbr in &n.neighbours {
            db.link(&n.slug, nbr, "related_to", 1.0);
        }
    }
    (db, dir)
}

fn populate_sqlite(dataset: &[Node]) -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE memories (id TEXT PRIMARY KEY);
         CREATE TABLE edges (from_id TEXT, to_id TEXT);
         CREATE INDEX idx_edges_from ON edges(from_id);",
    )
    .unwrap();
    conn.execute_batch("BEGIN").unwrap();
    {
        let mut ins = conn
            .prepare("INSERT INTO memories (id) VALUES (?1)")
            .unwrap();
        for n in dataset {
            ins.execute(params![n.id]).unwrap();
        }
    }
    {
        let mut ins = conn
            .prepare("INSERT INTO edges (from_id, to_id) VALUES (?1, ?2)")
            .unwrap();
        for n in dataset {
            for nbr in &n.neighbours {
                // store bare id (without collection prefix) to match recursive CTE
                let to_id = nbr.strip_prefix("memories/").unwrap_or(nbr);
                ins.execute(params![n.id, to_id]).unwrap();
            }
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    conn
}

// ── Queries ───────────────────────────────────────────────────────────────────

fn run_core_sql(db: &sekejap::CoreDB, source_id: &str) -> usize {
    db.query(&format!(
        "MATCH (a:memories)-[:related_to*1..{HOPS}]->(b) WHERE a._key = '{source_id}' RETURN b LIMIT {LIMIT}"
    ))
    .unwrap()
    .count()
}

fn run_core_atom(db: &sekejap::CoreDB, source_slug: &str) -> usize {
    db.one(source_slug)
        .hops_typed("related_to", HOPS as u32)
        .take(LIMIT)
        .count()
}

fn run_sekejap_sql(sk: &sekejap::CoreDB, source_id: &str) -> usize {
    sk.query(&format!(
        "MATCH (a:memories)-[:related_to*1..{HOPS}]->(b) WHERE a._key = '{source_id}' RETURN b LIMIT {LIMIT}"
    ))
    .unwrap()
    .count()
}

fn run_sqlite(conn: &rusqlite::Connection, source_id: &str) -> usize {
    let sql = "WITH RECURSIVE walk(id, depth) AS (
        SELECT ?1 AS id, 0 AS depth
        UNION ALL
        SELECT e.to_id, w.depth + 1
        FROM edges e JOIN walk w ON e.from_id = w.id
        WHERE w.depth < ?2
    )
    SELECT DISTINCT id FROM walk WHERE depth > 0 LIMIT ?3";
    let mut stmt = conn.prepare_cached(sql).unwrap();
    stmt.query_map(params![source_id, HOPS as i64, LIMIT as i64], |row| {
        row.get::<_, String>(0)
    })
    .unwrap()
    .count()
}

// ── Benchmarks ───────────────────────────────────────────────────────────────

fn bench_insert(c: &mut Criterion) {
    let dataset = build_dataset();

    let mut group = c.benchmark_group("graph_insert");
    group.sample_size(10);

    group.bench_function("sekejap_memory", |b| {
        b.iter(|| black_box(populate_core(&dataset)))
    });

    group.bench_function("sekejap_disk", |b| {
        b.iter(|| black_box(populate_sekejap(&dataset)))
    });

    group.bench_function("sqlite", |b| {
        b.iter(|| black_box(populate_sqlite(&dataset)))
    });

    group.finish();
}

fn bench_query(c: &mut Criterion) {
    let dataset   = build_dataset();
    let source    = &dataset[1234];
    let source_id = source.id.as_str();

    let core_db         = populate_core(&dataset);
    let (sekejap_db, _dir) = populate_sekejap(&dataset);
    let sqlite          = populate_sqlite(&dataset);

    // Sanity: all engines must return the same count.
    let sk_mem_sql_n  = run_core_sql(&core_db, source_id);
    let sk_mem_atom_n = run_core_atom(&core_db, &source.slug);
    let sk_n        = run_sekejap_sql(&sekejap_db, source_id);
    let sq_n        = run_sqlite(&sqlite, source_id);
    assert_eq!(sk_mem_sql_n, sk_mem_atom_n, "sekejap_memory_sql vs sekejap_memory_atom mismatch");
    assert_eq!(sk_mem_sql_n, sk_n,  "sekejap_memory_sql vs sekejap_disk mismatch");
    assert_eq!(sk_mem_sql_n, sq_n,  "sekejap_memory_sql vs sqlite mismatch");

    let mut group = c.benchmark_group(format!("graph_query_{HOPS}hop_{TOTAL_NODES}n"));

    group.bench_with_input(
        BenchmarkId::new("sekejap_memory_sql", source_id),
        source_id,
        |b, sid| b.iter(|| black_box(run_core_sql(&core_db, sid))),
    );

    group.bench_with_input(
        BenchmarkId::new("sekejap_memory_atom", source_id),
        source_id,
        |b, sid| {
            let slug = format!("memories/{sid}");
            b.iter(|| black_box(run_core_atom(&core_db, &slug)))
        },
    );

    group.bench_with_input(
        BenchmarkId::new("sekejap_disk_sql", source_id),
        source_id,
        |b, sid| b.iter(|| black_box(run_sekejap_sql(&sekejap_db, sid))),
    );

    group.bench_with_input(
        BenchmarkId::new("sqlite_cte", source_id),
        source_id,
        |b, sid| b.iter(|| black_box(run_sqlite(&sqlite, sid))),
    );

    group.finish();
}

criterion_group!(benches, bench_insert, bench_query);
criterion_main!(benches);
