//! Pure Rust Benchmark: Sekejap vs SQLite
//!
//! Run: cargo run --example benchmark --features fulltext --release

use rand::Rng;
use rusqlite::{params, Connection};
use sekejap::SekejapDB;
use std::collections::HashSet;
use std::time::Instant;

const NUM_RECORDS: usize = 10_000;
const NUM_EDGES: usize = 30_000;
const VEC_DIM: usize = 128;

struct BenchRow {
    scenario: String,
    operation: String,
    sqlite_secs: f64,
    sekejap_secs: f64,
}

impl BenchRow {
    fn speedup(&self) -> f64 {
        if self.sekejap_secs > 1e-9 {
            self.sqlite_secs / self.sekejap_secs
        } else {
            f64::INFINITY
        }
    }
}

// ── Data generation ─────────────────────────────────────────────────────────

struct Record {
    slug: String,
    name: String,
    body: String,
    lat: f32,
    lon: f32,
    vector: Vec<f32>,
}

fn generate_records(n: usize) -> Vec<Record> {
    let mut rng = rand::thread_rng();
    let words: Vec<&str> = vec![
        "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
        "iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi", "rho",
        "sigma", "tau", "upsilon", "phi", "chi", "psi", "omega", "lorem",
        "ipsum", "dolor", "amet", "quantum", "neural", "graph", "vector",
    ];
    (0..n)
        .map(|i| {
            let name = format!(
                "{}-{}-{}",
                words[rng.gen_range(0..words.len())],
                words[rng.gen_range(0..words.len())],
                i
            );
            let body: String = (0..20)
                .map(|_| words[rng.gen_range(0..words.len())])
                .collect::<Vec<_>>()
                .join(" ");
            let lat = rng.gen_range(-90.0f32..90.0);
            let lon = rng.gen_range(-180.0f32..180.0);
            let vector: Vec<f32> = (0..VEC_DIM).map(|_| rng.gen_range(-1.0f32..1.0)).collect();
            Record {
                slug: format!("users/{}", i),
                name,
                body,
                lat,
                lon,
                vector,
            }
        })
        .collect()
}

struct Edge {
    src: usize,
    dst: usize,
}

fn generate_edges(n: usize, max_node: usize) -> Vec<Edge> {
    let mut rng = rand::thread_rng();
    (0..n)
        .map(|_| Edge {
            src: rng.gen_range(0..max_node),
            dst: rng.gen_range(0..max_node),
        })
        .collect()
}

// ── SQLite helpers ──────────────────────────────────────────────────────────

fn sqlite_insert_simple(conn: &Connection, records: &[Record]) -> f64 {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS nodes (
            id TEXT PRIMARY KEY,
            name TEXT,
            body TEXT,
            lat REAL,
            lon REAL
        );",
    )
    .unwrap();
    let start = Instant::now();
    conn.execute_batch("BEGIN TRANSACTION").unwrap();
    {
        let mut stmt = conn
            .prepare_cached("INSERT OR REPLACE INTO nodes (id, name, body, lat, lon) VALUES (?1, ?2, ?3, ?4, ?5)")
            .unwrap();
        for r in records {
            stmt.execute(params![r.slug, r.name, r.body, r.lat, r.lon])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    start.elapsed().as_secs_f64()
}

fn sqlite_retrieve_simple(conn: &Connection, records: &[Record]) -> f64 {
    let start = Instant::now();
    let mut stmt = conn
        .prepare_cached("SELECT id, name, body FROM nodes WHERE id = ?1")
        .unwrap();
    for r in records.iter().take(1000) {
        let _row: String = stmt.query_row(params![r.slug], |row| row.get(0)).unwrap();
    }
    start.elapsed().as_secs_f64()
}

fn sqlite_insert_vectors(conn: &Connection, records: &[Record]) -> f64 {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS vectors (
            id TEXT PRIMARY KEY,
            vec BLOB
        );",
    )
    .unwrap();
    let start = Instant::now();
    conn.execute_batch("BEGIN TRANSACTION").unwrap();
    {
        let mut stmt = conn
            .prepare_cached("INSERT OR REPLACE INTO vectors (id, vec) VALUES (?1, ?2)")
            .unwrap();
        for r in records {
            let blob: Vec<u8> = r.vector.iter().flat_map(|v| v.to_le_bytes()).collect();
            stmt.execute(params![r.slug, blob]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    start.elapsed().as_secs_f64()
}

fn sqlite_retrieve_vector(conn: &Connection, query: &[f32]) -> (f64, usize) {
    let start = Instant::now();
    let mut stmt = conn.prepare_cached("SELECT id, vec FROM vectors").unwrap();
    let rows: Vec<(String, Vec<u8>)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();

    let mut scored: Vec<(f64, &str)> = rows
        .iter()
        .map(|(id, blob)| {
            let vec: Vec<f32> = blob
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            let dot: f64 = query.iter().zip(vec.iter()).map(|(&a, &b)| a as f64 * b as f64).sum();
            (dot, id.as_str())
        })
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let count = scored.iter().take(10).count();
    (start.elapsed().as_secs_f64(), count)
}

fn sqlite_insert_spatial(conn: &Connection) -> f64 {
    let start = Instant::now();
    conn.execute_batch("CREATE INDEX IF NOT EXISTS idx_lat_lon ON nodes(lat, lon)")
        .unwrap();
    start.elapsed().as_secs_f64()
}

fn sqlite_retrieve_spatial(conn: &Connection) -> (f64, usize) {
    let start = Instant::now();
    let mut stmt = conn
        .prepare_cached(
            "SELECT id FROM nodes WHERE lat BETWEEN -20.0 AND 20.0 AND lon BETWEEN -20.0 AND 20.0",
        )
        .unwrap();
    let count = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .count();
    (start.elapsed().as_secs_f64(), count)
}

fn sqlite_retrieve_vector_spatial(conn: &Connection, query: &[f32]) -> (f64, usize) {
    let start = Instant::now();
    let mut sp_stmt = conn
        .prepare_cached(
            "SELECT id FROM nodes WHERE lat BETWEEN -30.0 AND 30.0 AND lon BETWEEN -30.0 AND 30.0",
        )
        .unwrap();
    let spatial_ids: HashSet<String> = sp_stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();

    let mut v_stmt = conn.prepare_cached("SELECT id, vec FROM vectors").unwrap();
    let rows: Vec<(String, Vec<u8>)> = v_stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .filter_map(|r| r.ok())
        .filter(|(id, _)| spatial_ids.contains(id))
        .collect();

    let mut scored: Vec<(f64, String)> = rows
        .iter()
        .map(|(id, blob)| {
            let vec: Vec<f32> = blob
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            let dot: f64 = query.iter().zip(vec.iter()).map(|(&a, &b)| a as f64 * b as f64).sum();
            (dot, id.clone())
        })
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let count = scored.iter().take(10).count();
    (start.elapsed().as_secs_f64(), count)
}

fn sqlite_insert_fts(conn: &Connection, records: &[Record]) -> f64 {
    conn.execute_batch("CREATE VIRTUAL TABLE IF NOT EXISTS fts USING fts5(id, name, body);")
        .unwrap();
    let start = Instant::now();
    conn.execute_batch("BEGIN TRANSACTION").unwrap();
    {
        let mut stmt = conn
            .prepare_cached("INSERT INTO fts (id, name, body) VALUES (?1, ?2, ?3)")
            .unwrap();
        for r in records {
            stmt.execute(params![r.slug, r.name, r.body]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    start.elapsed().as_secs_f64()
}

fn sqlite_retrieve_fts_vector(conn: &Connection, query: &[f32]) -> (f64, usize) {
    let start = Instant::now();
    let mut fts_stmt = conn
        .prepare_cached("SELECT id FROM fts WHERE fts MATCH 'alpha OR beta OR gamma' LIMIT 500")
        .unwrap();
    let fts_ids: HashSet<String> = fts_stmt
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .collect();

    let mut v_stmt = conn.prepare_cached("SELECT id, vec FROM vectors").unwrap();
    let rows: Vec<(String, Vec<u8>)> = v_stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .filter_map(|r| r.ok())
        .filter(|(id, _)| fts_ids.contains(id))
        .collect();

    let mut scored: Vec<(f64, String)> = rows
        .iter()
        .map(|(id, blob)| {
            let vec: Vec<f32> = blob
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            let dot: f64 = query.iter().zip(vec.iter()).map(|(&a, &b)| a as f64 * b as f64).sum();
            (dot, id.clone())
        })
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let count = scored.iter().take(10).count();
    (start.elapsed().as_secs_f64(), count)
}

fn sqlite_insert_edges(conn: &Connection, edges: &[Edge]) -> f64 {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS edges (src TEXT, dst TEXT);
         CREATE INDEX IF NOT EXISTS idx_edges_src ON edges(src);",
    )
    .unwrap();
    let start = Instant::now();
    conn.execute_batch("BEGIN TRANSACTION").unwrap();
    {
        let mut stmt = conn
            .prepare_cached("INSERT INTO edges (src, dst) VALUES (?1, ?2)")
            .unwrap();
        for e in edges {
            stmt.execute(params![format!("users/{}", e.src), format!("users/{}", e.dst)])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    start.elapsed().as_secs_f64()
}

fn sqlite_graph_traversal(conn: &Connection, start_nodes: &[usize]) -> (f64, usize) {
    let start = Instant::now();
    let mut total = 0usize;
    for &node_id in start_nodes {
        let slug = format!("users/{}", node_id);
        let mut stmt = conn
            .prepare_cached(
                "WITH RECURSIVE hops(id, depth) AS (
                    SELECT dst, 1 FROM edges WHERE src = ?1
                    UNION
                    SELECT e.dst, h.depth + 1
                    FROM edges e JOIN hops h ON e.src = h.id
                    WHERE h.depth < 3
                )
                SELECT DISTINCT id FROM hops",
            )
            .unwrap();
        let count = stmt
            .query_map(params![slug], |row| row.get::<_, String>(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .count();
        total += count;
    }
    (start.elapsed().as_secs_f64(), total)
}

// ── main ────────────────────────────────────────────────────────────────────

fn main() {
    println!(
        "Generating {} records, {} edges, {}-dim vectors...",
        NUM_RECORDS, NUM_EDGES, VEC_DIM
    );
    let records = generate_records(NUM_RECORDS);
    let edges = generate_edges(NUM_EDGES, NUM_RECORDS);
    let mut rng = rand::thread_rng();
    let query_vec: Vec<f32> = (0..VEC_DIM).map(|_| rng.gen_range(-1.0f32..1.0)).collect();

    let mut results: Vec<BenchRow> = Vec::new();

    // ── SQLite setup ────────────────────────────────────────────────────
    let sqlite_path = "/tmp/sekejap_bench_sqlite.db";
    let _ = std::fs::remove_file(sqlite_path);
    let conn = Connection::open(sqlite_path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF;")
        .unwrap();

    // ── Sekejap setup (single DB with all features) ─────────────────────
    let sek_dir = tempfile::TempDir::new().unwrap();
    let sek_path = sek_dir.path();
    let db = SekejapDB::new(sek_path, NUM_RECORDS + 1000).unwrap();
    db.init_hnsw(32);
    #[cfg(feature = "fulltext")]
    db.init_fulltext(sek_path);

    db.schema()
        .define(
            "users",
            r#"{"hot_fields":{"vector":["dense"],"spatial":["geo"],"fulltext":["body"]}}"#,
        )
        .unwrap();

    // Pre-build all JSON strings once
    let items: Vec<(String, String)> = records
        .iter()
        .map(|r| {
            let json = serde_json::json!({
                "_id": &r.slug,
                "name": &r.name,
                "body": &r.body,
                "title": &r.name,
                "geo": {"loc": {"lat": r.lat, "lon": r.lon}},
                "vectors": {"dense": &r.vector},
            });
            (r.slug.clone(), serde_json::to_string(&json).unwrap())
        })
        .collect();
    let items_ref: Vec<(&str, &str)> =
        items.iter().map(|(s, j)| (s.as_str(), j.as_str())).collect();

    // =====================================================================
    // 1. INSERT SIMPLE (arena + slug + spatial + fulltext — no HNSW)
    // =====================================================================
    println!("\n[1/11] Insert simple...");
    let sqlite_insert = sqlite_insert_simple(&conn, &records);

    let sek_start = Instant::now();
    db.nodes().ingest_raw(&items_ref).unwrap();
    db.flush().unwrap();
    let sek_ingest_raw = sek_start.elapsed().as_secs_f64();

    // Build HNSW separately so we can measure it independently
    let sek_hnsw_start = Instant::now();
    db.nodes().build_hnsw().unwrap();
    let sek_hnsw_build = sek_hnsw_start.elapsed().as_secs_f64();

    let sek_insert_total = sek_ingest_raw + sek_hnsw_build;
    println!(
        "   Sekejap: ingest_raw={:.4}s, HNSW build={:.4}s, total={:.4}s",
        sek_ingest_raw, sek_hnsw_build, sek_insert_total
    );

    results.push(BenchRow {
        scenario: "1. Simple".into(),
        operation: "INSERTION SIMPLE".into(),
        sqlite_secs: sqlite_insert,
        sekejap_secs: sek_ingest_raw,
    });

    // =====================================================================
    // 2. RETRIEVE SIMPLE
    // =====================================================================
    println!("[2/11] Retrieve simple (1000x)...");
    let sqlite_ret = sqlite_retrieve_simple(&conn, &records);

    let sek_start = Instant::now();
    for r in records.iter().take(1000) {
        let _ = db.nodes().get(&r.slug);
    }
    let sek_ret = sek_start.elapsed().as_secs_f64();

    results.push(BenchRow {
        scenario: "".into(),
        operation: "RETRIEVAL SIMPLE".into(),
        sqlite_secs: sqlite_ret,
        sekejap_secs: sek_ret,
    });

    // =====================================================================
    // 3. INSERT VECTOR (= raw ingest + HNSW build)
    // =====================================================================
    println!("[3/11] Insert vector index...");
    let sqlite_vec_insert = sqlite_insert_vectors(&conn, &records);

    results.push(BenchRow {
        scenario: "2. Vector".into(),
        operation: "INSERTION WITH VECTOR INDEX".into(),
        sqlite_secs: sqlite_vec_insert,
        sekejap_secs: sek_insert_total,
    });

    // =====================================================================
    // 4. RETRIEVE VECTOR
    // =====================================================================
    println!("[4/11] Retrieve vector (top-10)...");
    let (sqlite_vec_ret, _) = sqlite_retrieve_vector(&conn, &query_vec);

    let sek_start = Instant::now();
    let sek_vec_result = db.nodes().all().similar(&query_vec, 10).collect().unwrap();
    let _sek_vec_count = sek_vec_result.data.len();
    let sek_vec_ret = sek_start.elapsed().as_secs_f64();

    results.push(BenchRow {
        scenario: "".into(),
        operation: "RETRIEVAL VECTOR".into(),
        sqlite_secs: sqlite_vec_ret,
        sekejap_secs: sek_vec_ret,
    });

    // =====================================================================
    // 5. INSERT SPATIAL
    // =====================================================================
    println!("[5/11] Insert spatial index...");
    let sqlite_sp_insert = sqlite_insert_spatial(&conn);

    results.push(BenchRow {
        scenario: "3. Spatial".into(),
        operation: "INSERTION WITH SPATIAL INDEX".into(),
        sqlite_secs: sqlite_sp_insert,
        sekejap_secs: sek_ingest_raw, // R-Tree built during ingest_raw
    });

    // =====================================================================
    // 6. RETRIEVE SPATIAL
    // =====================================================================
    println!("[6/11] Retrieve spatial (bbox -20..20)...");
    let (sqlite_sp_ret, sqlite_sp_count) = sqlite_retrieve_spatial(&conn);

    let sek_start = Instant::now();
    let sek_sp_result = db
        .nodes()
        .all()
        .within_bbox(-20.0, -20.0, 20.0, 20.0)
        .collect()
        .unwrap();
    let sek_sp_count = sek_sp_result.data.len();
    let sek_sp_ret = sek_start.elapsed().as_secs_f64();

    println!(
        "   Spatial results — SQLite: {} hits, Sekejap: {} hits",
        sqlite_sp_count, sek_sp_count
    );

    results.push(BenchRow {
        scenario: "".into(),
        operation: "RETRIEVAL POINT DISTANCE".into(),
        sqlite_secs: sqlite_sp_ret,
        sekejap_secs: sek_sp_ret,
    });

    // =====================================================================
    // 7. INSERT VECTOR + SPATIAL
    // =====================================================================
    println!("[7/11] Insert vector + spatial...");
    let sqlite_vs_insert = sqlite_vec_insert + sqlite_sp_insert;

    results.push(BenchRow {
        scenario: "4. V + S".into(),
        operation: "INSERTION WITH VECTOR AND SPATIAL".into(),
        sqlite_secs: sqlite_vs_insert,
        sekejap_secs: sek_insert_total, // ingest_raw + HNSW
    });

    // =====================================================================
    // 8. RETRIEVE VECTOR + SPATIAL
    // =====================================================================
    println!("[8/11] Retrieve vector + spatial...");
    let (sqlite_vs_ret, _) = sqlite_retrieve_vector_spatial(&conn, &query_vec);

    let sek_start = Instant::now();
    let bbox_result = db
        .nodes()
        .all()
        .within_bbox(-30.0, -30.0, 30.0, 30.0)
        .collect()
        .unwrap();
    let bbox_ids: HashSet<u32> = bbox_result.data.iter().map(|h| h.idx).collect();
    let vec_result = db.nodes().all().similar(&query_vec, 100).collect().unwrap();
    let _vs_hits: Vec<_> = vec_result
        .data
        .iter()
        .filter(|h| bbox_ids.contains(&h.idx))
        .take(10)
        .collect();
    let sek_vs_ret = sek_start.elapsed().as_secs_f64();

    results.push(BenchRow {
        scenario: "".into(),
        operation: "RETRIEVAL VECTOR AND SPATIAL".into(),
        sqlite_secs: sqlite_vs_ret,
        sekejap_secs: sek_vs_ret,
    });

    // =====================================================================
    // 9. INSERT VECTOR + FTS
    // =====================================================================
    println!("[9/11] Insert vector + fulltext...");
    let sqlite_fts_insert = sqlite_insert_fts(&conn, &records);
    let sqlite_vf_insert = sqlite_vec_insert + sqlite_fts_insert;

    results.push(BenchRow {
        scenario: "5. V + F".into(),
        operation: "INSERTION WITH VECTOR AND FULLTEXT".into(),
        sqlite_secs: sqlite_vf_insert,
        sekejap_secs: sek_insert_total, // ingest_raw (includes fulltext) + HNSW
    });

    // =====================================================================
    // 10. RETRIEVE VECTOR + FTS
    // =====================================================================
    println!("[10/11] Retrieve vector + fulltext...");
    let (sqlite_vf_ret, _) = sqlite_retrieve_fts_vector(&conn, &query_vec);

    #[cfg(feature = "fulltext")]
    let sek_vf_ret = {
        let start = Instant::now();
        let fts_result = db.nodes().all().matching("alpha beta gamma").collect().unwrap();
        let fts_ids: HashSet<u32> = fts_result.data.iter().map(|h| h.idx).collect();
        let vec_result = db.nodes().all().similar(&query_vec, 100).collect().unwrap();
        let _vf_hits: Vec<_> = vec_result
            .data
            .iter()
            .filter(|h| fts_ids.contains(&h.idx))
            .take(10)
            .collect();
        start.elapsed().as_secs_f64()
    };
    #[cfg(not(feature = "fulltext"))]
    let sek_vf_ret = {
        println!("   (fulltext feature disabled — N/A)");
        0.0
    };

    results.push(BenchRow {
        scenario: "".into(),
        operation: "RETRIEVAL OF TEXT WITH VECTOR".into(),
        sqlite_secs: sqlite_vf_ret,
        sekejap_secs: sek_vf_ret,
    });

    // =====================================================================
    // 11. GRAPH TRAVERSAL (100x 3-hop)
    // =====================================================================
    println!("[11/11] Graph traversal (100x 3-hop)...");
    let _sqlite_edge_insert = sqlite_insert_edges(&conn, &edges);

    let sek_start = Instant::now();
    let edge_owned: Vec<(String, String)> = edges
        .iter()
        .map(|e| (format!("users/{}", e.src), format!("users/{}", e.dst)))
        .collect();
    let edge_tuples: Vec<(&str, &str, &str, f32)> = edge_owned
        .iter()
        .map(|(s, d)| (s.as_str(), d.as_str(), "follows", 1.0f32))
        .collect();
    db.edges().ingest(&edge_tuples).unwrap();
    let _sek_edge_insert = sek_start.elapsed().as_secs_f64();

    let start_nodes: Vec<usize> = (0..100).map(|_| rng.gen_range(0..NUM_RECORDS)).collect();

    let (sqlite_graph, sqlite_graph_total) = sqlite_graph_traversal(&conn, &start_nodes);

    let sek_start = Instant::now();
    let mut sek_graph_total = 0usize;
    for &node_id in &start_nodes {
        let slug = format!("users/{}", node_id);
        let result = db
            .nodes()
            .one(&slug)
            .forward("follows")
            .hops(3)
            .collect()
            .unwrap();
        sek_graph_total += result.data.len();
    }
    let sek_graph = sek_start.elapsed().as_secs_f64();

    println!(
        "   Graph results — SQLite: {} total nodes, Sekejap: {} total nodes",
        sqlite_graph_total, sek_graph_total
    );

    results.push(BenchRow {
        scenario: "6. Graph".into(),
        operation: "MULTIPLE TRAVERSAL (100x 3-HOP)".into(),
        sqlite_secs: sqlite_graph,
        sekejap_secs: sek_graph,
    });

    // ── Output results ──────────────────────────────────────────────────
    println!("\n");

    // Python benchmark results (from sekejap-benchmark/RESULT.md)
    let python_data: Vec<(f64, f64)> = vec![
        (0.9587, 1.1074), // 1. Insert Simple:        SQLite Python, Sekejap Python
        (0.0080, 0.0021), // 2. Retrieve Simple
        (0.8626, 2.3149), // 3. Insert Vector
        (0.0245, 0.0002), // 4. Retrieve Vector
        (0.0225, 0.5631), // 5. Insert Spatial
        (0.0001, 0.0001), // 6. Retrieve Spatial
        (0.8851, 1.4390), // 7. Insert V+S
        (0.0001, 0.0004), // 8. Retrieve V+S
        (0.8976, 1.1601), // 9. Insert V+F
        (0.0003, 0.0006), // 10. Retrieve V+F
        (0.0133, 0.0012), // 11. Graph Traversal
    ];

    let header = "# Sekejap Benchmark Results — Rust vs Python (10k Records)\n";
    let table_header =
        "| Scenario | Operation | SQLite (Rust) | Sekejap (Rust) | SQLite (Python) | Sekejap (Python) |\n| :--- | :--- | :--- | :--- | :--- | :--- |";

    let mut output = String::new();
    output.push_str(header);
    output.push('\n');
    output.push_str(table_header);
    output.push('\n');

    for (i, row) in results.iter().enumerate() {
        let scenario = if row.scenario.is_empty() {
            "".to_string()
        } else {
            format!("**{}**", row.scenario)
        };
        let (py_sqlite, py_sekejap) = python_data[i];
        let line = format!(
            "| {} | {} | {:.4}s | {:.4}s | {:.4}s | {:.4}s |",
            scenario, row.operation, row.sqlite_secs, row.sekejap_secs, py_sqlite, py_sekejap
        );
        output.push_str(&line);
        output.push('\n');
    }

    print!("{}", output);

    // Write to file
    let result_path = std::path::Path::new("sekejap-benchmark/RESULT_RUST.md");
    if let Some(parent) = result_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::write(result_path, &output).unwrap();
    println!("Results written to sekejap-benchmark/RESULT_RUST.md");

    // Cleanup
    let _ = std::fs::remove_file(sqlite_path);
}
