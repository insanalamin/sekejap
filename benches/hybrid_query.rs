//! Benchmark: hybrid multi-modal query scenarios — sekejap vs SQLite
//!
//! These scenarios highlight where sekejap's fused graph + vector + spatial
//! pipeline gives a structural advantage over a plain relational store.
//!
//! 1. hybrid_spatial_vector  — spatial radius pre-filter → HNSW re-rank top-10
//! 2. hybrid_spatial_graph   — spatial radius → forward edge traversal
//! 3. root_cause_analysis    — typed BFS from a failed node → Leaves (root causes)
//! 4. hybrid_rag             — GIN keyword filter → HNSW vector re-rank top-20

use criterion::{criterion_group, criterion_main, Criterion, black_box};
use rusqlite::Connection;
use sekejap::{CosineDistance, CoreDB, Distance};

// ── Melbourne reference point ─────────────────────────────────────────────────

const CENTRE_LAT: f64 = -37.8136;
const CENTRE_LON: f64 = 144.9631;

// ── Deterministic vector generator ───────────────────────────────────────────

fn make_vec(dim: usize, seed: usize) -> Vec<f32> {
    (0..dim)
        .map(|i| {
            let x = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(i.wrapping_mul(1442695040888963407));
            let f = (x >> 33) as f32 / u32::MAX as f32;
            f * 2.0 - 1.0
        })
        .collect()
}

// ── Haversine distance (km) ───────────────────────────────────────────────────

fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6371.0f64;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    2.0 * r * a.sqrt().asin()
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. HYBRID SPATIAL + VECTOR
//
// Dataset: 5 000 Melbourne venues (GeoJSON Point) + 32-dim text embeddings.
// Query  : venues within 5 km of Melbourne CBD → HNSW top-10 by embedding.
//
// sekejap: spatial index → HNSW in one fused pipeline call.
// SQLite : bounding-box scan → Haversine filter in Rust → BLOB cosine top-10.
// ─────────────────────────────────────────────────────────────────────────────

const VENUES: usize = 5_000;
const VEC_DIM_VENUES: usize = 32;
const RADIUS_KM: f64 = 5.0;
const TOP_K_VEC: usize = 10;

fn setup_spatial_vector_core() -> (CoreDB, Vec<f32>) {
    let mut db = CoreDB::new();

    for i in 0..VENUES {
        let lat = CENTRE_LAT - 0.15 + (i % 300) as f64 * 0.001;
        let lon = CENTRE_LON - 0.20 + (i % 400) as f64 * 0.001;
        let slug = format!("venues/v{i}");
        db.put(
            &slug,
            &serde_json::json!({
                "_collection": "venues",
                "_key": format!("v{i}"),
                "name": format!("Venue {i}"),
                "geometry": { "type": "Point", "coordinates": [lon, lat] },
            })
            .to_string(),
        )
        .unwrap();
        db.put_vector(&slug, "emb", &make_vec(VEC_DIM_VENUES, i)).unwrap();
    }

    db.build_spatial_index();
    // Build HNSW after inserting all vectors
    db.build_hnsw_index("emb", 16, 200).unwrap();

    let query = make_vec(VEC_DIM_VENUES, VENUES + 1);
    (db, query)
}

fn setup_spatial_vector_sqlite() -> (Connection, Vec<f32>) {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE venues (
            key   TEXT PRIMARY KEY,
            name  TEXT,
            lat   REAL,
            lon   REAL,
            emb   BLOB
        );
        CREATE INDEX venues_lat ON venues(lat);
        CREATE INDEX venues_lon ON venues(lon);",
    )
    .unwrap();

    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO venues (key, name, lat, lon, emb) VALUES (?1,?2,?3,?4,?5)")
            .unwrap();
        for i in 0..VENUES {
            let lat = CENTRE_LAT - 0.15 + (i % 300) as f64 * 0.001;
            let lon = CENTRE_LON - 0.20 + (i % 400) as f64 * 0.001;
            let v = make_vec(VEC_DIM_VENUES, i);
            let blob: Vec<u8> = v.iter().flat_map(|f| f.to_le_bytes()).collect();
            stmt.execute(rusqlite::params![format!("v{i}"), format!("Venue {i}"), lat, lon, blob])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();

    let query = make_vec(VEC_DIM_VENUES, VENUES + 1);
    (conn, query)
}

fn bench_hybrid_spatial_vector(c: &mut Criterion) {
    let (sk_db, sk_query) = setup_spatial_vector_core();
    let (sq_conn, sq_query) = setup_spatial_vector_sqlite();

    let mut group = c.benchmark_group("hybrid_spatial_vector");

    // sekejap: spatial index → HNSW re-rank, fused pipeline
    group.bench_function("sekejap", |b| {
        b.iter(|| {
            black_box(
                sk_db
                    .collection("venues")
                    .st_dwithin(CENTRE_LAT, CENTRE_LON, RADIUS_KM)
                    .vector_near("emb", sk_query.clone(), TOP_K_VEC)
                    .count(),
            )
        })
    });

    // SQLite: bounding-box scan → Haversine in Rust → cosine top-k
    group.bench_function("sqlite", |b| {
        b.iter(|| {
            // Approximate degree delta for the bounding box
            let dlat = RADIUS_KM / 111.0;
            let dlon = RADIUS_KM / (111.0 * CENTRE_LAT.to_radians().cos().abs());
            let lat_min = CENTRE_LAT - dlat;
            let lat_max = CENTRE_LAT + dlat;
            let lon_min = CENTRE_LON - dlon;
            let lon_max = CENTRE_LON + dlon;

            let mut stmt = sq_conn
                .prepare_cached(
                    "SELECT lat, lon, emb FROM venues \
                     WHERE lat BETWEEN ?1 AND ?2 AND lon BETWEEN ?3 AND ?4",
                )
                .unwrap();

            let mut scored: Vec<f32> = stmt
                .query_map(
                    rusqlite::params![lat_min, lat_max, lon_min, lon_max],
                    |row| {
                        let lat: f64 = row.get(0)?;
                        let lon: f64 = row.get(1)?;
                        let blob: Vec<u8> = row.get(2)?;
                        Ok((lat, lon, blob))
                    },
                )
                .unwrap()
                .filter_map(|r| r.ok())
                .filter(|(lat, lon, _)| haversine_km(CENTRE_LAT, CENTRE_LON, *lat, *lon) <= RADIUS_KM)
                .map(|(_, _, blob)| {
                    let v: Vec<f32> = blob
                        .chunks_exact(4)
                        .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
                        .collect();
                    CosineDistance::eval(&sq_query, &v)
                })
                .collect();

            scored.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            scored.truncate(TOP_K_VEC);
            black_box(scored.len())
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. HYBRID SPATIAL + GRAPH
//
// Dataset: 2 000 infrastructure sites (GeoJSON), each "powers" 5 buildings.
//          → 10 000 "powers" edges total.
// Query  : find all sites within 5 km of CBD, then follow "powers" edges
//          to enumerate the buildings they serve.
//
// sekejap: spatial filter → forward graph hop, fused pipeline.
// SQLite : bounding-box + Haversine → JOIN on edges.
// ─────────────────────────────────────────────────────────────────────────────

const SITES: usize = 2_000;
const BUILDINGS_PER_SITE: usize = 5;

fn setup_spatial_graph_core() -> CoreDB {
    let mut db = CoreDB::new();

    for i in 0..SITES {
        let lat = CENTRE_LAT - 0.20 + (i % 400) as f64 * 0.001;
        let lon = CENTRE_LON - 0.25 + (i % 500) as f64 * 0.001;
        db.put(
            &format!("sites/s{i}"),
            &serde_json::json!({
                "_collection": "sites",
                "_key": format!("s{i}"),
                "geometry": { "type": "Point", "coordinates": [lon, lat] },
            })
            .to_string(),
        )
        .unwrap();

        for b in 0..BUILDINGS_PER_SITE {
            let bkey = format!("buildings/b{}_{}", i, b);
            db.put(
                &bkey,
                &serde_json::json!({
                    "_collection": "buildings",
                    "_key": format!("b{}_{}", i, b),
                    "site": format!("s{i}"),
                })
                .to_string(),
            )
            .unwrap();
            db.link(&format!("sites/s{i}"), &bkey, "powers", 1.0);
        }
    }

    db.build_spatial_index();
    db
}

fn setup_spatial_graph_sqlite() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE sites (
            key TEXT PRIMARY KEY,
            lat REAL,
            lon REAL
        );
        CREATE TABLE buildings (key TEXT PRIMARY KEY, site TEXT);
        CREATE TABLE powers (from_key TEXT, to_key TEXT);
        CREATE INDEX powers_from ON powers(from_key);
        CREATE INDEX sites_lat ON sites(lat);
        CREATE INDEX sites_lon ON sites(lon);",
    )
    .unwrap();

    conn.execute_batch("BEGIN").unwrap();
    {
        let mut s_stmt = conn
            .prepare("INSERT INTO sites (key, lat, lon) VALUES (?1,?2,?3)")
            .unwrap();
        let mut b_stmt = conn
            .prepare("INSERT INTO buildings (key, site) VALUES (?1,?2)")
            .unwrap();
        let mut e_stmt = conn
            .prepare("INSERT INTO powers (from_key, to_key) VALUES (?1,?2)")
            .unwrap();

        for i in 0..SITES {
            let lat = CENTRE_LAT - 0.20 + (i % 400) as f64 * 0.001;
            let lon = CENTRE_LON - 0.25 + (i % 500) as f64 * 0.001;
            s_stmt.execute(rusqlite::params![format!("s{i}"), lat, lon]).unwrap();

            for b in 0..BUILDINGS_PER_SITE {
                let bkey = format!("b{}_{}", i, b);
                b_stmt.execute(rusqlite::params![&bkey, format!("s{i}")]).unwrap();
                e_stmt.execute(rusqlite::params![format!("s{i}"), &bkey]).unwrap();
            }
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    conn
}

fn bench_hybrid_spatial_graph(c: &mut Criterion) {
    let sk_db = setup_spatial_graph_core();
    let sq_conn = setup_spatial_graph_sqlite();

    let mut group = c.benchmark_group("hybrid_spatial_graph");

    // sekejap: spatial index → forward edge hop
    group.bench_function("sekejap", |b| {
        b.iter(|| {
            black_box(
                sk_db
                    .collection("sites")
                    .st_dwithin(CENTRE_LAT, CENTRE_LON, RADIUS_KM)
                    .forward("powers")
                    .count(),
            )
        })
    });

    // SQLite: bounding-box + Haversine in Rust → JOIN on powers table
    group.bench_function("sqlite", |b| {
        b.iter(|| {
            let dlat = RADIUS_KM / 111.0;
            let dlon = RADIUS_KM / (111.0 * CENTRE_LAT.to_radians().cos().abs());
            let lat_min = CENTRE_LAT - dlat;
            let lat_max = CENTRE_LAT + dlat;
            let lon_min = CENTRE_LON - dlon;
            let lon_max = CENTRE_LON + dlon;

            // Step 1: sites in bounding box
            let mut s_stmt = sq_conn
                .prepare_cached(
                    "SELECT key, lat, lon FROM sites \
                     WHERE lat BETWEEN ?1 AND ?2 AND lon BETWEEN ?3 AND ?4",
                )
                .unwrap();

            let near_sites: Vec<String> = s_stmt
                .query_map(
                    rusqlite::params![lat_min, lat_max, lon_min, lon_max],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, f64>(1)?, row.get::<_, f64>(2)?)),
                )
                .unwrap()
                .filter_map(|r| r.ok())
                .filter(|(_, lat, lon)| haversine_km(CENTRE_LAT, CENTRE_LON, *lat, *lon) <= RADIUS_KM)
                .map(|(key, _, _)| key)
                .collect();

            // Step 2: buildings powered by those sites
            let count: usize = near_sites
                .iter()
                .map(|site_key| {
                    let mut e_stmt = sq_conn
                        .prepare_cached("SELECT COUNT(*) FROM powers WHERE from_key = ?1")
                        .unwrap();
                    e_stmt
                        .query_row(rusqlite::params![site_key], |row| row.get::<_, i64>(0))
                        .unwrap_or(0) as usize
                })
                .sum();

            black_box(count)
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. ROOT CAUSE ANALYSIS
//
// Dataset: 500 microservices in a binary dependency tree (depth 8 ≈ 255 nodes).
//          Edges: child →[depends_on]→ parent (parents are dependencies).
//          The root (svc0) has no parents; leaves have no children.
// Query  : starting from an arbitrary leaf, follow "depends_on" edges forward
//          until exhausted, then keep only Leaves (services with no further
//          dependencies) → the root causes.
//
// sekejap: typed BFS + Leaves filter.
// SQLite : recursive CTE forward + NOT IN anti-join for leaf test.
// ─────────────────────────────────────────────────────────────────────────────

const SERVICES: usize = 255; // 2^8 - 1 binary tree (depth 8)

fn setup_rca_core() -> CoreDB {
    let mut db = CoreDB::new();

    // Full binary tree: node i has children 2i+1 and 2i+2 (0-indexed).
    // Edges: child →[depends_on]→ parent (child depends on parent).
    for i in 0..SERVICES {
        db.put(
            &format!("services/svc{i}"),
            &serde_json::json!({
                "_collection": "services",
                "_key": format!("svc{i}"),
                "status": if i == 0 { "failed" } else { "healthy" },
            })
            .to_string(),
        )
        .unwrap();
    }
    // Parent → child edges reversed: child depends_on parent
    for i in 1..SERVICES {
        let parent = (i - 1) / 2;
        db.link(
            &format!("services/svc{i}"),
            &format!("services/svc{parent}"),
            "depends_on",
            1.0,
        );
    }
    db
}

fn setup_rca_sqlite() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE services (key TEXT PRIMARY KEY, status TEXT);
         CREATE TABLE depends_on (from_key TEXT, to_key TEXT);
         CREATE INDEX dep_from ON depends_on(from_key);
         CREATE INDEX dep_to   ON depends_on(to_key);",
    )
    .unwrap();

    conn.execute_batch("BEGIN").unwrap();
    {
        let mut s_stmt = conn
            .prepare("INSERT INTO services (key, status) VALUES (?1, ?2)")
            .unwrap();
        let mut e_stmt = conn
            .prepare("INSERT INTO depends_on (from_key, to_key) VALUES (?1,?2)")
            .unwrap();
        for i in 0..SERVICES {
            s_stmt
                .execute(rusqlite::params![
                    format!("svc{i}"),
                    if i == 0 { "failed" } else { "healthy" }
                ])
                .unwrap();
        }
        for i in 1..SERVICES {
            let parent = (i - 1) / 2;
            e_stmt
                .execute(rusqlite::params![format!("svc{i}"), format!("svc{parent}")])
                .unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    conn
}

fn bench_root_cause_analysis(c: &mut Criterion) {
    let sk_db = setup_rca_core();
    let sq_conn = setup_rca_sqlite();

    // Pick an arbitrary leaf node (e.g. svc200 in 0-indexed tree with 255 nodes)
    let start_key = "svc200";

    let mut group = c.benchmark_group("root_cause_analysis");

    // sekejap: typed BFS from leaf → Leaves (nodes with no depends_on edges out)
    group.bench_function("sekejap", |b| {
        b.iter(|| {
            black_box(
                sk_db
                    .one(&format!("services/{start_key}"))
                    .hops_typed("depends_on", 16)
                    .leaves()
                    .count(),
            )
        })
    });

    // SQLite: recursive CTE forward → anti-join for leaf detection
    group.bench_function("sqlite", |b| {
        b.iter(|| {
            let mut stmt = sq_conn.prepare_cached(
                "WITH RECURSIVE chain(key) AS (
                    SELECT ?1
                    UNION
                    SELECT d.to_key
                    FROM chain c JOIN depends_on d ON d.from_key = c.key
                )
                SELECT COUNT(*) FROM chain
                WHERE key NOT IN (SELECT from_key FROM depends_on)
                  AND key != ?1",
            ).unwrap();
            let count: i64 = stmt
                .query_row(rusqlite::params![start_key], |row| row.get(0))
                .unwrap_or(0);
            black_box(count)
        })
    });

    group.finish();
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. HYBRID RAG (retrieval-augmented generation retrieval)
//
// Dataset: 3 000 document chunks, each with:
//          - "content": text string (≈20% contain "Maribyrnong")
//          - "emb": 64-dim embedding
// Query  : GIN ILIKE keyword pre-filter → HNSW vector re-rank → top-20.
//          Simulates the retrieval phase of a RAG pipeline: "find chunks
//          that mention the keyword AND are semantically close to the query."
//
// sekejap: GIN index → HNSW re-rank, single atomic pipeline.
// SQLite : LIKE filter → BLOB cosine scan → top-20.
// ─────────────────────────────────────────────────────────────────────────────

const CHUNKS: usize = 3_000;
const VEC_DIM_CHUNKS: usize = 64;
const TOP_K_RAG: usize = 20;
const KEYWORD: &str = "Maribyrnong";

fn setup_rag_core() -> (CoreDB, Vec<f32>) {
    let mut db = CoreDB::new();

    for i in 0..CHUNKS {
        let content = if i % 5 == 0 {
            format!("Flooding in the Maribyrnong River basin affected area {i}")
        } else {
            format!("Urban planning document for precinct {i} near Footscray")
        };
        let slug = format!("chunks/c{i}");
        db.put(
            &slug,
            &serde_json::json!({
                "_collection": "chunks",
                "_key": format!("c{i}"),
                "content": content,
            })
            .to_string(),
        )
        .unwrap();
        db.put_vector(&slug, "emb", &make_vec(VEC_DIM_CHUNKS, i)).unwrap();
    }

    // GIN must be built AFTER data is loaded (batch build)
    db.build_gin_index("content");
    db.build_hnsw_index("emb", 16, 200).unwrap();

    let query = make_vec(VEC_DIM_CHUNKS, CHUNKS + 1);
    (db, query)
}

fn setup_rag_sqlite() -> (Connection, Vec<f32>) {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE chunks (
            key     TEXT PRIMARY KEY,
            content TEXT,
            emb     BLOB
        );
        CREATE INDEX chunks_content ON chunks(content);",
    )
    .unwrap();

    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt = conn
            .prepare("INSERT INTO chunks (key, content, emb) VALUES (?1,?2,?3)")
            .unwrap();
        for i in 0..CHUNKS {
            let content = if i % 5 == 0 {
                format!("Flooding in the Maribyrnong River basin affected area {i}")
            } else {
                format!("Urban planning document for precinct {i} near Footscray")
            };
            let v = make_vec(VEC_DIM_CHUNKS, i);
            let blob: Vec<u8> = v.iter().flat_map(|f| f.to_le_bytes()).collect();
            stmt.execute(rusqlite::params![format!("c{i}"), content, blob]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();

    let query = make_vec(VEC_DIM_CHUNKS, CHUNKS + 1);
    (conn, query)
}

fn bench_hybrid_rag(c: &mut Criterion) {
    let (sk_db, sk_query) = setup_rag_core();
    let (sq_conn, sq_query) = setup_rag_sqlite();

    let mut group = c.benchmark_group("hybrid_rag");

    // sekejap: GIN keyword candidates → HNSW re-rank in one fused pipeline
    group.bench_function("sekejap", |b| {
        b.iter(|| {
            black_box(
                sk_db
                    .collection("chunks")
                    .ilike("content", &format!("%{KEYWORD}%"))
                    .vector_near("emb", sk_query.clone(), TOP_K_RAG)
                    .count(),
            )
        })
    });

    // SQLite: LIKE filter → load BLOBs → cosine sort → top-k
    group.bench_function("sqlite", |b| {
        b.iter(|| {
            let mut stmt = sq_conn
                .prepare_cached("SELECT emb FROM chunks WHERE content LIKE ?1")
                .unwrap();

            let pattern = format!("%{KEYWORD}%");
            let mut scored: Vec<f32> = stmt
                .query_map(rusqlite::params![pattern], |row| row.get::<_, Vec<u8>>(0))
                .unwrap()
                .filter_map(|r| r.ok())
                .map(|blob| {
                    let v: Vec<f32> = blob
                        .chunks_exact(4)
                        .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
                        .collect();
                    CosineDistance::eval(&sq_query, &v)
                })
                .collect();

            scored.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            scored.truncate(TOP_K_RAG);
            black_box(scored.len())
        })
    });

    group.finish();
}

// ── Criterion wiring ─────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_hybrid_spatial_vector,
    bench_hybrid_spatial_graph,
    bench_root_cause_analysis,
    bench_hybrid_rag,
);
criterion_main!(benches);
