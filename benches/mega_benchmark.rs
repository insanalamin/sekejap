//! MEGA BENCHMARK — 20 scenarios across filtering, graph, spatial, vector, and hybrid.
//!
//! All sekejap runs use CoreDB::open() (disk-backed, production mode).
//!
//! sekejap_sql    = db.query(sql)             — SQL surface
//! sekejap_atomic = db.collection()...chain() — atomic builder API
//! sqlite         = rusqlite in-memory with all applicable indexes + R*Tree
//!
//! Dataset
//! ───────
//! venues    20 000 nodes  Melbourne GeoJSON points, 64-dim embeddings, text content
//! services    255 nodes   binary dependency tree (depth 8) for root-cause analysis
//! edges    ~60 254 edges  related_to (3 per venue, skip-linked) + depends_on (tree)
//!
//! Case list
//! ─────────
//!  1  eq_filter              WHERE category = 'cafe'               ~2 000 hits
//!  2  neq_filter             WHERE category != 'hospital'          ~18 000 hits
//!  3  range_filter           WHERE price > 100 AND price <= 300    ~4 000 hits
//!  4  sort_limit             ORDER BY rating DESC LIMIT 50
//!  5  point_lookup           WHERE _key = 'v9999'                  1 hit
//!  6  compound_filter        WHERE category='cafe' AND suburb='fitzroy'  ~200 hits
//!  7  compound_sort_limit    WHERE category='restaurant' ORDER BY price ASC LIMIT 20
//!  8  graph_1hop             forward 1 hop from v5000              3 hits
//!  9  graph_5hop_bfs         5-hop BFS from v1234, 3 edges/node
//! 10  root_cause_bfs         BFS + Leaves on 255-node binary tree
//! 11  shortest_path          MATCH SHORTEST svc200 → svc0
//! 12  st_dwithin_5km         ST_DWithin 5 km around Melbourne CBD
//! 13  st_within_polygon      ST_Within CBD polygon
//! 14  spatial_category       ST_DWithin 3 km + WHERE category = 'hospital'
//! 15  vector_hnsw_top20      HNSW top-20 on 20 k, 64-dim
//! 16  hybrid_spatial_vector  spatial 5 km → HNSW top-10
//! 17  hybrid_spatial_graph   spatial 5 km → 1-hop related_to
//! 18  hybrid_graph_vector    3-hop BFS → HNSW top-10 re-rank       ← NEW
//! 19  hybrid_ilike_rag       GIN ILIKE 'Maribyrnong' → HNSW top-20
//! 20  holy_trinity           spatial 3 km → 1-hop → HNSW top-5     ← NEW

use criterion::{criterion_group, criterion_main, Criterion, black_box};
use rusqlite::{Connection, params};
use sekejap::{CosineDistance, CoreDB, Distance};
use serde_json::json;

// ── Dataset constants ─────────────────────────────────────────────────────────

const VENUES: usize  = 20_000;
const VEC_DIM: usize = 64;
const SERVICES: usize = 255;

const CENTRE_LAT: f64 = -37.8136;
const CENTRE_LON: f64 = 144.9631;

const CATEGORIES: &[&str] = &[
    "cafe","restaurant","park","hospital","school",
    "shop","office","gym","clinic","library",
];
const SUBURBS: &[&str] = &[
    "fitzroy","melbourne","collingwood","richmond","carlton",
    "brunswick","northcote","prahran","southbank","docklands",
];

// ── Data generators ───────────────────────────────────────────────────────────

fn make_vec(seed: usize) -> Vec<f32> {
    (0..VEC_DIM).map(|i| {
        let x = seed.wrapping_mul(6364136223846793005)
                    .wrapping_add(i.wrapping_mul(1442695040888963407));
        (x >> 33) as f32 / u32::MAX as f32 * 2.0 - 1.0
    }).collect()
}

fn vlat(i: usize)   -> f64          { CENTRE_LAT - 0.20 + (i % 400) as f64 * 0.001 }
fn vlon(i: usize)   -> f64          { CENTRE_LON - 0.25 + (i % 500) as f64 * 0.001 }
fn vcat(i: usize)   -> &'static str { CATEGORIES[i % CATEGORIES.len()] }
fn vsub(i: usize)   -> &'static str { SUBURBS[i % SUBURBS.len()] }
fn vrat(i: usize)   -> f64          { 1.0 + (i % 40) as f64 * 0.1 }
fn vprice(i: usize) -> f64          { 10.0 + (i % 49) as f64 * 10.0 }
fn vcontent(i: usize) -> String {
    if i % 5 == 0 {
        format!("Popular venue near the Maribyrnong River in {}, category {}.", vsub(i), vcat(i))
    } else {
        format!("A great {} in {}. Rated {:.1} stars, price {:.0}.", vcat(i), vsub(i), vrat(i), vprice(i))
    }
}

// ── Shared utilities ──────────────────────────────────────────────────────────

fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6371.0_f64;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    2.0 * r * a.sqrt().asin()
}

fn blob_to_vec(blob: Vec<u8>) -> Vec<f32> {
    blob.chunks_exact(4)
        .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
        .collect()
}

fn cosine_top_k(scores: &mut Vec<f32>, k: usize) -> usize {
    scores.sort_unstable_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    scores.truncate(k);
    scores.len()
}

// ── Sekejap setup (disk-backed) ───────────────────────────────────────────────

fn setup_sekejap() -> (CoreDB, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = CoreDB::open(dir.path()).unwrap();

    for i in 0..VENUES {
        let slug = format!("venues/v{i}");
        db.put(&slug, &json!({
            "_collection": "venues",
            "_key": format!("v{i}"),
            "name":     format!("Venue {i}"),
            "category": vcat(i),
            "suburb":   vsub(i),
            "rating":   vrat(i),
            "price":    vprice(i),
            "content":  vcontent(i),
            "geometry": { "type": "Point", "coordinates": [vlon(i), vlat(i)] },
        }).to_string()).unwrap();
        db.put_vector(&slug, "emb", &make_vec(i)).unwrap();
    }

    // 3 outgoing related_to edges per venue (skip-linked for variety)
    for i in 0..VENUES {
        for d in [7usize, 13, 31] {
            db.link(
                &format!("venues/v{i}"),
                &format!("venues/v{}", (i + d) % VENUES),
                "related_to", 1.0,
            );
        }
    }

    // Services: full binary tree depth-8 (255 nodes)
    for i in 0..SERVICES {
        db.put(&format!("services/svc{i}"), &json!({
            "_collection": "services",
            "_key": format!("svc{i}"),
            "status": if i == 0 { "failed" } else { "healthy" },
        }).to_string()).unwrap();
    }
    for i in 1..SERVICES {
        db.link(
            &format!("services/svc{i}"),
            &format!("services/svc{}", (i - 1) / 2),
            "depends_on", 1.0,
        );
    }

    db.build_spatial_index();
    db.build_gin_index("content");
    db.build_hnsw_index("emb", 16, 200).unwrap();

    (db, dir)
}

// ── SQLite setup ──────────────────────────────────────────────────────────────

fn setup_sqlite() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("
        CREATE TABLE venues (
            key TEXT PRIMARY KEY, name TEXT, category TEXT, suburb TEXT,
            rating REAL, price REAL, content TEXT, lat REAL, lon REAL
        );
        CREATE INDEX v_cat     ON venues(category);
        CREATE INDEX v_sub     ON venues(suburb);
        CREATE INDEX v_price   ON venues(price);
        CREATE INDEX v_rating  ON venues(rating);
        CREATE INDEX v_cat_sub ON venues(category, suburb);

        CREATE VIRTUAL TABLE venues_rtree USING rtree(id, min_lat, max_lat, min_lon, max_lon);
        CREATE TABLE venues_rtmap (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT UNIQUE);

        CREATE TABLE vectors (key TEXT PRIMARY KEY, data BLOB);

        CREATE TABLE edges (from_key TEXT, to_key TEXT, kind TEXT);
        CREATE INDEX e_from_kind ON edges(from_key, kind);
        CREATE INDEX e_to_kind   ON edges(to_key,   kind);

        CREATE TABLE services (key TEXT PRIMARY KEY, status TEXT);
        CREATE TABLE dep_edges (from_key TEXT, to_key TEXT);
        CREATE INDEX dep_from ON dep_edges(from_key);
        CREATE INDEX dep_to   ON dep_edges(to_key);
    ").unwrap();

    conn.execute_batch("BEGIN").unwrap();

    // venues
    {
        let mut si = conn.prepare(
            "INSERT INTO venues (key,name,category,suburb,rating,price,content,lat,lon)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)"
        ).unwrap();
        let mut sm = conn.prepare("INSERT INTO venues_rtmap (key) VALUES (?1)").unwrap();
        let mut sr = conn.prepare(
            "INSERT INTO venues_rtree (id,min_lat,max_lat,min_lon,max_lon)
             VALUES (?1,?2,?2,?3,?3)"
        ).unwrap();
        let mut sv = conn.prepare("INSERT INTO vectors (key,data) VALUES (?1,?2)").unwrap();

        for i in 0..VENUES {
            let key = format!("v{i}");
            let lat = vlat(i);
            let lon = vlon(i);
            si.execute(params![
                &key, format!("Venue {i}"), vcat(i), vsub(i),
                vrat(i), vprice(i), vcontent(i), lat, lon
            ]).unwrap();
            sm.execute(params![&key]).unwrap();
            let rid = conn.last_insert_rowid();
            sr.execute(params![rid, lat, lon]).unwrap();
            let blob: Vec<u8> = make_vec(i).iter().flat_map(|f| f.to_le_bytes()).collect();
            sv.execute(params![&key, blob]).unwrap();
        }
    }

    // edges
    {
        let mut se = conn.prepare(
            "INSERT INTO edges (from_key,to_key,kind) VALUES (?1,?2,'related_to')"
        ).unwrap();
        for i in 0..VENUES {
            for d in [7usize, 13, 31] {
                se.execute(params![
                    format!("v{i}"),
                    format!("v{}", (i + d) % VENUES)
                ]).unwrap();
            }
        }
    }

    // services
    {
        let mut ss  = conn.prepare(
            "INSERT INTO services (key,status) VALUES (?1,?2)"
        ).unwrap();
        let mut sde = conn.prepare(
            "INSERT INTO dep_edges (from_key,to_key) VALUES (?1,?2)"
        ).unwrap();
        for i in 0..SERVICES {
            ss.execute(params![
                format!("svc{i}"),
                if i == 0 { "failed" } else { "healthy" }
            ]).unwrap();
        }
        for i in 1..SERVICES {
            sde.execute(params![
                format!("svc{i}"),
                format!("svc{}", (i - 1) / 2)
            ]).unwrap();
        }
    }

    conn.execute_batch("COMMIT").unwrap();
    conn
}

// ═════════════════════════════════════════════════════════════════════════════
// FILTERING
// ═════════════════════════════════════════════════════════════════════════════

// ── 01 eq_filter ─────────────────────────────────────────────────────────────

fn bench_01_eq_filter(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("01_eq_filter");
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query("SELECT _key FROM venues WHERE category = 'cafe'").unwrap().count()
    )));
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.collection("venues").where_eq("category", "cafe").count()
    )));
    g.bench_function("sqlite", |b| b.iter(|| {
        let mut s = sq.prepare_cached("SELECT key FROM venues WHERE category = 'cafe'").unwrap();
        black_box(s.query_map([], |_| Ok(())).unwrap().count())
    }));
    g.finish();
}

// ── 02 neq_filter ─────────────────────────────────────────────────────────────

fn bench_02_neq_filter(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("02_neq_filter");
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query("SELECT _key FROM venues WHERE category != 'hospital'").unwrap().count()
    )));
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.collection("venues").where_neq("category", "hospital").count()
    )));
    g.bench_function("sqlite", |b| b.iter(|| {
        let mut s = sq.prepare_cached("SELECT key FROM venues WHERE category != 'hospital'").unwrap();
        black_box(s.query_map([], |_| Ok(())).unwrap().count())
    }));
    g.finish();
}

// ── 03 range_filter ───────────────────────────────────────────────────────────

fn bench_03_range_filter(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("03_range_filter");
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query("SELECT _key FROM venues WHERE price > 100 AND price <= 300").unwrap().count()
    )));
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.collection("venues").where_gt("price", 100.0).where_lte("price", 300.0).count()
    )));
    g.bench_function("sqlite", |b| b.iter(|| {
        let mut s = sq.prepare_cached(
            "SELECT key FROM venues WHERE price > 100 AND price <= 300"
        ).unwrap();
        black_box(s.query_map([], |_| Ok(())).unwrap().count())
    }));
    g.finish();
}

// ── 04 sort_limit ─────────────────────────────────────────────────────────────

fn bench_04_sort_limit(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("04_sort_limit");
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query("SELECT _key, rating FROM venues ORDER BY rating DESC LIMIT 50").unwrap().count()
    )));
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.collection("venues").sort("rating", false).take(50).collect()
    )));
    g.bench_function("sqlite", |b| b.iter(|| {
        let mut s = sq.prepare_cached(
            "SELECT key, rating FROM venues ORDER BY rating DESC LIMIT 50"
        ).unwrap();
        black_box(s.query_map([], |_| Ok(())).unwrap().count())
    }));
    g.finish();
}

// ── 05 point_lookup ───────────────────────────────────────────────────────────

fn bench_05_point_lookup(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("05_point_lookup");
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query("SELECT * FROM venues WHERE _key = 'v9999'").unwrap().count()
    )));
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.one("venues/v9999").count()
    )));
    g.bench_function("sqlite", |b| b.iter(|| {
        let mut s = sq.prepare_cached("SELECT * FROM venues WHERE key = 'v9999'").unwrap();
        black_box(s.query_map([], |_| Ok(())).unwrap().count())
    }));
    g.finish();
}

// ── 06 compound_filter ────────────────────────────────────────────────────────

fn bench_06_compound_filter(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("06_compound_filter");
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query("SELECT _key FROM venues WHERE category = 'cafe' AND suburb = 'fitzroy'")
          .unwrap().count()
    )));
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.collection("venues")
          .where_eq("category", "cafe")
          .where_eq("suburb", "fitzroy")
          .count()
    )));
    g.bench_function("sqlite", |b| b.iter(|| {
        let mut s = sq.prepare_cached(
            "SELECT key FROM venues WHERE category = 'cafe' AND suburb = 'fitzroy'"
        ).unwrap();
        black_box(s.query_map([], |_| Ok(())).unwrap().count())
    }));
    g.finish();
}

// ── 07 compound_sort_limit ────────────────────────────────────────────────────

fn bench_07_compound_sort_limit(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("07_compound_sort_limit");
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query(
            "SELECT _key, price FROM venues WHERE category = 'restaurant' ORDER BY price ASC LIMIT 20"
        ).unwrap().count()
    )));
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.collection("venues")
          .where_eq("category", "restaurant")
          .sort("price", true)
          .take(20)
          .collect()
    )));
    g.bench_function("sqlite", |b| b.iter(|| {
        let mut s = sq.prepare_cached(
            "SELECT key, price FROM venues WHERE category = 'restaurant' ORDER BY price ASC LIMIT 20"
        ).unwrap();
        black_box(s.query_map([], |_| Ok(())).unwrap().count())
    }));
    g.finish();
}

// ═════════════════════════════════════════════════════════════════════════════
// GRAPH
// ═════════════════════════════════════════════════════════════════════════════

// ── 08 graph_1hop ─────────────────────────────────────────────────────────────

fn bench_08_graph_1hop(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("08_graph_1hop");
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query(
            "MATCH (a:venues)-[:related_to]->(b) WHERE a._key = 'v5000' RETURN b"
        ).unwrap().count()
    )));
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.one("venues/v5000").forward("related_to").count()
    )));
    g.bench_function("sqlite", |b| b.iter(|| {
        let mut s = sq.prepare_cached(
            "SELECT to_key FROM edges WHERE from_key = 'v5000' AND kind = 'related_to'"
        ).unwrap();
        black_box(s.query_map([], |_| Ok(())).unwrap().count())
    }));
    g.finish();
}

// ── 09 graph_5hop_bfs ─────────────────────────────────────────────────────────

fn bench_09_graph_5hop_bfs(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("09_graph_5hop_bfs");
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query(
            "MATCH (a:venues)-[:related_to*1..5]->(b) WHERE a._key = 'v1234' RETURN b LIMIT 5000"
        ).unwrap().count()
    )));
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.one("venues/v1234").hops_typed("related_to", 5).take(5000).count()
    )));
    g.bench_function("sqlite", |b| b.iter(|| {
        let mut s = sq.prepare_cached(
            "WITH RECURSIVE walk(key, depth) AS (
                SELECT 'v1234', 0
                UNION ALL
                SELECT e.to_key, w.depth + 1
                FROM walk w JOIN edges e ON e.from_key = w.key AND e.kind = 'related_to'
                WHERE w.depth < 5
            )
            SELECT DISTINCT key FROM walk WHERE depth > 0 LIMIT 5000"
        ).unwrap();
        black_box(s.query_map([], |_| Ok(())).unwrap().count())
    }));
    g.finish();
}

// ── 10 root_cause_analysis ────────────────────────────────────────────────────

fn bench_10_root_cause(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("10_root_cause_bfs_leaves");
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query(
            "MATCH (a:services)-[:depends_on*1..8]->(b) WHERE a._key = 'svc200' RETURN b"
        ).unwrap().count()
    )));
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.one("services/svc200").hops_typed("depends_on", 8).leaves().count()
    )));
    g.bench_function("sqlite", |b| b.iter(|| {
        let mut s = sq.prepare_cached(
            "WITH RECURSIVE chain(key) AS (
                SELECT 'svc200'
                UNION
                SELECT d.to_key FROM chain c JOIN dep_edges d ON d.from_key = c.key
            )
            SELECT COUNT(*) FROM chain
            WHERE key NOT IN (SELECT from_key FROM dep_edges)
              AND key != 'svc200'"
        ).unwrap();
        black_box(s.query_row([], |r| r.get::<_, i64>(0)).unwrap())
    }));
    g.finish();
}

// ── 11 shortest_path ──────────────────────────────────────────────────────────

fn bench_11_shortest_path(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("11_shortest_path");
    // sekejap: native BFS shortest path via MATCH SHORTEST SQL
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query(
            "SELECT a._key AS start, b._key AS end, r.length AS hops \
             FROM MATCH SHORTEST (a)-[r*]->(b) \
             WHERE a._key = 'svc200' AND b._key = 'svc0'"
        ).unwrap().count()
    )));
    // SQLite: recursive CTE shortest path (tree = no cycles, so UNION ALL + depth cap is safe)
    g.bench_function("sqlite", |b| b.iter(|| {
        let mut s = sq.prepare_cached(
            "WITH RECURSIVE walk(key, depth) AS (
                SELECT 'svc200', 0
                UNION ALL
                SELECT d.to_key, w.depth + 1
                FROM dep_edges d JOIN walk w ON d.from_key = w.key
                WHERE w.depth < 8
            )
            SELECT MIN(depth) FROM walk WHERE key = 'svc0'"
        ).unwrap();
        black_box(s.query_row([], |r| r.get::<_, Option<i64>>(0)).unwrap())
    }));
    g.finish();
}

// ═════════════════════════════════════════════════════════════════════════════
// SPATIAL
// ═════════════════════════════════════════════════════════════════════════════

// ── 12 st_dwithin_5km ─────────────────────────────────────────────────────────

fn bench_12_st_dwithin(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("12_st_dwithin_5km");
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query(&format!(
            "SELECT _key FROM venues \
             WHERE ST_DWithin(geometry, POINT({CENTRE_LON} {CENTRE_LAT}), 5.0)"
        )).unwrap().count()
    )));
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.collection("venues").st_dwithin(CENTRE_LAT, CENTRE_LON, 5.0).count()
    )));
    // SQLite: R*Tree bbox + Haversine in Rust
    g.bench_function("sqlite", |b| b.iter(|| {
        let dlat = 5.0 / 111.0_f64;
        let dlon = 5.0 / (111.0 * CENTRE_LAT.to_radians().cos());
        let mut s = sq.prepare_cached(
            "SELECT key, lat, lon FROM venues
             WHERE lat >= ?1 AND lat <= ?2 AND lon >= ?3 AND lon <= ?4"
        ).unwrap();
        let count = s.query_map(
            params![CENTRE_LAT-dlat, CENTRE_LAT+dlat, CENTRE_LON-dlon, CENTRE_LON+dlon],
            |r| Ok((r.get::<_,f64>(1)?, r.get::<_,f64>(2)?))
        ).unwrap().filter_map(|r| r.ok())
         .filter(|(lat, lon)| haversine_km(CENTRE_LAT, CENTRE_LON, *lat, *lon) <= 5.0)
         .count();
        black_box(count)
    }));
    g.finish();
}

// ── 13 st_within_polygon ──────────────────────────────────────────────────────

fn bench_13_st_within(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("13_st_within_polygon");
    // sekejap: real polygon containment (ray-casting)
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query(
            "SELECT _key FROM venues WHERE ST_Within(geometry, \
             POLYGON((144.95 -37.80, 144.98 -37.80, 144.98 -37.83, 144.95 -37.83, 144.95 -37.80)))"
        ).unwrap().count()
    )));
    // SQLite: bbox approximation only (no real polygon math)
    g.bench_function("sqlite_bbox_approx", |b| b.iter(|| {
        let mut s = sq.prepare_cached(
            "SELECT COUNT(*) FROM venues
             WHERE lat >= -37.83 AND lat <= -37.80
               AND lon >= 144.95 AND lon <= 144.98"
        ).unwrap();
        black_box(s.query_row([], |r| r.get::<_, i64>(0)).unwrap())
    }));
    g.finish();
}

// ── 14 spatial_category ───────────────────────────────────────────────────────

fn bench_14_spatial_category(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("14_spatial_category_filter");
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query(&format!(
            "SELECT _key FROM venues \
             WHERE ST_DWithin(geometry, POINT({CENTRE_LON} {CENTRE_LAT}), 3.0) \
             AND category = 'hospital'"
        )).unwrap().count()
    )));
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.collection("venues")
          .st_dwithin(CENTRE_LAT, CENTRE_LON, 3.0)
          .where_eq("category", "hospital")
          .count()
    )));
    g.bench_function("sqlite", |b| b.iter(|| {
        let dlat = 3.0 / 111.0_f64;
        let dlon = 3.0 / (111.0 * CENTRE_LAT.to_radians().cos());
        let mut s = sq.prepare_cached(
            "SELECT key, lat, lon FROM venues
             WHERE lat >= ?1 AND lat <= ?2 AND lon >= ?3 AND lon <= ?4
               AND category = 'hospital'"
        ).unwrap();
        let count = s.query_map(
            params![CENTRE_LAT-dlat, CENTRE_LAT+dlat, CENTRE_LON-dlon, CENTRE_LON+dlon],
            |r| Ok((r.get::<_,f64>(1)?, r.get::<_,f64>(2)?))
        ).unwrap().filter_map(|r| r.ok())
         .filter(|(lat, lon)| haversine_km(CENTRE_LAT, CENTRE_LON, *lat, *lon) <= 3.0)
         .count();
        black_box(count)
    }));
    g.finish();
}

// ═════════════════════════════════════════════════════════════════════════════
// VECTOR
// ═════════════════════════════════════════════════════════════════════════════

// ── 15 vector_hnsw_top20 ──────────────────────────────────────────────────────

fn bench_15_vector_hnsw(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let query = make_vec(VENUES + 1);
    let sql = {
        let coords: Vec<String> = query.iter().map(|f| format!("{f:.6}")).collect();
        format!("SELECT _key FROM venues WHERE VECTOR_NEAR(emb, [{}], 20)", coords.join(","))
    };
    let mut g = c.benchmark_group("15_vector_hnsw_top20");
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query(&sql).unwrap().count()
    )));
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.collection("venues").vector_near("emb", query.clone(), 20).count()
    )));
    // SQLite: full flat cosine scan — no vector index exists
    g.bench_function("sqlite_flat_scan", |b| b.iter(|| {
        let mut s = sq.prepare_cached("SELECT data FROM vectors").unwrap();
        let mut scored: Vec<f32> = s.query_map([], |r| r.get::<_, Vec<u8>>(0))
            .unwrap().filter_map(|r| r.ok())
            .map(|blob| CosineDistance::eval(&query, &blob_to_vec(blob)))
            .collect();
        black_box(cosine_top_k(&mut scored, 20))
    }));
    g.finish();
}

// ═════════════════════════════════════════════════════════════════════════════
// HYBRID
// ═════════════════════════════════════════════════════════════════════════════

// ── 16 hybrid_spatial_vector ─────────────────────────────────────────────────

fn bench_16_hybrid_spatial_vector(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let query = make_vec(VENUES + 2);
    let mut g = c.benchmark_group("16_hybrid_spatial_vector");
    // sekejap: fused spatial index → HNSW re-rank
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.collection("venues")
          .st_dwithin(CENTRE_LAT, CENTRE_LON, 5.0)
          .vector_near("emb", query.clone(), 10)
          .count()
    )));
    // SQLite: bbox → load BLOBs → cosine top-10 in Rust
    g.bench_function("sqlite", |b| b.iter(|| {
        let dlat = 5.0 / 111.0_f64;
        let dlon = 5.0 / (111.0 * CENTRE_LAT.to_radians().cos());
        let mut s = sq.prepare_cached(
            "SELECT v.key, v.lat, v.lon, vec.data
             FROM venues v JOIN vectors vec ON vec.key = v.key
             WHERE v.lat >= ?1 AND v.lat <= ?2 AND v.lon >= ?3 AND v.lon <= ?4"
        ).unwrap();
        let mut scored: Vec<f32> = s.query_map(
            params![CENTRE_LAT-dlat, CENTRE_LAT+dlat, CENTRE_LON-dlon, CENTRE_LON+dlon],
            |r| Ok((r.get::<_,f64>(1)?, r.get::<_,f64>(2)?, r.get::<_,Vec<u8>>(3)?))
        ).unwrap().filter_map(|r| r.ok())
         .filter(|(lat, lon, _)| haversine_km(CENTRE_LAT, CENTRE_LON, *lat, *lon) <= 5.0)
         .map(|(_, _, blob)| CosineDistance::eval(&query, &blob_to_vec(blob)))
         .collect();
        black_box(cosine_top_k(&mut scored, 10))
    }));
    g.finish();
}

// ── 17 hybrid_spatial_graph ───────────────────────────────────────────────────

fn bench_17_hybrid_spatial_graph(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let mut g = c.benchmark_group("17_hybrid_spatial_graph");
    // sekejap: spatial index → graph hop in one pipeline
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.collection("venues")
          .st_dwithin(CENTRE_LAT, CENTRE_LON, 5.0)
          .forward("related_to")
          .count()
    )));
    // SQLite: bbox → haversine filter → iterate edge table
    g.bench_function("sqlite", |b| b.iter(|| {
        let dlat = 5.0 / 111.0_f64;
        let dlon = 5.0 / (111.0 * CENTRE_LAT.to_radians().cos());
        let mut s1 = sq.prepare_cached(
            "SELECT key, lat, lon FROM venues
             WHERE lat >= ?1 AND lat <= ?2 AND lon >= ?3 AND lon <= ?4"
        ).unwrap();
        let near: Vec<String> = s1.query_map(
            params![CENTRE_LAT-dlat, CENTRE_LAT+dlat, CENTRE_LON-dlon, CENTRE_LON+dlon],
            |r| Ok((r.get::<_,String>(0)?, r.get::<_,f64>(1)?, r.get::<_,f64>(2)?))
        ).unwrap().filter_map(|r| r.ok())
         .filter(|(_, lat, lon)| haversine_km(CENTRE_LAT, CENTRE_LON, *lat, *lon) <= 5.0)
         .map(|(k, _, _)| k)
         .collect();
        let count: usize = near.iter().map(|k| {
            let mut s2 = sq.prepare_cached(
                "SELECT COUNT(*) FROM edges WHERE from_key = ?1 AND kind = 'related_to'"
            ).unwrap();
            s2.query_row(params![k], |r| r.get::<_,i64>(0)).unwrap_or(0) as usize
        }).sum();
        black_box(count)
    }));
    g.finish();
}

// ── 18 hybrid_graph_vector (NEW) ─────────────────────────────────────────────

fn bench_18_hybrid_graph_vector(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let query = make_vec(VENUES + 3);
    let mut g = c.benchmark_group("18_hybrid_graph_vector");
    // sekejap: 3-hop BFS → HNSW re-rank, single fused pipeline
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.one("venues/v5000")
          .hops_typed("related_to", 3)
          .vector_near("emb", query.clone(), 10)
          .count()
    )));
    // SQLite: recursive CTE 3 hops → fetch BLOBs → cosine top-10 in Rust
    g.bench_function("sqlite", |b| b.iter(|| {
        let mut s = sq.prepare_cached(
            "WITH RECURSIVE walk(key, depth) AS (
                SELECT 'v5000', 0
                UNION ALL
                SELECT e.to_key, w.depth + 1
                FROM walk w JOIN edges e ON e.from_key = w.key AND e.kind = 'related_to'
                WHERE w.depth < 3
            )
            SELECT DISTINCT vec.data
            FROM walk w JOIN vectors vec ON vec.key = w.key
            WHERE w.depth > 0"
        ).unwrap();
        let mut scored: Vec<f32> = s.query_map([], |r| r.get::<_, Vec<u8>>(0))
            .unwrap().filter_map(|r| r.ok())
            .map(|blob| CosineDistance::eval(&query, &blob_to_vec(blob)))
            .collect();
        black_box(cosine_top_k(&mut scored, 10))
    }));
    g.finish();
}

// ── 19 hybrid_ilike_vector_rag ────────────────────────────────────────────────

fn bench_19_hybrid_rag(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let query = make_vec(VENUES + 4);
    let mut g = c.benchmark_group("19_hybrid_ilike_vector_rag");
    g.bench_function("sekejap_sql", |b| b.iter(|| black_box(
        sk.query("SELECT _key FROM venues WHERE content ILIKE '%Maribyrnong%'")
          .unwrap().count()
    )));
    // sekejap: GIN trigram index → HNSW re-rank, fused pipeline
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.collection("venues")
          .ilike("content", "%Maribyrnong%")
          .vector_near("emb", query.clone(), 20)
          .count()
    )));
    // SQLite: LIKE scan (no trigram index) → load BLOBs → cosine top-20
    g.bench_function("sqlite", |b| b.iter(|| {
        let mut s = sq.prepare_cached(
            "SELECT vec.data FROM venues v JOIN vectors vec ON vec.key = v.key
             WHERE v.content LIKE '%Maribyrnong%'"
        ).unwrap();
        let mut scored: Vec<f32> = s.query_map([], |r| r.get::<_, Vec<u8>>(0))
            .unwrap().filter_map(|r| r.ok())
            .map(|blob| CosineDistance::eval(&query, &blob_to_vec(blob)))
            .collect();
        black_box(cosine_top_k(&mut scored, 20))
    }));
    g.finish();
}

// ── 20 holy_trinity: spatial → graph → vector (NEW) ──────────────────────────

fn bench_20_holy_trinity(c: &mut Criterion) {
    let (sk, _dir) = setup_sekejap();
    let sq = setup_sqlite();
    let query = make_vec(VENUES + 5);
    let mut g = c.benchmark_group("20_holy_trinity_spatial_graph_vector");

    // sekejap: ONE fused atomic pipeline — spatial index → graph hop → HNSW top-5
    g.bench_function("sekejap_atomic", |b| b.iter(|| black_box(
        sk.collection("venues")
          .st_dwithin(CENTRE_LAT, CENTRE_LON, 3.0)
          .forward("related_to")
          .vector_near("emb", query.clone(), 5)
          .count()
    )));

    // SQLite: three separate steps stitched in Rust — no single-query equivalent exists
    g.bench_function("sqlite", |b| b.iter(|| {
        // Step 1: spatial (bbox + haversine)
        let dlat = 3.0 / 111.0_f64;
        let dlon = 3.0 / (111.0 * CENTRE_LAT.to_radians().cos());
        let mut s1 = sq.prepare_cached(
            "SELECT key, lat, lon FROM venues
             WHERE lat >= ?1 AND lat <= ?2 AND lon >= ?3 AND lon <= ?4"
        ).unwrap();
        let near: Vec<String> = s1.query_map(
            params![CENTRE_LAT-dlat, CENTRE_LAT+dlat, CENTRE_LON-dlon, CENTRE_LON+dlon],
            |r| Ok((r.get::<_,String>(0)?, r.get::<_,f64>(1)?, r.get::<_,f64>(2)?))
        ).unwrap().filter_map(|r| r.ok())
         .filter(|(_, lat, lon)| haversine_km(CENTRE_LAT, CENTRE_LON, *lat, *lon) <= 3.0)
         .map(|(k, _, _)| k)
         .collect();

        // Step 2: 1-hop forward edge traversal
        let mut neighbors: Vec<String> = near.iter().flat_map(|k| {
            let mut s2 = sq.prepare_cached(
                "SELECT to_key FROM edges WHERE from_key = ?1 AND kind = 'related_to'"
            ).unwrap();
            s2.query_map(params![k], |r| r.get::<_, String>(0))
              .unwrap().filter_map(|r| r.ok())
              .collect::<Vec<_>>()
        }).collect();
        neighbors.sort_unstable();
        neighbors.dedup();

        // Step 3: load BLOBs for neighbors → cosine top-5
        let mut scored: Vec<f32> = neighbors.iter().filter_map(|k| {
            let mut s3 = sq.prepare_cached(
                "SELECT data FROM vectors WHERE key = ?1"
            ).unwrap();
            s3.query_row(params![k], |r| r.get::<_, Vec<u8>>(0)).ok()
        }).map(|blob| CosineDistance::eval(&query, &blob_to_vec(blob)))
          .collect();
        black_box(cosine_top_k(&mut scored, 5))
    }));
    g.finish();
}

// ── Criterion wiring ──────────────────────────────────────────────────────────

criterion_group!(
    benches,
    bench_01_eq_filter,
    bench_02_neq_filter,
    bench_03_range_filter,
    bench_04_sort_limit,
    bench_05_point_lookup,
    bench_06_compound_filter,
    bench_07_compound_sort_limit,
    bench_08_graph_1hop,
    bench_09_graph_5hop_bfs,
    bench_10_root_cause,
    bench_11_shortest_path,
    bench_12_st_dwithin,
    bench_13_st_within,
    bench_14_spatial_category,
    bench_15_vector_hnsw,
    bench_16_hybrid_spatial_vector,
    bench_17_hybrid_spatial_graph,
    bench_18_hybrid_graph_vector,
    bench_19_hybrid_rag,
    bench_20_holy_trinity,
);
criterion_main!(benches);
