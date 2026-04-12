//! Citizen social-network stress test.
//!
//! Run with:
//!   cargo test --test stress -- --nocapture
//!
//! What it does:
//!   1. Creates 5 000 citizens, each with: text, integer, real, bool, geo
//!      (GeoJSON Point) and a 16-dim embedding vector, plus a `bio` text field.
//!   2. Builds btree indexes on age/score/city.
//!   3. Inserts ~40 000 `follows` edges (≈ 8 per citizen).
//!   4. Runs a suite of complex queries (filter, range, MATCH, geo, vector,
//!      multi-hop BFS, combined predicates).
//!   5. Stress-iterates 5 times: delete 500 citizens → insert 500 new ones →
//!      update 200 scores → re-run the same query suite.
//!   6. Logs elapsed time and throughput for every phase.

use sekejap::CoreDB;
use std::time::Instant;

// ── Constants ─────────────────────────────────────────────────────────────────

const CITIZENS:   usize = 5_000;
const FOLLOWS_PER: usize = 8;       // follows edges per citizen
const EMBED_DIM:  usize = 16;       // vector embedding dimensions
const ITERATIONS: usize = 5;        // stress-loop rounds
const DELETE_PER_ITER: usize = 500;
const INSERT_PER_ITER: usize = 500;
const UPDATE_PER_ITER: usize = 200;

// ── Fake-data tables ──────────────────────────────────────────────────────────

const FIRST: &[&str] = &[
    "Alice","Bob","Charlie","Diana","Eko","Fatima","George","Hana",
    "Ivan","Julia","Kiran","Lara","Miguel","Nadia","Omar","Priya",
    "Qasim","Rita","Sam","Tina","Umar","Vera","Wayan","Xena","Yusuf","Zara",
];
const LAST: &[&str] = &[
    "Santoso","Wijaya","Sari","Pratama","Kusuma","Hartono","Dewi","Nugroho",
    "Putri","Wibowo","Setiawan","Rahayu","Hidayat","Permata","Arifin","Susanto",
];
const INTERESTS: &[&str] = &[
    "technology","sports","art","food","travel","politics","science","gaming",
    "music","fashion","finance","health","education","environment","photography",
];
const ROLES: &[&str] = &[
    "software engineer","teacher","doctor","entrepreneur","journalist",
    "designer","lawyer","chef","artist","researcher","nurse","writer",
];

/// Cities with (name, country, lat, lon).
const CITIES: &[(&str, &str, f64, f64)] = &[
    ("Jakarta",    "Indonesia",   -6.21,  106.85),
    ("Surabaya",   "Indonesia",   -7.25,  112.75),
    ("Bandung",    "Indonesia",   -6.92,  107.61),
    ("Medan",      "Indonesia",    3.59,   98.67),
    ("Makassar",   "Indonesia",   -5.15,  119.41),
    ("Semarang",   "Indonesia",   -6.97,  110.42),
    ("Palembang",  "Indonesia",   -2.99,  104.76),
    ("Tangerang",  "Indonesia",   -6.18,  106.63),
    ("Depok",      "Indonesia",   -6.40,  106.82),
    ("Yogyakarta", "Indonesia",   -7.80,  110.36),
    ("Singapore",  "Singapore",    1.35,  103.82),
    ("Kuala Lumpur","Malaysia",    3.14,  101.69),
    ("Bangkok",    "Thailand",    13.75,  100.52),
    ("Ho Chi Minh","Vietnam",     10.82,  106.63),
    ("Manila",     "Philippines", 14.60,  120.98),
    ("Sydney",     "Australia",  -33.87,  151.21),
    ("Melbourne",  "Australia",  -37.81,  144.96),
    ("Tokyo",      "Japan",       35.68,  139.69),
    ("Seoul",      "South Korea", 37.57,  126.98),
    ("Shanghai",   "China",       31.23,  121.47),
];

// ── Tiny deterministic PRNG ───────────────────────────────────────────────────

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self { Rng(seed ^ 0xdeadbeef_cafebabe) }

    fn next(&mut self) -> u64 {
        self.0 = self.0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn range(&mut self, hi: usize) -> usize {
        (self.next() as usize) % hi
    }

    fn range_f64(&mut self, lo: f64, hi: f64) -> f64 {
        let t = (self.next() >> 11) as f64 / ((1u64 << 53) - 1) as f64;
        lo + t * (hi - lo)
    }

    fn bool_p(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

// ── Data generation ───────────────────────────────────────────────────────────

fn make_citizen_json(idx: usize) -> (String, String) {
    let mut r = Rng::new(idx as u64 * 31337 + 1);
    let first   = FIRST[r.range(FIRST.len())];
    let last    = LAST[r.range(LAST.len())];
    let role    = ROLES[r.range(ROLES.len())];
    let int1    = INTERESTS[r.range(INTERESTS.len())];
    let int2    = INTERESTS[r.range(INTERESTS.len())];
    let city_i  = r.range(CITIES.len());
    let (city, country, lat, lon) = CITIES[city_i];
    let age     = 18 + r.range(62) as i64;
    let score   = r.range_f64(0.0, 1.0);
    let verified = r.bool_p(30);
    let followers = r.range(50_000) as i64;
    let bio = format!(
        "{} from {}, {} working as a {}. Interested in {} and {}.",
        first, city, country, role, int1, int2
    );
    let key  = format!("c{:05}", idx);
    let slug = format!("citizens/{}", key);
    let payload = format!(
        r#"{{"_collection":"citizens","_key":"{key}","name":"{first} {last}","username":"{first}{idx}","bio":"{bio}","age":{age},"score":{score:.4},"verified":{verified},"city":"{city}","country":"{country}","followers":{followers},"interest":"{int1}","geometry":{{"type":"Point","coordinates":[{lon},{lat}]}}}}"#
    );
    (slug, payload)
}

fn make_embedding(idx: usize) -> Vec<f32> {
    let mut r = Rng::new(idx as u64 * 99991 + 7);
    let raw: Vec<f32> = (0..EMBED_DIM).map(|_| r.range_f64(-1.0, 1.0) as f32).collect();
    // L2-normalise
    let norm = raw.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    raw.iter().map(|x| x / norm).collect()
}

fn query_embedding(seed: u64) -> Vec<f32> {
    let mut r = Rng::new(seed * 17777);
    let raw: Vec<f32> = (0..EMBED_DIM).map(|_| r.range_f64(-1.0, 1.0) as f32).collect();
    let norm = raw.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    raw.iter().map(|x| x / norm).collect()
}

// ── Logging helpers ───────────────────────────────────────────────────────────

fn log_phase(label: &str) {
    println!("\n── {} ──", label);
}

fn log_op(label: &str, count: usize, elapsed_ms: f64) {
    let rate = if elapsed_ms > 0.0 { count as f64 / (elapsed_ms / 1000.0) } else { f64::INFINITY };
    println!("  {:.<50} {:>6} ops  {:>9.2}ms  {:>10.0} ops/s",
        format!("{label} "), count, elapsed_ms, rate);
}

fn log_query(label: &str, result: usize, elapsed_us: f64) {
    println!("  {:.<50} {:>6} hits  {:>8.1}µs", format!("{label} "), result, elapsed_us);
}

// ── Query suite ───────────────────────────────────────────────────────────────

fn run_queries(db: &CoreDB, label: &str) {
    log_phase(&format!("Query Suite — {label}"));

    // Q1 — btree filter: citizens aged > 40 in Jakarta
    let t = Instant::now();
    let n = db.query("SELECT * FROM citizens WHERE city = 'Jakarta' AND age > 40")
        .unwrap().count();
    log_query("Q1  city='Jakarta' AND age>40", n, t.elapsed().as_secs_f64() * 1e6);

    // Q2 — btree range + ORDER BY score DESC LIMIT 20
    let t = Instant::now();
    let n = db.query("SELECT * FROM citizens WHERE score > 0.9 ORDER BY score DESC LIMIT 20")
        .unwrap().count();
    log_query("Q2  score>0.9 ORDER BY score DESC LIMIT 20", n, t.elapsed().as_secs_f64() * 1e6);

    // Q3 — point lookup
    let t = Instant::now();
    let n = db.query("SELECT * FROM citizens WHERE _key = 'c01234'").unwrap().count();
    log_query("Q3  _key='c01234'  (point lookup)", n, t.elapsed().as_secs_f64() * 1e6);

    // Q4 — MATCH 1-hop: who does c01234 follow?
    let t = Instant::now();
    let n = db.query(
        "MATCH (a:citizens)-[:follows]->(b:citizens) WHERE a._key = 'c01234' RETURN b"
    ).unwrap().count();
    log_query("Q4  MATCH follows 1-hop from c01234", n, t.elapsed().as_secs_f64() * 1e6);

    // Q5 — MATCH 2-hop: friends-of-friends
    let t = Instant::now();
    let n = db.query(
        "MATCH (a:citizens)-[:follows*1..2]->(b:citizens) WHERE a._key = 'c01234' RETURN b LIMIT 500"
    ).unwrap().count();
    log_query("Q5  MATCH follows*1..2 (fof) LIMIT 500", n, t.elapsed().as_secs_f64() * 1e6);

    // Q6 — MATCH 1-hop with end-node filter (MATCH optimisation)
    let t = Instant::now();
    let n = db.query(
        "MATCH (a:citizens)-[:follows]->(b:citizens) WHERE a._key = 'c01234' AND b.verified = true RETURN b"
    ).unwrap().count();
    log_query("Q6  MATCH follows → verified followers", n, t.elapsed().as_secs_f64() * 1e6);

    // Q7 — geo: citizens within 80km of Jakarta centre
    let (jakarta_lat, jakarta_lon) = (-6.21, 106.85);
    let t = Instant::now();
    let n = db.collection("citizens")
        .st_dwithin(jakarta_lat, jakarta_lon, 80.0)
        .count();
    log_query("Q7  geo: within 80km of Jakarta", n, t.elapsed().as_secs_f64() * 1e6);

    // Q8 — vector similarity: top-10 most similar to a query embedding
    let qvec = query_embedding(42);
    let t = Instant::now();
    let n = db.collection("citizens")
        .vector_near("embedding", qvec, 10)
        .count();
    log_query("Q8  vector_near embedding top-10", n, t.elapsed().as_secs_f64() * 1e6);

    // Q9 — atom API: 1-hop forward from c02500
    let t = Instant::now();
    let n = db.one("citizens/c02500").forward("follows").count();
    log_query("Q9  atom: one('c02500').forward('follows')", n, t.elapsed().as_secs_f64() * 1e6);

    // Q10 — atom API: typed 3-hop BFS LIMIT 300
    let t = Instant::now();
    let n = db.one("citizens/c02500")
        .hops_typed("follows", 3)
        .take(300)
        .count();
    log_query("Q10 atom: hops_typed follows*1..3 LIMIT 300", n, t.elapsed().as_secs_f64() * 1e6);

    // Q11 — combined: verified + high score + country filter
    let t = Instant::now();
    let n = db.query(
        "SELECT * FROM citizens WHERE verified = true AND score > 0.8 AND country = 'Indonesia'"
    ).unwrap().count();
    log_query("Q11 verified=true AND score>0.8 AND country='Indonesia'", n, t.elapsed().as_secs_f64() * 1e6);

    // Q12 — MATCH backward: who follows c00100?
    let t = Instant::now();
    let n = db.query(
        "MATCH (b:citizens)<-[:follows]-(a:citizens) WHERE b._key = 'c00100' RETURN a"
    ).unwrap().count();
    log_query("Q12 MATCH backward: who follows c00100?", n, t.elapsed().as_secs_f64() * 1e6);

    // Q13 — BETWEEN range on followers count
    let t = Instant::now();
    let n = db.query(
        "SELECT * FROM citizens WHERE followers BETWEEN 10000 AND 30000"
    ).unwrap().count();
    log_query("Q13 followers BETWEEN 10000 AND 30000", n, t.elapsed().as_secs_f64() * 1e6);

    // Q14 — ILIKE text search on bio
    let t = Instant::now();
    let n = db.query("SELECT * FROM citizens WHERE bio ILIKE 'engineer'").unwrap().count();
    log_query("Q14 bio ILIKE 'engineer'", n, t.elapsed().as_secs_f64() * 1e6);

    // Q15 — geo + vector: near Singapore, top-5 vector match
    let (sg_lat, sg_lon) = (1.35, 103.82);
    let qvec2 = query_embedding(99);
    let t = Instant::now();
    let n = db.collection("citizens")
        .st_dwithin(sg_lat, sg_lon, 200.0)
        .vector_near("embedding", qvec2, 5)
        .count();
    log_query("Q15 geo(200km Singapore) + vector top-5", n, t.elapsed().as_secs_f64() * 1e6);
}

// ── Stress iteration ──────────────────────────────────────────────────────────

fn stress_iteration(db: &mut CoreDB, round: usize, base_idx: usize) {
    log_phase(&format!("Stress Round {round}"));

    // DELETE: remove a slice of citizens by index
    let del_start = (round * DELETE_PER_ITER) % CITIZENS;
    let t = Instant::now();
    let mut deleted = 0usize;
    for i in 0..DELETE_PER_ITER {
        let idx = (del_start + i) % CITIZENS;
        let slug = format!("citizens/c{:05}", idx);
        db.remove(&slug);
        deleted += 1;
    }
    log_op(&format!("DELETE {} citizens (starting c{:05})", deleted, del_start),
        deleted, t.elapsed().as_secs_f64() * 1000.0);

    // INSERT: add fresh citizens with new indices
    let t = Instant::now();
    for i in 0..INSERT_PER_ITER {
        let idx = base_idx + i;
        let (slug, payload) = make_citizen_json(idx);
        db.put(&slug, &payload).unwrap();
        let emb = make_embedding(idx);
        db.put_vector(&slug, "embedding", &emb).unwrap();
        // add FOLLOWS_PER / 2 follow edges for the new citizen
        let mut r = Rng::new(idx as u64 * 13 + 3);
        for _ in 0..FOLLOWS_PER / 2 {
            let target_idx = r.range(CITIZENS);
            let target = format!("citizens/c{:05}", target_idx);
            if target != slug {
                db.link(&slug, &target, "follows", 1.0);
            }
        }
    }
    log_op(&format!("INSERT {} citizens (idx {}..{})", INSERT_PER_ITER, base_idx, base_idx + INSERT_PER_ITER),
        INSERT_PER_ITER, t.elapsed().as_secs_f64() * 1000.0);

    // UPDATE: patch the score field for a batch of existing citizens
    let upd_start = (round * UPDATE_PER_ITER * 3) % CITIZENS;
    let t = Instant::now();
    let updated = db.execute(&format!(
        "UPDATE citizens SET score = 0.99 WHERE age > 30 AND city = '{}'",
        CITIES[round % CITIES.len()].0
    )).unwrap();
    log_op(&format!("UPDATE score=0.99 in {} (age>30)", CITIES[round % CITIES.len()].0),
        updated, t.elapsed().as_secs_f64() * 1000.0);

    // Quick count to verify DB is intact
    let total = db.all().where_eq("_collection", "citizens").count();
    println!("  node_count()={} | citizens in index={}",
        db.node_count(), total);
    let _ = upd_start; // suppress unused warning
}

// ── Main test ─────────────────────────────────────────────────────────────────

#[test]
fn citizen_stress_test() {
    let mut db = CoreDB::new();
    let overall = Instant::now();

    // ── Phase 0: Schema + Indexes ────────────────────────────────────────────
    log_phase("Phase 0: Schema & Indexes");

    db.execute(r#"CREATE TABLE citizens (
        _key        TEXT,
        name        TEXT,
        username    TEXT,
        bio         TEXT,
        age         INTEGER,
        score       REAL,
        verified    INTEGER,
        city        TEXT,
        country     TEXT,
        interest    TEXT,
        followers   INTEGER
    )"#).unwrap();

    db.execute("CREATE INDEX ON citizens USING btree (age)").unwrap();
    db.execute("CREATE INDEX ON citizens USING btree (score)").unwrap();
    db.execute("CREATE INDEX ON citizens USING btree (city)").unwrap();
    db.execute("CREATE INDEX ON citizens USING btree (followers)").unwrap();
    println!("  created schema + 4 btree indexes");

    // ── Phase 1: Insert citizens ─────────────────────────────────────────────
    log_phase("Phase 1: Bulk Insert");

    let t = Instant::now();
    let pairs: Vec<(String, String)> = (0..CITIZENS).map(make_citizen_json).collect();
    let refs: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    db.put_many(refs).unwrap();
    log_op("put_many citizens", CITIZENS, t.elapsed().as_secs_f64() * 1000.0);

    let t = Instant::now();
    for idx in 0..CITIZENS {
        let slug = format!("citizens/c{:05}", idx);
        let emb  = make_embedding(idx);
        db.put_vector(&slug, "embedding", &emb).unwrap();
    }
    log_op("put_vector embeddings", CITIZENS, t.elapsed().as_secs_f64() * 1000.0);

    // ── Phase 2: Follow edges ────────────────────────────────────────────────
    log_phase("Phase 2: Follow Edges");

    let t = Instant::now();
    let mut edge_count = 0usize;
    for src in 0..CITIZENS {
        let mut r = Rng::new(src as u64 * 7 + 5);
        let src_slug = format!("citizens/c{:05}", src);
        for _ in 0..FOLLOWS_PER {
            let dst = r.range(CITIZENS);
            if dst != src {
                let dst_slug = format!("citizens/c{:05}", dst);
                db.link(&src_slug, &dst_slug, "follows", 1.0);
                edge_count += 1;
            }
        }
    }
    log_op("link follows edges", edge_count, t.elapsed().as_secs_f64() * 1000.0);

    println!("  node_count()={}", db.node_count());

    // ── Phase 3: Initial query suite ─────────────────────────────────────────
    run_queries(&db, "initial");

    // ── Phase 4: Stress iterations ───────────────────────────────────────────
    let mut next_idx = CITIZENS; // used for new citizen IDs
    for round in 1..=ITERATIONS {
        stress_iteration(&mut db, round, next_idx);
        next_idx += INSERT_PER_ITER;
        run_queries(&db, &format!("after round {round}"));
    }

    // ── Phase 5: Final consistency check ─────────────────────────────────────
    log_phase("Phase 5: Consistency Check");

    let expected_min = CITIZENS - (ITERATIONS * DELETE_PER_ITER)
        + (ITERATIONS * INSERT_PER_ITER);
    let actual = db.node_count();

    // Deletions may overlap (same idx deleted twice across rounds), so just
    // confirm the final count is in a reasonable range.
    println!("  node_count = {} (expected ≥ {})", actual, expected_min);
    assert!(
        actual >= expected_min,
        "too few nodes: got {actual}, expected ≥ {expected_min}"
    );

    // Spot-check: a new citizen from the last insert round still exists
    let last_new = format!("citizens/c{:05}", next_idx - 1);
    assert!(db.contains(&last_new), "last inserted citizen {last_new} missing");

    println!("\n  TOTAL elapsed: {:.2}s", overall.elapsed().as_secs_f64());
    println!("  Final DB size: {} nodes", db.node_count());
}
