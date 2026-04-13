use sekejap::CoreDB;

// ── Test 1: CREATE INDEX on empty table always succeeds ───────────────────────

#[test]
fn create_hnsw_on_empty_table_succeeds() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE docs (_key TEXT PRIMARY KEY, emb VECTOR)").unwrap();

    // Declaring HNSW on an empty table must always succeed — every database
    // allows declaring indexes before data is present.
    db.execute("CREATE INDEX ON docs USING hnsw (emb)").unwrap();

    // Insert a vector — HNSW is rebuilt automatically.
    db.execute("INSERT INTO docs (_key, emb) VALUES ('d1', [1.0, 0.0, 0.0, 0.0])").unwrap();
    db.execute("INSERT INTO docs (_key, emb) VALUES ('d2', [0.9, 0.1, 0.0, 0.0])").unwrap();

    // Query must work without an explicit REINDEX.
    let hits = db
        .query("SELECT * FROM docs WHERE VECTOR_NEAR(emb, [1.0, 0.0, 0.0, 0.0], 1)")
        .unwrap()
        .collect();

    assert_eq!(hits.len(), 1, "expected 1 nearest neighbour");
    assert_eq!(hits[0].slug, "docs/d1");
}

// ── Test 2: GIN auto-maintained when docs are inserted ───────────────────────

#[test]
fn gin_auto_maintained_on_insert() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE venues (_key TEXT PRIMARY KEY, name TEXT)").unwrap();

    // Declare GIN on empty table — must succeed.
    db.execute("CREATE INDEX ON venues USING gin (name)").unwrap();

    // Insert docs after declaring the index.
    db.execute("INSERT INTO venues (_key, name) VALUES ('v1', 'Fitzroy Pub')").unwrap();
    db.execute("INSERT INTO venues (_key, name) VALUES ('v2', 'Collingwood Bar')").unwrap();
    db.execute("INSERT INTO venues (_key, name) VALUES ('v3', 'Fitzroy Gardens Cafe')").unwrap();

    // ILIKE query must find the Fitzroy venues without any explicit REINDEX.
    let hits = db
        .query("SELECT * FROM venues WHERE name ILIKE '%Fitzroy%'")
        .unwrap()
        .collect();

    assert_eq!(hits.len(), 2, "expected 2 Fitzroy venues, got {}", hits.len());
    let slugs: std::collections::HashSet<_> = hits.iter().map(|h| h.slug.as_str()).collect();
    assert!(slugs.contains("venues/v1"), "expected venues/v1 in results");
    assert!(slugs.contains("venues/v3"), "expected venues/v3 in results");
}

// ── Test 3: REINDEX rebuilds GIN ─────────────────────────────────────────────

#[test]
fn reindex_rebuilds_gin() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE venues (_key TEXT PRIMARY KEY, name TEXT)").unwrap();

    // Insert before building the index.
    db.execute("INSERT INTO venues (_key, name) VALUES ('v1', 'Fitzroy Pub')").unwrap();
    db.execute("INSERT INTO venues (_key, name) VALUES ('v2', 'Collingwood Bar')").unwrap();
    db.execute("INSERT INTO venues (_key, name) VALUES ('v3', 'Fitzroy Gardens Cafe')").unwrap();

    // CREATE INDEX — data exists so GIN is built immediately.
    db.execute("CREATE INDEX ON venues USING gin (name)").unwrap();

    // REINDEX rebuilds it (no error).
    db.execute("REINDEX ON venues USING gin (name)").unwrap();

    // ILIKE query should find the Fitzroy venues.
    let hits = db
        .query("SELECT * FROM venues WHERE name ILIKE '%Fitzroy%'")
        .unwrap()
        .collect();

    assert_eq!(hits.len(), 2, "expected 2 Fitzroy venues, got {:?}", hits.len());
    let slugs: std::collections::HashSet<_> = hits.iter().map(|h| h.slug.as_str()).collect();
    assert!(slugs.contains("venues/v1"), "expected venues/v1 in results");
    assert!(slugs.contains("venues/v3"), "expected venues/v3 in results");
}

// ── Test 4: REINDEX rebuilds HNSW after adding vectors ───────────────────────

#[test]
fn reindex_rebuilds_hnsw_after_adding_vectors() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE docs (_key TEXT PRIMARY KEY, emb VECTOR)").unwrap();

    // CREATE INDEX on empty table — must succeed.
    db.execute("CREATE INDEX ON docs USING hnsw (emb)").unwrap();

    // Insert vectors.
    db.execute("INSERT INTO docs (_key, emb) VALUES ('d1', [1.0, 0.0, 0.0, 0.0])").unwrap();
    db.execute("INSERT INTO docs (_key, emb) VALUES ('d2', [0.9, 0.1, 0.0, 0.0])").unwrap();
    db.execute("INSERT INTO docs (_key, emb) VALUES ('d3', [0.0, 0.0, 1.0, 0.0])").unwrap();

    // REINDEX after adding vectors — must succeed.
    db.execute("REINDEX ON docs USING hnsw (emb)").unwrap();

    // VECTOR_NEAR query should return correct neighbours.
    let hits = db
        .query("SELECT * FROM docs WHERE VECTOR_NEAR(emb, [1.0, 0.0, 0.0, 0.0], 2)")
        .unwrap()
        .collect();

    assert_eq!(hits.len(), 2, "expected 2 nearest neighbours");
    let slugs: std::collections::HashSet<_> = hits.iter().map(|h| h.slug.as_str()).collect();
    assert!(slugs.contains("docs/d1"), "expected docs/d1 in results");
    assert!(slugs.contains("docs/d2"), "expected docs/d2 in results");
}
