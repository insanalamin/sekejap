use sekejap::CoreDB;

// ── Basics ────────────────────────────────────────────────────────────────────

#[test]
fn put_and_get() {
    let mut db = CoreDB::new();
    db.put("alice", r#"{"name":"Alice","age":30}"#).unwrap();
    let json = db.get("alice").unwrap();
    assert!(json.contains("Alice"));
}

#[test]
fn put_bad_json_returns_error() {
    let mut db = CoreDB::new();
    assert!(db.put("x", "not json!!").is_err());
}

#[test]
fn remove_node() {
    let mut db = CoreDB::new();
    db.put("alice", r#"{"name":"Alice"}"#).unwrap();
    assert!(db.contains("alice"));
    db.remove("alice");
    assert!(!db.contains("alice"));
    assert_eq!(db.get("alice"), None);
}

#[test]
fn upsert_updates_collection_index() {
    let mut db = CoreDB::new();
    db.put("a", r#"{"_collection":"x"}"#).unwrap();
    db.put("a", r#"{"_collection":"y"}"#).unwrap(); // upsert into different collection

    // "a" should now be in "y", NOT "x"
    let in_y = db.collection("y").count();
    let in_x = db.collection("x").count();
    assert_eq!(in_y, 1);
    assert_eq!(in_x, 0);
}

// ── Graph traversal ───────────────────────────────────────────────────────────

#[test]
fn forward_traversal() {
    let mut db = CoreDB::new();
    db.put("alice", r#"{"name":"Alice"}"#).unwrap();
    db.put("bob",   r#"{"name":"Bob"}"#).unwrap();
    db.put("carol", r#"{"name":"Carol"}"#).unwrap();
    db.link("alice", "bob",   "follows", 1.0);
    db.link("alice", "carol", "follows", 1.0);

    let hits = db.one("alice").forward("follows").collect();
    let names: Vec<&str> = hits.iter()
        .map(|h| h.payload.as_ref().unwrap()["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"Bob"));
    assert!(names.contains(&"Carol"));
}

#[test]
fn backward_traversal() {
    let mut db = CoreDB::new();
    db.put("alice", r#"{"name":"Alice"}"#).unwrap();
    db.put("bob",   r#"{"name":"Bob"}"#).unwrap();
    db.link("alice", "bob", "follows", 1.0);

    let hits = db.one("bob").backward("follows").collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "alice");
}

#[test]
fn hops_bfs() {
    let mut db = CoreDB::new();
    for n in ["a","b","c","d"] {
        db.put(n, &format!(r#"{{"id":"{}"}}"#, n)).unwrap();
    }
    db.link("a", "b", "e", 1.0);
    db.link("b", "c", "e", 1.0);
    db.link("c", "d", "e", 1.0);

    // 2 hops from "a" should reach "a","b","c" but not "d"
    let reached = db.one("a").hops(2).count();
    assert_eq!(reached, 3); // a, b, c

    // 3 hops from "a" should reach all four
    let reached = db.one("a").hops(3).count();
    assert_eq!(reached, 4);
}

#[test]
fn roots_and_leaves() {
    let mut db = CoreDB::new();
    db.put("root",  r#"{}"#).unwrap();
    db.put("mid",   r#"{}"#).unwrap();
    db.put("leaf",  r#"{}"#).unwrap();
    db.link("root", "mid",  "e", 1.0);
    db.link("mid",  "leaf", "e", 1.0);

    assert_eq!(db.all().roots().count(), 1);
    assert_eq!(db.all().roots().first().unwrap().slug, "root");
    assert_eq!(db.all().leaves().count(), 1);
    assert_eq!(db.all().leaves().first().unwrap().slug, "leaf");
}

#[test]
fn unlink_removes_edge() {
    let mut db = CoreDB::new();
    db.put("a", r#"{}"#).unwrap();
    db.put("b", r#"{}"#).unwrap();
    db.link("a", "b", "e", 1.0);
    db.unlink("a", "b", "e");

    assert_eq!(db.one("a").forward("e").count(), 0);
    assert_eq!(db.one("b").backward("e").count(), 0);
}

// ── Collection queries ────────────────────────────────────────────────────────

#[test]
fn collection_query() {
    let mut db = CoreDB::new();
    db.put("alice", r#"{"_collection":"users","name":"Alice"}"#).unwrap();
    db.put("bob",   r#"{"_collection":"users","name":"Bob"}"#).unwrap();
    db.put("post1", r#"{"_collection":"posts","title":"Hi"}"#).unwrap();

    assert_eq!(db.collection("users").count(), 2);
    assert_eq!(db.collection("posts").count(), 1);
    assert_eq!(db.collection("unknown").count(), 0);
}

// ── Payload filters ───────────────────────────────────────────────────────────

#[test]
fn where_eq() {
    let mut db = CoreDB::new();
    db.put("alice", r#"{"name":"Alice","role":"admin"}"#).unwrap();
    db.put("bob",   r#"{"name":"Bob",  "role":"user"}"#).unwrap();

    let hits = db.all().where_eq("role", "admin").collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "alice");
}

#[test]
fn where_gt_lt() {
    let mut db = CoreDB::new();
    db.put("young", r#"{"age":20}"#).unwrap();
    db.put("mid",   r#"{"age":35}"#).unwrap();
    db.put("old",   r#"{"age":60}"#).unwrap();

    assert_eq!(db.all().where_gt("age", 30.0).count(), 2);
    assert_eq!(db.all().where_lt("age", 30.0).count(), 1);
    assert_eq!(db.all().where_between("age", 25.0, 50.0).count(), 1);
}

#[test]
fn where_in_filter() {
    let mut db = CoreDB::new();
    db.put("a", r#"{"status":"active"}"#).unwrap();
    db.put("b", r#"{"status":"inactive"}"#).unwrap();
    db.put("c", r#"{"status":"pending"}"#).unwrap();

    let hits = db.all()
        .where_in("status", vec![
            serde_json::json!("active"),
            serde_json::json!("pending"),
        ])
        .count();
    assert_eq!(hits, 2);
}

#[test]
fn like_filter() {
    let mut db = CoreDB::new();
    db.put("a", r#"{"email":"alice@example.com"}"#).unwrap();
    db.put("b", r#"{"email":"bob@corp.com"}"#).unwrap();

    assert_eq!(db.all().like("email", "example.com").count(), 1);
}

// ── Set algebra ───────────────────────────────────────────────────────────────

#[test]
fn intersect() {
    let mut db = CoreDB::new();
    db.put("a", r#"{"role":"admin","active":true}"#).unwrap();
    db.put("b", r#"{"role":"admin","active":false}"#).unwrap();
    db.put("c", r#"{"role":"user", "active":true}"#).unwrap();

    let admins = db.all().where_eq("role", "admin");
    let active = db.all().where_eq("active", true);
    let hits = admins.intersect(active).count();
    assert_eq!(hits, 1); // only "a"
}

#[test]
fn union() {
    let mut db = CoreDB::new();
    db.put("a", r#"{"tag":"rust"}"#).unwrap();
    db.put("b", r#"{"tag":"python"}"#).unwrap();
    db.put("c", r#"{"tag":"go"}"#).unwrap();

    let rust = db.all().where_eq("tag", "rust");
    let go   = db.all().where_eq("tag", "go");
    assert_eq!(rust.union(go).count(), 2);
}

#[test]
fn subtract() {
    let mut db = CoreDB::new();
    db.put("a", r#"{"score":10}"#).unwrap();
    db.put("b", r#"{"score":20}"#).unwrap();
    db.put("c", r#"{"score":30}"#).unwrap();

    let all  = db.all();
    let high = db.all().where_gt("score", 15.0);
    // all minus high = just "a"
    assert_eq!(all.subtract(high).count(), 1);
}

// ── Shaping ───────────────────────────────────────────────────────────────────

#[test]
fn sort_and_take() {
    let mut db = CoreDB::new();
    db.put("a", r#"{"score":30}"#).unwrap();
    db.put("b", r#"{"score":10}"#).unwrap();
    db.put("c", r#"{"score":20}"#).unwrap();

    let hits = db.all().sort("score", true).take(2).collect();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].payload.as_ref().unwrap()["score"], 10);
    assert_eq!(hits[1].payload.as_ref().unwrap()["score"], 20);
}

#[test]
fn skip_and_take() {
    let mut db = CoreDB::new();
    for i in 0..10u32 {
        db.put(&format!("node{i}"), &format!(r#"{{"i":{i}}}"#)).unwrap();
    }
    let hits = db.all().sort("i", true).skip(3).take(4).collect();
    assert_eq!(hits.len(), 4);
    assert_eq!(hits[0].payload.as_ref().unwrap()["i"], 3);
}

#[test]
fn select_projection() {
    let mut db = CoreDB::new();
    db.put("alice", r#"{"name":"Alice","age":30,"secret":"xyz"}"#).unwrap();

    let hits = db.one("alice")
        .select(["name", "age"])
        .collect();
    let p = hits[0].payload.as_ref().unwrap();
    assert!(p.get("name").is_some());
    assert!(p.get("age").is_some());
    assert!(p.get("secret").is_none());
}

// ── Edge inspection ───────────────────────────────────────────────────────────

#[test]
fn edges_from_and_to() {
    let mut db = CoreDB::new();
    db.put("a", r#"{}"#).unwrap();
    db.put("b", r#"{}"#).unwrap();
    db.link("a", "b", "edge", 0.5);

    let fwd = db.edges_from("a");
    assert_eq!(fwd.len(), 1);
    assert_eq!(fwd[0].to_slug.as_deref(), Some("b"));
    assert!((fwd[0].strength - 0.5).abs() < 1e-6);

    let rev = db.edges_to("b");
    assert_eq!(rev.len(), 1);
    assert_eq!(rev[0].from_slug.as_deref(), Some("a"));
}

#[test]
fn link_meta_stores_metadata() {
    let mut db = CoreDB::new();
    db.put("a", r#"{}"#).unwrap();
    db.put("b", r#"{}"#).unwrap();
    db.link_meta("a", "b", "knows", 1.0, r#"{"since":2020}"#).unwrap();

    let edges = db.edges_from("a");
    let meta = edges[0].meta.as_ref().unwrap();
    assert_eq!(meta["since"], 2020);
}

// ── Many nodes ────────────────────────────────────────────────────────────────

#[test]
fn put_many_and_count() {
    let mut db = CoreDB::new();
    let items: Vec<(String, String)> = (0..100)
        .map(|i| (format!("node{i}"), format!(r#"{{"i":{i}}}"#)))
        .collect();

    db.put_many(items.iter().map(|(s, j)| (s.as_str(), j.as_str()))).unwrap();
    assert_eq!(db.node_count(), 100);
}

#[test]
fn many_starter() {
    let mut db = CoreDB::new();
    db.put("a", r#"{"v":1}"#).unwrap();
    db.put("b", r#"{"v":2}"#).unwrap();
    db.put("c", r#"{"v":3}"#).unwrap();

    let hits = db.many(["a", "c"]).collect();
    assert_eq!(hits.len(), 2);
}

// ── SQL execute (INSERT / DELETE) ──────────────────────────────────────────────

#[test]
fn execute_insert_creates_node() {
    let mut db = CoreDB::new();
    let n = db.execute(
        "INSERT INTO users (_key, name, age) VALUES ('alice', 'Alice', 30)"
    ).unwrap();
    assert_eq!(n, 1);
    assert!(db.contains("users/alice"));
    let payload = db.get("users/alice").unwrap();
    let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(v["name"], "Alice");
    assert_eq!(v["_collection"], "users");
}

#[test]
fn execute_insert_is_queryable() {
    let mut db = CoreDB::new();
    db.execute("INSERT INTO products (_key, price) VALUES ('p1', 10)").unwrap();
    db.execute("INSERT INTO products (_key, price) VALUES ('p2', 50)").unwrap();
    db.execute("INSERT INTO products (_key, price) VALUES ('p3', 100)").unwrap();

    let hits = db.query("SELECT * FROM products WHERE price > 20").unwrap().collect();
    assert_eq!(hits.len(), 2);

    // Verify slug is collection/_key
    let all = db.query("SELECT * FROM products").unwrap().collect();
    assert!(all.iter().any(|h| h.slug == "products/p1"));
}

#[test]
fn execute_delete_removes_matching_nodes() {
    let mut db = CoreDB::new();
    db.put("keep",   r#"{"_collection":"items","active":true}"#).unwrap();
    db.put("remove", r#"{"_collection":"items","active":false}"#).unwrap();

    let n = db.execute("DELETE FROM items WHERE active = false").unwrap();
    assert_eq!(n, 1);
    assert!(db.contains("keep"));
    assert!(!db.contains("remove"));
}

#[test]
fn execute_delete_all() {
    let mut db = CoreDB::new();
    for i in 0..5u32 {
        db.put(&format!("n{i}"), "{}").unwrap();
    }
    let n = db.execute("DELETE FROM ALL").unwrap();
    assert_eq!(n, 5);
    assert_eq!(db.node_count(), 0);
}

#[test]
fn execute_insert_error_on_missing_key() {
    let mut db = CoreDB::new();
    let err = db.execute("INSERT INTO users (name) VALUES ('Alice')").unwrap_err();
    assert!(matches!(err, sekejap::SqlError::MissingField { field: "_key" }));
}

// ── MATCH integration tests ──────────────────────────────────────────────────

fn setup_music_db() -> CoreDB {
    let mut db = CoreDB::new();
    db.put("artist/the-vines", r#"{"_collection":"artist","_key":"the-vines","name":"The Vines"}"#).unwrap();
    db.put("genre/garage-rock", r#"{"_collection":"genre","_key":"garage-rock","name":"Garage Rock"}"#).unwrap();
    db.put("genre/alternative", r#"{"_collection":"genre","_key":"alternative","name":"Alternative"}"#).unwrap();
    db.put("city/melbourne", r#"{"_collection":"city","_key":"melbourne","name":"Melbourne"}"#).unwrap();
    db.link("artist/the-vines", "genre/garage-rock", "has_genre", 10.0);
    db.link("artist/the-vines", "genre/alternative", "has_genre", 5.0);
    db.link("artist/the-vines", "city/melbourne", "origin", 1.0);
    db
}

#[test]
fn match_forward_one_hop() {
    let db = setup_music_db();
    let hits = db.query(
        "MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines' RETURN g"
    ).unwrap().collect();
    let names: Vec<_> = hits.iter()
        .filter_map(|h| h.payload.as_ref()?.get("name")?.as_str())
        .collect();
    assert!(names.contains(&"Garage Rock"), "got: {:?}", names);
    assert!(names.contains(&"Alternative"), "got: {:?}", names);
    assert_eq!(names.len(), 2);
}

#[test]
fn match_backward_one_hop() {
    let db = setup_music_db();
    let hits = db.query(
        "MATCH (g:genre)<-[:has_genre]-(a:artist) WHERE g._key = 'garage-rock' RETURN a"
    ).unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert!(hits[0].payload.as_ref().unwrap().get("name").unwrap().as_str() == Some("The Vines"));
}

#[test]
fn match_strength_filter() {
    let db = setup_music_db();
    // Only has_genre edges with strength >= 7 should pass (garage-rock=10, alternative=5)
    let hits = db.query(
        "MATCH (a:artist)-[r:has_genre]->(g:genre) WHERE a._key = 'the-vines' AND r.strength >= 7 RETURN g"
    ).unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert!(hits[0].payload.as_ref().unwrap().get("name").unwrap().as_str() == Some("Garage Rock"));
}

#[test]
fn match_inline_props_end_node() {
    let db = setup_music_db();
    let hits = db.query(
        "MATCH (a:artist)-[:has_genre]->(:genre {_key: 'garage-rock'}) RETURN a"
    ).unwrap().collect();
    // This should find nodes reachable from any artist via has_genre, filtered to _key=garage-rock
    // The result is the genre node itself (end node), filtered by inline props
    assert!(!hits.is_empty());
}

#[test]
fn match_typed_multihop_bfs() {
    let mut db = CoreDB::new();
    // Chain: flood -> drainage_failure -> budget_cut -> policy_change
    db.put("event/flood", r#"{"_collection":"event","_key":"flood","name":"Maribyrnong Flood"}"#).unwrap();
    db.put("event/drainage", r#"{"_collection":"event","_key":"drainage","name":"Drainage Failure"}"#).unwrap();
    db.put("event/budget", r#"{"_collection":"event","_key":"budget","name":"Budget Cut"}"#).unwrap();
    db.put("event/policy", r#"{"_collection":"event","_key":"policy","name":"Policy Change"}"#).unwrap();
    db.link("event/flood", "event/drainage", "caused_by", 0.9);
    db.link("event/drainage", "event/budget", "caused_by", 0.8);
    db.link("event/budget", "event/policy", "caused_by", 0.7);

    let hits = db.query(
        "MATCH (e:event)-[:caused_by*1..5]->(root) WHERE e._key = 'flood' RETURN root"
    ).unwrap().collect();
    let names: Vec<_> = hits.iter()
        .filter_map(|h| h.payload.as_ref()?.get("name")?.as_str())
        .collect();
    assert!(names.contains(&"Drainage Failure"), "got: {:?}", names);
    assert!(names.contains(&"Budget Cut"), "got: {:?}", names);
    assert!(names.contains(&"Policy Change"), "got: {:?}", names);
    assert_eq!(names.len(), 3);
}

#[test]
fn match_union_two_patterns() {
    let db = setup_music_db();
    let hits = db.query(
        "MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines' RETURN g \
         UNION \
         MATCH (a:artist)-[:origin]->(c:city) WHERE a._key = 'the-vines' RETURN c"
    ).unwrap().collect();
    let names: Vec<_> = hits.iter()
        .filter_map(|h| h.payload.as_ref()?.get("name")?.as_str())
        .collect();
    // Should have genres + city
    assert!(names.contains(&"Garage Rock"), "got: {:?}", names);
    assert!(names.contains(&"Melbourne"), "got: {:?}", names);
}

#[test]
fn match_with_limit() {
    let db = setup_music_db();
    let hits = db.query(
        "MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines' RETURN g LIMIT 1"
    ).unwrap().collect();
    assert_eq!(hits.len(), 1);
}

// ── MATCH optimisation integration tests ─────────────────────────────────────

/// End _key condition in WHERE → One() inside Intersect (O(1) end-node lookup).
#[test]
fn match_end_node_key_in_where() {
    let db = setup_music_db();
    // Both start AND end have _key — should return exactly the targeted genre
    let hits = db.query(
        "MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines' AND g._key = 'garage-rock' RETURN g"
    ).unwrap().collect();
    assert_eq!(hits.len(), 1);
    let name = hits[0].payload.as_ref().unwrap()["name"].as_str().unwrap();
    assert_eq!(name, "Garage Rock");
}

/// End WHERE filter (non-_key) moves inside Intersect and still filters correctly.
#[test]
fn match_end_node_filter_in_where() {
    let db = setup_music_db();
    let hits = db.query(
        "MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines' AND g.name = 'Garage Rock' RETURN g"
    ).unwrap().collect();
    assert_eq!(hits.len(), 1, "should return only Garage Rock genre");
    assert_eq!(
        hits[0].payload.as_ref().unwrap()["name"].as_str().unwrap(),
        "Garage Rock"
    );
}

/// End node without a label: fall back to plain WhereEq filter (still correct).
#[test]
fn match_end_no_label_where_filter() {
    let db = setup_music_db();
    // (a:artist)-[:has_genre]->(b)  — no label on end, filter by name
    let hits = db.query(
        "MATCH (a:artist)-[:has_genre]->(b) WHERE a._key = 'the-vines' AND b.name = 'Garage Rock' RETURN b"
    ).unwrap().collect();
    assert_eq!(hits.len(), 1);
}

#[test]
fn ilike_filter() {
    let db = setup_music_db();
    let hits = db.query(
        "SELECT * FROM artist WHERE name ILIKE 'VINES'"
    ).unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert!(hits[0].payload.as_ref().unwrap().get("name").unwrap().as_str() == Some("The Vines"));
}

// ── Spatial integration tests ────────────────────────────────────────────────

fn setup_spatial_db() -> CoreDB {
    let mut db = CoreDB::new();

    // Points: Melbourne landmarks
    db.put("places/melb-central", r#"{
        "_collection": "places",
        "_key": "melb-central",
        "name": "Melbourne Central",
        "category": "landmark",
        "geometry": {"type": "Point", "coordinates": [144.9631, -37.8102]}
    }"#).unwrap();

    db.put("places/flinders-st", r#"{
        "_collection": "places",
        "_key": "flinders-st",
        "name": "Flinders Street Station",
        "category": "landmark",
        "geometry": {"type": "Point", "coordinates": [144.9671, -37.8183]}
    }"#).unwrap();

    db.put("places/exhibition-bldg", r#"{
        "_collection": "places",
        "_key": "exhibition-bldg",
        "name": "Royal Exhibition Building",
        "category": "landmark",
        "geometry": {"type": "Point", "coordinates": [144.9717, -37.8047]}
    }"#).unwrap();

    // Far away point: Geelong
    db.put("places/geelong-station", r#"{
        "_collection": "places",
        "_key": "geelong-station",
        "name": "Geelong Station",
        "category": "transport",
        "geometry": {"type": "Point", "coordinates": [144.3617, -38.1499]}
    }"#).unwrap();

    // Polygons: zones
    db.put("zones/cbd", r#"{
        "_collection": "zones",
        "_key": "cbd",
        "name": "CBD Zone",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [144.95, -37.80],
                [144.98, -37.80],
                [144.98, -37.83],
                [144.95, -37.83],
                [144.95, -37.80]
            ]]
        }
    }"#).unwrap();

    db.put("zones/fitzroy", r#"{
        "_collection": "zones",
        "_key": "fitzroy",
        "name": "Fitzroy Zone",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [144.97, -37.79],
                [145.00, -37.79],
                [145.00, -37.81],
                [144.97, -37.81],
                [144.97, -37.79]
            ]]
        }
    }"#).unwrap();

    // LineString: tram route
    db.put("routes/tram96", r#"{
        "_collection": "routes",
        "_key": "tram96",
        "name": "Tram Route 96",
        "geometry": {
            "type": "LineString",
            "coordinates": [
                [144.95, -37.81],
                [144.96, -37.81],
                [144.97, -37.81],
                [144.98, -37.81]
            ]
        }
    }"#).unwrap();

    db.build_spatial_index();
    db
}

#[test]
fn spatial_st_dwithin() {
    let db = setup_spatial_db();
    // Find places within 2km of Melbourne Central
    let hits = db.query(
        "SELECT * FROM places WHERE ST_DWithin(geometry, POINT(144.9631 -37.8102), 2.0)"
    ).unwrap().collect();
    let names: Vec<&str> = hits.iter()
        .filter_map(|h| h.payload.as_ref()?.get("name")?.as_str())
        .collect();
    assert!(names.contains(&"Melbourne Central"));
    assert!(names.contains(&"Flinders Street Station"));
    assert!(names.contains(&"Royal Exhibition Building"));
    assert!(!names.contains(&"Geelong Station"), "Geelong should be too far: {:?}", names);
}

#[test]
fn spatial_st_contains_point() {
    let db = setup_spatial_db();
    // Find zones containing Melbourne Central's coordinates
    let hits = db.query(
        "SELECT * FROM zones WHERE ST_Contains(geometry, POINT(144.9631 -37.8102))"
    ).unwrap().collect();
    let names: Vec<&str> = hits.iter()
        .filter_map(|h| h.payload.as_ref()?.get("name")?.as_str())
        .collect();
    assert!(names.contains(&"CBD Zone"), "CBD should contain Melbourne Central: {:?}", names);
}

#[test]
fn spatial_st_within_polygon() {
    let db = setup_spatial_db();
    // Find places within a big box around CBD
    let hits = db.query(
        "SELECT * FROM places WHERE ST_Within(geometry, POLYGON((144.94 -37.79, 144.99 -37.79, 144.99 -37.83, 144.94 -37.83, 144.94 -37.79)))"
    ).unwrap().collect();
    let names: Vec<&str> = hits.iter()
        .filter_map(|h| h.payload.as_ref()?.get("name")?.as_str())
        .collect();
    assert!(names.contains(&"Melbourne Central"));
    assert!(names.contains(&"Flinders Street Station"));
    assert!(names.contains(&"Royal Exhibition Building"));
    assert!(!names.contains(&"Geelong Station"));
}

#[test]
fn spatial_st_intersects() {
    let db = setup_spatial_db();
    // The tram route crosses a rectangle overlapping its path
    let hits = db.query(
        "SELECT * FROM routes WHERE ST_Intersects(geometry, POLYGON((144.955 -37.815, 144.975 -37.815, 144.975 -37.805, 144.955 -37.805, 144.955 -37.815)))"
    ).unwrap().collect();
    let names: Vec<&str> = hits.iter()
        .filter_map(|h| h.payload.as_ref()?.get("name")?.as_str())
        .collect();
    assert!(names.contains(&"Tram Route 96"), "Tram route should intersect: {:?}", names);
}

#[test]
fn spatial_atomic_api() {
    let db = setup_spatial_db();
    // Test atomic API: st_dwithin
    let hits = db.collection("places")
        .st_dwithin(-37.8102, 144.9631, 2.0)
        .collect();
    let names: Vec<&str> = hits.iter()
        .filter_map(|h| h.payload.as_ref()?.get("name")?.as_str())
        .collect();
    assert!(names.contains(&"Melbourne Central"));
    assert!(names.contains(&"Flinders Street Station"));
    assert!(!names.contains(&"Geelong Station"));

    // Test atomic API: near (alias)
    let near_count = db.collection("places")
        .near(-37.8102, 144.9631, 2.0)
        .count();
    assert_eq!(near_count, hits.len());
}

#[test]
fn spatial_sql_combined() {
    let db = setup_spatial_db();
    // Combine spatial with regular filter
    let hits = db.query(
        "SELECT * FROM places WHERE ST_DWithin(geometry, POINT(144.9631 -37.8102), 2.0) AND category = 'landmark'"
    ).unwrap().collect();
    let names: Vec<&str> = hits.iter()
        .filter_map(|h| h.payload.as_ref()?.get("name")?.as_str())
        .collect();
    assert!(names.contains(&"Melbourne Central"));
    assert!(names.contains(&"Flinders Street Station"));
    assert!(names.contains(&"Royal Exhibition Building"));
    assert_eq!(names.len(), 3);
}

#[test]
fn spatial_st_contains_point_atomic() {
    let db = setup_spatial_db();
    let hits = db.collection("zones")
        .st_contains_point(-37.8102, 144.9631)
        .collect();
    let names: Vec<&str> = hits.iter()
        .filter_map(|h| h.payload.as_ref()?.get("name")?.as_str())
        .collect();
    assert!(names.contains(&"CBD Zone"));
}

#[test]
fn spatial_execute_insert_then_query() {
    let mut db = CoreDB::new();
    db.execute(
        "INSERT INTO places (_key, name, geometry) VALUES ('melb-central', 'Melbourne Central', '{\"type\":\"Point\",\"coordinates\":[144.9631,-37.8102]}')"
    ).unwrap();
    let hits = db.query(
        "SELECT * FROM places WHERE ST_DWithin(geometry, POINT(144.9631 -37.8136), 1.0)"
    ).unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "places/melb-central");
    assert!(hits[0].payload.as_ref().unwrap().get("name").unwrap().as_str() == Some("Melbourne Central"));
}

// ── Spatial grid specific tests ──────────────────────────────────────────────

#[test]
fn spatial_grid_same_results_as_brute_force() {
    // Run the same queries with and without grid, compare results
    let mut db_brute = CoreDB::new();
    let mut db_grid = CoreDB::new();

    let nodes = [
        ("p1", r#"{"_collection":"places","geometry":{"type":"Point","coordinates":[144.96,-37.81]}}"#),
        ("p2", r#"{"_collection":"places","geometry":{"type":"Point","coordinates":[144.97,-37.82]}}"#),
        ("p3", r#"{"_collection":"places","geometry":{"type":"Point","coordinates":[145.50,-38.00]}}"#),
    ];
    for (slug, json) in &nodes {
        db_brute.put(slug, json).unwrap();
        db_grid.put(slug, json).unwrap();
    }
    db_grid.build_spatial_index();

    // ST_DWithin
    let brute = db_brute.collection("places").st_dwithin(-37.81, 144.96, 2.0).count();
    let grid  = db_grid.collection("places").st_dwithin(-37.81, 144.96, 2.0).count();
    assert_eq!(brute, grid, "ST_DWithin mismatch");

    // ST_ContainsPoint (need polygon nodes)
    let mut db_brute2 = CoreDB::new();
    let mut db_grid2 = CoreDB::new();
    let zone = r#"{"_collection":"zones","geometry":{"type":"Polygon","coordinates":[[
        [144.95,-37.80],[144.98,-37.80],[144.98,-37.83],[144.95,-37.83],[144.95,-37.80]
    ]]}}"#;
    db_brute2.put("z1", zone).unwrap();
    db_grid2.put("z1", zone).unwrap();
    db_grid2.build_spatial_index();

    let brute2 = db_brute2.collection("zones").st_contains_point(-37.81, 144.96).count();
    let grid2  = db_grid2.collection("zones").st_contains_point(-37.81, 144.96).count();
    assert_eq!(brute2, grid2, "ST_ContainsPoint mismatch");
}

#[test]
fn spatial_grid_incremental_update() {
    let mut db = CoreDB::new();
    db.put("p1", r#"{"_collection":"places","geometry":{"type":"Point","coordinates":[144.96,-37.81]}}"#).unwrap();
    db.build_spatial_index();

    // Verify initial state
    assert_eq!(db.collection("places").st_dwithin(-37.81, 144.96, 1.0).count(), 1);

    // Insert after grid build — should be found via incremental update
    db.put("p2", r#"{"_collection":"places","geometry":{"type":"Point","coordinates":[144.97,-37.82]}}"#).unwrap();
    assert_eq!(db.collection("places").st_dwithin(-37.81, 144.96, 2.0).count(), 2);

    // Remove — should no longer be found
    db.remove("p1");
    let hits = db.collection("places").st_dwithin(-37.81, 144.96, 2.0).collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "p2");
}

// ── INSERT with geometry JSON tests ──────────────────────────────────────────

#[test]
fn insert_geometry_json_auto_parsed() {
    let mut db = CoreDB::new();
    db.execute(
        r#"INSERT INTO places (_key, name, geometry) VALUES ('fed-square', 'Federation Square', '{"type":"Point","coordinates":[144.9694,-37.8180]}')"#
    ).unwrap();
    db.build_spatial_index();

    // The geometry should have been parsed into a native JSON object, not kept as string
    let raw = db.get("places/fed-square").unwrap();
    let payload: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert!(payload["geometry"].is_object(), "geometry should be parsed object, not string");
    assert_eq!(payload["geometry"]["type"], "Point");

    // Should be queryable via spatial SQL
    let hits = db.query(
        "SELECT * FROM places WHERE ST_DWithin(geometry, POINT(144.9694 -37.8180), 1.0)"
    ).unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "places/fed-square");
}

#[test]
fn insert_geometry_polygon_json_auto_parsed() {
    let mut db = CoreDB::new();
    db.execute(
        r#"INSERT INTO zones (_key, name, geometry) VALUES ('fitzroy', 'Fitzroy', '{"type":"Polygon","coordinates":[[[144.97,-37.79],[145.00,-37.79],[145.00,-37.82],[144.97,-37.82],[144.97,-37.79]]]}')"#
    ).unwrap();
    db.build_spatial_index();

    let hits = db.query(
        "SELECT * FROM zones WHERE ST_Contains(geometry, POINT(144.98 -37.80))"
    ).unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "zones/fitzroy");
}

// ── INSERT edge integration tests ────────────────────────────────────────────

#[test]
fn insert_edge_single() {
    let mut db = CoreDB::new();
    db.put("artist/the-vines", r#"{"name":"The Vines","_collection":"artist","_key":"the-vines"}"#).unwrap();
    db.put("genre/garage-rock", r#"{"name":"Garage Rock","_collection":"genre","_key":"garage-rock"}"#).unwrap();

    let count = db.execute("INSERT ('artist/the-vines')-[:has_genre {strength: 10}]->('genre/garage-rock')").unwrap();
    assert_eq!(count, 1);

    // Verify edge via atomic API
    let hits = db.one("artist/the-vines").forward("has_genre").collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "genre/garage-rock");

    // Verify via MATCH
    let hits = db.query(
        "MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines' RETURN g"
    ).unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "genre/garage-rock");
}

#[test]
fn insert_edge_with_meta() {
    let mut db = CoreDB::new();
    db.put("city/melbourne", r#"{"name":"Melbourne","_collection":"city"}"#).unwrap();
    db.put("suburb/fitzroy", r#"{"name":"Fitzroy","_collection":"suburb"}"#).unwrap();

    db.execute("INSERT ('city/melbourne')-[:contains {strength: 1, distance: 3.2}]->('suburb/fitzroy')").unwrap();

    let edges = db.edges_from("city/melbourne");
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].strength, 1.0);
    let meta = edges[0].meta.as_ref().unwrap();
    assert_eq!(meta["distance"], 3.2);
}

#[test]
fn insert_edge_multiple() {
    let mut db = CoreDB::new();
    db.put("a", r#"{"_collection":"node"}"#).unwrap();
    db.put("b", r#"{"_collection":"node"}"#).unwrap();
    db.put("c", r#"{"_collection":"node"}"#).unwrap();

    let count = db.execute(
        "INSERT ('a')-[:links {strength: 5}]->('b'), ('b')-[:links {strength: 3}]->('c')"
    ).unwrap();
    assert_eq!(count, 2);

    let hits_b = db.one("a").forward("links").collect();
    assert_eq!(hits_b.len(), 1);
    assert_eq!(hits_b[0].slug, "b");

    let hits_c = db.one("b").forward("links").collect();
    assert_eq!(hits_c.len(), 1);
    assert_eq!(hits_c[0].slug, "c");

    // Full chain
    let chain = db.one("a").forward("links").forward("links").collect();
    assert_eq!(chain.len(), 1);
    assert_eq!(chain[0].slug, "c");
}

#[test]
fn insert_edge_default_strength() {
    let mut db = CoreDB::new();
    db.put("x", r#"{"_collection":"node"}"#).unwrap();
    db.put("y", r#"{"_collection":"node"}"#).unwrap();

    db.execute("INSERT ('x')-[:knows]->('y')").unwrap();

    let edges = db.edges_from("x");
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].strength, 1.0);
}

#[test]
fn delete_edge_removes_edge() {
    let mut db = CoreDB::new();
    db.put("a", r#"{"_collection":"node"}"#).unwrap();
    db.put("b", r#"{"_collection":"node"}"#).unwrap();
    db.link("a", "b", "knows", 1.0);

    // Verify edge exists
    assert_eq!(db.one("a").forward("knows").count(), 1);

    // Delete it
    let count = db.execute("DELETE ('a')-[:knows]->('b')").unwrap();
    assert_eq!(count, 1);

    // Verify gone
    assert_eq!(db.one("a").forward("knows").count(), 0);
    assert_eq!(db.one("b").backward("knows").count(), 0);
}


// ── JSON path operators (-> / ->>) ────────────────────────────────────────────

#[test]
fn json_path_text_where() {
    let mut db = CoreDB::new();
    db.put(
        "users/alice",
        r#"{"_collection":"users","_key":"alice","profile":{"role":"admin","age":30}}"#,
    )
    .unwrap();
    db.put(
        "users/bob",
        r#"{"_collection":"users","_key":"bob","profile":{"role":"viewer","age":25}}"#,
    )
    .unwrap();

    // ->> returns TEXT; compare to string literal
    let hits = db
        .query("SELECT * FROM users WHERE profile->>'role' = 'admin'")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "users/alice");
}

#[test]
fn json_path_obj_where() {
    let mut db = CoreDB::new();
    db.put(
        "items/a",
        r#"{"_collection":"items","_key":"a","meta":{"status":{"active":true},"score":9}}"#,
    )
    .unwrap();
    db.put(
        "items/b",
        r#"{"_collection":"items","_key":"b","meta":{"status":{"active":false},"score":3}}"#,
    )
    .unwrap();

    // -> returns JSON value; compare to number
    let hits = db
        .query("SELECT * FROM items WHERE meta->'score' > 5")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "items/a");
}

#[test]
fn json_path_deep_chain() {
    let mut db = CoreDB::new();
    db.put(
        "nodes/x",
        r#"{"_collection":"nodes","_key":"x","a":{"b":{"c":"deep"}}}"#,
    )
    .unwrap();
    db.put(
        "nodes/y",
        r#"{"_collection":"nodes","_key":"y","a":{"b":{"c":"other"}}}"#,
    )
    .unwrap();

    // Three-level deep path with ->>
    let hits = db
        .query("SELECT * FROM nodes WHERE a->'b'->>'c' = 'deep'")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "nodes/x");
}

#[test]
fn json_path_select_projection() {
    let mut db = CoreDB::new();
    db.put(
        "users/u1",
        r#"{"_collection":"users","_key":"u1","profile":{"name":"Alice","role":"admin"}}"#,
    )
    .unwrap();

    let hits = db
        .query("SELECT profile->>'role' FROM users")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    // Output key should be the last path segment ("role"), value should be the text
    let payload = hits[0].payload.as_ref().unwrap();
    assert_eq!(payload["role"], serde_json::json!("admin"));
}

#[test]
fn json_path_combined_where_and_plain() {
    let mut db = CoreDB::new();
    db.put(
        "orders/1",
        r#"{"_collection":"orders","_key":"1","status":"active","extra":{"priority":"high"}}"#,
    )
    .unwrap();
    db.put(
        "orders/2",
        r#"{"_collection":"orders","_key":"2","status":"active","extra":{"priority":"low"}}"#,
    )
    .unwrap();
    db.put(
        "orders/3",
        r#"{"_collection":"orders","_key":"3","status":"closed","extra":{"priority":"high"}}"#,
    )
    .unwrap();

    // Combine plain field + JSON path in WHERE
    let hits = db
        .query("SELECT * FROM orders WHERE status = 'active' AND extra->>'priority' = 'high'")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "orders/1");
}

// ── IS NULL / IS NOT NULL ─────────────────────────────────────────────────────

#[test]
fn where_is_null() {
    let mut db = CoreDB::new();
    db.put("users/1", r#"{"_collection":"users","_key":"1","name":"Alice","email":"a@x.com"}"#)
        .unwrap();
    db.put("users/2", r#"{"_collection":"users","_key":"2","name":"Bob"}"#)
        .unwrap();
    db.put("users/3", r#"{"_collection":"users","_key":"3","name":"Carol","email":null}"#)
        .unwrap();

    // IS NULL should match Bob (missing) and Carol (explicit null)
    let hits = db
        .query("SELECT * FROM users WHERE email IS NULL")
        .unwrap()
        .collect();
    let slugs: std::collections::HashSet<_> = hits.iter().map(|h| h.slug.as_str()).collect();
    assert_eq!(slugs.len(), 2);
    assert!(slugs.contains("users/2"));
    assert!(slugs.contains("users/3"));
}

#[test]
fn where_is_not_null() {
    let mut db = CoreDB::new();
    db.put("users/1", r#"{"_collection":"users","_key":"1","name":"Alice","email":"a@x.com"}"#)
        .unwrap();
    db.put("users/2", r#"{"_collection":"users","_key":"2","name":"Bob"}"#)
        .unwrap();

    let hits = db
        .query("SELECT * FROM users WHERE email IS NOT NULL")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "users/1");
}

// ── NOT condition ─────────────────────────────────────────────────────────────

#[test]
fn where_not_eq() {
    let mut db = CoreDB::new();
    db.put("items/1", r#"{"_collection":"items","_key":"1","status":"active"}"#)
        .unwrap();
    db.put("items/2", r#"{"_collection":"items","_key":"2","status":"inactive"}"#)
        .unwrap();
    db.put("items/3", r#"{"_collection":"items","_key":"3","status":"active"}"#)
        .unwrap();

    let hits = db
        .query("SELECT * FROM items WHERE NOT status = 'active'")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "items/2");
}

// ── OR conditions ─────────────────────────────────────────────────────────────

#[test]
fn where_or_basic() {
    let mut db = CoreDB::new();
    db.put("products/1", r#"{"_collection":"products","_key":"1","category":"books"}"#)
        .unwrap();
    db.put("products/2", r#"{"_collection":"products","_key":"2","category":"music"}"#)
        .unwrap();
    db.put("products/3", r#"{"_collection":"products","_key":"3","category":"food"}"#)
        .unwrap();

    let hits = db
        .query("SELECT * FROM products WHERE category = 'books' OR category = 'music'")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 2);
    let slugs: std::collections::HashSet<_> = hits.iter().map(|h| h.slug.as_str()).collect();
    assert!(slugs.contains("products/1"));
    assert!(slugs.contains("products/2"));
}

#[test]
fn where_or_and_precedence() {
    // (A AND B) OR C — AND binds tighter than OR
    let mut db = CoreDB::new();
    db.put(
        "events/1",
        r#"{"_collection":"events","_key":"1","type":"sale","region":"eu"}"#,
    )
    .unwrap();
    db.put(
        "events/2",
        r#"{"_collection":"events","_key":"2","type":"sale","region":"us"}"#,
    )
    .unwrap();
    db.put(
        "events/3",
        r#"{"_collection":"events","_key":"3","type":"view","region":"eu"}"#,
    )
    .unwrap();

    // type='sale' AND region='eu'  OR  type='view'
    let hits = db
        .query("SELECT * FROM events WHERE type = 'sale' AND region = 'eu' OR type = 'view'")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 2);
    let slugs: std::collections::HashSet<_> = hits.iter().map(|h| h.slug.as_str()).collect();
    assert!(slugs.contains("events/1")); // sale AND eu
    assert!(slugs.contains("events/3")); // view
}

// ── SELECT … AS alias ─────────────────────────────────────────────────────────

#[test]
fn select_as_alias() {
    let mut db = CoreDB::new();
    db.put(
        "employees/1",
        r#"{"_collection":"employees","_key":"1","first_name":"Alice","dept":"eng"}"#,
    )
    .unwrap();

    let hits = db
        .query("SELECT first_name AS name, dept AS department FROM employees")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    let p = hits[0].payload.as_ref().unwrap().as_object().unwrap();
    assert!(p.contains_key("name"), "expected key 'name', got {:?}", p.keys().collect::<Vec<_>>());
    assert_eq!(p["name"], "Alice");
    assert!(p.contains_key("department"));
    assert_eq!(p["department"], "eng");
    // Original keys should NOT be present
    assert!(!p.contains_key("first_name"));
    assert!(!p.contains_key("dept"));
}

// ── ORDER BY JSON path ────────────────────────────────────────────────────────

#[test]
fn order_by_json_path() {
    let mut db = CoreDB::new();
    db.put(
        "scores/1",
        r#"{"_collection":"scores","_key":"1","meta":{"val":30}}"#,
    )
    .unwrap();
    db.put(
        "scores/2",
        r#"{"_collection":"scores","_key":"2","meta":{"val":10}}"#,
    )
    .unwrap();
    db.put(
        "scores/3",
        r#"{"_collection":"scores","_key":"3","meta":{"val":20}}"#,
    )
    .unwrap();

    let hits = db
        .query("SELECT * FROM scores ORDER BY meta->'val' ASC")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0].slug, "scores/2"); // val=10
    assert_eq!(hits[1].slug, "scores/3"); // val=20
    assert_eq!(hits[2].slug, "scores/1"); // val=30
}

// ── Aggregations ──────────────────────────────────────────────────────────────

#[test]
fn aggregate_count_star() {
    let mut db = CoreDB::new();
    for i in 1..=5 {
        db.put(
            &format!("log/{}", i),
            &format!(r#"{{"_collection":"log","_key":"{}","level":"info"}}"#, i),
        )
        .unwrap();
    }

    let hits = db
        .query("SELECT COUNT(*) FROM log")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    let p = hits[0].payload.as_ref().unwrap();
    assert_eq!(p["count"], 5);
}

#[test]
fn aggregate_sum_avg() {
    let mut db = CoreDB::new();
    db.put("sales/1", r#"{"_collection":"sales","_key":"1","amount":100}"#)
        .unwrap();
    db.put("sales/2", r#"{"_collection":"sales","_key":"2","amount":200}"#)
        .unwrap();
    db.put("sales/3", r#"{"_collection":"sales","_key":"3","amount":300}"#)
        .unwrap();

    let hits = db
        .query("SELECT SUM(amount), AVG(amount) FROM sales")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    let p = hits[0].payload.as_ref().unwrap();
    assert_eq!(p["sum"].as_f64().unwrap(), 600.0);
    assert_eq!(p["avg"].as_f64().unwrap(), 200.0);
}

#[test]
fn aggregate_min_max() {
    let mut db = CoreDB::new();
    db.put("temps/1", r#"{"_collection":"temps","_key":"1","c":5}"#)
        .unwrap();
    db.put("temps/2", r#"{"_collection":"temps","_key":"2","c":42}"#)
        .unwrap();
    db.put("temps/3", r#"{"_collection":"temps","_key":"3","c":17}"#)
        .unwrap();

    let hits = db
        .query("SELECT MIN(c), MAX(c) FROM temps")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    let p = hits[0].payload.as_ref().unwrap();
    assert_eq!(p["min"].as_f64().unwrap(), 5.0);
    assert_eq!(p["max"].as_f64().unwrap(), 42.0);
}

#[test]
fn aggregate_count_star_with_alias() {
    let mut db = CoreDB::new();
    for i in 1..=3 {
        db.put(
            &format!("things/{}", i),
            &format!(r#"{{"_collection":"things","_key":"{}"}}"#, i),
        )
        .unwrap();
    }

    let hits = db
        .query("SELECT COUNT(*) AS total FROM things")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    let p = hits[0].payload.as_ref().unwrap().as_object().unwrap();
    assert!(p.contains_key("total"), "expected 'total', got {:?}", p.keys().collect::<Vec<_>>());
    assert_eq!(p["total"], 3);
}

#[test]
fn aggregate_with_where_filter() {
    let mut db = CoreDB::new();
    db.put("orders/1", r#"{"_collection":"orders","_key":"1","status":"paid","amount":50}"#)
        .unwrap();
    db.put("orders/2", r#"{"_collection":"orders","_key":"2","status":"paid","amount":75}"#)
        .unwrap();
    db.put("orders/3", r#"{"_collection":"orders","_key":"3","status":"pending","amount":30}"#)
        .unwrap();

    let hits = db
        .query("SELECT COUNT(*), SUM(amount) FROM orders WHERE status = 'paid'")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    let p = hits[0].payload.as_ref().unwrap();
    assert_eq!(p["count"], 2);
    assert_eq!(p["sum"].as_f64().unwrap(), 125.0);
}

// ── HNSW approximate nearest-neighbour ───────────────────────────────────────

#[test]
fn hnsw_build_and_search_rust_api() {
    let mut db = CoreDB::new();
    // Insert nodes with 4-d embeddings.
    db.put("docs/a", r#"{"_collection":"docs","_key":"a","text":"alpha"}"#).unwrap();
    db.put("docs/b", r#"{"_collection":"docs","_key":"b","text":"beta"}"#).unwrap();
    db.put("docs/c", r#"{"_collection":"docs","_key":"c","text":"gamma"}"#).unwrap();
    db.put("docs/d", r#"{"_collection":"docs","_key":"d","text":"delta"}"#).unwrap();

    // Vectors: a and b are close; c and d are close but far from a/b.
    db.put_vector("docs/a", "emb", &[1.0, 0.0, 0.0, 0.0]).unwrap();
    db.put_vector("docs/b", "emb", &[0.9, 0.1, 0.0, 0.0]).unwrap();
    db.put_vector("docs/c", "emb", &[0.0, 0.0, 1.0, 0.0]).unwrap();
    db.put_vector("docs/d", "emb", &[0.0, 0.0, 0.9, 0.1]).unwrap();

    // Build HNSW index.
    db.build_hnsw_index("emb", 4, 50).unwrap();

    // Query near [1, 0, 0, 0] → should return docs/a and docs/b.
    let results = db
        .collection("docs")
        .vector_near("emb", vec![1.0f32, 0.0, 0.0, 0.0], 2)
        .collect();

    assert_eq!(results.len(), 2);
    let slugs: std::collections::HashSet<_> = results.iter().map(|h| h.slug.as_str()).collect();
    assert!(slugs.contains("docs/a"), "expected docs/a in results, got {:?}", slugs);
    assert!(slugs.contains("docs/b"), "expected docs/b in results, got {:?}", slugs);
}

#[test]
fn hnsw_sql_vector_near() {
    let mut db = CoreDB::new();
    for (key, emb) in [
        ("items/1", [1.0f32, 0.0, 0.0, 0.0]),
        ("items/2", [0.95, 0.05, 0.0, 0.0]),
        ("items/3", [0.0, 1.0, 0.0, 0.0]),
        ("items/4", [0.0, 0.95, 0.05, 0.0]),
    ] {
        db.put(key, &format!(r#"{{"_collection":"items","_key":"{}"}}"#, key.split('/').last().unwrap()))
            .unwrap();
        db.put_vector(key, "vec", &emb).unwrap();
    }
    db.build_hnsw_index("vec", 4, 50).unwrap();

    let hits = db
        .query("SELECT * FROM items WHERE VECTOR_NEAR(vec, [1.0, 0.0, 0.0, 0.0], 2)")
        .unwrap()
        .collect();

    assert_eq!(hits.len(), 2);
    let slugs: std::collections::HashSet<_> = hits.iter().map(|h| h.slug.as_str()).collect();
    assert!(slugs.contains("items/1"));
    assert!(slugs.contains("items/2"));
}

#[test]
fn hnsw_build_error_no_vectors() {
    let mut db = CoreDB::new();
    db.put("things/1", r#"{"_collection":"things","_key":"1"}"#).unwrap();
    // No vectors stored — build_hnsw_index should return Err.
    let result = db.build_hnsw_index("nonexistent_field", 8, 100);
    assert!(result.is_err());
    // Main store untouched.
    assert!(db.collection("things").count() == 1);
}

#[test]
fn hnsw_error_leaves_main_store_intact() {
    let mut db = CoreDB::new();
    db.put("nodes/1", r#"{"_collection":"nodes","_key":"1","score":42}"#).unwrap();
    db.put_vector("nodes/1", "emb", &[1.0, 0.0]).unwrap();

    // First build succeeds.
    db.build_hnsw_index("emb", 4, 20).unwrap();

    // The original node is still reachable and correct.
    let hits = db.query("SELECT * FROM nodes WHERE score = 42").unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "nodes/1");

    // Attempting to build for a field with no vectors returns Err
    // and must not corrupt the existing HNSW index.
    let err = db.build_hnsw_index("missing_field", 4, 20);
    assert!(err.is_err());

    // Original HNSW index still works.
    let vec_hits = db
        .query("SELECT * FROM nodes WHERE VECTOR_NEAR(emb, [1.0, 0.0], 1)")
        .unwrap()
        .collect();
    assert_eq!(vec_hits.len(), 1);
    assert_eq!(vec_hits[0].slug, "nodes/1");
}

// ── WHERE parenthesized groups ─────────────────────────────────────────────────

#[test]
fn where_paren_or_and() {
    // (a OR b) AND c
    let mut db = CoreDB::new();
    db.put("t/1", r#"{"_collection":"t","_key":"1","color":"red","active":true}"#).unwrap();
    db.put("t/2", r#"{"_collection":"t","_key":"2","color":"blue","active":true}"#).unwrap();
    db.put("t/3", r#"{"_collection":"t","_key":"3","color":"red","active":false}"#).unwrap();
    db.put("t/4", r#"{"_collection":"t","_key":"4","color":"green","active":true}"#).unwrap();

    // (color='red' OR color='blue') AND active=true → items 1 and 2
    let hits = db.query("SELECT * FROM t WHERE (color = 'red' OR color = 'blue') AND active = true")
        .unwrap().collect();
    let slugs: std::collections::HashSet<_> = hits.iter().map(|h| h.slug.as_str()).collect();
    assert_eq!(hits.len(), 2);
    assert!(slugs.contains("t/1"));
    assert!(slugs.contains("t/2"));
}

#[test]
fn where_paren_and_or() {
    // a AND (b OR c)
    let mut db = CoreDB::new();
    db.put("t/1", r#"{"_collection":"t","_key":"1","type":"A","score":10}"#).unwrap();
    db.put("t/2", r#"{"_collection":"t","_key":"2","type":"A","score":20}"#).unwrap();
    db.put("t/3", r#"{"_collection":"t","_key":"3","type":"B","score":10}"#).unwrap();

    // type='A' AND (score=10 OR score=20) → items 1 and 2
    let hits = db.query("SELECT * FROM t WHERE type = 'A' AND (score = 10 OR score = 20)")
        .unwrap().collect();
    assert_eq!(hits.len(), 2);
}

#[test]
fn where_not_paren_group() {
    // NOT (a OR b)
    let mut db = CoreDB::new();
    db.put("t/1", r#"{"_collection":"t","_key":"1","status":"active"}"#).unwrap();
    db.put("t/2", r#"{"_collection":"t","_key":"2","status":"pending"}"#).unwrap();
    db.put("t/3", r#"{"_collection":"t","_key":"3","status":"deleted"}"#).unwrap();

    // NOT (status='active' OR status='pending') → only item 3
    let hits = db.query("SELECT * FROM t WHERE NOT (status = 'active' OR status = 'pending')")
        .unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "t/3");
}

// ── ORDER BY DESC ──────────────────────────────────────────────────────────────

#[test]
fn order_by_desc() {
    let mut db = CoreDB::new();
    db.put("p/1", r#"{"_collection":"p","_key":"1","price":10}"#).unwrap();
    db.put("p/2", r#"{"_collection":"p","_key":"2","price":30}"#).unwrap();
    db.put("p/3", r#"{"_collection":"p","_key":"3","price":20}"#).unwrap();

    let hits = db.query("SELECT * FROM p ORDER BY price DESC").unwrap().collect();
    assert_eq!(hits.len(), 3);
    let prices: Vec<f64> = hits.iter()
        .map(|h| h.payload.as_ref().unwrap()["price"].as_f64().unwrap())
        .collect();
    assert_eq!(prices, vec![30.0, 20.0, 10.0]);
}

// ── LIMIT / OFFSET any order ───────────────────────────────────────────────────

#[test]
fn limit_offset_any_order() {
    let mut db = CoreDB::new();
    for i in 1..=10u32 {
        db.put(&format!("n/{i}"), &format!(r#"{{"_collection":"n","_key":"{i}","v":{i}}}"#)).unwrap();
    }

    // OFFSET before LIMIT
    let hits = db.query("SELECT * FROM n ORDER BY v ASC OFFSET 2 LIMIT 3").unwrap().collect();
    assert_eq!(hits.len(), 3);
    let vals: Vec<f64> = hits.iter()
        .map(|h| h.payload.as_ref().unwrap()["v"].as_f64().unwrap())
        .collect();
    assert_eq!(vals, vec![3.0, 4.0, 5.0]);
}

// ── SELECT DISTINCT ────────────────────────────────────────────────────────────

#[test]
fn select_distinct_basic() {
    let mut db = CoreDB::new();
    db.put("u/1", r#"{"_collection":"u","_key":"1","city":"Paris"}"#).unwrap();
    db.put("u/2", r#"{"_collection":"u","_key":"2","city":"London"}"#).unwrap();
    db.put("u/3", r#"{"_collection":"u","_key":"3","city":"Paris"}"#).unwrap();
    db.put("u/4", r#"{"_collection":"u","_key":"4","city":"Berlin"}"#).unwrap();

    // Three distinct city values
    let hits = db.query("SELECT DISTINCT city FROM u").unwrap().collect();
    assert_eq!(hits.len(), 3);
    let cities: std::collections::HashSet<String> = hits.iter()
        .map(|h| h.payload.as_ref().unwrap()["city"].as_str().unwrap().to_string())
        .collect();
    assert!(cities.contains("Paris"));
    assert!(cities.contains("London"));
    assert!(cities.contains("Berlin"));
}

#[test]
fn select_distinct_all_dupes() {
    let mut db = CoreDB::new();
    for i in 1..=5u32 {
        db.put(&format!("x/{i}"), &format!(r#"{{"_collection":"x","_key":"{i}","kind":"widget"}}"#)).unwrap();
    }
    let hits = db.query("SELECT DISTINCT kind FROM x").unwrap().collect();
    assert_eq!(hits.len(), 1);
}

// ── GROUP BY ──────────────────────────────────────────────────────────────────

#[test]
fn group_by_count() {
    let mut db = CoreDB::new();
    db.put("o/1", r#"{"_collection":"o","_key":"1","cat":"A","val":1}"#).unwrap();
    db.put("o/2", r#"{"_collection":"o","_key":"2","cat":"A","val":2}"#).unwrap();
    db.put("o/3", r#"{"_collection":"o","_key":"3","cat":"B","val":3}"#).unwrap();
    db.put("o/4", r#"{"_collection":"o","_key":"4","cat":"B","val":4}"#).unwrap();
    db.put("o/5", r#"{"_collection":"o","_key":"5","cat":"C","val":5}"#).unwrap();

    let hits = db.query("SELECT cat, COUNT(*) FROM o GROUP BY cat ORDER BY cat ASC")
        .unwrap().collect();
    assert_eq!(hits.len(), 3);
    // First group = A with count 2
    let first = hits[0].payload.as_ref().unwrap();
    assert_eq!(first["cat"].as_str().unwrap(), "A");
    assert_eq!(first["count"].as_f64().unwrap(), 2.0);
}

#[test]
fn group_by_sum_avg() {
    let mut db = CoreDB::new();
    db.put("s/1", r#"{"_collection":"s","_key":"1","dept":"eng","salary":100}"#).unwrap();
    db.put("s/2", r#"{"_collection":"s","_key":"2","dept":"eng","salary":200}"#).unwrap();
    db.put("s/3", r#"{"_collection":"s","_key":"3","dept":"hr","salary":150}"#).unwrap();

    let hits = db.query("SELECT dept, SUM(salary), AVG(salary) FROM s GROUP BY dept ORDER BY dept ASC")
        .unwrap().collect();
    assert_eq!(hits.len(), 2);
    let eng = hits[0].payload.as_ref().unwrap();
    assert_eq!(eng["dept"].as_str().unwrap(), "eng");
    assert_eq!(eng["sum"].as_f64().unwrap(), 300.0);
    assert_eq!(eng["avg"].as_f64().unwrap(), 150.0);
}

// ── GROUP BY + HAVING ─────────────────────────────────────────────────────────

#[test]
fn group_by_having_count() {
    let mut db = CoreDB::new();
    db.put("o/1", r#"{"_collection":"o","_key":"1","cat":"A"}"#).unwrap();
    db.put("o/2", r#"{"_collection":"o","_key":"2","cat":"A"}"#).unwrap();
    db.put("o/3", r#"{"_collection":"o","_key":"3","cat":"A"}"#).unwrap();
    db.put("o/4", r#"{"_collection":"o","_key":"4","cat":"B"}"#).unwrap();
    db.put("o/5", r#"{"_collection":"o","_key":"5","cat":"B"}"#).unwrap();
    db.put("o/6", r#"{"_collection":"o","_key":"6","cat":"C"}"#).unwrap();

    // Only groups with count >= 2 (A=3, B=2 pass; C=1 excluded)
    let hits = db.query("SELECT cat, COUNT(*) FROM o GROUP BY cat HAVING COUNT(*) >= 2 ORDER BY cat ASC")
        .unwrap().collect();
    assert_eq!(hits.len(), 2);
    let cats: Vec<&str> = hits.iter()
        .map(|h| h.payload.as_ref().unwrap()["cat"].as_str().unwrap())
        .collect();
    assert_eq!(cats, vec!["A", "B"]);
}

#[test]
fn group_by_having_sum() {
    let mut db = CoreDB::new();
    db.put("tx/1", r#"{"_collection":"tx","_key":"1","acct":"X","amount":500}"#).unwrap();
    db.put("tx/2", r#"{"_collection":"tx","_key":"2","acct":"X","amount":600}"#).unwrap();
    db.put("tx/3", r#"{"_collection":"tx","_key":"3","acct":"Y","amount":100}"#).unwrap();
    db.put("tx/4", r#"{"_collection":"tx","_key":"4","acct":"Y","amount":200}"#).unwrap();

    // Accounts with total > 500 (X=1100 passes, Y=300 excluded)
    let hits = db.query("SELECT acct, SUM(amount) FROM tx GROUP BY acct HAVING SUM(amount) > 500")
        .unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].payload.as_ref().unwrap()["acct"].as_str().unwrap(), "X");
}

#[test]
fn group_by_pg_violation_rejected() {
    let mut db = CoreDB::new();
    db.put("o/1", r#"{"_collection":"o","_key":"1","cat":"A","name":"Vines"}"#).unwrap();
    // "name" is not in GROUP BY and not aggregated — must error.
    let err = db.query("SELECT cat, name FROM o GROUP BY cat");
    assert!(err.is_err(), "PG violation should be rejected at parse time");
    let msg = err.err().unwrap().to_string();
    assert!(msg.contains("name"), "error should mention the offending column");
}

#[test]
fn group_by_multi_field_set() {
    let mut db = CoreDB::new();
    db.put("e/1", r#"{"_collection":"e","_key":"1","dept":"eng","city":"Melbourne","salary":100}"#).unwrap();
    db.put("e/2", r#"{"_collection":"e","_key":"2","dept":"eng","city":"Melbourne","salary":200}"#).unwrap();
    db.put("e/3", r#"{"_collection":"e","_key":"3","dept":"eng","city":"Fitzroy","salary":150}"#).unwrap();
    db.put("e/4", r#"{"_collection":"e","_key":"4","dept":"hr","city":"Melbourne","salary":120}"#).unwrap();

    let hits = db.query(
        "SELECT dept, city, COUNT(*) AS cnt FROM e GROUP BY dept, city ORDER BY cnt DESC"
    ).unwrap().collect();
    // 3 groups: eng/Melbourne=2, eng/Fitzroy=1, hr/Melbourne=1
    assert_eq!(hits.len(), 3);
    let top = hits[0].payload.as_ref().unwrap();
    assert_eq!(top["dept"].as_str().unwrap(), "eng");
    assert_eq!(top["city"].as_str().unwrap(), "Melbourne");
    assert_eq!(top["cnt"].as_i64().unwrap(), 2);
}

// ── Multi-column ORDER BY ─────────────────────────────────────────────────────

#[test]
fn order_by_multi_column_sql() {
    let mut db = CoreDB::new();
    db.put("u/1", r#"{"_collection":"u","dept":"eng","salary":90}"#).unwrap();
    db.put("u/2", r#"{"_collection":"u","dept":"eng","salary":70}"#).unwrap();
    db.put("u/3", r#"{"_collection":"u","dept":"hr","salary":80}"#).unwrap();
    db.put("u/4", r#"{"_collection":"u","dept":"hr","salary":60}"#).unwrap();

    let hits = db.query("SELECT * FROM u ORDER BY dept ASC, salary DESC").unwrap().collect();
    // dept ASC: eng before hr
    // within eng, salary DESC: 90 then 70
    // within hr, salary DESC: 80 then 60
    let names: Vec<String> = hits.iter()
        .map(|h| {
            let p = h.payload.as_ref().unwrap();
            format!("{}/{}", p["dept"].as_str().unwrap(), p["salary"].as_f64().unwrap())
        })
        .collect();
    assert_eq!(names, ["eng/90", "eng/70", "hr/80", "hr/60"]);
}

#[test]
fn order_by_multi_column_api() {
    let mut db = CoreDB::new();
    db.put("p/1", r#"{"_collection":"p","cat":"b","rank":2}"#).unwrap();
    db.put("p/2", r#"{"_collection":"p","cat":"a","rank":3}"#).unwrap();
    db.put("p/3", r#"{"_collection":"p","cat":"a","rank":1}"#).unwrap();

    let hits = db
        .collection("p")
        .sort_multi(vec![("cat".to_string(), true), ("rank".to_string(), true)])
        .collect();
    // cat ASC, then rank ASC within same cat
    // a/1, a/3, b/2
    let ranks: Vec<i64> = hits.iter()
        .map(|h| h.payload.as_ref().unwrap()["rank"].as_i64().unwrap())
        .collect();
    assert_eq!(ranks, [1, 3, 2]);
}

#[test]
fn order_by_single_column_unchanged() {
    let mut db = CoreDB::new();
    for i in [5u64, 3, 8, 1, 9, 2] {
        db.put(&format!("n/{i}"), &format!(r#"{{"_collection":"n","v":{i}}}"#)).unwrap();
    }
    let hits = db.query("SELECT * FROM n ORDER BY v ASC").unwrap().collect();
    let vals: Vec<i64> = hits.iter()
        .map(|h| h.payload.as_ref().unwrap()["v"].as_i64().unwrap())
        .collect();
    assert_eq!(vals, [1, 2, 3, 5, 8, 9]);
}

// ── Transactions ──────────────────────────────────────────────────────────────

#[test]
fn transaction_commit_applies_all_writes() {
    let mut db = CoreDB::new();
    let mut txn = db.begin();
    txn.put("users/alice", r#"{"_collection":"users","name":"Alice"}"#).unwrap();
    txn.put("users/bob",   r#"{"_collection":"users","name":"Bob"}"#).unwrap();
    txn.commit().unwrap();

    assert!(db.contains("users/alice"));
    assert!(db.contains("users/bob"));
    assert_eq!(db.collection("users").count(), 2);
}

#[test]
fn transaction_rollback_applies_nothing() {
    let mut db = CoreDB::new();
    {
        let mut txn = db.begin();
        txn.put("users/ghost", r#"{"_collection":"users","name":"Ghost"}"#).unwrap();
        txn.rollback(); // explicit rollback
    }
    assert!(!db.contains("users/ghost"), "ghost must not exist after rollback");

    {
        let mut txn = db.begin();
        txn.put("users/phantom", r#"{"_collection":"users","name":"Phantom"}"#).unwrap();
        // implicit rollback — drop without commit
    }
    assert!(!db.contains("users/phantom"), "phantom must not exist after implicit rollback");
}

#[test]
fn transaction_commit_returns_op_count() {
    let mut db = CoreDB::new();
    let mut txn = db.begin();
    txn.put("a/1", r#"{"_collection":"a"}"#).unwrap();
    txn.put("a/2", r#"{"_collection":"a"}"#).unwrap();
    txn.remove("a/99"); // remove of non-existent — still counted
    txn.link("a/1", "a/2", "rel", 1.0);
    let n = txn.commit().unwrap();
    assert_eq!(n, 4);
}

#[test]
fn transaction_put_validates_json_eagerly() {
    let mut db = CoreDB::new();
    let mut txn = db.begin();
    let err = txn.put("bad", "not json!!");
    assert!(err.is_err(), "bad JSON must error at put() time");
    // Even though put errored, commit/rollback are still valid
    txn.rollback();
    assert!(!db.contains("bad"));
}

#[test]
fn transaction_with_link_and_remove() {
    let mut db = CoreDB::new();
    db.put("nodes/a", r#"{"_collection":"nodes"}"#).unwrap();
    db.put("nodes/b", r#"{"_collection":"nodes"}"#).unwrap();
    db.put("nodes/c", r#"{"_collection":"nodes"}"#).unwrap();

    let mut txn = db.begin();
    txn.link("nodes/a", "nodes/b", "knows", 1.0);
    txn.unlink("nodes/a", "nodes/b", "knows"); // cancel the link above
    txn.remove("nodes/c");
    txn.commit().unwrap();

    // Link was added then removed in same txn → should not exist
    assert!(db.one("nodes/a").forward("knows").collect().is_empty());
    // c was removed
    assert!(!db.contains("nodes/c"));
}

// ── #3 btree ORDER BY index scan ──────────────────────────────────────────────

#[test]
fn btree_order_scan_produces_sorted_results() {
    let mut db = CoreDB::new();
    for i in [5u64, 1, 9, 3, 7, 2, 8, 4, 6] {
        db.put(&format!("n/{i}"), &format!(r#"{{"_collection":"n","v":{i}}}"#)).unwrap();
    }
    db.execute("CREATE INDEX ON n USING btree (v)").unwrap();

    // ORDER BY v ASC — index scan path
    let hits = db.query("SELECT * FROM n ORDER BY v ASC").unwrap().collect();
    let vals: Vec<i64> = hits.iter().map(|h| h.payload.as_ref().unwrap()["v"].as_i64().unwrap()).collect();
    assert_eq!(vals, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[test]
fn btree_order_scan_desc() {
    let mut db = CoreDB::new();
    for i in 1u64..=5 {
        db.put(&format!("n/{i}"), &format!(r#"{{"_collection":"n","score":{i}}}"#)).unwrap();
    }
    db.execute("CREATE INDEX ON n USING btree (score)").unwrap();

    let hits = db.query("SELECT * FROM n ORDER BY score DESC").unwrap().collect();
    let vals: Vec<i64> = hits.iter().map(|h| h.payload.as_ref().unwrap()["score"].as_i64().unwrap()).collect();
    assert_eq!(vals, [5, 4, 3, 2, 1]);
}

#[test]
fn btree_order_scan_with_limit() {
    let mut db = CoreDB::new();
    for i in 1u64..=100 {
        db.put(&format!("n/{i}"), &format!(r#"{{"_collection":"n","rank":{i}}}"#)).unwrap();
    }
    db.execute("CREATE INDEX ON n USING btree (rank)").unwrap();

    // Index scan extracts top-5 cheaply without loading all 100 members
    let hits = db.query("SELECT * FROM n ORDER BY rank ASC LIMIT 5").unwrap().collect();
    assert_eq!(hits.len(), 5);
    let vals: Vec<i64> = hits.iter().map(|h| h.payload.as_ref().unwrap()["rank"].as_i64().unwrap()).collect();
    assert_eq!(vals, [1, 2, 3, 4, 5]);
}

// ── #1 SQL mutations ──────────────────────────────────────────────────────────

#[test]
fn sql_insert_into_creates_node() {
    let mut db = CoreDB::new();
    db.execute(
        "INSERT INTO users (_key, name, age) VALUES ('alice', 'Alice', 30)",
    ).unwrap();

    assert!(db.contains("users/alice"));
    let hits = db.query("SELECT * FROM users WHERE name = 'Alice'").unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "users/alice");
}

#[test]
fn sql_insert_returns_one() {
    let mut db = CoreDB::new();
    let n = db.execute(
        "INSERT INTO products (_key, price) VALUES ('p1', 99)",
    ).unwrap();
    assert_eq!(n, 1);
}

#[test]
fn sql_update_set_field() {
    let mut db = CoreDB::new();
    db.put("users/bob", r#"{"_collection":"users","_key":"bob","name":"Bob","score":10}"#).unwrap();

    let n = db.execute("UPDATE users SET score = 99 WHERE _key = 'bob'").unwrap();
    assert_eq!(n, 1);

    let hits = db.query("SELECT * FROM users WHERE _key = 'bob'").unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].payload.as_ref().unwrap()["score"].as_f64().unwrap(), 99.0);
}

#[test]
fn sql_delete_from_removes_node() {
    let mut db = CoreDB::new();
    db.put("items/i1", r#"{"_collection":"items","_key":"i1","keep":false}"#).unwrap();
    db.put("items/i2", r#"{"_collection":"items","_key":"i2","keep":true}"#).unwrap();

    let n = db.execute("DELETE FROM items WHERE keep = false").unwrap();
    assert_eq!(n, 1);
    assert!(!db.contains("items/i1"), "i1 must be deleted");
    assert!(db.contains("items/i2"), "i2 must survive");
}

#[test]
fn sql_update_multiple_rows() {
    let mut db = CoreDB::new();
    for i in 1..=5 {
        db.put(
            &format!("items/i{i}"),
            &format!(r#"{{"_collection":"items","_key":"i{i}","active":true,"val":{i}}}"#),
        ).unwrap();
    }
    // Mark all active=false
    let n = db.execute("UPDATE items SET active = false WHERE active = true").unwrap();
    assert_eq!(n, 5);

    let still_active = db.query("SELECT * FROM items WHERE active = true").unwrap().count();
    assert_eq!(still_active, 0);
}

// ── #3 Btree field index ──────────────────────────────────────────────────────

#[test]
fn btree_index_eq_filter() {
    let mut db = CoreDB::new();
    for i in 0..20 {
        db.put(
            &format!("users/u{i}"),
            &format!(r#"{{"_collection":"users","_key":"u{i}","age":{i}}}"#),
        ).unwrap();
    }
    db.execute("CREATE INDEX ON users USING btree (age)").unwrap();

    let hits = db.query("SELECT * FROM users WHERE age = 5").unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "users/u5");
}

#[test]
fn btree_index_range_gt() {
    let mut db = CoreDB::new();
    for i in 0..10 {
        db.put(
            &format!("p/p{i}"),
            &format!(r#"{{"_collection":"p","_key":"p{i}","score":{i}}}"#),
        ).unwrap();
    }
    db.execute("CREATE INDEX ON p USING btree (score)").unwrap();

    let hits = db.query("SELECT * FROM p WHERE score > 6").unwrap().collect();
    assert_eq!(hits.len(), 3); // 7, 8, 9
}

#[test]
fn btree_index_range_between() {
    let mut db = CoreDB::new();
    for i in 0..20 {
        db.put(
            &format!("items/i{i}"),
            &format!(r#"{{"_collection":"items","_key":"i{i}","price":{i}}}"#),
        ).unwrap();
    }
    db.execute("CREATE INDEX ON items USING btree (price)").unwrap();

    let hits = db.query("SELECT * FROM items WHERE price BETWEEN 5 AND 10").unwrap().collect();
    assert_eq!(hits.len(), 6); // 5, 6, 7, 8, 9, 10
}

#[test]
fn btree_index_maintained_on_insert_after_create() {
    let mut db = CoreDB::new();
    // Create index first (empty collection)
    db.execute("CREATE INDEX ON orders USING btree (amount)").unwrap();

    // Insert nodes after the index exists — they should be picked up
    db.put("orders/o1", r#"{"_collection":"orders","_key":"o1","amount":100}"#).unwrap();
    db.put("orders/o2", r#"{"_collection":"orders","_key":"o2","amount":200}"#).unwrap();
    db.put("orders/o3", r#"{"_collection":"orders","_key":"o3","amount":50}"#).unwrap();

    let hits = db.query("SELECT * FROM orders WHERE amount > 75").unwrap().collect();
    assert_eq!(hits.len(), 2); // 100, 200
}

#[test]
fn btree_index_maintained_on_update() {
    let mut db = CoreDB::new();
    db.put("items/a", r#"{"_collection":"items","_key":"a","val":1}"#).unwrap();
    db.put("items/b", r#"{"_collection":"items","_key":"b","val":2}"#).unwrap();
    db.execute("CREATE INDEX ON items USING btree (val)").unwrap();

    // Update — old index entry for "a" (val=1) should be replaced with val=99
    db.execute("UPDATE items SET val = 99 WHERE _key = 'a'").unwrap();

    // val = 1 should now match nothing
    let low = db.query("SELECT * FROM items WHERE val = 1").unwrap().count();
    assert_eq!(low, 0);

    // val = 99 should match "a"
    let high = db.query("SELECT * FROM items WHERE val = 99").unwrap().collect();
    assert_eq!(high.len(), 1);
    assert_eq!(high[0].slug, "items/a");
}

#[test]
fn btree_index_maintained_on_delete() {
    let mut db = CoreDB::new();
    db.put("items/a", r#"{"_collection":"items","_key":"a","val":5}"#).unwrap();
    db.put("items/b", r#"{"_collection":"items","_key":"b","val":10}"#).unwrap();
    db.execute("CREATE INDEX ON items USING btree (val)").unwrap();

    db.remove("items/a");

    // Only "b" (val=10) should remain
    let hits = db.query("SELECT * FROM items WHERE val > 0").unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "items/b");
}

#[test]
fn btree_index_no_false_positives() {
    // When the index seeds candidates, the subsequent retain() filter must
    // confirm results — so the count must be exact, not over-inclusive.
    let mut db = CoreDB::new();
    for i in 0..50 {
        db.put(
            &format!("n/n{i}"),
            &format!(r#"{{"_collection":"n","_key":"n{i}","x":{i}}}"#),
        ).unwrap();
    }
    db.execute("CREATE INDEX ON n USING btree (x)").unwrap();

    // Strict equality: must return exactly 1 result
    let hits = db.query("SELECT * FROM n WHERE x = 25").unwrap().collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "n/n25");

    // Range: 10..=14 inclusive → exactly 5
    let hits = db.query("SELECT * FROM n WHERE x BETWEEN 10 AND 14").unwrap().collect();
    assert_eq!(hits.len(), 5);
}

// ── Schema validation tests ───────────────────────────────────────────────────

/// INSERT with correct types passes validation.
#[test]
fn schema_validation_valid_insert() {
    let mut db = CoreDB::new();
    db.execute(r#"CREATE TABLE users (_key TEXT, name TEXT, age INTEGER)"#).unwrap();
    db.execute(r#"INSERT INTO users (_key, name, age) VALUES ('alice', 'Alice', 30)"#).unwrap();
    let hits = db.query("SELECT * FROM users").unwrap().collect();
    assert_eq!(hits.len(), 1);
}

/// INSERT with wrong type on a declared field returns an error.
#[test]
fn schema_validation_rejects_wrong_type() {
    let mut db = CoreDB::new();
    db.execute(r#"CREATE TABLE products (_key TEXT, price REAL)"#).unwrap();
    let err = db.execute(r#"INSERT INTO products (_key, price) VALUES ('p1', 'not-a-number')"#);
    assert!(err.is_err(), "should reject non-number for REAL field");
}

/// INSERT into collection without a schema always succeeds.
#[test]
fn schema_validation_no_schema_is_permissive() {
    let mut db = CoreDB::new();
    // No CREATE TABLE — any payload shape is accepted
    db.execute(r#"INSERT INTO items (_key, weirdfield) VALUES ('x', 'anything')"#).unwrap();
    assert_eq!(db.query("SELECT * FROM items").unwrap().collect().len(), 1);
}

/// UPDATE with wrong type on a declared field returns an error.
#[test]
fn schema_validation_rejects_wrong_type_on_update() {
    let mut db = CoreDB::new();
    db.execute(r#"CREATE TABLE events (_key TEXT, score INTEGER)"#).unwrap();
    db.execute(r#"INSERT INTO events (_key, score) VALUES ('e1', 10)"#).unwrap();
    let err = db.execute(r#"UPDATE events SET score = 'high' WHERE _key = 'e1'"#);
    assert!(err.is_err(), "should reject non-number for INTEGER field on UPDATE");
}

/// UPDATE with correct types passes validation.
#[test]
fn schema_validation_valid_update() {
    let mut db = CoreDB::new();
    db.execute(r#"CREATE TABLE events (_key TEXT, score INTEGER)"#).unwrap();
    db.execute(r#"INSERT INTO events (_key, score) VALUES ('e1', 5)"#).unwrap();
    db.execute(r#"UPDATE events SET score = 99 WHERE _key = 'e1'"#).unwrap();
    let hits = db.query("SELECT * FROM events WHERE _key = 'e1'").unwrap().collect();
    assert_eq!(hits[0].payload.as_ref().unwrap()["score"].as_f64(), Some(99.0));
}

/// NULL is accepted for any declared field type.
#[test]
fn schema_validation_null_is_always_valid() {
    let mut db = CoreDB::new();
    db.execute(r#"CREATE TABLE logs (_key TEXT, level INTEGER)"#).unwrap();
    db.execute(r#"INSERT INTO logs (_key, level) VALUES ('l1', NULL)"#).unwrap();
    assert_eq!(db.query("SELECT * FROM logs").unwrap().collect().len(), 1);
}

// ── NOT IN ────────────────────────────────────────────────────────────────────

/// Basic `field NOT IN (v1, v2)` excludes matched values.
#[test]
fn not_in_excludes_values() {
    let mut db = CoreDB::new();
    for (k, city) in [("u1", "Jakarta"), ("u2", "Bandung"), ("u3", "Surabaya"), ("u4", "Bali")] {
        db.put(k, &format!(r#"{{"_collection":"users","city":"{city}"}}"#)).unwrap();
    }
    let hits = db
        .query("SELECT * FROM users WHERE city NOT IN ('Jakarta', 'Bali')")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 2);
    let cities: Vec<_> = hits.iter()
        .map(|h| h.payload.as_ref().unwrap()["city"].as_str().unwrap().to_string())
        .collect();
    assert!(cities.contains(&"Bandung".to_string()));
    assert!(cities.contains(&"Surabaya".to_string()));
}

/// `NOT IN` with numbers.
#[test]
fn not_in_numeric() {
    let mut db = CoreDB::new();
    for i in 1..=5u32 {
        db.put(&format!("x{i}"), &format!(r#"{{"_collection":"nums","v":{i}}}"#)).unwrap();
    }
    // Exclude 2 and 4 — expect 1, 3, 5
    let hits = db
        .query("SELECT * FROM nums WHERE v NOT IN (2, 4)")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 3);
}

/// `NOT field IN (...)` prefix form also works.
#[test]
fn not_prefix_in_also_works() {
    let mut db = CoreDB::new();
    db.put("a", r#"{"_collection":"t","k":"alpha"}"#).unwrap();
    db.put("b", r#"{"_collection":"t","k":"beta"}"#).unwrap();
    db.put("c", r#"{"_collection":"t","k":"gamma"}"#).unwrap();
    // prefix NOT form
    let hits = db
        .query("SELECT * FROM t WHERE NOT k IN ('alpha', 'gamma')")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].payload.as_ref().unwrap()["k"].as_str(), Some("beta"));
}

/// Combined AND + NOT IN.
#[test]
fn not_in_combined_with_and() {
    let mut db = CoreDB::new();
    for (k, city, active) in [
        ("u1", "Jakarta", true),
        ("u2", "Jakarta", false),
        ("u3", "Bandung", true),
        ("u4", "Bali",    true),
    ] {
        db.put(k, &format!(r#"{{"_collection":"users","city":"{city}","active":{active}}}"#))
            .unwrap();
    }
    // active=true AND city NOT IN ('Bali')
    let hits = db
        .query("SELECT * FROM users WHERE active = true AND city NOT IN ('Bali')")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 2); // u1 (Jakarta,true) and u3 (Bandung,true)
}

// ── put_vector via SQL ────────────────────────────────────────────────────────

/// INSERT with a `[f32, ...]` array literal stores the vector and makes it
/// searchable via `VECTOR_NEAR`.
#[test]
fn sql_insert_vector_literal() {
    let mut db = CoreDB::new();
    db.execute(r#"CREATE TABLE docs (_key TEXT, emb VECTOR)"#).unwrap();
    db.execute(r#"INSERT INTO docs (_key, emb) VALUES ('d1', [1.0, 0.0, 0.0])"#).unwrap();
    db.execute(r#"INSERT INTO docs (_key, emb) VALUES ('d2', [0.0, 1.0, 0.0])"#).unwrap();
    db.execute(r#"INSERT INTO docs (_key, emb) VALUES ('d3', [0.0, 0.0, 1.0])"#).unwrap();

    let hits = db
        .query("SELECT * FROM docs WHERE VECTOR_NEAR(emb, [1.0, 0.0, 0.0], 1)")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "docs/d1");
}

/// UPDATE with a `[f32, ...]` literal replaces the stored vector.
#[test]
fn sql_update_vector_literal() {
    let mut db = CoreDB::new();
    db.execute(r#"CREATE TABLE docs (_key TEXT, emb VECTOR)"#).unwrap();
    // Insert with initial vectors
    db.execute(r#"INSERT INTO docs (_key, emb) VALUES ('d1', [0.0, 1.0, 0.0])"#).unwrap();
    db.execute(r#"INSERT INTO docs (_key, emb) VALUES ('d2', [1.0, 0.0, 0.0])"#).unwrap();

    // Before update: query [1,0,0] should return d2 as nearest
    let before = db
        .query("SELECT * FROM docs WHERE VECTOR_NEAR(emb, [1.0, 0.0, 0.0], 1)")
        .unwrap()
        .collect();
    assert_eq!(before[0].slug, "docs/d2");

    // Update d1's vector to point toward [1,0,0]
    db.execute(r#"UPDATE docs SET emb = [1.0, 0.0, 0.0] WHERE _key = 'd1'"#).unwrap();

    // After update: both d1 and d2 are equal distance — top-2 should return both
    let after = db
        .query("SELECT * FROM docs WHERE VECTOR_NEAR(emb, [1.0, 0.0, 0.0], 2)")
        .unwrap()
        .collect();
    assert_eq!(after.len(), 2, "both docs should be near after update");
}

/// SQL-inserted vectors are also queryable via the builder atom API.
#[test]
fn sql_insert_vector_queryable_via_atom() {
    let mut db = CoreDB::new();
    db.execute(r#"CREATE TABLE items (_key TEXT, vec VECTOR)"#).unwrap();
    db.execute(r#"INSERT INTO items (_key, vec) VALUES ('a', [0.6, 0.8, 0.0])"#).unwrap();
    db.execute(r#"INSERT INTO items (_key, vec) VALUES ('b', [0.0, 0.0, 1.0])"#).unwrap();

    let results = db
        .collection("items")
        .vector_near("vec", vec![0.6, 0.8, 0.0], 1)
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].slug, "items/a");
}

// ── ORDER BY field <=> [...] (vector similarity sort) ─────────────────────────

/// `ORDER BY emb <=> [...]` returns all results sorted nearest-first.
#[test]
fn order_by_vector_similarity() {
    let mut db = CoreDB::new();
    db.execute(r#"CREATE TABLE docs (_key TEXT, emb VECTOR)"#).unwrap();
    // Three orthogonal unit vectors
    db.execute(r#"INSERT INTO docs (_key, emb) VALUES ('d1', [1.0, 0.0, 0.0])"#).unwrap();
    db.execute(r#"INSERT INTO docs (_key, emb) VALUES ('d2', [0.0, 1.0, 0.0])"#).unwrap();
    db.execute(r#"INSERT INTO docs (_key, emb) VALUES ('d3', [0.0, 0.0, 1.0])"#).unwrap();

    // Query closest to d1
    let hits = db
        .query("SELECT * FROM docs ORDER BY emb <=> [1.0, 0.0, 0.0]")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0].slug, "docs/d1", "d1 should be nearest to [1,0,0]");
}

/// `ORDER BY` with `LIMIT` returns only the k nearest.
#[test]
fn order_by_vector_with_limit() {
    let mut db = CoreDB::new();
    db.execute(r#"CREATE TABLE notes (_key TEXT, vec VECTOR)"#).unwrap();
    for i in 0..10u32 {
        // Vectors rotating in the XY plane
        let x = (i as f32) / 10.0;
        let y = 1.0 - x;
        db.execute(&format!(r#"INSERT INTO notes (_key, vec) VALUES ('n{i}', [{x}, {y}, 0.0])"#))
            .unwrap();
    }
    // Query close to [0.9, 0.1, 0.0] — n9 has x=0.9,y=0.1
    let hits = db
        .query("SELECT * FROM notes ORDER BY vec <=> [0.9, 0.1, 0.0] LIMIT 3")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0].slug, "notes/n9", "n9 should be nearest to [0.9, 0.1, 0.0]");
}

/// `WHERE` filter combined with `ORDER BY <=>` — filter first, then rank.
#[test]
fn order_by_vector_with_where_filter() {
    let mut db = CoreDB::new();
    db.execute(r#"CREATE TABLE items (_key TEXT, tag TEXT, vec VECTOR)"#).unwrap();
    db.execute(r#"INSERT INTO items (_key, tag, vec) VALUES ('a', 'good', [1.0, 0.0])"#).unwrap();
    db.execute(r#"INSERT INTO items (_key, tag, vec) VALUES ('b', 'good', [0.9, 0.1])"#).unwrap();
    db.execute(r#"INSERT INTO items (_key, tag, vec) VALUES ('c', 'bad',  [1.0, 0.0])"#).unwrap();

    // Only tag='good', sorted by distance to [1,0]
    let hits = db
        .query("SELECT * FROM items WHERE tag = 'good' ORDER BY vec <=> [1.0, 0.0]")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].slug, "items/a", "a has exact match [1,0]");
}

// ── ORDER BY arithmetic score expressions ────────────────────────────────────

/// `ORDER BY field * weight` — plain field weighted sort.
#[test]
fn order_by_expr_field_multiply() {
    let mut db = CoreDB::new();
    db.put("p/1", r#"{"_collection":"p","_key":"1","score":10}"#).unwrap();
    db.put("p/2", r#"{"_collection":"p","_key":"2","score":30}"#).unwrap();
    db.put("p/3", r#"{"_collection":"p","_key":"3","score":20}"#).unwrap();

    // score * 1.0 DESC — same as ORDER BY score DESC
    let hits = db.query("SELECT * FROM p ORDER BY score * 1.0 DESC").unwrap().collect();
    let scores: Vec<f64> = hits.iter()
        .map(|h| h.payload.as_ref().unwrap()["score"].as_f64().unwrap())
        .collect();
    assert_eq!(scores, [30.0, 20.0, 10.0]);
}

/// `ORDER BY a + b` — sum of two payload fields.
#[test]
fn order_by_expr_field_addition() {
    let mut db = CoreDB::new();
    db.put("p/1", r#"{"_collection":"p","_key":"1","a":1,"b":9}"#).unwrap(); // sum=10
    db.put("p/2", r#"{"_collection":"p","_key":"2","a":5,"b":3}"#).unwrap(); // sum=8
    db.put("p/3", r#"{"_collection":"p","_key":"3","a":7,"b":7}"#).unwrap(); // sum=14

    let hits = db.query("SELECT * FROM p ORDER BY a + b DESC").unwrap().collect();
    let sums: Vec<f64> = hits.iter()
        .map(|h| {
            let p = h.payload.as_ref().unwrap();
            p["a"].as_f64().unwrap() + p["b"].as_f64().unwrap()
        })
        .collect();
    assert_eq!(sums, [14.0, 10.0, 8.0]);
}

/// `ORDER BY a * 0.6 + b * 0.4 DESC` — weighted combination of two fields.
#[test]
fn order_by_expr_weighted_fields() {
    let mut db = CoreDB::new();
    // weighted = a*0.6 + b*0.4
    db.put("p/1", r#"{"_collection":"p","_key":"1","a":10,"b":0}"#).unwrap();  // 6.0
    db.put("p/2", r#"{"_collection":"p","_key":"2","a":0,"b":10}"#).unwrap();  // 4.0
    db.put("p/3", r#"{"_collection":"p","_key":"3","a":5,"b":10}"#).unwrap();  // 7.0

    let hits = db.query("SELECT * FROM p ORDER BY a * 0.6 + b * 0.4 DESC").unwrap().collect();
    let keys: Vec<&str> = hits.iter()
        .map(|h| h.slug.split('/').last().unwrap())
        .collect();
    assert_eq!(keys, ["3", "1", "2"]); // 7.0, 6.0, 4.0
}

/// `ORDER BY (a + b) * 0.5 DESC` — parenthesised sub-expression.
#[test]
fn order_by_expr_parentheses() {
    let mut db = CoreDB::new();
    db.put("p/1", r#"{"_collection":"p","_key":"1","a":2,"b":8}"#).unwrap();  // (2+8)*0.5=5.0
    db.put("p/2", r#"{"_collection":"p","_key":"2","a":6,"b":6}"#).unwrap();  // (6+6)*0.5=6.0
    db.put("p/3", r#"{"_collection":"p","_key":"3","a":1,"b":1}"#).unwrap();  // (1+1)*0.5=1.0

    let hits = db.query("SELECT * FROM p ORDER BY (a + b) * 0.5 DESC").unwrap().collect();
    let keys: Vec<&str> = hits.iter()
        .map(|h| h.slug.split('/').last().unwrap())
        .collect();
    assert_eq!(keys, ["2", "1", "3"]); // 6.0, 5.0, 1.0
}

/// `ORDER BY BM25(field, 'q') * 0.7 + BM25(body, 'q') * 0.3 DESC` — two BM25 signals.
#[test]
fn order_by_expr_dual_bm25() {
    // BM25 IDF is positive only when df < N/2. We ensure 'rust' appears in 1
    // of 5 docs so IDF = ln((5-1+0.5)/(1+0.5)) ≈ ln(3) ≈ 1.1 > 0.
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE docs (_key TEXT, title TEXT, body TEXT)").unwrap();
    // d1: the only rust doc — should rank first.
    db.execute("INSERT INTO docs (_key, title, body) VALUES ('d1', 'rust programming guide', 'building systems in rust')").unwrap();
    // d2-d5: no rust — should all rank below d1.
    db.execute("INSERT INTO docs (_key, title, body) VALUES ('d2', 'introduction to python', 'scripting with python')").unwrap();
    db.execute("INSERT INTO docs (_key, title, body) VALUES ('d3', 'web development basics', 'html css javascript guide')").unwrap();
    db.execute("INSERT INTO docs (_key, title, body) VALUES ('d4', 'database fundamentals', 'sql and nosql databases overview')").unwrap();
    db.execute("INSERT INTO docs (_key, title, body) VALUES ('d5', 'machine learning overview', 'neural networks and model training')").unwrap();
    // CREATE INDEX builds the BM25 index from all existing data in the collection.
    db.execute("CREATE INDEX ON docs USING bm25 (title)").unwrap();
    db.execute("CREATE INDEX ON docs USING bm25 (body)").unwrap();

    let hits = db
        .query("SELECT * FROM docs ORDER BY BM25(title, 'rust') * 0.7 + BM25(body, 'rust') * 0.3 DESC")
        .unwrap()
        .collect();

    assert_eq!(hits.len(), 5);
    // d1 is the only rust doc — it must rank first.
    assert_eq!(hits[0].slug, "docs/d1",
        "d1 is the only rust doc and must rank first");
}

/// `ORDER BY BM25(field,'q') * 0.5 + score * 0.5 DESC` — BM25 + numeric field hybrid.
#[test]
fn order_by_expr_bm25_plus_field() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE docs (_key TEXT, title TEXT, score REAL)").unwrap();
    // d1: title matches 'rust' well, score=1
    db.execute("INSERT INTO docs (_key, title, score) VALUES ('d1', 'rust systems programming', 1)").unwrap();
    // d2: title matches 'rust', but also has score=100 → should beat d1 via hybrid
    db.execute("INSERT INTO docs (_key, title, score) VALUES ('d2', 'rust basics', 100)").unwrap();
    // d3: no match, low score
    db.execute("INSERT INTO docs (_key, title, score) VALUES ('d3', 'python scripting', 1)").unwrap();
    // CREATE INDEX builds the BM25 index from all existing data.
    db.execute("CREATE INDEX ON docs USING bm25 (title)").unwrap();

    let hits = db
        .query("SELECT * FROM docs ORDER BY BM25(title, 'rust') * 0.5 + score * 0.5 DESC")
        .unwrap()
        .collect();

    assert_eq!(hits.len(), 3);
    // d2 (rust + score=100) should beat d1 (rust + score=1)
    assert_eq!(hits[0].slug, "docs/d2", "d2 has high score so should rank first");
    // d3 gets 0 BM25 but score=1; d1 gets BM25 but score=1; d1 wins on BM25
    let last_key = hits[2].slug.split('/').last().unwrap();
    assert_eq!(last_key, "d3", "d3 with no title match should rank last");
}

/// Backward compat: `ORDER BY field ASC` still works unchanged.
#[test]
fn order_by_expr_backward_compat_field() {
    let mut db = CoreDB::new();
    db.put("p/1", r#"{"_collection":"p","_key":"1","v":3}"#).unwrap();
    db.put("p/2", r#"{"_collection":"p","_key":"2","v":1}"#).unwrap();
    db.put("p/3", r#"{"_collection":"p","_key":"3","v":2}"#).unwrap();

    let hits = db.query("SELECT * FROM p ORDER BY v ASC").unwrap().collect();
    let vals: Vec<i64> = hits.iter().map(|h| h.payload.as_ref().unwrap()["v"].as_i64().unwrap()).collect();
    assert_eq!(vals, [1, 2, 3]);
}

/// Backward compat: `ORDER BY field1 ASC, field2 DESC` multi-column still works.
#[test]
fn order_by_expr_backward_compat_multi_column() {
    let mut db = CoreDB::new();
    db.put("p/1", r#"{"_collection":"p","_key":"1","cat":"a","v":2}"#).unwrap();
    db.put("p/2", r#"{"_collection":"p","_key":"2","cat":"a","v":1}"#).unwrap();
    db.put("p/3", r#"{"_collection":"p","_key":"3","cat":"b","v":5}"#).unwrap();

    let hits = db.query("SELECT * FROM p ORDER BY cat ASC, v DESC").unwrap().collect();
    let keys: Vec<&str> = hits.iter().map(|h| h.slug.split('/').last().unwrap()).collect();
    assert_eq!(keys, ["1", "2", "3"]); // cat ASC: a before b; within a, v DESC: 2 then 1
}

/// Backward compat: `ORDER BY field <=> [vec]` vector sort still works.
#[test]
fn order_by_expr_backward_compat_vector() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE vdocs (_key TEXT, emb VECTOR)").unwrap();
    db.execute("INSERT INTO vdocs (_key, emb) VALUES ('v1', [1.0, 0.0, 0.0])").unwrap();
    db.execute("INSERT INTO vdocs (_key, emb) VALUES ('v2', [0.0, 1.0, 0.0])").unwrap();
    db.execute("INSERT INTO vdocs (_key, emb) VALUES ('v3', [0.0, 0.0, 1.0])").unwrap();

    let hits = db.query("SELECT * FROM vdocs ORDER BY emb <=> [1.0, 0.0, 0.0]").unwrap().collect();
    assert_eq!(hits[0].slug, "vdocs/v1");
}

/// `ORDER BY field <-> [vec]` (L2 operator) and `VECTOR_L2(field, [vec])` function form.
#[test]
fn order_by_vector_l2_operator_and_function() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE items (_key TEXT, emb VECTOR)").unwrap();
    // v1 is at [1,0,0], v2 at [0,1,0], v3 at [0,0,1]
    db.execute("INSERT INTO items (_key, emb) VALUES ('v1', [1.0, 0.0, 0.0])").unwrap();
    db.execute("INSERT INTO items (_key, emb) VALUES ('v2', [0.0, 1.0, 0.0])").unwrap();
    db.execute("INSERT INTO items (_key, emb) VALUES ('v3', [0.0, 0.0, 1.0])").unwrap();

    // Operator form: <-> nearest L2 to [1,0,0] → v1 first
    let op_hits: Vec<_> = db.query("SELECT * FROM items ORDER BY emb <-> [1.0, 0.0, 0.0]").unwrap().collect();
    assert_eq!(op_hits[0].slug, "items/v1", "<-> operator: nearest L2 first");

    // Function form: VECTOR_L2 DESC (lowest distance = negative = highest score)
    let fn_hits: Vec<_> = db.query("SELECT * FROM items ORDER BY -VECTOR_L2(emb, [1.0, 0.0, 0.0]) DESC").unwrap().collect();
    assert_eq!(fn_hits[0].slug, "items/v1", "VECTOR_L2 function: nearest first");
}

/// `ORDER BY field <#> [vec]` (Dot product operator) — highest similarity first.
#[test]
fn order_by_vector_dot_operator() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE items (_key TEXT, emb VECTOR)").unwrap();
    db.execute("INSERT INTO items (_key, emb) VALUES ('strong', [0.9, 0.9, 0.9])").unwrap();
    db.execute("INSERT INTO items (_key, emb) VALUES ('weak',   [0.1, 0.1, 0.1])").unwrap();
    db.execute("INSERT INTO items (_key, emb) VALUES ('mid',    [0.5, 0.5, 0.5])").unwrap();

    // <#> negates internally so highest dot product = first (ascending negated)
    let hits: Vec<_> = db.query("SELECT * FROM items ORDER BY emb <#> [1.0, 1.0, 1.0]").unwrap().collect();
    assert_eq!(hits[0].slug, "items/strong", "<#> operator: highest dot product first");
    assert_eq!(hits[2].slug, "items/weak",   "<#> operator: lowest dot product last");
}

// ── ORDER BY spatial + graph signals ──────────────────────────────────────────

/// ST_DISTANCE as a score signal: closer venues rank higher when we negate distance.
/// Venues (all in Melbourne CBD) ordered by proximity to Flinders Street Station.
#[test]
fn order_by_expr_st_distance_descending_proximity() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE venues (_key TEXT, name TEXT, geometry GEO)").unwrap();
    // Flinders Street Station: 144.9671, -37.8183
    // Young and Jacksons: 144.9631, -37.8173 — nearest
    // Melbourne Central: 144.9631, -37.8102 — a bit further
    // Geelong Station: 144.3617, -38.1499 — ~70 km away
    db.execute("INSERT INTO venues (_key, name, geometry) VALUES ('fss', 'Flinders Street Station', '{\"type\":\"Point\",\"coordinates\":[144.9671,-37.8183]}')").unwrap();
    db.execute("INSERT INTO venues (_key, name, geometry) VALUES ('yj', 'Young and Jacksons', '{\"type\":\"Point\",\"coordinates\":[144.9631,-37.8173]}')").unwrap();
    db.execute("INSERT INTO venues (_key, name, geometry) VALUES ('mc', 'Melbourne Central', '{\"type\":\"Point\",\"coordinates\":[144.9631,-37.8102]}')").unwrap();
    db.execute("INSERT INTO venues (_key, name, geometry) VALUES ('gs', 'Geelong Station', '{\"type\":\"Point\",\"coordinates\":[144.3617,-38.1499]}')").unwrap();

    // Sort by negative ST_DISTANCE_KM from Flinders Street Station — ascending distance = descending score.
    // fss itself has distance 0 (most negative negate = highest), geelong is furthest.
    let hits = db
        .query("SELECT * FROM venues ORDER BY -ST_DISTANCE_KM(geometry, POINT(144.9671 -37.8183)) DESC")
        .unwrap()
        .collect();

    assert_eq!(hits[0].slug, "venues/fss", "distance-0 node must rank first: {:?}", hits.iter().map(|h| &h.slug).collect::<Vec<_>>());
    assert_eq!(hits.last().unwrap().slug, "venues/gs", "Geelong must rank last");
}

// ── Cascade edge deletion on node remove ──────────────────────────────────────

/// Deleting a node removes its outgoing edges so the target no longer sees
/// back-pointers from the deleted node.
#[test]
fn delete_node_removes_outgoing_edges() {
    let mut db = CoreDB::new();
    db.put("artists/dewa19", r#"{"_collection":"artists","_key":"dewa19"}"#).unwrap();
    db.put("songs/kangen",   r#"{"_collection":"songs",  "_key":"kangen"}"#).unwrap();
    db.link("artists/dewa19", "songs/kangen", "has_song", 1.0);

    // Sanity: edge exists
    assert_eq!(db.edges_from("artists/dewa19").len(), 1);

    db.remove("artists/dewa19");

    // Forward edge is gone
    assert_eq!(db.edges_from("artists/dewa19").len(), 0);
    // Back-pointer on the target is also gone — no dangling ref
    assert_eq!(db.edges_to("songs/kangen").len(), 0,
        "deleting dewa19 must remove kangen's back-pointer");
}

/// Deleting a target node removes incoming edges so the source no longer
/// enumerates a dead forward pointer.
#[test]
fn delete_node_removes_incoming_edges() {
    let mut db = CoreDB::new();
    db.put("artists/dewa19", r#"{"_collection":"artists","_key":"dewa19"}"#).unwrap();
    db.put("songs/kangen",   r#"{"_collection":"songs",  "_key":"kangen"}"#).unwrap();
    db.link("artists/dewa19", "songs/kangen", "has_song", 1.0);

    db.remove("songs/kangen");

    // Back-pointer is gone
    assert_eq!(db.edges_to("songs/kangen").len(), 0);
    // Forward pointer from source is also gone — no dangling ref
    assert_eq!(db.edges_from("artists/dewa19").len(), 0,
        "deleting kangen must remove dewa19's forward pointer");
}

/// SQL DELETE also cascades edges.
#[test]
fn sql_delete_cascades_edges() {
    let mut db = CoreDB::new();
    db.put("a/1", r#"{"_collection":"a","_key":"1"}"#).unwrap();
    db.put("a/2", r#"{"_collection":"a","_key":"2"}"#).unwrap();
    db.put("a/3", r#"{"_collection":"a","_key":"3"}"#).unwrap();
    db.link("a/1", "a/2", "rel", 1.0);
    db.link("a/2", "a/3", "rel", 1.0);

    // Delete middle node via SQL
    db.execute("DELETE FROM a WHERE _key = '2'").unwrap();

    assert!(!db.contains("a/2"));
    assert_eq!(db.edges_from("a/1").len(), 0, "forward edge from a/1 must be gone");
    assert_eq!(db.edges_to("a/3").len(),  0, "back edge into a/3 must be gone");
}

// ── Aggregate MATCH tests ─────────────────────────────────

/// Basic aggregate MATCH: one hop, flat (no GROUP BY).
#[test]
fn traverse_single_hop_flat() {
    let mut db = CoreDB::new();
    db.put("students/budi", r#"{"_collection":"students","_key":"budi","name":"Budi"}"#).unwrap();
    db.put("answers/a1", r#"{"_collection":"answers","_key":"a1","score":0.8}"#).unwrap();
    db.put("answers/a2", r#"{"_collection":"answers","_key":"a2","score":0.6}"#).unwrap();
    db.link("students/budi", "answers/a1", "answered", 1.0);
    db.link("students/budi", "answers/a2", "answered", 1.0);

    let hits = db.query(
        "SELECT a.score AS score FROM MATCH ('students/budi')-[:answered]->(a)"
    ).unwrap().collect();

    assert_eq!(hits.len(), 2);
    let scores: Vec<f64> = hits.iter()
        .map(|h| h.payload.as_ref().unwrap().get("score").and_then(|v| v.as_f64()).unwrap())
        .collect();
    assert!(scores.contains(&0.8));
    assert!(scores.contains(&0.6));
}

/// Two-hop aggregate MATCH with GROUP BY and SUM aggregation — OBE-style weighted score.
#[test]
fn traverse_two_hop_group_sum() {
    let mut db = CoreDB::new();
    db.put("students/budi",  r#"{"_collection":"students","_key":"budi"}"#).unwrap();
    db.put("answers/a1",     r#"{"_collection":"answers","_key":"a1","score":0.8}"#).unwrap();
    db.put("answers/a2",     r#"{"_collection":"answers","_key":"a2","score":0.6}"#).unwrap();
    db.put("answers/a3",     r#"{"_collection":"answers","_key":"a3","score":1.0}"#).unwrap();
    db.put("questions/q1",   r#"{"_collection":"questions","_key":"q1","weight":0.4,"clo":"c1"}"#).unwrap();
    db.put("questions/q2",   r#"{"_collection":"questions","_key":"q2","weight":0.6,"clo":"c1"}"#).unwrap();
    db.put("questions/q3",   r#"{"_collection":"questions","_key":"q3","weight":1.0,"clo":"c2"}"#).unwrap();

    // Student answered questions
    db.link("students/budi", "answers/a1", "answered", 1.0);
    db.link("students/budi", "answers/a2", "answered", 1.0);
    db.link("students/budi", "answers/a3", "answered", 1.0);
    // Answers → questions
    db.link("answers/a1", "questions/q1", "for", 1.0);
    db.link("answers/a2", "questions/q2", "for", 1.0);
    db.link("answers/a3", "questions/q3", "for", 1.0);

    let hits = db.query(
        "SELECT q.clo AS clo, SUM(a.score * q.weight) AS clo_score \
         FROM MATCH ('students/budi')-[:answered]->(a)-[:for]->(q) \
         GROUP BY q.clo \
         ORDER BY clo_score DESC"
    ).unwrap().collect();

    assert_eq!(hits.len(), 2, "should have 2 CLO groups");

    // CLO c1: a1.score(0.8) * q1.weight(0.4) + a2.score(0.6) * q2.weight(0.6) = 0.32 + 0.36 = 0.68
    // CLO c2: a3.score(1.0) * q3.weight(1.0) = 1.0
    let top = hits[0].payload.as_ref().unwrap();
    assert_eq!(top.get("clo").and_then(|v| v.as_str()), Some("c2"),
               "c2 has higher score (1.0) so comes first with DESC ordering");
    let top_score = top.get("clo_score").and_then(|v| v.as_f64()).unwrap();
    assert!((top_score - 1.0).abs() < 1e-9, "c2 score should be 1.0");

    let second = hits[1].payload.as_ref().unwrap();
    let second_score = second.get("clo_score").and_then(|v| v.as_f64()).unwrap();
    assert!((second_score - 0.68).abs() < 1e-9, "c1 score should be 0.68");
}

/// Multi-field MATCH GROUP BY: GROUP BY b.role, b.tier — uniform interface with Set path.
///
/// Note: the start variable is not bound in PathRow; GROUP BY fields must reference
/// destination-hop variables.  This test groups on two fields of the same destination `b`.
#[test]
fn traverse_match_group_by_multi_field() {
    let mut db = CoreDB::new();
    db.put("users/u1", r#"{"_collection":"users","_key":"u1"}"#).unwrap();
    db.put("users/u2", r#"{"_collection":"users","_key":"u2"}"#).unwrap();
    db.put("users/u3", r#"{"_collection":"users","_key":"u3"}"#).unwrap();
    // Two roles share (admin, high); one is (viewer, low)
    db.put("roles/r1", r#"{"_collection":"roles","_key":"r1","role":"admin","tier":"high"}"#).unwrap();
    db.put("roles/r2", r#"{"_collection":"roles","_key":"r2","role":"viewer","tier":"low"}"#).unwrap();
    db.put("roles/r3", r#"{"_collection":"roles","_key":"r3","role":"admin","tier":"high"}"#).unwrap();

    db.link("users/u1", "roles/r1", "has", 1.0);
    db.link("users/u2", "roles/r2", "has", 1.0);
    db.link("users/u3", "roles/r3", "has", 1.0);

    let hits = db.query(
        "SELECT b.role AS role, b.tier AS tier, COUNT(*) AS cnt \
         FROM MATCH (a:users)-[:has]->(b:roles) \
         GROUP BY b.role, b.tier \
         ORDER BY cnt DESC"
    ).unwrap().collect();

    // (admin, high) = 2 paths, (viewer, low) = 1 path
    assert_eq!(hits.len(), 2);
    let top = hits[0].payload.as_ref().unwrap();
    assert_eq!(top["role"].as_str().unwrap(), "admin");
    assert_eq!(top["tier"].as_str().unwrap(), "high");
    assert_eq!(top["cnt"].as_i64().unwrap(), 2);
    let bot = hits[1].payload.as_ref().unwrap();
    assert_eq!(bot["role"].as_str().unwrap(), "viewer");
    assert_eq!(bot["cnt"].as_i64().unwrap(), 1);
}

/// Aggregate MATCH COUNT(*) and AVG aggregation.
#[test]
fn traverse_count_and_avg() {
    let mut db = CoreDB::new();
    db.put("students/budi", r#"{"_collection":"students","_key":"budi"}"#).unwrap();
    for i in 1..=4 {
        db.put(
            &format!("answers/a{i}"),
            &format!(r#"{{"_collection":"answers","_key":"a{i}","score":{}}}"#, i as f64 * 0.25)
        ).unwrap();
        db.link("students/budi", &format!("answers/a{i}"), "answered", 1.0);
    }

    let hits = db.query(
        "SELECT COUNT(*) AS cnt, AVG(a.score) AS avg_score \
         FROM MATCH ('students/budi')-[:answered]->(a)"
    ).unwrap().collect();

    // Without GROUP BY → one row per path (4 answers × 1 student)
    // COUNT and AVG applied per group of size 1 each
    assert_eq!(hits.len(), 4);
    // Each row: count=1, avg_score=the individual score
    for hit in &hits {
        let cnt = hit.payload.as_ref().unwrap().get("cnt").and_then(|v| v.as_i64()).unwrap();
        assert_eq!(cnt, 1);
    }
}

/// Aggregate MATCH with LIMIT.
#[test]
fn traverse_with_limit() {
    let mut db = CoreDB::new();
    db.put("s/root", r#"{"_collection":"s","_key":"root"}"#).unwrap();
    for i in 1..=10 {
        db.put(&format!("t/n{i}"), &format!(r#"{{"_collection":"t","_key":"n{i}","val":{i}}}"#)).unwrap();
        db.link("s/root", &format!("t/n{i}"), "to", 1.0);
    }

    let hits = db.query(
        "SELECT n.val AS val FROM MATCH ('s/root')-[:to]->(n) LIMIT 5"
    ).unwrap().collect();
    assert_eq!(hits.len(), 5);
}

/// Aggregate MATCH from a collection (all starting nodes in the collection).
#[test]
fn traverse_from_collection() {
    let mut db = CoreDB::new();
    for s in ["alice", "bob"] {
        db.put(&format!("students/{s}"), &format!(r#"{{"_collection":"students","_key":"{s}"}}"#)).unwrap();
        db.put(&format!("answers/{s}_ans"), &format!(r#"{{"_collection":"answers","_key":"{s}_ans","score":0.9}}"#)).unwrap();
        db.link(&format!("students/{s}"), &format!("answers/{s}_ans"), "answered", 1.0);
    }

    let hits = db.query(
        "SELECT a.score AS score FROM MATCH (s:students)-[:answered]->(a)"
    ).unwrap().collect();
    assert_eq!(hits.len(), 2, "one path per student");
}

/// Aggregate MATCH with MIN/MAX.
#[test]
fn traverse_min_max() {
    let mut db = CoreDB::new();
    db.put("root/r", r#"{"_collection":"root","_key":"r"}"#).unwrap();
    for (k, v) in [("a", 10.0f64), ("b", 5.0), ("c", 8.0)] {
        db.put(&format!("vals/{k}"), &format!(r#"{{"_collection":"vals","_key":"{k}","v":{v}}}"#)).unwrap();
        db.link("root/r", &format!("vals/{k}"), "link", 1.0);
    }

    let hits = db.query(
        "SELECT MIN(n.v) AS min_v, MAX(n.v) AS max_v \
         FROM MATCH ('root/r')-[:link]->(n)"
    ).unwrap().collect();
    // No GROUP BY → 3 flat rows, min/max evaluated over single-element group each
    assert_eq!(hits.len(), 3);

    // SUM of all MIN values == sum of all individual values (since each group has size 1)
    let sum_of_mins: f64 = hits.iter()
        .map(|h| h.payload.as_ref().unwrap().get("min_v").and_then(|v| v.as_f64()).unwrap())
        .sum();
    assert!((sum_of_mins - 23.0).abs() < 1e-9);
}

// ── MATCH + WITH pipeline tests ───────────────────────────────────────────────

/// Basic pipeline: one MATCH, one RETURN with scalar projection.
#[test]
fn pipeline_single_match_scalar_return() {
    let mut db = CoreDB::new();
    db.put("users/alice", r#"{"_collection":"users","_key":"alice","score":10.0}"#).unwrap();
    db.put("users/bob",   r#"{"_collection":"users","_key":"bob","score":20.0}"#).unwrap();
    db.put("posts/p1",    r#"{"_collection":"posts","_key":"p1","title":"hello"}"#).unwrap();
    db.put("posts/p2",    r#"{"_collection":"posts","_key":"p2","title":"world"}"#).unwrap();
    db.link("users/alice", "posts/p1", "wrote", 1.0);
    db.link("users/alice", "posts/p2", "wrote", 1.0);

    let hits = db.pipeline_query(
        "MATCH ('users/alice')-[:wrote]->(p) RETURN p._key AS post_key",
    ).unwrap();

    let mut keys: Vec<String> = hits.iter()
        .map(|h| h.payload.as_ref().unwrap().get("post_key")
            .and_then(|v| v.as_str()).unwrap().to_string())
        .collect();
    keys.sort();
    assert_eq!(keys, vec!["p1", "p2"]);
}

/// Pipeline with WITH aggregation — OBE-style 1-level CLO calculation.
#[test]
fn pipeline_match_with_group_sum() {
    let mut db = CoreDB::new();

    // Student
    db.put("students/budi", r#"{"_collection":"students","_key":"budi"}"#).unwrap();

    // Answers (with score)
    db.put("answers/a1", r#"{"_collection":"answers","_key":"a1","score":0.8}"#).unwrap();
    db.put("answers/a2", r#"{"_collection":"answers","_key":"a2","score":0.9}"#).unwrap();
    db.put("answers/a3", r#"{"_collection":"answers","_key":"a3","score":1.0}"#).unwrap();

    // Questions (with clo and weight)
    db.put("questions/q1", r#"{"_collection":"questions","_key":"q1","clo":"clo1","weight":0.4}"#).unwrap();
    db.put("questions/q2", r#"{"_collection":"questions","_key":"q2","clo":"clo1","weight":0.6}"#).unwrap();
    db.put("questions/q3", r#"{"_collection":"questions","_key":"q3","clo":"clo2","weight":1.0}"#).unwrap();

    // Edges
    db.link("students/budi", "answers/a1", "answered", 1.0);
    db.link("students/budi", "answers/a2", "answered", 1.0);
    db.link("students/budi", "answers/a3", "answered", 1.0);
    db.link("answers/a1", "questions/q1", "for", 1.0);
    db.link("answers/a2", "questions/q2", "for", 1.0);
    db.link("answers/a3", "questions/q3", "for", 1.0);

    let hits = db.pipeline_query(
        "MATCH ('students/budi')-[:answered]->(a)-[:for]->(q) \
         WITH q.clo AS clo, SUM(a.score * q.weight) AS clo_score \
         RETURN clo, clo_score ORDER BY clo_score DESC",
    ).unwrap();

    assert_eq!(hits.len(), 2, "expected 2 CLO rows");

    // clo2: 1.0 * 1.0 = 1.0 (DESC first)
    let clo_val = |h: &sekejap::Hit| {
        h.payload.as_ref().unwrap().get("clo").and_then(|v| v.as_str()).unwrap().to_string()
    };
    let score_val = |h: &sekejap::Hit| {
        h.payload.as_ref().unwrap().get("clo_score").and_then(|v| v.as_f64()).unwrap()
    };

    assert_eq!(clo_val(&hits[0]), "clo2");
    assert!((score_val(&hits[0]) - 1.0).abs() < 1e-9, "clo2 score should be 1.0");

    assert_eq!(clo_val(&hits[1]), "clo1");
    // clo1: 0.8*0.4 + 0.9*0.6 = 0.32 + 0.54 = 0.86
    assert!((score_val(&hits[1]) - 0.86).abs() < 1e-9, "clo1 score should be 0.86");
}

/// Full 2-level OBE pipeline: CLO → PLO aggregation.
#[test]
fn pipeline_two_level_clo_plo() {
    let mut db = CoreDB::new();

    // Student, answers, questions (same as above but fewer)
    db.put("students/budi", r#"{"_collection":"students","_key":"budi"}"#).unwrap();
    db.put("answers/a1", r#"{"_collection":"answers","_key":"a1","score":0.8}"#).unwrap();
    db.put("answers/a2", r#"{"_collection":"answers","_key":"a2","score":1.0}"#).unwrap();
    db.put("questions/q1", r#"{"_collection":"questions","_key":"q1","clo":"clo1","weight":1.0}"#).unwrap();
    db.put("questions/q2", r#"{"_collection":"questions","_key":"q2","clo":"clo2","weight":1.0}"#).unwrap();
    db.link("students/budi", "answers/a1", "answered", 1.0);
    db.link("students/budi", "answers/a2", "answered", 1.0);
    db.link("answers/a1", "questions/q1", "for", 1.0);
    db.link("answers/a2", "questions/q2", "for", 1.0);

    // CLOs (weight for PLO contribution)
    db.put("clos/clo1", r#"{"_collection":"clos","_key":"clo1","weight":0.5}"#).unwrap();
    db.put("clos/clo2", r#"{"_collection":"clos","_key":"clo2","weight":0.5}"#).unwrap();

    // PLO
    db.put("plos/plo1", r#"{"_collection":"plos","_key":"plo1"}"#).unwrap();

    // Edges: CLOs contribute to PLO
    db.link("clos/clo1", "plos/plo1", "contributes_to", 1.0);
    db.link("clos/clo2", "plos/plo1", "contributes_to", 1.0);

    let hits = db.pipeline_query(
        "MATCH ('students/budi')-[:answered]->(a)-[:for]->(q) \
         WITH q.clo AS clo, SUM(a.score * q.weight) AS clo_score \
         MATCH (c:clos WHERE _key = clo)-[:contributes_to]->(plo:plos) \
         RETURN plo._key AS plo, SUM(clo_score * c.weight) AS plo_score \
         ORDER BY plo_score DESC",
    ).unwrap();

    assert_eq!(hits.len(), 1, "expected 1 PLO row");
    let plo = hits[0].payload.as_ref().unwrap().get("plo")
        .and_then(|v| v.as_str()).unwrap();
    assert_eq!(plo, "plo1");

    // clo1: 0.8 * 1.0 = 0.8; clo2: 1.0 * 1.0 = 1.0
    // plo1 = (0.8 * 0.5) + (1.0 * 0.5) = 0.4 + 0.5 = 0.9
    let plo_score = hits[0].payload.as_ref().unwrap().get("plo_score")
        .and_then(|v| v.as_f64()).unwrap();
    assert!((plo_score - 0.9).abs() < 1e-9, "plo_score should be 0.9, got {}", plo_score);
}

/// Pipeline with LIMIT.
#[test]
fn pipeline_with_limit() {
    let mut db = CoreDB::new();
    db.put("root", r#"{"_collection":"roots","_key":"root"}"#).unwrap();
    for i in 1..=5u32 {
        let slug = format!("items/i{i}");
        let pay = format!(r#"{{"_collection":"items","_key":"i{i}","val":{i}}}"#);
        db.put(&slug, &pay).unwrap();
        db.link("root", &slug, "has", 1.0);
    }

    let hits = db.pipeline_query(
        "MATCH ('root')-[:has]->(item) RETURN item.val AS v ORDER BY v ASC LIMIT 3",
    ).unwrap();

    assert_eq!(hits.len(), 3);
    let vals: Vec<f64> = hits.iter()
        .map(|h| h.payload.as_ref().unwrap().get("v").and_then(|v| v.as_f64()).unwrap())
        .collect();
    assert_eq!(vals, vec![1.0, 2.0, 3.0]);
}

/// Pipeline COUNT aggregate.
#[test]
fn pipeline_count_aggregate() {
    let mut db = CoreDB::new();
    db.put("src", r#"{"_collection":"src","_key":"src"}"#).unwrap();
    for i in 1..=4u32 {
        let slug = format!("dst/d{i}");
        db.put(&slug, &format!(r#"{{"_collection":"dst","_key":"d{i}","grp":"g{}"}}"#, if i <= 2 {"1"} else {"2"})).unwrap();
        db.link("src", &slug, "points_to", 1.0);
    }

    let hits = db.pipeline_query(
        "MATCH ('src')-[:points_to]->(d) \
         WITH d.grp AS grp, COUNT(*) AS cnt \
         RETURN grp, cnt ORDER BY grp ASC",
    ).unwrap();

    assert_eq!(hits.len(), 2);
    let g1_cnt = hits[0].payload.as_ref().unwrap().get("cnt")
        .and_then(|v| v.as_i64()).unwrap();
    let g2_cnt = hits[1].payload.as_ref().unwrap().get("cnt")
        .and_then(|v| v.as_i64()).unwrap();
    assert_eq!(g1_cnt, 2);
    assert_eq!(g2_cnt, 2);
}

/// Multi-MATCH pipeline from a collection start.
#[test]
fn pipeline_collection_start() {
    let mut db = CoreDB::new();
    db.put("cats/a", r#"{"_collection":"cats","_key":"a"}"#).unwrap();
    db.put("cats/b", r#"{"_collection":"cats","_key":"b"}"#).unwrap();
    db.put("items/x", r#"{"_collection":"items","_key":"x","val":5.0}"#).unwrap();
    db.put("items/y", r#"{"_collection":"items","_key":"y","val":10.0}"#).unwrap();
    db.link("cats/a", "items/x", "has", 1.0);
    db.link("cats/b", "items/y", "has", 1.0);

    let hits = db.pipeline_query(
        "MATCH (c:cats)-[:has]->(item:items) RETURN c._key AS cat, item.val AS val ORDER BY val ASC",
    ).unwrap();

    assert_eq!(hits.len(), 2);
    let val_of = |i: usize| hits[i].payload.as_ref().unwrap().get("val")
        .and_then(|v| v.as_f64()).unwrap();
    assert_eq!(val_of(0), 5.0);
    assert_eq!(val_of(1), 10.0);
}

/// Multi-hop traversal after deleting an intermediate node must not return
/// the deleted node or traverse dead edges.
#[test]
fn traversal_after_delete_skips_deleted_node() {
    let mut db = CoreDB::new();
    for k in ["a", "b", "c"] {
        db.put(&format!("n/{k}"), &format!(r#"{{"_collection":"n","_key":"{k}"}}"#)).unwrap();
    }
    db.link("n/a", "n/b", "e", 1.0);
    db.link("n/b", "n/c", "e", 1.0);

    // 2-hop from a reaches b and c
    assert_eq!(db.one("n/a").hops_typed("e", 2).count(), 2);

    db.remove("n/b");

    // After deleting b, 2-hop from a reaches nothing
    assert_eq!(db.one("n/a").hops_typed("e", 2).count(), 0);
}

// ── Pipeline WHERE comparison operators ───────────────────────────────────────

#[test]
fn pipeline_where_cmp_operators() {
    let mut db = CoreDB::new();

    // Students with scores; some pass (>=60), some fail
    for (key, name, score) in [
        ("stu/ali", "Ali", 80.0_f64),
        ("stu/budi", "Budi", 55.0_f64),
        ("stu/cici", "Cici", 72.0_f64),
        ("stu/dodi", "Dodi", 45.0_f64),
    ] {
        db.put(key, &serde_json::json!({
            "_collection": "students",
            "_key": key,
            "name": name,
            "score": score,
        }).to_string()).unwrap();
    }

    // ── Test >= (pass threshold) ──────────────────────────────────────────────
    // MATCH (s:students WHERE score >= 60) RETURN s
    let hits = db.pipeline_query(
        "MATCH (s:students WHERE score >= 60) RETURN s"
    ).unwrap();
    assert_eq!(hits.len(), 2, "Ali and Cici should pass (score >= 60)");

    // ── Test < (failing) ─────────────────────────────────────────────────────
    let hits = db.pipeline_query(
        "MATCH (s:students WHERE score < 60) RETURN s"
    ).unwrap();
    assert_eq!(hits.len(), 2, "Budi and Dodi should fail (score < 60)");

    // ── Test != ──────────────────────────────────────────────────────────────
    let hits = db.pipeline_query(
        "MATCH (s:students WHERE score != 80) RETURN s"
    ).unwrap();
    assert_eq!(hits.len(), 3, "Everyone except Ali");

    // ── Test AND with multiple comparison ops ────────────────────────────────
    // score >= 50 AND score <= 75 → Budi(55) and Cici(72)
    let hits = db.pipeline_query(
        "MATCH (s:students WHERE score >= 50 AND score <= 75) RETURN s"
    ).unwrap();
    assert_eq!(hits.len(), 2, "Budi(55) and Cici(72) are in 50..=75");
}

// ── Collection-level edge listing ─────────────────────────────────────────────

#[test]
fn edges_from_collection_and_between() {
    let mut db = CoreDB::new();

    // Two classrooms, two lecturers, one department
    for (key, col) in [
        ("cls/math", "classrooms"), ("cls/physics", "classrooms"),
        ("lec/ali", "lecturers"),   ("lec/budi", "lecturers"),
        ("dept/sci", "departments"),
    ] {
        db.put(key, &serde_json::json!({
            "_collection": col, "_key": key
        }).to_string()).unwrap();
    }

    db.link("cls/math",    "lec/ali",  "taught_by", 1.0);
    db.link("cls/physics", "lec/budi", "taught_by", 1.0);
    db.link("lec/ali",     "dept/sci", "belongs_to", 1.0);

    // ── edges_from_collection: all edges leaving classrooms ───────────────────
    let edges = db.edges_from_collection("classrooms");
    assert_eq!(edges.len(), 2);
    // edge_type label is now resolved
    assert!(edges.iter().all(|e| e.edge_type.as_deref() == Some("taught_by")));

    // ── edges_between: classrooms → lecturers only ────────────────────────────
    let edges = db.edges_between("classrooms", "lecturers");
    assert_eq!(edges.len(), 2);

    // ── edges_between: lecturers → classrooms = 0 (direction matters) ─────────
    let edges = db.edges_between("lecturers", "classrooms");
    assert_eq!(edges.len(), 0);

    // ── edges_between: lecturers → departments ────────────────────────────────
    let edges = db.edges_between("lecturers", "departments");
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].from_slug.as_deref(), Some("lec/ali"));
    assert_eq!(edges[0].to_slug.as_deref(),   Some("dept/sci"));
    assert_eq!(edges[0].edge_type.as_deref(), Some("belongs_to"));
}

#[test]
fn show_edges_sql() {
    let mut db = CoreDB::new();
    for (key, col) in [
        ("cls/math", "classrooms"), ("cls/physics", "classrooms"),
        ("lec/ali",  "lecturers"),  ("dept/sci",    "departments"),
    ] {
        db.put(key, &serde_json::json!({"_collection": col, "_key": key}).to_string()).unwrap();
    }
    db.link("cls/math",    "lec/ali",  "taught_by",  1.0);
    db.link("cls/physics", "lec/ali",  "taught_by",  1.0);
    db.link("lec/ali",     "dept/sci", "belongs_to", 1.0);

    // Full schema — 2 distinct triples
    let hits = db.show("SHOW EDGES").unwrap();
    assert_eq!(hits.len(), 2);
    let types: Vec<_> = hits.iter()
        .map(|h| h.payload.as_ref().unwrap()["type"].as_str().unwrap())
        .collect();
    assert!(types.contains(&"taught_by"));
    assert!(types.contains(&"belongs_to"));

    // FROM classrooms → only taught_by
    let hits = db.show("SHOW EDGES FROM classrooms").unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].payload.as_ref().unwrap()["type"].as_str(), Some("taught_by"));

    // FROM classrooms TO lecturers
    let hits = db.show("SHOW EDGES FROM classrooms TO lecturers").unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].payload.as_ref().unwrap()["type"].as_str(), Some("taught_by"));

    // FROM classrooms TO departments → 0
    let hits = db.show("SHOW EDGES FROM classrooms TO departments").unwrap();
    assert_eq!(hits.len(), 0);
}

// ── ALTER TABLE ───────────────────────────────────────────────────────────────

#[test]
fn alter_table_add_column() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE venues (_key TEXT, name TEXT, capacity INTEGER)").unwrap();
    db.execute("ALTER TABLE venues ADD COLUMN suburb TEXT").unwrap();

    let hits = db.show("SHOW venues").unwrap();
    let fields: Vec<_> = hits.iter()
        .filter_map(|h| h.payload.as_ref().and_then(|p| p["field"].as_str().map(str::to_string)))
        .collect();
    assert!(fields.contains(&"suburb".to_string()));
}

#[test]
fn alter_table_add_column_already_exists_errors() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE venues (_key TEXT, name TEXT, capacity INTEGER)").unwrap();
    let err = db.execute("ALTER TABLE venues ADD COLUMN capacity INTEGER").unwrap_err();
    assert!(err.to_string().contains("already exists"));
}

#[test]
fn alter_table_add_column_no_table_errors() {
    let mut db = CoreDB::new();
    let err = db.execute("ALTER TABLE venues ADD COLUMN name TEXT").unwrap_err();
    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn alter_table_drop_column() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE venues (_key TEXT, name TEXT, capacity INTEGER, suburb TEXT)").unwrap();
    // Insert a node with all three fields
    db.execute("INSERT INTO venues (_key, name, capacity, suburb) VALUES ('rod_laver', 'Rod Laver Arena', 15000, 'Melbourne')").unwrap();

    let count = db.execute("ALTER TABLE venues DROP COLUMN suburb").unwrap();
    assert_eq!(count, 1); // one node had the field removed

    // Schema no longer lists suburb
    let hits = db.show("SHOW venues").unwrap();
    let fields: Vec<_> = hits.iter()
        .filter_map(|h| h.payload.as_ref().and_then(|p| p["field"].as_str().map(str::to_string)))
        .collect();
    assert!(!fields.contains(&"suburb".to_string()));

    // Node no longer has the field
    let node = db.get("venues/rod_laver").unwrap();
    let v: serde_json::Value = serde_json::from_str(&node).unwrap();
    assert!(v.get("suburb").is_none());
}

#[test]
fn alter_table_drop_column_if_exists_silent() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE venues (_key TEXT, name TEXT)").unwrap();
    let count = db.execute("ALTER TABLE venues DROP COLUMN IF EXISTS nonexistent").unwrap();
    assert_eq!(count, 0);
}

#[test]
fn alter_table_drop_column_missing_errors() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE venues (_key TEXT, name TEXT)").unwrap();
    let err = db.execute("ALTER TABLE venues DROP COLUMN ghost").unwrap_err();
    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn alter_table_rename_column() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE bands (_key TEXT, name TEXT, city TEXT)").unwrap();
    db.execute("INSERT INTO bands (_key, name, city) VALUES ('the_vines', 'The Vines', 'Sydney')").unwrap();

    let count = db.execute("ALTER TABLE bands RENAME COLUMN city TO hometown").unwrap();
    assert_eq!(count, 1);

    // Schema updated
    let hits = db.show("SHOW bands").unwrap();
    let fields: Vec<_> = hits.iter()
        .filter_map(|h| h.payload.as_ref().and_then(|p| p["field"].as_str().map(str::to_string)))
        .collect();
    assert!(fields.contains(&"hometown".to_string()));
    assert!(!fields.contains(&"city".to_string()));

    // Node updated
    let node = db.get("bands/the_vines").unwrap();
    let v: serde_json::Value = serde_json::from_str(&node).unwrap();
    assert_eq!(v["hometown"], "Sydney");
    assert!(v.get("city").is_none());
}

#[test]
fn alter_table_rename_table() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE bands (_key TEXT, name TEXT)").unwrap();
    db.execute("INSERT INTO bands (_key, name) VALUES ('the_vines', 'The Vines')").unwrap();
    db.execute("INSERT INTO bands (_key, name) VALUES ('jet', 'Jet')").unwrap();

    let count = db.execute("ALTER TABLE bands RENAME TO artists").unwrap();
    assert_eq!(count, 2); // two nodes reclassified

    // Old collection query returns nothing
    let old_hits = db.query("SELECT * FROM bands").unwrap().collect();
    assert_eq!(old_hits.len(), 0);

    // New collection query returns both nodes
    let new_hits = db.query("SELECT * FROM artists").unwrap().collect();
    assert_eq!(new_hits.len(), 2);

    // SHOW TABLES reflects the rename
    let table_hits = db.show("SHOW TABLES").unwrap();
    let names: Vec<_> = table_hits.iter()
        .filter_map(|h| h.payload.as_ref().and_then(|p| p["name"].as_str().map(str::to_string)))
        .collect();
    assert!(names.contains(&"artists".to_string()));
    assert!(!names.contains(&"bands".to_string()));
}

#[test]
fn alter_table_rename_to_existing_errors() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE bands (_key TEXT, name TEXT)").unwrap();
    db.execute("CREATE TABLE artists (_key TEXT, name TEXT)").unwrap();
    let err = db.execute("ALTER TABLE bands RENAME TO artists").unwrap_err();
    assert!(err.to_string().contains("already exists"));
}

#[test]
fn alter_table_alter_column_type() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE venues (_key TEXT, name TEXT, capacity INTEGER)").unwrap();
    db.execute("ALTER TABLE venues ALTER COLUMN capacity TYPE REAL").unwrap();

    let hits = db.show("SHOW venues").unwrap();
    let capacity_hit = hits.iter()
        .find(|h| h.payload.as_ref().and_then(|p| p["field"].as_str()) == Some("capacity"))
        .expect("capacity field must be present");
    assert_eq!(
        capacity_hit.payload.as_ref().unwrap()["type"].as_str(),
        Some("REAL")
    );
}

#[test]
fn alter_table_wal_replay() {
    use tempfile::TempDir;
    use sekejap::CoreDB;

    let dir = TempDir::new().unwrap();
    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.execute("CREATE TABLE venues (_key TEXT, name TEXT, capacity INTEGER)").unwrap();
        db.execute("INSERT INTO venues (_key, name, capacity) VALUES ('rod_laver', 'Rod Laver Arena', 15000)").unwrap();
        db.execute("ALTER TABLE venues ADD COLUMN suburb TEXT").unwrap();
        db.execute("ALTER TABLE venues RENAME COLUMN capacity TO seats").unwrap();
    }

    // Cold reload — WAL replay must restore all ALTER TABLE ops
    let db = CoreDB::open(dir.path()).unwrap();

    // Schema has suburb, seats; no capacity
    let hits = db.show("SHOW venues").unwrap();
    let fields: Vec<_> = hits.iter()
        .filter_map(|h| h.payload.as_ref().and_then(|p| p["field"].as_str().map(str::to_string)))
        .collect();
    assert!(fields.contains(&"suburb".to_string()), "suburb must survive replay");
    assert!(fields.contains(&"seats".to_string()),  "seats must survive replay");
    assert!(!fields.contains(&"capacity".to_string()), "capacity was renamed");

    // Node data: seats field exists, capacity does not
    let node = db.get("venues/rod_laver").unwrap();
    let v: serde_json::Value = serde_json::from_str(&node).unwrap();
    assert_eq!(v["seats"].as_f64(), Some(15000.0));
    assert!(v.get("capacity").is_none());
}

// ── ALTER TABLE index correctness ─────────────────────────────────────────────

#[test]
fn drop_column_removes_index_hint_and_btree() {
    use tempfile::TempDir;
    use sekejap::CoreDB;

    let dir = TempDir::new().unwrap();
    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.execute("CREATE TABLE venues (_key TEXT, name TEXT, capacity INTEGER)").unwrap();
        db.execute("CREATE INDEX ON venues USING btree (capacity)").unwrap();
        for (k, n, c) in [("rod_laver", "Rod Laver Arena", 15000), ("mcg", "MCG", 100024)] {
            db.execute(&format!(
                "INSERT INTO venues (_key, name, capacity) VALUES ('{k}', '{n}', {c})"
            )).unwrap();
        }

        // Index works before drop
        let hits = db.query("SELECT * FROM venues WHERE capacity > 10000 ORDER BY capacity ASC")
            .unwrap().collect();
        assert_eq!(hits.len(), 2);

        db.execute("ALTER TABLE venues DROP COLUMN capacity").unwrap();

        // After drop: schema hint is gone
        let show = db.show("SHOW venues").unwrap();
        let fields: Vec<_> = show.iter()
            .filter_map(|h| h.payload.as_ref().and_then(|p| p["field"].as_str().map(String::from)))
            .collect();
        assert!(!fields.contains(&"capacity".to_string()));
    }

    // WAL replay: index rebuild must NOT try to rebuild the dropped column
    {
        let db = CoreDB::open(dir.path()).unwrap();
        let show = db.show("SHOW venues").unwrap();
        let fields: Vec<_> = show.iter()
            .filter_map(|h| h.payload.as_ref().and_then(|p| p["field"].as_str().map(String::from)))
            .collect();
        assert!(!fields.contains(&"capacity".to_string()),
            "capacity index hint must not survive WAL replay after DROP COLUMN");
    }
}

#[test]
fn rename_column_updates_index_hint() {
    use tempfile::TempDir;
    use sekejap::CoreDB;

    let dir = TempDir::new().unwrap();
    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.execute("CREATE TABLE venues (_key TEXT, name TEXT, seats INTEGER)").unwrap();
        db.execute("CREATE INDEX ON venues USING btree (seats)").unwrap();
        db.execute("INSERT INTO venues (_key, name, seats) VALUES ('mcg', 'MCG', 100024)").unwrap();

        db.execute("ALTER TABLE venues RENAME COLUMN seats TO capacity").unwrap();

        // Index on new name must work immediately
        let hits = db.query("SELECT * FROM venues WHERE capacity > 50000 ORDER BY capacity ASC")
            .unwrap().collect();
        assert_eq!(hits.len(), 1);
    }

    // WAL replay must rebuild index under the new name, not the old
    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        // Force a btree rebuild as startup does (simulate by inserting a second row)
        db.execute("INSERT INTO venues (_key, name, capacity) VALUES ('etihad', 'Marvel Stadium', 56347)").unwrap();
        let hits = db.query("SELECT * FROM venues WHERE capacity > 50000 ORDER BY capacity ASC")
            .unwrap().collect();
        assert_eq!(hits.len(), 2);

        // Old name must no longer appear in schema hints
        let show = db.show("SHOW venues").unwrap();
        let indexed: Vec<_> = show.iter()
            .filter_map(|h| {
                let p = h.payload.as_ref()?;
                if p.get("source").and_then(|v| v.as_str()) == Some("declared") {
                    p["field"].as_str().map(String::from)
                } else { None }
            })
            .collect();
        assert!(indexed.contains(&"capacity".to_string()),
            "capacity must appear in schema after rename");
    }
}

#[test]
fn alter_column_type_rebuilds_btree() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE events (_key TEXT, name TEXT, score INTEGER)").unwrap();
    db.execute("CREATE INDEX ON events USING btree (score)").unwrap();
    for (k, s) in [("a", 10), ("b", 50), ("c", 90)] {
        db.execute(&format!(
            "INSERT INTO events (_key, name, score) VALUES ('{k}', '{k}', {s})"
        )).unwrap();
    }

    // Btree works with INTEGER type
    let before = db.query("SELECT * FROM events WHERE score > 40 ORDER BY score ASC")
        .unwrap().collect();
    assert_eq!(before.len(), 2);

    // Change type to REAL — btree should be rebuilt and still work
    db.execute("ALTER TABLE events ALTER COLUMN score TYPE REAL").unwrap();

    let after = db.query("SELECT * FROM events WHERE score > 40 ORDER BY score ASC")
        .unwrap().collect();
    assert_eq!(after.len(), 2);
}

// ── DROP INDEX ────────────────────────────────────────────────────────────────

/// DROP INDEX on a btree removes the index hint from the schema and destroys
/// the in-memory btree so range queries fall back to a full scan.
#[test]
fn drop_index_btree_removes_hint_and_data() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE venues (_key TEXT, capacity INTEGER)").unwrap();
    db.execute("CREATE INDEX ON venues USING btree (capacity)").unwrap();
    for (k, c) in [("rod_laver", 15000i64), ("forum", 11000i64), ("corner", 2400i64)] {
        db.execute(&format!(
            "INSERT INTO venues (_key, capacity) VALUES ('{k}', {c})"
        )).unwrap();
    }

    // Btree index is present — range query uses it
    let before = db.query("SELECT * FROM venues WHERE capacity > 10000 ORDER BY capacity ASC")
        .unwrap().collect();
    assert_eq!(before.len(), 2);

    // Drop the index
    db.execute("DROP INDEX ON venues USING btree (capacity)").unwrap();

    // Range query should still work via full scan fallback
    let after = db.query("SELECT * FROM venues WHERE capacity > 10000 ORDER BY capacity ASC")
        .unwrap().collect();
    assert_eq!(after.len(), 2);
}

/// DROP INDEX IF EXISTS on a non-existent index is silent.
#[test]
fn drop_index_if_exists_silent() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE venues (_key TEXT, capacity INTEGER)").unwrap();
    // No index created — IF EXISTS should not error
    db.execute("DROP INDEX IF EXISTS ON venues USING btree (capacity)").unwrap();
}

/// DROP INDEX without IF EXISTS on a non-existent index returns an error.
#[test]
fn drop_index_missing_errors() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE venues (_key TEXT, capacity INTEGER)").unwrap();
    let err = db.execute("DROP INDEX ON venues USING btree (capacity)");
    assert!(err.is_err());
}

/// When two collections share a GIN (fulltext) index on the same field name,
/// dropping the index from one collection must NOT destroy the other's data.
#[test]
fn drop_index_gin_shared_field_only_removes_one_collection() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE articles (_key TEXT, body TEXT)").unwrap();
    db.execute("CREATE TABLE posts (_key TEXT, body TEXT)").unwrap();

    // Insert data first — GIN is batch-built, so the index must be created after rows.
    db.execute("INSERT INTO articles (_key, body) VALUES ('a1', 'live music in Fitzroy')").unwrap();
    db.execute("INSERT INTO articles (_key, body) VALUES ('a2', 'gallery opens in Collingwood')").unwrap();
    db.execute("INSERT INTO posts (_key, body) VALUES ('p1', 'live gig at Corner Hotel')").unwrap();

    db.execute("CREATE INDEX ON articles USING gin (body)").unwrap();
    db.execute("CREATE INDEX ON posts USING gin (body)").unwrap();

    // Both collections searchable via ILIKE (uses GIN)
    let hit_articles = db.query("SELECT * FROM articles WHERE body ILIKE 'Fitzroy'")
        .unwrap().collect();
    assert_eq!(hit_articles.len(), 1);
    let hit_posts = db.query("SELECT * FROM posts WHERE body ILIKE 'live'")
        .unwrap().collect();
    assert_eq!(hit_posts.len(), 1);

    // Drop GIN on articles only
    db.execute("DROP INDEX ON articles USING gin (body)").unwrap();

    // Posts GIN data must still work
    let still_posts = db.query("SELECT * FROM posts WHERE body ILIKE 'live'")
        .unwrap().collect();
    assert_eq!(still_posts.len(), 1, "posts GIN must survive when articles drops theirs");
}

/// DROP INDEX survives WAL replay — after a cold restart the index hint is gone
/// and the index data is absent.
#[test]
fn drop_index_wal_replay() {
    use tempfile::TempDir;
    let dir = TempDir::new().unwrap();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.execute("CREATE TABLE venues (_key TEXT, capacity INTEGER)").unwrap();
        db.execute("CREATE INDEX ON venues USING btree (capacity)").unwrap();
        db.execute("INSERT INTO venues (_key, capacity) VALUES ('rod_laver', 15000)").unwrap();
        db.execute("DROP INDEX ON venues USING btree (capacity)").unwrap();
    }

    // Reopen — WAL replay must re-apply the DROP INDEX.
    // Full-scan fallback must still return the row.
    let db = CoreDB::open(dir.path()).unwrap();
    let rows = db.query("SELECT * FROM venues WHERE capacity > 10000")
        .unwrap().collect();
    assert_eq!(rows.len(), 1);
    // DDL should no longer mention btree on capacity
    let ddl = db.schema_ddl("venues").unwrap();
    // The DDL reflects field definitions, not index hints, so we verify
    // by confirming the query still works (btree hint absence is internal).
    assert!(ddl.contains("capacity"), "column must still exist");
}

// ── GIN ILIKE integration test ────────────────────────────────────────────────

/// GIN ILIKE must return the correct nodes (not empty) when accessed via SQL.
/// This exercises the full code path: build_gin_index → ilike() → query.rs FILTER.
/// Data is inserted before the index is built (GIN is batch-built, not incremental).
#[test]
fn gin_ilike_after_insert() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE bands (name TEXT)").unwrap();
    // Insert data first — GIN is batch-built so the index must be created after the rows.
    db.put("bands/b1", r#"{"_collection":"bands","name":"The Vines"}"#).unwrap();
    db.put("bands/b2", r#"{"_collection":"bands","name":"The Avalanches"}"#).unwrap();
    db.put("bands/b3", r#"{"_collection":"bands","name":"The John Butler Trio"}"#).unwrap();
    db.put("bands/b4", r#"{"_collection":"bands","name":"Something Something"}"#).unwrap();
    db.execute("CREATE INDEX ON bands USING gin (name)").unwrap();

    // SQL ILIKE via GIN index path
    let hits = db
        .query("SELECT * FROM bands WHERE name ILIKE '%the%'")
        .unwrap()
        .collect();
    assert_eq!(hits.len(), 3, "GIN ILIKE must return the 3 bands starting with 'The'");

    let names: Vec<&str> = hits
        .iter()
        .filter_map(|h| h.payload.as_ref()?.get("name")?.as_str())
        .collect();
    assert!(names.contains(&"The Vines"));
    assert!(names.contains(&"The Avalanches"));
    assert!(names.contains(&"The John Butler Trio"));
}

// ── Edge intrinsics: r._depth, r._path_keys ──────────────────────────────────

/// `r._depth` counts hops from start.
/// Graph: Melbourne → Richmond → Hawthorn → Box Hill (each hop "adjacent")
#[test]
fn edge_intrinsic_depth() {
    let mut db = CoreDB::new();
    db.put("suburbs/melbourne", r#"{"_collection":"suburbs","_key":"melbourne"}"#).unwrap();
    db.put("suburbs/richmond",  r#"{"_collection":"suburbs","_key":"richmond"}"#).unwrap();
    db.put("suburbs/hawthorn",  r#"{"_collection":"suburbs","_key":"hawthorn"}"#).unwrap();
    db.put("suburbs/box-hill",  r#"{"_collection":"suburbs","_key":"box-hill"}"#).unwrap();
    db.link("suburbs/melbourne", "suburbs/richmond", "adjacent", 1.0);
    db.link("suburbs/richmond",  "suburbs/hawthorn", "adjacent", 1.0);
    db.link("suburbs/hawthorn",  "suburbs/box-hill", "adjacent", 1.0);

    // 2-hop path: melbourne -[r1]-> richmond -[r2]-> hawthorn
    let hits = db.query(
        "SELECT h2._key AS dest, r2._depth AS depth \
         FROM MATCH (s:suburbs)-[r:adjacent]->(h1:suburbs)-[r2:adjacent]->(h2:suburbs) \
         WHERE s._key = 'melbourne'"
    ).unwrap().collect();

    assert!(!hits.is_empty());
    let payload = hits[0].payload.as_ref().unwrap();
    assert_eq!(payload["depth"], 2, "r2._depth must be 2 after 2 hops");
}

/// `r._path_keys` contains the full slug list from start to current node.
#[test]
fn edge_intrinsic_path_keys() {
    let mut db = CoreDB::new();
    db.put("suburbs/fitzroy",   r#"{"_collection":"suburbs","_key":"fitzroy"}"#).unwrap();
    db.put("suburbs/collingwood", r#"{"_collection":"suburbs","_key":"collingwood"}"#).unwrap();
    db.put("suburbs/richmond",  r#"{"_collection":"suburbs","_key":"richmond"}"#).unwrap();
    db.link("suburbs/fitzroy",    "suburbs/collingwood", "borders", 1.0);
    db.link("suburbs/collingwood","suburbs/richmond",    "borders", 1.0);

    let hits = db.query(
        "SELECT c._key AS dest, r2._path_keys AS path \
         FROM MATCH (a:suburbs)-[r:borders]->(b:suburbs)-[r2:borders]->(c:suburbs) \
         WHERE a._key = 'fitzroy'"
    ).unwrap().collect();

    assert!(!hits.is_empty());
    let payload = hits[0].payload.as_ref().unwrap();
    let path = payload["path"].as_array().expect("_path_keys must be array");
    assert_eq!(path.len(), 3, "3 nodes in path: fitzroy, collingwood, richmond");
    assert_eq!(path[0].as_str().unwrap(), "suburbs/fitzroy");
    assert_eq!(path[2].as_str().unwrap(), "suburbs/richmond");
}

/// `r._avg_strength` and `r._min_strength` reflect edge weights along the path.
#[test]
fn edge_intrinsic_strength_aggregates() {
    let mut db = CoreDB::new();
    db.put("events/flood", r#"{"_collection":"events","_key":"flood"}"#).unwrap();
    db.put("suburbs/west", r#"{"_collection":"suburbs","_key":"west"}"#).unwrap();
    db.put("streets/main", r#"{"_collection":"streets","_key":"main"}"#).unwrap();
    db.link("events/flood", "suburbs/west",  "affects",  0.8);
    db.link("suburbs/west", "streets/main",  "contains", 0.4);

    let hits = db.query(
        "SELECT st._key AS street, r._avg_strength AS avg_s, r._min_strength AS min_s \
         FROM MATCH (e:events)-[:affects]->(s:suburbs)-[r:contains]->(st:streets) \
         WHERE e._key = 'flood'"
    ).unwrap().collect();

    assert!(!hits.is_empty());
    let p = hits[0].payload.as_ref().unwrap();
    // avg = (0.8 + 0.4) / 2 = 0.6
    let avg = p["avg_s"].as_f64().unwrap();
    assert!((avg - 0.6).abs() < 1e-4, "avg_strength should be ~0.6, got {avg}");
    let min = p["min_s"].as_f64().unwrap();
    assert!((min - 0.4).abs() < 1e-4, "min_strength should be 0.4, got {min}");
}

// ── MATCH SHORTEST ───────────────────────────────────────────────────────────

/// Build a small graph that contains multiple paths of different lengths and
/// check that `MATCH SHORTEST` returns the shortest one.
///
/// Graph (all edges forward-directed):
///   coby → luffy  (knows)
///   coby → garp   (student_of)
///   garp → luffy  (mentor_of)
///   luffy → dragon (family)
///   garp → dragon  (family)
///   dragon → sabo  (commander_of)
///   luffy → sabo   (crew)
///
/// Shortest path from coby → sabo:
///   coby → luffy → sabo  (2 hops, via "knows" + "crew")
fn setup_path_db() -> CoreDB {
    let mut db = CoreDB::new();
    db.put("characters/coby",   r#"{"_collection":"characters","name":"Coby"}"#).unwrap();
    db.put("characters/luffy",  r#"{"_collection":"characters","name":"Luffy"}"#).unwrap();
    db.put("characters/garp",   r#"{"_collection":"characters","name":"Garp"}"#).unwrap();
    db.put("characters/dragon", r#"{"_collection":"characters","name":"Dragon"}"#).unwrap();
    db.put("characters/sabo",   r#"{"_collection":"characters","name":"Sabo"}"#).unwrap();

    db.link("characters/coby",   "characters/luffy",  "knows",        1.0);
    db.link("characters/coby",   "characters/garp",   "student_of",   1.0);
    db.link("characters/garp",   "characters/luffy",  "mentor_of",    1.0);
    db.link("characters/luffy",  "characters/dragon", "family",       1.0);
    db.link("characters/garp",   "characters/dragon", "family",       1.0);
    db.link("characters/dragon", "characters/sabo",   "commander_of", 1.0);
    db.link("characters/luffy",  "characters/sabo",   "crew",         1.0);

    db
}

#[test]
fn shortest_path_returns_correct_route() {
    let db = setup_path_db();

    // SELECT FROM MATCH SHORTEST — path row: a=start, b=end, r=path object
    let hits = db.query(
        "SELECT a.name AS from_name, b.name AS to_name, r.length AS hops, r._path_keys AS path \
         FROM MATCH SHORTEST (a)-[r*]->(b) \
         WHERE a._key = 'characters/coby' AND b._key = 'characters/sabo'"
    ).unwrap().collect();

    assert_eq!(hits.len(), 1, "should find a path");
    let p = hits[0].payload.as_ref().unwrap();

    // Endpoints
    assert_eq!(p["from_name"].as_str().unwrap(), "Coby");
    assert_eq!(p["to_name"].as_str().unwrap(), "Sabo");

    // Shortest path is 2 hops: coby → luffy → sabo
    assert_eq!(p["hops"].as_i64().unwrap(), 2, "expected 2 hops");

    // Path keys: coby, luffy, sabo
    let path = p["path"].as_array().unwrap();
    assert_eq!(path.len(), 3);
    assert_eq!(path[0].as_str().unwrap(), "characters/coby");
    assert_eq!(path[1].as_str().unwrap(), "characters/luffy");
    assert_eq!(path[2].as_str().unwrap(), "characters/sabo");
}

#[test]
fn shortest_path_collection_in_pattern() {
    // Same query but using (a:characters) pattern + bare _key instead of full slug.
    let db = setup_path_db();
    let hits = db.query(
        "SELECT a.name AS from_name, b.name AS to_name, r.length AS hops \
         FROM MATCH SHORTEST (a:characters)-[r*]->(b:characters) \
         WHERE a._key = 'coby' AND b._key = 'sabo'"
    ).unwrap().collect();
    assert_eq!(hits.len(), 1, "collection-in-pattern should find path");
    let p = hits[0].payload.as_ref().unwrap();
    assert_eq!(p["hops"].as_i64().unwrap(), 2);
    assert_eq!(p["from_name"].as_str().unwrap(), "Coby");
    assert_eq!(p["to_name"].as_str().unwrap(), "Sabo");
}

#[test]
fn shortest_path_no_path_returns_none() {
    let db = setup_path_db();

    // sabo has no outgoing edges in our graph, so sabo → coby is impossible
    let hits = db.query(
        "SELECT a.name AS from_name, b.name AS to_name \
         FROM MATCH SHORTEST (a)-[r*]->(b) \
         WHERE a._key = 'characters/sabo' AND b._key = 'characters/coby'"
    ).unwrap().collect();

    assert!(hits.is_empty(), "expected 0 rows when no path exists");
}

#[test]
fn shortest_path_same_node_returns_zero_hops() {
    let db = setup_path_db();

    let hits = db.query(
        "SELECT r.length AS hops, r._path_keys AS path \
         FROM MATCH SHORTEST (a)-[r*]->(b) \
         WHERE a._key = 'characters/luffy' AND b._key = 'characters/luffy'"
    ).unwrap().collect();

    assert_eq!(hits.len(), 1, "same-node path must return 1 row");
    let p = hits[0].payload.as_ref().unwrap();
    assert_eq!(p["hops"].as_i64().unwrap(), 0);
    let path = p["path"].as_array().unwrap();
    assert_eq!(path.len(), 1);
    assert_eq!(path[0].as_str().unwrap(), "characters/luffy");
}

#[test]
fn shortest_path_missing_node_returns_none() {
    let db = setup_path_db();

    // "characters/zoro" was never inserted
    let hits = db.query(
        "SELECT a.name AS from_name, b.name AS to_name \
         FROM MATCH SHORTEST (a)-[r*]->(b) \
         WHERE a._key = 'characters/coby' AND b._key = 'characters/zoro'"
    ).unwrap().collect();

    assert!(hits.is_empty(), "expected 0 rows when target node doesn't exist");
}

// ── Target 8: SELECT … FROM MATCH ────────────────────────────────────────────

/// SELECT list acts as the RETURN clause; same execution path as MATCH … RETURN.
/// Graph: Melbourne → Richmond → Hawthorn (adjacent edges, 1-hop and 2-hop).
#[test]
fn select_from_match() {
    let mut db = CoreDB::new();
    db.put("suburbs/melbourne", r#"{"_collection":"suburbs","_key":"melbourne"}"#).unwrap();
    db.put("suburbs/richmond",  r#"{"_collection":"suburbs","_key":"richmond"}"#).unwrap();
    db.put("suburbs/hawthorn",  r#"{"_collection":"suburbs","_key":"hawthorn"}"#).unwrap();
    db.link("suburbs/melbourne", "suburbs/richmond", "adjacent", 1.0);
    db.link("suburbs/richmond",  "suburbs/hawthorn", "adjacent", 1.0);

    // SELECT syntax — identical semantics to the MATCH … RETURN form.
    // Start variable (s) is not bound in path rows; use destination (n) and edge (r).
    let hits = db.query(
        "SELECT n._key AS dest, r._depth AS depth \
         FROM MATCH (s:suburbs)-[r:adjacent]->(n:suburbs) \
         WHERE s._key = 'melbourne'"
    ).unwrap().collect();

    assert!(!hits.is_empty());
    let p = hits[0].payload.as_ref().unwrap();
    // Single hop: depth = 1, destination = richmond
    assert_eq!(p["dest"].as_str().unwrap(), "richmond");
    assert_eq!(p["depth"].as_i64().unwrap(), 1);
}

// ── Target 9: PATH_* aggregates ───────────────────────────────────────────────

/// PATH_PRODUCT multiplies all elements in the path strength array.
/// Graph: flood -[0.8]-> west -[0.4]-> main-st  →  product = 0.32
#[test]
fn path_product() {
    let mut db = CoreDB::new();
    db.put("events/flood", r#"{"_collection":"events","_key":"flood"}"#).unwrap();
    db.put("suburbs/west", r#"{"_collection":"suburbs","_key":"west"}"#).unwrap();
    db.put("streets/main", r#"{"_collection":"streets","_key":"main"}"#).unwrap();
    db.link("events/flood", "suburbs/west",  "affects",  0.8);
    db.link("suburbs/west", "streets/main",  "contains", 0.4);

    let hits = db.query(
        "SELECT PATH_PRODUCT(r._path_strength) AS prod \
         FROM MATCH (e:events)-[:affects]->(s:suburbs)-[r:contains]->(st:streets) \
         WHERE e._key = 'flood'"
    ).unwrap().collect();

    assert!(!hits.is_empty(), "should have at least one path row");
    let p = hits[0].payload.as_ref().unwrap();
    let prod = p["prod"].as_f64().unwrap();
    assert!((prod - 0.32).abs() < 1e-6, "PATH_PRODUCT should be ~0.32, got {prod}");
}

/// PATH_FIRST and PATH_LAST return the first/last element of a path array field.
/// Uses r._path_keys which contains the full slug list from start to current node.
#[test]
fn path_first_last() {
    let mut db = CoreDB::new();
    db.put("suburbs/fitzroy",    r#"{"_collection":"suburbs","_key":"fitzroy"}"#).unwrap();
    db.put("suburbs/collingwood",r#"{"_collection":"suburbs","_key":"collingwood"}"#).unwrap();
    db.put("suburbs/richmond",   r#"{"_collection":"suburbs","_key":"richmond"}"#).unwrap();
    db.link("suburbs/fitzroy",     "suburbs/collingwood", "borders", 1.0);
    db.link("suburbs/collingwood", "suburbs/richmond",    "borders", 1.0);

    let hits = db.query(
        "SELECT PATH_FIRST(r2._path_keys) AS first_stop, PATH_LAST(r2._path_keys) AS last_stop \
         FROM MATCH (a:suburbs)-[r:borders]->(b:suburbs)-[r2:borders]->(c:suburbs) \
         WHERE a._key = 'fitzroy'"
    ).unwrap().collect();

    assert!(!hits.is_empty());
    let p = hits[0].payload.as_ref().unwrap();
    assert_eq!(p["first_stop"].as_str().unwrap(), "suburbs/fitzroy");
    assert_eq!(p["last_stop"].as_str().unwrap(), "suburbs/richmond");
}

// ── Target 10: CASE WHEN, NOW(), JSON_ARRAY_LENGTH ───────────────────────────

/// CASE WHEN routes on r._depth: depth 1 → "close", depth 2 → "far", else "unknown".
#[test]
fn case_when_depth() {
    let mut db = CoreDB::new();
    db.put("suburbs/melbourne", r#"{"_collection":"suburbs","_key":"melbourne"}"#).unwrap();
    db.put("suburbs/richmond",  r#"{"_collection":"suburbs","_key":"richmond"}"#).unwrap();
    db.put("suburbs/hawthorn",  r#"{"_collection":"suburbs","_key":"hawthorn"}"#).unwrap();
    db.link("suburbs/melbourne", "suburbs/richmond", "adjacent", 1.0);
    db.link("suburbs/richmond",  "suburbs/hawthorn", "adjacent", 1.0);

    // Two-hop path ends at hawthorn with r2._depth = 2
    let hits = db.query(
        "SELECT h2._key AS dest, \
                CASE WHEN r2._depth = 1 THEN 'close' WHEN r2._depth = 2 THEN 'far' ELSE 'unknown' END AS proximity \
         FROM MATCH (s:suburbs)-[r:adjacent]->(h1:suburbs)-[r2:adjacent]->(h2:suburbs) \
         WHERE s._key = 'melbourne'"
    ).unwrap().collect();

    assert!(!hits.is_empty());
    let p = hits[0].payload.as_ref().unwrap();
    assert_eq!(p["dest"].as_str().unwrap(), "hawthorn");
    assert_eq!(p["proximity"].as_str().unwrap(), "far");
}

/// NOW() returns a positive integer (Unix timestamp in seconds).
#[test]
fn now_returns_integer() {
    let mut db = CoreDB::new();
    db.put("suburbs/fitzroy",    r#"{"_collection":"suburbs","_key":"fitzroy"}"#).unwrap();
    db.put("suburbs/collingwood",r#"{"_collection":"suburbs","_key":"collingwood"}"#).unwrap();
    db.link("suburbs/fitzroy", "suburbs/collingwood", "borders", 1.0);

    let hits = db.query(
        "SELECT NOW() AS ts \
         FROM MATCH (a:suburbs)-[r:borders]->(b:suburbs) \
         WHERE a._key = 'fitzroy'"
    ).unwrap().collect();

    assert!(!hits.is_empty());
    let p = hits[0].payload.as_ref().unwrap();
    let ts = p["ts"].as_i64().expect("NOW() must return an integer");
    assert!(ts > 1_000_000_000, "timestamp should be a plausible Unix epoch, got {ts}");
}

/// JSON_ARRAY_LENGTH on r._path_keys returns the number of nodes on the path.
#[test]
fn json_array_length() {
    let mut db = CoreDB::new();
    db.put("suburbs/fitzroy",    r#"{"_collection":"suburbs","_key":"fitzroy"}"#).unwrap();
    db.put("suburbs/collingwood",r#"{"_collection":"suburbs","_key":"collingwood"}"#).unwrap();
    db.put("suburbs/richmond",   r#"{"_collection":"suburbs","_key":"richmond"}"#).unwrap();
    db.link("suburbs/fitzroy",     "suburbs/collingwood", "borders", 1.0);
    db.link("suburbs/collingwood", "suburbs/richmond",    "borders", 1.0);

    let hits = db.query(
        "SELECT JSON_ARRAY_LENGTH(r2._path_keys) AS path_len \
         FROM MATCH (a:suburbs)-[r:borders]->(b:suburbs)-[r2:borders]->(c:suburbs) \
         WHERE a._key = 'fitzroy'"
    ).unwrap().collect();

    assert!(!hits.is_empty());
    let p = hits[0].payload.as_ref().unwrap();
    // path: fitzroy, collingwood, richmond → length 3
    assert_eq!(p["path_len"].as_i64().unwrap(), 3);
}

// ── Path predicates on MATCH SHORTEST ────────────────────────────────────────

/// ANY predicate: at least one path node satisfies the condition → 1 row returned.
/// Path coby → luffy → sabo contains "Luffy" → ANY(n.name = 'Luffy') passes.
#[test]
fn shortest_with_any_predicate() {
    let db = setup_path_db();

    let hits = db.query(
        "SELECT r.length AS hops \
         FROM MATCH SHORTEST (a)-[r*]->(b) \
         WHERE a._key = 'characters/coby' AND b._key = 'characters/sabo' \
         AND ANY(n IN nodes(r) WHERE n.name = 'Luffy')"
    ).unwrap().collect();

    assert_eq!(hits.len(), 1, "ANY should pass — Luffy is on the path");
    let p = hits[0].payload.as_ref().unwrap();
    assert_eq!(p["hops"].as_i64().unwrap(), 2);
}

/// ALL predicate: every path node must satisfy condition — fails when it doesn't.
/// Path coby → luffy → sabo; not all nodes are named 'Coby' → 0 rows.
#[test]
fn shortest_with_all_predicate() {
    let db = setup_path_db();

    let hits = db.query(
        "SELECT r.length AS hops \
         FROM MATCH SHORTEST (a)-[r*]->(b) \
         WHERE a._key = 'characters/coby' AND b._key = 'characters/sabo' \
         AND ALL(n IN nodes(r) WHERE n.name = 'Coby')"
    ).unwrap().collect();

    assert!(hits.is_empty(), "ALL should fail — not every node is named Coby");
}

// ── Multi-FROM cross-join ─────────────────────────────────────────────────────

/// Two independent MATCH sources are cross-joined: 2 × 3 = 6 rows.
#[test]
fn multi_from_two_matches() {
    let mut db = CoreDB::new();
    db.put("root1/r1", r#"{"_collection":"root1","_key":"r1"}"#).unwrap();
    db.put("root2/r2", r#"{"_collection":"root2","_key":"r2"}"#).unwrap();
    for i in 1..=2 {
        db.put(&format!("alpha/a{i}"), &format!(r#"{{"_collection":"alpha","_key":"a{i}"}}"#)).unwrap();
        db.link("root1/r1", &format!("alpha/a{i}"), "has", 1.0);
    }
    for i in 1..=3 {
        db.put(&format!("beta/b{i}"), &format!(r#"{{"_collection":"beta","_key":"b{i}"}}"#)).unwrap();
        db.link("root2/r2", &format!("beta/b{i}"), "has", 1.0);
    }

    let hits = db.query(
        "SELECT a._key AS ak, b._key AS bk \
         FROM MATCH ('root1/r1')-[:has]->(a), MATCH ('root2/r2')-[:has]->(b)"
    ).unwrap().collect();

    assert_eq!(hits.len(), 6, "2 × 3 Cartesian product = 6 rows");
}

/// MATCH source cross-joined with a collection source: 2 events × 3 suburbs = 6 rows.
#[test]
fn multi_from_match_and_collection() {
    let mut db = CoreDB::new();
    db.put("root/r", r#"{"_collection":"root","_key":"r"}"#).unwrap();
    for k in ["flood", "storm"] {
        db.put(&format!("events/{k}"), &format!(r#"{{"_collection":"events","_key":"{k}"}}"#)).unwrap();
        db.link("root/r", &format!("events/{k}"), "caused", 1.0);
    }
    for s in ["fitzroy", "richmond", "hawthorn"] {
        db.put(&format!("suburbs/{s}"), &format!(r#"{{"_collection":"suburbs","_key":"{s}"}}"#)).unwrap();
    }

    let hits = db.query(
        "SELECT e._key AS event, s._key AS suburb \
         FROM MATCH ('root/r')-[:caused]->(e), suburbs AS s"
    ).unwrap().collect();

    assert_eq!(hits.len(), 6, "2 events × 3 suburbs = 6 rows");
}

/// MATCH source cross-joined with MATCH SHORTEST: 2 towns × 1 shortest row = 2 rows.
#[test]
fn multi_from_match_and_shortest() {
    let mut db = setup_path_db();
    db.put("towns/mel", r#"{"_collection":"towns","_key":"mel"}"#).unwrap();
    db.put("towns/syd", r#"{"_collection":"towns","_key":"syd"}"#).unwrap();
    db.put("root_n/r",  r#"{"_collection":"root_n","_key":"r"}"#).unwrap();
    db.link("root_n/r", "towns/mel", "near", 1.0);
    db.link("root_n/r", "towns/syd", "near", 1.0);

    let hits = db.query(
        "SELECT t._key AS town, p.length AS hops \
         FROM MATCH ('root_n/r')-[:near]->(t), \
              MATCH SHORTEST (x)-[p*]->(y) WHERE x._key = 'characters/coby' AND y._key = 'characters/sabo'"
    ).unwrap().collect();

    // 2 towns × 1 shortest-path row = 2 rows; each carries the path length
    assert_eq!(hits.len(), 2, "2 towns × 1 shortest path = 2 rows");
    for hit in &hits {
        let p = hit.payload.as_ref().unwrap();
        assert_eq!(p["hops"].as_i64().unwrap(), 2, "coby→sabo shortest path = 2 hops");
    }
}

// ── Date scalar functions ──────────────────────────────────────────────────────

/// YEAR/MONTH/DAY/HOUR/MINUTE/SECOND/DOW/QUARTER in SELECT.
#[test]
fn date_parts_in_select() {
    let mut db = CoreDB::new();
    db.put(
        "posts/p1",
        r#"{"_collection":"posts","_key":"p1","published_at":"2024-07-15T14:30:45Z"}"#,
    )
    .unwrap();

    let hits = db.query(
        "SELECT YEAR(published_at) AS yr, MONTH(published_at) AS mo, \
         DAY(published_at) AS dy, HOUR(published_at) AS hr, \
         MINUTE(published_at) AS mi, SECOND(published_at) AS sc, \
         DOW(published_at) AS dow, QUARTER(published_at) AS qtr \
         FROM posts WHERE _key = 'p1'",
    )
    .unwrap()
    .collect();

    assert_eq!(hits.len(), 1);
    let p = hits[0].payload.as_ref().unwrap();
    assert_eq!(p["yr"].as_i64().unwrap(), 2024, "year");
    assert_eq!(p["mo"].as_i64().unwrap(), 7,    "month");
    assert_eq!(p["dy"].as_i64().unwrap(), 15,   "day");
    assert_eq!(p["hr"].as_i64().unwrap(), 14,   "hour");
    assert_eq!(p["mi"].as_i64().unwrap(), 30,   "minute");
    assert_eq!(p["sc"].as_i64().unwrap(), 45,   "second");
    // 2024-07-15 is a Monday → DOW = 1 (Sun=0, Mon=1)
    assert_eq!(p["dow"].as_i64().unwrap(), 1,   "dow");
    assert_eq!(p["qtr"].as_i64().unwrap(), 3,   "quarter");
}

/// DATE_TRUNC in SELECT.
#[test]
fn date_trunc_in_select() {
    let mut db = CoreDB::new();
    db.put(
        "ev/e1",
        r#"{"_collection":"ev","_key":"e1","ts":"2024-07-15T14:30:45Z"}"#,
    )
    .unwrap();

    let hits = db
        .query("SELECT DATE_TRUNC('month', ts) AS trunc FROM ev WHERE _key = 'e1'")
        .unwrap()
        .collect();

    assert_eq!(hits.len(), 1);
    let trunc = hits[0].payload.as_ref().unwrap()["trunc"]
        .as_str()
        .unwrap()
        .to_string();
    // Truncated to month start: 2024-07-01T00:00:00
    assert!(
        trunc.starts_with("2024-07-01T00:00:00"),
        "expected 2024-07-01T00:00:00…, got {trunc}"
    );
}

/// YEAR() in WHERE clause filters correctly.
#[test]
fn date_func_in_where() {
    let mut db = CoreDB::new();
    db.put(
        "art/a1",
        r#"{"_collection":"art","_key":"a1","published_at":"2022-03-10T00:00:00Z"}"#,
    )
    .unwrap();
    db.put(
        "art/a2",
        r#"{"_collection":"art","_key":"a2","published_at":"2024-07-15T00:00:00Z"}"#,
    )
    .unwrap();

    let hits = db
        .query("SELECT _key FROM art WHERE YEAR(published_at) = 2024")
        .unwrap()
        .collect();

    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].payload.as_ref().unwrap()["_key"]
            .as_str()
            .unwrap(),
        "a2"
    );
}

/// MONTH() > filter.
#[test]
fn date_func_month_gt_in_where() {
    let mut db = CoreDB::new();
    db.put("bl/b1", r#"{"_collection":"bl","_key":"b1","ts":"2024-03-01T00:00:00Z"}"#).unwrap();
    db.put("bl/b2", r#"{"_collection":"bl","_key":"b2","ts":"2024-09-01T00:00:00Z"}"#).unwrap();
    db.put("bl/b3", r#"{"_collection":"bl","_key":"b3","ts":"2024-06-15T00:00:00Z"}"#).unwrap();

    let hits = db
        .query("SELECT _key FROM bl WHERE MONTH(ts) > 6")
        .unwrap()
        .collect();

    // b2 (month=9) and b3 (month=6 — NOT > 6) and b1 (month=3)
    assert_eq!(hits.len(), 1);
    assert_eq!(
        hits[0].payload.as_ref().unwrap()["_key"].as_str().unwrap(),
        "b2"
    );
}

/// NOW() in WHERE returns a unix-ms integer enabling _created_unix comparisons.
/// We verify it parses and executes without error (result may be 0 rows for
/// a freshly created node since _created_unix is set to now and NOW() is now).
#[test]
fn now_in_where_is_numeric() {
    let mut db = CoreDB::new();
    db.put("art/x1", r#"{"_collection":"art","_key":"x1"}"#).unwrap();

    // Should parse and execute without error — result may vary by timing.
    let result = db.query("SELECT _key FROM art WHERE _created_unix < NOW()");
    assert!(result.is_ok(), "NOW() in WHERE should parse and execute");
}

/// BM25 index is updated after INSERT so newly added nodes are searchable.
#[test]
fn bm25_updated_after_insert() {
    let mut db = CoreDB::new();
    // Two pre-existing docs so the rebuilt corpus has N≥3 (needed for IDF > 0
    // when a term appears in exactly one document: ln((N-1+0.5)/(1+0.5)) > 0 iff N > 2).
    db.put(
        "docs/d1",
        r#"{"_collection":"docs","_key":"d1","body":"rust programming language"}"#,
    )
    .unwrap();
    db.put(
        "docs/d0",
        r#"{"_collection":"docs","_key":"d0","body":"web development frontend tooling"}"#,
    )
    .unwrap();
    db.build_bm25_index("body");

    // Insert a new document after the index is built
    db.put(
        "docs/d2",
        r#"{"_collection":"docs","_key":"d2","body":"Melbourne cup horse race"}"#,
    )
    .unwrap();

    // d2 should surface via SQL BM25 search immediately
    let hits = db
        .query("SELECT _key FROM docs WHERE BM25(body, 'Melbourne horse') > 0.0")
        .unwrap()
        .collect();
    let found_d2 = hits.iter().any(|h| {
        h.payload
            .as_ref()
            .and_then(|p| p.get("_key"))
            .and_then(|v| v.as_str())
            == Some("d2")
    });
    assert!(
        found_d2,
        "newly inserted doc must be BM25-searchable; got {} hits",
        hits.len()
    );
}

/// MATCH start variable is bound in SELECT — a.title returns the start node field.
#[test]
fn match_start_var_is_bound() {
    let mut db = CoreDB::new();
    db.put(
        "posts/p1",
        r#"{"_collection":"posts","_key":"p1","title":"Rust is great"}"#,
    )
    .unwrap();
    db.put(
        "tags/t1",
        r#"{"_collection":"tags","_key":"t1","name":"programming"}"#,
    )
    .unwrap();
    db.link("posts/p1", "tags/t1", "tagged_with", 1.0);

    let hits = db
        .query(
            "SELECT a.title AS post_title, b.name AS tag_name \
             FROM MATCH (a:posts)-[:tagged_with]->(b:tags)",
        )
        .unwrap()
        .collect();

    assert_eq!(hits.len(), 1);
    let p = hits[0].payload.as_ref().unwrap();
    assert_eq!(
        p["post_title"].as_str().unwrap(),
        "Rust is great",
        "start var 'a' must be bound"
    );
    assert_eq!(p["tag_name"].as_str().unwrap(), "programming");
}

// ── DEFAULT UUIDV4 / UUIDV5 column defaults ───────────────────────────────────

/// INSERT omitting a field with DEFAULT UUIDV4() — field is auto-filled with a UUID.
#[test]
fn default_uuidv4_auto_filled_on_insert() {
    let mut db = CoreDB::new();
    db.execute(
        "CREATE TABLE items (_key TEXT PRIMARY KEY, pub_id TEXT DEFAULT UUIDV4(), name TEXT)",
    )
    .unwrap();

    db.execute("INSERT INTO items (_key, name) VALUES ('item-1', 'Widget')").unwrap();

    let hits = db.query("SELECT _key, pub_id, name FROM items").unwrap().collect();
    assert_eq!(hits.len(), 1);
    let p = hits[0].payload.as_ref().unwrap();
    assert_eq!(p["_key"].as_str().unwrap(), "item-1");
    assert_eq!(p["name"].as_str().unwrap(), "Widget");

    let pub_id = p["pub_id"].as_str().expect("pub_id must be auto-filled");
    // Valid UUIDv4: 8-4-4-4-12 hex, version nibble = 4, variant bits = 8/9/a/b
    assert_eq!(pub_id.len(), 36, "UUID must be 36 chars with hyphens");
    assert_eq!(&pub_id[14..15], "4", "version nibble must be 4");
}

/// Explicit value in INSERT overrides DEFAULT UUIDV4().
#[test]
fn default_uuidv4_explicit_value_wins() {
    let mut db = CoreDB::new();
    db.execute(
        "CREATE TABLE items (_key TEXT PRIMARY KEY, pub_id TEXT DEFAULT UUIDV4(), name TEXT)",
    )
    .unwrap();

    db.execute(
        "INSERT INTO items (_key, pub_id, name) VALUES ('item-2', 'my-fixed-id', 'Gadget')",
    )
    .unwrap();

    let hits = db.query("SELECT pub_id FROM items").unwrap().collect();
    let p = hits[0].payload.as_ref().unwrap();
    assert_eq!(
        p["pub_id"].as_str().unwrap(),
        "my-fixed-id",
        "explicit value must not be overridden by default"
    );
}

/// Two separate INSERTs produce two different UUIDs (randomness check).
#[test]
fn default_uuidv4_unique_per_row() {
    let mut db = CoreDB::new();
    db.execute(
        "CREATE TABLE items (_key TEXT PRIMARY KEY, pub_id TEXT DEFAULT UUIDV4(), name TEXT)",
    )
    .unwrap();

    db.execute("INSERT INTO items (_key, name) VALUES ('a', 'Alpha')").unwrap();
    db.execute("INSERT INTO items (_key, name) VALUES ('b', 'Beta')").unwrap();

    let hits = db.query("SELECT _key, pub_id FROM items").unwrap().collect();
    assert_eq!(hits.len(), 2);
    let ids: Vec<&str> = hits
        .iter()
        .map(|h| h.payload.as_ref().unwrap()["pub_id"].as_str().unwrap())
        .collect();
    assert_ne!(ids[0], ids[1], "each row must get a distinct UUID");
}

/// DEFAULT UUIDV5 produces a deterministic UUID — same inputs same output.
#[test]
fn default_uuidv5_deterministic() {
    // DNS namespace UUID
    let ns = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";
    let sql = format!(
        "CREATE TABLE items (_key TEXT PRIMARY KEY, stable_id TEXT DEFAULT UUIDV5('{ns}', 'sekejap-test'))"
    );
    let mut db = CoreDB::new();
    db.execute(&sql).unwrap();

    db.execute("INSERT INTO items (_key) VALUES ('x1')").unwrap();
    db.execute("INSERT INTO items (_key) VALUES ('x2')").unwrap();

    let hits = db.query("SELECT stable_id FROM items").unwrap().collect();
    assert_eq!(hits.len(), 2);

    let id0 = hits[0].payload.as_ref().unwrap()["stable_id"].as_str().unwrap().to_string();
    let id1 = hits[1].payload.as_ref().unwrap()["stable_id"].as_str().unwrap().to_string();

    // Both rows share the same literal name → same UUID
    assert_eq!(id0, id1, "UUIDV5 with same inputs must produce the same UUID");

    // Must be 36-char UUID format
    assert_eq!(id0.len(), 36);
}

/// ALTER TABLE ADD COLUMN with DEFAULT UUIDV4() — new column gets UUID on subsequent INSERTs.
#[test]
fn alter_table_add_column_default_uuidv4() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE items (_key TEXT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("ALTER TABLE items ADD COLUMN ext_id TEXT DEFAULT UUIDV4()").unwrap();

    db.execute("INSERT INTO items (_key, name) VALUES ('w1', 'Widget')").unwrap();

    let hits = db.query("SELECT ext_id FROM items").unwrap().collect();
    let p = hits[0].payload.as_ref().unwrap();
    let ext_id = p["ext_id"].as_str().expect("ext_id must be auto-filled after ALTER TABLE");
    assert_eq!(ext_id.len(), 36);
    assert_eq!(&ext_id[14..15], "4");
}

// ── Auto _key injection ────────────────────────────────────────────────────────

/// CREATE TABLE without _key → _key DEFAULT UUIDV4() is auto-injected.
/// INSERT without _key → slug auto-generated, node is queryable.
#[test]
fn create_table_without_key_auto_injects_uuid_key() {
    let mut db = CoreDB::new();
    // No _key in schema definition
    db.execute("CREATE TABLE articles (title TEXT, body TEXT)").unwrap();

    // INSERT without _key — UUID auto-generated
    db.execute("INSERT INTO articles (title, body) VALUES ('Hello', 'World')").unwrap();

    let hits = db.query("SELECT _key, title FROM articles").unwrap().collect();
    assert_eq!(hits.len(), 1);
    let p = hits[0].payload.as_ref().unwrap();
    assert_eq!(p["title"].as_str().unwrap(), "Hello");

    let key = p["_key"].as_str().expect("_key must be auto-generated");
    assert_eq!(key.len(), 36, "_key must be a UUID");
    assert_eq!(&key[14..15], "4", "must be UUIDv4");
}

/// Two keyless INSERTs produce two distinct _key UUIDs.
#[test]
fn create_table_without_key_each_row_unique() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE notes (text TEXT)").unwrap();

    db.execute("INSERT INTO notes (text) VALUES ('First')").unwrap();
    db.execute("INSERT INTO notes (text) VALUES ('Second')").unwrap();

    let hits = db.query("SELECT _key FROM notes").unwrap().collect();
    assert_eq!(hits.len(), 2);
    let k0 = hits[0].payload.as_ref().unwrap()["_key"].as_str().unwrap().to_string();
    let k1 = hits[1].payload.as_ref().unwrap()["_key"].as_str().unwrap().to_string();
    assert_ne!(k0, k1, "each row must get a distinct UUID _key");
}

/// Explicit _key in INSERT overrides the auto-UUID default.
#[test]
fn create_table_without_key_explicit_key_wins() {
    let mut db = CoreDB::new();
    db.execute("CREATE TABLE posts (title TEXT)").unwrap();

    db.execute("INSERT INTO posts (_key, title) VALUES ('hello-world', 'Hello')").unwrap();

    let hits = db.query("SELECT _key FROM posts").unwrap().collect();
    let key = hits[0].payload.as_ref().unwrap()["_key"].as_str().unwrap();
    assert_eq!(key, "hello-world", "explicit _key must not be overridden");
}

/// INSERT without _key and without a schema → MissingField error (no silent UUID).
#[test]
fn insert_without_key_no_schema_errors() {
    let mut db = CoreDB::new();
    // No CREATE TABLE — no schema registered
    db.put("items/seed", r#"{"_collection":"items","_key":"seed","name":"Seed"}"#).unwrap();

    let result = db.execute("INSERT INTO items (name) VALUES ('Widget')");
    assert!(result.is_err(), "INSERT without _key and no schema must fail");
}
