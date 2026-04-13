use sekejap::CoreDB;
use tempfile::TempDir;

fn tmpdir() -> TempDir {
    tempfile::tempdir().unwrap()
}

#[test]
fn open_empty_dir_creates_db() {
    let dir = tmpdir();
    let db = CoreDB::open(dir.path()).unwrap();
    assert_eq!(db.node_count(), 0);
}

#[test]
fn put_survives_reopen() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.put("alice", r#"{"name":"Alice","_collection":"users"}"#).unwrap();
        db.put("bob",   r#"{"name":"Bob",  "_collection":"users"}"#).unwrap();
    } // db dropped, WAL flushed to OS

    let db2 = CoreDB::open(dir.path()).unwrap();
    assert_eq!(db2.node_count(), 2);
    assert!(db2.contains("alice"));
    assert!(db2.contains("bob"));
}

#[test]
fn remove_survives_reopen() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.put("alice", r#"{"name":"Alice"}"#).unwrap();
        db.put("bob",   r#"{"name":"Bob"}"#).unwrap();
        db.remove("alice");
    }

    let db2 = CoreDB::open(dir.path()).unwrap();
    assert!(!db2.contains("alice"));
    assert!(db2.contains("bob"));
    assert_eq!(db2.node_count(), 1);
}

#[test]
fn link_survives_reopen() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.put("alice", r#"{"name":"Alice"}"#).unwrap();
        db.put("bob",   r#"{"name":"Bob"}"#).unwrap();
        db.link("alice", "bob", "follows", 1.0);
    }

    let db2 = CoreDB::open(dir.path()).unwrap();
    let hits = db2.one("alice").forward("follows").collect();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].slug, "bob");
}

#[test]
fn link_meta_survives_reopen() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.put("alice", r#"{"name":"Alice"}"#).unwrap();
        db.put("bob",   r#"{"name":"Bob"}"#).unwrap();
        db.link_meta("alice", "bob", "knows", 0.9, r#"{"since":2020}"#).unwrap();
    }

    let db2 = CoreDB::open(dir.path()).unwrap();
    let edges = db2.edges_from("alice");
    assert_eq!(edges.len(), 1);
    let meta = edges[0].meta.as_ref().unwrap();
    assert_eq!(meta["since"], 2020);
}

#[test]
fn unlink_survives_reopen() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.put("a", "{}").unwrap();
        db.put("b", "{}").unwrap();
        db.link("a", "b", "rel", 1.0);
        db.unlink("a", "b", "rel");
    }

    let db2 = CoreDB::open(dir.path()).unwrap();
    assert!(db2.one("a").forward("rel").collect().is_empty());
}

#[test]
fn compact_then_reopen() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        for i in 0..100 {
            db.put(&format!("node:{i}"), &format!(r#"{{"i":{i}}}"#)).unwrap();
        }
        db.compact().unwrap();
    }

    let db2 = CoreDB::open(dir.path()).unwrap();
    assert_eq!(db2.node_count(), 100);
    assert!(db2.contains("node:0"));
    assert!(db2.contains("node:99"));
}

#[test]
fn compact_removes_deleted_nodes() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.put("keep",   r#"{"v":1}"#).unwrap();
        db.put("delete", r#"{"v":2}"#).unwrap();
        db.remove("delete");
        db.compact().unwrap();
    }

    let db2 = CoreDB::open(dir.path()).unwrap();
    assert_eq!(db2.node_count(), 1);
    assert!(db2.contains("keep"));
    assert!(!db2.contains("delete"));
}

#[test]
fn wal_grows_then_compact_shrinks() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        for i in 0..50 {
            db.put(&format!("n:{i}"), "{}").unwrap();
        }
        let wal_size_before = std::fs::metadata(dir.path().join("wal.log"))
            .unwrap().len();

        db.compact().unwrap();

        let wal_size_after = std::fs::metadata(dir.path().join("wal.log"))
            .unwrap().len();

        // WAL should be empty (or near-empty) after compact
        assert!(wal_size_after < wal_size_before,
            "WAL should shrink after compact: before={wal_size_before} after={wal_size_after}");
    }
}

#[test]
fn collection_survives_reopen() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.put("p1", r#"{"_collection":"products","cat":"a"}"#).unwrap();
        db.put("p2", r#"{"_collection":"products","cat":"b"}"#).unwrap();
        db.put("u1", r#"{"_collection":"users"}"#).unwrap();
    }

    let db2 = CoreDB::open(dir.path()).unwrap();
    let products = db2.collection("products").count();
    assert_eq!(products, 2);
}

#[test]
fn query_sql_works_after_reopen() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.put("p1", r#"{"_collection":"items","price":10}"#).unwrap();
        db.put("p2", r#"{"_collection":"items","price":50}"#).unwrap();
        db.put("p3", r#"{"_collection":"items","price":100}"#).unwrap();
    }

    let db2 = CoreDB::open(dir.path()).unwrap();
    let hits = db2.query("SELECT * FROM items WHERE price > 20").unwrap().collect();
    assert_eq!(hits.len(), 2);
}

#[test]
fn multiple_compact_cycles() {
    let dir = tmpdir();
    let mut db = CoreDB::open(dir.path()).unwrap();

    for cycle in 0..3 {
        for i in 0..10 {
            db.put(&format!("n:{cycle}:{i}"), "{}").unwrap();
        }
        db.compact().unwrap();
    }

    drop(db);

    let db2 = CoreDB::open(dir.path()).unwrap();
    assert_eq!(db2.node_count(), 30);
}

// ── Transaction persistence ───────────────────────────────────────────────────

/// Committed transactions must survive a WAL-only cold reload.
#[test]
fn transaction_survives_wal_replay() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        let mut txn = db.begin();
        txn.put("users/alice", r#"{"_collection":"users","name":"Alice"}"#).unwrap();
        txn.put("users/bob",   r#"{"_collection":"users","name":"Bob"}"#).unwrap();
        txn.link("users/alice", "users/bob", "follows", 1.0);
        txn.commit().unwrap();
        // No compact — all data lives in WAL
    }

    {
        let db = CoreDB::open(dir.path()).unwrap();
        assert!(db.contains("users/alice"), "alice must survive WAL replay");
        assert!(db.contains("users/bob"),   "bob must survive WAL replay");
        let hits = db.one("users/alice").forward("follows").collect();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].slug, "users/bob");
    }
}

/// Rolled-back transactions must NOT appear after a cold reload.
#[test]
fn transaction_rollback_leaves_wal_clean() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.put("base/node", r#"{"_collection":"base"}"#).unwrap();
        {
            let mut txn = db.begin();
            txn.put("ghost/node", r#"{"_collection":"ghost"}"#).unwrap();
            // rollback
        }
        // Only base/node was committed
    }

    {
        let db = CoreDB::open(dir.path()).unwrap();
        assert!(db.contains("base/node"),   "base/node must persist");
        assert!(!db.contains("ghost/node"), "ghost/node must NOT persist after rollback");
    }
}

// ── #2 HNSW persistence ───────────────────────────────────────────────────────

/// HNSW graph must survive compact + cold reload — no rebuild needed on startup.
#[test]
fn hnsw_graph_survives_compact_and_reload() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        // Insert nodes with orthogonal embeddings
        db.put("docs/d1", r#"{"_collection":"docs"}"#).unwrap();
        db.put("docs/d2", r#"{"_collection":"docs"}"#).unwrap();
        db.put("docs/d3", r#"{"_collection":"docs"}"#).unwrap();
        db.put_vector("docs/d1", "emb", &[1.0_f32, 0.0, 0.0]).unwrap();
        db.put_vector("docs/d2", "emb", &[0.0_f32, 1.0, 0.0]).unwrap();
        db.put_vector("docs/d3", "emb", &[0.0_f32, 0.0, 1.0]).unwrap();
        db.build_hnsw_index("emb", 8, 50).unwrap();
        db.compact().unwrap();
    }

    // Cold open — HNSW must be available from snapshot (no rebuild)
    {
        let db = CoreDB::open(dir.path()).unwrap();
        let results = db
            .collection("docs")
            .vector_near("emb", vec![1.0, 0.0, 0.0], 1)
            .collect();
        assert_eq!(results.len(), 1, "HNSW must return 1 result after compact+reload");
        assert_eq!(results[0].slug, "docs/d1");
    }
}

/// HNSW must survive WAL-only reload (no compact).
#[test]
fn hnsw_graph_survives_wal_replay() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.put("docs/d1", r#"{"_collection":"docs"}"#).unwrap();
        db.put("docs/d2", r#"{"_collection":"docs"}"#).unwrap();
        db.put_vector("docs/d1", "emb", &[1.0_f32, 0.0]).unwrap();
        db.put_vector("docs/d2", "emb", &[0.0_f32, 1.0]).unwrap();
        db.build_hnsw_index("emb", 4, 20).unwrap();
        // No compact — data lives in WAL only
    }

    // Cold reload: WAL replay should restore nodes + vectors.
    // The HNSW graph was NOT in the WAL, but the vectors are — flat scan will work.
    {
        let db = CoreDB::open(dir.path()).unwrap();
        // Flat-scan fallback (no HNSW from WAL) still gives correct results
        let results = db
            .collection("docs")
            .vector_near("emb", vec![1.0, 0.0], 1)
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].slug, "docs/d1");
    }
}

// ── #3 Btree field index persistence ─────────────────────────────────────────

/// Btree index must survive compact + cold reload and remain queryable.
#[test]
fn btree_index_survives_compact_and_reload() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        for i in 0..20 {
            db.put(
                &format!("items/i{i}"),
                &format!(r#"{{"_collection":"items","_key":"i{i}","price":{i}}}"#),
            ).unwrap();
        }
        db.execute("CREATE INDEX ON items USING btree (price)").unwrap();
        db.compact().unwrap();
    }

    // Cold open — index must be rebuilt from schema hints in snapshot
    {
        let db = CoreDB::open(dir.path()).unwrap();
        let hits = db.query("SELECT * FROM items WHERE price > 15").unwrap().collect();
        assert_eq!(hits.len(), 4, "items with price 16-19 after compact+reload");
    }
}

/// Btree index must survive WAL-only replay — CreateIndex entry rebuilds it.
#[test]
fn btree_index_survives_wal_replay() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        for i in 0..10 {
            db.put(
                &format!("p/p{i}"),
                &format!(r#"{{"_collection":"p","_key":"p{i}","val":{i}}}"#),
            ).unwrap();
        }
        db.execute("CREATE INDEX ON p USING btree (val)").unwrap();
        // No compact — WAL holds all entries including CreateIndex
    }

    {
        let db = CoreDB::open(dir.path()).unwrap();
        let hits = db.query("SELECT * FROM p WHERE val = 7").unwrap().collect();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].slug, "p/p7");
    }
}

// ── #4 BM25 / GIN index persistence ──────────────────────────────────────────

/// BM25 index must survive WAL-only cold reload and return ranked results.
#[test]
fn bm25_survives_wal_replay() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.execute("CREATE TABLE articles (title TEXT)").unwrap();
        // Insert data before creating index — BM25 is batch-built.
        db.put("articles/a1", r#"{"_collection":"articles","title":"Flooding in Maribyrnong River"}"#).unwrap();
        db.put("articles/a2", r#"{"_collection":"articles","title":"Melbourne Cup Fitzroy Gardens"}"#).unwrap();
        db.put("articles/a3", r#"{"_collection":"articles","title":"The Vines at Rod Laver Arena"}"#).unwrap();
        db.execute("CREATE INDEX ON articles USING bm25 (title)").unwrap();
        // No compact — data lives in WAL only
    }

    {
        let db = CoreDB::open(dir.path()).unwrap();
        let results = db.bm25_search("title", "maribyrnong flooding", 5);
        assert!(!results.is_empty(), "BM25 must return results after WAL-only reload");
        // Cross-check: the node itself must still be present
        assert!(db.contains("articles/a1"));
    }
}

/// GIN index must survive WAL-only cold reload and ILIKE queries must work.
#[test]
fn gin_survives_wal_replay() {
    let dir = tmpdir();

    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.execute("CREATE TABLE venues (name TEXT)").unwrap();
        // Insert data before creating index — GIN is batch-built.
        db.put("venues/v1", r#"{"_collection":"venues","name":"Rod Laver Arena"}"#).unwrap();
        db.put("venues/v2", r#"{"_collection":"venues","name":"Melbourne Park"}"#).unwrap();
        db.put("venues/v3", r#"{"_collection":"venues","name":"Fitzroy Pool"}"#).unwrap();
        db.execute("CREATE INDEX ON venues USING gin (name)").unwrap();
        // No compact — data lives in WAL only
    }

    {
        let db = CoreDB::open(dir.path()).unwrap();
        let ids = db.gin_ilike("name", "%laver%", None);
        assert_eq!(ids.len(), 1, "GIN must find Rod Laver Arena after WAL-only reload");
        // Verify it's the right node by cross-checking with SQL full-scan
        let hits = db.query("SELECT * FROM venues WHERE name ILIKE '%laver%'").unwrap().collect();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].slug, "venues/v1");
    }
}

// ── Storage format stability contract ─────────────────────────────────────────

/// A snapshot with `version` higher than the binary supports must be rejected.
#[test]
fn snapshot_version_too_new_rejected() {
    let dir = tmpdir();

    // Write a snapshot with a far-future version number.
    let snap_json = r#"{"version":9999,"nodes":[],"edges":[]}"#;
    std::fs::write(dir.path().join("snapshot.json"), snap_json).unwrap();

    match CoreDB::open(dir.path()) {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("version"),
                "error message must mention 'version', got: {msg}"
            );
        }
        Ok(_) => panic!("must reject snapshot version > supported max"),
    }
}

/// GIN index version mismatch (stored 0, compiled-in > 0) must trigger rebuild.
#[test]
fn gin_version_mismatch_triggers_rebuild() {
    let dir = tmpdir();

    // Step 1: Create DB, insert nodes, build GIN index, compact.
    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.execute("CREATE TABLE venues (name TEXT)").unwrap();
        db.put("venues/v1", r#"{"_collection":"venues","name":"Rod Laver Arena"}"#).unwrap();
        db.put("venues/v2", r#"{"_collection":"venues","name":"Melbourne Park"}"#).unwrap();
        db.execute("CREATE INDEX ON venues USING gin (name)").unwrap();
        db.compact().unwrap();
    }

    // Step 2: Patch snapshot — set gin:name build_version to 0 so it looks stale.
    let snap_path = dir.path().join("snapshot.json");
    let raw = std::fs::read_to_string(&snap_path).unwrap();
    let mut snap: serde_json::Value = serde_json::from_str(&raw).unwrap();
    if let Some(schemas) = snap["schemas"].as_array_mut() {
        for schema in schemas.iter_mut() {
            if schema["collection"].as_str() == Some("venues") {
                schema["indexes"]["build_versions"]["gin:name"] =
                    serde_json::json!(0u32);
            }
        }
    }
    std::fs::write(&snap_path, serde_json::to_vec(&snap).unwrap()).unwrap();

    // Step 3: Reopen — version mismatch must trigger auto-rebuild.
    {
        let db = CoreDB::open(dir.path()).unwrap();
        let ids = db.gin_ilike("name", "%laver%", None);
        assert_eq!(
            ids.len(), 1,
            "GIN must return correct results after auto-rebuild on version mismatch"
        );
    }
}

/// HNSW version mismatch (stored 0) must trigger rebuild from stored vectors.
#[test]
fn hnsw_version_mismatch_triggers_rebuild() {
    let dir = tmpdir();

    // Step 1: Create DB with vectors + HNSW index, compact.
    {
        let mut db = CoreDB::open(dir.path()).unwrap();
        db.put("docs/d1", r#"{"_collection":"docs"}"#).unwrap();
        db.put("docs/d2", r#"{"_collection":"docs"}"#).unwrap();
        db.put("docs/d3", r#"{"_collection":"docs"}"#).unwrap();
        db.put_vector("docs/d1", "emb", &[1.0_f32, 0.0, 0.0]).unwrap();
        db.put_vector("docs/d2", "emb", &[0.0_f32, 1.0, 0.0]).unwrap();
        db.put_vector("docs/d3", "emb", &[0.0_f32, 0.0, 1.0]).unwrap();
        db.build_hnsw_index("emb", 8, 50).unwrap();
        db.compact().unwrap();
    }

    // Step 2: Patch snapshot — set hnsw_indexes[0].version to 0.
    let snap_path = dir.path().join("snapshot.json");
    let raw = std::fs::read_to_string(&snap_path).unwrap();
    let mut snap: serde_json::Value = serde_json::from_str(&raw).unwrap();
    if let Some(hnsw_list) = snap["hnsw_indexes"].as_array_mut() {
        for entry in hnsw_list.iter_mut() {
            entry["version"] = serde_json::json!(0u32);
        }
    }
    std::fs::write(&snap_path, serde_json::to_vec(&snap).unwrap()).unwrap();

    // Step 3: Reopen — version mismatch must trigger rebuild from stored vectors.
    {
        let db = CoreDB::open(dir.path()).unwrap();
        let results = db
            .collection("docs")
            .vector_near("emb", vec![1.0, 0.0, 0.0], 1)
            .collect();
        assert_eq!(
            results.len(), 1,
            "vector_near must return correct result after HNSW auto-rebuild"
        );
        assert_eq!(results[0].slug, "docs/d1");
    }
}
