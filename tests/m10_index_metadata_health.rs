use sekejap::SekejapDB;
use serde_json::json;
use tempfile::TempDir;

#[test]
fn describe_collection_reports_four_pillar_index_health() {
    let dir = TempDir::new().expect("tempdir");
    let db = SekejapDB::new(dir.path(), 20_000).expect("db");

    db.init_hnsw(16);

    #[cfg(feature = "fulltext")]
    db.init_fulltext(dir.path());

    db.schema()
        .define(
            "posts",
            &json!({
                "hot": {
                    "vector": ["vectors.dense"],
                    "spatial": ["coordinates"],
                    "fulltext": ["title", "content"],
                    "hash_index": ["status"],
                    "range_index": ["score"]
                }
            })
            .to_string(),
        )
        .expect("define schema");

    db.nodes()
        .put_json(
            &json!({
                "_id": "posts/a",
                "title": "Flood Warning",
                "content": "river overflow in district alpha",
                "status": "active",
                "score": 91,
                "vectors": {"dense": [0.12, 0.21, 0.31, 0.02]},
                "coordinates": {"lat": -6.2000, "lon": 106.8000}
            })
            .to_string(),
        )
        .expect("put a");

    db.nodes()
        .put_json(
            &json!({
                "_id": "posts/b",
                "title": "Operations Update",
                "content": "flood response ongoing",
                "status": "active",
                "score": 88,
                "vectors": {"dense": [0.11, 0.22, 0.30, 0.01]},
                "coordinates": {"lat": -6.2100, "lon": 106.8200}
            })
            .to_string(),
        )
        .expect("put b");

    db.nodes()
        .put_json(
            &json!({
                "_id": "posts/c",
                "title": "Dry Weather",
                "content": "normal conditions",
                "status": "archived",
                "score": 35,
                "vectors": {"dense": [0.91, 0.81, 0.71, 0.61]},
                "coordinates": {"lat": -7.0000, "lon": 107.0000}
            })
            .to_string(),
        )
        .expect("put c");

    db.edges()
        .link("posts/a", "posts/b", "depends_on", 1.0)
        .expect("link");

    db.flush().expect("flush");

    let graph = db
        .query(
            &json!({
                "pipeline": [
                    {"op": "one", "slug": "posts/a"},
                    {"op": "forward", "type": "depends_on"},
                    {"op": "where_eq", "field": "status", "value": "active"}
                ]
            })
            .to_string(),
        )
        .expect("graph query");
    assert!(!graph.data.is_empty());

    let vector = db
        .query(
            &json!({
                "pipeline": [
                    {"op": "all"},
                    {"op": "similar", "query": [0.11, 0.20, 0.30, 0.01], "k": 2}
                ]
            })
            .to_string(),
        )
        .expect("vector query");
    assert!(!vector.data.is_empty());

    let spatial = db
        .query(
            &json!({
                "pipeline": [
                    {"op": "all"},
                    {"op": "spatial_within_bbox", "min_lat": -6.3, "min_lon": 106.7, "max_lat": -6.1, "max_lon": 106.9}
                ]
            })
            .to_string(),
        )
        .expect("spatial query");
    assert_eq!(spatial.data.len(), 2);

    #[cfg(feature = "fulltext")]
    {
        let text = db
            .query(
                &json!({
                    "pipeline": [
                        {"op": "collection", "name": "posts"},
                        {"op": "matching", "text": "flood", "limit": 5, "title_weight": 2.0, "content_weight": 1.0}
                    ]
                })
                .to_string(),
            )
            .expect("fulltext query");
        assert!(!text.data.is_empty());
    }

    let coll = db.describe_collection("posts");
    assert_eq!(coll["exists"], json!(true));
    assert_eq!(coll["schema"]["hash_indexed_fields"], json!(["status"]));
    assert_eq!(coll["schema"]["range_indexed_fields"], json!(["score"]));

    assert_eq!(
        coll["indexes"]["graph"]["collection_bitmap_ready"],
        json!(true)
    );
    assert_eq!(coll["indexes"]["vector"]["hnsw_ready"], json!(true));
    assert_eq!(coll["indexes"]["spatial"]["rtree_ready"], json!(true));

    let hash_ready = coll["indexes"]["payload"]["hash_ready"]
        .as_array()
        .expect("hash_ready array");
    assert!(hash_ready
        .iter()
        .any(|entry| { entry["field"] == json!("status") && entry["ready"] == json!(true) }));

    let range_ready = coll["indexes"]["payload"]["range_ready"]
        .as_array()
        .expect("range_ready array");
    assert!(range_ready
        .iter()
        .any(|entry| { entry["field"] == json!("score") && entry["ready"] == json!(true) }));

    let all = db.describe();
    assert_eq!(all["graph"]["collection_bitmap_ready"], json!(true));
    assert_eq!(all["vector"]["index_impl"], json!("hnsw"));
    assert_eq!(all["spatial"]["index_impl"], json!("rtree"));
}
