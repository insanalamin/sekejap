use sekejap::SekejapDB;
use serde_json::json;
use tempfile::TempDir;

#[test]
fn spatial_bbox_and_polygon_pipeline_ops_work() {
    let dir = TempDir::new().expect("tempdir");
    let db = SekejapDB::new(dir.path(), 10_000).expect("db");

    db.nodes()
        .put_json(
            &json!({
                "_id":"places/a",
                "name":"A",
                "coordinates":{"lat":-6.2000,"lon":106.8000}
            })
            .to_string(),
        )
        .expect("put a");
    db.nodes()
        .put_json(
            &json!({
                "_id":"places/b",
                "name":"B",
                "coordinates":{"lat":-6.2100,"lon":106.8200}
            })
            .to_string(),
        )
        .expect("put b");
    db.nodes()
        .put_json(
            &json!({
                "_id":"places/c",
                "name":"C",
                "coordinates":{"lat":-7.0000,"lon":107.0000}
            })
            .to_string(),
        )
        .expect("put c");

    let bbox = db
        .query(
            &json!({
                "pipeline":[
                    {"op":"all"},
                    {"op":"spatial_within_bbox","min_lat":-6.3,"min_lon":106.7,"max_lat":-6.1,"max_lon":106.9}
                ]
            })
            .to_string(),
        )
        .expect("bbox query");
    assert_eq!(bbox.data.len(), 2);

    let polygon = db
        .query(
            &json!({
                "pipeline":[
                    {"op":"all"},
                    {"op":"spatial_within_polygon","polygon":[[-6.30,106.70],[-6.10,106.70],[-6.10,106.90],[-6.30,106.90]]}
                ]
            })
            .to_string(),
        )
        .expect("polygon query");
    assert_eq!(polygon.data.len(), 2);
}

#[cfg(feature = "fulltext")]
#[test]
fn fulltext_matching_weighted_returns_score_and_ranks() {
    let dir = TempDir::new().expect("tempdir");
    let db = SekejapDB::new(dir.path(), 10_000).expect("db");
    db.init_fulltext(dir.path());

    db.nodes()
        .put_json(
            &json!({
                "_id":"docs/title-first",
                "title":"Flood Alert",
                "content":"routine update"
            })
            .to_string(),
        )
        .expect("put title doc");
    db.nodes()
        .put_json(
            &json!({
                "_id":"docs/content-first",
                "title":"Routine Bulletin",
                "content":"flood flood flood in district"
            })
            .to_string(),
        )
        .expect("put content doc");
    db.flush().expect("flush");

    let out = db
        .query(
            &json!({
                "pipeline":[
                    {"op":"collection","name":"docs"},
                    {"op":"matching","text":"flood","limit":10,"title_weight":3.0,"content_weight":1.0}
                ]
            })
            .to_string(),
        )
        .expect("matching query");
    assert!(!out.data.is_empty());
    assert!(out.data[0].score.is_some());
}
