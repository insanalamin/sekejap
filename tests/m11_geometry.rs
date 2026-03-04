//! Integration tests for PostGIS-par geometry support.
//! Tests: GeoJSON node storage, st_within, st_intersects, st_contains, st_dwithin, query_skql.

use sekejap::{QueryCompiler, SekejapDB};
use serde_json::json;
use tempfile::TempDir;

fn setup_db() -> (SekejapDB, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = SekejapDB::new(dir.path(), 10_000).unwrap();

    // Point node via GeoJSON
    db.nodes()
        .put(
            "places/cimb-bangsar",
            &json!({
                "name": "CIMB Bangsar",
                "type": "bank",
                "geometry": { "type": "Point", "coordinates": [101.6710, 3.1291] }
            })
            .to_string(),
        )
        .unwrap();

    // Point node via legacy geo.loc
    db.nodes()
        .put(
            "places/klcc",
            &json!({
                "name": "KLCC",
                "type": "landmark",
                "geo": { "loc": { "lat": 3.1570, "lon": 101.7123 } }
            })
            .to_string(),
        )
        .unwrap();

    // Polygon node (a zone)
    db.nodes()
        .put(
            "zones/bangsar",
            &json!({
                "name": "Bangsar Zone",
                "type": "zone",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [101.665, 3.128], [101.678, 3.128],
                        [101.678, 3.135], [101.665, 3.135],
                        [101.665, 3.128]
                    ]]
                }
            })
            .to_string(),
        )
        .unwrap();

    // LineString node (a road)
    db.nodes()
        .put(
            "roads/jalan-ara",
            &json!({
                "name": "Jalan Ara",
                "type": "road",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [[101.668, 3.129], [101.674, 3.133]]
                }
            })
            .to_string(),
        )
        .unwrap();

    (db, dir)
}

// Bangsar bounding polygon (slightly enlarged to contain the point and road)
const BANGSAR_RING: [[f32; 2]; 5] = [
    [3.127, 101.664],
    [3.127, 101.679],
    [3.136, 101.679],
    [3.136, 101.664],
    [3.127, 101.664],
];

#[test]
fn test_near_geojson_point() {
    let (db, _dir) = setup_db();
    let result = db
        .query("collection \"places\"\nnear 3.1291 101.6710 0.5")
        .unwrap();
    assert!(
        !result.data.is_empty(),
        "near should find CIMB Bangsar within 0.5km"
    );
}

#[test]
fn test_st_within_point_in_polygon() {
    let (db, _dir) = setup_db();
    // CIMB Bangsar point should be within the Bangsar ring
    let result = db
        .query(
            &json!({
                "pipeline": [
                    {"op": "collection", "name": "places"},
                    {"op": "st_within", "polygon": BANGSAR_RING}
                ]
            })
            .to_string(),
        )
        .unwrap();
    let names: Vec<_> = result
        .data
        .iter()
        .filter_map(|h| {
            h.payload
                .as_deref()
                .and_then(|p| serde_json::from_str::<serde_json::Value>(p).ok())
                .and_then(|v| v.get("name").and_then(|n| n.as_str()).map(|s| s.to_owned()))
        })
        .collect();
    assert!(
        names.contains(&"CIMB Bangsar".to_owned()),
        "st_within should find CIMB Bangsar, got: {:?}",
        names
    );
    // KLCC (far away) should NOT be found
    assert!(
        !names.contains(&"KLCC".to_owned()),
        "st_within should NOT find KLCC"
    );
}

#[test]
fn test_st_intersects_linestring() {
    let (db, _dir) = setup_db();
    let result = db
        .query(
            &json!({
                "pipeline": [
                    {"op": "collection", "name": "roads"},
                    {"op": "st_intersects", "polygon": BANGSAR_RING}
                ]
            })
            .to_string(),
        )
        .unwrap();
    assert!(
        !result.data.is_empty(),
        "st_intersects should find Jalan Ara road crossing the Bangsar ring"
    );
}

#[test]
fn test_geojson_polygon_bbox_indexed() {
    let (db, _dir) = setup_db();
    // The Bangsar zone polygon bbox should be in the R-Tree
    let result = db
        .query(
            &json!({
                "pipeline": [
                    {"op": "collection", "name": "zones"},
                    {"op": "spatial_within_bbox",
                     "min_lat": 3.125, "min_lon": 101.660,
                     "max_lat": 3.140, "max_lon": 101.685}
                ]
            })
            .to_string(),
        )
        .unwrap();
    assert!(
        !result.data.is_empty(),
        "Bangsar zone should be found by bbox encompassing its envelope"
    );
}

#[test]
fn test_query_skql_text_format() {
    let qc = QueryCompiler::new();

    // Multi-line format
    let steps = qc
        .parse_text_pipeline(
            "collection \"crimes\"\nwhere_eq \"type\" \"robbery\"\nnear 3.1291 101.6710 1.0\nsort \"severity\" desc\ntake 20",
        )
        .unwrap();
    assert_eq!(steps.len(), 5, "5 ops in pipeline");

    // Pipe format
    let steps2 = qc
        .parse_text_pipeline(
            "collection \"crimes\" | where_eq \"status\" \"open\" | take 10",
        )
        .unwrap();
    assert_eq!(steps2.len(), 3, "3 ops via pipe");

    // Polygon inline
    let steps3 = qc
        .parse_text_pipeline("collection \"crimes\"\nst_within (3.128,101.665) (3.135,101.665) (3.135,101.678) (3.128,101.678) (3.128,101.665)")
        .unwrap();
    assert_eq!(steps3.len(), 2);
}

#[test]
fn test_skql_end_to_end() {
    let (db, _dir) = setup_db();
    let result = db
        .query(
            "collection \"places\"\nst_within (3.127,101.664) (3.127,101.679) (3.136,101.679) (3.136,101.664) (3.127,101.664)",
        )
        .unwrap();
    assert!(
        !result.data.is_empty(),
        "query_skql st_within should find nodes in Bangsar ring"
    );
}
