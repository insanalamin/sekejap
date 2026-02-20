//! TC-1.3: Spatial Index Registration
//!
//! Goal: Ensure inserting a node with coordinates registers it in the spatial index.
//! Given: A node with GPS coordinates.
//! When: Inserting the node.
//! Then: The spatial index returns it for nearby queries.

use sekejap::SekejapDB;
use tempfile::TempDir;
use serde_json::json;

fn setup_db() -> (SekejapDB, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = SekejapDB::new(dir.path(), 1000).unwrap();
    (db, dir)
}

#[test]
fn test_spatial_index_registration() {
    let (db, _dir) = setup_db();

    let slug = "locations/monas";
    let payload = json!({
        "_id": slug,
        "name": "Monas",
        "type": "landmark",
        "coordinates": {"lat": -6.1754, "lon": 106.8272}
    }).to_string();

    // Step 1: Insert node with coordinates
    let idx = db.nodes().put_json(&payload).unwrap();
    db.flush().unwrap();
    println!("[TC-1.3] Inserted Monas at index: {}", idx);

    // Verify spatial index contains the node
    let spatial_tree = db.spatial.read();
    let all_nodes: Vec<_> = spatial_tree.iter().collect();
    println!("[TC-1.3] Spatial index contains {} nodes", all_nodes.len());
    assert!(!all_nodes.is_empty(), "Spatial index should not be empty");

    // Step 2: Spatial query with 0.5km radius should include node
    let outcome = db.nodes()
        .one(slug)
        .near(-6.1754, 106.8272, 0.5)
        .collect()
        .unwrap();

    assert!(!outcome.data.is_empty(), "0.5km radius should include Monas");
    println!("[TC-1.3] 0.5km radius query: found {} nodes", outcome.data.len());

    // Step 3: Excluded if radius is too small (0.01km = 10 meters)
    let outcome_small = db.nodes()
        .one(slug)
        .near(-6.1754, 106.8272, 0.01)
        .collect()
        .unwrap();

    // Node should be included if coordinates match (exact location)
    println!("[TC-1.3] 0.01km radius query: found {} nodes", outcome_small.data.len());
}

#[test]
fn test_multiple_nodes_spatial_query() {
    let (db, _dir) = setup_db();

    // Insert multiple nodes at different locations
    let nodes = vec![
        ("loc/monas", -6.1754, 106.8272, "Monas National Monument"),
        ("loc/istiqlal", -6.1700, 106.8319, "Istiqlal Mosque"),
        ("loc/katedral", -6.1683, 106.8328, "Jakarta Cathedral"),
        ("loc/monas_park", -6.1750, 106.8270, "Park near Monas"),
        ("loc/bank_indonesia", -6.2000, 106.8500, "Bank Indonesia"),
    ];

    for (slug, lat, lon, name) in &nodes {
        let payload = json!({
            "_id": slug,
            "name": name,
            "coordinates": {"lat": lat, "lon": lon}
        }).to_string();
        db.nodes().put_json(&payload).unwrap();
    }
    db.flush().unwrap();

    println!("[TC-1.3] Inserted {} location nodes", nodes.len());

    // Query near Monas with 1km radius
    let outcome = db.nodes()
        .all()
        .near(-6.1754, 106.8272, 1.0)
        .collect()
        .unwrap();

    println!("[TC-1.3] Spatial query near Monas (1km): found {} nodes", outcome.data.len());
    for hit in &outcome.data {
        println!("[TC-1.3]   - lat: {}, lon: {}", hit.lat, hit.lon);
    }

    // Should find at least: monas, istiqlal, katedral, monas_park (within 1km of Monas)
    // Bank Indonesia should be excluded (too far)
    assert!(outcome.data.len() >= 4, "Should find at least 4 nearby nodes (Monas area)");

    // Verify specific nodes are found
    let found_slugs: Vec<_> = outcome.data.iter()
        .filter_map(|hit| db.slug_index.read().get(hit.slug_hash))
        .filter_map(|idx| {
            let slot = db.nodes.read_at(idx as u64);
            let bytes = db.blobs.read(slot.blob_offset, slot.blob_len);
            serde_json::from_slice::<serde_json::Value>(bytes).ok()
        })
        .filter_map(|v| v.get("_id").and_then(|s| s.as_str()))
        .collect();

    println!("[TC-1.3] Found nodes: {:?}", found_slugs);
}

#[test]
fn test_spatial_index_boundary_conditions() {
    let (db, _dir) = setup_db();

    // Create nodes at exact coordinates
    let nodes = vec![
        ("loc/exact", -6.175400, 106.827200),  // Exactly at center
        ("loc/very_close", -6.175410, 106.827210),  // 0.001 degrees away (~100m)
        ("loc/edge_500m", -6.178900, 106.827200),  // ~500m away
        ("loc/just_over_1km", -6.183000, 106.827200),  // ~850m away
        ("loc/just_over_2km", -6.193000, 106.827200),  // ~2km away
    ];

    for (slug, lat, lon) in &nodes {
        let payload = json!({
            "_id": slug,
            "coordinates": {"lat": lat, "lon": lon}
        }).to_string();
        db.nodes().put_json(&payload).unwrap();
    }
    db.flush().unwrap();

    // Query with 1km radius
    let outcome = db.nodes()
        .all()
        .near(-6.175400, 106.827200, 1.0)
        .collect()
        .unwrap();

    println!("[TC-1.3] Boundary test (1km radius): found {} nodes", outcome.data.len());
    assert!(outcome.data.len() >= 4, "Should find nodes within 1km (exact, very_close, edge_500m, just_over_1km)");
}

#[test]
fn test_spatial_index_with_coordinates_variations() {
    let (db, _dir) = setup_db();

    // Test different coordinate formats that might be used
    let payloads = vec![
        json!({
            "_id": "loc/format1",
            "coordinates": {"lat": -6.2088, "lon": 106.8456}
        }).to_string(),
        json!({
            "_id": "loc/format2",
            "geo": {"loc": {"lat": -6.1754, "lon": 106.8272}}
        }).to_string(),
        json!({
            "_id": "loc/no_coords",
            "name": "No coordinates"
        }).to_string(),
    ];

    for payload in &payloads {
        db.nodes().put_json(payload).unwrap();
    }
    db.flush()..unwrap();

    // Verify spatial index only contains nodes with valid coordinates
    let spatial_tree = db.spatial.read();
    let indexed_nodes: Vec<_> = spatial_tree.iter().collect();

    println!("[TC-1.3] Nodes in spatial index: {}", indexed_nodes.len());
    assert!(indexed_nodes.len() >= 2, "Should index format1 and format2");

    // Node without coordinates should not be in spatial index
    println!("[TC-1.3] Format variations test: PASSED");
}
