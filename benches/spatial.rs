//! Benchmark: sekejap SQL vs sekejap SQL vs SQLite — spatial queries
//!
//! Dataset: 10,000 GeoJSON Point nodes around Melbourne, 100 polygon zones, 100 routes.
//! Scenarios: st_dwithin, st_within, st_contains_point, st_intersects, combined

use criterion::{criterion_group, criterion_main, Criterion, black_box};
use rusqlite::params;
use serde_json::json;

// Melbourne CBD reference point
const CENTRE_LAT: f64 = -37.8102;
const CENTRE_LON: f64 = 144.9631;

// ── Setup helpers ────────────────────────────────────────────────────────────

fn setup_core() -> sekejap::CoreDB {
    let mut db = sekejap::CoreDB::new();

    for i in 0..10_000usize {
        let lat = -37.6 - (i as f64 % 600.0) * 0.001;
        let lon = 144.5 + (i as f64 % 1000.0) * 0.001;
        let cat = if i % 5 == 0 { "landmark" } else { "shop" };
        db.put(
            &format!("places/pt{i}"),
            &json!({
                "_collection": "places",
                "_key": format!("pt{i}"),
                "category": cat,
                "name": format!("Place {i}"),
                "geometry": { "type": "Point", "coordinates": [lon, lat] }
            }).to_string(),
        ).unwrap();
    }

    for i in 0..100usize {
        let base_lat = -37.7 - (i as f64 % 50.0) * 0.01;
        let base_lon = 144.8 + (i as f64 % 50.0) * 0.01;
        let d = 0.01;
        db.put(
            &format!("zones/z{i}"),
            &json!({
                "_collection": "zones",
                "_key": format!("z{i}"),
                "name": format!("Zone {i}"),
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [base_lon, base_lat],
                        [base_lon + d, base_lat],
                        [base_lon + d, base_lat - d],
                        [base_lon, base_lat - d],
                        [base_lon, base_lat]
                    ]]
                }
            }).to_string(),
        ).unwrap();
    }

    for i in 0..100usize {
        let base_lat = -37.7 - (i as f64 % 50.0) * 0.01;
        let base_lon = 144.8 + (i as f64 % 50.0) * 0.01;
        db.put(
            &format!("routes/r{i}"),
            &json!({
                "_collection": "routes",
                "_key": format!("r{i}"),
                "name": format!("Route {i}"),
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [base_lon, base_lat],
                        [base_lon + 0.01, base_lat - 0.005],
                        [base_lon + 0.02, base_lat - 0.01],
                        [base_lon + 0.03, base_lat - 0.005],
                        [base_lon + 0.04, base_lat]
                    ]
                }
            }).to_string(),
        ).unwrap();
    }

    db.build_spatial_index();
    db
}

fn setup_sekejap() -> (sekejap::CoreDB, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut db = sekejap::CoreDB::open(dir.path()).unwrap();

    for i in 0..10_000usize {
        let lat = -37.6 - (i as f64 % 600.0) * 0.001;
        let lon = 144.5 + (i as f64 % 1000.0) * 0.001;
        let cat = if i % 5 == 0 { "landmark" } else { "shop" };
        db.put(
            &format!("places/pt{i}"),
            &json!({
                "_collection": "places",
                "_key": format!("pt{i}"),
                "category": cat,
                "name": format!("Place {i}"),
                "geometry": { "type": "Point", "coordinates": [lon, lat] }
            }).to_string(),
        ).unwrap();
    }

    for i in 0..100usize {
        let base_lat = -37.7 - (i as f64 % 50.0) * 0.01;
        let base_lon = 144.8 + (i as f64 % 50.0) * 0.01;
        let d = 0.01;
        db.put(
            &format!("zones/z{i}"),
            &json!({
                "_collection": "zones",
                "_key": format!("z{i}"),
                "name": format!("Zone {i}"),
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [base_lon, base_lat],
                        [base_lon + d, base_lat],
                        [base_lon + d, base_lat - d],
                        [base_lon, base_lat - d],
                        [base_lon, base_lat]
                    ]]
                }
            }).to_string(),
        ).unwrap();
    }

    for i in 0..100usize {
        let base_lat = -37.7 - (i as f64 % 50.0) * 0.01;
        let base_lon = 144.8 + (i as f64 % 50.0) * 0.01;
        db.put(
            &format!("routes/r{i}"),
            &json!({
                "_collection": "routes",
                "_key": format!("r{i}"),
                "name": format!("Route {i}"),
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [base_lon, base_lat],
                        [base_lon + 0.01, base_lat - 0.005],
                        [base_lon + 0.02, base_lat - 0.01],
                        [base_lon + 0.03, base_lat - 0.005],
                        [base_lon + 0.04, base_lat]
                    ]
                }
            }).to_string(),
        ).unwrap();
    }

    db.build_spatial_index();
    (db, dir)
}

fn setup_sqlite() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE places (
            key TEXT PRIMARY KEY, category TEXT, name TEXT, lat REAL, lon REAL
        );
        CREATE TABLE zones (
            key TEXT PRIMARY KEY, name TEXT, min_lat REAL, max_lat REAL, min_lon REAL, max_lon REAL
        );
        CREATE TABLE routes (
            key TEXT PRIMARY KEY, name TEXT, lat1 REAL, lon1 REAL, lat2 REAL, lon2 REAL
        );
        CREATE VIRTUAL TABLE places_rtree USING rtree(id, min_lat, max_lat, min_lon, max_lon);
        CREATE TABLE places_id (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT);
        CREATE INDEX idx_places_cat ON places(category);"
    ).unwrap();

    conn.execute_batch("BEGIN").unwrap();
    {
        let mut stmt_place = conn.prepare(
            "INSERT INTO places (key, category, name, lat, lon) VALUES (?1,?2,?3,?4,?5)"
        ).unwrap();
        let mut stmt_id = conn.prepare(
            "INSERT INTO places_id (key) VALUES (?1)"
        ).unwrap();
        let mut stmt_rtree = conn.prepare(
            "INSERT INTO places_rtree (id, min_lat, max_lat, min_lon, max_lon) VALUES (?1,?2,?2,?3,?3)"
        ).unwrap();

        for i in 0..10_000usize {
            let lat = -37.6 - (i as f64 % 600.0) * 0.001;
            let lon = 144.5 + (i as f64 % 1000.0) * 0.001;
            let cat = if i % 5 == 0 { "landmark" } else { "shop" };
            let key = format!("pt{i}");
            stmt_place.execute(params![key, cat, format!("Place {i}"), lat, lon]).unwrap();
            stmt_id.execute(params![key]).unwrap();
            let id = conn.last_insert_rowid();
            stmt_rtree.execute(params![id, lat, lon]).unwrap();
        }
    }
    {
        let mut stmt_zone = conn.prepare(
            "INSERT INTO zones (key, name, min_lat, max_lat, min_lon, max_lon) VALUES (?1,?2,?3,?4,?5,?6)"
        ).unwrap();
        for i in 0..100usize {
            let base_lat = -37.7 - (i as f64 % 50.0) * 0.01;
            let base_lon = 144.8 + (i as f64 % 50.0) * 0.01;
            let d = 0.01;
            stmt_zone.execute(params![
                format!("z{i}"), format!("Zone {i}"),
                base_lat - d, base_lat, base_lon, base_lon + d
            ]).unwrap();
        }
    }
    conn.execute_batch("COMMIT").unwrap();
    conn
}

// ── st_dwithin ───────────────────────────────────────────────────────────────
// 10k points, proximity search around Melbourne CBD
// core: Haversine km | sekejap: Euclidean degree | SQLite: R*Tree bbox

fn bench_st_dwithin(c: &mut Criterion) {
    let core_db = setup_core();
    let (sk_db, _dir) = setup_sekejap();
    let sqlite = setup_sqlite();

    let mut group = c.benchmark_group("st_dwithin_10k");

    // core: Haversine 5km
    group.bench_function("core", |b| {
        b.iter(|| black_box(
            core_db.query(
                "SELECT * FROM places WHERE ST_DWithin(geometry, POINT(144.9631 -37.8102), 5.0)"
            ).unwrap().count()
        ))
    });

    // sekejap: degree-based distance (~0.045 ≈ 5km at Melbourne lat)
    group.bench_function("sekejap", |b| {
        b.iter(|| black_box(
            sk_db.query(
                "SELECT * FROM places WHERE ST_DWithin(geometry, POINT(144.9631 -37.8102), 0.045)"
            ).unwrap().count()
        ))
    });

    // SQLite: R*Tree bounding box (~0.045° around centre)
    group.bench_function("sqlite_rtree", |b| {
        b.iter(|| {
            let mut stmt = sqlite.prepare_cached(
                "SELECT COUNT(*) FROM places_id pi
                 JOIN places_rtree r ON pi.id = r.id
                 WHERE r.min_lat >= ?1 AND r.max_lat <= ?2
                   AND r.min_lon >= ?3 AND r.max_lon <= ?4"
            ).unwrap();
            let count: i64 = stmt.query_row(
                params![CENTRE_LAT - 0.045, CENTRE_LAT + 0.045, CENTRE_LON - 0.045, CENTRE_LON + 0.045],
                |r| r.get(0),
            ).unwrap();
            black_box(count)
        })
    });

    group.finish();
}

// ── st_within ────────────────────────────────────────────────────────────────
// Points within a polygon (CBD box)

fn bench_st_within(c: &mut Criterion) {
    let core_db = setup_core();
    let (sk_db, _dir) = setup_sekejap();
    let sqlite = setup_sqlite();

    let mut group = c.benchmark_group("st_within_polygon");

    group.bench_function("core", |b| {
        b.iter(|| black_box(
            core_db.query(
                "SELECT * FROM places WHERE ST_Within(geometry, POLYGON((144.95 -37.80, 144.98 -37.80, 144.98 -37.83, 144.95 -37.83, 144.95 -37.80)))"
            ).unwrap().count()
        ))
    });

    group.bench_function("sekejap", |b| {
        b.iter(|| black_box(
            sk_db.query(
                "SELECT * FROM places WHERE ST_Within(geometry, POLYGON((144.95 -37.80, 144.98 -37.80, 144.98 -37.83, 144.95 -37.83, 144.95 -37.80)))"
            ).unwrap().count()
        ))
    });

    // SQLite: bbox approximation (equivalent rectangle)
    group.bench_function("sqlite_bbox", |b| {
        b.iter(|| {
            let mut stmt = sqlite.prepare_cached(
                "SELECT COUNT(*) FROM places
                 WHERE lat >= -37.83 AND lat <= -37.80
                   AND lon >= 144.95 AND lon <= 144.98"
            ).unwrap();
            let count: i64 = stmt.query_row([], |r| r.get(0)).unwrap();
            black_box(count)
        })
    });

    group.finish();
}

// ── st_contains_point ────────────────────────────────────────────────────────
// Reverse geocoding: which zones contain a point? (100 polygon zones)

fn bench_st_contains_point(c: &mut Criterion) {
    let core_db = setup_core();
    let (sk_db, _dir) = setup_sekejap();
    let sqlite = setup_sqlite();

    let mut group = c.benchmark_group("st_contains_point");

    group.bench_function("core", |b| {
        b.iter(|| black_box(
            core_db.query(
                "SELECT * FROM zones WHERE ST_Contains(geometry, POINT(144.85 -37.75))"
            ).unwrap().count()
        ))
    });

    group.bench_function("sekejap", |b| {
        b.iter(|| black_box(
            sk_db.query(
                "SELECT * FROM zones WHERE ST_Contains(geometry, POINT(144.85 -37.75))"
            ).unwrap().count()
        ))
    });

    // SQLite: bbox containment check (simplified — real polygon would need extension)
    group.bench_function("sqlite_bbox", |b| {
        b.iter(|| {
            let mut stmt = sqlite.prepare_cached(
                "SELECT COUNT(*) FROM zones
                 WHERE ?1 >= min_lat AND ?1 <= max_lat
                   AND ?2 >= min_lon AND ?2 <= max_lon"
            ).unwrap();
            let count: i64 = stmt.query_row(
                params![-37.75, 144.85],
                |r| r.get(0),
            ).unwrap();
            black_box(count)
        })
    });

    group.finish();
}

// ── st_intersects ────────────────────────────────────────────────────────────
// Which zones intersect a query polygon? (100 zones)

fn bench_st_intersects(c: &mut Criterion) {
    let core_db = setup_core();
    let (sk_db, _dir) = setup_sekejap();
    let sqlite = setup_sqlite();

    let mut group = c.benchmark_group("st_intersects_zones");

    group.bench_function("core", |b| {
        b.iter(|| black_box(
            core_db.query(
                "SELECT * FROM zones WHERE ST_Intersects(geometry, POLYGON((144.82 -37.72, 144.88 -37.72, 144.88 -37.78, 144.82 -37.78, 144.82 -37.72)))"
            ).unwrap().count()
        ))
    });

    group.bench_function("sekejap", |b| {
        b.iter(|| black_box(
            sk_db.query(
                "SELECT * FROM zones WHERE ST_Intersects(geometry, POLYGON((144.82 -37.72, 144.88 -37.72, 144.88 -37.78, 144.82 -37.78, 144.82 -37.72)))"
            ).unwrap().count()
        ))
    });

    // SQLite: bbox overlap check
    group.bench_function("sqlite_bbox", |b| {
        b.iter(|| {
            let mut stmt = sqlite.prepare_cached(
                "SELECT COUNT(*) FROM zones
                 WHERE max_lat >= -37.78 AND min_lat <= -37.72
                   AND max_lon >= 144.82 AND min_lon <= 144.88"
            ).unwrap();
            let count: i64 = stmt.query_row([], |r| r.get(0)).unwrap();
            black_box(count)
        })
    });

    group.finish();
}

// ── combined: spatial + attribute filter ──────────────────────────────────────

fn bench_combined(c: &mut Criterion) {
    let core_db = setup_core();
    let (sk_db, _dir) = setup_sekejap();
    let sqlite = setup_sqlite();

    let mut group = c.benchmark_group("spatial_plus_filter");

    group.bench_function("core", |b| {
        b.iter(|| black_box(
            core_db.query(
                "SELECT * FROM places WHERE ST_DWithin(geometry, POINT(144.9631 -37.8102), 5.0) AND category = 'landmark'"
            ).unwrap().count()
        ))
    });

    group.bench_function("sekejap", |b| {
        b.iter(|| black_box(
            sk_db.query(
                "SELECT * FROM places WHERE ST_DWithin(geometry, POINT(144.9631 -37.8102), 0.045) AND category = 'landmark'"
            ).unwrap().count()
        ))
    });

    group.bench_function("sqlite", |b| {
        b.iter(|| {
            let mut stmt = sqlite.prepare_cached(
                "SELECT COUNT(*) FROM places_id pi
                 JOIN places_rtree r ON pi.id = r.id
                 JOIN places p ON p.key = pi.key
                 WHERE r.min_lat >= ?1 AND r.max_lat <= ?2
                   AND r.min_lon >= ?3 AND r.max_lon <= ?4
                   AND p.category = 'landmark'"
            ).unwrap();
            let count: i64 = stmt.query_row(
                params![CENTRE_LAT - 0.045, CENTRE_LAT + 0.045, CENTRE_LON - 0.045, CENTRE_LON + 0.045],
                |r| r.get(0),
            ).unwrap();
            black_box(count)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_st_dwithin,
    bench_st_within,
    bench_st_contains_point,
    bench_st_intersects,
    bench_combined,
);
criterion_main!(benches);
