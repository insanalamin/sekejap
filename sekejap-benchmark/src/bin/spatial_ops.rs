use rusqlite::{params, Connection};
use sekejap::SekejapDB;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::tempdir;

const TOTAL_RECORDS: usize = 5_000;

#[derive(Clone)]
struct SpatialRecord {
    id: String,
    geometry: String,
    lat: f64,
    lon: f64,
}

struct BenchmarkCase {
    name: &'static str,
    atomic_ms: f64,
    sql_ms: f64,
    sqlite_ms: f64,
    note: &'static str,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dataset = build_dataset(TOTAL_RECORDS);
    let temp = tempdir()?;
    let base = temp.path();

    let cases = vec![
        benchmark_spatial_insert(base, &dataset)?,
        benchmark_near_centroid_count(base, &dataset)?,
        benchmark_st_dwithin_count(base, &dataset)?,
        benchmark_st_dwithin_id_only(base, &dataset)?,
        benchmark_st_within_count(base, &dataset)?,
        benchmark_st_within_id_only(base, &dataset)?,
        benchmark_st_intersects_count(base, &dataset)?,
        benchmark_st_intersects_id_only(base, &dataset)?,
    ];

    let markdown = render_markdown(&cases);
    let out_path =
        PathBuf::from(r"C:\Users\Insan\AndroidStudioProjects\SekejapDB\sekejap-benchmark\techniques\spatial-ops\RESULT.md");
    fs::create_dir_all(out_path.parent().unwrap())?;
    fs::write(&out_path, markdown)?;
    println!("wrote {}", out_path.display());
    Ok(())
}

fn benchmark_spatial_insert(base: &Path, records: &[SpatialRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running spatial_insert_5000");
    let atomic_db = create_empty_sekejap_atomic(&base.join("insert_atomic"))?;
    let sql_db = create_empty_sekejap_sql(&base.join("insert_sql"))?;
    let mut sqlite_conn = create_empty_sqlite(&base.join("insert_sqlite.sqlite"))?;

    let atomic_ms = timed_ms(|| {
        let items = records
            .iter()
            .map(|r| (format!("places/{}", r.id), payload_json_string(r)))
            .collect::<Vec<_>>();
        let refs = items
            .iter()
            .map(|(slug, json)| (slug.as_str(), json.as_str()))
            .collect::<Vec<_>>();
        atomic_db.nodes().ingest_raw(&refs).unwrap();
    });
    let sql_ms = timed_ms(|| {
        for chunk in records.chunks(250) {
            sql_db.mutate(&sql_insert_batch(chunk)).unwrap();
        }
    });
    let sqlite_ms = timed_ms(|| {
        let tx = sqlite_conn.unchecked_transaction().unwrap();
        for record in records {
            insert_sqlite_record(&tx, record).unwrap();
        }
        tx.commit().unwrap();
    });

    Ok(BenchmarkCase {
        name: "spatial_insert_5000",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "geometry insert with centroid and bbox extraction",
    })
}

fn benchmark_near_centroid_count(base: &Path, records: &[SpatialRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running near_centroid_count_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "near", records)?;
    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db
            .nodes()
            .collection("places")
            .near(-37.905, 145.115, 0.03)
            .take(200)
            .count()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db
            .count("SELECT id FROM places WHERE ST_DWithin(geometry, POINT(145.115 -37.905), 0.03) LIMIT 200")
            .unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let mut stmt = sqlite_conn
            .prepare("SELECT id FROM places WHERE ((lon - 145.115)*(lon - 145.115) + (lat + 37.905)*(lat + 37.905)) <= (?1 * ?1) LIMIT 200")
            .unwrap();
        let _rows: Vec<String> = stmt
            .query_map(params![0.03f64], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
    });

    Ok(BenchmarkCase {
        name: "near_centroid_count_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "centroid-backed radius filter via near/ST_DWithin repeated 100x",
    })
}

fn benchmark_st_dwithin_count(base: &Path, records: &[SpatialRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running st_dwithin_count_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "dwithin_count", records)?;
    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db
            .nodes()
            .collection("places")
            .st_dwithin(-37.905, 145.115, 0.03)
            .take(200)
            .count()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db
            .count("SELECT id FROM places WHERE ST_DWithin(geometry, POINT(145.115 -37.905), 0.03) LIMIT 200")
            .unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let mut stmt = sqlite_conn
            .prepare("SELECT id FROM places WHERE ((lon - 145.115)*(lon - 145.115) + (lat + 37.905)*(lat + 37.905)) <= (?1 * ?1) LIMIT 200")
            .unwrap();
        let _rows: Vec<String> = stmt
            .query_map(params![0.03f64], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
    });

    Ok(BenchmarkCase {
        name: "st_dwithin_count_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "PostGIS-style centroid distance filter cost only repeated 100x",
    })
}

fn benchmark_st_dwithin_id_only(base: &Path, records: &[SpatialRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running st_dwithin_id_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "dwithin_id", records)?;
    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db
            .nodes()
            .collection("places")
            .st_dwithin(-37.905, 145.115, 0.03)
            .take(200)
            .select(&["id"])
            .collect()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db
            .query("SELECT id FROM places WHERE ST_DWithin(geometry, POINT(145.115 -37.905), 0.03) LIMIT 200")
            .unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let mut stmt = sqlite_conn
            .prepare("SELECT id FROM places WHERE ((lon - 145.115)*(lon - 145.115) + (lat + 37.905)*(lat + 37.905)) <= (?1 * ?1) LIMIT 200")
            .unwrap();
        let _rows: Vec<String> = stmt
            .query_map(params![0.03f64], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
    });

    Ok(BenchmarkCase {
        name: "st_dwithin_id_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "PostGIS-style centroid distance with id projection repeated 100x",
    })
}

fn benchmark_st_within_count(base: &Path, records: &[SpatialRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running st_within_count_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "within_count", records)?;
    let polygon = polygon_ring();
    let sql_query = format!(
        "SELECT id FROM places WHERE ST_Within(geometry, POLYGON(({}))) LIMIT 200",
        polygon_wkt_inner()
    );
    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db
            .nodes()
            .collection("places")
            .st_within(polygon.clone())
            .take(200)
            .count()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db.count(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let (min_lat, max_lat, min_lon, max_lon) = polygon_bbox();
        let mut stmt = sqlite_conn
            .prepare("SELECT id FROM places WHERE lat >= ?1 AND lat <= ?2 AND lon >= ?3 AND lon <= ?4 LIMIT 200")
            .unwrap();
        let _rows: Vec<String> = stmt
            .query_map(params![min_lat, max_lat, min_lon, max_lon], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
    });

    Ok(BenchmarkCase {
        name: "st_within_count_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "point-in-polygon within filter cost only repeated 100x",
    })
}

fn benchmark_st_within_id_only(base: &Path, records: &[SpatialRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running st_within_id_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "within_id", records)?;
    let polygon = polygon_ring();
    let sql_query = format!(
        "SELECT id FROM places WHERE ST_Within(geometry, POLYGON(({}))) LIMIT 200",
        polygon_wkt_inner()
    );
    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db
            .nodes()
            .collection("places")
            .st_within(polygon.clone())
            .take(200)
            .select(&["id"])
            .collect()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db.query(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let (min_lat, max_lat, min_lon, max_lon) = polygon_bbox();
        let mut stmt = sqlite_conn
            .prepare("SELECT id FROM places WHERE lat >= ?1 AND lat <= ?2 AND lon >= ?3 AND lon <= ?4 LIMIT 200")
            .unwrap();
        let _rows: Vec<String> = stmt
            .query_map(params![min_lat, max_lat, min_lon, max_lon], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
    });

    Ok(BenchmarkCase {
        name: "st_within_id_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "point-in-polygon within with id projection repeated 100x",
    })
}

fn benchmark_st_intersects_count(base: &Path, records: &[SpatialRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running st_intersects_count_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "intersects_count", records)?;
    let polygon = polygon_ring();
    let sql_query = format!(
        "SELECT id FROM places WHERE ST_Intersects(geometry, POLYGON(({}))) LIMIT 200",
        polygon_wkt_inner()
    );
    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db
            .nodes()
            .collection("places")
            .st_intersects(polygon.clone())
            .take(200)
            .count()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db.count(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let (min_lat, max_lat, min_lon, max_lon) = polygon_bbox();
        let mut stmt = sqlite_conn
            .prepare("SELECT id FROM places WHERE lat >= ?1 AND lat <= ?2 AND lon >= ?3 AND lon <= ?4 LIMIT 200")
            .unwrap();
        let _rows: Vec<String> = stmt
            .query_map(params![min_lat, max_lat, min_lon, max_lon], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
    });

    Ok(BenchmarkCase {
        name: "st_intersects_count_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "point-intersects-polygon filter cost only repeated 100x",
    })
}

fn benchmark_st_intersects_id_only(base: &Path, records: &[SpatialRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running st_intersects_id_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "intersects_id", records)?;
    let polygon = polygon_ring();
    let sql_query = format!(
        "SELECT id FROM places WHERE ST_Intersects(geometry, POLYGON(({}))) LIMIT 200",
        polygon_wkt_inner()
    );
    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db
            .nodes()
            .collection("places")
            .st_intersects(polygon.clone())
            .take(200)
            .select(&["id"])
            .collect()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db.query(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let (min_lat, max_lat, min_lon, max_lon) = polygon_bbox();
        let mut stmt = sqlite_conn
            .prepare("SELECT id FROM places WHERE lat >= ?1 AND lat <= ?2 AND lon >= ?3 AND lon <= ?4 LIMIT 200")
            .unwrap();
        let _rows: Vec<String> = stmt
            .query_map(params![min_lat, max_lat, min_lon, max_lon], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
    });

    Ok(BenchmarkCase {
        name: "st_intersects_id_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "point-intersects-polygon with id projection repeated 100x",
    })
}

fn provision_all(
    base: &Path,
    label: &str,
    records: &[SpatialRecord],
) -> Result<(SekejapDB, SekejapDB, Connection), Box<dyn std::error::Error>> {
    let atomic_dir = base.join(format!("{}_atomic", label));
    let sql_dir = base.join(format!("{}_sql", label));
    let sqlite_path = base.join(format!("{}_sqlite.sqlite", label));
    let atomic_db = create_empty_sekejap_atomic(&atomic_dir)?;
    let sql_db = create_empty_sekejap_sql(&sql_dir)?;
    let mut sqlite_conn = create_empty_sqlite(&sqlite_path)?;

    let items = records
        .iter()
        .map(|r| (format!("places/{}", r.id), payload_json_string(r)))
        .collect::<Vec<_>>();
    let refs = items
        .iter()
        .map(|(slug, json)| (slug.as_str(), json.as_str()))
        .collect::<Vec<_>>();
    atomic_db.nodes().ingest_raw(&refs)?;

    for chunk in records.chunks(250) {
        sql_db.mutate(&sql_insert_batch(chunk))?;
    }

    let tx = sqlite_conn.unchecked_transaction()?;
    for record in records {
        insert_sqlite_record(&tx, record)?;
    }
    tx.commit()?;

    Ok((atomic_db, sql_db, sqlite_conn))
}

fn create_empty_sekejap_atomic(path: &Path) -> Result<SekejapDB, Box<dyn std::error::Error>> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    let db = SekejapDB::new(path, TOTAL_RECORDS * 2)?;
    db.schema().define(
        "places",
        &json!({
            "hot_fields": {
                "hash_index": ["id"],
                "spatial": ["geometry"]
            }
        })
        .to_string(),
    )?;
    Ok(db)
}

fn create_empty_sekejap_sql(path: &Path) -> Result<SekejapDB, Box<dyn std::error::Error>> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    let db = SekejapDB::new(path, TOTAL_RECORDS * 2)?;
    db.mutate(
        "CREATE COLLECTION places (\
            id UUID PRIMARY KEY DEFAULT uuidv4(),\
            geometry GEOMETRY\
        ) WITH (\
            hash_index = [id],\
            spatial_index = [geometry]\
        )",
    )?;
    Ok(db)
}

fn create_empty_sqlite(path: &Path) -> Result<Connection, Box<dyn std::error::Error>> {
    if path.exists() {
        fs::remove_file(path)?;
    }
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "CREATE TABLE places (\
            id TEXT PRIMARY KEY,\
            geometry_json TEXT NOT NULL,\
            lat REAL NOT NULL,\
            lon REAL NOT NULL\
        );\
        CREATE INDEX idx_places_lat_lon ON places(lat, lon);",
    )?;
    Ok(conn)
}

fn payload_json_string(record: &SpatialRecord) -> String {
    json!({
        "id": record.id,
        "geometry": serde_json::from_str::<serde_json::Value>(&record.geometry).unwrap()
    })
    .to_string()
}

fn insert_sqlite_record(conn: &Connection, record: &SpatialRecord) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute(
        "INSERT INTO places (id, geometry_json, lat, lon) VALUES (?1, ?2, ?3, ?4)",
        params![record.id, record.geometry, record.lat, record.lon],
    )?;
    Ok(())
}

fn sql_insert_batch(records: &[SpatialRecord]) -> String {
    let values = records
        .iter()
        .map(|record| {
            format!(
                "('{}', '{}')",
                escape_sql(&record.id),
                escape_sql(&record.geometry)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO places (id, geometry) VALUES {values}")
}

fn escape_sql(value: &str) -> String {
    value.replace('\'', "''")
}

fn build_dataset(n: usize) -> Vec<SpatialRecord> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let lat = -37.92 + (i as f64 * 0.00003);
        let lon = 145.09 + (i as f64 * 0.00004);
        let geometry = json!({
            "type": "Point",
            "coordinates": [lon, lat]
        })
        .to_string();
        out.push(SpatialRecord {
            id: format!("place_{i:05}"),
            geometry,
            lat,
            lon,
        });
    }
    out
}

fn polygon_ring() -> Vec<[f32; 2]> {
    vec![
        [-37.910, 145.100],
        [-37.910, 145.140],
        [-37.885, 145.140],
        [-37.885, 145.100],
        [-37.910, 145.100],
    ]
}

fn polygon_wkt_inner() -> String {
    polygon_ring()
        .iter()
        .map(|p| format!("{} {}", p[1], p[0]))
        .collect::<Vec<_>>()
        .join(", ")
}

fn polygon_bbox() -> (f64, f64, f64, f64) {
    (-37.910, -37.885, 145.100, 145.140)
}

fn timed_loop_ms<F: FnMut()>(iterations: usize, mut f: F) -> f64 {
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    start.elapsed().as_secs_f64() * 1000.0
}

fn timed_ms<F: FnOnce()>(f: F) -> f64 {
    let start = Instant::now();
    f();
    start.elapsed().as_secs_f64() * 1000.0
}

fn render_markdown(cases: &[BenchmarkCase]) -> String {
    let mut out = String::new();
    out.push_str("# Spatial Ops Benchmark\n\n");
    out.push_str("Compare Sekejap Atomic vs Sekejap SQL vs SQLite on centroid-backed distance, within, and intersect spatial operations.\n\n");
    out.push_str("| Case | Sekejap Atomic ms | Sekejap SQL ms | SQLite ms | Note |\n");
    out.push_str("|---|---:|---:|---:|---|\n");
    for case in cases {
        out.push_str(&format!(
            "| {} | {:.3} | {:.3} | {:.3} | {} |\n",
            case.name, case.atomic_ms, case.sql_ms, case.sqlite_ms, case.note
        ));
    }
    out
}
