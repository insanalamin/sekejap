use chrono::NaiveDateTime;
use rand::{rngs::StdRng, Rng, SeedableRng};
use rusqlite::{params, Connection};
use sekejap::{SekejapDB, TimeQuery};
use serde_json::{json, json as serde_json, Value};
use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::tempdir;

const TOTAL_RECORDS: usize = 2_000;
const INSERT_COUNT: usize = 2_000;
const VECTOR_DIM: usize = 128;
const GRAPH_FANOUT: usize = 3;
const TOP_K: usize = 10;

#[derive(Clone)]
struct MemoryRecord {
    id: String,
    title: String,
    story: String,
    weather: &'static str,
    created_at: String,
    created_epoch_micros: i64,
    remembered_time: String,
    geometry: String,
    lat: f64,
    lon: f64,
    embedding: Vec<f32>,
    start_year: i64,
    end_year: i64,
    related_to: Vec<String>,
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
        benchmark_insert_multimodal_1000(base, &dataset)?,
        benchmark_graph_traverse(base, &dataset)?,
        benchmark_spatial_distance(base, &dataset)?,
        benchmark_exact_time_range_count(base, &dataset)?,
        benchmark_exact_time_range_id_only(base, &dataset)?,
        benchmark_exact_time_range_full_payload(base, &dataset)?,
        benchmark_memories_near_place_in_vague_time_count(base, &dataset)?,
        benchmark_memories_near_place_in_vague_time_id_only(base, &dataset)?,
        benchmark_memories_near_place_in_vague_time_full_payload(base, &dataset)?,
        benchmark_hybrid_fulltext_vague_spatial_count(base, &dataset)?,
        benchmark_hybrid_fulltext_vague_spatial_id_only(base, &dataset)?,
        benchmark_hybrid_fulltext_vague_spatial_full_payload(base, &dataset)?,
    ];

    let markdown = render_markdown(&cases);
    let out_path = PathBuf::from(r"C:\Users\Insan\AndroidStudioProjects\SekejapDB\sekejap-benchmark\cases\memory-time-space\RESULT.md");
    fs::create_dir_all(out_path.parent().unwrap())?;
    fs::write(&out_path, markdown)?;
    println!("wrote {}", out_path.display());
    Ok(())
}

fn benchmark_insert_multimodal_1000(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running insert_memory_time_space_2000");
    let atomic_db = create_empty_sekejap_atomic(&base.join("insert_atomic"))?;
    let sql_db = create_empty_sekejap_sql(&base.join("insert_sql"))?;
    let mut sqlite_conn = create_empty_sqlite(&base.join("insert_sqlite.sqlite"))?;

    let atomic_ms = timed_ms(|| {
        bulk_insert_atomic_nodes(&atomic_db, records).unwrap();
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
        name: "insert_memory_time_space_2000",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "2,000 memory records with exact time, vague time, spatial, text, and relation edges",
    })
}

fn benchmark_graph_traverse(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running memory_anchor_to_related_nearby_memories");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "graph", records)?;
    let source = &records[600];
    let source_id = &source.id;
    let source_slug = format!("memories/{}", source_id);
    let sql_query = format!("SELECT id FROM memories TRAVERSE FORWARD related_to TO memories WHERE id = '{}' AND ST_DWithin(geometry, POINT(145.115 -37.905), 0.03) LIMIT 20", escape_sql(source_id));

    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db.nodes().one(&source_slug).forward("related_to").st_dwithin(-37.905, 145.115, 0.03).take(20).collect().unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db.query(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let mut stmt = sqlite_conn.prepare("SELECT m.id FROM memory_edges e JOIN memories m ON m.id = e.to_id WHERE e.from_id = ?1 AND ((m.lon - 145.115)*(m.lon - 145.115) + (m.lat + 37.905)*(m.lat + 37.905)) <= (?2 * ?2) LIMIT 20").unwrap();
        let _rows: Vec<String> = stmt.query_map(params![source_id, 0.03f64], |row| row.get(0)).unwrap().map(Result::unwrap).collect();
    });

    Ok(BenchmarkCase { name: "memory_anchor_to_related_nearby_memories", atomic_ms, sql_ms, sqlite_ms, note: "anchored related-memory traversal near the same area repeated 100x" })
}

fn benchmark_vector_similarity(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running vector_similarity");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "vector", records)?;
    let query_vec = &records[123].embedding;
    let sql_query = format!("SELECT id FROM memories ORDER BY embedding <=> {} LIMIT {}", vector_literal(query_vec), TOP_K);

    let atomic_ms = timed_loop_ms(25, || {
        let _ = atomic_db.nodes().collection("memories").similar(query_vec, TOP_K).take(TOP_K).collect().unwrap();
    });
    let sql_ms = timed_loop_ms(25, || {
        let _ = sql_db.explain(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(25, || {
        let mut stmt = sqlite_conn.prepare("SELECT id, embedding_json FROM memories").unwrap();
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let emb: String = row.get(1)?;
            Ok((id, emb))
        }).unwrap();
        let mut scored = Vec::new();
        for row in rows {
            let (id, emb_json) = row.unwrap();
            let emb: Vec<f32> = serde_json::from_str(&emb_json).unwrap();
            scored.push((id, cosine_distance(query_vec, &emb)));
        }
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        let _top: Vec<_> = scored.into_iter().take(TOP_K).collect();
    });

    Ok(BenchmarkCase { name: "vector_similarity", atomic_ms, sql_ms, sqlite_ms, note: "atomic executes; SQL uses explain-only because vector SQL runtime is unstable; SQLite brute-forces cosine" })
}

fn benchmark_spatial_distance(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running spatial_distance");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "spatial", records)?;
    let sql_query = "SELECT * FROM memories WHERE ST_DWithin(geometry, POINT(145.115 -37.905), 0.03) LIMIT 100";

    let atomic_ms = timed_loop_ms(80, || {
        let _ = atomic_db.nodes().collection("memories").st_dwithin(-37.905, 145.115, 0.03).take(100).collect().unwrap();
    });
    let sql_ms = timed_loop_ms(80, || {
        let _ = sql_db.query(sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(80, || {
        let mut stmt = sqlite_conn.prepare(
            "SELECT id, title, story, weather, created_at, remembered_time_json, geometry_json, embedding_json \
             FROM memories \
             WHERE ((lon - 145.115)*(lon - 145.115) + (lat + 37.905)*(lat + 37.905)) <= (?1 * ?1) \
             LIMIT 100"
        ).unwrap();
        let _rows: Vec<(String, String, String, String, String, String, String, String)> = stmt
            .query_map(params![0.03f64], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            })
            .unwrap()
            .map(Result::unwrap)
            .collect();
    });

    Ok(BenchmarkCase { name: "spatial_distance", atomic_ms, sql_ms, sqlite_ms, note: "spatial radius query with full payload repeated 80x" })
}

fn benchmark_exact_time_range_count(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running exact_time_range_count");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "exact_time", records)?;
    let start = parse_timestamp_to_micros("2014-01-10 00:00:00");
    let end = parse_timestamp_to_micros("2014-01-20 23:59:59");
    let sql_query = "SELECT id FROM memories WHERE created_at >= TIMESTAMP '2014-01-10 00:00:00' AND created_at <= TIMESTAMP '2014-01-20 23:59:59' LIMIT 200";

    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db
            .nodes()
            .collection("memories")
            .where_between("createdEpochMicros", start as f64, end as f64)
            .take(200)
            .count()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db.count(sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let mut stmt = sqlite_conn.prepare("SELECT id FROM memories WHERE created_epoch_micros BETWEEN ?1 AND ?2 LIMIT 200").unwrap();
        let _rows: Vec<String> = stmt.query_map(params![start, end], |row| row.get(0)).unwrap().map(Result::unwrap).collect();
    });

    Ok(BenchmarkCase { name: "exact_created_at_recent_memories_count_only", atomic_ms, sql_ms, sqlite_ms, note: "exact created-at range filter cost only via count/id scan repeated 100x" })
}

fn benchmark_exact_time_range_id_only(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running exact_time_range_id_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "exact_time_id_only", records)?;
    let start = parse_timestamp_to_micros("2014-01-10 00:00:00");
    let end = parse_timestamp_to_micros("2014-01-20 23:59:59");
    let sql_query = "SELECT id FROM memories WHERE created_at >= TIMESTAMP '2014-01-10 00:00:00' AND created_at <= TIMESTAMP '2014-01-20 23:59:59' LIMIT 200";

    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db
            .nodes()
            .collection("memories")
            .where_between("createdEpochMicros", start as f64, end as f64)
            .take(200)
            .select(&["id"])
            .collect()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db.query(sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let mut stmt = sqlite_conn.prepare("SELECT id FROM memories WHERE created_epoch_micros BETWEEN ?1 AND ?2 LIMIT 200").unwrap();
        let _rows: Vec<String> = stmt.query_map(params![start, end], |row| row.get(0)).unwrap().map(Result::unwrap).collect();
    });

    Ok(BenchmarkCase { name: "exact_created_at_recent_memories_id_only", atomic_ms, sql_ms, sqlite_ms, note: "exact created-at range with id-only projection repeated 100x" })
}

fn benchmark_exact_time_range_full_payload(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running exact_time_range_full_payload");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "exact_time_full_payload", records)?;
    let start = parse_timestamp_to_micros("2014-01-10 00:00:00");
    let end = parse_timestamp_to_micros("2014-01-20 23:59:59");
    let sql_query = "SELECT * FROM memories WHERE created_at >= TIMESTAMP '2014-01-10 00:00:00' AND created_at <= TIMESTAMP '2014-01-20 23:59:59' LIMIT 200";

    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db
            .nodes()
            .collection("memories")
            .where_between("createdEpochMicros", start as f64, end as f64)
            .take(200)
            .collect()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db.query(sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let mut stmt = sqlite_conn.prepare("SELECT * FROM memories WHERE created_epoch_micros BETWEEN ?1 AND ?2 LIMIT 200").unwrap();
        let _rows = stmt.query(params![start, end]).unwrap();
        drop(_rows);
    });

    Ok(BenchmarkCase { name: "exact_created_at_recent_memories_full_payload", atomic_ms, sql_ms, sqlite_ms, note: "exact created-at range with full payload hydration repeated 100x" })
}

fn benchmark_memories_near_place_in_vague_time_count(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running memories_near_place_in_vague_time_count");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "vague_spatial", records)?;
    let tq = TimeQuery {
        start_year: 2014,
        end_year: 2016,
        start_fuzz_years: 0,
        end_fuzz_years: 0,
        months: vec![],
        weekdays: vec![],
        days_of_month: vec![],
        time_of_day: None,
        recurrence_step_months: None,
        global_fuzziness: 0.0,
    };
    let sql_query = "SELECT id FROM memories WHERE VAGUE_TIME_INTERSECTS(remembered_time, 2014, 2016) AND ST_DWithin(geometry, POINT(145.115 -37.905), 0.03) LIMIT 100";

    let atomic_ms = timed_loop_ms(80, || {
        let _ = atomic_db.nodes().collection("memories").time_intersects("remembered_time", tq.clone()).st_dwithin(-37.905, 145.115, 0.03).take(100).count().unwrap();
    });
    let sql_ms = timed_loop_ms(80, || {
        let _ = sql_db.count(sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(80, || {
        let mut stmt = sqlite_conn.prepare("SELECT id FROM memories WHERE start_year <= 2016 AND end_year >= 2014 AND ((lon - 145.115)*(lon - 145.115) + (lat + 37.905)*(lat + 37.905)) <= (?1 * ?1) LIMIT 100").unwrap();
        let _rows: Vec<String> = stmt.query_map(params![0.03f64], |row| row.get(0)).unwrap().map(Result::unwrap).collect();
    });

    Ok(BenchmarkCase { name: "memories_near_place_in_vague_time_count_only", atomic_ms, sql_ms, sqlite_ms, note: "vague-time + near-place filter cost only repeated 80x" })
}

fn benchmark_memories_near_place_in_vague_time_id_only(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running memories_near_place_in_vague_time_id_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "vague_spatial_id_only", records)?;
    let tq = TimeQuery {
        start_year: 2014,
        end_year: 2016,
        start_fuzz_years: 0,
        end_fuzz_years: 0,
        months: vec![],
        weekdays: vec![],
        days_of_month: vec![],
        time_of_day: None,
        recurrence_step_months: None,
        global_fuzziness: 0.0,
    };
    let sql_query = "SELECT id FROM memories WHERE VAGUE_TIME_INTERSECTS(remembered_time, 2014, 2016) AND ST_DWithin(geometry, POINT(145.115 -37.905), 0.03) LIMIT 100";

    let atomic_ms = timed_loop_ms(80, || {
        let _ = atomic_db.nodes().collection("memories").time_intersects("remembered_time", tq.clone()).st_dwithin(-37.905, 145.115, 0.03).take(100).select(&["id"]).collect().unwrap();
    });
    let sql_ms = timed_loop_ms(80, || {
        let _ = sql_db.query(sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(80, || {
        let mut stmt = sqlite_conn.prepare("SELECT id FROM memories WHERE start_year <= 2016 AND end_year >= 2014 AND ((lon - 145.115)*(lon - 145.115) + (lat + 37.905)*(lat + 37.905)) <= (?1 * ?1) LIMIT 100").unwrap();
        let _rows: Vec<String> = stmt.query_map(params![0.03f64], |row| row.get(0)).unwrap().map(Result::unwrap).collect();
    });

    Ok(BenchmarkCase { name: "memories_near_place_in_vague_time_id_only", atomic_ms, sql_ms, sqlite_ms, note: "vague-time + near-place with id-only projection repeated 80x" })
}

fn benchmark_memories_near_place_in_vague_time_full_payload(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running memories_near_place_in_vague_time_full_payload");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "vague_spatial_full_payload", records)?;
    let tq = TimeQuery {
        start_year: 2014,
        end_year: 2016,
        start_fuzz_years: 0,
        end_fuzz_years: 0,
        months: vec![],
        weekdays: vec![],
        days_of_month: vec![],
        time_of_day: None,
        recurrence_step_months: None,
        global_fuzziness: 0.0,
    };
    let sql_query = "SELECT * FROM memories WHERE VAGUE_TIME_INTERSECTS(remembered_time, 2014, 2016) AND ST_DWithin(geometry, POINT(145.115 -37.905), 0.03) LIMIT 100";

    let atomic_ms = timed_loop_ms(80, || {
        let _ = atomic_db.nodes().collection("memories").time_intersects("remembered_time", tq.clone()).st_dwithin(-37.905, 145.115, 0.03).take(100).collect().unwrap();
    });
    let sql_ms = timed_loop_ms(80, || {
        let _ = sql_db.query(sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(80, || {
        let mut stmt = sqlite_conn.prepare("SELECT * FROM memories WHERE start_year <= 2016 AND end_year >= 2014 AND ((lon - 145.115)*(lon - 145.115) + (lat + 37.905)*(lat + 37.905)) <= (?1 * ?1) LIMIT 100").unwrap();
        let _rows = stmt.query(params![0.03f64]).unwrap();
        drop(_rows);
    });

    Ok(BenchmarkCase { name: "memories_near_place_in_vague_time_full_payload", atomic_ms, sql_ms, sqlite_ms, note: "vague-time + near-place with full payload hydration repeated 80x" })
}

fn benchmark_hybrid_vector_spatial_exact(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running hybrid_vector_spatial_exact");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "hybrid_vse", records)?;
    let query_vec = &records[123].embedding;
    let start = parse_timestamp_to_micros("2024-01-10 00:00:00");
    let end = parse_timestamp_to_micros("2024-01-20 23:59:59");
    let sql_query = format!("SELECT id FROM memories WHERE VECTOR_NEAR(embedding, {}, 50) AND ST_DWithin(geometry, POINT(145.115 -37.905), 0.03) AND created_at >= TIMESTAMP '2024-01-10 00:00:00' AND created_at <= TIMESTAMP '2024-01-20 23:59:59' LIMIT 20", vector_literal(query_vec));

    let atomic_ms = timed_loop_ms(25, || {
        let _ = atomic_db.nodes().collection("memories").similar(query_vec, 50).st_dwithin(-37.905, 145.115, 0.03).where_between("createdEpochMicros", start as f64, end as f64).take(20).collect().unwrap();
    });
    let sql_ms = timed_loop_ms(25, || {
        let _ = sql_db.explain(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(25, || {
        let mut stmt = sqlite_conn.prepare("SELECT id, lat, lon, created_epoch_micros, embedding_json FROM memories").unwrap();
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let lat: f64 = row.get(1)?;
            let lon: f64 = row.get(2)?;
            let created: i64 = row.get(3)?;
            let emb: String = row.get(4)?;
            Ok((id, lat, lon, created, emb))
        }).unwrap();
        let mut scored = Vec::new();
        for row in rows {
            let (id, lat, lon, created, emb_json) = row.unwrap();
            if ((lon - 145.115)*(lon - 145.115) + (lat + 37.905)*(lat + 37.905)) > 0.03f64 * 0.03f64 { continue; }
            if created < start || created > end { continue; }
            let emb: Vec<f32> = serde_json::from_str(&emb_json).unwrap();
            scored.push((id, cosine_distance(query_vec, &emb)));
        }
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        let _top: Vec<_> = scored.into_iter().take(20).collect();
    });

    Ok(BenchmarkCase { name: "hybrid_vector_spatial_exact", atomic_ms, sql_ms, sqlite_ms, note: "atomic executes; SQL uses explain-only because vector runtime is unstable; SQLite brute-forces hybrid" })
}

fn benchmark_hybrid_fulltext_vague_spatial_count(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running hybrid_fulltext_vague_spatial_count");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "hybrid_fvs", records)?;
    let tq = TimeQuery {
        start_year: 2014,
        end_year: 2016,
        start_fuzz_years: 0,
        end_fuzz_years: 0,
        months: vec![],
        weekdays: vec![],
        days_of_month: vec![],
        time_of_day: None,
        recurrence_step_months: None,
        global_fuzziness: 0.0,
    };
    let sql_query = "SELECT id FROM memories WHERE MATCHING('desk lab') AND VAGUE_TIME_INTERSECTS(remembered_time, 2014, 2016) AND ST_DWithin(geometry, POINT(145.115 -37.905), 0.03) LIMIT 20";

    let atomic_ms = timed_loop_ms(25, || {
        let _ = atomic_db.nodes().all().matching("desk lab").time_intersects("remembered_time", tq.clone()).st_dwithin(-37.905, 145.115, 0.03).take(20).count().unwrap();
    });
    let sql_ms = timed_loop_ms(25, || {
        let _ = sql_db.count(sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(25, || {
        let mut stmt = sqlite_conn.prepare("SELECT m.id, m.lat, m.lon, m.start_year, m.end_year FROM memories_fts f JOIN memories m ON m.id = f.id WHERE memories_fts MATCH 'desk lab'").unwrap();
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let lat: f64 = row.get(1)?;
            let lon: f64 = row.get(2)?;
            let start_year: i64 = row.get(3)?;
            let end_year: i64 = row.get(4)?;
            Ok((id, lat, lon, start_year, end_year))
        }).unwrap();
        let mut out = Vec::new();
        for row in rows {
            let (id, lat, lon, start_year, end_year) = row.unwrap();
            if ((lon - 145.115)*(lon - 145.115) + (lat + 37.905)*(lat + 37.905)) > 0.03f64 * 0.03f64 { continue; }
            if start_year > 2016 || end_year < 2014 { continue; }
            out.push(id);
            if out.len() >= 20 { break; }
        }
    });

    Ok(BenchmarkCase { name: "hybrid_memory_search_count_only", atomic_ms, sql_ms, sqlite_ms, note: "fulltext + vague time + spatial filter cost only repeated 25x" })
}

fn benchmark_hybrid_fulltext_vague_spatial_id_only(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running hybrid_fulltext_vague_spatial_id_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "hybrid_fvs_id_only", records)?;
    let tq = TimeQuery {
        start_year: 2014,
        end_year: 2016,
        start_fuzz_years: 0,
        end_fuzz_years: 0,
        months: vec![],
        weekdays: vec![],
        days_of_month: vec![],
        time_of_day: None,
        recurrence_step_months: None,
        global_fuzziness: 0.0,
    };
    let sql_query = "SELECT id FROM memories WHERE MATCHING('desk lab') AND VAGUE_TIME_INTERSECTS(remembered_time, 2014, 2016) AND ST_DWithin(geometry, POINT(145.115 -37.905), 0.03) LIMIT 20";

    let atomic_ms = timed_loop_ms(25, || {
        let _ = atomic_db.nodes().all().matching("desk lab").time_intersects("remembered_time", tq.clone()).st_dwithin(-37.905, 145.115, 0.03).take(20).select(&["id"]).collect().unwrap();
    });
    let sql_ms = timed_loop_ms(25, || {
        let _ = sql_db.query(sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(25, || {
        let mut stmt = sqlite_conn.prepare("SELECT m.id FROM memories_fts f JOIN memories m ON m.id = f.id WHERE memories_fts MATCH 'desk lab' AND m.start_year <= 2016 AND m.end_year >= 2014 AND ((m.lon - 145.115)*(m.lon - 145.115) + (m.lat + 37.905)*(m.lat + 37.905)) <= (?1 * ?1) LIMIT 20").unwrap();
        let _rows: Vec<String> = stmt.query_map(params![0.03f64], |row| row.get(0)).unwrap().map(Result::unwrap).collect();
    });

    Ok(BenchmarkCase { name: "hybrid_memory_search_id_only", atomic_ms, sql_ms, sqlite_ms, note: "fulltext + vague time + spatial with id-only projection repeated 25x" })
}

fn benchmark_hybrid_fulltext_vague_spatial_full_payload(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running hybrid_fulltext_vague_spatial_full_payload");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "hybrid_fvs_full_payload", records)?;
    let tq = TimeQuery {
        start_year: 2014,
        end_year: 2016,
        start_fuzz_years: 0,
        end_fuzz_years: 0,
        months: vec![],
        weekdays: vec![],
        days_of_month: vec![],
        time_of_day: None,
        recurrence_step_months: None,
        global_fuzziness: 0.0,
    };
    let sql_query = "SELECT * FROM memories WHERE MATCHING('desk lab') AND VAGUE_TIME_INTERSECTS(remembered_time, 2014, 2016) AND ST_DWithin(geometry, POINT(145.115 -37.905), 0.03) LIMIT 20";

    let atomic_ms = timed_loop_ms(25, || {
        let _ = atomic_db.nodes().all().matching("desk lab").time_intersects("remembered_time", tq.clone()).st_dwithin(-37.905, 145.115, 0.03).take(20).collect().unwrap();
    });
    let sql_ms = timed_loop_ms(25, || {
        let _ = sql_db.query(sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(25, || {
        let mut stmt = sqlite_conn.prepare("SELECT m.* FROM memories_fts f JOIN memories m ON m.id = f.id WHERE memories_fts MATCH 'desk lab' AND m.start_year <= 2016 AND m.end_year >= 2014 AND ((m.lon - 145.115)*(m.lon - 145.115) + (m.lat + 37.905)*(m.lat + 37.905)) <= (?1 * ?1) LIMIT 20").unwrap();
        let _rows = stmt.query(params![0.03f64]).unwrap();
        drop(_rows);
    });

    Ok(BenchmarkCase { name: "hybrid_memory_search_full_payload", atomic_ms, sql_ms, sqlite_ms, note: "fulltext + vague time + spatial with full payload hydration repeated 25x" })
}

fn benchmark_hybrid_graph_fulltext_exact(base: &Path, records: &[MemoryRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running hybrid_graph_fulltext_exact");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "hybrid_gfe", records)?;
    let source_id = &records[600].id;
    let source_slug = format!("memories/{}", source_id);
    let start = parse_timestamp_to_micros("2024-01-10 00:00:00");
    let end = parse_timestamp_to_micros("2024-01-20 23:59:59");
    let sql_query = format!("SELECT id FROM memories TRAVERSE FORWARD related_to TO memories WHERE id = '{}' AND MATCHING('desk lab') AND created_at >= TIMESTAMP '2024-01-10 00:00:00' AND created_at <= TIMESTAMP '2024-01-20 23:59:59' LIMIT 20", escape_sql(source_id));

    let atomic_ms = timed_loop_ms(25, || {
        let _ = atomic_db.nodes().one(&source_slug).forward("related_to").matching("desk lab").where_between("createdEpochMicros", start as f64, end as f64).take(20).collect().unwrap();
    });
    let sql_ms = timed_loop_ms(25, || {
        let _ = sql_db.query(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(25, || {
        let mut stmt = sqlite_conn.prepare("SELECT m.id, m.created_epoch_micros FROM memory_edges e JOIN memories_fts f ON f.id = e.to_id JOIN memories m ON m.id = e.to_id WHERE e.from_id = ?1 AND memories_fts MATCH 'desk lab' LIMIT 100").unwrap();
        let rows = stmt.query_map(params![source_id], |row| {
            let id: String = row.get(0)?;
            let created: i64 = row.get(1)?;
            Ok((id, created))
        }).unwrap();
        let mut out = Vec::new();
        for row in rows {
            let (id, created) = row.unwrap();
            if created < start || created > end { continue; }
            out.push(id);
            if out.len() >= 20 { break; }
        }
    });

    Ok(BenchmarkCase { name: "hybrid_graph_fulltext_exact", atomic_ms, sql_ms, sqlite_ms, note: "graph traversal + fulltext + exact time repeated 25x" })
}

fn provision_all(base: &Path, label: &str, records: &[MemoryRecord]) -> Result<(SekejapDB, SekejapDB, Connection), Box<dyn std::error::Error>> {
    let atomic_dir = base.join(format!("{}_atomic", label));
    let sql_dir = base.join(format!("{}_sql", label));
    let sqlite_path = base.join(format!("{}_sqlite.sqlite", label));
    let atomic_db = create_empty_sekejap_atomic(&atomic_dir)?;
    let sql_db = create_empty_sekejap_sql(&sql_dir)?;
    let mut sqlite_conn = create_empty_sqlite(&sqlite_path)?;

    bulk_insert_atomic_full(&atomic_db, records)?;
    for record in records {
        sql_db.mutate(&sql_insert(record))?;
    }
    attach_edges(&sql_db, records)?;

    let tx = sqlite_conn.unchecked_transaction()?;
    for record in records {
        insert_sqlite_record(&tx, record)?;
    }
    for record in records {
        for to in &record.related_to {
            tx.execute("INSERT INTO memory_edges (from_id, to_id) VALUES (?1, ?2)", params![record.id, to])?;
        }
    }
    tx.commit()?;

    Ok((atomic_db, sql_db, sqlite_conn))
}

fn create_empty_sekejap_atomic(path: &Path) -> Result<SekejapDB, Box<dyn std::error::Error>> {
    if path.exists() { fs::remove_dir_all(path)?; }
    fs::create_dir_all(path)?;
    let db = SekejapDB::new(path, TOTAL_RECORDS * 4)?;
    db.init_hnsw(16);
    #[cfg(feature = "fulltext")]
    db.init_fulltext(path);
    db.schema().define(
        "memories",
        &serde_json!({
            "hot_fields": {
                "hash_index": ["id", "weather"],
                "range_index": ["createdEpochMicros"],
                "temporal": ["remembered_time"],
                "spatial": ["geometry"],
                "vector": ["embedding"],
                "fulltext": ["title", "story"]
            }
        }).to_string(),
    )?;
    Ok(db)
}

fn create_empty_sekejap_sql(path: &Path) -> Result<SekejapDB, Box<dyn std::error::Error>> {
    if path.exists() { fs::remove_dir_all(path)?; }
    fs::create_dir_all(path)?;
    let db = SekejapDB::new(path, TOTAL_RECORDS * 4)?;
    db.init_hnsw(16);
    #[cfg(feature = "fulltext")]
    db.init_fulltext(path);
    db.mutate(
        "CREATE COLLECTION memories (\
            id UUID PRIMARY KEY DEFAULT uuidv4(),\
            title TEXT,\
            story TEXT,\
            weather TEXT,\
            created_at TIMESTAMP,\
            remembered_time VAGUE_TIME,\
            geometry GEOMETRY,\
            embedding VECTOR(128)\
        ) WITH (\
            hash_index = [id, weather],\
            range_index = [created_at],\
            temporal_index = [remembered_time],\
            spatial_index = [geometry],\
            vector_index = [embedding],\
            fulltext_index = [title, story]\
        )"
    )?;
    Ok(db)
}

fn create_empty_sqlite(path: &Path) -> Result<Connection, Box<dyn std::error::Error>> {
    if path.exists() { fs::remove_file(path)?; }
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "CREATE TABLE memories (\
            id TEXT PRIMARY KEY,\
            title TEXT NOT NULL,\
            story TEXT NOT NULL,\
            weather TEXT NOT NULL,\
            created_at TEXT NOT NULL,\
            created_epoch_micros INTEGER NOT NULL,\
            remembered_time_json TEXT NOT NULL,\
            geometry_json TEXT NOT NULL,\
            lat REAL NOT NULL,\
            lon REAL NOT NULL,\
            embedding_json TEXT NOT NULL,\
            start_year INTEGER NOT NULL,\
            end_year INTEGER NOT NULL\
        );\
        CREATE TABLE memory_edges (\
            from_id TEXT NOT NULL,\
            to_id TEXT NOT NULL\
        );\
        CREATE INDEX idx_memories_created_epoch ON memories(created_epoch_micros);\
        CREATE INDEX idx_memories_start_end_year ON memories(start_year, end_year);\
        CREATE INDEX idx_edges_from ON memory_edges(from_id);\
        CREATE VIRTUAL TABLE memories_fts USING fts5(id UNINDEXED, title, story);"
    )?;
    Ok(conn)
}

fn bulk_insert_atomic_nodes(db: &SekejapDB, records: &[MemoryRecord]) -> Result<(), Box<dyn std::error::Error>> {
    let items = records.iter().map(|r| (format!("memories/{}", r.id), payload_json_string(r))).collect::<Vec<_>>();
    let refs = items.iter().map(|(slug, json)| (slug.as_str(), json.as_str())).collect::<Vec<_>>();
    db.nodes().ingest_raw(&refs)?;
    db.nodes().build_hnsw()?;
    Ok(())
}

fn bulk_insert_atomic_full(db: &SekejapDB, records: &[MemoryRecord]) -> Result<(), Box<dyn std::error::Error>> {
    bulk_insert_atomic_nodes(db, records)?;
    let edge_owned = records.iter().flat_map(|r| {
        r.related_to.iter().map(move |to| (format!("memories/{}", r.id), format!("memories/{}", to), "related_to".to_string(), 1.0f32))
    }).collect::<Vec<_>>();
    let edge_refs = edge_owned.iter().map(|(s,d,t,w)| (s.as_str(), d.as_str(), t.as_str(), *w)).collect::<Vec<_>>();
    db.edges().ingest(&edge_refs)?;
    Ok(())
}

fn attach_edges(db: &SekejapDB, records: &[MemoryRecord]) -> Result<(), Box<dyn std::error::Error>> {
    let edge_owned = records.iter().flat_map(|r| {
        r.related_to.iter().map(move |to| (format!("memories/{}", r.id), format!("memories/{}", to), "related_to".to_string(), 1.0f32))
    }).collect::<Vec<_>>();
    let edge_refs = edge_owned.iter().map(|(s,d,t,w)| (s.as_str(), d.as_str(), t.as_str(), *w)).collect::<Vec<_>>();
    db.edges().ingest(&edge_refs)?;
    Ok(())
}

fn payload_json_string(record: &MemoryRecord) -> String {
    json!({
        "_id": format!("memories/{}", record.id),
        "_collection": "memories",
        "_key": record.id,
        "id": record.id,
        "title": record.title,
        "story": record.story,
        "weather": record.weather,
        "created_at": record.created_at,
        "createdEpochMicros": record.created_epoch_micros,
        "remembered_time": serde_json::from_str::<Value>(&record.remembered_time).unwrap(),
        "geometry": serde_json::from_str::<Value>(&record.geometry).unwrap(),
        "embedding": record.embedding,
        "vectors": {"dense": record.embedding},
        "start_year": record.start_year,
        "end_year": record.end_year
    }).to_string()
}

fn insert_sqlite_record(conn: &Connection, record: &MemoryRecord) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute(
        "INSERT INTO memories (id, title, story, weather, created_at, created_epoch_micros, remembered_time_json, geometry_json, lat, lon, embedding_json, start_year, end_year) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![record.id, record.title, record.story, record.weather, record.created_at, record.created_epoch_micros, record.remembered_time, record.geometry, record.lat, record.lon, serde_json::to_string(&record.embedding)?, record.start_year, record.end_year],
    )?;
    conn.execute("INSERT INTO memories_fts (id, title, story) VALUES (?1, ?2, ?3)", params![record.id, record.title, record.story])?;
    Ok(())
}

fn sql_insert_batch(records: &[MemoryRecord]) -> String {
    let values = records.iter().map(|record| {
        format!(
            "('{}', '{}', '{}', '{}', TIMESTAMP '{}', '{}', '{}', {})",
            escape_sql(&record.id),
            escape_sql(&record.title),
            escape_sql(&record.story),
            escape_sql(record.weather),
            record.created_at,
            escape_sql(&record.remembered_time),
            escape_sql(&record.geometry),
            vector_literal(&record.embedding)
        )
    }).collect::<Vec<_>>().join(", ");
    format!("INSERT INTO memories (id, title, story, weather, created_at, remembered_time, geometry, embedding) VALUES {values}")
}

fn sql_insert(record: &MemoryRecord) -> String {

    format!(
        "INSERT INTO memories (id, title, story, weather, created_at, remembered_time, geometry, embedding) VALUES ('{}', '{}', '{}', '{}', TIMESTAMP '{}', '{}', '{}', {})",
        escape_sql(&record.id),
        escape_sql(&record.title),
        escape_sql(&record.story),
        escape_sql(record.weather),
        record.created_at,
        escape_sql(&record.remembered_time),
        escape_sql(&record.geometry),
        vector_literal(&record.embedding)
    )
}

fn escape_sql(value: &str) -> String { value.replace('\'', "''") }

fn vector_literal(values: &[f32]) -> String {
    let inner = values.iter().map(|v| format!("{:.6}", v)).collect::<Vec<_>>().join(", ");
    format!("[{}]", inner)
}

fn build_dataset(count: usize) -> Vec<MemoryRecord> {
    let mut rng = StdRng::seed_from_u64(42);
    let weathers = ["clear", "cloudy", "foggy", "rain", "snow", "storm"];
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let day = 1 + (i % 28) as u32;
        let month = 1 + ((i / 28) % 12) as u32;
        let year = 2012 + ((i / 300) as i32 % 12);
        let hour = 8 + ((i * 7) % 12) as u32;
        let minute = ((i * 13) % 60) as u32;
        let second = ((i * 17) % 60) as u32;
        let created_at = format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", year, month, day, hour, minute, second);
        let epoch = parse_timestamp_to_micros(&created_at);
        let start_year = year as i64 - (i % 3) as i64;
        let end_year = year as i64 + (i % 2) as i64;
        let lat = -37.92 + (i as f64 * 0.00007);
        let lon = 145.09 + (i as f64 * 0.00009);
        let weather = weathers[i % weathers.len()];
        let embedding = (0..VECTOR_DIM).map(|j| rng.gen_range(0.0..1.0) * (1.0 + (j % 5) as f32 * 0.01)).collect::<Vec<f32>>();
        let remembered_time = serde_json!({
            "bounds": {"startYear": start_year, "endYear": end_year},
            "constraints": {
                "months": [month],
                "daysOfMonth": [day],
                "timeOfDay": {
                    "startMinute": (hour * 60 + minute) as i64,
                    "endMinute": (hour * 60 + minute) as i64,
                    "fuzzyRadiusMinute": 5
                }
            },
            "globalFuzziness": 0.1
        }).to_string();
        let geometry = serde_json!({"type":"Point","coordinates":[lon, lat]}).to_string();
        let related_to = (1..=GRAPH_FANOUT).map(|delta| format!("mem_{:05}", (i + delta) % count)).collect::<Vec<_>>();
        out.push(MemoryRecord {
            id: format!("mem_{:05}", i),
            title: format!("Memory {}", i),
            story: format!("Desk note {} in Woodside lab with {} light and corridor traces", i, weather),
            weather,
            created_at,
            created_epoch_micros: epoch,
            remembered_time,
            geometry,
            lat,
            lon,
            embedding,
            start_year,
            end_year,
            related_to,
        });
    }
    out
}

fn parse_timestamp_to_micros(input: &str) -> i64 {
    NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S").unwrap().and_utc().timestamp_micros()
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom == 0.0 { 1.0 } else { 1.0 - (dot / denom) }
}

fn timed_ms<F: FnOnce()>(f: F) -> f64 {
    let start = Instant::now();
    f();
    start.elapsed().as_secs_f64() * 1000.0
}

fn timed_loop_ms<F: FnMut()>(iters: usize, mut f: F) -> f64 {
    let start = Instant::now();
    for _ in 0..iters { f(); }
    start.elapsed().as_secs_f64() * 1000.0
}

fn render_markdown(cases: &[BenchmarkCase]) -> String {
    let mut out = String::from("# Rust Benchmark\n\n");
    out.push_str("Compare Sekejap Atomic vs Sekejap SQL vs SQLite on graph, vector, spatial, exact time, vague time, fulltext, and hybrid scenarios.\n\n");
    out.push_str("| Case | Sekejap Atomic ms | Sekejap SQL ms | SQLite ms | Note |\n");
    out.push_str("|---|---:|---:|---:|---|\n");
    for case in cases {
        out.push_str(&format!("| {} | {:.3} | {:.3} | {:.3} | {} |\n", case.name, case.atomic_ms, case.sql_ms, case.sqlite_ms, case.note));
    }
    out
}
