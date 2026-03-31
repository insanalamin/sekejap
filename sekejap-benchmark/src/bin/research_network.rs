use rusqlite::{params, Connection};
use sekejap::SekejapDB;
use serde_json::json;
use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::tempdir;

const TOTAL_RESEARCHERS: usize = 2_000;
const DIM: usize = 64;
const TOP_K: usize = 20;
const SQL_BATCH_ROWS: usize = 200;
const EDGE_BATCH_ROWS: usize = 250;

#[derive(Clone)]
struct ResearcherRecord {
    id: String,
    name: String,
    institution: String,
    campus: String,
    title: String,
    abstract_text: String,
    lat: f32,
    lon: f32,
    embedding: Vec<f32>,
    collaborators: Vec<String>,
}

struct BenchmarkCase {
    name: &'static str,
    atomic_ms: f64,
    sql_ms: f64,
    sqlite_ms: f64,
    note: &'static str,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dataset = build_dataset(TOTAL_RESEARCHERS);
    let temp = tempdir()?;
    let base = temp.path();

    let cases = vec![
        benchmark_insert(base, &dataset)?,
        benchmark_topic_similarity_count(base, &dataset)?,
        benchmark_topic_similarity_id_only(base, &dataset)?,
        benchmark_nearby_campus_topic_lookup(base, &dataset)?,
        benchmark_nearby_campus_topic_lookup_id_only(base, &dataset)?,
        benchmark_collaboration_neighborhood(base, &dataset)?,
        benchmark_hybrid_vector_spatial_graph(base, &dataset)?,
        benchmark_hybrid_vector_spatial_graph_id_only(base, &dataset)?,
    ];

    let markdown = render_markdown(&cases);
    let out_path = PathBuf::from(
        r"C:\Users\Insan\AndroidStudioProjects\SekejapDB\sekejap-benchmark\cases\research-network\RESULT.md",
    );
    fs::create_dir_all(out_path.parent().unwrap())?;
    fs::write(&out_path, markdown)?;
    println!("wrote {}", out_path.display());
    Ok(())
}

fn benchmark_insert(
    base: &Path,
    records: &[ResearcherRecord],
) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running insert_research_network_2000");
    let atomic_db = create_empty_atomic(&base.join("insert_atomic"))?;
    let sql_db = create_empty_sql(&base.join("insert_sql"))?;
    let mut sqlite = create_empty_sqlite(&base.join("insert_sqlite.sqlite"))?;

    let atomic_ms = timed_ms(|| {
        bulk_insert_atomic(&atomic_db, records).unwrap();
        attach_edges_atomic(&atomic_db, records).unwrap();
    });
    let sql_ms = timed_ms(|| {
        bulk_insert_sql(&sql_db, records).unwrap();
        attach_edges_sql(&sql_db, records).unwrap();
    });
    let sqlite_ms = timed_ms(|| {
        let tx = sqlite.unchecked_transaction().unwrap();
        for record in records {
            insert_sqlite_researcher(&tx, record).unwrap();
        }
        for record in records {
            for to in &record.collaborators {
                tx.execute(
                    "INSERT INTO collaboration_edges (from_id, to_id) VALUES (?1, ?2)",
                    params![record.id, to],
                )
                .unwrap();
            }
        }
        tx.commit().unwrap();
    });

    Ok(BenchmarkCase {
        name: "insert_research_network_2000",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "researcher nodes with vector, campus point, and collaboration edges",
    })
}

fn benchmark_topic_similarity_count(
    base: &Path,
    records: &[ResearcherRecord],
) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running topic_similarity_search_count_only");
    let (atomic_db, sql_db, sqlite) = provision_all(base, "topic_similarity", records)?;
    let query_vec = &records[777].embedding;
    let sql_query = format!(
        "SELECT id FROM researchers WHERE VECTOR_NEAR(embedding, {}, {}) LIMIT {}",
        vector_literal(query_vec),
        TOP_K,
        TOP_K
    );

    let atomic_ms = timed_loop_ms(80, || {
        let _ = atomic_db
            .nodes()
            .collection("researchers")
            .similar(query_vec, TOP_K)
            .take(TOP_K)
            .count()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(80, || {
        let _ = sql_db.count(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(80, || {
        brute_force_sqlite_vector(&sqlite, query_vec, None, TOP_K);
    });

    Ok(BenchmarkCase {
        name: "topic_similarity_search_count_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "vector similarity over researcher topic embeddings",
    })
}

fn benchmark_topic_similarity_id_only(
    base: &Path,
    records: &[ResearcherRecord],
) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running topic_similarity_search_id_only");
    let (atomic_db, sql_db, sqlite) = provision_all(base, "topic_similarity_id", records)?;
    let query_vec = &records[777].embedding;
    let sql_query = format!(
        "SELECT id FROM researchers WHERE VECTOR_NEAR(embedding, {}, {}) LIMIT {}",
        vector_literal(query_vec),
        TOP_K,
        TOP_K
    );

    let atomic_ms = timed_loop_ms(80, || {
        let _ = atomic_db
            .nodes()
            .collection("researchers")
            .similar(query_vec, TOP_K)
            .take(TOP_K)
            .select(&["id"])
            .collect()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(80, || {
        let _ = sql_db.query(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(80, || {
        let _ = brute_force_sqlite_vector(&sqlite, query_vec, None, TOP_K);
    });

    Ok(BenchmarkCase {
        name: "topic_similarity_search_id_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "vector similarity with id projection",
    })
}

fn benchmark_nearby_campus_topic_lookup(
    base: &Path,
    records: &[ResearcherRecord],
) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running nearby_campus_topic_lookup_count_only");
    let (atomic_db, sql_db, sqlite) = provision_all(base, "nearby_campus", records)?;
    let query_vec = &records[321].embedding;
    let melb_lon = 144.9631f32;
    let melb_lat = -37.8136f32;
    let sql_query = format!(
        "SELECT id FROM researchers WHERE VECTOR_NEAR(embedding, {}, 100) AND ST_DWithin(geometry, POINT({} {}), 25.0) LIMIT {}",
        vector_literal(query_vec),
        melb_lon,
        melb_lat,
        TOP_K
    );

    let atomic_ms = timed_loop_ms(60, || {
        let _ = atomic_db
            .nodes()
            .collection("researchers")
            .similar(query_vec, 100)
            .st_dwithin(melb_lat, melb_lon, 25.0)
            .take(TOP_K)
            .count()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(60, || {
        let _ = sql_db.count(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(60, || {
        brute_force_sqlite_vector(&sqlite, query_vec, Some((melb_lat, melb_lon, 25.0)), TOP_K);
    });

    Ok(BenchmarkCase {
        name: "nearby_campus_topic_lookup_count_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "vector + spatial candidate lookup near Melbourne campuses",
    })
}

fn benchmark_nearby_campus_topic_lookup_id_only(
    base: &Path,
    records: &[ResearcherRecord],
) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running nearby_campus_topic_lookup_id_only");
    let (atomic_db, sql_db, sqlite) = provision_all(base, "nearby_campus_id", records)?;
    let query_vec = &records[321].embedding;
    let melb_lon = 144.9631f32;
    let melb_lat = -37.8136f32;
    let sql_query = format!(
        "SELECT id FROM researchers WHERE VECTOR_NEAR(embedding, {}, 100) AND ST_DWithin(geometry, POINT({} {}), 25.0) LIMIT {}",
        vector_literal(query_vec),
        melb_lon,
        melb_lat,
        TOP_K
    );

    let atomic_ms = timed_loop_ms(60, || {
        let _ = atomic_db
            .nodes()
            .collection("researchers")
            .similar(query_vec, 100)
            .st_dwithin(melb_lat, melb_lon, 25.0)
            .take(TOP_K)
            .select(&["id"])
            .collect()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(60, || {
        let _ = sql_db.query(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(60, || {
        let _ = brute_force_sqlite_vector(&sqlite, query_vec, Some((melb_lat, melb_lon, 25.0)), TOP_K);
    });

    Ok(BenchmarkCase {
        name: "nearby_campus_topic_lookup_id_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "vector + spatial candidate lookup with id projection",
    })
}

fn benchmark_collaboration_neighborhood(
    base: &Path,
    records: &[ResearcherRecord],
) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running collaboration_neighborhood_expansion");
    let (atomic_db, sql_db, sqlite) = provision_all(base, "collaboration", records)?;
    let source = &records[0].id;
    let sql_query = format!(
        "SELECT id FROM researchers TRAVERSE FORWARD collaborates_with TO researchers HOPS 3 WHERE id = '{}' LIMIT 64",
        escape_sql(source)
    );

    let atomic_ms = timed_loop_ms(120, || {
        let _ = atomic_db
            .nodes()
            .one(&format!("researchers/{source}"))
            .hops(3)
            .forward("collaborates_with")
            .take(64)
            .select(&["id"])
            .collect()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(120, || {
        let _ = sql_db.query(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(120, || {
        let _ = run_sqlite_recursive(&sqlite, source, 3, 64).unwrap();
    });

    Ok(BenchmarkCase {
        name: "collaboration_neighborhood_expansion",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "anchored collaboration traversal across three hops",
    })
}

fn benchmark_hybrid_vector_spatial_graph(
    base: &Path,
    records: &[ResearcherRecord],
) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running hybrid_vector_spatial_graph_search_count_only");
    let (atomic_db, sql_db, sqlite) = provision_all(base, "hybrid", records)?;
    let source = &records[0].id;
    let query_vec = &records[0].embedding;
    let melb_lon = 144.9631f32;
    let melb_lat = -37.8136f32;
    let sql_query = format!(
        "SELECT id FROM researchers TRAVERSE FORWARD collaborates_with TO researchers HOPS 2 WHERE id = '{}' AND VECTOR_NEAR(embedding, {}, 100) AND ST_DWithin(geometry, POINT({} {}), 40.0) LIMIT {}",
        escape_sql(source),
        vector_literal(query_vec),
        melb_lon,
        melb_lat,
        TOP_K
    );

    let atomic_ms = timed_loop_ms(60, || {
        let _ = atomic_db
            .nodes()
            .one(&format!("researchers/{source}"))
            .hops(2)
            .forward("collaborates_with")
            .similar(query_vec, 100)
            .st_dwithin(melb_lat, melb_lon, 40.0)
            .take(TOP_K)
            .count()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(60, || {
        let _ = sql_db.count(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(60, || {
        let _ = run_sqlite_hybrid(&sqlite, source, 2, query_vec, melb_lat, melb_lon, 40.0, TOP_K)
            .unwrap();
    });

    Ok(BenchmarkCase {
        name: "hybrid_vector_spatial_graph_search_count_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "anchored graph expansion refined by vector and spatial similarity",
    })
}

fn benchmark_hybrid_vector_spatial_graph_id_only(
    base: &Path,
    records: &[ResearcherRecord],
) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running hybrid_vector_spatial_graph_search_id_only");
    let (atomic_db, sql_db, sqlite) = provision_all(base, "hybrid_id", records)?;
    let source = &records[0].id;
    let query_vec = &records[0].embedding;
    let melb_lon = 144.9631f32;
    let melb_lat = -37.8136f32;
    let sql_query = format!(
        "SELECT id FROM researchers TRAVERSE FORWARD collaborates_with TO researchers HOPS 2 WHERE id = '{}' AND VECTOR_NEAR(embedding, {}, 100) AND ST_DWithin(geometry, POINT({} {}), 40.0) LIMIT {}",
        escape_sql(source),
        vector_literal(query_vec),
        melb_lon,
        melb_lat,
        TOP_K
    );

    let atomic_ms = timed_loop_ms(60, || {
        let _ = atomic_db
            .nodes()
            .one(&format!("researchers/{source}"))
            .hops(2)
            .forward("collaborates_with")
            .similar(query_vec, 100)
            .st_dwithin(melb_lat, melb_lon, 40.0)
            .take(TOP_K)
            .select(&["id"])
            .collect()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(60, || {
        let _ = sql_db.query(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(60, || {
        let _ = run_sqlite_hybrid(&sqlite, source, 2, query_vec, melb_lat, melb_lon, 40.0, TOP_K)
            .unwrap();
    });

    Ok(BenchmarkCase {
        name: "hybrid_vector_spatial_graph_search_id_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "anchored graph expansion refined by vector and spatial similarity with id projection",
    })
}

fn create_empty_atomic(path: &Path) -> Result<SekejapDB, Box<dyn std::error::Error>> {
    let db = SekejapDB::new(path, TOTAL_RESEARCHERS * 3)?;
    db.schema().define(
        "researchers",
        &json!({
            "hot_fields": {
                "hash_index": ["id", "campus"],
                "spatial": ["geometry"],
                "vector": ["embedding"]
            }
        })
        .to_string(),
    )?;
    Ok(db)
}

fn create_empty_sql(path: &Path) -> Result<SekejapDB, Box<dyn std::error::Error>> {
    let db = SekejapDB::new(path, TOTAL_RESEARCHERS * 3)?;
    db.mutate(&format!(
        "CREATE COLLECTION researchers (id TEXT PRIMARY KEY, name TEXT, institution TEXT, campus TEXT, title TEXT, abstract TEXT, geometry GEOMETRY, embedding VECTOR({})) WITH (hash_index = [id, campus], spatial_index = [geometry], vector_index = [embedding])",
        DIM
    ))?;
    Ok(db)
}

fn create_empty_sqlite(path: &Path) -> Result<Connection, Box<dyn std::error::Error>> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "CREATE TABLE researchers (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            institution TEXT NOT NULL,
            campus TEXT NOT NULL,
            title TEXT NOT NULL,
            abstract TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            embedding_json TEXT NOT NULL
        );
        CREATE TABLE collaboration_edges (
            from_id TEXT NOT NULL,
            to_id TEXT NOT NULL
        );
        CREATE INDEX idx_collab_from ON collaboration_edges(from_id);
        CREATE INDEX idx_researcher_campus ON researchers(campus);",
    )?;
    Ok(conn)
}

fn provision_all(
    base: &Path,
    label: &str,
    records: &[ResearcherRecord],
) -> Result<(SekejapDB, SekejapDB, Connection), Box<dyn std::error::Error>> {
    let atomic_db = create_empty_atomic(&base.join(format!("{label}_atomic")))?;
    bulk_insert_atomic(&atomic_db, records)?;
    attach_edges_atomic(&atomic_db, records)?;

    let sql_db = create_empty_sql(&base.join(format!("{label}_sql")))?;
    bulk_insert_sql(&sql_db, records)?;
    attach_edges_sql(&sql_db, records)?;

    let mut sqlite = create_empty_sqlite(&base.join(format!("{label}_sqlite.sqlite")))?;
    let tx = sqlite.unchecked_transaction()?;
    for record in records {
        insert_sqlite_researcher(&tx, record)?;
    }
    for record in records {
        for to in &record.collaborators {
            tx.execute(
                "INSERT INTO collaboration_edges (from_id, to_id) VALUES (?1, ?2)",
                params![record.id, to],
            )?;
        }
    }
    tx.commit()?;

    Ok((atomic_db, sql_db, sqlite))
}

fn bulk_insert_atomic(
    db: &SekejapDB,
    records: &[ResearcherRecord],
) -> Result<(), Box<dyn std::error::Error>> {
    let items = records
        .iter()
        .map(|r| (format!("researchers/{}", r.id), payload_json_string(r)))
        .collect::<Vec<_>>();
    let refs = items
        .iter()
        .map(|(slug, payload)| (slug.as_str(), payload.as_str()))
        .collect::<Vec<_>>();
    db.nodes().ingest_raw(&refs)?;
    db.init_hnsw(16);
    db.nodes().build_hnsw()?;
    Ok(())
}

fn bulk_insert_sql(
    db: &SekejapDB,
    records: &[ResearcherRecord],
) -> Result<(), Box<dyn std::error::Error>> {
    for chunk in records.chunks(SQL_BATCH_ROWS) {
        db.mutate(&sql_insert_batch(chunk))?;
    }
    db.init_hnsw(16);
    db.nodes().build_hnsw()?;
    Ok(())
}

fn attach_edges_atomic(
    db: &SekejapDB,
    records: &[ResearcherRecord],
) -> Result<(), Box<dyn std::error::Error>> {
    let edges = records
        .iter()
        .flat_map(|r| {
            r.collaborators.iter().map(move |to| {
                (
                    format!("researchers/{}", r.id),
                    format!("researchers/{to}"),
                    "collaborates_with".to_string(),
                    1.0f32,
                )
            })
        })
        .collect::<Vec<_>>();
    let refs = edges
        .iter()
        .map(|(from, to, ty, weight)| (from.as_str(), to.as_str(), ty.as_str(), *weight))
        .collect::<Vec<_>>();
    db.edges().ingest(&refs)?;
    Ok(())
}

fn attach_edges_sql(
    db: &SekejapDB,
    records: &[ResearcherRecord],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut edges = Vec::new();
    for r in records {
        for to in &r.collaborators {
            edges.push((r.id.as_str(), to.as_str()));
        }
    }
    for chunk in edges.chunks(EDGE_BATCH_ROWS) {
        db.mutate(&sql_relate_many_batch(chunk))?;
    }
    Ok(())
}

fn insert_sqlite_researcher(
    conn: &Connection,
    r: &ResearcherRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute(
        "INSERT INTO researchers (id, name, institution, campus, title, abstract, lat, lon, embedding_json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            r.id,
            r.name,
            r.institution,
            r.campus,
            r.title,
            r.abstract_text,
            r.lat,
            r.lon,
            serde_json::to_string(&r.embedding)?
        ],
    )?;
    Ok(())
}

fn build_dataset(total: usize) -> Vec<ResearcherRecord> {
    let campuses = [
        ("University of Melbourne", "Parkville", -37.7982f32, 144.9606f32),
        ("Monash University", "Clayton", -37.9105f32, 145.1362f32),
        ("RMIT University", "Melbourne CBD", -37.8080f32, 144.9633f32),
        ("Deakin University", "Geelong Waterfront", -38.1438f32, 144.3607f32),
        ("Federation University", "Ballarat", -37.5577f32, 143.8503f32),
        ("La Trobe University", "Bendigo", -36.7564f32, 144.2786f32),
    ];
    let topics = [
        "energy systems",
        "education policy",
        "computer vision",
        "public health",
        "agritech systems",
        "transport resilience",
        "data engineering",
        "cultural analytics",
    ];

    let mut out = Vec::with_capacity(total);
    for i in 0..total {
        let campus_idx = i % campuses.len();
        let topic_idx = i % topics.len();
        let (institution, campus, base_lat, base_lon) = campuses[campus_idx];
        let lat = base_lat + ((i % 11) as f32 - 5.0) * 0.003;
        let lon = base_lon + ((i % 13) as f32 - 6.0) * 0.003;
        let topic = topics[topic_idx];
        let id = format!("researcher_{i:05}");
        let collaborators = vec![
            format!("researcher_{:05}", (i + 1) % total),
            format!("researcher_{:05}", (i + topics.len()) % total),
            format!("researcher_{:05}", (i + campuses.len() * 3) % total),
        ];
        out.push(ResearcherRecord {
            id,
            name: format!("Researcher {i}"),
            institution: institution.to_string(),
            campus: campus.to_string(),
            title: format!("{topic} research cluster"),
            abstract_text: format!(
                "Research on {topic} at {campus}, with collaborations across Victoria and applied industry projects."
            ),
            lat,
            lon,
            embedding: build_embedding(i, topic_idx),
            collaborators,
        });
    }
    out
}

fn build_embedding(seed: usize, cluster: usize) -> Vec<f32> {
    (0..DIM)
        .map(|d| {
            let base = if d % 8 == cluster { 0.92 } else { 0.08 };
            base + ((seed * 19 + d * 11) % 100) as f32 / 700.0
        })
        .collect()
}

fn payload_json_string(record: &ResearcherRecord) -> String {
    json!({
        "_id": format!("researchers/{}", record.id),
        "_key": record.id,
        "id": record.id,
        "name": record.name,
        "institution": record.institution,
        "campus": record.campus,
        "title": record.title,
        "abstract": record.abstract_text,
        "geometry": {
            "type": "Point",
            "coordinates": [record.lon, record.lat]
        },
        "embedding": record.embedding
    })
    .to_string()
}

fn sql_insert_batch(records: &[ResearcherRecord]) -> String {
    let values = records
        .iter()
        .map(|r| {
            format!(
                "('{}', '{}', '{}', '{}', '{}', '{}', {}, {})",
                escape_sql(&r.id),
                escape_sql(&r.name),
                escape_sql(&r.institution),
                escape_sql(&r.campus),
                escape_sql(&r.title),
                escape_sql(&r.abstract_text),
                geometry_literal(r.lat, r.lon),
                vector_literal(&r.embedding)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO researchers (id, name, institution, campus, title, abstract, geometry, embedding) VALUES {values}"
    )
}

fn sql_relate_many_batch(edges: &[(&str, &str)]) -> String {
    let inner = edges
        .iter()
        .map(|(from, to)| {
            format!(
                "researchers/{} -> collaborates_with -> researchers/{}",
                escape_sql(from),
                escape_sql(to)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("RELATE MANY ({inner})")
}

fn geometry_literal(lat: f32, lon: f32) -> String {
    format!("'{{\"type\":\"Point\",\"coordinates\":[{lon:.6},{lat:.6}]}}'")
}

fn vector_literal(values: &[f32]) -> String {
    let inner = values
        .iter()
        .map(|v| format!("{v:.6}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{inner}]")
}

fn brute_force_sqlite_vector(
    conn: &Connection,
    query_vec: &[f32],
    spatial: Option<(f32, f32, f32)>,
    top_k: usize,
) -> Vec<String> {
    let mut stmt = conn
        .prepare("SELECT id, lat, lon, embedding_json FROM researchers")
        .unwrap();
    let rows = stmt
        .query_map([], |row| {
            let id: String = row.get(0)?;
            let lat: f32 = row.get(1)?;
            let lon: f32 = row.get(2)?;
            let emb_json: String = row.get(3)?;
            Ok((id, lat, lon, emb_json))
        })
        .unwrap();
    let mut scored = Vec::new();
    for row in rows {
        let (id, lat, lon, emb_json) = row.unwrap();
        if let Some((q_lat, q_lon, radius_km)) = spatial {
            if haversine_km(lat, lon, q_lat, q_lon) > radius_km {
                continue;
            }
        }
        let emb: Vec<f32> = serde_json::from_str(&emb_json).unwrap();
        scored.push((id, cosine_distance(query_vec, &emb)));
    }
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    scored.into_iter().take(top_k).map(|(id, _)| id).collect()
}

fn run_sqlite_recursive(
    conn: &Connection,
    source_id: &str,
    hops: usize,
    limit: usize,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let sql = "
        WITH RECURSIVE walk(id, depth) AS (
            SELECT to_id, 1 FROM collaboration_edges WHERE from_id = ?1
            UNION
            SELECT e.to_id, walk.depth + 1
            FROM collaboration_edges e
            JOIN walk ON e.from_id = walk.id
            WHERE walk.depth < ?2
        )
        SELECT DISTINCT id FROM walk LIMIT ?3";
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(params![source_id, hops as i64, limit as i64], |row| row.get(0))?;
    Ok(rows.map(Result::unwrap).collect())
}

fn run_sqlite_hybrid(
    conn: &Connection,
    source_id: &str,
    hops: usize,
    query_vec: &[f32],
    q_lat: f32,
    q_lon: f32,
    radius_km: f32,
    top_k: usize,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let sql = "
        WITH RECURSIVE walk(id, depth) AS (
            SELECT to_id, 1 FROM collaboration_edges WHERE from_id = ?1
            UNION
            SELECT e.to_id, walk.depth + 1
            FROM collaboration_edges e
            JOIN walk ON e.from_id = walk.id
            WHERE walk.depth < ?2
        )
        SELECT DISTINCT r.id, r.lat, r.lon, r.embedding_json
        FROM walk
        JOIN researchers r ON r.id = walk.id";
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(params![source_id, hops as i64], |row| {
        let id: String = row.get(0)?;
        let lat: f32 = row.get(1)?;
        let lon: f32 = row.get(2)?;
        let emb_json: String = row.get(3)?;
        Ok((id, lat, lon, emb_json))
    })?;

    let mut scored = Vec::new();
    for row in rows {
        let (id, lat, lon, emb_json) = row?;
        if haversine_km(lat, lon, q_lat, q_lon) > radius_km {
            continue;
        }
        let emb: Vec<f32> = serde_json::from_str(&emb_json)?;
        scored.push((id, cosine_distance(query_vec, &emb)));
    }
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    Ok(scored.into_iter().take(top_k).map(|(id, _)| id).collect())
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    1.0 - dot / (na.sqrt() * nb.sqrt()).max(1e-6)
}

fn haversine_km(lat1: f32, lon1: f32, lat2: f32, lon2: f32) -> f32 {
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    6371.0 * c
}

fn timed_ms<F>(mut f: F) -> f64
where
    F: FnMut(),
{
    let start = Instant::now();
    f();
    start.elapsed().as_secs_f64() * 1000.0
}

fn timed_loop_ms<F>(iters: usize, mut f: F) -> f64
where
    F: FnMut(),
{
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    start.elapsed().as_secs_f64() * 1000.0
}

fn escape_sql(value: &str) -> String {
    value.replace('\'', "''")
}

fn render_markdown(cases: &[BenchmarkCase]) -> String {
    let mut out = String::new();
    out.push_str("# Research Network Benchmark\n\n");
    out.push_str(
        "Compare Sekejap Atomic vs Sekejap SQL vs SQLite on researcher-topic vector search, campus proximity, and collaboration graph workloads.\n\n",
    );
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
