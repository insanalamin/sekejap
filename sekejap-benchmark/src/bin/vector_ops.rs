use rusqlite::{params, Connection};
use sekejap::SekejapDB;
use serde_json::json;
use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::tempdir;

const TOTAL_RECORDS: usize = 2_000;
const DIM: usize = 64;
const TOP_K: usize = 20;

#[derive(Clone)]
struct VectorRecord {
    id: String,
    title: String,
    embedding: Vec<f32>,
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
        benchmark_vector_insert(base, &dataset)?,
        benchmark_vector_similarity_count(base, &dataset)?,
        benchmark_vector_similarity_id_only(base, &dataset)?,
    ];

    let markdown = render_markdown(&cases);
    let out_path =
        PathBuf::from(r"C:\Users\Insan\AndroidStudioProjects\SekejapDB\sekejap-benchmark\techniques\vector-ops\RESULT.md");
    fs::create_dir_all(out_path.parent().unwrap())?;
    fs::write(&out_path, markdown)?;
    println!("wrote {}", out_path.display());
    Ok(())
}

fn benchmark_vector_insert(base: &Path, records: &[VectorRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running vector_insert_2000");
    let atomic_db = create_empty_sekejap_atomic(&base.join("insert_atomic"))?;
    let sql_db = create_empty_sekejap_sql(&base.join("insert_sql"))?;
    let mut sqlite_conn = create_empty_sqlite(&base.join("insert_sqlite.sqlite"))?;

    let atomic_ms = timed_ms(|| {
        let items = records
            .iter()
            .map(|r| (format!("docs/{}", r.id), payload_json_string(r)))
            .collect::<Vec<_>>();
        let refs = items
            .iter()
            .map(|(slug, json)| (slug.as_str(), json.as_str()))
            .collect::<Vec<_>>();
        atomic_db.nodes().ingest_raw(&refs).unwrap();
        atomic_db.init_hnsw(16);
        atomic_db.nodes().build_hnsw().unwrap();
    });
    let sql_ms = timed_ms(|| {
        for chunk in records.chunks(200) {
            sql_db.mutate(&sql_insert_batch(chunk)).unwrap();
        }
        sql_db.init_hnsw(16);
        sql_db.nodes().build_hnsw().unwrap();
    });
    let sqlite_ms = timed_ms(|| {
        let tx = sqlite_conn.unchecked_transaction().unwrap();
        for record in records {
            tx.execute(
                "INSERT INTO docs (id, title, embedding_json) VALUES (?1, ?2, ?3)",
                params![record.id, record.title, serde_json::to_string(&record.embedding).unwrap()],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    });

    Ok(BenchmarkCase {
        name: "vector_insert_2000",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "vector insert with HNSW build after bulk load",
    })
}

fn benchmark_vector_similarity_count(base: &Path, records: &[VectorRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running vector_similarity_count_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "count", records)?;
    let query_vec = &records[321].embedding;
    let sql_query = format!(
        "SELECT id FROM docs WHERE VECTOR_NEAR(embedding, {}, {}) LIMIT {}",
        vector_literal(query_vec),
        TOP_K,
        TOP_K
    );

    let atomic_ms = timed_loop_ms(80, || {
        let _ = atomic_db
            .nodes()
            .collection("docs")
            .similar(query_vec, TOP_K)
            .take(TOP_K)
            .count()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(80, || {
        let _ = sql_db.count(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(80, || {
        let mut stmt = sqlite_conn
            .prepare("SELECT id, embedding_json FROM docs")
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                let id: String = row.get(0)?;
                let emb_json: String = row.get(1)?;
                Ok((id, emb_json))
            })
            .unwrap();
        let mut scored = Vec::new();
        for row in rows {
            let (id, emb_json) = row.unwrap();
            let emb: Vec<f32> = serde_json::from_str(&emb_json).unwrap();
            scored.push((id, cosine_distance(query_vec, &emb)));
        }
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        let _top: Vec<_> = scored.into_iter().take(TOP_K).collect();
    });

    Ok(BenchmarkCase {
        name: "vector_similarity_count_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "top-k semantic similarity count path",
    })
}

fn benchmark_vector_similarity_id_only(base: &Path, records: &[VectorRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running vector_similarity_id_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "id_only", records)?;
    let query_vec = &records[321].embedding;
    let sql_query = format!(
        "SELECT id FROM docs WHERE VECTOR_NEAR(embedding, {}, {}) LIMIT {}",
        vector_literal(query_vec),
        TOP_K,
        TOP_K
    );

    let atomic_ms = timed_loop_ms(80, || {
        let _ = atomic_db
            .nodes()
            .collection("docs")
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
        let mut stmt = sqlite_conn
            .prepare("SELECT id, embedding_json FROM docs")
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                let id: String = row.get(0)?;
                let emb_json: String = row.get(1)?;
                Ok((id, emb_json))
            })
            .unwrap();
        let mut scored = Vec::new();
        for row in rows {
            let (id, emb_json) = row.unwrap();
            let emb: Vec<f32> = serde_json::from_str(&emb_json).unwrap();
            scored.push((id, cosine_distance(query_vec, &emb)));
        }
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        let _top: Vec<_> = scored.into_iter().take(TOP_K).map(|(id, _)| id).collect();
    });

    Ok(BenchmarkCase {
        name: "vector_similarity_id_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "top-k semantic similarity with id projection",
    })
}

fn create_empty_sekejap_atomic(path: &Path) -> Result<SekejapDB, Box<dyn std::error::Error>> {
    let db = SekejapDB::new(path, TOTAL_RECORDS * 2)?;
    db.schema().define(
        "docs",
        &json!({
            "hot_fields": {
                "hash_index": ["id"],
                "vector": ["embedding"]
            }
        })
        .to_string(),
    )?;
    Ok(db)
}

fn create_empty_sekejap_sql(path: &Path) -> Result<SekejapDB, Box<dyn std::error::Error>> {
    let db = SekejapDB::new(path, TOTAL_RECORDS * 2)?;
    db.mutate(
        &format!(
            "CREATE COLLECTION docs (id TEXT PRIMARY KEY, title TEXT, embedding VECTOR({})) WITH (hash_index = [id], vector_index = [embedding])",
            DIM
        ),
    )?;
    Ok(db)
}

fn create_empty_sqlite(path: &Path) -> Result<Connection, Box<dyn std::error::Error>> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "CREATE TABLE docs (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            embedding_json TEXT NOT NULL
        );
        CREATE INDEX idx_docs_title ON docs(title);",
    )?;
    Ok(conn)
}

fn provision_all(
    base: &Path,
    label: &str,
    records: &[VectorRecord],
) -> Result<(SekejapDB, SekejapDB, Connection), Box<dyn std::error::Error>> {
    let atomic_db = create_empty_sekejap_atomic(&base.join(format!("{label}_atomic")))?;
    let sql_db = create_empty_sekejap_sql(&base.join(format!("{label}_sql")))?;
    let mut sqlite = create_empty_sqlite(&base.join(format!("{label}_sqlite.sqlite")))?;

    let items = records
        .iter()
        .map(|r| (format!("docs/{}", r.id), payload_json_string(r)))
        .collect::<Vec<_>>();
    let refs = items
        .iter()
        .map(|(slug, json)| (slug.as_str(), json.as_str()))
        .collect::<Vec<_>>();
    atomic_db.nodes().ingest_raw(&refs)?;
    atomic_db.init_hnsw(16);
    atomic_db.nodes().build_hnsw()?;

    for chunk in records.chunks(200) {
        sql_db.mutate(&sql_insert_batch(chunk))?;
    }
    sql_db.init_hnsw(16);
    sql_db.nodes().build_hnsw()?;

    let tx = sqlite.unchecked_transaction()?;
    for record in records {
        tx.execute(
            "INSERT INTO docs (id, title, embedding_json) VALUES (?1, ?2, ?3)",
            params![record.id, record.title, serde_json::to_string(&record.embedding)?],
        )?;
    }
    tx.commit()?;

    Ok((atomic_db, sql_db, sqlite))
}

fn build_dataset(total: usize) -> Vec<VectorRecord> {
    (0..total)
        .map(|i| VectorRecord {
            id: format!("doc_{i:05}"),
            title: format!("Vector document {i}"),
            embedding: build_embedding(i),
        })
        .collect()
}

fn build_embedding(seed: usize) -> Vec<f32> {
    let cluster = seed % 10;
    (0..DIM)
        .map(|d| {
            let base = if d % 10 == cluster { 0.9 } else { 0.1 };
            base + ((seed * 31 + d * 17) % 100) as f32 / 500.0
        })
        .collect()
}

fn payload_json_string(record: &VectorRecord) -> String {
    json!({
        "_id": format!("docs/{}", record.id),
        "_key": record.id,
        "id": record.id,
        "title": record.title,
        "embedding": record.embedding
    })
    .to_string()
}

fn sql_insert_batch(records: &[VectorRecord]) -> String {
    let values = records
        .iter()
        .map(|r| {
            format!(
                "('{}', '{}', {})",
                escape_sql(&r.id),
                escape_sql(&r.title),
                vector_literal(&r.embedding)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO docs (id, title, embedding) VALUES {values}")
}

fn vector_literal(values: &[f32]) -> String {
    let inner = values
        .iter()
        .map(|v| format!("{v:.6}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{inner}]")
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
    out.push_str("# Vector Ops Benchmark\n\n");
    out.push_str("Compare Sekejap Atomic vs Sekejap SQL vs SQLite on vector-only operations.\n\n");
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
