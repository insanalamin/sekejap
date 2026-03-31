use rusqlite::{params, Connection};
use sekejap::SekejapDB;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::tempdir;

const TOTAL_RECORDS: usize = 5_000;

#[derive(Clone)]
struct TextRecord {
    id: String,
    title: String,
    body: String,
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
        benchmark_text_insert(base, &dataset)?,
        benchmark_fulltext_match_count(base, &dataset)?,
        benchmark_fulltext_match_id_only(base, &dataset)?,
        benchmark_ilike_count(base, &dataset)?,
        benchmark_ilike_id_only(base, &dataset)?,
    ];

    let markdown = render_markdown(&cases);
    let out_path =
        PathBuf::from(r"C:\Users\Insan\AndroidStudioProjects\SekejapDB\sekejap-benchmark\techniques\text-ops\RESULT.md");
    fs::create_dir_all(out_path.parent().unwrap())?;
    fs::write(&out_path, markdown)?;
    println!("wrote {}", out_path.display());
    Ok(())
}

fn benchmark_text_insert(base: &Path, records: &[TextRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running text_insert_5000");
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
    });
    let sql_ms = timed_ms(|| {
        for chunk in records.chunks(250) {
            sql_db.mutate(&sql_insert_batch(chunk)).unwrap();
        }
    });
    let sqlite_ms = timed_ms(|| {
        let tx = sqlite_conn.unchecked_transaction().unwrap();
        for record in records {
            tx.execute(
                "INSERT INTO docs (id, title, body) VALUES (?1, ?2, ?3)",
                params![record.id, record.title, record.body],
            )
            .unwrap();
            tx.execute(
                "INSERT INTO docs_fts (id, title, body) VALUES (?1, ?2, ?3)",
                params![record.id, record.title, record.body],
            )
            .unwrap();
        }
        tx.commit().unwrap();
    });

    Ok(BenchmarkCase {
        name: "text_insert_5000",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "text insert with fulltext index enabled",
    })
}

fn benchmark_fulltext_match_count(base: &Path, records: &[TextRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running fulltext_match_count_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "match_count", records)?;
    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db.nodes().all().matching("climate housing").take(50).count().unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db.count("SELECT id FROM docs WHERE MATCHING('climate housing') LIMIT 50").unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let mut stmt = sqlite_conn
            .prepare("SELECT id FROM docs_fts WHERE docs_fts MATCH 'climate housing' LIMIT 50")
            .unwrap();
        let _rows: Vec<String> = stmt.query_map([], |row| row.get(0)).unwrap().map(Result::unwrap).collect();
    });

    Ok(BenchmarkCase {
        name: "fulltext_match_count_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "native fulltext hit count",
    })
}

fn benchmark_fulltext_match_id_only(base: &Path, records: &[TextRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running fulltext_match_id_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "match_id", records)?;
    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db.nodes().all().matching("climate housing").take(50).select(&["id"]).collect().unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db.query("SELECT id FROM docs WHERE MATCHING('climate housing') LIMIT 50").unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let mut stmt = sqlite_conn
            .prepare("SELECT id FROM docs_fts WHERE docs_fts MATCH 'climate housing' LIMIT 50")
            .unwrap();
        let _rows: Vec<String> = stmt.query_map([], |row| row.get(0)).unwrap().map(Result::unwrap).collect();
    });

    Ok(BenchmarkCase {
        name: "fulltext_match_id_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "native fulltext with id projection",
    })
}

fn benchmark_ilike_count(base: &Path, records: &[TextRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running ilike_count_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "ilike_count", records)?;
    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db.nodes().collection("docs").ilike("body", "%climate housing%").take(50).count().unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db.count("SELECT id FROM docs WHERE body ILIKE '%climate housing%' LIMIT 50").unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let mut stmt = sqlite_conn
            .prepare("SELECT id FROM docs WHERE lower(body) LIKE lower('%climate housing%') LIMIT 50")
            .unwrap();
        let _rows: Vec<String> = stmt.query_map([], |row| row.get(0)).unwrap().map(Result::unwrap).collect();
    });

    Ok(BenchmarkCase {
        name: "ilike_count_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "payload text scan with ILIKE semantics",
    })
}

fn benchmark_ilike_id_only(base: &Path, records: &[TextRecord]) -> Result<BenchmarkCase, Box<dyn std::error::Error>> {
    println!("running ilike_id_only");
    let (atomic_db, sql_db, sqlite_conn) = provision_all(base, "ilike_id", records)?;
    let atomic_ms = timed_loop_ms(100, || {
        let _ = atomic_db.nodes().collection("docs").ilike("body", "%climate housing%").take(50).select(&["id"]).collect().unwrap();
    });
    let sql_ms = timed_loop_ms(100, || {
        let _ = sql_db.query("SELECT id FROM docs WHERE body ILIKE '%climate housing%' LIMIT 50").unwrap();
    });
    let sqlite_ms = timed_loop_ms(100, || {
        let mut stmt = sqlite_conn
            .prepare("SELECT id FROM docs WHERE lower(body) LIKE lower('%climate housing%') LIMIT 50")
            .unwrap();
        let _rows: Vec<String> = stmt.query_map([], |row| row.get(0)).unwrap().map(Result::unwrap).collect();
    });

    Ok(BenchmarkCase {
        name: "ilike_id_only",
        atomic_ms,
        sql_ms,
        sqlite_ms,
        note: "ILIKE with id projection",
    })
}

fn create_empty_sekejap_atomic(path: &Path) -> Result<SekejapDB, Box<dyn std::error::Error>> {
    let db = SekejapDB::new(path, TOTAL_RECORDS * 2)?;
    db.init_fulltext(path);
    db.schema().define(
        "docs",
        &json!({
            "hot_fields": {
                "hash_index": ["id"],
                "fulltext": ["title", "body"]
            }
        })
        .to_string(),
    )?;
    Ok(db)
}

fn create_empty_sekejap_sql(path: &Path) -> Result<SekejapDB, Box<dyn std::error::Error>> {
    let db = SekejapDB::new(path, TOTAL_RECORDS * 2)?;
    db.init_fulltext(path);
    db.mutate(
        "CREATE COLLECTION docs (id TEXT PRIMARY KEY, title TEXT, body TEXT) WITH (hash_index = [id], fulltext_index = [title, body])",
    )?;
    Ok(db)
}

fn create_empty_sqlite(path: &Path) -> Result<Connection, Box<dyn std::error::Error>> {
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "CREATE TABLE docs (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT NOT NULL
        );
        CREATE VIRTUAL TABLE docs_fts USING fts5(id, title, body);",
    )?;
    Ok(conn)
}

fn provision_all(
    base: &Path,
    label: &str,
    records: &[TextRecord],
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

    for chunk in records.chunks(250) {
        sql_db.mutate(&sql_insert_batch(chunk))?;
    }

    let tx = sqlite.unchecked_transaction()?;
    for record in records {
        tx.execute(
            "INSERT INTO docs (id, title, body) VALUES (?1, ?2, ?3)",
            params![record.id, record.title, record.body],
        )?;
        tx.execute(
            "INSERT INTO docs_fts (id, title, body) VALUES (?1, ?2, ?3)",
            params![record.id, record.title, record.body],
        )?;
    }
    tx.commit()?;

    Ok((atomic_db, sql_db, sqlite))
}

fn build_dataset(total: usize) -> Vec<TextRecord> {
    (0..total)
        .map(|i| {
            let special = i % 7 == 0;
            let topic = if i % 3 == 0 { "climate" } else { "housing" };
            let title = format!("Policy brief {i} on {topic}");
            let body = if special {
                format!("This article discusses climate housing pressure, public services, and regional planning case {i}.")
            } else {
                format!("This article discusses transport, budget, and local administration case {i}.")
            };
            TextRecord {
                id: format!("doc_{i:05}"),
                title,
                body,
            }
        })
        .collect()
}

fn payload_json_string(record: &TextRecord) -> String {
    json!({
        "_id": format!("docs/{}", record.id),
        "_key": record.id,
        "id": record.id,
        "title": record.title,
        "body": record.body
    })
    .to_string()
}

fn sql_insert_batch(records: &[TextRecord]) -> String {
    let values = records
        .iter()
        .map(|r| {
            format!(
                "('{}', '{}', '{}')",
                escape_sql(&r.id),
                escape_sql(&r.title),
                escape_sql(&r.body)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO docs (id, title, body) VALUES {values}")
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
    out.push_str("# Text Ops Benchmark\n\n");
    out.push_str("Compare Sekejap Atomic vs Sekejap SQL vs SQLite on text-only operations.\n\n");
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
