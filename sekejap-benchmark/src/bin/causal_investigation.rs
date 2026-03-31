use chrono::NaiveDateTime;
use rusqlite::{params, Connection};
use sekejap::SekejapDB;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::tempdir;

const INCIDENT_GROUPS: usize = 2_000;
const QUERY_ITERS: usize = 200;
const HOPS: u32 = 10;
const SQL_BATCH_ROWS: usize = 250;

#[derive(Clone)]
struct CaseNode {
    id: String,
    title: String,
    body: String,
    created_at: String,
    created_epoch_micros: i64,
    edges: Vec<String>,
}

struct InsertCase {
    atomic_ms: f64,
    sql_ms: f64,
    sqlite_ms: f64,
}

struct QueryCase {
    name: &'static str,
    atomic_ms: f64,
    sql_ms: f64,
    sqlite_ms: f64,
    result_count: usize,
    atomic_trace: String,
    sql_trace: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dataset = build_dataset(INCIDENT_GROUPS);
    let temp = tempdir()?;
    let base = temp.path();

    let insert = benchmark_insert(base, &dataset)?;
    let root = benchmark_root_cause_5_hops(base, &dataset)?;
    let exact = benchmark_root_cause_exact_time(base, &dataset)?;
    let fulltext = benchmark_root_cause_fulltext(base, &dataset)?;

    let out_dir = PathBuf::from(
        r"C:\Users\Insan\AndroidStudioProjects\SekejapDB\sekejap-benchmark\cases\causal-investigation",
    );
    fs::create_dir_all(&out_dir)?;
    fs::write(out_dir.join("RESULT.md"), render_markdown(&insert, &[root, exact, fulltext]))?;
    println!("wrote {}", out_dir.join("RESULT.md").display());
    Ok(())
}

fn benchmark_insert(base: &Path, dataset: &[CaseNode]) -> Result<InsertCase, Box<dyn std::error::Error>> {
    let atomic_db = create_empty_atomic(&base.join("causal_insert_atomic"))?;
    let sql_db = create_empty_sql(&base.join("causal_insert_sql"))?;
    let mut sqlite = create_empty_sqlite(&base.join("causal_insert_sqlite.sqlite"))?;

    let atomic_ms = timed_ms(|| {
        bulk_insert_atomic(&atomic_db, dataset).unwrap();
    });
    let sql_ms = timed_ms(|| {
        bulk_insert_sql(&sql_db, dataset).unwrap();
        attach_edges(&sql_db, dataset).unwrap();
    });
    let sqlite_ms = timed_ms(|| {
        let tx = sqlite.unchecked_transaction().unwrap();
        for node in dataset {
            insert_sqlite_node(&tx, node).unwrap();
        }
        for node in dataset {
            for to in &node.edges {
                tx.execute(
                    "INSERT INTO cause_edges (from_id, to_id) VALUES (?1, ?2)",
                    params![node.id, to],
                )
                .unwrap();
            }
        }
        tx.commit().unwrap();
    });

    Ok(InsertCase {
        atomic_ms,
        sql_ms,
        sqlite_ms,
    })
}

fn benchmark_root_cause_5_hops(
    base: &Path,
    dataset: &[CaseNode],
) -> Result<QueryCase, Box<dyn std::error::Error>> {
    with_provisioned(base, "root5", dataset, |atomic_db, sql_db, sqlite, source_id| {
        let source_slug = format!("cases/{}", source_id);
        let sql_query = format!(
            "SELECT id FROM cases TRAVERSE FORWARD caused_by TO cases HOPS {} WHERE id = '{}' LIMIT 64",
            HOPS,
            escape_sql(source_id)
        );
        let sqlite_query = "\
WITH RECURSIVE walk(id, depth) AS ( \
  SELECT ?1 AS id, 0 AS depth \
  UNION ALL \
  SELECT e.to_id, walk.depth + 1 \
  FROM cause_edges e \
  JOIN walk ON e.from_id = walk.id \
  WHERE walk.depth < ?2 \
) \
SELECT DISTINCT id FROM walk LIMIT ?3";

        let atomic_once = atomic_db
            .nodes()
            .one(&source_slug)
            .hops(HOPS)
            .forward("caused_by")
            .take(64)
            .select(&["id"])
            .collect()?;
        let sql_once = sql_db.query(&sql_query)?;
        let sqlite_once = run_sqlite_recursive(sqlite, sqlite_query, source_id, HOPS, 64)?;
        assert_eq!(atomic_once.data.len(), sql_once.data.len());
        assert_eq!(atomic_once.data.len(), sqlite_once.len());

        let atomic_ms = timed_loop_ms(QUERY_ITERS, || {
            let _ = atomic_db
                .nodes()
                .one(&source_slug)
                .hops(HOPS)
                .forward("caused_by")
                .take(64)
                .select(&["id"])
                .collect()
                .unwrap();
        });
        let sql_ms = timed_loop_ms(QUERY_ITERS, || {
            let _ = sql_db.query(&sql_query).unwrap();
        });
        let sqlite_ms = timed_loop_ms(QUERY_ITERS, || {
            let _ = run_sqlite_recursive(sqlite, sqlite_query, source_id, HOPS, 64).unwrap();
        });

        Ok(QueryCase {
            name: "ten_hop_root_cause_trace",
            atomic_ms,
            sql_ms,
            sqlite_ms,
            result_count: atomic_once.data.len(),
            atomic_trace: format_trace(&atomic_once.trace),
            sql_trace: format_trace(&sql_once.trace),
        })
    })
}

fn benchmark_root_cause_exact_time(
    base: &Path,
    dataset: &[CaseNode],
) -> Result<QueryCase, Box<dyn std::error::Error>> {
    with_provisioned(base, "root_exact", dataset, |atomic_db, sql_db, sqlite, source_id| {
        let source_slug = format!("cases/{}", source_id);
        let start = parse_timestamp_to_micros("2024-06-01 00:00:00");
        let end = parse_timestamp_to_micros("2024-12-31 23:59:59");
        let sql_query = format!(
            "SELECT id FROM cases TRAVERSE FORWARD caused_by TO cases HOPS {} WHERE id = '{}' AND created_at >= TIMESTAMP '2024-06-01 00:00:00' AND created_at <= TIMESTAMP '2024-12-31 23:59:59' LIMIT 64",
            HOPS,
            escape_sql(source_id)
        );
        let sqlite_query = "\
WITH RECURSIVE walk(id, depth) AS ( \
  SELECT ?1 AS id, 0 AS depth \
  UNION ALL \
  SELECT e.to_id, walk.depth + 1 \
  FROM cause_edges e \
  JOIN walk ON e.from_id = walk.id \
  WHERE walk.depth < ?2 \
) \
SELECT DISTINCT c.id \
FROM walk w \
JOIN cases c ON c.id = w.id \
WHERE c.created_epoch_micros BETWEEN ?3 AND ?4 \
LIMIT ?5";

        let atomic_once = atomic_db
            .nodes()
            .one(&source_slug)
            .hops(HOPS)
            .forward("caused_by")
            .where_between("createdEpochMicros", start as f64, end as f64)
            .take(64)
            .select(&["id"])
            .collect()?;
        let sql_once = sql_db.query(&sql_query)?;
        let sqlite_once = run_sqlite_exact(sqlite, sqlite_query, source_id, HOPS, start, end, 64)?;
        assert_eq!(atomic_once.data.len(), sql_once.data.len());
        assert_eq!(atomic_once.data.len(), sqlite_once.len());

        let atomic_ms = timed_loop_ms(QUERY_ITERS, || {
            let _ = atomic_db
                .nodes()
                .one(&source_slug)
                .hops(HOPS)
                .forward("caused_by")
                .where_between("createdEpochMicros", start as f64, end as f64)
                .take(64)
                .select(&["id"])
                .collect()
                .unwrap();
        });
        let sql_ms = timed_loop_ms(QUERY_ITERS, || {
            let _ = sql_db.query(&sql_query).unwrap();
        });
        let sqlite_ms = timed_loop_ms(QUERY_ITERS, || {
            let _ = run_sqlite_exact(sqlite, sqlite_query, source_id, HOPS, start, end, 64).unwrap();
        });

        Ok(QueryCase {
            name: "graph_trace_with_exact_time_constraint",
            atomic_ms,
            sql_ms,
            sqlite_ms,
            result_count: atomic_once.data.len(),
            atomic_trace: format_trace(&atomic_once.trace),
            sql_trace: format_trace(&sql_once.trace),
        })
    })
}

fn benchmark_root_cause_fulltext(
    base: &Path,
    dataset: &[CaseNode],
) -> Result<QueryCase, Box<dyn std::error::Error>> {
    with_provisioned(base, "root_text", dataset, |atomic_db, sql_db, sqlite, source_id| {
        let source_slug = format!("cases/{}", source_id);
        let sql_query = format!(
            "SELECT id FROM cases TRAVERSE FORWARD caused_by TO cases HOPS {} WHERE id = '{}' AND body ILIKE '%poor education%' LIMIT 64",
            HOPS,
            escape_sql(source_id)
        );
        let sqlite_query = "\
WITH RECURSIVE walk(id, depth) AS ( \
  SELECT ?1 AS id, 0 AS depth \
  UNION ALL \
  SELECT e.to_id, walk.depth + 1 \
  FROM cause_edges e \
  JOIN walk ON e.from_id = walk.id \
  WHERE walk.depth < ?2 \
) \
SELECT DISTINCT c.id \
FROM walk w \
JOIN cases c ON c.id = w.id \
WHERE lower(c.body) LIKE '%poor education%' \
LIMIT ?3";

        let atomic_once = atomic_db
            .nodes()
            .one(&source_slug)
            .hops(HOPS)
            .forward("caused_by")
            .ilike("body", "%poor education%")
            .take(64)
            .select(&["id"])
            .collect()?;
        let sql_once = sql_db.query(&sql_query)?;
        let sqlite_once = run_sqlite_recursive(sqlite, sqlite_query, source_id, HOPS, 64)?;
        assert_eq!(atomic_once.data.len(), sql_once.data.len());
        assert_eq!(atomic_once.data.len(), sqlite_once.len());

        let atomic_ms = timed_loop_ms(QUERY_ITERS, || {
            let _ = atomic_db
                .nodes()
                .one(&source_slug)
                .hops(HOPS)
                .forward("caused_by")
                .ilike("body", "%poor education%")
                .take(64)
                .select(&["id"])
                .collect()
                .unwrap();
        });
        let sql_ms = timed_loop_ms(QUERY_ITERS, || {
            let _ = sql_db.query(&sql_query).unwrap();
        });
        let sqlite_ms = timed_loop_ms(QUERY_ITERS, || {
            let _ = run_sqlite_recursive(sqlite, sqlite_query, source_id, HOPS, 64).unwrap();
        });

        Ok(QueryCase {
            name: "graph_trace_with_text_evidence",
            atomic_ms,
            sql_ms,
            sqlite_ms,
            result_count: atomic_once.data.len(),
            atomic_trace: format_trace(&atomic_once.trace),
            sql_trace: format_trace(&sql_once.trace),
        })
    })
}

fn with_provisioned<T, F>(
    base: &Path,
    label: &str,
    dataset: &[CaseNode],
    f: F,
) -> Result<T, Box<dyn std::error::Error>>
where
    F: FnOnce(&SekejapDB, &SekejapDB, &Connection, &str) -> Result<T, Box<dyn std::error::Error>>,
{
    let atomic_db = create_empty_atomic(&base.join(format!("{label}_atomic")))?;
    bulk_insert_atomic(&atomic_db, dataset)?;

    let sql_db = create_empty_sql(&base.join(format!("{label}_sql")))?;
    bulk_insert_sql(&sql_db, dataset)?;
    attach_edges(&sql_db, dataset)?;

    let mut sqlite = create_empty_sqlite(&base.join(format!("{label}_sqlite.sqlite")))?;
    let tx = sqlite.unchecked_transaction()?;
    for node in dataset {
        insert_sqlite_node(&tx, node)?;
    }
    for node in dataset {
        for to in &node.edges {
            tx.execute(
                "INSERT INTO cause_edges (from_id, to_id) VALUES (?1, ?2)",
                params![node.id, to],
            )?;
        }
    }
    tx.commit()?;

    let source_id = &dataset[0].id;
    f(&atomic_db, &sql_db, &sqlite, source_id)
}

fn build_dataset(groups: usize) -> Vec<CaseNode> {
    let mut out = Vec::with_capacity(groups * 7);
    for i in 0..groups {
        let place = match i % 6 {
            0 => "Geelong",
            1 => "Ballarat",
            2 => "Dandenong",
            3 => "Bendigo",
            4 => "Shepparton",
            _ => "Melbourne West",
        };
        let month = 1 + (i % 12) as u32;
        let day = 1 + (i % 28) as u32;
        let created_at = format!("2024-{month:02}-{day:02} 09:30:00");
        let epoch = parse_timestamp_to_micros(&created_at);

        let incident = format!("incident_{i:05}");
        let wet = format!("wet_road_{i:05}");
        let drainage = format!("drainage_{i:05}");
        let maintenance = format!("maintenance_{i:05}");
        let budget = format!("budget_{i:05}");
        let education = format!("education_{i:05}");
        let governance = format!("governance_{i:05}");
        let access = format!("access_{i:05}");
        let inequality = format!("inequality_{i:05}");
        let housing = format!("housing_{i:05}");
        let stress = format!("stress_{i:05}");
        let community = format!("community_{i:05}");
        let report = format!("report_{i:05}");

        out.push(CaseNode {
            id: incident.clone(),
            title: format!("Preventable crash at {place}"),
            body: format!("A preventable road crash was reported in {place}. Witnesses noted wet road conditions and poor visibility."),
            created_at: created_at.clone(),
            created_epoch_micros: epoch,
            edges: vec![wet.clone(), report.clone()],
        });
        out.push(CaseNode {
            id: wet.clone(),
            title: "Wet road".to_string(),
            body: format!("Wet road conditions contributed to loss of control near {place}."),
            created_at: created_at.clone(),
            created_epoch_micros: epoch,
            edges: vec![drainage.clone()],
        });
        out.push(CaseNode {
            id: drainage.clone(),
            title: "Drainage failure".to_string(),
            body: format!("Drainage failure left standing water on the road surface in {place}."),
            created_at: created_at.clone(),
            created_epoch_micros: epoch,
            edges: vec![maintenance.clone()],
        });
        out.push(CaseNode {
            id: maintenance.clone(),
            title: "Poor maintenance".to_string(),
            body: format!("Poor maintenance delayed the road safety response in {place}."),
            created_at: created_at.clone(),
            created_epoch_micros: epoch,
            edges: vec![budget.clone()],
        });
        out.push(CaseNode {
            id: budget.clone(),
            title: "Budget neglect".to_string(),
            body: format!("Budget neglect reduced maintenance quality and safety oversight in {place}."),
            created_at: created_at.clone(),
            created_epoch_micros: epoch,
            edges: vec![education.clone()],
        });
        out.push(CaseNode {
            id: education.clone(),
            title: "Poor education".to_string(),
            body: "Poor education and weak safety literacy shaped long-term preventable risk.".to_string(),
            created_at: created_at.clone(),
            created_epoch_micros: epoch,
            edges: vec![governance.clone()],
        });
        out.push(CaseNode {
            id: governance.clone(),
            title: "Weak governance".to_string(),
            body: "Weak governance reduced coordination, accountability, and sustained prevention.".to_string(),
            created_at: created_at.clone(),
            created_epoch_micros: epoch,
            edges: vec![access.clone()],
        });
        out.push(CaseNode {
            id: access.clone(),
            title: "Poor access".to_string(),
            body: "Poor access to services and safe infrastructure increased preventable exposure.".to_string(),
            created_at: created_at.clone(),
            created_epoch_micros: epoch,
            edges: vec![inequality.clone()],
        });
        out.push(CaseNode {
            id: inequality.clone(),
            title: "Structural inequality".to_string(),
            body: "Structural inequality created uneven safety outcomes across communities.".to_string(),
            created_at: created_at.clone(),
            created_epoch_micros: epoch,
            edges: vec![housing.clone()],
        });
        out.push(CaseNode {
            id: housing.clone(),
            title: "Housing insecurity".to_string(),
            body: "Housing insecurity and unstable living conditions increased daily risk and exhaustion.".to_string(),
            created_at: created_at.clone(),
            created_epoch_micros: epoch,
            edges: vec![stress.clone()],
        });
        out.push(CaseNode {
            id: stress.clone(),
            title: "Chronic stress".to_string(),
            body: "Chronic stress reduced attention, resilience, and long-term preventative capacity.".to_string(),
            created_at: created_at.clone(),
            created_epoch_micros: epoch,
            edges: vec![community.clone()],
        });
        out.push(CaseNode {
            id: community.clone(),
            title: "Community fragility".to_string(),
            body: "Community fragility amplified preventable harm when systems failed repeatedly.".to_string(),
            created_at: created_at.clone(),
            created_epoch_micros: epoch,
            edges: vec![],
        });
        out.push(CaseNode {
            id: report.clone(),
            title: "Regional report".to_string(),
            body: format!("Regional report from {place} linked the incident to wet road, maintenance failure, and poor education."),
            created_at,
            created_epoch_micros: epoch,
            edges: vec![education],
        });
    }
    out
}

fn create_empty_atomic(path: &Path) -> Result<SekejapDB, Box<dyn std::error::Error>> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    let db = SekejapDB::new(path, INCIDENT_GROUPS * 16)?;
    db.init_fulltext(path);
    db.schema().define(
        "cases",
        &json!({
            "hash": ["id"],
            "range": ["createdEpochMicros"],
            "fulltext": ["title", "body"]
        })
        .to_string(),
    )?;
    Ok(db)
}

fn create_empty_sql(path: &Path) -> Result<SekejapDB, Box<dyn std::error::Error>> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    let db = SekejapDB::new(path, INCIDENT_GROUPS * 16)?;
    db.init_fulltext(path);
    db.mutate(
        "CREATE COLLECTION cases (id TEXT PRIMARY KEY, title TEXT, body TEXT, created_at TIMESTAMP) WITH (hash_index = [id], range_index = [created_at], fulltext_index = [title, body])",
    )?;
    Ok(db)
}

fn create_empty_sqlite(path: &Path) -> Result<Connection, Box<dyn std::error::Error>> {
    if path.exists() {
        fs::remove_file(path)?;
    }
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "CREATE TABLE cases (\
            id TEXT PRIMARY KEY,\
            title TEXT NOT NULL,\
            body TEXT NOT NULL,\
            created_at TEXT NOT NULL,\
            created_epoch_micros INTEGER NOT NULL\
        );\
        CREATE TABLE cause_edges (from_id TEXT NOT NULL, to_id TEXT NOT NULL);\
        CREATE INDEX idx_cause_edges_from ON cause_edges(from_id);\
        CREATE INDEX idx_cases_created_epoch ON cases(created_epoch_micros);\
        CREATE VIRTUAL TABLE cases_fts USING fts5(id UNINDEXED, title, body);",
    )?;
    Ok(conn)
}

fn bulk_insert_atomic(db: &SekejapDB, dataset: &[CaseNode]) -> Result<(), Box<dyn std::error::Error>> {
    let items = dataset
        .iter()
        .map(|node| {
            (
                format!("cases/{}", node.id),
                json!({
                    "_id": format!("cases/{}", node.id),
                    "_collection": "cases",
                    "_key": node.id,
                    "id": node.id,
                    "title": node.title,
                    "body": node.body,
                    "created_at": node.created_at,
                    "createdEpochMicros": node.created_epoch_micros
                })
                .to_string(),
            )
        })
        .collect::<Vec<_>>();
    let refs = items
        .iter()
        .map(|(slug, payload)| (slug.as_str(), payload.as_str()))
        .collect::<Vec<_>>();
    db.nodes().ingest_raw(&refs)?;

    let edges = dataset
        .iter()
        .flat_map(|node| {
            node.edges.iter().map(move |to| {
                (
                    format!("cases/{}", node.id),
                    format!("cases/{}", to),
                    "caused_by".to_string(),
                    1.0f32,
                )
            })
        })
        .collect::<Vec<_>>();
    let edge_refs = edges
        .iter()
        .map(|(from, to, ty, weight)| (from.as_str(), to.as_str(), ty.as_str(), *weight))
        .collect::<Vec<_>>();
    db.edges().ingest(&edge_refs)?;
    Ok(())
}

fn bulk_insert_sql(db: &SekejapDB, dataset: &[CaseNode]) -> Result<(), Box<dyn std::error::Error>> {
    for chunk in dataset.chunks(SQL_BATCH_ROWS) {
        db.mutate(&sql_insert_batch(chunk))?;
    }
    Ok(())
}

fn attach_edges(db: &SekejapDB, dataset: &[CaseNode]) -> Result<(), Box<dyn std::error::Error>> {
    let edges = dataset
        .iter()
        .flat_map(|node| {
            node.edges.iter().map(move |to| {
                (
                    format!("cases/{}", node.id),
                    format!("cases/{}", to),
                    "caused_by".to_string(),
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

fn insert_sqlite_node(conn: &Connection, node: &CaseNode) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute(
        "INSERT INTO cases (id, title, body, created_at, created_epoch_micros) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![node.id, node.title, node.body, node.created_at, node.created_epoch_micros],
    )?;
    conn.execute(
        "INSERT INTO cases_fts (id, title, body) VALUES (?1, ?2, ?3)",
        params![node.id, node.title, node.body],
    )?;
    Ok(())
}

fn sql_insert_batch(nodes: &[CaseNode]) -> String {
    let values = nodes
        .iter()
        .map(|node| {
            format!(
                "('{}', '{}', '{}', TIMESTAMP '{}')",
                escape_sql(&node.id),
                escape_sql(&node.title),
                escape_sql(&node.body),
                node.created_at
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO cases (id, title, body, created_at) VALUES {values}"
    )
}

fn escape_sql(value: &str) -> String {
    value.replace('\'', "''")
}

fn run_sqlite_recursive(
    conn: &Connection,
    query: &str,
    source_id: &str,
    hops: u32,
    limit: usize,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query_map(params![source_id, hops, limit], |row| row.get(0))?;
    Ok(rows.map(Result::unwrap).collect())
}

fn run_sqlite_exact(
    conn: &Connection,
    query: &str,
    source_id: &str,
    hops: u32,
    start: i64,
    end: i64,
    limit: usize,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query_map(params![source_id, hops, start, end, limit], |row| row.get(0))?;
    Ok(rows.map(Result::unwrap).collect())
}

fn format_trace(trace: &sekejap::Trace) -> String {
    trace
        .steps
        .iter()
        .map(|step| {
            format!(
                "{} [{} -> {} via {} in {}us]",
                step.atom, step.input_size, step.output_size, step.index_used, step.time_us
            )
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn parse_timestamp_to_micros(input: &str) -> i64 {
    NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S")
        .unwrap()
        .and_utc()
        .timestamp_micros()
}

fn timed_ms<F: FnOnce()>(f: F) -> f64 {
    let start = Instant::now();
    f();
    start.elapsed().as_secs_f64() * 1000.0
}

fn timed_loop_ms<F: FnMut()>(iters: usize, mut f: F) -> f64 {
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    start.elapsed().as_secs_f64() * 1000.0
}

fn render_markdown(insert: &InsertCase, queries: &[QueryCase]) -> String {
    let mut out = String::from("# Causal Investigation Benchmark\n\n");
    out.push_str(&format!(
        "Dataset: {} incident groups, chained root-cause graph, repeated {}x.\n\n",
        INCIDENT_GROUPS, QUERY_ITERS
    ));
    out.push_str("## Insert\n\n");
    out.push_str("| Lane | Time ms |\n|---|---:|\n");
    out.push_str(&format!("| Sekejap Atomic | {:.3} |\n", insert.atomic_ms));
    out.push_str(&format!("| Sekejap SQL (batch VALUES) | {:.3} |\n", insert.sql_ms));
    out.push_str(&format!("| SQLite | {:.3} |\n\n", insert.sqlite_ms));

    out.push_str("## Read\n\n");
    out.push_str("| Case | Atomic ms | SQL ms | SQLite ms | Result count |\n");
    out.push_str("|---|---:|---:|---:|---:|\n");
    for query in queries {
        out.push_str(&format!(
            "| {} | {:.3} | {:.3} | {:.3} | {} |\n",
            query.name, query.atomic_ms, query.sql_ms, query.sqlite_ms, query.result_count
        ));
    }

    for query in queries {
        out.push_str(&format!("\n## {}\n\n", query.name));
        out.push_str("Atomic trace:\n\n```txt\n");
        out.push_str(&query.atomic_trace);
        out.push_str("\n```\n\nSQL trace:\n\n```txt\n");
        out.push_str(&query.sql_trace);
        out.push_str("\n```\n");
    }

    out
}
