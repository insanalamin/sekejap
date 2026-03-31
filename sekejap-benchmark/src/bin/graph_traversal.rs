use rusqlite::{params, Connection};
use sekejap::SekejapDB;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tempfile::tempdir;

const TOTAL_NODES: usize = 10_000;
const OUT_DEGREE: usize = 3;
const HOPS: u32 = 5;
const QUERY_ITERS: usize = 200;
const LIMIT: usize = 2_000;

#[derive(Clone)]
struct GraphNode {
    id: String,
    related_to: Vec<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dataset = build_graph_dataset(TOTAL_NODES);
    let temp = tempdir()?;
    let base = temp.path();

    let insert = benchmark_insert(base, &dataset)?;
    let query = benchmark_query(base, &dataset)?;

    let out_dir = PathBuf::from(
        r"C:\Users\Insan\AndroidStudioProjects\SekejapDB\sekejap-benchmark\techniques\graph-traversal",
    );
    fs::create_dir_all(&out_dir)?;
    fs::write(out_dir.join("RESULT.md"), render_markdown(&insert, &query))?;
    println!("wrote {}", out_dir.join("RESULT.md").display());
    Ok(())
}

struct InsertCase {
    atomic_ms: f64,
    sql_ms: f64,
    sqlite_ms: f64,
}

struct QueryCase {
    atomic_ms: f64,
    sql_ms: f64,
    sqlite_ms: f64,
    result_count: usize,
    atomic_trace: String,
    sql_trace: String,
    sqlite_plan: String,
}

fn benchmark_insert(
    base: &Path,
    dataset: &[GraphNode],
) -> Result<InsertCase, Box<dyn std::error::Error>> {
    let atomic_db = create_empty_atomic(&base.join("graph_insert_atomic"))?;
    let sql_db = create_empty_sql(&base.join("graph_insert_sql"))?;
    let mut sqlite = create_empty_sqlite(&base.join("graph_insert_sqlite.sqlite"))?;

    let atomic_ms = timed_ms(|| {
        bulk_insert_atomic(&atomic_db, dataset).unwrap();
    });
    let sql_ms = timed_ms(|| {
        for node in dataset {
            sql_db.mutate(&sql_insert_node(node)).unwrap();
        }
        attach_edges(&sql_db, dataset).unwrap();
    });
    let sqlite_ms = timed_ms(|| {
        let tx = sqlite.unchecked_transaction().unwrap();
        for node in dataset {
            insert_sqlite_node(&tx, node).unwrap();
        }
        for node in dataset {
            for to in &node.related_to {
                tx.execute(
                    "INSERT INTO memory_edges (from_id, to_id) VALUES (?1, ?2)",
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

fn benchmark_query(
    base: &Path,
    dataset: &[GraphNode],
) -> Result<QueryCase, Box<dyn std::error::Error>> {
    let atomic_db = create_empty_atomic(&base.join("graph_query_atomic"))?;
    bulk_insert_atomic(&atomic_db, dataset)?;

    let sql_db = create_empty_sql(&base.join("graph_query_sql"))?;
    for node in dataset {
        sql_db.mutate(&sql_insert_node(node))?;
    }
    attach_edges(&sql_db, dataset)?;

    let mut sqlite = create_empty_sqlite(&base.join("graph_query_sqlite.sqlite"))?;
    let tx = sqlite.unchecked_transaction()?;
    for node in dataset {
        insert_sqlite_node(&tx, node)?;
    }
    for node in dataset {
        for to in &node.related_to {
            tx.execute(
                "INSERT INTO memory_edges (from_id, to_id) VALUES (?1, ?2)",
                params![node.id, to],
            )?;
        }
    }
    tx.commit()?;

    let source_id = &dataset[1234].id;
    let source_slug = format!("memories/{}", source_id);
    let sql_query = format!(
        "SELECT id FROM memories TRAVERSE FORWARD related_to TO memories HOPS {} WHERE id = '{}' LIMIT {}",
        HOPS,
        escape_sql(source_id),
        LIMIT
    );
    let sqlite_query = "\
WITH RECURSIVE walk(id, depth) AS ( \
  SELECT ?1 AS id, 0 AS depth \
  UNION ALL \
  SELECT e.to_id, walk.depth + 1 \
  FROM memory_edges e \
  JOIN walk ON e.from_id = walk.id \
  WHERE walk.depth < ?2 \
) \
SELECT DISTINCT id FROM walk LIMIT ?3";

    let atomic_once = atomic_db
        .nodes()
        .one(&source_slug)
        .hops(HOPS)
        .forward("related_to")
        .take(LIMIT)
        .select(&["id"])
        .collect()?;
    let sql_once = sql_db.query(&sql_query)?;
    let sqlite_once = run_sqlite_recursive(&sqlite, sqlite_query, source_id, HOPS, LIMIT)?;

    assert_eq!(atomic_once.data.len(), sql_once.data.len());
    assert_eq!(atomic_once.data.len(), sqlite_once.len());

    let atomic_ms = timed_loop_ms(QUERY_ITERS, || {
        let _ = atomic_db
            .nodes()
            .one(&source_slug)
            .hops(HOPS)
            .forward("related_to")
            .take(LIMIT)
            .select(&["id"])
            .collect()
            .unwrap();
    });
    let sql_ms = timed_loop_ms(QUERY_ITERS, || {
        let _ = sql_db.query(&sql_query).unwrap();
    });
    let sqlite_ms = timed_loop_ms(QUERY_ITERS, || {
        let _ = run_sqlite_recursive(&sqlite, sqlite_query, source_id, HOPS, LIMIT).unwrap();
    });

    let sqlite_plan = explain_sqlite_recursive(&sqlite, sqlite_query, source_id, HOPS, LIMIT)?;

    Ok(QueryCase {
        atomic_ms,
        sql_ms,
        sqlite_ms,
        result_count: atomic_once.data.len(),
        atomic_trace: format_trace(&atomic_once.trace),
        sql_trace: format_trace(&sql_once.trace),
        sqlite_plan,
    })
}

fn build_graph_dataset(count: usize) -> Vec<GraphNode> {
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let related_to = (1..=OUT_DEGREE)
            .map(|delta| format!("mem_{:05}", (i + delta) % count))
            .collect::<Vec<_>>();
        out.push(GraphNode {
            id: format!("mem_{:05}", i),
            related_to,
        });
    }
    out
}

fn create_empty_atomic(path: &Path) -> Result<SekejapDB, Box<dyn std::error::Error>> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    let db = SekejapDB::new(path, TOTAL_NODES * 4)?;
    db.schema().define("memories", &json!({ "hash": ["id"] }).to_string())?;
    Ok(db)
}

fn create_empty_sql(path: &Path) -> Result<SekejapDB, Box<dyn std::error::Error>> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    fs::create_dir_all(path)?;
    let db = SekejapDB::new(path, TOTAL_NODES * 4)?;
    db.mutate(
        "CREATE COLLECTION memories (id TEXT PRIMARY KEY) WITH (hash_index = [id])",
    )?;
    Ok(db)
}

fn create_empty_sqlite(path: &Path) -> Result<Connection, Box<dyn std::error::Error>> {
    if path.exists() {
        fs::remove_file(path)?;
    }
    let conn = Connection::open(path)?;
    conn.execute_batch(
        "CREATE TABLE memories (id TEXT PRIMARY KEY);\
         CREATE TABLE memory_edges (from_id TEXT NOT NULL, to_id TEXT NOT NULL);\
         CREATE INDEX idx_edges_from ON memory_edges(from_id);",
    )?;
    Ok(conn)
}

fn bulk_insert_atomic(db: &SekejapDB, dataset: &[GraphNode]) -> Result<(), Box<dyn std::error::Error>> {
    let nodes = dataset
        .iter()
        .map(|node| {
            (
                format!("memories/{}", node.id),
                json!({
                    "_id": format!("memories/{}", node.id),
                    "_collection": "memories",
                    "_key": node.id,
                    "id": node.id,
                })
                .to_string(),
            )
        })
        .collect::<Vec<_>>();
    let refs = nodes
        .iter()
        .map(|(slug, payload)| (slug.as_str(), payload.as_str()))
        .collect::<Vec<_>>();
    db.nodes().ingest_raw(&refs)?;

    let edges = dataset
        .iter()
        .flat_map(|node| {
            node.related_to.iter().map(move |to| {
                (
                    format!("memories/{}", node.id),
                    format!("memories/{}", to),
                    "related_to".to_string(),
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

fn attach_edges(db: &SekejapDB, dataset: &[GraphNode]) -> Result<(), Box<dyn std::error::Error>> {
    let edges = dataset
        .iter()
        .flat_map(|node| {
            node.related_to.iter().map(move |to| {
                (
                    format!("memories/{}", node.id),
                    format!("memories/{}", to),
                    "related_to".to_string(),
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

fn insert_sqlite_node(conn: &Connection, node: &GraphNode) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute("INSERT INTO memories (id) VALUES (?1)", params![node.id])?;
    Ok(())
}

fn sql_insert_node(node: &GraphNode) -> String {
    format!(
        "INSERT INTO memories (id) VALUES ('{}')",
        escape_sql(&node.id)
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

fn explain_sqlite_recursive(
    conn: &Connection,
    query: &str,
    source_id: &str,
    hops: u32,
    limit: usize,
) -> Result<String, Box<dyn std::error::Error>> {
    let explain_sql = format!("EXPLAIN QUERY PLAN {query}");
    let mut stmt = conn.prepare(&explain_sql)?;
    let rows = stmt.query_map(params![source_id, hops, limit], |row| {
        let detail: String = row.get(3)?;
        Ok(detail)
    })?;
    let plan = rows.map(Result::unwrap).collect::<Vec<_>>().join(" | ");
    Ok(plan)
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

fn render_markdown(insert: &InsertCase, query: &QueryCase) -> String {
    format!(
        "# Graph Traversal Benchmark\n\n\
Dataset: {TOTAL_NODES} nodes, out-degree {OUT_DEGREE}, traversal depth {HOPS}, repeated {QUERY_ITERS}x.\n\n\
## Insert\n\n\
| Lane | Time ms |\n|---|---:|\n| Sekejap Atomic | {:.3} |\n| Sekejap SQL | {:.3} |\n| SQLite | {:.3} |\n\n\
## 5-Hop Traverse\n\n\
Each lane starts from the same source id, traverses `related_to` forward to depth {HOPS}, and returns `id` only.\n\n\
| Lane | Time ms |\n|---|---:|\n| Sekejap Atomic | {:.3} |\n| Sekejap SQL | {:.3} |\n| SQLite | {:.3} |\n\n\
Result count per run: {}\n\n\
## Trace\n\n\
Atomic:\n\n```\n{}\n```\n\n\
SQL:\n\n```\n{}\n```\n\n\
SQLite EXPLAIN QUERY PLAN:\n\n```\n{}\n```\n",
        insert.atomic_ms,
        insert.sql_ms,
        insert.sqlite_ms,
        query.atomic_ms,
        query.sql_ms,
        query.sqlite_ms,
        query.result_count,
        query.atomic_trace,
        query.sql_trace,
        query.sqlite_plan,
    )
}
