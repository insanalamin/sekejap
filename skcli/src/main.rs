//! sekejap — interactive REPL and one-shot query runner
//!
//! # Usage
//!
//! ```text
//! sekejap                            open in-memory REPL
//! sekejap <path>                     open persistent DB in REPL
//! sekejap --path <path>              same (explicit flag)
//! sekejap <path> "<SQL>"             run SQL, print results, exit
//! sekejap --path <path> "<SQL>"      run SQL, print results, exit
//! echo "SQL;" | sekejap <path>       pipe SQL script, exit when stdin closes
//! ```

use rustyline::DefaultEditor;
use sekejap::CoreDB;
use std::io::{self, IsTerminal, Read};
use std::time::Instant;

// ── Arg parsing ───────────────────────────────────────────────────────────────

struct Args {
    path: Option<String>,
    sql:  Option<String>,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1).peekable();
    let mut path = None;
    let mut sql  = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" | "-p" => {
                path = args.next();
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            "--version" | "-V" => {
                println!("sekejap {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            other => {
                if path.is_none() {
                    path = Some(other.to_string());
                } else {
                    sql = Some(other.to_string());
                }
            }
        }
    }

    Args { path, sql }
}

fn print_usage() {
    println!(
        "sekejap {}

USAGE:
  sekejap                          open in-memory REPL
  sekejap <path>                   open persistent DB in REPL
  sekejap --path <path>            same (explicit flag)
  sekejap <path> \"<SQL>\"           run SQL and exit
  sekejap --path <path> \"<SQL>\"    run SQL and exit
  echo \"SELECT...;\" | sekejap      pipe SQL script

OPTIONS:
  -p, --path <path>    database directory path
  -h, --help           show this help
  -V, --version        show version",
        env!("CARGO_PKG_VERSION")
    );
}

// ── DB open/create ────────────────────────────────────────────────────────────

fn open_db(path: &Option<String>) -> (CoreDB, String) {
    match path {
        Some(p) => match CoreDB::open(p) {
            Ok(db) => (db, p.clone()),
            Err(e) => {
                eprintln!("error: cannot open '{}': {}", p, e);
                std::process::exit(1);
            }
        },
        None => (CoreDB::new(), String::from(":memory:")),
    }
}

// ── Table renderer ────────────────────────────────────────────────────────────

const MAX_COL_WIDTH: usize = 52;
const MIN_COL_WIDTH: usize = 4;

fn format_duration(ns: u128) -> String {
    if ns < 1_000 {
        format!("{ns} ns")
    } else if ns < 1_000_000 {
        format!("{:.2} µs", ns as f64 / 1_000.0)
    } else if ns < 1_000_000_000 {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    } else {
        format!("{:.3} s", ns as f64 / 1_000_000_000.0)
    }
}

fn cell_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn truncate_cell(s: &str, max: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= max {
        s.to_string()
    } else {
        let cut: String = chars[..max - 1].iter().collect();
        format!("{cut}…")
    }
}

fn print_table(hits: Vec<sekejap::Hit>, elapsed_ns: u128) {
    let timing = format_duration(elapsed_ns);
    let count = hits.len();

    if hits.is_empty() {
        println!("(0 rows)  [{timing}]");
        return;
    }

    // Collect column names from first hit's payload keys
    let mut columns: Vec<String> = Vec::new();
    for hit in &hits {
        if let Some(serde_json::Value::Object(map)) = &hit.payload {
            for key in map.keys() {
                if !columns.contains(key) {
                    columns.push(key.clone());
                }
            }
        }
    }

    // No structured payload — show slug column
    if columns.is_empty() {
        let slug_w = hits.iter()
            .map(|h| h.slug.chars().count())
            .max().unwrap_or(5)
            .min(MAX_COL_WIDTH)
            .max("_slug".len());
        let line = "─".repeat(slug_w + 2);
        println!("┌{line}┐");
        println!("│ {:<slug_w$} │", "_slug");
        println!("├{line}┤");
        for hit in &hits {
            println!("│ {:<slug_w$} │", truncate_cell(&hit.slug, MAX_COL_WIDTH));
        }
        println!("└{line}┘");
        if count == 1 { println!("1 row  [{timing}]"); } else { println!("{count} rows  [{timing}]"); }
        return;
    }

    // Compute column widths
    let mut widths: Vec<usize> = columns.iter()
        .map(|c| c.chars().count().max(MIN_COL_WIDTH))
        .collect();
    for hit in &hits {
        if let Some(serde_json::Value::Object(map)) = &hit.payload {
            for (i, col) in columns.iter().enumerate() {
                let val = map.get(col).map(cell_str).unwrap_or_default();
                let display = val.chars().count().min(MAX_COL_WIDTH);
                if display > widths[i] {
                    widths[i] = display;
                }
            }
        }
    }

    let top = widths.iter().map(|w| "─".repeat(w + 2)).collect::<Vec<_>>().join("┬");
    let mid = widths.iter().map(|w| "─".repeat(w + 2)).collect::<Vec<_>>().join("┼");
    let bot = widths.iter().map(|w| "─".repeat(w + 2)).collect::<Vec<_>>().join("┴");

    println!("┌{top}┐");
    let hdr: Vec<String> = columns.iter().zip(&widths)
        .map(|(c, w)| format!(" {:<w$} ", c))
        .collect();
    println!("│{}│", hdr.join("│"));
    println!("├{mid}┤");

    for hit in &hits {
        let cells: Vec<String> = columns.iter().zip(&widths).map(|(col, w)| {
            let val = hit.payload.as_ref()
                .and_then(|p| p.get(col))
                .map(cell_str)
                .unwrap_or_default();
            format!(" {:<w$} ", truncate_cell(&val, MAX_COL_WIDTH))
        }).collect();
        println!("│{}│", cells.join("│"));
    }

    println!("└{bot}┘");
    if count == 1 { println!("1 row  [{timing}]"); } else { println!("{count} rows  [{timing}]"); }
}

// ── SQL execution ─────────────────────────────────────────────────────────────

fn run_sql(db: &mut CoreDB, sql: &str) -> bool {
    let first = sql.split_whitespace().next().unwrap_or("").to_uppercase();
    let t0 = Instant::now();
    match first.as_str() {
        "SELECT" => match db.query(sql) {
            Err(e) => eprintln!("error: {e}"),
            Ok(set) => {
                let hits = set.collect();
                print_table(hits, t0.elapsed().as_nanos());
            }
        },
        "MATCH" => {
            let is_pipeline = sql.split_whitespace().any(|w| w.to_uppercase() == "WITH");
            if is_pipeline {
                match db.pipeline_query(sql) {
                    Err(e) => eprintln!("error: {e}"),
                    Ok(hits) => print_table(hits, t0.elapsed().as_nanos()),
                }
            } else {
                match db.query(sql) {
                    Err(e) => eprintln!("error: {e}"),
                    Ok(set) => {
                        let hits = set.collect();
                        print_table(hits, t0.elapsed().as_nanos());
                    }
                }
            }
        }
        "INSERT" | "UPDATE" | "DELETE" | "CREATE" | "DROP" | "ALTER" => match db.execute(sql) {
            Err(e) => eprintln!("error: {e}"),
            Ok(n) => {
                let timing = format_duration(t0.elapsed().as_nanos());
                if n == 0 {
                    println!("ok  [{timing}]");
                } else if n == 1 {
                    println!("ok — 1 row affected  [{timing}]");
                } else {
                    println!("ok — {n} rows affected  [{timing}]");
                }
            }
        },
        "SHOW" => match db.show(sql) {
            Err(e) => eprintln!("error: {e}"),
            Ok(hits) => print_table(hits, t0.elapsed().as_nanos()),
        },
        _ => eprintln!("unknown statement — supported: SELECT MATCH SHOW INSERT UPDATE DELETE CREATE DROP ALTER"),
    }
    true
}

// ── Dot commands ──────────────────────────────────────────────────────────────

/// Handle a `.command` line. Returns false if the user wants to quit.
fn run_dot(db: &mut CoreDB, label: &mut String, line: &str) -> bool {
    let parts: Vec<&str> = line.splitn(2, ' ').collect();
    match parts[0] {
        ".quit" | ".q" | ".exit" => return false,

        ".help" => print_repl_help(),

        ".open" => {
            let p = parts.get(1).map(|s| s.trim()).unwrap_or("");
            if p.is_empty() {
                eprintln!("usage: .open <path>");
            } else {
                match CoreDB::open(p) {
                    Ok(new_db) => {
                        *db = new_db;
                        *label = p.to_string();
                        println!("opened: {p}");
                    }
                    Err(e) => eprintln!("error: {e}"),
                }
            }
        }

        ".tables" => {
            match db.show("SHOW TABLES") {
                Err(e) => eprintln!("error: {e}"),
                Ok(hits) => {
                    if hits.is_empty() {
                        println!("(no collections)");
                    } else {
                        println!("{:<30} {}", "name", "count");
                        println!("{}", "-".repeat(38));
                        for h in &hits {
                            let name  = h.payload.as_ref().and_then(|p| p["name"].as_str()).unwrap_or("");
                            let count = h.payload.as_ref().and_then(|p| p["count"].as_u64()).unwrap_or(0);
                            println!("{:<30} {}", name, count);
                        }
                    }
                }
            }
        }

        ".schema" => {
            let target = parts.get(1).map(|s| s.trim());
            let names = db.collection_names();
            let cols: Vec<&str> = match target {
                Some(t) if !t.is_empty() => vec![t],
                _ => names.iter().map(String::as_str).collect(),
            };
            let mut found_any = false;
            for col in cols {
                if let Some(ddl) = db.schema_ddl(col) {
                    println!("{ddl};");
                    found_any = true;
                } else if target.is_some() {
                    println!("-- no CREATE TABLE for '{col}'");
                    found_any = true;
                }
            }
            if !found_any {
                println!("(no schemas declared — use CREATE TABLE to add one)");
            }
        }

        ".compact" => match db.compact() {
            Ok(_) => println!("compacted"),
            Err(e) => eprintln!("error: {e}"),
        },

        ".stats" => {
            let nodes = db.node_count();
            let edges = db.edge_count();
            let colls = db.collection_names().len();
            println!("nodes       : {nodes}");
            println!("edges       : {edges}");
            println!("collections : {colls}");
        }

        ".edges" => {
            let arg = parts.get(1).map(|s| s.trim()).unwrap_or("");
            let sql = if arg.is_empty() {
                "SHOW EDGES".to_string()
            } else {
                format!("SHOW EDGES FROM {arg}")
            };
            match db.show(&sql) {
                Err(e) => eprintln!("error: {e}"),
                Ok(hits) => {
                    if hits.is_empty() {
                        println!("(no edges)");
                    } else if arg.is_empty() {
                        println!("{:<25} {:<20} {:<25} {}", "from", "type", "to", "count");
                        println!("{}", "-".repeat(78));
                        for h in &hits {
                            let p     = h.payload.as_ref();
                            let from  = p.and_then(|p| p["from"].as_str()).unwrap_or("");
                            let kind  = p.and_then(|p| p["type"].as_str()).unwrap_or("");
                            let to    = p.and_then(|p| p["to"].as_str()).unwrap_or("");
                            let count = p.and_then(|p| p["count"].as_u64()).unwrap_or(0);
                            println!("{:<25} {:<20} {:<25} {}", from, kind, to, count);
                        }
                    } else {
                        println!("{:<20} {}", "type", "count");
                        println!("{}", "-".repeat(28));
                        for h in &hits {
                            let p     = h.payload.as_ref();
                            let kind  = p.and_then(|p| p["type"].as_str()).unwrap_or("");
                            let count = p.and_then(|p| p["count"].as_u64()).unwrap_or(0);
                            println!("{:<20} {}", kind, count);
                        }
                    }
                }
            }
        }

        other => eprintln!("unknown command: {other}  (try .help)"),
    }
    true
}

fn print_repl_help() {
    println!(
        r#"
sekejap dot commands
────────────────────
.open <path>        open (or create) a persistent DB — replaces current DB
.tables             list all collections
.schema [name]      show CREATE TABLE DDL (all collections if name omitted)
.compact            flush snapshot, truncate WAL
.stats              show node / edge / collection counts
.edges              show full graph schema (from_col → type → to_col), distinct
.edges <col>        show distinct edge types leaving a collection
.help               show this help
.quit / .q / .exit  exit  (also Ctrl+D)

SQL (end each statement with ;)
────────────────────────────────
SELECT * FROM collection [WHERE ...] [ORDER BY ...] [LIMIT n] [OFFSET n];
SELECT * FROM ALL [WHERE ...];
INSERT INTO collection (_key, field, ...) VALUES ('key', val, ...);
UPDATE collection SET field = val [WHERE ...];
DELETE FROM collection [WHERE ...];
CREATE TABLE collection (_key TEXT PRIMARY KEY, field TYPE, ...);
ALTER TABLE collection ADD COLUMN field TYPE;
ALTER TABLE collection DROP COLUMN field;
ALTER TABLE collection RENAME COLUMN old TO new;
ALTER TABLE collection RENAME TO new_name;

Graph edges
───────────
INSERT ('from')-[:KIND {{strength: n}}]->('to');
DELETE ('from')-[:KIND]->('to');

Graph traversal (MATCH)
───────────────────────
MATCH (a:col)-[:rel*1..3]->(b:col) WHERE a._key = 'x' RETURN b;

Graph aggregation
─────────────────
SELECT b._key AS name, SUM(r.weight) AS total
FROM MATCH (a:col)-[r:edge]->(b:col)
GROUP BY b._key ORDER BY total DESC LIMIT 10;

Multi-FROM cross-join
─────────────────────
SELECT a.field AS af, b.field AS bf
FROM MATCH (a:col)-[:edge]->(b), collection_name AS alias;

SELECT list expressions
───────────────────────
var.field AS alias
COUNT(*) | SUM(expr) | AVG(expr) | MIN(expr) | MAX(expr)
PATH_AVG(r.field) | PATH_SUM | PATH_MIN | PATH_MAX | PATH_PRODUCT
PATH_FIRST(r.field) | PATH_LAST(r.field)
CASE WHEN r.field = val THEN 'x' WHEN ... ELSE 'y' END AS alias
AGE_DAYS(var.field) | AGE_HOURS(var.field) | NOW()
JSON_ARRAY_LENGTH(var.field)

Shortest path (0 rows = unreachable, 1 row = found)
────────────────────────────────────────────────────
SELECT a.field AS from_f, b.field AS to_f, r.length AS hops, r._path_keys AS route
FROM MATCH SHORTEST (a)-[r*]->(b)
WHERE a._key = 'start/slug' AND b._key = 'end/slug'
AND ANY(n IN nodes(r) WHERE n.field = 'val')

Introspection
─────────────
SHOW TABLES;
SHOW EDGES;
SHOW EDGES FROM collection;
SHOW EDGES FROM col1 TO col2;
SHOW <collection>;

Filters
───────
=  !=  >  <  >=  <=  BETWEEN n AND n
IN (v1, v2)  NOT IN (v1, v2)
LIKE 'pat'  ILIKE 'pat'
IS NULL  IS NOT NULL
AND  OR  NOT

Spatial
───────
ST_DWithin(geometry, POINT(lon lat), km)
ST_Contains / ST_Within / ST_Intersects
ORDER BY -ST_DISTANCE_KM(geometry, POINT(lon lat)) DESC

Vector
──────
WHERE VECTOR_NEAR(field, [f32, ...], k)
ORDER BY field <=> [f32, ...] ASC     -- cosine nearest-first
ORDER BY field <-> [f32, ...] ASC     -- L2 nearest-first
ORDER BY VECTOR_COSINE(field, [...]) * 0.7 + BM25(bio, 'q') * 0.3 DESC
"#
    );
}

// ── Script mode ───────────────────────────────────────────────────────────────

fn run_script(db: &mut CoreDB, script: &str) {
    let mut label = String::new();
    let mut buf = String::new();
    let mut in_str = false;
    let mut str_char = '\0';

    for line in script.lines() {
        let trimmed = line.trim();

        if !in_str && buf.trim().is_empty() && trimmed.starts_with('.') {
            if !run_dot(db, &mut label, trimmed) {
                return;
            }
            continue;
        }

        if !in_str && (trimmed.is_empty() || trimmed.starts_with("--")) {
            continue;
        }

        for ch in trimmed.chars() {
            match ch {
                '\'' | '"' if !in_str => { in_str = true; str_char = ch; buf.push(ch); }
                c if in_str && c == str_char => { in_str = false; buf.push(ch); }
                ';' if !in_str => {
                    let stmt = buf.trim().to_string();
                    buf.clear();
                    if !stmt.is_empty() {
                        run_sql(db, &stmt);
                    }
                }
                _ => buf.push(ch),
            }
        }
        if !buf.trim().is_empty() {
            buf.push(' ');
        }
    }

    let stmt = buf.trim().to_string();
    if !stmt.is_empty() {
        run_sql(db, &stmt);
    }
}

// ── REPL ──────────────────────────────────────────────────────────────────────

fn repl(mut db: CoreDB, mut label: String) {
    let history_path = std::env::var("HOME").ok()
        .map(|h| std::path::PathBuf::from(h).join(".sekejap_history"));

    let mut rl = DefaultEditor::new().expect("failed to init readline");
    if let Some(ref p) = history_path {
        let _ = rl.load_history(p);
    }

    println!("sekejap {}  —  {label}", env!("CARGO_PKG_VERSION"));
    println!("type .help for commands, .quit to exit\n");

    let mut buf = String::new();

    loop {
        let prompt = if buf.trim().is_empty() {
            "sekejap> ".to_string()
        } else {
            "      ...> ".to_string()
        };

        let line = match rl.readline(&prompt) {
            Ok(l) => l,
            Err(rustyline::error::ReadlineError::Eof)
            | Err(rustyline::error::ReadlineError::Interrupted) => break,
            Err(e) => {
                eprintln!("readline error: {e}");
                break;
            }
        };

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let _ = rl.add_history_entry(trimmed);

        if trimmed.starts_with('.') {
            buf.clear();
            if !run_dot(&mut db, &mut label, trimmed) {
                break;
            }
            continue;
        }

        if !buf.is_empty() {
            buf.push(' ');
        }
        buf.push_str(trimmed);

        if buf.trim_end().ends_with(';') {
            let sql = buf.trim_end_matches(';').trim().to_string();
            buf.clear();
            if !sql.is_empty() {
                run_sql(&mut db, &sql);
            }
        }
    }

    if let Some(ref p) = history_path {
        let _ = rl.save_history(p);
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() {
    let args = parse_args();
    let (mut db, label) = open_db(&args.path);

    if let Some(sql) = args.sql {
        run_script(&mut db, &sql);
        return;
    }

    if !io::stdin().is_terminal() {
        let mut script = String::new();
        io::stdin()
            .read_to_string(&mut script)
            .expect("failed to read stdin");
        run_script(&mut db, &script);
        return;
    }

    repl(db, label);
}
