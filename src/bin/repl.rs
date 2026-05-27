//! sekejap interactive REPL
//!
//! Usage:
//!   cargo run --bin repl
//!   cargo run --bin repl -- path/to/db
//!
//! Commands:
//!   .open <path>   open (or create) a persistent DB at path
//!   .compact       write snapshot + truncate WAL
//!   .help          show this help
//!   .quit / .q     exit
//!
//! SQL (terminate with ;):
//!   SELECT * FROM artist WHERE name = 'The Vines';
//!   INSERT INTO artist (_key, name, city) VALUES ('the-vines', 'The Vines', 'Sydney');
//!   UPDATE artist SET active = true WHERE _key = 'the-vines';
//!   DELETE FROM artist WHERE active = false;
//!   CREATE TABLE artist (_key TEXT PRIMARY KEY, name TEXT, city TEXT);
//!   ALTER TABLE artist ADD COLUMN active BOOLEAN;
//!   DROP TABLE artist;
//!   INSERT ('artist/the-vines')-[:has_genre {strength: 10}]->('genre/garage-rock');
//!   DELETE ('artist/the-vines')-[:has_genre]->('genre/garage-rock');
//!   SELECT g._key FROM MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines';
//!   MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines' RETURN g._key;
//!   SHOW TABLES;
//!   SHOW EDGES;

use sekejap::CoreDB;
use sekejap::Hit;
use std::io::{self, BufRead, Write};
use std::time::Instant;

fn main() {
    let arg = std::env::args().nth(1);

    let mut db: Option<CoreDB> = None;
    let mut db_label = String::from("(in-memory)");

    // If a path was given, open it immediately.
    if let Some(path) = arg {
        match CoreDB::open(&path) {
            Ok(d) => {
                db_label = path.clone();
                db = Some(d);
                println!("opened: {path}");
            }
            Err(e) => {
                eprintln!("error opening {path}: {e}");
                std::process::exit(1);
            }
        }
    }

    // Fall back to in-memory if no path given.
    if db.is_none() {
        db = Some(CoreDB::new());
    }
    let db = db.as_mut().unwrap();

    println!("sekejap REPL — {db_label}");
    println!("type .help for commands, .quit to exit");
    println!();

    let stdin = io::stdin();
    let mut buf = String::new();   // accumulates multi-line input until ';'

    loop {
        // Print prompt: "> " if fresh line, "  " if continuing multi-line
        if buf.trim().is_empty() {
            print!("> ");
        } else {
            print!("  ");
        }
        io::stdout().flush().unwrap();

        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break,   // EOF
            Err(e) => { eprintln!("read error: {e}"); break; }
            Ok(_) => {}
        }

        let trimmed = line.trim();

        // ── Dot commands ──────────────────────────────────────────────────────
        if trimmed.starts_with('.') {
            buf.clear();
            let parts: Vec<&str> = trimmed.splitn(2, ' ').collect();
            match parts[0] {
                ".quit" | ".q" => break,
                ".help" => print_help(),
                ".compact" => {
                    match db.compact() {
                        Ok(_)  => println!("compacted"),
                        Err(e) => eprintln!("error: {e}"),
                    }
                }
                ".open" => {
                    if parts.len() < 2 || parts[1].trim().is_empty() {
                        eprintln!("usage: .open <path>");
                    } else {
                        eprintln!("note: .open not supported after startup — restart with: cargo run --bin repl -- {}", parts[1].trim());
                    }
                }
                other => eprintln!("unknown command: {other}  (try .help)"),
            }
            continue;
        }

        // ── SQL accumulation ──────────────────────────────────────────────────
        if !trimmed.is_empty() {
            if !buf.is_empty() { buf.push(' '); }
            buf.push_str(trimmed);
        }

        // Execute when we see a semicolon at the end.
        if buf.trim_end().ends_with(';') {
            let sql = buf.trim_end_matches(';').trim().to_string();
            buf.clear();
            if sql.is_empty() { continue; }
            run(db, &sql);
        }
    }
}

fn fmt_duration(d: std::time::Duration) -> String {
    let us = d.as_micros();
    if us < 1_000 {
        format!("{us} µs")
    } else if us < 1_000_000 {
        format!("{:.2} ms", us as f64 / 1_000.0)
    } else {
        format!("{:.3} s", d.as_secs_f64())
    }
}

fn run(db: &mut CoreDB, sql: &str) {
    let first = sql.split_whitespace().next().unwrap_or("").to_uppercase();
    match first.as_str() {
        "EXPLAIN" => {
            let t0 = Instant::now();
            // Check for EXPLAIN ANALYZE
            let rest = sql.strip_prefix("EXPLAIN").unwrap_or(sql).trim();
            let (analyze, inner_sql) = if rest.to_uppercase().starts_with("ANALYZE") {
                (true, rest.strip_prefix("ANALYZE").or_else(|| rest.strip_prefix("analyze")).unwrap_or(rest).trim())
            } else {
                (false, rest)
            };
            let result = if analyze {
                db.explain_analyze(inner_sql)
            } else {
                db.explain(inner_sql)
            };
            match result {
                Err(e) => eprintln!("error: {e}"),
                Ok(hits) => {
                    let elapsed = t0.elapsed();
                    let count = hits.len();
                    for hit in hits {
                        match &hit.payload {
                            Some(v) => println!("{}", serde_json::to_string_pretty(v)
                                .unwrap_or_else(|_| v.to_string())),
                            None => println!("{}", hit.slug),
                        }
                    }
                    println!("── {} step{} in {} ──", count, if count == 1 { "" } else { "s" }, fmt_duration(elapsed));
                }
            }
        }
        "SELECT" | "MATCH" => {
            let t0 = Instant::now();
            match db.query(sql) {
                Err(e) => eprintln!("error: {e}"),
                Ok(set) => {
                    let hits: Vec<Hit> = set.collect();
                    let elapsed = t0.elapsed();
                    let count = hits.len();
                    for hit in hits {
                        match &hit.payload {
                            Some(v) => println!("{}", serde_json::to_string_pretty(v)
                                .unwrap_or_else(|_| v.to_string())),
                            None    => println!("{}", hit.slug),
                        }
                    }
                    println!("── {} row{} in {} ──", count, if count == 1 { "" } else { "s" }, fmt_duration(elapsed));
                }
            }
        }
        "SHOW" => {
            let t0 = Instant::now();
            match db.show(sql) {
                Err(e) => eprintln!("error: {e}"),
                Ok(hits) => {
                    let elapsed = t0.elapsed();
                    let count = hits.len();
                    for hit in hits {
                        match &hit.payload {
                            Some(v) => println!("{}", serde_json::to_string_pretty(v)
                                .unwrap_or_else(|_| v.to_string())),
                            None    => println!("{}", hit.slug),
                        }
                    }
                    println!("── {} row{} in {} ──", count, if count == 1 { "" } else { "s" }, fmt_duration(elapsed));
                }
            }
        }
        "INSERT" | "UPDATE" | "DELETE" | "CREATE" | "ALTER" | "DROP" | "REINDEX" => {
            let t0 = Instant::now();
            match db.execute(sql) {
                Err(e)    => eprintln!("error: {e}"),
                Ok(count) => {
                    let elapsed = t0.elapsed();
                    println!("ok — {} row{} affected in {}", count, if count == 1 { "" } else { "s" }, fmt_duration(elapsed));
                }
            }
        }
        _ => eprintln!("unknown statement — try SELECT, MATCH, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, SHOW"),
    }
}

fn print_help() {
    println!(r#"
sekejap REPL commands
──────────────────────────
.open <path>   open persistent DB (restart required)
.compact       flush snapshot, truncate WAL
.help          show this help
.quit / .q     exit

SQL (end each statement with ;)
────────────────────────────────
SELECT * FROM collection|ALL [WHERE ...] [ORDER BY ...] [LIMIT n] [OFFSET n];
INSERT INTO collection (_key, field, ...) VALUES ('key', val, ...);
UPDATE collection SET field = val [, ...] [WHERE ...];
DELETE FROM collection|ALL [WHERE ...];
INSERT ('from')-[:KIND {{strength: n, key: val}}]->('to');
DELETE ('from')-[:KIND]->('to');

Schema
──────
CREATE TABLE col (_key TEXT PRIMARY KEY, field TYPE, ...);
CREATE INDEX ON col USING hash|btree|gin|spatial|hnsw|bm25 (field);
ALTER TABLE col ADD COLUMN field TYPE;
ALTER TABLE col DROP COLUMN field;
ALTER TABLE col RENAME COLUMN old TO new;
ALTER TABLE col RENAME TO new_name;
DROP TABLE col;
DROP INDEX ON col USING type (field);
REINDEX ON col USING type (field);

Graph traversal
───────────────
SELECT b._key FROM MATCH (a:col)-[:edge]->(b:col) WHERE a._key = 'x';
SELECT b._key, COUNT(a) FROM MATCH (a:col)-[r:edge]->(b:col) GROUP BY b._key;

-- Multi-stage WITH chaining
SELECT c.name AS city, COUNT(*) AS n
FROM MATCH (a:users)-[:knows]->(b:users)
WHERE a._key = 'alice'
WITH b
MATCH (b)-[:lives_in]->(c:cities)
GROUP BY c.name;

-- MATCH...RETURN
MATCH (a:col)-[:edge]->(b:col) RETURN a._key, b.name;
MATCH (a:col)-[:e]->(b) WITH b MATCH (b)-[:e2]->(c) RETURN c._key;

-- Shortest path
SELECT r.length AS hops FROM MATCH SHORTEST (a)-[r*]->(b)
WHERE a._key = 'start' AND b._key = 'end';

-- Variable-depth hops
SELECT b._key FROM MATCH (a:col)-[:edge*1..5]->(b:col) WHERE a._key = 'x';

Spatial queries
───────────────
SELECT * FROM places WHERE ST_DWithin(geometry, POINT(lon lat), distance_km);
SELECT * FROM zones WHERE ST_Contains(geometry, POINT(lon lat));

Introspection
─────────────
SHOW TABLES;
SHOW EDGES;
SHOW EDGES FROM collection;
SHOW EDGES FROM col1 TO col2;
SHOW collection;

Filters:  =  !=  >  <  >=  <=  BETWEEN n AND n  IN (...)  LIKE 'pat'  ILIKE 'pat'
Spatial:  ST_DWithin  ST_Contains  ST_Within  ST_Intersects
Booleans: AND  OR  NOT  (parenthesized groups supported)
"#);
}
