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
//!   INSERT ('artist/the-vines')-[:has_genre {strength: 10}]->('genre/garage-rock');
//!   DELETE ('artist/the-vines')-[:has_genre]->('genre/garage-rock');
//!   MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines' RETURN g;

use sekejap::CoreDB;
use sekejap::Hit;
use std::io::{self, BufRead, Write};

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

fn run(db: &mut CoreDB, sql: &str) {
    let first = sql.split_whitespace().next().unwrap_or("").to_uppercase();
    match first.as_str() {
        "SELECT" | "MATCH" => {
            match db.query(sql) {
                Err(e) => eprintln!("error: {e}"),
                Ok(set) => {
                    let hits: Vec<Hit> = set.collect();
                    let count = hits.len();
                    for hit in hits {
                        match &hit.payload {
                            Some(v) => println!("{}", serde_json::to_string_pretty(v)
                                .unwrap_or_else(|_| v.to_string())),
                            None    => println!("{}", hit.slug),
                        }
                    }
                    println!("── {} row{} ──", count, if count == 1 { "" } else { "s" });
                }
            }
        }
        "INSERT" | "UPDATE" | "DELETE" => {
            match db.execute(sql) {
                Err(e)    => eprintln!("error: {e}"),
                Ok(count) => println!("ok — {} row{} affected", count, if count == 1 { "" } else { "s" }),
            }
        }
        _ => eprintln!("unknown statement — try SELECT, MATCH, INSERT, UPDATE, DELETE"),
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

MATCH (graph pattern)
─────────────────────
MATCH (a:artist)-[:has_genre]->(g:genre) WHERE a._key = 'the-vines' RETURN g;
MATCH (a:artist)-[r:has_genre]->(g:genre) WHERE a._key = 'the-vines' AND r.strength >= 7 RETURN g;
MATCH (e:event)-[:caused_by*1..5]->(root) WHERE e._key = 'flood' RETURN root;
MATCH (...) RETURN x UNION MATCH (...) RETURN y;

Spatial queries
───────────────
SELECT * FROM places WHERE ST_DWithin(geometry, POINT(lon lat), distance_km);
SELECT * FROM zones WHERE ST_Contains(geometry, POINT(lon lat));
SELECT * FROM places WHERE ST_Within(geometry, POLYGON((lon lat, lon lat, ...)));
SELECT * FROM routes WHERE ST_Intersects(geometry, POLYGON((lon lat, lon lat, ...)));

Filters:  =  !=  >  <  >=  <=  BETWEEN n AND n  IN (...)  LIKE 'pat'  ILIKE 'pat'
Spatial:  ST_DWithin  ST_Contains  ST_Within  ST_Intersects
Booleans: AND  (no OR / NOT yet)
"#);
}
