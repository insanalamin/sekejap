use sekejap::{SekejapDB, WriteOptions};
use rusqlite::{params, Connection};
use std::time::Instant;
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let num_writes = 10_000;
    println!("Benchmark: {} writes", num_writes);

    // --- SQLite (Rust) ---
    let sqlite_path = "/tmp/bench_rust_sqlite.db";
    if Path::new(sqlite_path).exists() {
        fs::remove_file(sqlite_path)?;
    }
    let conn = Connection::open(sqlite_path)?;
    conn.execute(
        "CREATE TABLE nodes (slug TEXT PRIMARY KEY, data TEXT)",
        [],
    )?;

    let start = Instant::now();
    // Use transaction for fairness (standard practice)
    conn.execute("BEGIN", [])?; 
    let mut stmt = conn.prepare("INSERT INTO nodes (slug, data) VALUES (?1, ?2)")?;
    for i in 0..num_writes {
        let slug = format!("node/{}", i);
        let data = format!(r#"{{"id": {}, "name": "Node {}"}}"#, i, i);
        stmt.execute(params![slug, data])?;
    }
    conn.execute("COMMIT", [])?;
    let duration = start.elapsed();
    println!("SQLite (Rust): {:.2?}", duration);


    // --- SekejapDB (Rust) ---
    let sekejap_path = "/tmp/bench_rust_sekejap";
    if Path::new(sekejap_path).exists() {
        fs::remove_dir_all(sekejap_path)?;
    }
    let mut db = SekejapDB::new(Path::new(sekejap_path))?;
    let opts = WriteOptions { publish_now: true, ..Default::default() };

    let start = Instant::now();
    for i in 0..num_writes {
        let slug = format!("node/{}", i);
        let data = format!(r#"{{"id": {}, "name": "Node {}"}}"#, i, i);
        db.write_with_options(&slug, &data, opts.clone())?;
    }
    // Explicit sync to be fair with SQLite's Commit
    db.flush()?; 
    let duration = start.elapsed();
    println!("Sekejap (Rust): {:.2?}", duration);
    
    Ok(())
}