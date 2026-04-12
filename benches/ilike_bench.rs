use criterion::{black_box, criterion_group, criterion_main, Criterion};
use memchr::memmem;
use rusqlite::params;
use sekejap::CoreDB;
use serde_json::json;

fn setup_core() -> CoreDB {
    let mut db = CoreDB::new();
    for i in 0..10_000usize {
        let cat = format!("cat{}", i % 10);
        let name = format!(
            "Product {} {}",
            i,
            if i % 3 == 0 {
                "Alpha"
            } else if i % 3 == 1 {
                "Beta"
            } else {
                "Gamma"
            }
        );
        db.put(
            &format!("p:{i}"),
            &json!({
                "_collection": "products",
                "category": cat,
                "name": name,
            })
            .to_string(),
        )
        .unwrap();
    }
    db
}

fn setup_core_indexed() -> CoreDB {
    let mut db = setup_core();
    db.build_text_indexes();
    db
}

fn setup_sqlite() -> rusqlite::Connection {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute(
        "CREATE TABLE products (id TEXT PRIMARY KEY, name TEXT, category TEXT)",
        [],
    )
    .unwrap();
    for i in 0..10_000usize {
        let cat = format!("cat{}", i % 10);
        let name = format!(
            "Product {} {}",
            i,
            if i % 3 == 0 {
                "Alpha"
            } else if i % 3 == 1 {
                "Beta"
            } else {
                "Gamma"
            }
        );
        conn.execute(
            "INSERT INTO products (id, name, category) VALUES (?1, ?2, ?3)",
            params![format!("p:{i}"), name, cat],
        )
        .unwrap();
    }
    conn
}

fn setup_sqlite_indexed(conn: &rusqlite::Connection) {
    conn.execute("CREATE INDEX idx_products_name ON products(name)", [])
        .unwrap();
}

fn bench_ilike(c: &mut Criterion) {
    let db_unindexed = setup_core();
    let db_indexed = setup_core_indexed();
    let conn_unindexed = setup_sqlite();
    let mut conn_indexed = setup_sqlite();
    setup_sqlite_indexed(&conn_indexed);

    let mut group = c.benchmark_group("ilike_10k");

    // Without text index (full scan)
    group.bench_function("core_fullscan_no_index", |b| {
        b.iter(|| {
            black_box(
                db_unindexed
                    .query("SELECT * FROM products WHERE name ILIKE '%Alpha%'")
                    .unwrap()
                    .count(),
            )
        })
    });

    // With text index
    group.bench_function("core_fullscan_indexed", |b| {
        b.iter(|| {
            black_box(
                db_indexed
                    .query("SELECT * FROM products WHERE name ILIKE '%Alpha%'")
                    .unwrap()
                    .count(),
            )
        })
    });

    // LIMIT 50 without index
    group.bench_function("core_limit50_no_index", |b| {
        b.iter(|| {
            black_box(
                db_unindexed
                    .query("SELECT * FROM products WHERE name ILIKE '%Alpha%' LIMIT 50")
                    .unwrap()
                    .count(),
            )
        })
    });

    // LIMIT 50 with index
    group.bench_function("core_limit50_indexed", |b| {
        b.iter(|| {
            black_box(
                db_indexed
                    .query("SELECT * FROM products WHERE name ILIKE '%Alpha%' LIMIT 50")
                    .unwrap()
                    .count(),
            )
        })
    });

    // SQLite without index
    group.bench_function("sqlite_fullscan_no_index", |b| {
        b.iter(|| {
            let mut stmt = conn_unindexed
                .prepare("SELECT * FROM products WHERE name LIKE '%Alpha%'")
                .unwrap();
            let rows = stmt.query_map([], |_| Ok(())).unwrap().count();
            black_box(rows)
        })
    });

    // SQLite with index (B-tree index, but LIKE with leading wildcard can't use it)
    group.bench_function("sqlite_fullscan_indexed", |b| {
        b.iter(|| {
            let mut stmt = conn_indexed
                .prepare("SELECT * FROM products WHERE name LIKE '%Alpha%'")
                .unwrap();
            let rows = stmt.query_map([], |_| Ok(())).unwrap().count();
            black_box(rows)
        })
    });

    // SQLite LIMIT 50
    group.bench_function("sqlite_limit50_no_index", |b| {
        b.iter(|| {
            let mut stmt = conn_unindexed
                .prepare("SELECT * FROM products WHERE name LIKE '%Alpha%' LIMIT 50")
                .unwrap();
            let rows = stmt.query_map([], |_| Ok(())).unwrap().count();
            black_box(rows)
        })
    });

    group.finish();

    // Raw SIMD benchmark: just substring search, no HashMap/JSON/DB overhead
    let raw_strings: Vec<String> = (0..10_000usize)
        .map(|i| {
            format!(
                "Product {} {}",
                i,
                if i % 3 == 0 {
                    "Alpha"
                } else if i % 3 == 1 {
                    "Beta"
                } else {
                    "Gamma"
                }
            )
        })
        .collect();

    let mut group_raw = c.benchmark_group("ilike_raw");
    group_raw.bench_function("simd_memchr", |b| {
        b.iter(|| {
            let finder = memmem::Finder::new("Alpha");
            let count = raw_strings
                .iter()
                .filter(|s| finder.find(s.as_bytes()).is_some())
                .count();
            black_box(count)
        })
    });
    group_raw.finish();
}

criterion_group!(benches, bench_ilike);
criterion_main!(benches);
