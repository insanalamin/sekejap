use criterion::{criterion_group, criterion_main, Criterion, black_box};
use sekejap::{CoreDB, sql::parse_and_compile};
use serde_json::json;

// ── Shared setup ──────────────────────────────────────────────────────────────

fn setup_db() -> CoreDB {
    let mut db = CoreDB::new();

    // 10,000 nodes across 10 categories, varied prices and stock flags
    for i in 0..10_000usize {
        let cat = format!("cat{}", i % 10);
        let price = (i % 200) as f64 + 10.0;
        let in_stock = i % 3 != 0;
        db.put(
            &format!("products/{i}"),
            &json!({
                "_collection": "products",
                "_key": i.to_string(),
                "category": cat,
                "price": price,
                "in_stock": in_stock,
                "name": format!("Product {i}"),
            })
            .to_string(),
        )
        .unwrap();
    }

    // 250 linear edges for graph traversal tests
    for i in 0..250usize {
        db.link(&format!("products/{i}"), &format!("products/{}", i + 1), "related", 1.0);
    }

    db
}

// ── simple_filter ─────────────────────────────────────────────────────────────
// WHERE category = 'cat3'  (returns ~1,000 of 10,000)

fn bench_simple_filter(c: &mut Criterion) {
    let db = setup_db();
    let mut group = c.benchmark_group("simple_filter");

    group.bench_function("atomic", |b| {
        b.iter(|| {
            black_box(
                db.collection("products")
                    .where_eq("category", "cat3")
                    .count(),
            )
        })
    });

    group.bench_function("sql", |b| {
        b.iter(|| {
            black_box(
                db.query("SELECT * FROM products WHERE category = 'cat3'")
                    .unwrap()
                    .count(),
            )
        })
    });

    group.finish();
}

// ── neq_filter ────────────────────────────────────────────────────────────────
// WHERE category != 'cat0'  (returns ~9,000 of 10,000)

fn bench_neq_filter(c: &mut Criterion) {
    let db = setup_db();
    let mut group = c.benchmark_group("neq_filter");

    group.bench_function("atomic", |b| {
        b.iter(|| {
            black_box(
                db.collection("products")
                    .where_neq("category", "cat0")
                    .count(),
            )
        })
    });

    group.bench_function("sql", |b| {
        b.iter(|| {
            black_box(
                db.query("SELECT * FROM products WHERE category != 'cat0'")
                    .unwrap()
                    .count(),
            )
        })
    });

    group.finish();
}

// ── range_filter ──────────────────────────────────────────────────────────────
// WHERE price > 50 AND price <= 150

fn bench_range_filter(c: &mut Criterion) {
    let db = setup_db();
    let mut group = c.benchmark_group("range_filter");

    group.bench_function("atomic", |b| {
        b.iter(|| {
            black_box(
                db.collection("products")
                    .where_gt("price", 50.0)
                    .where_lte("price", 150.0)
                    .count(),
            )
        })
    });

    group.bench_function("sql", |b| {
        b.iter(|| {
            black_box(
                db.query(
                    "SELECT * FROM products WHERE price > 50 AND price <= 150",
                )
                .unwrap()
                .count(),
            )
        })
    });

    group.finish();
}

// ── sort_take ─────────────────────────────────────────────────────────────────
// WHERE category = 'cat5' ORDER BY price ASC LIMIT 20

fn bench_sort_take(c: &mut Criterion) {
    let db = setup_db();
    let mut group = c.benchmark_group("sort_take");

    group.bench_function("atomic", |b| {
        b.iter(|| {
            black_box(
                db.collection("products")
                    .where_eq("category", "cat5")
                    .sort("price", true)
                    .take(20)
                    .collect(),
            )
        })
    });

    group.bench_function("sql", |b| {
        b.iter(|| {
            black_box(
                db.query(
                    "SELECT * FROM products WHERE category = 'cat5' ORDER BY price ASC LIMIT 20",
                )
                .unwrap()
                .collect(),
            )
        })
    });

    group.finish();
}

// ── graph_traverse ────────────────────────────────────────────────────────────
// MATCH 3 hops from products/0

fn bench_graph_traverse(c: &mut Criterion) {
    let db = setup_db();
    let mut group = c.benchmark_group("graph_traverse");

    group.bench_function("atomic", |b| {
        b.iter(|| black_box(db.one("products/0").hops(3).count()))
    });

    group.bench_function("sql", |b| {
        b.iter(|| {
            black_box(
                db.query(
                    "MATCH (a:products)-[:related*1..3]->(b:products) \
                     WHERE a._key = '0' RETURN b",
                )
                .unwrap()
                .count(),
            )
        })
    });

    group.finish();
}

// ── parse_overhead ────────────────────────────────────────────────────────────
// Measure tokenize + parse + compile with no DB execution

fn bench_parse_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_overhead");

    group.bench_function("simple", |b| {
        b.iter(|| {
            black_box(
                parse_and_compile(
                    black_box("SELECT * FROM products WHERE category = 'cat3'"),
                )
                .unwrap(),
            )
        })
    });

    group.bench_function("complex", |b| {
        b.iter(|| {
            black_box(
                parse_and_compile(black_box(
                    "SELECT name, price FROM products \
                     WHERE price > 50 AND price <= 150 \
                     ORDER BY price ASC LIMIT 20 OFFSET 5",
                ))
                .unwrap(),
            )
        })
    });

    group.bench_function("match", |b| {
        b.iter(|| {
            black_box(
                parse_and_compile(black_box(
                    "MATCH (a:products)-[:related*1..3]->(b:products) \
                     WHERE a._key = '0' RETURN b",
                ))
                .unwrap(),
            )
        })
    });

    group.finish();
}

// ── collection_all ────────────────────────────────────────────────────────────
// FROM collection vs FROM ALL  (full scan baseline)

fn bench_collection_all(c: &mut Criterion) {
    let db = setup_db();
    let mut group = c.benchmark_group("collection_all");

    group.bench_function("collection_atomic", |b| {
        b.iter(|| black_box(db.collection("products").count()))
    });

    group.bench_function("collection_sql", |b| {
        b.iter(|| {
            black_box(
                db.query("SELECT * FROM products").unwrap().count(),
            )
        })
    });

    group.bench_function("all_atomic", |b| {
        b.iter(|| black_box(db.all().count()))
    });

    group.bench_function("all_sql", |b| {
        b.iter(|| {
            black_box(db.query("SELECT * FROM ALL").unwrap().count())
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_simple_filter,
    bench_neq_filter,
    bench_range_filter,
    bench_sort_take,
    bench_graph_traverse,
    bench_parse_overhead,
    bench_collection_all,
);
criterion_main!(benches);
