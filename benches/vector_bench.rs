//! Vector search benchmarks — Phase 1 flat-scan baseline.
//!
//! Scenarios:
//!   flat_scan_{n}   — brute-force top-10 cosine search over N vectors (128-dim f32)
//!                     core vs SQLite (BLOB store + Rust-side distance)
//!   put_vector_{n}  — insert N vectors (128-dim f32)
//!                     core vs SQLite
//!   sql_vector_near — VECTOR_NEAR via SQL parser on 10K vectors (core only)
//!
//! SQLite stores vectors as raw f32 BLOBs. Distance is computed in Rust with
//! the same CosineDistance kernel — so this is a pure storage-layer comparison.
//!
//! These numbers become the before-baseline when HyperHNSW lands in Phase 2.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, black_box};
use rusqlite::Connection;
use sekejap::{CosineDistance, CoreDB, Distance};

const DIM: usize = 128;
const K: usize = 10;

fn make_vec(seed: usize) -> Vec<f32> {
    (0..DIM)
        .map(|i| {
            let x = ((seed * 6364136223846793005 + i * 1442695040888963407) >> 33) as f32;
            (x / u32::MAX as f32) * 2.0 - 1.0
        })
        .collect()
}

// ── sekejap setup ────────────────────────────────────────────────────────

fn setup_core(n: usize) -> (CoreDB, Vec<f32>) {
    let mut db = CoreDB::new();
    for i in 0..n {
        let slug = format!("vecs/v{i}");
        db.put(
            &slug,
            &format!(r#"{{"_collection":"vecs","_key":"v{i}"}}"#),
        )
        .unwrap();
        db.put_vector(&slug, "emb", &make_vec(i)).unwrap();
    }
    let query = make_vec(n + 1);
    (db, query)
}

// ── SQLite setup ──────────────────────────────────────────────────────────────

fn setup_sqlite(n: usize) -> (Connection, Vec<f32>) {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE vectors (
            slug  TEXT NOT NULL,
            field TEXT NOT NULL,
            data  BLOB NOT NULL
        );
        CREATE INDEX vectors_field ON vectors(field);",
    )
    .unwrap();

    {
        let mut stmt = conn
            .prepare("INSERT INTO vectors (slug, field, data) VALUES (?1, ?2, ?3)")
            .unwrap();
        for i in 0..n {
            let v = make_vec(i);
            let blob: Vec<u8> = v.iter().flat_map(|f| f.to_le_bytes()).collect();
            stmt.execute(rusqlite::params![format!("vecs/v{i}"), "emb", blob])
                .unwrap();
        }
    }

    let query = make_vec(n + 1);
    (conn, query)
}

/// Flat scan from SQLite: fetch all BLOBs for a field, deserialise, score, top-k.
fn sqlite_flat_scan(conn: &Connection, field: &str, query: &[f32], k: usize) -> usize {
    let mut stmt = conn
        .prepare_cached("SELECT data FROM vectors WHERE field = ?1")
        .unwrap();

    let mut scored: Vec<f32> = stmt
        .query_map([field], |row| row.get::<_, Vec<u8>>(0))
        .unwrap()
        .filter_map(|r| r.ok())
        .map(|blob| {
            let v: Vec<f32> = blob
                .chunks_exact(4)
                .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
                .collect();
            CosineDistance::eval(query, &v)
        })
        .collect();

    scored.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(k);
    scored.len()
}

// ── flat scan ─────────────────────────────────────────────────────────────────

fn bench_flat_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("flat_scan");

    for &n in &[1_000usize, 10_000, 50_000] {
        let (db, query) = setup_core(n);
        let (conn, sq_query) = setup_sqlite(n);

        group.bench_with_input(BenchmarkId::new("sekejap_memory", n), &n, |b, _| {
            b.iter(|| {
                db.collection("vecs")
                    .vector_near("emb", black_box(query.clone()), K)
                    .count()
            })
        });

        group.bench_with_input(BenchmarkId::new("sqlite", n), &n, |b, _| {
            b.iter(|| sqlite_flat_scan(&conn, "emb", black_box(&sq_query), K))
        });
    }

    group.finish();
}

// ── insert cost ───────────────────────────────────────────────────────────────

fn bench_put_vector(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_vector");

    for &n in &[1_000usize, 10_000] {
        group.bench_with_input(BenchmarkId::new("sekejap_memory", n), &n, |b, &n| {
            b.iter(|| {
                let mut db = CoreDB::new();
                for i in 0..n {
                    let slug = format!("vecs/v{i}");
                    db.put(
                        &slug,
                        &format!(r#"{{"_collection":"vecs","_key":"v{i}"}}"#),
                    )
                    .unwrap();
                    db.put_vector(&slug, "emb", black_box(&make_vec(i))).unwrap();
                }
                db
            })
        });

        group.bench_with_input(BenchmarkId::new("sqlite", n), &n, |b, &n| {
            b.iter(|| {
                let conn = Connection::open_in_memory().unwrap();
                conn.execute_batch(
                    "CREATE TABLE vectors (slug TEXT, field TEXT, data BLOB);",
                )
                .unwrap();
                {
                    let mut stmt = conn
                        .prepare("INSERT INTO vectors (slug, field, data) VALUES (?1, ?2, ?3)")
                        .unwrap();
                    for i in 0..n {
                        let blob: Vec<u8> = black_box(make_vec(i))
                            .iter()
                            .flat_map(|f| f.to_le_bytes())
                            .collect();
                        stmt.execute(rusqlite::params![format!("vecs/v{i}"), "emb", blob])
                            .unwrap();
                    }
                }
                conn
            })
        });
    }

    group.finish();
}

// ── SQL VECTOR_NEAR (core only — SQLite has no native equivalent) ─────────────

fn bench_sql_vector_near(c: &mut Criterion) {
    let (db, _) = setup_core(10_000);

    let sql = {
        let coords: Vec<String> = make_vec(99999).iter().map(|f| format!("{f:.6}")).collect();
        format!(
            "SELECT * FROM vecs WHERE VECTOR_NEAR(emb, [{}], {K})",
            coords.join(", ")
        )
    };

    c.bench_function("sql_vector_near_10k", |b| {
        b.iter(|| db.query(black_box(&sql)).unwrap().count())
    });
}

// ── distance kernel microbench ────────────────────────────────────────────────

fn bench_distance_kernels(c: &mut Criterion) {
    use sekejap::{DotProduct, L2Distance};

    let va: Vec<f32> = make_vec(1);
    let vb: Vec<f32> = make_vec(2);

    let mut group = c.benchmark_group("distance_128dim");

    group.bench_function("l2", |b| {
        b.iter(|| L2Distance::eval(black_box(&va), black_box(&vb)))
    });
    group.bench_function("dot", |b| {
        b.iter(|| DotProduct::eval(black_box(&va), black_box(&vb)))
    });
    group.bench_function("cosine", |b| {
        b.iter(|| CosineDistance::eval(black_box(&va), black_box(&vb)))
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_flat_scan,
    bench_put_vector,
    bench_sql_vector_near,
    bench_distance_kernels,
);
criterion_main!(benches);
