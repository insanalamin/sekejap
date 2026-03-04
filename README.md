# Sekejap DB

A **graph-first**, embedded multi-model database engine for Rust and Python.

Graph is the primary structure. **Vector**, **Spatial**, and **Full-Text** are first-class attributes on nodes, queryable in the same pipeline. Runs in-process — zero network overhead.

---

## Features

- **Graph-First**: Edges are first-class. Queries traverse the graph to prune the search space before applying other indexes.
- **HNSW Engine**: Parallel construction (Rayon), NEON/AVX2 SIMD CosineDistance, `ef_construction=32`. 44x faster batch build vs v0.2.
- **Spatial (PostGIS-par)**: R-Tree index on full GeoJSON geometry envelopes. Point, Polygon, LineString, Multi* — with DE-9IM predicates (`st_within`, `st_contains`, `st_intersects`, `st_dwithin`).
- **Full-Text**: Pluggable adapters — Tantivy (default) or SeekStorm.
- **Embedded**: Memory-mapped arenas, zero-copy storage.
- **SekejapQL**: Lightweight text query language — one op per line, compiles to the same pipeline as JSON.
- **CLI**: Interactive REPL (`skcli`) — SekejapQL and JSON queries, mutations, introspection.

---

## Quick Start

### Python
```python
import sekejap, json

db = sekejap.SekejapDB("./data", capacity=1_000_000)
db.init_hnsw(16)
db.init_fulltext()

# Write
db.put("persons/ali", json.dumps({
    "name": "Ali Hassan", "status": "wanted",
    "vectors": {"dense": [0.12, 0.87, ...]},
    "geo": {"loc": {"lat": 3.1105, "lon": 101.6682}}
}))
db.link("persons/ali", "crimes/robbery-001", "committed", 1.0)

# Query — SekejapQL
result = db.query_skql("""
    one "crimes/robbery-001"
    backward "committed"
    where_eq "status" "wanted"
    forward "lives_at"
    near 3.1291 101.6710 5.0
    select "name" "alias" "geo"
""")

db.flush()
db.close()
```

### Rust
```rust
use sekejap::SekejapDB;

let db = SekejapDB::new(std::path::Path::new("./data"), 1_000_000)?;
db.init_hnsw(16);
db.init_fulltext(std::path::Path::new("./data"));

db.nodes().put("persons/ali", r#"{"name":"Ali Hassan","status":"wanted"}"#)?;
db.edges().link("persons/ali", "crimes/robbery-001", "committed", 1.0)?;

// SekejapQL (auto-detected — anything not starting with '{')
let result = db.query("collection \"crimes\"\nwhere_eq \"type\" \"robbery\"\ntake 20")?;

// JSON pipeline (auto-detected — starts with '{')
let result = db.query(r#"{"pipeline":[{"op":"collection","name":"crimes"},{"op":"take","n":5}]}"#)?;

// Fluent builder (Rust only)
let result = db.nodes().collection("crimes")
    .where_eq("type", serde_json::json!("robbery"))
    .near(3.13, 101.67, 1.0)
    .take(20)
    .collect()?;
```

---

## SekejapQL — One Op Per Line

```
# Find all gang members → their crimes → near a location
one "persons/ali"
forward "member_of"
backward "member_of"
forward "committed"
near 3.1291 101.6710 2.0
sort "severity" desc
take 10
select "type" "severity" "occurred_at"

# News → crime → suspect network inside a polygon
collection "articles"
matching "armed robbery Bangsar 2024"
forward "reported_by"
backward "committed"
forward "member_of"
backward "member_of"
spatial_within_polygon (3.128,101.665) (3.135,101.665) (3.135,101.678) (3.128,101.678) (3.128,101.665)
where_eq "status" "wanted"
select "name" "alias" "geo" "priors"

# PostGIS-par geometry predicates
collection "zones"
st_intersects (3.128,101.665) (3.128,101.678) (3.135,101.678) (3.135,101.665) (3.128,101.665)
```

Pipe style also works: `collection "crimes" | where_eq "type" "robbery" | near 3.1 101.6 1.0 | take 20`

---

## CLI (`skcli`)

```bash
cargo run -p skcli -- --path ./data

sekejap> collection "crimes" | where_eq "type" "robbery" | take 10
sekejap> one "persons/ali" | forward "committed"
sekejap> count collection "crimes"
sekejap> explain collection "crimes" | take 5
sekejap> mutate {"mutation":"put_json","data":{"_id":"crimes/001","type":"robbery"}}
sekejap> \d crimes
sekejap> \l
```

---

## Benchmark (10k Records, Apple Silicon)

| Operation | SQLite (Rust) | Sekejap (Rust) | Speedup |
|---|---|---|---|
| Simple retrieval (1k lookups) | 3.4ms | 1.9ms | **1.8x** |
| Vector retrieval (k-NN) | 9.3ms | 0.3ms | **31x** |
| Graph traversal (100x 3-hop) | 8.5ms | 1.1ms | **7.7x** |
| V+S retrieval | 5.3ms | 1.7ms | **3.1x** |
| V+F retrieval | 3.7ms | <0.1ms | **instant** |

See [`docs/benchmark-results.md`](docs/benchmark-results.md) for full Rust vs Python results and methodology.

---

## Full Documentation

See the [`docs/`](docs/) folder:
- [`docs/api-reference.md`](docs/api-reference.md) — Complete API reference (setup, schema, mutations, all SekejapQL ops, JSON pipeline, data conventions, performance)
- [`docs/interface-table.md`](docs/interface-table.md) — Interface inventory and status
- [`docs/roadmap.md`](docs/roadmap.md) — Development roadmap and execution notes
- [`docs/benchmark-results.md`](docs/benchmark-results.md) — Benchmark results (Rust vs Python vs SQLite)

---

## Build

```bash
cargo test
cargo build --release

# CLI
cargo run -p skcli -- --path ./data

# Python wheel
maturin develop --release
```

## License

MIT
