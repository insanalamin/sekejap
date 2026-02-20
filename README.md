# Sekejap DB

A **graph-first**, embedded multi-model database engine for Rust and Python.

## 1: Overview 

Sekejap-DB is a **graph-native database** designed for high-performance, relationship-heavy workloads like **Root Cause Analysis (RCA)**, **RAG**, and **Agentic AI**.

It unifies **Graph**, **Vector**, **Spatial**, and **Full-Text** search into a single, cohesive engine where the **Graph** acts as the primary structure and other models serve as attributes or filters.

### 1.1 Features 

- **HNSW Engine**: Custom, SIMD-accelerated HNSW implementation for high-concurrency vector search.
- **Graph-First**: Relationships are first-class citizens. Queries traverse edges to prune the search space.
- **Hybrid Querying**: Native **Index Intersection** allows combining Graph, Vector, Spatial, and Text conditions via a pipeline API.
- **Embedded**: Runs directly in your application process (Rust/Python). Zero network overhead.
- **Zero-Copy Storage**: Memory-mapped arenas for ultra-fast data access.

---

## 2: Main Usage (Rust & Python)

### 2.1 Basic CRUD Operations

#### Write Data

**Rust:**
```rust
use sekejap::SekejapDB;

// Initialize with path and capacity
let db = SekejapDB::new(std::path::Path::new("./data"), 1_000_000)?;

// Simple write
db.nodes().put("event/001", r#"{"title": "Theft", "severity": "high"}"#)?;

// JSON with Vector & Geo
db.nodes().put("news/flood-2026", r#"{
    "title": "Flood in Jakarta",
    "vectors": { "dense": [0.1, 0.2, 0.3] },
    "geo": { "loc": { "lat": -6.2, "lon": 106.8 } }
}"#)?;
```

**Python:**
```python
import sekejap

# Initialize with path and capacity
db = sekejap.SekejapDB("./data", capacity=1000000)

# Simple write
db.put("event/001", '{"title": "Theft", "severity": "high"}')

# JSON with Vector & Geo
db.put("news/flood-2026", """
{
    "title": "Flood in Jakarta",
    "vectors": { "dense": [0.1, 0.2, 0.3] },
    "geo": { "loc": { "lat": -6.2, "lon": 106.8 } }
}
""")
```

#### Read Data

**Rust:**
```rust
if let Some(event) = db.nodes().get("news/flood-2026") {
    println!("Found: {}", event);
}
```

**Python:**
```python
event = db.get("news/flood-2026")
if event:
    print(f"Found: {event}")
```

#### Delete Data

**Rust:**
```rust
db.nodes().remove("event/001")?;
```

**Python:**
```python
db.remove("event/001")
```

---

### 2.2 Edges & Traversal

#### Link Nodes

**Rust:**
```rust
db.edges().link("poverty", "crime/001", "causal", 0.8)?;
```

**Python:**
```python
db.link("poverty", "crime/001", "causal", 0.8)
```

#### Hybrid Query Pipeline

Find events starting from "cuisine/italian", traversing backward via "related", and filtering by rating.

**Rust:**
```rust
let results = db.nodes().one("cuisine/italian")
    .backward("related")
    .where_gt("rating", 4.5)
    .collect()?;
```

**Python:**
```python
results = db.backward("cuisine/italian", "related")
for hit in results:
    print(hit.payload)
```

#### Pipeline query (SekejapQL JSON)

Same logic via a JSON pipeline: `one` → `forward`/`backward` → `where_*` → `take` → result.

**Rust:**
```rust
let q = r#"{"pipeline": [{"op": "one", "slug": "cuisine/italian"}, {"op": "backward", "type": "related"}, {"op": "where_gt", "field": "rating", "value": 4.5}, {"op": "take", "n": 10}]}"#;
let outcome = db.query_json(q)?;
```

**Python:**
```python
q = '{"pipeline": [{"op": "one", "slug": "cuisine/italian"}, {"op": "backward", "type": "related"}, {"op": "take", "n": 10}]}'
result = db.query_json(q)  # returns JSON string of outcome
```

---

## 3: Building and Testing

```bash
# Run tests
cargo test --all-features

# Run an example (e.g. RCA or benchmark)
cargo run --example real_world_rca --all-features
cargo run --example sqlite_competition_benchmark --all-features

# Benchmarks
cargo bench

# Check for errors
cargo check --all-features
```

## License

MIT