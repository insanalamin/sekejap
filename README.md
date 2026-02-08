# Sekejap-DB

A **graph-first**, embedded multi-model database engine for Rust and Python.

## 1: Overview 

**Think of it as "Data Legos": a transparent, honest engine that replaces hidden black-box magic with explicit connections, giving you the total freedom to see, build, and navigate your own logic brick-by-brick.**

Sekejap-DB is a **graph-native database** designed for high-performance, relationship-heavy workloads like **Root Cause Analysis (RCA)**, **RAG**, and **Agentic AI**.

It unifies **Graph**, **Vector**, **Spatial**, and **Full-Text** search into a single, cohesive engine where the **Graph** acts as the primary structure and other models serve as attributes or filters.

### 1.1 Features 

- **HNSW Engine**: Custom, SIMD-accelerated HNSW implementation (AVX2/FMA) for panic-free, high-concurrency vector search.
- **Graph-First**: Relationships are first-class citizens. Queries traverse edges to prune the search space before applying expensive vector or text filters.
- **Hybrid Querying**: Native **Index Intersection** allows combining Graph, Vector, Spatial, and Text conditions.
- **Embedded**: Runs directly in your application process (Rust/Python). Zero network overhead.
- **Atomic Primitives**: Exposes low-level "atoms" (`traverse`, `search_vector`) for building complex custom query logic.

### Graph-First Philosophy

```
┌─────────────────────────────────────────────────────────┐
│                   Sekejap-DB                            │
│                  Graph-First Design                     │
├─────────────────────────────────────────────────────────┤
│                                                         │
│   ┌─────────┐    causal_edge    ┌─────────┐            │
│   │  Node   │◄────────────────►│  Node   │            │
│   │ (vector)│     (0.85)        │  (geo)  │            │
│   └─────────┘                  └─────────┘            │
│         │                            │                │
│         │                            │                │
│    ┌────┴────┐                 ┌────┴────┐           │
│    │ Vectors │                 │ Geo data │           │
│    │(embeddings)│              │(Point/Polygon)│       │
│    └─────────┘                  └─────────┘            │
│                                                         │
│   → Graph is the CORE                                   │
│   → Vectors/Geo are ATTRIBUTES on nodes                 │
│   → Queries traverse RELATIONSHIPS                       │
└─────────────────────────────────────────────────────────┘
```

---

## 2: Main Usage (Rust & Python)

### 2.1 Basic CRUD Operations

#### Write Data

**Rust:**
```rust
use sekejap::SekejapDB;

let mut db = SekejapDB::new(std::path::Path::new("./data"))?;

// Simple write
db.write("event-001", r#"{"title": "Theft", "severity": "high"}"#)?;

// JSON with Vector & Geo
db.write_json(r#"{
    \"_id\": \"news/flood-2026\",
    \"title\": \"Flood in Jakarta\",
    \"vectors\": { \"dense\": [0.1, 0.2, 0.3] },
    \"geo\": { \"loc\": { \"lat\": -6.2, \"lon\": 106.8 } }
}"#)?;
```

**Python:**
```python
import sekejap

db = sekejap.SekejapDB("./data")

# Simple write
db.write("event-001", '{"title": "Theft", "severity": "high"}')

# JSON with Vector & Geo
db.write_json("""
{
    "_id": "news/flood-2026",
    "title": "Flood in Jakarta",
    "vectors": { "dense": [0.1, 0.2, 0.3] },
    "geo": { "loc": { "lat": -6.2, "lon": 106.8 } }
}
""")
```

#### Read Data

**Rust:**
```rust
if let Some(event) = db.read("news/flood-2026")? {
    println!("Found: {}", event);
}
```

**Python:**
```python
event = db.read("news/flood-2026")
if event:
    print(f"Found: {event}")
```

#### Delete Data

**Rust:**
```rust
// Cascade delete (removes edges too)
db.delete("event-001")?;

// Keep edges for audit trail
db.delete_with_options("event-001", sekejap::DeleteOptions {
    exclude_edges: true,
})?;
```

**Python:**
```python
# Cascade delete (removes edges too)
db.delete("event-001")

# Keep edges for audit trail
db.delete_with_options("event-001", sekejap.DeleteOptions(exclude_edges=True))
```

---

### 2.2 Defining Schema & Collections

To enable **Hybrid Search**, you must define which fields are indexed. This tells Sekejap-DB:
1.  Which fields are **Vectors** (and what HNSW model to use).
2.  Which fields are **Spatial** (Point vs Polygon).
3.  Which fields are **Full-Text** searchable.

**Rust:**
```rust
db.define_collection(r#"{
    \"news\": {
        \"hot_fields\": {
            \"vector\": [\"vectors.dense\"],
            \"spatial\": [\"geo.loc\"],
            \"fulltext\": [\"title\"]
        },
        \"vectors\": {
            \"dense\": { \"model\": \"bge-m3\", \"dims\": 1024, \"index_hnsw\": true }
        },
        \"spatial\": {
            \"loc\": { \"type\": \"Point\", \"index_rtree\": true }
        }
    }
}"#)?;
```

**Python:**
```python
# Define schema for 'news' collection
db.define_collection("""
{
    "news": {
        "hot_fields": {
            "vector": ["vectors.dense"],
            "spatial": ["geo.loc"],
            "fulltext": ["title"]
        },
        "vectors": {
            "dense": { "model": "bge-m3", "dims": 1024, "index_hnsw": true }
        },
        "spatial": {
            "loc": { "type": "Point", "index_rtree": true }
        }
    }
}
""")
```

---

### 2.3 Hybrid Query (Graph + Vector + Spatial + Text)

**Scenario:** Find events caused by "Heavy Rain" (Graph), in "South Jakarta" (Spatial), matching "Accident" (Text), and similar to a "Severe Crash" vector.

**Rust:**
```rust
// Use the Query Builder for automatic intersection
let results = db.query()
    .has_edge_from("causes/heavy-rain", "caused".to_string()) // Graph
    .spatial(-6.27, 106.81, 5.0)?                             // Spatial (5km)
    .fulltext("Accident")?                                    // Text
    .vector_search(vec![0.9, 0.1, 0.1], 10)                   // Vector
    .execute()?;
```

**Python:**
```python
# Use the Query Builder for automatic intersection
results = db.query() \
    .has_edge_from("causes/heavy-rain", "caused") \
    .spatial(-6.27, 106.81, 5.0) \
    .fulltext("Accident") \
    .vector_search([0.9, 0.1, 0.1], 10) \
    .execute()
```

---

### 2.4 Graph Traversal & Aggregation

**Scenario:** Count events rolling up to a District (Hierarchy: Event -> SubDistrict -> District).

**Rust:**
```rust
// Traverse 2 hops: Event -> SubDistrict -> District
// traverse_forward(slug, hops, min_weight, edge_type, time_window)
let results = db.traverse_forward("event-001", 2, 0.0, None, None)?;

// Logic to check if District node is in results.path...
```

**Python:**
```python
# Traverse 2 hops: Event -> SubDistrict -> District
# traverse_forward(slug, hops, min_weight, edge_type)
results = db.traverse_forward("event-001", 2, 0.0, None)

# Logic to check if District node is in results.path...
```

---

### 2.5 Causal Root Cause Analysis

**Scenario:** Find the root causes of a specific crime event (backward traversal).

**Rust:**
```rust
// traverse(slug, hops, min_weight, edge_type)
let results = db.traverse("crime-001", 5, 0.3, None)?;

for edge in &results.edges {
    println!("{} -> {} (weight: {:.2})", edge._from, edge._to, edge.weight);
}
```

**Python:**
```python
# traverse(slug, hops, min_weight, edge_type)
results = db.traverse("crime-001", 5, 0.3, None)

if results:
    for edge in results.edges:
        print(f"{edge.source} -> {edge.target} (weight: {edge.weight:.2})")
```

---

## 3: Architecture & Performance

### 3.1 Multi-Tier Storage Architecture

Sekejap-DB employs a unique three-tier storage design to balance write throughput, read latency, and graph traversability.

1.  **Tier 1: Ingestion Buffer (LSM-Tree)**
    *   **Purpose:** High-velocity write staging.
    *   **Behavior:** Accepts writes immediately. Data is "staged" and eventually promoted.

2.  **Tier 2: Serving Layer (CoW B+Tree)**
    *   **Purpose:** Low-latency reads and persistence.
    *   **Behavior:** Stores the canonical version of Nodes and Blobs. Optimized for random access by ID.

3.  **Tier 3: Knowledge Graph (Adjacency)**
    *   **Purpose:** Relationship traversal and RCA.
    *   **Structure:** In-memory adjacency lists (forward/reverse) backed by concurrent maps.
    *   **Behavior:** Edges connect Nodes across Tiers 1 and 2. Traversal algorithms run here.

**Data Flow:** Write -> Tier 1 -> (Async Promotion) -> Tier 2 -> (Graph Indexing) -> Tier 3.

### 3.2 Query Execution: Index Intersection

Unlike PostgreSQL (Cost-Based Planner) or ArangoDB (Inverted Index), SekejapDB uses **Explicit Set Intersection** to ensure deterministic performance.

1.  **Parallel Drivers:** Enabled searchers (Vector, Spatial, Graph) run independently to fetch candidate Node IDs.
2.  **Bitwise Intersection:** Candidate sets are intersected (`HashSet`).
3.  **Deterministic Latency:** Performance is predictable and scales with the selectivity of the *strongest* filter.

### 3.3 Vector Engine (Hyper-Sekejap HNSW)

Sekejap-DB implements a custom HNSW engine from scratch to resolve stability issues found in other libraries.

*   **SIMD Acceleration:** AVX2/FMA optimized distance kernels for x86_64.
*   **Zero-Panic:** Built with `crossbeam-epoch` for safe, lock-free concurrency.
*   **Dynamic Mmap:** Automatically expands storage files as data grows.
*   **Contiguous Layout:** Vectors stored in aligned, memory-mapped buffers for cache efficiency.

### 3.4 Spatial & Full-Text Indexing

*   **Spatial (R-Tree):** Uses `rstar` for O(log n) point-in-radius and polygon intersection queries. Spatial keys map directly to Node IDs.
*   **Full-Text (Tantivy):** Integrates Tantivy for schema-aware lexical search. Writes are staged in Tier 1 before being committed to the Tantivy index, ensuring near-real-time visibility.

---

## Installation

### Rust
Add to `Cargo.toml`:
```toml
[dependencies]
sekejap = { version = "0.1.0", features = ["fulltext", "vector", "spatial"] }
```

### Python
```bash
pip install sekejap
```

## Building and Testing

```bash
# Run tests
cargo test --all-features

# Run benchmarks
cargo run --example benchmark_sqlite_vs_sekejap --all-features

# Check for errors
cargo check --features all
```

## License

MIT
