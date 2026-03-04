# Sekejap DB — Skill Guide

> For AI agents, coding assistants, and developers who need to integrate or operate Sekejap.

---

## What is Sekejap

Sekejap is a **graph-first, embedded multi-model database** for Rust and Python. It combines four data pillars in one unified query pipeline:

| Pillar | Index | Use Case |
|---|---|---|
| **Graph** | Edge list + BFS | Relationships, traversals, network analysis |
| **Vector** | HNSW (128-dim, cosine) | Similarity search, embeddings, recommendations |
| **Spatial** | R-Tree (GeoJSON envelope) | Location queries, polygon containment, proximity |
| **Full-Text** | Tantivy / SeekStorm | Keyword search, weighted ranking |

Runs in-process. Zero network overhead. Memory-mapped storage.

---

## When to Use Sekejap

Use Sekejap when you need:
- Graph traversals combined with vector/spatial/text filters in a single query
- An embedded database with no external server process
- Multi-model queries without joining across separate systems
- Sub-millisecond retrieval for graph + vector + spatial workloads

Do NOT use Sekejap when you need:
- SQL joins, aggregations, or relational schemas — use SQLite/Postgres
- Distributed/multi-node deployments — Sekejap is single-process
- Vectors larger than 128 dimensions — fixed at 128-dim currently

---

## Setup Recipes

### Rust

```toml
# Cargo.toml
[dependencies]
sekejap = "0.4"
# With fulltext:
sekejap = { version = "0.4", features = ["fulltext"] }
```

```rust
use sekejap::SekejapDB;

let db = SekejapDB::new(std::path::Path::new("./data"), 1_000_000)?;
db.init_hnsw(16);                                    // vector index (BEFORE inserting vectors)
db.init_fulltext(std::path::Path::new("./data"));    // fulltext index (BEFORE inserting text)
db.schema().define("items", r#"{"hot_fields":{"hash":["status"],"range":["score"]}}"#)?;
```

### Python

```bash
pip install sekejap
```

```python
import sekejap, json

db = sekejap.SekejapDB("./data", capacity=1_000_000)
db.init_hnsw(16)
db.init_fulltext()
db.define_collection("items", '{"hot_fields":{"hash":["status"],"range":["score"]}}')
```

### CLI

```bash
cargo run -p skcli -- --path ./data
```

---

## Critical Rules

1. **`init_hnsw(m)`** must be called BEFORE writing nodes that contain `vectors.dense`
2. **`init_fulltext(path)`** must be called BEFORE writing nodes that contain `title`/`body`
3. **Nodes must exist** before creating edges to/from them
4. **Slugs** follow `collection/key` format (e.g. `"persons/lucci"`, `"crimes/robbery-001"`)
5. **`flush()`** persists all data to disk — call before exit

---

## Node Data Format

```json
{
    "_id": "collection/key",
    "name": "Any field",
    "vectors": {"dense": [0.1, 0.2, ..., 0.99]},
    "geo": {"loc": {"lat": 3.11, "lon": 101.67}},
    "geometry": {"type": "Polygon", "coordinates": [[[101.6, 3.1], ...]]},
    "title": "Fulltext title",
    "body": "Fulltext body"
}
```

| Field | Purpose | Required |
|---|---|---|
| `_id` | Slug (for `put_json` only) | No |
| `vectors.dense` | 128-dim f32 array for HNSW | No |
| `geo.loc.lat/lon` | Legacy point coordinates | No |
| `geometry` | GeoJSON (Point, Polygon, LineString, Multi*, etc.) | No |
| `title` | Fulltext title field | No |
| `body` or `content` | Fulltext body field | No |
| Any other field | Stored in payload, queryable via `where_*` | No |

---

## Task: Write Data

### Single node

```rust
db.nodes().put("persons/lucci", r#"{"name":"Rob Lucci","status":"wanted"}"#)?;
```

```python
db.put("persons/lucci", json.dumps({"name": "Rob Lucci", "status": "wanted"}))
```

### Batch (10-100x faster)

```rust
db.nodes().ingest(&[("persons/lucci", r#"{...}"#), ("persons/kaku", r#"{...}"#)])?;
db.nodes().build_hnsw()?;  // if using vectors
```

```python
db.ingest_nodes([("persons/lucci", json.dumps({...})), ...])
db.build_hnsw()  # if using vectors
```

### Edges

```rust
db.edges().link("persons/lucci", "crimes/robbery-001", "committed", 1.0)?;
db.edges().link_meta("persons/lucci", "crimes/robbery-001", "committed", 1.0, r#"{"role":"leader"}"#)?;
```

### Read / Delete

```rust
let json: Option<String> = db.nodes().get("persons/lucci");
db.nodes().remove("persons/lucci")?;
```

### Mutations (JSON format)

```rust
db.mutate(r#"{"mutation":"put_json","data":{"_id":"crimes/001","type":"robbery"}}"#)?;
db.mutate(r#"{"mutation":"link","source":"persons/lucci","target":"crimes/001","type":"committed","weight":1.0}"#)?;
db.mutate(r#"{"mutation":"remove","slug":"crimes/001"}"#)?;
db.mutate(r#"{"mutation":"unlink","source":"persons/lucci","target":"crimes/001","type":"committed"}"#)?;
```

---

## Task: Query Data

Three equivalent query interfaces — all execute the same pipeline:

### 1. SekejapQL (text, one op per line)

```
collection "crimes"
where_eq "type" "robbery"
near 3.1291 101.6710 1.0
sort "severity" desc
take 20
```

Pipe style: `collection "crimes" | where_eq "type" "robbery" | take 20`

```rust
let result = db.query("collection \"crimes\"\nwhere_eq \"type\" \"robbery\"\ntake 20")?;
let count = db.count("collection \"crimes\"")?;
let plan = db.explain("collection \"crimes\"\ntake 20")?;
```

```python
result = db.query_skql('collection "crimes"\nwhere_eq "type" "robbery"\ntake 20')
count = db.query_skql_count('collection "crimes"')
```

### 2. JSON Pipeline

```json
{"pipeline": [
    {"op": "collection", "name": "crimes"},
    {"op": "where_eq", "field": "type", "value": "robbery"},
    {"op": "near", "lat": 3.1291, "lon": 101.6710, "radius": 1.0},
    {"op": "sort", "field": "severity", "asc": false},
    {"op": "take", "n": 20}
]}
```

```rust
let result = db.query(r#"{"pipeline":[...]}"#)?;
```

### 3. Fluent Builder (Rust only)

```rust
let result = db.nodes().collection("crimes")
    .where_eq("type", serde_json::json!("robbery"))
    .near(3.13, 101.67, 1.0)
    .sort("severity", false)
    .take(20)
    .collect()?;
```

---

## All Query Ops Reference

### Starters (every query begins with one)

| Op | SekejapQL | Description |
|---|---|---|
| `one` | `one "slug"` | Single node |
| `many` | `many "s1" "s2"` | Multiple specific nodes |
| `collection` | `collection "name"` | All nodes in collection |
| `all` | `all` | Every node |

### Graph Traversal

| Op | SekejapQL | Description |
|---|---|---|
| `forward` | `forward "type"` | Follow outgoing edges |
| `backward` | `backward "type"` | Follow incoming edges |
| `hops` | `hops 3` | Multi-hop BFS |
| `forward_parallel` | `forward_parallel "type"` | Parallel outgoing (Rayon) |
| `backward_parallel` | `backward_parallel "type"` | Parallel incoming |
| `roots` | `roots` | Nodes with no incoming edges |
| `leaves` | `leaves` | Nodes with no outgoing edges |

### Vector Search

| Op | SekejapQL | Description |
|---|---|---|
| `similar` | `similar "slug" 10` | Top-k nearest neighbors via HNSW |

### Spatial Search

| Op | SekejapQL | Description |
|---|---|---|
| `near` | `near 3.13 101.67 1.0` | Within radius (km) |
| `spatial_within_bbox` | `spatial_within_bbox 3.1 101.6 3.2 101.7` | Bounding box |
| `spatial_within_polygon` | `spatial_within_polygon (3.1,101.6) (3.2,101.7) ...` | Centroid in polygon |
| `st_within` | `st_within (lat,lon) ...` | Geometry within polygon |
| `st_contains` | `st_contains (lat,lon) ...` | Geometry contains polygon |
| `st_intersects` | `st_intersects (lat,lon) ...` | Geometry intersects polygon |
| `st_dwithin` | `st_dwithin 3.13 101.67 1.0` | Centroid within distance |

### Full-Text

| Op | SekejapQL | Description |
|---|---|---|
| `matching` | `matching "search terms"` | Search title + body fields |

### Filters

| Op | SekejapQL | Description |
|---|---|---|
| `where_eq` | `where_eq "field" "value"` | Exact match |
| `where_gt` | `where_gt "field" 7` | Greater than |
| `where_lt` | `where_lt "field" 5` | Less than |
| `where_gte` | `where_gte "field" 7` | Greater or equal |
| `where_lte` | `where_lte "field" 5` | Less or equal |
| `where_between` | `where_between "field" 5 9` | Range inclusive |
| `where_in` | `where_in "field" "v1" "v2"` | IN list |

### Set Algebra

| Op | SekejapQL | Description |
|---|---|---|
| `intersect` | JSON only | AND: keep nodes in both |
| `union` | JSON only | OR: merge sets |
| `subtract` | JSON only | MINUS: remove right from left |

### Result Shaping

| Op | SekejapQL | Description |
|---|---|---|
| `sort` | `sort "field" desc` | Order results |
| `skip` | `skip 20` | Pagination offset |
| `take` | `take 10` | Limit results |
| `select` | `select "f1" "f2"` | Project fields |

### Terminal Methods (Rust fluent builder)

| Method | Returns | Description |
|---|---|---|
| `.collect()?` | `Outcome<Vec<Hit>>` | All hits with payloads |
| `.count()?` | `Outcome<usize>` | Count only |
| `.first()?` | `Outcome<Option<Hit>>` | First or None |
| `.exists()?` | `Outcome<bool>` | Any results? |
| `.avg("field")?` | `Outcome<f64>` | Average |
| `.sum("field")?` | `Outcome<f64>` | Sum |
| `.explain()` | `Plan` | Compiled steps (no execute) |

---

## Common Query Patterns

### Graph: Who is connected?

```
one "crimes/robbery-001"
backward "committed"
forward "lives_at"
select "name" "address" "geo"
```

### Graph: Multi-hop network

```
one "persons/lucci"
forward "associate_of"
hops 3
take 50
```

### Vector: Similar profiles

```
all
similar "persons/lucci" 20
where_eq "status" "wanted"
```

### Spatial: Crimes near a point

```
collection "crimes"
near 3.1291 101.6710 2.0
sort "severity" desc
take 20
```

### Spatial: Geometry intersection (PostGIS-par)

```
collection "zones"
st_intersects (3.128,101.665) (3.128,101.678) (3.135,101.678) (3.135,101.665) (3.128,101.665)
```

### Full-text: Keyword search

```
collection "articles"
matching "armed robbery 2024"
take 20
```

### Combined: Text → Graph → Spatial

```
collection "articles"
matching "armed robbery"
forward "reports"
backward "committed"
near 3.13 101.67 5.0
where_eq "status" "wanted"
sort "severity" desc
take 20
select "name" "alias" "geo"
```

### Hybrid: Vector + Spatial (Rust)

```rust
let spatial = db.nodes().all().within_bbox(3.1, 101.6, 3.2, 101.7).collect()?;
let spatial_ids: HashSet<u32> = spatial.data.iter().map(|h| h.idx).collect();
let similar = db.nodes().all().similar(&query_vec, 100).collect()?;
let combined: Vec<&Hit> = similar.data.iter()
    .filter(|h| spatial_ids.contains(&h.idx))
    .take(10).collect();
```

---

## CLI (`skcli`) Usage

```
sekejap> collection "crimes" | where_eq "type" "robbery" | take 10
sekejap> one "persons/lucci" | forward "committed"
sekejap> count collection "crimes"
sekejap> explain collection "crimes" | take 5
sekejap> mutate {"mutation":"put_json","data":{"_id":"crimes/001","type":"robbery"}}
sekejap> \l                     # list collections
sekejap> \d crimes              # describe collection
sekejap> \flush                 # persist to disk
```

---

## Introspection

```rust
let info = db.describe();                     // global: node count, index status
let col = db.describe_collection("crimes");   // collection: count, index readiness
```

Key fields in `describe()`:
- `graph.node_count`, `graph.edge_count`
- `vector.enabled`, `vector.indexed_nodes`
- `spatial.indexed_nodes`
- `fulltext.adapter` ("tantivy" | "seekstorm" | null)

Key fields in `describe_collection()`:
- `count`
- `indexes.graph.collection_bitmap_ready`
- `indexes.vector.hnsw_ready`
- `indexes.spatial.rtree_ready`
- `indexes.fulltext.adapter_ready`
- `indexes.payload.hash_ready` → list of hash-indexed fields
- `indexes.payload.range_ready` → list of range-indexed fields

---

## Gotchas

| Trap | Fix |
|---|---|
| Vectors not indexed | Call `init_hnsw(16)` BEFORE inserting nodes with vectors |
| Fulltext returns empty | Call `init_fulltext(path)` BEFORE inserting nodes with title/body |
| Edges fail silently | Source and target nodes must exist first |
| Slow single inserts | Use `ingest()` for batch (10-100x faster than `put()`) |
| `where_eq` is slow | Define `hot_fields` with `schema().define()` for O(1) hash lookups |
| Fulltext cold query ~23ms | First query after commit reloads Tantivy reader — subsequent queries ~2ms |
| Data not persisted | Call `flush()` before program exit |

---

## Performance Reference

| Operation | Complexity | Typical (10k records) |
|---|---|---|
| `ingest()` batch | O(N log N) | ~0.27s (no HNSW), ~0.53s (with HNSW) |
| `put()` single | O(1) | ~0.15ms |
| `get()` | O(1) | ~1.8us |
| `similar()` k-NN | O(log N) | ~0.6ms |
| `near()` / `within_bbox()` | O(log N) | ~0.5-0.9ms |
| `matching()` fulltext | O(index) | ~23ms cold, ~2ms warm |
| `forward().hops(3)` | O(degree * hops) | ~0.013ms |
| `where_eq` (hot field) | O(1) | ~0.01ms |
| `flush()` | O(data) | ~5ms |
