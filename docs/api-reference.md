# Sekejap-DB Reference

Sekejap is a **graph-first, embedded multi-model database** for Rust and Python. Graph is the primary structure. Vector, Spatial, and Full-Text are first-class attributes on graph nodes, queryable in the same pipeline.

---

## Data Model

Every piece of data is a **node** identified by a `slug` (`collection/key`). Nodes carry a JSON payload and optionally:
- a 128-dim float vector (`vectors.dense`)
- a geographic geometry — any GeoJSON type (Point, LineString, Polygon, Multi*, GeometryCollection) in the `"geometry"` field, or a legacy `geo.loc.lat` / `geo.loc.lon` point
- full-text fields (`title`, `content`)

Nodes are connected by typed, weighted, directed **edges**.

```
Node "persons/lucci"  ──[committed]──►  Node "crimes/robbery-001"
                                              │
                                        [occurred_at]
                                              │
                                              ▼
                                     Node "locations/cimb-bangsar"
```

Queries traverse this graph, then filter/rank using the other indexes.

---

## 1. Setup

**Python**
```python
import sekejap
db = sekejap.SekejapDB("./data", capacity=1_000_000)
db.close()
```

**Rust**
```rust
use sekejap::SekejapDB;
let db = SekejapDB::new(std::path::Path::new("./data"), 1_000_000)?;
// drop(db) closes
```

---

## 2. Schema (Optional but Recommended)

Define a collection to enable hot-field indexes. Unlisted fields are still stored in JSON — they just use a slower scan path.

**Python**
```python
db.define_collection("crimes", '{"hot_fields": {"hash": ["type", "status"], "range": ["severity"]}}')
db.init_hnsw(16)      # enable vector index (call once, m=16)
db.init_fulltext()    # enable full-text index (call once)
```

**Rust**
```rust
db.schema().define("crimes", r#"{"hot_fields": {"hash": ["type","status"], "range": ["severity"]}}"#)?;
db.init_hnsw(16);
db.init_fulltext();
```

---

## 3. Write — Nodes

### Single node

**Python**
```python
db.put("crimes/robbery-001", '{"type":"robbery","severity":9,"status":"open"}')
```

**Rust**
```rust
db.nodes().put("crimes/robbery-001", r#"{"type":"robbery","severity":9,"status":"open"}"#)?;
```

### Node with vector + geo + fulltext fields

```python
db.put("persons/lucci", json.dumps({
    "name": "Rob Lucci",
    "status": "wanted",
    "priors": ["robbery", "assault"],
    "vectors": {"dense": [0.12, 0.87, ...]},  # 128 floats
    "geo": {"loc": {"lat": 3.1105, "lon": 101.6682}},  # legacy point format
    "title": "Rob Lucci",
    "content": "Known associate of CP9, covert operations specialist"
}))

# GeoJSON geometry — Point, Polygon, LineString, etc.
db.put("zones/bangsar-south", json.dumps({
    "name": "Bangsar South",
    "geometry": {
        "type": "Polygon",
        "coordinates": [[
            [101.665, 3.128], [101.678, 3.128],
            [101.678, 3.135], [101.665, 3.135],
            [101.665, 3.128]
        ]]
    }
}))
```

### Batch ingestion (fast path — builds all indexes once at the end)

**Python**
```python
items = [("persons/lucci", json.dumps({...})), ("crimes/001", json.dumps({...}))]
db.ingest_nodes(items)
db.build_hnsw()   # call after ingest_nodes if using vectors
```

**Rust**
```rust
db.nodes().ingest(&[("persons/lucci", r#"{...}"#), ("crimes/001", r#"{...}"#)])?;
// or: ingest_raw() then build_hnsw() separately
db.nodes().build_hnsw()?;
```

### Read / Delete

```python
payload = db.get("crimes/robbery-001")   # returns JSON string or None
db.remove("crimes/robbery-001")
```

---

## 4. Write — Edges

```python
# Basic directed edge
db.link("persons/lucci", "crimes/robbery-001", "committed", 1.0)

# Edge with metadata
db.link_meta("persons/lucci", "crimes/robbery-001", "committed", 1.0,
             '{"role":"mastermind","timestamp":"2024-11-03T14:32:00+08:00"}')

# Remove edge
db.unlink("persons/lucci", "crimes/robbery-001", "committed")

# Batch edges (fast path)
db.ingest_edges([
    ("persons/lucci",   "crimes/robbery-001", "committed", 1.0),
    ("crimes/robbery-001", "locations/cimb", "occurred_at", 1.0),
])
```

---

## 5. Query — SekejapQL

SekejapQL is the primary query language. **One op per line**, or use `|` as a pipe separator. Args are positional. Strings in quotes, numbers bare.

```
# Multi-line style
collection "crimes"
where_eq "type" "robbery"
near 3.1291 101.6710 1.0
sort "severity" desc
take 20

# Pipe style (same result)
collection "crimes" | where_eq "type" "robbery" | near 3.1291 101.6710 1.0 | sort "severity" desc | take 20
```

Every op name is identical to the underlying JSON pipeline op. Zero overhead — it compiles to the same internal `Vec<Step>`.

**Python**
```python
result = db.query_skql('collection "crimes"\nwhere_eq "type" "robbery"\ntake 20')
count  = db.query_skql_count('collection "crimes"\nnear 3.1291 101.6710 1.0')
```

**Rust**
```rust
let result = db.query_skql("collection \"crimes\"\nwhere_eq \"type\" \"robbery\"\ntake 20")?;
let count  = db.query_skql_count("collection \"crimes\"\nnear 3.1291 101.6710 1.0")?;
let steps  = db.explain_skql("collection \"crimes\"\ntake 20")?;  // compile-only, no execute
```

> `intersect`, `union`, `subtract` (set algebra) require nested pipelines — use the JSON format for those.

---

### 5.1 Starters (every query begins with one)

| Op | Args | What it does |
|---|---|---|
| `one` | `"slug"` | Single node by slug |
| `many` | `"s1" "s2" ...` | Multiple specific slugs |
| `collection` | `"name"` | All nodes whose slug begins with `name/` |
| `all` | — | Every node in the database |

```
one "persons/lucci"
collection "crimes"
many "persons/lucci" "persons/kaku" "persons/jabra"
```

---

### 5.2 Graph Traversal

| Op | Args | What it does |
|---|---|---|
| `forward` | `"edge_type"` | Follow outgoing edges of given type |
| `backward` | `"edge_type"` | Follow incoming edges of given type |
| `forward_parallel` | `"edge_type"` | Same as forward, Rayon-parallel for large sets |
| `backward_parallel` | `"edge_type"` | Same as backward, Rayon-parallel |
| `hops` | `n` | Repeat last traversal up to n times (BFS) |
| `roots` | — | Keep only nodes with no incoming edges |
| `leaves` | — | Keep only nodes with no outgoing edges |

```
# Who committed this crime?
one "crimes/robbery-001"
backward "committed"

# All members of the same gang as Lucci
one "persons/lucci"
forward "member_of"
backward "member_of"

# Full network up to 3 hops
one "persons/lucci"
forward "associate_of"
hops 3
```

---

### 5.3 Vector Search

| Op | Args | What it does |
|---|---|---|
| `similar` | `"slug" k` | k nearest neighbors by vector, referenced by slug |

```
# Find persons with similar criminal profile (k=10)
similar "persons/lucci" 10

# Narrow by status after vector search
similar "persons/lucci" 20
where_eq "status" "wanted"
```

The slug is looked up to retrieve its stored vector. No need to pass raw floats.

---

### 5.4 Spatial

Sekejap spatial is PostGIS-par. Nodes carry any GeoJSON geometry type: Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon, GeometryCollection. The R-Tree is indexed on the full geometry bounding envelope — not just a single centroid.

#### Geometry node data

Store geometry in the standard `"geometry"` GeoJSON field:

```python
db.put("zones/bangsar", json.dumps({
    "name": "Bangsar Zone",
    "geometry": {
        "type": "Polygon",
        "coordinates": [[
            [101.665, 3.128], [101.678, 3.128],
            [101.678, 3.135], [101.665, 3.135],
            [101.665, 3.128]
        ]]
    }
}))

db.put("roads/jalan-ara", json.dumps({
    "name": "Jalan Ara",
    "geometry": {
        "type": "LineString",
        "coordinates": [[101.668, 3.129], [101.674, 3.133]]
    }
}))
```

Legacy `"geo": {"loc": {"lat": …, "lon": …}}` point format is also fully supported.

#### Point/radius and bbox ops

| Op | Args | What it does |
|---|---|---|
| `near` | `lat lon radius_km` | Nodes whose centroid is within radius of a point |
| `spatial_within_bbox` | `minlat minlon maxlat maxlon` | Nodes whose envelope overlaps the box |
| `spatial_intersects_bbox` | `minlat minlon maxlat maxlon` | Alias for spatial_within_bbox |
| `spatial_within_polygon` | `(lat,lon) (lat,lon) ...` | Nodes whose centroid is inside an arbitrary polygon |

```
# Crimes within 1km of CIMB Bangsar
collection "crimes"
near 3.1291 101.6710 1.0

# Suspects living within a bounding box
collection "persons"
spatial_within_bbox 3.100 101.640 3.160 101.720

# Crimes inside the Bangsar commercial zone (centroid test)
collection "crimes"
spatial_within_polygon (3.128,101.665) (3.135,101.665) (3.135,101.678) (3.128,101.678) (3.128,101.665)
```

#### DE-9IM geometry predicates (PostGIS-par)

These ops operate on the node's actual `"geometry"` field — not just the centroid. Use these for precise spatial relationships across all geometry types.

| Op | Args | What it does |
|---|---|---|
| `st_within` | `(lat,lon) (lat,lon) ...` | Node geometry is **completely within** the query polygon |
| `st_contains` | `(lat,lon) (lat,lon) ...` | Node geometry **contains** the query polygon |
| `st_intersects` | `(lat,lon) (lat,lon) ...` | Node geometry **intersects** the query polygon |
| `st_dwithin` | `lat lon distance_km` | Node centroid is within distance of a point |

```
# All zones that contain the Bangsar CIMB location
collection "zones"
st_contains (3.1291,101.671) (3.1291,101.671) (3.1291,101.671)

# Roads and zones that cross through the Bangsar commercial area
collection "roads"
st_intersects (3.128,101.665) (3.128,101.678) (3.135,101.678) (3.135,101.665) (3.128,101.665)

# Gang territories (polygons) within the KL city boundary
collection "territories"
st_within (3.100,101.620) (3.100,101.750) (3.200,101.750) (3.200,101.620) (3.100,101.620)

# Suspects near the crime scene (using actual geometry, not just centroid)
collection "persons"
st_dwithin 3.1291 101.6710 1.0
```

> All polygon args use `(lat,lon)` pairs separated by spaces. Close the ring by repeating the first point.

> R-Tree pre-filter on the bounding envelope runs before the precise predicate test — performance is index-first.

---

#### PostGIS ↔ Sekejap geometry mapping

| PostGIS | Sekejap node `"geometry"` type |
|---|---|
| `ST_Point` | `{"type": "Point", "coordinates": [lon, lat]}` |
| `ST_MultiPoint` | `{"type": "MultiPoint", "coordinates": [...]}` |
| `ST_LineString` | `{"type": "LineString", "coordinates": [...]}` |
| `ST_MultiLineString` | `{"type": "MultiLineString", "coordinates": [...]}` |
| `ST_Polygon` | `{"type": "Polygon", "coordinates": [[...]]}` |
| `ST_MultiPolygon` | `{"type": "MultiPolygon", "coordinates": [...]}` |
| `ST_GeometryCollection` | `{"type": "GeometryCollection", "geometries": [...]}` |

Coordinates in GeoJSON order: `[longitude, latitude]`.

---

### 5.5 Full-Text Search

| Op | Args | What it does |
|---|---|---|
| `matching` | `"text query"` | Full-text search across title + content fields |
| `matching` | `"text" limit:n title_weight:f content_weight:f` | With optional tuning args |

```
# Basic search
collection "articles"
matching "robbery Bangsar motorcycle"

# Weighted — title matches count 2x
collection "articles"
matching "armed robbery" limit:100 title_weight:2.0 content_weight:1.0
```

---

### 5.6 Payload Filters

Filter by indexed fields (hot fields use O(1)/O(log N) indexes; others scan JSON).

| Op | Args | What it does |
|---|---|---|
| `where_eq` | `"field" value` | Exact match (string, number, bool) |
| `where_gt` | `"field" value` | Field > value |
| `where_lt` | `"field" value` | Field < value |
| `where_gte` | `"field" value` | Field >= value |
| `where_lte` | `"field" value` | Field <= value |
| `where_between` | `"field" lo hi` | lo <= field <= hi |
| `where_in` | `"field" "v1" "v2" ...` | Field is one of the values |

```
where_eq "status" "wanted"
where_eq "active" true
where_gt "severity" 7
where_between "severity" 5 9
where_in "type" "robbery" "assault" "carjacking"
where_gt "occurred_at" "2024-01-01T00:00:00+08:00"
```

---

### 5.7 Set Algebra

Combine pipelines with boolean logic.

| Op | Args | What it does |
|---|---|---|
| `intersect` | `{ pipeline }` | Keep nodes present in both |
| `union` | `{ pipeline }` | Merge both sets |
| `subtract` | `{ pipeline }` | Remove right set from left |

```
# Wanted persons who appear in both the robbery case AND the stolen vehicle case
backward "committed"
intersect {
  one "crimes/vehicle-theft-002"
  backward "committed"
}

# All crimes EXCEPT those already solved
collection "crimes"
subtract {
  collection "crimes"
  where_eq "status" "solved"
}
```

---

### 5.8 Result Shaping

These ops apply after all filters are resolved.

| Op | Args | What it does |
|---|---|---|
| `sort` | `"field" asc\|desc` | Order results by a payload field |
| `skip` | `n` | Skip first n results (pagination) |
| `take` | `n` | Return at most n results |
| `select` | `"f1" "f2" ...` | Return only named fields from payload |

```
sort "occurred_at" desc
sort "severity" desc
skip 20
take 10
select "name" "alias" "geo" "status" "priors"
```

---

### 5.9 Full Query Examples

**All open robberies near KLCC (2km), sorted by severity:**
```
collection "crimes"
where_eq "type" "robbery"
where_eq "status" "open"
near 3.1570 101.7123 2.0
sort "severity" desc
take 20
select "type" "severity" "occurred_at" "report_no"
```

**Who did the Bangsar robbery, where do they live:**
```
one "crimes/robbery-2024-11-03-bangsar"
backward "committed"
where_eq "status" "wanted"
forward "lives_at"
select "name" "alias" "geo" "address"
```

**Full gang network → their crimes → crime locations in Bangsar polygon:**
```
one "persons/lucci"
forward "member_of"
backward "member_of"
forward "committed"
forward "occurred_at"
spatial_within_polygon (3.128,101.665) (3.135,101.665) (3.135,101.678) (3.128,101.678) (3.128,101.665)
select "name" "type" "severity" "occurred_at"
sort "occurred_at" desc
```

**News article → crime → suspects → gang → all members with similar profile:**
```
collection "articles"
matching "armed robbery Bangsar 2024"
forward "reported_by"
backward "committed"
forward "member_of"
backward "member_of"
similar "persons/lucci" 20
where_eq "status" "wanted"
select "name" "alias" "priors" "geo"
take 50
```

**Suspects living near the crime scene who fled to a known hideout area:**
```
collection "crimes"
where_eq "type" "robbery"
near 3.1291 101.6710 0.5
backward "committed"
forward "fled_to"
spatial_within_bbox 3.155 101.688 3.175 101.712
select "name" "alias" "address" "geo"
```

---

## 6. Query — JSON Pipeline (Underlying Format)

SekejapQL compiles to this JSON format. You can use JSON directly if preferred — both paths execute identically. The JSON format additionally supports `intersect`, `union`, and `subtract` with nested pipelines.

```json
{"pipeline": [
  {"op": "collection", "name": "crimes"},
  {"op": "where_eq", "field": "type", "value": "robbery"},
  {"op": "near", "lat": 3.1291, "lon": 101.6710, "radius": 1.0},
  {"op": "sort", "field": "severity", "asc": false},
  {"op": "take", "n": 20}
]}
```

Spatial ops in JSON:
```json
{"op": "st_within",     "polygon": [[3.128, 101.665], [3.135, 101.665], [3.135, 101.678], [3.128, 101.665]]}
{"op": "st_intersects", "polygon": [[3.128, 101.665], [3.135, 101.678], [3.128, 101.665]]}
{"op": "st_dwithin",    "lat": 3.1291, "lon": 101.6710, "distance_km": 1.0}
```

**Python**
```python
result = db.query('{"pipeline": [...]}')         # returns JSON string
count  = db.query_count('{"pipeline": [...]}')   # returns int
plan   = db.explain('{"pipeline": [...]}')       # returns compiled steps
```

**Rust**
```rust
let outcome = db.query(r#"{"pipeline": [...]}"#)?;
let count   = db.query_count(r#"{"pipeline": [...]}"#)?;
let steps   = db.explain(r#"{"pipeline": [...]}"#)?;
```

---

## 7. Mutation — JSON Format

```python
# Upsert node
db.mutate('{"mutation":"put_json","data":{"_id":"crimes/001","type":"robbery"}}')

# Create edge
db.mutate('{"mutation":"link","source":"persons/lucci","target":"crimes/001","type":"committed","weight":1.0}')

# Create edge with metadata
db.mutate('{"mutation":"link_meta","source":"persons/lucci","target":"crimes/001","type":"committed","weight":1.0,"meta_json":"{\"role\":\"mastermind\"}"}')

# Remove node
db.mutate('{"mutation":"remove","slug":"crimes/001"}')

# Remove edge
db.mutate('{"mutation":"unlink","source":"persons/lucci","target":"crimes/001","type":"committed"}')
```

---

## 8. Describe / Introspection

```python
info = db.describe()
# info["vector"]["enabled"]          → bool
# info["spatial"]["indexed_nodes"]   → int
# info["graph"]["node_count"]        → int
# info["fulltext"]["adapter"]        → "tantivy" | "seekstorm" | null

col = db.describe_collection("crimes")
# col["count"]                                   → int
# col["indexes"]["graph"]["collection_bitmap_ready"] → bool
# col["indexes"]["vector"]["hnsw_ready"]         → bool
# col["indexes"]["spatial"]["rtree_ready"]       → bool
# col["indexes"]["fulltext"]["adapter_ready"]    → bool
# col["indexes"]["payload"]["hash_ready"]        → ["type", "status"]
# col["indexes"]["payload"]["range_ready"]       → ["severity"]
```

---

## 9. Node Data Conventions

| Field | Type | Purpose |
|---|---|---|
| `_id` | `"collection/key"` | Slug (used with `put_json`) |
| `vectors.dense` | `[f32 × 128]` | Vector for HNSW similarity search |
| `geometry` | GeoJSON object | Spatial geometry — any GeoJSON type (Point, LineString, Polygon, Multi*, GeometryCollection). R-Tree indexed on full bounding envelope. |
| `geo.loc.lat` | `f32` | Legacy point latitude (still supported; prefer `geometry`) |
| `geo.loc.lon` | `f32` | Legacy point longitude |
| `title` | `string` | Full-text title field |
| `content` | `string` | Full-text body field |

GeoJSON coordinates are `[longitude, latitude]` (standard GeoJSON order). Polygon rings must be closed (last point == first point).

Any other fields are stored in JSON and queryable via `where_*` (indexed if listed in `hot_fields`).

---

## 10. API Reference

Complete method listing for both Rust and Python. All methods are available on both unless noted.

### Node Operations

| Method | Python | Rust (`db.nodes().`) | Returns | Notes |
|---|---|---|---|---|
| Write single node | `db.put(slug, json)` | `.put(slug, json)` | `u32` (idx) | Upsert by slug |
| Write by `_id` field | `db.put_json(json)` | `.put_json(json)` | `u32` (idx) | Reads slug from `_id` in payload |
| Read node | `db.get(slug)` | `.get(slug)` | `Option<String>` | Raw JSON or None |
| Delete node | `db.remove(slug)` | `.remove(slug)` | `()` | Tombstone (soft delete) |
| Batch ingest (fast) | `db.ingest_nodes([(slug, json), ...])` | `.ingest(items)` | `Vec<u32>` | Deferred indexing; call `build_hnsw()` if using vectors |
| Batch ingest raw | — | `.ingest_raw(items)` | `(Vec<u32>, Vec<u32>)` | No HNSW; build separately |
| Build HNSW | `db.build_hnsw()` | `.build_hnsw()` | `()` | After `ingest_raw` |

### Edge Operations

| Method | Python | Rust (`db.edges().`) | Returns | Notes |
|---|---|---|---|---|
| Create edge | `db.link(src, dst, type, weight)` | `.link(src, dst, type, weight)` | `()` | Directed, typed, weighted |
| Create edge + metadata | `db.link_meta(src, dst, type, weight, meta_json)` | `.link_meta(src, dst, type, weight, meta_json)` | `()` | ≤32 B inline, else blob |
| Remove edge | `db.unlink(src, dst, type)` | `.unlink(src, dst, type)` | `()` | Tombstone |
| Batch ingest edges | `db.ingest_edges([(src, dst, type, weight), ...])` | `.ingest(edges)` | `()` | Single-commit batch |

### Schema / Collection Operations

| Method | Python | Rust (`db.schema().`) | Returns | Notes |
|---|---|---|---|---|
| Define collection | `db.define_collection(name, json)` | `.define(name, json)` | `()` | Sets hot_fields |
| Collection count | `db.count_collection(name)` | `.count(name)` | `usize` | O(1) atomic read |

### Index Lifecycle

| Method | Python | Rust (`db.`) | Returns | Notes |
|---|---|---|---|---|
| Enable vector index | `db.init_hnsw(m=16)` | `db.init_hnsw(m)` | `()` | Call once before inserting vectors |
| Enable full-text index | `db.init_fulltext()` | `db.init_fulltext(path)` | `()` | Call once |

### Query

| Method | Python | Rust (`db.`) | Returns | Notes |
|---|---|---|---|---|
| SekejapQL query | `db.query_skql(text)` | `db.query_skql(text)` | hits / `Outcome<Vec<Hit>>` | Text format |
| SekejapQL count | `db.query_skql_count(text)` | `db.query_skql_count(text)` | `usize` / `Outcome<usize>` | |
| SekejapQL explain | `db.explain_skql(text)` | `db.explain_skql(text)` | `String` / `Vec<Step>` | Compile only |
| JSON pipeline query | `db.query(json)` | `db.query(json)` | hits / `Outcome<Vec<Hit>>` | |
| JSON pipeline count | `db.query_count(json)` | `db.query_count(json)` | `usize` / `Outcome<usize>` | |
| JSON pipeline explain | `db.explain(json)` | `db.explain(json)` | `String` / `Vec<Step>` | Compile only |
| Mutation | `db.mutate(json)` | `db.mutate(json)` | JSON / `Value` | put/link/remove/unlink |

**Rust-only typed query builder** (no Python equivalent — use `query_skql` from Python):
```rust
db.nodes().collection("crimes").near(3.13, 101.67, 1.0).where_eq("status", "open").take(20).collect()?;
db.nodes().one("persons/lucci").forward("committed").collect()?;
db.nodes().all().similar(&vec, 10).collect()?;
```

**Python-only convenience shortcuts** (wraps the fluent builder):
```python
db.one("persons/lucci")                     # → single PyHit or None
db.collection("crimes")                   # → list[PyHit]
db.forward("persons/lucci", "committed", max_hops=1)
db.backward("crimes/001", "committed")
db.near(3.1291, 101.6710, 1.0)           # across all nodes
db.similar(query_vec, k=10)
db.matching("robbery Bangsar")
```

### Introspection

| Method | Python | Rust (`db.`) | Returns |
|---|---|---|---|
| DB info | `db.describe()` | `db.describe()` | JSON string / `Value` |
| Collection info | `db.describe_collection(name)` | `db.describe_collection(name)` | JSON string / `Value` |

### Persistence

| Method | Python | Rust (`db.`) | Returns |
|---|---|---|---|
| Flush to disk | `db.flush()` | `db.flush()` | `()` |
| Backup to file | `db.backup(path)` | `db.backup(path)` | `()` |
| Restore from file | `db.restore(path)` | `db.restore(path)` | `()` |
| Close | `db.close()` | `drop(db)` | — |

---

## 11. Performance Notes

- **Batch ingestion** (`ingest_nodes`) is 10–100x faster than individual `put()` calls — it defers all index builds to a single commit.
- **HNSW build** (v0.3+): parallel construction via Rayon, NEON/AVX2 SIMD CosineDistance, adaptive `ef_construction`. 44x faster than v0.2 (115s → 2.6s at 10k nodes).
- **Graph traversal**: 10x faster than SQLite recursive CTE at 100× 3-hop queries.
- **Vector retrieval**: 170x faster than SQLite raw-bytes workaround.
- **hot_fields** (hash + range indexes) give O(1) and O(log N) `where_*` performance. Unindexed fields scan JSON blobs.
