# sekejap

Embedded, graph-first multimodel database. Graph traversal, spatial search, vector similarity, and full-text search — composable in a single query, zero external dependencies, runs in-process.

**Built for workloads that need more than one data model at a time:**

- root-cause analysis — traverse a causal graph filtered by text relevance
- hybrid RAG — find semantically similar nodes then walk their graph context
- knowledge graph discovery — spatial + graph + vector in one query
- spatiotemporal intelligence — who was where, connected to what, when

Available as a Rust library, Rust CLI, and Python library.

---

## Hello World — One Piece

The Grand Line is a graph. Islands are nodes. Sailing routes are edges. Characters have bounties, affiliations, fighting styles, and coordinates.

```python
from sekejap import DB

db = DB()

# ── Schema ────────────────────────────────────────────────────────────────────

db.execute("""
    CREATE TABLE characters (
        _key       TEXT PRIMARY KEY,
        name       TEXT,
        crew       TEXT,
        bounty     INTEGER,
        location   GEO,
        embedding  VECTOR
    )
""")

db.execute("""
    CREATE TABLE islands (
        _key     TEXT PRIMARY KEY,
        name     TEXT,
        sea      TEXT,
        geometry GEO
    )
""")

db.execute("CREATE INDEX ON characters USING hash    (crew)")
db.execute("CREATE INDEX ON characters USING btree   (bounty)")
db.execute("CREATE INDEX ON characters USING gin     (name)")
db.execute("CREATE INDEX ON characters USING spatial (location)")
db.execute("CREATE INDEX ON characters USING hnsw    (embedding)")
db.execute("CREATE INDEX ON islands    USING spatial (geometry)")

# ── Nodes ─────────────────────────────────────────────────────────────────────

db.execute("INSERT INTO characters (_key, name, crew, bounty) VALUES ('luffy',  'Monkey D. Luffy',  'straw-hat',  3000000000)")
db.execute("INSERT INTO characters (_key, name, crew, bounty) VALUES ('zoro',   'Roronoa Zoro',     'straw-hat',  1111000000)")
db.execute("INSERT INTO characters (_key, name, crew, bounty) VALUES ('sanji',  'Vinsmoke Sanji',   'straw-hat',  1032000000)")
db.execute("INSERT INTO characters (_key, name, crew, bounty) VALUES ('shanks', 'Red Hair Shanks',  'red-hair',   4048900000)")
db.execute("INSERT INTO characters (_key, name, crew, bounty) VALUES ('mihawk', 'Dracule Mihawk',   'shichibukai', 0)")

db.execute("INSERT INTO islands (_key, name, sea) VALUES ('marineford',     'Marineford',     'grand-line')")
db.execute("INSERT INTO islands (_key, name, sea) VALUES ('dressrosa',      'Dressrosa',      'grand-line')")
db.execute("INSERT INTO islands (_key, name, sea) VALUES ('wano',           'Wano Kuni',      'grand-line')")
db.execute("INSERT INTO islands (_key, name, sea) VALUES ('fishman-island', 'Fishman Island', 'grand-line')")

# ── Edges ─────────────────────────────────────────────────────────────────────

db.execute("INSERT ('characters/luffy')-[:rival {strength: 10}]->('characters/mihawk')")
db.execute("INSERT ('characters/zoro')-[:student_of {years: 3}]->('characters/mihawk')")
db.execute("INSERT ('characters/shanks')-[:allied_with {trust: 10}]->('characters/luffy')")
db.execute("INSERT ('islands/marineford')-[:route_to {days: 3}]->('islands/fishman-island')")
db.execute("INSERT ('islands/fishman-island')-[:route_to {days: 7}]->('islands/dressrosa')")
db.execute("INSERT ('islands/dressrosa')-[:route_to {days: 5}]->('islands/wano')")
```

```python
# ── Graph: who trained under Mihawk, and who are their rivals? ────────────────

hits = db.query("""
    SELECT b._key AS name
    FROM MATCH (a:characters)-[:student_of]->(:characters {_key: 'mihawk'})<-[:rival]-(b:characters)
""")

# ── Graph: reachable islands within 3 hops from Marineford ───────────────────

hits = db.query("""
    SELECT dest._key AS island
    FROM MATCH (start:islands)-[:route_to*1..3]->(dest:islands)
    WHERE start._key = 'marineford'
""")

# ── Aggregate: total route days from Marineford to each destination ───────────

hits = db.query("""
    SELECT dest._key AS island, SUM(r.days) AS total_days
    FROM MATCH (start:islands)-[r:route_to*1..3]->(dest:islands)
    WHERE start._key = 'marineford'
    GROUP BY dest._key
    ORDER BY total_days ASC
""")

# ── Spatial: islands within 1000 km of Marineford (0°, 0°) ───────────────────

hits = db.query("""
    SELECT * FROM islands
    WHERE ST_DWithin(geometry, POINT(0.0 0.0), 1000.0)
""")

# ── Vector: characters with similar fighting style to Zoro ───────────────────

zoro_vec = [0.95, 0.02, 0.01, 0.02]   # hypothetical embedding
hits = db.query(f"SELECT * FROM characters WHERE VECTOR_NEAR(embedding, {zoro_vec}, 5)")

# ── BM25: search bounty posters by wanted description ────────────────────────

hits = db.query("""
    SELECT * FROM characters
    WHERE BM25(description, 'swordsman pirate dangerous') > 0.3
    ORDER BY BM25(description, 'swordsman pirate dangerous') DESC
""")
```

---

## Data Types

| Type | SQL keyword | Stored as | Use for |
|---|---|---|---|
| Text | `TEXT` | UTF-8 string | names, categories, IDs |
| Integer | `INTEGER` | i64 | counts, years, bounties |
| Float | `REAL` | f64 | scores, weights, ratios |
| Timestamp | `TIMESTAMPTZ` | ISO-8601 | events, creation time |
| Geometry | `GEO` | GeoJSON object | points, polygons, lines |
| Vector | `VECTOR` | `[f32, ...]` array | embeddings |
| JSON | `JSON` | arbitrary JSON | nested / unstructured |

**GEO** accepts any GeoJSON geometry — `Point`, `Polygon`, `LineString`, `MultiPolygon`, etc.

**VECTOR** is inserted as a SQL array literal: `[0.12, -0.03, 0.87, ...]`

---

## Indexes

| Index | `USING` keyword | Enables |
|---|---|---|
| Hash | `hash` | `field = 'val'`, `IN (...)`, equality lookups |
| B-tree | `btree` | `>`, `<`, `BETWEEN`, `ORDER BY field` |
| GIN | `gin` | `ILIKE '%pattern%'` (exact trigram postings, no verification step) |
| Spatial | `spatial` | `ST_DWithin`, `ST_Contains`, `ST_Within`, `ST_Intersects` |
| HNSW | `hnsw` | `VECTOR_NEAR(field, [...], k)`, `ORDER BY field <=> [...]`, `VECTOR_COSINE(field, [...])` in score expressions |
| BM25 | `bm25` | `BM25(field, 'query') > score`, `ORDER BY BM25(...) DESC`, `BM25(...)` in score expressions |

All indexes are built via `CREATE INDEX`:

```sql
CREATE INDEX ON characters USING hash    (crew)
CREATE INDEX ON characters USING btree   (bounty)
CREATE INDEX ON characters USING gin     (name)
CREATE INDEX ON characters USING spatial (location)
CREATE INDEX ON characters USING hnsw    (embedding)
CREATE INDEX ON characters USING bm25    (bio)
```

Or declared inline in `CREATE TABLE WITH (...)`:

```sql
CREATE TABLE characters (
    _key      TEXT PRIMARY KEY,
    name      TEXT,
    bounty    INTEGER,
    location  GEO,
    embedding VECTOR,
    bio       TEXT
) WITH (hash: ['_key'], range: ['bounty'], fulltext: ['name'], spatial: ['location'], vector: ['embedding'], bm25: ['bio'])
```

**GIN** stores exact trigram→document postings (no lossy signatures), so `ILIKE` queries require no verification pass. GIN is maintained automatically on every insert — declaring the index before loading data is the standard workflow.

**HNSW** is rebuilt automatically after each `put_vector` call when an index is declared. For large bulk loads, call `REINDEX` once after all data is in to rebuild the graph in one pass.

**BM25** is batch-built at `CREATE INDEX` time. Run `REINDEX` after inserting new documents.

All index types survive a cold restart. Hash, B-tree, GIN, and BM25 indexes are rebuilt from persisted schema hints on open. HNSW and Spatial indexes are stored directly in the snapshot.

---

## Interfaces

sekejap has three interfaces. Use whichever fits the context.

### SQL

Standard SQL for schema, mutations, and queries. Use this most of the time.

```sql
-- Schema
CREATE TABLE islands (_key TEXT PRIMARY KEY, name TEXT, sea TEXT, geometry GEO)
CREATE INDEX ON islands USING spatial (geometry)

-- Mutations
INSERT INTO islands (_key, name, sea) VALUES ('wano', 'Wano Kuni', 'grand-line')
UPDATE islands SET sea = 'new-world' WHERE _key = 'wano'
DELETE FROM islands WHERE sea = 'east-blue'

-- Schema lifecycle
DROP TABLE islands
DROP TABLE IF EXISTS islands

-- DROP INDEX
DROP INDEX ON islands USING spatial (geometry)
DROP INDEX IF EXISTS ON islands USING btree (elevation)

-- REINDEX (force rebuild — useful after large bulk loads)
REINDEX ON researchers USING hnsw    (embedding)
REINDEX ON papers      USING bm25    (abstract)
REINDEX ON characters  USING gin     (name)

-- ALTER TABLE (PostgreSQL-style)
ALTER TABLE islands ADD COLUMN elevation INTEGER
ALTER TABLE islands DROP COLUMN elevation
ALTER TABLE islands DROP COLUMN IF EXISTS elevation
ALTER TABLE islands RENAME COLUMN sea TO ocean
ALTER TABLE islands RENAME TO atolls
ALTER TABLE islands ALTER COLUMN elevation TYPE REAL

-- Edges
INSERT ('islands/marineford')-[:route_to {days: 3}]->('islands/fishman-island')
DELETE ('islands/marineford')-[:route_to]->('islands/fishman-island')

-- Graph traversal
SELECT dest._key AS island
FROM MATCH (a:islands)-[:route_to*1..5]->(dest:islands)
WHERE a._key = 'marineford'

-- Graph aggregation
SELECT b._key AS name, COUNT(a) AS allies, SUM(r.strength) AS total_strength
FROM MATCH (a:characters)-[r:collaborated_with]->(b:characters)
GROUP BY b._key
ORDER BY total_strength DESC
LIMIT 10

-- Multi-stage graph query with WITH chaining
SELECT c.name AS island, COUNT(*) AS visitors
FROM MATCH (a:characters)-[:allied_with]->(b:characters)
WHERE a._key = 'luffy'
WITH b
MATCH (b)-[:visited]->(c:islands)
GROUP BY c.name
ORDER BY visitors DESC

-- MATCH...RETURN (Cypher-style, also via query())
MATCH (a:characters)-[:rival]->(b:characters)
RETURN a._key AS name, b.bounty AS rival_bounty

-- Edge intrinsics: _depth, _path_keys, _path_strength, _avg_strength, _min/_max_strength
-- Available on any named edge binding (e.g. [r:route_to] or [r*])
SELECT dest._key AS island, r2._depth AS hops, r2._path_keys AS route
FROM MATCH (start:islands)-[r:route_to]->(stop:islands)-[r2:route_to]->(dest:islands)
WHERE start._key = 'marineford'

-- PATH_* aggregates — operate on a JSON array in a path intrinsic field
-- PATH_AVG, PATH_SUM, PATH_MIN, PATH_MAX, PATH_PRODUCT, PATH_FIRST, PATH_LAST
SELECT c._key AS dest,
       PATH_PRODUCT(r2._path_strength) AS combined_reliability,
       PATH_FIRST(r2._path_keys)       AS departure,
       PATH_LAST(r2._path_keys)        AS arrival
FROM MATCH (a:islands)-[r:route_to]->(b:islands)-[r2:route_to]->(c:islands)
WHERE a._key = 'marineford'

-- CASE WHEN — conditional expression in SELECT list
SELECT b._key AS name,
       CASE WHEN r._depth = 1 THEN 'direct'
            WHEN r._depth = 2 THEN 'indirect'
            ELSE 'distant'
       END AS rivalry_type
FROM MATCH (a:characters)-[r:rival]->(b:characters)

-- Time expressions: NOW(), AGE_DAYS(var.field), AGE_HOURS(var.field)
-- NOW() returns current Unix timestamp (seconds as i64)
-- AGE_DAYS / AGE_HOURS accept a Unix int or "YYYY-MM-DD" string field
SELECT b._key AS name,
       AGE_DAYS(b.last_seen) AS days_since_seen,
       NOW()                  AS queried_at
FROM MATCH (a:characters)-[r:rival]->(b:characters)

-- JSON_ARRAY_LENGTH — length of a JSON array field
SELECT b._key AS dest, JSON_ARRAY_LENGTH(r._path_keys) AS hops_plus_one
FROM MATCH (a:islands)-[r:route_to*1..3]->(b:islands)
WHERE a._key = 'marineford'

-- Shortest path — 0 rows = unreachable, 1 row = found (path fields via r.*)
SELECT a.name AS from_name, b.name AS to_name, r.length AS hops, r._path_keys AS route
FROM MATCH SHORTEST (a)-[r*]->(b)
WHERE a._key = 'islands/marineford' AND b._key = 'islands/wano'
-- Path predicates (ANY / ALL / NONE / SINGLE)
AND ANY(n IN nodes(r) WHERE n.climate = 'tropical')

-- Multi-FROM cross-join — two independent sources Cartesian-producted
SELECT a._key AS island, b._key AS character
FROM MATCH ('crews/straw-hats')-[:member]->(b), islands AS a

-- Spatial
SELECT * FROM islands WHERE ST_DWithin(geometry, POINT(0.0 0.0), 500.0)
SELECT * FROM zones   WHERE ST_Contains(geometry, POINT(144.9671 -37.8183))

-- Vector
SELECT * FROM characters WHERE VECTOR_NEAR(embedding, [0.9, 0.1, 0.0], 5)

-- Full-text (GIN — fast exact ILIKE, no score)
SELECT * FROM characters WHERE name ILIKE '%shanks%'

-- Full-text (BM25 — relevance-ranked)
SELECT * FROM papers WHERE BM25(abstract, 'neural network') > 0.3
ORDER BY BM25(abstract, 'neural network') DESC

-- Arithmetic ORDER BY (weighted multi-signal ranking)
-- Combine any signals with +, -, *, /, (), and unary negation
ORDER BY BM25(title, 'pirate') * 0.7 + BM25(bio, 'pirate') * 0.3 DESC
ORDER BY BM25(title, 'pirate') * 0.5 + bounty * 0.5 DESC
ORDER BY VECTOR_COSINE(embedding, [0.9, 0.1, 0.0]) * 0.6 + BM25(bio, 'pirate') * 0.4 DESC
ORDER BY (BM25(title, 'luffy') + BM25(bio, 'luffy')) * 0.8 + threat_level * 0.2 DESC

-- Spatial signal: ST_DISTANCE_KM(geometry_field, POINT(lon lat)) → km (f64)
-- Negate to rank nearest-first: -ST_DISTANCE_KM(...) DESC
ORDER BY -ST_DISTANCE_KM(location, POINT(144.9671 -37.8183)) DESC

-- Vector distance operators (compile to same score node as the function forms)
-- a <=> b  ==  VECTOR_COSINE(a, b)    cosine distance (lower = more similar)
-- a <-> b  ==  VECTOR_L2(a, b)        Euclidean / L2
-- a <#> b  ==  VECTOR_DOT(a, b)       inner product (NOT negated, unlike pgvector)
-- a <+> b  ==  VECTOR_L1(a, b)        Manhattan / L1
ORDER BY embedding <=> [0.9, 0.1, 0.0] ASC   -- nearest cosine first

-- VECTOR_COSINE(field, [vec]) returns cosine distance (lower = more similar)
-- Numeric payload fields are coerced to f64 (absent or non-numeric = 0.0)
-- Default direction for score expressions is DESC (highest score first)

-- Filters
WHERE bounty BETWEEN 1000000000 AND 4000000000
WHERE crew IN ('straw-hat', 'red-hair')
WHERE name ILIKE '%luffy%'
WHERE description IS NOT NULL
AND / OR / NOT

-- Introspection
SHOW TABLES                                  -- all collections with row counts
SHOW EDGES                                   -- full graph schema with edge counts
SHOW EDGES FROM characters                   -- edge types leaving a collection + counts
SHOW EDGES FROM characters TO islands        -- edge types between two collections + counts
SHOW characters                              -- field structure (declared schema or inferred)
```

#### Graph path queries

`SELECT … FROM MATCH SHORTEST` finds the shortest directed path between two nodes.
Returns **1 row** when a path is found, **0 rows** when none exists.
Path fields are exposed via the path-bind variable (e.g. `r`):
`r.length`, `r._path_keys`, `r._path_strength`, `r.nodes`, `r.edges`.

```python
hits = db.query("""
    SELECT a.name AS from_name, b.name AS to_name,
           r.length AS hops, r._path_keys AS route
    FROM MATCH SHORTEST (a)-[r*]->(b)
    WHERE a._key = 'islands/marineford' AND b._key = 'islands/wano'
""")

if hits:
    import json
    row = json.loads(hits[0].payload)
    print(f"Shortest route: {row['hops']} hops")
    for slug in row['route']:
        print(" ", slug)
```

Path predicates filter on intermediate nodes:

```python
# Only keep paths where every node has a tropical climate
hits = db.query("""
    SELECT r.length AS hops
    FROM MATCH SHORTEST (a)-[r*]->(b)
    WHERE a._key = 'islands/marineford' AND b._key = 'islands/wano'
    AND ALL(n IN nodes(r) WHERE n.climate = 'tropical')
""")

### Atomic (Rust fluent builder)

Use when you need lower-level control — pre-resolved hashes, programmatic step composition, or performance-sensitive inner loops.

```rust
use sekejap::CoreDB;

let mut db = CoreDB::open("./data")?;

// Fluent scan with filters
let hits = db.collection("characters")
    .where_eq("crew", "straw-hat")
    .where_gte("bounty", 1_000_000_000)
    .order_by("bounty", true)   // true = descending
    .limit(10)
    .collect();

// Vector similarity
let hits = db.collection("characters")
    .vector_near("embedding", query_vec, 10)
    .collect();

// Spatial radius
let hits = db.collection("islands")
    .st_dwithin(-37.8183, 144.9671, 5.0)   // lat, lon, km
    .collect();

// Raw node operations
db.put("characters/luffy", r#"{"_collection":"characters","_key":"luffy","name":"Luffy"}"#)?;
db.get("characters/luffy");
db.remove("characters/luffy");

// Edges
db.link("characters/zoro", "characters/mihawk", "student_of", 1.0);
db.link_meta("islands/marineford", "islands/fishman-island", "route_to", 1.0, r#"{"days":3}"#)?;
db.unlink("characters/zoro", "characters/mihawk", "student_of");

// Shortest path — 0 rows = unreachable, 1 row = found
let hits = db.query(
    "SELECT a.name AS from_n, b.name AS to_n, r.length AS hops, r._path_keys AS route \
     FROM MATCH SHORTEST (a)-[r*]->(b) \
     WHERE a._key = 'islands/marineford' AND b._key = 'islands/wano'"
)?.collect();
if let Some(hit) = hits.first() {
    if let Some(p) = &hit.payload {
        println!("{} hops", p["hops"]);
        if let Some(arr) = p["route"].as_array() {
            for slug in arr { println!("  {}", slug); }
        }
    }
}
```

### Python DataFrame (`db.df`)

Use for data science workflows — loading from CSV/parquet, returning query results as DataFrames.

```python
import pandas as pd
import json
from sekejap import DB

db = DB("./data")

# ── Load from DataFrame ───────────────────────────────────────────────────────

df = pd.read_csv("characters.csv")
# map DataFrame columns to schema field names
db.df.load_nodes(df, "characters", id_col="character_id",
                 mapping={"character_id": "_key", "full_name": "name"})

df_routes = pd.read_csv("routes.csv")  # columns: from_island, to_island, days
db.df.load_edges(
    df_routes,
    source_col="from_island",
    target_col="to_island",
    edge_type="route_to",
    source_collection="islands",
    target_collection="islands",
    weight_col="days",
)

# ── Query → DataFrame ─────────────────────────────────────────────────────────

df = db.df.query("SELECT * FROM characters WHERE bounty >= 1000000000")
df = db.df.query("SELECT * FROM characters WHERE VECTOR_NEAR(embedding, [0.9, 0.1, 0.0], 20)")
df = db.df.query("SELECT * FROM islands WHERE ST_DWithin(geometry, POINT(0.0 0.0), 500.0)")

# ── Create collection from field spec ────────────────────────────────────────

db.df.create_collection(
    "characters",
    fields={
        "_key":      "TEXT PRIMARY KEY",
        "name":      "TEXT",
        "bounty":    "INTEGER",
        "location":  "GEO",
        "embedding": "VECTOR",
    },
    hash_index=["_key", "crew"],
    range_index=["bounty"],
    spatial_index=["location"],
    vector_index=["embedding"],
)
```

---

## Full Data Science Example — Grand Line Intelligence

A pirate intelligence system combining all four data models.

### Schema and data loading

```python
from sekejap import DB
import pandas as pd
import numpy as np
import json

db = DB("./grandline_db")

db.execute("""
    CREATE TABLE characters (
        _key       TEXT PRIMARY KEY,
        name       TEXT,
        crew       TEXT,
        role       TEXT,
        bounty     INTEGER,
        location   GEO,
        embedding  VECTOR
    )
""")
db.execute("""
    CREATE TABLE bounty_posters (
        _key        TEXT PRIMARY KEY,
        subject     TEXT,
        description TEXT,
        bounty      INTEGER,
        year        INTEGER
    )
""")

db.execute("CREATE INDEX ON characters     USING hash    (crew)")
db.execute("CREATE INDEX ON characters     USING btree   (bounty)")
db.execute("CREATE INDEX ON characters     USING gin     (name)")
db.execute("CREATE INDEX ON characters     USING spatial (location)")
db.execute("CREATE INDEX ON characters     USING hnsw    (embedding)")
db.execute("CREATE INDEX ON bounty_posters USING bm25    (description)")

# Load from CSV + numpy embeddings
df = pd.read_csv("characters.csv")
embeddings = np.load("embeddings.npy")   # shape (n, 384)
df["location"] = df.apply(
    lambda r: json.dumps({"type": "Point", "coordinates": [r.lon, r.lat]}), axis=1
)
df["embedding"] = [e.tolist() for e in embeddings]

db.df.load_nodes(df, "characters", id_col="character_id",
                 mapping={"character_id": "_key"})

db.df.load_edges(
    pd.read_csv("rivalries.csv"),
    source_col="from_id",
    target_col="to_id",
    edge_type="rival",
    source_collection="characters",
    target_collection="characters",
    weight_col="intensity",
)
```

### Spatial — powerful pirates in the New World

```python
df = db.df.query("""
    SELECT * FROM characters
    WHERE ST_DWithin(location, POINT(0.0 0.0), 500.0)
      AND crew != 'marine'
      AND bounty >= 1000000000
    ORDER BY bounty DESC
    LIMIT 20
""")
```

### Vector — characters with similar fighting style

```python
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")
vec = model.encode("swordsman close-range power haki").tolist()

df = db.df.query(f"""
    SELECT * FROM characters
    WHERE VECTOR_NEAR(embedding, {vec}, 10)
      AND bounty >= 500000000
""")
```

### Graph — rival and alliance networks, path queries

```python
# Shortest path between two characters (0 rows = unreachable)
hits = db.query("""
    SELECT r.length AS hops, r._path_keys AS route
    FROM MATCH SHORTEST (a)-[r*]->(b)
    WHERE a._key = 'characters/luffy' AND b._key = 'characters/mihawk'
""")
if hits:
    import json
    row = json.loads(hits[0].payload)
    print(f"Degrees of separation: {row['hops']}")
    for slug in row['route']:
        print(" ", slug)

# 2-hop rival network from Luffy
hits = db.query("""
    SELECT b._key AS name
    FROM MATCH (a:characters)-[:rival*1..2]->(b:characters)
    WHERE a._key = 'luffy'
""")

# Most feared pirates by rival count
hits = db.query("""
    SELECT b._key AS pirate, COUNT(a) AS rivals, SUM(r.intensity) AS total_threat
    FROM MATCH (a:characters)-[r:rival]->(b:characters)
    GROUP BY b._key
    ORDER BY total_threat DESC
    LIMIT 10
""")

# Cross-crew rivalries
hits = db.query("""
    SELECT a.crew AS from_crew, b.crew AS to_crew, COUNT(r) AS clashes
    FROM MATCH (a:characters)-[r:rival]->(b:characters)
    GROUP BY a.crew, b.crew
    ORDER BY clashes DESC
""")
```

### PATH_* aggregates — aggregate over path arrays

Edge bindings expose path intrinsics as JSON arrays: `_path_keys`, `_path_strength`, `_path_length`.
`PATH_*` functions aggregate over those arrays in a single SELECT expression.

```python
# PATH_PRODUCT: multiply all strengths along a 2-hop route (reliability score)
hits = db.query("""
    SELECT dest._key AS island,
           PATH_PRODUCT(r2._path_strength) AS route_reliability,
           PATH_FIRST(r2._path_keys)       AS departure,
           PATH_LAST(r2._path_keys)        AS arrival
    FROM MATCH (start:islands)-[r:route_to]->(mid:islands)-[r2:route_to]->(dest:islands)
    WHERE start._key = 'marineford'
    ORDER BY route_reliability DESC
""")

# PATH_AVG / PATH_SUM / PATH_MIN / PATH_MAX / PATH_FIRST / PATH_LAST work the same way
hits = db.query("""
    SELECT c._key AS target,
           PATH_MIN(r2._path_strength)  AS weakest_link,
           PATH_AVG(r2._path_strength)  AS avg_intensity
    FROM MATCH (a:characters)-[r:rival]->(b:characters)-[r2:rival]->(c:characters)
    WHERE a._key = 'shanks'
    ORDER BY avg_intensity DESC
""")
```

### CASE WHEN, time functions, JSON_ARRAY_LENGTH

```python
# CASE WHEN — conditional expression in SELECT list
hits = db.query("""
    SELECT b._key AS name,
           CASE WHEN r._depth = 1 THEN 'direct rival'
                WHEN r._depth = 2 THEN 'indirect rival'
                ELSE 'distant connection'
           END AS rivalry_type
    FROM MATCH (a:characters)-[r:rival]->(b:characters)
""")

# AGE_DAYS / AGE_HOURS — time since a field's epoch (Unix int or "YYYY-MM-DD")
# NOW() — current Unix timestamp in seconds
hits = db.query("""
    SELECT b._key                AS name,
           AGE_DAYS(b.last_seen) AS days_inactive,
           NOW()                 AS queried_at
    FROM MATCH (a:characters)-[r:rival]->(b:characters)
    ORDER BY days_inactive DESC
""")

# JSON_ARRAY_LENGTH — length of a JSON array field (e.g. _path_keys)
hits = db.query("""
    SELECT dest._key                       AS island,
           JSON_ARRAY_LENGTH(r._path_keys) AS stops
    FROM MATCH (start:islands)-[r:route_to*1..3]->(dest:islands)
    WHERE start._key = 'marineford'
    ORDER BY stops ASC
""")
```

### BM25 — search bounty posters

```python
df = db.df.query("""
    SELECT * FROM bounty_posters
    WHERE BM25(description, 'swordsman dangerous haki') > 0.2
      AND bounty >= 100000000
    ORDER BY BM25(description, 'swordsman dangerous haki') DESC
""")
```

### Multi-modal — spatial + graph + vector in one workflow

```python
# "Pirates near Marineford who are in Shanks' rival network
#  and have a similar fighting style to Whitebeard"

whitebeard_vec = model.encode("massive power conqueror close-range").tolist()

# Step 1: find pirates near Marineford (0°, 0°)
nearby = db.df.query("SELECT * FROM characters WHERE ST_DWithin(location, POINT(0.0 0.0), 300.0)")

# Step 2: walk Shanks' rival graph
rivals = db.query("""
    SELECT b._key AS name
    FROM MATCH (a:characters)-[:rival*1..3]->(b:characters)
    WHERE a._key = 'shanks'
""")
rival_keys = {json.loads(h.payload)["name"] for h in rivals if h.payload}

# Step 3: filter nearby pirates who appear in the rival graph
candidates = nearby[nearby["_key"].isin(rival_keys)]
keys_clause = ", ".join(f"'{k}'" for k in candidates["_key"])

# Step 4: rank by vector similarity to Whitebeard
result = db.df.query(f"""
    SELECT * FROM characters
    WHERE _key IN ({keys_clause})
      AND VECTOR_NEAR(embedding, {whitebeard_vec}, 5)
""")
```

---

## Installation

```bash
# Rust library
cargo add sekejap

# Rust CLI
cargo install sekejap-cli

# Python
pip install sekejap
```

## CLI

```bash
sekejap                              # in-memory REPL
sekejap ./data                       # persistent REPL
sekejap ./data "SELECT * FROM r;"    # one-shot
echo "SELECT...;" | sekejap ./data   # pipe script

sekejap> CREATE TABLE islands (_key TEXT, name TEXT, geometry GEO);
sekejap> INSERT INTO islands (_key, name, sea) VALUES ('wano', 'Wano Kuni', 'grand-line');
sekejap> SELECT * FROM islands WHERE ST_DWithin(geometry, POINT(0.0 0.0), 500.0);

-- Introspection (SQL)
sekejap> SHOW TABLES;
sekejap> SHOW EDGES;
sekejap> SHOW EDGES FROM characters;
sekejap> SHOW characters;

-- Introspection (dot commands — same results, tabular output)
sekejap> .tables
sekejap> .edges
sekejap> .edges characters
sekejap> .schema islands
sekejap> .stats
sekejap> .help
```

## License

MIT
