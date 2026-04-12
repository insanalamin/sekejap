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

db.execute("CREATE INDEX ON characters(crew)      USING hash")
db.execute("CREATE INDEX ON characters(bounty)    USING btree")
db.execute("CREATE INDEX ON characters(location)  USING spatial")
db.execute("CREATE INDEX ON characters(embedding) USING hnsw")
db.execute("CREATE INDEX ON islands(geometry)     USING spatial")

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
    MATCH (a:characters)-[:student_of]->(:characters {_key: 'mihawk'})<-[:rival]-(b:characters)
    RETURN b
""")

# ── Graph: reachable islands within 3 hops from Marineford ───────────────────

hits = db.query("""
    MATCH (start:islands)-[:route_to*1..3]->(dest:islands)
    WHERE start._key = 'marineford'
    RETURN dest
""")

# ── Aggregate: total route days from Marineford to each destination ───────────

hits = db.query("""
    MATCH (start:islands)-[r:route_to*1..3]->(dest:islands)
    WHERE start._key = 'marineford'
    RETURN dest._key AS island, SUM(r.days) AS total_days
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
| Spatial | `spatial` | `ST_DWithin`, `ST_Contains`, `ST_Within`, `ST_Intersects` |
| HNSW | `hnsw` | `VECTOR_NEAR(field, [...], k)`, `ORDER BY field <=> [...]` |
| BM25 | `bm25` | `BM25(field, 'query') > score`, `ORDER BY BM25(...) DESC` |

All indexes are built via `CREATE INDEX`:

```sql
CREATE INDEX ON characters(crew)      USING hash
CREATE INDEX ON characters(bounty)    USING btree
CREATE INDEX ON characters(location)  USING spatial
CREATE INDEX ON characters(embedding) USING hnsw
CREATE INDEX ON characters(bio)       USING bm25
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
) WITH (hash: ['_key'], range: ['bounty'], spatial: ['location'], vector: ['embedding'], bm25: ['bio'])
```

---

## Interfaces

sekejap has three interfaces. Use whichever fits the context.

### SQL

Standard SQL for schema, mutations, and queries. Use this most of the time.

```sql
-- Schema
CREATE TABLE islands (_key TEXT PRIMARY KEY, name TEXT, sea TEXT, geometry GEO)
CREATE INDEX ON islands(geometry) USING spatial

-- Mutations
INSERT INTO islands (_key, name, sea) VALUES ('wano', 'Wano Kuni', 'grand-line')
UPDATE islands SET sea = 'new-world' WHERE _key = 'wano'
DELETE FROM islands WHERE sea = 'east-blue'

-- Edges
INSERT ('islands/marineford')-[:route_to {days: 3}]->('islands/fishman-island')
DELETE ('islands/marineford')-[:route_to]->('islands/fishman-island')

-- Graph traversal
MATCH (a:islands)-[:route_to*1..5]->(dest:islands)
WHERE a._key = 'marineford'
RETURN dest

-- Graph aggregation
MATCH (a:characters)-[r:collaborated_with]->(b:characters)
RETURN b._key AS name, COUNT(a) AS allies, SUM(r.strength) AS total_strength
GROUP BY b._key
ORDER BY total_strength DESC
LIMIT 10

-- Spatial
SELECT * FROM islands WHERE ST_DWithin(geometry, POINT(0.0 0.0), 500.0)
SELECT * FROM zones   WHERE ST_Contains(geometry, POINT(144.9671 -37.8183))

-- Vector
SELECT * FROM characters WHERE VECTOR_NEAR(embedding, [0.9, 0.1, 0.0], 5)

-- Full-text
SELECT * FROM papers WHERE BM25(abstract, 'neural network') > 0.3
ORDER BY BM25(abstract, 'neural network') DESC

-- Filters
WHERE bounty BETWEEN 1000000000 AND 4000000000
WHERE crew IN ('straw-hat', 'red-hair')
WHERE name ILIKE '%luffy%'
WHERE description IS NOT NULL
AND / OR / NOT
```

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

## Full Data Science Example — Research Network (Melbourne)

A researcher discovery system combining all four data models.

### Schema and data loading

```python
from sekejap import DB
import pandas as pd
import numpy as np
import json

db = DB("./research_db")

db.execute("""
    CREATE TABLE researchers (
        _key        TEXT PRIMARY KEY,
        name        TEXT,
        institution TEXT,
        field       TEXT,
        h_index     INTEGER,
        location    GEO,
        embedding   VECTOR
    )
""")
db.execute("""
    CREATE TABLE papers (
        _key      TEXT PRIMARY KEY,
        title     TEXT,
        abstract  TEXT,
        year      INTEGER,
        citations INTEGER
    )
""")

db.execute("CREATE INDEX ON researchers(institution) USING hash")
db.execute("CREATE INDEX ON researchers(h_index)     USING btree")
db.execute("CREATE INDEX ON researchers(location)    USING spatial")
db.execute("CREATE INDEX ON researchers(embedding)   USING hnsw")
db.execute("CREATE INDEX ON papers(abstract)         USING bm25")

# Load from CSV + numpy embeddings
df = pd.read_csv("researchers.csv")
embeddings = np.load("embeddings.npy")   # shape (n, 384)
df["location"] = df.apply(
    lambda r: json.dumps({"type": "Point", "coordinates": [r.lon, r.lat]}), axis=1
)
df["embedding"] = [e.tolist() for e in embeddings]

db.df.load_nodes(df, "researchers", id_col="researcher_id",
                 mapping={"researcher_id": "_key"})

db.df.load_edges(
    pd.read_csv("collaborations.csv"),
    source_col="researcher_id",
    target_col="collaborator_id",
    edge_type="collaborated_with",
    source_collection="researchers",
    target_collection="researchers",
    weight_col="num_papers",
)
```

### Spatial — who's at which university near Flinders Street?

```python
df = db.df.query("""
    SELECT * FROM researchers
    WHERE ST_DWithin(location, POINT(144.9671 -37.8183), 5.0)
      AND field = 'machine_learning'
      AND h_index >= 10
    ORDER BY h_index DESC
    LIMIT 20
""")
```

### Vector — semantically similar researchers

```python
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")
vec = model.encode("deep learning for climate science").tolist()

df = db.df.query(f"""
    SELECT * FROM researchers
    WHERE VECTOR_NEAR(embedding, {vec}, 10)
      AND ST_DWithin(location, POINT(144.9671 -37.8183), 20.0)
""")
```

### Graph — collaboration network

```python
# 2-hop collaboration network
hits = db.query("""
    MATCH (a:researchers)-[:collaborated_with*1..2]->(b:researchers)
    WHERE a._key = 'ali-hakim'
    RETURN b
""")

# Who are the most connected researchers?
hits = db.query("""
    MATCH (a:researchers)-[r:collaborated_with]->(b:researchers)
    RETURN b._key AS researcher, COUNT(a) AS allies, SUM(r.weight) AS joint_papers
    GROUP BY b._key
    ORDER BY joint_papers DESC
    LIMIT 10
""")

# Cross-field collaboration
hits = db.query("""
    MATCH (a:researchers)-[r:collaborated_with]->(b:researchers)
    RETURN a.field AS from_field, b.field AS to_field, COUNT(r) AS links
    GROUP BY a.field, b.field
    ORDER BY links DESC
""")
```

### BM25 — relevant papers

```python
df = db.df.query("""
    SELECT * FROM papers
    WHERE BM25(abstract, 'urban heat island Victoria') > 0.2
      AND year >= 2020
      AND citations >= 50
    ORDER BY BM25(abstract, 'urban heat island Victoria') DESC
""")
```

### Multi-modal — spatial + graph + vector in one workflow

```python
# "ML researchers near Melbourne CBD who collaborated with authors
#  of highly-cited papers published after 2021"

papers = db.df.query("SELECT * FROM papers WHERE year >= 2021 AND citations >= 50")

author_rows = []
for _, paper in papers.iterrows():
    hits = db.query(f"""
        MATCH (p:papers)<-[:authored]-(r:researchers)
        WHERE p._key = '{paper["_key"]}'
        RETURN r
    """)
    author_rows += [json.loads(h.payload) for h in hits if h.payload]

prolific = pd.DataFrame(author_rows).drop_duplicates("_key")

keys = prolific["_key"].tolist()
in_clause = ", ".join(f"'{k}'" for k in keys)

result = db.df.query(f"""
    SELECT * FROM researchers
    WHERE _key IN ({in_clause})
      AND ST_DWithin(location, POINT(144.9671 -37.8183), 10.0)
      AND field = 'machine_learning'
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

sekejap> CREATE TABLE islands (_key TEXT, name TEXT, geometry GEO);
sekejap> SELECT * FROM islands WHERE ST_DWithin(geometry, POINT(0.0 0.0), 500.0);
sekejap> .tables
sekejap> .schema islands
sekejap> .stats
sekejap> .help
```

## License

MIT
