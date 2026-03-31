# User Guide

## What Sekejap Is For

Sekejap is for embedded workloads where one query may need several of these together:
- graph
- time
- space
- vector
- text

Typical examples:
- root-cause tracing
- memory retrieval around time and place
- researcher/topic search
- music and article recommendation
- prerequisite and knowledge graph navigation

## Open a Database

```rust
use sekejap::SekejapDB;
use std::path::Path;

let db = SekejapDB::new(Path::new("./data"), 1_000_000)?;
```

`capacity` is the planned node capacity for the mmap arenas.

Call `flush()` when you want durable persistence on disk:

```rust
db.flush()?;
```

## Input Styles

Sekejap accepts three input styles:
- SQL
- SekejapQL / pipeline text
- JSON pipeline / JSON mutation

For production app usage, use SQL first.

Recommendation by interface:
- Rust: SQL first, Atomic when you need lower-level builder control
- Python: SQL first
- CLI: SQL first

## Collections

Create a collection with indexed attributes:

```sql
CREATE COLLECTION memories (
  id TEXT PRIMARY KEY,
  title TEXT,
  story TEXT,
  created_at TIMESTAMP,
  remembered_time VAGUE_TIME,
  geometry GEOMETRY,
  embedding VECTOR(128)
) WITH (
  hash_index = [id],
  range_index = [created_at],
  temporal_index = [remembered_time],
  spatial_index = [geometry],
  vector_index = [embedding],
  fulltext_index = [title, story]
);
```

Index intent:
- `hash_index`: equality
- `range_index`: exact scalar ranges, including exact time
- `temporal_index`: vague time
- `spatial_index`: geometry / centroid-backed spatial search
- `vector_index`: ANN vector retrieval
- `fulltext_index`: Tantivy-backed `MATCHING(...)`

## Python

The Python package is built from `wrappers/python` and published as `sekejap`.

SQL-first example:

```python
import json
import sekejap

db = sekejap.SekejapDB("./data", capacity=1_000_000)

db.mutate("""
CREATE COLLECTION researchers (
  id TEXT PRIMARY KEY,
  name TEXT,
  geometry GEOMETRY,
  embedding VECTOR(128)
) WITH (
  hash_index = [id],
  spatial_index = [geometry],
  vector_index = [embedding]
)
""")

rows = json.loads(db.query("""
SELECT id
FROM researchers
WHERE VECTOR_NEAR(embedding, [0.12, 0.34, 0.56, 0.78], 20)
LIMIT 20
"""))
```

Atomic example:

```python
import json
import sekejap

db = sekejap.SekejapDB("./atomic-data", capacity=10000)
db.nodes().put_json(json.dumps({"_id": "researchers/r1", "name": "Alya"}))
db.nodes().put_json(json.dumps({"_id": "topics/t1", "title": "Electric Vehicles"}))
db.edges().link("researchers/r1", "topics/t1", "works_on", 1.0)

hits = db.nodes().one("researchers/r1").forward("works_on").take(10).collect()
```

Current wrapper state:
- SQL surface is available through `query()`, `count()`, `explain()`, `mutate()`
- Atomic surface is available through `nodes()`, `edges()`, `schema()`
- Python packaging and local wheel smoke are verified
- Python CLI entry point is available through `sekejap` and `python -m sekejap`

## CRUD

Insert:

```sql
INSERT INTO cases (id, title, created_at)
VALUES ('incident_00001', 'Crash near Geelong', TIMESTAMP '2024-06-15 09:30:00');
```

Update:

```sql
UPDATE cases
SET title = 'Updated crash near Geelong'
WHERE id = 'incident_00001';
```

Delete:

```sql
DELETE FROM cases
WHERE id = 'incident_00001';
```

## Graph Writes

Create a relation:

```sql
RELATE incidents/incident_00001 -> caused_by -> causes/wet_road_00001;
```

With weight and metadata:

```sql
RELATE incidents/incident_00001 -> caused_by -> causes/wet_road_00001
WEIGHT 0.92
META {"source":"regional_report","confidence":0.82};
```

Batch relation write:

```sql
RELATE MANY (
  incidents/incident_00001 -> caused_by -> causes/wet_road_00001,
  causes/wet_road_00001 -> caused_by -> causes/drainage_00001
);
```

Delete a relation:

```sql
UNRELATE incidents/incident_00001 -> caused_by -> causes/wet_road_00001;
```

Keywords:
- `RELATE`, `RELATE MANY`, `UNRELATE`, `TRAVERSE`, `FORWARD`, `BACKWARD`, `HOPS`, `WEIGHT`, `META` are SQL keywords in Sekejap
- `caused_by`, `collaborates_with`, `related_to` are user-defined edge type names

## Graph Reads

Anchored traversal:

```sql
SELECT id
FROM incidents
TRAVERSE FORWARD caused_by TO causes HOPS 5
WHERE id = 'incident_00001'
LIMIT 20;
```

Reverse traversal:

```sql
SELECT id
FROM causes
TRAVERSE BACKWARD caused_by TO incidents HOPS 3
WHERE id = 'wet_road_00001';
```

## Exact Time

Exact time is first-class and should be used for precise machine time.

```sql
SELECT id
FROM memories
WHERE created_at >= TIMESTAMP '2024-01-10 00:00:00'
  AND created_at <= TIMESTAMP '2024-01-20 23:59:59'
LIMIT 100;
```

Internally this lowers to the canonical exact backing field. You should query the user field such as `created_at`, not hidden scalar fields.

## Vague Time

Vague time is for uncertain human memory and fuzzy temporal constraints.

Typical use:

```sql
SELECT id
FROM memories
WHERE VAGUE_TIME_INTERSECTS(remembered_time, START_YEAR 2019, END_YEAR 2020)
LIMIT 100;
```

Vague time is still the main optimization area. It works, but it is not yet as strong as graph, exact time, vector, or point-centric spatial search.

## Spatial

Distance:

```sql
SELECT id
FROM researchers
WHERE ST_DWithin(geometry, POINT(144.9631 -37.8136), 25.0)
LIMIT 20;
```

Within:

```sql
SELECT id
FROM zones
WHERE ST_Within(
  geometry,
  POLYGON((144.95 -37.82, 144.98 -37.82, 144.98 -37.80, 144.95 -37.80, 144.95 -37.82))
);
```

Intersects:

```sql
SELECT id
FROM zones
WHERE ST_Intersects(
  geometry,
  POLYGON((144.95 -37.82, 144.98 -37.82, 144.98 -37.80, 144.95 -37.80, 144.95 -37.82))
);
```

Current state:
- point-centric spatial filtering is strong
- rectangle fast paths are in place
- richer spatial and polygon-heavy workloads are supported, but the main proven area is point-driven search

## Vector

Vector similarity:

```sql
SELECT id
FROM researchers
WHERE VECTOR_NEAR(embedding, [0.12, 0.34, 0.56, 0.78], 20)
LIMIT 20;
```

Current state:
- vector retrieval is one of the strongest technique areas
- SQLite fallback is only a brute-force baseline in benchmarks, not an equivalent vector engine

## Text

Full-text:

```sql
SELECT id
FROM articles
WHERE MATCHING('climate AND housing')
LIMIT 20;
```

Tantivy query syntax is available through `MATCHING(...)`, including:
- `AND`
- `OR`
- quoted phrases
- `-term` exclusion

Examples:

```sql
SELECT id FROM articles WHERE MATCHING('climate OR housing') LIMIT 20;
SELECT id FROM articles WHERE MATCHING('"public transport"') LIMIT 20;
SELECT id FROM articles WHERE MATCHING('climate -coal') LIMIT 20;
```

`LIKE` and `ILIKE` also exist:

```sql
SELECT id
FROM articles
WHERE title ILIKE '%transport%';
```

Current state:
- `MATCHING(...)` is the preferred text retrieval path
- `ILIKE` is improved but still fundamentally a scan-style path compared with full-text search

## Hybrid Retrieval

A hybrid query may combine graph, exact time, vague time, space, vector, and text.

Example:

```sql
SELECT id
FROM researchers
TRAVERSE FORWARD collaborates_with TO researchers HOPS 2
WHERE id = 'researcher_00000'
  AND VECTOR_NEAR(embedding, [0.12, 0.34, 0.56, 0.78], 100)
  AND ST_DWithin(geometry, POINT(144.9631 -37.8136), 40.0)
LIMIT 20;
```

Planner philosophy:
- anchored graph goes first
- exact time goes before vague time
- weight is always last
- otherwise choose the sharpest seed among exact time, spatial, text, and vector

Root-cause style example:

```sql
SELECT id
FROM cases
TRAVERSE FORWARD caused_by TO causes HOPS 5
WHERE id = 'incident_00001'
  AND MATCHING('wet road OR drainage OR education')
LIMIT 20;
```

## Aggregation

Basic aggregation is part of the SQL surface:
- `GROUP BY`
- `COUNT`
- `SUM`
- `AVG`
- `MIN`
- `MAX`

Example:

```sql
SELECT course.title, AVG(answer.grade)
FROM programmes
TRAVERSE FORWARD has_course TO courses course
TRAVERSE FORWARD has_classroom TO classrooms classroom
TRAVERSE FORWARD has_student TO students student
TRAVERSE FORWARD submitted TO assessment_answers answer
WHERE id = 'programme_001'
GROUP BY course.title;
```

The first implementation model is query first, aggregate after retrieval.

## CLI

The CLI binary is `sekejap`.

Open the REPL:

```bash
cargo run -p sekejap-cli -- ./data
```

One-shot:

```bash
sekejap ./data "SELECT id FROM researchers LIMIT 10;"
```

Useful internal commands:
- `.help`
- `.tables`
- `.describe`
- `.describe <collection>`
- `.flush`

## Rust API

Main methods:

```rust
db.query(input)?;
db.count(input)?;
db.explain(input)?;
db.mutate(input)?;
db.flush()?;
```

Lower-level Rust-only builders:

```rust
db.nodes().collection("researchers").take(10).collect()?;
db.edges().link("researchers/a", "researchers/b", "collaborates_with", 1.0)?;
db.schema().define("researchers", schema_json)?;
```

## Production Notes

- collection schemas now persist across reopen
- SQL create/write/query survives process restart
- call `flush()` at controlled durability points
- use `--features fulltext` for Tantivy-backed `MATCHING(...)`
- the current optimization focus after shipping is vague time and vague-time-heavy hybrids
