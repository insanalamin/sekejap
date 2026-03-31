# Sekejap

Sekejap is an embedded, graph-first multimodel database engine for workloads that combine graph, time, space, vector, and text.

Built for cases like:
- root-cause analysis
- hybrid retrieval and graph-aware RAG
- memory retrieval around time and place
- researcher, article, music, and knowledge graph discovery

## Hello World: Root Cause Analysis

Rust:

```rust
use sekejap::SekejapDB;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = SekejapDB::new(Path::new("./data"), 1_000_000)?;

    let rows = db.query(
        "SELECT id
         FROM cases
         TRAVERSE FORWARD caused_by TO causes HOPS 5
         WHERE id = 'incident_00001'
           AND MATCHING('wet road OR drainage OR education')
         LIMIT 20",
    )?;

    println!("hits = {}", rows.data.len());
    Ok(())
}
```

Python:

```python
import json
import sekejap

db = sekejap.SekejapDB("./data", capacity=1_000_000)

rows = json.loads(db.query("""
SELECT id
FROM cases
TRAVERSE FORWARD caused_by TO causes HOPS 5
WHERE id = 'incident_00001'
  AND MATCHING('wet road OR drainage OR education')
LIMIT 20
"""))

print(len(rows))
```

## Hello World: Hybrid Retrieval

Rust:

```rust
let rows = db.query(
    "SELECT id
     FROM researchers
     TRAVERSE FORWARD collaborates_with TO researchers HOPS 2
     WHERE id = 'researcher_00000'
       AND VECTOR_NEAR(embedding, [0.12, 0.34, 0.56, 0.78], 100)
       AND ST_DWithin(geometry, POINT(144.9631 -37.8136), 40.0)
     LIMIT 20",
)?;
```

Python:

```python
rows = json.loads(db.query("""
SELECT id
FROM researchers
TRAVERSE FORWARD collaborates_with TO researchers HOPS 2
WHERE id = 'researcher_00000'
  AND VECTOR_NEAR(embedding, [0.12, 0.34, 0.56, 0.78], 100)
  AND ST_DWithin(geometry, POINT(144.9631 -37.8136), 40.0)
LIMIT 20
"""))
```

## Available As

- Rust library
- Rust CLI
- Python library
- Python bindings with both SQL and Atomic interfaces

Common-user recommendation:
- use SQL first

Lower-level control:
- use Atomic / fluent builder when you need it

## Installation

### Rust

Library:

```bash
cargo add sekejap
```

CLI:

```bash
cargo install sekejap-cli
```

Then run:

```bash
sekejap
```

### Python

Library and CLI:

```bash
pip install sekejap
```

Then use either:

```bash
sekejap --help
```

or:

```python
import sekejap
```

### npm

Planned later for CLI distribution.

## Quick Usage

Rust main API:

- `SekejapDB::new(path, capacity)`
- `db.query(input)`
- `db.count(input)`
- `db.explain(input)`
- `db.mutate(input)`
- `db.flush()`
- `db.nodes()`
- `db.edges()`
- `db.schema()`

Python main API:

- `sekejap.SekejapDB(path, capacity=...)`
- `db.query(input)`
- `db.count(input)`
- `db.explain(input)`
- `db.mutate(input)`
- `db.flush()`
- `db.nodes()`
- `db.edges()`
- `db.schema()`

Input styles:
- SQL
- SekejapQL / pipeline text
- JSON pipeline and JSON mutation payloads

Recommended usage:
- Rust app code: SQL first, Atomic when lower-level control is needed
- Python app code: SQL first
- CLI: SQL first

## Current Status

Rust library is ready for production testing for the core surface:
- `CREATE COLLECTION`
- `INSERT INTO`
- `SELECT`
- `TRAVERSE`
- `RELATE`
- `RELATE MANY`
- `UPDATE`
- `DELETE FROM`
- `UNRELATE`

Collection schemas now persist across reopen, so SQL create/write/query survives process restarts.

Current strongest benchmark areas:
- anchored graph traversal
- exact-time filtering
- vector retrieval
- point-centric spatial filtering

Current weakest area:
- vague time and vague-time-heavy hybrid planning

## Docs

- [docs/user-guide.md](docs/user-guide.md)
- [docs/sekejap-sql.md](docs/sekejap-sql.md)

## License

MIT
