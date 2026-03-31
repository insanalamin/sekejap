# Graph Traversal Method

This benchmark isolates graph only.

## Dataset

- `10,000` nodes
- out-degree `3`
- deterministic edges:
  - node `mem_00000` links to `mem_00001`, `mem_00002`, `mem_00003`
  - node `mem_i` links to the next `3` ids modulo dataset size
- traversal depth: `5`

## Insert method

### Sekejap Atomic

- create collection schema directly with hash index on `id`
- batch-ingest all nodes with `nodes().ingest_raw(...)`
- batch-ingest all edges with `edges().ingest(...)`

### Sekejap SQL

- create collection via SQL DDL:
  - `CREATE COLLECTION memories (id TEXT PRIMARY KEY) WITH (hash_index = [id])`
- insert nodes row-by-row via SQL DML:
  - `INSERT INTO memories (id) VALUES ('mem_01234')`
- attach edges with `edges().ingest(...)`

Note:
- graph edge DDL/DML is not implemented in Sekejap SQL yet
- so the SQL lane uses SQL for collection creation and node inserts, but engine edge-ingest for relationships

### SQLite

- create `memories` table and `memory_edges` table
- create index on `memory_edges(from_id)`
- insert nodes and edges inside one transaction

## Query method

All three lanes start from the same source node id and perform a forward traversal to depth `5`.

### Sekejap Atomic

- start from `nodes().one("memories/<id>")`
- apply `.hops(5).forward("related_to")`
- project `id` only

### Sekejap SQL

- query:

```sql
SELECT id
FROM memories
TRAVERSE FORWARD related_to TO memories HOPS 5
WHERE id = 'mem_01234'
LIMIT 2000
```

### SQLite

- recursive CTE:

```sql
WITH RECURSIVE walk(id, depth) AS (
  SELECT ?1 AS id, 0 AS depth
  UNION ALL
  SELECT e.to_id, walk.depth + 1
  FROM memory_edges e
  JOIN walk ON e.from_id = walk.id
  WHERE walk.depth < ?2
)
SELECT DISTINCT id
FROM walk
LIMIT ?3
```

## Fairness notes

- query semantics are aligned:
  - same source
  - same edge type
  - same depth
  - same projected field
- insert semantics are not identical:
  - Atomic uses batch ingest
  - SQL uses row-by-row node inserts
  - SQLite uses transaction inserts
- this benchmark is mainly for graph query behavior, with insert shown separately
