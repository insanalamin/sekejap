# Sekejap SQL

Sekejap SQL is the main app-facing query surface for Sekejap.

Use [user-guide.md](user-guide.md) for the practical guide. This file is the short reference.

## Supported Statements

- `CREATE COLLECTION`
- `INSERT INTO`
- `SELECT`
- `TRAVERSE`
- `RELATE`
- `RELATE MANY`
- `UPDATE`
- `DELETE FROM`
- `UNRELATE`

## Supported Retrieval Domains

- graph
- exact time
- vague time
- spatial
- vector
- full-text
- scalar payload filters

## Planner Rules

- anchored graph goes first
- exact time goes before vague time
- weight is always last
- without an anchor, choose the sharpest seed among exact time, spatial, text, and vector

## Short Examples

Collection:

```sql
CREATE COLLECTION cases (
  id TEXT PRIMARY KEY,
  title TEXT,
  created_at TIMESTAMP
) WITH (
  hash_index = [id],
  range_index = [created_at]
);
```

CRUD:

```sql
INSERT INTO cases (id, title) VALUES ('c1', 'hello');
UPDATE cases SET title = 'updated' WHERE id = 'c1';
DELETE FROM cases WHERE id = 'c1';
```

Graph:

```sql
RELATE incidents/incident_00001 -> caused_by -> causes/wet_road_00001;
UNRELATE incidents/incident_00001 -> caused_by -> causes/wet_road_00001;
```

Traversal:

```sql
SELECT id
FROM incidents
TRAVERSE FORWARD caused_by TO causes HOPS 5
WHERE id = 'incident_00001'
LIMIT 20;
```

Spatial:

```sql
SELECT id
FROM researchers
WHERE ST_DWithin(geometry, POINT(144.9631 -37.8136), 25.0)
LIMIT 20;
```

Vector:

```sql
SELECT id
FROM researchers
WHERE VECTOR_NEAR(embedding, [0.12, 0.34, 0.56, 0.78], 20)
LIMIT 20;
```

Text:

```sql
SELECT id
FROM articles
WHERE MATCHING('climate AND housing')
LIMIT 20;
```
