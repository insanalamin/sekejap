# Sekejap-DB API Reference

Sekejap-DB maintains high parity between its Rust and Python interfaces.

---

## 1. Setup & Lifecycle

### Initialize Database
- **Python**: `db = sekejap.SekejapDB(path, capacity=1_000_000)`
- **Rust**: `let db = SekejapDB::new(Path::new(path), 1_000_000)?;`

### Close Database
- **Python**: `db.close()` (or use context manager)
- **Rust**: `drop(db);`

---

## 2. Node Operations

### db.put(slug, json_data)
Writes a JSON string to a specific slug.
- **Python**: `db.put("user/1", '{"name": "Alice"}')`
- **Rust**: `db.nodes().put("user/1", '{"name": "Alice"}')?`

### db.get(slug)
Retrieves the JSON string for a given slug.
- **Python**: `data = db.get("user/1")`
- **Rust**: `let data = db.nodes().get("user/1");`

### db.remove(slug)
Removes a node.
- **Python**: `db.remove("user/1")`
- **Rust**: `db.nodes().remove("user/1")?`

### db.ingest_nodes(items)
Fast batch ingestion of nodes.
- **Python**: `db.ingest_nodes([("u/1", '{"n":"A"}'), ("u/2", '{"n":"B"}')])`
- **Rust**: `db.nodes().ingest(&[("u/1", '{"n":"A"}'), ("u/2", '{"n":"B"}')])?`

---

## 3. Edge Operations

### db.link(source, target, type, weight)
Creates a directed relationship.
- **Python**: `db.link("u/1", "u/2", "follows", 1.0)`
- **Rust**: `db.edges().link("u/1", "u/2", "follows", 1.0)?`

---

## 4. Query Pipeline (Set API)

Sekejap-DB uses a pipeline-based "Set" API for all queries.

### Entry Points
- `db.nodes().all()`: Start with all nodes.
- `db.nodes().one(slug)`: Start with a single node.
- `db.nodes().collection(name)`: Start with all nodes in a collection.

### Transforms
- `.forward(type)`: Move to nodes pointed to by current set.
- `.backward(type)`: Move to nodes pointing to current set.
- `.hops(n)`: Set depth for traversal.
- `.where_eq(field, value)`: Filter by exact field value.
- `.where_gt(field, value)`: Filter by numeric comparison.
- `.near(lat, lon, radius_km)`: Filter by spatial radius.
- `.similar(vector, k)`: Filter by vector similarity.

### Terminals
- `.collect()`: Return all matching hits.
- `.count()`: Return number of matches.
- `.sum(field)`: Aggregate sum of a field.
- `.avg(field)`: Aggregate average of a field.

---

## 5. Utilities

### haversine_distance(lat1, lon1, lat2, lon2)
Calculates distance in KM.

### cosine_similarity(v1, v2)
Calculates semantic similarity.