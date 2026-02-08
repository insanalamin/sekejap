# Sekejap-DB API Reference

Sekejap-DB maintains **1:1 parity** between its Rust and Python interfaces. All logic, signatures, and behavior are identical unless otherwise noted.

---

## 1. Setup & Lifecycle

### Initialize Database
Opens or creates a Sekejap-DB instance at the specified path.
- **Python**: `db = sekejap.SekejapDB("./data")`
- **Rust**: `let db = SekejapDB::new(Path::new("./data"))?;`

### Close Database
Ensures all buffers are cleared and locks are released.
- **Python**: `db.close()` (or use `with sekejap.SekejapDB("./data") as db:`)
- **Rust**: `drop(db);`

---

## 2. Basic CRUD (Nodes)

### write(slug, json_data)
Writes a JSON string to a specific slug. Defaults to the `nodes/` collection.
- **Usage**: `db.write("user-1", '{"name": "Alice"}')`

### write_json(json_data)
Writes data where the ID is defined inside the JSON via `_id`. Required for custom collections.
- **Usage**: `db.write_json('{"_id": "events/crash-01", "type": "accident"}')`

### read(slug)
Retrieves the JSON string for a given slug.
- **Usage**: `data = db.read("events/crash-01")`

### update(slug, json_data)
Updates an existing node. Fails if the node doesn't exist.
- **Usage**: `db.update("user-1", '{"name": "Alice", "status": "active"}')`

### delete(slug)
Removes a node and its associated edges (cascade delete).
- **Usage**: `db.delete("user-1")`

### delete_with_options(slug, options)
Delete with control over edge behavior.
- **Usage**: `db.delete_with_options("user-1", sekejap.DeleteOptions(exclude_edges=True))`

---

## 3. Schema & Maintenance

### define_collection(json_schema)
Defines indices (Vector, Spatial, Text) for a collection. **Required for Hybrid Search.**
- **Usage**:
  ```python
  db.define_collection('{"events": {"hot_fields": {"vector": ["v"], "fulltext": ["t"]}}}')
  ```

### flush()
Manually promotes data from **Tier 1 (Buffer)** to **Tier 2 (Searchable)**.
- **Usage**: `count = db.flush()`

### backup(path) / restore(path)
Creates a JSON-line backup of all data or restores from one.
- **Usage**: `db.backup("./backup.json")` / `db.restore("./backup.json")`

---

## 4. Graph Operations

### add_edge(source, target, weight, edge_type)
Creates a directed relationship between two nodes.
- **Usage**: `db.add_edge("causes/rain", "events/flood", 0.9, "caused")`

### get_edges_from(slug)
Returns a list of all outgoing edges from a node.
- **Usage**: `edges = db.get_edges_from("causes/rain")`

### traverse_forward(slug, max_hops, min_weight, edge_type)
Traverses the graph to find effects (Target nodes). **Primary tool for Graph Joins.**
- **Usage**: `result = db.traverse_forward("causes/rain", 2, 0.5, "caused")`

### traverse(slug, max_hops, min_weight, edge_type)
Traverses the graph backward to find root causes (Source nodes). **Primary tool for RCA.**
- **Usage**: `result = db.traverse("events/flood", 5, 0.1, "caused")`

### update_edge(source, target, weight, edge_type)
Updates the weight or metadata of an existing edge.
- **Usage**: `db.update_edge("src", "dst", 1.0, "caused")`

### delete_edge(source, target, edge_type)
Removes a specific edge without deleting the nodes.
- **Usage**: `db.delete_edge("src", "dst", "caused")`

---

## 5. Multi-modal Search (Atomic)

*Note: These require `db.flush()` to have been called on the data.*

### search_vector(query_vector, k)
Finds the top `k` similar nodes using HNSW.
- **Usage**: `hits = db.search_vector([0.1, 0.2, ...], 10)`

### search_spatial(lat, lon, radius_km)
Finds nodes within a circular radius using R-Tree.
- **Usage**: `hits = db.search_spatial(-6.2, 106.8, 5.0)`

### search_text(query_string, limit)
Finds nodes matching a lexical query using Tantivy.
- **Usage**: `hits = db.search_text("severe accident", 20)`

---

## 6. Hybrid Query (The Builder)

The most powerful way to query. It performs **Index Intersection** across all models.

### query()
Starts a chainable query session.
- **Methods**: `.spatial()`, `.vector_search()`, `.fulltext()`, `.has_edge_from()`, `.limit()`, `.execute()`
- **Usage**:
  ```python
  results = db.query() \
      .has_edge_from("causes/rain", "caused") \
      .spatial(-6.2, 106.8, 5.0) \
      .fulltext("flood") \
      .execute()
  ```

---

## 7. Utilities

### haversine_distance(lat1, lon1, lat2, lon2)
Calculates distance in KM between two points.
- **Usage**: `dist = sekejap.haversine_distance(-6.1, 106.1, -6.2, 106.2)`

### cosine_similarity(v1, v2)
Calculates semantic similarity (0.0 to 1.0) between two vectors.
- **Usage**: `sim = sekejap.cosine_similarity([1, 0], [0, 1])`
