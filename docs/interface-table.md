# SeKejap Interface Table

## Public Surface (Canonical)

| keyword | status | description |
|---|---|---|
| `db.query(json)` | implemented | Unified JSON query pipeline execution. |
| `db.query_count(json)` | implemented | Same as `query`, returns count only. |
| `db.explain(json)` | implemented | Parses and returns compiled pipeline steps. |
| `db.mutate(json)` | implemented | Unified JSON mutation execution (`put`, `put_json`, `link`, `link_meta`, `remove`, `unlink`). |
| `db.describe()` | implemented | Global runtime/index/collection summary (graph/vector/spatial/fulltext + hash/range indexes). |
| `db.describe_collection(name)` | implemented | Collection-specific schema + count + index metadata. |

## Atomic Core (Ground Truth)

| keyword | status | description |
|---|---|---|
| `db.nodes().put(slug, json)` | implemented | Upsert by explicit slug. |
| `db.nodes().put_json(json)` | implemented | Upsert by `_id`/`_collection`+`_key` or edge payload. |
| `db.nodes().get(slug)` | implemented | Read current canonical node JSON. |
| `db.nodes().remove(slug)` | implemented | Tombstone node and remove canonical slug mapping. |
| `db.edges().link(source, target, type, weight)` | implemented | Create edge relation. |
| `db.edges().link_meta(...)` | implemented | Create edge with metadata (inline/blob). |
| `db.edges().unlink(source, target, type)` | implemented | Tombstone edge relation. |
| `db.schema().define(name, json)` | implemented | Define collection hot fields + index intent. |
| `db.schema().count(name)` | implemented | O(1) count by collection bitmap. |

## Query Pipeline Ops (Graph -> Vector -> Spatial -> Fulltext)

| keyword | status | description |
|---|---|---|
| `one`, `many`, `collection`, `all` | implemented | Graph-first starters. |
| `forward`, `backward`, `forward_parallel`, `backward_parallel`, `hops`, `roots`, `leaves` | implemented | Graph traversal and topology filters. |
| `similar` | implemented | Vector HNSW top-k retrieval. |
| `near` | implemented | Spatial radius query (RTree). |
| `spatial_within_bbox` | implemented | Spatial bbox filter/query. |
| `spatial_intersects_bbox` | implemented | Spatial bbox intersection semantics for point datasets. |
| `spatial_within_polygon` | implemented | Polygon containment for point coordinates. |
| `matching` | implemented | Fulltext search with `limit`, `title_weight`, `content_weight`, and score propagation. |
| `where_eq`, `where_between`, `where_gt`, `where_lt`, `where_gte`, `where_lte`, `where_in` | implemented | Payload and index-based predicates. |
| `intersect`, `union`, `subtract` | implemented | Set algebra. |
| `sort`, `skip`, `select`, `take` | implemented | Result shaping/pagination. |

## Compatibility Layer

| keyword | status | description |
|---|---|---|
| `query_json`, `query_json_count`, `explain_json`, `mutate_json` | compatibility | Backward aliases to unified `query/query_count/explain/mutate`. |
| `SekejapQL` type alias | compatibility | Backward type alias to `QueryCompiler`. |
