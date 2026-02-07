# Sekejap-DB

A **graph-first**, embedded multi-model database engine for Rust.

## Part 1: General Description

Sekejap-DB is a **graph-native database** where relationships are first-class citizens. Multi-model data (vectors, geo, text) attaches to graph nodes - the graph is the core, everything else enhances it.

### What makes it special?

- **Graph-First**: Built for relationship-heavy workloads (RCA, knowledge graphs, agentic AI)
- **Multi-Model Nodes**: Attach vectors, geo, and text to graph nodes
- **Embedded**: No server needed, runs in your Rust application
- **Causal Queries**: Native backward traversal for Root Cause Analysis
- **Fast**: Optimized for high-velocity writes and low-latency queries
- **MVCC**: Multi-Version Concurrency Control with soft deletes

### Graph-First Philosophy

```
┌─────────────────────────────────────────────────────────┐
│                   Sekejap-DB                            │
│                  Graph-First Design                     │
├─────────────────────────────────────────────────────────┤
│                                                         │
│   ┌─────────┐    causal_edge    ┌─────────┐            │
│   │  Node   │◄────────────────►│  Node   │            │
│   │ (vector)│     (0.85)        │  (geo)  │            │
│   └─────────┘                  └─────────┘            │
│         │                            │                │
│         │                            │                │
│    ┌────┴────┐                 ┌────┴────┐           │
│    │ Vectors │                 │ Geo data │           │
│    │(embeddings)│              │(Point/Polygon)│       │
│    └─────────┘                  └─────────┘            │
│                                                         │
│   → Graph is the CORE                                   │
│   → Vectors/Geo are ATTRIBUTES on nodes                 │
│   → Queries traverse RELATIONSHIPS                       │
└─────────────────────────────────────────────────────────┘
```

### Quick Example

```rust
use hsdl_sekejap::SekejapDB;

let mut db = SekejapDB::new("./data")?;

// Write a document with vector embedding
db.write_json(r#"{
    "_id": "news/flood-2026",
    "title": "Flood in Gedebage",
    "vectors": {
        "dense": {
            "model": "bge-m3",
            "dims": 1024,
            "data": [0.1, -0.2, 0.3, ...]
        }
    },
    "geo": {
        "center": {"type": "Point", "coordinates": [106.85, -6.88]}
    }
}"#)?;

// Find similar documents using vector search
let results = db.query()
    .vector_search(query_embedding, 10)
    .execute()?;

// Add causal relationship
db.add_edge("poverty", "crime-001", 0.7, "causal".to_string())?;

// Root cause analysis
let causes = db.traverse("crime-001", 5, 0.3)?;
```

---

## Part 2: Main Usage

### 2.1 Basic CRUD Operations

#### Write Data

```rust
use hsdl_sekejap::{SekejapDB, WriteOptions};

let mut db = SekejapDB::new("./data")?;

// Simple write (goes to Tier 1 staging, promoted later)
db.write("event-001", r#"{"title": "Theft", "severity": "high"}"#)?;

// Immediate write to Tier 2
db.write_with_options("event-002", data, WriteOptions {
    publish_now: true,
    ..Default::default()
})?;

// Batch write
db.write_many(vec![
    ("doc-1".to_string(), json1.to_string()),
    ("doc-2".to_string(), json2.to_string()),
])?;
```

#### Read Data

```rust
use hsdl_sekejap::{SekejapDB, ReadOptions};

// Read from Tier 2 only (validated data)
if let Some(event) = db.read("event-001")? {
    println!("Found: {}", event);
}

// Read including staged Tier 1 data
let event = db.read_with_options("event-001", ReadOptions {
    include_staged: true,
})?;
```

#### Delete Data

```rust
use hsdl_sekejap::{SekejapDB, DeleteOptions};

// Cascade delete (removes edges too)
db.delete("event-001")?;

// Keep edges for audit trail
db.delete_with_options("event-001", DeleteOptions {
    exclude_edges: true,
})?;
```

---

### 2.2 Multi-Model Data (Vectors, Geo, Text)

#### Adding Vectors

```rust
use hsdl_sekejap::{SekejapDB, WriteOptions};

// Option 1: WriteOptions with vector
let embedding = vec![0.1, -0.2, 0.3, 0.4, /* 1024 dims */];
db.write_with_options("doc-001", r#"{"title": "Document"}"#,
    WriteOptions {
        vector: Some(embedding),
        ..Default::default()
    })?;

// Option 2: JSON with vectors
db.write_json(r#"{
    "_id": "news/article-001",
    "title": "Breaking News",
    "vectors": {
        "dense": {
            "model": "bge-m3",
            "dims": 1024,
            "data": [0.1, -0.2, 0.3, ...]
        }
    }
}"#)?;
```

#### Adding Geo Data

```rust
use hsdl_sekejap::{SekejapDB, WriteOptions, Geometry, Polygon, Point};

// Point coordinates
db.write_with_options("jakarta", r#"{"title": "Jakarta"}"#,
    WriteOptions {
        latitude: -6.2088,
        longitude: 106.8456,
        ..Default::default()
    })?;

// Polygon geometry
let polygon = Geometry::Polygon(Polygon::new(vec![
    Point::new(106.8, -6.2),
    Point::new(106.9, -6.2),
    Point::new(106.9, -6.3),
    Point::new(106.8, -6.3),
    Point::new(106.8, -6.2),
]));

db.write_with_options("region-001", r#"{"title": "Central Region"}"#,
    WriteOptions {
        geometry: Some(polygon),
        ..Default::default()
    })?;

// Or use JSON
db.write_json(r#"{
    "_id": "places/gedebage-market",
    "geo": {
        "center": {"type": "Point", "coordinates": [106.85, -6.88]},
        "area": {"type": "Polygon", "coordinates": [[[106.8,-6.9], [107.0,-6.9], [107.0,-6.8], [106.8,-6.8], [106.8,-6.9]]]}
    }
}"#)?;
```

#### Querying Multi-Model Data

```rust
// Vector similarity search (requires "vector" feature)
let query_vec = vec![0.1, 0.2, 0.3, ...];
let similar = db.query()
    .vector_search(query_vec, 10)
    .execute()?;

// Spatial radius search (requires "spatial" feature)
let nearby = db.query()
    .spatial(-6.2088, 106.8456, 50.0)?
    .execute()?;

// Combined multi-model query
let results = db.query()
    .spatial(-6.2088, 106.8456, 50.0)?
    .limit(10)
    .execute()?;
```

---

### 2.3 Collections and Schema

#### Identity System

Sekejap-DB uses ArangoDB-style identity:

```rust
use hsdl_sekejap::types::EntityId;

// EntityId format: "collection/key"
let entity = EntityId::new("news", "flood-2026");
assert_eq!(entity.as_str(), "news/flood-2026");
assert_eq!(entity.collection(), "news");
assert_eq!(entity.key(), "flood-2026");

// Parse from string
let entity = EntityId::parse("places/gedebage-market")?;
```

#### Collections via JSON

Define collections and their schemas in JSON using the `define_collection()` API:

```rust
use hsdl_sekejap::SekejapDB;

let mut db = SekejapDB::new("./data")?;

// Define collections with their indexing schemas
db.define_collection(r#"{
    "news": {
        "hot_fields": {
            "vector": ["vectors.dense", "vectors.colbert"],
            "spatial": ["geo.area", "geo.center"],
            "fulltext": ["title", "content"]
        },
        "vectors": {
            "dense": { "model": "bge-m3", "dims": 1024, "index_hnsw": true },
            "colbert": { "model": "colbert-v2", "dims": 128, "index_hnsw": false }
        },
        "spatial": {
            "area": { "type": "Polygon", "index_rtree": true },
            "center": { "type": "Point", "index_rtree": true }
        }
    },
    "places": {
        "hot_fields": {
            "spatial": ["geo.boundary"],
            "fulltext": ["name", "description"]
        },
        "spatial": {
            "boundary": { "type": "Polygon", "index_rtree": true }
        }
    }
}"#)?;

// List registered collections
let collections = db.list_collections();
println!("Collections: {:?}", collections);

// Check if a collection exists
if db.has_collection("news") {
    let schema = db.get_collection_schema("news");
    println!("News schema: {:?}", schema);
}
```

#### Schema Definition via Rust Code

```rust
use hsdl_sekejap::types::{Collection, CollectionId, CollectionSchema, VectorSchema, SpatialSchema, GeoType, HotFields};

// Create collection
let mut collection = Collection::new(CollectionId::new("news"));

// Define schema programmatically
let mut schema = CollectionSchema::new();

// Add vector channels
{
    let vec_schema = schema.add_vector("dense".to_string(), "bge-m3".to_string(), 1024);
    vec_schema.index_hnsw = true;
}

{
    let vec_schema = schema.add_vector("colbert".to_string(), "colbert-v2".to_string(), 128);
    vec_schema.index_hnsw = false;
}

// Add spatial fields
let spatial_schema = schema.add_spatial("center".to_string(), GeoType::Point);
spatial_schema.index_rtree = true;

// Set hot fields for query optimization
let mut hot = HotFields::new();
hot.add_vector_field("vectors.dense");
hot.add_spatial_field("geo.center");
hot.add_fulltext_field("title");
hot.add_fulltext_field("content");

collection.set_schema(schema);
```

#### Schema Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Flex Mode** | No schema defined | Rapid prototyping, flexible data |
| **Schema Mode** | Full JSON/Rust schema | Production, deterministic indexing |

---

### 2.4 Causal Graph and Traversal

#### Adding Edges

```rust
use hsdl_sekejap::SekejapDB;

// Create edge with weight
db.add_edge("poverty", "crime-001", 0.8, "causal".to_string())?;

// Multiple edge types
db.add_edge("unemployment", "poverty", 0.85, "influences".to_string())?;
db.add_edge("economic-slump", "crime-001", 0.9, "causal".to_string())?;
db.add_edge("regulation", "economic-slump", 0.7, "affects".to_string())?;
```

#### Root Cause Analysis

```rust
// Backward BFS traversal from effect to causes
let results = db.traverse("crime-001", 5, 0.3)?;

println!("Found {} related events", results.path.len());
println!("Total evidence weight: {:.2}", results.total_weight);

for edge in &results.edges {
    println!("  {} -> {} (weight: {:.2})", 
        edge._from, edge._to, edge.weight);
}
```

---

### 2.5 Query Builder

```rust
use hsdl_sekejap::SekejapDB;

// Simple slug query
let results = db.query()
    .by_slug("crime-001")
    .execute()?;

// Multi-model query
let results = db.query()
    .spatial(-6.2, 106.8, 50.0)?
    .limit(10)
    .execute()?;

// With edge filter
let results = db.query()
    .has_edge_from("poverty", "causal".to_string())
    .limit(20)
    .execute()?;
```

---

## Part 3: Technical Architecture

### 3.1 Graph-First Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Sekejap-DB                                   │
│                    Graph-Native Design                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│   TIER 1: Ingestion Buffer (Writes)                                │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │  HashMap<slug_hash, NodeHeader>                             │   │
│   │  └─ Fast staging for high-velocity writes                   │   │
│   └─────────────────────────────────────────────────────────────┘   │
│                              │                                      │
│                              ▼                                      │
│   TIER 2: Serving Layer (Nodes)                                     │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │  head_index: HashMap<slug_hash, HeadPointer>               │   │
│   │  node_store: HashMap<(node_id, rev), NodeHeader>           │   │
│   │  blob_store: Large payload storage (JSON, vectors, geo)    │   │
│   │  └─ MVCC with revisions, tombstones                        │   │
│   └─────────────────────────────────────────────────────────────┘   │
│                              │                                      │
│                              ▼                                      │
│   TIER 3: Knowledge Graph (Edges) ⭐ CORE                            │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │  outgoing: HashMap<EntityId, Vec<WeightedEdge>>           │   │
│   │  incoming: HashMap<EntityId, Vec<EntityId>>              │   │
│   │  └─ Forward & reverse indexes for O(1) edge lookup       │   │
│   │                                                             │   │
│   │  CSR Sparse Matrix (optional):                             │   │
│   │  └─ 10-100x memory reduction for sparse graphs            │   │
│   │                                                             │   │
│   │  Bloom Filter (optional):                                  │   │
│   │  └─ Fast "edge exists?" checks, no false negatives        │   │
│   │                                                             │   │
│   │  Bitmap Traversal (optional):                              │   │
│   │  └─ O(1) set operations for BFS/DFS frontier             │   │
│   └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
│   ⭐ GRAPH IS THE CORE - Nodes and multi-model data enhance it      │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.2 Graph Query Excellence

#### Why Graph Queries Excel

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Graph Query Performance                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  EDGE LOOKUP: O(1) with HashMap                                    │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │  HashMap<EntityId, Vec<WeightedEdge>>                      │     │
│  │         │                                               │     │
│  │         │ Hash(entity_id)                              │     │
│  │         ▼                                               │     │
│  │  O(1) access to all outgoing/incoming edges            │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                                                                      │
│  BACKWARD TRAVERSAL: O(E) where E = edges traversed                │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │  traverse("crime", max_hops=5, threshold=0.3)            │     │
│  │         │                                                   │     │
│  │         ▼                                                   │     │
│  │  1. Find starting node (O(1))                              │     │
│  │  2. Get incoming edges (O(1) via incoming index)          │     │
│  │  3. Filter by weight_threshold (O(degree))                │     │
│  │  4. Queue unique predecessors (O(1) with HashSet)         │     │
│  │  5. Repeat until max_hops or empty queue                 │     │
│  │                                                              │     │
│  │  Total: O(total_edges_traversed)                           │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                                                                      │
│  EDGE FILTERS: Applied during traversal (no extra lookups)           │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │  - weight_threshold: Skip edges below threshold             │     │
│  │  - edge_type: Filter by "_type" (causal, influences...)    │     │
│  │  - time_window: Valid range [start, end]                   │     │
│  │  - decay: Effective weight with temporal decay             │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                                                                      │
│  CSR SPARSE OPTIMIZATION: 10-100x memory reduction                 │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │  Graph: 1M nodes, 10M edges (0.001% dense)                │     │
│  │  ├─ Adjacency List: ~800MB (each edge ~80 bytes)           │     │
│  │  └─ CSR Matrix: ~8MB (compressed)                         │     │
│  │                                                              │     │
│  │  Benefit: Fits in L2/L3 cache, faster iteration            │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

#### Traversal Flow Diagram

```
traverse(slug, max_hops=3, weight_threshold=0.3)
═══════════════════════════════════════════════════════════════════════

START: "crime-001"
    │
    ▼
LOOKUP HEAD INDEX (O(1))
    │
    ▼
GET NODE → EntityId("crime-001")
    │
    ▼
BACKWARD BFS ITERATION
    │
    ├── HOPS 0: ["crime-001"] (starting node)
    │
    ├── LOOKUP INCOMING INDEX (O(1))
    │   │
    │   └── incoming["crime-001"] → ["poverty", "economic-slump"]
    │
    ├── FILTER: weight >= 0.3?
    │   ├── poverty → crime-001 (weight: 0.8) ✓
    │   └── economic-slump → crime-001 (weight: 0.9) ✓
    │
    ├── ADD TO QUEUE: ["poverty", "economic-slump"]
    │   MARK VISITED: {"crime-001", "poverty", "economic-slump"}
    │
    ├── HOPS 1: "poverty"
    │   │
    │   └── incoming["poverty"] → ["unemployment"]
    │       unemployment → poverty (weight: 0.85) ✓
    │       ADD TO QUEUE: ["economic-slump", "unemployment"]
    │
    ├── HOPS 2: "economic-slump"
    │   │
    │   └── incoming["economic-slump"] → ["regulation"]
    │       regulation → economic-slump (weight: 0.7) ✓
    │       ADD TO QUEUE: ["unemployment", "regulation"]
    │
    ├── HOPS 3: "unemployment" (max_hops reached, stop)
    │
    ▼
RESULT:
  path: ["poverty", "economic-slump", "unemployment", "regulation"]
  edges: [4 weighted edges]
  total_weight: 3.25
═══════════════════════════════════════════════════════════════════════
```

### 3.3 MVCC (Multi-Version Concurrency Control)

```rust
// Each update creates a new revision
// Old versions are preserved for historical queries

// Version 0
db.write("doc-001", v1)?;  // rev = 0

// Version 1 (creates new revision)
db.write("doc-001", v2)?;  // rev = 1

// Version 2 (creates new revision)
db.write("doc-001", v3)?;  // rev = 2

// All versions are preserved
let v0 = storage.get_by_id(node_id, Some(0))?;
let v1 = storage.get_by_id(node_id, Some(1))?;
let current = storage.get_by_slug(slug_hash)?;  // rev = 2
```

### 3.4 Tombstones (Soft Deletes)

```rust
// Delete creates a tombstone (doesn't physically remove data)
db.delete("doc-001")?;

// Tombstone stores:
// - deleted_at: timestamp
// - reason: optional deletion reason

// get_by_slug returns None for deleted nodes
assert!(storage.get_by_slug(slug_hash).is_none());

// But historical versions still accessible
let old = storage.get_by_id(node_id, Some(0))?;
```

### 3.5 Key Data Structures

```
NodeHeader {
    node_id: u128,           // Unique identifier
    slug_hash: u64,           // Hashed slug for fast lookup
    rev: u64,                // Revision number (MVCC)
    payload_ptr: BlobPtr,    // Pointer to blob store
    vector_ptr: Option<BlobPtr>,  // Vector embedding
    deleted: bool,           // Tombstone flag
    tombstone: Option<Tombstone>,
    entity_id: Option<EntityId>,
}

WeightedEdge {
    _from: EntityId,         // Source entity
    _to: EntityId,           // Target entity
    weight: f32,             // Evidence strength (0-1)
    _type: String,           // User-defined edge type
    payload: Option<EdgePayload>,
}

EntityId {
    collection: CollectionId,  // e.g., "news"
    key: String,            // e.g., "flood-2026"
}
```

### 3.6 Feature Flags

| Feature | Description | Enables |
|---------|-------------|---------|
| `vector` | Vector similarity search | `.vector_search()`, HNSW index |
| `spatial` | Geo queries | `.spatial()`, R-tree index |
| `fulltext` | Full-text search | `.fulltext()`, Tantivy index |
| `all` | All features | `vector` + `spatial` + `fulltext` |

```bash
# Build with all features
cargo build --features all

# Build with specific features
cargo build --features "vector,spatial"
```

### 3.7 File Structure

```
src/
├── lib.rs              # Main API (SekejapDB struct)
├── types/
│   ├── mod.rs         # Type exports and options
│   ├── node.rs        # NodeHeader, NodePayload
│   ├── edge.rs        # WeightedEdge, EdgePayload
│   ├── blob.rs        # BlobStore for large payloads
│   ├── geometry.rs    # Point, Polygon, Polyline
│   ├── collection.rs  # EntityId, CollectionId
│   ├── schema.rs      # CollectionSchema, VectorSchema
│   └── ...
├── storage/
│   ├── single.rs      # MVCC storage (SingleStorage)
│   ├── ingestion.rs   # Tier 1 buffer
│   └── promote.rs     # Auto-promotion
├── graph/
│   ├── mod.rs         # CausalGraph
│   ├── concurrent.rs  # Thread-safe graph
│   └── ...
├── index/
│   ├── mod.rs         # SlugIndex, SpatialIndex
│   └── spatial.rs     # R-tree geo index
├── vectors/
│   ├── ops.rs         # Vector operations
│   └── index.rs       # Vector index
├── query.rs           # Query builder
├── atoms.rs           # Atomic operations
└── sekejapql.rs      # JSON query language
```

---

## Building and Testing

```bash
# Run tests
cargo test

# Build with all features
cargo build --features all

# Run specific example
cargo run --example custom_data

# Check for errors
cargo check --features all
```

---

## License

MIT
