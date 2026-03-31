# Python API Proposal

## Goal

Sekejap Python should support both:
- normal application code
- dataframe and notebook workflows

It should feel natural in Jupyter, but it should not force pandas on every user.

## Design Principles

1. normal DB API stays available
2. dataframe API is grouped under `db.df`
3. SQL is the recommended common-user path
4. Atomic API remains available for lower-level control
5. schema stays the source of truth for field types

## Top-Level Entry

Recommended constructors:

```python
import sekejap

db = sekejap.open("./data")
db = sekejap.create("./data")
```

Lower-level explicit constructor remains:

```python
db = sekejap.SekejapDB("./data", capacity=1_000_000)
```

Recommended semantics:
- `sekejap.open(path, capacity=...)`
  - open existing or create if missing
- `sekejap.create(path, capacity=...)`
  - explicit create/open intent

## Core DB API

This is the non-dataframe, normal Python API.

```python
db.query(input)
db.count(input)
db.explain(input)
db.mutate(input)
db.flush()
db.close()
```

Input styles:
- SQL
- SekejapQL / pipeline text
- JSON pipeline / JSON mutation

Recommendation:
- use SQL first for normal application code

## Atomic API

Lower-level builder and graph API remains available:

```python
db.nodes()
db.edges()
db.schema()
```

Examples:

```python
db.nodes().put_json('{"_id":"researchers/r1","name":"Alya"}')
db.edges().link("researchers/r1", "topics/t1", "works_on", 1.0)

hits = db.nodes().one("researchers/r1").forward("works_on").take(10).collect()
```

Recommendation:
- use Atomic when SQL is not expressive enough, or when lower-level control is needed

## DataFrame API

Dataframe integration should be grouped under:

```python
db.df
```

Recommended methods:

```python
db.df.create_collection(...)
db.df.load_nodes(...)
db.df.load_edges(...)
db.df.query(...)
db.df.explain(...)
```

This keeps pandas-specific behavior out of the core DB API.

## `db.df.create_collection`

Primary purpose:
- create a typed Sekejap collection from Python without forcing raw SQL strings

Recommended explicit form:

```python
db.df.create_collection(
    "researchers",
    fields={
        "id": "TEXT PRIMARY KEY",
        "name": "TEXT",
        "campus": "TEXT",
        "created_at": "TIMESTAMP",
        "geometry": "GEOMETRY",
        "embedding": "VECTOR(128)",
    },
    hash_index=["id", "campus"],
    range_index=["created_at"],
    spatial_index=["geometry"],
    vector_index=["embedding"],
    fulltext_index=["name"],
)
```

This should compile to `CREATE COLLECTION ...`.

## `db.df.load_nodes`

Primary purpose:
- load a pandas DataFrame into an existing typed Sekejap collection

Schema-first flow:

```python
db.df.load_nodes(
    df_researchers,
    collection="researchers",
    id_col="id",
)
```

With mapping:

```python
db.df.load_nodes(
    df_researchers,
    collection="researchers",
    id_col="researcher_id",
    mapping={
        "full_name": "name",
        "joined_at": "created_at",
        "coords": "geometry",
        "topic_vec": "embedding",
    },
    batch_size=1000,
)
```

Type rule:
- collection schema is the source of truth
- dataframe columns are mapped into schema fields
- pandas dtype inference is only a fallback convenience, not the truth

Accepted value shapes should include:
- `TIMESTAMP`: pandas datetime, Python datetime, ISO string
- `GEOMETRY`: GeoJSON dict or GeoJSON string
- `VECTOR(n)`: list, numpy array, or list-like object column
- `VAGUE_TIME`: dict or JSON string

## `db.df.load_edges`

Primary purpose:
- load edge rows from a pandas DataFrame

Fixed edge type:

```python
db.df.load_edges(
    df_edges,
    source_col="researcher_id",
    target_col="topic_id",
    edge_type="works_on",
    source_collection="researchers",
    target_collection="topics",
    weight_col="weight",
    batch_size=1000,
)
```

Per-row edge type:

```python
db.df.load_edges(
    df_edges,
    source_col="source",
    target_col="target",
    edge_type_col="edge_type",
    weight_col="weight",
    meta_col="meta",
)
```

## `db.df.query`

Primary purpose:
- run a Sekejap query and return a pandas DataFrame

Example:

```python
df = db.df.query("""
SELECT id, name
FROM researchers
WHERE VECTOR_NEAR(embedding, [0.12, 0.34, 0.56, 0.78], 20)
LIMIT 20
""")
```

Recommended options:

```python
df = db.df.query(
    "SELECT id, title FROM cases LIMIT 100",
    parse_json=True,
    index_col="id",
)
```

Behavior:
- execute normal Sekejap query
- convert result rows to pandas `DataFrame`
- optionally flatten selected payload fields

## `db.df.explain`

Primary purpose:
- make query plans notebook-friendly

Example:

```python
df_plan = db.df.explain("SELECT id FROM cases WHERE id = 'c1'")
```

Recommended output columns:
- step
- input_size
- output_size
- index_used
- time_us

## Recommended Usage Model

For most users:

```python
import sekejap

db = sekejap.open("./data")
```

Then choose one of these layers:

### 1. Normal app path

```python
rows = db.query("SELECT id, title FROM cases LIMIT 20")
```

### 2. Atomic path

```python
hits = db.nodes().collection("cases").take(20).collect()
```

### 3. Dataframe path

```python
df = db.df.query("SELECT id, title FROM cases LIMIT 20")
```

## Why This Shape

This gives Sekejap three strong Python identities at once:
- normal embedded DB for app code
- graph/vector/time/space engine for advanced control
- dataframe-native multimodel engine for Jupyter and AI/data science

That is the right path if Sekejap wants to become a top embedded multimodel database for Pythonic and notebook workflows.
