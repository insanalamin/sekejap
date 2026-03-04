# Sekejap Benchmark Results (10k Records, Apple Silicon)

## Combined Results — Rust vs Python

| Scenario | Operation | SQLite (Rust) | Sekejap (Rust) | SQLite (Python) | Sekejap (Python) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1. Simple** | INSERTION SIMPLE | 0.0128s | 0.1948s | 0.9587s | 1.1074s |
|  | RETRIEVAL SIMPLE | 0.0034s | 0.0019s | 0.0080s | 0.0021s |
| **2. Vector** | INSERTION WITH VECTOR INDEX | 0.0466s | 0.4415s | 0.8626s | 2.3149s |
|  | RETRIEVAL VECTOR | 0.0093s | 0.0003s | 0.0245s | 0.0002s |
| **3. Spatial** | INSERTION WITH SPATIAL INDEX | 0.0050s | 0.1948s | 0.0225s | 0.5631s |
|  | RETRIEVAL POINT DISTANCE | 0.0003s | 0.0005s | 0.0001s | 0.0001s |
| **4. V + S** | INSERTION WITH VECTOR AND SPATIAL | 0.0516s | 0.4415s | 0.8851s | 1.4390s |
|  | RETRIEVAL VECTOR AND SPATIAL | 0.0053s | 0.0017s | 0.0001s | 0.0004s |
| **5. V + F** | INSERTION WITH VECTOR AND FULLTEXT | 0.0806s | 0.4415s | 0.8976s | 1.1601s |
|  | RETRIEVAL OF TEXT WITH VECTOR | 0.0037s | 0.0000s | 0.0003s | 0.0006s |
| **6. Graph** | MULTIPLE TRAVERSAL (100x 3-HOP) | 0.0085s | 0.0011s | 0.0133s | 0.0012s |

## Key Takeaways

| Operation | Speedup (Rust) | Notes |
|---|---|---|
| Vector retrieval (k-NN) | **31x** faster | HNSW vs brute-force blob scan |
| Graph traversal (100x 3-hop) | **7.7x** faster | Native edge list vs recursive CTE |
| Simple retrieval (1k lookups) | **1.8x** faster | Hash lookup vs B-Tree |
| V+S retrieval | **3.1x** faster | HNSW + R-Tree vs brute-force + B-Tree |
| V+F retrieval | **instant** | HNSW + Tantivy vs brute-force + FTS5 |

## Methodology

- **10,000 records** with 128-dim vectors, lat/lon coordinates, and text fields
- **30,000 edges** for graph traversal scenarios
- SQLite has **no native vector or spatial index** — vector retrieval stores BLOBs, spatial uses B-Tree on (lat, lon)
- Sekejap builds real **HNSW + R-Tree + Tantivy** indexes
- Rust benchmark: `cargo run --example benchmark --features fulltext --release`
- Python benchmark: `sekejap-benchmark/benchmark.py`
