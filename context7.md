# Sekejap DB

> Graph-first, embedded multi-model database engine for Rust and Python. Combines Graph + Vector (HNSW) + Spatial (R-Tree, PostGIS-par) + Full-Text (Tantivy/SeekStorm) in one unified query pipeline. Runs in-process with zero network overhead.

## Documentation

- [Skill Guide](docs/skill.md): Setup recipes, all query ops, common patterns, gotchas — best starting point for agents
- [API Reference](docs/api-reference.md): Complete reference with every method, SekejapQL ops, JSON pipeline, data conventions
- [Interface Table](docs/interface-table.md): Canonical API surface and status
- [Benchmark Results](docs/benchmark-results.md): Rust vs Python vs SQLite performance comparison
- [Roadmap](docs/roadmap.md): Development phases and execution notes

## LLM Context Files

- [llms.txt](llms.txt): Concise API reference for LLM/agent integration
- [llms-full.txt](llms-full.txt): Comprehensive API reference with all methods, formats, and examples

## Source Code

- [src/db.rs](src/db.rs): Core SekejapDB struct — init, query, mutate, describe, flush, backup/restore
- [src/set.rs](src/set.rs): Query pipeline execution — all ops (graph, vector, spatial, fulltext, filters, set algebra)
- [src/sekejapql.rs](src/sekejapql.rs): SekejapQL text query parser/compiler
- [src/types.rs](src/types.rs): Core types — Hit, Outcome, Trace, Step, Plan
- [src/hnsw/](src/hnsw/): HNSW vector index — graph, algo, distance (SIMD), storage
- [src/geometry.rs](src/geometry.rs): GeoJSON geometry handling and DE-9IM predicates
- [src/fulltext/](src/fulltext/): Pluggable fulltext adapters — Tantivy (default), SeekStorm
- [src/index/](src/index/): Hash and range indexes for hot fields

## Wrappers

- [wrappers/python/src/lib.rs](wrappers/python/src/lib.rs): Python PyO3 bindings
- [wrappers/browser/src/lib.rs](wrappers/browser/src/lib.rs): WebAssembly/WASM bindings

## CLI

- [skcli/src/main.rs](skcli/src/main.rs): Interactive REPL with SekejapQL, JSON queries, mutations, introspection

## Tests

- [tests/](tests/): 11 integration test modules covering graph, vector, spatial, fulltext, upsert semantics, geometry, and index health
