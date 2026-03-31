# Sekejap Benchmark

This benchmark is split into two main folders only:

- `cases/`
- `techniques/`

`cases/` is the main benchmark surface. It follows real workload families.
`techniques/` is for focused tuning and diagnostics such as parser or spatial operator isolation.

## Cases

- `cases/memory-time-space/`
  - memories, places, exact time, vague time, and related-memory retrieval
- `cases/causal-investigation/`
  - anchored graph traversal, root-cause tracing, graph + time, graph + text
- `cases/life-knowledge-graph/`
  - habits, conditions, supporting texts, explanatory graph retrieval
- `cases/research-network/`
  - researchers, institutions, topics, campuses, vector + spatial + graph
- `cases/music-discovery/`
  - artists, songs, collections, venues, graph discovery
- `cases/learning-paths/`
  - concepts, prerequisites, course/path reasoning

## Techniques

- `techniques/graph-traversal/`
  - isolated graph traversal tuning
- `techniques/parser/`
  - parser-only benchmarking and parser strategy tuning
- `techniques/spatial-ops/`
  - isolated spatial operator benchmarking

## Lane Definitions

Each benchmark case should compare:

- `Sekejap Atomic`
- `Sekejap SQL`
- `SQLite`

Each result should also state whether the comparison is:

- `Native Comparable`
- `Approximate Comparable`
- `Sekejap Native Only`

## Policy

- Stabilize the SQL API first.
- Build workload benchmarks under `cases/`.
- Use `techniques/` only to isolate and tune a bottleneck.
- Tune one workload at a time instead of optimizing in the abstract.
