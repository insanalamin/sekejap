# TC-3.5: Temporal-Spatial Bucketing Optimization

Goal: Ensure hybrid queries meet latency target with bucketing.
Given: Millions of nodes across time and space.
When: Executing hybrid query with bucket skip optimization.
Then: Latency stays below 50ms.

Preconditions:
- Temporal and spatial buckets enabled.
- Benchmark dataset seeded (>=1M nodes).

Steps:
1. Run hybrid query with bucket pruning enabled.
2. Measure p95 latency over 100 runs.

Expected:
- p95 latency < 50ms.
- Bucketing reduces scanned candidate set by >90%.
