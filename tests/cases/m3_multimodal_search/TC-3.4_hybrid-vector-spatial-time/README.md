# TC-3.4: Hybrid Query (Vector + Spatial + Time)

Goal: Apply time filter, then spatial, then vector ranking.
Given: Nodes with vectors, coordinates, and timestamps.
When: Querying with V, radius R, time window T.
Then: Return nodes within time and spatial constraints, ranked by vector similarity.

Preconditions:
- Vector and spatial indexes enabled.

Steps:
1. Insert nodes across time and space with vectors.
2. Query with time window, radius 10km, k=5.

Expected:
- Results are within time window.
- Results are within radius.
- Order is by vector similarity.
