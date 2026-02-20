# TC-3.2: Spatial Radius Search

Goal: Return nodes within a radius.
Given: Nodes with coordinates around a city center.
When: Querying with radius 5km.
Then: Return only nodes within radius.

Preconditions:
- Spatial index enabled.

Steps:
1. Insert nodes at 1km, 3km, and 8km distances.
2. Query radius 5km.

Expected:
- Nodes at 1km and 3km returned.
- Node at 8km excluded.
