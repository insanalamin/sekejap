# Spatial Ops

This suite isolates the spatial operations that matter most for Sekejap:

- centroid-backed distance
- `ST_DWithin`
- `ST_Within`
- `ST_Intersects`

Dataset:
- 5,000 point geometries around a Melbourne-area coordinate band
- rectangle polygon queries so SQLite can approximate with scalar `lat/lon` bounds

Notes:
- `centroid` itself is internal metadata, so it is benchmarked through centroid-backed query ops (`near`, `ST_DWithin`)
- SQLite is an approximation baseline here, not a first-class spatial engine
