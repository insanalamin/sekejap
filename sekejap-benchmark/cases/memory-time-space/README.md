# Memory Time Space

## Workload Story

This suite represents the original Sekejap-style workload:
- memories
- places
- place hierarchy
- vague remembered time
- exact creation time
- spatial lookup
- memory relations

The goal is to benchmark retrieval patterns that feel like:
- find memories around a place
- find memories in a vague period
- find memories under a place subtree
- find related memories near the same area

## Dataset Shape

Entities:
- `memory`
- `place`
- `place_level`
- `room`

Victorian references:
- Melbourne CBD
- Carlton
- Fitzroy
- Docklands
- Monash Clayton
- Woodside Building
- Geelong Waterfront
- Ballarat Station

Relations:
- `located_in`
- `part_of`
- `related_to`

Fields:
- `created_at`
- `remembered_time`
- `geometry`
- `title`
- `story`
- `place_id`
- optional `embedding`

## Main Benchmark Cases

1. `memories_near_place_in_vague_time`
2. `place_hierarchy_rollup`
3. `memory_anchor_to_related_nearby_memories`
4. `exact_created_at_recent_memories`
5. `hybrid_memory_search`

## Fairness Notes

- `exact_time` is natively comparable
- `spatial` is approximately comparable if SQLite uses scalar lat/lon math
- `vague_time` is only approximately comparable in SQLite
- place hierarchy and memory graph are graph-native in Sekejap, recursive-SQL fallback in SQLite

## Primary Optimization Goal

Make time and space excellent for the original Sekejap memory workload.
