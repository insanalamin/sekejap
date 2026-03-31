# Research Network

## Workload Story

This suite represents a research discovery workload:
- researchers
- institutions
- campuses
- publications
- topics
- location

Typical questions:
- who in Victoria works on topic X?
- which nearby campuses have researchers in related topics?

## Dataset Shape

Entities:
- `researcher`
- `institution`
- `campus`
- `publication`
- `topic`
- `project`

Victorian references:
- University of Melbourne
- Monash University
- RMIT
- Deakin
- La Trobe
- Swinburne
- Melbourne
- Geelong
- Ballarat
- Bendigo

Relations:
- `affiliated_with`
- `works_on`
- `collaborates_with`
- `published`
- `located_at`

Fields:
- `created_at`
- `geometry`
- `title`
- `abstract`
- `embedding`

## Implemented Benchmark Cases

1. `insert_research_network_2000`
2. `topic_similarity_search_count_only`
3. `nearby_campus_topic_lookup_count_only`
4. `collaboration_neighborhood_expansion`
5. `hybrid_vector_spatial_graph_search_count_only`

Runner:
- `cargo run --bin research_network`

Result:
- `sekejap-benchmark/cases/research-network/RESULT.md`

## Fairness Notes

- vector is approximate in SQLite unless brute-force fallback is used
- spatial is approximate if SQLite uses scalar coordinate math
- graph is native in Sekejap and recursive in SQLite

## Primary Optimization Goal

Vector + spatial + graph should work together without planner waste.
