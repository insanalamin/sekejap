# Life Knowledge Graph

## Workload Story

This suite represents a graph of:
- life problems
- habits
- food / drink
- organs
- conditions
- supporting text passages

Typical question:
- if a person repeatedly does A and B, what conditions or organs are likely affected?

## Dataset Shape

Entities:
- `problem`
- `habit`
- `food`
- `drink`
- `organ`
- `condition`
- `passage`
- `advice`

Relations:
- `related_to`
- `worsens`
- `affects`
- `linked_to_text`
- `recommended_for`
- `contraindicated_for`

Fields:
- `created_at`
- `title`
- `body`
- optional `embedding`

## Main Benchmark Cases

1. `concept_neighborhood_lookup`
2. `multi_hop_effect_chain`
3. `reverse_lookup_from_condition`
4. `graph_plus_fulltext_passage_search`
5. `weighted_graph_text_relevance`

## Fairness Notes

- graph is native in Sekejap and recursive in SQLite
- fulltext is natively comparable
- weighted relevance is approximate in SQLite

## Primary Optimization Goal

Graph plus explanatory retrieval must be strong and predictable.
