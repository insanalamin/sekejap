# Learning Paths

## Workload Story

This suite represents concept dependency chains and recommended learning routes.

Typical questions:
- what prerequisites must I master before topic X?
- what path best prepares someone for advanced topic Y?

## Dataset Shape

Entities:
- `concept`
- `course`
- `lesson`
- `assessment`
- `path`

Examples:
- calculus
- linear algebra
- statistics
- machine learning
- deep learning
- reinforcement learning

Relations:
- `prerequisite_for`
- `teaches`
- `assesses`
- `belongs_to_path`

Fields:
- `created_at`
- `title`
- `description`
- optional `embedding`

## Main Benchmark Cases

1. `prerequisite_chain_traversal`
2. `reverse_lookup_for_target_topic`
3. `five_hop_competency_path`
4. `concept_similarity_search`
5. `weighted_recommended_path`

## Fairness Notes

- graph is native in Sekejap and recursive in SQLite
- vector is approximate in SQLite unless brute-force fallback is used
- weighted recommendation is approximate in SQLite

## Primary Optimization Goal

Graph-first path discovery and recommendation should be predictable and easy to optimize.
