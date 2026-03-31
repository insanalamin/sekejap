# Causal Investigation

## Workload Story

This suite represents incident and root-cause investigation:
- an incident happens
- there are immediate causes
- there are upstream causes behind those causes
- evidence/news items connect to the incident graph

Typical question:
- starting from an incident, what is the root cause several hops upstream?

## Dataset Shape

Entities:
- `incident`
- `cause`
- `factor`
- `institution`
- `location`
- `evidence`

Victorian references:
- Princes Highway
- Geelong
- Ballarat
- Dandenong
- Shepparton
- Bendigo
- western Melbourne suburbs

Relations:
- `caused_by`
- `contributed_by`
- `reported_in`
- `occurred_at`
- `linked_to`
- `upstream_of`

Fields:
- `created_at`
- optional `remembered_time`
- `geometry`
- `title`
- `body`
- optional `embedding`

## Main Benchmark Cases

1. `one_hop_cause_lookup`
2. `five_hop_root_cause_trace`
3. `reverse_trace_from_effect`
4. `graph_trace_with_exact_time_constraint`
5. `graph_trace_with_spatial_constraint`
6. `graph_trace_with_text_evidence`
7. `weighted_root_cause_ranking`

## Fairness Notes

- graph traversal is native in Sekejap and recursive fallback in SQLite
- root-cause ranking is only approximately comparable in SQLite
- exact time is natively comparable
- spatial is approximate if SQLite uses scalar coordinate math

## Primary Optimization Goal

Graph traversal must be excellent, especially anchored multi-hop root-cause tracing.
