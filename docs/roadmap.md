## Execution Update (2026-02-22)

Completed now:

1. Phase 1 (Contract Hardening)
- Canonical query/mutate contract examples added in `README.md`.
- Wrapper surface clarified to canonical `query/query_count/mutate` in wrapper module docs.
- `define_collection_internal` now accepts both `hot_fields` and `hot` for stable contract parsing.

2. Phase 2 (Indexing Clarity)
- `describe()` upgraded with explicit four-pillar index metadata and per-collection index readiness.
- `describe_collection(name)` upgraded with collection-level graph/vector/spatial/fulltext + payload index health.
- Added health+behavior test: `tests/m10_index_metadata_health.rs`.

3. Benchmark (sibling repo)
- Added `sekejap-benchmark/benches/four_pillars.rs`.
- Added bench registration in `sekejap-benchmark/Cargo.toml`.
- Saved output snapshot in `sekejap-benchmark/benches/FOUR_PILLARS_RESULTS.md`.
# SeKejap Plan (SeKejap First, then ToraJu)

## Scope Lock
- Priority order: **Graph -> Vector -> Spatial -> Temporal -> Fulltext**.
- Keep atomic core as ground truth (`nodes/edges/schema`).
- Keep one unified high-level interface: `query` and `mutate`.
- No ToraJu implementation work before SeKejap contract + tests are stable.

## Current Status

### Completed
- Unified facade methods:
  - `db.query`, `db.query_count`, `db.explain`, `db.mutate`, `db.describe`, `db.describe_collection`.
- Backward compatibility aliases preserved:
  - `query_json`, `query_json_count`, `explain_json`, `mutate_json`, `SekejapQL` alias.
- Query compiler renamed and standardized:
  - `QueryCompiler` as canonical parser.
- Query contract expanded and implemented:
  - `spatial_within_bbox`
  - `spatial_intersects_bbox`
  - `spatial_within_polygon`
  - weighted `matching` (`title_weight`, `content_weight`, `limit`).
- Hit payload contract improved:
  - `score` propagated in query outputs.
- Fulltext adapter API upgraded for weighted search.
- CLI updated to unified commands:
  - `query ...;`, `mutate ...;`, `describe [collection];`.
- Python and browser wrappers updated to canonical names (aliases retained).
- Interface inventory added:
  - `SEKEJAP_INTERFACE_TABLE.md`.
- Tests added and green:
  - `tests/m8_upsert_canonical_semantics.rs`
  - `tests/m9_query_contract_extensions.rs`.

### Validated
- `cargo check --all-features` passes.
- `cargo test --features fulltext --test m9_query_contract_extensions -- --nocapture` passes.
- `cargo test upsert_ -- --nocapture` passes.

## Next (SeKejap-only)

### Phase 0: Temporal Index Foundation
- Add first-class vague-time support as a dedicated engine module:
  - `src/index/time_index.rs`
- Keep canonical app storage external/file-based; SeKejap remains the derived local engine.
- Do not reduce temporal support to a single timestamp field or only `where_between`.
- Preserve spatial radius as a first-class composable filter with temporal overlap.

### Phase 0.1: Temporal Data Contract
- Freeze canonical compiled temporal entry fields:
  - `start_year`
  - `end_year`
  - `expanded_start_year`
  - `expanded_end_year`
  - `month_mask`
  - `weekday_mask`
  - `day_of_month_mask`
  - `time_of_day_start`
  - `time_of_day_end`
  - `time_of_day_fuzzy_radius`
  - `recurrence_step_months`
  - `global_fuzziness`
- Freeze payload parsing contract for vague time input field (`time` / `_time`).
- Add contract examples in docs for:
  - exact time
  - fuzzy year range
  - month-only range
  - weekday + time-of-day recurrence
  - deep-history range

### Phase 0.2: Temporal Query Ops
- Add first-class query steps:
  - `time_intersects`
  - `time_within`
  - `time_near`
- Keep these composable with:
  - graph traversal
  - spatial radius queries
  - vector ranking
- Add `explain()` support so temporal steps appear in execution traces.

### Phase 0.3: Incremental Reindex Lifecycle
- On node write/update:
  - remove old temporal index entry
  - compile new temporal entry
  - register bucket memberships
- On node delete:
  - remove temporal index entry
  - remove bucket memberships
- Match current spatial/hash/range update semantics.

### Phase 0.4: Hybrid Query Validation
- Add tests for:
  - exact temporal overlap
  - fuzzy temporal overlap
  - recurring month/weekday/time-of-day patterns
  - deep-history ranges
  - time + spatial radius intersection
  - graph + time + spatial hybrid query
- Confirm temporal additions do not regress existing graph/vector/spatial/fulltext paths.

### Phase 1: Contract Hardening
- Freeze canonical JSON shapes for:
  - graph traversal ops
  - vector `similar`
  - spatial bbox/polygon
  - temporal overlap ops
  - fulltext `matching` with score.
- Add explicit examples for each op in README and wrappers docs.

### Phase 2: Indexing Clarity
- Document and expose index metadata in `describe` outputs per collection:
  - graph relation index state
  - vector index state (HNSW)
  - spatial index state (RTree)
  - temporal index state
  - fulltext index state.
- Add index-health checks in tests (existence + expected hit behavior).

### Phase 3: Mutate Facade Coverage
- Ensure every atomic write path is reachable from `mutate` JSON:
  - node upsert/remove
  - edge link/link_meta/unlink
  - schema define/update.
- Add roundtrip tests (`mutate` then `query`) across 4 pillars.

### Phase 4: Performance Guardrails
- Add benchmark scenarios:
  - graph traversal depth + filter
  - vector top-k
  - spatial bbox/polygon
  - temporal overlap lookup
  - hybrid graph + spatial + temporal
  - fulltext weighted ranking.
- Record baseline numbers; fail CI only on severe regressions.

### Phase 5: ToraJu Handoff Contract
- Export stable minimal contract for ToraJu adapters:
  - `x.n.tables.read/query/insert` mapping to SeKejap atomic/query APIs.
- Keep ToraJu node logic thin: orchestration in ToraJu, data semantics in SeKejap.

## Definition of Done (Before ToraJu Buildout)
- Canonical interfaces and examples are documented.
- 5-pillar query/mutate paths are tested end-to-end.
- `describe` clearly reports per-pillar index status.
- Wrappers and CLI use unified naming (`query`/`mutate`).
