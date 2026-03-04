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
- Priority order: **Graph -> Vector -> Spatial -> Fulltext**.
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

### Phase 1: Contract Hardening
- Freeze canonical JSON shapes for:
  - graph traversal ops
  - vector `similar`
  - spatial bbox/polygon
  - fulltext `matching` with score.
- Add explicit examples for each op in README and wrappers docs.

### Phase 2: Indexing Clarity
- Document and expose index metadata in `describe` outputs per collection:
  - graph relation index state
  - vector index state (HNSW)
  - spatial index state (RTree)
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
  - fulltext weighted ranking.
- Record baseline numbers; fail CI only on severe regressions.

### Phase 5: ToraJu Handoff Contract
- Export stable minimal contract for ToraJu adapters:
  - `x.n.tables.read/query/insert` mapping to SeKejap atomic/query APIs.
- Keep ToraJu node logic thin: orchestration in ToraJu, data semantics in SeKejap.

## Definition of Done (Before ToraJu Buildout)
- Canonical interfaces and examples are documented.
- 4-pillar query/mutate paths are tested end-to-end.
- `describe` clearly reports per-pillar index status.
- Wrappers and CLI use unified naming (`query`/`mutate`).
