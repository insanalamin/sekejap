# Temporal Index Draft

## Goal

Make time a first-class citizen in SekejapDB, alongside:

- graph
- vector
- spatial
- full-text

This is not just "timestamp support". SekejapDB needs to support vague, recurring, and fuzzy time for spatiotemporal memory/history workloads.

Examples that must be supported:

- exact instant
- exact bounded range
- around July 1993
- around 1994 to 1996
- weekdays around 5pm from 1993 to 1995
- around 2002 to 2003, weekdays 12pm to 3pm
- monthly recurrence
- deep history ranges, including millions or billions of years ago

## Design Principle

Canonical app storage can keep rich time payloads in JSON files, but SekejapDB should compile those into a temporal index for fast local query.

So the architecture is:

1. canonical payload contains rich vague time
2. SekejapDB compiles that payload into temporal index fields
3. queries run against the compiled temporal index

## Why `RangeIndex` Is Not Enough

Current `RangeIndex` is useful for:

- exact timestamps
- numeric ranges
- simple `where_between`

It is not enough for:

- month masks
- weekday filters
- time-of-day windows
- fuzzy edges
- recurring schedules
- deep-time fuzzy overlap

So temporal indexing should be its own module, not just a convention on top of `RangeIndex`.

## Proposed Engine Surface

Add a new index module:

- `src/index/time_index.rs`

Add a new engine-owned compiled temporal representation:

```rust
pub struct TimeIndexEntry {
    pub node_idx: u32,
    pub start_year: i64,
    pub end_year: i64,
    pub expanded_start_year: i64,
    pub expanded_end_year: i64,
    pub month_mask: u16,
    pub weekday_mask: u8,
    pub day_of_month_mask: u32,
    pub has_time_of_day: bool,
    pub time_of_day_start: u16,
    pub time_of_day_end: u16,
    pub time_of_day_fuzzy_radius: u16,
    pub recurrence_step_months: Option<u8>,
    pub is_recurring: bool,
    pub global_fuzziness: f32,
}
```

## Input Payload Shape

The engine should read a conventional payload field, for example:

- `time`
- or `_time`

Example:

```json
{
  "_id": "memories/changi-2006",
  "title": "Departure through Changi",
  "time": {
    "kind": "recurring_range",
    "bounds": {
      "startYear": 2002,
      "endYear": 2003,
      "startFuzzYears": 0,
      "endFuzzYears": 1
    },
    "constraints": {
      "weekdays": [1, 2, 3, 4, 5],
      "months": [6, 7],
      "timeOfDay": {
        "startMinute": 720,
        "endMinute": 900,
        "fuzzyRadiusMinute": 20
      },
      "everyNMonths": 1
    },
    "globalFuzziness": 0.18
  }
}
```

The exact JSON contract can be finalized later, but the compiled semantics above are the core requirement.

## Bucket Layer

Temporal indexing needs both:

1. exact compiled fields
2. retrieval buckets

Compiled fields are for precise overlap/scoring.
Buckets are for candidate pruning.

Suggested bucket strategy:

- year buckets
- decade buckets
- century buckets
- deep-time coarse buckets

Example:

- a memory spanning `2002..2003` should hit:
  - year bucket `2002`
  - year bucket `2003`
  - decade bucket `2000s`
- a deep-history node spanning `-70,000,000..-60,000,000` should hit large coarse buckets

## Query Semantics

The temporal index must support overlap, not only equality.

Core operations:

- `time_intersects`
- `time_within`
- `time_near`
- `time_recurring_match`

Recommended first implementation:

```rust
pub enum Step {
    // existing steps...
    TimeIntersects(TimeQuery),
    TimeWithin(TimeQuery),
    TimeNear(TimeQuery),
}
```

Where `TimeQuery` can contain:

```rust
pub struct TimeQuery {
    pub start_year: i64,
    pub end_year: i64,
    pub start_fuzz_years: u16,
    pub end_fuzz_years: u16,
    pub month_mask: u16,
    pub weekday_mask: u8,
    pub day_of_month_mask: u32,
    pub has_time_of_day: bool,
    pub time_of_day_start: u16,
    pub time_of_day_end: u16,
    pub time_of_day_fuzzy_radius: u16,
    pub recurrence_step_months: Option<u8>,
    pub global_fuzziness: f32,
}
```

## Write / Update / Remove Lifecycle

Temporal indexing must be maintained incrementally.

When a node is written or updated:

1. remove old temporal entry if present
2. parse current payload time object
3. compile to `TimeIndexEntry`
4. register range/bucket memberships

When a node is removed:

1. remove temporal entry
2. remove bucket memberships

This should mirror current behavior for:

- spatial updates
- field hash/range index updates

## Interaction With Spatial Radius

Temporal indexing must work with spatial radius queries, because Sekejap workloads are spatiotemporal.

Important spatial requirement:

- point-only is not enough
- fuzzy spatial radius must remain first-class

Memory/place nodes often represent:

- exact point
- point + radius
- later polygon or area

Typical query:

- observed time window intersects node time
- observed circle intersects node spatial circle
- results ranked by time overlap + spatial proximity

So time and space should be designed to compose in the same query plan.

## Relation To Graph / Vector

A memory node should be queryable through all pillars:

- graph traversal
- temporal overlap
- spatial radius
- vector similarity
- full-text

Typical hybrid query:

1. start from observed node or collection
2. apply graph expansion if needed
3. apply temporal candidate pruning
4. apply spatial candidate pruning
5. apply vector ranking or full-text ranking
6. combine scores

## Recommended Internal API

```rust
pub trait TemporalIndex: Send + Sync {
    fn insert(&self, node_idx: u32, value: &serde_json::Value);
    fn remove(&self, node_idx: u32);
    fn lookup_intersects(&self, query: &TimeQuery) -> Vec<u32>;
    fn lookup_within(&self, query: &TimeQuery) -> Vec<u32>;
}
```

Backed by:

- compiled entries by node
- time bucket lookup tables
- optional sorted range helpers for year bounds

## Implementation Order

1. Add `time_index.rs`
2. Add payload parser for vague time
3. Add compiled entry + bucket compiler
4. Add `TimeIntersects` query step
5. Add write/update/remove maintenance hooks
6. Add tests for:
   - exact overlap
   - fuzzy year edges
   - month-only matching
   - weekday/time-of-day matching
   - recurring monthly matching
   - deep history
   - hybrid time + spatial query

## Non-Goal

This does not replace canonical application storage.

The app can still store memories in file/folder form. SekejapDB remains the derived embedded local engine that compiles those records into graph/vector/spatial/temporal indexes.
