//! Temporal index for exact and vague time.
//!
//! This module is the engine-side temporal pillar used for:
//!
//! - exact time
//! - bounded vague time
//! - recurring/habitual time
//! - fuzzy overlap queries
//! - hybrid temporal + spatial retrieval
//!
//! The public write/query contract should stay ergonomic. The engine normalizes
//! those temporal payloads into compiled masks, ranges, and buckets.
//!
//! ## Example: write memories with easy temporal payloads
//!
//! ```rust
//! use sekejap::SekejapDB;
//! use serde_json::json;
//! # use tempfile::TempDir;
//! # let dir = TempDir::new().unwrap();
//! # let db = SekejapDB::new(dir.path(), 1024).unwrap();
//!
//! db.schema().define("memories", &json!({
//!     "hot_fields": {
//!         "temporal": ["time"],
//!         "spatial": ["geo"]
//!     }
//! }).to_string())?;
//!
//! // Exact-style memory: one exact evening outside Flinders Street Station
//! db.nodes().put_json(&json!({
//!     "_id": "memories/flinders-2016-06-23-2135",
//!     "title": "Waiting outside Flinders Street Station",
//!     "time": {
//!         "bounds": { "startYear": 2016, "endYear": 2016 },
//!         "constraints": {
//!             "months": [6],
//!             "daysOfMonth": [23],
//!             "timeOfDay": {
//!                 "startMinute": 1295,
//!                 "endMinute": 1295,
//!                 "fuzzyRadiusMinute": 0
//!             }
//!         },
//!         "globalFuzziness": 0.0
//!     },
//!     "geo": { "loc": { "lat": -37.8183, "lon": 144.9671 } }
//! }).to_string())?;
//!
//! // Vague recurring memory: weekday afternoons around the CBD
//! db.nodes().put_json(&json!({
//!     "_id": "memories/cbd-afternoons-2008-2010",
//!     "title": "Weekday afternoons around the CBD",
//!     "time": {
//!         "kind": "recurring_range",
//!         "bounds": { "startYear": 2008, "endYear": 2010 },
//!         "constraints": {
//!             "weekdays": [1, 2, 3, 4, 5],
//!             "timeOfDay": {
//!                 "startMinute": 720,
//!                 "endMinute": 900,
//!                 "fuzzyRadiusMinute": 20
//!             }
//!         },
//!         "globalFuzziness": 0.15
//!     },
//!     "geo": { "loc": { "lat": -37.8136, "lon": 144.9631 } }
//! }).to_string())?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Example: query overlapping vague time
//!
//! ```rust
//! use sekejap::{SekejapDB, TimeQuery};
//! # use tempfile::TempDir;
//! # let dir = TempDir::new().unwrap();
//! # let db = SekejapDB::new(dir.path(), 1024).unwrap();
//! # db.schema().define("memories", r#"{"hot_fields":{"temporal":["time"]}}"#).unwrap();
//! # db.nodes().put_json(r#"{"_id":"memories/test","time":{"bounds":{"startYear":2009,"endYear":2009}}}"#).unwrap();
//!
//! let hits = db.nodes()
//!     .collection("memories")
//!     .time_intersects("time", TimeQuery {
//!         start_year: 2008,
//!         end_year: 2010,
//!         start_fuzz_years: 0,
//!         end_fuzz_years: 0,
//!         months: Vec::new(),
//!         weekdays: Vec::new(),
//!         days_of_month: Vec::new(),
//!         time_of_day: None,
//!         recurrence_step_months: None,
//!         global_fuzziness: 0.0,
//!     })
//!     .collect()?;
//!
//! assert_eq!(hits.data.len(), 1);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Example: combine temporal + spatial
//!
//! ```rust
//! use sekejap::{SekejapDB, TimeQuery};
//! # use tempfile::TempDir;
//! # let dir = TempDir::new().unwrap();
//! # let db = SekejapDB::new(dir.path(), 1024).unwrap();
//! # db.schema().define("memories", r#"{"hot_fields":{"temporal":["time"],"spatial":["geo"]}}"#).unwrap();
//! # db.nodes().put_json(r#"{
//! #   "_id":"memories/st-kilda-summer",
//! #   "time":{"bounds":{"startYear":2011,"endYear":2011}},
//! #   "geo":{"loc":{"lat":-37.8676,"lon":144.9800}}
//! # }"#).unwrap();
//!
//! let hits = db.nodes()
//!     .collection("memories")
//!     .time_intersects("time", TimeQuery {
//!         start_year: 2011,
//!         end_year: 2011,
//!         start_fuzz_years: 0,
//!         end_fuzz_years: 0,
//!         months: Vec::new(),
//!         weekdays: Vec::new(),
//!         days_of_month: Vec::new(),
//!         time_of_day: None,
//!         recurrence_step_months: None,
//!         global_fuzziness: 0.0,
//!     })
//!     .near(-37.8676, 144.9800, 0.02)
//!     .collect()?;
//!
//! assert_eq!(hits.data.len(), 1);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Example: modify a memory
//!
//! Upsert reindexes automatically. Just write the same `_id` again with a new
//! temporal payload.
//!
//! ```rust
//! use sekejap::SekejapDB;
//! use serde_json::json;
//! # use tempfile::TempDir;
//! # let dir = TempDir::new().unwrap();
//! # let db = SekejapDB::new(dir.path(), 1024).unwrap();
//! # db.schema().define("memories", r#"{"hot_fields":{"temporal":["time"]}}"#).unwrap();
//!
//! db.nodes().put_json(&json!({
//!     "_id": "memories/state-library-visit",
//!     "title": "State Library visit",
//!     "time": { "bounds": { "startYear": 2019, "endYear": 2019 } }
//! }).to_string())?;
//!
//! // Modify same memory later: narrower exact-ish month
//! db.nodes().put_json(&json!({
//!     "_id": "memories/state-library-visit",
//!     "title": "State Library visit",
//!     "time": {
//!         "bounds": { "startYear": 2019, "endYear": 2019 },
//!         "constraints": { "months": [8] }
//!     }
//! }).to_string())?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
use crate::types::{TimeOfDayQuery, TimeQuery};
use roaring::RoaringBitmap;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct TimeBucket {
    granularity_years: i64,
    slot: i64,
}

#[derive(Clone, Debug)]
struct TimeIndexEntry {
    node_idx: u32,
    start_year: i64,
    end_year: i64,
    expanded_start_year: i64,
    expanded_end_year: i64,
    month_mask: u16,
    weekday_mask: u8,
    day_of_month_mask: u32,
    has_time_of_day: bool,
    time_of_day_start: u16,
    time_of_day_end: u16,
    time_of_day_fuzzy_radius: u16,
    recurrence_step_months: Option<u8>,
    is_recurring: bool,
    global_fuzziness: f32,
    buckets: Vec<TimeBucket>,
}

#[derive(Clone, Debug)]
struct TimePayload {
    start_year: i64,
    end_year: i64,
    start_fuzz_years: u16,
    end_fuzz_years: u16,
    months: Vec<u8>,
    weekdays: Vec<u8>,
    days_of_month: Vec<u8>,
    time_of_day: Option<TimeOfDayQuery>,
    recurrence_step_months: Option<u8>,
    is_recurring: bool,
    global_fuzziness: f32,
}

pub struct TimeIndex {
    name: String,
    entries: RwLock<HashMap<u32, TimeIndexEntry>>,
    buckets: RwLock<HashMap<TimeBucket, RoaringBitmap>>,
}

impl TimeIndex {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            entries: RwLock::new(HashMap::new()),
            buckets: RwLock::new(HashMap::new()),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn count(&self) -> usize {
        self.entries.read().unwrap().len()
    }

    pub fn insert(&self, node_idx: u32, root: &Value, field: &str) {
        self.remove(node_idx);
        let Some(payload) = extract_time_payload(root, field) else {
            return;
        };
        let entry = compile_entry(node_idx, payload);
        {
            let mut buckets = self.buckets.write().unwrap();
            for bucket in &entry.buckets {
                buckets.entry(*bucket).or_default().insert(node_idx);
            }
        }
        self.entries.write().unwrap().insert(node_idx, entry);
    }

    pub fn remove(&self, node_idx: u32) {
        let Some(existing) = self.entries.write().unwrap().remove(&node_idx) else {
            return;
        };
        let mut buckets = self.buckets.write().unwrap();
        for bucket in &existing.buckets {
            let should_remove = if let Some(bitmap) = buckets.get_mut(bucket) {
                bitmap.remove(node_idx);
                bitmap.is_empty()
            } else {
                false
            };
            if should_remove {
                buckets.remove(bucket);
            }
        }
    }

    pub fn lookup_intersects(&self, query: &TimeQuery) -> Vec<u32> {
        let query_entry = compile_query_entry(query);

        let mut candidates = RoaringBitmap::new();
        {
            let buckets = self.buckets.read().unwrap();
            for bucket in &query_entry.buckets {
                if let Some(found) = buckets.get(bucket) {
                    candidates |= found.clone();
                }
            }
        }

        let entries = self.entries.read().unwrap();
        candidates
            .iter()
            .filter(|idx| entries.get(idx).is_some_and(|entry| intersects(entry, &query_entry)))
            .collect()
    }

    pub fn lookup_within(&self, query: &TimeQuery) -> Vec<u32> {
        let query_entry = compile_query_entry(query);
        let mut candidates = RoaringBitmap::new();
        {
            let buckets = self.buckets.read().unwrap();
            for bucket in &query_entry.buckets {
                if let Some(found) = buckets.get(bucket) {
                    candidates |= found.clone();
                }
            }
        }
        let entries = self.entries.read().unwrap();
        candidates
            .iter()
            .filter(|idx| entries.get(idx).is_some_and(|entry| within(entry, &query_entry)))
            .collect()
    }

    pub fn lookup_near(&self, query: &TimeQuery) -> Vec<u32> {
        let query_entry = compile_query_entry(query);
        let mut candidates = RoaringBitmap::new();
        {
            let buckets = self.buckets.read().unwrap();
            for bucket in &query_entry.buckets {
                if let Some(found) = buckets.get(bucket) {
                    candidates |= found.clone();
                }
            }
        }
        let entries = self.entries.read().unwrap();
        candidates
            .iter()
            .filter(|idx| entries.get(idx).is_some_and(|entry| near(entry, &query_entry)))
            .collect()
    }

    pub fn payload_intersects(root: &Value, field: &str, query: &TimeQuery) -> bool {
        let Some(payload) = extract_time_payload(root, field) else {
            return false;
        };
        let payload_entry = compile_entry(0, payload);
        let query_entry = compile_query_entry(query);
        intersects(&payload_entry, &query_entry)
    }

    pub fn payload_within(root: &Value, field: &str, query: &TimeQuery) -> bool {
        let Some(payload) = extract_time_payload(root, field) else {
            return false;
        };
        let payload_entry = compile_entry(0, payload);
        let query_entry = compile_query_entry(query);
        within(&payload_entry, &query_entry)
    }

    pub fn payload_near(root: &Value, field: &str, query: &TimeQuery) -> bool {
        let Some(payload) = extract_time_payload(root, field) else {
            return false;
        };
        let payload_entry = compile_entry(0, payload);
        let query_entry = compile_query_entry(query);
        near(&payload_entry, &query_entry)
    }
}

fn extract_time_payload(root: &Value, field: &str) -> Option<TimePayload> {
    let time = root.get(field)?;
    let bounds = time.get("bounds").unwrap_or(time);
    let constraints = time.get("constraints").unwrap_or(time);
    let start_year = bounds.get("startYear").or(bounds.get("start_year"))?.as_i64()?;
    let end_year = bounds
        .get("endYear")
        .or(bounds.get("end_year"))
        .and_then(|v| v.as_i64())
        .unwrap_or(start_year);
    let start_fuzz_years = bounds
        .get("startFuzzYears")
        .or(bounds.get("start_fuzz_years"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u16;
    let end_fuzz_years = bounds
        .get("endFuzzYears")
        .or(bounds.get("end_fuzz_years"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u16;
    let months = constraints
        .get("months")
        .and_then(|v| v.as_array())
        .map(|items| items.iter().filter_map(|v| v.as_u64().map(|n| n as u8)).collect())
        .unwrap_or_default();
    let weekdays = constraints
        .get("weekdays")
        .and_then(|v| v.as_array())
        .map(|items| items.iter().filter_map(|v| v.as_u64().map(|n| n as u8)).collect())
        .unwrap_or_default();
    let days_of_month = constraints
        .get("daysOfMonth")
        .or(constraints.get("days_of_month"))
        .and_then(|v| v.as_array())
        .map(|items| items.iter().filter_map(|v| v.as_u64().map(|n| n as u8)).collect())
        .unwrap_or_default();
    let recurrence_step_months = constraints
        .get("everyNMonths")
        .or(constraints.get("every_n_months"))
        .and_then(|v| v.as_u64())
        .map(|n| n as u8);
    let time_of_day = constraints
        .get("timeOfDay")
        .or(constraints.get("time_of_day"))
        .and_then(|tod| {
            Some(TimeOfDayQuery {
                start_minute: tod.get("startMinute").or(tod.get("start_minute"))?.as_u64()? as u16,
                end_minute: tod.get("endMinute").or(tod.get("end_minute"))?.as_u64()? as u16,
                fuzzy_radius_minute: tod
                    .get("fuzzyRadiusMinute")
                    .or(tod.get("fuzzy_radius_minute"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u16,
            })
        });
    let is_recurring = time
        .get("kind")
        .and_then(|v| v.as_str())
        .is_some_and(|kind| kind == "recurring_range")
        || recurrence_step_months.is_some();
    let global_fuzziness = time
        .get("globalFuzziness")
        .or(time.get("global_fuzziness"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0) as f32;

    Some(TimePayload {
        start_year,
        end_year,
        start_fuzz_years,
        end_fuzz_years,
        months,
        weekdays,
        days_of_month,
        time_of_day,
        recurrence_step_months,
        is_recurring,
        global_fuzziness,
    })
}

fn compile_entry(node_idx: u32, payload: TimePayload) -> TimeIndexEntry {
    let expanded_start_year = payload.start_year - i64::from(payload.start_fuzz_years);
    let expanded_end_year = payload.end_year + i64::from(payload.end_fuzz_years);
    let mut buckets = Vec::new();
    for granularity in [1_i64, 10, 100, 1_000, 1_000_000] {
        let start_slot = expanded_start_year.div_euclid(granularity);
        let end_slot = expanded_end_year.div_euclid(granularity);
        if end_slot - start_slot > 512 {
            continue;
        }
        for slot in start_slot..=end_slot {
            buckets.push(TimeBucket {
                granularity_years: granularity,
                slot,
            });
        }
    }
    TimeIndexEntry {
        node_idx,
        start_year: payload.start_year,
        end_year: payload.end_year,
        expanded_start_year,
        expanded_end_year,
        month_mask: mask_u16(&payload.months, 12),
        weekday_mask: mask_u8(&payload.weekdays, 7),
        day_of_month_mask: mask_u32(&payload.days_of_month, 31),
        has_time_of_day: payload.time_of_day.is_some(),
        time_of_day_start: payload.time_of_day.map(|it| it.start_minute).unwrap_or(0),
        time_of_day_end: payload.time_of_day.map(|it| it.end_minute).unwrap_or(24 * 60 - 1),
        time_of_day_fuzzy_radius: payload.time_of_day.map(|it| it.fuzzy_radius_minute).unwrap_or(0),
        recurrence_step_months: payload.recurrence_step_months,
        is_recurring: payload.is_recurring,
        global_fuzziness: payload.global_fuzziness.clamp(0.0, 1.0),
        buckets,
    }
}

fn compile_query_entry(query: &TimeQuery) -> TimeIndexEntry {
    compile_entry(0, TimePayload {
        start_year: query.start_year,
        end_year: query.end_year,
        start_fuzz_years: query.start_fuzz_years,
        end_fuzz_years: query.end_fuzz_years,
        months: query.months.clone(),
        weekdays: query.weekdays.clone(),
        days_of_month: query.days_of_month.clone(),
        time_of_day: query.time_of_day,
        recurrence_step_months: query.recurrence_step_months,
        is_recurring: query.recurrence_step_months.is_some(),
        global_fuzziness: query.global_fuzziness,
    })
}

fn intersects(left: &TimeIndexEntry, right: &TimeIndexEntry) -> bool {
    let _ = (left.node_idx, left.start_year, left.end_year, left.global_fuzziness, left.is_recurring);
    if left.expanded_start_year > right.expanded_end_year || right.expanded_start_year > left.expanded_end_year {
        return false;
    }
    if left.month_mask != 0 && right.month_mask != 0 && (left.month_mask & right.month_mask) == 0 {
        return false;
    }
    if left.weekday_mask != 0 && right.weekday_mask != 0 && (left.weekday_mask & right.weekday_mask) == 0 {
        return false;
    }
    if left.day_of_month_mask != 0
        && right.day_of_month_mask != 0
        && (left.day_of_month_mask & right.day_of_month_mask) == 0
    {
        return false;
    }
    if left.has_time_of_day && right.has_time_of_day {
        let left_start = left.time_of_day_start.saturating_sub(left.time_of_day_fuzzy_radius);
        let left_end = left.time_of_day_end.saturating_add(left.time_of_day_fuzzy_radius).min(24 * 60 - 1);
        let right_start = right.time_of_day_start.saturating_sub(right.time_of_day_fuzzy_radius);
        let right_end = right.time_of_day_end.saturating_add(right.time_of_day_fuzzy_radius).min(24 * 60 - 1);
        if left_start > right_end || right_start > left_end {
            return false;
        }
    }
    if let (Some(left_step), Some(right_step)) = (left.recurrence_step_months, right.recurrence_step_months) {
        if left_step != right_step {
            return false;
        }
    }
    true
}

fn within(left: &TimeIndexEntry, right: &TimeIndexEntry) -> bool {
    if left.expanded_start_year < right.expanded_start_year || left.expanded_end_year > right.expanded_end_year {
        return false;
    }
    if right.month_mask != 0 && (left.month_mask & right.month_mask) != left.month_mask {
        return false;
    }
    if right.weekday_mask != 0 && (left.weekday_mask & right.weekday_mask) != left.weekday_mask {
        return false;
    }
    if right.day_of_month_mask != 0 && (left.day_of_month_mask & right.day_of_month_mask) != left.day_of_month_mask {
        return false;
    }
    if left.has_time_of_day && right.has_time_of_day {
        let left_start = left.time_of_day_start.saturating_sub(left.time_of_day_fuzzy_radius);
        let left_end = left.time_of_day_end.saturating_add(left.time_of_day_fuzzy_radius).min(24 * 60 - 1);
        let right_start = right.time_of_day_start.saturating_sub(right.time_of_day_fuzzy_radius);
        let right_end = right.time_of_day_end.saturating_add(right.time_of_day_fuzzy_radius).min(24 * 60 - 1);
        if left_start < right_start || left_end > right_end {
            return false;
        }
    }
    true
}

fn near(left: &TimeIndexEntry, right: &TimeIndexEntry) -> bool {
    let gap = if left.expanded_end_year < right.expanded_start_year {
        right.expanded_start_year - left.expanded_end_year
    } else if right.expanded_end_year < left.expanded_start_year {
        left.expanded_start_year - right.expanded_end_year
    } else {
        0
    };
    let tolerance = i64::from(left.recurrence_step_months.unwrap_or(0).max(right.recurrence_step_months.unwrap_or(0)))
        .max((left.global_fuzziness.max(right.global_fuzziness) * 10.0).ceil() as i64)
        .max(i64::from(left.time_of_day_fuzzy_radius.max(right.time_of_day_fuzzy_radius) / 60))
        .max(i64::from(left.expanded_end_year - left.start_year).abs().min(2))
        .max(i64::from(right.expanded_end_year - right.start_year).abs().min(2))
        .max(1);
    gap <= tolerance || intersects(left, right)
}

fn mask_u16(values: &[u8], max: u8) -> u16 {
    values
        .iter()
        .copied()
        .filter(|value| (1..=max).contains(value))
        .fold(0_u16, |acc, value| acc | (1_u16 << (value - 1)))
}

fn mask_u8(values: &[u8], max: u8) -> u8 {
    values
        .iter()
        .copied()
        .filter(|value| (1..=max).contains(value))
        .fold(0_u8, |acc, value| acc | (1_u8 << (value - 1)))
}

fn mask_u32(values: &[u8], max: u8) -> u32 {
    values
        .iter()
        .copied()
        .filter(|value| (1..=max).contains(value))
        .fold(0_u32, |acc, value| acc | (1_u32 << (value - 1)))
}
