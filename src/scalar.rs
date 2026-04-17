//! Scalar functions for SQL expressions.

use chrono::{Datelike, Timelike};
use serde_json::{Number, Value};

pub fn str_length(s: &str) -> u64 {
    s.chars().count() as u64
}

pub fn str_lower(s: &str) -> String {
    s.to_lowercase()
}

pub fn str_upper(s: &str) -> String {
    s.to_uppercase()
}

pub fn str_trim(s: &str) -> String {
    s.trim().to_string()
}

pub fn str_ltrim(s: &str) -> String {
    s.trim_start().to_string()
}

pub fn str_rtrim(s: &str) -> String {
    s.trim_end().to_string()
}

pub fn str_substring(s: &str, start: usize, len: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    let start = start.saturating_sub(1);
    chars.into_iter().skip(start).take(len).collect()
}

pub fn str_replace(s: &str, old: &str, new: &str) -> String {
    s.replace(old, new)
}

pub fn str_concat(a: &str, b: &str) -> String {
    format!("{}{}", a, b)
}

pub fn str_concat_multi(values: &[String]) -> String {
    values.join("")
}

fn make_number(n: u64) -> Value {
    Value::Number(Number::from(n))
}

fn resolve_arg(arg: &Value, payload: &serde_json::Map<String, Value>) -> Option<Value> {
    match arg {
        Value::String(s) => {
            if let Some(field_value) = payload.get(s) {
                Some(field_value.clone())
            } else {
                Some(Value::String(s.clone()))
            }
        }
        other => Some(other.clone()),
    }
}

pub fn eval_scalar_func(
    func_name: &str,
    args: &[Value],
    payload: &serde_json::Map<String, Value>,
) -> Value {
    match func_name.to_uppercase().as_str() {
        "LENGTH" | "LEN" => {
            if let Some(Value::String(s)) = args.get(0).and_then(|a| resolve_arg(a, payload)) {
                make_number(str_length(&s))
            } else {
                Value::Null
            }
        }
        "LOWER" => {
            if let Some(Value::String(s)) = args.get(0).and_then(|a| resolve_arg(a, payload)) {
                Value::String(str_lower(&s))
            } else {
                Value::Null
            }
        }
        "UPPER" => {
            if let Some(Value::String(s)) = args.get(0).and_then(|a| resolve_arg(a, payload)) {
                Value::String(str_upper(&s))
            } else {
                Value::Null
            }
        }
        "TRIM" => {
            if let Some(Value::String(s)) = args.get(0).and_then(|a| resolve_arg(a, payload)) {
                Value::String(str_trim(&s))
            } else {
                Value::Null
            }
        }
        "LTRIM" => {
            if let Some(Value::String(s)) = args.get(0).and_then(|a| resolve_arg(a, payload)) {
                Value::String(str_ltrim(&s))
            } else {
                Value::Null
            }
        }
        "RTRIM" => {
            if let Some(Value::String(s)) = args.get(0).and_then(|a| resolve_arg(a, payload)) {
                Value::String(str_rtrim(&s))
            } else {
                Value::Null
            }
        }
        "SUBSTRING" => {
            if args.len() >= 3 {
                if let (
                    Some(Value::String(s)),
                    Some(Value::Number(start)),
                    Some(Value::Number(len)),
                ) = (resolve_arg(&args[0], payload), args.get(1), args.get(2))
                {
                    let start = start.as_u64().unwrap_or(1) as usize;
                    let len = len.as_u64().unwrap_or(1) as usize;
                    Value::String(str_substring(&s, start, len))
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            }
        }
        "REPLACE" => {
            if args.len() >= 3 {
                if let (
                    Some(Value::String(s)),
                    Some(Value::String(old)),
                    Some(Value::String(new)),
                ) = (resolve_arg(&args[0], payload), args.get(1), args.get(2))
                {
                    Value::String(str_replace(&s, old, new))
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            }
        }
        "CONCAT" => {
            let mut result = String::new();
            for arg in args {
                if let Some(resolved) = resolve_arg(arg, payload) {
                    match resolved {
                        Value::String(s) => result.push_str(&s),
                        other => {
                            if let Some(s) = other.as_str() {
                                result.push_str(s);
                            }
                        }
                    }
                }
            }
            Value::String(result)
        }
        "NOW" => Value::String(now()),
        "YEAR" => {
            if let Some(Value::String(s)) = args.get(0) {
                Value::Number(Number::from(year(s).unwrap_or(0) as u64))
            } else {
                Value::Null
            }
        }
        "MONTH" => {
            if let Some(Value::String(s)) = args.get(0) {
                Value::Number(Number::from(month(s).unwrap_or(0) as u64))
            } else {
                Value::Null
            }
        }
        "DAY" => {
            if let Some(Value::String(s)) = args.get(0) {
                Value::Number(Number::from(day(s).unwrap_or(0) as u64))
            } else {
                Value::Null
            }
        }
        "HOUR" => {
            if let Some(Value::String(s)) = args.get(0) {
                Value::Number(Number::from(hour(s).unwrap_or(0) as u64))
            } else {
                Value::Null
            }
        }
        "MINUTE" => {
            if let Some(Value::String(s)) = args.get(0) {
                Value::Number(Number::from(minute(s).unwrap_or(0) as u64))
            } else {
                Value::Null
            }
        }
        "SECOND" => {
            if let Some(Value::String(s)) = args.get(0) {
                Value::Number(Number::from(second(s).unwrap_or(0) as u64))
            } else {
                Value::Null
            }
        }
        "DOW" => {
            // 0 = Sunday, 6 = Saturday (PostgreSQL convention)
            if let Some(Value::String(s)) = args.get(0) {
                Value::Number(Number::from(dow(s).unwrap_or(0) as u64))
            } else {
                Value::Null
            }
        }
        "QUARTER" => {
            if let Some(Value::String(s)) = args.get(0) {
                Value::Number(Number::from(quarter(s).unwrap_or(0) as u64))
            } else {
                Value::Null
            }
        }
        "DATE_TRUNC" => {
            // args[0] = unit ('year','month','day','hour','minute','second')
            // args[1] = datetime field value (ISO string from payload)
            if args.len() >= 2 {
                let unit = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Value::Null,
                };
                let date_val = resolve_arg(&args[1], payload);
                if let Some(Value::String(s)) = date_val {
                    if let Some(dt) = parse_iso_datetime(&s) {
                        return Value::String(date_trunc(&unit, dt));
                    }
                }
                Value::Null
            } else {
                Value::Null
            }
        }
        "UUIDV4" => Value::String(uuid_v4()),
        "UUIDV5" => {
            if args.len() >= 2 {
                if let (Some(Value::String(ns)), Some(Value::String(name))) =
                    (args.get(0), args.get(1))
                {
                    Value::String(uuid_v5(ns, name))
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}

pub fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}

pub fn year(date_str: &str) -> Option<i32> {
    parse_iso_datetime(date_str).map(|dt| dt.year())
}

pub fn month(date_str: &str) -> Option<u32> {
    parse_iso_datetime(date_str).map(|dt| dt.month())
}

pub fn day(date_str: &str) -> Option<u32> {
    parse_iso_datetime(date_str).map(|dt| dt.day())
}

fn parse_iso_datetime(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&chrono::Utc))
}

pub fn hour(date_str: &str) -> Option<u32> {
    parse_iso_datetime(date_str).map(|dt| dt.hour())
}

pub fn minute(date_str: &str) -> Option<u32> {
    parse_iso_datetime(date_str).map(|dt| dt.minute())
}

pub fn second(date_str: &str) -> Option<u32> {
    parse_iso_datetime(date_str).map(|dt| dt.second())
}

/// Day of week: 0 = Sunday, 6 = Saturday (PostgreSQL convention).
pub fn dow(date_str: &str) -> Option<u32> {
    parse_iso_datetime(date_str).map(|dt| dt.weekday().num_days_from_sunday())
}

pub fn quarter(date_str: &str) -> Option<u32> {
    parse_iso_datetime(date_str).map(|dt| (dt.month() - 1) / 3 + 1)
}

/// Truncate a datetime to the given unit, returning an RFC 3339 string.
///
/// Supported units: year, quarter, month, day, hour, minute, second.
pub fn date_trunc(unit: &str, dt: chrono::DateTime<chrono::Utc>) -> String {
    use chrono::NaiveDateTime;
    let naive = dt.naive_utc();
    let nd = naive.date();
    let nt = naive.time();
    let zero = chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap();
    let truncated = match unit.to_lowercase().as_str() {
        "year" => NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(nd.year(), 1, 1).unwrap_or(nd),
            zero,
        ),
        "quarter" => {
            let q_month = ((nd.month() - 1) / 3) * 3 + 1;
            NaiveDateTime::new(
                chrono::NaiveDate::from_ymd_opt(nd.year(), q_month, 1).unwrap_or(nd),
                zero,
            )
        }
        "month" => NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(nd.year(), nd.month(), 1).unwrap_or(nd),
            zero,
        ),
        "day" => NaiveDateTime::new(nd, zero),
        "hour" => NaiveDateTime::new(
            nd,
            chrono::NaiveTime::from_hms_opt(nt.hour(), 0, 0).unwrap(),
        ),
        "minute" => NaiveDateTime::new(
            nd,
            chrono::NaiveTime::from_hms_opt(nt.hour(), nt.minute(), 0).unwrap(),
        ),
        "second" => NaiveDateTime::new(
            nd,
            chrono::NaiveTime::from_hms_opt(nt.hour(), nt.minute(), nt.second()).unwrap(),
        ),
        _ => naive,
    };
    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(truncated, chrono::Utc)
        .to_rfc3339()
}

pub fn uuid_v4() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub fn uuid_v5(namespace: &str, name: &str) -> String {
    let ns = uuid::Uuid::parse_str(namespace).unwrap_or(uuid::Uuid::NAMESPACE_DNS);
    uuid::Uuid::new_v5(&ns, name.as_bytes()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_length() {
        assert_eq!(str_length("hello"), 5);
        assert_eq!(str_length(""), 0);
        assert_eq!(str_length("日本語"), 3);
    }

    #[test]
    fn test_lower() {
        assert_eq!(str_lower("HELLO"), "hello");
        assert_eq!(str_lower("HeLLo"), "hello");
    }

    #[test]
    fn test_upper() {
        assert_eq!(str_upper("hello"), "HELLO");
        assert_eq!(str_upper("HeLLo"), "HELLO");
    }

    #[test]
    fn test_trim() {
        assert_eq!(str_trim("  hello  "), "hello");
        assert_eq!(str_trim("\t\ntest\t\n"), "test");
    }

    #[test]
    fn test_substring() {
        assert_eq!(str_substring("hello", 1, 3), "hel");
        assert_eq!(str_substring("hello", 2, 2), "el");
        assert_eq!(str_substring("hello", 4, 10), "lo");
    }

    #[test]
    fn test_replace() {
        assert_eq!(str_replace("hello world", "world", "rust"), "hello rust");
        assert_eq!(str_replace("aaaa", "a", "b"), "bbbb");
    }

    #[test]
    fn test_concat() {
        assert_eq!(str_concat("hello", " world"), "hello world");
        assert_eq!(
            str_concat_multi(&["a".to_string(), "b".to_string(), "c".to_string()]),
            "abc"
        );
    }
}
