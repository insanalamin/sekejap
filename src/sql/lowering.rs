use crate::sql::ast::{Expr, SelectItem, SqlStatement, TraverseDirection};
use crate::sql::parser::SqlError;
use crate::types::Step;
use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use serde_json::Value;

pub fn lower_statement(statement: &SqlStatement) -> Result<Vec<Step>, SqlError> {
    match statement {
        SqlStatement::Select(select) => {
            let collection_step = Step::Collection(seahash::hash(select.from.name.as_bytes()));
            let mut steps = Vec::new();

            let mut pushed_before_traverse = Vec::new();
            let mut pushed_after_traverse = Vec::new();
            if let Some(expr) = &select.selection {
                match expr {
                    Expr::Raw(raw) => {
                        let clauses = split_top_level_and(raw);
                        if select.traverse.is_some() {
                            for clause in clauses {
                                if let Some(step) =
                                    lower_source_pushdown_clause(clause.trim(), &select.from.name)?
                                {
                                    pushed_before_traverse.push(step);
                                } else {
                                    pushed_after_traverse.push(lower_clause(clause.trim())?);
                                }
                            }
                            pushed_after_traverse = combine_exact_time_bounds(pushed_after_traverse);
                        } else {
                            for clause in clauses {
                                pushed_after_traverse.push(lower_clause(clause.trim())?);
                            }
                            pushed_after_traverse = combine_exact_time_bounds(pushed_after_traverse);
                        }
                    }
                    _ => return Err(SqlError::new("only raw WHERE expressions are currently parsed")),
                }
            }

            if matches!(pushed_before_traverse.first(), Some(Step::One(_))) {
                steps.extend(pushed_before_traverse);
            } else {
                steps.push(collection_step);
                steps.extend(pushed_before_traverse);
            }

            if let Some(traverse) = &select.traverse {
                if let Some(hops) = traverse.hops {
                    steps.push(Step::Hops(hops));
                }
                let edge_hash = seahash::hash(traverse.edge_type.as_bytes());
                steps.push(match traverse.direction {
                    TraverseDirection::Forward => Step::Forward(edge_hash),
                    TraverseDirection::Backward => Step::Backward(edge_hash),
                });
            }

            steps.extend(pushed_after_traverse);

            for order in &select.order_by {
                if let Some(step) = lower_vector_order(order.field.as_str(), select.limit)? {
                    steps.push(step);
                } else {
                    steps.push(Step::Sort(order.field.clone(), order.ascending));
                }
            }

            if let Some(offset) = select.offset {
                steps.push(Step::Skip(offset));
            }

            if let Some(limit) = select.limit {
                steps.push(Step::Take(limit));
            }

            let mut fields = Vec::new();
            for item in &select.projection {
                match item {
                    SelectItem::Wildcard => {}
                    SelectItem::Field(field) => fields.push(field.clone()),
                    SelectItem::FunctionCall { name, .. } => {
                        return Err(SqlError::new(format!(
                            "function projection lowering is not implemented yet: {name}"
                        )))
                    }
                }
            }
            if !fields.is_empty() {
                steps.push(Step::Select(fields));
            }

            Ok(steps)
        }
        SqlStatement::CreateCollection(_)
        | SqlStatement::Insert(_)
        | SqlStatement::Relate(_)
        | SqlStatement::Update(_)
        | SqlStatement::Delete(_)
        | SqlStatement::Unrelate(_) => {
            Err(SqlError::new("only SELECT statements can be lowered into query steps"))
        }
    }
}

fn combine_exact_time_bounds(steps: Vec<Step>) -> Vec<Step> {
    let mut out = Vec::with_capacity(steps.len());
    let mut consumed = vec![false; steps.len()];

    for i in 0..steps.len() {
        if consumed[i] {
            continue;
        }
        match &steps[i] {
            Step::WhereGte(field_a, lo) => {
                let mut paired = false;
                for j in (i + 1)..steps.len() {
                    if consumed[j] {
                        continue;
                    }
                    if let Step::WhereLte(field_b, hi) = &steps[j] {
                        if field_a == field_b {
                            out.push(Step::WhereBetween(field_a.clone(), *lo, *hi));
                            consumed[j] = true;
                            paired = true;
                            break;
                        }
                    }
                }
                if !paired {
                    out.push(steps[i].clone());
                }
            }
            Step::WhereLte(field_a, hi) => {
                let mut paired = false;
                for j in (i + 1)..steps.len() {
                    if consumed[j] {
                        continue;
                    }
                    if let Step::WhereGte(field_b, lo) = &steps[j] {
                        if field_a == field_b {
                            out.push(Step::WhereBetween(field_a.clone(), *lo, *hi));
                            consumed[j] = true;
                            paired = true;
                            break;
                        }
                    }
                }
                if !paired {
                    out.push(steps[i].clone());
                }
            }
            _ => out.push(steps[i].clone()),
        }
    }

    out
}

fn lower_where(raw: &str) -> Result<Vec<Step>, SqlError> {
    let mut steps = Vec::new();
    for clause in split_top_level_and(raw) {
        steps.push(lower_clause(clause.trim())?);
    }
    Ok(steps)
}

fn lower_clause(raw: &str) -> Result<Step, SqlError> {
    if raw.is_empty() {
        return Err(SqlError::new("empty WHERE clause segment"));
    }

    if let Some(step) = lower_extract_year_clause(raw)? {
        return Ok(step);
    }
    if let Some(step) = lower_year_function_clause(raw)? {
        return Ok(step);
    }
    if let Some(step) = lower_vague_time_clause(raw)? {
        return Ok(step);
    }
    if let Some(step) = lower_spatial_clause(raw)? {
        return Ok(step);
    }
    if let Some(step) = lower_like_clause(raw)? {
        return Ok(step);
    }
    if let Some(step) = lower_matching_clause(raw)? {
        return Ok(step);
    }
    if let Some(step) = lower_vector_near_clause(raw)? {
        return Ok(step);
    }
    if let Some(step) = lower_between_clause(raw)? {
        return Ok(step);
    }
    if let Some(step) = lower_compare_clause(raw)? {
        return Ok(step);
    }

    Err(SqlError::new(format!("unsupported WHERE clause: {raw}")))
}

fn lower_like_clause(raw: &str) -> Result<Option<Step>, SqlError> {
    if let Some(idx) = find_keyword(raw, " ILIKE ") {
        let field = normalize_field_name(raw[..idx].trim());
        let pattern = parse_string_literal(raw[idx + " ILIKE ".len()..].trim())?;
        return Ok(Some(Step::Like(field, pattern, true)));
    }
    if let Some(idx) = find_keyword(raw, " LIKE ") {
        let field = normalize_field_name(raw[..idx].trim());
        let pattern = parse_string_literal(raw[idx + " LIKE ".len()..].trim())?;
        return Ok(Some(Step::Like(field, pattern, false)));
    }
    Ok(None)
}

fn lower_between_clause(raw: &str) -> Result<Option<Step>, SqlError> {
    let idx = match find_keyword(raw, " BETWEEN ") {
        Some(idx) => idx,
        None => return Ok(None),
    };
    let field_raw = raw[..idx].trim();
    let rest = &raw[idx + " BETWEEN ".len()..];
    let and_idx = find_keyword(rest, " AND ").ok_or_else(|| SqlError::new("BETWEEN requires AND"))?;
    let lo_raw = rest[..and_idx].trim();
    let hi_raw = rest[and_idx + " AND ".len()..].trim();

    if is_timestamp_literal(lo_raw) || is_timestamp_literal(hi_raw) || looks_like_exact_time_field(field_raw) {
        let field = exact_time_scalar_field(field_raw);
        let lo = parse_timestamp_literal(lo_raw)? as f64;
        let hi = parse_timestamp_literal(hi_raw)? as f64;
        return Ok(Some(Step::WhereBetween(field, lo, hi)));
    }

    let field = normalize_field_name(field_raw);
    Ok(Some(Step::WhereBetween(field, parse_numeric(lo_raw)?, parse_numeric(hi_raw)?)))
}

fn lower_compare_clause(raw: &str) -> Result<Option<Step>, SqlError> {
    for op in [">=", "<=", ">", "<", "="] {
        if let Some(idx) = find_operator(raw, op) {
            let left = raw[..idx].trim();
            let right = raw[idx + op.len()..].trim();
            if is_timestamp_literal(right) || looks_like_exact_time_field(left) {
                let field = exact_time_scalar_field(left);
                let micros = parse_timestamp_literal(right)? as f64;
                return Ok(Some(match op {
                    ">" => Step::WhereGt(field, micros),
                    ">=" => Step::WhereGte(field, micros),
                    "<" => Step::WhereLt(field, micros),
                    "<=" => Step::WhereLte(field, micros),
                    "=" => Step::WhereEq(field, Value::from(parse_timestamp_literal(right)?)),
                    _ => unreachable!(),
                }));
            }

            let field = normalize_field_name(left);
            return Ok(Some(match op {
                "=" => Step::WhereEq(field, parse_value(right)?),
                ">" => Step::WhereGt(field, parse_numeric(right)?),
                ">=" => Step::WhereGte(field, parse_numeric(right)?),
                "<" => Step::WhereLt(field, parse_numeric(right)?),
                "<=" => Step::WhereLte(field, parse_numeric(right)?),
                _ => unreachable!(),
            }));
        }
    }
    Ok(None)
}

fn lower_extract_year_clause(raw: &str) -> Result<Option<Step>, SqlError> {
    let upper = raw.to_uppercase();
    if !upper.starts_with("EXTRACT(YEAR FROM ") {
        return Ok(None);
    }
    let close_idx = raw.find(')').ok_or_else(|| SqlError::new("EXTRACT clause missing closing )"))?;
    let field_raw = raw["EXTRACT(YEAR FROM ".len()..close_idx].trim();
    let rest = raw[close_idx + 1..].trim();
    let eq = rest.strip_prefix('=').map(str::trim).ok_or_else(|| SqlError::new("EXTRACT currently supports only = comparisons"))?;
    let year = eq.parse::<i32>().map_err(|_| SqlError::new("EXTRACT YEAR comparison expects an integer year"))?;
    Ok(Some(lower_year_range(field_raw, year)?))
}

fn lower_year_function_clause(raw: &str) -> Result<Option<Step>, SqlError> {
    let upper = raw.to_uppercase();
    if !upper.starts_with("YEAR(") {
        return Ok(None);
    }
    let close_idx = raw.find(')').ok_or_else(|| SqlError::new("YEAR() clause missing closing )"))?;
    let field_raw = raw["YEAR(".len()..close_idx].trim();
    let rest = raw[close_idx + 1..].trim();
    let eq = rest.strip_prefix('=').map(str::trim).ok_or_else(|| SqlError::new("YEAR() currently supports only = comparisons"))?;
    let year = eq.parse::<i32>().map_err(|_| SqlError::new("YEAR() comparison expects an integer year"))?;
    Ok(Some(lower_year_range(field_raw, year)?))
}

fn lower_year_range(field_raw: &str, year: i32) -> Result<Step, SqlError> {
    let start = Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).single().ok_or_else(|| SqlError::new("invalid EXTRACT YEAR lower bound"))?;
    let end = Utc.with_ymd_and_hms(year, 12, 31, 23, 59, 59).single().ok_or_else(|| SqlError::new("invalid EXTRACT YEAR upper bound"))?;
    Ok(Step::WhereBetween(
        exact_time_scalar_field(field_raw),
        start.timestamp_micros() as f64,
        end.timestamp_micros() as f64,
    ))
}

fn split_top_level_and(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut depth = 0i32;
    let chars: Vec<char> = raw.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        if ch == '\'' {
            in_quote = !in_quote;
            current.push(ch);
            i += 1;
            continue;
        }
        if !in_quote {
            if ch == '(' { depth += 1; }
            if ch == ')' { depth -= 1; }
            if depth == 0 && i + 5 <= chars.len() {
                let slice: String = chars[i..(i + 5)].iter().collect();
                if slice.eq_ignore_ascii_case(" AND ") {
                    out.push(current.trim().to_string());
                    current.clear();
                    i += 5;
                    continue;
                }
            }
        }
        current.push(ch);
        i += 1;
    }
    if !current.trim().is_empty() {
        out.push(current.trim().to_string());
    }
    out
}

fn find_keyword(raw: &str, keyword: &str) -> Option<usize> {
    raw.to_uppercase().find(&keyword.to_uppercase())
}

fn find_operator(raw: &str, op: &str) -> Option<usize> {
    let mut in_quote = false;
    for (idx, ch) in raw.char_indices() {
        if ch == '\'' {
            in_quote = !in_quote;
        }
        if !in_quote && raw[idx..].starts_with(op) {
            return Some(idx);
        }
    }
    None
}

fn normalize_field_name(raw: &str) -> String {
    raw.rsplit('.').next().unwrap_or(raw).trim().to_string()
}

fn looks_like_exact_time_field(raw: &str) -> bool {
    let field = normalize_field_name(raw);
    field.ends_with("_at") || field.ends_with("At") || field.ends_with("EpochMicros")
}

fn exact_time_scalar_field(raw: &str) -> String {
    let field = normalize_field_name(raw);
    if field.ends_with("EpochMicros") {
        return field;
    }
    if let Some(base) = field.strip_suffix("_at") {
        return format!("{}EpochMicros", snake_to_camel(base));
    }
    if let Some(base) = field.strip_suffix("At") {
        return format!("{}EpochMicros", base);
    }
    format!("{}EpochMicros", field)
}

fn snake_to_camel(raw: &str) -> String {
    let mut out = String::new();
    let mut upper = false;
    for ch in raw.chars() {
        if ch == '_' {
            upper = true;
            continue;
        }
        if upper {
            out.extend(ch.to_uppercase());
            upper = false;
        } else {
            out.push(ch);
        }
    }
    out
}

fn parse_value(raw: &str) -> Result<Value, SqlError> {
    if raw.eq_ignore_ascii_case("true") {
        return Ok(Value::Bool(true));
    }
    if raw.eq_ignore_ascii_case("false") {
        return Ok(Value::Bool(false));
    }
    if raw.eq_ignore_ascii_case("null") {
        return Ok(Value::Null);
    }
    if raw.starts_with('\'') && raw.ends_with('\'') {
        return Ok(Value::String(parse_string_literal(raw)?));
    }
    if let Ok(n) = raw.parse::<i64>() {
        return Ok(Value::from(n));
    }
    if let Ok(n) = raw.parse::<f64>() {
        return Ok(Value::from(n));
    }
    Ok(Value::String(raw.to_string()))
}

fn parse_numeric(raw: &str) -> Result<f64, SqlError> {
    raw.parse::<f64>()
        .map_err(|_| SqlError::new(format!("expected numeric literal, got: {raw}")))
}

fn parse_string_literal(raw: &str) -> Result<String, SqlError> {
    if raw.len() >= 2 && raw.starts_with('\'') && raw.ends_with('\'') {
        Ok(raw[1..raw.len() - 1].replace("''", "'"))
    } else {
        Err(SqlError::new(format!("expected string literal, got: {raw}")))
    }
}

fn is_timestamp_literal(raw: &str) -> bool {
    raw.to_uppercase().starts_with("TIMESTAMP ") || raw.to_uppercase().starts_with("DATE ")
}

fn parse_timestamp_literal(raw: &str) -> Result<i64, SqlError> {
    let upper = raw.to_uppercase();
    let value = if upper.starts_with("TIMESTAMP ") {
        parse_string_literal(raw["TIMESTAMP ".len()..].trim())?
    } else if upper.starts_with("DATE ") {
        parse_string_literal(raw["DATE ".len()..].trim())?
    } else {
        return Err(SqlError::new(format!("expected TIMESTAMP/DATE literal, got: {raw}")));
    };

    if let Ok(dt) = DateTime::parse_from_rfc3339(&value) {
        return Ok(dt.timestamp_micros());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).timestamp_micros());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).timestamp_micros());
    }
    if let Ok(date) = NaiveDate::parse_from_str(&value, "%Y-%m-%d") {
        let dt = date.and_hms_opt(0, 0, 0).ok_or_else(|| SqlError::new("invalid DATE literal"))?;
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).timestamp_micros());
    }

    Err(SqlError::new(format!("unsupported TIMESTAMP/DATE literal: {value}")))
}

fn lower_vector_near_clause(raw: &str) -> Result<Option<Step>, SqlError> {
    let upper = raw.to_uppercase();
    if !upper.starts_with("VECTOR_NEAR(") {
        return Ok(None);
    }
    let close_idx = raw.rfind(')').ok_or_else(|| SqlError::new("VECTOR_NEAR missing closing )"))?;
    let inner = raw["VECTOR_NEAR(".len()..close_idx].trim();
    let args = split_top_level_csv(inner);
    if args.len() != 3 {
        return Err(SqlError::new("VECTOR_NEAR expects: field, query_vector, k"));
    }
    let _field = normalize_field_name(&args[0]);
    let query = parse_vector_literal(&args[1])?;
    let k = args[2]
        .trim()
        .parse::<usize>()
        .map_err(|_| SqlError::new("VECTOR_NEAR k must be a positive integer"))?;
    Ok(Some(Step::Similar(query, k)))
}

fn lower_vector_order(raw: &str, limit: Option<usize>) -> Result<Option<Step>, SqlError> {
    for op in ["<=>", "<->", "<#>"] {
        if let Some(idx) = raw.find(op) {
            let _field = normalize_field_name(raw[..idx].trim());
            let rhs = raw[idx + op.len()..].trim();
            let query = parse_vector_literal(rhs)?;
            return Ok(Some(Step::Similar(query, limit.unwrap_or(50))));
        }
    }
    Ok(None)
}

fn split_top_level_csv(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut paren_depth = 0i32;
    let mut bracket_depth = 0i32;

    for ch in raw.chars() {
        match ch {
            '\'' => {
                in_quote = !in_quote;
                current.push(ch);
            }
            '(' if !in_quote => {
                paren_depth += 1;
                current.push(ch);
            }
            ')' if !in_quote => {
                paren_depth -= 1;
                current.push(ch);
            }
            '[' if !in_quote => {
                bracket_depth += 1;
                current.push(ch);
            }
            ']' if !in_quote => {
                bracket_depth -= 1;
                current.push(ch);
            }
            ',' if !in_quote && paren_depth == 0 && bracket_depth == 0 => {
                out.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        out.push(current.trim().to_string());
    }

    out
}

fn parse_vector_literal(raw: &str) -> Result<Vec<f32>, SqlError> {
    let raw = raw.trim();
    if raw.starts_with('$') {
        return Err(SqlError::new("bind parameters for vector queries are specified in the SQL spec but not lowered yet; use a literal vector in the current scaffold"));
    }
    if !(raw.starts_with('[') && raw.ends_with(']')) {
        return Err(SqlError::new(format!("expected vector literal like [0.1, 0.2], got: {raw}")));
    }
    let inner = raw[1..raw.len() - 1].trim();
    let mut values = if inner.is_empty() {
        Vec::new()
    } else {
        inner
            .split(',')
            .map(|part| {
                part.trim()
                    .parse::<f32>()
                    .map_err(|_| SqlError::new(format!("invalid vector component: {part}")))
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    if values.len() < 128 {
        values.resize(128, 0.0);
    } else if values.len() > 128 {
        values.truncate(128);
    }
    Ok(values)
}

fn lower_vague_time_clause(raw: &str) -> Result<Option<Step>, SqlError> {
    let upper = raw.to_uppercase();
    if !upper.starts_with("VAGUE_TIME_INTERSECTS(") {
        return Ok(None);
    }
    let close_idx = raw.rfind(')').ok_or_else(|| SqlError::new("VAGUE_TIME_INTERSECTS missing closing )"))?;
    let inner = raw["VAGUE_TIME_INTERSECTS(".len()..close_idx].trim();
    let args = split_top_level_csv(inner);
    if args.len() < 3 {
        return Err(SqlError::new("VAGUE_TIME_INTERSECTS expects: field, start_year, end_year"));
    }
    let field = normalize_field_name(&args[0]);
    let start_year = args[1].trim().parse::<i64>().map_err(|_| SqlError::new("VAGUE_TIME_INTERSECTS start_year must be an integer"))?;
    let end_year = args[2].trim().parse::<i64>().map_err(|_| SqlError::new("VAGUE_TIME_INTERSECTS end_year must be an integer"))?;
    Ok(Some(Step::TimeIntersects(
        field,
        crate::types::TimeQuery {
            start_year,
            end_year,
            start_fuzz_years: 0,
            end_fuzz_years: 0,
            months: Vec::new(),
            weekdays: Vec::new(),
            days_of_month: Vec::new(),
            time_of_day: None,
            recurrence_step_months: None,
            global_fuzziness: 0.0,
        },
    )))
}

fn lower_spatial_clause(raw: &str) -> Result<Option<Step>, SqlError> {
    let upper = raw.to_uppercase();
    if upper.starts_with("ST_DWITHIN(") {
        let close_idx = raw.rfind(')').ok_or_else(|| SqlError::new("ST_DWithin missing closing )"))?;
        let inner = raw["ST_DWithin(".len()..close_idx].trim();
        let args = split_top_level_csv(inner);
        if args.len() != 3 {
            return Err(SqlError::new("ST_DWithin expects: geometry_field, POINT(lon lat), distance_km"));
        }
        let point = args[1].trim();
        let point_upper = point.to_uppercase();
        if !(point_upper.starts_with("POINT(") && point.ends_with(')')) {
            return Err(SqlError::new("ST_DWithin point argument must look like POINT(lon lat)"));
        }
        let point_inner = point[6..point.len() - 1].trim();
        let coords: Vec<&str> = point_inner.split_whitespace().collect();
        if coords.len() != 2 {
            return Err(SqlError::new("POINT must contain lon lat"));
        }
        let lon = coords[0].parse::<f32>().map_err(|_| SqlError::new("POINT lon must be numeric"))?;
        let lat = coords[1].parse::<f32>().map_err(|_| SqlError::new("POINT lat must be numeric"))?;
        let distance = args[2].trim().parse::<f32>().map_err(|_| SqlError::new("ST_DWithin distance must be numeric"))?;
        return Ok(Some(Step::StDWithin(lat, lon, distance)));
    }
    if upper.starts_with("ST_WITHIN(") {
        let close_idx = raw.rfind(')').ok_or_else(|| SqlError::new("ST_Within missing closing )"))?;
        let inner = raw["ST_Within(".len()..close_idx].trim();
        let args = split_top_level_csv(inner);
        if args.len() != 2 {
            return Err(SqlError::new("ST_Within expects: geometry_field, POLYGON((lon lat, ...))"));
        }
        return Ok(Some(Step::StWithin(parse_polygon_wkt(args[1].trim())?)));
    }
    if upper.starts_with("ST_INTERSECTS(") {
        let close_idx = raw.rfind(')').ok_or_else(|| SqlError::new("ST_Intersects missing closing )"))?;
        let inner = raw["ST_Intersects(".len()..close_idx].trim();
        let args = split_top_level_csv(inner);
        if args.len() != 2 {
            return Err(SqlError::new("ST_Intersects expects: geometry_field, POLYGON((lon lat, ...))"));
        }
        return Ok(Some(Step::StIntersects(parse_polygon_wkt(args[1].trim())?)));
    }
    Ok(None)
}

fn parse_polygon_wkt(raw: &str) -> Result<Vec<[f32; 2]>, SqlError> {
    let upper = raw.to_uppercase();
    if !(upper.starts_with("POLYGON((") && raw.ends_with("))")) {
        return Err(SqlError::new("polygon argument must look like POLYGON((lon lat, ...))"));
    }
    let inner = &raw["POLYGON((".len()..raw.len() - 2];
    let mut ring = Vec::new();
    for pair in inner.split(',') {
        let parts: Vec<&str> = pair.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(SqlError::new("polygon vertices must be lon lat pairs"));
        }
        let lon = parts[0]
            .parse::<f32>()
            .map_err(|_| SqlError::new("polygon lon must be numeric"))?;
        let lat = parts[1]
            .parse::<f32>()
            .map_err(|_| SqlError::new("polygon lat must be numeric"))?;
        ring.push([lat, lon]);
    }
    if ring.len() < 4 {
        return Err(SqlError::new("polygon ring must have at least 4 vertices"));
    }
    Ok(ring)
}


#[cfg(feature = "fulltext")]
fn lower_matching_clause(raw: &str) -> Result<Option<Step>, SqlError> {
    let upper = raw.to_uppercase();
    if !upper.starts_with("MATCHING(") {
        return Ok(None);
    }
    let close_idx = raw.rfind(')').ok_or_else(|| SqlError::new("MATCHING missing closing )"))?;
    let inner = raw["MATCHING(".len()..close_idx].trim();
    let args = split_args(inner);
    let text_arg = match args.len() {
        1 => args[0].trim(),
        2 => args[1].trim(),
        _ => return Err(SqlError::new("MATCHING expects: MATCHING('query') or MATCHING(field, 'query')")),
    };
    Ok(Some(Step::Matching {
        text: parse_string_literal(text_arg)?,
        limit: 100,
        title_weight: 1.0,
        content_weight: 1.0,
    }))
}

#[cfg(not(feature = "fulltext"))]
fn lower_matching_clause(_raw: &str) -> Result<Option<Step>, SqlError> {
    Ok(None)
}


fn lower_source_pushdown_clause(raw: &str, collection: &str) -> Result<Option<Step>, SqlError> {
    for op in ["="] {
        if let Some(idx) = find_operator(raw, op) {
            let left = normalize_field_name(raw[..idx].trim());
            let right = raw[idx + op.len()..].trim();
            if left == "_id" {
                let slug = parse_string_literal(right)?;
                return Ok(Some(Step::One(seahash::hash(slug.as_bytes()))));
            }
            if left == "id" || left == "_key" {
                let key = parse_string_literal(right)?;
                let slug = format!("{}/{}", collection, key);
                return Ok(Some(Step::One(seahash::hash(slug.as_bytes()))));
            }
        }
    }
    Ok(None)
}


fn split_args(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut depth = 0i32;
    for ch in raw.chars() {
        match ch {
            '\'' => {
                in_quote = !in_quote;
                current.push(ch);
            }
            '(' if !in_quote => {
                depth += 1;
                current.push(ch);
            }
            ')' if !in_quote => {
                depth -= 1;
                current.push(ch);
            }
            ',' if !in_quote && depth == 0 => {
                out.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        out.push(current.trim().to_string());
    }
    out
}
