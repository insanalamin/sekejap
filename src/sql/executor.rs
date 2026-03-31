use crate::db::SekejapDB;
use crate::sql::ast::{
    CollectionOptions, CreateCollectionStatement, DefaultExpr, DeleteStatement, Expr,
    InsertStatement, RelateStatement, SqlStatement, SqlType, UnrelateStatement, UpdateStatement,
};
use crate::sql::lowering::lower_statement;
use crate::sql::parser::{parse_sql, SqlError};
use crate::set::Set;
use crate::types::{Hit, Outcome, Step};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde_json::{Map, Value};
use uuid::Uuid;

pub fn lower_sql_query(input: &str) -> Result<Vec<Step>, Box<dyn std::error::Error>> {
    let statement = parse_sql(input)?;
    match &statement {
        SqlStatement::Select(_) => Ok(lower_statement(&statement)?),
        _ => Err("only SELECT can be lowered into query steps".into()),
    }
}

pub fn execute_sql_query(
    db: &SekejapDB,
    input: &str,
) -> Result<Outcome<Vec<Hit>>, Box<dyn std::error::Error>> {
    Set::from_steps(db, lower_sql_query(input)?).collect()
}

pub fn execute_sql_mutation(
    db: &SekejapDB,
    input: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    match parse_sql(input)? {
        SqlStatement::CreateCollection(stmt) => execute_create_collection(db, &stmt),
        SqlStatement::Insert(stmt) => execute_insert(db, &stmt),
        SqlStatement::Relate(stmt) => execute_relate(db, &stmt),
        SqlStatement::Update(stmt) => execute_update(db, &stmt),
        SqlStatement::Delete(stmt) => execute_delete(db, &stmt),
        SqlStatement::Unrelate(stmt) => execute_unrelate(db, &stmt),
        SqlStatement::Select(_) => Err("SELECT must be executed via query(), not mutate()".into()),
    }
}

fn execute_create_collection(
    db: &SekejapDB,
    stmt: &CreateCollectionStatement,
) -> Result<Value, Box<dyn std::error::Error>> {
    let mut hash_index = dedup(stmt.options.hash_index.clone());
    let mut range_index = stmt
        .options
        .range_index
        .iter()
        .map(|field| {
            stmt.fields
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(field))
                .and_then(|candidate| match candidate.data_type {
                    SqlType::Timestamp => Some(exact_time_scalar_field(&candidate.name)),
                    _ => None,
                })
                .unwrap_or_else(|| field.clone())
        })
        .collect::<Vec<_>>();
    range_index = dedup(range_index);
    let mut temporal_index = dedup(stmt.options.temporal_index.clone());
    let mut spatial_index = dedup(stmt.options.spatial_index.clone());
    let mut vector_index = dedup(stmt.options.vector_index.clone());
    let fulltext_index = dedup(stmt.options.fulltext_index.clone());

    for field in &stmt.fields {
        match field.data_type {
            SqlType::Timestamp => push_unique(&mut range_index, exact_time_scalar_field(&field.name)),
            SqlType::VagueTime => push_unique(&mut temporal_index, field.name.clone()),
            SqlType::Geometry => push_unique(&mut spatial_index, field.name.clone()),
            SqlType::Vector(_) => push_unique(&mut vector_index, field.name.clone()),
            _ => {}
        }
        if field.primary_key {
            push_unique(&mut hash_index, field.name.clone());
        }
    }

    let fields_json: Vec<Value> = stmt
        .fields
        .iter()
        .map(|field| {
            serde_json::json!({
                "name": field.name,
                "type": sql_type_name(&field.data_type),
                "primary_key": field.primary_key,
                "default": field.default.as_ref().map(default_expr_name),
            })
        })
        .collect();

    let schema_json = serde_json::json!({
        "fields": fields_json,
        "hot_fields": {
            "hash_index": hash_index,
            "range_index": range_index,
            "temporal": temporal_index,
            "spatial": spatial_index,
            "vector": vector_index,
            "fulltext": fulltext_index,
        }
    });

    db.schema().define(&stmt.name, &schema_json.to_string())?;

    if !stmt.fields.iter().all(|field| !matches!(field.data_type, SqlType::Vector(_)))
        && db.hnsw.read().is_none()
    {
        db.init_hnsw(16);
    }

    Ok(serde_json::json!({
        "ok": true,
        "sql": "create_collection",
        "collection": stmt.name,
        "schema": db.describe_collection(&stmt.name),
    }))
}

fn execute_insert(db: &SekejapDB, stmt: &InsertStatement) -> Result<Value, Box<dyn std::error::Error>> {
    let hash = seahash::hash(stmt.table.as_bytes());
    let schema = db
        .collections
        .get(&hash)
        .ok_or_else(|| format!("collection not defined: {}", stmt.table))?;
    let field_defs = schema.field_defs.clone();
    drop(schema);

    let mut bulk_items: Vec<(String, String, Value)> = Vec::with_capacity(stmt.rows.len());
    let mut any_vector_payload = false;

    for row in &stmt.rows {
        if stmt.columns.len() != row.len() {
            return Err("INSERT column count does not match value count".into());
        }
        let (slug, payload_json, payload_value, has_vector_payload) =
            build_insert_payload(stmt, &field_defs, row)?;
        any_vector_payload |= has_vector_payload;
        bulk_items.push((slug, payload_json, payload_value));
    }

    let item_refs = bulk_items
        .iter()
        .map(|(slug, payload, value)| (slug.as_str(), payload.as_str(), value))
        .collect::<Vec<_>>();

    let (indices, _) = db.ingest_nodes_raw_values(&item_refs)?;
    if any_vector_payload {
        if db.hnsw.read().is_none() {
            db.init_hnsw(16);
        }
        let _ = db.nodes().build_hnsw();
    }

    Ok(serde_json::json!({
        "ok": true,
        "sql": "insert",
        "collection": stmt.table,
        "rows": item_refs.len(),
        "idx": indices.last().copied(),
        "slug": bulk_items.last().map(|(slug, _, _)| slug.clone()).unwrap_or_default(),
    }))
}

fn execute_relate(db: &SekejapDB, stmt: &RelateStatement) -> Result<Value, Box<dyn std::error::Error>> {
    let plain_edges = stmt
        .edges
        .iter()
        .filter(|edge| edge.meta_json.is_none())
        .map(|edge| {
            (
                edge.source.as_str(),
                edge.target.as_str(),
                edge.edge_type.as_str(),
                edge.weight,
            )
        })
        .collect::<Vec<_>>();

    if !plain_edges.is_empty() {
        db.edges().ingest(&plain_edges)?;
    }

    let mut meta_count = 0usize;
    for edge in stmt.edges.iter().filter(|edge| edge.meta_json.is_some()) {
        let meta_json = edge.meta_json.as_ref().unwrap();
        db.edges()
            .link_meta(&edge.source, &edge.target, &edge.edge_type, edge.weight, meta_json)?;
        meta_count += 1;
    }

    Ok(serde_json::json!({
        "ok": true,
        "sql": "relate",
        "rows": stmt.edges.len(),
        "plain_rows": plain_edges.len(),
        "meta_rows": meta_count,
    }))
}

fn execute_update(db: &SekejapDB, stmt: &UpdateStatement) -> Result<Value, Box<dyn std::error::Error>> {
    let selection = require_where_clause(stmt.selection.as_ref(), "UPDATE")?;
    let hash = seahash::hash(stmt.table.as_bytes());
    let schema = db
        .collections
        .get(&hash)
        .ok_or_else(|| format!("collection not defined: {}", stmt.table))?;
    let field_defs = schema.field_defs.clone();
    drop(schema);

    for (field, _) in &stmt.assignments {
        let forbidden = field.eq_ignore_ascii_case("_id")
            || field.eq_ignore_ascii_case("_key")
            || field_defs.iter().any(|f| f.primary_key && f.name.eq_ignore_ascii_case(field));
        if forbidden {
            return Err(format!("UPDATE cannot modify identity field: {field}").into());
        }
    }

    let hits = db
        .query(&format!("SELECT * FROM {} WHERE {}", stmt.table, selection))?
        .data;
    let mut updated = 0usize;
    for hit in hits {
        let payload = hit.payload.as_deref().ok_or("UPDATE target missing payload")?;
        let mut value: Value = serde_json::from_str(payload)?;
        let object = value
            .as_object_mut()
            .ok_or("UPDATE target payload must be an object")?;
        let slug = object
            .get("_id")
            .and_then(|v| v.as_str())
            .ok_or("UPDATE target missing _id")?
            .to_string();

        for (field, expr) in &stmt.assignments {
            let field_def = field_defs
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(field));
            apply_update_value(object, field, expr, field_def)?;
        }

        let result = db.mutate(&serde_json::json!({
            "mutation": "put",
            "slug": slug,
            "data": Value::Object(object.clone()),
        }).to_string())?;
        if result["ok"].as_bool() == Some(true) {
            updated += 1;
        }
    }

    Ok(serde_json::json!({
        "ok": true,
        "sql": "update",
        "collection": stmt.table,
        "rows": updated,
    }))
}

fn execute_delete(db: &SekejapDB, stmt: &DeleteStatement) -> Result<Value, Box<dyn std::error::Error>> {
    let selection = require_where_clause(stmt.selection.as_ref(), "DELETE")?;
    let hits = db
        .query(&format!("SELECT _id FROM {} WHERE {}", stmt.table, selection))?
        .data;
    let mut deleted = 0usize;
    for hit in hits {
        let payload = hit.payload.as_deref().ok_or("DELETE target missing payload")?;
        let value: Value = serde_json::from_str(payload)?;
        let slug = value
            .get("_id")
            .and_then(|v| v.as_str())
            .ok_or("DELETE target missing _id")?;
        let result = db.mutate(&serde_json::json!({
            "mutation": "remove",
            "slug": slug,
        }).to_string())?;
        if result["ok"].as_bool() == Some(true) {
            deleted += 1;
        }
    }
    Ok(serde_json::json!({
        "ok": true,
        "sql": "delete",
        "collection": stmt.table,
        "rows": deleted,
    }))
}

fn execute_unrelate(
    db: &SekejapDB,
    stmt: &UnrelateStatement,
) -> Result<Value, Box<dyn std::error::Error>> {
    db.edges().unlink(&stmt.source, &stmt.target, &stmt.edge_type)?;
    Ok(serde_json::json!({
        "ok": true,
        "sql": "unrelate",
        "rows": 1,
    }))
}

fn build_insert_payload(
    stmt: &InsertStatement,
    field_defs: &[crate::types::CollectionFieldDef],
    row: &[Expr],
) -> Result<(String, String, Value, bool), Box<dyn std::error::Error>> {
    let mut object = Map::new();
    object.insert("_collection".to_string(), Value::String(stmt.table.clone()));

    for (column, expr) in stmt.columns.iter().zip(row.iter()) {
        let field_def = field_defs
            .iter()
            .find(|field| field.name.eq_ignore_ascii_case(column));
        apply_insert_value(&mut object, column, expr, field_def)?;
    }

    for field in field_defs {
        if !object.contains_key(&field.name) {
            if let Some(default) = parse_default_expr(field.default_expr.as_deref()) {
                match default {
                    DefaultExpr::UuidV4 => {
                        let value = Uuid::new_v4().to_string();
                        object.insert(field.name.clone(), Value::String(value));
                    }
                }
            }
        }
    }

    let primary_key_field = field_defs
        .iter()
        .find(|field| field.primary_key)
        .map(|field| field.name.clone())
        .or_else(|| {
            field_defs
                .iter()
                .find(|field| field.name == "id")
                .map(|field| field.name.clone())
        });

    if let Some(pk_field) = primary_key_field {
        if let Some(pk) = object.get(&pk_field).and_then(|value| value.as_str()) {
            object.insert("_key".to_string(), Value::String(pk.to_string()));
        }
    }

    if !object.contains_key("_key") {
        object.insert("_key".to_string(), Value::String(Uuid::new_v4().to_string()));
    }

    let key = object
        .get("_key")
        .and_then(|value| value.as_str())
        .ok_or("SQL insert payload missing _key")?;
    let slug = format!("{}/{}", stmt.table, key);
    object.insert("_id".to_string(), Value::String(slug.clone()));

    let has_vector_payload = payload_has_vector(&object);
    let payload = Value::Object(object);
    let payload_json = payload.to_string();
    Ok((slug, payload_json, payload, has_vector_payload))
}

fn apply_insert_value(
    object: &mut Map<String, Value>,
    column: &str,
    expr: &Expr,
    field_def: Option<&crate::types::CollectionFieldDef>,
) -> Result<(), Box<dyn std::error::Error>> {
    let field_type = field_def.map(|field| field.field_type.as_str()).unwrap_or("TEXT");
    match field_type {
        "TIMESTAMP" => {
            let text = extract_timestamp_text(expr)?;
            object.insert(column.to_string(), Value::String(text.clone()));
            object.insert(exact_time_scalar_field(column), Value::from(parse_timestamp_literal_to_micros(&text)?));
        }
        "VAGUE_TIME" | "GEOMETRY" | "JSON" => {
            object.insert(column.to_string(), expr_to_structured_json(expr)?);
        }
        "VECTOR" => {
            let vector = expr_to_f32_vec(expr)?;
            object.insert(
                column.to_string(),
                Value::Array(vector.iter().map(|v| Value::from(*v)).collect()),
            );
            let vectors = object
                .entry("vectors".to_string())
                .or_insert_with(|| Value::Object(Map::new()));
            if let Some(map) = vectors.as_object_mut() {
                map.insert(
                    "dense".to_string(),
                    Value::Array(vector.iter().map(|v| Value::from(*v)).collect()),
                );
            }
        }
        _ => {
            object.insert(column.to_string(), expr_to_json(expr)?);
        }
    }
    Ok(())
}

fn apply_update_value(
    object: &mut Map<String, Value>,
    column: &str,
    expr: &Expr,
    field_def: Option<&crate::types::CollectionFieldDef>,
) -> Result<(), Box<dyn std::error::Error>> {
    if matches!(expr, Expr::Null) {
        object.insert(column.to_string(), Value::Null);
        if field_def.map(|f| f.field_type.as_str()) == Some("TIMESTAMP") {
            object.remove(&exact_time_scalar_field(column));
        }
        if field_def.map(|f| f.field_type.as_str()) == Some("VECTOR") {
            if let Some(vectors) = object.get_mut("vectors").and_then(|v| v.as_object_mut()) {
                vectors.remove("dense");
            }
        }
        return Ok(());
    }
    apply_insert_value(object, column, expr, field_def)
}

fn expr_to_json(expr: &Expr) -> Result<Value, Box<dyn std::error::Error>> {
    Ok(match expr {
        Expr::StringLiteral(value) | Expr::TimestampLiteral(value) | Expr::Identifier(value) => {
            Value::String(value.clone())
        }
        Expr::Integer(value) => Value::from(*value),
        Expr::Float(value) => Value::from(*value),
        Expr::Boolean(value) => Value::Bool(*value),
        Expr::Null => Value::Null,
        Expr::ArrayLiteral(values) => Value::Array(
            values
                .iter()
                .map(expr_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Expr::FunctionCall { .. } | Expr::Raw(_) => {
            return Err("unsupported INSERT value expression".into())
        }
    })
}

fn expr_to_f32_vec(expr: &Expr) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
    match expr {
        Expr::ArrayLiteral(values) => values
            .iter()
            .map(|value| match value {
                Expr::Integer(v) => Ok(*v as f32),
                Expr::Float(v) => Ok(*v as f32),
                _ => Err("vector components must be numeric".into()),
            })
            .collect(),
        _ => Err("VECTOR fields require array literals".into()),
    }
}

fn extract_timestamp_text(expr: &Expr) -> Result<String, Box<dyn std::error::Error>> {
    match expr {
        Expr::TimestampLiteral(value) | Expr::StringLiteral(value) => Ok(value.clone()),
        _ => Err("TIMESTAMP fields require TIMESTAMP or string literals".into()),
    }
}

fn parse_timestamp_literal_to_micros(raw: &str) -> Result<i64, Box<dyn std::error::Error>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Ok(dt.timestamp_micros());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).timestamp_micros());
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M") {
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).timestamp_micros());
    }
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        let dt = date.and_hms_opt(0, 0, 0).ok_or("invalid DATE literal")?;
        return Ok(DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).timestamp_micros());
    }
    Err(format!("unsupported TIMESTAMP literal: {raw}").into())
}

fn exact_time_scalar_field(raw: &str) -> String {
    if let Some(base) = raw.strip_suffix("_at") {
        return format!("{}EpochMicros", snake_to_camel(base));
    }
    if let Some(base) = raw.strip_suffix("At") {
        return format!("{}EpochMicros", base);
    }
    format!("{}EpochMicros", raw)
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

fn sql_type_name(kind: &SqlType) -> String {
    match kind {
        SqlType::Text => "TEXT".to_string(),
        SqlType::Int => "INT".to_string(),
        SqlType::Float => "FLOAT".to_string(),
        SqlType::Bool => "BOOL".to_string(),
        SqlType::Json => "JSON".to_string(),
        SqlType::Timestamp => "TIMESTAMP".to_string(),
        SqlType::VagueTime => "VAGUE_TIME".to_string(),
        SqlType::Geometry => "GEOMETRY".to_string(),
        SqlType::Uuid => "UUID".to_string(),
        SqlType::Vector(dim) => format!("VECTOR({dim})"),
    }
}

fn default_expr_name(default: &DefaultExpr) -> &'static str {
    match default {
        DefaultExpr::UuidV4 => "uuidv4()",
    }
}

fn parse_default_expr(raw: Option<&str>) -> Option<DefaultExpr> {
    match raw {
        Some(value) if value.eq_ignore_ascii_case("uuidv4()") => Some(DefaultExpr::UuidV4),
        _ => None,
    }
}

fn push_unique(list: &mut Vec<String>, value: String) {
    if !list.iter().any(|item| item == &value) {
        list.push(value);
    }
}

fn dedup(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    for value in values {
        push_unique(&mut out, value);
    }
    out
}

fn payload_has_vector(object: &Map<String, Value>) -> bool {
    object
        .get("vectors")
        .and_then(|value| value.get("dense"))
        .and_then(|value| value.as_array())
        .map(|value| !value.is_empty())
        .unwrap_or(false)
}

fn expr_to_structured_json(expr: &Expr) -> Result<Value, Box<dyn std::error::Error>> {
    match expr {
        Expr::StringLiteral(value) => Ok(serde_json::from_str(value)?),
        Expr::Null => Ok(Value::Null),
        _ => Err("structured JSON fields require a JSON string literal".into()),
    }
}

fn require_where_clause<'a>(
    selection: Option<&'a Expr>,
    stmt_name: &str,
) -> Result<&'a str, Box<dyn std::error::Error>> {
    match selection {
        Some(Expr::Raw(raw)) if !raw.trim().is_empty() => Ok(raw.trim()),
        _ => Err(format!("{stmt_name} requires a WHERE clause").into()),
    }
}
