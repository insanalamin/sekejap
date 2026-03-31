use crate::sql::ast::{
    CollectionOptions, ColumnDef, CreateCollectionStatement, DefaultExpr, DeleteStatement, Expr,
    InsertStatement, RelateEdge, RelateStatement, SelectItem, SqlStatement, SqlType, TableRef,
    TraverseClause, TraverseDirection, UnrelateStatement, UpdateStatement,
};
use serde_json::Value;
use sqlparser::ast as spast;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub struct SqlError {
    message: String,
}

impl SqlError {
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

impl Display for SqlError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for SqlError {}

pub struct SqlCompiler;

impl SqlCompiler {
    pub fn parse(input: &str) -> Result<SqlStatement, SqlError> {
        parse_sql(input)
    }
}

pub fn parse_sql(input: &str) -> Result<SqlStatement, SqlError> {
    let sql = input.trim().trim_end_matches(';').trim();
    if sql.is_empty() {
        return Err(SqlError::new("empty SQL query"));
    }
    let upper = sql.to_uppercase();
    if upper.starts_with("SELECT ") {
        parse_select(sql)
    } else if upper.starts_with("CREATE COLLECTION ") {
        parse_create_collection(sql)
    } else if upper.starts_with("INSERT INTO ") {
        parse_insert(sql)
    } else if upper.starts_with("RELATE ") {
        parse_relate(sql)
    } else if upper.starts_with("UPDATE ") {
        parse_update(sql)
    } else if upper.starts_with("DELETE FROM ") {
        parse_delete(sql)
    } else if upper.starts_with("UNRELATE ") {
        parse_unrelate(sql)
    } else {
        Err(SqlError::new("unsupported SQL statement"))
    }
}

fn parse_select(sql: &str) -> Result<SqlStatement, SqlError> {
    let upper = sql.to_uppercase();
    let from_idx = upper.find(" FROM ").ok_or_else(|| SqlError::new("missing FROM clause"))?;
    let select_part = sql[6..from_idx].trim();
    let remainder = &sql[from_idx + 6..];

    let rem_upper = remainder.to_uppercase();
    let where_idx = rem_upper.find(" WHERE ");
    let traverse_idx = rem_upper.find(" TRAVERSE ");
    let order_idx = rem_upper.find(" ORDER BY ");
    let limit_idx = rem_upper.find(" LIMIT ");
    let offset_idx = rem_upper.find(" OFFSET ");

    let first_clause_idx = [where_idx, traverse_idx, order_idx, limit_idx, offset_idx]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(remainder.len());

    let from_part = remainder[..first_clause_idx].trim();
    let from = parse_table_ref(from_part)?;

    let traverse = traverse_idx
        .map(|idx| {
            let start = idx + " TRAVERSE ".len();
            let end = [where_idx, order_idx, limit_idx, offset_idx]
                .into_iter()
                .flatten()
                .filter(|candidate| *candidate > idx)
                .min()
                .unwrap_or(remainder.len());
            parse_traverse_clause(remainder[start..end].trim())
        })
        .transpose()?;

    let selection = where_idx.map(|idx| {
        let start = idx + " WHERE ".len();
        let end = [order_idx, limit_idx, offset_idx]
            .into_iter()
            .flatten()
            .filter(|candidate| *candidate > idx)
            .min()
            .unwrap_or(remainder.len());
        Expr::Raw(remainder[start..end].trim().to_string())
    });

    let order_by = if let Some(idx) = order_idx {
        let start = idx + " ORDER BY ".len();
        let end = [limit_idx, offset_idx]
            .into_iter()
            .flatten()
            .filter(|candidate| *candidate > idx)
            .min()
            .unwrap_or(remainder.len());
        parse_order_by(remainder[start..end].trim())?
    } else {
        Vec::new()
    };

    let limit = if let Some(idx) = limit_idx {
        let start = idx + " LIMIT ".len();
        let end = [offset_idx]
            .into_iter()
            .flatten()
            .filter(|candidate| *candidate > idx)
            .min()
            .unwrap_or(remainder.len());
        Some(parse_usize(remainder[start..end].trim(), "LIMIT")?)
    } else {
        None
    };

    let offset = if let Some(idx) = offset_idx {
        let start = idx + " OFFSET ".len();
        Some(parse_usize(remainder[start..].trim(), "OFFSET")?)
    } else {
        None
    };

    Ok(SqlStatement::Select(crate::sql::ast::SelectStatement {
        projection: parse_projection(select_part),
        from,
        traverse,
        selection,
        order_by,
        limit,
        offset,
        raw: sql.to_string(),
    }))
}

fn parse_create_collection(sql: &str) -> Result<SqlStatement, SqlError> {
    let prefix = "CREATE COLLECTION ";
    let after = sql[prefix.len()..].trim();
    let open_idx = after.find('(').ok_or_else(|| SqlError::new("CREATE COLLECTION missing field list"))?;
    let name = after[..open_idx].trim().to_string();
    if name.is_empty() {
        return Err(SqlError::new("CREATE COLLECTION missing collection name"));
    }
    let fields_close = find_matching(after, open_idx, '(', ')')?;
    let fields_raw = &after[open_idx + 1..fields_close];
    let fields = split_top_level_csv(fields_raw)
        .into_iter()
        .filter(|s| !s.trim().is_empty())
        .map(|s| parse_column_def(&s))
        .collect::<Result<Vec<_>, _>>()?;

    let mut options = CollectionOptions::default();
    let rest = after[fields_close + 1..].trim();
    if !rest.is_empty() {
        let upper = rest.to_uppercase();
        if !upper.starts_with("WITH") {
            return Err(SqlError::new("unsupported CREATE COLLECTION trailing clause"));
        }
        let with_body = rest[4..].trim();
        if !with_body.starts_with('(') {
            return Err(SqlError::new("WITH clause must use parentheses"));
        }
        let close_idx = find_matching(with_body, 0, '(', ')')?;
        options = parse_collection_options(&with_body[1..close_idx])?;
    }

    Ok(SqlStatement::CreateCollection(CreateCollectionStatement {
        name,
        fields,
        options,
        raw: sql.to_string(),
    }))
}

fn parse_insert(sql: &str) -> Result<SqlStatement, SqlError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|err| SqlError::new(format!("INSERT parse error: {err}")))?;
    let stmt = statements
        .into_iter()
        .next()
        .ok_or_else(|| SqlError::new("empty INSERT statement"))?;

    let spast::Statement::Insert(insert) = stmt else {
        return Err(SqlError::new("expected INSERT statement"));
    };

    let table = object_name_to_string(&insert.table_name)?;
    let columns = insert
        .columns
        .into_iter()
        .map(|ident| ident.value)
        .collect::<Vec<_>>();

    let source = insert
        .source
        .ok_or_else(|| SqlError::new("INSERT INTO currently supports VALUES only"))?;
    let rows = extract_insert_rows(*source.body)?;

    Ok(SqlStatement::Insert(InsertStatement {
        table,
        columns,
        rows,
        raw: sql.to_string(),
    }))
}

fn object_name_to_string(name: &spast::ObjectName) -> Result<String, SqlError> {
    let joined = name
        .0
        .iter()
        .map(|part| part.to_string())
        .collect::<Vec<_>>()
        .join(".");
    if joined.is_empty() {
        Err(SqlError::new("missing table name"))
    } else {
        Ok(joined)
    }
}

fn extract_insert_rows(body: spast::SetExpr) -> Result<Vec<Vec<Expr>>, SqlError> {
    match body {
        spast::SetExpr::Values(values) => {
            let rows = values
                .rows
                .into_iter()
                .map(|row| row.into_iter().map(convert_sqlparser_expr).collect())
                .collect::<Result<Vec<Vec<_>>, _>>()?;
            if rows.is_empty() {
                return Err(SqlError::new("INSERT INTO VALUES requires at least one row"));
            }
            Ok(rows)
        }
        _ => Err(SqlError::new("INSERT INTO currently supports VALUES only")),
    }
}

fn convert_sqlparser_expr(expr: spast::Expr) -> Result<Expr, SqlError> {
    match expr {
        spast::Expr::Identifier(ident) => Ok(Expr::Identifier(ident.value)),
        spast::Expr::Value(value) => match value {
            spast::Value::SingleQuotedString(v)
            | spast::Value::DoubleQuotedString(v)
            | spast::Value::TripleSingleQuotedString(v)
            | spast::Value::TripleDoubleQuotedString(v)
            | spast::Value::EscapedStringLiteral(v)
            | spast::Value::SingleQuotedByteStringLiteral(v)
            | spast::Value::DoubleQuotedByteStringLiteral(v)
            | spast::Value::NationalStringLiteral(v)
            | spast::Value::HexStringLiteral(v) => Ok(Expr::StringLiteral(v)),
            spast::Value::Number(v, _) => {
                if v.contains('.') || v.contains('e') || v.contains('E') {
                    v.parse::<f64>()
                        .map(Expr::Float)
                        .map_err(|_| SqlError::new(format!("invalid float literal: {v}")))
                } else {
                    v.parse::<i64>()
                        .map(Expr::Integer)
                        .map_err(|_| SqlError::new(format!("invalid integer literal: {v}")))
                }
            }
            spast::Value::Boolean(v) => Ok(Expr::Boolean(v)),
            spast::Value::Null => Ok(Expr::Null),
            other => Err(SqlError::new(format!("unsupported INSERT literal: {other}"))),
        },
        spast::Expr::Array(array) => Ok(Expr::ArrayLiteral(
            array
                .elem
                .into_iter()
                .map(convert_sqlparser_expr)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        spast::Expr::TypedString { data_type, value } => {
            let upper = data_type.to_string().to_uppercase();
            if upper == "TIMESTAMP" || upper == "DATETIME" {
                Ok(Expr::TimestampLiteral(value))
            } else {
                Ok(Expr::StringLiteral(value))
            }
        }
        spast::Expr::Cast { expr, data_type, .. } => {
            let mut out = convert_sqlparser_expr(*expr)?;
            let upper = data_type.to_string().to_uppercase();
            if upper == "TIMESTAMP" || upper == "DATETIME" {
                if let Expr::StringLiteral(value) = out {
                    out = Expr::TimestampLiteral(value);
                }
            }
            Ok(out)
        }
        spast::Expr::Function(function) => {
            let name = function.name.to_string();
            let mut args = Vec::new();
            match function.args {
                spast::FunctionArguments::None => {}
                spast::FunctionArguments::Subquery(_) => {
                    return Err(SqlError::new("subquery function args are not supported in INSERT values"));
                }
                spast::FunctionArguments::List(list) => {
                    for arg in list.args {
                        match arg {
                            spast::FunctionArg::Unnamed(spast::FunctionArgExpr::Expr(expr)) => {
                                args.push(convert_sqlparser_expr(expr)?);
                            }
                            spast::FunctionArg::Named { arg, .. } | spast::FunctionArg::ExprNamed { arg, .. } => {
                                match arg {
                                    spast::FunctionArgExpr::Expr(expr) => args.push(convert_sqlparser_expr(expr)?),
                                    _ => return Err(SqlError::new("unsupported named function arg in INSERT values")),
                                }
                            }
                            _ => return Err(SqlError::new("unsupported function arg in INSERT values")),
                        }
                    }
                }
            }
            Ok(Expr::FunctionCall { name, args })
        }
        spast::Expr::UnaryOp { op, expr } => match op {
            spast::UnaryOperator::Minus => match convert_sqlparser_expr(*expr)? {
                Expr::Integer(v) => Ok(Expr::Integer(-v)),
                Expr::Float(v) => Ok(Expr::Float(-v)),
                _ => Err(SqlError::new("unsupported unary minus in INSERT values")),
            },
            spast::UnaryOperator::Plus => convert_sqlparser_expr(*expr),
            _ => Err(SqlError::new("unsupported unary operator in INSERT values")),
        },
        other => Err(SqlError::new(format!("unsupported INSERT value expression: {other}"))),
    }
}

fn parse_relate(sql: &str) -> Result<SqlStatement, SqlError> {
    let prefix = "RELATE ";
    let after = sql[prefix.len()..].trim();
    let upper = after.to_uppercase();
    let edges = if upper.starts_with("MANY") {
        let body = after[4..].trim();
        if !body.starts_with('(') {
            return Err(SqlError::new("RELATE MANY must use parentheses"));
        }
        let close = find_matching(body, 0, '(', ')')?;
        let inner = &body[1..close];
        let mut edges = Vec::new();
        for item in split_top_level_csv(inner) {
            let item = item.trim();
            if !item.is_empty() {
                edges.push(parse_relate_edge(item)?);
            }
        }
        if edges.is_empty() {
            return Err(SqlError::new("RELATE MANY requires at least one edge"));
        }
        edges
    } else {
        vec![parse_relate_edge(after)?]
    };

    Ok(SqlStatement::Relate(RelateStatement {
        edges,
        raw: sql.to_string(),
    }))
}

fn parse_update(sql: &str) -> Result<SqlStatement, SqlError> {
    let upper = sql.to_uppercase();
    let set_idx = upper
        .find(" SET ")
        .ok_or_else(|| SqlError::new("UPDATE missing SET clause"))?;
    let table = sql["UPDATE ".len()..set_idx].trim().to_string();
    if table.is_empty() {
        return Err(SqlError::new("UPDATE missing table name"));
    }
    let rest = &sql[set_idx + " SET ".len()..];
    let rest_upper = rest.to_uppercase();
    let where_idx = rest_upper.find(" WHERE ");
    let set_part = match where_idx {
        Some(idx) => &rest[..idx],
        None => rest,
    };
    let assignments = split_top_level_csv(set_part)
        .into_iter()
        .map(|item| {
            let Some(eq_idx) = item.find('=') else {
                return Err(SqlError::new(format!("invalid UPDATE assignment: {item}")));
            };
            let field = item[..eq_idx].trim().to_string();
            let expr = parse_value_expr(item[eq_idx + 1..].trim())?;
            Ok((field, expr))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if assignments.is_empty() {
        return Err(SqlError::new("UPDATE requires at least one assignment"));
    }
    let selection = where_idx.map(|idx| Expr::Raw(rest[idx + " WHERE ".len()..].trim().to_string()));
    Ok(SqlStatement::Update(UpdateStatement {
        table,
        assignments,
        selection,
        raw: sql.to_string(),
    }))
}

fn parse_delete(sql: &str) -> Result<SqlStatement, SqlError> {
    let prefix = "DELETE FROM ";
    let rest = &sql[prefix.len()..];
    let rest_upper = rest.to_uppercase();
    let where_idx = rest_upper.find(" WHERE ");
    let table = match where_idx {
        Some(idx) => rest[..idx].trim(),
        None => rest.trim(),
    };
    if table.is_empty() {
        return Err(SqlError::new("DELETE FROM missing table name"));
    }
    let selection = where_idx.map(|idx| Expr::Raw(rest[idx + " WHERE ".len()..].trim().to_string()));
    Ok(SqlStatement::Delete(DeleteStatement {
        table: table.to_string(),
        selection,
        raw: sql.to_string(),
    }))
}

fn parse_unrelate(sql: &str) -> Result<SqlStatement, SqlError> {
    let raw = sql["UNRELATE ".len()..].trim();
    let edge = parse_relate_edge(raw)?;
    Ok(SqlStatement::Unrelate(UnrelateStatement {
        source: edge.source,
        edge_type: edge.edge_type,
        target: edge.target,
        raw: sql.to_string(),
    }))
}

fn parse_relate_edge(raw: &str) -> Result<RelateEdge, SqlError> {
    let first_arrow = raw
        .find("->")
        .ok_or_else(|| SqlError::new("RELATE requires source -> edge_type -> target"))?;
    let second_arrow = raw[first_arrow + 2..]
        .find("->")
        .map(|idx| idx + first_arrow + 2)
        .ok_or_else(|| SqlError::new("RELATE requires source -> edge_type -> target"))?;

    let source = raw[..first_arrow].trim().to_string();
    let edge_type = raw[first_arrow + 2..second_arrow].trim().to_string();
    let tail = raw[second_arrow + 2..].trim();

    if source.is_empty() || edge_type.is_empty() || tail.is_empty() {
        return Err(SqlError::new("RELATE requires source, edge type, and target"));
    }

    let mut tail_parts = tail.splitn(2, char::is_whitespace);
    let target = tail_parts.next().unwrap_or_default().trim().to_string();
    let mut remainder = tail_parts.next().unwrap_or_default().trim();
    if target.is_empty() {
        return Err(SqlError::new("RELATE target cannot be empty"));
    }

    let mut weight = 1.0f32;
    let mut meta_json = None;

    while !remainder.is_empty() {
        let upper = remainder.to_uppercase();
        if upper.starts_with("WEIGHT ") {
            let rest = remainder[6..].trim_start();
            let end = find_keyword_boundary(rest, &["META"]);
            let weight_raw = rest[..end].trim();
            weight = weight_raw
                .parse::<f32>()
                .map_err(|_| SqlError::new("RELATE WEIGHT must be numeric"))?;
            remainder = rest[end..].trim_start();
        } else if upper.starts_with("META ") {
            let rest = remainder[4..].trim_start();
            if rest.is_empty() {
                return Err(SqlError::new("RELATE META requires JSON payload"));
            }
            let raw_json = rest.trim();
            let parsed: Value = serde_json::from_str(raw_json)
                .map_err(|e| SqlError::new(format!("RELATE META must be valid JSON: {e}; raw={raw_json}")))?;
            meta_json = Some(parsed.to_string());
            remainder = "";
        } else {
            return Err(SqlError::new(format!("unsupported RELATE clause: {remainder}")));
        }
    }

    Ok(RelateEdge {
        source,
        edge_type,
        target,
        weight,
        meta_json,
    })
}

fn parse_values_rows(raw: &str) -> Result<Vec<Vec<Expr>>, SqlError> {
    let mut rows = Vec::new();
    let chars: Vec<char> = raw.chars().collect();
    let mut i = 0usize;

    while i < chars.len() {
        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= chars.len() {
            break;
        }
        if chars[i] != '(' {
            return Err(SqlError::new("VALUES rows must use parentheses"));
        }
        let close = find_matching(raw, i, '(', ')')?;
        let inner = &raw[i + 1..close];
        let row = split_top_level_csv(inner)
            .into_iter()
            .map(|s| parse_value_expr(&s))
            .collect::<Result<Vec<_>, _>>()?;
        rows.push(row);
        i = close + 1;

        while i < chars.len() && chars[i].is_whitespace() {
            i += 1;
        }
        if i < chars.len() {
            if chars[i] != ',' {
                return Err(SqlError::new("VALUES rows must be separated by commas"));
            }
            i += 1;
        }
    }

    if rows.is_empty() {
        return Err(SqlError::new("INSERT INTO VALUES requires at least one row"));
    }

    Ok(rows)
}

fn parse_projection(raw: &str) -> Vec<SelectItem> {
    if raw.trim() == "*" {
        return vec![SelectItem::Wildcard];
    }
    raw.split(',')
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .map(|part| SelectItem::Field(part.to_string()))
        .collect()
}

fn parse_table_ref(raw: &str) -> Result<TableRef, SqlError> {
    let parts: Vec<&str> = raw.split_whitespace().collect();
    match parts.as_slice() {
        [name] => Ok(TableRef { name: (*name).to_string(), alias: None }),
        [name, alias] => Ok(TableRef { name: (*name).to_string(), alias: Some((*alias).to_string()) }),
        [name, as_kw, alias] if as_kw.eq_ignore_ascii_case("AS") => Ok(TableRef { name: (*name).to_string(), alias: Some((*alias).to_string()) }),
        _ => Err(SqlError::new(format!("unsupported table reference: {raw}"))),
    }
}

fn parse_traverse_clause(raw: &str) -> Result<TraverseClause, SqlError> {
    let parts: Vec<&str> = raw.split_whitespace().collect();
    if parts.len() < 4 {
        return Err(SqlError::new("TRAVERSE requires: DIRECTION edge_type TO target [alias]"));
    }
    let direction = match parts[0].to_uppercase().as_str() {
        "FORWARD" => TraverseDirection::Forward,
        "BACKWARD" => TraverseDirection::Backward,
        other => return Err(SqlError::new(format!("unsupported TRAVERSE direction: {other}"))),
    };
    if !parts[2].eq_ignore_ascii_case("TO") {
        return Err(SqlError::new("TRAVERSE must use TO before target collection"));
    }
    let hops_idx = parts.iter().position(|part| part.eq_ignore_ascii_case("HOPS"));
    let target_parts = match hops_idx {
        Some(idx) => &parts[3..idx],
        None => &parts[3..],
    };
    let target = match target_parts {
        [name] => TableRef { name: (*name).to_string(), alias: None },
        [name, alias] => TableRef { name: (*name).to_string(), alias: Some((*alias).to_string()) },
        _ => return Err(SqlError::new("unsupported TRAVERSE target")),
    };
    let hops = match hops_idx {
        Some(idx) => {
            if idx + 1 >= parts.len() {
                return Err(SqlError::new("TRAVERSE HOPS requires a positive integer"));
            }
            Some(
                parts[idx + 1]
                    .parse::<u32>()
                    .map_err(|_| SqlError::new("TRAVERSE HOPS requires a positive integer"))?,
            )
        }
        None => None,
    };
    Ok(TraverseClause {
        direction,
        edge_type: parts[1].to_string(),
        target,
        hops,
    })
}

fn parse_order_by(raw: &str) -> Result<Vec<crate::sql::ast::OrderByItem>, SqlError> {
    split_top_level_csv(raw)
        .into_iter()
        .map(|item| {
            let item = item.trim();
            if item.is_empty() {
                return Err(SqlError::new("empty ORDER BY item"));
            }
            let upper = item.to_uppercase();
            if let Some(stripped) = upper.strip_suffix(" DESC") {
                let len = stripped.len();
                return Ok(crate::sql::ast::OrderByItem {
                    field: item[..len].trim().to_string(),
                    ascending: false,
                });
            }
            if let Some(stripped) = upper.strip_suffix(" ASC") {
                let len = stripped.len();
                return Ok(crate::sql::ast::OrderByItem {
                    field: item[..len].trim().to_string(),
                    ascending: true,
                });
            }
            Ok(crate::sql::ast::OrderByItem {
                field: item.to_string(),
                ascending: true,
            })
        })
        .collect()
}

fn parse_column_def(raw: &str) -> Result<ColumnDef, SqlError> {
    let parts: Vec<&str> = raw.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(SqlError::new(format!("invalid column definition: {raw}")));
    }
    let name = parts[0].to_string();
    let data_type = parse_sql_type(parts[1])?;
    let upper = raw.to_uppercase();
    let primary_key = upper.contains("PRIMARY KEY");
    let default = if upper.contains("DEFAULT UUIDV4()") {
        Some(DefaultExpr::UuidV4)
    } else {
        None
    };
    Ok(ColumnDef {
        name,
        data_type,
        primary_key,
        default,
    })
}

fn parse_sql_type(raw: &str) -> Result<SqlType, SqlError> {
    let upper = raw.to_uppercase();
    match upper.as_str() {
        "TEXT" => Ok(SqlType::Text),
        "INT" | "INTEGER" | "BIGINT" => Ok(SqlType::Int),
        "FLOAT" | "DOUBLE" | "REAL" => Ok(SqlType::Float),
        "BOOL" | "BOOLEAN" => Ok(SqlType::Bool),
        "JSON" => Ok(SqlType::Json),
        "TIMESTAMP" => Ok(SqlType::Timestamp),
        "VAGUE_TIME" => Ok(SqlType::VagueTime),
        "GEOMETRY" => Ok(SqlType::Geometry),
        "UUID" => Ok(SqlType::Uuid),
        _ if upper.starts_with("VECTOR(") && upper.ends_with(')') => {
            let inner = &upper[7..upper.len() - 1];
            let dim = inner.parse::<usize>().map_err(|_| SqlError::new("VECTOR dimension must be an integer"))?;
            Ok(SqlType::Vector(dim))
        }
        _ => Err(SqlError::new(format!("unsupported SQL type: {raw}"))),
    }
}

fn parse_collection_options(raw: &str) -> Result<CollectionOptions, SqlError> {
    let mut out = CollectionOptions::default();
    for item in split_top_level_csv(raw) {
        let Some(eq_idx) = item.find('=') else {
            return Err(SqlError::new(format!("invalid WITH option: {item}")));
        };
        let key = item[..eq_idx].trim().to_lowercase();
        let value = parse_identifier_array(item[eq_idx + 1..].trim())?;
        match key.as_str() {
            "hash_index" => out.hash_index = value,
            "range_index" => out.range_index = value,
            "temporal_index" => out.temporal_index = value,
            "spatial_index" => out.spatial_index = value,
            "vector_index" => out.vector_index = value,
            "fulltext_index" => out.fulltext_index = value,
            other => return Err(SqlError::new(format!("unsupported WITH option: {other}"))),
        }
    }
    Ok(out)
}

fn parse_identifier_array(raw: &str) -> Result<Vec<String>, SqlError> {
    let raw = raw.trim();
    if !(raw.starts_with('[') && raw.ends_with(']')) {
        return Err(SqlError::new(format!("expected [a, b, c], got: {raw}")));
    }
    let inner = &raw[1..raw.len() - 1];
    Ok(split_top_level_csv(inner)
        .into_iter()
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

fn parse_value_expr(raw: &str) -> Result<Expr, SqlError> {
    let raw = raw.trim();
    let upper = raw.to_uppercase();
    if upper.starts_with("TIMESTAMP ") || upper.starts_with("DATE ") {
        return Ok(Expr::TimestampLiteral(parse_string_literal(raw[raw.find(' ').unwrap() + 1..].trim())?));
    }
    if raw.starts_with('[') && raw.ends_with(']') {
        let inner = &raw[1..raw.len() - 1];
        let items = if inner.trim().is_empty() {
            Vec::new()
        } else {
            split_top_level_csv(inner)
                .into_iter()
                .map(|item| parse_value_expr(&item))
                .collect::<Result<Vec<_>, _>>()?
        };
        return Ok(Expr::ArrayLiteral(items));
    }
    if raw.starts_with('\'') && raw.ends_with('\'') {
        return Ok(Expr::StringLiteral(parse_string_literal(raw)?));
    }
    if raw.eq_ignore_ascii_case("true") {
        return Ok(Expr::Boolean(true));
    }
    if raw.eq_ignore_ascii_case("false") {
        return Ok(Expr::Boolean(false));
    }
    if raw.eq_ignore_ascii_case("null") {
        return Ok(Expr::Null);
    }
    if let Ok(n) = raw.parse::<i64>() {
        return Ok(Expr::Integer(n));
    }
    if let Ok(n) = raw.parse::<f64>() {
        return Ok(Expr::Float(n));
    }
    Ok(Expr::Identifier(raw.to_string()))
}

fn parse_string_literal(raw: &str) -> Result<String, SqlError> {
    if raw.len() >= 2 && raw.starts_with('\'') && raw.ends_with('\'') {
        Ok(raw[1..raw.len() - 1].replace("''", "'"))
    } else {
        Err(SqlError::new(format!("expected string literal, got: {raw}")))
    }
}

fn split_top_level_csv(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut paren_depth = 0i32;
    let mut bracket_depth = 0i32;
    let mut brace_depth = 0i32;

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
            '{' if !in_quote => {
                brace_depth += 1;
                current.push(ch);
            }
            '}' if !in_quote => {
                brace_depth -= 1;
                current.push(ch);
            }
            ',' if !in_quote && paren_depth == 0 && bracket_depth == 0 && brace_depth == 0 => {
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

fn find_keyword_boundary(raw: &str, keywords: &[&str]) -> usize {
    let upper = raw.to_uppercase();
    keywords
        .iter()
        .filter_map(|keyword| upper.find(&format!(" {keyword}")))
        .min()
        .unwrap_or(raw.len())
}

fn find_matching(raw: &str, open_idx: usize, open: char, close: char) -> Result<usize, SqlError> {
    let chars: Vec<char> = raw.chars().collect();
    let mut depth = 0i32;
    let mut in_quote = false;
    for (idx, ch) in chars.iter().enumerate().skip(open_idx) {
        if *ch == '\'' {
            in_quote = !in_quote;
            continue;
        }
        if in_quote {
            continue;
        }
        if *ch == open {
            depth += 1;
        } else if *ch == close {
            depth -= 1;
            if depth == 0 {
                return Ok(idx);
            }
        }
    }
    Err(SqlError::new("unclosed delimited block"))
}

fn parse_usize(raw: &str, clause: &str) -> Result<usize, SqlError> {
    raw.parse::<usize>()
        .map_err(|_| SqlError::new(format!("{clause} expects a positive integer")))
}
