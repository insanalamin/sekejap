use sekejap::{lower_sql_statement, parse_sql, SqlStatement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

const SELECT_ITERS: usize = 20_000;
const DDL_ITERS: usize = 10_000;

struct ParseCase {
    name: &'static str,
    parser: &'static str,
    sql_bytes: usize,
    iterations: usize,
    parse_ms: f64,
    per_parse_us: f64,
}

struct ParseLowerCase {
    name: &'static str,
    sql_bytes: usize,
    iterations: usize,
    parse_and_lower_ms: f64,
    per_op_us: f64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ddl_sql = "CREATE COLLECTION cases (id TEXT PRIMARY KEY, title TEXT, body TEXT, created_at TIMESTAMP) WITH (hash_index = [id], range_index = [created_at], fulltext_index = [title, body])";

    let select_plain_anchor = "SELECT id FROM cases WHERE id = 'incident_00000' LIMIT 64";
    let select_plain_exact_time = "SELECT id FROM cases WHERE id = 'incident_00000' AND created_at >= TIMESTAMP '2024-06-01 00:00:00' AND created_at <= TIMESTAMP '2024-12-31 23:59:59' LIMIT 64";
    let select_plain_text = "SELECT id FROM cases WHERE id = 'incident_00000' AND body ILIKE '%poor education%' LIMIT 64";
    let select_anchor = "SELECT id FROM cases TRAVERSE FORWARD caused_by TO cases HOPS 10 WHERE id = 'incident_00000' LIMIT 64";
    let select_exact_time = "SELECT id FROM cases TRAVERSE FORWARD caused_by TO cases HOPS 10 WHERE id = 'incident_00000' AND created_at >= TIMESTAMP '2024-06-01 00:00:00' AND created_at <= TIMESTAMP '2024-12-31 23:59:59' LIMIT 64";
    let select_text = "SELECT id FROM cases TRAVERSE FORWARD caused_by TO cases HOPS 10 WHERE id = 'incident_00000' AND body ILIKE '%poor education%' LIMIT 64";

    let insert1 = sql_insert_batch(&build_case_rows(1));
    let insert10 = sql_insert_batch(&build_case_rows(10));
    let insert50 = sql_insert_batch(&build_case_rows(50));
    let insert250 = sql_insert_batch(&build_case_rows(250));

    let parse_cases = vec![
        bench_parse_hand("ddl_create_collection", ddl_sql, DDL_ITERS)?,
        bench_parse_hand("select_plain_anchor", select_plain_anchor, SELECT_ITERS)?,
        bench_parse_hand("select_plain_exact_time", select_plain_exact_time, SELECT_ITERS)?,
        bench_parse_hand("select_plain_text", select_plain_text, SELECT_ITERS)?,
        bench_parse_hand("select_traverse_anchor", select_anchor, SELECT_ITERS)?,
        bench_parse_hand("select_traverse_exact_time", select_exact_time, SELECT_ITERS)?,
        bench_parse_hand("select_traverse_text", select_text, SELECT_ITERS)?,
        bench_parse_sqlparser_select("select_plain_anchor", select_plain_anchor, SELECT_ITERS)?,
        bench_parse_sqlparser_select("select_plain_exact_time", select_plain_exact_time, SELECT_ITERS)?,
        bench_parse_sqlparser_select("select_plain_text", select_plain_text, SELECT_ITERS)?,
        bench_parse_sqlparser_traverse_hybrid("select_traverse_anchor", select_anchor, SELECT_ITERS)?,
        bench_parse_sqlparser_traverse_hybrid("select_traverse_exact_time", select_exact_time, SELECT_ITERS)?,
        bench_parse_sqlparser_traverse_hybrid("select_traverse_text", select_text, SELECT_ITERS)?,
        bench_parse_hand("insert_values_1", &insert1, 2_000)?,
        bench_parse_hand("insert_values_10", &insert10, 1_000)?,
        bench_parse_hand("insert_values_50", &insert50, 250)?,
        bench_parse_hand("insert_values_250", &insert250, 50)?,
        bench_parse_sqlparser_insert("insert_values_1", &insert1, 2_000)?,
        bench_parse_sqlparser_insert("insert_values_10", &insert10, 1_000)?,
        bench_parse_sqlparser_insert("insert_values_50", &insert50, 250)?,
        bench_parse_sqlparser_insert("insert_values_250", &insert250, 50)?,
    ];

    let parse_lower_cases = vec![
        bench_parse_and_lower("select_traverse_anchor", select_anchor, SELECT_ITERS)?,
        bench_parse_and_lower("select_traverse_exact_time", select_exact_time, SELECT_ITERS)?,
        bench_parse_and_lower("select_traverse_text", select_text, SELECT_ITERS)?,
    ];

    let out_dir =
        PathBuf::from(r"C:\Users\Insan\AndroidStudioProjects\SekejapDB\sekejap-benchmark\techniques\parser");
    fs::create_dir_all(&out_dir)?;
    fs::write(out_dir.join("RESULT.md"), render_markdown(&parse_cases, &parse_lower_cases))?;
    println!("wrote {}", out_dir.join("RESULT.md").display());
    Ok(())
}

fn bench_parse_hand(
    name: &'static str,
    sql: &str,
    iterations: usize,
) -> Result<ParseCase, Box<dyn std::error::Error>> {
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = parse_sql(sql)?;
    }
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    Ok(ParseCase {
        name,
        parser: "hand",
        sql_bytes: sql.len(),
        iterations,
        parse_ms: ms,
        per_parse_us: (ms * 1000.0) / iterations as f64,
    })
}

fn bench_parse_sqlparser_select(
    name: &'static str,
    sql: &str,
    iterations: usize,
) -> Result<ParseCase, Box<dyn std::error::Error>> {
    let dialect = GenericDialect {};
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = Parser::parse_sql(&dialect, sql)?;
    }
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    Ok(ParseCase {
        name,
        parser: "sqlparser-rs",
        sql_bytes: sql.len(),
        iterations,
        parse_ms: ms,
        per_parse_us: (ms * 1000.0) / iterations as f64,
    })
}

fn bench_parse_sqlparser_insert(
    name: &'static str,
    sql: &str,
    iterations: usize,
) -> Result<ParseCase, Box<dyn std::error::Error>> {
    let dialect = GenericDialect {};
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = Parser::parse_sql(&dialect, sql)?;
    }
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    Ok(ParseCase {
        name,
        parser: "sqlparser-rs",
        sql_bytes: sql.len(),
        iterations,
        parse_ms: ms,
        per_parse_us: (ms * 1000.0) / iterations as f64,
    })
}

fn bench_parse_sqlparser_traverse_hybrid(
    name: &'static str,
    sql: &str,
    iterations: usize,
) -> Result<ParseCase, Box<dyn std::error::Error>> {
    let dialect = GenericDialect {};
    let (standard_sql, traverse_clause) = split_select_and_traverse(sql)?;
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = Parser::parse_sql(&dialect, &standard_sql)?;
        let _ = parse_traverse_clause_heuristic(&traverse_clause)?;
    }
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    Ok(ParseCase {
        name,
        parser: "sqlparser+heuristic",
        sql_bytes: sql.len(),
        iterations,
        parse_ms: ms,
        per_parse_us: (ms * 1000.0) / iterations as f64,
    })
}

fn bench_parse_and_lower(
    name: &'static str,
    sql: &str,
    iterations: usize,
) -> Result<ParseLowerCase, Box<dyn std::error::Error>> {
    let start = Instant::now();
    for _ in 0..iterations {
        let stmt = parse_sql(sql)?;
        match stmt {
            SqlStatement::Select(_) => {
                let _ = lower_sql_statement(&stmt)?;
            }
            _ => return Err("parse+lower benchmark expects SELECT only".into()),
        }
    }
    let ms = start.elapsed().as_secs_f64() * 1000.0;
    Ok(ParseLowerCase {
        name,
        sql_bytes: sql.len(),
        iterations,
        parse_and_lower_ms: ms,
        per_op_us: (ms * 1000.0) / iterations as f64,
    })
}

fn build_case_rows(n: usize) -> Vec<(String, String, String, String)> {
    (0..n)
        .map(|i| {
            let id = format!("incident_{i:05}");
            let title = format!("Preventable crash at Geelong {i}");
            let body = format!(
                "A preventable road crash was reported near Geelong. Wet road, drainage failure, and poor education were noted in case {i}."
            );
            let created_at = "2024-06-15 09:30:00".to_string();
            (id, title, body, created_at)
        })
        .collect()
}

fn sql_insert_batch(rows: &[(String, String, String, String)]) -> String {
    let values = rows
        .iter()
        .map(|(id, title, body, created_at)| {
            format!(
                "('{}', '{}', '{}', TIMESTAMP '{}')",
                escape_sql(id),
                escape_sql(title),
                escape_sql(body),
                created_at
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO cases (id, title, body, created_at) VALUES {values}")
}

fn escape_sql(value: &str) -> String {
    value.replace('\'', "''")
}

fn render_markdown(parse_cases: &[ParseCase], parse_lower_cases: &[ParseLowerCase]) -> String {
    let mut out = String::from("# SQL Parser Benchmark\n\n");
    out.push_str("Goal: isolate current Sekejap SQL parser cost from engine execution cost.\n\n");
    out.push_str("Parser under test: current in-repo hand-written parser in `src/sql/parser.rs`.\n\n");

    out.push_str("## Parse Only\n\n");
    out.push_str("| Case | Parser | SQL bytes | Iterations | Total ms | Per parse us |\n");
    out.push_str("|---|---|---:|---:|---:|---:|\n");
    for case in parse_cases {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {:.3} | {:.3} |\n",
            case.name, case.parser, case.sql_bytes, case.iterations, case.parse_ms, case.per_parse_us
        ));
    }

    out.push_str("\n## Parse + Lower\n\n");
    out.push_str("| Case | SQL bytes | Iterations | Total ms | Per op us |\n");
    out.push_str("|---|---:|---:|---:|---:|\n");
    for case in parse_lower_cases {
        out.push_str(&format!(
            "| {} | {} | {} | {:.3} | {:.3} |\n",
            case.name, case.sql_bytes, case.iterations, case.parse_and_lower_ms, case.per_op_us
        ));
    }

    out.push_str("\n## Notes\n\n");
    out.push_str("- `INSERT ... VALUES ...` is measured as parse-only because inserts do not lower into query steps.\n");
    out.push_str("- `sqlparser+heuristic` means `sqlparser-rs` parsed the standard SQL skeleton and a lightweight Sekejap parser handled only the `TRAVERSE` clause.\n");
    out.push_str("- These numbers isolate frontend cost only; they do not include writes, indexes, or graph execution.\n");
    out.push_str("- If large `VALUES` batches already look bad here, the SQL insert problem is parser/materialization bound before the engine even runs.\n");
    out
}

fn split_select_and_traverse(sql: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    let upper = sql.to_uppercase();
    let Some(traverse_idx) = upper.find(" TRAVERSE ") else {
        return Err("missing TRAVERSE clause".into());
    };
    let after = &sql[traverse_idx + " TRAVERSE ".len()..];
    let after_upper = after.to_uppercase();
    let where_idx = after_upper.find(" WHERE ");
    let order_idx = after_upper.find(" ORDER BY ");
    let limit_idx = after_upper.find(" LIMIT ");
    let offset_idx = after_upper.find(" OFFSET ");
    let traverse_end = [where_idx, order_idx, limit_idx, offset_idx]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(after.len());

    let traverse_clause = after[..traverse_end].trim().to_string();
    let mut standard_sql = String::new();
    standard_sql.push_str(sql[..traverse_idx].trim_end());
    if traverse_end < after.len() {
        standard_sql.push(' ');
        standard_sql.push_str(after[traverse_end..].trim_start());
    }
    Ok((standard_sql, traverse_clause))
}

fn parse_traverse_clause_heuristic(raw: &str) -> Result<(), Box<dyn std::error::Error>> {
    let parts: Vec<&str> = raw.split_whitespace().collect();
    if parts.len() < 4 {
        return Err("TRAVERSE requires direction, edge type, TO, and target".into());
    }
    match parts[0].to_uppercase().as_str() {
        "FORWARD" | "BACKWARD" => {}
        _ => return Err("unsupported TRAVERSE direction".into()),
    }
    if !parts[2].eq_ignore_ascii_case("TO") {
        return Err("TRAVERSE must use TO".into());
    }
    if let Some(hops_idx) = parts.iter().position(|part| part.eq_ignore_ascii_case("HOPS")) {
        if hops_idx + 1 >= parts.len() {
            return Err("TRAVERSE HOPS requires integer".into());
        }
        let _ = parts[hops_idx + 1].parse::<u32>()?;
    }
    Ok(())
}
