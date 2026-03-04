use clap::Parser;
use colored::*;
use comfy_table::Table;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use sekejap::SekejapDB;
use std::path::Path;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the database directory
    #[arg(short, long, default_value = "./sekejap_data")]
    path: String,

    /// Initial node capacity
    #[arg(short, long, default_value_t = 1_000_000)]
    capacity: usize,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let db_path = Path::new(&args.path);

    println!("{}", "SekejapDB CLI".bold().green());
    println!("Connecting to {}...", args.path);

    let db = match SekejapDB::new(db_path, args.capacity) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("{} Failed to open database: {}", "Error:".red(), e);
            return Ok(());
        }
    };

    println!("{}", "Connected!".green());
    println!("Type 'help' or '\\?' for help.");

    let mut rl = DefaultEditor::new()?;
    if rl.load_history("history.txt").is_err() {
        // No previous history
    }

    loop {
        let readline = rl.readline("sekejap> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(line);

                if line.starts_with('\\') || is_internal_command(line) {
                    handle_command(line, &db);
                } else {
                    handle_query(line, &db);
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history("history.txt")?;
    Ok(())
}

fn is_internal_command(line: &str) -> bool {
    let cmd = line.split_whitespace().next().unwrap_or("");
    matches!(
        cmd,
        "help" | "exit" | "quit" | "ls" | "list" | "collections" | "clear" | "cls" | "describe"
    )
}

fn handle_command(line: &str, db: &SekejapDB) {
    let parts: Vec<&str> = line.split_whitespace().collect();
    let cmd = parts.get(0).unwrap_or(&"").trim_start_matches('\\');

    match cmd {
        "?" | "help" => print_help(),
        "q" | "exit" | "quit" => std::process::exit(0),
        "l" | "ls" | "list" | "collections" => list_collections(db),
        "d" | "desc" | "describe" => {
            if let Some(col) = parts.get(1) {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&db.describe_collection(col))
                        .unwrap_or_else(|_| "{}".to_string())
                );
            } else {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&db.describe())
                        .unwrap_or_else(|_| "{}".to_string())
                );
            }
        }
        "clear" | "cls" => print!("\x1B[2J\x1B[1;1H"),
        "flush" => {
            if let Err(e) = db.flush() {
                println!("{} Failed to flush: {}", "Error:".red(), e);
            } else {
                println!("{}", "Database flushed to disk.".green());
            }
        }
        _ => println!("Unknown command: {}", cmd),
    }
}

fn print_help() {
    println!("\n{}", "Available Commands:".bold().underline());
    println!("  \\? or help           Show this help");
    println!("  \\q or exit           Quit the CLI");
    println!("  \\l or ls             List all collections");
    println!("  \\d <name>            Describe collection schema and index settings");
    println!("  describe [name]      Show describe output (global or collection)");
    println!("  \\flush               Flush data to disk");
    println!("  clear                Clear the screen");
    println!("\n{}", "Querying (SekejapQL):".bold().underline());
    println!("  collection \"crimes\"                  SekejapQL query (auto-detected)");
    println!("  collection \"crimes\" | take 5         Pipe-style SekejapQL");
    println!("  count collection \"crimes\"            Count results");
    println!("  explain collection \"crimes\"          Show compiled pipeline steps");
    println!("\n{}", "Querying (JSON):".bold().underline());
    println!("  query {{...}};                        Execute JSON query pipeline");
    println!("  count {{...}};                        Count results from JSON pipeline");
    println!("  explain {{...}};                      Show compiled steps from JSON");
    println!("\n{}", "Mutations:".bold().underline());
    println!("  mutate {{...}};                       Execute JSON mutation");
    println!("\n{}", "Examples:".bold().underline());
    println!("  collection \"crimes\" | where_eq \"type\" \"robbery\" | take 10");
    println!("  one \"persons/ali\" | forward \"committed\" | take 5");
    println!("  all | similar \"persons/ali\" 10");
    println!("  query {{\"pipeline\": [{{\"op\": \"all\"}}, {{\"op\": \"take\", \"n\": 5}}]}};");
}

fn list_collections(db: &SekejapDB) {
    let mut table = Table::new();
    table.set_header(vec!["Collection Hash", "Count"]);

    for entry in db.collections.iter() {
        let hash = entry.key();
        let count = db
            .collection_counts
            .get(hash)
            .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(0);
        table.add_row(vec![format!("{:x}", hash), count.to_string()]);
    }

    // Also check collection_counts for ones that might not have a schema defined yet
    for entry in db.collection_counts.iter() {
        if !db.collections.contains_key(entry.key()) {
            table.add_row(vec![
                format!("{:x}", entry.key()),
                entry
                    .value()
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .to_string(),
            ]);
        }
    }

    println!("{}", table);
}

fn handle_query(line: &str, db: &SekejapDB) {
    let line = line.trim().trim_end_matches(';').trim();
    if line.is_empty() {
        return;
    }

    // describe (global or collection)
    if line == "describe" {
        println!(
            "{}",
            serde_json::to_string_pretty(&db.describe()).unwrap_or_else(|_| "{}".to_string())
        );
        return;
    }
    if let Some(rest) = line.strip_prefix("describe ") {
        println!(
            "{}",
            serde_json::to_string_pretty(&db.describe_collection(rest.trim()))
                .unwrap_or_else(|_| "{}".to_string())
        );
        return;
    }

    // mutate
    if let Some(json) = line.strip_prefix("mutate ") {
        let start = Instant::now();
        match db.mutate(json.trim()) {
            Ok(out) => {
                let duration = start.elapsed();
                println!(
                    "{} mutation completed in {:.4}s",
                    "Success:".green(),
                    duration.as_secs_f64()
                );
                println!(
                    "{}",
                    serde_json::to_string_pretty(&out).unwrap_or_else(|_| out.to_string())
                );
            }
            Err(e) => eprintln!("{} {}", "Mutation Error:".red(), e),
        }
        return;
    }

    // explain — show compiled pipeline steps without executing
    if let Some(rest) = line.strip_prefix("explain ") {
        let input = rest.strip_prefix("query ").unwrap_or(rest).trim();
        match db.explain(input) {
            Ok(steps) => {
                println!("{} {} steps compiled", "Plan:".green(), steps.len());
                for (i, step) in steps.iter().enumerate() {
                    println!("  {}: {:?}", i + 1, step);
                }
            }
            Err(e) => eprintln!("{} {}", "Explain Error:".red(), e),
        }
        return;
    }

    // count — return count only
    if let Some(rest) = line.strip_prefix("count ") {
        let input = rest.strip_prefix("query ").unwrap_or(rest).trim();
        let start = Instant::now();
        match db.count(input) {
            Ok(outcome) => {
                let duration = start.elapsed();
                println!(
                    "{} {} results in {:.4}s",
                    "Count:".green(),
                    outcome.data,
                    duration.as_secs_f64()
                );
                print_trace(&outcome.trace);
            }
            Err(e) => eprintln!("{} {}", "Count Error:".red(), e),
        }
        return;
    }

    // query — SekejapQL text or JSON pipeline (auto-detected)
    let input = line.strip_prefix("query ").unwrap_or(line).trim();
    let start = Instant::now();
    match db.query(input) {
        Ok(outcome) => {
            let duration = start.elapsed();
            let hits = outcome.data;

            println!(
                "{} {} hits in {:.4}s",
                "Success:".green(),
                hits.len(),
                duration.as_secs_f64()
            );

            if !hits.is_empty() {
                let mut table = Table::new();
                table.set_header(vec!["Idx", "Slug Hash", "Payload (Preview)"]);

                for hit in hits.iter().take(20) {
                    let payload = hit.payload.as_deref().unwrap_or("{}");
                    let preview = if payload.len() > 60 {
                        format!("{}...", &payload[..60])
                    } else {
                        payload.to_string()
                    };
                    table.add_row(vec![
                        hit.idx.to_string(),
                        format!("{:x}", hit.slug_hash),
                        match hit.score {
                            Some(score) => format!("{preview} [score={score:.4}]"),
                            None => preview,
                        },
                    ]);
                }
                println!("{}", table);
                if hits.len() > 20 {
                    println!("... and {} more.", hits.len() - 20);
                }
            }

            print_trace(&outcome.trace);
        }
        Err(e) => {
            eprintln!("{} {}", "Query Error:".red(), e);
        }
    }
}

fn print_trace(trace: &sekejap::Trace) {
    if !trace.steps.is_empty() {
        println!("\n{}", "Execution Trace:".dimmed());
        for step in &trace.steps {
            println!(
                "  -> {} (in: {}, out: {}, index: {}, time: {}us)",
                step.atom, step.input_size, step.output_size, step.index_used, step.time_us
            );
        }
    }
}
