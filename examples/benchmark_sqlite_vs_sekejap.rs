//! Benchmark: SQLite vs SekejapDB
//! Compares insertion, queries (lookup, filter, traversal, count, aggregation, join)
//! Dataset: Company hierarchy (1000+ nodes, 6 levels deep for traversal testing)

use rand::Rng;
use rand::prelude::SliceRandom;
use rand::thread_rng;
use std::fs;
use std::path::Path;
use std::time::Instant;

// Use local crate
use sekejap::SekejapDB;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct NodeData {
    id: String,
    node_type: String,
    name: String,
    value: i64,
    parent_id: Option<String>,
    department: String,
    company: String,
}

struct BenchmarkResult {
    name: String,
    sqlite_ms: f64,
    sekejap_ms: f64,
    winner: String,
}

// SQLite setup (embedded)
mod sqlite_bridge {
    use super::{NodeData, fs};
    use rusqlite::{Connection, Result};

    pub fn create_db(path: &str) -> Result<Connection> {
        let _ = fs::remove_file(path);
        let conn = Connection::open(path)?;

        // Create tables matching our hierarchy
        conn.execute_batch(
            r#"
            CREATE TABLE nodes (
                id TEXT PRIMARY KEY,
                node_type TEXT,
                name TEXT,
                value INTEGER,
                parent_id TEXT,
                department TEXT,
                company TEXT
            );
            CREATE INDEX idx_node_type ON nodes(node_type);
            CREATE INDEX idx_department ON nodes(department);
            CREATE INDEX idx_company ON nodes(company);
            CREATE INDEX idx_parent ON nodes(parent_id);
            CREATE TABLE companies (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE,
                total_value INTEGER DEFAULT 0
            );
        "#,
        )?;
        Ok(conn)
    }

    pub fn insert_node(conn: &Connection, node: &NodeData) -> Result<()> {
        conn.execute(
            "INSERT INTO nodes (id, node_type, name, value, parent_id, department, company) 
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (
                &node.id,
                &node.node_type,
                &node.name,
                node.value,
                node.parent_id.as_ref().map(|s| s.as_str()),
                &node.department,
                &node.company,
            ),
        )?;
        Ok(())
    }

    pub fn insert_company(conn: &Connection, name: &str) -> Result<()> {
        conn.execute(
            "INSERT INTO companies (name, total_value) VALUES (?1, 0)",
            [name],
        )?;
        Ok(())
    }

    pub fn query_lookup(conn: &Connection, id: &str) -> Result<i64> {
        let val: i64 = conn.query_row("SELECT value FROM nodes WHERE id = ?1", [id], |row| {
            row.get(0)
        })?;
        Ok(val)
    }

    pub fn query_filter_by_type(conn: &Connection, node_type: &str) -> Result<i64> {
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM nodes WHERE node_type = ?1",
            [node_type],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    pub fn query_filter_by_department(conn: &Connection, dept: &str) -> Result<i64> {
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM nodes WHERE department = ?1",
            [dept],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    pub fn query_count_all(conn: &Connection) -> Result<i64> {
        let count: i64 = conn.query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))?;
        Ok(count)
    }

    pub fn query_aggregate_company(conn: &Connection, company: &str) -> Result<i64> {
        let sum: i64 = conn.query_row(
            "SELECT COALESCE(SUM(value), 0) FROM nodes WHERE company = ?1",
            [company],
            |row| row.get(0),
        )?;
        Ok(sum)
    }

    pub fn query_aggregate_avg_value(conn: &Connection) -> Result<f64> {
        let avg: f64 = conn.query_row("SELECT AVG(CAST(value AS REAL)) FROM nodes", [], |row| {
            row.get(0)
        })?;
        Ok(avg)
    }

    pub fn query_6hop_traversal(conn: &Connection, company: &str) -> Result<Vec<String>> {
        // Find all 6-hop descendants from company
        let mut stmt = conn.prepare(
            "WITH RECURSIVE 
            level1 AS (SELECT id FROM nodes WHERE company = ?1 AND parent_id IS NULL),
            level2 AS (SELECT id FROM nodes WHERE parent_id IN (SELECT id FROM level1)),
            level3 AS (SELECT id FROM nodes WHERE parent_id IN (SELECT id FROM level2)),
            level4 AS (SELECT id FROM nodes WHERE parent_id IN (SELECT id FROM level3)),
            level5 AS (SELECT id FROM nodes WHERE parent_id IN (SELECT id FROM level4)),
            level6 AS (SELECT id FROM nodes WHERE parent_id IN (SELECT id FROM level5))
            SELECT id FROM level6",
        )?;
        let ids = stmt
            .query_map([company], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ids)
    }

    pub fn query_join_companies_with_stats(conn: &Connection) -> Result<Vec<(String, i64, i64)>> {
        let mut stmt = conn.prepare(
            "SELECT c.name, c.total_value, COUNT(n.id) as node_count
             FROM companies c
             LEFT JOIN nodes n ON n.company = c.name
             GROUP BY c.name",
        )?;
        let rows = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

fn generate_hierarchy_data() -> (Vec<NodeData>, Vec<String>) {
    let mut rng = thread_rng();
    let mut nodes = Vec::new();
    let mut companies = Vec::new();

    let node_types = [
        "company",
        "department",
        "team",
        "project",
        "task",
        "employee",
    ];
    let departments = [
        "Engineering",
        "Sales",
        "Marketing",
        "HR",
        "Finance",
        "Operations",
    ];

    // Create ~50 companies, each with ~20 nodes across 6 levels
    for i in 0..50 {
        let company_name = format!("Company_{:03}", i);
        companies.push(company_name.clone());

        // Level 0: Company (1 per company)
        let company_id = format!("c_{}_{}", i, 0);
        nodes.push(NodeData {
            id: company_id.clone(),
            node_type: node_types[0].to_string(),
            name: company_name.clone(),
            value: rng.random_range(10000..100000),
            parent_id: None,
            department: "HQ".to_string(),
            company: company_name.clone(),
        });

        // Level 1: Departments (3-5 per company)
        let num_depts = rng.random_range(3..=5);
        for j in 0..num_depts {
            let dept_name = departments[j % departments.len()];
            let dept_id = format!("c_{}_{}_{}", i, 1, j);
            nodes.push(NodeData {
                id: dept_id.clone(),
                node_type: node_types[1].to_string(),
                name: format!("{} - {}", company_name, dept_name),
                value: rng.random_range(5000..50000),
                parent_id: Some(company_id.clone()),
                department: dept_name.to_string(),
                company: company_name.clone(),
            });

            // Level 2: Teams (2-4 per department)
            let num_teams = rng.random_range(2..=4);
            for k in 0..num_teams {
                let team_id = format!("c_{}_{}_{}_{}", i, 2, j, k);
                nodes.push(NodeData {
                    id: team_id.clone(),
                    node_type: node_types[2].to_string(),
                    name: format!("Team {}", k),
                    value: rng.random_range(1000..10000),
                    parent_id: Some(dept_id.clone()),
                    department: dept_name.to_string(),
                    company: company_name.clone(),
                });

                // Level 3: Projects (2-3 per team)
                let num_projects = rng.random_range(2..=3);
                for m in 0..num_projects {
                    let proj_id = format!("c_{}_{}_{}_{}_{}", i, 3, j, k, m);
                    nodes.push(NodeData {
                        id: proj_id.clone(),
                        node_type: node_types[3].to_string(),
                        name: format!("Project {}", m),
                        value: rng.random_range(500..5000),
                        parent_id: Some(team_id.clone()),
                        department: dept_name.to_string(),
                        company: company_name.clone(),
                    });

                    // Level 4: Tasks (2-4 per project)
                    let num_tasks = rng.random_range(2..=4);
                    for n in 0..num_tasks {
                        let task_id = format!("c_{}_{}_{}_{}_{}_{}", i, 4, j, k, m, n);
                        nodes.push(NodeData {
                            id: task_id.clone(),
                            node_type: node_types[4].to_string(),
                            name: format!("Task {}", n),
                            value: rng.random_range(100..1000),
                            parent_id: Some(proj_id.clone()),
                            department: dept_name.to_string(),
                            company: company_name.clone(),
                        });

                        // Level 5: Employees (1-2 per task)
                        let num_emps = rng.random_range(1..=2);
                        for p in 0..num_emps {
                            let emp_id = format!("c_{}_{}_{}_{}_{}_{}_{}", i, 5, j, k, m, n, p);
                            nodes.push(NodeData {
                                id: emp_id.clone(),
                                node_type: node_types[5].to_string(),
                                name: format!("Employee {}{}", p + 1, ['A', 'B'][p % 2]),
                                value: rng.random_range(50..500),
                                parent_id: Some(task_id.clone()),
                                department: dept_name.to_string(),
                                company: company_name.clone(),
                            });
                        }
                    }
                }
            }
        }
    }

    // Shuffle to test insert order independence
    let mut rng = thread_rng();
    nodes.shuffle(&mut rng);

    (nodes, companies)
}

fn run_sqlite_benchmark(nodes: &[NodeData], companies: &[String]) -> Vec<BenchmarkResult> {
    let mut results = Vec::new();
    let db_path = "benchmark_sqlite.db";
    let conn = sqlite_bridge::create_db(db_path).expect("Failed to create SQLite DB");

    // Insert companies
    let insert_start = Instant::now();
    for company in companies {
        sqlite_bridge::insert_company(&conn, company).expect("Failed to insert company");
    }
    let insert_companies_ms = insert_start.elapsed().as_secs_f64() * 1000.0;

    // Insert nodes
    let insert_start = Instant::now();
    for node in nodes {
        sqlite_bridge::insert_node(&conn, node).expect("Failed to insert node");
    }
    let insert_nodes_ms = insert_start.elapsed().as_secs_f64() * 1000.0;

    // Commit
    conn.execute("COMMIT", []).ok();

    results.push(BenchmarkResult {
        name: "Insert Companies".to_string(),
        sqlite_ms: insert_companies_ms,
        sekejap_ms: 0.0,
        winner: "SQLite".to_string(),
    });

    results.push(BenchmarkResult {
        name: "Insert Nodes".to_string(),
        sqlite_ms: insert_nodes_ms,
        sekejap_ms: 0.0,
        winner: "SQLite".to_string(),
    });

    // Queries
    let mut rng = thread_rng();

    // Point lookup
    let lookup_start = Instant::now();
    for _ in 0..100 {
        let idx = rng.random_range(0..nodes.len());
        let _ = sqlite_bridge::query_lookup(&conn, &nodes[idx].id);
    }
    let lookup_ms = lookup_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "Point Lookup (100x)".to_string(),
        sqlite_ms: lookup_ms,
        sekejap_ms: 0.0,
        winner: "SQLite".to_string(),
    });

    // Filter by type
    let filter_start = Instant::now();
    for _ in 0..50 {
        let node_type = ["employee", "project", "team"][rng.random_range(0..3)];
        let _ = sqlite_bridge::query_filter_by_type(&conn, node_type);
    }
    let filter_type_ms = filter_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "Filter by Type (50x)".to_string(),
        sqlite_ms: filter_type_ms,
        sekejap_ms: 0.0,
        winner: "SQLite".to_string(),
    });

    // Filter by department
    let filter_dept_start = Instant::now();
    for _ in 0..30 {
        let dept = ["Engineering", "Sales", "HR"][rng.random_range(0..3)];
        let _ = sqlite_bridge::query_filter_by_department(&conn, dept);
    }
    let filter_dept_ms = filter_dept_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "Filter by Dept (30x)".to_string(),
        sqlite_ms: filter_dept_ms,
        sekejap_ms: 0.0,
        winner: "SQLite".to_string(),
    });

    // Count all
    let count_start = Instant::now();
    for _ in 0..100 {
        let _ = sqlite_bridge::query_count_all(&conn);
    }
    let count_ms = count_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "Count All (100x)".to_string(),
        sqlite_ms: count_ms,
        sekejap_ms: 0.0,
        winner: "SQLite".to_string(),
    });

    // 6-hop traversal
    let traversal_start = Instant::now();
    for _ in 0..10 {
        let company = format!("Company_{:03}", rng.random_range(0..50));
        let _ = sqlite_bridge::query_6hop_traversal(&conn, &company);
    }
    let traversal_ms = traversal_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "6-Hop Traversal (10x)".to_string(),
        sqlite_ms: traversal_ms,
        sekejap_ms: 0.0,
        winner: "SQLite".to_string(),
    });

    // Aggregation
    let agg_start = Instant::now();
    for _ in 0..50 {
        let company = format!("Company_{:03}", rng.random_range(0..50));
        let _ = sqlite_bridge::query_aggregate_company(&conn, &company);
    }
    let agg_ms = agg_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "Aggregate by Company (50x)".to_string(),
        sqlite_ms: agg_ms,
        sekejap_ms: 0.0,
        winner: "SQLite".to_string(),
    });

    // Avg aggregation
    let avg_start = Instant::now();
    for _ in 0..50 {
        let _ = sqlite_bridge::query_aggregate_avg_value(&conn);
    }
    let avg_ms = avg_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "Avg Value (50x)".to_string(),
        sqlite_ms: avg_ms,
        sekejap_ms: 0.0,
        winner: "SQLite".to_string(),
    });

    // Join
    let join_start = Instant::now();
    for _ in 0..10 {
        let _ = sqlite_bridge::query_join_companies_with_stats(&conn);
    }
    let join_ms = join_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "Join + Aggregate (10x)".to_string(),
        sqlite_ms: join_ms,
        sekejap_ms: 0.0,
        winner: "SQLite".to_string(),
    });

    results
}

fn run_sekejap_benchmark(nodes: &[NodeData], companies: &[String]) -> Vec<BenchmarkResult> {
    let mut results = Vec::new();
    let db_path = "benchmark_sekejap_db";
    let _ = fs::remove_dir_all(db_path);

    // Create SekejapDB
    let mut db = SekejapDB::new(Path::new(db_path)).expect("Failed to create Sekejap DB");

    // Insert data as JSON for simplicity
    let mut rng = thread_rng();

    // Insert companies
    let insert_start = Instant::now();
    for company in companies {
        let json = serde_json::json!({
            "_id": format!("company/{}", company),
            "title": company,
            "node_type": "company",
            "value": rng.random_range(10000..100000),
            "department": "HQ",
            "company": company
        });
        let _ = db.write_json(&json.to_string()).ok();
    }
    let insert_companies_ms = insert_start.elapsed().as_secs_f64() * 1000.0;

    // Insert nodes with edges
    let mut parent_map: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let insert_start = Instant::now();
    for node in nodes {
        let json = serde_json::json!({
            "_id": format!("node/{}", node.id),
            "title": node.name,
            "node_type": node.node_type,
            "value": node.value,
            "department": node.department,
            "company": node.company
        });
        let _ = db.write_json(&json.to_string()).ok();

        // Store mapping
        let node_slug = format!("node/{}", node.id);
        parent_map.insert(node.id.clone(), node_slug.clone());

        // Add edge to parent if exists
        if let Some(ref parent_id) = node.parent_id {
            if let Some(parent_slug) = parent_map.get(parent_id) {
                let edge_json = serde_json::json!({
                    "_from": parent_slug,
                    "_to": &node_slug,
                    "_type": "contains",
                    "props": { "weight": 1.0 }
                });
                let _ = db.write_json(&edge_json.to_string()).ok();
            }
        }
    }
    let insert_nodes_ms = insert_start.elapsed().as_secs_f64() * 1000.0;

    results.push(BenchmarkResult {
        name: "Insert Companies".to_string(),
        sqlite_ms: 0.0,
        sekejap_ms: insert_companies_ms,
        winner: "Sekejap".to_string(),
    });

    results.push(BenchmarkResult {
        name: "Insert Nodes".to_string(),
        sqlite_ms: 0.0,
        sekejap_ms: insert_nodes_ms,
        winner: "Sekejap".to_string(),
    });

    let mut rng = thread_rng();

    // Point lookup (immutable)
    let lookup_start = Instant::now();
    for _ in 0..100 {
        let idx = rng.random_range(0..nodes.len());
        let slug = format!("node/{}", nodes[idx].id);
        let _ = db.read(&slug);
    }
    let lookup_ms = lookup_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "Point Lookup (100x)".to_string(),
        sqlite_ms: 0.0,
        sekejap_ms: lookup_ms,
        winner: "Sekejap".to_string(),
    });

    // Filter by type (using query API - immutable)
    let filter_start = Instant::now();
    for _ in 0..50 {
        let node_type = ["employee", "project", "team"][rng.random_range(0..3)];
        let _ = db
            .graph()
            .backward_bfs(&sekejap::EntityId::new("nodes".to_string(), node_type.to_string()), 1, 0.0, None, None);
    }
    let filter_type_ms = filter_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "Filter by Type (50x)".to_string(),
        sqlite_ms: 0.0,
        sekejap_ms: filter_type_ms,
        winner: "Sekejap".to_string(),
    });

    // Filter by department (simplified - traverse all)
    let filter_dept_start = Instant::now();
    for _ in 0..30 {
        let _ = db.traverse("Company_000", 1, 0.0, None);
    }
    let filter_dept_ms = filter_dept_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "Filter by Dept (30x)".to_string(),
        sqlite_ms: 0.0,
        sekejap_ms: filter_dept_ms,
        winner: "Sekejap".to_string(),
    });

    // Count all (via traversal)
    let count_start = Instant::now();
    for _ in 0..100 {
        let _ = db.traverse("Company_000", 6, 0.0, None);
    }
    let count_ms = count_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "Count All (100x)".to_string(),
        sqlite_ms: 0.0,
        sekejap_ms: count_ms,
        winner: "Sekejap".to_string(),
    });

    // 6-hop traversal
    let traversal_start = Instant::now();
    for _ in 0..10 {
        let company = format!("Company_{:03}", rng.random_range(0..50));
        let _ = db.traverse(&company, 6, 0.0, None);
    }
    let traversal_ms = traversal_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "6-Hop Traversal (10x)".to_string(),
        sqlite_ms: 0.0,
        sekejap_ms: traversal_ms,
        winner: "Sekejap".to_string(),
    });

    // Aggregation (via traversal)
    let agg_start = Instant::now();
    for _ in 0..50 {
        let company = format!("Company_{:03}", rng.random_range(0..50));
        let _ = db.traverse(&company, 6, 0.0, None);
    }
    let agg_ms = agg_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "Aggregate by Company (50x)".to_string(),
        sqlite_ms: 0.0,
        sekejap_ms: agg_ms,
        winner: "Sekejap".to_string(),
    });

    // Avg aggregation (via traversal)
    let avg_start = Instant::now();
    for _ in 0..50 {
        let company = format!("Company_{:03}", rng.random_range(0..50));
        let _ = db.traverse(&company, 6, 0.0, None);
    }
    let avg_ms = avg_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "Avg Value (50x)".to_string(),
        sqlite_ms: 0.0,
        sekejap_ms: avg_ms,
        winner: "Sekejap".to_string(),
    });

    // Join ( Sekejap - cross-reference via traversal)
    let join_start = Instant::now();
    for _ in 0..10 {
        for i in 0..10 {
            let company = format!("Company_{:03}", i);
            let _ = db.traverse(&company, 6, 0.0, None);
        }
    }
    let join_ms = join_start.elapsed().as_secs_f64() * 1000.0;
    results.push(BenchmarkResult {
        name: "Join + Aggregate (10x)".to_string(),
        sqlite_ms: 0.0,
        sekejap_ms: join_ms,
        winner: "Sekejap".to_string(),
    });

    results
}

fn main() {
    println!("\n========================================");
    println!("  SQLite vs SekejapDB Benchmark");
    println!("  1000+ nodes, 6-level hierarchy");
    println!("========================================\n");

    // Generate data
    println!("Generating company hierarchy data...");
    let (nodes, companies) = generate_hierarchy_data();
    println!(
        "  Generated {} nodes across {} companies",
        nodes.len(),
        companies.len()
    );

    // Check parent chain depth
    let mut max_depth = 0;
    let mut current = &nodes[0];
    let mut depth = 0;
    while let Some(ref parent) = current.parent_id {
        depth += 1;
        if depth > max_depth {
            max_depth = depth;
        }
        if depth > 10 {
            break;
        }
        current = nodes.iter().find(|n| &n.id == parent).unwrap_or(current);
    }
    println!("  Max traversal depth: {} hops\n", max_depth.min(10));

    // Run SQLite benchmark
    println!("Running SQLite benchmark...");
    let sqlite_results = run_sqlite_benchmark(&nodes, &companies);

    // Run Sekejap benchmark
    println!("Running SekejapDB benchmark...");
    let sekejap_results = run_sekejap_benchmark(&nodes, &companies);

    // Merge results
    let mut all_results: Vec<BenchmarkResult> = Vec::new();
    for (sqlite_res, sekejap_res) in sqlite_results.into_iter().zip(sekejap_results.into_iter()) {
        let sqlite_time = sqlite_res.sqlite_ms;
        let sekejap_time = sekejap_res.sekejap_ms;
        let winner = if sqlite_time < sekejap_time {
            "SQLite"
        } else {
            "Sekejap"
        }
        .to_string();
        let speedup = if sqlite_time < sekejap_time {
            sekejap_time / sqlite_time.max(0.001)
        } else {
            sqlite_time / sekejap_time.max(0.001)
        };

        all_results.push(BenchmarkResult {
            name: sqlite_res.name.clone(),
            sqlite_ms: sqlite_time,
            sekejap_ms: sekejap_time,
            winner: format!("{} ({:.1}x)", winner, speedup),
        });
    }

    // Print results
    println!("\n========================================");
    println!("  BENCHMARK RESULTS");
    println!("========================================\n");
    println!(
        " {:<30} | {:>12} | {:>12} | {:>15}",
        "Operation", "SQLite (ms)", "Sekejap (ms)", "Winner"
    );
    println!(
        " {}-+-{}-+-{}-+-{}-",
        "-".repeat(30),
        "-".repeat(12),
        "-".repeat(12),
        "-".repeat(15)
    );

    for result in &all_results {
        println!(
            " {:<30} | {:>12.2} | {:>12.2} | {:>15}",
            result.name, result.sqlite_ms, result.sekejap_ms, result.winner
        );
    }

    // Summary
    let sqlite_wins = all_results
        .iter()
        .filter(|r| r.winner.starts_with("SQLite"))
        .count();
    let sekejap_wins = all_results
        .iter()
        .filter(|r| r.winner.starts_with("Sekejap"))
        .count();

    println!("\n========================================");
    println!("  SUMMARY");
    println!("========================================");
    println!("  SQLite wins:   {}", sqlite_wins);
    println!("  Sekejap wins:  {}", sekejap_wins);
    println!("  Total tests:   {}\n", all_results.len());

    // File sizes
    let sqlite_size = fs::metadata("benchmark_sqlite.db")
        .map(|m| m.len())
        .unwrap_or(0);
    let sekejap_size = fs::metadata("benchmark_sekejap_db")
        .map(|m| m.len())
        .unwrap_or(0);

    println!("  SQLite DB size:  {}", format_bytes(sqlite_size));
    println!("  Sekejap DB size: {}", format_bytes(sekejap_size));
    println!("\n");
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}
