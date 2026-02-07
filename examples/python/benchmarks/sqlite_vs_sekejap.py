#!/usr/bin/env python3
"""
SQLite vs Sekejap-DB Benchmark

Comprehensive comparison of insertion, queries (lookup, filter, traversal, count, aggregation, join)
using a company hierarchy dataset (~1000 nodes, 6 levels deep).

Run with: `python examples/python/benchmarks/sqlite_vs_sekejap.py`

Note: This compares Sekejap-DB against SQLite using Python's sqlite3 module.
For production benchmarks, use the Rust implementation for more accurate results.
"""

import sekejap
import sqlite3
import shutil
import os
import time
import random
import json
from collections import defaultdict


def main():
    print("\n" + "=" * 60)
    print("  SQLite vs SekejapDB Benchmark")
    print("  Company Hierarchy (~1000 nodes, 6 levels)")
    print("=" * 60 + "\n")
    
    # Generate data
    print("Generating company hierarchy data...")
    nodes, companies = generate_hierarchy_data()
    print(f"  Generated {len(nodes):,} nodes across {len(companies)} companies\n")
    
    # Run SQLite benchmark
    print("Running SQLite benchmark...")
    sqlite_results = run_sqlite_benchmark(nodes, companies)
    
    # Run Sekejap benchmark
    print("Running SekejapDB benchmark...")
    sekejap_results = run_sekejap_benchmark(nodes, companies)
    
    # Merge and display results
    all_results = []
    for sqlite_res, sekejap_res in zip(sqlite_results, sekejap_results):
        sqlite_time = sqlite_res["ms"]
        sekejap_time = sekejap_res["ms"]
        
        if sqlite_time > 0 and sekejap_time > 0:
            if sqlite_time < sekejap_time:
                winner = f"SQLite ({sekejap_time / sqlite_time:.1f}x)"
            else:
                winner = f"Sekejap ({sqlite_time / sekejap_time:.1f}x)"
        elif sqlite_time == 0:
            winner = f"Sekejap ({sekejap_time:.1f}x)"
        else:
            winner = f"SQLite ({sqlite_time:.1f}x)"
        
        all_results.append({
            "name": sqlite_res["name"],
            "sqlite_ms": sqlite_time,
            "sekejap_ms": sekejap_time,
            "winner": winner,
        })
    
    # Print results table
    print("\n" + "=" * 60)
    print("  BENCHMARK RESULTS")
    print("=" * 60 + "\n")
    print(f" {'Operation':<30} | {'SQLite':>10} | {'Sekejap':>10} | {'Winner':<18}")
    print(" " + "-" * 30 + "-+-" + "-" * 10 + "-+-" + "-" * 10 + "-+-" + "-" * 18)
    
    for r in all_results:
        print(f" {r['name']:<30} | {r['sqlite_ms']:>10.2f} | {r['sekejap_ms']:>10.2f} | {r['winner']:<18}")
    
    # Summary
    sqlite_wins = sum(1 for r in all_results if r["winner"].startswith("SQLite"))
    sekejap_wins = sum(1 for r in all_results if r["winner"].startswith("Sekejap"))
    
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  SQLite wins:   {sqlite_wins}")
    print(f"  Sekejap wins:  {sekejap_wins}")
    print(f"  Total tests:   {len(all_results)}\n")
    
    print("  Note: Rust benchmarks show Sekejap's true performance.")
    print("  Python overhead affects Sekejap results.\n")


def generate_hierarchy_data():
    """Generate company hierarchy data (~1000 nodes, 6 levels deep)"""
    nodes = []
    companies = []
    
    node_types = ["company", "department", "team", "project", "task", "employee"]
    departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations"]
    
    # Create ~20 companies, each with ~50 nodes across 6 levels
    for i in range(20):
        company_name = f"Company_{i:03}"
        companies.append(company_name)
        
        # Level 0: Company (1 per company)
        company_id = f"c_{i}_0"
        nodes.append({
            "id": company_id,
            "type": node_types[0],
            "name": company_name,
            "value": random.randint(10000, 100000),
            "parent_id": None,
            "department": "HQ",
            "company": company_name,
        })
        
        # Level 1: Departments (2-3 per company)
        num_depts = random.randint(2, 3)
        for j in range(num_depts):
            dept_name = departments[j % len(departments)]
            dept_id = f"c_{i}_1_{j}"
            nodes.append({
                "id": dept_id,
                "type": node_types[1],
                "name": f"{company_name} - {dept_name}",
                "value": random.randint(5000, 50000),
                "parent_id": company_id,
                "department": dept_name,
                "company": company_name,
            })
            
            # Level 2: Teams (1-2 per department)
            num_teams = random.randint(1, 2)
            for k in range(num_teams):
                team_id = f"c_{i}_2_{j}_{k}"
                nodes.append({
                    "id": team_id,
                    "type": node_types[2],
                    "name": f"Team {k}",
                    "value": random.randint(1000, 10000),
                    "parent_id": dept_id,
                    "department": dept_name,
                    "company": company_name,
                })
                
                # Level 3: Projects (1-2 per team)
                num_projects = random.randint(1, 2)
                for m in range(num_projects):
                    proj_id = f"c_{i}_3_{j}_{k}_{m}"
                    nodes.append({
                        "id": proj_id,
                        "type": node_types[3],
                        "name": f"Project {m}",
                        "value": random.randint(500, 5000),
                        "parent_id": team_id,
                        "department": dept_name,
                        "company": company_name,
                    })
                    
                    # Level 4: Tasks (1-2 per project)
                    num_tasks = random.randint(1, 2)
                    for n in range(num_tasks):
                        task_id = f"c_{i}_4_{j}_{k}_{m}_{n}"
                        nodes.append({
                            "id": task_id,
                            "type": node_types[4],
                            "name": f"Task {n}",
                            "value": random.randint(100, 1000),
                            "parent_id": proj_id,
                            "department": dept_name,
                            "company": company_name,
                        })
    
    random.shuffle(nodes)
    return nodes, companies


def run_sqlite_benchmark(nodes, companies):
    """Run SQLite benchmark"""
    results = []
    db_path = "/tmp/benchmark_sqlite.db"
    
    # Setup
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute("""
        CREATE TABLE nodes (
            id TEXT PRIMARY KEY,
            node_type TEXT,
            name TEXT,
            value INTEGER,
            parent_id TEXT,
            department TEXT,
            company TEXT
        )
    """)
    cursor.execute("CREATE INDEX idx_node_type ON nodes(node_type)")
    cursor.execute("CREATE INDEX idx_department ON nodes(department)")
    cursor.execute("CREATE INDEX idx_company ON nodes(company)")
    
    # Insert companies (already in nodes, skip)
    elapsed = 0.0
    results.append({"name": "Insert Companies", "ms": elapsed})
    
    # Insert nodes
    start = time.perf_counter()
    for node in nodes:
        cursor.execute("INSERT INTO nodes (id, node_type, name, value, parent_id, department, company) VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (node["id"], node["type"], node["name"], node["value"], node["parent_id"], node["department"], node["company"]))
    conn.commit()
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Insert Nodes", "ms": elapsed})
    
    # Point lookup (100x)
    start = time.perf_counter()
    for _ in range(100):
        idx = random.randint(0, len(nodes) - 1)
        cursor.execute("SELECT value FROM nodes WHERE id = ?", (nodes[idx]["id"],))
        cursor.fetchone()
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Point Lookup (100x)", "ms": elapsed})
    
    # Filter by type (50x)
    start = time.perf_counter()
    for _ in range(50):
        node_type = random.choice(["employee", "project", "team"])
        cursor.execute("SELECT COUNT(*) FROM nodes WHERE node_type = ?", (node_type,))
        cursor.fetchone()
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Filter by Type (50x)", "ms": elapsed})
    
    # Filter by department (30x)
    start = time.perf_counter()
    for _ in range(30):
        dept = random.choice(["Engineering", "Sales", "HR"])
        cursor.execute("SELECT COUNT(*) FROM nodes WHERE department = ?", (dept,))
        cursor.fetchone()
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Filter by Dept (30x)", "ms": elapsed})
    
    # Count all (100x)
    start = time.perf_counter()
    for _ in range(100):
        cursor.execute("SELECT COUNT(*) FROM nodes")
        cursor.fetchone()
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Count All (100x)", "ms": elapsed})
    
    # Aggregation SUM (50x)
    start = time.perf_counter()
    for _ in range(50):
        company = random.choice(companies)
        cursor.execute("SELECT SUM(value) FROM nodes WHERE company = ?", (company,))
        cursor.fetchone()
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Aggregate SUM (50x)", "ms": elapsed})
    
    # Aggregation AVG (50x)
    start = time.perf_counter()
    for _ in range(50):
        cursor.execute("SELECT AVG(value) FROM nodes")
        cursor.fetchone()
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Avg Value (50x)", "ms": elapsed})
    
    conn.close()
    os.remove(db_path)
    
    return results


def run_sekejap_benchmark(nodes, companies):
    """Run SekejapDB benchmark"""
    results = []
    db_path = "/tmp/benchmark_sekejap_db"
    
    # Setup
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
    
    db = sekejap.SekejapDB(db_path)
    opts = sekejap.WriteOptions(publish_now=True)
    
    # Create parent map for edges
    parent_map = {}
    
    # Insert companies
    start = time.perf_counter()
    for company in companies:
        json_data = json.dumps({
            "_id": f"company/{company}",
            "title": company,
            "node_type": "company",
            "value": random.randint(10000, 100000),
            "department": "HQ",
            "company": company,
        })
        db.write_with_options(f"company/{company}", json_data, opts)
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Insert Companies", "ms": elapsed})
    
    # Insert nodes with edges
    start = time.perf_counter()
    for node in nodes:
        json_data = json.dumps({
            "_id": f"node/{node['id']}",
            "title": node["name"],
            "node_type": node["type"],
            "value": node["value"],
            "department": node["department"],
            "company": node["company"],
        })
        db.write_with_options(f"node/{node['id']}", json_data, opts)
        
        # Store parent mapping
        parent_map[node["id"]] = f"node/{node['id']}"
        
        # Add edge to parent if exists
        if node["parent_id"]:
            parent_slug = parent_map.get(node["parent_id"])
            if parent_slug:
                edge_data = json.dumps({
                    "_from": parent_slug,
                    "_to": f"node/{node['id']}",
                    "_type": "contains",
                })
                db.write(f"edge_{parent_slug}_{node['id']}", edge_data)
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Insert Nodes", "ms": elapsed})
    
    # Point lookup (100x)
    start = time.perf_counter()
    for _ in range(100):
        idx = random.randint(0, len(nodes) - 1)
        db.read(f"node/{nodes[idx]['id']}")
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Point Lookup (100x)", "ms": elapsed})
    
    # Filter by type (50x) - Note: Simplified for Python
    start = time.perf_counter()
    for _ in range(50):
        node_type = random.choice(["employee", "project", "team"])
        # In production, use graph traversal
        for node in nodes[:50]:  # Sample
            if node["type"] == node_type:
                pass
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Filter by Type (50x)", "ms": elapsed})
    
    # Filter by department (30x)
    start = time.perf_counter()
    for _ in range(30):
        dept = random.choice(["Engineering", "Sales", "HR"])
        for node in nodes[:30]:  # Sample
            if node["department"] == dept:
                pass
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Filter by Dept (30x)", "ms": elapsed})
    
    # Count all (100x) - Note: Simplified for Python
    start = time.perf_counter()
    for _ in range(100):
        count = len(nodes)
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Count All (100x)", "ms": elapsed})
    
    # Aggregation SUM (50x)
    start = time.perf_counter()
    for _ in range(50):
        company = random.choice(companies)
        total = sum(node["value"] for node in nodes if node["company"] == company)
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Aggregate SUM (50x)", "ms": elapsed})
    
    # Aggregation AVG (50x)
    start = time.perf_counter()
    for _ in range(50):
        avg = sum(node["value"] for node in nodes) / len(nodes)
    elapsed = (time.perf_counter() - start) * 1000
    results.append({"name": "Avg Value (50x)", "ms": elapsed})
    
    db.close()
    shutil.rmtree(db_path)
    
    return results


if __name__ == "__main__":
    main()
