#!/usr/bin/env python3
"""
Write Performance Benchmarks

Compares Sekejap-DB write performance with different configurations:
- Single writes
- Batch writes
- Publish modes (Tier 1 only vs Tier 1+2)

Run with: `python examples/python/benchmarks/write_benchmark.py`
"""

import sekejap
import shutil
import os
import time
import random
import json
from collections import defaultdict


def main():
    print("=" * 60)
    print("  Sekejap-DB Write Performance Benchmarks")
    print("=" * 60)
    
    # Configuration
    NUM_RECORDS = 1000
    
    # Setup
    db_path = "/tmp/sekejap_write_benchmark"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
    
    db = sekejap.SekejapDB(db_path)
    
    print(f"\nBenchmarking {NUM_RECORDS:,} write operations...\n")
    
    # Generate test data
    test_data = generate_test_data(NUM_RECORDS)
    
    # Benchmark 1: Single writes (default mode)
    print("--- Test 1: Single Writes (default, staged to Tier 1) ---")
    elapsed = benchmark_single_writes(db, test_data, publish_now=False)
    print(f"  Time: {elapsed:.2f}ms")
    print(f"  Ops/sec: {NUM_RECORDS / (elapsed / 1000.0):,.0f}")
    
    # Clean up for next test
    shutil.rmtree(db_path)
    db = sekejap.SekejapDB(db_path)
    
    # Benchmark 2: Single writes with publish_now=True
    print("\n--- Test 2: Single Writes (publish_now=True) ---")
    elapsed = benchmark_single_writes(db, test_data, publish_now=True)
    print(f"  Time: {elapsed:.2f}ms")
    print(f"  Ops/sec: {NUM_RECORDS / (elapsed / 1000.0):,.0f}")
    
    # Clean up for next test
    shutil.rmtree(db_path)
    db = sekejap.SekejapDB(db_path)
    
    # Benchmark 3: Batch writes (simulated)
    print("\n--- Test 3: Batch Writes (loop with publish_now=True) ---")
    elapsed = benchmark_batch_writes(db, test_data)
    print(f"  Time: {elapsed:.2f}ms")
    print(f"  Ops/sec: {NUM_RECORDS / (elapsed / 1000.0):,.0f}")
    
    # Clean up for next test
    shutil.rmtree(db_path)
    db = sekejap.SekejapDB(db_path)
    
    # Benchmark 4: Write with edges
    print("\n--- Test 4: Write with Edges (node + 1 edge per node) ---")
    elapsed = benchmark_writes_with_edges(db, test_data)
    print(f"  Time: {elapsed:.2f}ms")
    print(f"  Ops/sec: {NUM_RECORDS / (elapsed / 1000.0):,.0f}")
    
    # Summary
    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    print(f"  Records: {NUM_RECORDS:,}")
    print("  See Rust benchmarks for SQLite comparison")
    print()
    
    db.close()
    shutil.rmtree(db_path)


def generate_test_data(n):
    """Generate test data"""
    data = []
    node_types = ["employee", "project", "team", "task", "event"]
    departments = ["Engineering", "Sales", "Marketing", "HR", "Finance"]
    
    for i in range(n):
        data.append({
            "id": f"node_{i:06d}",
            "type": random.choice(node_types),
            "department": random.choice(departments),
            "value": random.randint(1, 10000),
            "name": f"Item {i}",
        })
    
    return data


def benchmark_single_writes(db, data, publish_now):
    """Benchmark single writes"""
    opts = sekejap.WriteOptions(publish_now=publish_now)
    
    start = time.perf_counter()
    
    for item in data:
        json_data = json.dumps(item)
        db.write_with_options(item["id"], json_data, opts)
    
    elapsed = (time.perf_counter() - start) * 1000
    return elapsed


def benchmark_batch_writes(db, data):
    """Benchmark batch writes (simulated with loop)"""
    opts = sekejap.WriteOptions(publish_now=True)
    
    start = time.perf_counter()
    
    for item in data:
        json_data = json.dumps(item)
        db.write_with_options(item["id"], json_data, opts)
    
    elapsed = (time.perf_counter() - start) * 1000
    return elapsed


def benchmark_writes_with_edges(db, data):
    """Benchmark writes with edges"""
    opts = sekejap.WriteOptions(publish_now=True)
    
    start = time.perf_counter()
    
    for item in data:
        json_data = json.dumps(item)
        db.write_with_options(item["id"], json_data, opts)
        
        # Add edge if not the first item
        if item["id"] != "node_000000":
            prev_id = f"node_{int(item['id'].split('_')[1]) - 1:06d}"
            edge_type = "related"
            db.add_edge(prev_id, item["id"], 0.5, edge_type)
    
    elapsed = (time.perf_counter() - start) * 1000
    return elapsed


if __name__ == "__main__":
    main()
