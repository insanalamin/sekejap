#!/usr/bin/env python3
"""
Sekejap-DB Python Examples - Basic Operations

A graph-first, embedded multi-model database engine.

Usage:
    cd wrappers/python
    pip install maturin
    maturin develop
    python examples/python/basic_operations.py
"""

import sekejap
import shutil
import os


def main():
    # Setup
    db_path = "/tmp/sekejap_demo"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)
    
    # Create database
    db = sekejap.SekejapDB(db_path)
    print("✓ Database created")
    
    # Instant write with publish_now=True (immediate read)
    opts = sekejap.WriteOptions(publish_now=True)
    node_id = db.write_with_options("crime-2024", '{"title": "Theft Incident", "tags": ["person", "vehicle"]}', opts)
    print(f"✓ Written with immediate read: {node_id}")
    
    # Read immediately
    event = db.read("crime-2024")
    print(f"✓ Read: {event[:100]}...")
    
    # Batch write with immediate read
    items = [
        ("event-1", '{"title": "Event 1"}'),
        ("event-2", '{"title": "Event 2"}'),
        ("event-3", '{"title": "Event 3"}'),
    ]
    ids = []
    for key, data in items:
        ids.append(db.write_with_options(key, data, opts))
    print(f"✓ Batch write: {len(ids)} events")
    
    # Add edge (causal relationship)
    # Note: Poverty event must exist first
    db.write_with_options("poverty", '{"title": "Poverty Event"}', opts)
    db.add_edge("poverty", "crime-2024", 0.7, "causal")
    print("✓ Edge added: poverty → crime-2024 (causal)")
    
    # Context manager usage
    with sekejap.SekejapDB("/tmp/sekejap_ctx") as ctx_db:
        ctx_db.write_with_options("ctx-event", '{"title": "Context event"}', opts)
        print("✓ Context manager works")
    
    # Cleanup
    db.close()
    print("\n✅ All operations completed successfully!")


if __name__ == "__main__":
    main()
