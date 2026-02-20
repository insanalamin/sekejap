import sekejap
import os
import shutil
import time

def setup_db():
    if os.path.exists("./test_data_py"):
        shutil.rmtree("./test_data_py")
    return sekejap.SekejapDB("./test_data_py")

def test_basic_crud():
    print("\n--- Testing Basic CRUD ---")
    db = setup_db()
    
    # 1. Write - defaults to nodes/ slug
    db.write("event-001", '{"title": "Theft", "severity": "high"}')
    print("Write success")
    
    # 2. JSON Write with Vector & Geo
    db.write_json("""
{
    \"_id\": \"news/flood-2026\",
    \"title\": \"Flood in Jakarta\",
    \"vectors\": { \"dense\": [0.1, 0.2, 0.3] },
    \"geo\": { \"loc\": { \"lat\": -6.2, \"lon\": 106.8 } }
}
""")
    print("Write JSON success")
    
    # 3. Read
    event = db.read("news/flood-2026")
    if event:
        print(f"Found: {event}")
    else:
        raise Exception("Failed to read news/flood-2026")
        
    # 4. Delete
    db.delete("event-001")
    if db.read("event-001") is not None:
        raise Exception("Delete failed (node still exists)")
    print("Delete success")
        
    # 5. Delete with Options
    db.write("event-002", '{"title": "To Delete"}')
    db.delete_with_options("event-002", sekejap.DeleteOptions(exclude_edges=True))
    if db.read("event-002") is not None:
        raise Exception("Delete with options failed")
    print("Delete with options success")

def test_hybrid_query():
    print("\n--- Testing Hybrid Query ---")
    db = setup_db()
    
    # Setup Data
    db.write_json("""
{
    \"_id\": \"events/crash-kemang\",
    \"title\": \"Severe Traffic Accident in Kemang\",
    \"vectors\": { \"dense\": [0.9, 0.1, 0.1] },
    \"geo\": { \"loc\": { \"lat\": -6.27, \"lon\": 106.81 } }
}
""")
    
    db.write_json("""
{
    \"_id\": \"causes/heavy-rain\",
    \"title\": \"Heavy Rain\"
}
""")
    
    db.add_edge("causes/heavy-rain", "events/crash-kemang", 0.9, "caused")
    
    # Manual Flush to ensure indexing
    db.flush()
    time.sleep(0.5) 
    
    # Execute Query Builder
    results = db.query() \
        .has_edge_from("causes/heavy-rain", "caused") \
        .spatial(-6.27, 106.81, 5.0) \
        .fulltext("Accident") \
        .vector_search([0.9, 0.1, 0.1], 10) \
        .execute()
        
    print(f"Query Results: {results}")
    assert "events/crash-kemang" in results
    print("Hybrid Query success")

def test_traversal_aggregation():
    print("\n--- Testing Traversal & Aggregation ---")
    db = setup_db()
    
    # Hierarchy: Event -> SubDistrict -> District
    db.write_json('{"_id": "events/crash-1", "title": "Crash 1"}')
    db.write_json('{"_id": "regions/sub-1", "title": "SubDistrict 1"}')
    db.write_json('{"_id": "regions/3273", "title": "District 3273"}')
    
    # Link them
    db.add_edge("events/crash-1", "regions/sub-1", 1.0, "located_in")
    db.add_edge("regions/sub-1", "regions/3273", 1.0, "part_of")
    
    # Traverse 2 hops forward
    hierarchy = db.traverse_forward("events/crash-1", 2, 0.0, None)
    
    if hierarchy:
        path = hierarchy.path
        print(f"Path: {path}")
        assert "regions/3273" in path
        print("Hierarchy traversal success")
    else:
        raise Exception("Traversal returned None")

def test_causal_rca():
    print("\n--- Testing Causal RCA (Backward) ---")
    db = setup_db()
    
    db.write("crime-001", '{"title": "Crime"}') # nodes/crime-001
    db.write("poverty", '{"title": "Poverty"}') # nodes/poverty
    db.add_edge("poverty", "crime-001", 0.8, "causes")
    
    # Backward traversal
    results = db.traverse("crime-001", 5, 0.3, None)
    
    found_cause = False
    if results:
        for edge in results.edges:
            print(f"{edge.source} -> {edge.target} (weight: {edge.weight:.2})")
            if edge.source == "nodes/poverty" and edge.target == "nodes/crime-001":
                found_cause = True
    
    assert found_cause
    print("Causal RCA success")

if __name__ == "__main__":
    try:
        test_basic_crud()
        test_hybrid_query()
        test_traversal_aggregation()
        test_causal_rca()
        print("\n✅ ALL PYTHON PARITY TESTS PASSED")
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        exit(1)