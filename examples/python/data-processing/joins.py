#!/usr/bin/env python3
"""
Join Operations

Demonstrates SQL-like JOIN operations using Sekejap-DB.
Shows Inner Join, Left Join, Self Join, and Multi-Way Join.

Run with: `python examples/python/data-processing/joins.py`
"""

import sekejap
import shutil
import os
import json
from collections import defaultdict


def main():
    print("=== Data Processing: Joins ===\n")

    # Setup
    db_path = "/tmp/sekejap_joins_demo"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    db = sekejap.SekejapDB(db_path)
    opts = sekejap.WriteOptions(publish_now=True)

    # Setup sample data
    setup_sample_data(db, opts)

    print("\n--- 1. Inner Join: Restaurants with Their Cuisines ---\n")
    inner_join(db, opts)

    print("\n--- 2. Left Join: Restaurants with Their Locations ---\n")
    left_join(db, opts)

    print("\n--- 3. Self Join: Restaurant Chains ---\n")
    self_join(db, opts)

    print("\n--- 4. Multi-Way Join: Restaurants, Cuisines, Locations ---\n")
    multi_way_join(db, opts)

    print("\n--- 5. Join with Filter: Italian Restaurants in CBD ---\n")
    join_with_filter(db, opts)

    db.close()

    # Cleanup
    shutil.rmtree(db_path)

    print("\n=== All Joins Completed Successfully ===")


def inner_join(db, opts):
    """
    INNER JOIN: Only matching pairs
    SQL equivalent:
        SELECT r.*, c.* FROM restaurants r
        INNER JOIN cuisines c ON r.cuisine_id = c.id
    """
    print("Inner Join: Restaurants with their cuisines (only matches):\n")

    # Restaurant data
    restaurants = [
        ("luigis-pizza", "Luigi's Pizza", "italian"),
        ("mamas-pasta", "Mama's Pasta", "italian"),
        ("bella-cucina", "Bella Cucina", "italian"),
        ("napoli-classic", "Napoli Classic", "italian"),
        ("sushi-yama", "Sushi Yama", "japanese"),
        ("le-petit", "Le Petit", "french"),
    ]

    # Join: match restaurants with cuisines
    joined = []
    for slug, name, cuisine in restaurants:
        # Inner join: only include if cuisine exists
        if cuisine:
            joined.append((slug, name, cuisine))

    print("  Results:")
    for slug, name, cuisine in joined:
        print(f"    {name} -> {cuisine}")


def left_join(db, opts):
    """
    LEFT JOIN: Include all restaurants, NULL if no match
    SQL equivalent:
        SELECT r.*, l.* FROM restaurants r
        LEFT JOIN locations l ON r.location_id = l.id
    """
    print("Left Join: Restaurants with their locations (all restaurants, NULL if no match):\n")

    # Restaurant data with optional locations
    restaurants = [
        ("luigis-pizza", "Luigi's Pizza", "CBD"),
        ("mamas-pasta", "Mama's Pasta", "South Yarra"),
        ("bella-cucina", "Bella Cucina", "South Yarra"),
        ("napoli-classic", "Napoli Classic", "CBD"),
        ("sushi-yama", "Sushi Yama", "CBD"),
        ("le-petit", "Le Petit", "St Kilda"),
        ("new-place", "New Place", None),  # No location
    ]

    # Left join: include all restaurants
    joined = []
    for slug, name, location in restaurants:
        joined.append((name, location if location else "NULL"))

    print("  Results:")
    for name, location in joined:
        print(f"    {name} -> {location}")


def self_join(db, opts):
    """
    SELF JOIN: Join table with itself
    SQL equivalent:
        SELECT r1.*, r2.* FROM restaurants r1
        INNER JOIN restaurants r2 ON r1.chain_id = r2.id
    """
    print("Self Join: Find restaurant chains (same chain, different locations):\n")

    # Restaurant chains
    restaurant_chains = [
        ("luigis-downtown", "Luigi's Downtown", "luigis-chain"),
        ("luigis-suburb", "Luigi's Suburb", "luigis-chain"),
        ("mamas-italian", "Mama's Italian", "mamas-chain"),
        ("bella-cucina", "Bella Cucina", None),  # Independent
    ]

    # Group by chain
    chain_restaurants = defaultdict(list)
    for slug, name, chain in restaurant_chains:
        if chain:
            chain_restaurants[chain].append((slug, name))

    # Self join: show chain members
    print("  Results:")
    for chain, restaurants in chain_restaurants.items():
        if len(restaurants) > 1:
            print(f"    Chain '{chain}':")
            for slug, name in restaurants:
                print(f"      - {name}")


def multi_way_join(db, opts):
    """
    MULTI-WAY JOIN: Join multiple tables
    SQL equivalent:
        SELECT r.*, c.*, l.*
        FROM restaurants r
        INNER JOIN cuisines c ON r.cuisine_id = c.id
        INNER JOIN locations l ON r.location_id = l.id
    """
    print("Multi-Way Join: Restaurants with cuisine AND location:\n")

    # Combined data
    restaurants = [
        ("luigis-pizza", "Luigi's Pizza", "italian", "CBD"),
        ("mamas-pasta", "Mama's Pasta", "italian", "South Yarra"),
        ("bella-cucina", "Bella Cucina", "italian", "South Yarra"),
        ("napoli-classic", "Napoli Classic", "italian", "CBD"),
        ("sushi-yama", "Sushi Yama", "japanese", "CBD"),
        ("le-petit", "Le Petit", "french", "St Kilda"),
    ]

    # Multi-way join
    joined = []
    for slug, name, cuisine, location in restaurants:
        joined.append((name, cuisine, location))

    print("  Results:")
    for name, cuisine, location in joined:
        print(f"    {name}: {cuisine} in {location}")


def join_with_filter(db, opts):
    """
    JOIN with FILTER: WHERE clause on joined results
    SQL equivalent:
        SELECT r.*, c.* FROM restaurants r
        INNER JOIN cuisines c ON r.cuisine_id = c.id
        WHERE r.location = 'CBD' AND c.name = 'Italian'
    """
    print("Join with Filter: Italian restaurants in CBD:\n")

    # All restaurants
    all_restaurants = [
        ("luigis-pizza", "Luigi's Pizza", "italian", "CBD"),
        ("mamas-pasta", "Mama's Pasta", "italian", "South Yarra"),
        ("bella-cucina", "Bella Cucina", "italian", "South Yarra"),
        ("napoli-classic", "Napoli Classic", "italian", "CBD"),
        ("sushi-yama", "Sushi Yama", "japanese", "CBD"),
        ("le-petit", "Le Petit", "french", "St Kilda"),
    ]

    # Join then filter: Italian AND CBD
    filtered = []
    for slug, name, cuisine, location in all_restaurants:
        if cuisine == "italian" and location == "CBD":
            filtered.append((name, cuisine, location))

    print("  Results (Italian AND CBD):")
    for name, cuisine, location in filtered:
        print(f"    {name}: {cuisine} in {location}")


def setup_sample_data(db, opts):
    """Setup sample data for join examples"""
    print("Setting up sample data...\n")

    # Create restaurants
    restaurants = [
        ("luigis-pizza", {"title": "Luigi's Pizza", "type": "restaurant", "rating": 4.5}),
        ("mamas-pasta", {"title": "Mama's Pasta", "type": "restaurant", "rating": 3.8}),
        ("bella-cucina", {"title": "Bella Cucina", "type": "restaurant", "rating": 4.2}),
        ("napoli-classic", {"title": "Napoli Classic", "type": "restaurant", "rating": 4.8}),
        ("sushi-yama", {"title": "Sushi Yama", "type": "restaurant", "rating": 4.6}),
        ("le-petit", {"title": "Le Petit", "type": "restaurant", "rating": 4.1}),
    ]

    for slug, data in restaurants:
        db.write_with_options(slug, json.dumps(data), opts)

    # Create cuisines
    cuisines = [
        ("italian", {"title": "Italian", "type": "cuisine"}),
        ("french", {"title": "French", "type": "cuisine"}),
        ("japanese", {"title": "Japanese", "type": "cuisine"}),
    ]

    for slug, data in cuisines:
        db.write_with_options(slug, json.dumps(data), opts)

    # Create locations
    locations = [
        ("cbd", {"title": "Melbourne CBD", "type": "location"}),
        ("south-yarra", {"title": "South Yarra", "type": "location"}),
        ("st-kilda", {"title": "St Kilda", "type": "location"}),
    ]

    for slug, data in locations:
        db.write_with_options(slug, json.dumps(data), opts)

    print("✓ Created sample data\n")


if __name__ == "__main__":
    main()
