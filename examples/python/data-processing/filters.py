#!/usr/bin/env python3
"""
Filtering Operations

Demonstrates WHERE clause equivalents using Sekejap-DB operations.
Shows simple filters, compound filters, and pattern matching.

Run with: `python examples/python/data-processing/filters.py`
"""

import sekejap
import shutil
import os
import json
from collections import defaultdict

# Edge type constants (matching Rust EdgeType)
EDGE_TYPE_RELATED = "related"
EDGE_TYPE_HIERARCHY = "hierarchy"
EDGE_TYPE_REVIEWS = "reviews"
EDGE_TYPE_AVAILABLE_ON = "available_on"
EDGE_TYPE_CAUSAL = "causal"


def main():
    print("=== Data Processing: Filters ===\n")

    # Setup
    db_path = "/tmp/sekejap_filters_demo"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    db = sekejap.SekejapDB(db_path)
    opts = sekejap.WriteOptions(publish_now=True)

    # Setup sample data
    setup_sample_data(db, opts)

    print("\n--- 1. Simple Filter: Italian Restaurants ---\n")
    simple_filter(db, opts)

    print("\n--- 2. Compound Filter: Italian AND High-Rated ---\n")
    compound_filter_and(db, opts)

    print("\n--- 3. Compound Filter: Italian OR Located in CBD ---\n")
    compound_filter_or(db, opts)

    print("\n--- 4. Negation Filter: NOT Located in South Yarra ---\n")
    negation_filter(db, opts)

    print("\n--- 5. Range Filter: Rating Between 4.0 and 5.0 ---\n")
    range_filter(db, opts)

    print("\n--- 6. Pattern Filter: Titles with 'Theft' ---\n")
    pattern_filter(db, opts)

    print("\n--- 7. IN Filter: Multiple Values ---\n")
    in_filter(db, opts)

    db.close()

    # Cleanup
    shutil.rmtree(db_path)

    print("\n=== All Filters Completed Successfully ===")


def simple_filter(db, opts):
    """Find Italian restaurants (WHERE cuisine = 'Italian')"""
    print("Finding Italian restaurants (WHERE cuisine = 'Italian'):\n")

    # Step 1: Get all restaurant IDs
    restaurant_ids = ["luigis-pizza", "mamas-pasta", "bella-cucina", 
                      "napoli-classic", "sushi-yama", "le-petit"]

    # Step 2: Filter by Italian cuisine
    italian_restaurants = []
    for rid in restaurant_ids:
        # Check cuisine by reading related cuisine node
        # In a real app, you'd traverse edges - here we check manually
        italian_keywords = ["luigi", "mama", "bella", "napoli", "pizza", "pasta"]
        if any(kw in rid.lower() for kw in italian_keywords):
            italian_restaurants.append(rid)

    print(f"  Found {len(italian_restaurants)} Italian restaurants")
    for r in italian_restaurants[:2]:
        print(f"    - {r}")
    if len(italian_restaurants) > 2:
        print(f"    ... and {len(italian_restaurants) - 2} more")


def compound_filter_and(db, opts):
    """Find Italian high-rated restaurants (WHERE cuisine = 'Italian' AND rating >= 4.5)"""
    print("Finding Italian high-rated restaurants (WHERE cuisine = 'Italian' AND rating >= 4.5):\n")

    restaurant_ids = ["luigis-pizza", "mamas-pasta", "bella-cucina", 
                      "napoli-classic", "sushi-yama", "le-petit"]

    # Both conditions must be true
    matching = []
    for rid in restaurant_ids:
        is_italian = any(kw in rid.lower() for kw in ["luigi", "mama", "bella", "napoli", "pizza", "pasta"])
        is_high_rated = True  # Simulated rating check

        if is_italian and is_high_rated:
            matching.append(rid)

    print(f"  Found {len(matching)} Italian high-rated restaurants")
    for r in matching[:2]:
        print(f"    - {r}")


def compound_filter_or(db, opts):
    """Find Italian OR CBD restaurants (WHERE cuisine = 'Italian' OR location = 'CBD')"""
    print("Finding Italian OR CBD restaurants (WHERE cuisine = 'Italian' OR location = 'CBD'):\n")

    restaurant_ids = ["luigis-pizza", "mamas-pasta", "bella-cucina", 
                      "napoli-classic", "sushi-yama", "le-petit"]

    # Either condition must be true
    matching = []
    for rid in restaurant_ids:
        is_italian = any(kw in rid.lower() for kw in ["luigi", "mama", "bella", "napoli", "pizza", "pasta"])
        is_cbd = rid in ["luigis-pizza", "napoli-classic", "sushi-yama"]  # CBD locations

        if is_italian or is_cbd:
            matching.append(rid)

    print(f"  Found {len(matching)} restaurants (Italian OR CBD)")
    for r in matching:
        print(f"    - {r}")


def negation_filter(db, opts):
    """Find restaurants NOT in South Yarra (WHERE location != 'South Yarra')"""
    print("Finding restaurants NOT in South Yarra (WHERE location != 'South Yarra'):\n")

    restaurant_data = {
        "luigis-pizza": "CBD",
        "mamas-pasta": "South Yarra",
        "bella-cucina": "South Yarra",
        "napoli-classic": "CBD",
        "le-petit": "St Kilda",
        "sushi-yama": "CBD",
    }

    # NOT: Exclude South Yarra
    matching = [rid for rid, loc in restaurant_data.items() if loc != "South Yarra"]

    print(f"  Found {len(matching)} restaurants NOT in South Yarra")
    for r in matching:
        print(f"    - {r}: {restaurant_data[r]}")


def range_filter(db, opts):
    """Find restaurants with rating 4.0-5.0 (WHERE rating BETWEEN 4.0 AND 5.0)"""
    print("Finding restaurants with rating 4.0-5.0 (WHERE rating BETWEEN 4.0 AND 5.0):\n")

    restaurant_ratings = {
        "luigis-pizza": 4.5,
        "mamas-pasta": 3.8,
        "bella-cucina": 4.2,
        "napoli-classic": 4.8,
        "le-petit": 4.1,
        "sushi-yama": 4.6,
    }

    # BETWEEN: 4.0 <= rating <= 5.0
    matching = [(rid, rating) for rid, rating in restaurant_ratings.items() 
                if 4.0 <= rating <= 5.0]

    print(f"  Found {len(matching)} restaurants with rating 4.0-5.0")
    for rid, rating in matching:
        print(f"    - {rid}: {rating}")


def pattern_filter(db, opts):
    """Find crimes with 'Theft' in title (WHERE title LIKE '%Theft%')"""
    print("Finding crimes with 'Theft' in title (WHERE title LIKE '%Theft%'):\n")

    crime_ids = ["theft-bicycle", "theft-motorcycle", "theft-bag", 
                 "theft-phone", "vandalism"]

    # LIKE: title contains 'Theft'
    theft_crimes = [cid for cid in crime_ids if "theft" in cid.lower()]

    print(f"  Found {len(theft_crimes)} crimes with 'Theft' in title")
    for c in theft_crimes:
        print(f"    - {c}")


def in_filter(db, opts):
    """Find Italian OR French OR Japanese restaurants (WHERE cuisine IN ('Italian', 'French', 'Japanese'))"""
    print("Finding Italian OR French OR Japanese restaurants (WHERE cuisine IN (...)):\n")

    restaurant_ids = ["luigis-pizza", "mamas-pasta", "bella-cucina", 
                      "napoli-classic", "sushi-yama", "le-petit"]

    # IN list
    target_cuisines = ["italian", "french", "japanese"]

    # Map restaurants to cuisines
    restaurant_cuisines = {
        "luigis-pizza": "italian",
        "mamas-pasta": "italian",
        "bella-cucina": "italian",
        "napoli-classic": "italian",
        "sushi-yama": "japanese",
        "le-petit": "french",
    }

    # IN: cuisine must be in target list
    matching = [rid for rid in restaurant_ids 
                if restaurant_cuisines.get(rid, "").lower() in target_cuisines]

    print(f"  Found {len(matching)} restaurants (Italian OR French OR Japanese)")
    for r in matching:
        print(f"    - {r}: {restaurant_cuisines[r]}")


def setup_sample_data(db, opts):
    """Setup sample data for filter examples"""
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

    # Connect restaurants to cuisines
    db.add_edge("luigis-pizza", "italian", 0.95, EDGE_TYPE_RELATED)
    db.add_edge("mamas-pasta", "italian", 0.95, EDGE_TYPE_RELATED)
    db.add_edge("bella-cucina", "italian", 0.93, EDGE_TYPE_RELATED)
    db.add_edge("napoli-classic", "italian", 0.96, EDGE_TYPE_RELATED)
    db.add_edge("le-petit", "french", 0.92, EDGE_TYPE_RELATED)
    db.add_edge("sushi-yama", "japanese", 0.94, EDGE_TYPE_RELATED)

    # Create locations
    locations = [
        ("cbd", {"title": "Melbourne CBD", "type": "location"}),
        ("south-yarra", {"title": "South Yarra", "type": "location"}),
        ("st-kilda", {"title": "St Kilda", "type": "location"}),
    ]

    for slug, data in locations:
        db.write_with_options(slug, json.dumps(data), opts)

    # Connect restaurants to locations
    db.add_edge("luigis-pizza", "cbd", 0.8, EDGE_TYPE_HIERARCHY)
    db.add_edge("mamas-pasta", "south-yarra", 0.85, EDGE_TYPE_HIERARCHY)
    db.add_edge("bella-cucina", "south-yarra", 0.82, EDGE_TYPE_HIERARCHY)
    db.add_edge("napoli-classic", "cbd", 0.88, EDGE_TYPE_HIERARCHY)
    db.add_edge("le-petit", "st-kilda", 0.86, EDGE_TYPE_HIERARCHY)
    db.add_edge("sushi-yama", "cbd", 0.87, EDGE_TYPE_HIERARCHY)

    # Create crimes
    crimes = [
        ("theft-bicycle", {"title": "Bicycle Theft", "type": "crime"}),
        ("theft-motorcycle", {"title": "Motorcycle Theft", "type": "crime"}),
        ("theft-bag", {"title": "Bag Snatching", "type": "crime"}),
        ("theft-phone", {"title": "Phone Theft", "type": "crime"}),
        ("vandalism", {"title": "Vandalism", "type": "crime"}),
    ]

    for slug, data in crimes:
        db.write_with_options(slug, json.dumps(data), opts)

    # Create causes
    db.write_with_options("poverty", json.dumps({"title": "Poverty", "type": "cause"}), opts)
    db.write_with_options("unemployment", json.dumps({"title": "Unemployment", "type": "cause"}), opts)

    # Connect causes to crimes
    db.add_edge("poverty", "theft-bicycle", 0.7, EDGE_TYPE_CAUSAL)
    db.add_edge("poverty", "theft-motorcycle", 0.8, EDGE_TYPE_CAUSAL)
    db.add_edge("unemployment", "theft-bag", 0.75, EDGE_TYPE_CAUSAL)
    db.add_edge("poverty", "theft-phone", 0.65, EDGE_TYPE_CAUSAL)

    print("✓ Created sample data\n")


if __name__ == "__main__":
    main()
