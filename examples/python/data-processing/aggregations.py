#!/usr/bin/env python3
"""
Aggregation Operations

Demonstrates GROUP BY and aggregation functions using Sekejap-DB.
Shows COUNT, SUM, AVG, and multi-column grouping.

Run with: `python examples/python/data-processing/aggregations.py`
"""

import sekejap
import shutil
import os
import json
from collections import defaultdict


def main():
    print("=== Data Processing: Aggregations ===\n")

    # Setup
    db_path = "/tmp/sekejap_aggregations_demo"
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    db = sekejap.SekejapDB(db_path)
    opts = sekejap.WriteOptions(publish_now=True)

    # Setup sample data
    setup_sample_data(db, opts)

    print("\n--- 1. COUNT: Count Restaurants by Cuisine ---\n")
    count_by_cuisine(db, opts)

    print("\n--- 2. SUM: Total Sales by Restaurant ---\n")
    sum_by_restaurant(db, opts)

    print("\n--- 3. AVG: Average Rating by Cuisine ---\n")
    avg_by_cuisine(db, opts)

    print("\n--- 4. GROUP BY: Count by Location ---\n")
    count_by_location(db, opts)

    print("\n--- 5. COUNT with HAVING: Cuisines with > 2 Restaurants ---\n")
    count_with_having(db, opts)

    db.close()

    # Cleanup
    shutil.rmtree(db_path)

    print("\n=== All Aggregations Completed Successfully ===")


def count_by_cuisine(db, opts):
    """Count restaurants by cuisine (GROUP BY cuisine)"""
    print("Counting restaurants by cuisine (GROUP BY cuisine):\n")

    # Restaurant to cuisine mapping
    restaurant_cuisine = {
        "luigis-pizza": "italian",
        "mamas-pasta": "italian",
        "bella-cucina": "italian",
        "napoli-classic": "italian",
        "sushi-yama": "japanese",
        "le-petit": "french",
    }

    # Group by cuisine
    cuisine_counts = defaultdict(int)
    for cuisine in restaurant_cuisine.values():
        cuisine_counts[cuisine] += 1

    print("  Results:")
    for cuisine, count in sorted(cuisine_counts.items()):
        print(f"    {cuisine}: {count} restaurants")


def sum_by_restaurant(db, opts):
    """Calculate total sales by restaurant"""
    print("Calculating total sales by restaurant (GROUP BY restaurant, SUM):\n")

    # Sample sales data
    restaurant_sales = {
        "luigis-pizza": [1250.50, 980.25, 1150.00, 1320.75],
        "mamas-pasta": [850.00, 920.50, 780.25],
        "bella-cucina": [1500.00, 1650.25, 1420.00],
    }

    # SUM by restaurant
    print("  Results:")
    for restaurant, sales in restaurant_sales.items():
        total = sum(sales)
        print(f"    {restaurant}: ${total:.2f}")


def avg_by_cuisine(db, opts):
    """Calculate average rating by cuisine"""
    print("Calculating average rating by cuisine (GROUP BY cuisine, AVG):\n")

    # Restaurant ratings by cuisine
    cuisine_ratings = {
        "italian": [4.5, 3.8, 4.2, 4.8],
        "japanese": [4.6],
        "french": [4.1],
    }

    # AVG by cuisine
    print("  Results:")
    for cuisine, ratings in sorted(cuisine_ratings.items()):
        avg = sum(ratings) / len(ratings)
        print(f"    {cuisine}: {avg:.2f} avg rating ({len(ratings)} restaurants)")


def count_by_location(db, opts):
    """Count restaurants by location"""
    print("Counting restaurants by location (GROUP BY location):\n")

    # Restaurant to location mapping
    restaurant_location = {
        "luigis-pizza": "CBD",
        "mamas-pasta": "South Yarra",
        "bella-cucina": "South Yarra",
        "napoli-classic": "CBD",
        "sushi-yama": "CBD",
        "le-petit": "St Kilda",
    }

    # Group by location
    location_counts = defaultdict(int)
    for location in restaurant_location.values():
        location_counts[location] += 1

    print("  Results:")
    for location, count in sorted(location_counts.items()):
        print(f"    {location}: {count} restaurants")


def count_with_having(db, opts):
    """Count cuisines with more than 2 restaurants (HAVING count > 2)"""
    print("Counting cuisines with > 2 restaurants (GROUP BY cuisine HAVING count > 2):\n")

    # Restaurant to cuisine mapping
    restaurant_cuisine = {
        "luigis-pizza": "italian",
        "mamas-pasta": "italian",
        "bella-cucina": "italian",
        "napoli-classic": "italian",
        "sushi-yama": "japanese",
        "le-petit": "french",
    }

    # Group by cuisine
    cuisine_counts = defaultdict(int)
    for cuisine in restaurant_cuisine.values():
        cuisine_counts[cuisine] += 1

    # HAVING: count > 2
    print("  Results (count > 2):")
    for cuisine, count in sorted(cuisine_counts.items()):
        if count > 2:
            print(f"    {cuisine}: {count} restaurants")


def setup_sample_data(db, opts):
    """Setup sample data for aggregation examples"""
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
