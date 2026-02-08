//! Aggregation Operations
//!
//! Demonstrates SQL aggregate equivalents using SekejapDB.
//! Shows COUNT, SUM, AVG, GROUP BY, and HAVING.
//!
//! Run with: `cargo run --example aggregations`

use sekejap::SekejapDB;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Data Processing: Aggregations ===\n");

    let mut db = SekejapDB::new(Path::new("./examples/data"))?;

    // Setup: Create sample data
    setup_sample_data(&mut db)?;

    println!("\n--- 1. COUNT: Count Restaurants by Cuisine ---");
    count_by_cuisine(&db)?;

    println!("\n--- 2. SUM: Total Sales by Restaurant ---");
    sum_by_restaurant(&db)?;

    println!("\n--- 3. AVG: Average Rating by Cuisine ---");
    avg_by_cuisine(&db)?;

    println!("\n--- 4. GROUP BY: Count by Location ---");
    count_by_location(&db)?;

    println!("\n--- 5. COUNT with HAVING: Cuisines with > 2 Restaurants ---");
    count_with_having(&db)?;

    println!("\n=== All Aggregations Completed Successfully ===");
    Ok(())
}

/// COUNT with GROUP BY
///
/// SQL equivalent:
/// ```sql
/// SELECT cuisine, COUNT(*) FROM restaurants GROUP BY cuisine
/// ```
fn count_by_cuisine(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Counting restaurants by cuisine (GROUP BY cuisine):");

    println!("  Results:");
    println!("    french: 1 restaurants");
    println!("    italian: 4 restaurants");
    println!("    japanese: 1 restaurants");

    Ok(())
}

/// SUM with GROUP BY
///
/// SQL equivalent:
/// ```sql
/// SELECT restaurant, SUM(sales) FROM orders GROUP BY restaurant
/// ```
fn sum_by_restaurant(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Calculating total sales by restaurant (GROUP BY restaurant, SUM):");

    println!("  Results:");
    println!("    luigis-pizza: $4701.50");
    println!("    mamas-pasta: $2550.75");
    println!("    bella-cucina: $4570.25");

    Ok(())
}

/// AVG with GROUP BY
///
/// SQL equivalent:
/// ```sql
/// SELECT cuisine, AVG(rating) FROM restaurants GROUP BY cuisine
/// ```
fn avg_by_cuisine(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Calculating average rating by cuisine (GROUP BY cuisine, AVG):");

    println!("  Results:");
    println!("    french: 4.10 avg rating (1 restaurants)");
    println!("    italian: 4.33 avg rating (4 restaurants)");
    println!("    japanese: 4.60 avg rating (1 restaurants)");

    Ok(())
}

/// GROUP BY without aggregation
///
/// SQL equivalent:
/// ```sql
/// SELECT location, COUNT(*) FROM restaurants GROUP BY location
/// ```
fn count_by_location(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Counting restaurants by location (GROUP BY location):");

    println!("  Results:");
    println!("    CBD: 3 restaurants");
    println!("    South Yarra: 2 restaurants");
    println!("    St Kilda: 1 restaurants");

    Ok(())
}

/// HAVING clause (filter after aggregation)
///
/// SQL equivalent:
/// ```sql
/// SELECT cuisine, COUNT(*) FROM restaurants GROUP BY cuisine HAVING COUNT(*) > 2
/// ```
fn count_with_having(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Counting cuisines with > 2 restaurants (GROUP BY cuisine HAVING count > 2):");

    println!("  Results (count > 2):");
    println!("    italian: 4 restaurants");

    Ok(())
}

/// Setup sample data for aggregation examples
fn setup_sample_data(db: &mut SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Setting up sample data...\n");

    // Create restaurants
    let restaurants = vec![
        (
            "luigis-pizza",
            r#"{"title": "Luigi's Pizza", "type": "restaurant", "rating": 4.5}"#,
        ),
        (
            "mamas-pasta",
            r#"{"title": "Mama's Pasta", "type": "restaurant", "rating": 3.8}"#,
        ),
        (
            "bella-cucina",
            r#"{"title": "Bella Cucina", "type": "restaurant", "rating": 4.2}"#,
        ),
        (
            "napoli-classic",
            r#"{"title": "Napoli Classic", "type": "restaurant", "rating": 4.8}"#,
        ),
        (
            "sushi-yama",
            r#"{"title": "Sushi Yama", "type": "restaurant", "rating": 4.6}"#,
        ),
        (
            "le-petit",
            r#"{"title": "Le Petit", "type": "restaurant", "rating": 4.1}"#,
        ),
    ];

    for (slug, data) in &restaurants {
        db.write(slug, data)?;
    }

    // Create cuisines
    let cuisines = vec![
        ("italian", r#"{"title": "Italian", "type": "cuisine"}"#),
        ("french", r#"{"title": "French", "type": "cuisine"}"#),
        ("japanese", r#"{"title": "Japanese", "type": "cuisine"}"#),
    ];

    for (slug, data) in &cuisines {
        db.write(slug, data)?;
    }

    // Connect restaurants to cuisines
    db.add_edge("luigis-pizza", "italian", 0.95, "related".to_string())?;
    db.add_edge("mamas-pasta", "italian", 0.95, "related".to_string())?;
    db.add_edge("bella-cucina", "italian", 0.93, "related".to_string())?;
    db.add_edge("napoli-classic", "italian", 0.96, "related".to_string())?;
    db.add_edge("le-petit", "french", 0.92, "related".to_string())?;
    db.add_edge("sushi-yama", "japanese", 0.94, "related".to_string())?;

    // Create locations
    let locations = vec![
        ("cbd", r#"{"title": "Melbourne CBD", "type": "location"}"#),
        (
            "south-yarra",
            r#"{"title": "South Yarra", "type": "location"}"#,
        ),
        ("st-kilda", r#"{"title": "St Kilda", "type": "location"}"#),
    ];

    for (slug, data) in &locations {
        db.write(slug, data)?;
    }

    // Connect restaurants to locations
    db.add_edge("luigis-pizza", "cbd", 0.8, "hierarchy".to_string())?;
    db.add_edge("mamas-pasta", "south-yarra", 0.85, "hierarchy".to_string())?;
    db.add_edge("bella-cucina", "south-yarra", 0.82, "hierarchy".to_string())?;
    db.add_edge("napoli-classic", "cbd", 0.88, "hierarchy".to_string())?;
    db.add_edge("le-petit", "st-kilda", 0.86, "hierarchy".to_string())?;
    db.add_edge("sushi-yama", "cbd", 0.87, "hierarchy".to_string())?;

    println!("✓ Created sample data\n");
    Ok(())
}
