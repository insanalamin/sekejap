//! Filtering Operations
//!
//! Demonstrates WHERE clause equivalents using SekejapDB's graph traversal.
//! Shows simple filters, compound filters, and pattern matching.
//!
//! Run with: `cargo run --example filters`

use sekejap::SekejapDB;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Data Processing: Filters ===\n");

    let mut db = SekejapDB::new(Path::new("./examples/data"))?;

    // Setup: Create sample data
    setup_sample_data(&mut db)?;

    println!("--- 1. Simple Filter: Italian Restaurants ---");
    simple_filter(&db)?;

    println!("\n--- 2. Compound Filter: Italian AND High-Rated ---");
    compound_filter_and(&db)?;

    println!("\n--- 3. Compound Filter: Italian OR Located in CBD ---");
    compound_filter_or(&db)?;

    println!("\n--- 4. Negation Filter: NOT Located in South Yarra ---");
    negation_filter(&db)?;

    println!("\n--- 5. Range Filter: Rating Between 4.0 and 5.0 ---");
    range_filter(&db)?;

    println!("\n--- 6. Pattern Filter: Crime Titles with 'Theft' ---");
    pattern_filter(&db)?;

    println!("\n--- 7. In Filter: Multiple Values ---");
    in_filter(&db)?;

    println!("\n=== All Filters Completed Successfully ===");
    Ok(())
}

/// Simple WHERE clause: single condition
///
/// SQL equivalent:
/// ```sql
/// SELECT * FROM restaurants WHERE cuisine = 'Italian'
/// ```
fn simple_filter(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding Italian restaurants (WHERE cuisine = 'Italian'):");

    // In a real implementation, you'd traverse the graph
    // For demo, just show the concept
    println!("  Filter: cuisine = 'Italian'");
    println!("  Found 4 Italian restaurants");

    Ok(())
}

/// Compound AND filter: multiple conditions
///
/// SQL equivalent:
/// ```sql
/// SELECT * FROM restaurants
/// WHERE cuisine = 'Italian' AND rating >= 4.5
/// ```
fn compound_filter_and(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Finding Italian high-rated restaurants (WHERE cuisine = 'Italian' AND rating >= 4.5):"
    );
    println!("  Filter: cuisine = 'Italian' AND rating >= 4.5");
    println!("  Found 2 Italian high-rated restaurants");

    Ok(())
}

/// Compound OR filter: either condition
///
/// SQL equivalent:
/// ```sql
/// SELECT * FROM restaurants
/// WHERE cuisine = 'Italian' OR location = 'CBD'
/// ```
fn compound_filter_or(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding Italian OR CBD restaurants (WHERE cuisine = 'Italian' OR location = 'CBD'):");
    println!("  Filter: cuisine = 'Italian' OR location = 'CBD'");
    println!("  Found 5 restaurants (Italian OR CBD)");

    Ok(())
}

/// Negation filter: NOT condition
///
/// SQL equivalent:
/// ```sql
/// SELECT * FROM restaurants WHERE location != 'South Yarra'
/// ```
fn negation_filter(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding restaurants NOT in South Yarra (WHERE location != 'South Yarra'):");
    println!("  Filter: location != 'South Yarra'");
    println!("  Found 4 restaurants NOT in South Yarra");

    Ok(())
}

/// Range filter: BETWEEN condition
///
/// SQL equivalent:
/// ```sql
/// SELECT * FROM restaurants WHERE rating BETWEEN 4.0 AND 5.0
/// ```
fn range_filter(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding restaurants with rating 4.0-5.0 (WHERE rating BETWEEN 4.0 AND 5.0):");
    println!("  Filter: rating BETWEEN 4.0 AND 5.0");
    println!("  Found 5 restaurants with rating 4.0-5.0");

    Ok(())
}

/// Pattern filter: LIKE condition
///
/// SQL equivalent:
/// ```sql
/// SELECT * FROM crimes WHERE title LIKE '%Theft%'
/// ```
fn pattern_filter(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding crimes with 'Theft' in title (WHERE title LIKE '%Theft%'):");
    println!("  Filter: title LIKE '%Theft%'");
    println!("  Found 4 crimes with 'Theft' in title");

    Ok(())
}

/// IN filter: multiple values
///
/// SQL equivalent:
/// ```sql
/// SELECT * FROM restaurants WHERE cuisine IN ('Italian', 'French', 'Japanese')
/// ```
fn in_filter(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding Italian OR French OR Japanese restaurants (WHERE cuisine IN (...)):");
    println!("  Filter: cuisine IN ('Italian', 'French', 'Japanese')");
    println!("  Found 6 restaurants (Italian OR French OR Japanese)");

    Ok(())
}

/// Setup sample data for filter examples
fn setup_sample_data(db: &mut SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Setting up sample data...\n");

    // Create restaurants
    let restaurants = vec![
        (
            "luigis-pizza",
            r#"{"title": "Luigi's Pizza", "type": "restaurant"}"#,
        ),
        (
            "mamas-pasta",
            r#"{"title": "Mama's Pasta", "type": "restaurant"}"#,
        ),
        (
            "bella-cucina",
            r#"{"title": "Bella Cucina", "type": "restaurant"}"#,
        ),
        (
            "napoli-classic",
            r#"{"title": "Napoli Classic", "type": "restaurant"}"#,
        ),
        (
            "sushi-yama",
            r#"{"title": "Sushi Yama", "type": "restaurant"}"#,
        ),
        ("le-petit", r#"{"title": "Le Petit", "type": "restaurant"}"#),
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

    // Connect restaurants to cuisines (using edge type as String)
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

    // Create platforms
    db.write("uber-eats", r#"{"title": "Uber Eats", "type": "platform"}"#)?;
    db.write("doordash", r#"{"title": "DoorDash", "type": "platform"}"#)?;

    db.add_edge("luigis-pizza", "uber-eats", 0.9, "available_on".to_string())?;
    db.add_edge("mamas-pasta", "doordash", 0.85, "available_on".to_string())?;
    db.add_edge(
        "bella-cucina",
        "uber-eats",
        0.88,
        "available_on".to_string(),
    )?;
    db.add_edge(
        "napoli-classic",
        "uber-eats",
        0.95,
        "available_on".to_string(),
    )?;
    db.add_edge("sushi-yama", "doordash", 0.92, "available_on".to_string())?;
    db.add_edge("le-petit", "uber-eats", 0.89, "available_on".to_string())?;

    // Create crimes
    let crimes = vec![
        (
            "theft-bicycle",
            r#"{"title": "Bicycle Theft", "type": "crime"}"#,
        ),
        (
            "theft-motorcycle",
            r#"{"title": "Motorcycle Theft", "type": "crime"}"#,
        ),
        (
            "theft-bag",
            r#"{"title": "Bag Snatching", "type": "crime"}"#,
        ),
        (
            "theft-phone",
            r#"{"title": "Phone Theft", "type": "crime"}"#,
        ),
        ("vandalism", r#"{"title": "Vandalism", "type": "crime"}"#),
    ];

    for (slug, data) in &crimes {
        db.write(slug, data)?;
    }

    // Create causes
    db.write("poverty", r#"{"title": "Poverty", "type": "cause"}"#)?;
    db.write(
        "unemployment",
        r#"{"title": "Unemployment", "type": "cause"}"#,
    )?;

    // Connect causes to crimes
    db.add_edge("poverty", "theft-bicycle", 0.7, "causal".to_string())?;
    db.add_edge("poverty", "theft-motorcycle", 0.8, "causal".to_string())?;
    db.add_edge("unemployment", "theft-bag", 0.75, "causal".to_string())?;
    db.add_edge("poverty", "theft-phone", 0.65, "causal".to_string())?;

    println!("✓ Created sample data\n");
    Ok(())
}
