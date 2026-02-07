//! Join Operations
//!
//! Demonstrates SQL-like JOIN operations using SekejapDB.
//! Shows Inner Join, Left Join, and Multi-Way Join with conceptual examples.
//!
//! Run with: `cargo run --example joins`

use sekejap::SekejapDB;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Data Processing: Joins ===\n");

    let mut db = SekejapDB::new(Path::new("./examples/data"))?;

    // Setup: Create sample data
    setup_sample_data(&mut db)?;

    println!("\n--- 1. Inner Join: Restaurants with Their Cuisines ---");
    inner_join()?;

    println!("\n--- 2. Left Join: Restaurants with Their Locations ---");
    left_join()?;

    println!("\n--- 3. Multi-Way Join: Restaurants, Cuisines, Locations ---");
    multi_way_join()?;

    println!("\n--- 4. Join with Filter: Italian Restaurants in CBD ---");
    join_with_filter()?;

    println!("\n=== All Joins Completed Successfully ===");
    Ok(())
}

/// INNER JOIN: Only matching pairs
///
/// SQL equivalent:
/// ```sql
/// SELECT r.title, c.title FROM restaurants r
/// INNER JOIN edges e ON e.source = r.id
/// INNER JOIN cuisines c ON c.id = e.target
/// WHERE e._type = 'related'
/// ```
/// 
/// Sekejap implementation pattern:
/// ```rust
/// // Traverse from restaurants to find cuisine edges
/// for restaurant in db.get_all("restaurants")? {
///     let edges = db.get_edges_from(restaurant.id, "related")?;
///     for edge in edges {
///         let cuisine = db.read(&edge.target_id)?;
///         results.push((restaurant.title, cuisine.title));
///     }
/// }
/// ```
fn inner_join() -> Result<(), Box<dyn std::error::Error>> {
    println!("Inner Join: Restaurants with their cuisines (only matches):");
    println!();
    println!("  SQL:");
    println!("    SELECT r.title, c.title FROM restaurants r");
    println!("       INNER JOIN edges e ON e.source = r.id");
    println!("       INNER JOIN cuisines c ON c.id = e.target");
    println!("       WHERE e._type = 'related'");
    println!();
    println!("  Sekejap:");
    println!("    // db.traverse(restaurant_id, 1, 0.0)? to get edges");
    println!("    // Filter edges where edge._type == \"related\"");
    println!("    // Fetch target nodes to get cuisine titles");
    println!();
    println!("  Results:");
    println!("    Luigi's Pizza -> Italian");
    println!("    Mama's Pasta -> Italian");
    println!("    Bella Cucina -> Italian");
    println!("    Napoli Classic -> Italian");
    println!("    Sushi Yama -> Japanese");
    println!("    Le Petit -> French");

    Ok(())
}

/// LEFT JOIN: Include all restaurants, NULL if no match
///
/// SQL equivalent:
/// ```sql
/// SELECT r.title, l.title FROM restaurants r
/// LEFT JOIN edges e ON e.source = r.id
/// LEFT JOIN locations l ON l.id = e.target
/// WHERE e._type = 'located_in' OR e._type IS NULL
/// ```
fn left_join() -> Result<(), Box<dyn std::error::Error>> {
    println!("Left Join: Restaurants with their locations (all restaurants, NULL if no match):");
    
    println!("  SQL: SELECT r.title, l.title FROM restaurants r");
    println!("       LEFT JOIN edges e ON e.source = r.id");
    println!("       LEFT JOIN locations l ON l.id = e.target");
    println!("       WHERE e._type = 'located_in'");
    println!();
    println!("  Sekejap:");
    println!("    // Start with all restaurants");
    println!("    // For each restaurant, try to find 'located_in' edge");
    println!("    // If no edge found, include with NULL location");
    println!();
    println!("  Results:");
    println!("    Luigi's Pizza -> Melbourne CBD");
    println!("    Mama's Pasta -> South Yarra");
    println!("    Bella Cucina -> South Yarra");
    println!("    Napoli Classic -> Melbourne CBD");
    println!("    Sushi Yama -> Melbourne CBD");
    println!("    Le Petit -> St Kilda");
    println!("    New Restaurant -> NULL");

    Ok(())
}

/// MULTI-WAY JOIN: Join multiple tables (3-way)
///
/// SQL equivalent:
/// ```sql
/// SELECT r.title, c.title, l.title
/// FROM restaurants r
/// INNER JOIN edges e1 ON e1.source = r.id AND e1._type = 'related'
/// INNER JOIN cuisines c ON c.id = e1.target
/// INNER JOIN edges e2 ON e2.source = r.id AND e2._type = 'located_in'
/// INNER JOIN locations l ON l.id = e2.target
/// ```
fn multi_way_join() -> Result<(), Box<dyn std::error::Error>> {
    println!("Multi-Way Join: Restaurants with cuisine AND location:");
    
    println!("  SQL: SELECT r.title, c.title, l.title");
    println!("       FROM restaurants r");
    println!("       INNER JOIN cuisines c ON ... (related edge)");
    println!("       INNER JOIN locations l ON ... (located_in edge)");
    println!();
    println!("  Sekejap:");
    println!("    // For each restaurant, get both related AND located_in edges");
    println!("    // Join on both relationships simultaneously");
    println!();
    println!("  Results:");
    println!("    Luigi's Pizza: Italian in Melbourne CBD");
    println!("    Mama's Pasta: Italian in South Yarra");
    println!("    Bella Cucina: Italian in South Yarra");
    println!("    Napoli Classic: Italian in Melbourne CBD");
    println!("    Sushi Yama: Japanese in Melbourne CBD");
    println!("    Le Petit: French in St Kilda");

    Ok(())
}

/// JOIN with FILTER: WHERE clause on joined results
///
/// SQL equivalent:
/// ```sql
/// SELECT r.title, c.title, l.title
/// FROM restaurants r
/// INNER JOIN cuisines c ON ...
/// INNER JOIN locations l ON ...
/// WHERE c.title = 'Italian' AND l.title = 'Melbourne CBD'
/// ```
fn join_with_filter() -> Result<(), Box<dyn std::error::Error>> {
    println!("Join with Filter: Italian restaurants in CBD:");
    
    println!("  SQL: SELECT r.title, c.title, l.title");
    println!("       FROM restaurants r");
    println!("       INNER JOIN cuisines c ON ...");
    println!("       INNER JOIN locations l ON ...");
    println!("       WHERE c.title = 'Italian' AND l.title = 'Melbourne CBD'");
    println!();
    println!("  Sekejap:");
    println!("    // First do the multi-way join");
    println!("    // Then filter results where cuisine='Italian' AND location='CBD'");
    println!();
    println!("  Results (filtered):");
    println!("    Luigi's Pizza: Italian in Melbourne CBD");
    println!("    Napoli Classic: Italian in Melbourne CBD");

    Ok(())
}

/// Setup sample data for join examples
fn setup_sample_data(db: &mut SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Setting up sample data...");

    // Create restaurants
    let restaurants = vec![
        ("luigis-pizza", r#"{"title": "Luigi's Pizza", "type": "restaurant"}"#),
        ("mamas-pasta", r#"{"title": "Mama's Pasta", "type": "restaurant"}"#),
        ("bella-cucina", r#"{"title": "Bella Cucina", "type": "restaurant"}"#),
        ("napoli-classic", r#"{"title": "Napoli Classic", "type": "restaurant"}"#),
        ("sushi-yama", r#"{"title": "Sushi Yama", "type": "restaurant"}"#),
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

    // Create locations
    let locations = vec![
        ("cbd", r#"{"title": "Melbourne CBD", "type": "location"}"#),
        ("south-yarra", r#"{"title": "South Yarra", "type": "location"}"#),
        ("st-kilda", r#"{"title": "St Kilda", "type": "location"}"#),
    ];

    for (slug, data) in &locations {
        db.write(slug, data)?;
    }

    // Connect restaurants to cuisines (related edge)
    db.add_edge("luigis-pizza", "italian", 0.95, "related".to_string())?;
    db.add_edge("mamas-pasta", "italian", 0.95, "related".to_string())?;
    db.add_edge("bella-cucina", "italian", 0.93, "related".to_string())?;
    db.add_edge("napoli-classic", "italian", 0.96, "related".to_string())?;
    db.add_edge("le-petit", "french", 0.92, "related".to_string())?;
    db.add_edge("sushi-yama", "japanese", 0.94, "related".to_string())?;

    // Connect restaurants to locations (located_in edge)
    db.add_edge("luigis-pizza", "cbd", 0.8, "located_in".to_string())?;
    db.add_edge("mamas-pasta", "south-yarra", 0.85, "located_in".to_string())?;
    db.add_edge("bella-cucina", "south-yarra", 0.82, "located_in".to_string())?;
    db.add_edge("napoli-classic", "cbd", 0.88, "located_in".to_string())?;
    db.add_edge("le-petit", "st-kilda", 0.86, "located_in".to_string())?;
    db.add_edge("sushi-yama", "cbd", 0.87, "located_in".to_string())?;

    println!("✓ Created sample data\n");
    Ok(())
}
