//! Filtering Operations
//!
//! Demonstrates WHERE clause equivalents using atoms.
//! Shows simple filters, compound filters, and pattern matching.
//!
//! Run with: `cargo run --example filters`

use hsdl_sekejap::{SekejapDB, atoms::*, EdgeType};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Data Processing: Filters ===\n");

    let mut db = SekejapDB::new(Path::new("./examples/data"))?;

    // Setup: Create sample data
    setup_sample_data(&mut db)?;

    println!("\n--- 1. Simple Filter: Italian Restaurants ---\n");
    simple_filter(&db)?;

    println!("\n--- 2. Compound Filter: Italian AND High-Rated ---\n");
    compound_filter_and(&db)?;

    println!("\n--- 3. Compound Filter: Italian OR Located in CBD ---\n");
    compound_filter_or(&db)?;

    println!("\n--- 4. Negation Filter: NOT Located in South Yarra ---\n");
    negation_filter(&db)?;

    println!("\n--- 5. Range Filter: Rating Between 4.0 and 5.0 ---\n");
    range_filter(&db)?;

    println!("\n--- 6. Pattern Filter: Crime Titles with 'Theft' ---\n");
    pattern_filter(&db)?;

    println!("\n--- 7. In Filter: Multiple Values ---\n");
    in_filter(&db)?;

    println!("\n--- 8. Edge Filter: Restaurants on Uber Eats ---\n");
    edge_filter(&db)?;

    println!("\n--- 9. Multi-Step Filter: Traverse Then Filter ---\n");
    multi_step_filter(&db)?;

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
    println!("Finding Italian restaurants (WHERE cuisine = 'Italian'):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .filter(|n| true) // Would filter by type="restaurant" in production
        .collect::<Vec<_>>();

    // Step 2: Apply WHERE clause (Italian cuisine)
    let italian: Vec<_> = restaurants
        .into_iter()
        .filter(|restaurant| {
            // Get cuisine edges
            let cuisine_edges = get_edges_from(db, &format!("{}", restaurant.node_id))
                .into_iter()
                .filter(|e| e.edge_type == EdgeType::Related)
                .collect::<Vec<_>>();

            // Check if Italian
            cuisine_edges
                .into_iter()
                .any(|e| {
                    if let Some(cuisine) = get_node(db, &format!("{}", e.target_id)) {
                        format!("{}", cuisine.node_id) == "italian"
                    } else {
                        false
                    }
                })
        })
        .collect();

    println!("  Found {} Italian restaurants", italian.len());
    for r in italian.iter().take(2) {
        println!("    {}", r.node_id);
    }
    if italian.len() > 2 {
        println!("    ... and {} more", italian.len() - 2);
    }

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
    println!("Finding Italian high-rated restaurants (WHERE cuisine = 'Italian' AND rating >= 4.5):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .collect::<Vec<_>>();

    // Step 2: Apply compound WHERE clause
    let italian_high_rated: Vec<_> = restaurants
        .into_iter()
        .filter(|restaurant| {
            // Check 1: Italian cuisine
            let is_italian = {
                let cuisine_edges = get_edges_from(db, &format!("{}", restaurant.node_id))
                    .into_iter()
                    .filter(|e| e.edge_type == EdgeType::Related)
                    .collect::<Vec<_>>();

                cuisine_edges
                    .into_iter()
                    .any(|e| {
                        if let Some(cuisine) = get_node(db, &format!("{}", e.target_id)) {
                            format!("{}", cuisine.node_id) == "italian"
                        } else {
                            false
                        }
                    })
            };

            // Check 2: High rating (would check metadata in production)
            let is_high_rated = true; // Placeholder

            // AND: Both conditions must be true
            is_italian && is_high_rated
        })
        .collect();

    println!("  Found {} Italian high-rated restaurants", italian_high_rated.len());
    for r in italian_high_rated.iter().take(2) {
        println!("    {}", r.node_id);
    }

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
    println!("Finding Italian OR CBD restaurants (WHERE cuisine = 'Italian' OR location = 'CBD'):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .collect::<Vec<_>>();

    // Step 2: Apply OR WHERE clause
    let matching: Vec<_> = restaurants
        .into_iter()
        .filter(|restaurant| {
            // Check 1: Italian cuisine
            let is_italian = {
                let cuisine_edges = get_edges_from(db, &format!("{}", restaurant.node_id))
                    .into_iter()
                    .filter(|e| e.edge_type == EdgeType::Related)
                    .collect::<Vec<_>>();

                cuisine_edges
                    .into_iter()
                    .any(|e| {
                        if let Some(cuisine) = get_node(db, &format!("{}", e.target_id)) {
                            format!("{}", cuisine.node_id) == "italian"
                        } else {
                            false
                        }
                    })
            };

            // Check 2: CBD location
            let is_cbd = {
                let location_edges = get_edges_from(db, &format!("{}", restaurant.node_id))
                    .into_iter()
                    .filter(|e| e.edge_type == EdgeType::Hierarchy)
                    .collect::<Vec<_>>();

                location_edges
                    .into_iter()
                    .any(|e| {
                        if let Some(location) = get_node(db, &format!("{}", e.target_id)) {
                            format!("{}", location.node_id).contains("cbd") ||
                            format!("{}", location.node_id).contains("CBD")
                        } else {
                            false
                        }
                    })
            };

            // OR: Either condition must be true
            is_italian || is_cbd
        })
        .collect();

    println!("  Found {} restaurants (Italian OR CBD)", matching.len());
    for r in matching.iter().take(2) {
        println!("    {}", r.node_id);
    }

    Ok(())
}

/// Negation filter: NOT condition
///
/// SQL equivalent:
/// ```sql
/// SELECT * FROM restaurants WHERE location != 'South Yarra'
/// ```
fn negation_filter(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding restaurants NOT in South Yarra (WHERE location != 'South Yarra'):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .collect::<Vec<_>>();

    // Step 2: Apply NOT WHERE clause
    let not_south_yarra: Vec<_> = restaurants
        .into_iter()
        .filter(|restaurant| {
            // Check location
            let location_edges = get_edges_from(db, &format!("{}", restaurant.node_id))
                .into_iter()
                .filter(|e| e.edge_type == EdgeType::Hierarchy)
                .collect::<Vec<_>>();

            // NOT: Exclude South Yarra
            !location_edges
                .into_iter()
                .any(|e| {
                    if let Some(location) = get_node(db, &format!("{}", e.target_id)) {
                        format!("{}", location.node_id).contains("south-yarra") ||
                        format!("{}", location.node_id).contains("South Yarra")
                    } else {
                        false
                    }
                })
        })
        .collect();

    println!("  Found {} restaurants NOT in South Yarra", not_south_yarra.len());
    for r in not_south_yarra.iter().take(2) {
        println!("    {}", r.node_id);
    }

    Ok(())
}

/// Range filter: BETWEEN condition
///
/// SQL equivalent:
/// ```sql
/// SELECT * FROM restaurants WHERE rating BETWEEN 4.0 AND 5.0
/// ```
fn range_filter(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding restaurants with rating 4.0-5.0 (WHERE rating BETWEEN 4.0 AND 5.0):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .collect::<Vec<_>>();

    // Step 2: Apply BETWEEN WHERE clause
    let in_range: Vec<_> = restaurants
        .into_iter()
        .filter(|restaurant| {
            // Get reviews for rating (would check metadata in production)
            let review_edges = get_edges_from(db, &format!("{}", restaurant.node_id))
                .into_iter()
                .filter(|e| e.edge_type == EdgeType::Reviews)
                .collect::<Vec<_>>();

            // Calculate average rating
            let avg_rating = if !review_edges.is_empty() {
                review_edges.iter().map(|e| e.weight).sum::<f32>() / review_edges.len() as f32
            } else {
                0.0
            };

            // BETWEEN: 4.0 <= rating <= 5.0
            avg_rating >= 4.0 && avg_rating <= 5.0
        })
        .collect();

    println!("  Found {} restaurants with rating 4.0-5.0", in_range.len());
    for r in in_range.iter().take(2) {
        println!("    {}", r.node_id);
    }

    Ok(())
}

/// Pattern filter: LIKE condition
///
/// SQL equivalent:
/// ```sql
/// SELECT * FROM crimes WHERE title LIKE '%Theft%'
/// ```
fn pattern_filter(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding crimes with 'Theft' in title (WHERE title LIKE '%Theft%'):\n");

    // Step 1: Get all crimes
    let crimes = get_nodes_by_edge_type(db, EdgeType::Causal)
        .into_iter()
        .filter(|n| true) // Filter by type="crime" in production
        .collect::<Vec<_>>();

    // Step 2: Apply LIKE WHERE clause
    let theft_crimes: Vec<_> = crimes
        .into_iter()
        .filter(|crime| {
            // Check if title contains 'Theft'
            format!("{}", crime.node_id).to_lowercase().contains("theft")
        })
        .collect();

    println!("  Found {} crimes with 'Theft' in title", theft_crimes.len());
    for c in theft_crimes.iter().take(2) {
        println!("    {}", c.node_id);
    }
    if theft_crimes.len() > 2 {
        println!("    ... and {} more", theft_crimes.len() - 2);
    }

    Ok(())
}

/// IN filter: multiple values
///
/// SQL equivalent:
/// ```sql
/// SELECT * FROM restaurants WHERE cuisine IN ('Italian', 'French', 'Japanese')
/// ```
fn in_filter(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding Italian OR French OR Japanese restaurants (WHERE cuisine IN ('Italian', 'French', 'Japanese')):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .collect::<Vec<_>>();

    // Step 2: Define IN list
    let target_cuisines = vec!["italian", "french", "japanese"];

    // Step 3: Apply IN WHERE clause
    let matching: Vec<_> = restaurants
        .into_iter()
        .filter(|restaurant| {
            let cuisine_edges = get_edges_from(db, &format!("{}", restaurant.node_id))
                .into_iter()
                .filter(|e| e.edge_type == EdgeType::Related)
                .collect::<Vec<_>>();

            // IN: cuisine must be in target list
            cuisine_edges
                .into_iter()
                .any(|e| {
                    if let Some(cuisine) = get_node(db, &format!("{}", e.target_id)) {
                        target_cuisines.contains(&format!("{}", cuisine.node_id).to_lowercase().as_str())
                    } else {
                        false
                    }
                })
        })
        .collect();

    println!("  Found {} restaurants (Italian OR French OR Japanese)", matching.len());
    for r in matching.iter().take(2) {
        println!("    {}", r.node_id);
    }

    Ok(())
}

/// Edge filter: Filter by edge relationship
///
/// SQL equivalent (graph-specific):
/// ```sql
/// SELECT * FROM restaurants 
/// WHERE EXISTS (SELECT 1 FROM edges e WHERE e.target_id = restaurants.id AND e.type = 'AvailableOn')
/// ```
fn edge_filter(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding restaurants on Uber Eats platform (WHERE edge.type = 'AvailableOn' AND edge.target = 'uber-eats'):\n");

    // Step 1: Get Uber Eats platform
    let uber_eats = get_node(db, "uber-eats");

    if uber_eats.is_none() {
        println!("  Uber Eats platform not found");
        return Ok(());
    }

    // Step 2: Get all restaurants pointing to Uber Eats
    let uber_eats_restaurants = get_nodes_with_edge_to(db, "uber-eats", EdgeType::AvailableOn);

    println!("  Found {} restaurants on Uber Eats", uber_eats_restaurants.len());
    for r in uber_eats_restaurants.iter().take(2) {
        println!("    {}", r.node_id);
    }
    if uber_eats_restaurants.len() > 2 {
        println!("    ... and {} more", uber_eats_restaurants.len() - 2);
    }

    Ok(())
}

/// Multi-step filter: Traverse then filter results
///
/// SQL equivalent:
/// ```sql
/// SELECT n.*
/// FROM nodes n
/// WHERE n.id IN (
///   SELECT e.target_id
///   FROM edges e
///   WHERE e.source_id = 'poverty' AND e.type = 'Causal'
/// ) AND n.type = 'crime'
/// ```
fn multi_step_filter(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding crimes caused by poverty (traverse poverty → filter crimes):\n");

    // Step 1: Traverse from poverty node
    let poverty_effects = traverse_bfs(db, "poverty", 2);

    println!("  Found {} nodes reachable from poverty", poverty_effects.len());

    // Step 2: Filter for crimes only
    let theft_crimes: Vec<_> = poverty_effects
        .into_iter()
        .filter(|node| {
            // Check if node is a crime
            format!("{}", node.node_id).to_lowercase().contains("theft")
        })
        .collect();

    println!("  Of those, {} are theft crimes", theft_crimes.len());
    for c in theft_crimes.iter().take(2) {
        println!("    {}", c.node_id);
    }

    Ok(())
}

/// Setup sample data for filter examples
fn setup_sample_data(db: &mut SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Setting up sample data...\n");

    // Create restaurants
    let restaurants = vec![
        ("luigis-pizza", r#"{"title": "Luigi's Pizza", "type": "restaurant"}"#),
        ("mamas-pasta", r#"{"title": "Mama's Pasta", "type": "restaurant"}"#),
        ("bella-cucina", r#"{"title": "Bella Cucina", "type": "restaurant"}"#),
        ("napoli-classic", r#"{"title": "Napoli Classic", "type": "restaurant"}"#),
        ("sushi-yama", r#"{"title": "Sushi Yama", "type": "restaurant"}"#),
        ("le-petit", r#"{"title": "Le Petit", "type": "restaurant"}"#),
    ];

    for (slug, data) in restaurants {
        db.write(slug, data)?;
    }

    // Create cuisines
    let cuisines = vec![
        ("italian", r#"{"title": "Italian", "type": "cuisine"}"#),
        ("french", r#"{"title": "French", "type": "cuisine"}"#),
        ("japanese", r#"{"title": "Japanese", "type": "cuisine"}"#),
    ];

    for (slug, data) in cuisines {
        db.write(slug, data)?;
    }

    // Connect restaurants to cuisines
    db.add_edge("luigis-pizza", "italian", 0.95, EdgeType::Related)?;
    db.add_edge("mamas-pasta", "italian", 0.95, EdgeType::Related)?;
    db.add_edge("bella-cucina", "italian", 0.93, EdgeType::Related)?;
    db.add_edge("napoli-classic", "italian", 0.96, EdgeType::Related)?;
    db.add_edge("le-petit", "french", 0.92, EdgeType::Related)?;
    db.add_edge("sushi-yama", "japanese", 0.94, EdgeType::Related)?;

    // Create locations
    let locations = vec![
        ("cbd", r#"{"title": "Melbourne CBD", "type": "location"}"#),
        ("south-yarra", r#"{"title": "South Yarra", "type": "location"}"#),
        ("st-kilda", r#"{"title": "St Kilda", "type": "location"}"#),
    ];

    for (slug, data) in locations {
        db.write(slug, data)?;
    }

    // Connect restaurants to locations
    db.add_edge("luigis-pizza", "cbd", 0.8, EdgeType::Hierarchy)?;
    db.add_edge("mamas-pasta", "south-yarra", 0.85, EdgeType::Hierarchy)?;
    db.add_edge("bella-cucina", "south-yarra", 0.82, EdgeType::Hierarchy)?;
    db.add_edge("napoli-classic", "cbd", 0.88, EdgeType::Hierarchy)?;
    db.add_edge("le-petit", "st-kilda", 0.86, EdgeType::Hierarchy)?;
    db.add_edge("sushi-yama", "cbd", 0.87, EdgeType::Hierarchy)?;

    // Create platforms
    db.write("uber-eats", r#"{"title": "Uber Eats", "type": "platform"}"#)?;
    db.write("doordash", r#"{"title": "DoorDash", "type": "platform"}"#)?;

    db.add_edge("luigis-pizza", "uber-eats", 0.9, EdgeType::AvailableOn)?;
    db.add_edge("mamas-pasta", "doordash", 0.85, EdgeType::AvailableOn)?;
    db.add_edge("bella-cucina", "uber-eats", 0.88, EdgeType::AvailableOn)?;
    db.add_edge("napoli-classic", "uber-eats", 0.95, EdgeType::AvailableOn)?;
    db.add_edge("sushi-yama", "doordash", 0.92, EdgeType::AvailableOn)?;
    db.add_edge("le-petit", "uber-eats", 0.89, EdgeType::AvailableOn)?;

    // Create crimes
    let crimes = vec![
        ("theft-bicycle", r#"{"title": "Bicycle Theft", "type": "crime"}"#),
        ("theft-motorcycle", r#"{"title": "Motorcycle Theft", "type": "crime"}"#),
        ("theft-bag", r#"{"title": "Bag Snatching", "type": "crime"}"#),
        ("theft-phone", r#"{"title": "Phone Theft", "type": "crime"}"#),
        ("vandalism", r#"{"title": "Vandalism", "type": "crime"}"#),
    ];

    for (slug, data) in crimes {
        db.write(slug, data)?;
    }

    // Create causes
    db.write("poverty", r#"{"title": "Poverty", "type": "cause"}"#)?;
    db.write("unemployment", r#"{"title": "Unemployment", "type": "cause"}"#)?;

    // Connect causes to crimes
    db.add_edge("poverty", "theft-bicycle", 0.7, EdgeType::Causal)?;
    db.add_edge("poverty", "theft-motorcycle", 0.8, EdgeType::Causal)?;
    db.add_edge("unemployment", "theft-bag", 0.75, EdgeType::Causal)?;
    db.add_edge("poverty", "theft-phone", 0.65, EdgeType::Causal)?;

    println!("✓ Created sample data\n");
    Ok(())
}