//! Aggregation Functions
//!
//! Demonstrates GROUP BY, COUNT, SUM, AVG operations using atoms.
//! Shows common SQL aggregation patterns composed from primitives.
//!
//! Run with: `cargo run --example aggregations`

use sekejap::{SekejapDB, atoms::*, EdgeType};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Data Processing: Aggregations ===\n");

    let mut db = SekejapDB::new(Path::new("./examples/data"))?;

    // Setup: Create sample data
    setup_sample_data(&mut db)?;

    println!("\n--- 1. COUNT: Restaurants per Platform ---\n");
    count_by_platform(&db)?;

    println!("\n--- 2. SUM: Total Reviews Across Restaurants ---\n");
    sum_reviews(&db)?;

    println!("\n--- 3. AVG: Average Rating per Restaurant ---\n");
    avg_rating(&db)?;

    println!("\n--- 4. GROUP BY: Crimes by Type ---\n");
    group_by_crime_type(&db)?;

    println!("\n--- 5. GROUP BY: Crimes by Location ---\n");
    group_by_location(&db)?;

    println!("\n--- 6. GROUP BY + HAVING: Crime Hotspots ---\n");
    group_by_having(&db)?;

    println!("\n--- 7. Multi-Column GROUP BY: Restaurants by Cuisine + Rating ---\n");
    multi_column_group_by(&db)?;

    println!("\n--- 8. Aggregation with JOIN: Restaurant Statistics ---\n");
    aggregation_with_join(&db)?;

    println!("\n=== All Aggregations Completed Successfully ===");
    Ok(())
}

/// COUNT: Count items per category
///
/// SQL equivalent:
/// ```sql
/// SELECT platform, COUNT(*)
/// FROM restaurants
/// GROUP BY platform
/// ```
fn count_by_platform(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Counting restaurants per platform (GROUP BY + COUNT):\n");

    // Step 1: Get all platforms
    let platforms = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .filter(|n| true) // Filter by type="platform" in production
        .collect::<Vec<_>>();

    // Step 2: COUNT restaurants for each platform
    for platform in &platforms {
        let restaurants = get_nodes_with_edge_to(db, &format!("{}", platform.node_id), EdgeType::AvailableOn);
        println!("  {}: {} restaurants", platform.node_id, restaurants.len());
    }

    Ok(())
}

/// SUM: Sum values across related items
///
/// SQL equivalent:
/// ```sql
/// SELECT restaurant_id, SUM(rating)
/// FROM reviews
/// GROUP BY restaurant_id
/// ```
fn sum_reviews(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Calculating total reviews per restaurant (GROUP BY + SUM):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .take(3)
        .collect::<Vec<_>>();

    // Step 2: SUM reviews for each
    for restaurant in &restaurants {
        let reviews = get_edges_from(db, &format!("{}", restaurant.node_id))
            .into_iter()
            .filter(|e| e.edge_type == EdgeType::Reviews)
            .collect::<Vec<_>>();

        let total_reviews = reviews.len();
        let total_weight = reviews.iter().map(|e| e.weight).sum::<f32>();

        println!("  {}: {} reviews (total weight: {:.2})", 
            restaurant.node_id, 
            total_reviews,
            total_weight
        );
    }

    Ok(())
}

/// AVG: Average values per group
///
/// SQL equivalent:
/// ```sql
/// SELECT restaurant_id, AVG(rating)
/// FROM reviews
/// GROUP BY restaurant_id
/// ```
fn avg_rating(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Calculating average rating per restaurant (GROUP BY + AVG):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .take(3)
        .collect::<Vec<_>>();

    // Step 2: Calculate AVG for each
    for restaurant in &restaurants {
        let reviews = get_edges_from(db, &format!("{}", restaurant.node_id))
            .into_iter()
            .filter(|e| e.edge_type == EdgeType::Reviews)
            .collect::<Vec<_>>();

        let count = reviews.len();
        let avg_rating = if count > 0 {
            reviews.iter().map(|e| e.weight).sum::<f32>() / count as f32
        } else {
            0.0
        };

        println!("  {}: {} reviews, avg rating: {:.2}", 
            restaurant.node_id, 
            count,
            avg_rating
        );
    }

    Ok(())
}

/// GROUP BY: Crimes by type
///
/// SQL equivalent:
/// ```sql
/// SELECT crime_type, COUNT(*), AVG(severity)
/// FROM crimes
/// GROUP BY crime_type
/// ```
fn group_by_crime_type(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Grouping crimes by type (GROUP BY + COUNT + AVG):\n");

    // Step 1: Get all crimes
    let crimes = get_nodes_by_edge_type(db, EdgeType::Causal)
        .into_iter()
        .filter(|n| true) // Filter by type="crime" in production
        .collect::<Vec<_>>();

    // Step 2: Group by type (would check metadata in production)
    let mut crime_types = std::collections::HashMap::new();

    for crime in &crimes {
        let crime_type = "theft"; // Would extract from metadata
        *crime_types.entry(crime_type.to_string()).or_insert(0) += 1;
    }

    // Step 3: Display results
    for (crime_type, count) in crime_types {
        println!("  {}: {} crimes", crime_type, count);
    }

    println!("\nTotal unique crime types: {}", crime_types.len());
    Ok(())
}

/// GROUP BY: Crimes by location
///
/// SQL equivalent:
/// ```sql
/// SELECT location, COUNT(*), MAX(severity)
/// FROM crimes
/// GROUP BY location
/// ```
fn group_by_location(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Grouping crimes by location (GROUP BY + COUNT + MAX):\n");

    // Step 1: Get all crimes
    let crimes = get_nodes_by_edge_type(db, EdgeType::Causal)
        .into_iter()
        .take(5)
        .collect::<Vec<_>>();

    // Step 2: Group by location (would check metadata in production)
    let mut location_counts = std::collections::HashMap::new();

    for crime in &crimes {
        let location = "jakarta"; // Would extract from metadata
        *location_counts.entry(location.to_string()).or_insert(0) += 1;
    }

    // Step 3: Display results
    for (location, count) in location_counts {
        println!("  {}: {} crimes", location, count);
    }

    Ok(())
}

/// GROUP BY + HAVING: Filter aggregated results
///
/// SQL equivalent:
/// ```sql
/// SELECT location, COUNT(*)
/// FROM crimes
/// GROUP BY location
/// HAVING COUNT(*) >= 3
/// ```
fn group_by_having(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding crime hotspots (GROUP BY + COUNT + HAVING >= 3):\n");

    // Step 1: Get all crimes
    let crimes = get_nodes_by_edge_type(db, EdgeType::Causal)
        .into_iter()
        .take(10)
        .collect::<Vec<_>>();

    // Step 2: Group by location
    let mut location_counts = std::collections::HashMap::new();

    for crime in &crimes {
        let location = "jakarta"; // Would extract from metadata
        *location_counts.entry(location.to_string()).or_insert(0) += 1;
    }

    // Step 3: Filter with HAVING clause
    let hotspots: Vec<_> = location_counts
        .into_iter()
        .filter(|(_, count)| *count >= 3)
        .collect();

    // Step 4: Display results
    if hotspots.is_empty() {
        println!("  No hotspots found (need 3+ crimes per location)");
    } else {
        println!("  Crime hotspots (>= 3 crimes):");
        for (location, count) in hotspots {
            println!("    {}: {} crimes", location, count);
        }
    }

    Ok(())
}

/// Multi-Column GROUP BY: Multiple grouping keys
///
/// SQL equivalent:
/// ```sql
/// SELECT cuisine, rating_tier, COUNT(*)
/// FROM restaurants
/// GROUP BY cuisine, rating_tier
/// ```
fn multi_column_group_by(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Grouping restaurants by cuisine + rating tier (MULTI-COLUMN GROUP BY):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .collect::<Vec<_>>();

    // Step 2: Multi-column grouping
    let mut groups = std::collections::HashMap::new();

    for restaurant in &restaurants {
        // Get cuisine
        let cuisine = get_edges_from(db, &format!("{}", restaurant.node_id))
            .into_iter()
            .filter(|e| e.edge_type == EdgeType::Related)
            .next()
            .and_then(|e| get_node(db, &format!("{}", e.target_id)));

        let cuisine_name = if let Some(c) = cuisine {
            format!("{}", c.node_id)
        } else {
            "unknown".to_string()
        };

        // Determine rating tier
        let rating_tier = "high"; // Would extract from metadata

        // Multi-column grouping key
        let key = (cuisine_name, rating_tier.to_string());
        *groups.entry(key).or_insert(0) += 1;
    }

    // Step 3: Display results
    println!("  Restaurant counts by (cuisine, rating_tier):");
    let mut sorted_groups: Vec<_> = groups.into_iter().collect();
    sorted_groups.sort_by(|a, b| b.1.cmp(&a.1));

    for ((cuisine, tier), count) in sorted_groups {
        println!("    {} ({}): {} restaurants", cuisine, tier, count);
    }

    Ok(())
}

/// Aggregation with JOIN: Statistics across related tables
///
/// SQL equivalent:
/// ```sql
/// SELECT r.id, COUNT(rev.id), AVG(rev.rating), MAX(rev.rating)
/// FROM restaurants r
/// LEFT JOIN reviews rev ON r.id = rev.restaurant_id
/// GROUP BY r.id
/// ```
fn aggregation_with_join(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Restaurant statistics with JOIN + aggregation (COUNT + AVG + MAX):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .take(3)
        .collect::<Vec<_>>();

    // Step 2: JOIN and aggregate
    for restaurant in &restaurants {
        let reviews = get_edges_from(db, &format!("{}", restaurant.node_id))
            .into_iter()
            .filter(|e| e.edge_type == EdgeType::Reviews)
            .collect::<Vec<_>>();

        let count = reviews.len();
        let avg = if count > 0 {
            reviews.iter().map(|e| e.weight).sum::<f32>() / count as f32
        } else {
            0.0
        };
        let max = reviews.iter().map(|e| e.weight).fold(0.0f32, f32::max);

        println!("  {}:", restaurant.node_id);
        println!("    Count: {}", count);
        println!("    Avg:   {:.2}", avg);
        println!("    Max:   {:.2}", max);
    }

    Ok(())
}

/// Setup sample data for aggregation examples
fn setup_sample_data(db: &mut SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Setting up sample data...\n");

    // Create restaurants
    let restaurants = vec![
        ("luigis-pizza", r#"{"title": "Luigi's Pizza", "type": "restaurant"}"#),
        ("mamas-pasta", r#"{"title": "Mama's Pasta", "type": "restaurant"}"#),
        ("bella-cucina", r#"{"title": "Bella Cucina", "type": "restaurant"}"#),
        ("napoli-classic", r#"{"title": "Napoli Classic", "type": "restaurant"}"#),
        ("gelateria-roma", r#"{"title": "Gelateria Roma", "type": "restaurant"}"#),
    ];

    for (slug, data) in restaurants {
        db.write(slug, data)?;
    }

    // Create reviews
    let reviews = vec![
        ("review-1", r#"{"title": "Great!", "rating": 5}"#),
        ("review-2", r#"{"title": "Good", "rating": 4}"#),
        ("review-3", r#"{"title": "Amazing", "rating": 5}"#),
        ("review-4", r#"{"title": "Okay", "rating": 3}"#),
        ("review-5", r#"{"title": "Excellent", "rating": 4}"#),
        ("review-6", r#"{"title": "Perfect", "rating": 5}"#),
        ("review-7", r#"{"title": "Decent", "rating": 3}"#),
        ("review-8", r#"{"title": "Wonderful", "rating": 4}"#),
    ];

    for (slug, data) in reviews {
        db.write(slug, data)?;
    }

    // Connect reviews to restaurants
    db.add_edge("review-1", "luigis-pizza", 0.9, EdgeType::Reviews)?;
    db.add_edge("review-2", "mamas-pasta", 0.85, EdgeType::Reviews)?;
    db.add_edge("review-3", "luigis-pizza", 0.95, EdgeType::Reviews)?;
    db.add_edge("review-4", "bella-cucina", 0.88, EdgeType::Reviews)?;
    db.add_edge("review-5", "napoli-classic", 0.92, EdgeType::Reviews)?;
    db.add_edge("review-6", "luigis-pizza", 0.97, EdgeType::Reviews)?;
    db.add_edge("review-7", "gelateria-roma", 0.86, EdgeType::Reviews)?;
    db.add_edge("review-8", "mamas-pasta", 0.89, EdgeType::Reviews)?;

    // Create platforms
    db.write("uber-eats", r#"{"title": "Uber Eats", "type": "platform"}"#)?;
    db.write("doordash", r#"{"title": "DoorDash", "type": "platform"}"#)?;

    db.add_edge("luigis-pizza", "uber-eats", 0.9, EdgeType::AvailableOn)?;
    db.add_edge("mamas-pasta", "doordash", 0.85, EdgeType::AvailableOn)?;
    db.add_edge("bella-cucina", "uber-eats", 0.88, EdgeType::AvailableOn)?;
    db.add_edge("napoli-classic", "uber-eats", 0.95, EdgeType::AvailableOn)?;
    db.add_edge("gelateria-roma", "doordash", 0.88, EdgeType::AvailableOn)?;

    // Create crimes
    let crimes = vec![
        ("theft-1", r#"{"title": "Bike Theft", "type": "crime", "crime_type": "theft"}"#),
        ("theft-2", r#"{"title": "Motorcycle Theft", "type": "crime", "crime_type": "theft"}"#),
        ("theft-3", r#"{"title": "Bag Snatching", "type": "crime", "crime_type": "theft"}"#),
        ("theft-4", r#"{"title": "Shoplifting", "type": "crime", "crime_type": "theft"}"#),
        ("theft-5", r#"{"title": "Phone Theft", "type": "crime", "crime_type": "theft"}"#),
    ];

    for (slug, data) in crimes {
        db.write(slug, data)?;
    }

    println!("✓ Created sample data\n");
    Ok(())
}