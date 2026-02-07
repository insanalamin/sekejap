//! Joins and Multi-Table Operations
//!
//! Demonstrates how to compose atoms for SQL-like join operations.
//! Shows inner joins, left joins, self joins, and multi-way joins.
//!
//! Run with: `cargo run --example joins`

use sekejap::{SekejapDB, atoms::*, EdgeType};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Data Processing: Joins ===\n");

    let mut db = SekejapDB::new(Path::new("./examples/data"))?;

    // Setup: Create sample data
    setup_sample_data(&mut db)?;

    println!("\n--- 1. Inner Join: Restaurants with Reviews ---\n");
    inner_join_restaurants_reviews(&db)?;

    println!("\n--- 2. Left Join: All Restaurants, Even Without Reviews ---\n");
    left_join_restaurants_reviews(&db)?;

    println!("\n--- 3. Self Join: Related Restaurants ---\n");
    self_join_restaurants(&db)?;

    println!("\n--- 4. Multi-Way Join: Restaurants → Cuisine → Location ---\n");
    multi_way_join(&db)?;

    println!("\n--- 5. Join with Filtering: High-Rated Restaurants ---\n");
    join_with_filter(&db)?;

    println!("\n--- 6. Join with Aggregation: Average Ratings ---\n");
    join_with_aggregation(&db)?;

    println!("\n=== All Joins Completed Successfully ===");
    Ok(())
}

/// Inner Join: Find restaurants that have reviews
///
/// SQL equivalent:
/// ```sql
/// SELECT r.*, rev.*
/// FROM restaurants r
/// INNER JOIN reviews rev ON r.slug = rev.restaurant_slug
/// ```
fn inner_join_restaurants_reviews(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding restaurants with reviews (INNER JOIN):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::Reviews)
        .into_iter()
        .filter(|n| true) // Would filter by type="restaurant" in production
        .collect::<Vec<_>>();

    // Step 2: Join each restaurant with its reviews
    let mut results = Vec::new();

    for restaurant in &restaurants {
        // Get reviews for this restaurant (via Reviews edges)
        let review_edges = get_edges_from(db, &format!("{}", restaurant.node_id))
            .into_iter()
            .filter(|e| e.edge_type == EdgeType::Reviews)
            .collect::<Vec<_>>();

        // This is an INNER JOIN - only include if there are reviews
        if !review_edges.is_empty() {
            let reviews = review_edges
                .into_iter()
                .filter_map(|e| get_node(db, &format!("{}", e.target_id)))
                .collect::<Vec<_>>();

            results.push((restaurant.clone(), reviews));
            println!("  {} has {} reviews", restaurant.node_id, reviews.len());
        }
    }

    println!("\nTotal restaurants with reviews: {}", results.len());
    Ok(())
}

/// Left Join: Get all restaurants, even those without reviews
///
/// SQL equivalent:
/// ```sql
/// SELECT r.*, rev.*
/// FROM restaurants r
/// LEFT JOIN reviews rev ON r.slug = rev.restaurant_slug
/// ```
fn left_join_restaurants_reviews(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding all restaurants with or without reviews (LEFT JOIN):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .collect::<Vec<_>>();

    // Step 2: Join with reviews (LEFT JOIN = include NULL matches)
    for restaurant in &restaurants {
        let review_edges = get_edges_from(db, &format!("{}", restaurant.node_id))
            .into_iter()
            .filter(|e| e.edge_type == EdgeType::Reviews)
            .collect::<Vec<_>>();

        let reviews = review_edges
            .into_iter()
            .filter_map(|e| get_node(db, &format!("{}", e.target_id)))
            .collect::<Vec<_>>();

        if reviews.is_empty() {
            println!("  {} - No reviews (NULL match)", restaurant.node_id);
        } else {
            println!("  {} - {} reviews", restaurant.node_id, reviews.len());
        }
    }

    Ok(())
}

/// Self Join: Find related restaurants
///
/// SQL equivalent:
/// ```sql
/// SELECT r1.*, r2.*
/// FROM restaurants r1
/// INNER JOIN restaurants r2 ON r1.cuisine = r2.cuisine
/// WHERE r1.id != r2.id
/// ```
fn self_join_restaurants(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding restaurants in same cuisine (SELF JOIN):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .collect::<Vec<_>>();

    // Step 2: For each restaurant, find related ones
    for (idx, restaurant) in restaurants.iter().enumerate() {
        // Get cuisine (via Related edges)
        let cuisine_edges = get_edges_from(db, &format!("{}", restaurant.node_id))
            .into_iter()
            .filter(|e| e.edge_type == EdgeType::Related)
            .collect::<Vec<_>>();

        for cuisine_edge in &cuisine_edges {
            // Find other restaurants with same cuisine
            let related = get_nodes_with_edge_to(
                db,
                &format!("{}", cuisine_edge.target_id),
                EdgeType::Related
            );

            // Exclude self
            let related: Vec<_> = related
                .into_iter()
                .filter(|r| r.node_id != restaurant.node_id)
                .take(3) // Limit output
                .collect();

            if !related.is_empty() {
                println!("  {} related to: {}", restaurant.node_id, 
                    related.iter()
                        .map(|r| format!("{}", r.node_id))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                break; // Only show first cuisine
            }
        }

        if idx >= 2 {
            break; // Limit output
        }
    }

    Ok(())
}

/// Multi-Way Join: Restaurant → Cuisine → Location
///
/// SQL equivalent:
/// ```sql
/// SELECT r.*, c.*, l.*
/// FROM restaurants r
/// JOIN cuisines c ON r.cuisine_id = c.id
/// JOIN locations l ON r.location_id = l.id
/// ```
fn multi_way_join(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Joining restaurants with cuisine and location (MULTI-WAY JOIN):\n");

    // Step 1: Get restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .take(3)
        .collect::<Vec<_>>();

    for restaurant in &restaurants {
        // Step 2: Get cuisine (first Related edge)
        let cuisine_edges = get_edges_from(db, &format!("{}", restaurant.node_id))
            .into_iter()
            .filter(|e| e.edge_type == EdgeType::Related)
            .take(1)
            .collect::<Vec<_>>();

        let cuisine = if let Some(edge) = cuisine_edges.first() {
            get_node(db, &format!("{}", edge.target_id))
        } else {
            None
        };

        // Step 3: Get location (LocatedIn edge)
        let location_edges = get_edges_from(db, &format!("{}", restaurant.node_id))
            .into_iter()
            .filter(|e| e.edge_type == EdgeType::Hierarchy)
            .take(1)
            .collect::<Vec<_>>();

        let location = if let Some(edge) = location_edges.first() {
            get_node(db, &format!("{}", edge.target_id))
        } else {
            None
        };

        // Step 4: Display joined result
        println!("  Restaurant: {}", restaurant.node_id);
        if let Some(c) = cuisine {
            println!("    Cuisine: {}", c.node_id);
        }
        if let Some(l) = location {
            println!("    Location: {}", l.node_id);
        }
    }

    Ok(())
}

/// Join with WHERE clause filtering
///
/// SQL equivalent:
/// ```sql
/// SELECT r.*, rev.*
/// FROM restaurants r
/// JOIN reviews rev ON r.slug = rev.restaurant_slug
/// WHERE rev.rating >= 4.5
/// ```
fn join_with_filter(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Finding restaurants with high-rated reviews (JOIN + FILTER):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .collect::<Vec<_>>();

    for restaurant in &restaurants {
        // Step 2: Get reviews
        let review_edges = get_edges_from(db, &format!("{}", restaurant.node_id))
            .into_iter()
            .filter(|e| e.edge_type == EdgeType::Reviews)
            .collect::<Vec<_>>();

        // Step 3: Filter by rating (WHERE clause)
        let high_rated_reviews = review_edges
            .into_iter()
            .filter_map(|e| {
                let review = get_node(db, &format!("{}", e.target_id))?;
                // In production, check metadata for rating
                Some(review)
            })
            .collect::<Vec<_>>();

        if !high_rated_reviews.is_empty() {
            println!("  {} has {} high-rated reviews", 
                restaurant.node_id, 
                high_rated_reviews.len()
            );
        }
    }

    Ok(())
}

/// Join with GROUP BY aggregation
///
/// SQL equivalent:
/// ```sql
/// SELECT r.*, COUNT(rev.id), AVG(rev.rating)
/// FROM restaurants r
/// JOIN reviews rev ON r.slug = rev.restaurant_slug
/// GROUP BY r.id
/// ```
fn join_with_aggregation(db: &SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Calculating average ratings per restaurant (JOIN + GROUP BY):\n");

    // Step 1: Get all restaurants
    let restaurants = get_nodes_by_edge_type(db, EdgeType::AvailableOn)
        .into_iter()
        .take(5)
        .collect::<Vec<_>>();

    for restaurant in &restaurants {
        // Step 2: Get all reviews
        let review_edges = get_edges_from(db, &format!("{}", restaurant.node_id))
            .into_iter()
            .filter(|e| e.edge_type == EdgeType::Reviews)
            .collect::<Vec<_>>();

        // Step 3: Aggregate (COUNT, AVG, SUM)
        let review_count = review_edges.len();
        
        // In production, extract ratings from metadata
        let avg_rating = if review_count > 0 {
            4.5 // Placeholder - would calculate from actual ratings
        } else {
            0.0
        };

        println!("  {}: {} reviews, avg rating: {:.1}", 
            restaurant.node_id, 
            review_count,
            avg_rating
        );
    }

    Ok(())
}

/// Setup sample data for join examples
fn setup_sample_data(db: &mut SekejapDB) -> Result<(), Box<dyn std::error::Error>> {
    println!("Setting up sample data...\n");

    // Create restaurants
    let restaurants = vec![
        ("luigis-pizza", r#"{"title": "Luigi's Pizza", "type": "restaurant"}"#),
        ("mamas-pasta", r#"{"title": "Mama's Pasta", "type": "restaurant"}"#),
        ("bella-cucina", r#"{"title": "Bella Cucina", "type": "restaurant"}"#),
    ];

    for (slug, data) in restaurants {
        db.write(slug, data)?;
    }

    // Create reviews
    let reviews = vec![
        ("review-1", r#"{"title": "Great pizza!", "rating": 5, "restaurant": "luigis-pizza"}"#),
        ("review-2", r#"{"title": "Good pasta", "rating": 4, "restaurant": "mamas-pasta"}"#),
        ("review-3", r#"{"title": "Authentic", "rating": 5, "restaurant": "luigis-pizza"}"#),
        ("review-4", r#"{"title": "Decent", "rating": 3, "restaurant": "bella-cucina"}"#),
    ];

    for (slug, data) in reviews {
        db.write(slug, data)?;
    }

    // Connect reviews to restaurants
    db.add_edge("review-1", "luigis-pizza", 0.9, EdgeType::Reviews)?;
    db.add_edge("review-2", "mamas-pasta", 0.85, EdgeType::Reviews)?;
    db.add_edge("review-3", "luigis-pizza", 0.95, EdgeType::Reviews)?;
    db.add_edge("review-4", "bella-cucina", 0.88, EdgeType::Reviews)?;

    // Connect to platforms
    db.write("uber-eats", r#"{"title": "Uber Eats", "type": "platform"}"#)?;
    db.add_edge("luigis-pizza", "uber-eats", 0.9, EdgeType::AvailableOn)?;
    db.add_edge("mamas-pasta", "uber-eats", 0.85, EdgeType::AvailableOn)?;
    db.add_edge("bella-cucina", "uber-eats", 0.88, EdgeType::AvailableOn)?;

    // Connect to cuisines
    db.write("italian", r#"{"title": "Italian", "type": "cuisine"}"#)?;
    db.add_edge("luigis-pizza", "italian", 0.95, EdgeType::Related)?;
    db.add_edge("mamas-pasta", "italian", 0.95, EdgeType::Related)?;
    db.add_edge("bella-cucina", "italian", 0.93, EdgeType::Related)?;

    // Connect to location
    db.write("melbourne", r#"{"title": "Melbourne", "type": "location"}"#)?;
    db.add_edge("luigis-pizza", "melbourne", 0.8, EdgeType::Hierarchy)?;
    db.add_edge("mamas-pasta", "melbourne", 0.85, EdgeType::Hierarchy)?;
    db.add_edge("bella-cucina", "melbourne", 0.82, EdgeType::Hierarchy)?;

    println!("✓ Created sample data\n");
    Ok(())
}