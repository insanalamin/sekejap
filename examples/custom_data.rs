//! Custom Data Examples
//!
//! This example demonstrates how to insert custom data into Sekejap-DB
//! using the unified Payload trait for both nodes and edges.

use sekejap::{SekejapDB, Payload, EdgePayload, NodePayload, WriteOptions};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize database
    let mut db = SekejapDB::new(Path::new("./example_data"))?;

    println!("=== Custom Data Examples ===\n");

    // ============================================
    // Example 1: Node with Custom Metadata
    // ============================================
    println!("1. Creating node with custom metadata...");
    
    let crime_data = r#"{
        "title": "Theft Incident",
        "coordinates": {"lat": -6.2088, "lon": 106.8456},
        "severity": "high",
        "reported_by": "witness_001",
        "evidence_count": 3,
        "tags": ["theft", "urgent", "Jakarta"]
    }"#;
    
    // Use publish_now to write directly to Tier 2 (for traversal)
    db.write_with_options("crime-001", crime_data, WriteOptions { publish_now: true, ..Default::default() })?;
    println!("✓ Created node: crime-001 (published to Tier 2)\n");

    // ============================================
    // Example 2: Multiple Nodes with Domain-Specific Data
    // ============================================
    println!("2. Creating domain-specific nodes (food analysis)...");
    
    let food_items: Vec<(String, String)> = vec![
        ("nasi-goreng".to_string(), r#"{
            "title": "Nasi Goreng",
            "cuisine": "Indonesian",
            "spice_level": "medium",
            "calories": 450,
            "protein_grams": 15,
            "tags": ["street-food", "popular", "rice"]
        }"#.to_string()),
        ("rendang".to_string(), r#"{
            "title": "Rendang",
            "cuisine": "Indonesian",
            "spice_level": "high",
            "calories": 600,
            "protein_grams": 25,
            "tags": ["specialty", "beef", "spicy"]
        }"#.to_string()),
        ("sate-ayam".to_string(), r#"{
            "title": "Sate Ayam",
            "cuisine": "Indonesian",
            "spice_level": "mild",
            "calories": 380,
            "protein_grams": 20,
            "tags": ["grilled", "chicken", "peanut-sauce"]
        }"#.to_string()),
    ];
    
    db.write_many(food_items)?;
    println!("✓ Created 3 food nodes\n");

    // ============================================
    // Example 3: Edge with Custom Type
    // ============================================
    println!("3. Creating edge with custom type...");
    
    // First create source nodes
    db.write("poverty", r#"{
        "title": "Poverty Level",
        "severity": "high",
        "metrics": {"income_level": "low", "employment_rate": 0.85}
    }"#)?;
    
    // User-defined edge type "causal" with weight
    db.add_edge("poverty", "crime-001", 0.8, "causal".to_string())?;
    println!("✓ Created causal edge: poverty -> crime-001 (weight: 0.8)\n");

    // ============================================
    // Example 4: Edge with Custom Metadata (Payload)
    // ============================================
    println!("4. Creating edge with custom metadata...");
    
    // Note: To use EdgePayload with custom metadata, you'd need to enhance
    // add_edge() API. For now, edges use string-based edge_type.
    
    // User-defined edge types for food analysis
    db.add_edge("nasi-goreng", "rendang", 0.6, "similar_cuisine".to_string())?;
    db.add_edge("rendang", "sate-ayam", 0.5, "both_indonesian".to_string())?;
    println!("✓ Created food relationship edges\n");

    // ============================================
    // Example 5: Domain-Specific Edge Types
    // ============================================
    println!("5. Creating domain-specific edge types...");
    
    // Economic analysis domain
    db.write("economic-slump", r#"{"title": "Economic Slump", "severity": "critical"}"#)?;
    db.add_edge("economic-slump", "crime-001", 0.9, "causal".to_string())?;
    
    // Social impact domain
    db.write("unemployment", r#"{"title": "Unemployment Rate", "percentage": 8.5}"#)?;
    db.add_edge("unemployment", "poverty", 0.85, "influences".to_string())?;
    
    // Hierarchy domain
    db.write("regulation", r#"{"title": "New Regulations", "type": "legal"}"#)?;
    db.add_edge("regulation", "economic-slump", 0.7, "affects".to_string())?;
    
    println!("✓ Created domain-specific edges:");
    println!("  - causal: economic-slump -> crime-001");
    println!("  - influences: unemployment -> poverty");
    println!("  - affects: regulation -> economic-slump\n");

    // ============================================
    // Example 6: Using Payload Trait Polymorphically
    // ============================================
    println!("6. Using Payload trait polymorphically...");
    
    // Read node payload
    if let Some(node_json) = db.read("crime-001")? {
        let node_payload: NodePayload = serde_json::from_str(&node_json)?;
        
        // Use Payload trait methods
        print_entity_info(&node_payload);
        
        // Access node-specific fields (use Debug format for Option types)
        println!("  Content: {:?}", node_payload.content);
        println!("  Metadata: {:?}", node_payload.metadata);
    }
    
    // Create edge payload using existing builder methods
    let edge_payload = EdgePayload::new("causal")
        .with_title("Causal Relationship")
        .with_prop("confidence", serde_json::json!(0.95))
        .with_prop("method", serde_json::json!("statistical_analysis"))
        .with_prop("sample_size", serde_json::json!(1000))
        .with_prop("p_value", serde_json::json!(0.02));
    
    print_entity_info(&edge_payload);
    
    println!();

    // ============================================
    // Example 7: Multi-Domain Knowledge Graph
    // ============================================
    println!("7. Building multi-domain knowledge graph...");
    
    // Domain: Food Science
    db.write("protein", r#"{"title": "Protein", "type": "nutrient"}"#)?;
    db.add_edge("protein", "rendang", 0.8, "rich_in".to_string())?;
    
    // Domain: Health
    db.write("diabetes-risk", r#"{"title": "Diabetes Risk", "level": "moderate"}"#)?;
    db.add_edge("nasi-goreng", "diabetes-risk", 0.6, "increases_risk".to_string())?;
    
    // Domain: Economy
    db.write("tourism-revenue", r#"{"title": "Tourism Revenue", "currency": "IDR"}"#)?;
    db.add_edge("nasi-goreng", "tourism-revenue", 0.7, "contributes_to".to_string())?;
    
    println!("✓ Created multi-domain edges:");
    println!("  - rich_in: protein -> rendang");
    println!("  - increases_risk: nasi-goreng -> diabetes-risk");
    println!("  - contributes_to: nasi-goreng -> tourism-revenue\n");

    // ============================================
    // Example 8: Traversing Custom Edges
    // ============================================
    println!("8. Traversing custom edge types...");
    
    let traversal = db.traverse("crime-001", 3, 0.5)?;
    println!("✓ Traversal found {} related events", traversal.path.len());
    println!("  Total weight: {:.2}", traversal.total_weight);
    println!("  Edges traversed: {}", traversal.edges.len());
    
    println!("\n=== Example Complete ===");
    println!("Check ./example_data/ for database files");

    Ok(())
}

/// Generic function that works with any Payload implementation
fn print_entity_info<P: Payload>(entity: &P) {
    println!("Entity Info:");
    println!("  Type: {}", entity.get_type());
    println!("  Title: {}", entity.get_title());
    println!("  Timestamp: {}", entity.get_timestamp());
    
    // Access metadata
    if entity.has_metadata_key("confidence") {
        if let Some(confidence) = entity.get_metadata("confidence") {
            println!("  Confidence: {}", confidence);
        }
    }
}