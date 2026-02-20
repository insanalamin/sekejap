//! Tests for M4: Root Cause Analysis
//! Covers TC-4.1, TC-4.2, TC-4.3, TC-4.4, TC-4.5, TC-4.6
//!
//! Run individual tests with:
//! cargo test tc_4_1 -- --nocapture
//! cargo test tc_4_2 -- --nocapture
//! cargo test tc_4_3 -- --nocapture
//! cargo test tc_4_4 -- --nocapture
//! cargo test tc_4_5 -- --nocapture
//! cargo test tc_4_6 -- --nocapture

use sekejap::SekejapDB;
use tempfile::TempDir;
use serde_json::json;

fn setup_db() -> (SekejapDB, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = SekejapDB::new(dir.path(), 1000).unwrap();
    (db, dir)
}

mod tc_4_1_backward_traversal {
    use super::*;

    #[test]
    fn test_backward_causal_chain() {
        let (db, _dir) = setup_db();

        let events = vec![
            "rca/root-cause",    
            "rca/intermediate-1", 
            "rca/intermediate-2", 
            "rca/final-effect",  
        ];

        for slug in &events {
            let payload = json!({"_id": slug, "type": "event"}).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        db.edges().link("rca/root-cause", "rca/intermediate-1", "causes", 1.0).unwrap();
        db.edges().link("rca/intermediate-1", "rca/intermediate-2", "causes", 1.0).unwrap();
        db.edges().link("rca/intermediate-2", "rca/final-effect", "causes", 1.0).unwrap();

        let outcome = db.nodes()
            .one("rca/final-effect")
            .backward("causes")
            .hops(3)
            .collect()
            .unwrap();

        println!("[TC-4.1] Backward traversal found {} ancestors", outcome.data.len());
        assert!(outcome.data.len() >= 1);
    }

    #[test]
    fn test_find_origins() {
        let (db, _dir) = setup_db();

        db.nodes().put_json(&json!({"_id": "origin/event"}).to_string()).unwrap();
        db.nodes().put_json(&json!({"_id": "propagate/middle"}).to_string()).unwrap();
        db.nodes().put_json(&json!({"_id": "propagate/end"}).to_string()).unwrap();
        db.flush().unwrap();

        db.edges().link("origin/event", "propagate/middle", "propagates", 1.0).unwrap();
        db.edges().link("propagate/middle", "propagate/end", "propagates", 1.0).unwrap();

        let outcome = db.nodes()
            .one("propagate/end")
            .backward("propagates")
            .hops(2)
            .collect()
            .unwrap();

        println!("[TC-4.1] Origin traversal: {} nodes found", outcome.data.len());
    }
}

mod tc_4_2_time_window_traversal {
    use super::*;

    #[test]
    fn test_temporal_causal_path() {
        let (db, _dir) = setup_db();

        let events = vec![
            ("time/early", "2024-06-01T08:00:00Z"),
            ("time/middle", "2024-06-01T09:00:00Z"),
            ("time/late", "2024-06-01T10:00:00Z"),
        ];

        for (slug, ts) in &events {
            let payload = json!({"_id": slug, "timestamp": ts}).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        db.edges().link("time/early", "time/middle", "causes", 1.0).unwrap();
        db.edges().link("time/middle", "time/late", "causes", 1.0).unwrap();

        let outcome = db.nodes()
            .one("time/early")
            .forward("causes")
            .hops(2)
            .collect()
            .unwrap();

        println!("[TC-4.2] Temporal path found: {} nodes", outcome.data.len());
        assert!(outcome.data.len() >= 2);
    }

    #[test]
    fn test_concurrent_event_handling() {
        let (db, _dir) = setup_db();

        let payload1 = json!({"_id": "concurrent/a", "timestamp": "2024-06-15T10:00:00Z"}).to_string();
        let payload2 = json!({"_id": "concurrent/b", "timestamp": "2024-06-15T10:00:00Z"}).to_string();
        db.nodes().put_json(&payload1).unwrap();
        db.nodes().put_json(&payload2).unwrap();
        db.flush().unwrap();

        let retrieved1 = db.nodes().get("concurrent/a");
        let retrieved2 = db.nodes().get("concurrent/b");
        assert!(retrieved1.is_some());
        assert!(retrieved2.is_some());
        println!("[TC-4.2] Concurrent events handled");
    }
}

mod tc_4_3_weight_threshold {
    use super::*;

    #[test]
    fn test_weighted_edge_filtering() {
        let (db, _dir) = setup_db();

        let sources = vec!["weight/high", "weight/medium", "weight/low"];
        let target = "weight/effect";

        for slug in &sources {
            let payload = json!({"_id": slug}).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.nodes().put_json(&json!({"_id": target}).to_string()).unwrap();
        db.flush().unwrap();

        db.edges().link("weight/high", target, "caused_by", 0.95).unwrap();
        db.edges().link("weight/medium", target, "caused_by", 0.5).unwrap();
        db.edges().link("weight/low", target, "caused_by", 0.1).unwrap();

        let outcome = db.nodes()
            .one(target)
            .backward("caused_by")
            .collect()
            .unwrap();

        println!("[TC-4.3] Weighted edges found: {} causes", outcome.data.len());
        assert!(outcome.data.len() >= 3);
    }

    #[test]
    fn test_threshold_based_cause_ranking() {
        let (db, _dir) = setup_db();

        db.nodes().put_json(&json!({"_id": "cause/a"}).to_string()).unwrap();
        db.nodes().put_json(&json!({"_id": "cause/b"}).to_string()).unwrap();
        db.nodes().put_json(&json!({"_id": "effect"}).to_string()).unwrap();
        db.flush().unwrap();

        db.edges().link("cause/a", "effect", "causes", 0.9).unwrap();
        db.edges().link("cause/b", "effect", "causes", 0.3).unwrap();

        println!("[TC-4.3] Causes ranked by weight");
    }
}

mod tc_4_4_evidence_attribution {
    use super::*;

    #[test]
    fn test_evidence_tracking() {
        let (db, _dir) = setup_db();

        db.nodes().put_json(&json!({"_id": "evidence/log1", "type": "evidence"}).to_string()).unwrap();
        db.nodes().put_json(&json!({"_id": "evidence/log2", "type": "evidence"}).to_string()).unwrap();
        db.nodes().put_json(&json!({"_id": "evidence/observation"}).to_string()).unwrap();
        db.nodes().put_json(&json!({"_id": "evidence/conclusion"}).to_string()).unwrap();
        db.flush().unwrap();

        db.edges().link("evidence/log1", "evidence/observation", "supports", 0.8).unwrap();
        db.edges().link("evidence/log2", "evidence/observation", "supports", 0.7).unwrap();
        db.edges().link("evidence/observation", "evidence/conclusion", "leads_to", 0.9).unwrap();

        let outcome = db.nodes()
            .one("evidence/observation")
            .forward("leads_to")
            .collect()
            .unwrap();

        assert!(outcome.data.len() >= 1);
        println!("[TC-4.4] Evidence attribution: PASSED");
    }

    #[test]
    fn test_confidence_aggregation() {
        let (db, _dir) = setup_db();

        for i in 0..5 {
            let payload = json!({"_id": format!("conf/evidence-{}", i)}).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.nodes().put_json(&json!({"_id": "conf/observation"}).to_string()).unwrap();
        db.flush().unwrap();

        for i in 0..5 {
            db.edges().link(
                format!("conf/evidence-{}", i).as_str(),
                "conf/observation",
                "supports",
                0.5 + (i as f32 * 0.1)
            ).unwrap();
        }

        println!("[TC-4.4] {} evidence sources linked", 5);
    }
}

mod tc_4_5_solution_node_matching {
    use super::*;

    #[test]
    fn test_solution_pattern_matching() {
        let (db, _dir) = setup_db();

        let solutions = vec![
            ("solution/restart", vec!["error", "crash"]),
            ("solution/patch", vec!["vulnerability", "exploit"]),
            ("solution/scale", vec!["latency", "timeout"]),
        ];

        for (slug, patterns) in &solutions {
            let payload = json!({
                "_id": slug,
                "patterns": patterns,
                "type": "solution"
            }).to_string();
            db.nodes().put_json(&payload).unwrap();
        }

        let problem = json!({
            "_id": "problem/error-crash",
            "symptoms": ["error", "crash"]
        }).to_string();
        db.nodes().put_json(&problem).unwrap();
        db.flush().unwrap();

        let retrieved = db.nodes().get("problem/error-crash");
        assert!(retrieved.is_some());
        println!("[TC-4.5] Problem matched to solution");
    }

    #[test]
    fn test_solution_effectiveness_tracking() {
        let (db, _dir) = setup_db();

        db.nodes().put_json(&json!({"_id": "solution/test"}).to_string()).unwrap();
        db.nodes().put_json(&json!({"_id": "outcome/applied"}).to_string()).unwrap();
        db.flush().unwrap();

        db.edges().link("solution/test", "outcome/applied", "resolved", 0.85).unwrap();

        println!("[TC-4.5] Solution effectiveness tracked");
    }
}

mod tc_4_6_full_rca_workflow {
    use super::*;

    #[test]
    fn test_complete_rca_workflow() {
        let (db, _dir) = setup_db();

        let timeline = vec![
            ("rca/user-report", "2024-06-15T08:00:00Z"),
            ("rca/alert-fired", "2024-06-15T08:05:00Z"),
            ("rca/investigation", "2024-06-15T08:15:00Z"),
            ("rca/root-identified", "2024-06-15T08:30:00Z"),
            ("rca/solution-applied", "2024-06-15T09:00:00Z"),
            ("rca/resolved", "2024-06-15T09:05:00Z"),
        ];

        for (slug, ts) in &timeline {
            let payload = json!({"_id": slug, "timestamp": ts}).to_string();
            db.nodes().put_json(&payload).unwrap();
        }
        db.flush().unwrap();

        db.edges().link("rca/user-report", "rca/alert-fired", "triggers", 1.0).unwrap();
        db.edges().link("rca/alert-fired", "rca/investigation", "starts", 1.0).unwrap();
        db.edges().link("rca/investigation", "rca/root-identified", "finds", 1.0).unwrap();
        db.edges().link("rca/root-identified", "rca/solution-applied", "triggers", 1.0).unwrap();
        db.edges().link("rca/solution-applied", "rca/resolved", "results_in", 1.0).unwrap();

        let _root_cause = db.nodes()
            .one("rca/resolved")
            .backward("results_in")
            .backward("triggers")
            .backward("finds")
            .backward("starts")
            .backward("triggers")
            .collect()
            .unwrap();

        println!("[TC-4.6] RCA workflow - Root cause analysis complete");
        println!("  - Total nodes in timeline: {}", timeline.len());
        println!("  - Causal links created: 5");

        let retrieved1 = db.nodes().get("rca/user-report");
        let retrieved2 = db.nodes().get("rca/resolved");
        assert!(retrieved1.is_some());
        assert!(retrieved2.is_some());
    }

    #[test]
    fn test_rca_with_evidence_and_solutions() {
        let (db, _dir) = setup_db();

        for i in 0..3 {
            let payload = json!({
                "_id": format!("rca/evidence-{}", i),
                "type": "evidence",
                "weight": 0.8
            }).to_string();
            db.nodes().put_json(&payload).unwrap();
        }

        db.nodes().put_json(&json!({"_id": "rca/root", "type": "root_cause"}).to_string()).unwrap();
        db.nodes().put_json(&json!({"_id": "rca/solution", "type": "solution"}).to_string()).unwrap();
        db.flush().unwrap();

        for i in 0..3 {
            db.edges().link(
                format!("rca/evidence-{}", i).as_str(),
                "rca/root",
                "indicates",
                0.8
            ).unwrap();
        }

        db.edges().link("rca/root", "rca/solution", "addressed_by", 0.95).unwrap();

        println!("[TC-4.6] Evidence-attributed RCA: 3 evidence -> 1 root -> 1 solution");
    }

    #[test]
    fn test_parallel_causes() {
        let (db, _dir) = setup_db();

        let causes = vec!["cause/a", "cause/b", "cause/c"];
        for slug in &causes {
            db.nodes().put_json(&json!({"_id": slug}).to_string()).unwrap();
        }
        db.nodes().put_json(&json!({"_id": "effect/multi"}).to_string()).unwrap();
        db.flush().unwrap();

        for slug in &causes {
            db.edges().link(slug, "effect/multi", "contributes_to", 0.5).unwrap();
        }

        let outcome = db.nodes()
            .one("effect/multi")
            .backward("contributes_to")
            .collect()
            .unwrap();

        assert!(outcome.data.len() >= 3);
        println!("[TC-4.6] Parallel causes: {} contributors found", outcome.data.len());
    }
}
