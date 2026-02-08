//! Integration Test: Geometry and Spatial Operations
//!
//! Tests Point, Polygon, Polyline types and spatial queries.

use sekejap::{Geometry, Point, Polygon, Polyline, SekejapDB, WriteOptions};
use tempfile::TempDir;

#[cfg(feature = "spatial")]
mod spatial_tests {
    use super::*;

    #[test]
    fn test_write_with_polygon() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        let polygon = Polygon::new(vec![
            Point::new(106.8, -6.2),
            Point::new(106.9, -6.2),
            Point::new(106.9, -6.3),
            Point::new(106.8, -6.3),
            Point::new(106.8, -6.2),
        ]);

        let node_id = db
            .write_with_options(
                "jakarta-zone",
                r#"{"title": "Jakarta Zone"}"#,
                WriteOptions {
                    geometry: Some(Geometry::Polygon(polygon)),
                    publish_now: true,
                    ..Default::default()
                },
            )
            .unwrap();

        assert!(node_id > 0);
    }

    #[test]
    fn test_write_with_polyline() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        let polyline = Polyline::new(vec![
            Point::new(106.8, -6.2),
            Point::new(106.85, -6.25),
            Point::new(106.9, -6.3),
        ]);

        let node_id = db
            .write_with_options(
                "route-001",
                r#"{"title": "Delivery Route"}"#,
                WriteOptions {
                    geometry: Some(Geometry::Polyline(polyline)),
                    publish_now: true,
                    ..Default::default()
                },
            )
            .unwrap();

        assert!(node_id > 0);
    }

    #[test]
    fn test_write_with_point() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        let point = Point::new(106.8456, -6.2088);

        let node_id = db
            .write_with_options(
                "location-001",
                r#"{"title": "Jakarta Landmark"}"#,
                WriteOptions {
                    geometry: Some(Geometry::Point(point)),
                    publish_now: true,
                    ..Default::default()
                },
            )
            .unwrap();

        assert!(node_id > 0);
    }

    #[test]
    fn test_polygon_area_calculation() {
        // 10x10 square = 100 area
        let square = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 10.0),
            Point::new(0.0, 10.0),
            Point::new(0.0, 0.0),
        ]);

        assert!((square.area() - 100.0).abs() < 1e-6);
    }

    #[test]
    fn test_polyline_length() {
        // Rectangle path: (0,0)->(3,0)=3, (3,0)->(3,4)=4, (3,4)->(0,4)=3, (0,4)->(0,0)=4. Total = 14
        let polyline = Polyline::new(vec![
            Point::new(0.0, 0.0),
            Point::new(3.0, 0.0),
            Point::new(3.0, 4.0),
            Point::new(0.0, 4.0),
            Point::new(0.0, 0.0),
        ]);

        assert!((polyline.length() - 14.0).abs() < 1e-6);
    }

    #[test]
    fn test_point_in_polygon_inside() {
        let polygon = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 10.0),
            Point::new(0.0, 10.0),
            Point::new(0.0, 0.0),
        ]);

        let inside = Point::new(5.0, 5.0);
        assert!(sekejap::point_in_polygon(&inside, &polygon));
    }

    #[test]
    fn test_point_in_polygon_outside() {
        let polygon = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 10.0),
            Point::new(0.0, 10.0),
            Point::new(0.0, 0.0),
        ]);

        let outside = Point::new(15.0, 15.0);
        assert!(!sekejap::point_in_polygon(&outside, &polygon));
    }

    #[test]
    fn test_polyline_intersects_polygon_crossing() {
        let polygon = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 10.0),
            Point::new(0.0, 10.0),
            Point::new(0.0, 0.0),
        ]);

        // Polyline crosses the polygon
        let crossing = Polyline::new(vec![Point::new(-5.0, 5.0), Point::new(15.0, 5.0)]);

        assert!(sekejap::polyline_intersects_polygon(&crossing, &polygon));
    }

    #[test]
    fn test_polyline_intersects_polygon_outside() {
        let polygon = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 10.0),
            Point::new(0.0, 10.0),
            Point::new(0.0, 0.0),
        ]);

        // Polyline is completely outside
        let outside = Polyline::new(vec![Point::new(20.0, 20.0), Point::new(30.0, 30.0)]);

        assert!(!sekejap::polyline_intersects_polygon(&outside, &polygon));
    }

    #[test]
    fn test_polyline_inside_polygon() {
        let polygon = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 10.0),
            Point::new(0.0, 10.0),
            Point::new(0.0, 0.0),
        ]);

        // Polyline is completely inside
        let inside = Polyline::new(vec![
            Point::new(2.0, 2.0),
            Point::new(5.0, 5.0),
            Point::new(8.0, 2.0),
        ]);

        assert!(sekejap::polyline_intersects_polygon(&inside, &polygon));
    }

    #[test]
    fn test_polygon_with_holes() {
        let exterior = vec![
            Point::new(0.0, 0.0),
            Point::new(20.0, 0.0),
            Point::new(20.0, 20.0),
            Point::new(0.0, 20.0),
            Point::new(0.0, 0.0),
        ];

        // Hole in the middle
        let hole = vec![
            Point::new(5.0, 5.0),
            Point::new(15.0, 5.0),
            Point::new(15.0, 15.0),
            Point::new(5.0, 15.0),
            Point::new(5.0, 5.0),
        ];

        let polygon = Polygon::with_holes(exterior, vec![hole]);

        assert!(polygon.is_valid());
        assert_eq!(polygon.interiors.len(), 1);

        // Point in hole should be false
        let in_hole = Point::new(10.0, 10.0);
        assert!(!sekejap::point_in_polygon(&in_hole, &polygon));

        // Point in exterior but not hole should be true
        let outside_hole = Point::new(2.0, 2.0);
        assert!(sekejap::point_in_polygon(&outside_hole, &polygon));
    }

    #[test]
    fn test_geometry_bounds() {
        let polygon = Polygon::new(vec![
            Point::new(5.0, 5.0),
            Point::new(15.0, 5.0),
            Point::new(15.0, 15.0),
            Point::new(5.0, 15.0),
            Point::new(5.0, 5.0),
        ]);

        let (min, max) = polygon.bounds();
        assert_eq!(min.x, 5.0);
        assert_eq!(min.y, 5.0);
        assert_eq!(max.x, 15.0);
        assert_eq!(max.y, 15.0);
    }

    #[test]
    fn test_distance_calculation() {
        let a = Point::new(0.0, 0.0);
        let b = Point::new(3.0, 4.0);

        let dist = sekejap::distance(&a, &b);
        assert!((dist - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_multiple_geometry_types() {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SekejapDB::new(temp_dir.path()).unwrap();

        // Zone polygon
        db.write_with_options(
            "zone-1",
            r#"{"title": "Delivery Zone"}"#,
            WriteOptions {
                geometry: Some(Geometry::Polygon(Polygon::new(vec![
                    Point::new(106.8, -6.2),
                    Point::new(106.9, -6.2),
                    Point::new(106.9, -6.3),
                    Point::new(106.8, -6.3),
                    Point::new(106.8, -6.2),
                ]))),
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();

        // Route polyline
        db.write_with_options(
            "route-1",
            r#"{"title": "Delivery Route"}"#,
            WriteOptions {
                geometry: Some(Geometry::Polyline(Polyline::new(vec![
                    Point::new(106.75, -6.15),
                    Point::new(106.85, -6.25),
                    Point::new(106.95, -6.35),
                ]))),
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();

        // Checkpoint point
        db.write_with_options(
            "checkpoint-1",
            r#"{"title": "Checkpoint"}"#,
            WriteOptions {
                geometry: Some(Geometry::Point(Point::new(106.85, -6.25))),
                publish_now: true,
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(db.storage().len(), 3);
    }
}

#[test]
fn test_point_creation() {
    let point = Point::new(106.8456, -6.2088);
    assert_eq!(point.x, 106.8456);
    assert_eq!(point.y, -6.2088);
}

#[test]
fn test_point_from_lat_lon() {
    let point = Point::from_lat_lon(-6.2088, 106.8456);
    assert_eq!(point.x, 106.8456); // lon
    assert_eq!(point.y, -6.2088); // lat
}

#[test]
fn test_polygon_validity() {
    let valid = Polygon::new(vec![
        Point::new(0.0, 0.0),
        Point::new(10.0, 0.0),
        Point::new(10.0, 10.0),
        Point::new(0.0, 10.0),
        Point::new(0.0, 0.0),
    ]);
    assert!(valid.is_valid());

    let invalid = Polygon::new(vec![
        Point::new(0.0, 0.0),
        Point::new(10.0, 0.0),
        Point::new(10.0, 10.0),
    ]);
    assert!(!invalid.is_valid());
}

#[test]
fn test_polyline_validity() {
    let valid = Polyline::new(vec![
        Point::new(0.0, 0.0),
        Point::new(10.0, 0.0),
        Point::new(10.0, 10.0),
    ]);
    assert!(valid.is_valid());

    let invalid = Polyline::new(vec![Point::new(0.0, 0.0)]);
    assert!(!invalid.is_valid());
}

#[test]
fn test_geometry_display() {
    let point = Geometry::Point(Point::new(1.0, 2.0));
    assert!(format!("{}", point).contains("Point"));

    let polygon = Geometry::Polygon(Polygon::new(vec![
        Point::new(0.0, 0.0),
        Point::new(1.0, 0.0),
        Point::new(1.0, 1.0),
        Point::new(0.0, 1.0),
        Point::new(0.0, 0.0),
    ]));
    assert!(format!("{}", polygon).contains("Polygon"));

    let polyline = Geometry::Polyline(Polyline::new(vec![
        Point::new(0.0, 0.0),
        Point::new(1.0, 1.0),
    ]));
    assert!(format!("{}", polyline).contains("Polyline"));
}
