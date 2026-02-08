//! Geometry Types for Sekejap-DB
//!
//! Provides Point, Polygon, Polyline, and unified Geometry enum
//! for spatial data storage and queries.

use serde::{Deserialize, Serialize};
use std::fmt;

/// 2D Point in Cartesian/Geographic coordinates
///
/// Can represent:
/// - Geographic coordinates (longitude, latitude)
/// - Cartesian coordinates (x, y)
///
/// # Example
///
/// ```rust
/// use sekejap::types::Point;
///
/// let point = Point::new(106.8456, -6.2088);  // Jakarta
/// println!("Longitude: {}, Latitude: {}", point.x, point.y);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Point {
    /// X coordinate (typically longitude)
    pub x: f64,
    /// Y coordinate (typically latitude)
    pub y: f64,
}

impl Point {
    /// Create a new point
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    /// Create from geographic coordinates (lat, lon)
    pub fn from_lat_lon(lat: f64, lon: f64) -> Self {
        Self { x: lon, y: lat }
    }

    /// Get latitude (y coordinate)
    pub fn latitude(&self) -> f64 {
        self.y
    }

    /// Get longitude (x coordinate)
    pub fn longitude(&self) -> f64 {
        self.x
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Point({:.6}, {:.6})", self.x, self.y)
    }
}

/// Polygon geometry with optional interior holes (rings)
///
/// A polygon consists of:
/// - **Exterior ring**: The outer boundary (must be counter-clockwise)
/// - **Interior rings**: Optional holes (must be clockwise)
///
/// # Example
///
/// ```rust
/// use sekejap::types::{Polygon, Point};
///
/// let polygon = Polygon::new(vec![
///     Point::new(106.8, -6.2),
///     Point::new(106.9, -6.2),
///     Point::new(106.9, -6.3),
///     Point::new(106.8, -6.3),
///     Point::new(106.8, -6.2),  // Close the ring
/// ]);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Polygon {
    /// Outer boundary ring (must be closed)
    pub exterior: Vec<Point>,
    /// Interior holes (optional)
    pub interiors: Vec<Vec<Point>>,
}

impl Polygon {
    /// Create a new polygon with exterior ring
    pub fn new(exterior: Vec<Point>) -> Self {
        Self {
            exterior,
            interiors: Vec::new(),
        }
    }

    /// Create a polygon with holes
    pub fn with_holes(exterior: Vec<Point>, interiors: Vec<Vec<Point>>) -> Self {
        Self {
            exterior,
            interiors,
        }
    }

    /// Check if polygon is valid (at least 3 points, closed ring)
    pub fn is_valid(&self) -> bool {
        if self.exterior.len() < 4 {
            return false;
        }
        // Check if first and last point are the same (closed ring)
        self.exterior.first() == self.exterior.last()
    }

    /// Get the bounding box of the polygon
    pub fn bounds(&self) -> (Point, Point) {
        let mut min_x = f64::INFINITY;
        let mut max_x = f64::NEG_INFINITY;
        let mut min_y = f64::INFINITY;
        let mut max_y = f64::NEG_INFINITY;

        for point in &self.exterior {
            min_x = min_x.min(point.x);
            max_x = max_x.max(point.x);
            min_y = min_y.min(point.y);
            max_y = max_y.max(point.y);
        }

        (Point::new(min_x, min_y), Point::new(max_x, max_y))
    }

    /// Get number of vertices
    pub fn num_vertices(&self) -> usize {
        let interior_count: usize = self.interiors.iter().map(|r| r.len()).sum();
        self.exterior.len() + interior_count
    }

    /// Calculate area using shoelace formula
    /// Returns absolute area in square units
    pub fn area(&self) -> f64 {
        let mut area = 0.0;
        let n = self.exterior.len();

        for i in 0..n {
            let j = (i + 1) % n;
            area += self.exterior[i].x * self.exterior[j].y;
            area -= self.exterior[j].x * self.exterior[i].y;
        }

        (area / 2.0).abs()
    }
}

impl fmt::Display for Polygon {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Polygon({} vertices", self.exterior.len())?;
        if !self.interiors.is_empty() {
            write!(f, ", {} holes", self.interiors.len())?;
        }
        write!(f, ")")
    }
}

/// Polyline (LineString) - sequence of connected points
///
/// Represents:
/// - Routes and paths
/// - Boundaries and edges
/// - Trajectories
///
/// # Example
///
/// ```rust
/// use sekejap::types::{Polyline, Point};
///
/// let route = Polyline::new(vec![
///     Point::new(106.8456, -6.2088),  // Jakarta
///     Point::new(106.8556, -6.2188),
///     Point::new(106.8656, -6.2288),
/// ]);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Polyline {
    /// Sequence of points (must have at least 2 points)
    pub points: Vec<Point>,
}

impl Polyline {
    /// Create a new polyline
    pub fn new(points: Vec<Point>) -> Self {
        Self { points }
    }

    /// Check if polyline is valid (at least 2 points)
    pub fn is_valid(&self) -> bool {
        self.points.len() >= 2
    }

    /// Get the bounding box of the polyline
    pub fn bounds(&self) -> (Point, Point) {
        let mut min_x = f64::INFINITY;
        let mut max_x = f64::NEG_INFINITY;
        let mut min_y = f64::INFINITY;
        let mut max_y = f64::NEG_INFINITY;

        for point in &self.points {
            min_x = min_x.min(point.x);
            max_x = max_x.max(point.x);
            min_y = min_y.min(point.y);
            max_y = max_y.max(point.y);
        }

        (Point::new(min_x, min_y), Point::new(max_x, max_y))
    }

    /// Calculate total length using Euclidean distance
    pub fn length(&self) -> f64 {
        if self.points.len() < 2 {
            return 0.0;
        }

        let mut length = 0.0;
        for i in 0..self.points.len() - 1 {
            length += distance(&self.points[i], &self.points[i + 1]);
        }
        length
    }

    /// Get number of vertices
    pub fn num_points(&self) -> usize {
        self.points.len()
    }

    /// Get start point
    pub fn start(&self) -> Option<Point> {
        self.points.first().cloned()
    }

    /// Get end point
    pub fn end(&self) -> Option<Point> {
        self.points.last().cloned()
    }
}

impl fmt::Display for Polyline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Polyline({} points)", self.points.len())
    }
}

/// Unified Geometry enum for spatial types
///
/// Provides a single type that can represent any geometry:
/// - Points (locations, markers)
/// - Polygons (zones, regions, boundaries)
/// - Polylines (routes, paths, edges)
///
/// # Example
///
/// ```rust
/// use sekejap::types::{Geometry, Point, Polygon, Polyline};
///
/// let point_geom = Geometry::Point(Point::new(106.8, -6.2));
/// let polygon_geom = Geometry::Polygon(Polygon::new(vec![
///     Point::new(106.8, -6.2),
///     Point::new(106.9, -6.2),
///     Point::new(106.9, -6.3),
///     Point::new(106.8, -6.3),
///     Point::new(106.8, -6.2),
/// ]));
/// let polyline_geom = Geometry::Polyline(Polyline::new(vec![
///     Point::new(106.8, -6.2),
///     Point::new(106.85, -6.25),
///     Point::new(106.9, -6.3),
/// ]));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Geometry {
    /// Point geometry
    Point(Point),
    /// Polygon geometry (area)
    Polygon(Polygon),
    /// Polyline geometry (line)
    Polyline(Polyline),
}

impl Geometry {
    /// Get the bounding box of the geometry
    pub fn bounds(&self) -> (Point, Point) {
        match self {
            Geometry::Point(p) => (*p, *p),
            Geometry::Polygon(poly) => poly.bounds(),
            Geometry::Polyline(line) => line.bounds(),
        }
    }

    /// Check if geometry is valid
    pub fn is_valid(&self) -> bool {
        match self {
            Geometry::Point(_) => true,
            Geometry::Polygon(poly) => poly.is_valid(),
            Geometry::Polyline(line) => line.is_valid(),
        }
    }

    /// Get geometry type as string
    pub fn geometry_type(&self) -> &'static str {
        match self {
            Geometry::Point(_) => "Point",
            Geometry::Polygon(_) => "Polygon",
            Geometry::Polyline(_) => "Polyline",
        }
    }
}

impl fmt::Display for Geometry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Geometry::Point(p) => write!(f, "{}", p),
            Geometry::Polygon(p) => write!(f, "{}", p),
            Geometry::Polyline(l) => write!(f, "{}", l),
        }
    }
}

/// Calculate Euclidean distance between two points
pub fn distance(a: &Point, b: &Point) -> f64 {
    let dx = b.x - a.x;
    let dy = b.y - a.y;
    (dx * dx + dy * dy).sqrt()
}

/// Calculate squared distance (faster, avoids sqrt)
pub fn distance_squared(a: &Point, b: &Point) -> f64 {
    let dx = b.x - a.x;
    let dy = b.y - a.y;
    dx * dx + dy * dy
}

/// Check if a point is on a line segment (with tolerance)
pub fn point_on_segment(
    point: &Point,
    segment_start: &Point,
    segment_end: &Point,
    tolerance: f64,
) -> bool {
    // Check if point is on the line
    let _d1 = distance_squared(point, segment_start);
    let _d2 = distance_squared(point, segment_end);
    let d3 = distance_squared(segment_start, segment_end);

    // Use dot product to check if point is between endpoints
    let dot = (point.x - segment_start.x) * (segment_end.x - segment_start.x)
        + (point.y - segment_start.y) * (segment_end.y - segment_start.y);
    let param = if d3 == 0.0 { 0.0 } else { dot / d3 };

    // Check if point is on the segment
    if !(0.0..=1.0).contains(&param) {
        return false;
    }

    let closest_x = segment_start.x + param * (segment_end.x - segment_start.x);
    let closest_y = segment_start.y + param * (segment_end.y - segment_start.y);

    let dist = ((point.x - closest_x).powi(2) + (point.y - closest_y).powi(2)).sqrt();
    dist <= tolerance
}

/// Point-in-polygon test using ray casting algorithm
///
/// Returns true if point is inside the polygon (excluding boundary)
///
/// # Arguments
///
/// * `point` - The point to test
/// * `polygon` - The polygon to test against
///
/// # Example
///
/// ```rust
/// use sekejap::types::{Point, Polygon, point_in_polygon};
///
/// let polygon = Polygon::new(vec![
///     Point::new(0.0, 0.0),
///     Point::new(10.0, 0.0),
///     Point::new(10.0, 10.0),
///     Point::new(0.0, 10.0),
///     Point::new(0.0, 0.0),
/// ]);
///
/// let inside = Point::new(5.0, 5.0);
/// let outside = Point::new(15.0, 15.0);
///
/// assert!(point_in_polygon(&inside, &polygon));
/// assert!(!point_in_polygon(&outside, &polygon));
/// ```
pub fn point_in_polygon(point: &Point, polygon: &Polygon) -> bool {
    let mut inside = false;
    let n = polygon.exterior.len();

    // Ray casting algorithm
    for i in 0..n {
        let j = (i + 1) % n;
        let pi = &polygon.exterior[i];
        let pj = &polygon.exterior[j];

        // Check if ray crosses edge
        if ((pi.y > point.y) != (pj.y > point.y))
            && (point.x < (pj.x - pi.x) * (point.y - pi.y) / (pj.y - pi.y) + pi.x)
        {
            inside = !inside;
        }
    }

    // Check interior rings (holes) - point must NOT be in holes
    for interior in &polygon.interiors {
        if point_in_polygon_ring(point, interior) {
            return false; // Point is in a hole
        }
    }

    inside
}

/// Point-in-polygon ring (simpler, no hole checking)
fn point_in_polygon_ring(point: &Point, ring: &[Point]) -> bool {
    let mut inside = false;
    let n = ring.len();

    for i in 0..n {
        let j = (i + 1) % n;
        let pi = &ring[i];
        let pj = &ring[j];

        if ((pi.y > point.y) != (pj.y > point.y))
            && (point.x < (pj.x - pi.x) * (point.y - pi.y) / (pj.y - pi.y) + pi.x)
        {
            inside = !inside;
        }
    }

    inside
}

/// Check if a polyline intersects a polygon
///
/// Returns true if any segment of the polyline crosses the polygon
/// (including boundary intersection)
///
/// # Arguments
///
/// * `polyline` - The polyline to test
/// * `polygon` - The polygon to test against
pub fn polyline_intersects_polygon(polyline: &Polyline, polygon: &Polygon) -> bool {
    // Check each segment of the polyline
    for i in 0..polyline.points.len() - 1 {
        let seg_start = &polyline.points[i];
        let seg_end = &polyline.points[i + 1];

        // Check if segment intersects polygon
        if segment_intersects_polygon(seg_start, seg_end, polygon) {
            return true;
        }
    }

    // Check if polyline is completely inside polygon
    if polyline.points.iter().all(|p| point_in_polygon(p, polygon)) {
        return true;
    }

    false
}

/// Check if a line segment intersects a polygon
fn segment_intersects_polygon(seg_start: &Point, seg_end: &Point, polygon: &Polygon) -> bool {
    // Check if segment endpoints are on different sides of polygon edges
    let n = polygon.exterior.len();

    for i in 0..n {
        let j = (i + 1) % n;
        let poly_start = &polygon.exterior[i];
        let poly_end = &polygon.exterior[j];

        if segments_intersect(seg_start, seg_end, poly_start, poly_end) {
            return true;
        }
    }

    false
}

/// Check if two line segments intersect
///
/// Returns true if segment [p1, p2] intersects segment [p3, p4]
fn segments_intersect(p1: &Point, p2: &Point, p3: &Point, p4: &Point) -> bool {
    // Calculate orientation
    let o1 = orientation(p1, p2, p3);
    let o2 = orientation(p1, p2, p4);
    let o3 = orientation(p3, p4, p1);
    let o4 = orientation(p3, p4, p2);

    // General case
    if o1 != o2 && o3 != o4 {
        return true;
    }

    // Special cases (collinear)
    if o1 == 0 && on_segment(p1, p2, p3) {
        return true;
    }
    if o2 == 0 && on_segment(p1, p2, p4) {
        return true;
    }
    if o3 == 0 && on_segment(p3, p4, p1) {
        return true;
    }
    if o4 == 0 && on_segment(p3, p4, p2) {
        return true;
    }

    false
}

/// Calculate orientation of ordered triplet (p, q, r)
/// Returns 0 if collinear, 1 if clockwise, 2 if counterclockwise
fn orientation(p: &Point, q: &Point, r: &Point) -> i32 {
    let val = (q.y - p.y) * (r.x - q.x) - (q.x - p.x) * (r.y - q.y);

    if val.abs() < 1e-10 {
        0
    } else if val > 0.0 {
        1
    } else {
        2
    }
}

/// Check if point q lies on segment pr
fn on_segment(p: &Point, r: &Point, q: &Point) -> bool {
    q.x <= p.x.max(r.x) && q.x >= p.x.min(r.x) && q.y <= p.y.max(r.y) && q.y >= p.y.min(r.y)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_polygon_bounds() {
        let polygon = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 10.0),
            Point::new(0.0, 10.0),
            Point::new(0.0, 0.0),
        ]);
        let (min, max) = polygon.bounds();
        assert_eq!(min.x, 0.0);
        assert_eq!(min.y, 0.0);
        assert_eq!(max.x, 10.0);
        assert_eq!(max.y, 10.0);
    }

    #[test]
    fn test_polygon_area() {
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
    fn test_polyline_length() {
        let line = Polyline::new(vec![Point::new(0.0, 0.0), Point::new(3.0, 4.0)]);
        assert!((line.length() - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_point_in_polygon() {
        let polygon = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 10.0),
            Point::new(0.0, 10.0),
            Point::new(0.0, 0.0),
        ]);

        let inside = Point::new(5.0, 5.0);
        let outside = Point::new(15.0, 15.0);
        let on_boundary = Point::new(5.0, 0.0);

        assert!(point_in_polygon(&inside, &polygon));
        assert!(!point_in_polygon(&outside, &polygon));
        // Boundary behavior may vary
    }

    #[test]
    fn test_polyline_intersects_polygon() {
        let polygon = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 10.0),
            Point::new(0.0, 10.0),
            Point::new(0.0, 0.0),
        ]);

        // Polyline crossing the polygon
        let crossing = Polyline::new(vec![Point::new(-5.0, 5.0), Point::new(15.0, 5.0)]);
        assert!(polyline_intersects_polygon(&crossing, &polygon));

        // Polyline completely outside
        let outside = Polyline::new(vec![Point::new(20.0, 20.0), Point::new(30.0, 30.0)]);
        assert!(!polyline_intersects_polygon(&outside, &polygon));

        // Polyline completely inside
        let inside = Polyline::new(vec![
            Point::new(2.0, 2.0),
            Point::new(5.0, 5.0),
            Point::new(8.0, 2.0),
        ]);
        assert!(polyline_intersects_polygon(&inside, &polygon));
    }

    #[test]
    fn test_distance() {
        let a = Point::new(0.0, 0.0);
        let b = Point::new(3.0, 4.0);
        assert!((distance(&a, &b) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_geometry_bounds() {
        let point_geom = Geometry::Point(Point::new(5.0, 5.0));
        assert_eq!(
            point_geom.bounds(),
            (Point::new(5.0, 5.0), Point::new(5.0, 5.0))
        );

        let polygon = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(10.0, 0.0),
            Point::new(10.0, 10.0),
            Point::new(0.0, 10.0),
            Point::new(0.0, 0.0),
        ]);
        let polygon_geom = Geometry::Polygon(polygon);
        let (min, max) = polygon_geom.bounds();
        assert_eq!(min.x, 0.0);
        assert_eq!(max.x, 10.0);
    }

    #[test]
    fn test_geometry_type() {
        let point_geom = Geometry::Point(Point::new(0.0, 0.0));
        let polygon_geom = Geometry::Polygon(Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(1.0, 0.0),
            Point::new(1.0, 1.0),
            Point::new(0.0, 1.0),
            Point::new(0.0, 0.0),
        ]));
        let polyline_geom = Geometry::Polyline(Polyline::new(vec![
            Point::new(0.0, 0.0),
            Point::new(1.0, 1.0),
        ]));

        assert_eq!(point_geom.geometry_type(), "Point");
        assert_eq!(polygon_geom.geometry_type(), "Polygon");
        assert_eq!(polyline_geom.geometry_type(), "Polyline");
    }
}
