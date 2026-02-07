//! Geo types for Sekejap-DB
//!
//! Provides PostGIS/QGIS compatible geometry storage with:
//! - Point, LineString, Polygon, MultiPolygon support
//! - Structured coordinates [lon, lat] per GeoJSON spec
//! - Named geo features for multiple geometries per entity
//!
//! # Example
//!
//! ```rust
//! use hsdl_sekejap::types::{GeoGeometry, GeoFeature, GeoStore};
//!
//! // Point geometry
//! let point = GeoGeometry::Point { coordinates: [106.8456, -6.2088] };
//!
//! // Polygon geometry (PostGIS standard [lon, lat])
//! let polygon = GeoGeometry::Polygon {
//!     coordinates: vec![vec![
//!         [106.7, -6.9],
//!         [107.0, -6.9],
//!         [107.0, -6.8],
//!         [106.7, -6.8],
//!         [106.7, -6.9],
//!     ]],
//! };
//!
//! // Geo feature with name
//! let feature = GeoFeature::new("center".to_string(), point);
//!
//! // Geo store for multiple features
//! let mut geo_store = GeoStore::new();
//! geo_store.insert("center".to_string(), feature);
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Geometry types following GeoJSON/PostGIS specification
///
/// Coordinates order: [longitude, latitude] (PostGIS standard)
/// All coordinates are in WGS84 (EPSG:4326) by default
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum GeoGeometry {
    /// Point geometry - single location
    ///
    /// # JSON
    /// ```json
    /// { "type": "Point", "coordinates": [106.8456, -6.2088] }
    /// ```
    Point {
        /// [longitude, latitude]
        coordinates: [f64; 2],
    },
    
    /// LineString geometry - sequence of points
    ///
    /// # JSON
    /// ```json
    /// { "type": "LineString", "coordinates": [[106.8, -6.2], [106.85, -6.25], [106.9, -6.3]] }
    /// ```
    LineString {
        /// Array of [lon, lat] points (at least 2)
        coordinates: Vec<[f64; 2]>,
    },
    
    /// Polygon geometry - closed area with optional holes
    ///
    /// First ring is exterior boundary (counter-clockwise).
    /// Additional rings are interior holes (clockwise).
    ///
    /// # JSON
    /// ```json
    /// {
    ///   "type": "Polygon",
    ///   "coordinates": [
    ///     [[106.7, -6.9], [107.0, -6.9], [107.0, -6.8], [106.7, -6.8], [106.7, -6.9]]
    ///   ]
    /// }
    /// ```
    Polygon {
        /// Rings: first is exterior, rest are holes
        coordinates: Vec<Vec<[f64; 2]>>,
    },
    
    /// MultiPolygon geometry - multiple polygons
    ///
    /// For complex geometries like archipelagos
    ///
    /// # JSON
    /// ```json
    /// {
    ///   "type": "MultiPolygon",
    ///   "coordinates": [
    ///     [[[106.7, -6.9], [107.0, -6.9], [107.0, -6.8], [106.7, -6.8], [106.7, -6.9]]]
    ///   ]
    /// }
    /// ```
    MultiPolygon {
        /// Array of polygon coordinate arrays
        coordinates: Vec<Vec<Vec<[f64; 2]>>>,
    },
}

impl GeoGeometry {
    /// Create a Point geometry
    pub fn point(lon: f64, lat: f64) -> Self {
        Self::Point { coordinates: [lon, lat] }
    }
    
    /// Create a LineString geometry
    pub fn line_string(coordinates: Vec<[f64; 2]>) -> Self {
        Self::LineString { coordinates }
    }
    
    /// Create a simple polygon from exterior ring
    pub fn polygon(exterior: Vec<[f64; 2]>) -> Self {
        Self::Polygon { coordinates: vec![exterior] }
    }
    
    /// Create a polygon with holes
    pub fn polygon_with_holes(exterior: Vec<[f64; 2]>, holes: Vec<Vec<[f64; 2]>>) -> Self {
        let mut coords = vec![exterior];
        coords.extend(holes);
        Self::Polygon { coordinates: coords }
    }
    
    /// Get geometry type as string
    pub fn geometry_type(&self) -> &'static str {
        match self {
            GeoGeometry::Point { .. } => "Point",
            GeoGeometry::LineString { .. } => "LineString",
            GeoGeometry::Polygon { .. } => "Polygon",
            GeoGeometry::MultiPolygon { .. } => "MultiPolygon",
        }
    }
    
    /// Check if geometry is valid
    pub fn is_valid(&self) -> bool {
        match self {
            GeoGeometry::Point { .. } => true,
            GeoGeometry::LineString { coordinates: coords } => coords.len() >= 2,
            GeoGeometry::Polygon { coordinates } => {
                !coordinates.is_empty() && coordinates.iter().all(|ring| ring.len() >= 4)
            }
            GeoGeometry::MultiPolygon { coordinates } => {
                !coordinates.is_empty() && coordinates.iter().all(|poly| {
                    !poly.is_empty() && poly.iter().all(|ring| ring.len() >= 4)
                })
            }
        }
    }
    
    /// Get bounding box (min_lon, min_lat, max_lon, max_lat)
    pub fn bounds(&self) -> (f64, f64, f64, f64) {
        let mut min_lon = f64::INFINITY;
        let mut min_lat = f64::INFINITY;
        let mut max_lon = f64::NEG_INFINITY;
        let mut max_lat = f64::NEG_INFINITY;
        
        self.visit_points(|[lon, lat]| {
            min_lon = min_lon.min(lon);
            min_lat = min_lat.min(lat);
            max_lon = max_lon.max(lon);
            max_lat = max_lat.max(lat);
        });
        
        (min_lon, min_lat, max_lon, max_lat)
    }
    
    /// Get centroid (approximate center point)
    pub fn centroid(&self) -> Option<[f64; 2]> {
        let (min_lon, min_lat, max_lon, max_lat) = self.bounds();
        if min_lon == f64::INFINITY {
            None
        } else {
            Some([(min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0])
        }
    }
    
    /// Get total number of vertices
    pub fn vertex_count(&self) -> usize {
        let mut count = 0;
        self.visit_points(|_| count += 1);
        count
    }
    
    /// Visit all points in the geometry
    fn visit_points<F: FnMut([f64; 2])>(&self, mut f: F) {
        match self {
            GeoGeometry::Point { coordinates } => f(*coordinates),
            GeoGeometry::LineString { coordinates } => {
                for coord in coordinates {
                    f(*coord);
                }
            }
            GeoGeometry::Polygon { coordinates } => {
                for ring in coordinates {
                    for coord in ring {
                        f(*coord);
                    }
                }
            }
            GeoGeometry::MultiPolygon { coordinates } => {
                for poly in coordinates {
                    for ring in poly {
                        for coord in ring {
                            f(*coord);
                        }
                    }
                }
            }
        }
    }
    
    /// Calculate approximate area in square degrees (for Polygon/MultiPolygon)
    pub fn area(&self) -> f64 {
        match self {
            GeoGeometry::Polygon { coordinates: _ } => {
                // Simple bounding box area for quick estimate
                let (min_lon, min_lat, max_lon, max_lat) = self.bounds();
                (max_lon - min_lon) * (max_lat - min_lat)
            }
            GeoGeometry::MultiPolygon { coordinates } => {
                coordinates.iter().fold(0.0, |acc, poly| {
                    let (min_lon, min_lat, max_lon, max_lat) = {
                        let mut min_lon = f64::INFINITY;
                        let mut min_lat = f64::INFINITY;
                        let mut max_lon = f64::NEG_INFINITY;
                        let mut max_lat = f64::NEG_INFINITY;
                        for ring in poly {
                            for coord in ring {
                                min_lon = min_lon.min(coord[0]);
                                min_lat = min_lat.min(coord[1]);
                                max_lon = max_lon.max(coord[0]);
                                max_lat = max_lat.max(coord[1]);
                            }
                        }
                        (min_lon, min_lat, max_lon, max_lat)
                    };
                    acc + (max_lon - min_lon) * (max_lat - min_lat)
                })
            }
            _ => 0.0,
        }
    }
    
    /// Convert to internal Point representation (for simple cases)
    pub fn to_point(&self) -> Option<super::Point> {
        match self {
            GeoGeometry::Point { coordinates } => {
                Some(super::Point::new(coordinates[0], coordinates[1]))
            }
            _ => self.centroid().map(|c| super::Point::new(c[0], c[1])),
        }
    }
}

impl fmt::Display for GeoGeometry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeoGeometry::Point { coordinates } => {
                write!(f, "Point([{}, {}])", coordinates[0], coordinates[1])
            }
            GeoGeometry::LineString { coordinates } => {
                write!(f, "LineString({} points)", coordinates.len())
            }
            GeoGeometry::Polygon { coordinates } => {
                write!(f, "Polygon({} rings)", coordinates.len())
            }
            GeoGeometry::MultiPolygon { coordinates } => {
                write!(f, "MultiPolygon({} polygons)", coordinates.len())
            }
        }
    }
}

/// A named geo feature within an entity
///
/// Each entity can have multiple geo features (e.g., "center", "area", "boundary")
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeoFeature {
    /// Feature name (e.g., "center", "area", "boundary")
    pub name: String,
    
    /// Geometry data
    pub geometry: GeoGeometry,
    
    /// Optional SRID (default: 4326 = WGS84)
    #[serde(default = "default_srid")]
    pub srid: u32,
}

fn default_srid() -> u32 {
    4326
}

impl GeoFeature {
    /// Create a new geo feature
    pub fn new(name: String, geometry: GeoGeometry) -> Self {
        Self {
            name,
            geometry,
            srid: default_srid(),
        }
    }
    
    /// Create with custom SRID
    pub fn with_srid(name: String, geometry: GeoGeometry, srid: u32) -> Self {
        Self { name, geometry, srid }
    }
    
    /// Get feature name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Get geometry
    pub fn geometry(&self) -> &GeoGeometry {
        &self.geometry
    }
    
    /// Get geometry type
    pub fn geometry_type(&self) -> &'static str {
        self.geometry.geometry_type()
    }
    
    /// Check if valid
    pub fn is_valid(&self) -> bool {
        self.geometry.is_valid()
    }
    
    /// Get centroid
    pub fn centroid(&self) -> Option<[f64; 2]> {
        self.geometry.centroid()
    }
    
    /// Get bounds
    pub fn bounds(&self) -> (f64, f64, f64, f64) {
        self.geometry.bounds()
    }
}

impl fmt::Display for GeoFeature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GeoFeature({}, {})", self.name, self.geometry)
    }
}

/// Store for multiple geo features (named)
///
/// Provides O(1) access to named geo features.
/// Each entity can have multiple geometries (center, area, boundary, etc.)
///
/// # Example
///
/// ```rust
/// use hsdl_sekejap::types::{GeoStore, GeoGeometry, GeoFeature};
///
/// let mut geo_store = GeoStore::new();
///
/// // Add center point
/// let center = GeoFeature::new(
///     "center".to_string(),
///     GeoGeometry::point(106.85, -6.88),
/// );
/// geo_store.insert("center".to_string(), center);
///
/// // Add area polygon
/// let area = GeoFeature::new(
///     "area".to_string(),
///     GeoGeometry::polygon(vec![
///         [106.7, -6.9], [107.0, -6.9], [107.0, -6.8], [106.7, -6.8], [106.7, -6.9],
///     ]),
/// );
/// geo_store.insert("area".to_string(), area);
///
/// // Query
/// if let Some(feature) = geo_store.get("center") {
///     println!("Center: {}", feature.geometry());
/// }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GeoStore(HashMap<String, GeoFeature>);

impl GeoStore {
    /// Create a new empty geo store
    pub fn new() -> Self {
        Self(HashMap::new())
    }
    
    /// Insert a geo feature
    pub fn insert(&mut self, name: String, feature: GeoFeature) -> Option<GeoFeature> {
        self.0.insert(name, feature)
    }
    
    /// Get a geo feature by name
    pub fn get(&self, name: &str) -> Option<&GeoFeature> {
        self.0.get(name)
    }
    
    /// Get mutable reference to a geo feature
    pub fn get_mut(&mut self, name: &str) -> Option<&mut GeoFeature> {
        self.0.get_mut(name)
    }
    
    /// Check if feature exists
    pub fn contains(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }
    
    /// Remove a feature
    pub fn remove(&mut self, name: &str) -> Option<GeoFeature> {
        self.0.remove(name)
    }
    
    /// Get number of features
    pub fn len(&self) -> usize {
        self.0.len()
    }
    
    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    
    /// Iterate over features
    pub fn iter(&self) -> impl Iterator<Item = (&str, &GeoFeature)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }
    
    /// Iterate mutably over features
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&str, &mut GeoFeature)> {
        self.0.iter_mut().map(|(k, v)| (k.as_str(), v))
    }
    
    /// Get all feature names
    pub fn names(&self) -> Vec<&str> {
        self.0.keys().map(|s| s.as_str()).collect()
    }
    
    /// Get total vertex count across all features
    pub fn total_vertex_count(&self) -> usize {
        self.0.values().map(|f| f.geometry.vertex_count()).sum()
    }
    
    /// Get combined bounds of all features
    pub fn combined_bounds(&self) -> Option<(f64, f64, f64, f64)> {
        let mut min_lon = f64::INFINITY;
        let mut min_lat = f64::INFINITY;
        let mut max_lon = f64::NEG_INFINITY;
        let mut max_lat = f64::NEG_INFINITY;
        
        for feature in self.0.values() {
            let (f_min_lon, f_min_lat, f_max_lon, f_max_lat) = feature.geometry.bounds();
            min_lon = min_lon.min(f_min_lon);
            min_lat = min_lat.min(f_min_lat);
            max_lon = max_lon.max(f_max_lon);
            max_lat = max_lat.max(f_max_lat);
        }
        
        if min_lon == f64::INFINITY {
            None
        } else {
            Some((min_lon, min_lat, max_lon, max_lat))
        }
    }
    
    /// Get first point geometry (for indexing)
    pub fn first_point(&self) -> Option<super::Point> {
        for feature in self.0.values() {
            if let Some(point) = feature.geometry.to_point() {
                return Some(point);
            }
        }
        None
    }
}

impl fmt::Display for GeoStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GeoStore({} features)", self.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_point_geometry() {
        let point = GeoGeometry::point(106.8456, -6.2088);
        assert!(point.is_valid());
        assert_eq!(point.geometry_type(), "Point");
        assert_eq!(point.vertex_count(), 1);
        
        let centroid = point.centroid().unwrap();
        assert!((centroid[0] - 106.8456).abs() < 0.0001);
        assert!((centroid[1] - (-6.2088)).abs() < 0.0001);
    }
    
    #[test]
    fn test_polygon_geometry() {
        let polygon = GeoGeometry::polygon(vec![
            [106.7, -6.9],
            [107.0, -6.9],
            [107.0, -6.8],
            [106.7, -6.8],
            [106.7, -6.9],
        ]);
        assert!(polygon.is_valid());
        assert_eq!(polygon.geometry_type(), "Polygon");
        assert_eq!(polygon.vertex_count(), 5);
    }
    
    #[test]
    fn test_polygon_with_holes() {
        let exterior = vec![
            [106.7, -6.9],
            [107.0, -6.9],
            [107.0, -6.8],
            [106.7, -6.8],
            [106.7, -6.9],
        ];
        let hole = vec![
            [106.8, -6.85],
            [106.9, -6.85],
            [106.9, -6.82],
            [106.8, -6.82],
            [106.8, -6.85],
        ];
        let polygon = GeoGeometry::polygon_with_holes(exterior, vec![hole]);
        assert!(polygon.is_valid());
        assert_eq!(polygon.vertex_count(), 10); // 5 + 5
    }
    
    #[test]
    fn test_geo_feature() {
        let feature = GeoFeature::new(
            "center".to_string(),
            GeoGeometry::point(106.85, -6.88),
        );
        assert_eq!(feature.name(), "center");
        assert_eq!(feature.geometry_type(), "Point");
        assert!(feature.is_valid());
    }
    
    #[test]
    fn test_geo_store() {
        let mut store = GeoStore::new();
        assert!(store.is_empty());
        
        store.insert("center".to_string(), GeoFeature::new("center".to_string(), GeoGeometry::point(106.85, -6.88)));
        store.insert("area".to_string(), GeoFeature::new("area".to_string(), GeoGeometry::polygon(vec![
            [106.7, -6.9], [107.0, -6.9], [107.0, -6.8], [106.7, -6.8], [106.7, -6.9],
        ])));
        
        assert_eq!(store.len(), 2);
        assert!(store.contains("center"));
        assert!(!store.contains("missing"));
        
        let bounds = store.combined_bounds().unwrap();
        assert!((bounds.0 - 106.7).abs() < 0.0001);
        assert!((bounds.2 - 107.0).abs() < 0.0001);
        
        let total_vertices = store.total_vertex_count();
        assert_eq!(total_vertices, 6); // 1 + 5
    }
    
    #[test]
    fn test_bounds() {
        let polygon = GeoGeometry::polygon(vec![
            [106.7, -6.9],
            [107.0, -6.9],
            [107.0, -6.8],
            [106.7, -6.8],
            [106.7, -6.9],
        ]);
        
        let (min_lon, min_lat, max_lon, max_lat) = polygon.bounds();
        assert!((min_lon - 106.7).abs() < 0.0001);
        assert!((max_lon - 107.0).abs() < 0.0001);
        assert!((min_lat - (-6.9)).abs() < 0.0001);
        assert!((max_lat - (-6.8)).abs() < 0.0001);
    }
}
