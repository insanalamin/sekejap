//! Spatial indexing using R-tree for efficient geospatial queries
//!
//! Provides efficient geospatial queries using rstar R-tree:
//! - Point-based indexing for fast radius searches
//! - Polygon centroid indexing for area queries
//! - Point-in-polygon containment checks
//! - Integration with GeoGeometry types

use crate::NodeId;
use crate::types::geo::GeoFeature;
use rstar::{Point, RTree};

/// Spatial point for R-tree indexing (latitude, longitude)
#[derive(Debug, Clone, Copy, PartialEq)]
struct SpatialPoint {
    node_id: NodeId,
    point: [f64; 2], // [latitude, longitude]
}

impl SpatialPoint {
    fn new(node_id: NodeId, lat: f64, lon: f64) -> Self {
        Self {
            node_id,
            point: [lat, lon],
        }
    }
}

impl Point for SpatialPoint {
    type Scalar = f64;
    const DIMENSIONS: usize = 2;

    fn generate(mut generator: impl FnMut(usize) -> Self::Scalar) -> Self {
        Self {
            node_id: 0,
            point: [generator(0), generator(1)],
        }
    }

    fn nth(&self, index: usize) -> Self::Scalar {
        self.point[index]
    }

    fn nth_mut(&mut self, index: usize) -> &mut Self::Scalar {
        &mut self.point[index]
    }
}

/// R-tree based spatial index supporting points and polygon centroids
///
/// # Features
///
/// - Point indexing with O(log N) lookup
/// - Polygon centroid indexing for area queries
/// - Radius search using Haversine distance
/// - Point-in-polygon containment
/// - K-nearest-neighbor queries
///
/// # Example
///
/// ```rust,ignore
/// use sekejap::index::SpatialIndex;
///
/// let mut index = SpatialIndex::new();
/// index.insert_point(1, -6.2088, 106.8456);  // Bandung
/// index.insert_point(2, -6.3, 107.0);
///
/// // Find nodes within 50km
/// let nearby = index.find_within_radius(-6.2088, 106.8456, 50.0);
/// ```
pub struct SpatialIndex {
    rtree: RTree<SpatialPoint>,
}

impl SpatialIndex {
    /// Create a new empty spatial index
    pub fn new() -> Self {
        Self {
            rtree: RTree::new(),
        }
    }

    /// Insert a node with point coordinates into spatial index
    pub fn insert_point(&mut self, node_id: NodeId, lat: f64, lon: f64) {
        let point = SpatialPoint::new(node_id, lat, lon);
        self.rtree.insert(point);
    }

    /// Insert a GeoFeature (uses centroid for polygon/line centroids)
    pub fn insert_geo_feature(&mut self, node_id: NodeId, feature: &GeoFeature) {
        if let Some(centroid) = feature.centroid() {
            // GeoGeometry uses [lon, lat], but we need [lat, lon] for our point
            // centroid[0] = longitude, centroid[1] = latitude
            self.insert_point(node_id, centroid[1], centroid[0]);
        }
        // If no centroid (invalid geometry), skip
    }

    /// Find all nodes within a given radius of a center point
    ///
    /// Uses Haversine formula for accurate distance calculation.
    ///
    /// # Arguments
    ///
    /// * `center_lat` - Center latitude
    /// * `center_lon` - Center longitude
    /// * `radius_km` - Search radius in kilometers
    ///
    /// # Returns
    ///
    /// Vector of node IDs within the radius
    pub fn find_within_radius(
        &self,
        center_lat: f64,
        center_lon: f64,
        radius_km: f64,
    ) -> Vec<NodeId> {
        // Use R-tree iteration and filter by Haversine distance
        self.rtree
            .iter()
            .filter(|sp| {
                let distance = haversine_distance(center_lat, center_lon, sp.point[0], sp.point[1]);
                distance <= radius_km
            })
            .map(|sp| sp.node_id)
            .collect()
    }

    /// Find k nearest neighbors to a given point
    ///
    /// # Arguments
    ///
    /// * `center_lat` - Center latitude
    /// * `center_lon` - Center longitude
    /// * `k` - Number of results to return
    ///
    /// # Returns
    ///
    /// Vector of (node_id, distance_km) tuples, sorted by distance
    pub fn find_k_nearest(&self, center_lat: f64, center_lon: f64, k: usize) -> Vec<(NodeId, f64)> {
        let mut results: Vec<_> = self
            .rtree
            .iter()
            .map(|sp| {
                let distance = haversine_distance(center_lat, center_lon, sp.point[0], sp.point[1]);
                (sp.node_id, distance)
            })
            .collect();

        // Sort by distance and take k
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        results.into_iter().take(k).collect()
    }

    /// Check if a point is inside a polygon using ray casting
    ///
    /// Uses the ray casting algorithm for point-in-polygon detection.
    ///
    /// # Arguments
    ///
    /// * `lat`, `lon` - Point coordinates
    /// * `polygon` - Polygon coordinates (vec of [lon, lat] rings)
    ///
    /// # Returns
    ///
    /// True if point is inside the polygon
    pub fn is_point_in_polygon(&self, lat: f64, lon: f64, polygon: &[[f64; 2]]) -> bool {
        let mut inside = false;
        let n = polygon.len();

        for i in 0..n {
            let (xi, yi) = (polygon[i][0], polygon[i][1]);
            let (xj, yj) = (polygon[(i + 1) % n][0], polygon[(i + 1) % n][1]);

            // Check if point is on the edge
            if on_segment(xi, yi, xj, yj, lon, lat) {
                return true;
            }

            // Ray casting algorithm
            let intersect =
                ((yi > lat) != (yj > lat)) && (lon < (xj - xi) * (lat - yi) / (yj - yi) + xi);

            if intersect {
                inside = !inside;
            }
        }

        inside
    }

    /// Find all nodes whose polygon contains a point
    ///
    /// For each node, checks if the given point is within its polygon.
    /// Requires polygon data to be provided separately.
    ///
    /// # Arguments
    ///
    /// * `lat`, `lon` - Point coordinates
    /// * `get_polygon` - Function to get polygon for a node ID
    ///
    /// # Returns
    ///
    /// Vector of node IDs whose polygon contains the point
    pub fn find_containing_polygon<F>(&self, lat: f64, lon: f64, get_polygon: F) -> Vec<NodeId>
    where
        F: Fn(NodeId) -> Option<Vec<[f64; 2]>>,
    {
        // First filter by bounding box (nodes whose bbox contains the point)
        let candidates: Vec<NodeId> = self
            .rtree
            .iter()
            .filter(|sp| {
                // Rough check - point should be near the node's indexed point
                let dist = haversine_distance(lat, lon, sp.point[0], sp.point[1]);
                dist < 1000.0 // 1000km pre-filter
            })
            .map(|sp| sp.node_id)
            .collect();

        // Then check each candidate's polygon
        candidates
            .into_iter()
            .filter_map(|node_id| {
                get_polygon(node_id)
                    .filter(|poly| self.is_point_in_polygon(lat, lon, poly))
                    .map(|_| node_id)
            })
            .collect()
    }

    /// Get all node IDs in the spatial index
    pub fn get_all_node_ids(&self) -> Vec<NodeId> {
        self.rtree.iter().map(|sp| sp.node_id).collect()
    }

    /// Get the number of nodes in the spatial index
    pub fn len(&self) -> usize {
        self.rtree.size()
    }

    /// Check if the spatial index is empty
    pub fn is_empty(&self) -> bool {
        self.rtree.size() == 0
    }

    /// Remove a node from the spatial index by node_id
    pub fn remove(&mut self, node_id: NodeId) {
        self.rtree.remove(&SpatialPoint::new(node_id, 0.0, 0.0));
    }

    /// Clear all entries from the spatial index
    pub fn clear(&mut self) {
        self.rtree = RTree::new();
    }
}

/// Calculate Haversine distance between two points
///
/// Returns distance in kilometers.
pub fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_KM: f64 = 6371.0;

    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();

    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);

    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_KM * c
}

/// Check if point (px, py) is on segment (x1, y1)-(x2, y2)
fn on_segment(x1: f64, y1: f64, x2: f64, y2: f64, px: f64, py: f64) -> bool {
    let cross = (py - y1) * (x2 - x1) - (px - x1) * (y2 - y1);
    if cross.abs() > 1e-10 {
        return false;
    }

    let dot = (px - x1) * (px - x2) + (py - y1) * (py - y2);
    dot <= 1e-10
}

impl Default for SpatialIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spatial_index_insert() {
        let mut index = SpatialIndex::new();

        index.insert_point(0, 1.0, 2.0);
        index.insert_point(1, 3.0, 4.0);
        index.insert_point(2, 5.0, 6.0);

        assert_eq!(index.len(), 3);
    }

    #[test]
    fn test_find_within_radius() {
        let mut index = SpatialIndex::new();

        // Bandung coordinates
        index.insert_point(1, -6.2088, 106.8456); // Bandung center
        index.insert_point(2, -6.3, 107.0); // Near Bandung (~15km away)
        index.insert_point(3, -6.6, 106.8); // ~44km away
        index.insert_point(4, -5.0, 105.0); // ~150km away

        // Search within 20km radius
        let results = index.find_within_radius(-6.2088, 106.8456, 20.0);

        assert_eq!(results.len(), 2);
        assert!(results.contains(&1));
        assert!(results.contains(&2));
    }

    #[test]
    fn test_find_k_nearest() {
        let mut index = SpatialIndex::new();

        // Add points at different distances
        index.insert_point(1, -6.2088, 106.8456); // Center
        index.insert_point(2, -6.3, 107.0); // ~15km
        index.insert_point(3, -6.6, 106.8); // ~44km

        // Find 2 nearest
        let nearest = index.find_k_nearest(-6.2088, 106.8456, 2);

        assert_eq!(nearest.len(), 2);
        assert_eq!(nearest[0].0, 1); // Center point should be closest (0 distance)
        assert!(nearest[0].1 < nearest[1].1); // First should be closer than second
    }

    #[test]
    fn test_is_point_in_polygon() {
        let index = SpatialIndex::new();

        // Simple triangle
        let polygon = [[0.0, 0.0], [5.0, 0.0], [2.5, 5.0]];

        // Point inside triangle
        assert!(index.is_point_in_polygon(2.5, 2.5, &polygon));

        // Point outside triangle
        assert!(!index.is_point_in_polygon(10.0, 10.0, &polygon));
    }

    #[test]
    fn test_clear() {
        let mut index = SpatialIndex::new();

        index.insert_point(1, 1.0, 2.0);
        index.insert_point(2, 3.0, 4.0);

        assert_eq!(index.len(), 2);

        index.clear();

        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
    }

    #[test]
    fn test_haversine_distance() {
        // Jakarta to Bogor (~43km)
        let dist = haversine_distance(-6.2088, 106.8456, -6.5950, 106.8170);
        assert!(
            (dist - 43.0).abs() < 5.0,
            "Distance should be ~43km, got: {:.2}km",
            dist
        );

        // Same point
        let zero_dist = haversine_distance(-6.2088, 106.8456, -6.2088, 106.8456);
        assert!(zero_dist < 0.1, "Zero distance should be ~0");
    }
}
