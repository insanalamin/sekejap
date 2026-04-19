//! Pure geometry functions for spatial queries.
//!
//! All coordinates: `[lon, lat]` in GeoJSON, `(lat, lon)` in function params (PostGIS convention).
//! No external crate dependencies — everything is hand-rolled.

use serde_json::Value;
use std::collections::HashMap;

const EARTH_RADIUS_KM: f64 = 6371.0;

// ── Haversine distance ───────────────────────────────────────────────────────

/// Great-circle distance between two points in kilometres.
pub fn haversine_km(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();

    let a = (d_lat / 2.0).sin().powi(2) + lat1_r.cos() * lat2_r.cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    EARTH_RADIUS_KM * c
}

/// Euclidean distance in degrees (fast, for small distances).
fn euclidean_degrees(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    ((lat2 - lat1).powi(2) + (lon2 - lon1).powi(2)).sqrt()
}

// ── Spatial measurements ─────────────────────────────────────────────────────

/// Compute ST_Distance between two geometries in km (uses Haversine for points).
/// Returns None if either geometry is invalid.
pub fn distance_km(geom1: &Value, geom2: &Value) -> Option<f64> {
    let coords1 = extract_geojson_coords(geom1);
    let coords2 = extract_geojson_coords(geom2);
    if coords1.is_empty() || coords2.is_empty() {
        return None;
    }

    // For point-to-point, use Haversine
    if coords1.len() == 1 && coords2.len() == 1 {
        return Some(haversine_km(
            coords1[0][0],
            coords1[0][1],
            coords2[0][0],
            coords2[0][1],
        ));
    }

    // For general case, find minimum distance between any two points
    let mut min_dist = f64::MAX;
    for c1 in &coords1 {
        for c2 in &coords2 {
            let d = euclidean_degrees(c1[0], c1[1], c2[0], c2[1]);
            if d < min_dist {
                min_dist = d;
            }
        }
    }
    // Convert degrees to km (approximate at mid-latitudes)
    Some(min_dist * 111.0)
}

/// Compute ST_Length of a LineString in km.
/// Returns None if geometry is not a LineString.
pub fn length_km(geom: &Value) -> Option<f64> {
    let coords = extract_geojson_coords(geom);
    if coords.len() < 2 {
        return None;
    }

    let mut total = 0.0;
    for i in 0..coords.len() - 1 {
        total += haversine_km(
            coords[i][0],
            coords[i][1],
            coords[i + 1][0],
            coords[i + 1][1],
        );
    }
    Some(total)
}

/// Compute ST_Area of a Polygon in square km using Shoelace formula.
/// Returns None if geometry is not a Polygon.
pub fn area_km2(geom: &Value) -> Option<f64> {
    let coords = extract_geojson_coords(geom);
    if coords.len() < 3 {
        return None;
    }

    // Shoelace formula works on [lon, lat] - returns area in degree-squared
    let mut area_deg2 = 0.0;
    let n = coords.len();
    for i in 0..n {
        let j = (i + 1) % n;
        area_deg2 += coords[i][1] * coords[j][0]; // lon * next_lat
        area_deg2 -= coords[j][1] * coords[i][0]; // next_lat * lon
    }
    area_deg2 = area_deg2.abs() / 2.0;

    // Convert degree² to km² (at mid-latitude, 1 degree lat ≈ 111km, 1 degree lon ≈ 111km * cos(lat))
    let avg_lat = coords.iter().map(|c| c[0]).sum::<f64>() / coords.len() as f64;
    let lat_factor = 111.0;
    let lon_factor = 111.0 * avg_lat.to_radians().cos();
    Some(area_deg2 * lat_factor * lon_factor)
}

// ── Centroid extraction ──────────────────────────────────────────────────────

/// Extract `(lat, lon)` centroid from a node payload via GeoJSON geometry.
pub fn extract_centroid(payload: &Value) -> Option<(f64, f64)> {
    let geom = payload.get("geometry")?;
    let coords = extract_geojson_coords(geom);
    if coords.is_empty() {
        return None;
    }
    let n = coords.len() as f64;
    let lat = coords.iter().map(|c| c[0]).sum::<f64>() / n;
    let lon = coords.iter().map(|c| c[1]).sum::<f64>() / n;
    Some((lat, lon))
}

// ── Spatial metadata ─────────────────────────────────────────────────────────

/// Cached spatial metadata for a node: centroid + axis-aligned bounding box.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SpatialMeta {
    pub centroid_lat: f64,
    pub centroid_lon: f64,
    pub bbox_min_lat: f64,
    pub bbox_min_lon: f64,
    pub bbox_max_lat: f64,
    pub bbox_max_lon: f64,
}

/// Extract spatial metadata from a node payload via GeoJSON geometry.
pub fn extract_spatial_meta(payload: &Value) -> Option<SpatialMeta> {
    let geom = payload.get("geometry")?;
    let coords = extract_geojson_coords(geom);
    if coords.is_empty() {
        return None;
    }
    let n = coords.len() as f64;
    let centroid_lat = coords.iter().map(|c| c[0]).sum::<f64>() / n;
    let centroid_lon = coords.iter().map(|c| c[1]).sum::<f64>() / n;
    let mut min_lat = f64::MAX;
    let mut max_lat = f64::MIN;
    let mut min_lon = f64::MAX;
    let mut max_lon = f64::MIN;
    for c in &coords {
        min_lat = min_lat.min(c[0]);
        max_lat = max_lat.max(c[0]);
        min_lon = min_lon.min(c[1]);
        max_lon = max_lon.max(c[1]);
    }
    Some(SpatialMeta {
        centroid_lat,
        centroid_lon,
        bbox_min_lat: min_lat,
        bbox_min_lon: min_lon,
        bbox_max_lat: max_lat,
        bbox_max_lon: max_lon,
    })
}

// ── Spatial grid (spatial hashing) ──────────────────────────────────────────

/// Grid-based spatial index using spatial hashing.
/// Maps `(cell_lat, cell_lon)` → `Vec<node_hash>` for fast candidate lookup.
pub(crate) struct SpatialGrid {
    cell_size: f64,
    cells: HashMap<(i32, i32), Vec<u64>>,
    meta: HashMap<u64, SpatialMeta>,
}

impl SpatialGrid {
    /// Build the grid from an iterator of `(node_hash, SpatialMeta)`.
    pub fn build(items: impl Iterator<Item = (u64, SpatialMeta)>) -> Self {
        let collected: Vec<(u64, SpatialMeta)> = items.collect();
        if collected.is_empty() {
            return Self {
                cell_size: 0.01,
                cells: HashMap::new(),
                meta: HashMap::new(),
            };
        }

        // Compute data extent for auto cell size
        let mut min_lat = f64::MAX;
        let mut max_lat = f64::MIN;
        let mut min_lon = f64::MAX;
        let mut max_lon = f64::MIN;
        for (_, m) in &collected {
            min_lat = min_lat.min(m.bbox_min_lat);
            max_lat = max_lat.max(m.bbox_max_lat);
            min_lon = min_lon.min(m.bbox_min_lon);
            max_lon = max_lon.max(m.bbox_max_lon);
        }
        let lat_range = max_lat - min_lat;
        let lon_range = max_lon - min_lon;
        let cell_size = (lat_range.max(lon_range) / 100.0).max(0.001);

        let mut grid = Self {
            cell_size,
            cells: HashMap::new(),
            meta: HashMap::new(),
        };

        for (hash, m) in collected {
            grid.insert_into_cells(hash, &m);
            grid.meta.insert(hash, m);
        }

        grid
    }

    /// Insert a node into the grid.
    pub fn insert(&mut self, hash: u64, meta: SpatialMeta) {
        self.insert_into_cells(hash, &meta);
        self.meta.insert(hash, meta);
    }

    /// Remove a node from the grid.
    pub fn remove(&mut self, hash: u64) {
        if let Some(meta) = self.meta.remove(&hash) {
            let cells = self.cells_for_bbox(&meta);
            for key in cells {
                if let Some(v) = self.cells.get_mut(&key) {
                    v.retain(|&h| h != hash);
                }
            }
        }
    }

    /// Get cached spatial metadata for a node.
    pub fn get_meta(&self, hash: u64) -> Option<&SpatialMeta> {
        self.meta.get(&hash)
    }

    /// Return candidate node hashes within `km` of `(lat, lon)`.
    pub fn candidates_within_distance(&self, lat: f64, lon: f64, km: f64) -> Vec<u64> {
        // Convert km to approximate degree range (conservative)
        let deg = km / 111.0; // 1 degree ≈ 111 km
        let lat_expand = deg;
        let lon_expand = deg / (lat.to_radians().cos().abs().max(0.01));

        self.candidates_in_bbox(
            lat - lat_expand,
            lon - lon_expand,
            lat + lat_expand,
            lon + lon_expand,
        )
    }

    /// Return candidate node hashes whose bbox overlaps the query bbox.
    pub fn candidates_in_bbox(
        &self,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
    ) -> Vec<u64> {
        let min_cell_lat = (min_lat / self.cell_size).floor() as i32;
        let max_cell_lat = (max_lat / self.cell_size).floor() as i32;
        let min_cell_lon = (min_lon / self.cell_size).floor() as i32;
        let max_cell_lon = (max_lon / self.cell_size).floor() as i32;

        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();

        for cy in min_cell_lat..=max_cell_lat {
            for cx in min_cell_lon..=max_cell_lon {
                if let Some(hashes) = self.cells.get(&(cy, cx)) {
                    for &h in hashes {
                        if seen.insert(h) {
                            // Bbox overlap check against the node's actual bbox
                            if let Some(m) = self.meta.get(&h) {
                                if m.bbox_max_lat >= min_lat
                                    && m.bbox_min_lat <= max_lat
                                    && m.bbox_max_lon >= min_lon
                                    && m.bbox_min_lon <= max_lon
                                {
                                    result.push(h);
                                }
                            }
                        }
                    }
                }
            }
        }

        result
    }

    /// Return candidate node hashes whose bbox could contain `(lat, lon)`.
    /// Checks the point's cell plus 8 neighbours, then applies bbox pre-filter.
    pub fn candidates_containing_point(&self, lat: f64, lon: f64) -> Vec<u64> {
        let cy = (lat / self.cell_size).floor() as i32;
        let cx = (lon / self.cell_size).floor() as i32;

        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();

        for dy in -1..=1i32 {
            for dx in -1..=1i32 {
                if let Some(hashes) = self.cells.get(&(cy + dy, cx + dx)) {
                    for &h in hashes {
                        if seen.insert(h) {
                            if let Some(m) = self.meta.get(&h) {
                                if lat >= m.bbox_min_lat
                                    && lat <= m.bbox_max_lat
                                    && lon >= m.bbox_min_lon
                                    && lon <= m.bbox_max_lon
                                {
                                    result.push(h);
                                }
                            }
                        }
                    }
                }
            }
        }

        result
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    fn cell_key(&self, lat: f64, lon: f64) -> (i32, i32) {
        (
            (lat / self.cell_size).floor() as i32,
            (lon / self.cell_size).floor() as i32,
        )
    }

    fn cells_for_bbox(&self, meta: &SpatialMeta) -> Vec<(i32, i32)> {
        let min_cy = (meta.bbox_min_lat / self.cell_size).floor() as i32;
        let max_cy = (meta.bbox_max_lat / self.cell_size).floor() as i32;
        let min_cx = (meta.bbox_min_lon / self.cell_size).floor() as i32;
        let max_cx = (meta.bbox_max_lon / self.cell_size).floor() as i32;

        // Cap at 10,000 cells to avoid blow-up for huge polygons
        let cell_count = (max_cy - min_cy + 1) as u64 * (max_cx - min_cx + 1) as u64;
        if cell_count > 10_000 {
            // Fall back to centroid cell only
            return vec![self.cell_key(meta.centroid_lat, meta.centroid_lon)];
        }

        let mut keys = Vec::with_capacity(cell_count as usize);
        for cy in min_cy..=max_cy {
            for cx in min_cx..=max_cx {
                keys.push((cy, cx));
            }
        }
        keys
    }

    fn insert_into_cells(&mut self, hash: u64, meta: &SpatialMeta) {
        let keys = self.cells_for_bbox(meta);
        for key in keys {
            self.cells.entry(key).or_default().push(hash);
        }
    }
}

// ── Point-in-polygon (ray casting) ───────────────────────────────────────────

/// Test whether a point is inside a polygon ring using the ray-casting algorithm.
///
/// Ring format: `[[lat, lon], ...]` (internal format, NOT GeoJSON `[lon, lat]`).
pub fn point_in_polygon(lat: f64, lon: f64, ring: &[[f64; 2]]) -> bool {
    let n = ring.len();
    if n < 3 {
        return false;
    }
    let mut inside = false;
    let mut j = n - 1;
    for i in 0..n {
        let (yi, xi) = (ring[i][0], ring[i][1]);
        let (yj, xj) = (ring[j][0], ring[j][1]);
        if ((yi > lat) != (yj > lat)) && (lon < (xj - xi) * (lat - yi) / (yj - yi) + xi) {
            inside = !inside;
        }
        j = i;
    }
    inside
}

// ── Segment intersection ─────────────────────────────────────────────────────

/// Test whether two line segments intersect.
/// Points are `[lat, lon]`.
fn segments_intersect(a1: [f64; 2], a2: [f64; 2], b1: [f64; 2], b2: [f64; 2]) -> bool {
    let d1 = cross(a1, a2, b1);
    let d2 = cross(a1, a2, b2);
    let d3 = cross(b1, b2, a1);
    let d4 = cross(b1, b2, a2);

    if ((d1 > 0.0 && d2 < 0.0) || (d1 < 0.0 && d2 > 0.0))
        && ((d3 > 0.0 && d4 < 0.0) || (d3 < 0.0 && d4 > 0.0))
    {
        return true;
    }

    // Collinear cases
    if d1 == 0.0 && on_segment(a1, a2, b1) {
        return true;
    }
    if d2 == 0.0 && on_segment(a1, a2, b2) {
        return true;
    }
    if d3 == 0.0 && on_segment(b1, b2, a1) {
        return true;
    }
    if d4 == 0.0 && on_segment(b1, b2, a2) {
        return true;
    }

    false
}

/// Cross product of vectors (p2-p1) x (p3-p1).
fn cross(p1: [f64; 2], p2: [f64; 2], p3: [f64; 2]) -> f64 {
    (p2[0] - p1[0]) * (p3[1] - p1[1]) - (p2[1] - p1[1]) * (p3[0] - p1[0])
}

/// Check if point `p` lies on segment `a`–`b` (assuming collinear).
fn on_segment(a: [f64; 2], b: [f64; 2], p: [f64; 2]) -> bool {
    p[0] >= a[0].min(b[0])
        && p[0] <= a[0].max(b[0])
        && p[1] >= a[1].min(b[1])
        && p[1] <= a[1].max(b[1])
}

// ── GeoJSON helpers ──────────────────────────────────────────────────────────

/// Flatten any GeoJSON geometry into a list of `[lat, lon]` points.
fn extract_geojson_coords(geom: &Value) -> Vec<[f64; 2]> {
    let geo_type = match geom.get("type").and_then(|t| t.as_str()) {
        Some(t) => t,
        None => return vec![],
    };
    let coords = match geom.get("coordinates") {
        Some(c) => c,
        None => return vec![],
    };

    match geo_type {
        "Point" => {
            // [lon, lat]
            if let (Some(lon), Some(lat)) = (
                coords.get(0).and_then(|v| v.as_f64()),
                coords.get(1).and_then(|v| v.as_f64()),
            ) {
                vec![[lat, lon]]
            } else {
                vec![]
            }
        }
        "LineString" | "MultiPoint" => {
            // [[lon, lat], ...]
            flatten_coord_array(coords)
        }
        "Polygon" => {
            // [[[lon, lat], ...], ...]  — first ring is outer
            coords
                .as_array()
                .map(|rings| {
                    rings
                        .iter()
                        .flat_map(|ring| flatten_coord_array(ring))
                        .collect()
                })
                .unwrap_or_default()
        }
        "MultiLineString" => coords
            .as_array()
            .map(|lines| {
                lines
                    .iter()
                    .flat_map(|line| flatten_coord_array(line))
                    .collect()
            })
            .unwrap_or_default(),
        "MultiPolygon" => {
            // [[[[lon, lat], ...], ...], ...]
            coords
                .as_array()
                .map(|polys| {
                    polys
                        .iter()
                        .flat_map(|poly| {
                            poly.as_array()
                                .map(|rings| {
                                    rings
                                        .iter()
                                        .flat_map(|ring| flatten_coord_array(ring))
                                        .collect::<Vec<_>>()
                                })
                                .unwrap_or_default()
                        })
                        .collect()
                })
                .unwrap_or_default()
        }
        _ => vec![],
    }
}

/// Convert a GeoJSON coordinate array `[[lon, lat], ...]` to `Vec<[lat, lon]>`.
fn flatten_coord_array(arr: &Value) -> Vec<[f64; 2]> {
    arr.as_array()
        .map(|pts| {
            pts.iter()
                .filter_map(|p| {
                    let lon = p.get(0)?.as_f64()?;
                    let lat = p.get(1)?.as_f64()?;
                    Some([lat, lon])
                })
                .collect()
        })
        .unwrap_or_default()
}

/// For Polygon/MultiPolygon, return the outer rings in `[lat, lon]` internal format.
fn extract_polygon_rings(geom: &Value) -> Vec<Vec<[f64; 2]>> {
    let geo_type = match geom.get("type").and_then(|t| t.as_str()) {
        Some(t) => t,
        None => return vec![],
    };
    let coords = match geom.get("coordinates") {
        Some(c) => c,
        None => return vec![],
    };

    match geo_type {
        "Polygon" => {
            // First element is the outer ring
            coords
                .as_array()
                .and_then(|rings| rings.first())
                .map(|ring| vec![flatten_coord_array(ring)])
                .unwrap_or_default()
        }
        "MultiPolygon" => coords
            .as_array()
            .map(|polys| {
                polys
                    .iter()
                    .filter_map(|poly| {
                        poly.as_array()?
                            .first()
                            .map(|ring| flatten_coord_array(ring))
                    })
                    .collect()
            })
            .unwrap_or_default(),
        _ => vec![],
    }
}

// ── High-level predicates ────────────────────────────────────────────────────

/// Node geometry contains query point (reverse geocoding).
///
/// For Polygon: point-in-polygon test.
/// For MultiPolygon: any polygon contains the point.
pub fn geom_contains_point(payload: &Value, lat: f64, lon: f64) -> bool {
    let geom = match payload.get("geometry") {
        Some(g) => g,
        None => return false,
    };
    let rings = extract_polygon_rings(geom);
    rings.iter().any(|ring| point_in_polygon(lat, lon, ring))
}

/// Node geometry completely within query polygon.
///
/// For Point: centroid inside ring.
/// For Polygon/LineString: all vertices inside ring.
pub fn geom_within_polygon(payload: &Value, ring: &[[f64; 2]]) -> bool {
    let geom = match payload.get("geometry") {
        Some(g) => g,
        None => return false,
    };
    let coords = extract_geojson_coords(geom);
    if coords.is_empty() {
        return false;
    }
    coords.iter().all(|c| point_in_polygon(c[0], c[1], ring))
}

/// Node geometry intersects query polygon.
///
/// True if: any vertex of node inside query, or any vertex of query inside node,
/// or any edge of node crosses any edge of query.
pub fn geom_intersects_polygon(payload: &Value, ring: &[[f64; 2]]) -> bool {
    let geom = match payload.get("geometry") {
        Some(g) => g,
        None => return false,
    };

    let node_coords = extract_geojson_coords(geom);
    if node_coords.is_empty() {
        return false;
    }

    // Any vertex of node geometry inside query polygon
    if node_coords
        .iter()
        .any(|c| point_in_polygon(c[0], c[1], ring))
    {
        return true;
    }

    // Any vertex of query polygon inside node geometry (if node is a polygon)
    let node_rings = extract_polygon_rings(geom);
    for nr in &node_rings {
        if ring.iter().any(|c| point_in_polygon(c[0], c[1], nr)) {
            return true;
        }
    }

    // Edge crossing: any segment of node geometry crosses any segment of query polygon
    let node_edges = edges_from_coords(&node_coords);
    let query_edges = edges_from_ring(ring);
    for (a1, a2) in &node_edges {
        for (b1, b2) in &query_edges {
            if segments_intersect(*a1, *a2, *b1, *b2) {
                return true;
            }
        }
    }

    false
}

/// Node geometry contains query polygon.
///
/// All query polygon vertices must be inside the node's geometry.
pub fn geom_contains_polygon(payload: &Value, ring: &[[f64; 2]]) -> bool {
    let geom = match payload.get("geometry") {
        Some(g) => g,
        None => return false,
    };
    let node_rings = extract_polygon_rings(geom);
    if node_rings.is_empty() || ring.is_empty() {
        return false;
    }
    // All query vertices must be inside at least one of the node's polygon rings
    ring.iter()
        .all(|c| node_rings.iter().any(|nr| point_in_polygon(c[0], c[1], nr)))
}

// ── Edge helpers ─────────────────────────────────────────────────────────────

/// Build edges from a list of coordinates (connecting consecutive pairs).
fn edges_from_coords(coords: &[[f64; 2]]) -> Vec<([f64; 2], [f64; 2])> {
    if coords.len() < 2 {
        return vec![];
    }
    coords.windows(2).map(|w| (w[0], w[1])).collect()
}

/// Build edges from a polygon ring (including the closing edge).
fn edges_from_ring(ring: &[[f64; 2]]) -> Vec<([f64; 2], [f64; 2])> {
    if ring.len() < 2 {
        return vec![];
    }
    let mut edges: Vec<([f64; 2], [f64; 2])> = ring.windows(2).map(|w| (w[0], w[1])).collect();
    // Close the ring
    if let (Some(&first), Some(&last)) = (ring.first(), ring.last()) {
        if first != last {
            edges.push((last, first));
        }
    }
    edges
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_haversine_melbourne_to_geelong() {
        // Melbourne CBD to Geelong ~ 65 km
        let d = haversine_km(-37.8136, 144.9631, -38.1499, 144.3617);
        assert!((d - 65.0).abs() < 5.0, "expected ~65km, got {d}km");
    }

    #[test]
    fn test_haversine_same_point() {
        let d = haversine_km(-37.8136, 144.9631, -37.8136, 144.9631);
        assert!(d < 0.001);
    }

    #[test]
    fn test_point_in_polygon_inside() {
        // Simple square around Melbourne CBD
        let ring = [
            [-37.80, 144.95],
            [-37.80, 144.98],
            [-37.83, 144.98],
            [-37.83, 144.95],
        ];
        assert!(point_in_polygon(-37.81, 144.96, &ring));
    }

    #[test]
    fn test_point_in_polygon_outside() {
        let ring = [
            [-37.80, 144.95],
            [-37.80, 144.98],
            [-37.83, 144.98],
            [-37.83, 144.95],
        ];
        // Geelong is outside
        assert!(!point_in_polygon(-38.15, 144.36, &ring));
    }

    #[test]
    fn test_extract_centroid_point() {
        let payload = json!({
            "geometry": {
                "type": "Point",
                "coordinates": [144.9631, -37.8136]
            }
        });
        let (lat, lon) = extract_centroid(&payload).unwrap();
        assert!((lat - (-37.8136)).abs() < 1e-4);
        assert!((lon - 144.9631).abs() < 1e-4);
    }

    #[test]
    fn test_extract_centroid_polygon() {
        let payload = json!({
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [144.95, -37.80],
                    [144.98, -37.80],
                    [144.98, -37.83],
                    [144.95, -37.83],
                    [144.95, -37.80]
                ]]
            }
        });
        let (lat, lon) = extract_centroid(&payload).unwrap();
        // Average of all 5 vertices (including closing = first)
        assert!((lat - (-37.812)).abs() < 0.01, "lat={lat}");
        assert!((lon - 144.962).abs() < 0.01, "lon={lon}");
    }

    #[test]
    fn test_extract_centroid_multipoint() {
        let payload = json!({
            "geometry": {
                "type": "MultiPoint",
                "coordinates": [
                    [144.9631, -37.8136],
                    [144.9700, -37.8200],
                    [144.9800, -37.8300]
                ]
            }
        });
        let (lat, lon) = extract_centroid(&payload).unwrap();
        assert!((lat - (-37.8212)).abs() < 0.001, "lat={lat}");
        assert!((lon - 144.9710).abs() < 0.001, "lon={lon}");
    }

    #[test]
    fn test_extract_centroid_multipolygon() {
        let payload = json!({
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [[
                    [[144.95, -37.80], [144.98, -37.80], [144.98, -37.83], [144.95, -37.83], [144.95, -37.80]]
                ], [
                    [[145.00, -37.85], [145.03, -37.85], [145.03, -37.88], [145.00, -37.88], [145.00, -37.85]]
                ]]
            }
        });
        let (lat, lon) = extract_centroid(&payload).unwrap();
        assert!((lat - (-37.84)).abs() < 0.01, "lat={lat}");
        assert!((lon - 144.99).abs() < 0.01, "lon={lon}");
    }

    #[test]
    fn test_extract_centroid_multilinestring() {
        let payload = json!({
            "geometry": {
                "type": "MultiLineString",
                "coordinates": [
                    [[144.95, -37.80], [144.98, -37.80]],
                    [[144.96, -37.81], [144.99, -37.81]]
                ]
            }
        });
        let (lat, lon) = extract_centroid(&payload).unwrap();
        assert!((lat - (-37.805)).abs() < 0.01, "lat={lat}");
        assert!((lon - 144.97).abs() < 0.01, "lon={lon}");
    }

    #[test]
    fn test_segments_intersect() {
        // X-shaped crossing
        assert!(segments_intersect(
            [0.0, 0.0],
            [1.0, 1.0],
            [0.0, 1.0],
            [1.0, 0.0],
        ));
    }

    #[test]
    fn test_segments_no_intersect() {
        // Parallel segments
        assert!(!segments_intersect(
            [0.0, 0.0],
            [1.0, 0.0],
            [0.0, 1.0],
            [1.0, 1.0],
        ));
    }

    #[test]
    fn test_geom_contains_point_polygon() {
        let payload = json!({
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [144.95, -37.80],
                    [144.98, -37.80],
                    [144.98, -37.83],
                    [144.95, -37.83],
                    [144.95, -37.80]
                ]]
            }
        });
        assert!(geom_contains_point(&payload, -37.81, 144.96));
        assert!(!geom_contains_point(&payload, -38.15, 144.36));
    }

    #[test]
    fn test_geom_within_polygon() {
        let ring = [
            [-37.80, 144.94],
            [-37.80, 144.99],
            [-37.84, 144.99],
            [-37.84, 144.94],
        ];
        // Point inside big ring
        let payload = json!({
            "geometry": {
                "type": "Point",
                "coordinates": [144.96, -37.81]
            }
        });
        assert!(geom_within_polygon(&payload, &ring));

        // Point outside big ring
        let outside = json!({
            "geometry": {
                "type": "Point",
                "coordinates": [145.50, -38.00]
            }
        });
        assert!(!geom_within_polygon(&outside, &ring));
    }

    #[test]
    fn test_geom_intersects_polygon() {
        // A line that crosses a query rectangle
        let payload = json!({
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [144.94, -37.81],
                    [144.99, -37.81]
                ]
            }
        });
        let ring = [
            [-37.80, 144.95],
            [-37.80, 144.98],
            [-37.83, 144.98],
            [-37.83, 144.95],
        ];
        assert!(geom_intersects_polygon(&payload, &ring));
    }
}
