//! Geometry utilities: GeoJSON parsing, bbox/centroid extraction, spatial predicates.
//!
//! Nodes carry geometry in two forms:
//! - `"geometry"` field: standard GeoJSON object (any geometry type)
//! - `"geo": {"loc": {"lat": …, "lon": …}}` or `"coords": {"lat": …, "lon": …}`: legacy point
//!
//! All coordinates are stored as f32 for index efficiency; predicates use f64 for accuracy.

use geo::{
    algorithm::{
        bounding_rect::BoundingRect, centroid::Centroid, contains::Contains,
        intersects::Intersects,
    },
    Coord, LineString, Point, Polygon,
};
use serde_json::Value;

/// Spatial metadata extracted from a node payload.
#[derive(Debug, Clone, Copy)]
pub struct GeoInfo {
    /// Centroid latitude (y) — used for `near` and `st_dwithin`.
    pub centroid_lat: f32,
    /// Centroid longitude (x).
    pub centroid_lon: f32,
    /// Bounding envelope (min / max corners). For a point, min == max == centroid.
    pub bbox_min_lat: f32,
    pub bbox_min_lon: f32,
    pub bbox_max_lat: f32,
    pub bbox_max_lon: f32,
}

/// Extract spatial metadata from a node JSON payload.
///
/// Resolution order:
/// 1. `"geometry"` — GeoJSON object (any type). Centroid + bounding rect are computed.
/// 2. `"geo.loc"` — legacy `{lat, lon}` point.
/// 3. `"coords"` — alternative `{lat, lon}` point.
///
/// Returns `None` if no usable geometry is found.
pub fn extract_geo_info(value: &Value) -> Option<GeoInfo> {
    // ── GeoJSON geometry field ──────────────────────────────────────────────
    if let Some(geom_val) = value.get("geometry") {
        if let Some(info) = parse_geojson_value(geom_val) {
            return Some(info);
        }
    }

    // ── Legacy point: geo.loc.lat / geo.loc.lon ────────────────────────────
    if let Some(loc) = value.get("geo").and_then(|g| g.get("loc")) {
        if let (Some(lat), Some(lon)) = (
            loc.get("lat").and_then(|v| v.as_f64()),
            loc.get("lon").and_then(|v| v.as_f64()),
        ) {
            return Some(point_info(lat as f32, lon as f32));
        }
    }

    // ── Alternative: coords.lat / coords.lon ──────────────────────────────
    if let Some(coords) = value.get("coords") {
        if let (Some(lat), Some(lon)) = (
            coords.get("lat").and_then(|v| v.as_f64()),
            coords.get("lon").and_then(|v| v.as_f64()),
        ) {
            return Some(point_info(lat as f32, lon as f32));
        }
    }

    // ── Alternative: coordinates.lat / coordinates.lon (legacy custom field) ──
    // Note: standard GeoJSON uses "coordinates" as an array; we only check if it's an object.
    if let Some(coords) = value.get("coordinates") {
        if coords.is_object() {
            if let (Some(lat), Some(lon)) = (
                coords.get("lat").and_then(|v| v.as_f64()),
                coords.get("lon").and_then(|v| v.as_f64()),
            ) {
                return Some(point_info(lat as f32, lon as f32));
            }
        }
    }

    None
}

/// Parse a raw `serde_json::Value` that represents a GeoJSON geometry object.
pub fn parse_geojson_value(geom_val: &Value) -> Option<GeoInfo> {
    let geom_str = serde_json::to_string(geom_val).ok()?;
    let gj_geom: geojson::Geometry = serde_json::from_str(&geom_str).ok()?;
    let geo_geom = geo::Geometry::<f64>::try_from(gj_geom).ok()?;
    extract_from_geo_geometry(&geo_geom)
}

// ============================================================================
// Spatial predicates (PostGIS-par)
// ============================================================================

/// True if the node's geometry is **completely within** the query polygon.
/// For nodes without a `"geometry"` field, the centroid point is tested.
pub fn geom_within_polygon(node_json: &Value, query_ring: &[[f32; 2]]) -> bool {
    let query_poly = ring_to_polygon(query_ring);
    match node_geometry(node_json) {
        Some(geom) => all_coords_within(&geom, &query_poly),
        None => {
            let (lat, lon) = centroid_from_json(node_json);
            let pt = Point::new(lon as f64, lat as f64);
            query_poly.contains(&pt)
        }
    }
}

/// True if the node's geometry **contains** the entire query polygon.
/// Only meaningful for Polygon / MultiPolygon node geometries.
pub fn geom_contains_polygon(node_json: &Value, query_ring: &[[f32; 2]]) -> bool {
    let query_poly = ring_to_polygon(query_ring);
    match node_geometry(node_json) {
        Some(geo::Geometry::Polygon(poly)) => {
            // query polygon vertices all within node polygon
            all_coords_within(&geo::Geometry::Polygon(query_poly), &poly)
        }
        Some(geo::Geometry::MultiPolygon(mpoly)) => mpoly
            .0
            .iter()
            .any(|p| all_coords_within(&geo::Geometry::Polygon(query_poly.clone()), p)),
        _ => false,
    }
}

/// True if the node's geometry **intersects** the query polygon.
pub fn geom_intersects_polygon(node_json: &Value, query_ring: &[[f32; 2]]) -> bool {
    let query_poly = ring_to_polygon(query_ring);
    match node_geometry(node_json) {
        Some(geom) => match geom {
            geo::Geometry::Point(pt) => query_poly.intersects(&pt),
            geo::Geometry::MultiPoint(mp) => mp.0.iter().any(|pt| query_poly.intersects(pt)),
            geo::Geometry::LineString(ls) => query_poly.intersects(&ls),
            geo::Geometry::MultiLineString(mls) => {
                mls.0.iter().any(|ls| query_poly.intersects(ls))
            }
            geo::Geometry::Polygon(poly) => query_poly.intersects(&poly),
            geo::Geometry::MultiPolygon(mpoly) => {
                mpoly.0.iter().any(|p| query_poly.intersects(p))
            }
            geo::Geometry::GeometryCollection(gc) => gc
                .0
                .iter()
                .any(|g| geom_intersects_geom(g, &query_poly)),
            _ => false,
        },
        None => {
            let (lat, lon) = centroid_from_json(node_json);
            let pt = Point::new(lon as f64, lat as f64);
            query_poly.intersects(&pt)
        }
    }
}

// ============================================================================
// Internal helpers
// ============================================================================

fn point_info(lat: f32, lon: f32) -> GeoInfo {
    GeoInfo {
        centroid_lat: lat,
        centroid_lon: lon,
        bbox_min_lat: lat,
        bbox_min_lon: lon,
        bbox_max_lat: lat,
        bbox_max_lon: lon,
    }
}

fn extract_from_geo_geometry(geom: &geo::Geometry<f64>) -> Option<GeoInfo> {
    let bbox = geom.bounding_rect()?;
    let centroid = geom.centroid()?;
    Some(GeoInfo {
        centroid_lat: centroid.y() as f32,
        centroid_lon: centroid.x() as f32,
        bbox_min_lat: bbox.min().y as f32,
        bbox_min_lon: bbox.min().x as f32,
        bbox_max_lat: bbox.max().y as f32,
        bbox_max_lon: bbox.max().x as f32,
    })
}

/// Parse GeoJSON geometry from the node's `"geometry"` field.
fn node_geometry(node_json: &Value) -> Option<geo::Geometry<f64>> {
    let geom_val = node_json.get("geometry")?;
    let geom_str = serde_json::to_string(geom_val).ok()?;
    let gj_geom: geojson::Geometry = serde_json::from_str(&geom_str).ok()?;
    geo::Geometry::<f64>::try_from(gj_geom).ok()
}

/// Extract centroid coordinates from legacy point fields.
fn centroid_from_json(node_json: &Value) -> (f32, f32) {
    if let Some(info) = extract_geo_info(node_json) {
        return (info.centroid_lat, info.centroid_lon);
    }
    (0.0, 0.0)
}

/// Convert our `[lat, lon]` ring array into a `geo::Polygon`.
/// In geo crate: x = longitude, y = latitude.
pub fn ring_to_polygon(ring: &[[f32; 2]]) -> Polygon<f64> {
    let coords: Vec<Coord<f64>> = ring
        .iter()
        .map(|p| Coord {
            x: p[1] as f64, // lon → x
            y: p[0] as f64, // lat → y
        })
        .collect();
    Polygon::new(LineString::from(coords), vec![])
}

/// Collect all coordinate points from a geometry (for within tests).
fn coords_of(geom: &geo::Geometry<f64>) -> Vec<Point<f64>> {
    let mut pts = Vec::new();
    match geom {
        geo::Geometry::Point(pt) => pts.push(*pt),
        geo::Geometry::MultiPoint(mp) => pts.extend(mp.0.iter().copied()),
        geo::Geometry::LineString(ls) => pts.extend(ls.points()),
        geo::Geometry::MultiLineString(mls) => {
            for ls in &mls.0 {
                pts.extend(ls.points());
            }
        }
        geo::Geometry::Polygon(poly) => {
            pts.extend(poly.exterior().points());
            for ring in poly.interiors() {
                pts.extend(ring.points());
            }
        }
        geo::Geometry::MultiPolygon(mpoly) => {
            for poly in &mpoly.0 {
                pts.extend(poly.exterior().points());
                for ring in poly.interiors() {
                    pts.extend(ring.points());
                }
            }
        }
        geo::Geometry::GeometryCollection(gc) => {
            for g in &gc.0 {
                pts.extend(coords_of(g));
            }
        }
        _ => {}
    }
    pts
}

/// Returns true if ALL vertices of `geom` are inside `poly`.
fn all_coords_within(geom: &geo::Geometry<f64>, poly: &Polygon<f64>) -> bool {
    let pts = coords_of(geom);
    if pts.is_empty() {
        return false;
    }
    pts.iter().all(|pt| poly.contains(pt))
}

fn geom_intersects_geom(geom: &geo::Geometry<f64>, query_poly: &Polygon<f64>) -> bool {
    match geom {
        geo::Geometry::Point(pt) => query_poly.intersects(pt),
        geo::Geometry::LineString(ls) => query_poly.intersects(ls),
        geo::Geometry::Polygon(poly) => query_poly.intersects(poly),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_point_geo_info() {
        let v = json!({"geo": {"loc": {"lat": 3.1291, "lon": 101.6710}}});
        let info = extract_geo_info(&v).unwrap();
        assert!((info.centroid_lat - 3.1291).abs() < 1e-4);
        assert!((info.centroid_lon - 101.671).abs() < 1e-4);
        assert_eq!(info.bbox_min_lat, info.centroid_lat);
        assert_eq!(info.bbox_max_lat, info.centroid_lat);
    }

    #[test]
    fn test_geojson_polygon_geo_info() {
        let v = json!({
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[101.665, 3.128], [101.678, 3.128], [101.678, 3.135], [101.665, 3.135], [101.665, 3.128]]
                ]
            }
        });
        let info = extract_geo_info(&v).unwrap();
        assert!(info.bbox_min_lon < info.bbox_max_lon);
        assert!(info.bbox_min_lat < info.bbox_max_lat);
    }

    #[test]
    fn test_point_within_polygon() {
        let polygon = vec![
            [3.128f32, 101.665],
            [3.128, 101.678],
            [3.135, 101.678],
            [3.135, 101.665],
            [3.128, 101.665],
        ];
        let point_inside = json!({"geo": {"loc": {"lat": 3.131, "lon": 101.671}}});
        let point_outside = json!({"geo": {"loc": {"lat": 3.100, "lon": 101.600}}});
        assert!(geom_within_polygon(&point_inside, &polygon));
        assert!(!geom_within_polygon(&point_outside, &polygon));
    }

    #[test]
    fn test_linestring_intersects_polygon() {
        let polygon = vec![
            [3.128f32, 101.665],
            [3.128, 101.678],
            [3.135, 101.678],
            [3.135, 101.665],
            [3.128, 101.665],
        ];
        let line_node = json!({
            "geometry": {
                "type": "LineString",
                "coordinates": [[101.660, 3.130], [101.680, 3.132]]
            }
        });
        assert!(geom_intersects_polygon(&line_node, &polygon));
    }

    #[test]
    fn test_geojson_point_geo_info() {
        let v = json!({
            "geometry": {
                "type": "Point",
                "coordinates": [101.6710, 3.1291]
            }
        });
        let info = extract_geo_info(&v).unwrap();
        assert!((info.centroid_lat - 3.1291).abs() < 1e-4);
        assert!((info.centroid_lon - 101.671).abs() < 1e-4);
    }
}
