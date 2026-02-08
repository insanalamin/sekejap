//! Hashing utilities for Sekejap-DB

/// Hash for slugs using SeaHash
pub fn hash_slug(slug: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    slug.hash(&mut hasher);
    hasher.finish()
}

/// Simple spatial hash from coordinates (hyperminimalist geohash-like)
pub fn hash_spatial(lat: f64, lon: f64) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    (lat.to_bits()).hash(&mut hasher);
    (lon.to_bits()).hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_slug() {
        let h1 = hash_slug("jakarta-crime-2024");
        let h2 = hash_slug("jakarta-crime-2024");
        let h3 = hash_slug("different-slug");

        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_hash_spatial() {
        let h1 = hash_spatial(-6.2088, 106.8456);
        let h2 = hash_spatial(-6.2088, 106.8456);
        let h3 = hash_spatial(-6.2089, 106.8456);

        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
    }
}
