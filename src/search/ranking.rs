// Cascade ranking (Meilisearch-style) is implemented in SearchIndex::score().
// Rules in priority order: words → typo → proximity → field_order → exactness.
// Each rule maps to [0.0, 1.0] and occupies a separate magnitude band (1e12..1e0).
