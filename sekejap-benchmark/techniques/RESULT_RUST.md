# Rust Benchmark

Compare Sekejap Atomic vs Sekejap SQL vs SQLite on graph, vector, spatial, exact time, vague time, fulltext, and hybrid scenarios.

| Case | Sekejap Atomic ms | Sekejap SQL ms | SQLite ms | Note |
|---|---:|---:|---:|---|
| insert_multimodal_1000 | 567.856 | 645.922 | 48.748 | 1,000 records with vector, spatial, exact time, vague time, and fulltext payload |
| graph_traverse | 1.169 | 5045.112 | 2.573 | source node forward traversal repeated 100x |
| vector_similarity | 436165.299 | 1.531 | 895.824 | atomic executes; SQL uses explain-only because vector SQL runtime is unstable; SQLite brute-forces cosine |
| spatial_distance | 51.614 | 453.213 | 6.015 | spatial radius query repeated 80x |
| exact_time_range | 12305.418 | 5016.390 | 2.478 | exact timestamp range repeated 100x |
| vague_time_intersects | 10158.641 | 876.210 | 12.507 | vague-time overlap query repeated 80x |
| fulltext_match | 10.580 | 131.133 | 4.428 | fulltext search repeated 50x |
| hybrid_vector_spatial_exact | 431381.463 | 3.569 | 45.261 | atomic executes; SQL uses explain-only because vector runtime is unstable; SQLite brute-forces hybrid |
| hybrid_fulltext_vague_spatial | 3183.870 | 47.529 | 15.385 | fulltext + vague time + spatial hybrid repeated 25x |
| hybrid_graph_fulltext_exact | 12.648 | 1266.755 | 45.743 | graph traversal + fulltext + exact time repeated 25x |
