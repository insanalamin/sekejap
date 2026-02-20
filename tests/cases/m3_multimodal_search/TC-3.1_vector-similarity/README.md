# TC-3.1: Vector Similarity Search

Goal: Return top N nodes by cosine similarity.
Given: Nodes with 1536-dim vectors.
When: Querying with vector V.
Then: Return top N sorted by similarity.

Preconditions:
- Vector index enabled.

Test Data:
- 3 nodes with vectors where node B is closest to query vector.

Steps:
1. Insert nodes with vectors.
2. Query vector search with k=2.

Expected:
- Results contain top 2 nearest nodes.
- Node B appears before others.
