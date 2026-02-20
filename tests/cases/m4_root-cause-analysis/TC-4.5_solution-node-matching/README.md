# TC-4.5: Solution Node Matching

Goal: Find solution nodes via vector similarity.
Given: Cause node vector.
When: Searching for solution nodes.
Then: Return best matching solutions.

Preconditions:
- Solution nodes indexed with vectors.

Steps:
1. Insert cause node "Lack of Sports Centers" with vector.
2. Insert solution node "Build Community Sports Center" with similar vector.
3. Run vector similarity search.

Expected:
- Solution node is top result with high similarity score.
