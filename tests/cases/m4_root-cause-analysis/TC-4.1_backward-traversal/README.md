# TC-4.1: Backward Edge Traversal

Goal: Return all ancestors in a causal chain.
Given: Node with incoming causal edges.
When: Tracing backward 3 hops.
Then: Return all ancestor nodes.

Preconditions:
- Causal edges stored.

Steps:
1. Create chain A -> B -> C -> D where D is target.
2. Traverse backward from D with hops=3.

Expected:
- Results include C, B, A.
