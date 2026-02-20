# TC-4.3: Evidence Weight Thresholding

Goal: Filter causal edges by weight threshold.
Given: Edges with weights 0.3, 0.7, 0.9.
When: Traversing with threshold 0.6.
Then: Only edges >= 0.6 are followed.

Preconditions:
- Edge weights stored.

Steps:
1. Create edges with weights 0.3, 0.7, 0.9.
2. Traverse with threshold 0.6.

Expected:
- Only edges with weights 0.7 and 0.9 are traversed.
