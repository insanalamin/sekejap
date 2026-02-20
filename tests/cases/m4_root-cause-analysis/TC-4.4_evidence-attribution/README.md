# TC-4.4: Evidence Attribution

Goal: Resolve evidence pointers on causal edges.
Given: Edge with evidence pointers.
When: Dereferencing evidence.
Then: Return list of source IDs.

Preconditions:
- Evidence store available.

Test Data:
- Edge evidence_ptrs: ["paper-123", "news-456"]

Steps:
1. Create edge with evidence pointers.
2. Request evidence for that edge.

Expected:
- Evidence IDs returned in stored order.
- Missing evidence IDs return a clear error.
