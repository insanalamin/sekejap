# TC-2.4: Hierarchy Edge Creation

Goal: Create causal edge during fusion.
Given: Cause node and Effect node identified.
When: Fusion identifies causal relationship.
Then: Directed edge Cause -> Effect is created.

Preconditions:
- Edge store enabled.

Test Data:
- Cause: events/rainstorm-2024
- Effect: events/flood-2024
- Edge type: caused_by

Steps:
1. Insert cause and effect nodes.
2. Create causal relationship via fusion logic.
3. Traverse forward from cause with edge type caused_by.

Expected:
- Effect node appears in traversal results.
- Edge weight matches fusion confidence.
