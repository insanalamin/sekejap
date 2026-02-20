# TC-4.6: Full RCA Workflow

Goal: Execute full RCA pipeline end-to-end.
Given: Problem node with causal edges and solution nodes.
When: Running RCA with 5-year window and weight threshold 0.6.
Then: Return causal chain with evidence and solutions.

Preconditions:
- Graph populated with causal edges and evidence.
- Solution vectors indexed.

Steps:
1. Traverse backward 5 hops with time window and weight threshold.
2. Collect evidence for each edge.
3. Run solution matching on root causes.

Expected:
- RCA output includes causal chain and evidence.
- At least one solution recommendation returned.
