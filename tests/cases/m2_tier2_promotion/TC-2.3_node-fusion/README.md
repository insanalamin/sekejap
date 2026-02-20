# TC-2.3: Node Fusion

Goal: Merge two duplicate nodes into one.
Given: Two duplicates with overlapping entities.
When: Fusing them into a single node.
Then: Result merges entities, uses latest timestamp, and preserves vector.

Preconditions:
- Duplicate candidates identified.

Test Data:
- Node A: {"_id":"events/accident-1","who":["Driver A"],"when":"2024-05-01T10:00:00Z","vector_id":101}
- Node B: {"_id":"events/accident-2","who":["Driver A","Passenger B"],"when":"2024-05-01T11:00:00Z","vector_id":102}

Steps:
1. Fuse Node A and Node B.
2. Read fused node.

Expected:
- Fused node contains union of entities (Driver A, Passenger B).
- Timestamp equals the latest source timestamp.
- Vector is selected by policy (latest or merged).
- Original nodes are tombstoned or aliased to fused node.
