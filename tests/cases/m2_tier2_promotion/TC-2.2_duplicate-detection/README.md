# TC-2.2: Duplicate Detection

Goal: Identify candidate duplicates in Tier 1.
Given: Multiple nodes with highly similar content.
When: Running duplicate detection.
Then: The system surfaces duplicate candidate pairs for fusion.

Preconditions:
- Duplicate detection job enabled.

Test Data:
- Node A:
```json
{"_id":"events/riot-001","summary":"Riot near station","who":["Group A"],"where":"Station X"}
```
- Node B:
```json
{"_id":"events/riot-002","summary":"Riot at Station X","who":["Group A"],"where":"Station X"}
```

Steps:
1. Insert both nodes into Tier 1.
2. Run duplicate detection.

Expected:
- The duplicate detector flags A and B as candidates.
- Similarity score exceeds configured threshold.
