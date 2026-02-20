# TC-3.3: Time-Based Filtering

Goal: Return nodes within a time window.
Given: Nodes spanning multiple years.
When: Querying with [start, end].
Then: Return only nodes in that window.

Preconditions:
- Timestamp field present and indexed.

Steps:
1. Insert nodes dated 2019, 2022, 2024.
2. Query window 2021-01-01 to 2023-12-31.

Expected:
- Only 2022 node returned.
