# TC-5.4: B+Tree Size vs Payload Size

Goal: Validate identity vs payload split reduces index size.
Given: 1M nodes with 10KB payloads each.
When: Measuring B+Tree size.
Then: B+Tree should be ~100x smaller than payload size.

Preconditions:
- B+Tree or primary index implemented.
- Benchmark dataset prepared.

Steps:
1. Insert 1M nodes with 10KB payloads.
2. Measure index size and payload size.

Expected:
- Index size <= 1% of payload size.
- Index size growth is linear with number of nodes, not payload size.
